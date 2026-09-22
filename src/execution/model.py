"""Static execution manifests, not worker code or a runtime state machine.

DAG records remain authoritative. In particular, an isolation *candidate* still
needs a semantics-preserving code adapter, input preparation and binding commits.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
import hashlib
import json
from types import MappingProxyType
from typing import Mapping

from dag_runtime.dag_model import (
    BindingEvent, Certainty, DAG, Definition, DependencyEdge, EffectKind,
    GraphValidationError, TaskNode, Value,
)


class ExecutionValidationError(ValueError):
    """An execution contract is inconsistent or cannot represent this DAG."""


class ExecutionMode(str, Enum):
    ISOLATED_CANDIDATE = "isolated_candidate"
    SHARED_CONTEXT = "shared_context"
    NATIVE_REGION = "native_region"


class ValueKind(str, Enum):
    IMMUTABLE = "immutable"
    SHARED_REFERENCE = "shared_reference"
    CODE_BINDING = "code_binding"
    NATIVE_REFERENCE = "native_reference"
    NAMESPACE_STATE = "namespace_state"
    COMPLETION_STATE = "completion_state"
    OBJECT_STATE = "object_state"
    DISCARDED_RESULT = "discarded_result"


class ObjectAccess(str, Enum):
    SNAPSHOT_CANDIDATE = "snapshot_candidate"
    LIVE_REFERENCE = "live_reference"
    MUTATE = "mutate"


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ExecutionValidationError(message)


def _nonempty(value: str, label: str) -> None:
    _require(isinstance(value, str) and bool(value.strip()), f"{label} must be nonempty text")


def _sha256(value: str, label: str) -> None:
    _require(isinstance(value, str) and len(value) == 64
             and all(c in "0123456789abcdef" for c in value), f"Invalid {label} SHA-256")


def _digest(record: dict) -> str:
    payload = json.dumps(record, sort_keys=True, ensure_ascii=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _graph_digest(dag: DAG) -> str:
    # Includes analysis outcomes, not only source: budgets can change the graph.
    return _digest(dag.to_dict())


@dataclass(frozen=True)
class ProgramIdentity:
    """Content identity plus caller-supplied environment/package compatibility keys.

    source_sha256 hashes the exact UTF-8 encoded DAG source string, not original
    file bytes. Environment/package keys are requirements, not verified installs.
    """
    source_sha256: str
    filename: str
    environment_id: str
    package_id: str | None = None
    id: str = field(init=False)

    def __post_init__(self):
        _sha256(self.source_sha256, "source")
        _nonempty(self.filename, "filename")
        _nonempty(self.environment_id, "environment_id")
        if self.package_id is not None:
            _nonempty(self.package_id, "package_id")
        object.__setattr__(self, "id", _digest({
            "source": self.source_sha256, "filename": self.filename,
            "environment": self.environment_id, "package": self.package_id,
        }))


@dataclass(frozen=True)
class CodeRequirement:
    """Original program context and exact definition versions, never a callable name lookup."""
    program_id: str
    definition_ids: tuple[str, ...] = ()

    def __post_init__(self):
        _sha256(self.program_id, "program")
        object.__setattr__(self, "definition_ids", tuple(self.definition_ids))
        for ident in self.definition_ids:
            _nonempty(ident, "definition_id")
        _require(len(set(self.definition_ids)) == len(self.definition_ids), "Duplicate code definitions")


@dataclass(frozen=True)
class ValueRequirement:
    """Logical value semantics; no Python object, serialization or location is stored.

    A native reference can be absent and must not be eagerly captured. State
    tokens establish ordering/context, not transferable data. Alias derivations
    and unpacking projections remain on the original Value record.
    """
    value: Value
    kind: ValueKind = field(init=False)

    def __post_init__(self):
        v = self.value
        _require(isinstance(v, Value), "Value record is required")
        _require(v.origin in {"result", "alias", "definition", "builtin", "external",
                              "namespace", "state", "object_state"}, f"Unknown value origin: {v.origin}")
        if v.origin in {"external", "namespace", "state", "object_state"}:
            _require(v.storage == "native_namespace", f"Native value has non-native storage: {v.id}")
        if v.origin in {"definition", "builtin"}:
            _require(v.storage == "definition", f"Code binding has non-code storage: {v.id}")
        if v.origin == "state":
            kinds = {"@namespace": ValueKind.NAMESPACE_STATE, "@completion": ValueKind.COMPLETION_STATE}
            _require(v.name in kinds, f"Unrecognized state token: {v.name}")
            kind = kinds[v.name]
        elif v.origin == "object_state":
            kind = ValueKind.OBJECT_STATE
        elif v.origin == "result" and v.name == "@discard":
            kind = ValueKind.DISCARDED_RESULT
        else:
            kinds = {"immutable_value": ValueKind.IMMUTABLE,
                     "shared_reference": ValueKind.SHARED_REFERENCE,
                     "definition": ValueKind.CODE_BINDING,
                     "native_namespace": ValueKind.NATIVE_REFERENCE}
            _require(v.storage in kinds, f"Unknown value storage: {v.storage}")
            kind = kinds[v.storage]
        object.__setattr__(self, "kind", kind)

    @property
    def id(self) -> str:
        return self.value.id

    @property
    def is_state_token(self) -> bool:
        return self.kind in {ValueKind.NAMESPACE_STATE, ValueKind.COMPLETION_STATE, ValueKind.OBJECT_STATE}


@dataclass(frozen=True)
class ObjectRequirement:
    """Access to one established reference group; different IDs do not prove disjointness.

    Empty state_inputs means the DAG supplies no explicit mutation token here;
    value producers and all prerequisite edges still apply. It never means
    'read whichever object version happens to be present'.
    """
    object_id: str
    input_ids: tuple[str, ...]
    state_inputs: tuple[str, ...]
    state_outputs: tuple[str, ...]
    access: ObjectAccess

    def __post_init__(self):
        _nonempty(self.object_id, "object_id")
        _require(isinstance(self.access, ObjectAccess), "Invalid object access")
        for name in ("input_ids", "state_inputs", "state_outputs"):
            values = tuple(getattr(self, name))
            for ident in values:
                _nonempty(ident, name)
            _require(len(set(values)) == len(values), f"Duplicate {name}")
            object.__setattr__(self, name, values)
        if self.access == ObjectAccess.MUTATE:
            _require(bool(self.input_ids) and bool(self.state_outputs),
                     "Object mutation requires a live reference and an output state")


@dataclass(frozen=True)
class TaskManifest:
    """One atomic DAG computation and its logical execution requirements.

    task.source/span are retained evidence within code.program_id. They are not
    authorization to exec a snippet in fresh globals, or to run the complete
    module again to obtain a callable. Native regions require original context.
    """
    task: TaskNode
    code: CodeRequirement
    inputs: tuple[ValueRequirement, ...]
    outputs: tuple[ValueRequirement, ...]
    prerequisites: tuple[DependencyEdge, ...]
    binding_events: tuple[BindingEvent, ...] = ()
    mode: ExecutionMode = field(init=False)
    objects: tuple[ObjectRequirement, ...] = field(init=False)

    def __post_init__(self):
        _require(isinstance(self.task, TaskNode) and isinstance(self.code, CodeRequirement),
                 "TaskNode and CodeRequirement are required")
        for name in ("inputs", "outputs", "prerequisites", "binding_events"):
            object.__setattr__(self, name, tuple(getattr(self, name)))
        _require(all(isinstance(v, ValueRequirement) for v in (*self.inputs, *self.outputs)),
                 "ValueRequirement records are required")
        _require(all(isinstance(e, DependencyEdge) for e in self.prerequisites),
                 "DependencyEdge records are required")
        t = self.task
        _require(t.runnable, f"Non-runnable task: {t.id}")
        _require(t.placement in {"isolated_candidate", "shared_namespace"}, f"Unknown placement: {t.placement}")
        _require(tuple(v.id for v in self.inputs) == t.inputs, f"Inputs differ from DAG task {t.id}")
        _require(tuple(v.id for v in self.outputs) == t.outputs, f"Outputs differ from DAG task {t.id}")
        _require({e.source for e in self.prerequisites} == t.dependencies
                 and all(e.target == t.id for e in self.prerequisites)
                 and len(self.prerequisites) == len(t.dependencies), f"Prerequisites differ for {t.id}")
        definitions = tuple(dict.fromkeys(v.value.definition_id for v in self.inputs if v.value.definition_id))
        _require(set(definitions) <= set(self.code.definition_ids), f"Code definitions differ for {t.id}")
        if t.placement == "isolated_candidate":
            _require(t.certainty == Certainty.CERTAIN and t.effect == EffectKind.PURE
                     and not t.characteristics.may_raise and not t.characteristics.possible_side_effects
                     and not t.characteristics.unknown_calls and not t.mutated_objects
                     and t.region_scope is None and t.kind in {"expression", "call", "module_docstring"}
                     and bool(t.proof), f"Inconsistent isolation candidate: {t.id}")
            _require(all(v.kind != ValueKind.NATIVE_REFERENCE and not v.value.may_be_unbound
                         for v in (*self.inputs, *self.outputs)), f"Native lookup in isolation candidate: {t.id}")
            _require(not any(v.is_state_token for v in self.outputs), f"State write in isolation candidate: {t.id}")
            mode = ExecutionMode.ISOLATED_CANDIDATE
        else:
            mode = ExecutionMode.NATIVE_REGION if t.region_scope is not None else ExecutionMode.SHARED_CONTEXT
        object.__setattr__(self, "mode", mode)
        object.__setattr__(self, "objects", self._object_requirements())

    @property
    def task_id(self) -> str:
        return self.task.id

    @property
    def dependencies(self) -> frozenset[str]:
        return self.task.dependencies

    @property
    def state_inputs(self) -> tuple[ValueRequirement, ...]:
        return tuple(v for v in self.inputs if v.is_state_token or v.value.origin == "namespace")

    @property
    def state_outputs(self) -> tuple[ValueRequirement, ...]:
        return tuple(v for v in self.outputs if v.is_state_token or v.value.origin == "namespace")

    @property
    def reported_output_ids(self) -> tuple[str, ...]:
        """Successful computation acknowledges these IDs, including state and native views.

        Materialized alias bindings have their own producer and are reported.
        Zero-work aliases are derived from existing values, and
        discarded expressions need no retained result. This is NOT a list of
        namespace bindings to commit now: BindingEvents remain source ordered.
        """
        return tuple(v.id for v in self.outputs
                     if v.value.alias_of is None and v.kind != ValueKind.DISCARDED_RESULT)

    def _object_requirements(self) -> tuple[ObjectRequirement, ...]:
        references: dict[str, list[str]] = {}
        state_in: dict[str, list[str]] = {}
        state_out: dict[str, list[str]] = {}
        for v in self.inputs:
            if v.kind == ValueKind.SHARED_REFERENCE:
                references.setdefault(v.value.object_id, []).append(v.id)
            elif v.kind == ValueKind.OBJECT_STATE:
                state_in.setdefault(v.value.object_id, []).append(v.id)
        for v in self.outputs:
            if v.kind == ValueKind.OBJECT_STATE:
                state_out.setdefault(v.value.object_id, []).append(v.id)
        objects = dict.fromkeys((*references, *state_in, *state_out, *self.task.mutated_objects))
        result = []
        for oid in objects:
            access = (ObjectAccess.MUTATE if oid in self.task.mutated_objects else
                      ObjectAccess.SNAPSHOT_CANDIDATE if self.mode == ExecutionMode.ISOLATED_CANDIDATE else
                      ObjectAccess.LIVE_REFERENCE)
            result.append(ObjectRequirement(oid, tuple(references.get(oid, ())),
                                            tuple(state_in.get(oid, ())), tuple(state_out.get(oid, ())), access))
        return tuple(result)


@dataclass(frozen=True)
class AttemptIdentity:
    """Caller-assigned run/attempt identity; no retry or dispatch state is kept."""
    plan_id: str
    run_id: str
    task_id: str
    attempt_id: str

    def __post_init__(self):
        _sha256(self.plan_id, "plan")
        for name in ("run_id", "task_id", "attempt_id"):
            _nonempty(getattr(self, name), name)


class FailureKind(str, Enum):
    PYTHON_EXCEPTION = "python_exception"
    INPUT_UNAVAILABLE = "input_unavailable"
    ENVIRONMENT_MISMATCH = "environment_mismatch"
    RESOURCE_EXHAUSTED = "resource_exhausted"
    EXECUTION_ERROR = "execution_error"


@dataclass(frozen=True)
class FailureInfo:
    """Descriptive failure only; never retain a live exception or traceback frame."""
    kind: FailureKind
    message: str
    exception_type: str | None = None
    traceback_text: str | None = None

    def __post_init__(self):
        _require(isinstance(self.kind, FailureKind), "Invalid failure kind")
        _require(isinstance(self.message, str), "Failure message must be text")
        _require(self.traceback_text is None or isinstance(self.traceback_text, str), "Traceback must be text")
        if self.kind == FailureKind.PYTHON_EXCEPTION:
            _nonempty(self.exception_type, "exception_type")
        elif self.exception_type is not None:
            _nonempty(self.exception_type, "exception_type")


@dataclass(frozen=True)
class TaskSuccess:
    """Computation succeeded; binding/state commitment is a separate integration obligation.

    Native-view IDs acknowledge views, not eagerly captured objects or assertions
    of boundness. This report alone must never call DAG.mark_completed().
    """
    attempt: AttemptIdentity
    output_ids: tuple[str, ...]
    clean_exit: bool = False
    stdout_tail: str = ""
    stderr_tail: str = ""
    stdout_truncated: bool = False
    stderr_truncated: bool = False

    def __post_init__(self):
        _require(isinstance(self.attempt, AttemptIdentity), "AttemptIdentity is required")
        object.__setattr__(self, "output_ids", tuple(self.output_ids))
        for ident in self.output_ids:
            _nonempty(ident, "output_id")
        _require(len(set(self.output_ids)) == len(self.output_ids), "Duplicate reported outputs")
        _require(type(self.clean_exit) is bool, "clean_exit must be bool")
        _require(isinstance(self.stdout_tail, str), "stdout_tail must be text")
        _require(isinstance(self.stderr_tail, str), "stderr_tail must be text")
        _require(type(self.stdout_truncated) is bool, "stdout_truncated must be bool")
        _require(type(self.stderr_truncated) is bool, "stderr_truncated must be bool")


@dataclass(frozen=True)
class TaskFailure:
    """Failure never establishes successful outputs or unlocks dependents.

    Native execution may already have changed state: failure is not rollback or
    permission to retry, including for a callback-free operation that may raise.
    """
    attempt: AttemptIdentity
    failure: FailureInfo
    stdout_tail: str = ""
    stderr_tail: str = ""
    stdout_truncated: bool = False
    stderr_truncated: bool = False

    def __post_init__(self):
        _require(isinstance(self.attempt, AttemptIdentity), "AttemptIdentity is required")
        _require(isinstance(self.failure, FailureInfo), "FailureInfo is required")
        _require(isinstance(self.stdout_tail, str), "stdout_tail must be text")
        _require(isinstance(self.stderr_tail, str), "stderr_tail must be text")
        _require(type(self.stdout_truncated) is bool, "stdout_truncated must be bool")
        _require(type(self.stderr_truncated) is bool, "stderr_truncated must be bool")


@dataclass(frozen=True)
class ExecutionPlan:
    """Immutable, self-validating contract for one analyzed module.

    Construct with lower_dag(). Direct construction is checked too. The graph
    digest detects accidental DAG/manifest drift, not maliciously forged proofs.
    A future code adapter must preserve the original module compiler context
    (including future imports), aliases, lifetimes and source-order BindingEvents.
    """
    program: ProgramIdentity
    source: str
    dag_digest: str
    tasks: tuple[TaskManifest, ...]
    values: tuple[Value, ...]
    edges: tuple[DependencyEdge, ...]
    definitions: tuple[Definition, ...]
    bindings: tuple[BindingEvent, ...]
    final_bindings: Mapping[str, str]
    final_namespace: str | None
    final_object_states: Mapping[str, str]
    assumptions: tuple[str, ...]
    diagnostics: tuple[str, ...]
    id: str = field(init=False)
    task_index: Mapping[str, TaskManifest] = field(init=False, repr=False, compare=False)
    value_index: Mapping[str, Value] = field(init=False, repr=False, compare=False)
    definition_index: Mapping[str, Definition] = field(init=False, repr=False, compare=False)

    def __post_init__(self):
        _require(isinstance(self.program, ProgramIdentity), "ProgramIdentity is required")
        for name in ("tasks", "values", "edges", "definitions", "bindings", "assumptions", "diagnostics"):
            object.__setattr__(self, name, tuple(getattr(self, name)))
        for name, record_type in (("tasks", TaskManifest), ("values", Value), ("edges", DependencyEdge),
                                  ("definitions", Definition), ("bindings", BindingEvent)):
            _require(all(isinstance(r, record_type) for r in getattr(self, name)),
                     f"Invalid {name} record type")
        for name in ("final_bindings", "final_object_states"):
            object.__setattr__(self, name, MappingProxyType(dict(getattr(self, name))))
        self.validate()
        object.__setattr__(self, "id", _digest({"execution_schema": 1, "program": self.program.id,
                                               "dag": self.dag_digest}))
        object.__setattr__(self, "task_index", MappingProxyType({t.task_id: t for t in self.tasks}))
        object.__setattr__(self, "value_index", MappingProxyType({v.id: v for v in self.values}))
        object.__setattr__(self, "definition_index", MappingProxyType({d.id: d for d in self.definitions}))

    def _as_dag(self) -> DAG:
        # Delegate ownership, aliases, edges and cycles to the frozen-v1 validator.
        try:
            return DAG((m.task for m in self.tasks), self.values, self.edges,
                       definitions=self.definitions, bindings=self.bindings,
                       final_bindings=self.final_bindings, final_namespace=self.final_namespace,
                       final_object_states=self.final_object_states, assumptions=self.assumptions,
                       diagnostics=self.diagnostics, source=self.source, filename=self.program.filename)
        except GraphValidationError as error:
            raise ExecutionValidationError(f"Invalid execution graph: {error}") from error

    def validate(self) -> None:
        _require(isinstance(self.source, str), "Source must be text")
        _require(hashlib.sha256(self.source.encode("utf-8")).hexdigest() == self.program.source_sha256,
                 "Program source digest mismatch")
        _sha256(self.dag_digest, "DAG")
        dag = self._as_dag()
        _require(_graph_digest(dag) == self.dag_digest, "DAG provenance mismatch")
        for value in self.values:
            ValueRequirement(value)  # Also check producerless/unused setup values.
        incoming: dict[str, list[DependencyEdge]] = {t: [] for t in dag.tasks}
        for edge in self.edges:
            incoming[edge.target].append(edge)
        definition_ids = {d.id for d in self.definitions}
        for manifest in self.tasks:
            _require(set(manifest.code.definition_ids) <= definition_ids,
                     "Task references an unknown code definition")
            _require(manifest.code.program_id == self.program.id, "Task references a different program")
            for requirement in (*manifest.inputs, *manifest.outputs):
                _require(dag.values.get(requirement.id) == requirement.value,
                         f"Value requirement differs from DAG: {requirement.id}")
            _require(manifest.prerequisites == tuple(incoming[manifest.task_id]),
                     f"Dependency reasons differ for {manifest.task_id}")

    def validate_against(self, dag: DAG) -> None:
        """Reject a different analyzed source, options outcome, or edited graph."""
        dag.validate()
        self.validate()
        _require(dag.execution_permitted and _graph_digest(dag) == self.dag_digest,
                 "Plan does not describe the supplied DAG")


    def context_seed_requirements(self, task_id: str) -> tuple[ValueRequirement, ...]:
        """Transferable isolated bindings that must exist in a persistent context.

        A native/shared task executes in the module namespace.  When an earlier
        isolated task ran on another worker, its latest still-live binding is not
        automatically present in that namespace even if the current statement
        does not name it directly.  Reconstruct only values proven to come from
        isolated ancestors of this task, and only when that isolated value is the
        newest ancestor-produced version for its binding name.

        This is deliberately narrower than copying the whole worker namespace:
        native-produced bindings remain owned by the persistent context, while
        immutable values and isolated shared-reference snapshots have authenticated
        data-plane representations that can be transferred safely.
        """
        manifest = self.task_index.get(task_id)
        _require(manifest is not None, f"Unknown task: {task_id}")

        # F58 follow-up: this helper describes the namespace requirements of a
        # task *when it executes in a persistent context*, not merely tasks whose
        # static mode is shared/native. F16 deliberately keeps a narrow subset of
        # isolated-labelled tasks (for example shared-reference producers) inside
        # that context. Those tasks still need isolated ancestor bindings that may
        # have been produced on another worker. Returning early by static mode made
        # such context-executed tasks raise NameError only under distributed placement.
        ancestors: set[str] = set()
        pending = list(manifest.dependencies)
        while pending:
            current = pending.pop()
            if current in ancestors:
                continue
            ancestors.add(current)
            pending.extend(self.task_index[current].dependencies)

        latest: dict[str, Value] = {}
        for value in self.values:
            if value.producer not in ancestors or not value.name.isidentifier():
                continue
            previous = latest.get(value.name)
            if previous is None or value.version > previous.version:
                latest[value.name] = value

        seeds: list[ValueRequirement] = []
        for value in latest.values():
            producer = self.task_index[value.producer] if value.producer is not None else None
            if producer is None or producer.mode != ExecutionMode.ISOLATED_CANDIDATE:
                continue
            requirement = ValueRequirement(value)
            if requirement.kind in {ValueKind.IMMUTABLE, ValueKind.SHARED_REFERENCE}:
                seeds.append(requirement)

        # Stable source order keeps request materialization deterministic.
        seeds.sort(key=lambda req: (
            self.task_index[req.value.producer].task.span.line if req.value.producer else -1,
            req.value.version, req.id,
        ))
        return tuple(seeds)

    def immutable_representation_id(self, value_id: str) -> str:
        """Return the physical immutable representation backing a logical value.

        Plain immutable aliases do not create new worker payload bytes: the DAG
        records them as source-ordered binding events over the same produced
        immutable value.  This resolver is intentionally narrow.  It does NOT
        collapse shared references, object-state versions, projections, native
        references, code bindings, or state tokens.  Readiness/binding commit
        semantics therefore remain independent from data residency.
        """
        value = self.value_index.get(value_id)
        _require(value is not None, f"Unknown value: {value_id}")
        if ValueRequirement(value).kind != ValueKind.IMMUTABLE:
            return value_id
        seen: set[str] = set()
        current = value
        while current.alias_of is not None:
            _require(current.id not in seen, "Cycle in immutable alias chain")
            seen.add(current.id)
            base = self.value_index.get(current.alias_of)
            _require(base is not None, f"Missing alias source: {current.id}")
            if (ValueRequirement(base).kind != ValueKind.IMMUTABLE
                    or current.object_id != base.object_id
                    or current.projection != base.projection):
                break
            current = base
        return current.id

    def validate_result(self, result: TaskSuccess | TaskFailure, *, expected_attempt: AttemptIdentity) -> None:
        """Check correlation and output ownership without mutating readiness or run state."""
        _require(isinstance(result, (TaskSuccess, TaskFailure)), "Unknown result record")
        _require(isinstance(expected_attempt, AttemptIdentity), "Expected AttemptIdentity is required")
        _require(result.attempt == expected_attempt, "Result attempt mismatch")
        _require(expected_attempt.plan_id == self.id, "Result plan mismatch")
        _require(expected_attempt.task_id in self.task_index, "Unknown result task")
        if isinstance(result, TaskSuccess):
            expected = self.task_index[expected_attempt.task_id].reported_output_ids
            _require(set(result.output_ids) == set(expected), "Successful outputs differ from manifest")
