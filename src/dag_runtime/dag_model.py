"""Graph records and readiness bookkeeping. Standard library only, no executor."""
from __future__ import annotations

from collections import deque
from dataclasses import asdict, dataclass
from enum import Enum
from types import MappingProxyType
from typing import Iterable, Mapping


class Certainty(str, Enum):
    CERTAIN = "certain"
    CONSERVATIVE = "conservative"


class EdgeKind(str, Enum):
    DATA = "data"
    ORDER = "order"
    STATE = "state"


class EffectKind(str, Enum):
    """Effect scope, independent of whether successful completion is proved.

    PURE may still raise. NAMESPACE includes unknown external effects because
    arbitrary Python callbacks can reach module bindings. ESCAPE requires native
    scope retention; it is not a claim that every dynamic operation escapes.
    """
    PURE = "pure"
    OBJECT_LOCAL = "object_local"
    NAMESPACE = "namespace"
    ESCAPE = "namespace_escape"


@dataclass(frozen=True)
class SourceSpan:
    filename: str
    line: int
    column: int
    end_line: int
    end_column: int


@dataclass(frozen=True)
class Characteristics:
    ast_nodes: int = 0
    call_count: int = 0
    loop_count: int = 0
    max_loop_depth: int = 0
    comprehension_count: int = 0
    unknown_calls: bool = False
    possible_side_effects: bool = False
    may_raise: bool = False
    complexity: str = "simple"


@dataclass(frozen=True)
class Value:
    id: str
    name: str
    version: int
    producer: str | None
    origin: str  # result, alias, definition, builtin, external, namespace, state, object_state
    type_hint: str = "unknown"
    object_id: str = ""
    alias_of: str | None = None
    projection: tuple[int | str, ...] = ()
    may_be_unbound: bool = False
    definition_id: str | None = None
    storage: str = 'native_namespace'

    @property
    def label(self) -> str:
        return f"{self.name}#{self.version}"


@dataclass(frozen=True)
class EdgeReason:
    kind: EdgeKind
    certainty: Certainty
    text: str
    value_id: str | None = None


@dataclass(frozen=True)
class DependencyEdge:
    source: str
    target: str
    reasons: tuple[EdgeReason, ...]


@dataclass(frozen=True)
class TaskNode:
    id: str
    kind: str
    label: str
    callable_repr: str | None
    span: SourceSpan
    source: str
    ast_type: str
    inputs: tuple[str, ...]
    outputs: tuple[str, ...]
    dependencies: frozenset[str]
    dependents: frozenset[str]
    certainty: Certainty
    conservative_reasons: tuple[str, ...]
    proof: str
    characteristics: Characteristics
    placement: str  # isolated_candidate or shared_namespace
    runnable: bool = True
    effect: EffectKind = EffectKind.NAMESPACE
    namespace_epoch: int = 0
    mutated_objects: tuple[str, ...] = ()
    region_scope: str | None = None  # statement, tail, module
    statement_count: int = 1  # direct source statements retained, not dynamic iterations


@dataclass(frozen=True)
class BindingEvent:
    """Source-order namespace binding; aliases and plain definitions cost no task."""
    name: str
    value_id: str
    span: SourceSpan
    kind: str  # assignment, alias, definition, region


@dataclass(frozen=True)
class Definition:
    id: str
    name: str
    span: SourceSpan
    source: str
    free_names: tuple[str, ...]
    proof_eligible: bool
    reason: str
    characteristics: Characteristics


class GraphValidationError(ValueError):
    pass


class ReadinessError(ValueError):
    pass


def _indexed(items: Iterable, key, label: str) -> dict:
    result = {}
    for item in items:
        ident = key(item)
        if ident in result:
            raise GraphValidationError(f"Duplicate {label}: {ident}")
        result[ident] = item
    return result


class DAG:
    """Immutable graph indexes; create a ReadinessState for each independent run.

    Values describe bindings and object references, not serialized byte blobs.
    Structural readiness does not certify input residency or placement feasibility.
    """

    def __init__(
        self, tasks: Iterable[TaskNode] = (), values: Iterable[Value] = (),
        edges: Iterable[DependencyEdge] = (), *,
        final_bindings: Mapping[str, str] | None = None,
        bindings: Iterable[BindingEvent] = (), definitions: Iterable[Definition] = (),
        diagnostics: Iterable[str] = (), source: str = "", filename: str = "<source>",
        execution_permitted: bool = True, assumptions: Iterable[str] = (),
        final_namespace: str | None = None,
        final_object_states: Mapping[str, str] | None = None,
    ):
        self.tasks = MappingProxyType(_indexed(tasks, lambda t: t.id, "task ID"))
        self.values = MappingProxyType(_indexed(values, lambda v: v.id, "value ID"))
        self.edges = MappingProxyType(_indexed(edges, lambda e: (e.source, e.target), "edge"))
        self.definitions = MappingProxyType(_indexed(definitions, lambda d: d.id, "definition ID"))
        self.final_bindings = MappingProxyType(dict(final_bindings or {}))
        self.final_namespace = final_namespace
        self.final_object_states = MappingProxyType(dict(final_object_states or {}))
        self.bindings = tuple(bindings)
        self.diagnostics = tuple(diagnostics)
        self.source = source
        self.filename = filename
        self.execution_permitted = execution_permitted
        self.assumptions = tuple(assumptions)
        self._default_readiness: ReadinessState | None = None
        self.validate()

    def sources(self) -> tuple[str, ...]:
        return tuple(t.id for t in self.tasks.values() if not t.dependencies)

    def sinks(self) -> tuple[str, ...]:
        return tuple(t.id for t in self.tasks.values() if not t.dependents)

    def topological_order(self) -> tuple[str, ...]:
        counts = {t.id: len(t.dependencies) for t in self.tasks.values()}
        queue = deque(t for t, count in counts.items() if count == 0)
        # Build adjacency in edge insertion order, avoiding sorting at every step.
        outgoing = {t: [] for t in self.tasks}
        for a, b in self.edges:
            if a not in outgoing or b not in counts:
                raise GraphValidationError(f"Missing task referenced by edge {a} -> {b}")
            outgoing[a].append(b)
        order = []
        while queue:
            current = queue.popleft()
            order.append(current)
            for child in outgoing[current]:
                counts[child] -= 1
                if counts[child] == 0:
                    queue.append(child)
        if len(order) != len(self.tasks):
            raise GraphValidationError("Cycle or inconsistent adjacency in DAG")
        return tuple(order)

    def validate(self) -> None:
        expected_deps = {t: set() for t in self.tasks}
        expected_children = {t: set() for t in self.tasks}
        for (a, b), edge in self.edges.items():
            if a not in self.tasks or b not in self.tasks:
                raise GraphValidationError(f"Missing task in edge {a} -> {b}")
            if a == b:
                raise GraphValidationError(f"Self dependency at {a}")
            if not edge.reasons or len(set(edge.reasons)) != len(edge.reasons):
                raise GraphValidationError(f"Empty or duplicated reasons for {a} -> {b}")
            expected_deps[b].add(a)
            expected_children[a].add(b)
            for reason in edge.reasons:
                if not reason.text:
                    raise GraphValidationError("Dependency reason must not be empty")
                if reason.value_id is not None:
                    if reason.value_id not in self.values:
                        raise GraphValidationError(f"Unknown edge value {reason.value_id}")
                    value = self.values[reason.value_id]
                    if value.producer != a or value.id not in self.tasks[b].inputs:
                        raise GraphValidationError("Edge value does not connect its producer and reader")
        input_sets = {t.id: set(t.inputs) for t in self.tasks.values()}
        output_sets = {t.id: set(t.outputs) for t in self.tasks.values()}
        for task in self.tasks.values():
            if task.effect == EffectKind.OBJECT_LOCAL and (
                not task.mutated_objects or task.placement != 'shared_namespace' or
                task.characteristics.may_raise or task.certainty != Certainty.CERTAIN
            ):
                raise GraphValidationError(f'Invalid bounded object effect for {task.id}')
            for object_id in task.mutated_objects:
                if not any(v.origin == 'object_state' and v.object_id == object_id
                           for v in (self.values[i] for i in task.outputs if i in self.values)):
                    raise GraphValidationError(f'Missing object-state output for {task.id}')
            if self.execution_permitted and not task.runnable:
                raise GraphValidationError('Executable graph contains a non-runnable task')
            if task.dependencies != expected_deps[task.id] or task.dependents != expected_children[task.id]:
                raise GraphValidationError(f"Inconsistent adjacency for {task.id}")
            if len(input_sets[task.id]) != len(task.inputs) or len(output_sets[task.id]) != len(task.outputs):
                raise GraphValidationError(f"Duplicate task values for {task.id}")
            if task.certainty == Certainty.CONSERVATIVE and not task.conservative_reasons:
                raise GraphValidationError(f"Missing conservative reason for {task.id}")
            for value_id in input_sets[task.id] | output_sets[task.id]:
                if value_id not in self.values:
                    raise GraphValidationError(f"Missing value {value_id}")
            for value_id in task.inputs:
                value = self.values[value_id]
                if value.producer is not None:
                    edge = self.edges.get((value.producer, task.id))
                    if edge is None or not any(r.value_id == value_id for r in edge.reasons):
                        raise GraphValidationError(f"Missing data/state edge for {value.label}")
            for value_id in task.outputs:
                if self.values[value_id].producer != task.id:
                    raise GraphValidationError(f"Wrong output producer for {value_id}")
        for value in self.values.values():
            if value.definition_id is not None and value.definition_id not in self.definitions:
                raise GraphValidationError(f'Missing callable definition for {value.id}')
            if value.producer is not None:
                if value.producer not in self.tasks or value.id not in output_sets[value.producer]:
                    raise GraphValidationError(f"Invalid producer for {value.id}")
            if value.alias_of is not None:
                if value.alias_of not in self.values:
                    raise GraphValidationError(f"Missing alias source for {value.id}")
                base = self.values[value.alias_of]
                if base.object_id != value.object_id or base.producer != value.producer:
                    raise GraphValidationError(f"Inconsistent alias identity for {value.id}")
            elif value.origin == 'alias' and value.producer is not None:
                # F29 / Phase-1 materialized aliases: alias bindings now have their
                # own binding-event producer, so alias_of may intentionally be None.
                # The producer must still consume a value from the same identity
                # group; otherwise an externally-constructed graph could claim an
                # arbitrary object_id and silently break Python `is` semantics.
                producer = self.tasks[value.producer]
                if not any(
                    self.values[input_id].object_id == value.object_id
                    for input_id in producer.inputs
                    if input_id in self.values
                ):
                    raise GraphValidationError(f"Inconsistent alias identity for {value.id}")
        # Validate alias chains once, including externally constructed graphs.
        done = set()
        for ident in self.values:
            active = set()
            cur = ident
            while cur is not None and cur not in done:
                if cur in active:
                    raise GraphValidationError("Cycle in alias chain")
                active.add(cur)
                cur = self.values[cur].alias_of
            done.update(active)
        for value_id in self.final_bindings.values():
            if value_id not in self.values:
                raise GraphValidationError("Missing final value")
        if self.final_namespace is not None:
            if self.final_namespace not in self.values or self.values[self.final_namespace].origin != 'state':
                raise GraphValidationError('Missing/invalid final namespace token')
        for object_id, value_id in self.final_object_states.items():
            value = self.values.get(value_id)
            if value is None or value.origin != 'object_state' or value.object_id != object_id:
                raise GraphValidationError('Missing/invalid final object-state token')
        for binding in self.bindings:
            if binding.value_id not in self.values:
                raise GraphValidationError("Missing binding value")
        self.topological_order()

    def initial_ready_tasks(self) -> tuple[str, ...]:
        return self.sources() if self.execution_permitted else ()

    def new_readiness(self) -> ReadinessState:
        return ReadinessState(self)

    def mark_completed(self, task_id: str) -> tuple[str, ...]:
        if self._default_readiness is None:
            self._default_readiness = self.new_readiness()
        return self._default_readiness.mark_completed(task_id)

    def required_values(self, task_id: str) -> tuple[Value, ...]:
        return tuple(self.values[v] for v in self.tasks[task_id].inputs)

    def explain_dependency(self, before: str, after: str) -> DependencyEdge | None:
        self.tasks[before], self.tasks[after]  # reject misspelled IDs
        return self.edges.get((before, after))

    def dependency_path(self, before: str, after: str) -> tuple[str, ...]:
        self.tasks[before], self.tasks[after]
        parents: dict[str, str | None] = {before: None}
        queue = deque([before])
        while queue:
            current = queue.popleft()
            if current == after:
                path = []
                while current is not None:
                    path.append(current)
                    current = parents[current]
                return tuple(reversed(path))
            for child in sorted(self.tasks[current].dependents):
                if child not in parents:
                    parents[child] = current
                    queue.append(child)
        return ()

    def explain_parallelism(self, first: str, second: str) -> dict:
        a, b = self.tasks[first], self.tasks[second]
        path = self.dependency_path(first, second) or self.dependency_path(second, first)
        if path:
            return {"allowed": False, "path": path, "reason": "Dependency path imposes order."}
        allowed = all(t.certainty == Certainty.CERTAIN and t.runnable for t in (a, b))
        return {
            "allowed": allowed,
            "reason": "No dependency path; both operations passed the bounded proof." if allowed else
                      "Absence of a path alone is not a proof for conservative operations.",
            "proofs": (a.proof, b.proof),
            "effects": (a.effect.value, b.effect.value),
            "namespace_epochs": (a.namespace_epoch, b.namespace_epoch),
            "inputs": (a.inputs, b.inputs),
            "conditions": self.assumptions,
        }

    def metrics(self) -> dict:
        """Static diagnostics, O(T+E). Generation width is not maximum DAG width.

        An edge pair can have several kinds, so kind counts need not sum to E.
        Hidden statements count direct statements inside native regions only.
        """
        levels, widths = {}, {}
        for ident in self.topological_order():
            level = max((levels[p] + 1 for p in self.tasks[ident].dependencies), default=0)
            levels[ident] = level
            widths[level] = widths.get(level, 0) + 1
        return {
            'tasks': len(self.tasks), 'values': len(self.values), 'edge_pairs': len(self.edges),
            'certain_tasks': sum(t.certainty == Certainty.CERTAIN for t in self.tasks.values()),
            'conservative_tasks': sum(t.certainty == Certainty.CONSERVATIVE for t in self.tasks.values()),
            'edges_by_kind': {kind.value: sum(any(r.kind == kind for r in e.reasons)
                                              for e in self.edges.values()) for kind in EdgeKind},
            'effects': {kind.value: sum(t.effect == kind for t in self.tasks.values()) for kind in EffectKind},
            'opaque_regions': sum(t.kind in {'opaque_region', 'opaque_module'} for t in self.tasks.values()),
            'whole_tail_collapses': sum(t.region_scope == 'tail' for t in self.tasks.values()),
            'hidden_statements': sum(max(0, t.statement_count - 1) for t in self.tasks.values()),
            'isolated_candidates': sum(t.placement == 'isolated_candidate' for t in self.tasks.values()),
            'max_generation_width': max(widths.values(), default=0),
        }

    def to_dict(self) -> dict:
        def task_dict(t: TaskNode) -> dict:
            record = asdict(t)
            record["dependencies"] = sorted(t.dependencies)
            record["dependents"] = sorted(t.dependents)
            return record
        return {
            "schema_version": 2, "filename": self.filename, "source": self.source,
            "execution_permitted": self.execution_permitted,
            "assumptions": list(self.assumptions), "diagnostics": list(self.diagnostics),
            "tasks": [task_dict(t) for t in self.tasks.values()],
            "values": [asdict(v) for v in self.values.values()],
            "edges": [asdict(e) for e in self.edges.values()],
            "definitions": [asdict(d) for d in self.definitions.values()],
            "bindings": [asdict(b) for b in self.bindings],
            "final_bindings": dict(self.final_bindings),
            "final_namespace": self.final_namespace,
            "final_object_states": dict(self.final_object_states),
            "sources": list(self.sources()), "sinks": list(self.sinks()),
        }


class ReadinessState:
    """Monotonic success bookkeeping: O(V+E) over an entire successful run.

    There is no dispatch, execution, input transfer, retry, or timing here.
    Failed tasks must never be marked completed. Duplicate completion is an error.
    """

    def __init__(self, dag: DAG):
        self.dag = dag
        self._remaining = {t.id: len(t.dependencies) for t in dag.tasks.values()}
        self._completed: set[str] = set()
        self._ready = dict.fromkeys(dag.initial_ready_tasks())
        self._children = {t: [] for t in dag.tasks}
        for a, b in dag.edges:
            self._children[a].append(b)

    @property
    def ready(self) -> tuple[str, ...]:
        return tuple(self._ready)

    @property
    def completed(self) -> frozenset[str]:
        return frozenset(self._completed)

    def mark_completed(self, task_id: str) -> tuple[str, ...]:
        if task_id not in self.dag.tasks:
            raise KeyError(task_id)
        if not self.dag.execution_permitted:
            raise ReadinessError("Source is invalid; this diagnostic graph cannot execute")
        if task_id in self._completed:
            raise ReadinessError(f"Task {task_id} was already completed")
        if task_id not in self._ready:
            raise ReadinessError(f"Task {task_id} still has incomplete dependencies")
        self._completed.add(task_id)
        del self._ready[task_id]
        unlocked = []
        for child in self._children[task_id]:
            self._remaining[child] -= 1
            if self._remaining[child] == 0:
                self._ready[child] = None
                unlocked.append(child)
        return tuple(unlocked)
