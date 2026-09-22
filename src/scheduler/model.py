"""Immutable coordinator facts and scheduling proposals. No live runtime state.

All task, value, context and materialization IDs in a ClusterSnapshot belong to
its plan/run. Only enrolled compute workers belong in ``workers``.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import math

from execution import AttemptIdentity, ExecutionMode, ExecutionPlan, ValueKind, ValueRequirement


class SnapshotValidationError(ValueError):
    """Contradictory or foreign coordinator facts; never silently repaired."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise SnapshotValidationError(message)


def _text(value: str, label: str) -> None:
    _require(isinstance(value, str) and bool(value.strip()), f"{label} must be nonempty text")


def _integer(value: int, label: str, minimum: int = 0) -> None:
    _require(type(value) is int and value >= minimum, f"{label} must be an integer >= {minimum}")


def _freeze_ids(record, name: str) -> None:
    values = getattr(record, name)
    _require(not isinstance(values, str), f"{name} must be a collection of IDs")
    values = tuple(values)
    for value in values:
        _text(value, name)
    _require(len(values) == len(set(values)), f"Duplicate {name}")
    object.__setattr__(record, name, frozenset(values))


def _freeze_records(record, name: str, kind: type, key) -> None:
    values = tuple(getattr(record, name))
    _require(all(isinstance(v, kind) for v in values), f"{name} requires {kind.__name__} records")
    keys = [key(v) for v in values]
    _require(len(keys) == len(set(keys)), f"Duplicate {name}")
    object.__setattr__(record, name, values)


class ReplicaStatus(str, Enum):
    AVAILABLE = "available"
    IN_FLIGHT = "in_flight"


class DataForm(str, Enum):
    IMMUTABLE_VALUE = "immutable_value"
    OBJECT_SNAPSHOT = "object_snapshot"


class CommitmentPhase(str, Enum):
    RESERVED = "reserved"
    DISPATCHED = "dispatched"
    WAITING_TRANSFER = "waiting_transfer"
    RUNNING = "running"


class UnplacedReason(str, Enum):
    NO_ELIGIBLE_WORKER = "no_eligible_worker"
    OFFLINE = "offline"
    NOT_ACCEPTING = "not_accepting"
    NO_CAPACITY = "no_capacity"
    ENVIRONMENT_MISMATCH = "environment_mismatch"
    PROGRAM_UNAVAILABLE = "program_unavailable"
    MODE_UNSUPPORTED = "mode_unsupported"
    AFFINITY_MISMATCH = "affinity_mismatch"
    CONTEXT_UNAVAILABLE = "context_unavailable"
    INPUT_UNAVAILABLE = "input_unavailable"


@dataclass(frozen=True)
class ReadyTask:
    """Coordinator-authorized READY task; age counts coordinator scheduling rounds.

    ready_sequence is monotonic within a run. wait_rounds must persist across
    snapshots; the scheduler never advances it or reads a clock.
    """
    task_id: str
    ready_sequence: int
    wait_rounds: int = 0

    def __post_init__(self):
        _text(self.task_id, "task_id")
        _integer(self.ready_sequence, "ready_sequence")
        _integer(self.wait_rounds, "wait_rounds")


@dataclass(frozen=True)
class WorkerState:
    """Enrolled compute worker, never a coordinator hardware description.

    Counters cover ALL plans. Non-running commitments (including transfers)
    occupy reserved_slots. prepared_program_ids include code/definition setup;
    supported_modes attests an adapter preserving that execution contract.
    Neither assertion is inferred from CPU/RAM or a familiar package name.
    """
    worker_id: str
    total_slots: int
    running_slots: int = 0
    reserved_slots: int = 0
    online: bool = True
    accepting_work: bool = True
    cpu_percent: float = 0.0
    total_memory_bytes: int = 0
    available_memory_bytes: int = 0
    cpu_cores: int = 1
    environment_ids: frozenset[str] = frozenset()
    prepared_program_ids: frozenset[str] = frozenset()
    supported_modes: frozenset[ExecutionMode] = frozenset()

    def __post_init__(self):
        _text(self.worker_id, "worker_id")
        for name in ("total_slots", "running_slots", "reserved_slots",
                     "total_memory_bytes", "available_memory_bytes"):
            _integer(getattr(self, name), name)
        _integer(self.cpu_cores, "cpu_cores", 1)
        _require(type(self.online) is bool and type(self.accepting_work) is bool,
                 "online/accepting_work must be bool")
        _require(type(self.cpu_percent) in (int, float) and 0 <= self.cpu_percent <= 100
                 and math.isfinite(self.cpu_percent), "CPU must be finite and between 0 and 100")
        _require(self.running_slots + self.reserved_slots <= self.total_slots,
                 "running + reserved exceeds worker capacity")
        _require(self.available_memory_bytes <= self.total_memory_bytes,
                 "available memory exceeds total memory")
        for name in ("environment_ids", "prepared_program_ids"):
            _freeze_ids(self, name)
        modes = tuple(self.supported_modes)
        _require(all(isinstance(m, ExecutionMode) for m in modes), "Invalid supported execution mode")
        _require(len(modes) == len(set(modes)), "Duplicate supported modes")
        object.__setattr__(self, "supported_modes", frozenset(modes))

    @property
    def free_slots(self) -> int:
        return self.total_slots - self.running_slots - self.reserved_slots


@dataclass(frozen=True)
class Replica:
    worker_id: str
    status: ReplicaStatus = ReplicaStatus.AVAILABLE

    def __post_init__(self):
        _text(self.worker_id, "worker_id")
        _require(isinstance(self.status, ReplicaStatus), "Invalid replica status")


@dataclass(frozen=True)
class DataLocation:
    """A coordinator-certified transferable input representation, not a live object.

    OBJECT_SNAPSHOT certifies preparation under the execution alias/lifetime
    contract. object_state_id identifies its exact version; None means the
    initial producer-established version, NEVER whichever state is current.
    This record cannot describe native references, code bindings or state tokens.
    A missing size stays unknown, even for a locally available representation.
    """
    value_id: str
    form: DataForm
    replicas: tuple[Replica, ...] = ()
    size_bytes: int | None = None
    object_state_id: str | None = None

    def __post_init__(self):
        _text(self.value_id, "value_id")
        _require(isinstance(self.form, DataForm), "Invalid data form")
        _freeze_records(self, "replicas", Replica, lambda r: r.worker_id)
        if self.size_bytes is not None:
            _integer(self.size_bytes, "size_bytes")
        if self.object_state_id is not None:
            _text(self.object_state_id, "object_state_id")
            _require(self.form == DataForm.OBJECT_SNAPSHOT, "Only object snapshots have state versions")


@dataclass(frozen=True)
class TaskCommitment:
    attempt: AttemptIdentity
    worker_id: str
    phase: CommitmentPhase

    def __post_init__(self):
        _require(isinstance(self.attempt, AttemptIdentity), "AttemptIdentity required")
        _text(self.worker_id, "worker_id")
        _require(isinstance(self.phase, CommitmentPhase), "Invalid commitment phase")


@dataclass(frozen=True)
class TaskAffinity:
    task_id: str
    required_worker: str | None = None
    allowed_workers: frozenset[str] | None = None
    context_id: str | None = None

    def __post_init__(self):
        _text(self.task_id, "task_id")
        for name in ("required_worker", "context_id"):
            if getattr(self, name) is not None:
                _text(getattr(self, name), name)
        if self.allowed_workers is not None:
            _freeze_ids(self, "allowed_workers")
            _require(self.required_worker is None or self.required_worker in self.allowed_workers,
                     "required_worker excluded by allowed_workers")


@dataclass(frozen=True)
class WorkerContext:
    """Native context ownership plus exact tasks prepared in this snapshot.

    prepared_task_ids certifies their original scope, binding/definition setup,
    live aliases, required state versions and exception behavior. It does NOT
    grant readiness. Absence of a name in Python need not mean missing payload.
    available_slots counts admissible entries AFTER active/reserved context use.
    One is the conservative default for a non-reentrant native context; more
    requires an adapter that safely supports concurrent entries. Context setup
    itself is future worker work, never coordinator computation.
    """
    context_id: str
    worker_id: str
    prepared_task_ids: frozenset[str] = frozenset()
    available_slots: int = 1

    def __post_init__(self):
        _text(self.context_id, "context_id")
        _text(self.worker_id, "worker_id")
        _freeze_ids(self, "prepared_task_ids")
        _integer(self.available_slots, "context available_slots")


@dataclass(frozen=True)
class ClusterSnapshot:
    plan_id: str
    run_id: str
    snapshot_id: str
    ready: tuple[ReadyTask, ...] = ()
    workers: tuple[WorkerState, ...] = ()
    data: tuple[DataLocation, ...] = ()
    commitments: tuple[TaskCommitment, ...] = ()
    affinities: tuple[TaskAffinity, ...] = ()
    contexts: tuple[WorkerContext, ...] = ()
    blocked_task_ids: frozenset[str] = frozenset()
    completed_task_ids: frozenset[str] = frozenset()

    def __post_init__(self):
        for name in ("plan_id", "run_id", "snapshot_id"):
            _text(getattr(self, name), name)
        for name, kind, key in (
            ("ready", ReadyTask, lambda t: t.task_id),
            ("workers", WorkerState, lambda w: w.worker_id),
            ("data", DataLocation, lambda d: (d.value_id, d.object_state_id)),
            ("commitments", TaskCommitment, lambda c: c.attempt.task_id),
            ("affinities", TaskAffinity, lambda a: a.task_id),
            ("contexts", WorkerContext, lambda c: c.context_id),
        ):
            _freeze_records(self, name, kind, key)
        for name in ("blocked_task_ids", "completed_task_ids"):
            _freeze_ids(self, name)

    def validate(self, plan: ExecutionPlan) -> None:
        """Cross-reference facts, not dependency readiness or actual remote state."""
        _require(isinstance(plan, ExecutionPlan), "ExecutionPlan required")
        _require(self.plan_id == plan.id, "Foreign plan_id")
        tasks = plan.task_index.keys()
        workers = {w.worker_id: w for w in self.workers}
        ready = {t.task_id for t in self.ready}
        committed = {c.attempt.task_id for c in self.commitments}
        groups = (ready, committed, self.blocked_task_ids, self.completed_task_ids)
        seen: set[str] = set()
        for group in groups:
            _require(group <= tasks, "Unknown task ID")
            _require(not seen.intersection(group), "Task has contradictory lifecycle facts (READY/committed/blocked/completed)")
            seen.update(group)
        running = dict.fromkeys(workers, 0)
        reserved = dict.fromkeys(workers, 0)
        for commitment in self.commitments:
            attempt = commitment.attempt
            _require(attempt.plan_id == self.plan_id and attempt.run_id == self.run_id,
                     "Foreign commitment plan/run identity")
            _require(commitment.worker_id in workers, "Commitment references unknown worker")
            counts = running if commitment.phase == CommitmentPhase.RUNNING else reserved
            counts[commitment.worker_id] += 1
        for worker_id, worker in workers.items():
            _require(running[worker_id] <= worker.running_slots and reserved[worker_id] <= worker.reserved_slots,
                     "Commitments exceed reported running/reserved counters")
        for affinity in self.affinities:
            _require(affinity.task_id in tasks, "Affinity references unknown task")
            _require(affinity.required_worker is None or affinity.required_worker in workers,
                     "Affinity references unknown required worker")
            _require(affinity.allowed_workers is None or affinity.allowed_workers <= workers.keys(),
                     "Affinity references unknown allowed worker")
        contexts = {c.context_id: c for c in self.contexts}
        for context in self.contexts:
            _require(context.worker_id in workers, "Context references unknown worker")
            _require(context.prepared_task_ids <= tasks, "Context references unknown task")
        for affinity in self.affinities:
            context = contexts.get(affinity.context_id)
            if context is not None:
                _require(affinity.required_worker is None or affinity.required_worker == context.worker_id,
                         "Context owner conflicts with required worker")
                _require(affinity.allowed_workers is None or context.worker_id in affinity.allowed_workers,
                         "Context owner excluded by affinity")
        for location in self.data:
            _require(location.value_id in plan.value_index, "Location references unknown value")
            value = plan.value_index[location.value_id]
            kind = ValueRequirement(value).kind
            expected = {ValueKind.IMMUTABLE: DataForm.IMMUTABLE_VALUE,
                        ValueKind.SHARED_REFERENCE: DataForm.OBJECT_SNAPSHOT}.get(kind)
            _require(expected is not None and location.form == expected,
                     "Locations require immutable data or certified object snapshots; code/native/state is not payload")
            _require(all(r.worker_id in workers for r in location.replicas), "Location references unknown worker")
            if location.object_state_id is not None:
                state = plan.value_index.get(location.object_state_id)
                _require(state is not None and ValueRequirement(state).kind == ValueKind.OBJECT_STATE
                         and state.object_id == value.object_id, "Snapshot state belongs to a different or unknown object")


@dataclass(frozen=True)
class SchedulerPolicy:
    """Transparent bounds, not runtime predictions or fitted weights."""
    fairness_after_rounds: int = 8
    descendant_budget_bytes: int = 8 * 1024 * 1024

    def __post_init__(self):
        _integer(self.fairness_after_rounds, "fairness_after_rounds", 1)
        _integer(self.descendant_budget_bytes, "descendant_budget_bytes")


@dataclass(frozen=True)
class StructuralPriority:
    downstream_depth: int
    descendant_count: int | None
    immediate_dependents: int

    def __post_init__(self):
        for name in ("downstream_depth", "immediate_dependents"):
            _integer(getattr(self, name), name)
        if self.descendant_count is not None:
            _integer(self.descendant_count, "descendant_count")


@dataclass(frozen=True)
class TaskPriority:
    promoted: bool
    ready_sequence: int
    wait_rounds: int
    feasible_workers: int
    structure: StructuralPriority

    def __post_init__(self):
        _require(type(self.promoted) is bool and isinstance(self.structure, StructuralPriority), "Invalid task priority")
        for name in ("ready_sequence", "wait_rounds", "feasible_workers"):
            _integer(getattr(self, name), name)


@dataclass(frozen=True)
class Locality:
    local_input_ids: tuple[str, ...] = ()
    remote_input_ids: tuple[str, ...] = ()
    known_remote_bytes: int = 0
    unknown_remote_inputs: int = 0

    def __post_init__(self):
        for name in ("local_input_ids", "remote_input_ids"):
            _freeze_records(self, name, str, lambda ident: ident)
            for ident in getattr(self, name):
                _text(ident, name)
        _require(not set(self.local_input_ids).intersection(self.remote_input_ids), "Input both local and remote")
        _integer(self.known_remote_bytes, "known_remote_bytes")
        _integer(self.unknown_remote_inputs, "unknown_remote_inputs")
        _require(self.unknown_remote_inputs <= len(self.remote_input_ids), "Too many unknown remote inputs")


@dataclass(frozen=True)
class WorkerPreference:
    locality: Locality
    free_slots_before: int
    ram_pressure_band: int
    cpu_pressure_band: int
    available_memory_bytes: int
    cpu_cores: int

    def __post_init__(self):
        _require(isinstance(self.locality, Locality), "Locality required")
        for name in ("free_slots_before", "ram_pressure_band", "cpu_pressure_band", "available_memory_bytes"):
            _integer(getattr(self, name), name)
        _integer(self.cpu_cores, "cpu_cores", 1)
        _require(self.ram_pressure_band <= 10 and self.cpu_pressure_band <= 10, "Invalid pressure band")


@dataclass(frozen=True)
class Placement:
    task_id: str
    worker_id: str
    context_id: str | None
    priority: TaskPriority
    preference: WorkerPreference

    def __post_init__(self):
        _text(self.task_id, "task_id")
        _text(self.worker_id, "worker_id")
        if self.context_id is not None:
            _text(self.context_id, "context_id")
        _require(isinstance(self.priority, TaskPriority) and isinstance(self.preference, WorkerPreference),
                 "Typed placement facts required")


@dataclass(frozen=True)
class WorkerRejection:
    worker_id: str
    reasons: tuple[UnplacedReason, ...]
    missing_input_ids: tuple[str, ...] = ()

    def __post_init__(self):
        _text(self.worker_id, "worker_id")
        _freeze_records(self, "reasons", UnplacedReason, lambda r: r)
        _freeze_records(self, "missing_input_ids", str, lambda ident: ident)


@dataclass(frozen=True)
class UnplacedTask:
    task_id: str
    reason: UnplacedReason
    workers: tuple[WorkerRejection, ...]

    def __post_init__(self):
        _text(self.task_id, "task_id")
        _require(isinstance(self.reason, UnplacedReason), "Invalid unplaced reason")
        _freeze_records(self, "workers", WorkerRejection, lambda w: w.worker_id)
        _require(all(w.reasons for w in self.workers), "Unplaced task contains an eligible worker")


@dataclass(frozen=True)
class SchedulingDecision:
    """Proposal only. Coordinator must revalidate and atomically reserve it."""
    plan_id: str
    run_id: str
    snapshot_id: str
    placements: tuple[Placement, ...]
    unplaced: tuple[UnplacedTask, ...]

    def __post_init__(self):
        for name in ("plan_id", "run_id", "snapshot_id"):
            _text(getattr(self, name), name)
        _freeze_records(self, "placements", Placement, lambda p: p.task_id)
        _freeze_records(self, "unplaced", UnplacedTask, lambda p: p.task_id)
        _require(not {p.task_id for p in self.placements}.intersection(u.task_id for u in self.unplaced),
                 "Task both placed and unplaced")
