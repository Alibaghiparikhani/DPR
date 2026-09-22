"""Deterministic placement over immutable execution contracts and snapshot facts."""
from __future__ import annotations

from dataclasses import dataclass, field
import heapq
from types import MappingProxyType
from typing import Mapping

from dag_runtime.dag_model import DAG
from execution import ExecutionMode, ExecutionPlan, ObjectAccess, TaskManifest, ValueKind

from .model import (
    ClusterSnapshot, DataLocation, Locality, Placement, ReadyTask, ReplicaStatus,
    SchedulerPolicy, SchedulingDecision, SnapshotValidationError,
    StructuralPriority, TaskPriority, UnplacedReason, UnplacedTask,
    WorkerPreference, WorkerRejection, WorkerState,
)


DataKey = tuple[str, str | None]
InputLookup = tuple[str, DataKey]  # logical task input ID -> physical data-location key


def _structure(plan: ExecutionPlan, budget: int) -> Mapping[str, StructuralPriority]:
    # A temporary view of the SAME original records delegates graph validation
    # and topological ordering to DAG. No inferred edges, AST pass or readiness.
    graph = DAG(tasks=(m.task for m in plan.tasks), values=plan.values,
                edges=plan.edges, definitions=plan.definitions)
    order = graph.topological_order()
    positions = {task_id: i for i, task_id in enumerate(order)}
    # Bounded exact bitsets. This is a conservative allocation estimate, not a
    # resident-memory measurement. Above the bound, omit ALL descendant counts.
    exact_counts = len(order) * (2 * ((len(order) + 7) // 8) + 128) <= budget
    descendants: dict[str, int] = {}
    result: dict[str, StructuralPriority] = {}
    for task_id in reversed(order):
        children = plan.task_index[task_id].task.dependents
        depth = max((result[c].downstream_depth + 1 for c in children), default=0)
        count = None
        if exact_counts:
            bits = 0
            for child in children:
                bits |= descendants[child] | (1 << positions[child])
            descendants[task_id] = bits
            count = bits.bit_count()  # shared diamond descendants counted once
        result[task_id] = StructuralPriority(depth, count, len(children))
    return MappingProxyType(result)


def _input_keys(plan: ExecutionPlan, task: TaskManifest) -> tuple[tuple[InputLookup, ...], tuple[str, ...]]:
    """Map logical inputs to certified transferable representation keys.

    Only immutable plain aliases are resolved to their backing representation.
    Shared-reference snapshot identities stay exact because alias/object-state
    semantics there are versioned and must never be inferred by the scheduler.
    """
    if task.mode != ExecutionMode.ISOLATED_CANDIDATE:
        # F58: a prepared context owns native-produced state, but isolated ancestors
        # may have executed on another worker.  Their latest live transferable
        # bindings must therefore participate in locality/transfer planning.
        keys: list[InputLookup] = []
        for value in plan.context_seed_requirements(task.task_id):
            if value.kind == ValueKind.IMMUTABLE:
                keys.append((value.id, (plan.immutable_representation_id(value.id), None)))
            elif value.kind == ValueKind.SHARED_REFERENCE:
                # Isolated shared-reference outputs are published as an object
                # snapshot with no native object-state version yet.
                keys.append((value.id, (value.id, None)))
        return tuple(keys), ()
    objects = {obj.object_id: obj for obj in task.objects}
    keys: list[InputLookup] = []
    unsupported = []
    for value in task.inputs:
        if value.kind == ValueKind.IMMUTABLE:
            keys.append((value.id, (plan.immutable_representation_id(value.id), None)))
        elif value.kind == ValueKind.SHARED_REFERENCE:
            obj = objects[value.value.object_id]
            if obj.access != ObjectAccess.SNAPSHOT_CANDIDATE or len(obj.state_inputs) > 1:
                unsupported.append(value.id)
            else:
                keys.append((value.id, (value.id, next(iter(obj.state_inputs), None))))
        elif value.kind != ValueKind.CODE_BINDING and not value.is_state_token:
            unsupported.append(value.id)  # fail closed for future unsupported kinds
    return tuple(keys), tuple(unsupported)


class _SnapshotIndex:
    """Call-local lookup indexes. No reservations or mutable truth live here."""
    def __init__(self, snapshot: ClusterSnapshot):
        self.workers = {w.worker_id: w for w in sorted(snapshot.workers, key=lambda w: w.worker_id)}
        self.affinities = {a.task_id: a for a in snapshot.affinities}
        self.contexts = {c.context_id: c for c in snapshot.contexts}
        self.data: dict[DataKey, tuple[DataLocation, frozenset[str]]] = {}
        for location in snapshot.data:
            sources = frozenset(r.worker_id for r in location.replicas
                                if r.status == ReplicaStatus.AVAILABLE and self.workers[r.worker_id].online)
            self.data[location.value_id, location.object_state_id] = location, sources


def _locality(keys: tuple[InputLookup, ...], unsupported: tuple[str, ...],
              worker_id: str, facts: _SnapshotIndex) -> tuple[Locality, tuple[str, ...]]:
    local, remote, missing = [], [], list(unsupported)
    known_bytes = unknown_count = 0
    for logical_id, key in keys:
        entry = facts.data.get(key)
        if entry is None or not entry[1]:
            missing.append(logical_id)
            continue
        location, sources = entry
        if worker_id in sources:
            local.append(logical_id)
        else:
            remote.append(logical_id)
            if location.size_bytes is None:
                unknown_count += 1
            else:
                known_bytes += location.size_bytes
    return Locality(tuple(local), tuple(remote), known_bytes, unknown_count), tuple(missing)


@dataclass(frozen=True)
class _Pair:
    locality: Locality
    rejection: WorkerRejection
    context_id: str | None


def _evaluate(task: TaskManifest, worker: WorkerState, plan: ExecutionPlan,
              facts: _SnapshotIndex, needs: tuple[tuple[InputLookup, ...], tuple[str, ...]]) -> _Pair:
    reasons: list[UnplacedReason] = []
    if not worker.online:
        reasons.append(UnplacedReason.OFFLINE)
    if not worker.accepting_work:
        reasons.append(UnplacedReason.NOT_ACCEPTING)
    if not worker.free_slots:
        reasons.append(UnplacedReason.NO_CAPACITY)
    if plan.program.environment_id not in worker.environment_ids:
        reasons.append(UnplacedReason.ENVIRONMENT_MISMATCH)
    if task.code.program_id not in worker.prepared_program_ids:
        reasons.append(UnplacedReason.PROGRAM_UNAVAILABLE)
    if task.mode not in worker.supported_modes:
        reasons.append(UnplacedReason.MODE_UNSUPPORTED)
    affinity = facts.affinities.get(task.task_id)
    context_id = affinity.context_id if affinity else None
    if affinity and ((affinity.required_worker is not None and affinity.required_worker != worker.worker_id)
                     or (affinity.allowed_workers is not None and worker.worker_id not in affinity.allowed_workers)):
        reasons.append(UnplacedReason.AFFINITY_MISMATCH)
    if task.mode != ExecutionMode.ISOLATED_CANDIDATE or context_id is not None:
        context = facts.contexts.get(context_id)
        if (context is None or context.worker_id != worker.worker_id
                or task.task_id not in context.prepared_task_ids or context.available_slots == 0):
            reasons.append(UnplacedReason.CONTEXT_UNAVAILABLE)
    locality, missing = _locality(*needs, worker.worker_id, facts)
    if missing:
        reasons.append(UnplacedReason.INPUT_UNAVAILABLE)
    return _Pair(locality, WorkerRejection(worker.worker_id, tuple(reasons), missing), context_id)


def _task_key(ready: ReadyTask, structure: StructuralPriority, feasible: int,
              policy: SchedulerPolicy) -> tuple:
    importance = (-structure.downstream_depth, -(structure.descendant_count or 0),
                  -structure.immediate_dependents)
    if ready.wait_rounds >= policy.fairness_after_rounds:
        # FIFO promotion takes precedence over structural importance/scarcity.
        return (0, ready.ready_sequence, -ready.wait_rounds, feasible, *importance, ready.task_id)
    return (1, feasible, *importance, ready.ready_sequence, -ready.wait_rounds, ready.task_id)


def _preference(worker: WorkerState, free: int, locality: Locality) -> WorkerPreference:
    total = worker.total_memory_bytes
    ram_band = ((total - worker.available_memory_bytes) * 10 // total) if total else 10
    return WorkerPreference(locality, free, ram_band, int(worker.cpu_percent // 10),
                            worker.available_memory_bytes, worker.cpu_cores)


def _worker_key(worker_id: str, preference: WorkerPreference) -> tuple:
    data = preference.locality
    return (bool(data.remote_input_ids), data.unknown_remote_inputs, data.known_remote_bytes,
            len(data.remote_input_ids), -preference.free_slots_before,
            preference.ram_pressure_band, preference.cpu_pressure_band,
            -preference.available_memory_bytes, -preference.cpu_cores, worker_id)


def _unplaced(task_id: str, pairs: Mapping[str, _Pair], free: Mapping[str, int],
              context_free: Mapping[str, int]) -> UnplacedTask:
    failures = []
    for worker_id, pair in pairs.items():
        reasons = set(pair.rejection.reasons)
        if free[worker_id] == 0:
            reasons.add(UnplacedReason.NO_CAPACITY)
        if pair.context_id is not None and context_free.get(pair.context_id, 0) == 0:
            reasons.add(UnplacedReason.CONTEXT_UNAVAILABLE)
        failures.append(WorkerRejection(worker_id, tuple(r for r in UnplacedReason if r in reasons),
                                        pair.rejection.missing_input_ids))
    # Preserve mixed failures rather than pretend one remedy would fix them all.
    reason_sets = {f.reasons for f in failures}
    uniform = next(iter(reason_sets)) if len(reason_sets) == 1 else ()
    reason = uniform[0] if len(uniform) == 1 else UnplacedReason.NO_ELIGIBLE_WORKER
    return UnplacedTask(task_id, reason, tuple(failures))


@dataclass(frozen=True)
class Scheduler:
    """Reusable STATIC plan preprocessing, never a copy of live coordinator state.

    schedule(snapshot) is a pure function of this fixed plan/policy and snapshot.
    There are no callbacks, worker objects, readiness counters, attempt allocation
    or cached decisions. All shadow reservations die when a call returns.
    """
    plan: ExecutionPlan
    policy: SchedulerPolicy = field(default_factory=SchedulerPolicy)
    structure: Mapping[str, StructuralPriority] = field(init=False, repr=False)
    _needs: Mapping[str, tuple[tuple[InputLookup, ...], tuple[str, ...]]] = field(init=False, repr=False)

    def __post_init__(self):
        if not isinstance(self.plan, ExecutionPlan) or not isinstance(self.policy, SchedulerPolicy):
            raise SnapshotValidationError("ExecutionPlan and SchedulerPolicy required")
        object.__setattr__(self, "structure", _structure(self.plan, self.policy.descendant_budget_bytes))
        object.__setattr__(self, "_needs", MappingProxyType({m.task_id: _input_keys(self.plan, m) for m in self.plan.tasks}))

    def schedule(self, snapshot: ClusterSnapshot) -> SchedulingDecision:
        if not isinstance(snapshot, ClusterSnapshot):
            raise SnapshotValidationError("ClusterSnapshot required")
        snapshot.validate(self.plan)
        facts = _SnapshotIndex(snapshot)
        free = {ident: w.free_slots for ident, w in facts.workers.items()}
        context_free = {ident: c.available_slots for ident, c in facts.contexts.items()}
        ready = {t.task_id: t for t in snapshot.ready}
        pairs: dict[str, dict[str, _Pair]] = {}
        worker_tasks: dict[str, set[str]] = {w: set() for w in facts.workers}
        context_tasks: dict[str, set[tuple[str, str]]] = {c: set() for c in facts.contexts}
        eligible: dict[str, set[str]] = {}
        heap = []
        for task_id in sorted(ready):
            task = self.plan.task_index[task_id]
            pairs[task_id] = {w: _evaluate(task, worker, self.plan, facts, self._needs[task_id])
                              for w, worker in facts.workers.items()}
            eligible[task_id] = {w for w, pair in pairs[task_id].items() if not pair.rejection.reasons}
            count = len(eligible[task_id])
            for worker_id in eligible[task_id]:
                worker_tasks[worker_id].add(task_id)
                context_id = pairs[task_id][worker_id].context_id
                if context_id is not None:
                    context_tasks[context_id].add((task_id, worker_id))
            if count:
                heapq.heappush(heap, (_task_key(ready[task_id], self.structure[task_id], count, self.policy), task_id, count))
        pending = set(ready)
        placements = []
        while heap:
            _, task_id, count = heapq.heappop(heap)
            if task_id not in pending or count != len(eligible[task_id]) or count == 0:
                continue
            options = []
            for worker_id, pair in pairs[task_id].items():
                if worker_id in eligible[task_id]:
                    preference = _preference(facts.workers[worker_id], free[worker_id], pair.locality)
                    options.append((_worker_key(worker_id, preference), worker_id, preference))
            _, worker_id, preference = min(options)
            info = ready[task_id]
            priority = TaskPriority(info.wait_rounds >= self.policy.fairness_after_rounds,
                                    info.ready_sequence, info.wait_rounds, count, self.structure[task_id])
            placements.append(Placement(task_id, worker_id, pairs[task_id][worker_id].context_id,
                                        priority, preference))
            pending.remove(task_id)
            free[worker_id] -= 1
            exhausted_pairs = set()
            if free[worker_id] == 0:
                exhausted_pairs.update((other, worker_id) for other in worker_tasks[worker_id])
            context_id = pairs[task_id][worker_id].context_id
            if context_id is not None:
                context_free[context_id] -= 1
                if context_free[context_id] == 0:
                    exhausted_pairs.update(context_tasks[context_id])
            # Worker and context may fill together. Remove each pair only once.
            for other, owner in sorted(exhausted_pairs):
                if other in pending and owner in eligible[other]:
                    eligible[other].remove(owner)
                    count = len(eligible[other])
                    if count:
                        key = _task_key(ready[other], self.structure[other], count, self.policy)
                        heapq.heappush(heap, (key, other, count))
        unplaced = tuple(_unplaced(task_id, pairs[task_id], free, context_free) for task_id in sorted(pending))
        return SchedulingDecision(self.plan.id, snapshot.run_id, snapshot.snapshot_id, tuple(placements), unplaced)


def schedule(plan: ExecutionPlan, snapshot: ClusterSnapshot, *,
             policy: SchedulerPolicy | None = None) -> SchedulingDecision:
    """Convenience API. Reuse Scheduler(plan) to preprocess structure only once."""
    return Scheduler(plan, policy or SchedulerPolicy()).schedule(snapshot)
