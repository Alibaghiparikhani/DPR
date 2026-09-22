"""Malformed/coordinator-contradictory facts fail, without changing frozen layers."""
from dataclasses import FrozenInstanceError, replace

import pytest

from execution import AttemptIdentity, ExecutionMode, ValueKind
from scheduler import (
    ClusterSnapshot, CommitmentPhase, DataForm, DataLocation, Locality, ReadyTask,
    Replica, ReplicaStatus, Scheduler, SchedulerPolicy, SchedulingDecision,
    SnapshotValidationError, TaskAffinity, TaskCommitment, WorkerContext,
    WorkerState, schedule,
)


@pytest.mark.parametrize("field,value", [
    ("total_slots", -1), ("total_slots", 1.5), ("total_slots", True),
    ("running_slots", -1), ("running_slots", 2), ("reserved_slots", 2),
    ("total_memory_bytes", -1), ("available_memory_bytes", -1),
    ("available_memory_bytes", 100_000), ("cpu_cores", 0),
    ("cpu_percent", -1), ("cpu_percent", 101), ("cpu_percent", float("nan")),
    ("cpu_percent", float("inf")), ("cpu_percent", True), ("cpu_percent", 10**1000),
    ("online", 1), ("accepting_work", "yes"), ("worker_id", ""),
    ("supported_modes", {"isolated_candidate"}), ("environment_ids", "environment"),
])
def test_impossible_worker_facts_rejected(roots, worker, field, value):
    _, plan = roots
    with pytest.raises(SnapshotValidationError):
        replace(worker(plan), **{field: value})


def test_combined_reservations_cannot_exceed_capacity(roots, worker):
    with pytest.raises(SnapshotValidationError, match="capacity"):
        worker(roots[1], total_slots=2, running_slots=1, reserved_slots=2)


@pytest.mark.parametrize("constructor", [
    lambda: ReadyTask("T1", -1), lambda: ReadyTask("T1", 0, -1),
    lambda: ReadyTask("", 0), lambda: Replica(""), lambda: Replica("W1", "available"),
    lambda: DataLocation("V1", DataForm.IMMUTABLE_VALUE, size_bytes=-1),
    lambda: DataLocation("V1", DataForm.IMMUTABLE_VALUE, size_bytes=True),
    lambda: DataLocation("V1", "immutable_value"),
    lambda: DataLocation("V1", DataForm.IMMUTABLE_VALUE, object_state_id="S1"),
    lambda: DataLocation("V1", DataForm.IMMUTABLE_VALUE, (Replica("W1"), Replica("W1"))),
    lambda: TaskAffinity("T1", required_worker="W1", allowed_workers={"W2"}),
    lambda: SchedulerPolicy(fairness_after_rounds=0),
    lambda: SchedulerPolicy(descendant_budget_bytes=-1),
    lambda: WorkerContext("C", "W1", available_slots=-1),
    lambda: Locality(["V1"], ["V1"]),
    lambda: Locality(unknown_remote_inputs=1),
])
def test_malformed_records_rejected(constructor):
    with pytest.raises(SnapshotValidationError):
        constructor()


@pytest.mark.parametrize("field", ["plan_id", "run_id", "snapshot_id"])
def test_empty_snapshot_identity_rejected(roots, snapshot, field):
    with pytest.raises(SnapshotValidationError):
        replace(snapshot(roots[1]), **{field: ""})


@pytest.mark.parametrize("kind", ["workers", "ready", "affinities", "contexts", "data", "commitments"])
def test_duplicate_snapshot_records_rejected(roots, worker, snapshot, kind):
    dag, plan = roots
    t = dag.sources()[0]
    records = {
        "workers": worker(plan), "ready": ReadyTask(t, 0), "affinities": TaskAffinity(t),
        "contexts": WorkerContext("C1", "W1"),
        "data": DataLocation(plan.final_bindings["a"], DataForm.IMMUTABLE_VALUE),
        "commitments": TaskCommitment(AttemptIdentity(plan.id, "run-1", t, "a1"), "W1", CommitmentPhase.RESERVED),
    }
    with pytest.raises(SnapshotValidationError, match="Duplicate"):
        replace(snapshot(plan), **{kind: (records[kind], records[kind])})


@pytest.mark.parametrize("kind", ["ready", "blocked_task_ids", "completed_task_ids", "affinities", "contexts"])
def test_foreign_task_references_rejected(roots, worker, snapshot, kind):
    _, plan = roots
    changes = {"ready": (ReadyTask("foreign", 0),), "blocked_task_ids": {"foreign"},
               "completed_task_ids": {"foreign"}, "affinities": (TaskAffinity("foreign"),),
               "contexts": (WorkerContext("C", "W1", {"foreign"}),)}
    with pytest.raises(SnapshotValidationError, match="[Uu]nknown task"):
        schedule(plan, replace(snapshot(plan, workers=(worker(plan),)), **{kind: changes[kind]}))


@pytest.mark.parametrize("kind", ["required", "allowed", "context", "replica", "commitment"])
def test_unknown_worker_references_rejected(roots, worker, snapshot, kind):
    dag, plan = roots
    task = dag.sources()[0]
    changes = {
        "required": {"affinities": (TaskAffinity(task, required_worker="missing"),)},
        "allowed": {"affinities": (TaskAffinity(task, allowed_workers={"missing"}),)},
        "context": {"contexts": (WorkerContext("C", "missing"),)},
        "replica": {"data": (DataLocation(plan.final_bindings["a"], DataForm.IMMUTABLE_VALUE, (Replica("missing"),)),)},
        "commitment": {"commitments": (TaskCommitment(AttemptIdentity(plan.id, "run-1", task, "a1"),
                                                        "missing", CommitmentPhase.RUNNING),)},
    }
    with pytest.raises(SnapshotValidationError, match="[Uu]nknown.*worker"):
        schedule(plan, replace(snapshot(plan, workers=(worker(plan),)), **changes[kind]))


@pytest.mark.parametrize("kind", ["plan", "attempt_plan", "attempt_run"])
def test_plan_and_run_identity_cannot_be_mixed(roots, worker, snapshot, kind):
    dag, plan = roots
    state = snapshot(plan, workers=(worker(plan, running_slots=1),))
    if kind == "plan":
        state = replace(state, plan_id="foreign-plan")
    else:
        attempt = AttemptIdentity("0" * 64 if kind == "attempt_plan" else plan.id,
                                  "foreign" if kind == "attempt_run" else "run-1", dag.sources()[0], "a1")
        state = replace(state, commitments=(TaskCommitment(attempt, "W1", CommitmentPhase.RUNNING),))
    with pytest.raises(SnapshotValidationError, match="Foreign"):
        schedule(plan, state)


@pytest.mark.parametrize("inactive", ["blocked_task_ids", "completed_task_ids"])
def test_ready_and_inactive_is_corrupt(roots, worker, snapshot, inactive):
    dag, plan = roots
    task = dag.sources()[0]
    state = replace(snapshot(plan, (task,), (worker(plan),)), **{inactive: {task}})
    with pytest.raises(SnapshotValidationError, match="contradictory"):
        schedule(plan, state)


def test_missing_context_is_unplaced_but_conflicting_context_is_invalid(roots, worker, snapshot):
    dag, plan = roots
    task = dag.sources()[0]
    state = snapshot(plan, (task,), (worker(plan, "W1"), worker(plan, "W2")),
                     affinities=(TaskAffinity(task, required_worker="W1", context_id="C"),))
    assert not schedule(plan, state).placements  # known worker; context not prepared yet
    with pytest.raises(SnapshotValidationError, match="conflicts"):
        schedule(plan, replace(state, contexts=(WorkerContext("C", "W2", {task}),)))


@pytest.mark.parametrize("phase", list(CommitmentPhase))
def test_commitments_must_be_covered_by_counters(roots, worker, snapshot, phase):
    dag, plan = roots
    commitment = TaskCommitment(AttemptIdentity(plan.id, "run-1", dag.sources()[0], "a1"), "W1", phase)
    state = snapshot(plan, workers=(worker(plan),), commitments=(commitment,))
    with pytest.raises(SnapshotValidationError, match="counters"):
        schedule(plan, state)


def test_unknown_value_is_invalid(roots, worker, snapshot):
    _, plan = roots
    state = snapshot(plan, workers=(worker(plan),), data=(DataLocation("foreign", DataForm.IMMUTABLE_VALUE),))
    with pytest.raises(SnapshotValidationError, match="unknown value"):
        schedule(plan, state)


@pytest.mark.parametrize("source,kind", [
    ("getattr(obj,name)\n1+2", ValueKind.NAMESPACE_STATE),
    ("a=1\nb=10//a\n1+2", ValueKind.COMPLETION_STATE),
    ("a=[1]\na.append(2)\nb=sum(a)", ValueKind.OBJECT_STATE),
    ("unknown()", ValueKind.NATIVE_REFERENCE),
    ("def f():return 1\na=f()", ValueKind.CODE_BINDING),
])
def test_state_native_and_code_cannot_have_payload_locations(build_plan, worker, snapshot, source, kind):
    """Even a bogus zero/huge size cannot turn a semantic token into data."""
    _, plan = build_plan(source)
    value = next(v for m in plan.tasks for v in (*m.inputs, *m.outputs) if v.kind == kind)
    state = snapshot(plan, workers=(worker(plan),), data=(DataLocation(value.id, DataForm.IMMUTABLE_VALUE,
                                                                      (Replica("W1"),), 10**12),))
    with pytest.raises(SnapshotValidationError, match="not payload"):
        schedule(plan, state)


def test_lists_require_explicit_snapshot_form(build_plan, worker, snapshot):
    _, plan = build_plan("a=[1]\nb=sum(a)")
    data = DataLocation(plan.final_bindings["a"], DataForm.IMMUTABLE_VALUE, (Replica("W1"),))
    with pytest.raises(SnapshotValidationError, match="certified object snapshots"):
        schedule(plan, snapshot(plan, workers=(worker(plan),), data=(data,)))


@pytest.mark.parametrize("state_kind", ["other_object", "scalar", "foreign"])
def test_snapshot_cannot_claim_foreign_object_version(build_plan, worker, snapshot, state_kind):
    _, plan = build_plan("a=[1]\nb=[2]\nb.append(3)\nz=1")
    state_id = {"other_object": next(v.id for m in plan.tasks for v in m.outputs if v.kind == ValueKind.OBJECT_STATE),
                "scalar": plan.final_bindings["z"], "foreign": "foreign"}[state_kind]
    data = DataLocation(plan.final_bindings["a"], DataForm.OBJECT_SNAPSHOT, (Replica("W1"),), object_state_id=state_id)
    with pytest.raises(SnapshotValidationError, match="different or unknown object"):
        schedule(plan, snapshot(plan, workers=(worker(plan),), data=(data,)))


def test_inputs_and_outputs_are_deeply_immutable(roots, worker, snapshot):
    dag, plan = roots
    workers = [worker(plan)]
    ids = [dag.sources()[0]]
    state = snapshot(plan, ids, workers)
    decision = schedule(plan, state)
    workers.clear()
    ids.clear()
    assert len(state.workers) == len(state.ready) == 1
    with pytest.raises(FrozenInstanceError):
        state.snapshot_id = "other"
    with pytest.raises(FrozenInstanceError):
        decision.placements[0].worker_id = "other"
    with pytest.raises(FrozenInstanceError):
        state.workers[0].reserved_slots = 1
    with pytest.raises(TypeError):
        Scheduler(plan).structure[dag.sources()[0]] = None
    local_ids = ["V1"]
    locality = Locality(local_ids)
    local_ids.clear()
    assert locality.local_input_ids == ("V1",)
    with pytest.raises(SnapshotValidationError, match="Duplicate"):
        replace(decision, placements=decision.placements * 2)


def test_only_worker_records_can_enter_cluster(roots, snapshot):
    """A generic/coordinator hardware dictionary is not an enrolled WorkerState."""
    with pytest.raises(SnapshotValidationError, match="WorkerState"):
        snapshot(roots[1], workers=({"coordinator": True, "cpu_cores": 100},))


def test_scheduler_rejects_noncontracts(roots):
    with pytest.raises(SnapshotValidationError):
        Scheduler({})
    with pytest.raises(SnapshotValidationError):
        Scheduler(roots[1]).schedule({})
