"""Each test states the expected legal placement or preference, using real plans."""
from dataclasses import replace

import pytest

from execution import AttemptIdentity, ExecutionMode
from scheduler import (
    CommitmentPhase, DataForm, DataLocation, ReadyTask, Replica, ReplicaStatus,
    Scheduler, SchedulerPolicy, SnapshotValidationError, TaskAffinity,
    TaskCommitment, UnplacedReason, WorkerContext, schedule,
)


def test_empty_ready_never_invents_work(roots, worker, snapshot):
    """No supplied READY tasks => no proposals, despite idle slots and DAG roots."""
    _, plan = roots
    decision = schedule(plan, snapshot(plan, workers=(worker(plan),)))
    assert decision.placements == decision.unplaced == ()


def test_no_workers_has_no_coordinator_fallback(roots, snapshot):
    """A control-only cluster leaves all ready computations unplaced."""
    dag, plan = roots
    decision = schedule(plan, snapshot(plan, dag.sources()))
    assert not decision.placements
    assert {u.task_id for u in decision.unplaced} == set(dag.sources())
    assert all(u.reason == UnplacedReason.NO_ELIGIBLE_WORKER for u in decision.unplaced)
    assert all(not u.workers for u in decision.unplaced)


def test_one_task_one_worker_identity(roots, worker, snapshot):
    """One compatible slot yields exactly one scoped proposal."""
    dag, plan = roots
    state = snapshot(plan, dag.sources()[:1], (worker(plan),))
    decision = schedule(plan, state)
    assert [(p.task_id, p.worker_id) for p in decision.placements] == [(dag.sources()[0], "W1")]
    assert (decision.plan_id, decision.run_id, decision.snapshot_id) == (plan.id, state.run_id, state.snapshot_id)
    assert not decision.unplaced


@pytest.mark.parametrize("change,reason", [
    ({"online": False}, UnplacedReason.OFFLINE),
    ({"accepting_work": False}, UnplacedReason.NOT_ACCEPTING),
    ({"total_slots": 0}, UnplacedReason.NO_CAPACITY),
    ({"running_slots": 1}, UnplacedReason.NO_CAPACITY),
    ({"reserved_slots": 1}, UnplacedReason.NO_CAPACITY),
    ({"environment_ids": {"other-env"}}, UnplacedReason.ENVIRONMENT_MISMATCH),
    ({"prepared_program_ids": {"other-program"}}, UnplacedReason.PROGRAM_UNAVAILABLE),
    ({"supported_modes": set()}, UnplacedReason.MODE_UNSUPPORTED),
])
def test_hard_ineligibility_cannot_be_outscored(roots, worker, snapshot, change, reason):
    """A hard failure wins over idle CPU, large RAM and any favorable score."""
    dag, plan = roots
    w = worker(plan, cpu_percent=0, **change)
    decision = schedule(plan, snapshot(plan, dag.sources()[:1], (w,)))
    assert not decision.placements
    assert decision.unplaced[0].reason == reason
    assert decision.unplaced[0].workers[0].reasons == (reason,)


def test_program_id_includes_package_identity(roots, worker, snapshot):
    """Same source/environment but a different package is not prepared code."""
    dag, plan = roots
    from execution import lower_dag
    other = lower_dag(dag, environment_id=plan.program.environment_id, package_id="other-package")
    w = worker(plan, prepared_program_ids={other.program.id})
    assert schedule(plan, snapshot(plan, dag.sources()[:1], (w,))).unplaced[0].reason == UnplacedReason.PROGRAM_UNAVAILABLE


def test_same_call_and_existing_reservations(roots, worker, snapshot):
    """4 - 1 running - 1 reserved leaves two slots for four ready tasks."""
    dag, plan = roots
    w = worker(plan, total_slots=4, running_slots=1, reserved_slots=1)
    state = snapshot(plan, dag.sources(), (w,))
    result = schedule(plan, state)
    assert len(result.placements) == len(result.unplaced) == 2
    assert [p.preference.free_slots_before for p in result.placements] == [2, 1]
    assert all(u.reason == UnplacedReason.NO_CAPACITY for u in result.unplaced)
    assert w.free_slots == 2
    assert schedule(plan, state) == result  # proposals do not reserve live truth


@pytest.mark.parametrize("phase", list(CommitmentPhase))
def test_commitments_not_rescheduled_or_subtracted_twice(roots, worker, snapshot, phase):
    """Existing commitment occupies reported counters, leaving one slot for another task."""
    dag, plan = roots
    a, b, *_ = dag.sources()
    commitment = TaskCommitment(AttemptIdentity(plan.id, "run-1", a, "attempt-1"), "W1", phase)
    fields = {"running_slots" if phase == CommitmentPhase.RUNNING else "reserved_slots": 1}
    w = worker(plan, total_slots=2, **fields)
    state = snapshot(plan, (b,), (w,), commitments=(commitment,))
    assert [p.task_id for p in schedule(plan, state).placements] == [b]
    with pytest.raises(SnapshotValidationError, match="contradictory"):
        schedule(plan, replace(state, ready=(ReadyTask(a, 0), ReadyTask(b, 1))))


def test_scarce_task_keeps_only_worker(roots, worker, snapshot):
    """An earlier flexible task must not steal W1 from a constrained sibling."""
    dag, plan = roots
    flexible, scarce, *_ = dag.sources()
    state = snapshot(plan, (flexible, scarce), (worker(plan, "W1"), worker(plan, "W2")),
                     affinities=(TaskAffinity(scarce, required_worker="W1"),))
    result = schedule(plan, state)
    assert [(p.task_id, p.worker_id) for p in result.placements] == [(scarce, "W1"), (flexible, "W2")]
    assert [p.priority.feasible_workers for p in result.placements] == [1, 1]


def test_scarcity_updated_when_worker_fills(roots, worker, snapshot):
    """After W1 fills, W1/W2 task becomes scarce and precedes a W2/W3 task."""
    dag, plan = roots
    flexible, narrowing, pinned, *_ = dag.sources()
    state = snapshot(plan, (flexible, narrowing, pinned), tuple(worker(plan, f"W{i}") for i in (1, 2, 3)),
                     affinities=(TaskAffinity(pinned, required_worker="W1"),
                                 TaskAffinity(narrowing, allowed_workers={"W1", "W2"}),
                                 TaskAffinity(flexible, allowed_workers={"W2", "W3"})))
    assert [(p.task_id, p.worker_id) for p in schedule(plan, state).placements] == [
        (pinned, "W1"), (narrowing, "W2"), (flexible, "W3")]


def test_empty_allowed_workers_excludes_everyone(roots, worker, snapshot):
    dag, plan = roots
    task = dag.sources()[0]
    state = snapshot(plan, (task,), (worker(plan),), affinities=(TaskAffinity(task, allowed_workers=set()),))
    assert schedule(plan, state).unplaced[0].reason == UnplacedReason.AFFINITY_MISMATCH


@pytest.mark.parametrize("source,mode", [
    ("getattr(obj, name)", ExecutionMode.SHARED_CONTEXT),
    ("globals()\nx=1", ExecutionMode.NATIVE_REGION),
    ("a=[1]\na.append(2)", ExecutionMode.SHARED_CONTEXT),
    ("a=1\nb=10//a", ExecutionMode.SHARED_CONTEXT),
])
def test_native_and_shared_require_exact_prepared_context(build_plan, worker, snapshot, source, mode):
    """A worker pin alone is insufficient; only prepared context owner W2 can run it."""
    _, plan = build_plan(source)
    task = plan.tasks[-1]
    assert task.mode == mode
    workers = (worker(plan, "W1"), worker(plan, "W2", cpu_percent=99))
    state = snapshot(plan, (task.task_id,), workers,
                     affinities=(TaskAffinity(task.task_id, required_worker="W2"),))
    assert not schedule(plan, state).placements
    state = snapshot(plan, (task.task_id,), workers,
                     affinities=(TaskAffinity(task.task_id, context_id="native"),),
                     contexts=(WorkerContext("native", "W2", {task.task_id}),))
    result = schedule(plan, state)
    assert [(p.worker_id, p.context_id) for p in result.placements] == [("W2", "native")]
    unavailable = replace(state, contexts=(WorkerContext("native", "W2"),))
    assert not schedule(plan, unavailable).placements


def test_other_task_context_does_not_certify_this_task(build_plan, worker, snapshot):
    _, plan = build_plan("unknown()\nother()")
    a, b = plan.tasks
    state = snapshot(plan, (b.task_id,), (worker(plan),),
                     affinities=(TaskAffinity(b.task_id, context_id="native"),),
                     contexts=(WorkerContext("native", "W1", {a.task_id}),))
    assert schedule(plan, state).unplaced[0].reason == UnplacedReason.CONTEXT_UNAVAILABLE


@pytest.mark.parametrize("available,expected", [(0, 0), (1, 1), (2, 2)])
def test_context_capacity_is_independent_of_worker_slots(build_plan, worker, snapshot, available, expected):
    """Independent list mutations may share a worker but cannot overbook a native context."""
    _, plan = build_plan("a=[1]\nb=[2]\na.append(3)\nb.append(4)")
    ids = tuple(t.task_id for t in plan.tasks[-2:])
    assert not plan.tasks[-2].dependencies.intersection(ids)
    state = snapshot(plan, ids, (worker(plan, total_slots=8),),
                     contexts=(WorkerContext("C", "W1", set(ids), available),),
                     affinities=tuple(TaskAffinity(t, context_id="C") for t in ids))
    result = schedule(plan, state)
    assert len(result.placements) == expected
    assert all(u.reason == UnplacedReason.CONTEXT_UNAVAILABLE for u in result.unplaced)


def test_context_exhaustion_does_not_block_unrelated_worker_slot(roots, worker, snapshot):
    dag, plan = roots
    a, b, c, *_ = dag.sources()
    state = snapshot(plan, (a, b, c), (worker(plan, total_slots=3),),
                     contexts=(WorkerContext("C", "W1", {a, b}),),
                     affinities=(TaskAffinity(a, context_id="C"), TaskAffinity(b, context_id="C")))
    result = schedule(plan, state)
    assert [p.task_id for p in result.placements] == [a, c]
    assert result.unplaced[0].task_id == b


@pytest.fixture
def input_plan(build_plan):
    dag, plan = build_plan("a=1\nb=2\nc=a+b")
    return dag, plan, plan.tasks[-1], plan.final_bindings["a"], plan.final_bindings["b"]


def loc(value_id, holders=(), size=None, *, status=ReplicaStatus.AVAILABLE):
    return DataLocation(value_id, DataForm.IMMUTABLE_VALUE, tuple(Replica(w, status) for w in holders), size)


def test_all_local_beats_idle_remote_worker(input_plan, worker, snapshot):
    """Known huge local inputs favor busy-but-available W1 without predicting duration."""
    _, plan, task, a, b = input_plan
    workers = (worker(plan, "W1", total_slots=4, running_slots=3, cpu_percent=90),
               worker(plan, "W2", total_slots=8, cpu_percent=0))
    state = snapshot(plan, (task.task_id,), workers, data=(loc(a, ("W1",), 10**12), loc(b, ("W1",), 1)))
    result = schedule(plan, state)
    assert result.placements[0].worker_id == "W1"
    assert set(result.placements[0].preference.locality.local_input_ids) == {a, b}
    # No free slot on W1: transfer now, never wait for imagined future availability.
    state = replace(state, workers=(replace(workers[0], running_slots=4), workers[1]))
    chosen = schedule(plan, state).placements[0]
    assert chosen.worker_id == "W2"
    assert chosen.preference.locality.known_remote_bytes == 10**12 + 1


def test_split_locality_prefers_fewer_known_remote_bytes(input_plan, worker, snapshot):
    _, plan, task, a, b = input_plan
    state = snapshot(plan, (task.task_id,), (worker(plan, "W1"), worker(plan, "W2")),
                     data=(loc(a, ("W1",), 1000), loc(b, ("W2",), 10)))
    chosen = schedule(plan, state).placements[0]
    assert chosen.worker_id == "W1"
    assert chosen.preference.locality.known_remote_bytes == 10


def test_unknown_remote_is_explicit_not_free(input_plan, worker, snapshot):
    """Prefer transferring a known size to one unknown transfer, even if the known size is large."""
    _, plan, task, a, b = input_plan
    state = snapshot(plan, (task.task_id,), (worker(plan, "W1"), worker(plan, "W2")),
                     data=(loc(a, ("W1",), None), loc(b, ("W2",), 10**12)))
    chosen = schedule(plan, state).placements[0]
    assert chosen.worker_id == "W1"
    assert chosen.preference.locality.unknown_remote_inputs == 0
    state = replace(state, affinities=(TaskAffinity(task.task_id, required_worker="W2"),))
    chosen = schedule(plan, state).placements[0]
    assert chosen.preference.locality.unknown_remote_inputs == 1
    assert chosen.preference.locality.remote_input_ids == (a,)
    assert state.data[0].size_bytes is None


def test_unknown_inputs_still_place_without_waiting(input_plan, worker, snapshot):
    _, plan, task, a, b = input_plan
    state = snapshot(plan, (task.task_id,), (worker(plan, "W1", accepting_work=False), worker(plan, "W2")),
                     data=(loc(a, ("W1",)), loc(b, ("W1",))))
    chosen = schedule(plan, state).placements[0]
    assert chosen.worker_id == "W2"
    assert chosen.preference.locality.unknown_remote_inputs == 2


def test_replicated_inputs_no_artificial_transfer(input_plan, worker, snapshot):
    _, plan, task, a, b = input_plan
    state = snapshot(plan, (task.task_id,), (worker(plan, "W1"), worker(plan, "W2", cpu_percent=10)),
                     data=(loc(a, ("W1", "W2")), loc(b, ("W1", "W2"))))
    chosen = schedule(plan, state).placements[0]
    assert chosen.worker_id == "W2"
    assert not chosen.preference.locality.remote_input_ids
    assert chosen.preference.locality.unknown_remote_inputs == 0


@pytest.mark.parametrize("missing_kind", ["absent", "no_replicas", "in_flight", "offline"])
def test_input_requires_usable_source(input_plan, worker, snapshot, missing_kind):
    _, plan, task, a, b = input_plan
    workers = (worker(plan, "W1", online=missing_kind != "offline"), worker(plan, "W2"))
    data = [loc(a, ("W2",), 1)]
    if missing_kind != "absent":
        data.append(loc(b, () if missing_kind == "no_replicas" else ("W1",), 100,
                        status=ReplicaStatus.IN_FLIGHT if missing_kind == "in_flight" else ReplicaStatus.AVAILABLE))
    result = schedule(plan, snapshot(plan, (task.task_id,), workers, data=data))
    assert not result.placements
    assert all(UnplacedReason.INPUT_UNAVAILABLE in r.reasons for r in result.unplaced[0].workers)
    assert all(r.missing_input_ids == (b,) for r in result.unplaced[0].workers)


def test_inflight_destination_is_remote_not_local(input_plan, worker, snapshot):
    _, plan, task, a, b = input_plan
    data = (DataLocation(a, DataForm.IMMUTABLE_VALUE,
                         (Replica("W1"), Replica("W2", ReplicaStatus.IN_FLIGHT)), 100), loc(b, ("W2",), 1))
    state = snapshot(plan, (task.task_id,), (worker(plan, "W1"), worker(plan, "W2")), data=data,
                     affinities=(TaskAffinity(task.task_id, required_worker="W2"),))
    assert schedule(plan, state).placements[0].preference.locality.remote_input_ids == (a,)


@pytest.mark.parametrize("better,worse", [
    ({"cpu_percent": 10}, {"cpu_percent": 80}),
    ({"available_memory_bytes": 14_000}, {"available_memory_bytes": 1000}),
    ({"total_slots": 4}, {"total_slots": 2}),
    ({"cpu_cores": 8}, {"cpu_cores": 2}),
    ({"total_memory_bytes": 32_000, "available_memory_bytes": 24_000}, {}),
])
def test_current_load_and_heterogeneity_break_locality_ties(roots, worker, snapshot, better, worse):
    dag, plan = roots
    state = snapshot(plan, dag.sources()[:1], (worker(plan, "W1", **worse), worker(plan, "W2", **better)))
    assert schedule(plan, state).placements[0].worker_id == "W2"


def test_tiny_cpu_fluctuation_uses_same_pressure_band(roots, worker, snapshot):
    dag, plan = roots
    state = snapshot(plan, dag.sources()[:1], (worker(plan, "W1", cpu_percent=42), worker(plan, "W2", cpu_percent=41)))
    assert schedule(plan, state).placements[0].worker_id == "W1"  # final ID tie-break


def test_downstream_depth_prioritizes_longer_chain(build_plan, worker, snapshot):
    dag, plan = build_plan("short=1\nlong=2\nx=long+1\ny=x+1\nz=y+1")
    state = snapshot(plan, dag.sources(), (worker(plan),))
    chosen = schedule(plan, state).placements[0]
    assert chosen.task_id == plan.tasks[1].task_id
    assert chosen.priority.structure.downstream_depth == 3


def test_exact_descendant_count_and_diamond_deduplication(build_plan):
    _, plan = build_plan("a=1\nb=a+1\nc=a+2\nd=b+c")
    scheduler = Scheduler(plan)
    assert scheduler.structure[plan.tasks[0].task_id].descendant_count == 3
    assert scheduler.structure[plan.tasks[0].task_id].immediate_dependents == 2
    bounded = Scheduler(plan, SchedulerPolicy(descendant_budget_bytes=0))
    assert all(s.descendant_count is None for s in bounded.structure.values())
    assert bounded.structure[plan.tasks[0].task_id].downstream_depth == 2


def test_descendants_break_equal_depth_tie(build_plan, worker, snapshot):
    dag, plan = build_plan("a=1\nb=2\nc=a+1\nd=c+1\ne=b+1\nf=e+1\ng=e+2")
    chosen = schedule(plan, snapshot(plan, dag.sources(), (worker(plan),))).placements[0]
    assert chosen.task_id == plan.tasks[1].task_id  # same depth, more distinct descendants


def test_immediate_unlocks_break_equal_depth_descendant_tie(build_plan, worker, snapshot):
    dag, plan = build_plan("a=1\nb=2\nc=a+1\nd=c+1\ne=c+2\nf=b+1\ng=b+2\nh=f+g")
    chosen = schedule(plan, snapshot(plan, dag.sources(), (worker(plan),))).placements[0]
    assert chosen.task_id == plan.tasks[1].task_id  # both depth=2, descendants=3; b unlocks 2


def test_ready_order_breaks_equal_structure(roots, worker, snapshot):
    dag, plan = roots
    state = snapshot(plan, tuple(reversed(dag.sources())), (worker(plan),))
    assert schedule(plan, state).placements[0].task_id == dag.sources()[-1]


def test_age_promotion_prevents_structural_starvation(build_plan, worker, snapshot):
    dag, plan = build_plan("old=1\nnew=2\nx=new+1\ny=x+1")
    old, new = dag.sources()
    state = snapshot(plan, workers=(worker(plan),), ready=(ReadyTask(old, 0, 7), ReadyTask(new, 100, 0)))
    scheduler = Scheduler(plan)
    assert scheduler.schedule(state).placements[0].task_id == new
    promoted = replace(state, ready=(ReadyTask(old, 0, 8), ReadyTask(new, 100, 0)))
    chosen = scheduler.schedule(promoted).placements[0]
    assert chosen.task_id == old and chosen.priority.promoted
    assert not scheduler.schedule(state).placements[0].priority.promoted  # no stored age


def test_promoted_fifo_ignores_new_high_priority_arrivals(roots, worker, snapshot):
    dag, plan = roots
    a, b, c, d = dag.sources()
    state = snapshot(plan, workers=(worker(plan),), ready=(ReadyTask(d, 3, 9), ReadyTask(b, 1, 8),
                                                        ReadyTask(c, 2, 0), ReadyTask(a, 0, 8)))
    assert schedule(plan, state).placements[0].task_id == a
