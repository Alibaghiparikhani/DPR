import pytest
import protocol as p

from coordinator import (
    EventDisposition, PlacementRejected, RunStatus, TaskStatus,
)
from coordinator.tests.helpers import accept, connect, dispatch_for, start, succeed, worker_state


def test_diamond_advances_only_after_commit(coordinator, diamond):
    dag, plan = diamond
    w = connect(coordinator, plan, slots=2)
    coordinator.submit(plan, run_id="run-1")
    initial = coordinator.inspect_run("run-1")
    roots = dag.initial_ready_tasks()
    assert len(roots) == 1
    assert dict(initial.tasks)[roots[0]] == TaskStatus.READY

    result = coordinator.schedule("run-1")
    assert len(result.dispatched) == 1
    a = dispatch_for(w)
    accept(w, a); start(w, a)
    _, disposition = succeed(w, plan, a)
    assert disposition == EventDisposition.APPLIED

    after_a = coordinator.inspect_run("run-1")
    ready = [t for t, status in after_a.tasks if status == TaskStatus.READY]
    assert len(ready) == 2
    assert roots[0] in after_a.completed_task_ids

    result = coordinator.schedule("run-1")
    assert len(result.dispatched) == 2
    msgs = [m for m in w.drain() if isinstance(m, p.TaskDispatch)]
    assert len(msgs) == 2
    for msg in msgs:
        accept(w, msg); start(w, msg); succeed(w, plan, msg)

    ready = [t for t, status in coordinator.inspect_run("run-1").tasks if status == TaskStatus.READY]
    assert len(ready) == 1
    coordinator.schedule("run-1")
    d = dispatch_for(w)
    accept(w, d); start(w, d); succeed(w, plan, d)
    assert coordinator.inspect_run("run-1").status == RunStatus.SUCCEEDED
    coordinator.validate_state()


def test_scheduler_real_integration_uses_two_workers(coordinator, build_plan):
    _, plan = build_plan("a=1\nb=2\n")
    w1 = connect(coordinator, plan, "W1", slots=1, port=9001)
    w2 = connect(coordinator, plan, "W2", slots=1, port=9002)
    coordinator.submit(plan, run_id="run-1")
    result = coordinator.schedule("run-1")
    assert len(result.dispatched) == 2
    assert len([m for m in w1.drain() if isinstance(m, p.TaskDispatch)]) == 1
    assert len([m for m in w2.drain() if isinstance(m, p.TaskDispatch)]) == 1
    coordinator.validate_state()


def test_no_prepared_program_leaves_task_unplaced(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    w = connect(coordinator, plan, prepared=False)
    coordinator.submit(plan, run_id="run-1")
    result = coordinator.schedule("run-1")
    assert not result.dispatched
    assert len(result.unplaced_task_ids) == 1
    assert not w.drain()


def test_stale_scheduler_decision_rejected(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    w = connect(coordinator, plan)
    coordinator.submit(plan, run_id="run-1")
    decision = coordinator.propose("run-1")
    w.send(p.Heartbeat(worker_state(plan, "W1"), 1, message_id="hb"))
    with pytest.raises(PlacementRejected):
        coordinator.accept_decision("run-1", decision)
    assert coordinator.inspect_run("run-1").current_attempts == ()


def test_capacity_reserved_and_released_exactly_once(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    w = connect(coordinator, plan, slots=1)
    coordinator.submit(plan, run_id="run-1")
    coordinator.schedule("run-1")
    dispatch = dispatch_for(w)
    assert coordinator.inspect_worker("W1").state.free_slots == 0
    accept(w, dispatch); start(w, dispatch)
    assert coordinator.inspect_worker("W1").state.running_slots == 1
    success, _ = succeed(w, plan, dispatch)
    assert coordinator.inspect_worker("W1").state.free_slots == 1
    assert w.send(success) == EventDisposition.DUPLICATE
    assert coordinator.inspect_worker("W1").state.free_slots == 1
    coordinator.validate_state()


def test_multi_placement_staging_failure_is_atomic(build_plan):
    from coordinator import Coordinator, CoordinatorError
    from coordinator.tests.helpers import connect
    import protocol as p

    _, plan = build_plan("a=1\nb=2\n")
    counters = {}

    def ids(kind):
        if kind == "attempt":
            return "duplicate-attempt"
        counters[kind] = counters.get(kind, 0) + 1
        return f"{kind}-{counters[kind]}"

    c = Coordinator(id_source=ids)
    w = connect(c, plan, "W1", slots=2)
    c.submit(plan, run_id="r")
    decision = c.propose("r")
    assert len(decision.placements) == 2

    with pytest.raises(CoordinatorError, match="attempt identity reused"):
        c.accept_decision("r", decision)

    assert all(c.get_task("r", task.task_id).current_attempt_id is None for task in plan.tasks)
    assert not [message for message in w.drain() if isinstance(message, p.TaskDispatch)]
    c.validate_state()
