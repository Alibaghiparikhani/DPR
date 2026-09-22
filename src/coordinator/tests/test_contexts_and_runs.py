import pytest
import protocol as p

from coordinator import EventDisposition, InvalidRunTransition, RunStatus, TaskStatus
from coordinator.tests.helpers import accept, connect, dispatch_for, start, succeed
from scheduler import TaskAffinity, WorkerContext


def test_empty_plan_is_immediately_succeeded(coordinator, build_plan):
    _, plan = build_plan("")
    coordinator.submit(plan, run_id="empty")
    snapshot = coordinator.inspect_run("empty")
    assert snapshot.status == RunStatus.SUCCEEDED
    assert snapshot.tasks == ()
    coordinator.validate_state()


def test_duplicate_run_id_rejected(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    coordinator.submit(plan, run_id="r")
    with pytest.raises(InvalidRunTransition):
        coordinator.submit(plan, run_id="r")


def test_context_preparation_enables_shared_context_task(coordinator, build_plan):
    _, plan = build_plan("a=[1,2]\na.append(3)\n")
    root, shared = plan.tasks
    assert shared.mode.value == "shared_context"
    w = connect(coordinator, plan, "W1")
    affinity = TaskAffinity(shared.task_id, required_worker="W1", context_id="ctx-1")
    coordinator.submit(plan, run_id="r", affinities=(affinity,))

    coordinator.schedule("r")
    d = dispatch_for(w)
    assert d.attempt.task_id == root.task_id
    accept(w, d); start(w, d); succeed(w, plan, d)

    # READY but no native/shared context yet.
    result = coordinator.schedule("r")
    assert result.dispatched == ()
    assert coordinator.get_task("r", shared.task_id).status == TaskStatus.READY

    command = coordinator.request_context_preparation("r", "W1", "ctx-1", (shared.task_id,))
    assert command in w.drain()
    context = WorkerContext("ctx-1", "W1", frozenset({shared.task_id}), 1)
    assert w.send(p.ContextPrepared(
        plan.id, "r", context,
        message_id="context-ready", correlation_id=command.message_id,
    )) == EventDisposition.APPLIED
    result = coordinator.schedule("r")
    assert len(result.dispatched) == 1
    dispatch = dispatch_for(w)
    assert dispatch.context_id == "ctx-1"
    assert dispatch.mode == shared.mode


def test_context_unavailable_removes_schedulability(coordinator, build_plan):
    _, plan = build_plan("a=[1,2]\na.append(3)\n")
    root, shared = plan.tasks
    w = connect(coordinator, plan, "W1")
    affinity = TaskAffinity(shared.task_id, required_worker="W1", context_id="ctx")
    context = WorkerContext("ctx", "W1", frozenset({shared.task_id}), 1)
    coordinator.submit(plan, run_id="r", affinities=(affinity,), contexts=(context,))
    coordinator.schedule("r")
    d = dispatch_for(w); accept(w,d); start(w,d); succeed(w,plan,d)
    assert w.send(p.ContextUnavailable(
        "W1", plan.id, "r", "ctx", "evicted", message_id="ctx-gone"
    )) == EventDisposition.APPLIED
    assert coordinator.inspect_run("r").status == RunStatus.FAILED
    with pytest.raises(InvalidRunTransition):
        coordinator.schedule("r")


def test_ready_wait_rounds_persist_across_unplaced_rounds(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    connect(coordinator, plan, "W1", prepared=False)
    coordinator.submit(plan, run_id="r")
    coordinator.schedule("r")
    snap = coordinator.build_snapshot("r")
    assert snap.ready[0].wait_rounds == 1
    coordinator.schedule("r")
    snap = coordinator.build_snapshot("r")
    assert snap.ready[0].wait_rounds == 2
