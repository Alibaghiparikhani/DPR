import protocol as p

from coordinator import EventDisposition, RunStatus, TaskStatus
from coordinator.tests.helpers import accept, connect, dispatch_for, start, succeed


def test_cancel_before_scheduling(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    connect(coordinator, plan)
    coordinator.submit(plan, run_id="r")
    assert coordinator.cancel_run("r") == ()
    run = coordinator.inspect_run("r")
    assert run.status == RunStatus.CANCELLED
    assert all(status == TaskStatus.CANCELLED for _, status in run.tasks)
    coordinator.validate_state()


def test_cancel_running_task_and_acknowledge(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    w = connect(coordinator, plan)
    coordinator.submit(plan, run_id="r")
    coordinator.schedule("r"); d = dispatch_for(w)
    accept(w, d); start(w, d)
    messages = coordinator.cancel_run("r", reason="stop")
    assert len(messages) == 1
    cancel, = w.drain()
    assert cancel == messages[0]
    response = p.TaskCancellationResult(
        "W1", d.attempt, p.CancellationOutcome.CANCELLED, "",
        message_id="cancelled", correlation_id=cancel.message_id,
    )
    assert w.send(response) == EventDisposition.APPLIED
    assert coordinator.inspect_run("r").status == RunStatus.CANCELLED
    assert coordinator.inspect_worker("W1").state.free_slots == 2
    coordinator.validate_state()


def test_success_racing_with_cancellation_commits_effect_but_run_cancels(coordinator, build_plan):
    _, plan = build_plan("a=1\nb=a+1\n")
    w = connect(coordinator, plan)
    coordinator.submit(plan, run_id="r")
    coordinator.schedule("r"); d = dispatch_for(w)
    accept(w, d); start(w, d)
    coordinator.cancel_run("r"); w.drain()  # cancellation command
    _, disposition = succeed(w, plan, d)
    assert disposition == EventDisposition.APPLIED
    run = coordinator.inspect_run("r")
    assert run.status == RunStatus.CANCELLED
    assert dict(run.tasks)[d.attempt.task_id] == TaskStatus.COMMITTED
    # dependent work was unlocked in DAG bookkeeping but never made schedulable.
    assert not any(status == TaskStatus.READY for _, status in run.tasks)
    coordinator.validate_state()


def test_duplicate_cancel_does_not_emit_second_command(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    w = connect(coordinator, plan)
    coordinator.submit(plan, run_id="r")
    coordinator.schedule("r"); dispatch_for(w)
    first = coordinator.cancel_run("r")
    assert len(first) == 1
    w.drain()
    second = coordinator.cancel_run("r")
    assert second == ()
    assert w.drain() == ()


def test_duplicate_cancellation_ack_is_idempotent(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    w = connect(coordinator, plan)
    coordinator.submit(plan, run_id="r")
    coordinator.schedule("r"); d = dispatch_for(w)
    accept(w, d); start(w, d)
    coordinator.cancel_run("r"); cancel = next(m for m in w.drain() if isinstance(m, p.CancelTask))
    response = p.TaskCancellationResult(
        "W1", d.attempt, p.CancellationOutcome.CANCELLED, "",
        message_id="cancelled", correlation_id=cancel.message_id,
    )
    assert w.send(response) == EventDisposition.APPLIED
    assert w.send(response) == EventDisposition.DUPLICATE
    assert coordinator.inspect_worker("W1").state.free_slots == 2


def test_too_late_cancellation_allows_late_success(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    w = connect(coordinator, plan)
    coordinator.submit(plan, run_id="r")
    coordinator.schedule("r"); d = dispatch_for(w)
    accept(w,d); start(w,d)
    coordinator.cancel_run("r"); cancel = next(m for m in w.drain() if isinstance(m,p.CancelTask))
    too_late = p.TaskCancellationResult(
        "W1", d.attempt, p.CancellationOutcome.TOO_LATE, "already running",
        message_id="too-late", correlation_id=cancel.message_id,
    )
    assert w.send(too_late) == EventDisposition.APPLIED
    _, result = succeed(w, plan, d)
    assert result == EventDisposition.APPLIED
    assert coordinator.inspect_run("r").status == RunStatus.CANCELLED
    assert coordinator.get_task("r", d.attempt.task_id).status == TaskStatus.COMMITTED


def test_cancel_while_waiting_for_input_transfer_never_sends_cancel_task(coordinator, build_plan):
    from coordinator.tests.helpers import worker_state
    _, plan = build_plan("a=1\nb=a+1\n")
    w1 = connect(coordinator, plan, "W1", slots=1, port=9001)
    w2 = connect(coordinator, plan, "W2", slots=1, port=9002)
    coordinator.submit(plan, run_id="r")
    coordinator.schedule("r")
    root = next(m for m in w1.drain() if isinstance(m, p.TaskDispatch))
    accept(w1, root); start(w1, root); succeed(w1, plan, root)
    # W1 remains an online data source but cannot take the dependent task.
    w1.send(p.Heartbeat(
        worker_state(plan, "W1", slots=1, accepting=False), 1, message_id="hb"
    )); w1.drain()
    coordinator.schedule("r")
    prepares = [m for m in w2.drain() if isinstance(m, p.PrepareReceive)]
    assert len(prepares) == 1
    current = coordinator.inspect_run("r").current_attempts[0]
    assert coordinator.get_task("r", current.task_id).status == TaskStatus.WAITING_TRANSFER

    assert coordinator.cancel_run("r") == ()
    assert not [m for m in w2.drain() if isinstance(m, p.CancelTask)]
    assert coordinator.inspect_run("r").status == RunStatus.CANCELLED
    transfer = prepares[0].transfer
    assert coordinator.get_transfer(transfer.transfer_id, transfer.transfer_attempt_id).status.value == "failed"
    coordinator.validate_state()
