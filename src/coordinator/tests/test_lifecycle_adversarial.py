import pytest
import protocol as p

from coordinator import (
    EventDisposition, InvalidTaskTransition, InvalidWorkerMessage, RunStatus,
    StaleWorkerSession, TaskStatus,
)
from coordinator.tests.helpers import accept, connect, dispatch_for, start, succeed, worker_state
from execution import ExecutionValidationError, FailureInfo, FailureKind, TaskFailure, TaskSuccess


def test_started_before_accepted_rejected_without_mutation(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    w = connect(coordinator, plan)
    coordinator.submit(plan, run_id="r"); coordinator.schedule("r")
    d = dispatch_for(w)
    with pytest.raises(InvalidTaskTransition):
        start(w, d)
    assert coordinator.get_task("r", d.attempt.task_id).status == TaskStatus.DISPATCHED
    coordinator.validate_state()


def test_success_before_started_rejected(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    w = connect(coordinator, plan)
    coordinator.submit(plan, run_id="r"); coordinator.schedule("r")
    d = dispatch_for(w); accept(w, d)
    message = p.TaskSucceeded(
        "W1", TaskSuccess(d.attempt, plan.task_index[d.attempt.task_id].reported_output_ids),
        message_id="early", correlation_id=d.message_id,
    )
    with pytest.raises(InvalidTaskTransition):
        w.send(message)
    assert coordinator.get_task("r", d.attempt.task_id).status == TaskStatus.ACCEPTED


def test_rejection_after_acceptance_rejected(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    w = connect(coordinator, plan)
    coordinator.submit(plan, run_id="r"); coordinator.schedule("r")
    d = dispatch_for(w); accept(w, d)
    with pytest.raises(InvalidTaskTransition):
        w.send(p.TaskRejected(
            "W1", d.attempt, p.RejectionCode.BUSY, "late",
            message_id="late-reject", correlation_id=d.message_id,
        ))


def test_duplicate_accept_after_start_is_harmless(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    w = connect(coordinator, plan)
    coordinator.submit(plan, run_id="r"); coordinator.schedule("r")
    d = dispatch_for(w); accept(w, d); start(w, d)
    assert accept(w, d, suffix="replay") == EventDisposition.DUPLICATE
    assert coordinator.get_task("r", d.attempt.task_id).status == TaskStatus.RUNNING


def test_invalid_success_output_does_not_advance_readiness(coordinator, build_plan):
    _, plan = build_plan("a=1\nb=a+1\n")
    w = connect(coordinator, plan)
    coordinator.submit(plan, run_id="r"); coordinator.schedule("r")
    d = dispatch_for(w); accept(w,d); start(w,d)
    bad = p.TaskSucceeded(
        "W1", TaskSuccess(d.attempt, ("not-an-output",)),
        message_id="bad-result", correlation_id=d.message_id,
    )
    with pytest.raises(ExecutionValidationError):
        w.send(bad)
    run = coordinator.inspect_run("r")
    assert not run.completed_task_ids
    assert coordinator.get_task("r", d.attempt.task_id).status == TaskStatus.RUNNING
    coordinator.validate_state()


def test_wrong_task_event_correlation_rejected(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    w = connect(coordinator, plan)
    coordinator.submit(plan, run_id="r"); coordinator.schedule("r")
    d = dispatch_for(w)
    with pytest.raises(InvalidWorkerMessage):
        w.send(p.TaskAccepted(
            "W1", d.attempt, message_id="x", correlation_id="wrong-command"
        ))


@pytest.mark.parametrize("stage", ["dispatched", "accepted", "running"])
def test_worker_loss_at_active_stages_retries(stage, coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    w1 = connect(coordinator, plan, "W1", slots=1, port=9001)
    w2 = connect(coordinator, plan, "W2", slots=1, port=9002)
    coordinator.submit(plan, run_id="r"); coordinator.schedule("r")
    d = next(m for m in w1.drain() if isinstance(m, p.TaskDispatch))
    if stage in {"accepted", "running"}: accept(w1,d)
    if stage == "running": start(w1,d)
    w1.send(p.WorkerGoodbye("W1", "lost", message_id=f"bye-{stage}"))
    assert coordinator.get_task("r", d.attempt.task_id).status == TaskStatus.READY
    coordinator.schedule("r")
    retry = dispatch_for(w2)
    assert retry.attempt.attempt_id != d.attempt.attempt_id
    coordinator.validate_state()


def test_worker_loss_after_commit_never_retries(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    w = connect(coordinator, plan, "W1")
    coordinator.submit(plan, run_id="r"); coordinator.schedule("r")
    d=dispatch_for(w); accept(w,d); start(w,d); succeed(w,plan,d)
    assert coordinator.inspect_run("r").status == RunStatus.SUCCEEDED
    w.send(p.WorkerGoodbye("W1", "after commit", message_id="bye"))
    task = coordinator.get_task("r", d.attempt.task_id)
    assert task.status == TaskStatus.COMMITTED
    assert len(task.attempt_ids) == 1
    coordinator.validate_state()


def test_success_then_failure_cannot_rollback_commit(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    w=connect(coordinator,plan); coordinator.submit(plan,run_id="r"); coordinator.schedule("r")
    d=dispatch_for(w); accept(w,d); start(w,d)
    _, result=succeed(w,plan,d); assert result == EventDisposition.APPLIED
    failure = p.TaskFailed(
        "W1", TaskFailure(d.attempt, FailureInfo(FailureKind.EXECUTION_ERROR,"late")),
        message_id="late-failure", correlation_id=d.message_id,
    )
    assert w.send(failure) == EventDisposition.DUPLICATE
    assert coordinator.inspect_run("r").status == RunStatus.SUCCEEDED


def test_heartbeat_that_already_counts_running_task_is_not_double_counted(coordinator, build_plan):
    _, plan=build_plan("a=1\n")
    w=connect(coordinator,plan,slots=2); coordinator.submit(plan,run_id="r"); coordinator.schedule("r")
    d=dispatch_for(w); accept(w,d); start(w,d)
    raw=worker_state(plan,"W1",slots=2,running=1)
    w.send(p.Heartbeat(raw,1,message_id="hb")); w.drain()
    effective=coordinator.inspect_worker("W1").state
    assert effective.running_slots == 1
    assert effective.free_slots == 1
    coordinator.validate_state()


def test_one_slot_capacity_shared_across_runs(coordinator, build_plan):
    _, plan=build_plan("a=1\n")
    w=connect(coordinator,plan,slots=1)
    coordinator.submit(plan,run_id="A"); coordinator.submit(plan,run_id="B")
    assert len(coordinator.schedule("A").dispatched)==1
    da=dispatch_for(w)
    assert coordinator.schedule("B").dispatched == ()
    accept(w,da); start(w,da); succeed(w,plan,da)
    assert len(coordinator.schedule("B").dispatched)==1
