import pytest
import protocol as p

from coordinator import EventDisposition, RunStatus, StaleWorkerSession, TaskStatus
from coordinator.tests.helpers import accept, connect, dispatch_for, fail, start, succeed
from execution import FailureInfo, FailureKind, TaskSuccess


def test_failure_retry_uses_new_attempt_and_keeps_history(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    w = connect(coordinator, plan)
    coordinator.submit(plan, run_id="run-1")
    coordinator.schedule("run-1")
    a1 = dispatch_for(w)
    accept(w, a1); start(w, a1)
    fail(w, a1, FailureInfo(FailureKind.EXECUTION_ERROR, "transient"))
    assert coordinator.get_task("run-1", a1.attempt.task_id).status == TaskStatus.READY
    coordinator.schedule("run-1")
    a2 = dispatch_for(w)
    assert a2.attempt.attempt_id != a1.attempt.attempt_id
    task = coordinator.get_task("run-1", a1.attempt.task_id)
    assert task.attempt_ids == [a1.attempt.attempt_id, a2.attempt.attempt_id]


def test_python_exception_is_terminal_not_retried(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    w = connect(coordinator, plan)
    coordinator.submit(plan, run_id="run-1")
    coordinator.schedule("run-1")
    dispatch = dispatch_for(w)
    accept(w, dispatch); start(w, dispatch)
    failure = FailureInfo(FailureKind.PYTHON_EXCEPTION, "bad", "ValueError")
    fail(w, dispatch, failure)
    assert coordinator.inspect_run("run-1").status == RunStatus.FAILED
    coordinator.validate_state()


def test_worker_loss_retries_on_other_worker_and_old_session_cannot_commit(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    w1 = connect(coordinator, plan, "W1", slots=1, port=9001)
    w2 = connect(coordinator, plan, "W2", slots=1, port=9002)
    coordinator.submit(plan, run_id="run-1")
    coordinator.schedule("run-1")
    d1 = [m for m in w1.drain() if isinstance(m, p.TaskDispatch)]
    if not d1:
        # deterministic scheduler could pick W2 if IDs/ranking ever change; normalize roles.
        w1, w2 = w2, w1
        d1 = [m for m in w1.drain() if isinstance(m, p.TaskDispatch)]
    a1 = d1[0]
    accept(w1, a1); start(w1, a1)
    w1.send(p.WorkerGoodbye(w1.handle.worker_id, "lost", message_id="bye"))
    coordinator.schedule("run-1")
    a2 = dispatch_for(w2)
    late = p.TaskSucceeded(
        worker_id=w1.handle.worker_id,
        result=TaskSuccess(a1.attempt, plan.task_index[a1.attempt.task_id].reported_output_ids),
        message_id="late", correlation_id=a1.message_id,
    )
    with pytest.raises(StaleWorkerSession):
        w1.send(late)
    accept(w2, a2); start(w2, a2); succeed(w2, plan, a2)
    run = coordinator.inspect_run("run-1")
    assert run.status == RunStatus.SUCCEEDED
    assert coordinator.get_task("run-1", a1.attempt.task_id).committed_attempt_id == a2.attempt.attempt_id
    coordinator.validate_state()


def test_failure_then_late_success_same_session_is_stale(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    w = connect(coordinator, plan)
    coordinator.submit(plan, run_id="run-1")
    coordinator.schedule("run-1")
    a1 = dispatch_for(w)
    accept(w, a1); start(w, a1)
    fail(w, a1, FailureInfo(FailureKind.EXECUTION_ERROR, "retry"))
    late = p.TaskSucceeded(
        worker_id="W1", result=TaskSuccess(a1.attempt, plan.task_index[a1.attempt.task_id].reported_output_ids),
        message_id="late", correlation_id=a1.message_id,
    )
    assert w.send(late) == EventDisposition.STALE
    assert coordinator.get_task("run-1", a1.attempt.task_id).status == TaskStatus.READY


def test_duplicate_success_is_idempotent(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    w = connect(coordinator, plan)
    coordinator.submit(plan, run_id="run-1")
    coordinator.schedule("run-1")
    d = dispatch_for(w)
    accept(w, d); start(w, d)
    msg, assert_applied = succeed(w, plan, d)
    assert assert_applied == EventDisposition.APPLIED
    assert w.send(msg) == EventDisposition.DUPLICATE
    assert coordinator.inspect_run("run-1").status == RunStatus.SUCCEEDED
    coordinator.validate_state()


def test_retry_exhaustion_fails_run(build_plan):
    from coordinator import Coordinator, RetryPolicy
    _, plan = build_plan("a=1\n")
    c = Coordinator(retry_policy=RetryPolicy(max_attempts_per_task=2))
    w = connect(c, plan)
    c.submit(plan, run_id="r")
    for i in range(2):
        c.schedule("r")
        d = dispatch_for(w)
        accept(w, d); start(w, d)
        fail(w, d, FailureInfo(FailureKind.EXECUTION_ERROR, f"failure-{i}"))
    assert c.inspect_run("r").status == RunStatus.FAILED
    c.validate_state()


@pytest.mark.parametrize("code,retryable", [
    (p.RejectionCode.BUSY, True),
    (p.RejectionCode.PROGRAM_UNAVAILABLE, True),
    (p.RejectionCode.INPUT_UNAVAILABLE, True),
    (p.RejectionCode.INVALID_REQUEST, False),
])
def test_task_rejection_policy(coordinator, build_plan, code, retryable):
    _, plan = build_plan("a=1\n")
    w = connect(coordinator, plan)
    coordinator.submit(plan, run_id="r")
    coordinator.schedule("r")
    d = dispatch_for(w)
    result = w.send(p.TaskRejected(
        "W1", d.attempt, code, "nope",
        message_id="reject", correlation_id=d.message_id,
    ))
    assert result == EventDisposition.APPLIED
    expected = TaskStatus.READY if retryable else TaskStatus.FAILED
    assert coordinator.get_task("r", d.attempt.task_id).status == expected
