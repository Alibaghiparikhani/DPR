import pytest

import protocol as p
from coordinator import (
    Coordinator,
    EventDisposition,
    InvalidRunTransition,
    OperationLimits,
    RetryPolicy,
    RunStatus,
    TaskStatus,
    TransferStatus,
)
from coordinator.tests.helpers import accept, connect, dispatch_for, release_terminal_objects, start, succeed, worker_state


def _heartbeat_accepting(fake, plan, accepting, seq=1):
    state = worker_state(plan, fake.handle.worker_id, accepting=accepting)
    assert fake.send(
        p.Heartbeat(state, seq, message_id=f"hb-{fake.handle.worker_id}-{seq}")
    ) == EventDisposition.APPLIED
    fake.drain()


def _waiting_remote_consumer(coordinator, build_plan, source_text="a=1\nb=a+1\n"):
    _, plan = build_plan(source_text)
    w1 = connect(coordinator, plan, "W1", slots=2, port=9001)
    w2 = connect(coordinator, plan, "W2", slots=2, port=9002)
    coordinator.submit(plan, run_id="r")
    coordinator.schedule("r")
    root = dispatch_for(w1)
    accept(w1, root); start(w1, root); succeed(w1, plan, root)
    _heartbeat_accepting(w1, plan, False)
    coordinator.schedule("r")
    prepare = next(m for m in w2.drain() if isinstance(m, p.PrepareReceive))
    attempt = coordinator.inspect_run("r").current_attempts[0]
    assert coordinator.get_task("r", attempt.task_id).status == TaskStatus.WAITING_TRANSFER
    assert w2.send(p.ReceiveReady(
        "W2", prepare.transfer, message_id="ready", correlation_id=prepare.message_id,
    )) == EventDisposition.APPLIED
    request = next(m for m in w1.drain() if isinstance(m, p.TransferRequest))
    return plan, w1, w2, attempt, prepare, request


def test_failed_transfer_accepts_late_destination_completion_as_cleanup(build_plan):
    c = Coordinator(retry_policy=RetryPolicy(max_attempts_per_task=3))
    plan, w1, w2, attempt, prepare, request = _waiting_remote_consumer(c, build_plan)
    c.cancel_run("r")
    assert c.inspect_run("r").status == RunStatus.CANCELLED
    record = c.get_transfer(request.transfer.transfer_id, request.transfer.transfer_attempt_id)
    assert record.status == TransferStatus.FAILED
    assert not record.cleanup_confirmed

    # The run is logically cancelled, so late physical completion is cleanup evidence only.
    assert w2.send(p.TransferCompleted(
        "W2", request.transfer, prepare.size_bytes,
        message_id="late-complete", correlation_id=prepare.message_id,
    )) == EventDisposition.APPLIED
    record = c.get_transfer(request.transfer.transfer_id, request.transfer.transfer_attempt_id)
    assert record.status == TransferStatus.FAILED
    # F5/F6: logical late completion proves bytes may exist; cleanup is only
    # confirmed after the destination acknowledges the staged physical release.
    assert not record.cleanup_confirmed
    assert release_terminal_objects(c, w1, w2) >= 1
    assert c.get_transfer(request.transfer.transfer_id, request.transfer.transfer_attempt_id).cleanup_confirmed
    release_terminal_objects(c, w1, w2)
    c.discard_terminal_run("r")


def test_one_participant_failure_does_not_certify_other_participant_cleanup(build_plan):
    c = Coordinator(retry_policy=RetryPolicy(max_attempts_per_task=3))
    _, w1, w2, _, prepare, request = _waiting_remote_consumer(c, build_plan)

    assert w1.send(p.TransferFailed(
        "W1", request.transfer, p.TransferFailureCode.IO_ERROR, "source io",
        message_id="source-failed", correlation_id=request.message_id,
    )) == EventDisposition.APPLIED
    c.cancel_run("r")
    record = c.get_transfer(request.transfer.transfer_id, request.transfer.transfer_attempt_id)
    assert record.status == TransferStatus.FAILED
    assert not record.cleanup_confirmed
    release_terminal_objects(c, w1, w2)
    with pytest.raises(InvalidRunTransition, match="unresolved transfer"):
        c.discard_terminal_run("r")

    # The receiver independently confirms its own terminal state.
    assert w2.send(p.TransferFailed(
        "W2", request.transfer, p.TransferFailureCode.CANCELLED, "receiver stopped",
        message_id="destination-failed", correlation_id=prepare.message_id,
    )) == EventDisposition.APPLIED
    assert c.get_transfer(request.transfer.transfer_id, request.transfer.transfer_attempt_id).cleanup_confirmed
    c.discard_terminal_run("r")


def test_lost_both_transfer_sessions_resolves_failed_cleanup(build_plan):
    c = Coordinator(retry_policy=RetryPolicy(max_attempts_per_task=3))
    _, w1, w2, _, _, request = _waiting_remote_consumer(c, build_plan)
    c.cancel_run("r")
    assert not c.get_transfer(request.transfer.transfer_id, request.transfer.transfer_attempt_id).cleanup_confirmed
    w1.send(p.WorkerGoodbye("W1", "gone", message_id="bye-1"))
    w2.send(p.WorkerGoodbye("W2", "gone", message_id="bye-2"))
    assert c.get_transfer(request.transfer.transfer_id, request.transfer.transfer_attempt_id).cleanup_confirmed
    c.discard_terminal_run("r")


def test_materialized_alias_inputs_use_two_physical_transfers_under_two_record_limit(build_plan):
    _, plan = build_plan("a=1\nb=a\nc=a+b\n")
    producer, binding, consumer = plan.tasks
    limits = OperationLimits(max_transfer_records_global=2, max_transfer_records_per_run=2)
    c = Coordinator(operation_limits=limits)
    w1 = connect(c, plan, "W1", slots=2, port=9001)
    w2 = connect(c, plan, "W2", slots=1, port=9002)
    from scheduler import TaskAffinity, WorkerContext
    c.submit(
        plan, run_id="r",
        affinities=(
            TaskAffinity(binding.task_id, required_worker="W1", context_id="alias-ctx"),
            TaskAffinity(consumer.task_id, required_worker="W2"),
        ),
        contexts=(WorkerContext("alias-ctx", "W1", frozenset({binding.task_id}), 1),),
    )
    c.schedule("r")
    root = dispatch_for(w1); accept(w1, root); start(w1, root); succeed(w1, plan, root)
    c.schedule("r")
    alias_dispatch = dispatch_for(w1); assert alias_dispatch.attempt.task_id == binding.task_id
    accept(w1, alias_dispatch); start(w1, alias_dispatch); succeed(w1, plan, alias_dispatch)

    result = c.schedule("r")
    assert len(result.dispatched) == 1
    prepares = [m for m in w2.drain() if isinstance(m, p.PrepareReceive)]
    assert len(prepares) == 2
    current = c.inspect_run("r").current_attempts[0]
    assert len(c.get_attempt("r", current.attempt_id).pending_transfers) == 2
    c.validate_state()


def test_cancel_clock_failure_is_atomic(build_plan):
    _, plan = build_plan("a=1")
    fail_clock = False

    def clock():
        if fail_clock:
            raise RuntimeError("clock failed")
        return 0.0

    c = Coordinator(clock=clock)
    w = connect(c, plan, "W1")
    c.submit(plan, run_id="r")
    c.schedule("r")
    dispatch = dispatch_for(w)
    accept(w, dispatch); start(w, dispatch)
    before = c.inspect_run("r")
    fail_clock = True
    with pytest.raises(RuntimeError, match="clock failed"):
        c.cancel_run("r")
    assert c.inspect_run("r") == before
    assert w.drain() == ()


def test_final_transfer_dispatch_id_failure_is_atomic_and_retryable(build_plan):
    c = Coordinator(retry_policy=RetryPolicy(max_attempts_per_task=3))
    _, w1, w2, attempt, prepare, request = _waiting_remote_consumer(c, build_plan)
    old_id = c._id
    c._id = lambda kind: (_ for _ in ()).throw(RuntimeError("id failed"))
    completion = p.TransferCompleted(
        "W2", request.transfer, prepare.size_bytes,
        message_id="done", correlation_id=prepare.message_id,
    )
    try:
        with pytest.raises(RuntimeError, match="id failed"):
            w2.send(completion)
    finally:
        c._id = old_id

    record = c.get_transfer(request.transfer.transfer_id, request.transfer.transfer_attempt_id)
    # The failed local continuation must not consume the terminal observation.
    assert record.status != TransferStatus.COMPLETED
    waiting = c.get_attempt("r", attempt.attempt_id)
    assert waiting.status.value == "waiting_transfer"
    assert waiting.pending_transfers
    assert waiting.dispatch_message_id is None

    assert w2.send(completion) == EventDisposition.APPLIED
    dispatch = dispatch_for(w2)
    assert dispatch.attempt == attempt
    assert c.get_task("r", attempt.task_id).status == TaskStatus.DISPATCHED
    c.validate_state()


def test_accepting_work_is_revalidated_before_transfer_gated_dispatch(build_plan):
    c = Coordinator(retry_policy=RetryPolicy(max_attempts_per_task=3))
    plan, w1, w2, attempt, prepare, request = _waiting_remote_consumer(c, build_plan)
    state = worker_state(plan, "W2", accepting=False)
    assert w2.send(p.Heartbeat(state, 1, message_id="withdraw")) == EventDisposition.APPLIED
    w2.drain()

    assert w2.send(p.TransferCompleted(
        "W2", request.transfer, prepare.size_bytes,
        message_id="done", correlation_id=prepare.message_id,
    )) == EventDisposition.APPLIED
    assert not [m for m in w2.drain() if isinstance(m, p.TaskDispatch)]
    task = c.get_task("r", attempt.task_id)
    assert task.status == TaskStatus.READY
    c.validate_state()


def test_cancel_before_receive_ready_accepts_late_preparation_cleanup(build_plan):
    _, plan = build_plan("a=1\nb=a+1\n")
    c = Coordinator()
    w1 = connect(c, plan, "W1", slots=1, port=9001)
    w2 = connect(c, plan, "W2", slots=1, port=9002)
    c.submit(plan, run_id="r"); c.schedule("r")
    root = dispatch_for(w1); accept(w1, root); start(w1, root); succeed(w1, plan, root)
    _heartbeat_accepting(w1, plan, False)
    c.schedule("r")
    prepare = next(m for m in w2.drain() if isinstance(m, p.PrepareReceive))
    c.cancel_run("r")
    record = c.get_transfer(prepare.transfer.transfer_id, prepare.transfer.transfer_attempt_id)
    assert record.status == TransferStatus.FAILED and not record.cleanup_confirmed
    assert w2.send(p.ReceivePreparationFailed(
        "W2", prepare.transfer, p.TransferFailureCode.CANCELLED, "cancelled",
        message_id="prep-stop", correlation_id=prepare.message_id,
    )) == EventDisposition.APPLIED
    assert c.get_transfer(prepare.transfer.transfer_id, prepare.transfer.transfer_attempt_id).cleanup_confirmed
    release_terminal_objects(c, w1, w2)
    c.discard_terminal_run("r")


def test_superseded_failed_attempt_still_accepts_late_cleanup(build_plan):
    from coordinator.tests.helpers import seed_location
    from scheduler import DataForm

    _, plan = build_plan("a=1")
    c = Coordinator()
    w1 = connect(c, plan, "W1", port=9001)
    w2 = connect(c, plan, "W2", port=9002)
    c.submit(plan, run_id="r")
    value = next(v for v in plan.values if v.storage == "immutable_value")
    data = p.DataReference(plan.id, "r", value.id, DataForm.IMMUTABLE_VALUE)
    seed_location(c, plan, "r", "W1", data, 10)

    first = p.TransferIdentity(data, "logical", "try-1", "W1", "W2")
    prep1 = c.start_transfer(first, size_bytes=10); w2.drain()
    w2.send(p.ReceiveReady("W2", first, message_id="r1", correlation_id=prep1.message_id))
    req1 = next(m for m in w1.drain() if isinstance(m, p.TransferRequest))
    w1.send(p.TransferFailed(
        "W1", first, p.TransferFailureCode.IO_ERROR, "source failed",
        message_id="sf1", correlation_id=req1.message_id,
    ))
    assert not c.get_transfer("logical", "try-1").cleanup_confirmed

    second = p.TransferIdentity(data, "logical", "try-2", "W1", "W2")
    c.start_transfer(second, size_bytes=10); w2.drain()
    assert w2.send(p.TransferFailed(
        "W2", first, p.TransferFailureCode.CANCELLED, "old receiver stopped",
        message_id="df1", correlation_id=prep1.message_id,
    )) == EventDisposition.APPLIED
    assert c.get_transfer("logical", "try-1").cleanup_confirmed


def test_validator_rejects_waiting_transfer_without_pending_continuation(build_plan):
    c = Coordinator()
    _, _, _, attempt, _, _ = _waiting_remote_consumer(c, build_plan)
    c._runs["r"].attempts[attempt.attempt_id].pending_transfers.clear()
    with pytest.raises(AssertionError, match="no pending transfer"):
        c.validate_state()
