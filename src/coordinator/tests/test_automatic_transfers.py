import protocol as p

from coordinator import CoordinatorFailureCode, EventDisposition, TaskStatus, TransferStatus
from coordinator.tests.helpers import accept, connect, dispatch_for, start, succeed, worker_state
from scheduler import DataForm


def _heartbeat_accepting(fake, plan, accepting, seq):
    state = worker_state(plan, fake.handle.worker_id, accepting=accepting)
    msg = p.Heartbeat(state, seq, message_id=f"hb-{fake.handle.worker_id}-{seq}")
    fake.send(msg)
    fake.drain()  # ack (+ any membership update already queued)


def _finish_transfer(source, destination, prepare):
    transfer = prepare.transfer
    assert destination.send(p.ReceiveReady(
        destination.handle.worker_id, transfer,
        message_id=f"ready-{transfer.transfer_attempt_id}",
        correlation_id=prepare.message_id,
    )) == EventDisposition.APPLIED
    request = next(m for m in source.drain() if isinstance(m, p.TransferRequest)
                   and m.transfer == transfer)
    assert source.send(p.TransferAccepted(
        source.handle.worker_id, transfer,
        message_id=f"accepted-{transfer.transfer_attempt_id}",
        correlation_id=request.message_id,
    )) == EventDisposition.APPLIED
    assert source.send(p.TransferStarted(
        source.handle.worker_id, transfer,
        message_id=f"started-{transfer.transfer_attempt_id}",
        correlation_id=request.message_id,
    )) == EventDisposition.APPLIED
    assert destination.send(p.TransferCompleted(
        destination.handle.worker_id, transfer, prepare.size_bytes,
        message_id=f"complete-{transfer.transfer_attempt_id}",
        correlation_id=prepare.message_id,
    )) == EventDisposition.APPLIED


def test_remote_input_waits_for_transfer_before_dispatch(coordinator, build_plan):
    _, plan = build_plan("a=1\nb=a+1\n")
    w1 = connect(coordinator, plan, "W1", slots=1, port=9001)
    w2 = connect(coordinator, plan, "W2", slots=1, port=9002)
    coordinator.submit(plan, run_id="r")

    coordinator.schedule("r")
    root = dispatch_for(w1)
    accept(w1, root); start(w1, root); succeed(w1, plan, root)

    # Force the dependent computation to W2 while W1 remains an online source.
    _heartbeat_accepting(w1, plan, False, 1)
    result = coordinator.schedule("r")
    assert len(result.dispatched) == 1  # attempt allocated/reserved, not wire-dispatched yet
    task_id = result.dispatched[0].task_id
    assert coordinator.get_task("r", task_id).status == TaskStatus.WAITING_TRANSFER
    assert coordinator.inspect_worker("W2").state.free_slots == 0

    outbound = w2.drain()
    prepares = [m for m in outbound if isinstance(m, p.PrepareReceive)]
    assert len(prepares) == 1
    assert not [m for m in outbound if isinstance(m, p.TaskDispatch)]

    _finish_transfer(w1, w2, prepares[0])
    dispatch = dispatch_for(w2)
    assert dispatch.attempt.task_id == task_id
    assert coordinator.get_task("r", task_id).status == TaskStatus.DISPATCHED
    coordinator.validate_state()


def test_multiple_remote_inputs_all_complete_before_dispatch(coordinator, build_plan):
    _, plan = build_plan("a=1\nb=2\nc=a+b\n")
    w1 = connect(coordinator, plan, "W1", slots=2, port=9001)
    coordinator.submit(plan, run_id="r")
    coordinator.schedule("r")
    roots = [m for m in w1.drain() if isinstance(m, p.TaskDispatch)]
    assert len(roots) == 2
    for d in roots:
        accept(w1, d); start(w1, d); succeed(w1, plan, d)

    w2 = connect(coordinator, plan, "W2", slots=1, port=9002)
    # membership update sent to W1 should not matter
    _heartbeat_accepting(w1, plan, False, 1)
    coordinator.schedule("r")
    prepares = [m for m in w2.drain() if isinstance(m, p.PrepareReceive)]
    assert len(prepares) == 2
    attempt = coordinator.inspect_run("r").current_attempts[0]
    assert coordinator.get_task("r", attempt.task_id).status == TaskStatus.WAITING_TRANSFER

    _finish_transfer(w1, w2, prepares[0])
    assert not [m for m in w2.drain() if isinstance(m, p.TaskDispatch)]
    _finish_transfer(w1, w2, prepares[1])
    d = dispatch_for(w2)
    assert d.attempt == attempt
    coordinator.validate_state()


def test_transfer_preparation_failure_retries_task_and_releases_capacity(coordinator, build_plan):
    _, plan = build_plan("a=1\nb=a+1\n")
    w1 = connect(coordinator, plan, "W1", slots=1, port=9001)
    w2 = connect(coordinator, plan, "W2", slots=1, port=9002)
    coordinator.submit(plan, run_id="r")
    coordinator.schedule("r")
    root = dispatch_for(w1)
    accept(w1, root); start(w1, root); succeed(w1, plan, root)
    _heartbeat_accepting(w1, plan, False, 1)

    coordinator.schedule("r")
    first_attempt = coordinator.inspect_run("r").current_attempts[0]
    prepare = next(m for m in w2.drain() if isinstance(m, p.PrepareReceive))
    assert coordinator.inspect_worker("W2").state.free_slots == 0
    assert w2.send(p.ReceivePreparationFailed(
        "W2", prepare.transfer, p.TransferFailureCode.IO_ERROR, "disk full",
        message_id="prep-failed", correlation_id=prepare.message_id,
    )) == EventDisposition.APPLIED
    task = coordinator.get_task("r", first_attempt.task_id)
    assert task.status == TaskStatus.READY
    assert task.current_attempt_id is None
    assert coordinator.inspect_worker("W2").state.free_slots == 1
    assert coordinator.get_attempt("r", first_attempt.attempt_id).pending_transfers == set()

    coordinator.schedule("r")
    second_attempt = coordinator.inspect_run("r").current_attempts[0]
    assert second_attempt.attempt_id != first_attempt.attempt_id
    coordinator.validate_state()


def test_source_loss_while_waiting_transfer_does_not_dispatch(coordinator, build_plan):
    _, plan = build_plan("a=1\nb=a+1\n")
    w1 = connect(coordinator, plan, "W1", slots=1, port=9001)
    w2 = connect(coordinator, plan, "W2", slots=1, port=9002)
    coordinator.submit(plan, run_id="r")
    coordinator.schedule("r")
    root = dispatch_for(w1)
    accept(w1, root); start(w1, root); succeed(w1, plan, root)
    _heartbeat_accepting(w1, plan, False, 1)
    coordinator.schedule("r")
    attempt = coordinator.inspect_run("r").current_attempts[0]
    prepare = next(m for m in w2.drain() if isinstance(m, p.PrepareReceive))

    w1.send(p.WorkerGoodbye("W1", "lost source", message_id="bye"))
    task = coordinator.get_task("r", attempt.task_id)
    assert task.status == TaskStatus.FAILED
    run = coordinator.inspect_run("r")
    assert run.status.value == "failed"
    assert run.failure.code == CoordinatorFailureCode.DATA_LOST
    assert not [m for m in w2.drain() if isinstance(m, p.TaskDispatch)]
    # Destination's late readiness for the obsolete transfer is harmless.
    assert w2.send(p.ReceiveReady(
        "W2", prepare.transfer, message_id="late-ready", correlation_id=prepare.message_id
    )) == EventDisposition.STALE
    coordinator.validate_state()


def test_shared_reference_success_does_not_forge_snapshot_location(coordinator, build_plan):
    _, plan = build_plan("a=[1,2]\nb=a[0]\n")
    w = connect(coordinator, plan, "W1", slots=1)
    coordinator.submit(plan, run_id="r")
    coordinator.schedule("r")
    producer = dispatch_for(w)
    accept(w, producer); start(w, producer); succeed(w, plan, producer, publish_available=False)
    assert coordinator.data_locations("r") == ()

    # The dependent is READY but cannot run until an explicit certified snapshot exists.
    result = coordinator.schedule("r")
    assert result.dispatched == ()
    shared_value = plan.task_index[producer.attempt.task_id].outputs[0]
    data = p.DataReference(plan.id, "r", shared_value.id, DataForm.OBJECT_SNAPSHOT, None)
    w.send(p.ObjectAvailable("W1", data, 64, message_id="snapshot-ready"))
    result = coordinator.schedule("r")
    assert len(result.dispatched) == 1
    assert isinstance(dispatch_for(w), p.TaskDispatch)
