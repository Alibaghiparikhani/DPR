import pytest
import protocol as p

from coordinator import (
    EventDisposition, StaleWorkerSession, TransferStatus,
)
from coordinator.tests.helpers import connect, seed_location
from scheduler import DataForm


def _data_for(plan, run_id="r"):
    # Choose an immutable value from this simple plan.
    value = next(v for v in plan.values if v.storage == "immutable_value")
    return p.DataReference(plan.id, run_id, value.id, DataForm.IMMUTABLE_VALUE)


def _announce(worker, data, size=123):
    run = worker.coordinator._run(data.run_id)
    seed_location(worker.coordinator, run.plan, data.run_id, worker.handle.worker_id, data, size)
    return EventDisposition.APPLIED


def test_location_available_unavailable_and_worker_loss(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    w1 = connect(coordinator, plan, "W1", port=9001)
    w2 = connect(coordinator, plan, "W2", port=9002)
    coordinator.submit(plan, run_id="r")
    data = _data_for(plan)
    assert _announce(w1, data) == EventDisposition.APPLIED
    assert _announce(w2, data) == EventDisposition.APPLIED
    location, = coordinator.data_locations("r")
    assert {x.worker_id for x in location.replicas} == {"W1", "W2"}
    w1.send(p.ObjectUnavailable("W1", data, "evict", message_id="evict"))
    location, = coordinator.data_locations("r")
    assert {x.worker_id for x in location.replicas} == {"W2"}
    w2.send(p.WorkerGoodbye("W2", "bye", message_id="bye"))
    assert coordinator.data_locations("r") == ()


def test_transfer_full_lifecycle_and_destination_only_publication(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    source = connect(coordinator, plan, "W1", port=9001)
    destination = connect(coordinator, plan, "W2", port=9002)
    coordinator.submit(plan, run_id="r")
    data = _data_for(plan)
    _announce(source, data, 500)
    transfer = p.TransferIdentity(data, "T", "TA1", "W1", "W2")
    prepare = coordinator.start_transfer(transfer, size_bytes=500)
    assert destination.drain() == (prepare,)
    assert {r.worker_id for r in coordinator.data_locations("r")[0].replicas} == {"W1"}

    ready = p.ReceiveReady("W2", transfer, message_id="ready", correlation_id=prepare.message_id)
    assert destination.send(ready) == EventDisposition.APPLIED
    requests = [m for m in source.drain() if isinstance(m, p.TransferRequest)]
    assert len(requests) == 1
    request = requests[0]
    assert request.transfer == transfer
    assert destination.drain() == ()

    assert source.send(p.TransferAccepted(
        "W1", transfer, message_id="accepted", correlation_id=request.message_id
    )) == EventDisposition.APPLIED
    assert source.send(p.TransferStarted(
        "W1", transfer, message_id="started", correlation_id=request.message_id
    )) == EventDisposition.APPLIED
    assert {r.worker_id for r in coordinator.data_locations("r")[0].replicas} == {"W1"}
    assert destination.send(p.TransferCompleted(
        "W2", transfer, 500, message_id="complete", correlation_id=prepare.message_id
    )) == EventDisposition.APPLIED
    assert coordinator.get_transfer("T", "TA1").status == TransferStatus.COMPLETED
    assert {r.worker_id for r in coordinator.data_locations("r")[0].replicas} == {"W1", "W2"}
    coordinator.validate_state()


def test_duplicate_receive_ready_does_not_emit_second_send(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    source = connect(coordinator, plan, "W1", port=9001)
    destination = connect(coordinator, plan, "W2", port=9002)
    coordinator.submit(plan, run_id="r")
    data = _data_for(plan); _announce(source, data)
    transfer = p.TransferIdentity(data, "T", "TA1", "W1", "W2")
    prepare = coordinator.start_transfer(transfer)
    destination.drain()
    ready = p.ReceiveReady("W2", transfer, message_id="ready", correlation_id=prepare.message_id)
    assert destination.send(ready) == EventDisposition.APPLIED
    first = [m for m in source.drain() if isinstance(m, p.TransferRequest)]; assert len(first) == 1
    assert destination.send(ready) == EventDisposition.DUPLICATE
    assert source.drain() == ()


def test_destination_preparation_failure_never_contacts_source(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    source = connect(coordinator, plan, "W1", port=9001)
    destination = connect(coordinator, plan, "W2", port=9002)
    coordinator.submit(plan, run_id="r")
    data = _data_for(plan); _announce(source, data)
    transfer = p.TransferIdentity(data, "T", "TA1", "W1", "W2")
    prepare = coordinator.start_transfer(transfer); destination.drain()
    failure = p.ReceivePreparationFailed(
        "W2", transfer, p.TransferFailureCode.IO_ERROR, "disk",
        message_id="fail", correlation_id=prepare.message_id,
    )
    assert destination.send(failure) == EventDisposition.APPLIED
    assert not [m for m in source.drain() if isinstance(m, p.TransferRequest)]
    assert coordinator.get_transfer("T", "TA1").status == TransferStatus.FAILED


def test_completion_before_source_notifications_is_valid_cross_stream_reordering(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    source = connect(coordinator, plan, "W1", port=9001)
    destination = connect(coordinator, plan, "W2", port=9002)
    coordinator.submit(plan, run_id="r")
    data = _data_for(plan); _announce(source, data)
    transfer = p.TransferIdentity(data, "T", "TA1", "W1", "W2")
    prepare = coordinator.start_transfer(transfer); destination.drain()
    destination.send(p.ReceiveReady("W2", transfer, message_id="rr", correlation_id=prepare.message_id))
    requests = [m for m in source.drain() if isinstance(m, p.TransferRequest)]
    assert len(requests) == 1
    request = requests[0]
    assert destination.send(p.TransferCompleted(
        "W2", transfer, None, message_id="complete", correlation_id=prepare.message_id
    )) == EventDisposition.APPLIED
    assert any(r.worker_id == "W2" for r in coordinator.data_locations("r")[0].replicas)
    assert source.send(p.TransferAccepted(
        "W1", transfer, message_id="late-accepted", correlation_id=request.message_id
    )) == EventDisposition.DUPLICATE
    assert source.send(p.TransferStarted(
        "W1", transfer, message_id="late-started", correlation_id=request.message_id
    )) == EventDisposition.DUPLICATE


def test_failed_transfer_can_retry_with_new_attempt_and_late_old_is_stale(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    source = connect(coordinator, plan, "W1", port=9001)
    destination = connect(coordinator, plan, "W2", port=9002)
    coordinator.submit(plan, run_id="r")
    data = _data_for(plan); _announce(source, data)
    t1 = p.TransferIdentity(data, "T", "TA1", "W1", "W2")
    prep1 = coordinator.start_transfer(t1); destination.drain()
    destination.send(p.ReceivePreparationFailed(
        "W2", t1, p.TransferFailureCode.IO_ERROR, "no", message_id="f1", correlation_id=prep1.message_id
    ))
    t2 = p.TransferIdentity(data, "T", "TA2", "W1", "W2")
    coordinator.start_transfer(t2); destination.drain()
    late = p.ReceiveReady("W2", t1, message_id="late", correlation_id=prep1.message_id)
    assert destination.send(late) == EventDisposition.STALE
    assert coordinator.get_transfer("T", "TA2").status == TransferStatus.DESTINATION_PREPARING


def test_worker_loss_fails_active_transfer(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    source = connect(coordinator, plan, "W1", port=9001)
    destination = connect(coordinator, plan, "W2", port=9002)
    coordinator.submit(plan, run_id="r")
    data = _data_for(plan); _announce(source, data)
    transfer = p.TransferIdentity(data, "T", "TA1", "W1", "W2")
    coordinator.start_transfer(transfer); destination.drain()
    source.send(p.WorkerGoodbye("W1", "lost", message_id="bye"))
    assert coordinator.get_transfer("T", "TA1").status == TransferStatus.FAILED
