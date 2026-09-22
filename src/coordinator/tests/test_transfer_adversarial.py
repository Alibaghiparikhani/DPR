import pytest
import protocol as p

from coordinator import (
    EventDisposition, InvalidDataLocation, InvalidTransferTransition,
    InvalidWorkerMessage, TransferStatus,
)
from coordinator.tests.helpers import connect, seed_location
from scheduler import DataForm


def setup_transfer(coordinator, build_plan):
    _, plan=build_plan("a=1\n")
    source=connect(coordinator,plan,"W1",port=9001)
    destination=connect(coordinator,plan,"W2",port=9002)
    coordinator.submit(plan,run_id="r")
    value=next(v for v in plan.values if v.storage=="immutable_value")
    data=p.DataReference(plan.id,"r",value.id,DataForm.IMMUTABLE_VALUE)
    seed_location(coordinator, plan, "r", "W1", data, 100)
    transfer=p.TransferIdentity(data,"T","TA1","W1","W2")
    prepare=coordinator.start_transfer(transfer,size_bytes=100)
    destination.drain()
    return plan,source,destination,data,transfer,prepare


def ready_and_request(source,destination,transfer,prepare):
    destination.send(p.ReceiveReady("W2",transfer,message_id="ready",correlation_id=prepare.message_id))
    return next(m for m in source.drain() if isinstance(m,p.TransferRequest) and m.transfer==transfer)


def started(source,destination,transfer,prepare):
    request=ready_and_request(source,destination,transfer,prepare)
    source.send(p.TransferAccepted("W1",transfer,message_id="accepted",correlation_id=request.message_id))
    source.send(p.TransferStarted("W1",transfer,message_id="started",correlation_id=request.message_id))
    return request


def test_wrong_destination_completion_correlation_rejected(coordinator,build_plan):
    _,source,destination,_,transfer,prepare=setup_transfer(coordinator,build_plan)
    started(source,destination,transfer,prepare)
    with pytest.raises(InvalidWorkerMessage):
        destination.send(p.TransferCompleted("W2",transfer,100,message_id="done",correlation_id="source-command"))
    assert coordinator.get_transfer("T","TA1").status==TransferStatus.TRANSFERRING


def test_source_cannot_authoritatively_complete_transfer(coordinator,build_plan):
    _,source,destination,_,transfer,prepare=setup_transfer(coordinator,build_plan)
    started(source,destination,transfer,prepare)
    # The frozen protocol rejects this before coordinator routing; destination is authoritative.
    with pytest.raises(p.ValidationError):
        p.TransferCompleted("W1",transfer,100,message_id="forged",correlation_id=prepare.message_id)
    assert coordinator.get_transfer("T","TA1").status==TransferStatus.TRANSFERRING


def test_source_failure_after_start_never_publishes_destination(coordinator,build_plan):
    _,source,destination,data,transfer,prepare=setup_transfer(coordinator,build_plan)
    request=started(source,destination,transfer,prepare)
    assert source.send(p.TransferFailed(
        "W1",transfer,p.TransferFailureCode.IO_ERROR,"send failed",
        message_id="sf",correlation_id=request.message_id,
    ))==EventDisposition.APPLIED
    assert coordinator.get_transfer("T","TA1").status==TransferStatus.FAILED
    assert {r.worker_id for r in coordinator.data_locations("r")[0].replicas}=={"W1"}


def test_destination_failure_after_start_never_publishes_destination(coordinator,build_plan):
    _,source,destination,data,transfer,prepare=setup_transfer(coordinator,build_plan)
    started(source,destination,transfer,prepare)
    assert destination.send(p.TransferFailed(
        "W2",transfer,p.TransferFailureCode.INTEGRITY_ERROR,"bad digest",
        message_id="df",correlation_id=prepare.message_id,
    ))==EventDisposition.APPLIED
    assert coordinator.get_transfer("T","TA1").status==TransferStatus.FAILED
    assert {r.worker_id for r in coordinator.data_locations("r")[0].replicas}=={"W1"}


def test_duplicate_completion_is_idempotent(coordinator,build_plan):
    _,source,destination,_,transfer,prepare=setup_transfer(coordinator,build_plan)
    started(source,destination,transfer,prepare)
    msg=p.TransferCompleted("W2",transfer,100,message_id="done",correlation_id=prepare.message_id)
    assert destination.send(msg)==EventDisposition.APPLIED
    assert destination.send(msg)==EventDisposition.DUPLICATE
    assert {r.worker_id for r in coordinator.data_locations("r")[0].replicas}=={"W1","W2"}


def test_conflicting_completion_size_is_atomic(coordinator,build_plan):
    _,source,destination,_,transfer,prepare=setup_transfer(coordinator,build_plan)
    started(source,destination,transfer,prepare)
    with pytest.raises(InvalidDataLocation):
        destination.send(p.TransferCompleted("W2",transfer,101,message_id="bad-size",correlation_id=prepare.message_id))
    assert coordinator.get_transfer("T","TA1").status==TransferStatus.TRANSFERRING
    assert {r.worker_id for r in coordinator.data_locations("r")[0].replicas}=={"W1"}


def test_cannot_retry_completed_logical_transfer(coordinator,build_plan):
    _,source,destination,data,transfer,prepare=setup_transfer(coordinator,build_plan)
    started(source,destination,transfer,prepare)
    destination.send(p.TransferCompleted("W2",transfer,100,message_id="done",correlation_id=prepare.message_id))
    retry=p.TransferIdentity(data,"T","TA2","W1","W2")
    with pytest.raises(InvalidTransferTransition):
        coordinator.start_transfer(retry,size_bytes=100)


def test_source_eviction_before_destination_ready_aborts_without_send(coordinator,build_plan):
    _,source,destination,data,transfer,prepare=setup_transfer(coordinator,build_plan)
    # discard membership noise first
    source.drain()
    source.send(p.ObjectUnavailable("W1",data,"evicted",message_id="gone"))
    result=destination.send(p.ReceiveReady("W2",transfer,message_id="ready",correlation_id=prepare.message_id))
    assert result==EventDisposition.APPLIED
    assert coordinator.get_transfer("T","TA1").status==TransferStatus.FAILED
    assert not [m for m in source.drain() if isinstance(m,p.TransferRequest)]


def test_receive_preparation_failure_after_ready_is_invalid(coordinator,build_plan):
    _,source,destination,_,transfer,prepare=setup_transfer(coordinator,build_plan)
    ready_and_request(source,destination,transfer,prepare)
    with pytest.raises(InvalidTransferTransition):
        destination.send(p.ReceivePreparationFailed(
            "W2",transfer,p.TransferFailureCode.IO_ERROR,"late",
            message_id="late",correlation_id=prepare.message_id,
        ))


def test_source_transfer_event_requires_source_command_correlation(coordinator,build_plan):
    _,source,destination,_,transfer,prepare=setup_transfer(coordinator,build_plan)
    request=ready_and_request(source,destination,transfer,prepare)
    with pytest.raises(InvalidWorkerMessage):
        source.send(p.TransferAccepted("W1",transfer,message_id="a",correlation_id=prepare.message_id))
    assert coordinator.get_transfer("T","TA1").status==TransferStatus.SOURCE_REQUESTED


def test_source_failure_explicitly_cancels_destination_physical_receive(coordinator,build_plan):
    _,source,destination,_,transfer,prepare=setup_transfer(coordinator,build_plan)
    request=started(source,destination,transfer,prepare)
    destination.drain()
    source.send(p.TransferFailed(
        "W1",transfer,p.TransferFailureCode.IO_ERROR,"send failed",
        message_id="source-failed",correlation_id=request.message_id,
    ))
    cancels=[m for m in destination.drain() if isinstance(m,p.CancelTransfer)]
    assert len(cancels)==1 and cancels[0].transfer==transfer
    record=coordinator.get_transfer("T","TA1")
    assert record.status==TransferStatus.FAILED
    assert record.source_cleanup_confirmed and record.destination_cancel_requested


def test_destination_failure_explicitly_cancels_source_physical_sender(coordinator,build_plan):
    _,source,destination,_,transfer,prepare=setup_transfer(coordinator,build_plan)
    request=started(source,destination,transfer,prepare)
    source.drain()
    destination.send(p.TransferFailed(
        "W2",transfer,p.TransferFailureCode.INTEGRITY_ERROR,"bad digest",
        message_id="destination-failed",correlation_id=prepare.message_id,
    ))
    cancels=[m for m in source.drain() if isinstance(m,p.CancelTransfer)]
    assert len(cancels)==1 and cancels[0].transfer==transfer
    record=coordinator.get_transfer("T","TA1")
    assert record.status==TransferStatus.FAILED
    assert record.destination_cleanup_confirmed and record.source_cancel_requested


def test_run_cancel_stages_both_transfer_participant_cleanup_commands_once(coordinator,build_plan):
    _,source,destination,_,transfer,prepare=setup_transfer(coordinator,build_plan)
    started(source,destination,transfer,prepare)
    source.drain(); destination.drain()
    coordinator.cancel_run("r")
    source_cancels=[m for m in source.drain() if isinstance(m,p.CancelTransfer)]
    destination_cancels=[m for m in destination.drain() if isinstance(m,p.CancelTransfer)]
    assert len(source_cancels)==1 and source_cancels[0].transfer==transfer
    assert len(destination_cancels)==1 and destination_cancels[0].transfer==transfer
    # Repeated terminal cancellation cannot enqueue another physical cancel.
    assert coordinator.cancel_run("r")==()
    assert not [m for m in source.drain() if isinstance(m,p.CancelTransfer)]
    assert not [m for m in destination.drain() if isinstance(m,p.CancelTransfer)]
