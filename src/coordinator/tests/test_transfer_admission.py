"""Transfers wait on the coordinator instead of piling up on workers.

A wide fan-in used to send every PrepareReceive / TransferRequest at once, so a
worker's data-plane limits or the transfer deadline failed the run.  Now each
worker takes part in at most `max_transfers_per_worker` transfers per direction;
the rest are queued on the coordinator, hold nothing on any worker and have no
deadline running until they are sent.
"""
import pytest

import protocol as p

from coordinator import (
    Coordinator, OperationLimits, RetryPolicy, RunStatus, TaskStatus,
    TransferStatus,
)
from coordinator.errors import UnknownTransfer
from coordinator.tests.helpers import accept, connect, start, succeed, worker_state
from coordinator.tests.test_automatic_transfers import _finish_transfer

WIDTH = 12


def _fan_in_source(width: int) -> str:
    lines = [f"a{i} = {i}" for i in range(width)]
    lines.append("total = " + " + ".join(f"a{i}" for i in range(width)))
    return "\n".join(lines)


def _stage_fan_in(coordinator, build_plan, width=WIDTH):
    """Roots on W1, the fan-in forced onto W2; returns (w1, w2, prepares seen so far)."""
    _, plan = build_plan(_fan_in_source(width))
    w1 = connect(coordinator, plan, "W1", slots=width, port=9001)
    coordinator.submit(plan, run_id="r")
    coordinator.schedule("r")
    roots = [m for m in w1.drain() if isinstance(m, p.TaskDispatch)]
    assert len(roots) == width
    for dispatch in roots:
        accept(w1, dispatch); start(w1, dispatch); succeed(w1, plan, dispatch)
    w2 = connect(coordinator, plan, "W2", slots=1, port=9002)
    w1.send(p.Heartbeat(worker_state(plan, "W1", accepting=False), 1, message_id="hb-W1-1"))
    w1.drain()
    coordinator.schedule("r")
    return plan, w1, w2


def _prepares(fake, dispatched=None):
    messages = fake.drain()
    if dispatched is not None:
        dispatched.extend(m for m in messages if isinstance(m, p.TaskDispatch))
    return [m for m in messages if isinstance(m, p.PrepareReceive)]


def test_fan_in_is_admitted_in_bounded_batches_and_completes(coordinator, build_plan):
    limit = coordinator.operation_limits.max_transfers_per_worker
    assert WIDTH > limit
    _, w1, w2 = _stage_fan_in(coordinator, build_plan)
    attempt = coordinator.inspect_run("r").current_attempts[0]
    assert coordinator.get_task("r", attempt.task_id).status == TaskStatus.WAITING_TRANSFER

    dispatched = []
    outstanding = _prepares(w2, dispatched)
    assert len(outstanding) == limit  # the rest wait on the coordinator
    finished = 0
    while outstanding:
        _finish_transfer(w1, w2, outstanding.pop(0))
        finished += 1
        released = _prepares(w2, dispatched)
        assert len(released) <= 1
        outstanding.extend(released)
        assert len(outstanding) <= limit
    assert finished == WIDTH
    assert [d.attempt for d in dispatched] == [attempt]
    coordinator.validate_state()


def test_queued_transfers_have_no_deadline_until_sent(coordinator, build_plan):
    limit = coordinator.operation_limits.max_transfers_per_worker
    _, w1, w2 = _stage_fan_in(coordinator, build_plan)
    assert len(_prepares(w2)) == limit
    expired = coordinator.expire_operations(now=coordinator.operation_timeouts.transfer + 1)
    # Only the transfers actually sent can expire; the queued ones never started.
    assert len([name for name in expired if name.startswith("transfer:")]) == limit


def test_cancelled_run_sends_no_cleanup_for_queued_transfers(coordinator, build_plan):
    limit = coordinator.operation_limits.max_transfers_per_worker
    _, w1, w2 = _stage_fan_in(coordinator, build_plan)
    sent = _prepares(w2)
    assert len(sent) == limit
    coordinator.cancel_run("r")
    cancels = [m for m in w2.drain() if isinstance(m, p.CancelTransfer)]
    assert {m.transfer for m in cancels} == {m.transfer for m in sent}
    assert coordinator.inspect_run("r").status == RunStatus.CANCELLED
    assert not _prepares(w2)  # nothing queued is ever sent afterwards
    coordinator.validate_state()


def test_worker_events_cannot_name_a_queued_transfer(coordinator, build_plan):
    _, w1, w2 = _stage_fan_in(coordinator, build_plan)
    sent = {m.transfer.transfer_id for m in _prepares(w2)}
    queued = next(record for record in coordinator._transfers.values()
                  if record.identity.transfer_id not in sent)
    assert queued.status == TransferStatus.DESTINATION_PREPARING
    with pytest.raises(UnknownTransfer):
        w2.send(p.ReceiveReady("W2", queued.identity, message_id="forged",
                               correlation_id=queued.destination_request_id))


def test_lost_destination_drops_its_queued_transfers(coordinator, build_plan):
    _, w1, w2 = _stage_fan_in(coordinator, build_plan)
    assert _prepares(w2)
    coordinator.disconnect_session(w2.handle)
    assert not coordinator._queued_transfers
    assert all(record.status.terminal and record.cleanup_confirmed
               for record in coordinator._transfers.values())
    coordinator.validate_state()


def test_limit_is_configurable():
    assert OperationLimits(max_transfers_per_worker=2).max_transfers_per_worker == 2
    with pytest.raises(ValueError):
        OperationLimits(max_transfers_per_worker=0)


def test_small_limit_still_completes(build_plan):
    coordinator = Coordinator(retry_policy=RetryPolicy(max_attempts_per_task=3),
                              operation_limits=OperationLimits(max_transfers_per_worker=1))
    _, w1, w2 = _stage_fan_in(coordinator, build_plan, width=5)
    attempt = coordinator.inspect_run("r").current_attempts[0]
    dispatched = []
    outstanding = _prepares(w2, dispatched)
    assert len(outstanding) == 1
    while outstanding:
        _finish_transfer(w1, w2, outstanding.pop(0))
        outstanding.extend(_prepares(w2, dispatched))
    assert [d.attempt for d in dispatched] == [attempt]


def test_failed_run_cleans_up_sibling_transfers(build_plan):
    """When one input fails and the run fails with it, the other prepared inputs are
    cancelled on their workers at once rather than left to time out there."""
    coordinator = Coordinator(retry_policy=RetryPolicy(max_attempts_per_task=1))
    _, w1, w2 = _stage_fan_in(coordinator, build_plan, width=3)
    sent = _prepares(w2)
    assert len(sent) == 3
    failing = sent[0]
    w2.send(p.ReceivePreparationFailed(
        "W2", failing.transfer, p.TransferFailureCode.IO_ERROR, "disk full",
        message_id="prep-failed", correlation_id=failing.message_id))
    assert coordinator.inspect_run("r").status == RunStatus.FAILED
    cancelled = {m.transfer for m in w2.drain() if isinstance(m, p.CancelTransfer)}
    assert cancelled == {m.transfer for m in sent[1:]}
    coordinator.validate_state()


def test_transfer_deadline_grows_with_the_value_size():
    from coordinator import OperationTimeouts
    timeouts = OperationTimeouts()
    assert timeouts.transfer_deadline(None) == timeouts.transfer
    big = 256 * 1024 * 1024
    assert timeouts.transfer_deadline(big) == timeouts.transfer + big / timeouts.transfer_bytes_per_second
    with pytest.raises(ValueError):
        OperationTimeouts(transfer_bytes_per_second=0)


def test_large_transfer_is_not_expired_while_within_its_allowance(coordinator, build_plan):
    _, w1, w2 = _stage_fan_in(coordinator, build_plan, width=2)
    prepares = _prepares(w2)
    assert len(prepares) == 2
    big = next(coordinator._transfers[(m.transfer.transfer_id, m.transfer.transfer_attempt_id)]
               for m in prepares)
    big.size_bytes = 64 * 1024 * 1024
    past_fixed = coordinator.operation_timeouts.transfer + 1
    expired = coordinator.expire_operations(now=past_fixed)
    assert f"transfer:{big.identity.transfer_id}/{big.identity.transfer_attempt_id}" not in expired
