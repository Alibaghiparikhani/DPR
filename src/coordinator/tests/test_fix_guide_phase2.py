import pytest

import protocol as p
from coordinator import Coordinator, EventDisposition, TransferStatus
from coordinator.tests.helpers import accept, connect, dispatch_for, start, succeed, worker_state


def _hb(fake, plan, accepting, seq):
    fake.send(p.Heartbeat(worker_state(plan, fake.handle.worker_id, accepting=accepting), seq,
                          message_id=f"hb-{fake.handle.worker_id}-{seq}"))
    fake.drain()


def _inflight_transfer(build_plan):
    c = Coordinator()
    _, plan = build_plan("a=1\nb=a+1\n")
    w1 = connect(c, plan, "W1", slots=1, port=9001)
    w2 = connect(c, plan, "W2", slots=1, port=9002)
    c.submit(plan, run_id="r"); c.schedule("r")
    root = dispatch_for(w1); accept(w1, root); start(w1, root); succeed(w1, plan, root)
    _hb(w1, plan, False, 1); c.schedule("r")
    prep = next(m for m in w2.drain() if isinstance(m, p.PrepareReceive))
    w2.send(p.ReceiveReady("W2", prep.transfer, message_id="rr", correlation_id=prep.message_id))
    req = next(m for m in w1.drain() if isinstance(m, p.TransferRequest))
    return c, plan, w1, w2, prep, req


def test_f1_late_source_transfer_events_after_cancel_are_stale(build_plan):
    for kind in ("accepted", "started"):
        c, _, w1, _, prep, req = _inflight_transfer(build_plan)
        if kind == "started":
            assert w1.send(p.TransferAccepted("W1", prep.transfer, message_id="pre-a",
                                              correlation_id=req.message_id)) == EventDisposition.APPLIED
        c.cancel_run("r")
        assert c.get_transfer(prep.transfer.transfer_id, prep.transfer.transfer_attempt_id).status == TransferStatus.FAILED
        msg = (p.TransferAccepted("W1", prep.transfer, message_id="late-a", correlation_id=req.message_id)
               if kind == "accepted" else
               p.TransferStarted("W1", prep.transfer, message_id="late-s", correlation_id=req.message_id))
        assert w1.send(msg) == EventDisposition.STALE
        c.validate_state()
        assert "W1" in c.worker_ids(active_only=True)


def test_f2_task_rejected_after_cancel_finishes_cancellation(build_plan):
    c = Coordinator()
    _, plan = build_plan("a=1\n")
    w = connect(c, plan, "W1", slots=1)
    c.submit(plan, run_id="r"); c.schedule("r")
    dispatch = dispatch_for(w)
    rejection = p.TaskRejected("W1", dispatch.attempt, p.RejectionCode.BUSY, "busy",
                               message_id="reject", correlation_id=dispatch.message_id)
    c.cancel_run("r")
    assert w.send(rejection) == EventDisposition.STALE
    assert c.inspect_run("r").status.value == "cancelled"
    assert c.get_task("r", dispatch.attempt.task_id).status.value == "cancelled"
    assert c.get_attempt("r", dispatch.attempt.attempt_id).status.value == "cancelled"
    c.validate_state()


def _run_to_completion(c, plan, worker, run_id):
    # Mirrors the service: reclaimable terminal runs are pruned before admission.
    c.prune_releasable_terminal_runs()
    c.submit(plan, run_id=run_id)
    c.schedule(run_id)
    task = dispatch_for(worker)
    accept(worker, task); start(worker, task); succeed(worker, plan, task)
    # A terminal run only becomes reclaimable once its physical replicas are
    # released (F5/F6), so complete the release handshake the way a worker does.
    c.request_terminal_object_releases()
    for message in worker.drain():
        if isinstance(message, p.ReleaseObject):
            worker.send(p.ObjectReleased(
                message.worker_id, message.data,
                message_id=f"released-{run_id}-{message.message_id}",
                correlation_id=message.message_id,
            ))
    return run_id


def test_terminal_runs_are_retained_without_history_until_memory_pressure(build_plan):
    """F4/F59: dropping terminal runs immediately would erase the only record of
    their outcome when no durable history store is configured, so `run_status`
    would answer UnknownRun moments after a run finished.  Retain them until
    admission pressure requires reclaiming, then drop the oldest first."""
    from coordinator import OperationLimits

    limit = 8
    c = Coordinator(operation_limits=OperationLimits(max_runs_in_memory=limit))
    _, plan = build_plan("a=1\n")
    w1 = connect(c, plan, "W1", slots=1, port=9001)

    retained = max(1, (limit * 3) // 4)
    for index in range(retained):
        _run_to_completion(c, plan, w1, f"r{index}")
        c.prune_releasable_terminal_runs()
    # Below the high-water mark every terminal run stays queryable.
    assert len(c.run_ids()) == retained
    assert c.inspect_run("r0").status.terminal

    # Past the mark the oldest terminal runs are reclaimed, keeping admission open.
    for index in range(retained, limit + 4):
        _run_to_completion(c, plan, w1, f"r{index}")
        c.prune_releasable_terminal_runs()
    assert len(c.run_ids()) <= limit
    assert "r0" not in c.run_ids()
    assert f"r{limit + 3}" in c.run_ids()
    c.validate_state()


@pytest.mark.parametrize("limit", [1, 2, 3, 8])
def test_terminal_run_retention_never_blocks_admission(build_plan, limit):
    """Retention must stay strictly below the admission limit.

    Retaining up to `max_runs_in_memory` would keep the last slot occupied, so a
    small limit (notably 1) would refuse every submission after the first.
    """
    from coordinator import OperationLimits

    c = Coordinator(operation_limits=OperationLimits(max_runs_in_memory=limit))
    _, plan = build_plan("a=1\n")
    w1 = connect(c, plan, "W1", slots=1, port=9001)
    for index in range(limit + 6):
        _run_to_completion(c, plan, w1, f"r{index}")
    assert len(c.run_ids()) <= limit
    c.validate_state()


def test_unpreparable_run_records_a_visible_run_level_reason(build_plan):
    """A run that no worker can prepare must not archive as a bare "failed".

    The task-level FailureInfo is not surfaced by `dpr status`/history, so the
    run-level cause has to be recorded too, otherwise operators see no reason for
    the most likely production failure (a package a worker cannot install).
    """
    from coordinator import CoordinatorFailureCode

    c = Coordinator()
    _, plan = build_plan("a=1\n")
    connect(c, plan, "W1", slots=1, port=9001)
    c.submit(plan, run_id="r")
    c.fail_run_unpreparable("r", "W1: package installation failed: cache budget exceeded")

    snapshot = c.inspect_run("r")
    assert snapshot.status.terminal
    assert snapshot.failure is not None, "run-level failure was not recorded"
    assert snapshot.failure.code is CoordinatorFailureCode.PLACEMENT_CONSTRAINT_LOST
    assert "could prepare the program" in snapshot.failure.detail
    assert "cache budget exceeded" in snapshot.failure.detail
    c.validate_state()
