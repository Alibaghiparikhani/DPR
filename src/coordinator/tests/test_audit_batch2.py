from __future__ import annotations

from dataclasses import replace

import pytest
import protocol as p

from coordinator import (
    CapacityConflict, ContextConflict, Coordinator, EventDisposition,
    InvalidRunTransition, InvalidTransferTransition, RunStatus, SQLiteRunHistoryStore,
    TaskStatus, TransferStatus,
)
from coordinator.tests.helpers import accept, connect, dispatch_for, release_terminal_objects, seed_location, start, succeed, worker_state
from scheduler import DataForm, TaskAffinity, WorkerContext


class FailingHistoryStore:
    def __init__(self):
        self.calls = 0
    def archive_run(self, run, transfers, *, archived_at):
        self.calls += 1
        raise OSError("history unavailable")
    def has_run(self, run_id):
        return False


def _immutable_data(plan, run_id="r"):
    value = next(v for v in plan.values if v.storage == "immutable_value")
    return p.DataReference(plan.id, run_id, value.id, DataForm.IMMUTABLE_VALUE)


def test_transfer_completion_may_arrive_before_source_notifications(coordinator, build_plan):
    _, plan = build_plan("a=1")
    source = connect(coordinator, plan, "W1", port=9001)
    destination = connect(coordinator, plan, "W2", port=9002)
    coordinator.submit(plan, run_id="r")
    data = _immutable_data(plan)
    seed_location(coordinator, plan, "r", "W1", data, 10)
    transfer = p.TransferIdentity(data, "T", "TA1", "W1", "W2")
    prepare = coordinator.start_transfer(transfer, size_bytes=10); destination.drain()
    destination.send(p.ReceiveReady("W2", transfer, message_id="ready", correlation_id=prepare.message_id))
    request = next(m for m in source.drain() if isinstance(m, p.TransferRequest))

    assert destination.send(p.TransferCompleted(
        "W2", transfer, 10, message_id="done", correlation_id=prepare.message_id,
    )) == EventDisposition.APPLIED
    assert coordinator.get_transfer("T", "TA1").status == TransferStatus.COMPLETED
    # Source-side observations are causally valid but may arrive later on an
    # independent control stream.
    assert source.send(p.TransferAccepted(
        "W1", transfer, message_id="accepted", correlation_id=request.message_id,
    )) == EventDisposition.DUPLICATE
    assert source.send(p.TransferStarted(
        "W1", transfer, message_id="started", correlation_id=request.message_id,
    )) == EventDisposition.DUPLICATE
    coordinator.validate_state()


def test_history_failure_does_not_interrupt_multi_run_worker_loss(build_plan):
    _, plan = build_plan("print(1)")
    store = FailingHistoryStore()
    c = Coordinator(history_store=store)
    w = connect(c, plan, "W1", slots=2)
    task = plan.tasks[0]
    for run_id, ctx_id in (("r1", "c1"), ("r2", "c2")):
        c.submit(plan, run_id=run_id, affinities=(TaskAffinity(task.task_id, required_worker="W1", context_id=ctx_id),),
                 contexts=(WorkerContext(ctx_id, "W1", frozenset({task.task_id}), 1),))
        c.schedule(run_id)
        d = dispatch_for(w); accept(w, d); start(w, d)
    assert w.send(p.WorkerGoodbye("W1", "lost", message_id="bye")) == EventDisposition.APPLIED
    assert c.inspect_run("r1").status == RunStatus.FAILED
    assert c.inspect_run("r2").status == RunStatus.FAILED
    assert c.history_archive_error("r1") is not None
    assert c.history_archive_error("r2") is not None
    c.validate_state()


def test_pruning_refuses_unresolved_manual_transfer(tmp_path, build_plan):
    _, plan = build_plan("a=1\nb=2")
    store = SQLiteRunHistoryStore(tmp_path / "h.sqlite3")
    c = Coordinator(history_store=store)
    source = connect(c, plan, "W1", slots=2, port=9001)
    c.submit(plan, run_id="r")
    c.schedule("r")
    dispatches = [m for m in source.drain() if isinstance(m, p.TaskDispatch)]
    assert len(dispatches) == 2
    first, second = dispatches
    for d in dispatches:
        accept(source, d); start(source, d)
    succeed(source, plan, first)
    output_id = plan.task_index[first.attempt.task_id].outputs[0].id
    ref = p.DataReference(plan.id, "r", output_id, DataForm.IMMUTABLE_VALUE)
    destination = connect(c, plan, "W2", port=9002)
    source.drain()  # membership update from W2 registration
    transfer = p.TransferIdentity(ref, "manual", "m1", "W1", "W2")
    c.start_transfer(transfer); destination.drain()
    succeed(source, plan, second)
    assert c.inspect_run("r").status == RunStatus.SUCCEEDED
    # Remove the ordinary terminal replicas first so this regression isolates
    # the still-unresolved manual transfer as the pruning blocker.
    release_terminal_objects(c, source, destination)
    with pytest.raises(InvalidRunTransition, match="unresolved transfer"):
        c.prune_terminal_run("r")
    assert c.get_transfer("manual", "m1").status == TransferStatus.DESTINATION_PREPARING


def test_new_transfer_on_terminal_run_is_rejected(tmp_path, build_plan):
    _, plan = build_plan("a=1")
    store = SQLiteRunHistoryStore(tmp_path / "h.sqlite3")
    c = Coordinator(history_store=store)
    source = connect(c, plan, "W1", port=9001)
    connect(c, plan, "W2", port=9002)
    c.submit(plan, run_id="r"); c.schedule("r")
    d = dispatch_for(source); accept(source, d); start(source, d); succeed(source, plan, d)
    loc = c.data_locations("r")[0]
    ref = p.DataReference(plan.id, "r", loc.value_id, loc.form, loc.object_state_id)
    with pytest.raises(InvalidTransferTransition, match="running run"):
        c.start_transfer(p.TransferIdentity(ref, "late", "a1", "W1", "W2"))


def test_heartbeat_capacity_shrink_is_rejected_atomically(coordinator, build_plan):
    _, plan = build_plan("a=1\nb=2")
    w = connect(coordinator, plan, "W1", slots=2)
    coordinator.submit(plan, run_id="r")
    coordinator.schedule("r")
    ds = [m for m in w.drain() if isinstance(m, p.TaskDispatch)]
    assert len(ds) == 2
    for d in ds:
        accept(w, d); start(w, d)
    before = coordinator.inspect_worker("W1")
    shrunken = worker_state(plan, "W1", slots=1)
    with pytest.raises(CapacityConflict):
        w.send(p.Heartbeat(shrunken, 1, message_id="shrink"), now=1)
    after = coordinator.inspect_worker("W1")
    assert after.state.total_slots == before.state.total_slots == 2
    coordinator.build_snapshot("r")
    coordinator.validate_state()


def test_context_unavailable_during_execution_keeps_capacity_tombstone(coordinator, build_plan):
    _, plan = build_plan("print(1)")
    task = plan.tasks[0]
    w = connect(coordinator, plan, "W1", slots=1)
    context = WorkerContext("ctx", "W1", frozenset({task.task_id}), 1)
    coordinator.submit(plan, run_id="r",
                       affinities=(TaskAffinity(task.task_id, required_worker="W1", context_id="ctx"),),
                       contexts=(context,))
    coordinator.schedule("r")
    d = dispatch_for(w); accept(w, d); start(w, d)
    assert w.send(p.ContextUnavailable(
        "W1", plan.id, "r", "ctx", "gone", message_id="gone",
    )) == EventDisposition.APPLIED
    assert coordinator.inspect_run("r").status == RunStatus.FAILED
    assert coordinator.get_attempt("r", d.attempt.attempt_id).status.value == "orphaned"
    coordinator.validate_state()


def test_cancel_invalid_reason_is_failure_atomic(coordinator, build_plan):
    _, plan = build_plan("a=1")
    w = connect(coordinator, plan, "W1")
    coordinator.submit(plan, run_id="r")
    coordinator.schedule("r")
    d = dispatch_for(w); accept(w, d); start(w, d)
    before = coordinator.inspect_run("r")
    with pytest.raises((TypeError, ValueError)):
        coordinator.cancel_run("r", reason=123)  # type: ignore[arg-type]
    assert coordinator.inspect_run("r") == before
    assert coordinator.get_attempt("r", d.attempt.attempt_id).cancel_message_id is None
    assert w.drain() == ()
    messages = coordinator.cancel_run("r", reason="valid")
    assert len(messages) == 1


def test_manual_transfer_invalid_size_is_failure_atomic(coordinator, build_plan):
    _, plan = build_plan("a=1")
    source = connect(coordinator, plan, "W1", port=9001)
    destination = connect(coordinator, plan, "W2", port=9002)
    coordinator.submit(plan, run_id="r")
    data = _immutable_data(plan)
    seed_location(coordinator, plan, "r", "W1", data, None)
    transfer = p.TransferIdentity(data, "T", "TA1", "W1", "W2")
    with pytest.raises((TypeError, ValueError)):
        coordinator.start_transfer(transfer, size_bytes=-1)
    with pytest.raises(Exception):
        coordinator.get_transfer("T", "TA1")
    assert destination.drain() == ()
    valid = coordinator.start_transfer(transfer, size_bytes=1)
    assert destination.drain() == (valid,)


def test_submit_rejects_unknown_initial_context_task_atomically(coordinator, build_plan):
    _, plan = build_plan("a=1")
    bad = WorkerContext("ctx", "W1", frozenset({"does-not-exist"}), 1)
    with pytest.raises(ContextConflict):
        coordinator.submit(plan, run_id="r", contexts=(bad,))
    with pytest.raises(Exception):
        coordinator.inspect_run("r")


def test_program_eviction_while_transfer_pending_prevents_dispatch(coordinator, build_plan):
    _, plan = build_plan("a=1\nb=a+1")
    producer, consumer = plan.tasks
    w1 = connect(coordinator, plan, "W1", slots=1, port=9001)
    w2 = connect(coordinator, plan, "W2", slots=1, port=9002)
    coordinator.submit(plan, run_id="r", affinities=(TaskAffinity(consumer.task_id, required_worker="W2"),))
    coordinator.schedule("r")
    d1 = dispatch_for(w1); accept(w1, d1); start(w1, d1); succeed(w1, plan, d1)
    coordinator.schedule("r")
    prepare = next(m for m in w2.drain() if isinstance(m, p.PrepareReceive))
    assert w2.send(p.ProgramUnavailable("W2", plan.program.id, "gone", message_id="unavail")) == EventDisposition.APPLIED
    w2.send(p.ReceiveReady("W2", prepare.transfer, message_id="rr", correlation_id=prepare.message_id))
    request = next(m for m in w1.drain() if isinstance(m, p.TransferRequest))
    w1.send(p.TransferAccepted("W1", request.transfer, message_id="a", correlation_id=request.message_id))
    w1.send(p.TransferStarted("W1", request.transfer, message_id="s", correlation_id=request.message_id))
    w2.send(p.TransferCompleted("W2", request.transfer, None, message_id="c", correlation_id=prepare.message_id))
    assert not [m for m in w2.drain() if isinstance(m, p.TaskDispatch)]
    assert coordinator.get_task("r", consumer.task_id).status == TaskStatus.READY
    coordinator.validate_state()


def test_durable_run_identity_cannot_be_reused_after_prune(tmp_path, build_plan):
    _, p1 = build_plan("a=1")
    _, p2 = build_plan("a=2")
    store = SQLiteRunHistoryStore(tmp_path / "h.sqlite3")
    c = Coordinator(history_store=store)
    w = connect(c, p1, "W1")
    c.submit(p1, run_id="same")
    c.schedule("same")
    d = dispatch_for(w); accept(w, d); start(w, d); succeed(w, p1, d)
    assert release_terminal_objects(c, w) >= 1
    c.prune_terminal_run("same")
    with pytest.raises(InvalidRunTransition, match="durable history"):
        c.submit(p2, run_id="same")
    assert store.load_run("same").run.plan_id == p1.id


def test_history_store_rejects_conflicting_direct_rearchive(tmp_path, build_plan):
    _, p1 = build_plan("a=1")
    _, p2 = build_plan("a=2")
    store = SQLiteRunHistoryStore(tmp_path / "h.sqlite3")
    c1 = Coordinator(history_store=store)
    w1 = connect(c1, p1, "W1")
    c1.submit(p1, run_id="same"); c1.schedule("same")
    d = dispatch_for(w1); accept(w1, d); start(w1, d); succeed(w1, p1, d)
    c2 = Coordinator()
    c2.submit(p2, run_id="same")
    # force terminal state through normal execution without using the history store
    w2 = connect(c2, p2, "W2"); c2.schedule("same")
    d2 = dispatch_for(w2); accept(w2, d2); start(w2, d2); succeed(w2, p2, d2)
    with pytest.raises(ValueError, match="different execution"):
        store.archive_run(c2._runs["same"], (), archived_at=1.0)
    assert store.load_run("same").run.plan_id == p1.id


def test_real_sqlite_lock_does_not_interrupt_worker_loss_reconciliation(tmp_path, build_plan):
    import sqlite3

    class FastLockStore(SQLiteRunHistoryStore):
        def _connect(self):
            db = sqlite3.connect(self.path, timeout=0)
            db.execute("PRAGMA foreign_keys = ON")
            return db

    _, plan = build_plan("print(1)")
    store = FastLockStore(tmp_path / "locked.sqlite3")
    c = Coordinator(history_store=store)
    w = connect(c, plan, "W1", slots=2)
    task = plan.tasks[0]
    for run_id, context_id in (("one", "ctx-one"), ("two", "ctx-two")):
        c.submit(plan, run_id=run_id,
                 affinities=(TaskAffinity(task.task_id, required_worker="W1", context_id=context_id),),
                 contexts=(WorkerContext(context_id, "W1", frozenset({task.task_id}), 1),))
        c.schedule(run_id)
        d = dispatch_for(w); accept(w, d); start(w, d)

    lock = sqlite3.connect(store.path, timeout=0)
    lock.execute("BEGIN EXCLUSIVE")
    try:
        assert w.send(p.WorkerGoodbye("W1", "lost", message_id="bye")) == EventDisposition.APPLIED
        assert c.inspect_run("one").status == RunStatus.FAILED
        assert c.inspect_run("two").status == RunStatus.FAILED
        assert "locked" in c.history_archive_error("one").lower()
        assert "locked" in c.history_archive_error("two").lower()
        c.validate_state()
    finally:
        lock.rollback(); lock.close()

    # The failed administrative archive remains recoverable after storage returns.
    c.prune_terminal_run("one")
    c.prune_terminal_run("two")
    assert store.has_run("one") and store.has_run("two")


def test_cancel_late_id_source_failure_is_atomic(coordinator, build_plan):
    _, plan = build_plan("a=1\nb=2")
    w = connect(coordinator, plan, "W1", slots=2)
    coordinator.submit(plan, run_id="r"); coordinator.schedule("r")
    dispatches = [m for m in w.drain() if isinstance(m, p.TaskDispatch)]
    assert len(dispatches) == 2
    for d in dispatches:
        accept(w, d); start(w, d)
    before = coordinator.inspect_run("r")
    old_id = coordinator._id
    calls = 0
    def failing(kind):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("id source failed")
        return f"staged-{kind}-{calls}"
    coordinator._id = failing
    try:
        with pytest.raises(RuntimeError, match="id source failed"):
            coordinator.cancel_run("r")
    finally:
        coordinator._id = old_id
    assert coordinator.inspect_run("r") == before
    assert all(coordinator.get_attempt("r", d.attempt.attempt_id).cancel_message_id is None for d in dispatches)
    assert w.drain() == ()


def test_capacity_can_shrink_after_reservations_are_resolved(coordinator, build_plan):
    _, plan = build_plan("a=1")
    w = connect(coordinator, plan, "W1", slots=2)
    coordinator.submit(plan, run_id="r"); coordinator.schedule("r")
    d = dispatch_for(w); accept(w, d); start(w, d); succeed(w, plan, d)
    shrunken = worker_state(plan, "W1", slots=1)
    assert w.send(p.Heartbeat(shrunken, 1, message_id="shrink"), now=1) == EventDisposition.APPLIED
    assert coordinator.inspect_worker("W1").state.total_slots == 1


def test_submit_rejects_inactive_initial_context_owner(coordinator, build_plan):
    _, plan = build_plan("print(1)")
    task = plan.tasks[0]
    context = WorkerContext("ctx", "missing", frozenset({task.task_id}), 1)
    with pytest.raises(ContextConflict, match="active worker"):
        coordinator.submit(plan, run_id="r", contexts=(context,))


def test_submit_rejects_initial_context_affinity_owner_conflict(coordinator, build_plan):
    _, plan = build_plan("print(1)")
    task = plan.tasks[0]
    connect(coordinator, plan, "W1", port=9001)
    connect(coordinator, plan, "W2", port=9002)
    context = WorkerContext("ctx", "W2", frozenset({task.task_id}), 1)
    affinity = TaskAffinity(task.task_id, required_worker="W1", context_id="ctx")
    with pytest.raises(ContextConflict, match="required worker"):
        coordinator.submit(plan, run_id="r", affinities=(affinity,), contexts=(context,))


def test_durable_run_identity_reuse_is_rejected_after_coordinator_restart(tmp_path, build_plan):
    _, p1 = build_plan("a=1")
    _, p2 = build_plan("a=2")
    store = SQLiteRunHistoryStore(tmp_path / "h.sqlite3")
    first = Coordinator(history_store=store)
    w = connect(first, p1, "W1")
    first.submit(p1, run_id="same"); first.schedule("same")
    d = dispatch_for(w); accept(w, d); start(w, d); succeed(w, p1, d)
    # F19 archives terminal history asynchronously; a restart-history test must
    # wait until the archive is actually durable before constructing the new
    # coordinator instance.
    assert store.wait_for_archive("same", timeout=1.0)
    second = Coordinator(history_store=SQLiteRunHistoryStore(store.path))
    with pytest.raises(InvalidRunTransition, match="durable history"):
        second.submit(p2, run_id="same")
    assert store.load_run("same").run.plan_id == p1.id


def test_completion_before_source_authorization_is_still_rejected(coordinator, build_plan):
    _, plan = build_plan("a=1")
    source = connect(coordinator, plan, "W1", port=9001)
    destination = connect(coordinator, plan, "W2", port=9002)
    coordinator.submit(plan, run_id="r")
    data = _immutable_data(plan); seed_location(coordinator, plan, "r", "W1", data, 10)
    transfer = p.TransferIdentity(data, "T", "TA1", "W1", "W2")
    prepare = coordinator.start_transfer(transfer, size_bytes=10); destination.drain()
    with pytest.raises(InvalidTransferTransition):
        destination.send(p.TransferCompleted(
            "W2", transfer, 10, message_id="premature", correlation_id=prepare.message_id,
        ))


def test_rejected_capacity_heartbeat_does_not_refresh_liveness(coordinator, build_plan):
    _, plan = build_plan("a=1\nb=2")
    w = connect(coordinator, plan, "W1", slots=2, now=0)
    coordinator.submit(plan, run_id="r"); coordinator.schedule("r")
    dispatches = [m for m in w.drain() if isinstance(m, p.TaskDispatch)]
    assert len(dispatches) == 2
    for d in dispatches:
        accept(w, d); start(w, d)
    with pytest.raises(CapacityConflict):
        w.send(p.Heartbeat(worker_state(plan, "W1", slots=1), 1, message_id="bad"), now=99)
    assert coordinator.inspect_worker("W1").last_seen == 0


def test_receive_ready_id_generation_failure_leaves_transfer_retryable(coordinator, build_plan):
    _, plan = build_plan("a=1")
    source = connect(coordinator, plan, "W1", port=9001)
    destination = connect(coordinator, plan, "W2", port=9002)
    coordinator.submit(plan, run_id="r")
    data = _immutable_data(plan); seed_location(coordinator, plan, "r", "W1", data, 10)
    transfer = p.TransferIdentity(data, "T", "TA1", "W1", "W2")
    prepare = coordinator.start_transfer(transfer, size_bytes=10); destination.drain()
    ready = p.ReceiveReady("W2", transfer, message_id="rr", correlation_id=prepare.message_id)
    old_id = coordinator._id
    coordinator._id = lambda kind: (_ for _ in ()).throw(RuntimeError("id failure"))
    try:
        with pytest.raises(RuntimeError, match="id failure"):
            destination.send(ready)
    finally:
        coordinator._id = old_id
    record = coordinator.get_transfer("T", "TA1")
    assert record.status == TransferStatus.DESTINATION_PREPARING
    assert record.source_request_id is None
    assert destination.send(ready) == EventDisposition.APPLIED
    assert any(isinstance(m, p.TransferRequest) for m in source.drain())


def test_validate_state_rejects_corrupted_initial_context_fact(coordinator, build_plan):
    _, plan = build_plan("print(1)")
    task = plan.tasks[0]
    connect(coordinator, plan, "W1")
    context = WorkerContext("ctx", "W1", frozenset({task.task_id}), 1)
    coordinator.submit(plan, run_id="r", contexts=(context,))
    coordinator._runs["r"].contexts["ctx"] = WorkerContext("ctx", "W1", frozenset({"ghost"}), 1)
    with pytest.raises(AssertionError, match="unknown task"):
        coordinator.validate_state()
