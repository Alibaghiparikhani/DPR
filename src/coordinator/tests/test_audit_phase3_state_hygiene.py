from __future__ import annotations

from pathlib import Path

import pytest
import protocol as p
from execution import FailureInfo, FailureKind

from coordinator import (
    ContextConflict,
    Coordinator,
    InvalidRunTransition,
    OperationLimits,
    OperationalLimitExceeded,
    SQLiteRunHistoryStore,
    UnknownRun,
)
from coordinator.tests.helpers import accept, connect, dispatch_for, start, succeed
from scheduler import TaskAffinity, WorkerContext


class ToggleWriteHistoryStore(SQLiteRunHistoryStore):
    """Real SQLite archive with deterministic write-failure injection."""

    def __init__(self, path: Path) -> None:
        self.fail_writes = False
        super().__init__(path)

    def archive_run(self, run, transfers, *, archived_at):
        if self.fail_writes:
            raise OSError("forced archive refresh failure")
        return super().archive_run(run, transfers, archived_at=archived_at)


def test_context_loss_while_cancelling_retains_then_retires_tombstone(build_plan):
    _, plan = build_plan("print(1)")
    task = plan.tasks[0]
    coordinator = Coordinator()
    worker = connect(coordinator, plan, "W1", slots=1)
    context = WorkerContext("ctx", "W1", frozenset({task.task_id}), 1)
    coordinator.submit(
        plan,
        run_id="r",
        affinities=(TaskAffinity(task.task_id, required_worker="W1", context_id="ctx"),),
        contexts=(context,),
    )
    coordinator.schedule("r")
    dispatch = dispatch_for(worker)
    accept(worker, dispatch)
    start(worker, dispatch)
    coordinator.cancel_run("r")
    cancel = next(message for message in worker.drain() if isinstance(message, p.CancelTask))

    assert worker.send(p.ContextUnavailable(
        "W1", plan.id, "r", "ctx", "gone", message_id="ctx-gone",
    )).value == "applied"
    assert "ctx" in coordinator._runs["r"].contexts
    assert "ctx" in coordinator._runs["r"].unavailable_context_ids
    coordinator.validate_state()

    worker.send(p.TaskCancellationResult(
        "W1", dispatch.attempt, p.CancellationOutcome.CANCELLED, "",
        message_id="cancelled", correlation_id=cancel.message_id,
    ))
    assert "ctx" not in coordinator._runs["r"].contexts
    assert "ctx" not in coordinator._runs["r"].unavailable_context_ids
    coordinator.validate_state()


def test_dynamic_context_preparation_rejects_affinity_conflict_before_enqueue(build_plan):
    _, plan = build_plan("print(1)")
    task = plan.tasks[0]
    coordinator = Coordinator()
    connect(coordinator, plan, "W1", port=9001)
    w2 = connect(coordinator, plan, "W2", port=9002)
    coordinator.submit(
        plan,
        run_id="r",
        affinities=(TaskAffinity(task.task_id, required_worker="W1", context_id="ctx"),),
    )

    with pytest.raises(ContextConflict, match="required worker"):
        coordinator.request_context_preparation("r", "W2", "ctx", (task.task_id,))
    assert w2.drain() == ()
    assert coordinator.inspect_pending_operations().active_count == 0
    assert coordinator._runs["r"].contexts == {}
    coordinator.validate_state()


def test_history_refresh_failure_blocks_prune_until_latest_state_is_durable(tmp_path, build_plan):
    _, plan = build_plan("print(1)")
    task = plan.tasks[0]
    store = ToggleWriteHistoryStore(tmp_path / "history.sqlite3")
    coordinator = Coordinator(history_store=store)
    worker = connect(coordinator, plan, "W1", slots=1)
    context = WorkerContext("ctx", "W1", frozenset({task.task_id}), 1)
    coordinator.submit(
        plan,
        run_id="r",
        affinities=(TaskAffinity(task.task_id, required_worker="W1", context_id="ctx"),),
        contexts=(context,),
    )
    coordinator.schedule("r")
    dispatch = dispatch_for(worker)
    accept(worker, dispatch)
    start(worker, dispatch)
    worker.send(p.ContextUnavailable(
        "W1", plan.id, "r", "ctx", "gone", message_id="ctx-gone",
    ))
    cancel = next(message for message in worker.drain() if isinstance(message, p.CancelTask))
    archived = store.load_run("r")
    assert archived is not None and archived.attempts[0].status == "orphaned"

    store.fail_writes = True
    worker.send(p.TaskCancellationResult(
        "W1", dispatch.attempt, p.CancellationOutcome.CANCELLED, "",
        message_id="cancelled", correlation_id=cancel.message_id,
    ))
    assert coordinator.history_archive_error("r") is not None
    assert store.load_run("r").attempts[0].status == "orphaned"

    with pytest.raises(InvalidRunTransition, match="current terminal run state"):
        coordinator.prune_terminal_run("r")
    assert coordinator.inspect_run("r").status.terminal

    store.fail_writes = False
    coordinator.prune_terminal_run("r")
    with pytest.raises(UnknownRun):
        coordinator.inspect_run("r")
    refreshed = store.load_run("r")
    assert refreshed is not None and refreshed.attempts[0].status == "cancelled"


def _prepare_context(coordinator, worker, plan, run_id, context_id, task_id):
    command = coordinator.request_context_preparation(
        run_id, worker.handle.worker_id, context_id, (task_id,),
    )
    assert command is not None
    worker.drain()
    worker.send(p.ContextPrepared(
        plan.id,
        run_id,
        WorkerContext(context_id, worker.handle.worker_id, frozenset({task_id}), 1),
        message_id=f"prepared-{context_id}",
        correlation_id=command.message_id,
    ))


def test_retained_context_limit_bounds_completed_preparations_and_release_restores_admission(build_plan):
    _, plan = build_plan("a=1\nb=2")
    limits = OperationLimits(
        max_retained_contexts_global=2,
        max_retained_contexts_per_worker=2,
        max_retained_contexts_per_run=2,
    )
    coordinator = Coordinator(operation_limits=limits)
    worker = connect(coordinator, plan, "W1", slots=1)
    coordinator.submit(plan, run_id="r")
    first_task = plan.tasks[0].task_id

    _prepare_context(coordinator, worker, plan, "r", "ctx-1", first_task)
    _prepare_context(coordinator, worker, plan, "r", "ctx-2", first_task)
    assert coordinator.inspect_pending_operations().active_count == 0
    assert len(coordinator._runs["r"].contexts) == 2

    with pytest.raises(OperationalLimitExceeded, match="retained-context"):
        coordinator.request_context_preparation("r", "W1", "ctx-3", (first_task,))
    assert set(coordinator._runs["r"].contexts) == {"ctx-1", "ctx-2"}

    # Once the task certified by ctx-1 is complete while the run stays live,
    # ContextUnavailable is terminal evidence that safely retires the unused
    # retained context and restores one admission slot.
    coordinator.schedule("r")
    dispatch = dispatch_for(worker)
    assert dispatch.attempt.task_id == first_task
    accept(worker, dispatch)
    start(worker, dispatch)
    succeed(worker, plan, dispatch)
    worker.send(p.ContextUnavailable(
        "W1", plan.id, "r", "ctx-1", "released", message_id="release-ctx-1",
    ))
    assert "ctx-1" not in coordinator._runs["r"].contexts
    _prepare_context(coordinator, worker, plan, "r", "ctx-3", first_task)
    assert set(coordinator._runs["r"].contexts) == {"ctx-2", "ctx-3"}
    coordinator.validate_state()


def test_initial_contexts_obey_same_retention_limit_atomically(build_plan):
    _, plan = build_plan("a=1\nb=2")
    limits = OperationLimits(
        max_retained_contexts_global=1,
        max_retained_contexts_per_worker=1,
        max_retained_contexts_per_run=1,
    )
    coordinator = Coordinator(operation_limits=limits)
    connect(coordinator, plan, "W1")
    task = plan.tasks[0].task_id
    contexts = (
        WorkerContext("ctx-1", "W1", frozenset({task}), 1),
        WorkerContext("ctx-2", "W1", frozenset({task}), 1),
    )
    with pytest.raises(OperationalLimitExceeded, match="retained-context"):
        coordinator.submit(plan, run_id="r", contexts=contexts)
    with pytest.raises(UnknownRun):
        coordinator.inspect_run("r")


def test_pending_context_reserves_retained_capacity_before_worker_creation(build_plan):
    _, plan = build_plan("a=1")
    limits = OperationLimits(
        max_retained_contexts_global=1,
        max_retained_contexts_per_worker=1,
        max_retained_contexts_per_run=1,
    )
    coordinator = Coordinator(operation_limits=limits)
    worker = connect(coordinator, plan, "W1")
    coordinator.submit(plan, run_id="r")
    task = plan.tasks[0].task_id
    first = coordinator.request_context_preparation("r", "W1", "ctx-1", (task,))
    assert first is not None
    worker.drain()
    with pytest.raises(OperationalLimitExceeded, match="retained-context"):
        coordinator.request_context_preparation("r", "W1", "ctx-2", (task,))
    assert coordinator.inspect_pending_operations().active_count == 1
    coordinator.validate_state()


def test_failed_context_tombstone_retires_when_owner_session_is_lost(build_plan):
    _, plan = build_plan("print(1)")
    task = plan.tasks[0]
    coordinator = Coordinator()
    worker = connect(coordinator, plan, "W1", slots=1)
    context = WorkerContext("ctx", "W1", frozenset({task.task_id}), 1)
    coordinator.submit(
        plan,
        run_id="r",
        affinities=(TaskAffinity(task.task_id, required_worker="W1", context_id="ctx"),),
        contexts=(context,),
    )
    coordinator.schedule("r")
    dispatch = dispatch_for(worker)
    accept(worker, dispatch)
    start(worker, dispatch)
    worker.send(p.ContextUnavailable(
        "W1", plan.id, "r", "ctx", "gone", message_id="ctx-gone",
    ))
    assert "ctx" in coordinator._runs["r"].unavailable_context_ids

    worker.send(p.WorkerGoodbye("W1", "disconnect", message_id="bye"))
    assert "ctx" not in coordinator._runs["r"].contexts
    assert "ctx" not in coordinator._runs["r"].unavailable_context_ids
    coordinator.validate_state()


def test_retained_context_per_run_limit_is_independent(build_plan):
    _, plan = build_plan("a=1")
    limits = OperationLimits(
        max_retained_contexts_global=10,
        max_retained_contexts_per_worker=10,
        max_retained_contexts_per_run=1,
    )
    coordinator = Coordinator(operation_limits=limits)
    worker = connect(coordinator, plan, "W1")
    coordinator.submit(plan, run_id="r")
    task = plan.tasks[0].task_id
    _prepare_context(coordinator, worker, plan, "r", "ctx-1", task)
    with pytest.raises(OperationalLimitExceeded, match="run r retained-context"):
        coordinator.request_context_preparation("r", "W1", "ctx-2", (task,))
    coordinator.validate_state()


def test_retained_context_per_worker_limit_spans_runs(build_plan):
    _, plan = build_plan("a=1")
    limits = OperationLimits(
        max_retained_contexts_global=10,
        max_retained_contexts_per_worker=1,
        max_retained_contexts_per_run=10,
    )
    coordinator = Coordinator(operation_limits=limits)
    worker = connect(coordinator, plan, "W1")
    coordinator.submit(plan, run_id="r1")
    coordinator.submit(plan, run_id="r2")
    task = plan.tasks[0].task_id
    _prepare_context(coordinator, worker, plan, "r1", "ctx-1", task)
    with pytest.raises(OperationalLimitExceeded, match="worker W1 retained-context"):
        coordinator.request_context_preparation("r2", "W1", "ctx-2", (task,))
    coordinator.validate_state()


@pytest.mark.parametrize(
    "affinity, worker_id, context_id, match",
    (
        (lambda task: TaskAffinity(task, allowed_workers=frozenset({"W1"}), context_id="ctx"), "W2", "ctx", "allowed workers"),
        (lambda task: TaskAffinity(task, required_worker="W1", context_id="ctx"), "W1", "other", "context affinity"),
    ),
)
def test_dynamic_context_preparation_reuses_all_affinity_constraints(
    build_plan, affinity, worker_id, context_id, match,
):
    _, plan = build_plan("print(1)")
    task = plan.tasks[0]
    coordinator = Coordinator()
    connect(coordinator, plan, "W1", port=9001)
    w2 = connect(coordinator, plan, "W2", port=9002)
    coordinator.submit(plan, run_id="r", affinities=(affinity(task.task_id),))
    with pytest.raises(ContextConflict, match=match):
        coordinator.request_context_preparation("r", worker_id, context_id, (task.task_id,))
    assert w2.drain() == ()
    coordinator.validate_state()


def test_failed_or_duplicate_pending_context_does_not_leak_retention_reservation(build_plan):
    _, plan = build_plan("a=1")
    limits = OperationLimits(
        max_retained_contexts_global=1,
        max_retained_contexts_per_worker=1,
        max_retained_contexts_per_run=1,
    )
    coordinator = Coordinator(operation_limits=limits)
    worker = connect(coordinator, plan, "W1")
    coordinator.submit(plan, run_id="r")
    task = plan.tasks[0].task_id

    first = coordinator.request_context_preparation("r", "W1", "ctx-1", (task,))
    duplicate = coordinator.request_context_preparation("r", "W1", "ctx-1", (task,))
    assert first is not None and duplicate == first
    assert coordinator.inspect_pending_operations().active_count == 1
    worker.drain()

    worker.send(p.ContextPreparationFailed(
        "W1", plan.id, "r", "ctx-1",
        FailureInfo(FailureKind.EXECUTION_ERROR, "failed"),
        message_id="ctx-failed", correlation_id=first.message_id,
    ))
    assert coordinator.inspect_pending_operations().active_count == 0
    second = coordinator.request_context_preparation("r", "W1", "ctx-2", (task,))
    assert second is not None
    coordinator.validate_state()
