from __future__ import annotations

import pytest
import protocol as p
from execution import FailureInfo, FailureKind
from coordinator import Coordinator, OutboundBackpressure, RunStatus
from coordinator.tests.helpers import accept, connect, dispatch_for, fail, start, worker_state
from scheduler import TaskAffinity, WorkerContext


def _run_with_context(coordinator, plan, worker_id="W1", *, run_id="r"):
    worker = connect(coordinator, plan, worker_id, slots=1)
    task = plan.tasks[0]
    context = WorkerContext("ctx", worker_id, frozenset({task.task_id}), 1)
    coordinator.submit(
        plan, run_id=run_id,
        affinities=(TaskAffinity(task.task_id, required_worker=worker_id, context_id="ctx"),),
        contexts=(context,),
    )
    return worker, task


def test_run_cancellation_sends_explicit_context_release_even_when_context_idle(build_plan):
    _, plan = build_plan("globals()")
    coordinator = Coordinator()
    worker, _ = _run_with_context(coordinator, plan)
    coordinator.cancel_run("r", reason="caller cancelled")
    messages = worker.drain()
    releases = [m for m in messages if isinstance(m, p.ReleaseContext)]
    assert len(releases) == 1
    release = releases[0]
    assert (release.worker_id, release.plan_id, release.run_id, release.context_id) == (
        "W1", plan.id, "r", "ctx"
    )
    assert coordinator.inspect_run("r").status is RunStatus.CANCELLED


def test_native_failure_stages_context_retirement_after_terminal_failure(build_plan):
    _, plan = build_plan('raise ValueError("boom")')
    coordinator = Coordinator()
    worker, task = _run_with_context(coordinator, plan)
    coordinator.schedule("r")
    dispatch = dispatch_for(worker)
    assert dispatch.attempt.task_id == task.task_id and dispatch.context_id == "ctx"
    accept(worker, dispatch); start(worker, dispatch)
    fail(worker, dispatch, FailureInfo(FailureKind.PYTHON_EXCEPTION, "boom", "builtins.ValueError", "trace"))
    messages = worker.drain()
    assert any(isinstance(m, p.ReleaseContext) and m.context_id == "ctx" for m in messages)
    assert coordinator.inspect_run("r").status is RunStatus.FAILED


def test_context_release_backpressure_is_failure_atomic_for_cancel_run(build_plan):
    _, plan = build_plan("globals()")
    coordinator = Coordinator(outbox_limit=1)
    worker, _ = _run_with_context(coordinator, plan)
    # Occupy the sole outbound slot with a replaceable heartbeat acknowledgement.
    worker.send(p.Heartbeat(worker_state(plan, "W1", slots=1), 1, message_id="hb"))
    assert coordinator.inspect_run("r").status is RunStatus.RUNNING
    with pytest.raises(OutboundBackpressure):
        coordinator.cancel_run("r")
    # No partial cancellation/context mutation occurred.
    assert coordinator.inspect_run("r").status is RunStatus.RUNNING
    assert "ctx" in coordinator._runs["r"].contexts
