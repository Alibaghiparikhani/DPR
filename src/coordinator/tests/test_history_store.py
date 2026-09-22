from __future__ import annotations

import pytest

from coordinator import Coordinator, InvalidRunTransition, SQLiteRunHistoryStore, UnknownRun
from coordinator.tests.helpers import accept, connect, dispatch_for, release_terminal_objects, start, succeed


def _finish_single_task(coordinator, plan, run_id="r"):
    coordinator.submit(plan, run_id=run_id)
    worker = connect(coordinator, plan)
    coordinator.schedule(run_id)
    dispatch = dispatch_for(worker)
    accept(worker, dispatch)
    start(worker, dispatch)
    succeed(worker, plan, dispatch)
    return worker, dispatch


def test_terminal_run_is_archived_and_survives_in_memory_pruning(tmp_path, build_plan):
    _, plan = build_plan("x = 1 + 2")
    store = SQLiteRunHistoryStore(tmp_path / "history.sqlite3")
    coordinator = Coordinator(history_store=store)
    worker, _ = _finish_single_task(coordinator, plan)

    bundle = store.load_run("r")
    assert bundle is not None
    assert bundle.run.status == "succeeded"
    assert bundle.run.plan_id == plan.id
    assert len(bundle.tasks) == len(plan.tasks)
    assert len(bundle.attempts) == 1
    assert bundle.attempts[0].status == "committed"

    # Phase 3 requires proof that terminal replicas are physically gone
    # before coordinator state may be pruned.
    assert release_terminal_objects(coordinator, worker) >= 1
    coordinator.prune_terminal_run("r")
    with pytest.raises(UnknownRun):
        coordinator.inspect_run("r")
    persisted = store.load_run("r")
    assert persisted is not None and persisted.run.status == "succeeded"


def test_archive_is_idempotent_for_same_terminal_run(tmp_path, build_plan):
    _, plan = build_plan("x = 1 + 2")
    store = SQLiteRunHistoryStore(tmp_path / "history.sqlite3")
    coordinator = Coordinator(history_store=store)
    _finish_single_task(coordinator, plan)
    run = coordinator._runs["r"]
    coordinator._archive_terminal_run(run)
    coordinator._archive_terminal_run(run)
    bundle = store.load_run("r")
    assert bundle is not None
    assert len(bundle.tasks) == len(plan.tasks)
    assert len(bundle.attempts) == 1


def test_pruning_without_durable_store_is_refused(build_plan):
    _, plan = build_plan("x = 1 + 2")
    coordinator = Coordinator()
    worker, _ = _finish_single_task(coordinator, plan)
    assert release_terminal_objects(coordinator, worker) >= 1
    with pytest.raises(InvalidRunTransition, match="durably archived"):
        coordinator.prune_terminal_run("r")


def test_history_store_rejects_nonterminal_archive(tmp_path, build_plan):
    _, plan = build_plan("x = 1 + 2")
    store = SQLiteRunHistoryStore(tmp_path / "history.sqlite3")
    coordinator = Coordinator(history_store=store)
    coordinator.submit(plan, run_id="r")
    with pytest.raises(ValueError, match="terminal"):
        store.archive_run(coordinator._runs["r"], (), archived_at=0.0)


def test_delete_persisted_run_cascades_history(tmp_path, build_plan):
    _, plan = build_plan("x = 1 + 2")
    store = SQLiteRunHistoryStore(tmp_path / "history.sqlite3")
    coordinator = Coordinator(history_store=store)
    _finish_single_task(coordinator, plan)
    assert store.delete_run("r") is True
    assert store.load_run("r") is None
    assert store.delete_run("r") is False
