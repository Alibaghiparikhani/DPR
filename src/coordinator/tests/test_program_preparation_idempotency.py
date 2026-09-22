from __future__ import annotations

import pytest

import protocol as p
from coordinator import EventDisposition, InvalidWorkerMessage
from coordinator.model import PendingProgramPreparation
from coordinator.tests.helpers import connect
from dag_runtime.dag_engine import AnalysisOptions, analyze_source
from execution import lower_dag


def _plan(source: str = "a=1\n", *, environment: str = "test-env", package: str | None = "test-package"):
    return lower_dag(analyze_source(source), environment_id=environment, package_id=package)


def _same_program_different_plans():
    source = "def f(x):\n    y=x+1\n    return y\na=f(1)\nb=2\n"
    shallow = analyze_source(source, options=AnalysisOptions(max_function_ast_nodes=1))
    deep = analyze_source(source, options=AnalysisOptions(max_function_ast_nodes=192))
    one = lower_dag(shallow, environment_id="test-env", package_id="test-package")
    two = lower_dag(deep, environment_id="test-env", package_id="test-package")
    assert one.program == two.program
    assert one.id != two.id
    return one, two


def test_one_program_preparation_request_creates_one_pending_operation(coordinator):
    plan = _plan()
    w = connect(coordinator, plan, "W1", prepared=False)
    command = coordinator.request_program_preparation("W1", plan)
    assert command is not None
    assert w.drain() == (command,)
    assert coordinator.inspect_pending_operations().active_count == 1
    pending = next(iter(coordinator._pending_ops.active_requests()))
    assert isinstance(pending, PendingProgramPreparation)
    assert pending.message_id == command.message_id
    coordinator.validate_state()


def test_5000_identical_pending_program_requests_are_idempotent(coordinator):
    plan = _plan()
    w = connect(coordinator, plan, "W1", prepared=False)
    first = coordinator.request_program_preparation("W1", plan)
    assert first is not None
    for _ in range(4_999):
        assert coordinator.request_program_preparation("W1", plan) == first
    assert coordinator.inspect_pending_operations().active_count == 1
    assert w.drain() == (first,)
    assert coordinator.inspect_pending_operations().retired_count == 0
    coordinator.validate_state()


def test_successfully_prepared_program_is_not_requested_again(coordinator):
    plan = _plan()
    w = connect(coordinator, plan, "W1", prepared=False)
    command = coordinator.request_program_preparation("W1", plan)
    w.drain()
    response = p.ProgramPrepared(
        "W1", plan.id, plan.program.id,
        message_id="prepared", correlation_id=command.message_id,
    )
    assert w.send(response) == EventDisposition.APPLIED
    assert coordinator.request_program_preparation("W1", plan) is None
    assert w.drain() == ()
    assert coordinator.inspect_pending_operations().active_count == 0
    coordinator.validate_state()


def test_reconnect_does_not_reuse_old_pending_program_preparation(coordinator):
    plan = _plan()
    old = connect(coordinator, plan, "W1", prepared=False, port=9001)
    first = coordinator.request_program_preparation("W1", plan)
    old.drain()

    new = connect(coordinator, plan, "W1", prepared=False, port=9002)
    new.drain()
    second = coordinator.request_program_preparation("W1", plan)
    assert second is not None
    assert second.message_id != first.message_id
    assert new.drain() == (second,)
    active = [r for r in coordinator._pending_ops.active_requests() if isinstance(r, PendingProgramPreparation)]
    assert len(active) == 1
    assert active[0].worker_generation == new.handle.generation

    with pytest.raises(InvalidWorkerMessage):
        new.send(p.ProgramPrepared(
            "W1", plan.id, plan.program.id,
            message_id="late-old", correlation_id=first.message_id,
        ))
    assert plan.program.id not in coordinator.inspect_worker("W1").state.prepared_program_ids
    assert new.send(p.ProgramPrepared(
        "W1", plan.id, plan.program.id,
        message_id="current", correlation_id=second.message_id,
    )) == EventDisposition.APPLIED
    coordinator.validate_state()


def test_different_programs_are_not_deduplicated(coordinator):
    one = _plan("a=1\n")
    two = _plan("a=2\n")
    w = connect(coordinator, one, "W1", prepared=False)
    first = coordinator.request_program_preparation("W1", one)
    second = coordinator.request_program_preparation("W1", two)
    assert first is not None and second is not None
    assert first.message_id != second.message_id
    assert coordinator.inspect_pending_operations().active_count == 2
    assert w.drain() == (first, second)


@pytest.mark.parametrize(
    ("environment", "package"),
    [
        ("other-env", "test-package"),
        ("test-env", "other-package"),
    ],
)
def test_different_environment_or_package_is_not_deduplicated(coordinator, environment, package):
    one = _plan()
    two = _plan(environment=environment, package=package)
    assert one.program.id != two.program.id
    w = connect(coordinator, one, "W1", prepared=False)
    first = coordinator.request_program_preparation("W1", one)
    second = coordinator.request_program_preparation("W1", two)
    assert first is not None and second is not None
    assert first.message_id != second.message_id
    assert coordinator.inspect_pending_operations().active_count == 2
    assert w.drain() == (first, second)


def test_different_plan_scope_with_same_program_is_not_deduplicated(coordinator):
    one, two = _same_program_different_plans()
    w = connect(coordinator, one, "W1", prepared=False)
    first = coordinator.request_program_preparation("W1", one)
    second = coordinator.request_program_preparation("W1", two)
    assert first is not None and second is not None
    assert first.program == second.program
    assert first.plan_id != second.plan_id
    assert first.message_id != second.message_id
    assert coordinator.inspect_pending_operations().active_count == 2
    assert w.drain() == (first, second)


def test_same_program_on_different_workers_is_not_deduplicated(coordinator):
    plan = _plan()
    w1 = connect(coordinator, plan, "W1", prepared=False, port=9001)
    w2 = connect(coordinator, plan, "W2", prepared=False, port=9002)
    w1.drain()  # membership update from W2 registration
    first = coordinator.request_program_preparation("W1", plan)
    second = coordinator.request_program_preparation("W2", plan)
    assert first is not None and second is not None
    assert first.worker_id != second.worker_id
    assert coordinator.inspect_pending_operations().active_count == 2
    assert w1.drain() == (first,)
    assert w2.drain() == (second,)


def test_stale_response_for_deduplicated_old_request_cannot_mutate_current_state(coordinator):
    plan = _plan()
    old = connect(coordinator, plan, "W1", prepared=False, port=9001)
    first = coordinator.request_program_preparation("W1", plan)
    assert coordinator.request_program_preparation("W1", plan) == first
    old.drain()

    new = connect(coordinator, plan, "W1", prepared=False, port=9002)
    new.drain()
    current = coordinator.request_program_preparation("W1", plan)
    new.drain()

    with pytest.raises(InvalidWorkerMessage):
        new.send(p.ProgramPrepared(
            "W1", plan.id, plan.program.id,
            message_id="late", correlation_id=first.message_id,
        ))
    assert plan.program.id not in coordinator.inspect_worker("W1").state.prepared_program_ids
    assert coordinator.inspect_pending_operations().active_count == 1
    assert current.message_id in coordinator.inspect_pending_operations().active_message_ids


def test_active_pending_count_tracks_distinct_program_operations_not_repeated_calls(coordinator):
    one = _plan("a=1\n")
    two = _plan("a=2\n")
    w = connect(coordinator, one, "W1", prepared=False)
    first = coordinator.request_program_preparation("W1", one)
    for _ in range(2_000):
        assert coordinator.request_program_preparation("W1", one) == first
    second = coordinator.request_program_preparation("W1", two)
    for _ in range(2_000):
        assert coordinator.request_program_preparation("W1", two) == second
    assert coordinator.inspect_pending_operations().active_count == 2
    assert w.drain() == (first, second)
    coordinator.validate_state()


def test_validate_state_rejects_duplicate_active_program_preparation_contracts(coordinator):
    plan = _plan()
    w = connect(coordinator, plan, "W1", prepared=False)
    first = coordinator.request_program_preparation("W1", plan)
    w.drain()
    duplicate = PendingProgramPreparation(
        "forged-duplicate", "W1", w.handle.generation, plan.id, plan.program
    )
    coordinator._pending_ops._active[duplicate.message_id] = duplicate
    with pytest.raises(AssertionError, match="duplicate active program-preparation contract"):
        coordinator.validate_state()
