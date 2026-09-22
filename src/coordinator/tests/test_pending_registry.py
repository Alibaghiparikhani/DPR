from __future__ import annotations

import pytest

from coordinator.errors import CoordinatorError, InvalidWorkerMessage
from coordinator.model import (
    ContextPreparationContract,
    PendingContextPreparation,
    PendingProgramPreparation,
    ProgramPreparationContract,
    SessionHandle,
)
from coordinator.pending import PendingOperationRegistry


def _registry(*, limit: int = 3, base: str = "message") -> PendingOperationRegistry:
    return PendingOperationRegistry(
        history_limit=lambda: limit,
        new_base_message_id=lambda: base,
    )


def _program_request(plan, message_id: str = "p1", generation: int = 1):
    return PendingProgramPreparation(message_id, "W1", generation, plan.id, plan.program)


def _context_request(plan, message_id: str = "c1", *, run_id: str = "r",
                     context_id: str = "ctx", generation: int = 1):
    task_id = plan.tasks[0].task_id
    return PendingContextPreparation(
        message_id, "W1", generation, plan.id, run_id, plan.program.id,
        context_id, (task_id,),
    )


def test_registry_generates_aba_safe_message_ids_even_with_repeated_base():
    registry = _registry(base="same")
    assert [registry.new_message_id() for _ in range(3)] == [
        "same~p1", "same~p2", "same~p3",
    ]


def test_program_contract_lookup_and_duplicate_record_are_exact(build_plan):
    _, plan = build_plan("a=1\n")
    registry = _registry()
    request = _program_request(plan)
    registry.record(request)
    assert registry.find_program(request.contract) == request
    assert registry.find_program(ProgramPreparationContract(
        "W1", 2, plan.id, plan.program,
    )) is None
    with pytest.raises(CoordinatorError, match="duplicate active program-preparation contract"):
        registry.record(_program_request(plan, "p2"))


def test_context_identity_allows_only_one_active_operation(build_plan):
    _, plan = build_plan("a=1\n")
    registry = _registry()
    request = _context_request(plan)
    registry.record(request)
    assert registry.find_context("r", "ctx") == request
    conflicting = PendingContextPreparation(
        "c2", "W1", 1, plan.id, "r", plan.program.id, "ctx", (),
    )
    with pytest.raises(CoordinatorError, match="duplicate active context-preparation identity"):
        registry.record(conflicting)


def test_resolution_is_bound_to_exact_worker_generation(build_plan):
    _, plan = build_plan("a=1\n")
    registry = _registry()
    request = _program_request(plan)
    registry.record(request)
    current = SessionHandle("W1", 1, "session-1")
    resolved = registry.resolve(current, request.message_id, PendingProgramPreparation)
    assert resolved.request == request and resolved.active
    with pytest.raises(InvalidWorkerMessage, match="different worker session"):
        registry.resolve(SessionHandle("W1", 2, "session-2"), request.message_id,
                         PendingProgramPreparation)


def test_complete_moves_request_to_bounded_tombstone_history(build_plan):
    _, plan = build_plan("a=1\n")
    limit = 2
    registry = PendingOperationRegistry(
        history_limit=lambda: limit,
        new_base_message_id=lambda: "m",
    )
    requests = [_program_request(plan, f"p{i}") for i in range(3)]
    for request in requests:
        registry.record(request)
        assert registry.complete(request.message_id) == request
    view = registry.view()
    assert view.active_count == 0
    assert view.retired_message_ids == ("p1", "p2")
    with pytest.raises(InvalidWorkerMessage, match="unknown PendingProgramPreparation correlation"):
        registry.resolve(SessionHandle("W1", 1, "s"), "p0", PendingProgramPreparation)
    assert not registry.resolve(
        SessionHandle("W1", 1, "s"), "p2", PendingProgramPreparation,
    ).active


def test_session_invalidation_retires_only_that_generation(build_plan):
    _, plan = build_plan("a=1\n")
    registry = _registry(limit=5)
    old = _program_request(plan, "old", generation=1)
    current = _program_request(plan, "current", generation=2)
    registry.record(old)
    registry.record(current)
    retired = registry.invalidate_session(SessionHandle("W1", 1, "old-session"))
    assert retired == (old,)
    assert registry.view().active_message_ids == ("current",)
    assert registry.view().retired_message_ids == ("old",)


def test_run_invalidation_retires_context_preparation_not_program(build_plan):
    _, plan = build_plan("a=1\n")
    registry = _registry(limit=5)
    program = _program_request(plan)
    context = _context_request(plan)
    registry.record(program)
    registry.record(context)
    assert registry.invalidate_run("r") == (context,)
    assert registry.view().active_message_ids == (program.message_id,)


def test_validate_detects_stale_generation_without_coordinator_state(build_plan):
    _, plan = build_plan("a=1\n")
    registry = _registry()
    registry.record(_program_request(plan, generation=1))
    with pytest.raises(AssertionError, match="stale worker session"):
        registry.validate({"W1": 2})


def test_contract_objects_make_semantic_equality_explicit(build_plan):
    _, plan = build_plan("a=1\n")
    program = _program_request(plan)
    context = _context_request(plan)
    assert program.contract == ProgramPreparationContract("W1", 1, plan.id, plan.program)
    assert context.contract == ContextPreparationContract(
        "W1", 1, plan.id, "r", plan.program.id, "ctx", (plan.tasks[0].task_id,),
    )


def test_registry_view_is_immutable_and_does_not_expose_internal_mappings(build_plan):
    from dataclasses import FrozenInstanceError

    _, plan = build_plan("a=1\n")
    registry = _registry()
    registry.record(_program_request(plan))
    view = registry.view()
    with pytest.raises(FrozenInstanceError):
        view.active_count = 99
    assert isinstance(view.active_message_ids, tuple)
    assert registry.view().active_count == 1
