"""Result contracts describe attempts and logical outputs, never scheduling/commit policy."""
from dataclasses import replace

import pytest

from execution import (
    AttemptIdentity, ExecutionValidationError, FailureInfo, FailureKind,
    TaskFailure, TaskSuccess, ValueKind,
)


def attempt(plan, task):
    return AttemptIdentity(plan.id, "run-1", task.task_id, "attempt-1")


def test_success_matches_exact_outputs_but_does_not_advance_readiness(lowered):
    """Reporting computation success leaves the original DAG tracker unchanged until integration commits it."""
    dag, plan = lowered("x=1\ny=x+1")
    task = plan.tasks[0]
    identity = attempt(plan, task)
    result = TaskSuccess(identity, task.reported_output_ids)
    tracker = dag.new_readiness()
    plan.validate_result(result, expected_attempt=identity)
    assert tracker.completed == frozenset() and tracker.ready == (task.task_id,)
    assert dag._default_readiness is None


def test_native_success_reports_view_identity_without_claiming_boundness(lowered):
    """Conditional output IDs denote native views; success need not prefetch absent x or capture arbitrary objects."""
    _, plan = lowered("if flag:\n    x=1")
    task, = plan.tasks
    x = next(v for v in task.outputs if v.value.name == "x")
    assert x.kind == ValueKind.NATIVE_REFERENCE and x.value.may_be_unbound
    identity = attempt(plan, task)
    plan.validate_result(TaskSuccess(identity, task.reported_output_ids), expected_attempt=identity)


def test_aliases_and_discarded_results_are_reported_by_their_owning_steps(lowered):
    """The producer reports a, the materialized alias reports b, and append reports only state."""
    _, plan = lowered("a=[1]\nb=a\na.append(2)")
    create, bind, append = plan.tasks
    assert create.reported_output_ids == (create.outputs[0].id,)
    assert bind.reported_output_ids == (bind.outputs[0].id,)
    token = next(v.id for v in append.outputs if v.is_state_token)
    assert append.reported_output_ids == (token,)
    for task in plan.tasks:
        identity = attempt(plan, task)
        plan.validate_result(TaskSuccess(identity, task.reported_output_ids), expected_attempt=identity)


def test_empty_computation_outputs_are_valid(lowered):
    """A pure discarded expression succeeds with no retained output object or fabricated @discard lookup."""
    _, plan = lowered("1+2")
    task, = plan.tasks
    assert task.outputs[0].kind == ValueKind.DISCARDED_RESULT
    identity = attempt(plan, task)
    plan.validate_result(TaskSuccess(identity, ()), expected_attempt=identity)


def test_failure_never_unlocks_may_raise_descendants(lowered):
    """Division failure is structured data only; branches remain blocked, with no successful outputs or rollback claim."""
    dag, plan = lowered("n=0\nq=1//n\na=1\nb=2")
    n, q, a, b = plan.tasks
    tracker = dag.new_readiness()
    tracker.mark_completed(n.task_id)
    identity = attempt(plan, q)
    failure = TaskFailure(identity, FailureInfo(FailureKind.PYTHON_EXCEPTION, "integer division by zero",
                                               exception_type="builtins.ZeroDivisionError", traceback_text="fixture traceback"))
    plan.validate_result(failure, expected_attempt=identity)
    assert tracker.ready == (q.task_id,) and tracker.completed == {n.task_id}
    assert a.task_id not in tracker.ready and b.task_id not in tracker.ready
    assert not hasattr(failure, "output_ids")


@pytest.mark.parametrize("kind", [FailureKind.INPUT_UNAVAILABLE, FailureKind.ENVIRONMENT_MISMATCH, FailureKind.EXECUTION_ERROR])
def test_non_python_failure_contract(lowered, kind):
    """Preparation/adapter failures have typed categories without fabricating a Python exception or choosing a retry."""
    _, plan = lowered("a=1")
    identity = attempt(plan, plan.tasks[0])
    plan.validate_result(TaskFailure(identity, FailureInfo(kind, "unavailable")), expected_attempt=identity)


@pytest.mark.parametrize("field,new", [("run_id", "run-2"), ("task_id", "other"), ("attempt_id", "attempt-2"), ("plan_id", "0" * 64)])
def test_result_correlation_rejects_wrong_identity(lowered, field, new):
    """An otherwise plausible result from another run/task/attempt/plan cannot satisfy this attempt."""
    _, plan = lowered("a=1")
    task, = plan.tasks
    identity = attempt(plan, task)
    result = TaskSuccess(replace(identity, **{field: new}), task.reported_output_ids)
    with pytest.raises(ExecutionValidationError, match="attempt mismatch"):
        plan.validate_result(result, expected_attempt=identity)


@pytest.mark.parametrize("field,new,match", [("plan_id", "0" * 64, "plan mismatch"), ("task_id", "missing", "Unknown result task")])
def test_even_matching_expectation_must_belong_to_plan(lowered, field, new, match):
    """Matching caller/result identities are still checked against this plan and real task index."""
    _, plan = lowered("a=1")
    identity = replace(attempt(plan, plan.tasks[0]), **{field: new})
    with pytest.raises(ExecutionValidationError, match=match):
        plan.validate_result(TaskSuccess(identity, ()), expected_attempt=identity)


@pytest.mark.parametrize("outputs", [(), ("missing",), ("V000002",), ("V000001", "V000002")])
def test_missing_foreign_and_extra_outputs_rejected(lowered, outputs):
    """A successful task reports exactly its required logical outputs, not another producer's IDs."""
    _, plan = lowered("a=1\nb=2")
    task = plan.tasks[0]
    identity = attempt(plan, task)
    with pytest.raises(ExecutionValidationError, match="outputs differ"):
        plan.validate_result(TaskSuccess(identity, outputs), expected_attempt=identity)


def test_result_records_reject_duplicate_outputs_and_bad_exception_data(lowered):
    """Malformed reports fail construction; actual exceptions/frames are not accepted as descriptive fields."""
    _, plan = lowered("a=1")
    identity = attempt(plan, plan.tasks[0])
    with pytest.raises(ExecutionValidationError, match="Duplicate"):
        TaskSuccess(identity, ("V000001", "V000001"))
    for args in [("made-up", "message"), (FailureKind.PYTHON_EXCEPTION, "message"),
                 (FailureKind.EXECUTION_ERROR, ValueError("not text"))]:
        with pytest.raises(ExecutionValidationError):
            FailureInfo(*args)
    with pytest.raises(ExecutionValidationError, match="Traceback"):
        FailureInfo(FailureKind.EXECUTION_ERROR, "message", traceback_text=object())
    with pytest.raises(ExecutionValidationError, match="FailureInfo"):
        TaskFailure(identity, ValueError("not a contract"))


def test_attempt_ids_cannot_be_empty(lowered):
    """Caller-provided run and attempt IDs must be explicit and nonempty."""
    _, plan = lowered("a=1")
    identity = attempt(plan, plan.tasks[0])
    for field in ("run_id", "task_id", "attempt_id"):
        with pytest.raises(ExecutionValidationError):
            replace(identity, **{field: ""})


def test_result_requires_typed_attempt_and_output_ids(lowered):
    """Loose attempt dictionaries and blank/non-string output IDs are rejected before correlation."""
    _, plan = lowered("a=1")
    identity = attempt(plan, plan.tasks[0])
    with pytest.raises(ExecutionValidationError, match="AttemptIdentity"):
        TaskSuccess({"task_id": identity.task_id}, ())
    with pytest.raises(ExecutionValidationError, match="AttemptIdentity"):
        TaskFailure("attempt", FailureInfo(FailureKind.EXECUTION_ERROR, "failed"))
    for outputs in (("",), (None,), (1,)):
        with pytest.raises(ExecutionValidationError, match="output_id"):
            TaskSuccess(identity, outputs)
    with pytest.raises(ExecutionValidationError, match="Expected AttemptIdentity"):
        plan.validate_result(TaskSuccess(identity, ()), expected_attempt={})
