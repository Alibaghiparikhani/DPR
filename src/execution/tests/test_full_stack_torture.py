"""High-stress integration checks across dag_runtime -> execution lowering."""

from __future__ import annotations

import random

import pytest

from dag_runtime.dag_engine import analyze_source
from dag_runtime.dag_model import Certainty, EffectKind
from execution import (
    AttemptIdentity,
    ExecutionMode,
    ExecutionValidationError,
    ObjectAccess,
    TaskSuccess,
    ValueKind,
    lower_dag,
)


SOURCE = """
def scale(values): return [x*2 for x in values]
def shift(values): return [x+1 for x in values]
def score(x): return x+1
old_score=score
def score(x): return x*2

left=[1,2,3]
left_alias=left
right=[10,20,30]

left_before=sum(left)
right_before=sum(right)

left.append(4)
right.append(40)

left2=scale(left)
right2=shift(right)

l0=left2[0]
l_last=left2[-1]
r0=right2[0]
r_last=right2[-1]

left_score=l0+l_last
right_score=r0+r_last
joined=left_score+right_score

old_v=old_score(joined)
new_v=score(joined)
versions_joined=old_v+new_v

divisor=2
guarded=100//divisor

post_a=versions_joined+guarded
post_b=versions_joined*2
post_join=post_a+post_b

getattr(left, "append")

1+2
3*4

namespace=globals()

tail_a=10
tail_b=20
tail_result=tail_a+tail_b
""".lstrip()


def manifest(plan, source):
    return next(
        m for m in plan.tasks
        if m.task.source.strip() == source
    )


def containing_manifest(plan, text):
    return next(
        m for m in plan.tasks
        if text in m.task.source
    )


def test_full_stack_torture_semantics_and_lowering():
    dag = analyze_source(
        SOURCE,
        filename="full_stack_torture.py",
    )

    before = dag.to_dict()

    plan = lower_dag(
        dag,
        environment_id="cpython-test-env-v1",
        package_id="project-v1",
    )

    # --------------------------------------------------
    # Basic DAG -> execution preservation
    # --------------------------------------------------

    dag.validate()
    plan.validate()
    plan.validate_against(dag)

    # Lowering must not modify the original DAG.
    assert dag.to_dict() == before

    assert tuple(m.task for m in plan.tasks) == tuple(
        dag.tasks.values()
    )

    assert plan.values == tuple(dag.values.values())
    assert plan.edges == tuple(dag.edges.values())
    assert plan.bindings == dag.bindings

    # --------------------------------------------------
    # Make sure this fixture actually stresses everything
    # --------------------------------------------------

    assert len(dag.tasks) == 29

    assert {
        m.mode for m in plan.tasks
    } == {
        ExecutionMode.ISOLATED_CANDIDATE,
        ExecutionMode.SHARED_CONTEXT,
        ExecutionMode.NATIVE_REGION,
    }

    metrics = dag.metrics()

    assert metrics["certain_tasks"] == 26
    assert metrics["conservative_tasks"] == 3

    assert metrics["effects"] == {
        "pure": 25,
        "object_local": 2,
        "namespace": 1,
        "namespace_escape": 1,
    }

    assert metrics["whole_tail_collapses"] == 1

    # --------------------------------------------------
    # Two independent mutable branches
    # --------------------------------------------------

    left = manifest(plan, "left=[1,2,3]")
    right = manifest(plan, "right=[10,20,30]")

    left_read = manifest(
        plan,
        "left_before=sum(left)",
    )

    right_read = manifest(
        plan,
        "right_before=sum(right)",
    )

    left_append = manifest(
        plan,
        "left.append(4)",
    )

    right_append = manifest(
        plan,
        "right.append(40)",
    )

    # Completely unrelated roots.
    assert not dag.dependency_path(
        left.task_id,
        right.task_id,
    )

    # The mutations of different objects must not
    # accidentally serialize each other.
    assert not dag.dependency_path(
        left_append.task_id,
        right_append.task_id,
    )

    left_alias = manifest(plan, "left_alias=left")
    assert left_append.dependencies == {
        left.task_id,
        left_alias.task_id,
        left_read.task_id,
    }

    assert right_append.dependencies == {
        right.task_id,
        right_read.task_id,
    }

    assert (
        left_append.task.effect
        == right_append.task.effect
        == EffectKind.OBJECT_LOCAL
    )

    assert (
        left_append.mode
        == right_append.mode
        == ExecutionMode.SHARED_CONTEXT
    )

    assert (
        left_append.objects[0].access
        == ObjectAccess.MUTATE
    )

    assert (
        right_append.objects[0].access
        == ObjectAccess.MUTATE
    )

    # They really are different object groups.
    assert (
        left_append.objects[0].object_id
        != right_append.objects[0].object_id
    )

    # --------------------------------------------------
    # Reads after mutation require exact object state
    # --------------------------------------------------

    scale = manifest(
        plan,
        "left2=scale(left)",
    )

    shift = manifest(
        plan,
        "right2=shift(right)",
    )

    assert (
        scale.mode
        == shift.mode
        == ExecutionMode.ISOLATED_CANDIDATE
    )

    assert (
        scale.state_inputs[0].kind
        == ValueKind.OBJECT_STATE
    )

    assert (
        shift.state_inputs[0].kind
        == ValueKind.OBJECT_STATE
    )

    assert (
        scale.objects[0].state_inputs
        == left_append.objects[0].state_outputs
    )

    assert (
        shift.objects[0].state_inputs
        == right_append.objects[0].state_outputs
    )

    assert scale.code.definition_ids == (
        "D000001",
    )

    assert shift.code.definition_ids == (
        "D000002",
    )

    # --------------------------------------------------
    # Function redefinition/version identity
    # --------------------------------------------------

    old_call = manifest(
        plan,
        "old_v=old_score(joined)",
    )

    new_call = manifest(
        plan,
        "new_v=score(joined)",
    )

    assert old_call.code.definition_ids == (
        "D000003",
    )

    assert new_call.code.definition_ids == (
        "D000004",
    )

    assert (
        old_call.code.definition_ids
        != new_call.code.definition_ids
    )

    # Both consume joined, but neither should depend
    # on the other.
    assert not dag.dependency_path(
        old_call.task_id,
        new_call.task_id,
    )

    assert not dag.dependency_path(
        new_call.task_id,
        old_call.task_id,
    )

    # --------------------------------------------------
    # Exception-only completion fence
    # --------------------------------------------------

    division = manifest(
        plan,
        "guarded=100//divisor",
    )

    post_a = manifest(
        plan,
        "post_a=versions_joined+guarded",
    )

    post_b = manifest(
        plan,
        "post_b=versions_joined*2",
    )

    assert (
        division.task.certainty
        == Certainty.CONSERVATIVE
    )

    assert (
        division.task.effect
        == EffectKind.PURE
    )

    assert division.task.characteristics.may_raise

    assert (
        division.mode
        == ExecutionMode.SHARED_CONTEXT
    )

    completion, = (
        value
        for value in division.outputs
        if value.kind == ValueKind.COMPLETION_STATE
    )

    assert division.task_id in post_a.dependencies
    assert division.task_id in post_b.dependencies

    # post_b does not consume guarded directly, so its
    # ordering must come from the completion fence.
    assert completion in post_b.state_inputs

    # Recovery after the fence must restore parallelism.
    assert not dag.dependency_path(
        post_a.task_id,
        post_b.task_id,
    )

    assert not dag.dependency_path(
        post_b.task_id,
        post_a.task_id,
    )

    # --------------------------------------------------
    # Recoverable namespace barrier
    # --------------------------------------------------

    reflection = manifest(
        plan,
        'getattr(left, "append")',
    )

    literal_a = manifest(
        plan,
        "1+2",
    )

    literal_b = manifest(
        plan,
        "3*4",
    )

    assert (
        reflection.task.effect
        == EffectKind.NAMESPACE
    )

    assert (
        reflection.task.certainty
        == Certainty.CONSERVATIVE
    )

    assert (
        reflection.mode
        == ExecutionMode.SHARED_CONTEXT
    )

    # Analysis recovers after the barrier.
    assert (
        literal_a.mode
        == literal_b.mode
        == ExecutionMode.ISOLATED_CANDIDATE
    )

    assert literal_a.dependencies == {
        reflection.task_id
    }

    assert literal_b.dependencies == {
        reflection.task_id
    }

    assert (
        literal_a.state_inputs
        == literal_b.state_inputs
    )

    assert (
        literal_a.state_inputs[0].kind
        == ValueKind.NAMESPACE_STATE
    )

    # They are siblings again, not falsely serialized.
    assert not dag.dependency_path(
        literal_a.task_id,
        literal_b.task_id,
    )

    assert not dag.dependency_path(
        literal_b.task_id,
        literal_a.task_id,
    )

    # --------------------------------------------------
    # True namespace escape
    # --------------------------------------------------

    tail = containing_manifest(
        plan,
        "namespace=globals()",
    )

    assert tail is plan.tasks[-1]

    assert (
        tail.mode
        == ExecutionMode.NATIVE_REGION
    )

    assert (
        tail.task.effect
        == EffectKind.ESCAPE
    )

    assert tail.task.region_scope == "tail"

    assert tail.task.statement_count == 4

    assert (
        "tail_result=tail_a+tail_b"
        in tail.task.source
    )

    assert all(
        task.task_id in tail.dependencies
        for task in (
            reflection,
            literal_a,
            literal_b,
        )
    )

    # --------------------------------------------------
    # Alias + binding semantics
    # --------------------------------------------------

    alias_event = next(
        event
        for event in plan.bindings
        if event.name == "left_alias"
    )

    assert alias_event.kind == "alias"

    alias_value = plan.value_index[alias_event.value_id]
    assert alias_value.origin == "alias"
    assert alias_value.object_id == left.outputs[0].value.object_id
    assert alias_value.producer == left_alias.task_id

    # Same function name, two different definitions.
    assert [
        definition.id
        for definition in plan.definitions
        if definition.name == "score"
    ] == [
        "D000003",
        "D000004",
    ]


def test_full_stack_torture_random_readiness_matches_execution_plan():
    """
    Explore thousands of legal schedules.

    Every time the DAG says a task is READY, the execution
    manifest must agree that all of its producers and
    prerequisite tasks have completed.
    """

    dag = analyze_source(
        SOURCE,
        filename="full_stack_torture.py",
    )

    plan = lower_dag(
        dag,
        environment_id="cpython-test-env-v1",
        package_id="project-v1",
    )

    rng = random.Random(0xE7EC)

    seen_orders = set()

    for _ in range(2000):
        readiness = dag.new_readiness()

        completed = set()
        order = []

        while readiness.ready:
            task_id = rng.choice(
                list(readiness.ready)
            )

            task_manifest = (
                plan.task_index[task_id]
            )

            # Scheduler-style invariant:
            # no task can appear ready while a declared
            # dependency is unfinished.
            assert (
                task_manifest.dependencies
                <= completed
            )

            assert {
                edge.source
                for edge
                in task_manifest.prerequisites
            } <= completed

            # Every produced input must already have its
            # producer completed.
            for requirement in task_manifest.inputs:
                producer = (
                    requirement.value.producer
                )

                if (
                    producer is not None
                    and producer != task_id
                ):
                    assert producer in completed

            readiness.mark_completed(task_id)

            completed.add(task_id)
            order.append(task_id)

        # Every legal run must finish the entire graph.
        assert completed == set(dag.tasks)
        assert not readiness.ready

        seen_orders.add(tuple(order))

    # Make sure we did not accidentally test the same
    # schedule 2,000 times.
    assert len(seen_orders) > 250


def test_full_stack_torture_identity_and_result_contract():
    dag = analyze_source(
        SOURCE,
        filename="full_stack_torture.py",
    )

    plan_a = lower_dag(
        dag,
        environment_id="cpython-test-env-v1",
        package_id="project-v1",
    )

    plan_b = lower_dag(
        dag,
        environment_id="cpython-test-env-v1",
        package_id="project-v1",
    )

    plan_other_environment = lower_dag(
        dag,
        environment_id="cpython-test-env-v2",
        package_id="project-v1",
    )

    # --------------------------------------------------
    # Deterministic lowering
    # --------------------------------------------------

    assert plan_a.id == plan_b.id
    assert plan_a.program == plan_b.program
    assert plan_a.tasks == plan_b.tasks
    assert (
        plan_a.dag_digest
        == plan_b.dag_digest
    )

    # Changing the environment changes the execution
    # contract identity, but not the DAG itself.
    assert (
        plan_other_environment.id
        != plan_a.id
    )

    assert (
        plan_other_environment.program.id
        != plan_a.program.id
    )

    assert (
        plan_other_environment.dag_digest
        == plan_a.dag_digest
    )

    # --------------------------------------------------
    # Result / attempt correlation
    # --------------------------------------------------

    task = manifest(
        plan_a,
        "versions_joined=old_v+new_v",
    )

    attempt = AttemptIdentity(
        plan_a.id,
        "run-42",
        task.task_id,
        "attempt-1",
    )

    success = TaskSuccess(
        attempt,
        task.reported_output_ids,
    )

    # Correct result must validate.
    plan_a.validate_result(
        success,
        expected_attempt=attempt,
    )

    # A result from another attempt must be rejected.
    wrong_attempt = AttemptIdentity(
        plan_a.id,
        "run-42",
        task.task_id,
        "attempt-2",
    )

    with pytest.raises(
        ExecutionValidationError
    ):
        plan_a.validate_result(
            success,
            expected_attempt=wrong_attempt,
        )

    # Claiming success without the task's required
    # outputs must also be rejected.
    with pytest.raises(
        ExecutionValidationError
    ):
        plan_a.validate_result(
            TaskSuccess(attempt, ()),
            expected_attempt=attempt,
        )
