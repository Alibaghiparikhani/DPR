from __future__ import annotations

import protocol as p

from coordinator import CoordinatorFailureCode, RunStatus
from coordinator.tests.helpers import accept, connect, dispatch_for, start, succeed
from scheduler import TaskAffinity, WorkerContext


def _binding_tasks(plan):
    return tuple(t for t in plan.tasks if t.task.kind == "binding")


def _submit_with_binding_context(coordinator, plan, *, run_id="r", worker_id="W1", extra_affinities=()):
    bindings = _binding_tasks(plan)
    affinities = tuple(
        TaskAffinity(t.task_id, required_worker=worker_id, context_id="alias-ctx")
        for t in bindings
    ) + tuple(extra_affinities)
    contexts = ()
    if bindings:
        contexts = (WorkerContext(
            "alias-ctx", worker_id, frozenset(t.task_id for t in bindings), 1
        ),)
    coordinator.submit(plan, run_id=run_id, affinities=affinities, contexts=contexts)


def _commit_next(coordinator, worker, plan, run_id="r"):
    result = coordinator.schedule(run_id)
    assert len(result.dispatched) == 1
    dispatch = dispatch_for(worker)
    accept(worker, dispatch)
    start(worker, dispatch)
    succeed(worker, plan, dispatch)
    return dispatch


def test_immutable_alias_consumer_schedules_from_materialized_alias_representation(coordinator, build_plan):
    _, plan = build_plan("a=1\nb=a\nc=b+1")
    producer, binding, consumer = plan.tasks
    worker = connect(coordinator, plan, "W1", slots=1)
    _submit_with_binding_context(coordinator, plan)

    assert _commit_next(coordinator, worker, plan).attempt.task_id == producer.task_id
    assert _commit_next(coordinator, worker, plan).attempt.task_id == binding.task_id

    result = coordinator.schedule("r")
    assert len(result.dispatched) == 1
    consumer_dispatch = dispatch_for(worker)
    assert consumer_dispatch.attempt.task_id == consumer.task_id
    assert plan.immutable_representation_id(consumer.inputs[0].id) == binding.reported_output_ids[0]
    coordinator.validate_state()


def test_remote_immutable_alias_consumer_prepares_transfer_of_materialized_value(coordinator, build_plan):
    _, plan = build_plan("a=1\nb=a\nc=b+1")
    producer, binding, consumer = plan.tasks
    w1 = connect(coordinator, plan, "W1", slots=1, port=9001)
    w2 = connect(coordinator, plan, "W2", slots=1, port=9002)
    _submit_with_binding_context(
        coordinator, plan, extra_affinities=(TaskAffinity(consumer.task_id, required_worker="W2"),),
    )

    assert _commit_next(coordinator, w1, plan).attempt.task_id == producer.task_id
    assert _commit_next(coordinator, w1, plan).attempt.task_id == binding.task_id

    result = coordinator.schedule("r")
    assert len(result.dispatched) == 1
    outbound = w2.drain()
    assert not [m for m in outbound if isinstance(m, p.TaskDispatch)]
    prepare = next(m for m in outbound if isinstance(m, p.PrepareReceive))
    alias_id = consumer.inputs[0].id
    assert prepare.transfer.data.value_id == plan.immutable_representation_id(alias_id)
    assert prepare.transfer.data.value_id == binding.reported_output_ids[0]
    coordinator.validate_state()


def test_loss_of_materialized_alias_replica_fails_alias_consumer(coordinator, build_plan):
    _, plan = build_plan("a=1\nb=a\nc=b+1")
    producer, binding, _consumer = plan.tasks
    w1 = connect(coordinator, plan, "W1", slots=1, port=9001)
    connect(coordinator, plan, "W2", slots=1, port=9002)
    _submit_with_binding_context(coordinator, plan)
    assert _commit_next(coordinator, w1, plan).attempt.task_id == producer.task_id
    assert _commit_next(coordinator, w1, plan).attempt.task_id == binding.task_id

    w1.send(p.WorkerGoodbye("W1", "producer lost", message_id="bye-W1"))
    snap = coordinator.inspect_run("r")
    assert snap.status == RunStatus.FAILED
    assert snap.failure is not None
    assert snap.failure.code == CoordinatorFailureCode.DATA_LOST
    coordinator.validate_state()


def test_alias_binding_after_may_raise_statement_keeps_completion_dependency(build_plan):
    dag, plan = build_plan("x=1\nq=1//0\nb=x\nc=b+1")
    producer, may_raise, binding, consumer = plan.tasks
    assert plan.immutable_representation_id(consumer.inputs[0].id) == binding.reported_output_ids[0]
    assert may_raise.task_id in binding.dependencies
    assert binding.task_id in consumer.dependencies
    assert dag.initial_ready_tasks() == (producer.task_id,)


def test_immutable_alias_chain_materializes_each_binding_before_consumer(coordinator, build_plan):
    _, plan = build_plan("a=1\nb=a\nc=b\nd=c+1")
    producer, binding_b, binding_c, consumer = plan.tasks
    alias = consumer.inputs[0]
    assert plan.immutable_representation_id(alias.id) == binding_c.reported_output_ids[0]

    worker = connect(coordinator, plan, "W1", slots=1)
    _submit_with_binding_context(coordinator, plan)
    for expected in (producer, binding_b, binding_c):
        assert _commit_next(coordinator, worker, plan).attempt.task_id == expected.task_id
    result = coordinator.schedule("r")
    assert len(result.dispatched) == 1
    assert dispatch_for(worker).attempt.task_id == consumer.task_id
    coordinator.validate_state()
