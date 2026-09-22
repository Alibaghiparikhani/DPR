from __future__ import annotations

from scheduler import DataForm, DataLocation, Replica, WorkerState, schedule


def test_immutable_alias_uses_materialized_representation_and_reports_logical_input(build_plan, worker, snapshot):
    _, plan = build_plan("a=1\nb=a\nc=b+1")
    producer, bind, consumer = plan.tasks
    alias = consumer.inputs[0]
    base_id = plan.immutable_representation_id(alias.id)
    assert alias.value.origin == "alias"
    assert alias.value.object_id == producer.outputs[0].value.object_id
    assert alias.value.producer == bind.task_id
    data = DataLocation(base_id, DataForm.IMMUTABLE_VALUE, (Replica("W1"),), 8)
    state = snapshot(plan, (consumer.task_id,), (worker(plan, "W1"),), data=(data,))
    placement = schedule(plan, state).placements[0]
    assert placement.preference.locality.local_input_ids == (alias.id,)


def test_remote_immutable_alias_uses_backing_representation_for_transfer_planning(build_plan, worker, snapshot):
    _, plan = build_plan("a=1\nb=a\nc=b+1")
    _, _, consumer = plan.tasks
    alias = consumer.inputs[0]
    base_id = plan.immutable_representation_id(alias.id)
    data = DataLocation(base_id, DataForm.IMMUTABLE_VALUE, (Replica("W1"),), 8)
    full = worker(plan, "W1", total_slots=1, running_slots=1)
    remote = worker(plan, "W2", total_slots=1)
    state = snapshot(plan, (consumer.task_id,), (full, remote), data=(data,))
    placement = schedule(plan, state).placements[0]
    assert placement.worker_id == "W2"
    assert placement.preference.locality.remote_input_ids == (alias.id,)
