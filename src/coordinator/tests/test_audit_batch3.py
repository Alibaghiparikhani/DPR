from __future__ import annotations

import pytest
import protocol as p

from coordinator import (
    Coordinator,
    InvalidDataLocation,
    OperationLimits,
    OperationalLimitExceeded,
)
from coordinator.tests.helpers import (
    accept,
    connect,
    dispatch_for,
    release_terminal_objects,
    seed_location,
    start,
    succeed,
)
from scheduler import DataForm, WorkerContext


def _immutable_output_ref(plan, run_id="r"):
    output = plan.tasks[0].outputs[0]
    return p.DataReference(
        plan.id,
        run_id,
        plan.immutable_representation_id(output.id),
        DataForm.IMMUTABLE_VALUE,
    )


def test_replacement_worker_generation_cannot_reassert_historical_producer_data(build_plan):
    _, plan = build_plan("a=1\nb=a+1\n")
    coordinator = Coordinator()
    old = connect(coordinator, plan, "W1", slots=1, port=9001)
    coordinator.submit(plan, run_id="r")
    coordinator.schedule("r")
    dispatch = dispatch_for(old)
    accept(old, dispatch)
    start(old, dispatch)
    succeed(old, plan, dispatch)

    data = _immutable_output_ref(plan)
    # A different certified replica keeps the live run recoverable when the
    # original producer session disappears.
    connect(coordinator, plan, "W2", slots=1, port=9002)
    seed_location(coordinator, plan, "r", "W2", data, 8)
    old.send(p.WorkerGoodbye("W1", "restart", message_id="bye-old"))

    replacement = connect(coordinator, plan, "W1", slots=1, port=9003)
    assert replacement.handle.generation > old.handle.generation
    with pytest.raises(InvalidDataLocation, match="provenance"):
        replacement.send(p.ObjectAvailable("W1", data, 8, message_id="stale-producer-claim"))
    assert {replica.worker_id for replica in coordinator.data_locations("r")[0].replicas} == {"W2"}


def test_same_generation_committed_producer_claim_remains_valid(build_plan):
    _, plan = build_plan("a=1\nb=a+1\n")
    coordinator = Coordinator()
    worker = connect(coordinator, plan, "W1", slots=1)
    coordinator.submit(plan, run_id="r")
    coordinator.schedule("r")
    dispatch = dispatch_for(worker)
    accept(worker, dispatch)
    start(worker, dispatch)
    succeed(worker, plan, dispatch)
    data = _immutable_output_ref(plan)
    # The success path already certified the replica; a same-generation
    # ObjectAvailable may refine/idempotently restate it.
    worker.send(p.ObjectAvailable("W1", data, 8, message_id="same-generation"))
    coordinator.validate_state()


def test_distinct_pending_operations_are_admission_bounded_and_release_capacity(build_plan):
    _, plan = build_plan("a=1\n")
    limits = OperationLimits(
        max_active_pending_global=2,
        max_active_pending_per_worker=2,
        max_active_pending_per_run=2,
    )
    coordinator = Coordinator(operation_limits=limits)
    worker = connect(coordinator, plan, "W1")
    coordinator.submit(plan, run_id="r")
    task_id = plan.tasks[0].task_id

    first = coordinator.request_context_preparation("r", "W1", "ctx-1", (task_id,))
    worker.drain()
    second = coordinator.request_context_preparation("r", "W1", "ctx-2", (task_id,))
    worker.drain()
    assert first is not None and second is not None
    assert coordinator.inspect_pending_operations().active_count == 2

    with pytest.raises(OperationalLimitExceeded, match="pending-operation"):
        coordinator.request_context_preparation("r", "W1", "ctx-3", (task_id,))
    assert coordinator.inspect_pending_operations().active_count == 2

    worker.send(p.ContextPrepared(
        plan_id=plan.id,
        run_id="r",
        context=WorkerContext("ctx-1", "W1", frozenset({task_id}), 1),
        message_id="prepared-ctx-1",
        correlation_id=first.message_id,
    ))
    third = coordinator.request_context_preparation("r", "W1", "ctx-3", (task_id,))
    assert third is not None
    assert coordinator.inspect_pending_operations().active_count == 2
    coordinator.validate_state()


def test_transfer_record_admission_is_bounded_even_when_outboxes_are_drained(build_plan):
    _, plan = build_plan("a=1\nb=a+1\n")
    limits = OperationLimits(max_transfer_records_global=2, max_transfer_records_per_run=2)
    coordinator = Coordinator(operation_limits=limits)
    source = connect(coordinator, plan, "W1", port=9001)
    destination = connect(coordinator, plan, "W2", port=9002)
    coordinator.submit(plan, run_id="r")
    data = _immutable_output_ref(plan)
    seed_location(coordinator, plan, "r", "W1", data, 8)

    for index in range(2):
        transfer = p.TransferIdentity(data, f"copy-{index}", f"attempt-{index}", "W1", "W2")
        coordinator.start_transfer(transfer, size_bytes=8)
        destination.drain()
    with pytest.raises(OperationalLimitExceeded, match="transfer-record"):
        coordinator.start_transfer(
            p.TransferIdentity(data, "copy-2", "attempt-2", "W1", "W2"),
            size_bytes=8,
        )
    coordinator.validate_state()


def test_run_retention_is_bounded_and_explicit_discard_frees_admission(build_plan):
    _, plan = build_plan("a=1\n")
    coordinator = Coordinator(operation_limits=OperationLimits(max_runs_in_memory=1))
    worker = connect(coordinator, plan, "W1")
    coordinator.submit(plan, run_id="old")
    coordinator.schedule("old")
    dispatch = dispatch_for(worker)
    accept(worker, dispatch)
    start(worker, dispatch)
    succeed(worker, plan, dispatch)

    with pytest.raises(OperationalLimitExceeded, match="run limit"):
        coordinator.submit(plan, run_id="new")
    assert release_terminal_objects(coordinator, worker) >= 1
    coordinator.discard_terminal_run("old")
    coordinator.submit(plan, run_id="new")
    coordinator.validate_state()


def test_known_worker_identity_history_has_explicit_admission_bound(build_plan):
    _, plan = build_plan("a=1\n")
    coordinator = Coordinator(operation_limits=OperationLimits(max_known_worker_identities=2))
    connect(coordinator, plan, "W1", port=9001)
    connect(coordinator, plan, "W2", port=9002)
    with pytest.raises(OperationalLimitExceeded, match="worker-identity"):
        connect(coordinator, plan, "W3", port=9003)
    coordinator.validate_state()


def test_scheduler_created_transfer_is_admission_checked_before_attempt_mutation(build_plan):
    from scheduler import TaskAffinity

    _, plan = build_plan("a=1\nb=a+1\n")
    root, consumer = plan.tasks
    limits = OperationLimits(max_transfer_records_global=1, max_transfer_records_per_run=1)
    coordinator = Coordinator(operation_limits=limits)
    w1 = connect(coordinator, plan, "W1", slots=1, port=9001)
    w2 = connect(coordinator, plan, "W2", slots=1, port=9002)
    coordinator.submit(
        plan,
        run_id="r",
        affinities=(
            TaskAffinity(root.task_id, required_worker="W1"),
            TaskAffinity(consumer.task_id, required_worker="W2"),
        ),
    )
    coordinator.schedule("r")
    dispatch = dispatch_for(w1)
    accept(w1, dispatch)
    start(w1, dispatch)
    succeed(w1, plan, dispatch)
    data = _immutable_output_ref(plan)

    # Consume the only retained transfer-record slot without completing it.
    coordinator.start_transfer(
        p.TransferIdentity(data, "manual", "manual-attempt", "W1", "W2"),
        size_bytes=8,
    )
    w2.drain()

    before = coordinator.inspect_run("r")
    with pytest.raises(OperationalLimitExceeded, match="transfer-record"):
        coordinator.schedule("r")
    after = coordinator.inspect_run("r")
    assert after == before
    assert coordinator.get_task("r", consumer.task_id).current_attempt_id is None
    coordinator.validate_state()


def test_worker_identity_limit_allows_reconnect_of_already_known_identity(build_plan):
    _, plan = build_plan("a=1\n")
    coordinator = Coordinator(operation_limits=OperationLimits(max_known_worker_identities=1))
    old = connect(coordinator, plan, "W1", port=9001)
    replacement = connect(coordinator, plan, "W1", port=9002)
    assert replacement.handle.generation == old.handle.generation + 1
    with pytest.raises(OperationalLimitExceeded):
        connect(coordinator, plan, "W2", port=9003)
    coordinator.validate_state()
