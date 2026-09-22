from dataclasses import replace

import pytest
import protocol as p

from coordinator import (
    CoordinatorFailureCode,
    EventDisposition,
    InvalidDataLocation,
    InvalidWorkerMessage,
    PlacementRejected,
    RunStatus,
    TaskStatus,
)
from coordinator.tests.helpers import accept, connect, dispatch_for, fail, start, succeed, worker_state
from execution import FailureInfo, FailureKind
from scheduler import DataForm, SchedulingDecision, TaskAffinity, WorkerContext


def _dispatches(fake):
    return [m for m in fake.drain() if isinstance(m, p.TaskDispatch)]


def test_context_capacity_counts_active_users_across_schedule_rounds(coordinator, build_plan):
    _, plan = build_plan("a=[1]\nb=[2]\na.append(3)\nb.append(4)\n")
    roots = [t for t in plan.tasks if t.mode.value == "isolated_candidate"]
    shared = [t for t in plan.tasks if t.mode.value == "shared_context"]
    assert len(roots) == 2 and len(shared) == 2
    w = connect(coordinator, plan, "W1", slots=4)
    affinities = tuple(TaskAffinity(t.task_id, required_worker="W1", context_id="ctx") for t in shared)
    context = WorkerContext("ctx", "W1", frozenset(t.task_id for t in shared), 1)
    coordinator.submit(plan, run_id="r", affinities=affinities, contexts=(context,))

    coordinator.schedule("r")
    for d in _dispatches(w):
        accept(w, d); start(w, d); succeed(w, plan, d)

    first = coordinator.schedule("r")
    assert len(first.dispatched) == 1
    d1 = dispatch_for(w)
    accept(w, d1); start(w, d1)

    second = coordinator.schedule("r")
    assert second.dispatched == ()
    assert sum(1 for t in shared if coordinator.get_task("r", t.task_id).status == TaskStatus.RUNNING) == 1
    assert sum(1 for t in shared if coordinator.get_task("r", t.task_id).status == TaskStatus.READY) == 1
    coordinator.validate_state()


def test_failed_run_keeps_capacity_for_unconfirmed_sibling_execution(coordinator, build_plan):
    _, plan = build_plan("a=1\nb=2\n")
    w = connect(coordinator, plan, "W1", slots=2)
    coordinator.submit(plan, run_id="r1")
    coordinator.schedule("r1")
    dispatches = _dispatches(w)
    assert len(dispatches) == 2
    for d in dispatches:
        accept(w, d); start(w, d)

    fail(w, dispatches[0], FailureInfo(FailureKind.PYTHON_EXCEPTION, "boom", exception_type="RuntimeError"))
    assert coordinator.inspect_run("r1").status == RunStatus.FAILED
    # The failed attempt is physically finished, but its sibling has not yet
    # confirmed termination and must still occupy one slot.
    assert coordinator.inspect_worker("W1").state.free_slots == 1

    plan2 = plan
    coordinator.submit(plan2, run_id="r2")
    result = coordinator.schedule("r2")
    assert len(result.dispatched) == 1
    coordinator.validate_state()


def test_lost_required_worker_affinity_fails_cleanly_instead_of_invalid_snapshot(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    w1 = connect(coordinator, plan, "W1", slots=1, port=9001)
    connect(coordinator, plan, "W2", slots=1, port=9002)
    task_id = plan.tasks[0].task_id
    coordinator.submit(plan, run_id="r", affinities=(TaskAffinity(task_id, required_worker="W1"),))
    w1.send(p.WorkerGoodbye("W1", "gone", message_id="bye"))
    snap = coordinator.inspect_run("r")
    assert snap.status == RunStatus.FAILED
    assert snap.failure is not None
    assert snap.failure.code == CoordinatorFailureCode.PLACEMENT_CONSTRAINT_LOST
    coordinator.validate_state()


def test_allowed_worker_affinity_is_normalized_after_one_member_is_lost(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    w1 = connect(coordinator, plan, "W1", slots=1, port=9001)
    w2 = connect(coordinator, plan, "W2", slots=1, port=9002)
    task_id = plan.tasks[0].task_id
    coordinator.submit(
        plan,
        run_id="r",
        affinities=(TaskAffinity(task_id, allowed_workers=frozenset({"W1", "W2"})),),
    )
    w1.send(p.WorkerGoodbye("W1", "gone", message_id="bye"))
    snapshot = coordinator.build_snapshot("r")
    affinity = next(a for a in snapshot.affinities if a.task_id == task_id)
    assert affinity.allowed_workers == frozenset({"W2"})
    result = coordinator.schedule("r")
    assert len(result.dispatched) == 1
    assert dispatch_for(w2).attempt.task_id == task_id
    coordinator.validate_state()


def test_accept_decision_rejects_forged_worker_even_at_same_revision(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    connect(coordinator, plan, "W1", prepared=True, port=9001)
    connect(coordinator, plan, "W2", prepared=False, port=9002)
    coordinator.submit(plan, run_id="r")
    decision = coordinator.propose("r")
    assert len(decision.placements) == 1
    forged_placement = replace(decision.placements[0], worker_id="W2")
    forged = SchedulingDecision(
        decision.plan_id, decision.run_id, decision.snapshot_id,
        (forged_placement,), decision.unplaced,
    )
    with pytest.raises(PlacementRejected):
        coordinator.accept_decision("r", forged)
    assert coordinator.inspect_run("r").current_attempts == ()


def test_unsolicited_object_available_cannot_forge_replica(coordinator, build_plan):
    _, plan = build_plan("a=1\nb=a+1\n")
    w1 = connect(coordinator, plan, "W1", slots=1, port=9001)
    w2 = connect(coordinator, plan, "W2", slots=1, port=9002)
    coordinator.submit(plan, run_id="r")
    coordinator.schedule("r")
    d = dispatch_for(w1)
    accept(w1, d); start(w1, d); succeed(w1, plan, d)
    value_id = plan.task_index[d.attempt.task_id].outputs[0].id
    data = p.DataReference(plan.id, "r", value_id, DataForm.IMMUTABLE_VALUE)

    with pytest.raises(InvalidDataLocation):
        w2.send(p.ObjectAvailable("W2", data, 10, message_id="forged"))
    assert {r.worker_id for r in coordinator.data_locations("r")[0].replicas} == {"W1"}

    # The actual producing worker may refine the metadata for its existing replica.
    assert w1.send(p.ObjectAvailable("W1", data, 10, message_id="producer")) in {
        EventDisposition.APPLIED, EventDisposition.DUPLICATE,
    }


def test_committed_shared_reference_producer_may_certify_its_snapshot(coordinator, build_plan):
    _, plan = build_plan("a=[1,2]\nb=a[0]\n")
    w = connect(coordinator, plan, "W1", slots=1)
    coordinator.submit(plan, run_id="r")
    coordinator.schedule("r")
    producer = dispatch_for(w)
    accept(w, producer); start(w, producer); succeed(w, plan, producer, publish_available=False)
    shared_value = plan.task_index[producer.attempt.task_id].outputs[0]
    data = p.DataReference(plan.id, "r", shared_value.id, DataForm.OBJECT_SNAPSHOT, None)
    assert w.send(p.ObjectAvailable("W1", data, 64, message_id="snapshot")) == EventDisposition.APPLIED


def test_context_unavailable_rejects_wrong_plan_scope(coordinator, build_plan):
    _, plan = build_plan("a=[1]\na.append(2)\n")
    root, shared = plan.tasks
    w = connect(coordinator, plan, "W1")
    affinity = TaskAffinity(shared.task_id, required_worker="W1", context_id="ctx")
    context = WorkerContext("ctx", "W1", frozenset({shared.task_id}), 1)
    coordinator.submit(plan, run_id="r", affinities=(affinity,), contexts=(context,))
    with pytest.raises(InvalidWorkerMessage):
        w.send(p.ContextUnavailable(
            "W1", "f" * 64, "r", "ctx", "forged", message_id="bad-plan",
        ))
    assert coordinator.inspect_run("r").status == RunStatus.RUNNING


def test_online_false_heartbeat_reconciles_running_isolated_work(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    w1 = connect(coordinator, plan, "W1", slots=1, port=9001)
    w2 = connect(coordinator, plan, "W2", slots=1, port=9002)
    coordinator.submit(plan, run_id="r")
    coordinator.schedule("r")
    d = dispatch_for(w1)
    accept(w1, d); start(w1, d)

    offline = replace(worker_state(plan, "W1", slots=1), online=False)
    assert w1.send(p.Heartbeat(offline, 1, message_id="offline"), now=5) == EventDisposition.APPLIED
    assert coordinator.inspect_worker("W1").state.online is False
    assert coordinator.get_task("r", d.attempt.task_id).status == TaskStatus.READY
    result = coordinator.schedule("r")
    assert len(result.dispatched) == 1
    assert dispatch_for(w2).attempt.task_id == d.attempt.task_id
    coordinator.validate_state()


def test_context_capacity_two_slots_releases_only_after_completion(coordinator, build_plan):
    _, plan = build_plan(
        "a=[1]\nb=[2]\nc=[3]\na.append(4)\nb.append(5)\nc.append(6)\n"
    )
    roots = [t for t in plan.tasks if t.mode.value == "isolated_candidate"]
    shared = [t for t in plan.tasks if t.mode.value == "shared_context"]
    assert len(roots) == 3 and len(shared) == 3
    w = connect(coordinator, plan, "W1", slots=6)
    affinities = tuple(TaskAffinity(t.task_id, required_worker="W1", context_id="ctx") for t in shared)
    context = WorkerContext("ctx", "W1", frozenset(t.task_id for t in shared), 2)
    coordinator.submit(plan, run_id="r", affinities=affinities, contexts=(context,))
    coordinator.schedule("r")
    for d in _dispatches(w):
        accept(w, d); start(w, d); succeed(w, plan, d)

    first = coordinator.schedule("r")
    assert len(first.dispatched) == 2
    running = _dispatches(w)
    for d in running:
        accept(w, d); start(w, d)
    assert coordinator.schedule("r").dispatched == ()

    succeed(w, plan, running[0])
    next_round = coordinator.schedule("r")
    assert len(next_round.dispatched) == 1
    coordinator.validate_state()


def _failed_parallel_run_with_orphan(coordinator, build_plan):
    _, plan = build_plan("a=1\nb=2\n")
    w = connect(coordinator, plan, "W1", slots=2)
    coordinator.submit(plan, run_id="r")
    coordinator.schedule("r")
    dispatches = _dispatches(w)
    for d in dispatches:
        accept(w, d); start(w, d)
    fail(w, dispatches[0], FailureInfo(
        FailureKind.PYTHON_EXCEPTION, "boom", exception_type="RuntimeError"
    ))
    sibling = dispatches[1]
    return plan, w, sibling


def test_orphan_terminal_result_releases_capacity_but_cannot_commit(coordinator, build_plan):
    plan, w, sibling = _failed_parallel_run_with_orphan(coordinator, build_plan)
    assert coordinator.get_attempt("r", sibling.attempt.attempt_id).status.value == "orphaned"
    assert coordinator.inspect_worker("W1").state.free_slots == 1
    _, disposition = succeed(w, plan, sibling, suffix="late")
    assert disposition == EventDisposition.STALE
    assert coordinator.inspect_worker("W1").state.free_slots == 2
    assert coordinator.get_task("r", sibling.attempt.task_id).status == TaskStatus.FAILED
    coordinator.validate_state()


def test_orphan_cancellation_confirmation_releases_capacity_once(coordinator, build_plan):
    _, w, sibling = _failed_parallel_run_with_orphan(coordinator, build_plan)
    cancel = next(
        m for m in w.drain()
        if isinstance(m, p.CancelTask) and m.attempt == sibling.attempt
    )
    result = p.TaskCancellationResult(
        worker_id="W1", attempt=sibling.attempt,
        outcome=p.CancellationOutcome.CANCELLED,
        message_id="cancelled", correlation_id=cancel.message_id,
    )
    assert w.send(result) == EventDisposition.STALE
    assert coordinator.inspect_worker("W1").state.free_slots == 2
    assert w.send(result) == EventDisposition.DUPLICATE
    assert coordinator.inspect_worker("W1").state.free_slots == 2
    coordinator.validate_state()


def test_worker_loss_retires_orphan_physical_reservation(coordinator, build_plan):
    _, w, sibling = _failed_parallel_run_with_orphan(coordinator, build_plan)
    w.send(p.WorkerGoodbye("W1", "gone", message_id="bye"))
    assert coordinator.get_attempt("r", sibling.attempt.attempt_id).status.value == "lost"
    coordinator.validate_state()


def test_stale_proposal_rejected_when_worker_eligibility_changes(build_plan):
    from coordinator import Coordinator

    _, plan = build_plan("a=1\n")
    mutations = [
        lambda state: replace(state, prepared_program_ids=frozenset()),
        lambda state: replace(state, environment_ids=frozenset()),
        lambda state: replace(state, supported_modes=frozenset()),
        lambda state: replace(state, accepting_work=False),
    ]
    for index, mutate in enumerate(mutations):
        c = Coordinator()
        w = connect(c, plan, "W1", port=9100 + index)
        c.submit(plan, run_id="r")
        decision = c.propose("r")
        changed = mutate(worker_state(plan, "W1"))
        w.send(p.Heartbeat(changed, 1, message_id=f"changed-{index}"))
        with pytest.raises(PlacementRejected):
            c.accept_decision("r", decision)


def test_forged_context_in_scheduler_placement_is_rejected(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    connect(coordinator, plan, "W1")
    coordinator.submit(plan, run_id="r")
    decision = coordinator.propose("r")
    forged = SchedulingDecision(
        decision.plan_id, decision.run_id, decision.snapshot_id,
        (replace(decision.placements[0], context_id="forged-context"),),
        decision.unplaced,
    )
    with pytest.raises(PlacementRejected):
        coordinator.accept_decision("r", forged)


def test_preparation_deadline_retires_request_and_allows_fresh_retry(build_plan):
    from coordinator import Coordinator, OperationTimeouts

    _, plan = build_plan("a=1\n")
    c = Coordinator(operation_timeouts=OperationTimeouts(preparation=5, dispatch_ack=5, start=5,
                                                          transfer=5, cancellation=5))
    w = connect(c, plan, "W1", prepared=False)
    first = c.request_program_preparation("W1", plan)
    w.drain()
    expired = c.expire_operations(now=6)
    assert f"preparation:{first.message_id}" in expired
    assert c.inspect_pending_operations().active_count == 0
    second = c.request_program_preparation("W1", plan)
    assert second.message_id != first.message_id


def test_dispatch_deadline_quarantines_compute_worker_before_retry(build_plan):
    from coordinator import Coordinator, OperationTimeouts

    _, plan = build_plan("a=1\n")
    c = Coordinator(operation_timeouts=OperationTimeouts(preparation=5, dispatch_ack=5, start=5,
                                                          transfer=5, cancellation=5))
    w1 = connect(c, plan, "W1", slots=1, port=9201)
    w2 = connect(c, plan, "W2", slots=1, port=9202)
    c.submit(plan, run_id="r")
    c.schedule("r")
    d = dispatch_for(w1)
    assert f"dispatch:{d.attempt.attempt_id}" in c.expire_operations(now=6)
    assert c.inspect_worker("W1").state.online is False
    assert c.get_task("r", d.attempt.task_id).status == TaskStatus.READY
    c.schedule("r")
    assert dispatch_for(w2).attempt.task_id == d.attempt.task_id


def test_accepted_but_not_started_deadline_quarantines_worker(build_plan):
    from coordinator import Coordinator, OperationTimeouts

    _, plan = build_plan("a=1\n")
    c = Coordinator(operation_timeouts=OperationTimeouts(preparation=5, dispatch_ack=5, start=5,
                                                          transfer=5, cancellation=5))
    w1 = connect(c, plan, "W1", slots=1, port=9211)
    connect(c, plan, "W2", slots=1, port=9212)
    c.submit(plan, run_id="r")
    c.schedule("r")
    d = dispatch_for(w1)
    accept(w1, d)
    assert f"start:{d.attempt.attempt_id}" in c.expire_operations(now=6)
    assert c.inspect_worker("W1").state.online is False
    assert c.get_task("r", d.attempt.task_id).status == TaskStatus.READY


def test_running_task_has_no_default_execution_deadline(build_plan):
    from coordinator import Coordinator, OperationTimeouts

    _, plan = build_plan("a=1\n")
    c = Coordinator(operation_timeouts=OperationTimeouts(preparation=1, dispatch_ack=1, start=1,
                                                          transfer=1, cancellation=1, execution=None))
    w = connect(c, plan, "W1")
    c.submit(plan, run_id="r")
    c.schedule("r")
    d = dispatch_for(w); accept(w, d); start(w, d)
    assert c.expire_operations(now=10_000) == ()
    assert c.get_task("r", d.attempt.task_id).status == TaskStatus.RUNNING


def test_configured_execution_deadline_quarantines_worker(build_plan):
    from coordinator import Coordinator, OperationTimeouts

    _, plan = build_plan("a=1\n")
    c = Coordinator(operation_timeouts=OperationTimeouts(preparation=5, dispatch_ack=5, start=5,
                                                          transfer=5, cancellation=5, execution=5))
    w1 = connect(c, plan, "W1", slots=1, port=9221)
    connect(c, plan, "W2", slots=1, port=9222)
    c.submit(plan, run_id="r")
    c.schedule("r")
    d = dispatch_for(w1); accept(w1, d); start(w1, d)
    assert f"execution:{d.attempt.attempt_id}" in c.expire_operations(now=6)
    assert c.inspect_worker("W1").state.online is False
    assert c.get_task("r", d.attempt.task_id).status == TaskStatus.READY


def test_transfer_control_deadline_fails_transfer(build_plan):
    from coordinator import Coordinator, OperationTimeouts, TransferStatus
    from coordinator.tests.helpers import seed_location

    _, plan = build_plan("a=1\n")
    c = Coordinator(operation_timeouts=OperationTimeouts(preparation=5, dispatch_ack=5, start=5,
                                                          transfer=5, cancellation=5))
    connect(c, plan, "W1", port=9231)
    connect(c, plan, "W2", port=9232)
    c.submit(plan, run_id="r")
    value = next(v for v in plan.values if v.storage == "immutable_value")
    data = p.DataReference(plan.id, "r", value.id, DataForm.IMMUTABLE_VALUE)
    seed_location(c, plan, "r", "W1", data, 10)
    transfer = p.TransferIdentity(data, "T", "TA1", "W1", "W2")
    c.start_transfer(transfer, size_bytes=10)
    expired = c.expire_operations(now=6)
    assert "transfer:T/TA1" in expired
    assert c.get_transfer("T", "TA1").status == TransferStatus.FAILED


def test_cancellation_deadline_quarantines_worker_and_finishes_cancel(build_plan):
    from coordinator import Coordinator, OperationTimeouts

    _, plan = build_plan("a=1\n")
    c = Coordinator(operation_timeouts=OperationTimeouts(preparation=5, dispatch_ack=5, start=5,
                                                          transfer=5, cancellation=5))
    w = connect(c, plan, "W1")
    c.submit(plan, run_id="r")
    c.schedule("r")
    d = dispatch_for(w); accept(w, d); start(w, d)
    c.cancel_run("r")
    assert f"cancellation:{d.attempt.attempt_id}" in c.expire_operations(now=6)
    assert c.inspect_worker("W1").state.online is False
    assert c.inspect_run("r").status == RunStatus.CANCELLED


def test_heartbeat_ack_outbox_is_coalesced_and_bounded(build_plan):
    from coordinator import Coordinator

    _, plan = build_plan("a=1\n")
    c = Coordinator(outbox_limit=4)
    w = connect(c, plan, "W1")
    for sequence in range(1, 5001):
        w.send(p.Heartbeat(worker_state(plan, "W1"), sequence, message_id=f"hb-{sequence}"))
    messages = w.drain()
    acks = [m for m in messages if isinstance(m, p.HeartbeatAck)]
    assert len(acks) == 1
    assert acks[0].sequence == 5000


def test_critical_outbox_backpressure_is_bounded_and_atomic(build_plan):
    from coordinator import Coordinator, OutboundBackpressure

    _, plan1 = build_plan("a=1\n")
    _, plan2 = build_plan("b=2\n")
    c = Coordinator(outbox_limit=1)
    w = connect(c, plan1, "W1", prepared=False)
    first = c.request_program_preparation("W1", plan1)
    assert first is not None
    assert c.inspect_pending_operations().active_count == 1
    with pytest.raises(OutboundBackpressure):
        c.request_program_preparation("W1", plan2)
    assert c.inspect_pending_operations().active_count == 1
    assert len(w.drain()) == 1


def test_membership_updates_are_coalesced(build_plan):
    from coordinator import Coordinator

    _, plan = build_plan("a=1\n")
    c = Coordinator(outbox_limit=4)
    w1 = connect(c, plan, "W1", port=9301)
    connect(c, plan, "W2", port=9302)
    connect(c, plan, "W3", port=9303)
    updates = [m for m in w1.drain() if isinstance(m, p.MembershipUpdate)]
    assert len(updates) == 1
    assert {e.worker_id for e in updates[0].members} == {"W1", "W2", "W3"}


def test_membership_limit_rejects_before_authoritative_mutation(build_plan):
    from coordinator import CapacityConflict, Coordinator, UnknownWorker

    _, plan = build_plan("a=1\n")
    c = Coordinator()
    c.MAX_CLUSTER_MEMBERS = 2
    connect(c, plan, "W1", port=9311)
    connect(c, plan, "W2", port=9312)
    hello = p.WorkerHello(
        worker=worker_state(plan, "W3"),
        endpoint=p.WorkerEndpoint("W3", "w3.lan", 9313),
        supported_versions=(p.PROTOCOL_VERSION,),
        message_id="hello-W3",
    )
    with pytest.raises(CapacityConflict):
        c.register_worker(hello)
    with pytest.raises(UnknownWorker):
        c.inspect_worker("W3")
    assert c._generations.get("W3") is None


def test_invalid_generated_session_id_fails_before_registration_mutation(build_plan):
    from coordinator import Coordinator, CoordinatorError, UnknownWorker

    _, plan = build_plan("a=1\n")

    def bad_ids(kind):
        if kind == "session":
            return "x" * (p.MAX_IDENTIFIER_BYTES + 1)
        return f"{kind}-ok"

    c = Coordinator(id_source=bad_ids)
    hello = p.WorkerHello(
        worker_state(plan, "W1"),
        p.WorkerEndpoint("W1", "127.0.0.1", 9001),
        supported_versions=(p.PROTOCOL_VERSION,),
        message_id="hello",
    )
    with pytest.raises(CoordinatorError):
        c.register_worker(hello)
    with pytest.raises(UnknownWorker):
        c.inspect_worker("W1")
    assert c._generations.get("W1", 0) == 0


def test_coordinator_quarantine_cannot_be_cleared_by_same_session_heartbeat(build_plan):
    from coordinator import Coordinator, OperationTimeouts

    _, plan = build_plan("a=1\n")
    c = Coordinator(operation_timeouts=OperationTimeouts(
        preparation=5, dispatch_ack=5, start=5, transfer=5, cancellation=5,
    ))
    w1 = connect(c, plan, "W1", slots=1, port=9501)
    w2 = connect(c, plan, "W2", slots=1, port=9502)
    c.submit(plan, run_id="r")
    c.schedule("r")
    d = dispatch_for(w1)
    assert c.expire_operations(now=6)
    assert c.inspect_worker("W1").state.online is False

    # A heartbeat cannot prove that the timed-out execution from this generation
    # disappeared, so the same session stays quarantined.
    w1.send(p.Heartbeat(worker_state(plan, "W1", slots=1), 1, message_id="back"), now=7)
    assert c.inspect_worker("W1").state.online is False
    c.schedule("r")
    assert dispatch_for(w2).attempt.task_id == d.attempt.task_id

    # A reconnect establishes a new generation and may compute again.
    fresh = connect(c, plan, "W1", slots=1, port=9503)
    assert fresh.handle.generation > w1.handle.generation
    assert c.inspect_worker("W1").state.online is True


def test_inactive_worker_records_are_reclaimed_but_generation_epoch_is_retained(build_plan):
    from coordinator import Coordinator

    _, plan = build_plan("a=1\n")
    c = Coordinator()
    old = connect(c, plan, "W1", port=9601)
    generation = old.handle.generation
    old.send(p.WorkerGoodbye("W1", "gone", message_id="bye"))
    assert "W1" not in c._workers
    assert c._generations["W1"] == generation
    fresh = connect(c, plan, "W1", port=9602)
    assert fresh.handle.generation == generation + 1


def test_prepared_context_cannot_claim_uncommitted_object_state_output(coordinator, build_plan):
    _, plan = build_plan("a=[1]\na.append(2)\n")
    root, shared = plan.tasks
    w = connect(coordinator, plan, "W1", slots=2)
    context = WorkerContext("ctx", "W1", frozenset({shared.task_id}), 1)
    affinity = TaskAffinity(shared.task_id, required_worker="W1", context_id="ctx")
    coordinator.submit(plan, run_id="r", affinities=(affinity,), contexts=(context,))

    coordinator.schedule("r")
    root_dispatch = dispatch_for(w)
    accept(w, root_dispatch); start(w, root_dispatch); succeed(w, plan, root_dispatch)

    obj = plan.task_index[shared.task_id].objects[0]
    assert obj.state_outputs
    forged_state = obj.state_outputs[0]
    data = p.DataReference(
        plan.id, "r", root.outputs[0].id, DataForm.OBJECT_SNAPSHOT, forged_state,
    )
    with pytest.raises(InvalidDataLocation):
        w.send(p.ObjectAvailable("W1", data, 64, message_id="premature-state"))
