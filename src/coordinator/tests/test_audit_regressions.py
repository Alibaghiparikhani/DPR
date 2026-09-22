from coordinator.tests.helpers import seed_location
import pytest
import protocol as p

from coordinator import (
    AttemptStatus, Coordinator, CoordinatorError, CoordinatorFailureCode,
    EventDisposition, InvalidRunTransition, InvalidWorkerMessage, RetryPolicy,
    RunStatus, TaskStatus, TransferStatus,
)
from coordinator.tests.helpers import (
    accept, connect, dispatch_for, fail, start, succeed, worker_state,
)
from execution import FailureInfo, FailureKind, ProgramIdentity
from scheduler import DataForm, TaskAffinity, WorkerContext


def _prepare_shared_run(coordinator, build_plan, run_id="r", *, two_mutations=False):
    source = "a=[1,2]\na.append(3)\na.append(4)\n" if two_mutations else "a=[1,2]\na.append(3)\n"
    _, plan = build_plan(source)
    root = plan.tasks[0]
    shared_tasks = plan.tasks[1:]
    w = connect(coordinator, plan, "W1", slots=1)
    affinity = tuple(TaskAffinity(t.task_id, required_worker="W1", context_id="ctx") for t in shared_tasks)
    context = WorkerContext("ctx", "W1", frozenset(t.task_id for t in shared_tasks), 1)
    coordinator.submit(plan, run_id=run_id, affinities=affinity, contexts=(context,))
    coordinator.schedule(run_id)
    d = dispatch_for(w)
    assert d.attempt.task_id == root.task_id
    accept(w, d); start(w, d); succeed(w, plan, d)
    return plan, root, shared_tasks, w


def _immutable_ref(plan, run_id, task_id):
    output = next(o for o in plan.task_index[task_id].outputs if o.kind.value == "immutable")
    return p.DataReference(plan.id, run_id, output.id, DataForm.IMMUTABLE_VALUE)


@pytest.mark.parametrize("failure", [
    FailureInfo(FailureKind.EXECUTION_ERROR, "adapter/infrastructure failure"),
    FailureInfo(FailureKind.PYTHON_EXCEPTION, "user failure after mutation", "ValueError"),
])
def test_shared_context_failure_after_start_is_never_replayed(coordinator, build_plan, failure):
    plan, _, (shared,), w = _prepare_shared_run(coordinator, build_plan)
    coordinator.schedule("r")
    d = dispatch_for(w)
    assert d.attempt.task_id == shared.task_id
    accept(w, d); start(w, d)
    fail(w, d, failure)
    snap = coordinator.inspect_run("r")
    assert snap.status == RunStatus.FAILED
    assert snap.failure.code == CoordinatorFailureCode.NATIVE_STATE_UNCERTAIN
    assert coordinator.get_task("r", shared.task_id).status == TaskStatus.FAILED
    assert len(coordinator.get_task("r", shared.task_id).attempt_ids) == 1
    coordinator.validate_state()


def test_shared_context_worker_loss_after_dispatch_is_not_retried(coordinator, build_plan):
    plan, _, (shared,), w = _prepare_shared_run(coordinator, build_plan)
    coordinator.schedule("r")
    d = dispatch_for(w)
    assert d.attempt.task_id == shared.task_id
    # No acceptance/start acknowledgement is enough to prove it did not execute.
    w.send(p.WorkerGoodbye("W1", "connection lost", message_id="bye"))
    snap = coordinator.inspect_run("r")
    assert snap.status == RunStatus.FAILED
    assert snap.failure.code == CoordinatorFailureCode.NATIVE_STATE_UNCERTAIN
    assert coordinator.get_attempt("r", d.attempt.attempt_id).status == AttemptStatus.LOST
    coordinator.validate_state()


def test_native_context_owner_loss_before_task_starts_is_context_lost(coordinator, build_plan):
    _, plan = build_plan("globals()\nx=1\n")
    task = plan.tasks[0]
    assert task.mode.value == "native_region"
    w = connect(coordinator, plan, "W1", slots=1)
    affinity = TaskAffinity(task.task_id, required_worker="W1", context_id="native")
    context = WorkerContext("native", "W1", frozenset({task.task_id}), 1)
    coordinator.submit(plan, run_id="r", affinities=(affinity,), contexts=(context,))
    w.send(p.WorkerGoodbye("W1", "owner gone", message_id="bye"))
    snap = coordinator.inspect_run("r")
    assert snap.status == RunStatus.FAILED
    assert snap.failure.code == CoordinatorFailureCode.CONTEXT_LOST
    with pytest.raises(InvalidRunTransition):
        coordinator.schedule("r")
    coordinator.validate_state()


def test_native_worker_loss_while_running_is_state_uncertain(coordinator, build_plan):
    _, plan = build_plan("globals()\nx=1\n")
    task = plan.tasks[0]
    w = connect(coordinator, plan, "W1", slots=1)
    affinity = TaskAffinity(task.task_id, required_worker="W1", context_id="native")
    context = WorkerContext("native", "W1", frozenset({task.task_id}), 1)
    coordinator.submit(plan, run_id="r", affinities=(affinity,), contexts=(context,))
    coordinator.schedule("r")
    d = dispatch_for(w); accept(w, d); start(w, d)
    w.send(p.WorkerGoodbye("W1", "owner gone", message_id="bye"))
    snap = coordinator.inspect_run("r")
    assert snap.status == RunStatus.FAILED
    assert snap.failure.code == CoordinatorFailureCode.NATIVE_STATE_UNCERTAIN
    coordinator.validate_state()


def test_context_owner_loss_after_committed_mutation_fails_remaining_native_work(coordinator, build_plan):
    plan, _, shared, w = _prepare_shared_run(coordinator, build_plan, two_mutations=True)
    first, second = shared
    coordinator.schedule("r")
    d = dispatch_for(w)
    assert d.attempt.task_id == first.task_id
    accept(w, d); start(w, d); succeed(w, plan, d)
    assert coordinator.get_task("r", first.task_id).status == TaskStatus.COMMITTED
    assert coordinator.get_task("r", second.task_id).status == TaskStatus.READY
    w.send(p.WorkerGoodbye("W1", "context lost after mutation", message_id="bye"))
    snap = coordinator.inspect_run("r")
    assert snap.status == RunStatus.FAILED
    assert snap.failure.code == CoordinatorFailureCode.CONTEXT_LOST
    assert coordinator.get_task("r", first.task_id).status == TaskStatus.COMMITTED
    coordinator.validate_state()


def test_isolated_infrastructure_failure_still_retries_and_is_bounded(build_plan):
    _, plan = build_plan("a=1\n")
    c = Coordinator(retry_policy=RetryPolicy(max_attempts_per_task=2))
    w = connect(c, plan)
    c.submit(plan, run_id="r")
    first_id = None
    for index in range(2):
        c.schedule("r")
        d = dispatch_for(w)
        if first_id is None:
            first_id = d.attempt.attempt_id
        else:
            assert d.attempt.attempt_id != first_id
        accept(w, d); start(w, d)
        fail(w, d, FailureInfo(FailureKind.EXECUTION_ERROR, f"infra-{index}"))
    assert c.inspect_run("r").status == RunStatus.FAILED
    assert len(c.get_task("r", plan.tasks[0].task_id).attempt_ids) == 2
    c.validate_state()


def test_retry_policy_rejects_user_exception_as_retry_category():
    with pytest.raises(ValueError):
        RetryPolicy(retry_failure_kinds=frozenset({FailureKind.PYTHON_EXCEPTION}))


def test_new_session_cannot_satisfy_old_program_preparation(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    old = connect(coordinator, plan, "W1", prepared=False)
    cmd = coordinator.request_program_preparation("W1", plan)
    old.drain()
    new = connect(coordinator, plan, "W1", prepared=False, port=9001)
    new.drain()
    with pytest.raises(InvalidWorkerMessage):
        new.send(p.ProgramPrepared("W1", plan.id, plan.program.id,
                                   message_id="late", correlation_id=cmd.message_id))
    assert plan.program.id not in coordinator.inspect_worker("W1").state.prepared_program_ids


def test_new_session_cannot_satisfy_old_context_preparation(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    old = connect(coordinator, plan, "W1")
    coordinator.submit(plan, run_id="r")
    task_id = plan.tasks[0].task_id
    cmd = coordinator.request_context_preparation("r", "W1", "ctx", (task_id,))
    old.drain()
    new = connect(coordinator, plan, "W1", port=9001)
    new.drain()
    context = WorkerContext("ctx", "W1", frozenset({task_id}), 1)
    with pytest.raises(InvalidWorkerMessage):
        new.send(p.ContextPrepared(plan.id, "r", context,
                                   message_id="late", correlation_id=cmd.message_id))


def test_wrong_worker_cannot_satisfy_pending_program_command(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    w1 = connect(coordinator, plan, "W1", prepared=False, port=9001)
    w2 = connect(coordinator, plan, "W2", prepared=False, port=9002)
    cmd = coordinator.request_program_preparation("W1", plan)
    w1.drain(); w2.drain()
    with pytest.raises(InvalidWorkerMessage):
        w2.send(p.ProgramPrepared("W2", plan.id, plan.program.id,
                                  message_id="wrong-worker", correlation_id=cmd.message_id))


@pytest.mark.parametrize("other", ["environment", "package"])
def test_program_preparation_rejects_different_exact_program_identity(coordinator, build_plan, other):
    _, plan = build_plan("a=1\n")
    w = connect(coordinator, plan, "W1", prepared=False)
    cmd = coordinator.request_program_preparation("W1", plan)
    w.drain()
    environment = "other-env" if other == "environment" else plan.program.environment_id
    package = "other-package" if other == "package" else plan.program.package_id
    forged = ProgramIdentity(plan.program.source_sha256, plan.program.filename, environment, package)
    with pytest.raises(InvalidWorkerMessage):
        w.send(p.ProgramPrepared("W1", plan.id, forged.id,
                                 message_id=f"wrong-{other}", correlation_id=cmd.message_id))
    assert forged.id not in coordinator.inspect_worker("W1").state.prepared_program_ids


def test_program_preparation_rejects_wrong_plan_and_failure_identity(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    _, other_plan = build_plan("b=2\n")
    w = connect(coordinator, plan, "W1", prepared=False)
    cmd = coordinator.request_program_preparation("W1", plan)
    w.drain()
    with pytest.raises(InvalidWorkerMessage):
        w.send(p.ProgramPrepared("W1", other_plan.id, plan.program.id,
                                 message_id="wrong-plan", correlation_id=cmd.message_id))
    with pytest.raises(InvalidWorkerMessage):
        w.send(p.ProgramPreparationFailed(
            "W1", plan.id, other_plan.program.id,
            FailureInfo(FailureKind.ENVIRONMENT_MISMATCH, "wrong program"),
            message_id="wrong-failure", correlation_id=cmd.message_id,
        ))


def test_duplicate_program_prepared_response_is_idempotent(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    w = connect(coordinator, plan, "W1", prepared=False)
    cmd = coordinator.request_program_preparation("W1", plan)
    w.drain()
    response = p.ProgramPrepared("W1", plan.id, plan.program.id,
                                 message_id="prepared", correlation_id=cmd.message_id)
    assert w.send(response) == EventDisposition.APPLIED
    assert w.send(response) == EventDisposition.DUPLICATE


@pytest.mark.parametrize("variant", ["context_id", "tasks", "plan", "run"])
def test_context_preparation_requires_exact_requested_contract(coordinator, build_plan, variant):
    _, plan = build_plan("a=[1,2]\na.append(3)\n")
    _, shared = plan.tasks
    _, other_plan = build_plan("z=3\n")
    w = connect(coordinator, plan, "W1")
    coordinator.submit(plan, run_id="r")
    cmd = coordinator.request_context_preparation("r", "W1", "ctx-requested", (shared.task_id,))
    w.drain()
    context_id = "ctx-forged" if variant == "context_id" else "ctx-requested"
    tasks = frozenset() if variant == "tasks" else frozenset({shared.task_id})
    context = WorkerContext(context_id, "W1", tasks, 1)
    plan_id = other_plan.id if variant == "plan" else plan.id
    run_id = "other-run" if variant == "run" else "r"
    with pytest.raises(InvalidWorkerMessage):
        w.send(p.ContextPrepared(plan_id, run_id, context,
                                 message_id=f"forged-{variant}", correlation_id=cmd.message_id))


def test_duplicate_context_prepared_response_is_idempotent(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    w = connect(coordinator, plan, "W1")
    coordinator.submit(plan, run_id="r")
    task_id = plan.tasks[0].task_id
    cmd = coordinator.request_context_preparation("r", "W1", "ctx", (task_id,))
    w.drain()
    context = WorkerContext("ctx", "W1", frozenset({task_id}), 1)
    response = p.ContextPrepared(plan.id, "r", context,
                                 message_id="ready", correlation_id=cmd.message_id)
    assert w.send(response) == EventDisposition.APPLIED
    assert w.send(response) == EventDisposition.DUPLICATE


def test_context_response_after_run_cancellation_cannot_mutate_state(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    w = connect(coordinator, plan, "W1")
    coordinator.submit(plan, run_id="r")
    task_id = plan.tasks[0].task_id
    cmd = coordinator.request_context_preparation("r", "W1", "ctx", (task_id,))
    w.drain()
    coordinator.cancel_run("r")
    context = WorkerContext("ctx", "W1", frozenset({task_id}), 1)
    with pytest.raises(InvalidWorkerMessage):
        w.send(p.ContextPrepared(plan.id, "r", context,
                                 message_id="late", correlation_id=cmd.message_id))


def test_pending_correlations_remain_unique_even_if_injected_id_source_repeats(build_plan):
    _, plan = build_plan("a=1\n")
    _, other = build_plan("b=2\n")
    counters = {"session": 0}
    def ids(kind):
        if kind == "message":
            return "fixed-message"
        counters[kind] = counters.get(kind, 0) + 1
        return f"{kind}-{counters[kind]}"
    c = Coordinator(id_source=ids)
    connect(c, plan, "W1", prepared=False)
    first = c.request_program_preparation("W1", plan)
    second = c.request_program_preparation("W1", other)
    assert first.message_id != second.message_id
    assert first.message_id.endswith("~p1")
    assert second.message_id.endswith("~p2")


def test_loss_of_only_committed_immutable_replica_fails_run(coordinator, build_plan):
    _, plan = build_plan("a=1\nb=a+1\n")
    w1 = connect(coordinator, plan, "W1", slots=1)
    coordinator.submit(plan, run_id="r")
    coordinator.schedule("r")
    d = dispatch_for(w1)
    accept(w1, d); start(w1, d); succeed(w1, plan, d)
    connect(coordinator, plan, "W2", slots=1, port=9002)
    w1.send(p.WorkerGoodbye("W1", "lost sole replica", message_id="bye"))
    snap = coordinator.inspect_run("r")
    assert snap.status == RunStatus.FAILED
    assert snap.failure.code == CoordinatorFailureCode.DATA_LOST
    coordinator.validate_state()


def test_losing_one_of_multiple_replicas_does_not_fail_run(coordinator, build_plan):
    _, plan = build_plan("a=1\nb=a+1\n")
    w1 = connect(coordinator, plan, "W1", slots=1, port=9001)
    coordinator.submit(plan, run_id="r")
    coordinator.schedule("r")
    d = dispatch_for(w1)
    accept(w1, d); start(w1, d); succeed(w1, plan, d)
    w2 = connect(coordinator, plan, "W2", slots=1, port=9002)
    data = _immutable_ref(plan, "r", d.attempt.task_id)
    seed_location(coordinator, plan, "r", "W2", data, 8)
    w1.send(p.WorkerGoodbye("W1", "one replica lost", message_id="bye1"))
    assert coordinator.inspect_run("r").status == RunStatus.RUNNING
    result = coordinator.schedule("r")
    assert len(result.dispatched) == 1
    coordinator.validate_state()


def test_offline_replica_does_not_count_as_surviving_replica(coordinator, build_plan):
    _, plan = build_plan("a=1\nb=a+1\n")
    w1 = connect(coordinator, plan, "W1", slots=1, port=9001)
    coordinator.submit(plan, run_id="r")
    coordinator.schedule("r")
    d = dispatch_for(w1); accept(w1, d); start(w1, d); succeed(w1, plan, d)
    w2 = connect(coordinator, plan, "W2", slots=1, port=9002)
    data = _immutable_ref(plan, "r", d.attempt.task_id)
    seed_location(coordinator, plan, "r", "W2", data, 8)
    w2.send(p.WorkerGoodbye("W2", "replica offline", message_id="bye2"))
    assert coordinator.inspect_run("r").status == RunStatus.RUNNING
    w1.send(p.WorkerGoodbye("W1", "last live replica gone", message_id="bye1"))
    snap = coordinator.inspect_run("r")
    assert snap.status == RunStatus.FAILED
    assert snap.failure.code == CoordinatorFailureCode.DATA_LOST


def test_loss_of_required_object_snapshot_version_fails_run(coordinator, build_plan):
    _, plan = build_plan("a=[1,2]\nb=a[0]\n")
    producer, consumer = plan.tasks
    w1 = connect(coordinator, plan, "W1", slots=1, port=9001)
    coordinator.submit(plan, run_id="r")
    coordinator.schedule("r")
    d = dispatch_for(w1); accept(w1, d); start(w1, d); succeed(w1, plan, d)
    shared = plan.task_index[producer.task_id].outputs[0]
    data = p.DataReference(plan.id, "r", shared.id, DataForm.OBJECT_SNAPSHOT, None)
    seed_location(coordinator, plan, "r", "W1", data, 64)
    connect(coordinator, plan, "W2", slots=1, port=9002)
    w1.send(p.WorkerGoodbye("W1", "snapshot owner lost", message_id="bye"))
    snap = coordinator.inspect_run("r")
    assert snap.status == RunStatus.FAILED
    assert snap.failure.code == CoordinatorFailureCode.DATA_LOST
    assert coordinator.get_task("r", consumer.task_id).status == TaskStatus.FAILED


def test_explicit_unavailability_of_last_required_replica_fails_run(coordinator, build_plan):
    _, plan = build_plan("a=1\nb=a+1\n")
    w = connect(coordinator, plan, "W1", slots=1)
    coordinator.submit(plan, run_id="r")
    coordinator.schedule("r")
    d = dispatch_for(w); accept(w, d); start(w, d); succeed(w, plan, d)
    data = _immutable_ref(plan, "r", d.attempt.task_id)
    assert w.send(p.ObjectUnavailable("W1", data, "evicted", message_id="gone")) == EventDisposition.APPLIED
    snap = coordinator.inspect_run("r")
    assert snap.status == RunStatus.FAILED
    assert snap.failure.code == CoordinatorFailureCode.DATA_LOST


def test_destination_reconnect_cannot_satisfy_old_transfer_preparation(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    source = connect(coordinator, plan, "W1", port=9001)
    destination = connect(coordinator, plan, "W2", port=9002)
    coordinator.submit(plan, run_id="r")
    value = next(v for v in plan.values if v.storage == "immutable_value")
    data = p.DataReference(plan.id, "r", value.id, DataForm.IMMUTABLE_VALUE)
    seed_location(coordinator, plan, "r", "W1", data, 10)
    transfer = p.TransferIdentity(data, "T", "TA1", "W1", "W2")
    prepare = coordinator.start_transfer(transfer, size_bytes=10)
    destination.drain()
    replacement = connect(coordinator, plan, "W2", port=9012)
    replacement.drain()
    with pytest.raises(InvalidWorkerMessage):
        replacement.send(p.ReceiveReady("W2", transfer,
                                        message_id="late", correlation_id=prepare.message_id))


def test_source_reconnect_cannot_satisfy_old_transfer_request(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    source = connect(coordinator, plan, "W1", port=9001)
    destination = connect(coordinator, plan, "W2", port=9002)
    coordinator.submit(plan, run_id="r")
    value = next(v for v in plan.values if v.storage == "immutable_value")
    data = p.DataReference(plan.id, "r", value.id, DataForm.IMMUTABLE_VALUE)
    seed_location(coordinator, plan, "r", "W1", data, 10)
    transfer = p.TransferIdentity(data, "T", "TA1", "W1", "W2")
    prepare = coordinator.start_transfer(transfer, size_bytes=10)
    destination.drain()
    destination.send(p.ReceiveReady("W2", transfer,
                                    message_id="ready", correlation_id=prepare.message_id))
    request = next(m for m in source.drain() if isinstance(m, p.TransferRequest))
    replacement = connect(coordinator, plan, "W1", port=9011)
    replacement.drain()
    with pytest.raises(InvalidWorkerMessage):
        replacement.send(p.TransferAccepted("W1", transfer,
                                            message_id="late", correlation_id=request.message_id))


def test_inspection_getters_return_defensive_state_copies(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    w = connect(coordinator, plan, "W1")
    coordinator.submit(plan, run_id="r")
    task_id = plan.tasks[0].task_id
    leaked_task = coordinator.get_task("r", task_id)
    leaked_task.status = TaskStatus.FAILED
    leaked_task.attempt_ids.append("forged")
    assert coordinator.get_task("r", task_id).status == TaskStatus.READY
    assert coordinator.get_task("r", task_id).attempt_ids == []

    coordinator.schedule("r")
    d = dispatch_for(w)
    leaked_attempt = coordinator.get_attempt("r", d.attempt.attempt_id)
    leaked_attempt.status = AttemptStatus.COMMITTED
    leaked_attempt.pending_transfers.add(("fake", "fake"))
    actual_attempt = coordinator.get_attempt("r", d.attempt.attempt_id)
    assert actual_attempt.status == AttemptStatus.DISPATCHED
    assert actual_attempt.pending_transfers == set()


def test_transfer_inspection_is_defensive_copy(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    source = connect(coordinator, plan, "W1", port=9001)
    connect(coordinator, plan, "W2", port=9002)
    coordinator.submit(plan, run_id="r")
    value = next(v for v in plan.values if v.storage == "immutable_value")
    data = p.DataReference(plan.id, "r", value.id, DataForm.IMMUTABLE_VALUE)
    seed_location(coordinator, plan, "r", "W1", data, 10)
    transfer = p.TransferIdentity(data, "T", "TA1", "W1", "W2")
    coordinator.start_transfer(transfer, size_bytes=10)
    leaked = coordinator.get_transfer("T", "TA1")
    leaked.status = TransferStatus.COMPLETED
    leaked.failure_detail = "forged"
    actual = coordinator.get_transfer("T", "TA1")
    assert actual.status == TransferStatus.DESTINATION_PREPARING
    assert actual.failure_detail is None


def test_stale_program_success_cannot_satisfy_retried_preparation(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    w = connect(coordinator, plan, "W1", prepared=False)
    first = coordinator.request_program_preparation("W1", plan)
    w.drain()
    failed = p.ProgramPreparationFailed(
        "W1", plan.id, plan.program.id,
        FailureInfo(FailureKind.ENVIRONMENT_MISMATCH, "temporary setup failure"),
        message_id="failed", correlation_id=first.message_id,
    )
    assert w.send(failed) == EventDisposition.APPLIED
    second = coordinator.request_program_preparation("W1", plan)
    w.drain()
    assert second.message_id != first.message_id
    with pytest.raises(InvalidWorkerMessage):
        w.send(p.ProgramPrepared("W1", plan.id, plan.program.id,
                                 message_id="late-first", correlation_id=first.message_id))
    assert plan.program.id not in coordinator.inspect_worker("W1").state.prepared_program_ids
    assert w.send(p.ProgramPrepared("W1", plan.id, plan.program.id,
                                    message_id="second-ok", correlation_id=second.message_id)) == EventDisposition.APPLIED


def test_context_preparation_failure_must_match_exact_request(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    w = connect(coordinator, plan, "W1")
    coordinator.submit(plan, run_id="r")
    task_id = plan.tasks[0].task_id
    cmd = coordinator.request_context_preparation("r", "W1", "ctx", (task_id,))
    w.drain()
    with pytest.raises(InvalidWorkerMessage):
        w.send(p.ContextPreparationFailed(
            "W1", plan.id, "r", "other-ctx",
            FailureInfo(FailureKind.EXECUTION_ERROR, "nope"),
            message_id="wrong", correlation_id=cmd.message_id,
        ))


def test_new_session_cannot_ack_old_dispatch(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    old = connect(coordinator, plan, "W1", slots=1)
    coordinator.submit(plan, run_id="r")
    coordinator.schedule("r")
    dispatch = dispatch_for(old)
    replacement = connect(coordinator, plan, "W1", slots=1, port=9001)
    replacement.drain()
    with pytest.raises(InvalidWorkerMessage):
        replacement.send(p.TaskAccepted(
            "W1", dispatch.attempt,
            message_id="late-accept", correlation_id=dispatch.message_id,
        ))


def test_new_session_cannot_ack_old_cancellation(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    old = connect(coordinator, plan, "W1", slots=1)
    coordinator.submit(plan, run_id="r")
    coordinator.schedule("r")
    dispatch = dispatch_for(old)
    accept(old, dispatch); start(old, dispatch)
    cancel, = coordinator.cancel_run("r")
    old.drain()
    replacement = connect(coordinator, plan, "W1", slots=1, port=9001)
    replacement.drain()
    with pytest.raises(InvalidWorkerMessage):
        replacement.send(p.TaskCancellationResult(
            "W1", dispatch.attempt, p.CancellationOutcome.CANCELLED, "late old-session ack",
            message_id="late-cancel", correlation_id=cancel.message_id,
        ))


def test_context_loss_after_committed_mutation_fails_unmaterialized_snapshot_consumer(coordinator, build_plan):
    _, plan = build_plan("a=[1]\na.append(2)\nx=sum(a)\n")
    root, mutation, consumer = plan.tasks
    w = connect(coordinator, plan, "W1", slots=1)
    affinity = TaskAffinity(mutation.task_id, required_worker="W1", context_id="ctx")
    context = WorkerContext("ctx", "W1", frozenset({mutation.task_id}), 1)
    coordinator.submit(plan, run_id="r", affinities=(affinity,), contexts=(context,))
    coordinator.schedule("r")
    d = dispatch_for(w); assert d.attempt.task_id == root.task_id
    accept(w, d); start(w, d); succeed(w, plan, d)
    coordinator.schedule("r")
    d = dispatch_for(w); assert d.attempt.task_id == mutation.task_id
    accept(w, d); start(w, d); succeed(w, plan, d, publish_available=False)
    assert coordinator.get_task("r", consumer.task_id).status == TaskStatus.READY
    # The initial object snapshot from the isolated producer is expected to exist;
    # what is deliberately absent is the post-mutation state snapshot that the
    # downstream isolated consumer would need after context loss.
    state_id = mutation.objects[0].state_outputs[0]
    assert not [location for location in coordinator.data_locations("r")
                if location.object_state_id == state_id]
    w.send(p.WorkerGoodbye("W1", "native owner lost before snapshot materialization", message_id="bye"))
    snap = coordinator.inspect_run("r")
    assert snap.status == RunStatus.FAILED
    assert snap.failure.code == CoordinatorFailureCode.CONTEXT_LOST
    coordinator.validate_state()


def test_worker_reported_offline_replica_does_not_count_as_valid_availability(coordinator, build_plan):
    from dataclasses import replace
    _, plan = build_plan("a=1\nb=a+1\n")
    w1 = connect(coordinator, plan, "W1", slots=1)
    coordinator.submit(plan, run_id="r")
    coordinator.schedule("r")
    d = dispatch_for(w1); accept(w1, d); start(w1, d); succeed(w1, plan, d)
    offline = replace(worker_state(plan, "W1", slots=1), online=False)
    w1.send(p.Heartbeat(offline, 1, message_id="offline"))
    snap = coordinator.inspect_run("r")
    assert snap.status == RunStatus.FAILED
    assert snap.failure.code == CoordinatorFailureCode.DATA_LOST
    coordinator.validate_state()
