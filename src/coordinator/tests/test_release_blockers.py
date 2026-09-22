import protocol as p
import pytest

from coordinator import (
    ContextConflict, Coordinator, EventDisposition, InvalidWorkerMessage,
)
from coordinator.model import PendingContextPreparation
from coordinator.tests.helpers import accept, connect, dispatch_for, start, succeed
from scheduler import TaskAffinity, WorkerContext


def _shared_plan(build_plan):
    _, plan = build_plan("a=[1,2]\na.append(3)\na.append(4)\n")
    root, t2, t3 = plan.tasks
    return plan, root, t2, t3


def _prepared_response(plan, run_id, context, command, *, message_id="ctx-ready"):
    return p.ContextPrepared(
        plan.id, run_id, context,
        message_id=message_id,
        correlation_id=command.message_id,
    )


def test_identical_duplicate_pending_context_request_is_idempotent(coordinator, build_plan):
    plan, _, t2, t3 = _shared_plan(build_plan)
    w = connect(coordinator, plan, "W1")
    coordinator.submit(plan, run_id="r")

    first = coordinator.request_context_preparation("r", "W1", "ctx", (t2.task_id, t3.task_id))
    second = coordinator.request_context_preparation("r", "W1", "ctx", (t3.task_id, t2.task_id))
    assert second == first
    commands = [m for m in w.drain() if isinstance(m, p.PrepareContext)]
    assert commands == [first]
    assert coordinator.inspect_pending_operations().active_count == 1
    coordinator.validate_state()


def test_conflicting_pending_context_task_set_is_rejected(coordinator, build_plan):
    plan, _, t2, t3 = _shared_plan(build_plan)
    w = connect(coordinator, plan, "W1")
    coordinator.submit(plan, run_id="r")

    first = coordinator.request_context_preparation("r", "W1", "ctx", (t2.task_id, t3.task_id))
    with pytest.raises(ContextConflict):
        coordinator.request_context_preparation("r", "W1", "ctx", (t3.task_id,))
    assert [m for m in w.drain() if isinstance(m, p.PrepareContext)] == [first]
    coordinator.validate_state()


def test_conflicting_pending_context_owner_is_rejected(coordinator, build_plan):
    plan, _, t2, t3 = _shared_plan(build_plan)
    w1 = connect(coordinator, plan, "W1", port=9001)
    w2 = connect(coordinator, plan, "W2", port=9002)
    w1.drain(); w2.drain()
    coordinator.submit(plan, run_id="r")

    coordinator.request_context_preparation("r", "W1", "ctx", (t2.task_id, t3.task_id))
    with pytest.raises(ContextConflict):
        coordinator.request_context_preparation("r", "W2", "ctx", (t2.task_id, t3.task_id))
    assert not [m for m in w2.drain() if isinstance(m, p.PrepareContext)]
    coordinator.validate_state()


def test_same_context_id_is_independently_scoped_by_run(coordinator, build_plan):
    plan, _, t2, t3 = _shared_plan(build_plan)
    w = connect(coordinator, plan, "W1")
    coordinator.submit(plan, run_id="r1")
    coordinator.submit(plan, run_id="r2")

    one = coordinator.request_context_preparation("r1", "W1", "ctx", (t2.task_id, t3.task_id))
    two = coordinator.request_context_preparation("r2", "W1", "ctx", (t3.task_id,))
    assert one is not None and two is not None and one.message_id != two.message_id
    coordinator.validate_state()


def test_identical_request_after_context_prepared_is_already_satisfied(coordinator, build_plan):
    plan, _, t2, t3 = _shared_plan(build_plan)
    w = connect(coordinator, plan, "W1")
    coordinator.submit(plan, run_id="r")
    command = coordinator.request_context_preparation("r", "W1", "ctx", (t2.task_id, t3.task_id))
    w.drain()
    context = WorkerContext("ctx", "W1", frozenset({t2.task_id, t3.task_id}), 2)
    assert w.send(_prepared_response(plan, "r", context, command)) == EventDisposition.APPLIED

    assert coordinator.request_context_preparation("r", "W1", "ctx", (t3.task_id, t2.task_id)) is None
    assert not [m for m in w.drain() if isinstance(m, p.PrepareContext)]
    coordinator.validate_state()


def test_conflicting_request_after_context_prepared_is_rejected(coordinator, build_plan):
    plan, _, t2, t3 = _shared_plan(build_plan)
    w = connect(coordinator, plan, "W1")
    coordinator.submit(plan, run_id="r")
    command = coordinator.request_context_preparation("r", "W1", "ctx", (t2.task_id, t3.task_id))
    w.drain()
    context = WorkerContext("ctx", "W1", frozenset({t2.task_id, t3.task_id}), 1)
    w.send(_prepared_response(plan, "r", context, command))

    with pytest.raises(ContextConflict):
        coordinator.request_context_preparation("r", "W1", "ctx", (t3.task_id,))
    assert coordinator.build_snapshot("r").contexts == (context,)
    coordinator.validate_state()


def test_duplicate_identical_context_prepared_response_is_idempotent(coordinator, build_plan):
    plan, _, t2, t3 = _shared_plan(build_plan)
    w = connect(coordinator, plan, "W1")
    coordinator.submit(plan, run_id="r")
    command = coordinator.request_context_preparation("r", "W1", "ctx", (t2.task_id, t3.task_id))
    w.drain()
    context = WorkerContext("ctx", "W1", frozenset({t2.task_id, t3.task_id}), 1)
    response = _prepared_response(plan, "r", context, command)
    assert w.send(response) == EventDisposition.APPLIED
    assert w.send(response) == EventDisposition.DUPLICATE
    assert coordinator.build_snapshot("r").contexts == (context,)


def test_conflicting_late_context_prepared_response_cannot_replace_authoritative_context(coordinator, build_plan):
    plan, _, t2, t3 = _shared_plan(build_plan)
    w = connect(coordinator, plan, "W1")
    coordinator.submit(plan, run_id="r")
    command = coordinator.request_context_preparation("r", "W1", "ctx", (t2.task_id, t3.task_id))
    w.drain()
    full = WorkerContext("ctx", "W1", frozenset({t2.task_id, t3.task_id}), 1)
    assert w.send(_prepared_response(plan, "r", full, command, message_id="full")) == EventDisposition.APPLIED

    narrow = WorkerContext("ctx", "W1", frozenset({t3.task_id}), 1)
    with pytest.raises(InvalidWorkerMessage):
        w.send(_prepared_response(plan, "r", narrow, command, message_id="narrow"))
    assert coordinator.build_snapshot("r").contexts == (full,)
    coordinator.validate_state()


def test_response_side_defense_rejects_forged_conflicting_pending_contract(coordinator, build_plan):
    plan, _, t2, t3 = _shared_plan(build_plan)
    w = connect(coordinator, plan, "W1")
    coordinator.submit(plan, run_id="r")
    command = coordinator.request_context_preparation("r", "W1", "ctx", (t2.task_id, t3.task_id))
    w.drain()
    full = WorkerContext("ctx", "W1", frozenset({t2.task_id, t3.task_id}), 1)
    w.send(_prepared_response(plan, "r", full, command))

    forged_id = "forged-pending"
    coordinator._pending_ops._active[forged_id] = PendingContextPreparation(
        forged_id, "W1", w.handle.generation, plan.id, "r", plan.program.id,
        "ctx", (t3.task_id,),
    )
    narrow = WorkerContext("ctx", "W1", frozenset({t3.task_id}), 1)
    with pytest.raises(InvalidWorkerMessage):
        w.send(p.ContextPrepared(plan.id, "r", narrow,
                                 message_id="forged", correlation_id=forged_id))
    assert forged_id not in coordinator.inspect_pending_operations().active_message_ids
    assert coordinator.build_snapshot("r").contexts == (full,)
    coordinator.validate_state()


def test_context_conflict_cannot_strand_previously_valid_shared_task(coordinator, build_plan):
    plan, root, t2, t3 = _shared_plan(build_plan)
    w = connect(coordinator, plan, "W1", slots=1)
    affinities = (
        TaskAffinity(t2.task_id, required_worker="W1", context_id="ctx"),
        TaskAffinity(t3.task_id, required_worker="W1", context_id="ctx"),
    )
    coordinator.submit(plan, run_id="r", affinities=affinities)
    coordinator.schedule("r")
    root_dispatch = dispatch_for(w)
    accept(w, root_dispatch); start(w, root_dispatch); succeed(w, plan, root_dispatch)

    command = coordinator.request_context_preparation("r", "W1", "ctx", (t2.task_id, t3.task_id))
    w.drain()
    full = WorkerContext("ctx", "W1", frozenset({t2.task_id, t3.task_id}), 1)
    w.send(_prepared_response(plan, "r", full, command))
    with pytest.raises(ContextConflict):
        coordinator.request_context_preparation("r", "W1", "ctx", (t3.task_id,))

    snapshot = coordinator.build_snapshot("r")
    snapshot.validate(plan)
    scheduled = coordinator.schedule("r")
    assert scheduled.dispatched and scheduled.dispatched[0].task_id == t2.task_id
    coordinator.validate_state()


def test_submit_rejects_duplicate_initial_context_identity(coordinator, build_plan):
    plan, _, t2, t3 = _shared_plan(build_plan)
    first = WorkerContext("ctx", "W1", frozenset({t2.task_id, t3.task_id}), 1)
    second = WorkerContext("ctx", "W2", frozenset({t3.task_id}), 1)
    with pytest.raises(ContextConflict):
        coordinator.submit(plan, run_id="r", contexts=(first, second))


def test_validate_state_detects_conflicting_internal_pending_contexts(coordinator, build_plan):
    plan, _, t2, t3 = _shared_plan(build_plan)
    w = connect(coordinator, plan, "W1")
    coordinator.submit(plan, run_id="r")
    one = PendingContextPreparation("one", "W1", w.handle.generation, plan.id, "r",
                                    plan.program.id, "ctx", (t2.task_id, t3.task_id))
    two = PendingContextPreparation("two", "W1", w.handle.generation, plan.id, "r",
                                    plan.program.id, "ctx", (t3.task_id,))
    coordinator._pending_ops._active["one"] = one
    coordinator._pending_ops._active["two"] = two
    with pytest.raises(AssertionError, match="conflicting pending contracts"):
        coordinator.validate_state()


def _complete_context(coordinator, w, plan, run_id, index):
    task_id = plan.tasks[0].task_id
    context_id = f"ctx-{index}"
    command = coordinator.request_context_preparation(run_id, w.handle.worker_id, context_id, (task_id,))
    w.drain()
    context = WorkerContext(context_id, w.handle.worker_id, frozenset({task_id}), 1)
    response = _prepared_response(plan, run_id, context, command, message_id=f"ready-{index}")
    assert w.send(response) == EventDisposition.APPLIED
    return command, response, context


def test_completed_pending_history_is_bounded_and_evicts_oldest_deterministically(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    w = connect(coordinator, plan, "W1")
    coordinator.submit(plan, run_id="r")
    coordinator.MAX_PENDING_HISTORY = 3

    completed = [_complete_context(coordinator, w, plan, "r", i) for i in range(5)]
    assert coordinator.inspect_pending_operations().active_count == 0
    assert coordinator.inspect_pending_operations().retired_count == 3
    assert list(coordinator.inspect_pending_operations().retired_message_ids) == [
        completed[2][0].message_id,
        completed[3][0].message_id,
        completed[4][0].message_id,
    ]


def test_recent_completed_response_is_still_idempotently_recognized(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    w = connect(coordinator, plan, "W1")
    coordinator.submit(plan, run_id="r")
    coordinator.MAX_PENDING_HISTORY = 2
    command, response, _ = _complete_context(coordinator, w, plan, "r", 0)
    assert command.message_id in coordinator.inspect_pending_operations().retired_message_ids
    assert w.send(response) == EventDisposition.DUPLICATE


def test_evicted_ancient_correlation_is_rejected_and_cannot_mutate_state(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    w = connect(coordinator, plan, "W1")
    coordinator.submit(plan, run_id="r")
    coordinator.MAX_PENDING_HISTORY = 2
    ancient_command, ancient_response, ancient_context = _complete_context(coordinator, w, plan, "r", 0)
    _complete_context(coordinator, w, plan, "r", 1)
    _complete_context(coordinator, w, plan, "r", 2)
    assert ancient_command.message_id not in coordinator.inspect_pending_operations().retired_message_ids

    # Remove the already-established context so a stale response would visibly mutate
    # state if an evicted correlation were accidentally accepted.
    del coordinator._runs["r"].contexts[ancient_context.context_id]
    with pytest.raises(InvalidWorkerMessage, match="unknown PendingContextPreparation correlation"):
        w.send(ancient_response)
    assert ancient_context.context_id not in coordinator._runs["r"].contexts


def test_active_pending_operations_are_never_evicted_by_completed_history_bound(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    task_id = plan.tasks[0].task_id
    w = connect(coordinator, plan, "W1")
    coordinator.submit(plan, run_id="r")
    coordinator.MAX_PENDING_HISTORY = 2

    active = coordinator.request_context_preparation("r", "W1", "active", (task_id,))
    w.drain()
    for i in range(5):
        _complete_context(coordinator, w, plan, "r", i)
    assert active.message_id in coordinator.inspect_pending_operations().active_message_ids
    assert coordinator.inspect_pending_operations().retired_count == 2

    context = WorkerContext("active", "W1", frozenset({task_id}), 1)
    assert w.send(_prepared_response(plan, "r", context, active, message_id="active-ready")) == EventDisposition.APPLIED


def test_eviction_cannot_enable_pending_correlation_aba_reuse(build_plan):
    _, plan = build_plan("a=1\n")
    _, other = build_plan("b=2\n")
    _, third_plan = build_plan("c=3\n")
    counters = {}
    def ids(kind):
        if kind == "message":
            return "same-base"
        counters[kind] = counters.get(kind, 0) + 1
        return f"{kind}-{counters[kind]}"

    c = Coordinator(id_source=ids)
    c.MAX_PENDING_HISTORY = 1
    w = connect(c, plan, "W1", prepared=False)
    first = c.request_program_preparation("W1", plan)
    w.drain()
    response = p.ProgramPrepared("W1", plan.id, plan.program.id,
                                 message_id="done", correlation_id=first.message_id)
    assert w.send(response) == EventDisposition.APPLIED
    second = c.request_program_preparation("W1", other)
    w.drain()
    # Complete another distinct preparation so first's tombstone is evicted.
    assert w.send(p.ProgramPrepared("W1", other.id, other.program.id,
                                    message_id="done2", correlation_id=second.message_id)) == EventDisposition.APPLIED
    third = c.request_program_preparation("W1", third_plan)
    assert len({first.message_id, second.message_id, third.message_id}) == 3
    with pytest.raises(InvalidWorkerMessage):
        w.send(response)


def test_repeated_attempt_id_from_injected_source_cannot_overwrite_attempt_history(build_plan):
    from coordinator import Coordinator, CoordinatorError, RetryPolicy
    from coordinator.tests.helpers import fail
    from execution import FailureInfo, FailureKind

    _, plan = build_plan("a=1\n")
    counters = {}
    def ids(kind):
        if kind == "attempt":
            return "same-attempt"
        counters[kind] = counters.get(kind, 0) + 1
        return f"{kind}-{counters[kind]}"

    c = Coordinator(id_source=ids, retry_policy=RetryPolicy(max_attempts_per_task=2))
    w = connect(c, plan, "W1")
    c.submit(plan, run_id="r")
    c.schedule("r")
    d1 = dispatch_for(w)
    accept(w, d1); start(w, d1)
    fail(w, d1, FailureInfo(FailureKind.EXECUTION_ERROR, "retryable"))

    with pytest.raises(CoordinatorError, match="attempt identity reused"):
        c.schedule("r")
    task = c.get_task("r", plan.tasks[0].task_id)
    assert task.attempt_ids == ["same-attempt"]
    assert c.get_attempt("r", "same-attempt").identity == d1.attempt


def test_repeated_generated_transfer_identity_cannot_overwrite_transfer_state(build_plan):
    from coordinator import Coordinator, CoordinatorError
    from scheduler import TaskAffinity

    _, plan = build_plan("a=1\nb=2\nc=a+b\n")
    a, b, ctask = plan.tasks
    counters = {}
    def ids(kind):
        if kind == "transfer":
            return "same-transfer"
        if kind == "transfer-attempt":
            return "same-transfer-attempt"
        counters[kind] = counters.get(kind, 0) + 1
        return f"{kind}-{counters[kind]}"

    coord = Coordinator(id_source=ids)
    w1 = connect(coord, plan, "W1", slots=2, port=9001)
    w2 = connect(coord, plan, "W2", slots=1, port=9002)
    w1.drain(); w2.drain()
    coord.submit(plan, run_id="r", affinities=(
        TaskAffinity(a.task_id, required_worker="W1"),
        TaskAffinity(b.task_id, required_worker="W1"),
        TaskAffinity(ctask.task_id, required_worker="W2"),
    ))
    coord.schedule("r")
    for dispatch in [m for m in w1.drain() if isinstance(m, p.TaskDispatch)]:
        accept(w1, dispatch); start(w1, dispatch); succeed(w1, plan, dispatch)
    assert coord.get_task("r", ctask.task_id).status.value == "ready"

    with pytest.raises(CoordinatorError, match="generated transfer identity reused"):
        coord.schedule("r")
    assert coord._transfers == {}
    assert coord.get_task("r", ctask.task_id).current_attempt_id is None


def test_duplicate_response_with_changed_context_capacity_is_conflicting(coordinator, build_plan):
    plan, _, t2, t3 = _shared_plan(build_plan)
    w = connect(coordinator, plan, "W1")
    coordinator.submit(plan, run_id="r")
    command = coordinator.request_context_preparation("r", "W1", "ctx", (t2.task_id, t3.task_id))
    w.drain()
    original = WorkerContext("ctx", "W1", frozenset({t2.task_id, t3.task_id}), 1)
    w.send(_prepared_response(plan, "r", original, command, message_id="first"))
    changed_capacity = WorkerContext("ctx", "W1", frozenset({t2.task_id, t3.task_id}), 2)
    with pytest.raises(InvalidWorkerMessage):
        w.send(_prepared_response(plan, "r", changed_capacity, command, message_id="changed"))
    assert coordinator.build_snapshot("r").contexts == (original,)


def test_same_context_id_may_exist_in_different_runs_with_different_plans(coordinator, build_plan):
    _, plan1 = build_plan("a=1\n")
    _, plan2 = build_plan("b=2\n")
    w = connect(coordinator, plan1, "W1")
    coordinator.submit(plan1, run_id="r1")
    coordinator.submit(plan2, run_id="r2")
    one = coordinator.request_context_preparation("r1", "W1", "ctx", (plan1.tasks[0].task_id,))
    two = coordinator.request_context_preparation("r2", "W1", "ctx", (plan2.tasks[0].task_id,))
    assert one is not None and two is not None and one.plan_id != two.plan_id
    coordinator.validate_state()


def test_validate_state_checks_pending_history_bound_and_active_retired_disjointness(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    w = connect(coordinator, plan, "W1")
    coordinator.submit(plan, run_id="r")
    coordinator.MAX_PENDING_HISTORY = 1
    command = coordinator.request_context_preparation("r", "W1", "ctx", (plan.tasks[0].task_id,))
    request = next(r for r in coordinator._pending_ops.active_requests() if r.message_id == command.message_id)
    coordinator._pending_ops._history[command.message_id] = request
    with pytest.raises(AssertionError, match="both active and retired"):
        coordinator.validate_state()
