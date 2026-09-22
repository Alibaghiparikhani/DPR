from __future__ import annotations

import protocol as p
import pytest

from coordinator import Coordinator, RetryPolicy, RunStatus, TaskStatus, AttemptStatus
from coordinator.tests.helpers import connect, worker_state, accept, start
from execution import FailureInfo, FailureKind, TaskFailure


class FailingIds:
    def __init__(self):
        self.counts: dict[str, int] = {}
        self.fail_at: tuple[str, int] | None = None

    def __call__(self, kind: str) -> str:
        n = self.counts.get(kind, 0) + 1
        self.counts[kind] = n
        if self.fail_at == (kind, n):
            raise RuntimeError(f"{kind} provider failed at {n}")
        return f"{kind}-{n}"

    def fail_next(self, kind: str, offset: int = 1) -> None:
        self.fail_at = (kind, self.counts.get(kind, 0) + offset)


def _hello(plan, worker_id: str, *, port: int = 9000, slots: int = 2) -> p.WorkerHello:
    return p.WorkerHello(
        worker=worker_state(plan, worker_id, slots=slots),
        endpoint=p.WorkerEndpoint(worker_id, f"{worker_id.lower()}.lan", port),
        supported_versions=(p.PROTOCOL_VERSION,),
        message_id=f"hello-{worker_id}-{port}",
    )


def _run_shape(c: Coordinator, run_id: str):
    run = c._runs[run_id]
    return (
        run.status,
        tuple((tid, t.status, t.current_attempt_id, t.failure) for tid, t in sorted(run.tasks.items())),
        tuple((aid, a.status, a.cancel_message_id, a.phase_since, a.failure)
              for aid, a in sorted(run.attempts.items())),
        c._revision,
    )


def _start_independent(c: Coordinator, plan, *, count: int):
    w = connect(c, plan, "W1", slots=count)
    c.submit(plan, run_id="r")
    c.schedule("r")
    dispatches = [m for m in w.drain() if isinstance(m, p.TaskDispatch)]
    assert len(dispatches) == count
    for d in dispatches:
        accept(w, d)
        start(w, d)
    w.drain()
    return w, dispatches


def _python_failure(attempt, *, message_id="failed") -> p.TaskFailed:
    return p.TaskFailed(
        worker_id="W1",
        result=TaskFailure(
            attempt,
            FailureInfo(FailureKind.PYTHON_EXCEPTION, "boom", exception_type="ValueError"),
        ),
        message_id=message_id,
        correlation_id=None,  # filled by caller
    )


def test_initial_registration_message_id_failure_is_atomic(build_plan):
    _, plan = build_plan("a=1")
    ids = FailingIds(); ids.fail_next("message")
    c = Coordinator(id_source=ids)
    before = (dict(c._workers), dict(c._generations), c._revision, c._membership_revision)
    with pytest.raises(RuntimeError, match="message provider failed"):
        c.register_worker(_hello(plan, "W1"), now=1.0)
    assert (dict(c._workers), dict(c._generations), c._revision, c._membership_revision) == before
    c.validate_state()


def test_reconnect_accept_id_failure_preserves_old_live_session(build_plan):
    _, plan = build_plan("a=1")
    ids = FailingIds()
    c = Coordinator(id_source=ids)
    old = connect(c, plan, "W1", slots=1)
    c.submit(plan, run_id="r")
    c.schedule("r")
    dispatch = next(m for m in old.drain() if isinstance(m, p.TaskDispatch))
    accept(old, dispatch); start(old, dispatch); old.drain()
    before = (_run_shape(c, "r"), c.inspect_worker("W1"), c._membership_revision)

    # Reconnect consumes a session ID and then must create WorkerAccepted. The
    # failure must happen before the old authoritative generation is retired.
    ids.fail_next("message")
    with pytest.raises(RuntimeError, match="message provider failed"):
        c.register_worker(_hello(plan, "W1", port=9001, slots=1), now=2.0)

    assert (_run_shape(c, "r"), c.inspect_worker("W1"), c._membership_revision) == before
    assert c._workers["W1"].handle == old.handle
    c.validate_state()


def test_registration_membership_broadcast_id_failure_is_atomic(build_plan):
    _, plan = build_plan("a=1")
    ids = FailingIds()
    c = Coordinator(id_source=ids)
    w1 = connect(c, plan, "W1", port=9001); w2 = connect(c, plan, "W2", port=9002)
    w1.drain(); w2.drain()
    before = (
        tuple(sorted(c._workers)), dict(c._generations), c._revision, c._membership_revision,
        tuple(c._workers["W1"].outbox), tuple(c._workers["W2"].outbox),
    )
    # New registration needs WorkerAccepted plus one MembershipUpdate per old member.
    # Fail on the second broadcast update, after earlier IDs have been generated.
    ids.fail_next("message", offset=3)
    with pytest.raises(RuntimeError, match="message provider failed"):
        c.register_worker(_hello(plan, "W3", port=9003), now=3.0)
    after = (
        tuple(sorted(c._workers)), dict(c._generations), c._revision, c._membership_revision,
        tuple(c._workers["W1"].outbox), tuple(c._workers["W2"].outbox),
    )
    assert after == before
    c.validate_state()


def test_heartbeat_ack_id_failure_is_atomic(build_plan):
    _, plan = build_plan("a=1")
    ids = FailingIds(); c = Coordinator(id_source=ids)
    w = connect(c, plan, "W1"); w.drain()
    worker = c._workers["W1"]
    before = (worker.last_seen, worker.last_sequence, worker.reported, c._revision, tuple(worker.outbox))
    ids.fail_next("message")
    heartbeat = p.Heartbeat(worker_state(plan, "W1", slots=3), 1, message_id="hb-1")
    with pytest.raises(RuntimeError, match="message provider failed"):
        w.send(heartbeat, now=5.0)
    worker = c._workers["W1"]
    assert (worker.last_seen, worker.last_sequence, worker.reported, c._revision, tuple(worker.outbox)) == before
    c.validate_state()


def test_run_failure_clock_failure_is_atomic_with_live_sibling(build_plan):
    _, plan = build_plan("a=1\nb=2")
    fail_clock = False
    def clock():
        if fail_clock:
            raise RuntimeError("clock provider failed")
        return 0.0
    c = Coordinator(clock=clock, retry_policy=RetryPolicy(max_attempts_per_task=1))
    w, dispatches = _start_independent(c, plan, count=2)
    before = (_run_shape(c, "r"), tuple(w.handle for _ in [0]), tuple(c._workers["W1"].outbox))
    fail_clock = True
    victim = dispatches[0]
    msg = p.TaskFailed(
        "W1", TaskFailure(victim.attempt,
            FailureInfo(FailureKind.PYTHON_EXCEPTION, "boom", exception_type="ValueError")),
        message_id="task-failed", correlation_id=victim.message_id,
    )
    # Explicit now bypasses handle_message's clock so the injected failure occurs
    # specifically while staging the terminal run transition.
    with pytest.raises(RuntimeError, match="clock provider failed"):
        w.send(msg, now=10.0)
    assert (_run_shape(c, "r"), tuple(w.handle for _ in [0]), tuple(c._workers["W1"].outbox)) == before
    c.validate_state()


@pytest.mark.parametrize("cancel_position", [1, 2])
def test_run_failure_cancel_id_failure_is_atomic_across_siblings(build_plan, cancel_position):
    _, plan = build_plan("a=1\nb=2\nc=3")
    ids = FailingIds()
    c = Coordinator(id_source=ids, retry_policy=RetryPolicy(max_attempts_per_task=1))
    w, dispatches = _start_independent(c, plan, count=3)
    before = (_run_shape(c, "r"), tuple(c._workers["W1"].outbox))
    ids.fail_next("message", offset=cancel_position)
    victim = dispatches[0]
    msg = p.TaskFailed(
        "W1", TaskFailure(victim.attempt,
            FailureInfo(FailureKind.PYTHON_EXCEPTION, "boom", exception_type="ValueError")),
        message_id=f"failed-{cancel_position}", correlation_id=victim.message_id,
    )
    with pytest.raises(RuntimeError, match="message provider failed"):
        w.send(msg, now=10.0)
    assert (_run_shape(c, "r"), tuple(c._workers["W1"].outbox)) == before
    c.validate_state()


def test_offline_heartbeat_nested_failure_staging_is_atomic(build_plan):
    from dataclasses import replace

    _, plan = build_plan("a=1\nb=2")
    ids = FailingIds()
    c = Coordinator(id_source=ids, retry_policy=RetryPolicy(max_attempts_per_task=1))
    w1 = connect(c, plan, "W1", slots=1, port=9011)
    w2 = connect(c, plan, "W2", slots=1, port=9012)
    w1.drain(); w2.drain()
    c.submit(plan, run_id="r")
    c.schedule("r")
    dispatches = {}
    for w in (w1, w2):
        ds = [m for m in w.drain() if isinstance(m, p.TaskDispatch)]
        assert len(ds) == 1
        dispatches[w.handle.worker_id] = ds[0]
        accept(w, ds[0]); start(w, ds[0]); w.drain()

    before = (
        _run_shape(c, "r"), c.inspect_worker("W1"), c.inspect_worker("W2"),
        tuple(c._workers["W1"].outbox), tuple(c._workers["W2"].outbox),
    )
    # First new message ID is HeartbeatAck; second is sibling cancellation staged
    # because losing W1 exhausts the first task's retry policy and fails the run.
    ids.fail_next("message", offset=2)
    offline = replace(worker_state(plan, "W1", slots=1), online=False, accepting_work=False)
    with pytest.raises(RuntimeError, match="message provider failed"):
        w1.send(p.Heartbeat(offline, 1, message_id="offline"), now=10.0)
    after = (
        _run_shape(c, "r"), c.inspect_worker("W1"), c.inspect_worker("W2"),
        tuple(c._workers["W1"].outbox), tuple(c._workers["W2"].outbox),
    )
    assert after == before
    c.validate_state()


def test_worker_goodbye_membership_id_failure_is_atomic(build_plan):
    _, plan = build_plan("a=1")
    ids = FailingIds(); c = Coordinator(id_source=ids)
    w1 = connect(c, plan, "W1", port=9021); w2 = connect(c, plan, "W2", port=9022)
    w1.drain(); w2.drain()
    before = (
        tuple(sorted(c._workers)), c.inspect_worker("W1"), c.inspect_worker("W2"),
        c._revision, c._membership_revision,
        tuple(c._workers["W1"].outbox), tuple(c._workers["W2"].outbox),
    )
    ids.fail_next("message")
    with pytest.raises(RuntimeError, match="message provider failed"):
        w1.send(p.WorkerGoodbye("W1", "bye", message_id="bye"), now=5.0)
    after = (
        tuple(sorted(c._workers)), c.inspect_worker("W1"), c.inspect_worker("W2"),
        c._revision, c._membership_revision,
        tuple(c._workers["W1"].outbox), tuple(c._workers["W2"].outbox),
    )
    assert after == before
    c.validate_state()


def test_reconnect_nested_run_failure_id_failure_preserves_old_cluster(build_plan):
    _, plan = build_plan("a=1\nb=2")
    ids = FailingIds()
    c = Coordinator(id_source=ids, retry_policy=RetryPolicy(max_attempts_per_task=1))
    w1 = connect(c, plan, "W1", slots=1, port=9031)
    w2 = connect(c, plan, "W2", slots=1, port=9032)
    w1.drain(); w2.drain()
    c.submit(plan, run_id="r")
    c.schedule("r")
    for w in (w1, w2):
        d = next(m for m in w.drain() if isinstance(m, p.TaskDispatch))
        accept(w, d); start(w, d); w.drain()
    before = (
        _run_shape(c, "r"), c.inspect_worker("W1"), c.inspect_worker("W2"),
        c._membership_revision, tuple(c._workers["W2"].outbox),
    )
    # Registration pre-stages: accepted, loss membership, admission membership;
    # the fourth message ID is the sibling cancellation needed by old-session loss.
    ids.fail_next("message", offset=4)
    with pytest.raises(RuntimeError, match="message provider failed"):
        c.register_worker(_hello(plan, "W1", port=9033, slots=1), now=11.0)
    after = (
        _run_shape(c, "r"), c.inspect_worker("W1"), c.inspect_worker("W2"),
        c._membership_revision, tuple(c._workers["W2"].outbox),
    )
    assert after == before
    assert c._workers["W1"].handle == w1.handle
    c.validate_state()


def test_context_loss_provider_failure_does_not_publish_unavailable_fact(build_plan):
    from scheduler import TaskAffinity, WorkerContext

    _, plan = build_plan("print(1)\na=1")
    ids = FailingIds(); c = Coordinator(id_source=ids)
    w = connect(c, plan, "W1", slots=2)
    task_ids = tuple(t.task_id for t in plan.tasks)
    context = WorkerContext("ctx", "W1", frozenset(task_ids), 2)
    c.submit(
        plan, run_id="r",
        affinities=tuple(TaskAffinity(tid, required_worker="W1", context_id="ctx") for tid in task_ids),
        contexts=(context,),
    )
    c.schedule("r")
    dispatches = [m for m in w.drain() if isinstance(m, p.TaskDispatch)]
    assert len(dispatches) == 1
    for d in dispatches:
        accept(w, d); start(w, d)
    w.drain()
    run = c._runs["r"]
    before = (
        _run_shape(c, "r"), dict(run.contexts), frozenset(run.unavailable_context_ids),
        tuple(c._workers["W1"].outbox), c._workers["W1"].last_seen,
    )
    ids.fail_next("message")
    with pytest.raises(RuntimeError, match="message provider failed"):
        w.send(p.ContextUnavailable(
            "W1", plan.id, "r", "ctx", "gone", message_id="ctx-gone",
        ), now=20.0)
    run = c._runs["r"]
    after = (
        _run_shape(c, "r"), dict(run.contexts), frozenset(run.unavailable_context_ids),
        tuple(c._workers["W1"].outbox), c._workers["W1"].last_seen,
    )
    assert after == before
    c.validate_state()


def test_last_replica_loss_provider_failure_is_atomic(build_plan):
    from scheduler import DataForm

    _, plan = build_plan("a=1\nb=a+1\nc=2")
    ids = FailingIds(); c = Coordinator(id_source=ids)
    w = connect(c, plan, "W1", slots=2)
    c.submit(plan, run_id="r")
    c.schedule("r")
    dispatches = [m for m in w.drain() if isinstance(m, p.TaskDispatch)]
    assert len(dispatches) == 2
    by_task = {d.attempt.task_id: d for d in dispatches}
    producer = plan.tasks[0]
    producer_dispatch = by_task[producer.task_id]
    sibling_dispatch = next(d for d in dispatches if d is not producer_dispatch)
    accept(w, producer_dispatch); start(w, producer_dispatch)
    accept(w, sibling_dispatch); start(w, sibling_dispatch)
    from coordinator.tests.helpers import succeed
    succeed(w, plan, producer_dispatch)
    w.drain()
    output_id = producer.outputs[0].id
    data = p.DataReference(plan.id, "r", output_id, DataForm.IMMUTABLE_VALUE)
    assert c._locations.has(data, "W1", w.handle.generation)
    before = (
        _run_shape(c, "r"), tuple(c.data_locations("r")),
        tuple(c._workers["W1"].outbox), c._workers["W1"].last_seen,
    )
    ids.fail_next("message")
    with pytest.raises(RuntimeError, match="message provider failed"):
        w.send(p.ObjectUnavailable("W1", data, "evicted", message_id="gone"), now=30.0)
    after = (
        _run_shape(c, "r"), tuple(c.data_locations("r")),
        tuple(c._workers["W1"].outbox), c._workers["W1"].last_seen,
    )
    assert after == before
    assert c._locations.has(data, "W1", w.handle.generation)
    c.validate_state()


def test_transfer_failure_provider_failure_is_atomic_with_live_sibling(build_plan):
    """A transfer failure that would fail the run must stage sibling cleanup first."""
    from dataclasses import replace
    from coordinator.tests.helpers import succeed

    _, plan = build_plan("a=1\nb=a+1\nc=2")
    ids = FailingIds()
    c = Coordinator(id_source=ids, retry_policy=RetryPolicy(max_attempts_per_task=1))
    w1 = connect(c, plan, "W1", slots=1, port=9041)
    w2 = connect(c, plan, "W2", slots=1, port=9042)
    w3 = connect(c, plan, "W3", slots=1, port=9043)
    w1.drain(); w2.drain(); w3.drain()

    c.submit(plan, run_id="r")
    c.schedule("r")
    initial: dict[str, p.TaskDispatch] = {}
    for w in (w1, w2, w3):
        ds = [m for m in w.drain() if isinstance(m, p.TaskDispatch)]
        for d in ds:
            initial[w.handle.worker_id] = d
    assert len(initial) == 2

    # The deterministic scheduler places the two independent roots on W1/W2.
    d1 = initial["W1"]
    d2 = initial["W2"]
    accept(w1, d1); start(w1, d1)
    accept(w2, d2); start(w2, d2)
    w1.drain(); w2.drain()

    producer_task_id = plan.tasks[0].task_id
    if d1.attempt.task_id == producer_task_id:
        producer_worker, producer_dispatch, sibling_worker = w1, d1, w2
    else:
        producer_worker, producer_dispatch, sibling_worker = w2, d2, w1
    succeed(producer_worker, plan, producer_dispatch)
    producer_worker.drain()

    # Keep the producer as the source but stop it from receiving new compute work;
    # the other root remains physically running, so terminal failure needs a
    # sibling cancellation message. W3 becomes the remote consumer.
    state = replace(
        worker_state(plan, producer_worker.handle.worker_id, slots=1),
        accepting_work=False,
    )
    producer_worker.send(p.Heartbeat(state, 1, message_id="producer-no-new-work"), now=5.0)
    producer_worker.drain()

    c.schedule("r")
    prepare = next(m for m in w3.drain() if isinstance(m, p.PrepareReceive))
    w3.send(p.ReceiveReady(
        "W3", prepare.transfer, message_id="ready-transfer",
        correlation_id=prepare.message_id,
    ), now=6.0)
    source_worker = w1 if prepare.transfer.source_worker_id == "W1" else w2
    request = next(m for m in source_worker.drain() if isinstance(m, p.TransferRequest))

    before = (
        _run_shape(c, "r"),
        c.get_transfer(request.transfer.transfer_id, request.transfer.transfer_attempt_id),
        tuple(c._workers["W1"].outbox),
        tuple(c._workers["W2"].outbox),
        tuple(c._workers["W3"].outbox),
    )
    # Retry budget for the waiting consumer is exhausted. Failing the transfer
    # therefore fails the run and needs to cancel the still-running root. The
    # cancellation ID must be obtained before transfer/run state is mutated.
    ids.fail_next("message")
    with pytest.raises(RuntimeError, match="message provider failed"):
        source_worker.send(p.TransferFailed(
            source_worker.handle.worker_id,
            request.transfer,
            p.TransferFailureCode.IO_ERROR,
            "source failed",
            message_id="transfer-failed",
            correlation_id=request.message_id,
        ), now=7.0)

    after = (
        _run_shape(c, "r"),
        c.get_transfer(request.transfer.transfer_id, request.transfer.transfer_attempt_id),
        tuple(c._workers["W1"].outbox),
        tuple(c._workers["W2"].outbox),
        tuple(c._workers["W3"].outbox),
    )
    assert after == before
    # The sibling is deliberately still active; this confirms the failed local
    # transition neither lost nor silently cancelled it.
    sibling_id = d2.attempt.attempt_id if sibling_worker is w2 else d1.attempt.attempt_id
    assert c.get_attempt("r", sibling_id).status == AttemptStatus.RUNNING
    c.validate_state()
