import pytest
import protocol as p

from coordinator import EventDisposition, StaleWorkerSession
from coordinator.tests.helpers import connect, worker_state


def test_register_and_heartbeat_updates_state(coordinator, diamond):
    _, plan = diamond
    w = connect(coordinator, plan)
    heartbeat = p.Heartbeat(
        worker=worker_state(plan, "W1", slots=3), sequence=1, message_id="hb-1"
    )
    assert w.send(heartbeat, now=5) == EventDisposition.APPLIED
    ack, = w.drain()
    assert isinstance(ack, p.HeartbeatAck)
    assert ack.correlation_id == "hb-1"
    assert coordinator.inspect_worker("W1").state.total_slots == 3
    coordinator.validate_state()


def test_duplicate_and_stale_heartbeat(coordinator, diamond):
    _, plan = diamond
    w = connect(coordinator, plan)
    hb = p.Heartbeat(worker_state(plan, "W1"), 5, message_id="hb5")
    assert w.send(hb) == EventDisposition.APPLIED
    w.drain()
    dup = p.Heartbeat(worker_state(plan, "W1"), 5, message_id="hb5b")
    assert w.send(dup) == EventDisposition.DUPLICATE
    w.drain()
    old = p.Heartbeat(worker_state(plan, "W1"), 4, message_id="hb4")
    assert w.send(old) == EventDisposition.STALE


def test_reconnect_invalidates_old_session(coordinator, diamond):
    _, plan = diamond
    old = connect(coordinator, plan, "W1")
    new = connect(coordinator, plan, "W1", port=9001)
    assert new.handle.generation == old.handle.generation + 1
    with pytest.raises(StaleWorkerSession):
        old.send(p.Heartbeat(worker_state(plan, "W1"), 1, message_id="late"))


def test_expiry_is_deterministic_without_sleep(coordinator, diamond):
    _, plan = diamond
    connect(coordinator, plan, "W1", now=10)
    assert coordinator.expire_workers(now=39) == ()
    assert coordinator.expire_workers(now=41) == ("W1",)


def test_goodbye_invalidates_session(coordinator, diamond):
    _, plan = diamond
    w = connect(coordinator, plan)
    assert w.send(p.WorkerGoodbye("W1", "bye", message_id="bye")) == EventDisposition.APPLIED
    with pytest.raises(StaleWorkerSession):
        w.send(p.Heartbeat(worker_state(plan, "W1"), 2, message_id="x"))


def test_program_preparation_updates_schedulable_capability(coordinator, diamond):
    _, plan = diamond
    w = connect(coordinator, plan, prepared=False)
    cmd = coordinator.request_program_preparation("W1", plan)
    assert w.drain() == (cmd,)
    result = p.ProgramPrepared(
        "W1", plan.id, plan.program.id,
        message_id="prepared", correlation_id=cmd.message_id,
    )
    assert w.send(result) == EventDisposition.APPLIED
    assert plan.program.id in coordinator.inspect_worker("W1").state.prepared_program_ids
