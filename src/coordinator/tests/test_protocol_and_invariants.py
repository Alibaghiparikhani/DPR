import random
import pytest
import protocol as p

from coordinator import EventDisposition, InvalidWorkerMessage, TaskStatus
from coordinator.tests.helpers import accept, connect, dispatch_for, start, succeed


def test_real_protocol_encode_frame_fragment_decode_then_handle(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    w = connect(coordinator, plan)
    coordinator.submit(plan, run_id="r")
    coordinator.schedule("r")
    dispatch = dispatch_for(w)
    message = p.TaskAccepted(
        "W1", dispatch.attempt,
        message_id="accepted", correlation_id=dispatch.message_id,
    )
    wire = p.frame_payload(p.encode_message(message))
    decoder = p.FrameDecoder()
    payloads = []
    for i in range(0, len(wire), 3):
        payloads.extend(decoder.feed(wire[i:i+3]))
    assert len(payloads) == 1
    decoded = p.decode_message(payloads[0])
    assert decoded == message
    assert w.send(decoded) == EventDisposition.APPLIED


def test_wrong_worker_identity_rejected(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    w = connect(coordinator, plan)
    coordinator.submit(plan, run_id="r")
    coordinator.schedule("r"); d = dispatch_for(w)
    bad = p.TaskAccepted("OTHER", d.attempt, message_id="bad", correlation_id=d.message_id)
    with pytest.raises(InvalidWorkerMessage):
        w.send(bad)


def test_multi_run_isolation(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    w = connect(coordinator, plan, slots=2)
    coordinator.submit(plan, run_id="A")
    coordinator.submit(plan, run_id="B")
    coordinator.schedule("A"); da = dispatch_for(w)
    coordinator.schedule("B"); db = dispatch_for(w)
    assert da.attempt.task_id == db.attempt.task_id
    assert da.attempt.run_id != db.attempt.run_id
    accept(w, da); start(w, da); succeed(w, plan, da)
    assert coordinator.inspect_run("A").completed_task_ids
    assert not coordinator.inspect_run("B").completed_task_ids
    coordinator.validate_state()


def test_seeded_event_sequence_preserves_invariants(coordinator, build_plan):
    _, plan = build_plan("a=1\nb=2\nc=a+b\n")
    w = connect(coordinator, plan, slots=2)
    coordinator.submit(plan, run_id="r")
    rng = random.Random(81426)
    for _ in range(20):
        run = coordinator.inspect_run("r")
        if run.status.terminal:
            break
        if any(status == TaskStatus.READY for _, status in run.tasks):
            coordinator.schedule("r")
        dispatches = [m for m in w.drain() if isinstance(m, p.TaskDispatch)]
        rng.shuffle(dispatches)
        for d in dispatches:
            accept(w, d)
            if rng.choice([True, False]):
                assert accept(w, d) == EventDisposition.DUPLICATE
            start(w, d)
            succeed(w, plan, d)
            coordinator.validate_state()
    assert coordinator.inspect_run("r").status.terminal
    coordinator.validate_state()


def test_validate_state_detects_task_attempt_status_mismatch(coordinator, build_plan):
    _, plan = build_plan("a=1\n")
    w = connect(coordinator, plan)
    coordinator.submit(plan, run_id="r")
    coordinator.schedule("r")
    dispatch = dispatch_for(w)
    run = coordinator._runs["r"]
    run.tasks[dispatch.attempt.task_id].status = TaskStatus.RUNNING
    with pytest.raises(AssertionError, match="task/attempt active status mismatch"):
        coordinator.validate_state()
