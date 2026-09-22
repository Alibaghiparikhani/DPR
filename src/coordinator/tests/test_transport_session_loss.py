from coordinator.tests.helpers import connect


def test_disconnect_session_retires_exact_current_transport_session(coordinator, diamond):
    _, plan = diamond
    worker = connect(coordinator, plan, "W1")
    assert coordinator.disconnect_session(worker.handle, reason="socket EOF") is True
    assert coordinator.disconnect_session(worker.handle, reason="late duplicate EOF") is False


def test_stale_connection_teardown_cannot_retire_replacement_generation(coordinator, diamond):
    _, plan = diamond
    old = connect(coordinator, plan, "W1", port=9000)
    new = connect(coordinator, plan, "W1", port=9001)
    assert new.handle.generation == old.handle.generation + 1
    assert coordinator.disconnect_session(old.handle, reason="old socket finally closed") is False
    assert coordinator.inspect_worker("W1").handle == new.handle
