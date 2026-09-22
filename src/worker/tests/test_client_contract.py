from __future__ import annotations

import pytest

import protocol as p
from runtime_security import TlsCredentials, TlsPolicy
from scheduler import WorkerState
from worker import WorkerControlConfig, WorkerControlSession, WorkerReconnectPolicy


def tls():
    return TlsPolicy(TlsCredentials("node.pem", "node.key", "ca.pem"))


def test_reconnect_policy_is_explicitly_bounded():
    assert WorkerReconnectPolicy().max_attempts is None
    with pytest.raises(ValueError):
        WorkerReconnectPolicy(-1, 0)
    with pytest.raises(ValueError):
        WorkerReconnectPolicy(1, -0.1)


def test_worker_config_binds_node_endpoint_and_state_identity():
    with pytest.raises(ValueError, match="match node_id"):
        WorkerControlConfig(
            "W1", b"x" * 32, "127.0.0.1", 1234, "localhost",
            p.WorkerEndpoint("W2", "127.0.0.1", 9002), WorkerState("W1", 1), tls(),
        )


def test_worker_config_requires_real_psk_and_connect_port():
    with pytest.raises(ValueError, match="32 bytes"):
        WorkerControlConfig(
            "W1", b"short", "127.0.0.1", 1234, "localhost",
            p.WorkerEndpoint("W1", "127.0.0.1", 9001), WorkerState("W1", 1), tls(),
        )
    with pytest.raises(ValueError, match="1..65535"):
        WorkerControlConfig(
            "W1", b"x" * 32, "127.0.0.1", 0, "localhost",
            p.WorkerEndpoint("W1", "127.0.0.1", 9001), WorkerState("W1", 1), tls(),
        )


def test_worker_control_session_retains_exact_accepted_session_id():
    session = WorkerControlSession("W1", "session-17")
    assert session.worker_id == "W1"
    assert session.session_id == "session-17"
