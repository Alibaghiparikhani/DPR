from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

import protocol as p
from coordinator import Coordinator, RunStatus, SQLiteRunHistoryStore
from dag_runtime.dag_engine import analyze_source
from execution import lower_dag
from networking import (
    CoordinatorClient, CoordinatorClientConfig, CoordinatorNetworkService, TransportLimits,
)
from program_package import PackageCache, PackageRepository, build_package
from runtime_security import NodeAuthenticator, TlsCredentials, TlsPolicy
from scheduler import WorkerState
from worker import WorkerControlClient, WorkerControlConfig, WorkerExecutionRuntime, WorkerReconnectPolicy

pytestmark = pytest.mark.asyncio


def _secret(node: str) -> bytes:
    return (node.encode() * 32)[:32]


def _server_tls(certs):
    return TlsPolicy(TlsCredentials(certs.server_cert, certs.server_key, certs.ca))


def _node_tls(certs, node: str):
    return TlsPolicy(TlsCredentials(certs.cert(node), certs.key(node), certs.ca))


async def eventually(predicate, timeout=6.0):
    async with asyncio.timeout(timeout):
        while True:
            value = predicate()
            if value:
                return value
            await asyncio.sleep(.01)


async def test_authenticated_client_submits_real_package_and_runtime_executes(tmp_path: Path, tls_certs):
    root = tmp_path / "program"
    root.mkdir()
    source = "answer = 20 + 22\n"
    (root / "main.py").write_text(source, encoding="utf-8")
    # Keep this package larger than one 32 KiB binary chunk so the real
    # client->coordinator and coordinator->worker paths exercise multi-frame
    # chunking under the protocol decoder's 64 KiB per-JSON-string budget.
    import hashlib
    (root / "asset.bin").write_bytes(b"".join(
        hashlib.sha256(str(i).encode("ascii")).digest() for i in range(2048)
    ))
    artifact = build_package(root)
    assert len(artifact.archive_bytes) > 32 * 1024
    plan = lower_dag(analyze_source(source, filename="main.py"), environment_id="env-test", package_id=artifact.package_id)

    coordinator = Coordinator(heartbeat_timeout=1.0, history_store=SQLiteRunHistoryStore(tmp_path / "history-client.sqlite3"))
    repository = PackageRepository()
    service = CoordinatorNetworkService(
        coordinator,
        tls_policy=_server_tls(tls_certs),
        authenticator=NodeAuthenticator({"W1": _secret("W1"), "W4": _secret("W4")}),
        package_repository=repository, client_node_ids=frozenset({"W4"}),
        limits=TransportLimits(handshake_timeout=2, write_timeout=2, maintenance_interval=.01),
    )
    await service.start()
    state = WorkerState("W1", 1, environment_ids=frozenset({"env-test"}))
    runtime = WorkerExecutionRuntime("W1", state, PackageCache(tmp_path / "cache-W1"))
    worker = WorkerControlClient(
        WorkerControlConfig(
            "W1", _secret("W1"), "127.0.0.1", service.listening_port, "localhost",
            p.WorkerEndpoint("W1", "127.0.0.1", 9401), state, _node_tls(tls_certs, "W1"),
            heartbeat_interval=.05, reconnect=WorkerReconnectPolicy(0, 0),
            limits=TransportLimits(handshake_timeout=2, write_timeout=2, maintenance_interval=.01),
        ), runtime=runtime,
    )
    worker_task = asyncio.create_task(worker.run())
    await asyncio.wait_for(worker.active.wait(), 3)
    client = CoordinatorClient(CoordinatorClientConfig(
        "W4", _secret("W4"), "127.0.0.1", service.listening_port, "localhost",
        _node_tls(tls_certs, "W4"),
        TransportLimits(handshake_timeout=2, write_timeout=2, maintenance_interval=.01),
    ))
    try:
        async with client:
            submitted = await client.submit_package(
                run_id="cli-run", environment_id="env-test", entrypoint="main.py",
                plan_id=plan.id, package_id=artifact.package_id, archive_bytes=artifact.archive_bytes,
            )
            assert submitted.plan_id == plan.id
            async with asyncio.timeout(5):
                while True:
                    summary = await client.run_status("cli-run", include_tasks=False)
                    if summary.status == "succeeded":
                        break
                    await asyncio.sleep(.01)
            assert summary.status == "succeeded"
            assert summary.task_count == 1
            assert summary.tasks == ()
            assert summary.tasks_truncated is True
            view = await client.run_status("cli-run")
            assert view.status == "succeeded"
            assert view.task_count == 1
            assert view.tasks[0].status == "committed"
            cluster = await client.cluster_status()
            assert cluster.workers[0].worker_id == "W1"
            # F4 pruning is asynchronous relative to the status response.  The
            # durable per-run status above is the contract; cluster.run_ids may
            # still contain the terminal run for a short maintenance interval.
            assert repository.get(artifact.package_id) is not None
            # Terminal CLI-driver state is transient; maintenance must retire it
            # instead of scanning historical runs forever.
            await eventually(lambda: "cli-run" not in service._managed_runs)
    finally:
        await worker.stop()
        await worker_task
        await service.stop()


async def test_client_cancel_drives_real_coordinator_cancellation(tmp_path: Path, tls_certs):
    root = tmp_path / "program2"
    root.mkdir()
    source = "import time\ntime.sleep(30)\n"
    (root / "main.py").write_text(source, encoding="utf-8")
    artifact = build_package(root)
    plan = lower_dag(analyze_source(source, filename="main.py"), environment_id="env-test", package_id=artifact.package_id)

    coordinator = Coordinator(heartbeat_timeout=1.0, history_store=SQLiteRunHistoryStore(tmp_path / "history-cancel.sqlite3"))
    repository = PackageRepository()
    service = CoordinatorNetworkService(
        coordinator, tls_policy=_server_tls(tls_certs),
        authenticator=NodeAuthenticator({"W1": _secret("W1"), "W4": _secret("W4")}),
        package_repository=repository, client_node_ids=frozenset({"W4"}),
        limits=TransportLimits(handshake_timeout=2, write_timeout=2, maintenance_interval=.01),
    )
    await service.start()
    state = WorkerState("W1", 1, environment_ids=frozenset({"env-test"}))
    runtime = WorkerExecutionRuntime("W1", state, PackageCache(tmp_path / "cache2-W1"))
    worker = WorkerControlClient(
        WorkerControlConfig(
            "W1", _secret("W1"), "127.0.0.1", service.listening_port, "localhost",
            p.WorkerEndpoint("W1", "127.0.0.1", 9402), state, _node_tls(tls_certs, "W1"),
            heartbeat_interval=.05, reconnect=WorkerReconnectPolicy(0, 0),
            limits=TransportLimits(handshake_timeout=2, write_timeout=2, maintenance_interval=.01),
        ), runtime=runtime,
    )
    worker_task = asyncio.create_task(worker.run())
    await asyncio.wait_for(worker.active.wait(), 3)
    try:
        async with CoordinatorClient(CoordinatorClientConfig(
            "W4", _secret("W4"), "127.0.0.1", service.listening_port, "localhost",
            _node_tls(tls_certs, "W4"), TransportLimits(handshake_timeout=2, write_timeout=2, maintenance_interval=.01),
        )) as client:
            await client.submit_package(
                run_id="cancel-run", environment_id="env-test", entrypoint="main.py",
                plan_id=plan.id, package_id=artifact.package_id, archive_bytes=artifact.archive_bytes,
            )
            await eventually(lambda: bool(runtime.active_attempt_ids))
            response = await client.cancel_run("cancel-run")
            assert response.status in {"cancelling", "cancelled"}
            async with asyncio.timeout(5):
                while True:
                    status = await client.run_status("cancel-run", include_tasks=False)
                    if status.status == "cancelled":
                        break
                    await asyncio.sleep(.01)
            await eventually(lambda: not runtime.active_attempt_ids)
    finally:
        await worker.stop()
        await worker_task
        await service.stop()

async def test_client_rejects_verified_package_with_wrong_plan_identity_without_admission(tmp_path: Path, tls_certs):
    from networking import ClientOperationError

    root = tmp_path / "bad-plan-program"
    root.mkdir()
    source = "answer = 6 * 7\n"
    (root / "main.py").write_text(source, encoding="utf-8")
    artifact = build_package(root)
    coordinator = Coordinator()
    repository = PackageRepository()
    service = CoordinatorNetworkService(
        coordinator, tls_policy=_server_tls(tls_certs),
        authenticator=NodeAuthenticator({"W4": _secret("W4")}),
        package_repository=repository, client_node_ids=frozenset({"W4"}),
        limits=TransportLimits(handshake_timeout=2, write_timeout=2, maintenance_interval=.01),
    )
    await service.start()
    try:
        async with CoordinatorClient(CoordinatorClientConfig(
            "W4", _secret("W4"), "127.0.0.1", service.listening_port, "localhost",
            _node_tls(tls_certs, "W4"),
            TransportLimits(handshake_timeout=2, write_timeout=2, maintenance_interval=.01),
        )) as client:
            with pytest.raises(ClientOperationError, match="plan identity"):
                await client.submit_package(
                    run_id="bad-plan", environment_id="env-test", entrypoint="main.py",
                    plan_id="0" * 64, package_id=artifact.package_id,
                    archive_bytes=artifact.archive_bytes,
                )
        assert coordinator.run_ids() == ()
        assert repository.get(artifact.package_id) is None
        assert service._active_submissions == 0
    finally:
        await service.stop()


async def test_client_submission_limit_is_fail_closed_and_disconnect_releases_slot(tmp_path: Path, tls_certs):
    import hashlib

    root = tmp_path / "submission-limit"
    root.mkdir()
    source = "x = 1\n"
    (root / "main.py").write_text(source, encoding="utf-8")
    artifact = build_package(root)
    plan = lower_dag(
        analyze_source(source, filename="main.py"),
        environment_id="env-test", package_id=artifact.package_id,
    )
    service = CoordinatorNetworkService(
        Coordinator(), tls_policy=_server_tls(tls_certs),
        authenticator=NodeAuthenticator({"W4": _secret("W4")}),
        package_repository=PackageRepository(), max_active_submissions=1,
        client_node_ids=frozenset({"W4"}),
        limits=TransportLimits(handshake_timeout=2, write_timeout=2, maintenance_interval=.01),
    )
    await service.start()
    config = CoordinatorClientConfig(
        "W4", _secret("W4"), "127.0.0.1", service.listening_port, "localhost",
        _node_tls(tls_certs, "W4"),
        TransportLimits(handshake_timeout=2, write_timeout=2, maintenance_interval=.01),
    )
    first, second = CoordinatorClient(config), CoordinatorClient(config)
    await first.connect()
    await second.connect()
    try:
        start1 = p.RunSubmitStart(
            client_id="W4", run_id="held", environment_id="env-test", entrypoint="main.py",
            plan_id=plan.id, package_id=artifact.package_id,
            archive_size=len(artifact.archive_bytes),
            archive_sha256=hashlib.sha256(artifact.archive_bytes).hexdigest(),
            message_id="held-start",
        )
        await first._write(start1)
        await eventually(lambda: service._active_submissions == 1)
        start2 = p.RunSubmitStart(
            client_id="W4", run_id="rejected", environment_id="env-test", entrypoint="main.py",
            plan_id=plan.id, package_id=artifact.package_id,
            archive_size=len(artifact.archive_bytes),
            archive_sha256=hashlib.sha256(artifact.archive_bytes).hexdigest(),
            message_id="rejected-start",
        )
        await second._write(start2)
        response = await second._read()
        assert isinstance(response, p.ClientOperationFailed)
        assert response.code == "backpressure"
        assert service._active_submissions == 1
        await first.close()
        await eventually(lambda: service._active_submissions == 0)
    finally:
        await first.close()
        await second.close()
        await service.stop()


async def test_authenticated_client_cannot_inject_worker_control_messages(tmp_path: Path, tls_certs):
    coordinator = Coordinator(heartbeat_timeout=1.0)
    service = CoordinatorNetworkService(
        coordinator, tls_policy=_server_tls(tls_certs),
        authenticator=NodeAuthenticator({"W1": _secret("W1"), "W4": _secret("W4")}),
        package_repository=PackageRepository(), client_node_ids=frozenset({"W4"}),
        limits=TransportLimits(handshake_timeout=2, write_timeout=2, maintenance_interval=.01),
    )
    await service.start()
    state = WorkerState("W1", 1, environment_ids=frozenset({"env-test"}))
    worker = WorkerControlClient(WorkerControlConfig(
        "W1", _secret("W1"), "127.0.0.1", service.listening_port, "localhost",
        p.WorkerEndpoint("W1", "127.0.0.1", 9403), state, _node_tls(tls_certs, "W1"),
        heartbeat_interval=.05, reconnect=WorkerReconnectPolicy(0, 0),
        limits=TransportLimits(handshake_timeout=2, write_timeout=2, maintenance_interval=.01),
    ))
    worker_task = asyncio.create_task(worker.run())
    await asyncio.wait_for(worker.active.wait(), 3)
    client = CoordinatorClient(CoordinatorClientConfig(
        "W4", _secret("W4"), "127.0.0.1", service.listening_port, "localhost",
        _node_tls(tls_certs, "W4"),
        TransportLimits(handshake_timeout=2, write_timeout=2, maintenance_interval=.01),
    ))
    try:
        await client.connect()
        generation = coordinator.inspect_worker("W1").handle.generation
        await client._write(p.WorkerGoodbye(
            worker_id="W1", reason="malicious client injection", message_id="forged-goodbye"
        ))
        with pytest.raises(Exception):
            await client._read()
        view = coordinator.inspect_worker("W1")
        assert view.handle.generation == generation
        assert view.state.online
    finally:
        await client.close()
        await worker.stop()
        await worker_task
        await service.stop()

async def test_run_limit_failure_does_not_consume_package_repository_capacity(tmp_path: Path, tls_certs):
    from coordinator import OperationLimits
    from networking import ClientOperationError

    root = tmp_path / "run-limit-program"
    root.mkdir()
    source = "x = 42\n"
    (root / "main.py").write_text(source, encoding="utf-8")
    artifact = build_package(root)
    plan = lower_dag(
        analyze_source(source, filename="main.py"),
        environment_id="env-test", package_id=artifact.package_id,
    )
    coordinator = Coordinator(operation_limits=OperationLimits(max_runs_in_memory=1))
    coordinator.submit(plan, run_id="already-full")
    repository = PackageRepository(max_bytes=len(artifact.archive_bytes) * 2)
    service = CoordinatorNetworkService(
        coordinator, tls_policy=_server_tls(tls_certs),
        authenticator=NodeAuthenticator({"W4": _secret("W4")}),
        package_repository=repository, client_node_ids=frozenset({"W4"}),
        limits=TransportLimits(handshake_timeout=2, write_timeout=2, maintenance_interval=.01),
    )
    await service.start()
    try:
        async with CoordinatorClient(CoordinatorClientConfig(
            "W4", _secret("W4"), "127.0.0.1", service.listening_port, "localhost",
            _node_tls(tls_certs, "W4"),
            TransportLimits(handshake_timeout=2, write_timeout=2, maintenance_interval=.01),
        )) as client:
            with pytest.raises(ClientOperationError, match="run limit"):
                await client.submit_package(
                    run_id="rejected-for-limit", environment_id="env-test", entrypoint="main.py",
                    plan_id=plan.id, package_id=artifact.package_id,
                    archive_bytes=artifact.archive_bytes,
                )
        assert coordinator.run_ids() == ("already-full",)
        assert repository.get(artifact.package_id) is None
    finally:
        await service.stop()

async def test_authenticated_worker_identity_is_not_implicitly_operator_authorized(tls_certs):
    service = CoordinatorNetworkService(
        Coordinator(), tls_policy=_server_tls(tls_certs),
        authenticator=NodeAuthenticator({"W1": _secret("W1"), "W4": _secret("W4")}),
        package_repository=PackageRepository(), client_node_ids=frozenset({"W4"}),
        limits=TransportLimits(handshake_timeout=2, write_timeout=2, maintenance_interval=.01),
    )
    await service.start()
    worker_as_operator = CoordinatorClient(CoordinatorClientConfig(
        "W1", _secret("W1"), "127.0.0.1", service.listening_port, "localhost",
        _node_tls(tls_certs, "W1"),
        TransportLimits(handshake_timeout=2, write_timeout=2, maintenance_interval=.01),
    ))
    try:
        with pytest.raises(Exception):
            await worker_as_operator.connect()
        assert service.coordinator.run_ids() == ()
    finally:
        await worker_as_operator.close()
        await service.stop()

async def test_submission_failure_after_repository_preflight_does_not_publish_package(tmp_path: Path, tls_certs, monkeypatch):
    from networking import ClientOperationError

    root = tmp_path / "atomic-submit"
    root.mkdir()
    source = "x = 42\n"
    (root / "main.py").write_text(source, encoding="utf-8")
    artifact = build_package(root)
    plan = lower_dag(
        analyze_source(source, filename="main.py"),
        environment_id="env-test", package_id=artifact.package_id,
    )
    coordinator = Coordinator()
    repository = PackageRepository(max_bytes=len(artifact.archive_bytes) * 2)
    original_submit = coordinator.submit

    def fail_submit(*args, **kwargs):
        raise RuntimeError("forced run admission failure")

    monkeypatch.setattr(coordinator, "submit", fail_submit)
    service = CoordinatorNetworkService(
        coordinator, tls_policy=_server_tls(tls_certs),
        authenticator=NodeAuthenticator({"W4": _secret("W4")}),
        package_repository=repository, client_node_ids=frozenset({"W4"}),
        limits=TransportLimits(handshake_timeout=2, write_timeout=2, maintenance_interval=.01),
    )
    await service.start()
    try:
        async with CoordinatorClient(CoordinatorClientConfig(
            "W4", _secret("W4"), "127.0.0.1", service.listening_port, "localhost",
            _node_tls(tls_certs, "W4"),
            TransportLimits(handshake_timeout=2, write_timeout=2, maintenance_interval=.01),
        )) as client:
            with pytest.raises(ClientOperationError, match="forced run admission failure"):
                await client.submit_package(
                    run_id="atomic-fail", environment_id="env-test", entrypoint="main.py",
                    plan_id=plan.id, package_id=artifact.package_id,
                    archive_bytes=artifact.archive_bytes,
                )
        assert coordinator.run_ids() == ()
        assert repository.get(artifact.package_id) is None
    finally:
        monkeypatch.setattr(coordinator, "submit", original_submit)
        await service.stop()

async def test_client_operation_timeout_is_distinct_from_handshake_timeout(tmp_path: Path, tls_certs, monkeypatch):
    """Verified submission work may legitimately outlive the TLS/auth timeout."""
    import time as _time

    root = tmp_path / "slow-submit"
    root.mkdir()
    source = "x = 42\n"
    (root / "main.py").write_text(source, encoding="utf-8")
    artifact = build_package(root)
    plan = lower_dag(
        analyze_source(source, filename="main.py"),
        environment_id="env-test", package_id=artifact.package_id,
    )
    coordinator = Coordinator()
    service = CoordinatorNetworkService(
        coordinator, tls_policy=_server_tls(tls_certs),
        authenticator=NodeAuthenticator({"W4": _secret("W4")}),
        package_repository=PackageRepository(), client_node_ids=frozenset({"W4"}),
        limits=TransportLimits(handshake_timeout=.5, write_timeout=2, maintenance_interval=.01),
    )
    original_stage = service._stage_client_submission

    def slow_stage(path, start):
        _time.sleep(.75)
        return original_stage(path, start)

    monkeypatch.setattr(service, "_stage_client_submission", slow_stage)
    await service.start()
    try:
        config = CoordinatorClientConfig(
            "W4", _secret("W4"), "127.0.0.1", service.listening_port, "localhost",
            _node_tls(tls_certs, "W4"),
            TransportLimits(handshake_timeout=.5, write_timeout=2, maintenance_interval=.01),
            operation_timeout=2.0,
        )
        async with CoordinatorClient(config) as client:
            submitted = await client.submit_package(
                run_id="slow-submit", environment_id="env-test", entrypoint="main.py",
                plan_id=plan.id, package_id=artifact.package_id,
                archive_bytes=artifact.archive_bytes,
            )
            assert submitted.run_id == "slow-submit"
        assert coordinator.run_ids() == ("slow-submit",)
    finally:
        await service.stop()


async def test_concurrent_operations_on_one_client_session_are_serialized(tmp_path: Path, tls_certs):
    """One connection is a single-flight request/response stream.

    Without serialization, concurrent callers interleave frames on the same
    stream and asyncio raises "read() called while another coroutine is already
    waiting for incoming data", permanently breaking the session.  Concurrent
    use must queue instead.
    """
    coordinator = Coordinator(heartbeat_timeout=1.0)
    service = CoordinatorNetworkService(
        coordinator,
        tls_policy=_server_tls(tls_certs),
        authenticator=NodeAuthenticator({"W4": _secret("W4")}),
        package_repository=PackageRepository(), client_node_ids=frozenset({"W4"}),
        limits=TransportLimits(handshake_timeout=2, write_timeout=2, maintenance_interval=.01),
    )
    await service.start()
    try:
        async with CoordinatorClient(CoordinatorClientConfig(
            "W4", _secret("W4"), "127.0.0.1", service.listening_port, "localhost",
            _node_tls(tls_certs, "W4"),
            TransportLimits(handshake_timeout=2, write_timeout=2, maintenance_interval=.01),
        )) as client:
            responses = await asyncio.gather(*(client.cluster_status() for _ in range(8)))
            assert len(responses) == 8
            assert all(isinstance(response, p.ClusterStatusResponse) for response in responses)
            # The session must remain usable afterwards.
            assert isinstance(await client.cluster_status(), p.ClusterStatusResponse)
    finally:
        await service.stop()


async def test_duplicate_worker_identity_is_logged_but_routine_reconnect_is_quiet(tmp_path: Path, tls_certs):
    """Two processes sharing one --id displace each other indefinitely.

    Admission must stay unchanged (refusing the newcomer would block recovery from
    a hung worker), but the cause has to be visible.  A routine reconnect of the
    same process must not produce the warning, or it becomes noise.
    """
    import logging

    records: list[logging.LogRecord] = []

    class _Capture(logging.Handler):
        def emit(self, record): records.append(record)

    logger = logging.getLogger("networking.coordinator_service")
    handler = _Capture(level=logging.WARNING)
    logger.addHandler(handler)

    coordinator = Coordinator(heartbeat_timeout=5.0)
    service = CoordinatorNetworkService(
        coordinator,
        tls_policy=_server_tls(tls_certs),
        authenticator=NodeAuthenticator({"W1": _secret("W1")}),
        package_repository=PackageRepository(),
        limits=TransportLimits(handshake_timeout=2, write_timeout=2, maintenance_interval=.01),
    )
    await service.start()

    def hello(port: int) -> p.WorkerHello:
        state = WorkerState("W1", 1, environment_ids=frozenset({"env-test"}))
        return p.WorkerHello(
            worker=state, endpoint=p.WorkerEndpoint("W1", "127.0.0.1", port),
            supported_versions=(p.PROTOCOL_VERSION,), message_id=f"hello-{port}",
        )

    class _Conn:
        session = None

    def shared_id_warnings():
        return [r for r in records if "share" in r.getMessage()]

    try:
        first = await service._admit(_Conn(), hello(9401))
        assert first.generation == 1

        # same endpoint == the same process reconnecting: no warning
        records.clear()
        await service._admit(_Conn(), hello(9401))
        assert not shared_id_warnings(), "routine reconnect produced a duplicate-identity warning"

        # different endpoint while the previous session is live == duplicate identity
        records.clear()
        third = await service._admit(_Conn(), hello(9402))
        warnings = shared_id_warnings()
        assert warnings, "duplicate worker identity was not reported"
        assert "9402" in warnings[0].getMessage() and "9401" in warnings[0].getMessage()

        # admission itself is unchanged: the newcomer owns the identity
        assert third.generation > first.generation
        assert coordinator.inspect_worker("W1").endpoint.port == 9402
        coordinator.validate_state()
    finally:
        logger.removeHandler(handler)
        await service.stop()


async def test_submission_verification_closes_its_temporary_cache(tmp_path: Path, tls_certs, monkeypatch):
    """The verification cache must be closed before its temp directory is removed.

    It holds an open lock file for its lifetime, and Windows cannot delete an open
    file, so leaving it open fails submission with WinError 32.  Linux tolerates it,
    so assert the close explicitly rather than relying on cleanup succeeding.
    """
    from program_package import PackageCache

    closed: list[int] = []
    real_close = PackageCache.close

    def tracking_close(self):
        closed.append(id(self))
        return real_close(self)

    monkeypatch.setattr(PackageCache, "close", tracking_close)

    coordinator = Coordinator(heartbeat_timeout=5.0)
    service = CoordinatorNetworkService(
        coordinator,
        tls_policy=_server_tls(tls_certs),
        authenticator=NodeAuthenticator({"C1": _secret("C1")}),
        package_repository=PackageRepository(), client_node_ids=frozenset({"C1"}),
        limits=TransportLimits(handshake_timeout=2, write_timeout=2, maintenance_interval=.01),
    )
    await service.start()
    try:
        source = "a = 1\nb = a + 1\n"
        root = tmp_path / "prog"; root.mkdir()
        (root / "main.py").write_text(source, encoding="utf-8")
        artifact = build_package(root)
        plan = lower_dag(analyze_source(source, filename="main.py"),
                         environment_id="env-test", package_id=artifact.package_id)
        archive = tmp_path / "archive.zip"
        archive.write_bytes(artifact.archive_bytes)

        start = p.RunSubmitStart(
            client_id="C1", run_id="r1", environment_id="env-test", entrypoint="main.py",
            plan_id=plan.id, package_id=artifact.package_id,
            archive_size=len(artifact.archive_bytes),
            archive_sha256=artifact.archive_sha256, message_id="submit-1",
        )
        service._stage_client_submission(archive, start)
        assert closed, "verification cache was left open; Windows cannot delete its lock file"
    finally:
        await service.stop()
