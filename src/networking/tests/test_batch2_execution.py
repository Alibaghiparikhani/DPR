from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from coordinator import Coordinator, RunStatus, TaskStatus
from dag_runtime.dag_engine import analyze_source
from execution import lower_dag
from networking import CoordinatorNetworkService, TransportLimits
from program_package import PackageCache, PackageRepository, build_package
from runtime_security import NodeAuthenticator, TlsCredentials, TlsPolicy
from scheduler import WorkerState
from worker import (
    IsolatedExecutionLimits, LocalDataStore, WorkerControlClient, WorkerControlConfig,
    WorkerExecutionRuntime, WorkerReconnectPolicy,
)
import protocol as p

pytestmark = pytest.mark.asyncio


def _secret(node: str) -> bytes:
    return (node.encode() * 32)[:32]


def _server_tls(certs):
    return TlsPolicy(TlsCredentials(certs.server_cert, certs.server_key, certs.ca))


def _client_tls(certs, node: str):
    return TlsPolicy(TlsCredentials(certs.cert(node), certs.key(node), certs.ca))


def _program(tmp_path: Path, source: str):
    root = tmp_path / "program"
    root.mkdir()
    (root / "main.py").write_text(source, encoding="utf-8")
    artifact = build_package(root)
    plan = lower_dag(
        analyze_source(source, filename="main.py"),
        environment_id="env-test", package_id=artifact.package_id,
    )
    return artifact, plan


async def _eventually(predicate, timeout: float = 5.0):
    async with asyncio.timeout(timeout):
        while True:
            try:
                value = predicate()
                if value:
                    return value
            except Exception:
                pass
            await asyncio.sleep(.01)


async def _stack(tmp_path: Path, certs, artifact, nodes=("W1",), *, heartbeat=.05,
                 runtime_limits=None, reconnect=0, repository=None):
    repository = repository or PackageRepository()
    if repository.get(artifact.package_id) is None:
        repository.add(artifact)
    coordinator = Coordinator(heartbeat_timeout=1.0)
    service = CoordinatorNetworkService(
        coordinator,
        tls_policy=_server_tls(certs),
        authenticator=NodeAuthenticator({node: _secret(node) for node in nodes}),
        package_repository=repository,
        limits=TransportLimits(handshake_timeout=2, write_timeout=2, maintenance_interval=.01),
    )
    await service.start()
    clients = []
    runtimes = []
    tasks = []
    for index, node in enumerate(nodes):
        state = WorkerState(node, 1, environment_ids=frozenset({"env-test"}))
        runtime = WorkerExecutionRuntime(
            node, state, PackageCache(tmp_path / f"cache-{node}"), limits=runtime_limits,
            data_store=LocalDataStore(tmp_path / f"runtime-data-{node}"),
        )
        config = WorkerControlConfig(
            node_id=node, secret=_secret(node), coordinator_host="127.0.0.1",
            coordinator_port=service.listening_port, server_hostname="localhost",
            endpoint=p.WorkerEndpoint(node, "127.0.0.1", 9300 + index), initial_state=state,
            tls_policy=_client_tls(certs, node), heartbeat_interval=heartbeat,
            reconnect=WorkerReconnectPolicy(reconnect, .03),
            limits=TransportLimits(handshake_timeout=2, write_timeout=2, maintenance_interval=.01),
        )
        client = WorkerControlClient(config, runtime=runtime)
        clients.append(client); runtimes.append(runtime)
        tasks.append(asyncio.create_task(client.run()))
    await asyncio.gather(*(asyncio.wait_for(client.active.wait(), 3) for client in clients))
    return coordinator, service, clients, runtimes, tasks, repository


async def _stop(service, clients, tasks):
    await asyncio.gather(*(client.stop() for client in clients), return_exceptions=True)
    await asyncio.gather(*tasks, return_exceptions=True)
    await service.stop()


async def _prepare_all(coordinator, plan, nodes):
    for node in nodes:
        assert coordinator.request_program_preparation(node, plan) is not None
    await _eventually(lambda: all(
        plan.program.id in coordinator.inspect_worker(node).state.prepared_program_ids
        for node in nodes
    ), timeout=5)


async def test_real_tls_package_prepare_child_execution_and_logical_success(tmp_path, tls_certs):
    artifact, plan = _program(tmp_path, "a = 20 + 22\n")
    c, service, clients, runtimes, tasks, _ = await _stack(tmp_path, tls_certs, artifact)
    try:
        await _prepare_all(c, plan, ("W1",))
        c.submit(plan, run_id="r")
        result = c.schedule("r")
        assert len(result.dispatched) == 1
        await _eventually(lambda: c.inspect_run("r").status is RunStatus.SUCCEEDED)
        attempt_id = result.dispatched[0].attempt_id
        diag = await _eventually(lambda: runtimes[0].diagnostics(attempt_id))
        assert diag.outcome == "success" and diag.exit_code == 0
        assert not runtimes[0].active_attempt_ids
    finally:
        await _stop(service, clients, tasks)


async def test_three_real_workers_execute_independent_children_with_capacity(tmp_path, tls_certs):
    source = '''from dag_runtime import task\n@task\ndef one():\n    import time; time.sleep(.15); return 1\n@task\ndef two():\n    import time; time.sleep(.15); return 2\n@task\ndef three():\n    import time; time.sleep(.15); return 3\na=one()\nb=two()\nc=three()\n'''
    artifact, plan = _program(tmp_path, source)
    nodes = ("W1", "W2", "W3")
    c, service, clients, runtimes, tasks, _ = await _stack(tmp_path, tls_certs, artifact, nodes)
    try:
        await _prepare_all(c, plan, nodes)
        c.submit(plan, run_id="r")
        result = c.schedule("r")
        assert len(result.dispatched) == 3
        await _eventually(lambda: c.inspect_run("r").status is RunStatus.SUCCEEDED, timeout=6)
        await _eventually(lambda: all(
            sum(rt.diagnostics(a.attempt_id) is not None for a in result.dispatched) == 1
            for rt in runtimes
        ), timeout=3)
        diagnosed = [sum(rt.diagnostics(a.attempt_id) is not None for a in result.dispatched) for rt in runtimes]
        assert diagnosed == [1, 1, 1]
    finally:
        await _stop(service, clients, tasks)


async def test_real_coordinator_cancellation_reaps_child_before_terminal_result(tmp_path, tls_certs):
    source = '''from dag_runtime import task\n@task\ndef slow():\n    import time; time.sleep(20); return 1\nx=slow()\n'''
    artifact, plan = _program(tmp_path, source)
    limits = IsolatedExecutionLimits(cancel_grace_seconds=.05, kill_wait_seconds=.3)
    c, service, clients, runtimes, tasks, _ = await _stack(tmp_path, tls_certs, artifact, runtime_limits=limits)
    try:
        await _prepare_all(c, plan, ("W1",))
        c.submit(plan, run_id="r"); result = c.schedule("r")
        attempt = result.dispatched[0]
        await _eventually(lambda: c.get_task("r", attempt.task_id).status is TaskStatus.RUNNING)
        c.cancel_run("r", reason="integration cancellation")
        await _eventually(lambda: c.inspect_run("r").status is RunStatus.CANCELLED)
        await _eventually(lambda: not runtimes[0].active_attempt_ids)
        diag = await _eventually(lambda: runtimes[0].diagnostics(attempt.attempt_id))
        assert diag.outcome.startswith("cancelled")
    finally:
        await _stop(service, clients, tasks)


class CountingRepository(PackageRepository):
    def __init__(self):
        super().__init__(); self.get_count = 0
    def get(self, package_id: str):
        self.get_count += 1
        return super().get(package_id)


async def test_verified_package_cache_hit_avoids_retransmission(tmp_path, tls_certs):
    artifact, plan = _program(tmp_path, "a=42\n")
    repo = CountingRepository(); repo.add(artifact); repo.get_count = 0
    c, service, clients, runtimes, tasks, _ = await _stack(
        tmp_path, tls_certs, artifact, heartbeat=.05, repository=repo,
    )
    try:
        await _prepare_all(c, plan, ("W1",))
        first_gets = repo.get_count
        assert first_gets >= 1
        # Coordinator knows the exact immutable package is prepared, so an identical
        # request is idempotent and no second package lookup/transfer is created.
        assert c.request_program_preparation("W1", plan) is None
        await asyncio.sleep(.08)
        assert repo.get_count == first_gets
        assert runtimes[0].cache.verify(artifact.package_id) is not None
    finally:
        await _stop(service, clients, tasks)


async def test_corrupted_prepared_cache_is_never_executed(tmp_path, tls_certs):
    artifact, plan = _program(tmp_path, "a=42\n")
    # Long heartbeat keeps the coordinator's already-prepared snapshot in place long
    # enough to force a dispatch into the worker's immediate pre-execution recheck.
    c, service, clients, runtimes, tasks, _ = await _stack(tmp_path, tls_certs, artifact, heartbeat=10)
    try:
        await _prepare_all(c, plan, ("W1",))
        entry = runtimes[0].cache.content_path(artifact.package_id) / "main.py"
        entry.write_text("a=99\n", encoding="utf-8")
        c.submit(plan, run_id="r"); result = c.schedule("r")
        assert len(result.dispatched) == 1
        await _eventually(lambda: c.get_task("r", result.dispatched[0].task_id).status is TaskStatus.READY)
        assert c.inspect_run("r").status is RunStatus.RUNNING
        assert plan.program.id not in c.inspect_worker("W1").state.prepared_program_ids
        assert runtimes[0].diagnostics(result.dispatched[0].attempt_id) is None
    finally:
        await _stop(service, clients, tasks)


def _runtime_worker_process_code(certs, service, node: str, cache_root: Path, stop_file: Path) -> str:
    return f'''
import asyncio
from pathlib import Path
import protocol as p
from networking import TransportLimits
from program_package import PackageCache
from runtime_security import TlsCredentials, TlsPolicy
from scheduler import WorkerState
from worker import LocalDataStore, WorkerControlClient, WorkerControlConfig, WorkerExecutionRuntime, WorkerReconnectPolicy
async def main():
    state=WorkerState({node!r},1,environment_ids=frozenset({{"env-test"}}))
    runtime=WorkerExecutionRuntime({node!r},state,PackageCache({str(cache_root)!r}),data_store=LocalDataStore({str(cache_root) + "-data"!r}))
    cfg=WorkerControlConfig({node!r},{_secret(node)!r},"127.0.0.1",{service.listening_port},"localhost",
        p.WorkerEndpoint({node!r},"127.0.0.1",9450 + int({node!r}[1:])),state,
        TlsPolicy(TlsCredentials({str(certs.cert(node))!r},{str(certs.key(node))!r},{str(certs.ca)!r})),
        heartbeat_interval=.05,reconnect=WorkerReconnectPolicy(0,0),
        limits=TransportLimits(handshake_timeout=2,write_timeout=2,maintenance_interval=.01))
    client=WorkerControlClient(cfg,runtime=runtime)
    task=asyncio.create_task(client.run())
    await asyncio.wait_for(client.active.wait(),3)
    print("ACTIVE",flush=True)
    stop=Path({str(stop_file)!r})
    while not stop.exists():
        if task.done():
            await task
            return
        await asyncio.sleep(.02)
    await client.stop(); await task
asyncio.run(main())
'''


async def test_real_worker_os_process_runs_real_isolated_grandchild(tmp_path, tls_certs):
    import sys
    source = "a = 20 + 22\n"
    artifact, plan = _program(tmp_path, source)
    repo = PackageRepository(); repo.add(artifact)
    c = Coordinator(heartbeat_timeout=1.0)
    service = CoordinatorNetworkService(
        c, tls_policy=_server_tls(tls_certs), authenticator=NodeAuthenticator({"W1": _secret("W1")}),
        package_repository=repo,
        limits=TransportLimits(handshake_timeout=2, write_timeout=2, maintenance_interval=.01),
    )
    await service.start()
    stop = tmp_path / "stop-worker"
    root = str(Path(__file__).resolve().parents[2])
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-c", _runtime_worker_process_code(tls_certs, service, "W1", tmp_path / "proc-cache", stop),
        cwd=root, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    try:
        assert (await asyncio.wait_for(proc.stdout.readline(), 4)).strip() == b"ACTIVE"
        await _eventually(lambda: c.inspect_worker("W1").handle.generation == 1)
        await _prepare_all(c, plan, ("W1",))
        c.submit(plan, run_id="proc-run"); result = c.schedule("proc-run")
        assert len(result.dispatched) == 1
        await _eventually(lambda: c.inspect_run("proc-run").status is RunStatus.SUCCEEDED, timeout=6)
        stop.touch()
        code = await asyncio.wait_for(proc.wait(), 4)
        if code != 0:
            raise AssertionError((code, (await proc.stderr.read()).decode("utf-8", "replace")))
    finally:
        stop.touch(exist_ok=True)
        if proc.returncode is None:
            proc.kill(); await proc.wait()
        await service.stop()


async def test_killing_real_worker_process_during_task_reconciles_and_kills_child_on_linux(tmp_path, tls_certs):
    import os
    import sys
    pid_file = tmp_path / "executor.pid"
    source = f'''from dag_runtime import task\n@task\ndef slow():\n    import os, time\n    with open({str(pid_file)!r}, "w") as f:\n        f.write(str(os.getpid())); f.flush(); os.fsync(f.fileno())\n    time.sleep(30)\n    return 1\nx=slow()\n'''
    artifact, plan = _program(tmp_path, source)
    repo = PackageRepository(); repo.add(artifact)
    c = Coordinator(heartbeat_timeout=.5)
    service = CoordinatorNetworkService(
        c, tls_policy=_server_tls(tls_certs), authenticator=NodeAuthenticator({"W1": _secret("W1")}),
        package_repository=repo,
        limits=TransportLimits(handshake_timeout=2, write_timeout=2, maintenance_interval=.01),
    )
    await service.start()
    stop = tmp_path / "stop-worker"
    root = str(Path(__file__).resolve().parents[2])
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-c", _runtime_worker_process_code(tls_certs, service, "W1", tmp_path / "kill-cache", stop),
        cwd=root, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    try:
        assert (await asyncio.wait_for(proc.stdout.readline(), 4)).strip() == b"ACTIVE"
        await _prepare_all(c, plan, ("W1",))
        c.submit(plan, run_id="kill-run"); dispatched = c.schedule("kill-run").dispatched
        assert len(dispatched) == 1
        await _eventually(lambda: pid_file.exists() and c.get_task("kill-run", dispatched[0].task_id).status is TaskStatus.RUNNING, timeout=6)
        child_pid = int(pid_file.read_text())
        proc.kill(); await asyncio.wait_for(proc.wait(), 3)
        await _eventually(lambda: _worker_absent(c, "W1"), timeout=3)
        await _eventually(lambda: c.get_task("kill-run", dispatched[0].task_id).status is TaskStatus.READY, timeout=3)
        if sys.platform.startswith("linux"):
            def child_gone():
                try:
                    os.kill(child_pid, 0)
                except ProcessLookupError:
                    return True
                return False
            await _eventually(child_gone, timeout=3)
    finally:
        if proc.returncode is None:
            proc.kill(); await proc.wait()
        await service.stop()


def _worker_absent(coordinator, node: str) -> bool:
    try:
        coordinator.inspect_worker(node)
    except Exception:
        return True
    return False


async def test_session_reconnect_kills_old_child_and_only_new_attempt_can_commit(tmp_path, tls_certs):
    marker = tmp_path / "first-attempt-marker"
    source = f'''from dag_runtime import task\n@task\ndef once_slow():\n    from pathlib import Path\n    import time\n    p=Path({str(marker)!r})\n    if not p.exists():\n        p.write_text("first")\n        time.sleep(30)\n    return 42\nx=once_slow()\n'''
    artifact, plan = _program(tmp_path, source)
    c, service, clients, runtimes, tasks, _ = await _stack(
        tmp_path, tls_certs, artifact, reconnect=3,
        runtime_limits=IsolatedExecutionLimits(cancel_grace_seconds=.05, kill_wait_seconds=.3),
    )
    client = clients[0]
    try:
        await _prepare_all(c, plan, ("W1",))
        first_handle = c.inspect_worker("W1").handle
        c.submit(plan, run_id="reconnect-run"); first = c.schedule("reconnect-run").dispatched[0]
        await _eventually(lambda: marker.exists() and c.get_task("reconnect-run", first.task_id).status is TaskStatus.RUNNING, timeout=6)
        assert client._writer is not None
        client._writer.transport.abort()
        new_handle = await _eventually(
            lambda: (view.handle if (view := c.inspect_worker("W1")).handle.generation > first_handle.generation else None),
            timeout=5,
        )
        assert new_handle.session_id != first_handle.session_id
        await _eventually(lambda: not runtimes[0].active_attempt_ids, timeout=3)
        await _eventually(lambda: c.get_task("reconnect-run", first.task_id).status is TaskStatus.READY, timeout=3)
        second_result = c.schedule("reconnect-run")
        assert len(second_result.dispatched) == 1
        second = second_result.dispatched[0]
        assert second.attempt_id != first.attempt_id
        await _eventually(lambda: c.inspect_run("reconnect-run").status is RunStatus.SUCCEEDED, timeout=6)
        task_record = c.get_task("reconnect-run", first.task_id)
        assert task_record.committed_attempt_id == second.attempt_id
        diagnostic = await _eventually(lambda: runtimes[0].diagnostics(second.attempt_id), timeout=3)
        assert diagnostic.outcome == "success"
    finally:
        await _stop(service, clients, tasks)


async def test_coordinator_shutdown_mid_task_fences_and_reaps_worker_execution(tmp_path, tls_certs):
    source = '''from dag_runtime import task\n@task\ndef slow():\n    import time; time.sleep(30); return 1\nx=slow()\n'''
    artifact, plan = _program(tmp_path, source)
    c, service, clients, runtimes, tasks, _ = await _stack(
        tmp_path, tls_certs, artifact,
        runtime_limits=IsolatedExecutionLimits(cancel_grace_seconds=.05, kill_wait_seconds=.3),
    )
    try:
        await _prepare_all(c, plan, ("W1",))
        c.submit(plan, run_id="shutdown-run"); attempt = c.schedule("shutdown-run").dispatched[0]
        await _eventually(lambda: c.get_task("shutdown-run", attempt.task_id).status is TaskStatus.RUNNING)
        await service.stop()
        await _eventually(lambda: not runtimes[0].active_attempt_ids, timeout=3)
        await _eventually(lambda: c.get_task("shutdown-run", attempt.task_id).status is TaskStatus.READY, timeout=3)
        await asyncio.wait_for(tasks[0], 3)
    finally:
        await clients[0].stop()
        await asyncio.gather(*tasks, return_exceptions=True)
        await service.stop()


async def test_three_real_worker_os_processes_execute_distributed_children(tmp_path, tls_certs):
    import os
    import sys
    markers = tmp_path / "worker-markers"; markers.mkdir()
    # Phase 5 deliberately scrubs arbitrary worker environment variables from
    # submitted code. Use each executor's parent worker PID as an observable
    # per-worker marker instead of relying on inherited DPR_TEST_WORKER.
    source = f'''from dag_runtime import task\n@task\ndef one():\n    import os,time; from pathlib import Path; Path({str(markers)!r},str(os.getppid())).touch(); time.sleep(.15); return 1\n@task\ndef two():\n    import os,time; from pathlib import Path; Path({str(markers)!r},str(os.getppid())).touch(); time.sleep(.15); return 2\n@task\ndef three():\n    import os,time; from pathlib import Path; Path({str(markers)!r},str(os.getppid())).touch(); time.sleep(.15); return 3\na=one()\nb=two()\nc=three()\n'''
    artifact, plan = _program(tmp_path, source)
    nodes=("W1","W2","W3")
    repo=PackageRepository(); repo.add(artifact)
    c=Coordinator(heartbeat_timeout=1.0)
    service=CoordinatorNetworkService(
        c,tls_policy=_server_tls(tls_certs),authenticator=NodeAuthenticator({n:_secret(n) for n in nodes}),
        package_repository=repo,limits=TransportLimits(handshake_timeout=2,write_timeout=2,maintenance_interval=.01),
    )
    await service.start()
    root=str(Path(__file__).resolve().parents[2])
    procs=[]; stops=[]
    for node in nodes:
        stop=tmp_path/f"stop-{node}"; stops.append(stop)
        env=dict(os.environ); env["DPR_TEST_WORKER"]=node
        proc=await asyncio.create_subprocess_exec(
            sys.executable,"-c",_runtime_worker_process_code(tls_certs,service,node,tmp_path/f"process-cache-{node}",stop),
            cwd=root,stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.PIPE,env=env,
        )
        procs.append(proc)
    try:
        lines=await asyncio.gather(*(asyncio.wait_for(proc.stdout.readline(),4) for proc in procs))
        assert lines == [b"ACTIVE\n"] * 3
        await _prepare_all(c,plan,nodes)
        c.submit(plan,run_id="three-process-run"); result=c.schedule("three-process-run")
        assert len(result.dispatched) == 3
        await _eventually(lambda:c.inspect_run("three-process-run").status is RunStatus.SUCCEEDED,timeout=20)
        assert len({path.name for path in markers.iterdir()}) == 3
        for stop in stops: stop.touch()
        codes=await asyncio.gather(*(asyncio.wait_for(proc.wait(),4) for proc in procs))
        if codes != [0,0,0]:
            errors=[(await proc.stderr.read()).decode("utf-8","replace") for proc in procs]
            raise AssertionError((codes,errors))
    finally:
        for stop in stops: stop.touch(exist_ok=True)
        for proc in procs:
            if proc.returncode is None: proc.kill()
        await asyncio.gather(*(proc.wait() for proc in procs if proc.returncode is None),return_exceptions=True)
        await service.stop()


async def test_connection_package_delivery_is_single_flight_per_package(tmp_path):
    from types import SimpleNamespace
    from networking.coordinator_service import _ServerConnection

    artifact, plan = _program(tmp_path, "a=42\n")
    fake_service = SimpleNamespace(
        limits=SimpleNamespace(
            read_chunk_size=4096,
            inbound_queue_messages=8,
            outbound_queue_messages=8,
        )
    )
    connection = _ServerConnection(fake_service, asyncio.StreamReader(), object())
    one = p.PrepareProgram("W1", plan.id, plan.program, message_id="prep-one")
    two = p.PrepareProgram("W1", plan.id, plan.program, message_id="prep-two")

    assert connection.enqueue_package_delivery(one, artifact)
    assert connection.enqueue_package_delivery(two, artifact)
    assert connection.outbound.qsize() == 1

    connection.note_package_response(p.ProgramPrepared(
        "W1", plan.id, plan.program.id,
        message_id="prepared-one", correlation_id=one.message_id,
    ))
    assert connection.enqueue_package_delivery(two, artifact)
    assert connection.outbound.qsize() == 2
