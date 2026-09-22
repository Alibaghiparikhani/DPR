from __future__ import annotations

import asyncio
from pathlib import Path
import socket

import pytest

from coordinator import Coordinator, RunStatus, TaskStatus
from dag_runtime.dag_engine import analyze_source
from execution import ExecutionMode, lower_dag
from networking import CoordinatorNetworkService, TransportLimits
from program_package import PackageCache, PackageRepository, build_package
import protocol as p
from runtime_security import NodeAuthenticator, TlsCredentials, TlsPolicy
from scheduler import TaskAffinity, WorkerState
from worker import (
    DataPlaneLimits, DataStoreLimits, LocalDataStore, WorkerControlClient,
    WorkerControlConfig, WorkerDataPlane, WorkerDataPlaneConfig,
    WorkerExecutionRuntime, WorkerReconnectPolicy, IsolatedExecutionLimits,
)

pytestmark = pytest.mark.asyncio


def _secret(node: str) -> bytes:
    return (node.encode() * 32)[:32]


def _server_tls(certs):
    return TlsPolicy(TlsCredentials(certs.server_cert, certs.server_key, certs.ca))


def _worker_tls(certs, node):
    return TlsPolicy(TlsCredentials(certs.cert(node), certs.key(node), certs.ca))


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _program(tmp_path: Path, source: str):
    root = tmp_path / "program"
    root.mkdir()
    (root / "main.py").write_text(source, encoding="utf-8")
    artifact = build_package(root)
    plan = lower_dag(
        analyze_source(source, filename="main.py"), environment_id="env-test",
        package_id=artifact.package_id,
    )
    return artifact, plan


async def eventually(predicate, timeout=8.0):
    async with asyncio.timeout(timeout):
        while True:
            try:
                value = predicate()
                if value:
                    return value
            except Exception:
                pass
            await asyncio.sleep(.01)


async def stack(tmp_path, certs, artifact, nodes=("W1", "W2"), *, data_plane_limits=None, execution_limits=None):
    repo=PackageRepository(); repo.add(artifact)
    coordinator=Coordinator(heartbeat_timeout=1.0)
    # These integration tests inspect terminal attempts/transfers after completion.
    # Phase-3 auto-pruning is covered by its own lifecycle suite; retain terminal
    # metadata here so P2P/context assertions are not racing an unrelated prune.
    coordinator.prune_releasable_terminal_runs = lambda: 0
    service=CoordinatorNetworkService(
        coordinator, tls_policy=_server_tls(certs),
        authenticator=NodeAuthenticator({node:_secret(node) for node in nodes}),
        package_repository=repo,
        limits=TransportLimits(handshake_timeout=2,write_timeout=2,maintenance_interval=.01),
    )
    await service.start()
    clients=[]; runtimes=[]; tasks=[]; endpoints=[]
    for node in nodes:
        port=_free_port(); endpoint=p.WorkerEndpoint(node,"127.0.0.1",port)
        state=WorkerState(node,1,environment_ids=frozenset({"env-test"}))
        store=LocalDataStore(
            tmp_path/f"data-{node}",
            limits=DataStoreLimits(max_bytes=32*1024*1024,max_value_bytes=16*1024*1024),
        )
        plane=WorkerDataPlane(
            WorkerDataPlaneConfig(
                node,"127.0.0.1",port,_worker_tls(certs,node),
                data_plane_limits or DataPlaneLimits(max_transfer_bytes=16*1024*1024,chunk_bytes=4096,
                                idle_timeout=2,total_timeout=10,cleanup_timeout=1),
            ), store,
        )
        runtime=WorkerExecutionRuntime(
            node,state,PackageCache(tmp_path/f"cache-{node}"),
            limits=execution_limits, data_store=store,data_plane=plane,
        )
        cfg=WorkerControlConfig(
            node,_secret(node),"127.0.0.1",service.listening_port,"localhost",
            endpoint,state,_worker_tls(certs,node),heartbeat_interval=.05,
            reconnect=WorkerReconnectPolicy(0,0),
            limits=TransportLimits(handshake_timeout=2,write_timeout=2,maintenance_interval=.01),
        )
        client=WorkerControlClient(cfg,runtime=runtime)
        clients.append(client); runtimes.append(runtime); endpoints.append(endpoint)
        tasks.append(asyncio.create_task(client.run()))
    await asyncio.gather(*(asyncio.wait_for(client.active.wait(),4) for client in clients))
    return coordinator,service,clients,runtimes,tasks,endpoints


async def stop(service,clients,tasks):
    await asyncio.gather(*(client.stop() for client in clients),return_exceptions=True)
    await asyncio.gather(*tasks,return_exceptions=True)
    await service.stop()


async def prepare_all(c,plan,nodes):
    for node in nodes:
        assert c.request_program_preparation(node,plan) is not None
    await eventually(lambda: all(
        plan.program.id in c.inspect_worker(node).state.prepared_program_ids for node in nodes
    ))


async def drive(c,run_id,*,timeout=8.0):
    async with asyncio.timeout(timeout):
        while not c.inspect_run(run_id).status.terminal:
            try:
                c.schedule(run_id)
            except Exception:
                pass
            await asyncio.sleep(.01)
    return c.inspect_run(run_id).status


async def test_real_shared_context_snapshot_moves_w1_to_w2_and_consumer_executes(tmp_path,tls_certs):
    artifact,plan=_program(tmp_path,"a=[1]\na.append(2)\nx=len(a)\n")
    assert [task.mode for task in plan.tasks] == [
        ExecutionMode.ISOLATED_CANDIDATE,ExecutionMode.SHARED_CONTEXT,
        ExecutionMode.ISOLATED_CANDIDATE,
    ]
    c,service,clients,runtimes,tasks,_=await stack(tmp_path,tls_certs,artifact)
    try:
        await prepare_all(c,plan,("W1","W2"))
        t0,t1,t2=plan.tasks
        affinities=(
            TaskAffinity(t0.task_id,required_worker="W1",context_id="ctx"),
            TaskAffinity(t1.task_id,required_worker="W1",context_id="ctx"),
            TaskAffinity(t2.task_id,required_worker="W2"),
        )
        c.submit(plan,run_id="r",affinities=affinities)
        assert c.request_context_preparation("r","W1","ctx",(t0.task_id,t1.task_id)) is not None
        await eventually(lambda: "ctx" in runtimes[0].context_ids)
        assert await drive(c,"r") is RunStatus.SUCCEEDED
        # The exact mutated object-state snapshot was transferred to W2. Terminal
        # release may already have deleted the physical bytes, so assert the
        # authoritative completed transfer record rather than post-run retention.
        state_id=t1.objects[0].state_outputs[0]
        ref=p.DataReference(plan.id,"r",t0.outputs[0].id,__import__('scheduler').DataForm.OBJECT_SNAPSHOT,state_id)
        assert any(record.identity.data == ref and record.identity.destination_worker_id == "W2"
                   and record.status.value == "completed" for record in c._transfers.values())
        # W2 executed the actual consumer only after the transfer completed.
        consumer=c.get_task("r",t2.task_id)
        assert consumer.status is TaskStatus.COMMITTED
        attempt=c.get_attempt("r",consumer.committed_attempt_id)
        assert attempt.worker_id == "W2"
        # Diagnostics are stored by the worker just after it emits the terminal
        # report, so under load this must be awaited rather than asserted instantly.
        await eventually(lambda: runtimes[1].diagnostics(attempt.identity.attempt_id) is not None)
        assert runtimes[1].diagnostics(attempt.identity.attempt_id).outcome == "success"
        # No runtime payload is represented as package-transfer bytes on this path;
        # direct P2P state is gone by completion and coordinator stores metadata only.
        assert all(runtime.data_plane.active_receive_count == 0 for runtime in runtimes)
        assert all(runtime.data_plane.active_send_count == 0 for runtime in runtimes)
        assert all(not runtime._send_commands and not runtime._receive_commands for runtime in runtimes)
    finally:
        await stop(service,clients,tasks)


async def test_real_native_region_uses_persistent_worker_context(tmp_path,tls_certs):
    artifact,plan=_program(tmp_path,"globals()\nx=2\ny=3\n")
    native=plan.tasks[0]; assert native.mode is ExecutionMode.NATIVE_REGION
    c,service,clients,runtimes,tasks,_=await stack(tmp_path,tls_certs,artifact,nodes=("W1",))
    try:
        await prepare_all(c,plan,("W1",))
        c.submit(plan,run_id="native",affinities=(TaskAffinity(native.task_id,required_worker="W1",context_id="ctx"),))
        c.request_context_preparation("native","W1","ctx",(native.task_id,))
        await eventually(lambda: "ctx" in runtimes[0].context_ids)
        assert await drive(c,"native") is RunStatus.SUCCEEDED
        await eventually(lambda: runtimes[0].context_ids == frozenset(), timeout=3)
    finally:
        await stop(service,clients,tasks)


async def test_three_real_workers_keep_control_heartbeats_live_with_data_plane_enabled(tmp_path,tls_certs):
    artifact,plan=_program(tmp_path,"a=1\nb=2\nc=3\n")
    nodes=("W1","W2","W3")
    c,service,clients,runtimes,tasks,_=await stack(tmp_path,tls_certs,artifact,nodes)
    try:
        await prepare_all(c,plan,nodes)
        affinities=tuple(TaskAffinity(task.task_id,required_worker=node) for task,node in zip(plan.tasks,nodes))
        c.submit(plan,run_id="three",affinities=affinities)
        assert await drive(c,"three") is RunStatus.SUCCEEDED
        await eventually(lambda: all(client.last_ack_sequence >= 0 for client in clients))
        assert all(runtime.data_plane.listening_port > 0 for runtime in runtimes)
    finally:
        await stop(service,clients,tasks)


def _batch3_worker_process_code(certs, service, node: str, cache_root: Path, data_root: Path,
                                stop_file: Path, p2p_port: int) -> str:
    return f'''
import asyncio
from pathlib import Path
import protocol as p
from networking import TransportLimits
from program_package import PackageCache
from runtime_security import TlsCredentials, TlsPolicy
from scheduler import WorkerState
from worker import (DataPlaneLimits,DataStoreLimits,LocalDataStore,WorkerControlClient,
    WorkerControlConfig,WorkerDataPlane,WorkerDataPlaneConfig,WorkerExecutionRuntime,
    WorkerReconnectPolicy)
async def main():
    node={node!r}
    state=WorkerState(node,1,environment_ids=frozenset({{"env-test"}}))
    tls=TlsPolicy(TlsCredentials({str(certs.cert(node))!r},{str(certs.key(node))!r},{str(certs.ca)!r}))
    store=LocalDataStore(Path({str(data_root)!r}),limits=DataStoreLimits(
        max_bytes=32*1024*1024,max_value_bytes=16*1024*1024))
    plane=WorkerDataPlane(WorkerDataPlaneConfig(
        node,"127.0.0.1",{p2p_port},tls,
        DataPlaneLimits(max_transfer_bytes=16*1024*1024,chunk_bytes=4096,
                        idle_timeout=2,total_timeout=10,cleanup_timeout=1)),store)
    runtime=WorkerExecutionRuntime(node,state,PackageCache(Path({str(cache_root)!r})),
                                   data_store=store,data_plane=plane)
    cfg=WorkerControlConfig(node,{_secret(node)!r},"127.0.0.1",{service.listening_port},"localhost",
        p.WorkerEndpoint(node,"127.0.0.1",{p2p_port}),state,tls,heartbeat_interval=.05,
        reconnect=WorkerReconnectPolicy(0,0),
        limits=TransportLimits(handshake_timeout=2,write_timeout=2,maintenance_interval=.01))
    client=WorkerControlClient(cfg,runtime=runtime)
    task=asyncio.create_task(client.run())
    await asyncio.wait_for(client.active.wait(),4)
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


async def _start_batch3_worker_process(tmp_path, tls_certs, service, node: str, suffix: str):
    import sys
    stop_file=tmp_path/f"stop-{node}-{suffix}"
    port=_free_port()
    root=str(Path(__file__).resolve().parents[2])
    proc=await asyncio.create_subprocess_exec(
        sys.executable,"-c",_batch3_worker_process_code(
            tls_certs,service,node,tmp_path/f"cache-{node}-{suffix}",
            tmp_path/f"data-{node}-{suffix}",stop_file,port),
        cwd=root,stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.PIPE,
    )
    line=await asyncio.wait_for(proc.stdout.readline(),5)
    if line.strip()!=b"ACTIVE":
        error=(await proc.stderr.read()).decode("utf-8","replace")
        raise AssertionError((line,error,proc.returncode))
    return proc,stop_file,port


async def _stop_process(proc, stop_file: Path):
    stop_file.touch(exist_ok=True)
    if proc.returncode is None:
        try:
            await asyncio.wait_for(proc.wait(),5)
        except asyncio.TimeoutError:
            proc.kill(); await proc.wait()
    if proc.returncode not in (0,None):
        error=(await proc.stderr.read()).decode("utf-8","replace")
        raise AssertionError((proc.returncode,error))


async def test_two_real_worker_os_processes_run_context_and_direct_p2p_consumer(tmp_path,tls_certs):
    artifact,plan=_program(tmp_path,"a=[1]\na.append(2)\nx=len(a)\n")
    repo=PackageRepository(); repo.add(artifact)
    c=Coordinator(heartbeat_timeout=1.0)
    service=CoordinatorNetworkService(
        c,tls_policy=_server_tls(tls_certs),
        authenticator=NodeAuthenticator({n:_secret(n) for n in ("W1","W2")}),
        package_repository=repo,
        limits=TransportLimits(handshake_timeout=2,write_timeout=2,maintenance_interval=.01),
    )
    await service.start()
    p1=s1=p2=s2=None
    try:
        p1,s1,_=await _start_batch3_worker_process(tmp_path,tls_certs,service,"W1","one")
        p2,s2,_=await _start_batch3_worker_process(tmp_path,tls_certs,service,"W2","one")
        await prepare_all(c,plan,("W1","W2"))
        t0,t1,t2=plan.tasks
        c.submit(plan,run_id="proc-p2p",affinities=(
            TaskAffinity(t0.task_id,required_worker="W1",context_id="ctx"),
            TaskAffinity(t1.task_id,required_worker="W1",context_id="ctx"),
            TaskAffinity(t2.task_id,required_worker="W2"),
        ))
        assert c.request_context_preparation("proc-p2p","W1","ctx",(t0.task_id,t1.task_id)) is not None
        assert await drive(c,"proc-p2p",timeout=12) is RunStatus.SUCCEEDED
        assert any(record.status.value=="completed" for record in c._transfers.values())
        consumer=c.get_task("proc-p2p",t2.task_id)
        attempt=c.get_attempt("proc-p2p",consumer.committed_attempt_id)
        assert attempt.worker_id=="W2"
    finally:
        if p1 is not None:
            await _stop_process(p1,s1)
        if p2 is not None:
            await _stop_process(p2,s2)
        await service.stop()


async def test_killed_real_context_worker_is_not_replayed_after_same_id_reconnect(tmp_path,tls_certs):
    import os,sys
    pid_file=tmp_path/"context-child.pid"
    source=(
        "import os\nimport time\n"
        f"with open({str(pid_file)!r}, 'w') as f:\n    f.write(str(os.getpid())); f.flush(); os.fsync(f.fileno())\n"
        "time.sleep(30)\n"
    )
    artifact,plan=_program(tmp_path,source)
    repo=PackageRepository(); repo.add(artifact)
    c=Coordinator(heartbeat_timeout=.5)
    c.prune_releasable_terminal_runs = lambda: 0
    service=CoordinatorNetworkService(
        c,tls_policy=_server_tls(tls_certs),authenticator=NodeAuthenticator({"W1":_secret("W1")}),
        package_repository=repo,
        limits=TransportLimits(handshake_timeout=2,write_timeout=2,maintenance_interval=.01),
    )
    await service.start()
    proc=stop=replacement=replacement_stop=None
    try:
        proc,stop,_=await _start_batch3_worker_process(tmp_path,tls_certs,service,"W1","old")
        await prepare_all(c,plan,("W1",))
        affinities=tuple(TaskAffinity(t.task_id,required_worker="W1",context_id="ctx") for t in plan.tasks)
        c.submit(plan,run_id="context-kill",affinities=affinities)
        c.request_context_preparation("context-kill","W1","ctx",tuple(t.task_id for t in plan.tasks))
        async with asyncio.timeout(8):
            while not pid_file.exists():
                if not c.inspect_run("context-kill").status.terminal:
                    c.schedule("context-kill")
                await asyncio.sleep(.01)
        child_pid=int(pid_file.read_text())
        attempts_before=sum(len(t.attempt_ids) for t in c._runs["context-kill"].tasks.values())
        proc.kill(); await asyncio.wait_for(proc.wait(),4)
        await eventually(lambda:c.inspect_run("context-kill").status is RunStatus.FAILED,timeout=5)
        failure=c.inspect_run("context-kill").failure
        assert failure is not None and failure.code.value in {"native_state_uncertain","context_lost"}
        if sys.platform.startswith("linux"):
            def child_gone():
                try: os.kill(child_pid,0)
                except ProcessLookupError: return True
                return False
            await eventually(child_gone,timeout=4)
        replacement,replacement_stop,_=await _start_batch3_worker_process(tmp_path,tls_certs,service,"W1","new")
        await eventually(lambda:c.inspect_worker("W1").handle.generation>=2,timeout=4)
        for _ in range(5):
            with __import__('contextlib').suppress(Exception): c.schedule("context-kill")
            await asyncio.sleep(.02)
        attempts_after=sum(len(t.attempt_ids) for t in c._runs["context-kill"].tasks.values())
        assert attempts_after==attempts_before
        assert c.inspect_run("context-kill").status is RunStatus.FAILED
    finally:
        if proc is not None and proc.returncode is None:
            proc.kill(); await proc.wait()
        if replacement is not None:
            await _stop_process(replacement,replacement_stop)
        await service.stop()


async def test_coordinator_cancellation_aborts_real_inflight_p2p_and_prevents_publication(tmp_path,tls_certs):
    source='''from dag_runtime import task
@task
def make():
    return b"x" * 4000000
@task
def consume(v):
    return len(v)
a=make()
b=consume(a)
'''
    artifact,plan=_program(tmp_path,source)
    t0,t1=plan.tasks
    limits=DataPlaneLimits(max_transfer_bytes=16*1024*1024,chunk_bytes=128,
                           idle_timeout=3,total_timeout=30,cleanup_timeout=1)
    exec_limits=IsolatedExecutionLimits(result_bytes=12*1024*1024,local_value_bytes=32*1024*1024)
    c,service,clients,runtimes,tasks,_=await stack(
        tmp_path,tls_certs,artifact,data_plane_limits=limits,execution_limits=exec_limits,
    )
    try:
        await prepare_all(c,plan,("W1","W2"))
        c.submit(plan,run_id="cancel-p2p",affinities=(
            TaskAffinity(t0.task_id,required_worker="W1"),
            TaskAffinity(t1.task_id,required_worker="W2"),
        ))
        # Drive until the producer commits and the dependent transfer physically starts.
        async with asyncio.timeout(12):
            while not any(rt.data_plane.active_send_count for rt in runtimes):
                if not c.inspect_run("cancel-p2p").status.terminal:
                    c.schedule("cancel-p2p")
                await asyncio.sleep(.001)
        c.cancel_run("cancel-p2p",reason="test cancellation during byte transfer")
        await eventually(lambda:all(
            rt.data_plane.active_send_count==0 and rt.data_plane.active_receive_count==0
            for rt in runtimes
        ),timeout=5)
        await eventually(lambda:c.inspect_run("cancel-p2p").status is RunStatus.CANCELLED,timeout=5)
        ref=p.DataReference(plan.id,"cancel-p2p",t0.outputs[0].id,__import__('scheduler').DataForm.IMMUTABLE_VALUE)
        assert runtimes[1].data_store.get(ref,session_id=clients[1].session.session_id) is None
        assert all(record.status.value=="failed" for record in c._transfers.values())
    finally:
        await stop(service,clients,tasks)


async def test_real_materialized_alias_consumer_uses_two_physical_p2p_transfers(tmp_path,tls_certs):
    artifact,plan=_program(tmp_path,"a=1\nb=a\nc=a+b\n")
    assert len(plan.tasks)==3
    producer,binding,consumer=plan.tasks
    c,service,clients,runtimes,tasks,_=await stack(tmp_path,tls_certs,artifact)
    try:
        await prepare_all(c,plan,("W1","W2"))
        c.submit(plan,run_id="alias-p2p",affinities=(
            TaskAffinity(producer.task_id,required_worker="W1"),
            TaskAffinity(binding.task_id,required_worker="W1",context_id="alias-ctx"),
            TaskAffinity(consumer.task_id,required_worker="W2"),
        ))
        assert c.request_context_preparation("alias-p2p","W1","alias-ctx",(binding.task_id,)) is not None
        await eventually(lambda:"alias-ctx" in runtimes[0].context_ids)
        assert await drive(c,"alias-p2p") is RunStatus.SUCCEEDED
        completed=[r for r in c._transfers.values()
                   if r.identity.data.run_id=="alias-p2p" and r.status.value=="completed"]
        assert len(completed)==2
        committed=c.get_task("alias-p2p",consumer.task_id)
        assert c.get_attempt("alias-p2p",committed.committed_attempt_id).worker_id=="W2"
    finally:
        await stop(service,clients,tasks)
