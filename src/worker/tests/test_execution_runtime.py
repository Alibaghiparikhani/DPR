from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from dag_runtime.dag_engine import analyze_source
from execution import AttemptIdentity, ExecutionMode, FailureKind, lower_dag
import protocol as p
from program_package import PackageCache, build_package
from scheduler import WorkerState
from worker import IsolatedExecutionLimits, WorkerExecutionRuntime

pytestmark = pytest.mark.asyncio


async def eventually(predicate, timeout=5.0):
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(.01)


def build(tmp_path: Path, source: str, *, slots=2, limits=None):
    root = tmp_path / "src"; root.mkdir()
    (root / "main.py").write_text(source, encoding="utf-8")
    artifact = build_package(root)
    plan = lower_dag(analyze_source(source, filename="main.py"), environment_id="env-test", package_id=artifact.package_id)
    state = WorkerState("W1", slots, environment_ids=frozenset({"env-test"}))
    runtime = WorkerExecutionRuntime("W1", state, PackageCache(tmp_path / "cache"), limits=limits)
    return root, artifact, plan, runtime


async def prepare(runtime, artifact, plan, out, session_id="session-1"):
    async def send(message): out.append(message)
    await runtime.session_started(session_id, send)
    command = p.PrepareProgram("W1", plan.id, plan.program, message_id=f"prepare-1-{session_id}")
    await runtime.handle_message(command, session_id=session_id)
    start = p.PackageTransferStart(
        "W1", plan.id, plan.program.id, artifact.package_id,
        len(artifact.archive_bytes), artifact.archive_sha256,
        message_id=f"package-start-{session_id}", correlation_id=command.message_id,
    )
    await runtime.handle_message(start, session_id=session_id)
    offset = 0
    chunk_size = runtime.cache.limits.chunk_bytes
    while offset < len(artifact.archive_bytes):
        chunk = artifact.archive_bytes[offset:offset + chunk_size]
        await runtime.handle_message(p.PackageTransferChunk(
            "W1", artifact.package_id, offset, chunk.hex(),
            message_id=f"chunk-{offset}-{session_id}", correlation_id=command.message_id,
        ), session_id=session_id)
        offset += len(chunk)
    await runtime.handle_message(p.PackageTransferEnd(
        "W1", artifact.package_id, len(artifact.archive_bytes), artifact.archive_sha256,
        message_id=f"package-end-{session_id}", correlation_id=command.message_id,
    ), session_id=session_id)
    assert any(isinstance(m, p.ProgramPrepared) for m in out)


def dispatch(plan, task_id, attempt_id, *, run_id="run-1", mode=ExecutionMode.ISOLATED_CANDIDATE, context_id=None):
    return p.TaskDispatch(
        "W1", AttemptIdentity(plan.id, run_id, task_id, attempt_id), plan.program.id,
        mode, context_id, message_id=f"dispatch-{attempt_id}",
    )


async def wait_terminal(out, attempt_id):
    await eventually(lambda: any(
        isinstance(m, (p.TaskSucceeded, p.TaskFailed, p.TaskRejected, p.TaskCancellationResult))
        and (getattr(m, "result", None).attempt.attempt_id if hasattr(m, "result") else m.attempt.attempt_id) == attempt_id
        for m in out
    ))


async def test_real_isolated_success_and_worker_local_dependency_values(tmp_path):
    _, artifact, plan, runtime = build(tmp_path, "x=20\ny=22\na=x+y\n")
    out=[]; await prepare(runtime, artifact, plan, out)
    try:
        for task, aid in zip(plan.tasks[:2], ("a1", "a2")):
            await runtime.handle_message(dispatch(plan, task.task_id, aid), session_id="session-1")
        await eventually(lambda: sum(isinstance(m, p.TaskSucceeded) for m in out) >= 2)
        await runtime.handle_message(dispatch(plan, plan.tasks[2].task_id, "a3"), session_id="session-1")
        await wait_terminal(out, "a3")
        success = [m for m in out if isinstance(m, p.TaskSucceeded) and m.result.attempt.attempt_id == "a3"]
        assert len(success) == 1 and success[0].result.output_ids == plan.tasks[2].reported_output_ids
    finally:
        await runtime.shutdown()


async def test_python_exception_and_import_failure_are_reported_without_crashing_worker(tmp_path):
    source = '''from dag_runtime import task\n@task\ndef boom():\n    raise ValueError("bad value")\nx=boom()\n'''
    _, artifact, plan, runtime = build(tmp_path, source)
    out=[]; await prepare(runtime, artifact, plan, out)
    try:
        task = next(t for t in plan.tasks if t.mode is ExecutionMode.ISOLATED_CANDIDATE)
        await runtime.handle_message(dispatch(plan, task.task_id, "boom"), session_id="session-1")
        await wait_terminal(out, "boom")
        failure = next(m for m in out if isinstance(m, p.TaskFailed))
        assert failure.result.failure.kind is FailureKind.PYTHON_EXCEPTION
        assert "bad value" in failure.result.failure.message
    finally: await runtime.shutdown()


async def test_abrupt_child_exit_is_execution_failure(tmp_path):
    source = '''from dag_runtime import task\n@task\ndef die():\n    import os\n    os._exit(7)\nx=die()\n'''
    _, artifact, plan, runtime = build(tmp_path, source)
    out=[]; await prepare(runtime, artifact, plan, out)
    try:
        task = next(t for t in plan.tasks if t.mode is ExecutionMode.ISOLATED_CANDIDATE)
        await runtime.handle_message(dispatch(plan, task.task_id, "die"), session_id="session-1")
        await wait_terminal(out, "die")
        failure = next(m for m in out if isinstance(m, p.TaskFailed))
        assert failure.result.failure.kind is FailureKind.EXECUTION_ERROR
        assert "exited" in failure.result.failure.message
    finally: await runtime.shutdown()


async def test_timeout_terminates_and_reaps_child(tmp_path):
    source = '''from dag_runtime import task\n@task\ndef slow():\n    import time\n    time.sleep(10)\n    return 1\nx=slow()\n'''
    limits = IsolatedExecutionLimits(task_timeout_seconds=.15, cancel_grace_seconds=.05, kill_wait_seconds=.2)
    _, artifact, plan, runtime = build(tmp_path, source, limits=limits)
    out=[]; await prepare(runtime, artifact, plan, out)
    try:
        task = next(t for t in plan.tasks if t.mode is ExecutionMode.ISOLATED_CANDIDATE)
        await runtime.handle_message(dispatch(plan, task.task_id, "timeout"), session_id="session-1")
        await wait_terminal(out, "timeout")
        failure = next(m for m in out if isinstance(m, p.TaskFailed))
        assert "deadline" in failure.result.failure.message
        await eventually(lambda: not runtime.active_attempt_ids)
    finally: await runtime.shutdown()


async def test_cancellation_is_physical_idempotent_and_releases_capacity(tmp_path):
    source = '''from dag_runtime import task\n@task\ndef slow():\n    import time\n    time.sleep(10)\n    return 1\nx=slow()\n'''
    _, artifact, plan, runtime = build(tmp_path, source, slots=1)
    out=[]; await prepare(runtime, artifact, plan, out)
    try:
        task = next(t for t in plan.tasks if t.mode is ExecutionMode.ISOLATED_CANDIDATE)
        d = dispatch(plan, task.task_id, "cancel")
        await runtime.handle_message(d, session_id="session-1")
        await eventually(lambda: any(isinstance(m, p.TaskStarted) for m in out))
        cancel = p.CancelTask("W1", d.attempt, "stop", message_id="cancel-command")
        await runtime.handle_message(cancel, session_id="session-1")
        await eventually(lambda: any(isinstance(m, p.TaskCancellationResult) for m in out))
        first = [m for m in out if isinstance(m, p.TaskCancellationResult)][0]
        assert first.outcome is p.CancellationOutcome.CANCELLED
        await eventually(lambda: not runtime.active_attempt_ids)
        duplicate = p.CancelTask("W1", d.attempt, "again", message_id="cancel-command-2")
        await runtime.handle_message(duplicate, session_id="session-1")
        assert [m for m in out if isinstance(m, p.TaskCancellationResult)][-1].outcome is p.CancellationOutcome.TOO_LATE
        assert runtime.decorate_state(runtime.base_state).free_slots == 1
    finally: await runtime.shutdown()


async def test_huge_stdout_stderr_are_drained_and_bounded(tmp_path):
    source = '''from dag_runtime import task\n@task\ndef noisy():\n    import os\n    os.write(1, b"x" * 200000)\n    os.write(2, b"y" * 200000)\n    return 1\nx=noisy()\n'''
    limits = IsolatedExecutionLimits(stdout_bytes=1024, stderr_bytes=2048)
    _, artifact, plan, runtime = build(tmp_path, source, limits=limits)
    out=[]; await prepare(runtime, artifact, plan, out)
    try:
        task = next(t for t in plan.tasks if t.mode is ExecutionMode.ISOLATED_CANDIDATE)
        await runtime.handle_message(dispatch(plan, task.task_id, "noisy"), session_id="session-1")
        await wait_terminal(out, "noisy")
        await eventually(lambda: runtime.diagnostics("noisy") is not None)
        diag = runtime.diagnostics("noisy")
        assert diag is not None and len(diag.stdout.encode()) <= 1024 and len(diag.stderr.encode()) <= 2048
        assert diag.stdout_truncated and diag.stderr_truncated
    finally: await runtime.shutdown()


async def test_corrupted_cache_is_withdrawn_and_not_executed(tmp_path):
    _, artifact, plan, runtime = build(tmp_path, "a=42\n")
    out=[]; await prepare(runtime, artifact, plan, out)
    try:
        (runtime.cache.content_path(artifact.package_id) / "main.py").write_text("a=99\n")
        d = dispatch(plan, plan.tasks[0].task_id, "corrupt")
        await runtime.handle_message(d, session_id="session-1")
        assert any(isinstance(m, p.ProgramUnavailable) for m in out)
        rejected = next(m for m in out if isinstance(m, p.TaskRejected))
        assert rejected.code is p.RejectionCode.PROGRAM_UNAVAILABLE
        assert plan.program.id not in runtime.prepared_program_ids
    finally: await runtime.shutdown()


async def test_unsupported_mode_and_capacity_are_rejected(tmp_path):
    source = '''from dag_runtime import task\n@task\ndef slow():\n    import time\n    time.sleep(.5)\n    return 1\nx=slow()\n'''
    _, artifact, plan, runtime = build(tmp_path, source, slots=1)
    out=[]; await prepare(runtime, artifact, plan, out)
    try:
        task = next(t for t in plan.tasks if t.mode is ExecutionMode.ISOLATED_CANDIDATE)
        unsupported = dispatch(plan, task.task_id, "shared", mode=ExecutionMode.SHARED_CONTEXT, context_id="ctx")
        await runtime.handle_message(unsupported, session_id="session-1")
        assert next(m for m in out if isinstance(m, p.TaskRejected)).code is p.RejectionCode.MODE_UNSUPPORTED
        first = dispatch(plan, task.task_id, "one", run_id="run-a")
        await runtime.handle_message(first, session_id="session-1")
        await eventually(lambda: "one" in runtime.active_attempt_ids)
        second = dispatch(plan, task.task_id, "two", run_id="run-b")
        await runtime.handle_message(second, session_id="session-1")
        busy = [m for m in out if isinstance(m, p.TaskRejected) and m.attempt.attempt_id == "two"]
        assert busy and busy[-1].code is p.RejectionCode.BUSY
    finally: await runtime.shutdown()


async def test_session_loss_kills_old_execution_and_never_rebinds_result(tmp_path):
    source = '''from dag_runtime import task\n@task\ndef slow():\n    import time\n    time.sleep(10)\n    return 1\nx=slow()\n'''
    _, artifact, plan, runtime = build(tmp_path, source)
    out=[]; await prepare(runtime, artifact, plan, out)
    task = next(t for t in plan.tasks if t.mode is ExecutionMode.ISOLATED_CANDIDATE)
    await runtime.handle_message(dispatch(plan, task.task_id, "old"), session_id="session-1")
    await eventually(lambda: "old" in runtime.active_attempt_ids)
    await runtime.session_lost("session-1")
    await eventually(lambda: not runtime.active_attempt_ids)
    before = len(out)
    async def send_new(message): out.append(message)
    await runtime.session_started("session-2", send_new)
    await asyncio.sleep(.1)
    assert len(out) == before
    await runtime.shutdown()


async def test_cancellation_before_child_start_is_terminal_without_spawn_leak(tmp_path):
    source = '''from dag_runtime import task\n@task\ndef slow():\n    import time; time.sleep(10); return 1\nx=slow()\n'''
    _, artifact, plan, runtime = build(tmp_path, source)
    out=[]; await prepare(runtime, artifact, plan, out)
    try:
        task = next(t for t in plan.tasks if t.mode is ExecutionMode.ISOLATED_CANDIDATE)
        d = dispatch(plan, task.task_id, "prestart")
        await runtime.handle_message(d, session_id="session-1")
        # No yield between dispatch reservation and cancellation: this exercises
        # the accepted-but-not-yet-spawned physical state.
        await runtime.handle_message(
            p.CancelTask("W1", d.attempt, "cancel before start", message_id="cancel-prestart"),
            session_id="session-1",
        )
        await wait_terminal(out, "prestart")
        cancelled = [m for m in out if isinstance(m, p.TaskCancellationResult) and m.attempt == d.attempt]
        assert len(cancelled) == 1 and cancelled[0].outcome is p.CancellationOutcome.CANCELLED
        assert not [m for m in out if isinstance(m, p.TaskSucceeded) and m.result.attempt == d.attempt]
        await eventually(lambda: not runtime.active_attempt_ids)
    finally: await runtime.shutdown()


async def test_import_failure_is_bounded_python_failure(tmp_path):
    source = '''from dag_runtime import task\n@task\ndef broken():\n    import definitely_missing_dpr_module_12345\n    return 1\nx=broken()\n'''
    _, artifact, plan, runtime = build(tmp_path, source)
    out=[]; await prepare(runtime, artifact, plan, out)
    try:
        task = next(t for t in plan.tasks if t.mode is ExecutionMode.ISOLATED_CANDIDATE)
        await runtime.handle_message(dispatch(plan, task.task_id, "importfail"), session_id="session-1")
        await wait_terminal(out, "importfail")
        failure = next(m for m in out if isinstance(m, p.TaskFailed) and m.result.attempt.attempt_id == "importfail")
        assert failure.result.failure.kind is FailureKind.PYTHON_EXCEPTION
        assert "definitely_missing_dpr_module" in (failure.result.failure.message + (failure.result.failure.traceback_text or ""))
    finally: await runtime.shutdown()


async def test_malformed_child_value_metadata_is_rejected(monkeypatch, tmp_path):
    import worker.runtime as runtime_module
    source = "a=42\n"
    _, artifact, plan, runtime = build(tmp_path, source)
    out=[]; await prepare(runtime, artifact, plan, out)
    fake_dir = tmp_path / "fake-bootstrap"; fake_dir.mkdir()
    fake_runtime = fake_dir / "runtime.py"; fake_runtime.write_text("# marker")
    fake_child = fake_dir / "isolated_child.py"
    fake_child.write_text('''import json,sys\nfrom pathlib import Path\nreq=json.loads(Path(sys.argv[1]).read_text())\nout_id=req["outputs"][0][0]\nPath(sys.argv[2]).write_text(json.dumps({"status":"success","clean_exit":False,"outputs":{out_id:{"file":f"output-{out_id}.bin","size_bytes":1,"sha256":"0"*64,"serialization":"evil"}}}))\n''')
    monkeypatch.setattr(runtime_module, "__file__", str(fake_runtime))
    try:
        await runtime.handle_message(dispatch(plan, plan.tasks[0].task_id, "malformed"), session_id="session-1")
        await wait_terminal(out, "malformed")
        failure = next(m for m in out if isinstance(m, p.TaskFailed) and m.result.attempt.attempt_id == "malformed")
        assert failure.result.failure.kind is FailureKind.EXECUTION_ERROR
        assert "descriptor is malformed" in failure.result.failure.message
    finally: await runtime.shutdown()


async def test_wrong_plan_or_program_identity_never_spawns_child(tmp_path):
    _, artifact, plan, runtime = build(tmp_path, "a=42\n")
    out=[]; await prepare(runtime, artifact, plan, out)
    try:
        task = plan.tasks[0]
        wrong_program = p.TaskDispatch(
            "W1", AttemptIdentity(plan.id, "r", task.task_id, "wrong-program"),
            "0" * 64, ExecutionMode.ISOLATED_CANDIDATE, None,
            message_id="dispatch-wrong-program",
        )
        await runtime.handle_message(wrong_program, session_id="session-1")
        wrong_plan = p.TaskDispatch(
            "W1", AttemptIdentity("1" * 64, "r", task.task_id, "wrong-plan"),
            plan.program.id, ExecutionMode.ISOLATED_CANDIDATE, None,
            message_id="dispatch-wrong-plan",
        )
        await runtime.handle_message(wrong_plan, session_id="session-1")
        rejects = [m for m in out if isinstance(m, p.TaskRejected)]
        assert len(rejects) >= 2 and all(m.code is p.RejectionCode.PROGRAM_UNAVAILABLE for m in rejects[-2:])
        assert not runtime.active_attempt_ids
    finally: await runtime.shutdown()


async def test_success_cancellation_race_has_one_execution_terminal_outcome(tmp_path):
    source = '''from dag_runtime import task\n@task\ndef quick():\n    import time; time.sleep(.02); return 7\nx=quick()\n'''
    _, artifact, plan, runtime = build(tmp_path, source)
    out=[]; await prepare(runtime, artifact, plan, out)
    try:
        task = next(t for t in plan.tasks if t.mode is ExecutionMode.ISOLATED_CANDIDATE)
        d = dispatch(plan, task.task_id, "race")
        await runtime.handle_message(d, session_id="session-1")
        await eventually(lambda: any(isinstance(m, p.TaskStarted) and m.attempt == d.attempt for m in out))
        await asyncio.sleep(.015)
        await runtime.handle_message(p.CancelTask("W1", d.attempt, "race", message_id="race-cancel"), session_id="session-1")
        await eventually(lambda: not runtime.active_attempt_ids)
        succeeded = [m for m in out if isinstance(m, p.TaskSucceeded) and m.result.attempt == d.attempt]
        cancelled = [m for m in out if isinstance(m, p.TaskCancellationResult) and m.attempt == d.attempt and m.outcome is p.CancellationOutcome.CANCELLED]
        assert not (succeeded and cancelled)
        assert len(succeeded) + len(cancelled) == 1
    finally: await runtime.shutdown()


async def test_same_package_preparation_is_single_flight_across_program_identities(tmp_path):
    source = "a=42\n"
    root = tmp_path / "src"; root.mkdir(); (root / "main.py").write_text(source)
    artifact = build_package(root)
    plan1 = lower_dag(analyze_source(source, filename="main.py"), environment_id="env-1", package_id=artifact.package_id)
    plan2 = lower_dag(analyze_source(source, filename="main.py"), environment_id="env-2", package_id=artifact.package_id)
    state = WorkerState("W1", 2, environment_ids=frozenset({"env-1", "env-2"}))
    runtime = WorkerExecutionRuntime("W1", state, PackageCache(tmp_path / "cache"))
    out=[]
    async def send(message): out.append(message)
    await runtime.session_started("session-1", send)
    one = p.PrepareProgram("W1", plan1.id, plan1.program, message_id="prep-one")
    two = p.PrepareProgram("W1", plan2.id, plan2.program, message_id="prep-two")
    await runtime.handle_message(one, session_id="session-1")
    await runtime.handle_message(two, session_id="session-1")
    assert len(runtime._package_owner) == 1
    start = p.PackageTransferStart("W1", plan1.id, plan1.program.id, artifact.package_id,
        len(artifact.archive_bytes), artifact.archive_sha256, message_id="start", correlation_id=one.message_id)
    await runtime.handle_message(start, session_id="session-1")
    offset=0
    while offset < len(artifact.archive_bytes):
        chunk=artifact.archive_bytes[offset:offset+runtime.cache.limits.chunk_bytes]
        await runtime.handle_message(p.PackageTransferChunk("W1",artifact.package_id,offset,chunk.hex(),message_id=f"c{offset}",correlation_id=one.message_id),session_id="session-1")
        offset += len(chunk)
    await runtime.handle_message(p.PackageTransferEnd("W1",artifact.package_id,len(artifact.archive_bytes),artifact.archive_sha256,message_id="end",correlation_id=one.message_id),session_id="session-1")
    try:
        prepared = [m for m in out if isinstance(m, p.ProgramPrepared)]
        assert {m.program_id for m in prepared} == {plan1.program.id, plan2.program.id}
        assert not runtime._preparations and not runtime._package_owner
    finally: await runtime.shutdown()


@pytest.mark.skipif(__import__("os").name != "posix", reason="POSIX process-group containment test")
async def test_cancellation_terminates_obvious_spawned_process_tree(tmp_path):
    import os
    pid_file = tmp_path / "grandchild.pid"
    source = f'''from dag_runtime import task\n@task\ndef spawn():\n    import subprocess, sys, time\n    child=subprocess.Popen([sys.executable,"-c", "import os,time; open({str(pid_file)!r},'w').write(str(os.getpid())); time.sleep(30)"])\n    time.sleep(30)\n    return child.pid\nx=spawn()\n'''
    _, artifact, plan, runtime = build(tmp_path, source)
    out=[]; await prepare(runtime, artifact, plan, out)
    try:
        task = next(t for t in plan.tasks if t.mode is ExecutionMode.ISOLATED_CANDIDATE)
        d=dispatch(plan,task.task_id,"tree")
        await runtime.handle_message(d,session_id="session-1")
        await eventually(lambda: pid_file.exists(), timeout=5)
        grandchild=int(pid_file.read_text())
        await runtime.handle_message(p.CancelTask("W1",d.attempt,"tree cancel",message_id="tree-cancel"),session_id="session-1")
        await eventually(lambda: not runtime.active_attempt_ids)
        def gone():
            try: os.kill(grandchild,0)
            except ProcessLookupError: return True
            return False
        await eventually(gone, timeout=3)
    finally: await runtime.shutdown()


async def test_unproven_termination_retains_physical_capacity_until_reap(monkeypatch, tmp_path):
    source = '''from dag_runtime import task\n@task\ndef slow():\n    import time; time.sleep(30); return 1\nx=slow()\n'''
    _, artifact, plan, runtime = build(tmp_path, source, slots=1)
    out=[]; await prepare(runtime, artifact, plan, out)
    task = next(t for t in plan.tasks if t.mode is ExecutionMode.ISOLATED_CANDIDATE)
    d=dispatch(plan,task.task_id,"unreaped")
    await runtime.handle_message(d,session_id="session-1")
    await eventually(lambda: any(isinstance(m,p.TaskStarted) and m.attempt==d.attempt for m in out))
    active=runtime._active["unreaped"]
    original=runtime._terminate
    async def cannot_prove(_active): return False
    monkeypatch.setattr(runtime,"_terminate",cannot_prove)
    cleaned=await runtime.session_lost("session-1")
    assert cleaned is False
    assert "unreaped" in runtime.active_attempt_ids
    state=runtime.decorate_state(runtime.base_state)
    assert state.free_slots == 0 and state.running_slots + state.reserved_slots == 1
    monkeypatch.setattr(runtime,"_terminate",original)
    assert await original(active) is True
    await eventually(lambda:not runtime.active_attempt_ids)
    assert runtime.decorate_state(runtime.base_state).free_slots == 1


async def test_protocol_message_id_never_becomes_package_staging_path(tmp_path):
    _, artifact, plan, runtime = build(tmp_path, "a=42\n")
    out=[]
    async def send(message): out.append(message)
    await runtime.session_started("session-1", send)
    command = p.PrepareProgram("W1", plan.id, plan.program, message_id="../../outside")
    await runtime.handle_message(command, session_id="session-1")
    start = p.PackageTransferStart(
        "W1", plan.id, plan.program.id, artifact.package_id,
        len(artifact.archive_bytes), artifact.archive_sha256,
        message_id="start-opaque-id", correlation_id=command.message_id,
    )
    await runtime.handle_message(start, session_id="session-1")
    try:
        prep = runtime._preparations[command.message_id]
        assert prep.path is not None
        assert prep.path.parent == runtime.cache.staging
        assert "outside" not in prep.path.name
        assert not (tmp_path / "outside").exists()
    finally:
        await runtime.session_lost("session-1")


async def test_cancelled_executor_task_keeps_capacity_until_unproven_child_reaps(monkeypatch, tmp_path):
    source = '''from dag_runtime import task\n@task\ndef slow():\n    import time; time.sleep(30); return 1\nx=slow()\n'''
    _, artifact, plan, runtime = build(tmp_path, source, slots=1)
    out=[]; await prepare(runtime, artifact, plan, out)
    task = next(t for t in plan.tasks if t.mode is ExecutionMode.ISOLATED_CANDIDATE)
    d = dispatch(plan, task.task_id, "cancelled-reaper")
    await runtime.handle_message(d, session_id="session-1")
    await eventually(lambda: any(isinstance(m, p.TaskStarted) and m.attempt == d.attempt for m in out))
    active = runtime._active[d.attempt.attempt_id]
    original = runtime._terminate

    async def cannot_prove(_active):
        return False

    monkeypatch.setattr(runtime, "_terminate", cannot_prove)
    active.task.cancel()
    await asyncio.sleep(.05)
    assert d.attempt.attempt_id in runtime.active_attempt_ids
    assert runtime.decorate_state(runtime.base_state).free_slots == 0
    assert not active.task.done()

    monkeypatch.setattr(runtime, "_terminate", original)
    assert await original(active) is True
    await eventually(lambda: active.task.done())
    await eventually(lambda: not runtime.active_attempt_ids)
    assert runtime.decorate_state(runtime.base_state).free_slots == 1


async def test_multi_output_publication_failure_rolls_back_uncommitted_store_entries(tmp_path, monkeypatch):
    from worker import DataStoreFull
    _, artifact, plan, runtime = build(tmp_path, "a,b=(1,2)\n")
    out=[]; await prepare(runtime,artifact,plan,out)
    task=plan.tasks[0]
    assert len(task.reported_output_ids)==2
    original=runtime.data_store.publish_file
    calls=0
    def fail_second(*args,**kwargs):
        nonlocal calls
        calls+=1
        if calls==2:
            raise DataStoreFull("forced second publication failure")
        return original(*args,**kwargs)
    monkeypatch.setattr(runtime.data_store,"publish_file",fail_second)
    try:
        await runtime.handle_message(dispatch(plan,task.task_id,"rollback"),session_id="session-1")
        await wait_terminal(out,"rollback")
        assert any(isinstance(m,p.TaskFailed) and m.result.attempt.attempt_id=="rollback" for m in out)
        assert runtime.data_store.item_count==0
        assert not any(isinstance(m,p.ObjectAvailable) for m in out)
    finally:
        await runtime.shutdown()


async def test_new_control_session_clears_retired_attempt_identities(tmp_path: Path):
    """A restarted coordinator begins a fresh attempt-identity sequence.

    Attempt IDs are only meaningful inside the session that issued them, so a
    worker must not reject the new coordinator's first dispatches as duplicates
    of attempts retired under the previous session.
    """
    _, artifact, plan, runtime = build(tmp_path, "a=1\n")
    out = []
    await prepare(runtime, artifact, plan, out)
    manifest = plan.tasks[0]

    await runtime.handle_message(dispatch(plan, manifest.task_id, "attempt-1"), session_id="session-1")
    await wait_terminal(out, "attempt-1")
    assert any(isinstance(m, p.TaskSucceeded) for m in out)

    # Same session: a repeated identity is still a duplicate and must be rejected.
    out.clear()
    await runtime.handle_message(dispatch(plan, manifest.task_id, "attempt-1"), session_id="session-1")
    await eventually(lambda: any(isinstance(m, p.TaskRejected) for m in out))
    assert out[-1].code is p.RejectionCode.STALE_ATTEMPT

    # New session (coordinator restart): the same identity must be accepted.
    out.clear()
    await runtime.session_lost("session-1")
    await prepare(runtime, artifact, plan, out, session_id="session-2")
    await runtime.handle_message(dispatch(plan, manifest.task_id, "attempt-1"), session_id="session-2")
    await wait_terminal(out, "attempt-1")
    assert not any(isinstance(m, p.TaskRejected) for m in out), "restarted coordinator rejected as stale"
    assert any(isinstance(m, p.TaskSucceeded) for m in out)
    assert await runtime.shutdown()


async def test_utf8_bom_entrypoint_prepares_and_runs(tmp_path: Path):
    """CPython accepts a leading UTF-8 BOM, and so must the whole pipeline.

    The CLI, the coordinator and this worker-side verification all decode the
    entrypoint with utf-8-sig.  If any one of them disagreed, the packaged bytes
    would hash differently from `program.source_sha256` and preparation would fail
    with an opaque integrity error.
    """
    source = "a = 1\nb = a + 1\n"
    root = tmp_path / "src"; root.mkdir()
    (root / "main.py").write_bytes("\ufeff".encode("utf-8") + source.encode("utf-8"))
    assert (root / "main.py").read_bytes()[:3] == b"\xef\xbb\xbf"

    artifact = build_package(root)
    decoded = (root / "main.py").read_bytes().decode("utf-8-sig")
    plan = lower_dag(analyze_source(decoded, filename="main.py"),
                     environment_id="env-test", package_id=artifact.package_id)
    state = WorkerState("W1", 2, environment_ids=frozenset({"env-test"}))
    runtime = WorkerExecutionRuntime("W1", state, PackageCache(tmp_path / "cache"))

    out = []
    await prepare(runtime, artifact, plan, out)
    assert plan.program.id in runtime.prepared_program_ids
    assert not any(isinstance(m, p.ProgramPreparationFailed) for m in out)

    manifest = plan.tasks[0]
    await runtime.handle_message(dispatch(plan, manifest.task_id, "attempt-bom"), session_id="session-1")
    await wait_terminal(out, "attempt-bom")
    assert any(isinstance(m, p.TaskSucceeded) for m in out)
    assert await runtime.shutdown()


async def test_isolated_output_is_bounded_by_value_limit_not_result_metadata(tmp_path):
    """A task may return more than the 1 MiB metadata bound; only the value limit caps it."""
    source = ('from dag_runtime import task\n@task\ndef big(n):\n    return b"v" * n\n'
              'x=big(3000000)\n')
    _, artifact, plan, runtime = build(tmp_path, source)
    out = []; await prepare(runtime, artifact, plan, out)
    try:
        task = next(t for t in plan.tasks if t.mode is ExecutionMode.ISOLATED_CANDIDATE)
        await runtime.handle_message(dispatch(plan, task.task_id, "big"), session_id="session-1")
        await wait_terminal(out, "big")
        assert any(isinstance(m, p.TaskSucceeded) for m in out), out
    finally: await runtime.shutdown()


async def test_isolated_output_over_value_limit_is_resource_exhausted(tmp_path):
    source = ('from dag_runtime import task\n@task\ndef big(n):\n    return b"v" * n\n'
              'x=big(3000000)\n')
    limits = IsolatedExecutionLimits(local_value_bytes=2 * 1024 * 1024)
    _, artifact, plan, runtime = build(tmp_path, source, limits=limits)
    out = []; await prepare(runtime, artifact, plan, out)
    try:
        task = next(t for t in plan.tasks if t.mode is ExecutionMode.ISOLATED_CANDIDATE)
        await runtime.handle_message(dispatch(plan, task.task_id, "big"), session_id="session-1")
        await wait_terminal(out, "big")
        failure = next(m for m in out if isinstance(m, p.TaskFailed))
        assert failure.result.failure.kind is FailureKind.RESOURCE_EXHAUSTED
    finally: await runtime.shutdown()


async def test_isolated_input_bound_follows_the_value_limit(tmp_path):
    import hashlib
    from worker import isolated_child
    payload = b"\x80\x05N."  # pickle of None
    path = tmp_path / "value.bin"; path.write_bytes(payload)
    desc = {"path": str(path), "size_bytes": 80 * 1024 * 1024,
            "sha256": hashlib.sha256(payload).hexdigest(), "serialization": "pickle-v1"}
    with pytest.raises(ValueError, match="size out of bounds"):
        isolated_child._load_descriptor(desc, 64 * 1024 * 1024)
    # Past the old fixed 64 MiB bound the declared size is accepted, and the file is
    # then checked against it byte for byte.
    with pytest.raises(ValueError, match="truncated"):
        isolated_child._load_descriptor(desc, 256 * 1024 * 1024)
