from __future__ import annotations

import asyncio
import pickle

import pytest

from execution import ExecutionMode
import protocol as p
from scheduler import DataForm

from .test_execution_runtime import build, dispatch, eventually, prepare, wait_terminal

pytestmark = pytest.mark.asyncio


async def _prepare_context(runtime, plan, out, *, context_id="ctx", task_ids=None):
    task_ids = tuple(task_ids or (task.task_id for task in plan.tasks))
    command = p.PrepareContext(
        "W1", plan.id, "run-1", plan.program.id, context_id, task_ids,
        message_id=f"prepare-{context_id}",
    )
    await runtime.handle_message(command, session_id="session-1")
    await eventually(lambda: any(
        isinstance(message, p.ContextPrepared)
        and message.context.context_id == context_id for message in out
    ))
    return command


async def test_persistent_context_preserves_mutable_state_and_publishes_exact_snapshots(tmp_path):
    _, artifact, plan, runtime = build(tmp_path, "a=[1]\na.append(2)\nx=len(a)\n")
    out = []
    await prepare(runtime, artifact, plan, out)
    await _prepare_context(runtime, plan, out)
    try:
        for index, manifest in enumerate(plan.tasks):
            await runtime.handle_message(
                dispatch(plan, manifest.task_id, f"ctx-{index}", mode=manifest.mode, context_id="ctx"),
                session_id="session-1",
            )
            await wait_terminal(out, f"ctx-{index}")
            assert any(
                isinstance(message, p.TaskSucceeded)
                and message.result.attempt.attempt_id == f"ctx-{index}" for message in out
            )

        first = plan.tasks[0].outputs[0]
        mutation = plan.tasks[1].objects[0].state_outputs[0]
        initial = p.DataReference(plan.id, "run-1", first.id, DataForm.OBJECT_SNAPSHOT, None)
        mutated = p.DataReference(plan.id, "run-1", first.id, DataForm.OBJECT_SNAPSHOT, mutation)
        final = runtime._immutable_data_ref(plan, "run-1", plan.tasks[2].outputs[0].id)
        assert runtime.data_store.get(initial, session_id="session-1") is not None
        assert runtime.data_store.get(mutated, session_id="session-1") is not None
        final_entry = runtime.data_store.get(final, session_id="session-1")
        assert final_entry is not None
        assert pickle.loads(final_entry.path.read_bytes()) == 2
        # Context retirement is asynchronous: the final TaskSucceeded is emitted
        # before the context teardown completes, so under load this must be awaited
        # rather than asserted instantaneously.
        await eventually(lambda: runtime.context_ids == frozenset())
        await eventually(lambda: any(
            isinstance(message, p.ContextUnavailable) and message.context_id == "ctx"
            for message in out
        ))
    finally:
        assert await runtime.shutdown()


async def test_real_native_region_executes_only_in_prepared_context(tmp_path):
    _, artifact, plan, runtime = build(tmp_path, "globals()\nx=2\ny=3\n")
    out = []
    await prepare(runtime, artifact, plan, out)
    native = plan.tasks[0]
    assert native.mode is ExecutionMode.NATIVE_REGION
    try:
        await _prepare_context(runtime, plan, out, task_ids=(native.task_id,))
        await runtime.handle_message(
            dispatch(plan, native.task_id, "native", mode=native.mode, context_id="ctx"),
            session_id="session-1",
        )
        await wait_terminal(out, "native")
        assert any(
            isinstance(message, p.TaskSucceeded)
            and message.result.attempt.attempt_id == "native" for message in out
        )
    finally:
        assert await runtime.shutdown()


async def test_context_capacity_one_rejects_overlapping_attempt_and_cleanup_restores_admission(tmp_path):
    source = "import time\ntime.sleep(.25)\nx=1\n"
    _, artifact, plan, runtime = build(tmp_path, source, slots=2)
    out = []
    await prepare(runtime, artifact, plan, out)
    task = plan.tasks[0]
    await _prepare_context(runtime, plan, out, task_ids=(task.task_id,))
    try:
        first = dispatch(plan, task.task_id, "first", mode=task.mode, context_id="ctx")
        await runtime.handle_message(first, session_id="session-1")
        await eventually(lambda: any(
            isinstance(message, p.TaskStarted) and message.attempt.attempt_id == "first" for message in out
        ))
        second = dispatch(plan, task.task_id, "second", mode=task.mode, context_id="ctx")
        await runtime.handle_message(second, session_id="session-1")
        await eventually(lambda: any(
            isinstance(message, p.TaskRejected) and message.attempt.attempt_id == "second" for message in out
        ))
        assert next(
            message for message in out
            if isinstance(message, p.TaskRejected) and message.attempt.attempt_id == "second"
        ).code is p.RejectionCode.BUSY
        await wait_terminal(out, "first")
        await eventually(lambda: "first" not in runtime.active_attempt_ids)
    finally:
        assert await runtime.shutdown()


async def test_context_cancellation_reaps_whole_context_and_is_idempotent(tmp_path):
    source = "import time\ntime.sleep(10)\n"
    _, artifact, plan, runtime = build(tmp_path, source, slots=1)
    out = []
    await prepare(runtime, artifact, plan, out)
    task = plan.tasks[0]
    await _prepare_context(runtime, plan, out, task_ids=(task.task_id,))
    try:
        d = dispatch(plan, task.task_id, "cancel-native", mode=task.mode, context_id="ctx")
        await runtime.handle_message(d, session_id="session-1")
        await eventually(lambda: any(
            isinstance(message, p.TaskStarted) and message.attempt.attempt_id == "cancel-native" for message in out
        ))
        cancel = p.CancelTask("W1", d.attempt, "stop", message_id="cancel-native")
        await runtime.handle_message(cancel, session_id="session-1")
        await eventually(lambda: any(
            isinstance(message, p.TaskCancellationResult)
            and message.attempt.attempt_id == "cancel-native" for message in out
        ))
        await eventually(lambda: not runtime.context_ids and not runtime.active_attempt_ids)
        duplicate = p.CancelTask("W1", d.attempt, "again", message_id="cancel-native-2")
        await runtime.handle_message(duplicate, session_id="session-1")
        await eventually(lambda: sum(
            isinstance(message, p.TaskCancellationResult)
            and message.attempt.attempt_id == "cancel-native" for message in out
        ) >= 2)
        outcomes = [
            message.outcome for message in out if isinstance(message, p.TaskCancellationResult)
            and message.attempt.attempt_id == "cancel-native"
        ]
        assert outcomes[0] is p.CancellationOutcome.CANCELLED
        assert outcomes[-1] is p.CancellationOutcome.TOO_LATE
    finally:
        assert await runtime.shutdown()


async def test_session_loss_retires_context_and_never_rebinds_old_attempt(tmp_path):
    source = "import time\ntime.sleep(10)\n"
    _, artifact, plan, runtime = build(tmp_path, source, slots=1)
    out = []
    await prepare(runtime, artifact, plan, out)
    task = plan.tasks[0]
    await _prepare_context(runtime, plan, out, task_ids=(task.task_id,))
    d = dispatch(plan, task.task_id, "old", mode=task.mode, context_id="ctx")
    await runtime.handle_message(d, session_id="session-1")
    await eventually(lambda: any(isinstance(message, p.TaskStarted) for message in out))
    assert await runtime.session_lost("session-1")
    assert not runtime.context_ids and not runtime.active_attempt_ids
    new_out = []
    async def send(message): new_out.append(message)
    await runtime.session_started("session-2", send)
    await asyncio.sleep(.05)
    assert not any(
        isinstance(message, (p.TaskSucceeded, p.TaskFailed, p.TaskCancellationResult))
        and getattr(getattr(message, "result", None), "attempt", getattr(message, "attempt", None)).attempt_id == "old"
        for message in new_out
    )
    assert await runtime.shutdown()

async def test_context_process_crash_fails_attempt_and_retires_context(tmp_path):
    _, artifact, plan, runtime = build(tmp_path, '__import__("os")._exit(17)\n')
    out=[]; await prepare(runtime, artifact, plan, out)
    task=plan.tasks[0]
    assert task.mode in {ExecutionMode.SHARED_CONTEXT, ExecutionMode.NATIVE_REGION}
    await _prepare_context(runtime, plan, out, task_ids=(task.task_id,))
    try:
        await runtime.handle_message(
            dispatch(plan, task.task_id, "crash", mode=task.mode, context_id="ctx"),
            session_id="session-1",
        )
        await wait_terminal(out, "crash")
        failure=next(m for m in out if isinstance(m,p.TaskFailed) and m.result.attempt.attempt_id=="crash")
        assert failure.result.failure.kind.value == "execution_error"
        await eventually(lambda: not runtime.context_ids and not runtime.active_attempt_ids)
        assert any(isinstance(m,p.ContextUnavailable) and m.context_id=="ctx" for m in out)
    finally:
        assert await runtime.shutdown()


async def test_python_exception_retires_context_because_mutation_is_uncertain(tmp_path):
    _, artifact, plan, runtime = build(tmp_path, 'raise ValueError("boom")\n')
    out=[]; await prepare(runtime, artifact, plan, out)
    task=plan.tasks[0]
    await _prepare_context(runtime, plan, out, task_ids=(task.task_id,))
    try:
        await runtime.handle_message(
            dispatch(plan,task.task_id,"raise",mode=task.mode,context_id="ctx"),
            session_id="session-1",
        )
        await wait_terminal(out,"raise")
        failure=next(m for m in out if isinstance(m,p.TaskFailed) and m.result.attempt.attempt_id=="raise")
        assert failure.result.failure.kind.value == "python_exception"
        await eventually(lambda: not runtime.context_ids)
        assert any(isinstance(m,p.ContextUnavailable) and m.context_id=="ctx" for m in out)
    finally:
        assert await runtime.shutdown()


async def test_conflicting_context_preparation_is_rejected_without_replacement(tmp_path):
    _, artifact, plan, runtime = build(tmp_path, "a=1\nb=2\n")
    out=[]; await prepare(runtime,artifact,plan,out)
    first=plan.tasks[0]
    await _prepare_context(runtime,plan,out,context_id="ctx",task_ids=(first.task_id,))
    original_ids=runtime.context_ids
    conflicting=p.PrepareContext(
        "W1",plan.id,"run-1",plan.program.id,"ctx",(plan.tasks[-1].task_id,),
        message_id="conflict",
    )
    try:
        await runtime.handle_message(conflicting,session_id="session-1")
        await eventually(lambda:any(isinstance(m,p.ContextPreparationFailed) and m.correlation_id=="conflict" for m in out))
        assert runtime.context_ids==original_ids==frozenset({"ctx"})
    finally:
        assert await runtime.shutdown()


async def test_release_context_is_idempotent_and_restores_context_admission(tmp_path):
    from worker import IsolatedExecutionLimits
    limits=IsolatedExecutionLimits(max_contexts=1)
    _, artifact, plan, runtime = build(tmp_path,"a=1\nb=2\n",limits=limits)
    out=[]; await prepare(runtime,artifact,plan,out)
    await _prepare_context(runtime,plan,out,context_id="ctx-1",task_ids=(plan.tasks[0].task_id,))
    blocked=p.PrepareContext("W1",plan.id,"run-1",plan.program.id,"ctx-2",(plan.tasks[1].task_id,),message_id="blocked")
    try:
        await runtime.handle_message(blocked,session_id="session-1")
        await eventually(lambda:any(isinstance(m,p.ContextPreparationFailed) and m.correlation_id=="blocked" for m in out))
        release=p.ReleaseContext("W1",plan.id,"run-1","ctx-1","done",message_id="release-1")
        await runtime.handle_message(release,session_id="session-1")
        await eventually(lambda:not runtime.context_ids)
        await runtime.handle_message(release,session_id="session-1")
        assert sum(isinstance(m,p.ContextUnavailable) and m.context_id=="ctx-1" for m in out)>=2
        retry=p.PrepareContext("W1",plan.id,"run-1",plan.program.id,"ctx-2",(plan.tasks[1].task_id,),message_id="retry")
        await runtime.handle_message(retry,session_id="session-1")
        await eventually(lambda:any(isinstance(m,p.ContextPrepared) and m.context.context_id=="ctx-2" for m in out))
    finally:
        assert await runtime.shutdown()


async def test_wrong_context_and_wrong_run_dispatch_do_not_touch_prepared_context(tmp_path):
    _, artifact, plan, runtime = build(tmp_path,"a=1\n")
    out=[]; await prepare(runtime,artifact,plan,out)
    task=plan.tasks[0]
    await _prepare_context(runtime,plan,out,task_ids=(task.task_id,))
    try:
        wrong_ctx=dispatch(plan,task.task_id,"wrong-ctx",mode=task.mode,context_id="other")
        await runtime.handle_message(wrong_ctx,session_id="session-1")
        wrong_run=dispatch(plan,task.task_id,"wrong-run",run_id="run-2",mode=task.mode,context_id="ctx")
        await runtime.handle_message(wrong_run,session_id="session-1")
        await eventually(lambda:sum(isinstance(m,p.TaskRejected) for m in out)>=2)
        assert runtime.context_ids==frozenset({"ctx"})
        assert not runtime.active_attempt_ids
    finally:
        assert await runtime.shutdown()


async def test_context_multi_output_publication_failure_rolls_back_before_task_failure(tmp_path, monkeypatch):
    from worker import DataStoreFull
    _,artifact,plan,runtime=build(tmp_path,"a,b=(1,2)\n")
    out=[]; await prepare(runtime,artifact,plan,out)
    task=plan.tasks[0]
    await _prepare_context(runtime,plan,out,task_ids=(task.task_id,))
    original=runtime.data_store.publish_file
    calls=0
    def fail_second(*args,**kwargs):
        nonlocal calls
        calls+=1
        if calls==2:
            raise DataStoreFull("forced context publication failure")
        return original(*args,**kwargs)
    monkeypatch.setattr(runtime.data_store,"publish_file",fail_second)
    try:
        await runtime.handle_message(
            dispatch(plan,task.task_id,"ctx-rollback",mode=task.mode,context_id="ctx"),
            session_id="session-1",
        )
        await wait_terminal(out,"ctx-rollback")
        assert any(isinstance(m,p.TaskFailed) and m.result.attempt.attempt_id=="ctx-rollback" for m in out)
        assert runtime.data_store.item_count==0
        await eventually(lambda:not runtime.context_ids)
    finally:
        assert await runtime.shutdown()
