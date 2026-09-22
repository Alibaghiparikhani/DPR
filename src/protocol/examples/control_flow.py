"""In-memory protocol smoke test. Reports are fixtures, not executed computation."""

from execution import AttemptIdentity, TaskSuccess, lower_dag
from dag_runtime.dag_engine import analyze_source
from scheduler import DataForm, WorkerState

from protocol import (
    DataReference,
    FrameDecoder,
    ObjectAvailable,
    PrepareProgram,
    PrepareReceive,
    ProgramPrepared,
    ReceiveReady,
    TaskAccepted,
    TaskDispatch,
    TaskStarted,
    TaskSucceeded,
    TransferAccepted,
    TransferCompleted,
    TransferIdentity,
    TransferRequest,
    TransferStarted,
    WorkerAccepted,
    WorkerEndpoint,
    WorkerHello,
    decode_message,
    encode_message,
    frame_payload,
)


def main() -> None:
    dag = analyze_source("x=1\ny=x+2\n")
    readiness = dag.new_readiness()
    plan = lower_dag(
        dag, environment_id="example-environment", package_id="example-package"
    )
    task = plan.tasks[0]
    worker = WorkerState(
        "worker-a",
        2,
        cpu_cores=2,
        environment_ids=frozenset({plan.program.environment_id}),
        supported_modes=frozenset({task.mode}),
    )
    a = WorkerEndpoint("worker-a", "192.0.2.1", 9001)
    b = WorkerEndpoint("worker-b", "192.0.2.2", 9002)
    attempt = AttemptIdentity(plan.id, "example-run", task.task_id, "attempt-1")
    data = DataReference(
        plan.id, attempt.run_id, task.reported_output_ids[0], DataForm.IMMUTABLE_VALUE
    )
    transfer = TransferIdentity(
        data, "transfer-1", "transfer-attempt-1", a.worker_id, b.worker_id
    )
    messages = (
        WorkerHello(worker, a, message_id="hello"),
        WorkerAccepted(
            a.worker_id,
            "session-1",
            1,
            (a, b),
            message_id="accepted",
            correlation_id="hello",
        ),
        PrepareProgram(a.worker_id, plan.id, plan.program, message_id="prepare"),
        ProgramPrepared(
            a.worker_id,
            plan.id,
            plan.program.id,
            message_id="prepared",
            correlation_id="prepare",
        ),
        TaskDispatch(
            a.worker_id, attempt, plan.program.id, task.mode, message_id="dispatch"
        ),
        TaskAccepted(
            a.worker_id, attempt, message_id="task-accepted", correlation_id="dispatch"
        ),
        TaskStarted(
            a.worker_id, attempt, message_id="started", correlation_id="dispatch"
        ),
        TaskSucceeded(
            a.worker_id,
            TaskSuccess(attempt, task.reported_output_ids),
            message_id="succeeded",
            correlation_id="dispatch",
        ),
        ObjectAvailable(a.worker_id, data, message_id="available"),
        PrepareReceive(transfer, message_id="prepare-receive"),
        ReceiveReady(
            b.worker_id,
            transfer,
            message_id="receive-ready",
            correlation_id="prepare-receive",
        ),
        TransferRequest(transfer, b, message_id="transfer"),
        TransferAccepted(
            a.worker_id, transfer, message_id="sending", correlation_id="transfer"
        ),
        TransferStarted(
            a.worker_id,
            transfer,
            message_id="transfer-started",
            correlation_id="transfer",
        ),
        TransferCompleted(
            b.worker_id,
            transfer,
            message_id="received",
            correlation_id="prepare-receive",
        ),
    )
    stream = b"".join(frame_payload(encode_message(m)) for m in messages)
    decoder = FrameDecoder()
    decoded = []
    for offset in range(0, len(stream), 7):
        decoded.extend(
            decode_message(payload)
            for payload in decoder.feed(stream[offset : offset + 7])
        )
    decoder.finish()
    assert tuple(decoded) == messages
    preparation, ready, send, accepted, started, completed = decoded[-6:]
    assert all(m.transfer == transfer for m in decoded[-6:])
    assert ready.correlation_id == completed.correlation_id == preparation.message_id
    assert accepted.correlation_id == started.correlation_id == send.message_id
    assert completed.correlation_id != send.message_id
    assert not readiness.completed
    print(
        f"Protocol smoke passed: {len(decoded)} typed messages, {len(stream)} framed bytes, 7-byte chunks."
    )
    print(
        "No tasks executed, no DAG results committed, no network or data transfer performed."
    )


if __name__ == "__main__":
    main()
