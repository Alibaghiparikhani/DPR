"""Representative records for every registered control message."""

import protocol as p
from execution import (
    AttemptIdentity,
    ExecutionMode,
    FailureInfo,
    FailureKind,
    ProgramIdentity,
    TaskFailure,
    TaskSuccess,
)
from scheduler import DataForm, WorkerContext, WorkerState

PLAN = "a" * 64
PROGRAM = ProgramIdentity("b" * 64, "project/α.py", "env-3.12-lock", "package-revision")
ATTEMPT = AttemptIdentity(PLAN, "run-1", "T000001", "attempt-1")
FAILURE = FailureInfo(
    FailureKind.PYTHON_EXCEPTION,
    "bad value\n\x00",
    "builtins.ValueError",
    "Traceback:\n  α.py:2\n",
)
WORKER = WorkerState(
    "W1",
    8,
    running_slots=2,
    reserved_slots=1,
    cpu_percent=12.5,
    total_memory_bytes=8192,
    available_memory_bytes=4096,
    cpu_cores=4,
    environment_ids=frozenset({PROGRAM.environment_id, "env-b"}),
    prepared_program_ids=frozenset({PROGRAM.id}),
    supported_modes=frozenset(ExecutionMode),
)
ENDPOINT = p.WorkerEndpoint("W1", "worker-one.local", 9000)
DESTINATION = p.WorkerEndpoint("W2", "::1", 9001)
DATA = p.DataReference(PLAN, "run-1", "V1", DataForm.OBJECT_SNAPSHOT, "state-2")
TRANSFER = p.TransferIdentity(DATA, "transfer-1", "transfer-attempt-1", "W1", "W2")
CONTEXT = WorkerContext("context-1", "W1", frozenset({"T000001", "T000002"}), 1)


def samples() -> tuple[p.Message, ...]:
    command = dict(message_id="command-1")
    reply = dict(message_id="reply-1", correlation_id="command-1")
    source_reply = dict(message_id="source-reply", correlation_id="send-command")
    destination_reply = dict(
        message_id="destination-reply", correlation_id="receive-command"
    )
    return (
        p.WorkerHello(WORKER, ENDPOINT, (1, 2), **command),
        p.WorkerAccepted("W1", "session-1", 1, (ENDPOINT, DESTINATION), **reply),
        p.WorkerRejected("W1", p.RejectionCode.NOT_ADMITTED, "not admitted", **reply),
        p.MembershipUpdate("W1", "session-1", 2, (ENDPOINT, DESTINATION), **command),
        p.Heartbeat(WORKER, 18, **command),
        p.HeartbeatAck("W1", 18, **reply),
        p.WorkerGoodbye("W1", "shutdown", **command),
        p.PrepareProgram("W1", PLAN, PROGRAM, **command),
        p.ProgramPrepared("W1", PLAN, PROGRAM.id, **reply),
        p.ProgramPreparationFailed("W1", PLAN, PROGRAM.id, FAILURE, **reply),
        p.ProgramUnavailable("W1", PROGRAM.id, "evicted", **command),
        p.PrepareContext(
            "W1", PLAN, "run-1", PROGRAM.id, "context-1", ("T000001",), **command
        ),
        p.ContextPrepared(PLAN, "run-1", CONTEXT, **reply),
        p.ContextPreparationFailed("W1", PLAN, "run-1", "context-1", FAILURE, **reply),
        p.ContextUnavailable("W1", PLAN, "run-1", "context-1", "lost", **command),
        p.TaskDispatch(
            "W1",
            ATTEMPT,
            PROGRAM.id,
            ExecutionMode.NATIVE_REGION,
            "context-1",
            **command,
        ),
        p.TaskAccepted("W1", ATTEMPT, **reply),
        p.TaskRejected("W1", ATTEMPT, p.RejectionCode.BUSY, "slots occupied", **reply),
        p.TaskStarted("W1", ATTEMPT, **reply),
        p.TaskSucceeded("W1", TaskSuccess(ATTEMPT, ("V1", "state-2")), **reply),
        p.TaskFailed("W1", TaskFailure(ATTEMPT, FAILURE), **reply),
        p.CancelTask("W1", ATTEMPT, "caller cancelled", **command),
        p.TaskCancellationResult(
            "W1", ATTEMPT, p.CancellationOutcome.TOO_LATE, "already ended", **reply
        ),
        p.ObjectAvailable("W1", DATA, 512, **command),
        p.ObjectUnavailable("W1", DATA, "evicted", **command),
        p.ReleaseObject("W1", DATA, "no longer needed", **command),
        p.ObjectReleased("W1", DATA, **reply),
        p.TransferRequest(TRANSFER, DESTINATION, message_id="send-command"),
        p.TransferAccepted("W1", TRANSFER, **source_reply),
        p.TransferStarted("W1", TRANSFER, **source_reply),
        p.TransferCompleted("W2", TRANSFER, 512, **destination_reply),
        p.TransferFailed(
            "W2",
            TRANSFER,
            p.TransferFailureCode.INTEGRITY_ERROR,
            "digest mismatch",
            **destination_reply,
        ),
        p.ErrorReport(p.ProtocolErrorCode.INVALID_SCHEMA, "malformed payload", **reply),
        p.PrepareReceive(TRANSFER, 512, message_id="receive-command"),
        p.ReceiveReady("W2", TRANSFER, **destination_reply),
        p.ReceivePreparationFailed(
            "W2",
            TRANSFER,
            p.TransferFailureCode.IO_ERROR,
            "cannot prepare",
            **destination_reply,
        ),
        p.AuthenticationRequest("W1", "auth-session-1", **command),
        p.AuthenticationChallenge("W1", "auth-session-1", "nonce-1", 100, **reply),
        p.AuthenticationProof("W1", "auth-session-1", "nonce-1", 100, "ab" * 32, **reply),
        p.AuthenticationAccepted("W1", "auth-session-1", **reply),
        p.PackageTransferStart("W1", PLAN, PROGRAM.id, "c" * 64, 3, "d" * 64, **reply),
        p.PackageTransferChunk("W1", "c" * 64, 0, "616263", **reply),
        p.PackageTransferEnd("W1", "c" * 64, 3, "d" * 64, **reply),
        p.CancelTransfer("W1", TRANSFER, "cancelled", **command),
        p.ReleaseContext("W1", PLAN, "run-1", "context-1", "terminal run", **command),
        p.ClientHello("client-1", **command),
        p.ClientAccepted("client-1", "client-session-1", **reply),
        p.RunSubmitStart("client-1", "run-1", "env-3.12-lock", "project/main.py",
                         PLAN, "c" * 64, 3, "d" * 64, **command),
        p.RunSubmitReady("run-1", "c" * 64, **reply),
        p.RunSubmitChunk("client-1", "run-1", "c" * 64, 0, "616263", **reply),
        p.RunSubmitEnd("client-1", "run-1", "c" * 64, 3, "d" * 64, **reply),
        p.RunSubmitted("run-1", PLAN, "running", **reply),
        p.RunStatusRequest("client-1", "run-1", **command),
        p.RunStatusResponse(
            "run-1", PLAN, "running",
            (p.RunTaskView("T000001", "running", "attempt-1", "W1", None, ""),),
            1, (), False, None, "", **reply,
        ),
        p.ClusterStatusRequest("client-1", **command),
        p.ClusterStatusResponse(
            (p.ClientWorkerView("W1", 1, "session-1", True, True, 8, 2, 1,
                                "worker-one.local", 9000),),
            ("run-1",), **reply,
        ),
        p.CancelRunRequest("client-1", "run-1", "operator cancelled", **command),
        p.CancelRunResponse("run-1", "cancelling", **reply),
        p.ClientOperationFailed("run_submit", "invalid_request", "bad submission", **reply),
    )


MESSAGES = samples()
