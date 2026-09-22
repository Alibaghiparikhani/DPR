"""Frozen control records with an explicit, closed wire schema.

Messages name previously prepared plans/tasks; they never carry Python objects,
manifests, source snippets, or executable callables. Peer claims are not proofs.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Callable

from execution import (
    AttemptIdentity,
    ExecutionMode,
    FailureInfo,
    FailureKind,
    ProgramIdentity,
    TaskFailure,
    TaskSuccess,
)
from scheduler import (
    DataForm,
    DataLocation,
    Replica,
    ReplicaStatus,
    WorkerContext,
    WorkerState,
)

from .limits import (
    MAX_CAPACITY,
    MAX_COLLECTION_ITEMS,
    MAX_DETAIL_BYTES,
    MAX_IDENTIFIER_BYTES,
    MAX_METADATA_BYTES,
    MAX_TRACEBACK_BYTES,
    MAX_VERSION,
)
from .validation import Schema, Spec, encode_record, require, schema_table
from .version import PROTOCOL_VERSION, validate_version


class RejectionCode(str, Enum):
    UNSUPPORTED_VERSION = "unsupported_version"
    NOT_ADMITTED = "not_admitted"
    BUSY = "busy"
    PROGRAM_UNAVAILABLE = "program_unavailable"
    ENVIRONMENT_MISMATCH = "environment_mismatch"
    MODE_UNSUPPORTED = "mode_unsupported"
    CONTEXT_UNAVAILABLE = "context_unavailable"
    INPUT_UNAVAILABLE = "input_unavailable"
    STALE_ATTEMPT = "stale_attempt"
    INVALID_REQUEST = "invalid_request"


class CancellationOutcome(str, Enum):
    CANCELLED = "cancelled"
    TOO_LATE = "too_late"
    NOT_FOUND = "not_found"
    UNSUPPORTED = "unsupported"


class TransferFailureCode(str, Enum):
    SOURCE_UNAVAILABLE = "source_unavailable"
    DESTINATION_UNAVAILABLE = "destination_unavailable"
    DATA_UNAVAILABLE = "data_unavailable"
    VERSION_MISMATCH = "version_mismatch"
    INTEGRITY_ERROR = "integrity_error"
    CANCELLED = "cancelled"
    IO_ERROR = "io_error"


class ProtocolErrorCode(str, Enum):
    UNSUPPORTED_VERSION = "unsupported_version"
    MALFORMED_MESSAGE = "malformed_message"
    UNKNOWN_MESSAGE_TYPE = "unknown_message_type"
    INVALID_FIELD = "invalid_field"
    INVALID_SCHEMA = "invalid_schema"
    FRAME_TOO_LARGE = "frame_too_large"
    RESOURCE_LIMIT = "resource_limit"


class _Record:
    __slots__ = ()

    def __post_init__(self) -> None:
        require(type(self) in SCHEMAS, "record", "unsupported record class")
        encode_record(self, type(self), SCHEMAS)


@dataclass(frozen=True, slots=True, kw_only=True)
class Message(_Record):
    """Caller-assigned identity; correlation names the originating command.

    All response/event continuations of a command require its message_id as
    correlation_id. Unsolicited notices and commands prohibit correlation_id.
    ErrorReport permits either because malformed envelopes may have no valid ID.
    """

    message_id: str
    correlation_id: str | None = None
    protocol_version: int = PROTOCOL_VERSION


@dataclass(frozen=True, slots=True)
class WorkerEndpoint(_Record):
    worker_id: str
    host: str
    port: int


@dataclass(frozen=True, slots=True)
class DataReference(_Record):
    """Run-scoped transferable representation, using scheduler.DataForm.

    object_state_id=None means the initial producer-established snapshot.
    It never means latest. State tokens themselves are not DataForm values.
    """

    plan_id: str
    run_id: str
    value_id: str
    form: DataForm
    object_state_id: str | None = None

    @classmethod
    def from_location(
        cls, plan_id: str, run_id: str, location: DataLocation
    ) -> DataReference:
        """Copy a certified location key, without copying replicas or inventing size."""
        encode_record(location, DataLocation, SCHEMAS)
        return cls(
            plan_id, run_id, location.value_id, location.form, location.object_state_id
        )

    def to_location(
        self, replicas: tuple[Replica, ...] = (), size_bytes: int | None = None
    ) -> DataLocation:
        """Project metadata into a snapshot whose caller must preserve plan/run scope."""
        encode_record(self, DataReference, SCHEMAS)
        # Validate before the parent model can normalize an arbitrary iterable.
        from .validation import convert

        convert(replicas, REPLICAS, SCHEMAS, decode=False, path="replicas")
        convert(size_bytes, OPTIONAL_COUNT, SCHEMAS, decode=False, path="size_bytes")
        require(
            len({r.worker_id for r in replicas}) == len(replicas),
            "replicas",
            "duplicate worker",
        )
        result = DataLocation(
            self.value_id, self.form, replicas, size_bytes, self.object_state_id
        )
        encode_record(result, DataLocation, SCHEMAS)
        return result


@dataclass(frozen=True, slots=True)
class TransferIdentity(_Record):
    """Caller-owned operation/attempt identity shared by both control-channel legs.

    Message IDs identify individual commands; they are not transfer identities.
    Every event must retain this complete identity, including data scope/route.
    """

    data: DataReference
    transfer_id: str
    transfer_attempt_id: str
    source_worker_id: str
    destination_worker_id: str


@dataclass(frozen=True, slots=True)
class AuthenticationRequest(Message):
    """Pre-admission application authentication intent for one TLS connection."""

    node_id: str
    session_id: str


@dataclass(frozen=True, slots=True)
class AuthenticationChallenge(Message):
    node_id: str
    session_id: str
    nonce: str
    issued_at: int


@dataclass(frozen=True, slots=True)
class AuthenticationProof(Message):
    node_id: str
    session_id: str
    nonce: str
    issued_at: int
    mac_hex: str


@dataclass(frozen=True, slots=True)
class AuthenticationAccepted(Message):
    node_id: str
    session_id: str




@dataclass(frozen=True, slots=True)
class ClientHello(Message):
    """Authenticated operator/client admission after the common TLS+HMAC handshake."""

    client_id: str


@dataclass(frozen=True, slots=True)
class ClientAccepted(Message):
    client_id: str
    session_id: str


@dataclass(frozen=True, slots=True)
class RunSubmitStart(Message):
    """Begin one bounded deterministic package submission from an authenticated client."""

    client_id: str
    run_id: str
    environment_id: str
    entrypoint: str
    plan_id: str
    package_id: str
    archive_size: int
    archive_sha256: str


@dataclass(frozen=True, slots=True)
class RunSubmitReady(Message):
    """Coordinator accepted bounded submission staging; package bytes may follow."""

    run_id: str
    package_id: str


@dataclass(frozen=True, slots=True)
class RunSubmitChunk(Message):
    client_id: str
    run_id: str
    package_id: str
    offset: int
    data_hex: str


@dataclass(frozen=True, slots=True)
class RunSubmitEnd(Message):
    client_id: str
    run_id: str
    package_id: str
    archive_size: int
    archive_sha256: str


@dataclass(frozen=True, slots=True)
class RunSubmitted(Message):
    run_id: str
    plan_id: str
    status: str


@dataclass(frozen=True, slots=True)
class RunStatusRequest(Message):
    client_id: str
    run_id: str
    include_tasks: bool = True


@dataclass(frozen=True, slots=True)
class RunTaskView(_Record):
    task_id: str
    status: str
    attempt_id: str | None = None
    worker_id: str | None = None
    failure_kind: str | None = None
    detail: str = ""
    output_ids: tuple[str, ...] = ()
    exception_type: str | None = None
    stdout_tail: str = ""
    stderr_tail: str = ""
    stdout_truncated: bool = False
    stderr_truncated: bool = False


@dataclass(frozen=True, slots=True)
class ClientContextView(_Record):
    context_id: str
    worker_id: str
    available_slots: int
    prepared_task_count: int


@dataclass(frozen=True, slots=True)
class RunStatusResponse(Message):
    run_id: str
    plan_id: str
    status: str
    tasks: tuple[RunTaskView, ...]
    task_count: int
    contexts: tuple[ClientContextView, ...] = ()
    tasks_truncated: bool = False
    failure_kind: str | None = None
    failure_detail: str = ""


@dataclass(frozen=True, slots=True)
class ClientWorkerView(_Record):
    worker_id: str
    generation: int
    session_id: str
    online: bool
    accepting_work: bool
    total_slots: int
    running_slots: int
    reserved_slots: int
    host: str
    port: int


@dataclass(frozen=True, slots=True)
class ClusterStatusRequest(Message):
    client_id: str


@dataclass(frozen=True, slots=True)
class ClusterStatusResponse(Message):
    workers: tuple[ClientWorkerView, ...]
    run_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class CancelRunRequest(Message):
    client_id: str
    run_id: str
    reason: str = "run cancelled by client"


@dataclass(frozen=True, slots=True)
class CancelRunResponse(Message):
    run_id: str
    status: str


@dataclass(frozen=True, slots=True)
class ClientOperationFailed(Message):
    operation: str
    code: str
    detail: str = ""


@dataclass(frozen=True, slots=True)
class WorkerHello(Message):
    worker: WorkerState
    endpoint: WorkerEndpoint
    supported_versions: tuple[int, ...] = (PROTOCOL_VERSION,)


@dataclass(frozen=True, slots=True)
class WorkerAccepted(Message):
    worker_id: str
    session_id: str
    selected_version: int
    members: tuple[WorkerEndpoint, ...] = ()


@dataclass(frozen=True, slots=True)
class WorkerRejected(Message):
    worker_id: str
    code: RejectionCode
    detail: str = ""


@dataclass(frozen=True, slots=True)
class MembershipUpdate(Message):
    """A full membership view for one admitted session, with caller-owned revision."""

    worker_id: str
    session_id: str
    revision: int
    members: tuple[WorkerEndpoint, ...]


@dataclass(frozen=True, slots=True)
class Heartbeat(Message):
    worker: WorkerState
    sequence: int


@dataclass(frozen=True, slots=True)
class HeartbeatAck(Message):
    worker_id: str
    sequence: int


@dataclass(frozen=True, slots=True)
class WorkerGoodbye(Message):
    worker_id: str
    reason: str = ""


@dataclass(frozen=True, slots=True)
class PrepareProgram(Message):
    worker_id: str
    plan_id: str
    program: ProgramIdentity


@dataclass(frozen=True, slots=True)
class ProgramPrepared(Message):
    worker_id: str
    plan_id: str
    program_id: str


@dataclass(frozen=True, slots=True)
class ProgramPreparationFailed(Message):
    worker_id: str
    plan_id: str
    program_id: str
    failure: FailureInfo


@dataclass(frozen=True, slots=True)
class ProgramUnavailable(Message):
    worker_id: str
    program_id: str
    reason: str = ""


@dataclass(frozen=True, slots=True)
class PackageTransferStart(Message):
    """Begin one bounded package delivery correlated to PrepareProgram."""

    worker_id: str
    plan_id: str
    program_id: str
    package_id: str
    size_bytes: int
    archive_sha256: str


@dataclass(frozen=True, slots=True)
class PackageTransferChunk(Message):
    """One ordered package byte range encoded as lowercase hex."""

    worker_id: str
    package_id: str
    offset: int
    data_hex: str


@dataclass(frozen=True, slots=True)
class PackageTransferEnd(Message):
    worker_id: str
    package_id: str
    size_bytes: int
    archive_sha256: str


@dataclass(frozen=True, slots=True)
class PrepareContext(Message):
    worker_id: str
    plan_id: str
    run_id: str
    program_id: str
    context_id: str
    task_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ContextPrepared(Message):
    plan_id: str
    run_id: str
    context: WorkerContext


@dataclass(frozen=True, slots=True)
class ContextPreparationFailed(Message):
    worker_id: str
    plan_id: str
    run_id: str
    context_id: str
    failure: FailureInfo


@dataclass(frozen=True, slots=True)
class ContextUnavailable(Message):
    worker_id: str
    plan_id: str
    run_id: str
    context_id: str
    reason: str = ""


@dataclass(frozen=True, slots=True)
class ReleaseContext(Message):
    """Coordinator requests physical retirement of one exact run context."""

    worker_id: str
    plan_id: str
    run_id: str
    context_id: str
    reason: str = ""


@dataclass(frozen=True, slots=True)
class TaskDispatch(Message):
    """Request one exact attempt on a prepared plan, without supplying executable code."""

    worker_id: str
    attempt: AttemptIdentity
    program_id: str
    mode: ExecutionMode
    context_id: str | None = None


@dataclass(frozen=True, slots=True)
class TaskAccepted(Message):
    worker_id: str
    attempt: AttemptIdentity


@dataclass(frozen=True, slots=True)
class TaskRejected(Message):
    worker_id: str
    attempt: AttemptIdentity
    code: RejectionCode
    detail: str = ""


@dataclass(frozen=True, slots=True)
class TaskStarted(Message):
    worker_id: str
    attempt: AttemptIdentity


@dataclass(frozen=True, slots=True)
class TaskSucceeded(Message):
    """Logical output acknowledgements, including native views/tokens; never a commit."""

    worker_id: str
    result: TaskSuccess


@dataclass(frozen=True, slots=True)
class TaskFailed(Message):
    worker_id: str
    result: TaskFailure


@dataclass(frozen=True, slots=True)
class CancelTask(Message):
    worker_id: str
    attempt: AttemptIdentity
    reason: str = ""


@dataclass(frozen=True, slots=True)
class TaskCancellationResult(Message):
    """Cancellation is an observation, not rollback, commit, or retry permission."""

    worker_id: str
    attempt: AttemptIdentity
    outcome: CancellationOutcome
    detail: str = ""


@dataclass(frozen=True, slots=True)
class ObjectAvailable(Message):
    worker_id: str
    data: DataReference
    size_bytes: int | None = None


@dataclass(frozen=True, slots=True)
class ObjectUnavailable(Message):
    worker_id: str
    data: DataReference
    reason: str = ""


@dataclass(frozen=True, slots=True)
class ReleaseObject(Message):
    worker_id: str
    data: DataReference
    reason: str = ""


@dataclass(frozen=True, slots=True)
class ObjectReleased(Message):
    worker_id: str
    data: DataReference


@dataclass(frozen=True, slots=True)
class PrepareReceive(Message):
    """Coordinator asks the destination to prepare this exact transfer attempt.

    The optional Batch-3 authorization tuple is emitted as an all-or-none set by
    the coordinator.  It binds the byte-plane authorization to the exact source
    and destination control sessions without changing TransferIdentity itself.
    Legacy/unit constructions may omit it; a real byte-plane worker fails closed
    if authorization is absent.
    """

    transfer: TransferIdentity
    size_bytes: int | None = None
    source_session_id: str | None = None
    destination_session_id: str | None = None
    authorization: str | None = None


@dataclass(frozen=True, slots=True)
class ReceiveReady(Message):
    """Destination is prepared; correlates to its PrepareReceive command."""

    worker_id: str
    transfer: TransferIdentity


@dataclass(frozen=True, slots=True)
class ReceivePreparationFailed(Message):
    """Destination could not prepare; correlates to PrepareReceive, before readiness."""

    worker_id: str
    transfer: TransferIdentity
    code: TransferFailureCode
    detail: str = ""


@dataclass(frozen=True, slots=True)
class TransferRequest(Message):
    """Source-only send command, issued after matching destination ReceiveReady."""

    transfer: TransferIdentity
    destination: WorkerEndpoint
    source_session_id: str | None = None
    destination_session_id: str | None = None
    authorization: str | None = None


@dataclass(frozen=True, slots=True)
class TransferAccepted(Message):
    """Source accepts its TransferRequest; correlation names that source command."""

    worker_id: str
    transfer: TransferIdentity


@dataclass(frozen=True, slots=True)
class TransferStarted(Message):
    """Source starts sending; correlation names its TransferRequest."""

    worker_id: str
    transfer: TransferIdentity


@dataclass(frozen=True, slots=True)
class TransferCompleted(Message):
    """Destination confirms validated receipt; correlates to PrepareReceive.

    It never requires a source-only TransferRequest message ID. A source send
    alone cannot establish destination possession or coordinator commitment.
    """

    worker_id: str
    transfer: TransferIdentity
    size_bytes: int | None = None


@dataclass(frozen=True, slots=True)
class TransferFailed(Message):
    """Participant failure, without retry or rollback policy.

    Source worker: correlate to TransferRequest. Destination worker: correlate
    to PrepareReceive, after readiness. worker_id unambiguously identifies the
    origin because a TransferIdentity always has distinct source/destination.
    """

    worker_id: str
    transfer: TransferIdentity
    code: TransferFailureCode
    detail: str = ""


@dataclass(frozen=True, slots=True)
class CancelTransfer(Message):
    """Coordinator cancellation/cleanup command for one exact transfer attempt.

    This does not change logical task retry policy.  A participant uses the
    original PrepareReceive/TransferRequest correlation when reporting terminal
    cleanup evidence through TransferFailed.
    """

    worker_id: str
    transfer: TransferIdentity
    reason: str = ""


@dataclass(frozen=True, slots=True)
class ErrorReport(Message):
    """Structured peer error; distinct from local ProtocolError and TaskFailure."""

    code: ProtocolErrorCode
    detail: str = ""


# Schema descriptors are local constants, never interpreted from peer input.
ID = Spec("text", minimum=1, maximum=MAX_IDENTIFIER_BYTES, identifier=True)
HASH = Spec("hash", minimum=1, maximum=64)
METADATA = Spec("text", minimum=1, maximum=MAX_METADATA_BYTES)
DETAIL = Spec("text", maximum=MAX_DETAIL_BYTES)
PACKAGE_CHUNK_HEX = Spec("text", minimum=2, maximum=256 * 1024)
CLIENT_PACKAGE_CHUNK_HEX = Spec("text", minimum=2, maximum=256 * 1024)
STATUS = Spec("text", minimum=1, maximum=64, identifier=True)
COUNT = Spec("int")
CAPACITY = Spec("int", maximum=MAX_CAPACITY)
BOOL = Spec("bool")
VERSION = Spec("int", minimum=1, maximum=MAX_VERSION)


def _optional(spec: Spec) -> Spec:
    return Spec("optional", item=spec)


def _record(cls: type) -> Spec:
    return Spec("record", cls=cls)


def _enum(cls: type[Enum]) -> Spec:
    return Spec("enum", cls=cls)


IDS = Spec("tuple", item=ID, unique=True, maximum=MAX_COLLECTION_ITEMS)
OPTIONAL_ID = _optional(ID)
OPTIONAL_COUNT = _optional(COUNT)
ATTEMPT = _record(AttemptIdentity)
DATA = _record(DataReference)
TRANSFER = _record(TransferIdentity)
FAILURE = _record(FailureInfo)
ENDPOINTS = Spec("tuple", item=_record(WorkerEndpoint), maximum=MAX_COLLECTION_ITEMS)
REPLICAS = Spec("tuple", item=_record(Replica), maximum=MAX_COLLECTION_ITEMS)


def _program(p: ProgramIdentity) -> None:
    expected = ProgramIdentity(
        p.source_sha256, p.filename, p.environment_id, p.package_id
    )
    require(p.id == expected.id, "program.id", "derived identity mismatch")


def _failure(f: FailureInfo) -> None:
    require(
        f.kind != FailureKind.PYTHON_EXCEPTION or f.exception_type is not None,
        "failure.exception_type",
        "Python exception requires a type name",
    )


def _worker(w: WorkerState) -> None:
    require(
        w.running_slots + w.reserved_slots <= w.total_slots,
        "worker",
        "occupancy exceeds total slots",
    )
    require(
        w.available_memory_bytes <= w.total_memory_bytes,
        "worker",
        "available memory exceeds total",
    )


def _data(d: DataReference | DataLocation) -> None:
    require(
        d.object_state_id is None or d.form == DataForm.OBJECT_SNAPSHOT,
        "data.object_state_id",
        "only snapshots have state versions",
    )


def _location(d: DataLocation) -> None:
    _data(d)
    require(
        len({r.worker_id for r in d.replicas}) == len(d.replicas),
        "replicas",
        "duplicate worker",
    )


def _transfer(t: TransferIdentity) -> None:
    require(
        t.source_worker_id != t.destination_worker_id,
        "transfer",
        "source equals destination",
    )


def _members(m: WorkerAccepted | MembershipUpdate) -> None:
    require(
        len({e.worker_id for e in m.members}) == len(m.members),
        "members",
        "duplicate worker",
    )


def _hello(m: WorkerHello) -> None:
    require(
        m.worker.worker_id == m.endpoint.worker_id,
        "hello",
        "worker and endpoint disagree",
    )
    require(
        m.protocol_version in m.supported_versions,
        "hello",
        "envelope version not advertised",
    )


def _accepted(m: WorkerAccepted) -> None:
    validate_version(m.selected_version)
    _members(m)


def _package_chunk(m: PackageTransferChunk) -> None:
    require(len(m.data_hex) % 2 == 0, "package.data_hex", "hex data length must be even")
    require(all(c in "0123456789abcdef" for c in m.data_hex), "package.data_hex", "expected lowercase hex")


def _dispatch(m: TaskDispatch) -> None:
    require(
        m.mode == ExecutionMode.ISOLATED_CANDIDATE or m.context_id is not None,
        "dispatch.context_id",
        "native/shared modes require an exact context",
    )


def _authorization_tuple(m: PrepareReceive | TransferRequest) -> None:
    values = (m.source_session_id, m.destination_session_id, m.authorization)
    require(
        all(value is None for value in values) or all(value is not None for value in values),
        "authorization",
        "session/authorization fields must be all present or all absent",
    )


def _destination(m: TransferRequest) -> None:
    require(
        m.destination.worker_id == m.transfer.destination_worker_id,
        "transfer",
        "destination mismatch",
    )
    _authorization_tuple(m)


def _prepare_receive(m: PrepareReceive) -> None:
    _authorization_tuple(m)


def _cancel_transfer(m: CancelTransfer) -> None:
    require(
        m.worker_id in {m.transfer.source_worker_id, m.transfer.destination_worker_id},
        "worker_id",
        "worker is not a transfer participant",
    )


def _receive_report(m: ReceiveReady | ReceivePreparationFailed) -> None:
    require(
        m.worker_id == m.transfer.destination_worker_id,
        "transfer",
        "receive preparation report must name destination",
    )


def _source_report(m: TransferAccepted | TransferStarted) -> None:
    require(
        m.worker_id == m.transfer.source_worker_id,
        "transfer",
        "report must name source worker",
    )


def _completed(m: TransferCompleted) -> None:
    require(
        m.worker_id == m.transfer.destination_worker_id,
        "transfer",
        "completion must name destination",
    )


def _transfer_failed(m: TransferFailed) -> None:
    require(
        m.worker_id in (m.transfer.source_worker_id, m.transfer.destination_worker_id),
        "transfer",
        "reporter is not a transfer participant",
    )


@dataclass(frozen=True, slots=True)
class MessageDefinition:
    wire_type: str
    cls: type[Message]
    correlation: str
    fields: tuple[tuple[str, Spec], ...]
    check: Callable[[object], None] | None = None


# All wire identifiers are explicit; Python class names are not a wire contract.
DEFINITIONS = (
    MessageDefinition(
        "auth.request", AuthenticationRequest, "none",
        (("node_id", ID), ("session_id", ID)),
    ),
    MessageDefinition(
        "auth.challenge", AuthenticationChallenge, "required",
        (("node_id", ID), ("session_id", ID), ("nonce", ID), ("issued_at", COUNT)),
    ),
    MessageDefinition(
        "auth.proof", AuthenticationProof, "required",
        (("node_id", ID), ("session_id", ID), ("nonce", ID), ("issued_at", COUNT), ("mac_hex", ID)),
    ),
    MessageDefinition(
        "auth.accepted", AuthenticationAccepted, "required",
        (("node_id", ID), ("session_id", ID)),
    ),
    MessageDefinition(
        "client.hello", ClientHello, "none", (("client_id", ID),),
    ),
    MessageDefinition(
        "client.accepted", ClientAccepted, "required",
        (("client_id", ID), ("session_id", ID)),
    ),
    MessageDefinition(
        "client.run_submit_start", RunSubmitStart, "none",
        (("client_id", ID), ("run_id", ID), ("environment_id", ID),
         ("entrypoint", METADATA), ("plan_id", HASH), ("package_id", HASH),
         ("archive_size", COUNT), ("archive_sha256", HASH)),
    ),
    MessageDefinition(
        "client.run_submit_ready", RunSubmitReady, "required",
        (("run_id", ID), ("package_id", HASH)),
    ),
    MessageDefinition(
        "client.run_submit_chunk", RunSubmitChunk, "required",
        (("client_id", ID), ("run_id", ID), ("package_id", HASH),
         ("offset", COUNT), ("data_hex", CLIENT_PACKAGE_CHUNK_HEX)),
        _package_chunk,
    ),
    MessageDefinition(
        "client.run_submit_end", RunSubmitEnd, "required",
        (("client_id", ID), ("run_id", ID), ("package_id", HASH),
         ("archive_size", COUNT), ("archive_sha256", HASH)),
    ),
    MessageDefinition(
        "client.run_submitted", RunSubmitted, "required",
        (("run_id", ID), ("plan_id", HASH), ("status", STATUS)),
    ),
    MessageDefinition(
        "client.run_status_request", RunStatusRequest, "none",
        (("client_id", ID), ("run_id", ID), ("include_tasks", BOOL)),
    ),
    MessageDefinition(
        "client.run_status_response", RunStatusResponse, "required",
        (("run_id", ID), ("plan_id", HASH), ("status", STATUS),
         ("tasks", Spec("tuple", item=_record(RunTaskView), maximum=1024)),
         ("task_count", COUNT),
         ("contexts", Spec("tuple", item=_record(ClientContextView), maximum=1024)),
         ("tasks_truncated", BOOL),
         ("failure_kind", _optional(STATUS)), ("failure_detail", DETAIL)),
    ),
    MessageDefinition(
        "client.cluster_status_request", ClusterStatusRequest, "none",
        (("client_id", ID),),
    ),
    MessageDefinition(
        "client.cluster_status_response", ClusterStatusResponse, "required",
        (("workers", Spec("tuple", item=_record(ClientWorkerView), maximum=1024)),
         ("run_ids", Spec("tuple", item=ID, maximum=1024, unique=True))),
    ),
    MessageDefinition(
        "client.cancel_run_request", CancelRunRequest, "none",
        (("client_id", ID), ("run_id", ID), ("reason", DETAIL)),
    ),
    MessageDefinition(
        "client.cancel_run_response", CancelRunResponse, "required",
        (("run_id", ID), ("status", STATUS)),
    ),
    MessageDefinition(
        "client.operation_failed", ClientOperationFailed, "required",
        (("operation", STATUS), ("code", STATUS), ("detail", DETAIL)),
    ),
    MessageDefinition(
        "worker.hello",
        WorkerHello,
        "none",
        (
            ("worker", _record(WorkerState)),
            ("endpoint", _record(WorkerEndpoint)),
            (
                "supported_versions",
                Spec("tuple", item=VERSION, minimum=1, maximum=32, unique=True),
            ),
        ),
        _hello,
    ),
    MessageDefinition(
        "worker.accepted",
        WorkerAccepted,
        "required",
        (
            ("worker_id", ID),
            ("session_id", ID),
            ("selected_version", VERSION),
            ("members", ENDPOINTS),
        ),
        _accepted,
    ),
    MessageDefinition(
        "worker.rejected",
        WorkerRejected,
        "required",
        (("worker_id", ID), ("code", _enum(RejectionCode)), ("detail", DETAIL)),
    ),
    MessageDefinition(
        "worker.membership",
        MembershipUpdate,
        "none",
        (
            ("worker_id", ID),
            ("session_id", ID),
            ("revision", COUNT),
            ("members", ENDPOINTS),
        ),
        _members,
    ),
    MessageDefinition(
        "worker.heartbeat",
        Heartbeat,
        "none",
        (("worker", _record(WorkerState)), ("sequence", COUNT)),
    ),
    MessageDefinition(
        "worker.heartbeat_ack",
        HeartbeatAck,
        "required",
        (("worker_id", ID), ("sequence", COUNT)),
    ),
    MessageDefinition(
        "worker.goodbye", WorkerGoodbye, "none", (("worker_id", ID), ("reason", DETAIL))
    ),
    MessageDefinition(
        "program.prepare",
        PrepareProgram,
        "none",
        (("worker_id", ID), ("plan_id", HASH), ("program", _record(ProgramIdentity))),
    ),
    MessageDefinition(
        "program.prepared",
        ProgramPrepared,
        "required",
        (("worker_id", ID), ("plan_id", HASH), ("program_id", HASH)),
    ),
    MessageDefinition(
        "program.preparation_failed",
        ProgramPreparationFailed,
        "required",
        (
            ("worker_id", ID),
            ("plan_id", HASH),
            ("program_id", HASH),
            ("failure", FAILURE),
        ),
    ),
    MessageDefinition(
        "program.unavailable",
        ProgramUnavailable,
        "none",
        (("worker_id", ID), ("program_id", HASH), ("reason", DETAIL)),
    ),
    MessageDefinition(
        "package.start", PackageTransferStart, "required",
        (("worker_id", ID), ("plan_id", HASH), ("program_id", HASH),
         ("package_id", HASH), ("size_bytes", COUNT), ("archive_sha256", HASH)),
    ),
    MessageDefinition(
        "package.chunk", PackageTransferChunk, "required",
        (("worker_id", ID), ("package_id", HASH), ("offset", COUNT), ("data_hex", PACKAGE_CHUNK_HEX)),
        _package_chunk,
    ),
    MessageDefinition(
        "package.end", PackageTransferEnd, "required",
        (("worker_id", ID), ("package_id", HASH), ("size_bytes", COUNT), ("archive_sha256", HASH)),
    ),
    MessageDefinition(
        "context.prepare",
        PrepareContext,
        "none",
        (
            ("worker_id", ID),
            ("plan_id", HASH),
            ("run_id", ID),
            ("program_id", HASH),
            ("context_id", ID),
            ("task_ids", IDS),
        ),
    ),
    MessageDefinition(
        "context.prepared",
        ContextPrepared,
        "required",
        (("plan_id", HASH), ("run_id", ID), ("context", _record(WorkerContext))),
    ),
    MessageDefinition(
        "context.preparation_failed",
        ContextPreparationFailed,
        "required",
        (
            ("worker_id", ID),
            ("plan_id", HASH),
            ("run_id", ID),
            ("context_id", ID),
            ("failure", FAILURE),
        ),
    ),
    MessageDefinition(
        "context.unavailable",
        ContextUnavailable,
        "none",
        (
            ("worker_id", ID),
            ("plan_id", HASH),
            ("run_id", ID),
            ("context_id", ID),
            ("reason", DETAIL),
        ),
    ),
    MessageDefinition(
        "context.release",
        ReleaseContext,
        "none",
        (
            ("worker_id", ID),
            ("plan_id", HASH),
            ("run_id", ID),
            ("context_id", ID),
            ("reason", DETAIL),
        ),
    ),
    MessageDefinition(
        "task.dispatch",
        TaskDispatch,
        "none",
        (
            ("worker_id", ID),
            ("attempt", ATTEMPT),
            ("program_id", HASH),
            ("mode", _enum(ExecutionMode)),
            ("context_id", OPTIONAL_ID),
        ),
        _dispatch,
    ),
    MessageDefinition(
        "task.accepted",
        TaskAccepted,
        "required",
        (("worker_id", ID), ("attempt", ATTEMPT)),
    ),
    MessageDefinition(
        "task.rejected",
        TaskRejected,
        "required",
        (
            ("worker_id", ID),
            ("attempt", ATTEMPT),
            ("code", _enum(RejectionCode)),
            ("detail", DETAIL),
        ),
    ),
    MessageDefinition(
        "task.started",
        TaskStarted,
        "required",
        (("worker_id", ID), ("attempt", ATTEMPT)),
    ),
    MessageDefinition(
        "task.succeeded",
        TaskSucceeded,
        "required",
        (("worker_id", ID), ("result", _record(TaskSuccess))),
    ),
    MessageDefinition(
        "task.failed",
        TaskFailed,
        "required",
        (("worker_id", ID), ("result", _record(TaskFailure))),
    ),
    MessageDefinition(
        "task.cancel",
        CancelTask,
        "none",
        (("worker_id", ID), ("attempt", ATTEMPT), ("reason", DETAIL)),
    ),
    MessageDefinition(
        "task.cancellation_result",
        TaskCancellationResult,
        "required",
        (
            ("worker_id", ID),
            ("attempt", ATTEMPT),
            ("outcome", _enum(CancellationOutcome)),
            ("detail", DETAIL),
        ),
    ),
    MessageDefinition(
        "object.available",
        ObjectAvailable,
        "none",
        (("worker_id", ID), ("data", DATA), ("size_bytes", OPTIONAL_COUNT)),
    ),
    MessageDefinition(
        "object.unavailable",
        ObjectUnavailable,
        "none",
        (("worker_id", ID), ("data", DATA), ("reason", DETAIL)),
    ),
    MessageDefinition(
        "object.release",
        ReleaseObject,
        "none",
        (("worker_id", ID), ("data", DATA), ("reason", DETAIL)),
    ),
    MessageDefinition(
        "object.released",
        ObjectReleased,
        "required",
        (("worker_id", ID), ("data", DATA)),
    ),
    MessageDefinition(
        "transfer.prepare_receive",
        PrepareReceive,
        "none",
        (("transfer", TRANSFER), ("size_bytes", OPTIONAL_COUNT),
         ("source_session_id", OPTIONAL_ID), ("destination_session_id", OPTIONAL_ID),
         ("authorization", OPTIONAL_ID)),
        _prepare_receive,
    ),
    MessageDefinition(
        "transfer.receive_ready",
        ReceiveReady,
        "required",
        (("worker_id", ID), ("transfer", TRANSFER)),
        _receive_report,
    ),
    MessageDefinition(
        "transfer.receive_preparation_failed",
        ReceivePreparationFailed,
        "required",
        (
            ("worker_id", ID),
            ("transfer", TRANSFER),
            ("code", _enum(TransferFailureCode)),
            ("detail", DETAIL),
        ),
        _receive_report,
    ),
    MessageDefinition(
        "transfer.request",
        TransferRequest,
        "none",
        (("transfer", TRANSFER), ("destination", _record(WorkerEndpoint)),
         ("source_session_id", OPTIONAL_ID), ("destination_session_id", OPTIONAL_ID),
         ("authorization", OPTIONAL_ID)),
        _destination,
    ),
    MessageDefinition(
        "transfer.accepted",
        TransferAccepted,
        "required",
        (("worker_id", ID), ("transfer", TRANSFER)),
        _source_report,
    ),
    MessageDefinition(
        "transfer.started",
        TransferStarted,
        "required",
        (("worker_id", ID), ("transfer", TRANSFER)),
        _source_report,
    ),
    MessageDefinition(
        "transfer.completed",
        TransferCompleted,
        "required",
        (("worker_id", ID), ("transfer", TRANSFER), ("size_bytes", OPTIONAL_COUNT)),
        _completed,
    ),
    MessageDefinition(
        "transfer.failed",
        TransferFailed,
        "required",
        (
            ("worker_id", ID),
            ("transfer", TRANSFER),
            ("code", _enum(TransferFailureCode)),
            ("detail", DETAIL),
        ),
        _transfer_failed,
    ),
    MessageDefinition(
        "transfer.cancel",
        CancelTransfer,
        "none",
        (("worker_id", ID), ("transfer", TRANSFER), ("reason", DETAIL)),
        _cancel_transfer,
    ),
    MessageDefinition(
        "protocol.error",
        ErrorReport,
        "optional",
        (("code", _enum(ProtocolErrorCode)), ("detail", DETAIL)),
    ),
)


def _message_check(definition: MessageDefinition) -> Callable[[Message], None]:
    def check(m: Message) -> None:
        validate_version(m.protocol_version)
        if definition.correlation == "required":
            require(
                m.correlation_id is not None,
                "correlation_id",
                "required for command response",
            )
        elif definition.correlation == "none":
            require(
                m.correlation_id is None,
                "correlation_id",
                "prohibited for command/notice",
            )
        require(
            m.correlation_id != m.message_id,
            "correlation_id",
            "message cannot correlate to itself",
        )
        if definition.check is not None:
            definition.check(m)

    return check


ENVELOPE_FIELDS = (
    ("message_id", ID),
    ("correlation_id", OPTIONAL_ID),
    ("protocol_version", VERSION),
)

SCHEMAS = schema_table(
    (
        Schema(
            ProgramIdentity,
            (
                ("source_sha256", HASH),
                ("filename", METADATA),
                ("environment_id", ID),
                ("package_id", OPTIONAL_ID),
                ("id", HASH),
            ),
            _program,
            frozenset({"id"}),
        ),
        Schema(
            AttemptIdentity,
            (("plan_id", HASH), ("run_id", ID), ("task_id", ID), ("attempt_id", ID)),
        ),
        Schema(
            FailureInfo,
            (
                ("kind", _enum(FailureKind)),
                ("message", DETAIL),
                ("exception_type", _optional(METADATA)),
                (
                    "traceback_text",
                    _optional(Spec("text", maximum=MAX_TRACEBACK_BYTES)),
                ),
            ),
            _failure,
        ),
        Schema(TaskSuccess, (("attempt", ATTEMPT), ("output_ids", IDS), ("clean_exit", BOOL),
                             ("stdout_tail", DETAIL), ("stderr_tail", DETAIL),
                             ("stdout_truncated", BOOL), ("stderr_truncated", BOOL))),
        Schema(TaskFailure, (("attempt", ATTEMPT), ("failure", FAILURE),
                             ("stdout_tail", DETAIL), ("stderr_tail", DETAIL),
                             ("stdout_truncated", BOOL), ("stderr_truncated", BOOL))),
        Schema(
            WorkerState,
            (
                ("worker_id", ID),
                ("total_slots", CAPACITY),
                ("running_slots", CAPACITY),
                ("reserved_slots", CAPACITY),
                ("online", BOOL),
                ("accepting_work", BOOL),
                ("cpu_percent", Spec("real", maximum=100)),
                ("total_memory_bytes", COUNT),
                ("available_memory_bytes", COUNT),
                ("cpu_cores", Spec("int", minimum=1, maximum=MAX_CAPACITY)),
                ("environment_ids", Spec("set", item=ID)),
                ("prepared_program_ids", Spec("set", item=HASH)),
                ("supported_modes", Spec("set", item=_enum(ExecutionMode))),
            ),
            _worker,
        ),
        Schema(
            WorkerContext,
            (
                ("context_id", ID),
                ("worker_id", ID),
                ("prepared_task_ids", Spec("set", item=ID)),
                ("available_slots", CAPACITY),
            ),
        ),
        Schema(Replica, (("worker_id", ID), ("status", _enum(ReplicaStatus)))),
        Schema(
            DataLocation,
            (
                ("value_id", ID),
                ("form", _enum(DataForm)),
                ("replicas", REPLICAS),
                ("size_bytes", OPTIONAL_COUNT),
                ("object_state_id", OPTIONAL_ID),
            ),
            _location,
        ),
        Schema(
            RunTaskView,
            (("task_id", ID), ("status", STATUS), ("attempt_id", OPTIONAL_ID),
             ("worker_id", OPTIONAL_ID), ("failure_kind", _optional(STATUS)),
             ("detail", DETAIL), ("output_ids", IDS),
             ("exception_type", _optional(METADATA)),
             ("stdout_tail", DETAIL), ("stderr_tail", DETAIL),
             ("stdout_truncated", BOOL), ("stderr_truncated", BOOL)),
        ),
        Schema(
            ClientContextView,
            (("context_id", ID), ("worker_id", ID), ("available_slots", CAPACITY),
             ("prepared_task_count", COUNT)),
        ),
        Schema(
            ClientWorkerView,
            (("worker_id", ID), ("generation", COUNT), ("session_id", ID),
             ("online", BOOL), ("accepting_work", BOOL), ("total_slots", CAPACITY),
             ("running_slots", CAPACITY), ("reserved_slots", CAPACITY),
             ("host", ID), ("port", Spec("int", minimum=1, maximum=65535))),
        ),
        Schema(
            WorkerEndpoint,
            (
                ("worker_id", ID),
                ("host", ID),
                ("port", Spec("int", minimum=1, maximum=65535)),
            ),
        ),
        Schema(
            DataReference,
            (
                ("plan_id", HASH),
                ("run_id", ID),
                ("value_id", ID),
                ("form", _enum(DataForm)),
                ("object_state_id", OPTIONAL_ID),
            ),
            _data,
        ),
        Schema(
            TransferIdentity,
            (
                ("data", DATA),
                ("transfer_id", ID),
                ("transfer_attempt_id", ID),
                ("source_worker_id", ID),
                ("destination_worker_id", ID),
            ),
            _transfer,
        ),
        *(
            Schema(d.cls, ENVELOPE_FIELDS + d.fields, _message_check(d))
            for d in DEFINITIONS
        ),
    )
)
