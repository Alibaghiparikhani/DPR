"""Typed in-memory coordinator state. No network I/O or user-code execution."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Callable

from execution import AttemptIdentity, FailureInfo, FailureKind, ProgramIdentity
from protocol import DataReference, Message, TransferIdentity, WorkerEndpoint
from scheduler import WorkerState


class RunStatus(str, Enum):
    RUNNING = "running"
    CANCELLING = "cancelling"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"

    @property
    def terminal(self) -> bool:
        return self in {self.SUCCEEDED, self.FAILED, self.CANCELLED}


class TaskStatus(str, Enum):
    BLOCKED = "blocked"
    READY = "ready"
    WAITING_TRANSFER = "waiting_transfer"
    DISPATCHED = "dispatched"
    ACCEPTED = "accepted"
    RUNNING = "running"
    CANCELLING = "cancelling"
    COMMITTED = "committed"
    FAILED = "failed"
    CANCELLED = "cancelled"

    @property
    def active(self) -> bool:
        return self in {self.WAITING_TRANSFER, self.DISPATCHED, self.ACCEPTED, self.RUNNING, self.CANCELLING}


class AttemptStatus(str, Enum):
    WAITING_TRANSFER = "waiting_transfer"
    DISPATCHED = "dispatched"
    ACCEPTED = "accepted"
    RUNNING = "running"
    CANCEL_REQUESTED = "cancel_requested"
    # Logically obsolete but still potentially executing on the worker.  ORPHANED
    # never has commit authority, but it retains physical capacity until the
    # worker proves termination or the session is lost.
    ORPHANED = "orphaned"
    COMMITTED = "committed"
    FAILED = "failed"
    REJECTED = "rejected"
    CANCELLED = "cancelled"
    LOST = "lost"
    STALE = "stale"

    @property
    def active(self) -> bool:
        return self in {self.WAITING_TRANSFER, self.DISPATCHED, self.ACCEPTED, self.RUNNING, self.CANCEL_REQUESTED}

    @property
    def occupies_capacity(self) -> bool:
        return self.active or self is self.ORPHANED


class TransferStatus(str, Enum):
    DESTINATION_PREPARING = "destination_preparing"
    DESTINATION_READY = "destination_ready"
    SOURCE_REQUESTED = "source_requested"
    SOURCE_ACCEPTED = "source_accepted"
    TRANSFERRING = "transferring"
    COMPLETED = "completed"
    FAILED = "failed"

    @property
    def terminal(self) -> bool:
        return self in {self.COMPLETED, self.FAILED}


class EventDisposition(str, Enum):
    APPLIED = "applied"
    DUPLICATE = "duplicate"
    STALE = "stale"


class CoordinatorFailureCode(str, Enum):
    """Terminal coordinator-level causes not expressible as worker TaskFailure kinds."""

    CONTEXT_LOST = "context_lost"
    NATIVE_STATE_UNCERTAIN = "native_state_uncertain"
    DATA_LOST = "data_lost"
    PLACEMENT_CONSTRAINT_LOST = "placement_constraint_lost"


@dataclass(frozen=True, slots=True)
class CoordinatorFailure:
    code: CoordinatorFailureCode
    detail: str

    def __post_init__(self) -> None:
        if not isinstance(self.code, CoordinatorFailureCode):
            raise ValueError("invalid coordinator failure code")
        if not isinstance(self.detail, str) or not self.detail.strip():
            raise ValueError("coordinator failure detail must be nonempty text")


@dataclass(frozen=True, slots=True)
class SessionHandle:
    worker_id: str
    generation: int
    session_id: str


@dataclass(slots=True)
class WorkerRecord:
    handle: SessionHandle
    endpoint: WorkerEndpoint
    reported: WorkerState
    last_seen: float
    last_sequence: int = -1
    outbox: list[Message] = field(default_factory=list)
    active: bool = True
    # Coordinator-imposed quarantine survives heartbeats for this session. A
    # fresh session generation is required before the worker may compute again.
    compute_quarantined: bool = False


@dataclass(slots=True)
class AttemptRecord:
    identity: AttemptIdentity
    worker_id: str
    worker_generation: int
    dispatch_message_id: str | None
    status: AttemptStatus = AttemptStatus.DISPATCHED
    context_id: str | None = None
    pending_transfers: set[tuple[str, str]] = field(default_factory=set)
    cancel_message_id: str | None = None
    failure: FailureInfo | None = None
    stdout_tail: str = ""
    stderr_tail: str = ""
    stdout_truncated: bool = False
    stderr_truncated: bool = False
    phase_since: float = 0.0


@dataclass(slots=True)
class TaskRecord:
    task_id: str
    status: TaskStatus
    ready_sequence: int | None = None
    wait_rounds: int = 0
    current_attempt_id: str | None = None
    attempt_ids: list[str] = field(default_factory=list)
    committed_attempt_id: str | None = None
    failure: FailureInfo | None = None


@dataclass(slots=True)
class LocationRecord:
    data: DataReference
    # worker_id -> exact session generation that certified possession. Scheduler
    # snapshots still expose only worker IDs; generation is coordinator authority.
    replicas: dict[str, int] = field(default_factory=dict)
    size_bytes: int | None = None


@dataclass(slots=True)
class TransferRecord:
    identity: TransferIdentity
    status: TransferStatus
    destination_request_id: str
    source_generation: int
    destination_generation: int
    source_session_id: str = ""
    destination_session_id: str = ""
    authorization: str = ""
    source_request_id: str | None = None
    size_bytes: int | None = None
    failure_detail: str | None = None
    consumer_run_id: str | None = None
    consumer_attempt_id: str | None = None
    phase_since: float = 0.0
    # Cleanup evidence is tracked independently for the two participants. A
    # logical transfer failure does not prove either peer released its physical
    # transfer state. Destination success is end-to-end terminal evidence; a
    # participant failure/loss certifies only that participant.
    source_cleanup_confirmed: bool = False
    destination_cleanup_confirmed: bool = False
    # Explicit physical cleanup commands are independently tracked so repeated
    # logical failure paths do not flood a participant with duplicate cancels.
    source_cancel_requested: bool = False
    destination_cancel_requested: bool = False

    @property
    def cleanup_confirmed(self) -> bool:
        """Whether all participants that may hold transfer state are resolved."""
        if not self.destination_cleanup_confirmed:
            return False
        # Before a source request exists, only the prepared destination can own
        # transfer-side resources. Once a request is issued, both participants
        # require terminal evidence unless destination completion proves the
        # end-to-end operation completed (that path marks both sides).
        return self.source_request_id is None or self.source_cleanup_confirmed


@dataclass(frozen=True, slots=True)
class ProgramPreparationContract:
    worker_id: str
    worker_generation: int
    plan_id: str
    program: ProgramIdentity


@dataclass(frozen=True, slots=True)
class ContextPreparationContract:
    worker_id: str
    worker_generation: int
    plan_id: str
    run_id: str
    program_id: str
    context_id: str
    task_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class PendingProgramPreparation:
    message_id: str
    worker_id: str
    worker_generation: int
    plan_id: str
    program: ProgramIdentity
    created_at: float = 0.0

    @property
    def contract(self) -> ProgramPreparationContract:
        return ProgramPreparationContract(
            self.worker_id, self.worker_generation, self.plan_id, self.program,
        )


@dataclass(frozen=True, slots=True)
class PendingContextPreparation:
    message_id: str
    worker_id: str
    worker_generation: int
    plan_id: str
    run_id: str
    program_id: str
    context_id: str
    task_ids: tuple[str, ...]
    created_at: float = 0.0

    @property
    def contract(self) -> ContextPreparationContract:
        return ContextPreparationContract(
            self.worker_id, self.worker_generation, self.plan_id, self.run_id,
            self.program_id, self.context_id, self.task_ids,
        )


PendingRequest = PendingProgramPreparation | PendingContextPreparation


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts_per_task: int = 2
    retry_worker_loss: bool = True
    retry_failure_kinds: frozenset[FailureKind] = frozenset({
        FailureKind.INPUT_UNAVAILABLE,
        FailureKind.ENVIRONMENT_MISMATCH,
        FailureKind.EXECUTION_ERROR,
    })

    def __post_init__(self) -> None:
        if type(self.max_attempts_per_task) is not int or self.max_attempts_per_task < 1:
            raise ValueError("max_attempts_per_task must be an integer >= 1")
        if not all(isinstance(v, FailureKind) for v in self.retry_failure_kinds):
            raise ValueError("retry_failure_kinds must contain FailureKind values")
        if FailureKind.PYTHON_EXCEPTION in self.retry_failure_kinds:
            raise ValueError("PYTHON_EXCEPTION is a user failure and cannot be automatically retried")




@dataclass(frozen=True, slots=True)
class OperationTimeouts:
    """Coordinator-side liveness bounds for control operations.

    Running user computation is unbounded by default.  A caller may opt into an
    execution timeout; expiration conservatively retires the worker session rather
    than pretending the old process stopped.
    """

    preparation: float = 60.0
    dispatch_ack: float = 30.0
    start: float = 30.0
    transfer: float = 60.0
    cancellation: float = 30.0
    execution: float | None = None
    # A transfer step may take `transfer` seconds plus its size at this rate, so a
    # large value on a slow network is not failed while it is still moving.
    # (Workers catch a transfer that stops moving within seconds regardless.)
    transfer_bytes_per_second: int = 256 * 1024

    def __post_init__(self) -> None:
        for name in ("preparation", "dispatch_ack", "start", "transfer", "cancellation"):
            value = getattr(self, name)
            if type(value) not in (int, float) or value <= 0:
                raise ValueError(f"{name} timeout must be positive")
        if type(self.transfer_bytes_per_second) is not int or self.transfer_bytes_per_second < 1:
            raise ValueError("transfer_bytes_per_second must be a positive integer")

    def transfer_deadline(self, size_bytes: int | None) -> float:
        return self.transfer + (size_bytes or 0) / self.transfer_bytes_per_second
        if self.execution is not None and (type(self.execution) not in (int, float) or self.execution <= 0):
            raise ValueError("execution timeout must be positive or None")


@dataclass(frozen=True, slots=True)
class OperationLimits:
    """Bound in-memory operational state without evicting live work.

    When a limit is reached the coordinator fails closed with explicit
    backpressure. Callers may prune terminal history or wait for operations to
    complete before admitting more work.
    """

    max_active_pending_global: int = 4096
    max_active_pending_per_worker: int = 512
    max_active_pending_per_run: int = 1024
    max_transfer_records_global: int = 16384
    max_transfer_records_per_run: int = 4096
    max_retained_contexts_global: int = 16384
    max_retained_contexts_per_worker: int = 4096
    max_retained_contexts_per_run: int = 4096
    max_runs_in_memory: int = 2048
    max_known_worker_identities: int = 16384
    # Transfers each worker takes part in at once, per direction.  Further ones
    # wait on the coordinator, where they hold no worker resources and no
    # deadline runs, instead of piling up on a worker until its limits or the
    # transfer deadline fail them.  Equal to the workers' default stream limits,
    # so a transfer that has been sent never waits on a worker either.
    max_transfers_per_worker: int = 4

    def __post_init__(self) -> None:
        for name in (
            "max_active_pending_global",
            "max_active_pending_per_worker",
            "max_active_pending_per_run",
            "max_transfer_records_global",
            "max_transfer_records_per_run",
            "max_retained_contexts_global",
            "max_retained_contexts_per_worker",
            "max_retained_contexts_per_run",
            "max_runs_in_memory",
            "max_known_worker_identities",
            "max_transfers_per_worker",
        ):
            value = getattr(self, name)
            if type(value) is not int or value < 1:
                raise ValueError(f"{name} must be a positive integer")


class SequentialIdSource:
    """Deterministic caller-owned ID source; no randomness or wall clock."""
    def __init__(self) -> None:
        self._counters: dict[str, int] = {}

    def __call__(self, kind: str) -> str:
        value = self._counters.get(kind, 0) + 1
        self._counters[kind] = value
        return f"{kind}-{value}"


IdSource = Callable[[str], str]
Clock = Callable[[], float]


@dataclass(frozen=True, slots=True)
class RunSnapshot:
    run_id: str
    status: RunStatus
    tasks: tuple[tuple[str, TaskStatus], ...]
    current_attempts: tuple[AttemptIdentity, ...]
    completed_task_ids: frozenset[str]
    failure: CoordinatorFailure | None = None


@dataclass(frozen=True, slots=True)
class ScheduleResult:
    dispatched: tuple[AttemptIdentity, ...]
    unplaced_task_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class WorkerView:
    handle: SessionHandle
    endpoint: WorkerEndpoint
    state: WorkerState
    last_seen: float
