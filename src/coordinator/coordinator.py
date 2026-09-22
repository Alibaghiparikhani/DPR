"""Deterministic synchronous coordinator control plane.

Networking later supplies typed protocol messages and drains typed outbound
commands. This module opens no sockets and executes no user code.
"""
from __future__ import annotations

from collections import deque
from copy import deepcopy
from dataclasses import dataclass, replace
from typing import Iterable

from execution import (
    AttemptIdentity, ExecutionMode, ExecutionPlan, FailureInfo, FailureKind,
    ObjectAccess, TaskSuccess, ValueKind,
)
import protocol as p
from scheduler import (
    ClusterSnapshot, CommitmentPhase, DataForm, DataLocation, Placement, ReadyTask, Replica, ReplicaStatus,
    SchedulingDecision, TaskAffinity, TaskCommitment,
    WorkerContext, WorkerState,
)

from .errors import (
    CapacityConflict, ContextConflict, CoordinatorError, InvalidDataLocation, InvalidRunTransition,
    InvalidTaskTransition, InvalidTransferTransition, InvalidWorkerMessage,
    OperationalLimitExceeded, OutboundBackpressure, PlacementRejected, StaleWorkerSession, UnknownAttempt,
    UnknownRun, UnknownTask, UnknownTransfer, UnknownWorker,
)
from .locations import ObjectLocationIndex
from .history import RunHistoryStore
from .model import (
    AttemptRecord, AttemptStatus, Clock, CoordinatorFailure, CoordinatorFailureCode,
    ContextPreparationContract, EventDisposition, IdSource, OperationLimits, OperationTimeouts, PendingContextPreparation,
    PendingProgramPreparation, ProgramPreparationContract, RetryPolicy,
    RunSnapshot, RunStatus, ScheduleResult, SequentialIdSource, SessionHandle,
    TaskRecord, TaskStatus, TransferRecord, TransferStatus, WorkerRecord, WorkerView,
)
from .pending import PendingOperationRegistry, PendingRegistryView
from .runs import CoordinatorRun


def _zero_clock() -> float:
    return 0.0


@dataclass(frozen=True, slots=True)
class _StagedPlacement:
    """Validated placement effects prepared before authoritative mutation."""

    placement: Placement
    attempt: AttemptRecord
    dispatch: p.TaskDispatch | None
    transfers: tuple[tuple[TransferRecord, p.PrepareReceive], ...]


@dataclass(frozen=True, slots=True)
class _StagedMembershipBroadcast:
    """Provider-resolved membership refresh ready for a mutation-free apply phase."""

    revision: int
    messages: tuple[tuple[WorkerRecord, p.MembershipUpdate], ...]


@dataclass(frozen=True, slots=True)
class _StagedRunFailure:
    """All fallible cleanup material needed to commit one terminal run failure."""

    orphaned_at: float | None
    cancellations: tuple[tuple[str, WorkerRecord, p.CancelTask], ...]
    transfer_cancellations: tuple[tuple[str, WorkerRecord, p.CancelTransfer], ...]
    context_releases: tuple[tuple[str, WorkerRecord, p.ReleaseContext], ...]


class Coordinator:
    """Authoritative in-memory control-plane state machine."""

    MAX_PENDING_HISTORY = 1024
    MAX_CLUSTER_MEMBERS = p.MAX_COLLECTION_ITEMS
    MAX_PRUNED_RUN_TOMBSTONES = 4096

    def __init__(self, *, retry_policy: RetryPolicy | None = None,
                 id_source: IdSource | None = None, clock: Clock | None = None,
                 heartbeat_timeout: float = 30.0,
                 operation_timeouts: OperationTimeouts | None = None,
                 operation_limits: OperationLimits | None = None,
                 outbox_limit: int = 4096,
                 history_store: RunHistoryStore | None = None) -> None:
        if heartbeat_timeout <= 0:
            raise ValueError("heartbeat_timeout must be positive")
        if type(outbox_limit) is not int or outbox_limit < 1:
            raise ValueError("outbox_limit must be a positive integer")
        self.retry_policy = retry_policy or RetryPolicy()
        self.operation_timeouts = operation_timeouts or OperationTimeouts()
        self.operation_limits = operation_limits or OperationLimits()
        self.outbox_limit = outbox_limit
        self._history_store = history_store
        self._id = id_source or SequentialIdSource()
        self._clock = clock or _zero_clock
        self.heartbeat_timeout = float(heartbeat_timeout)
        self._workers: dict[str, WorkerRecord] = {}
        self._generations: dict[str, int] = {}
        self._runs: dict[str, CoordinatorRun] = {}
        self._locations = ObjectLocationIndex()
        self._transfers: dict[tuple[str, str], TransferRecord] = {}
        self._active_transfer_attempt: dict[str, str] = {}
        # Admission control for the data plane (see OperationLimits.
        # max_transfers_per_worker).  A queued transfer exists only here: its
        # PrepareReceive has not been sent, so no worker holds anything for it.
        self._queued_transfers: dict[tuple[str, str], p.PrepareReceive] = {}
        self._issued_transfers: set[tuple[str, str]] = set()
        self._pending_ops = PendingOperationRegistry(
            history_limit=lambda: self.MAX_PENDING_HISTORY,
            new_base_message_id=lambda: self._new("message"),
            active_global_limit=lambda: self.operation_limits.max_active_pending_global,
            active_per_worker_limit=lambda: self.operation_limits.max_active_pending_per_worker,
            active_per_run_limit=lambda: self.operation_limits.max_active_pending_per_run,
        )
        # Terminal history is diagnostic infrastructure, never authoritative live
        # state. Failures are remembered so they can be retried later without
        # interrupting worker-loss/cancellation reconciliation.
        self._history_archive_errors: dict[str, str] = {}
        # F8: recently released run ids remain recognizable so delayed, otherwise
        # well-formed worker observations degrade to STALE instead of UnknownRun.
        self._pruned_run_tombstones: deque[str] = deque()
        self._pruned_run_tombstone_set: set[str] = set()
        # F5/F6: release is an acknowledged physical lifecycle, not a metadata drop.
        # Keyed by exact worker generation + representation so retries cannot cross sessions.
        self._pending_object_releases: dict[tuple[str, int, p.DataReference], str] = {}
        self._revision = 0
        self._membership_revision = 0

    # ---------- identity / inspection ----------
    def _new(self, kind: str) -> str:
        value = self._id(kind)
        if not isinstance(value, str) or not value.strip():
            raise CoordinatorError(f"ID source produced invalid {kind} identity")
        try:
            encoded = value.encode("utf-8", errors="strict")
        except UnicodeEncodeError:
            raise CoordinatorError(f"ID source produced invalid {kind} identity") from None
        if (len(encoded) > p.MAX_IDENTIFIER_BYTES
                or any(ord(char) < 32 or 127 <= ord(char) <= 159 for char in value)):
            raise CoordinatorError(
                f"ID source produced {kind} identity outside protocol identifier bounds"
            )
        return value

    def _touch(self) -> None:
        self._revision += 1

    def inspect_run(self, run_id: str) -> RunSnapshot:
        run = self._run(run_id)
        attempts = tuple(
            run.attempts[t.current_attempt_id].identity
            for t in run.tasks.values() if t.current_attempt_id is not None
        )
        return RunSnapshot(
            run_id, run.status,
            tuple((task_id, run.tasks[task_id].status) for task_id in sorted(run.tasks)),
            attempts, run.readiness.completed, run.failure,
        )

    def inspect_run_summary(self, run_id: str):
        """Return O(1) operator summary fields without materializing task/attempt views."""
        run = self._run(run_id)
        return run.status, len(run.tasks), run.failure

    def run_ids(self) -> tuple[str, ...]:
        """Return current in-memory run identities for operator inspection."""
        return tuple(sorted(self._runs))

    def run_plan_id(self, run_id: str) -> str:
        return self._run(run_id).plan.id

    def worker_ids(self, *, active_only: bool = False) -> tuple[str, ...]:
        """Return known worker identities without exposing mutable records."""
        return tuple(sorted(
            worker_id for worker_id, worker in self._workers.items()
            if not active_only or worker.active
        ))

    def inspect_contexts(self, run_id: str) -> tuple[WorkerContext, ...]:
        run = self._run(run_id)
        return tuple(run.contexts[key] for key in sorted(run.contexts))

    def inspect_worker(self, worker_id: str) -> WorkerView:
        worker = self._worker(worker_id)
        return WorkerView(worker.handle, worker.endpoint, self._effective_worker_state(worker_id), worker.last_seen)

    def get_task(self, run_id: str, task_id: str) -> TaskRecord:
        run = self._run(run_id)
        try:
            return deepcopy(run.tasks[task_id])
        except KeyError as error:
            raise UnknownTask(task_id) from error

    def get_task_manifest(self, run_id: str, task_id: str):
        """Return the immutable execution manifest for operator diagnostics."""
        run = self._run(run_id)
        try:
            return run.plan.task_index[task_id]
        except KeyError as error:
            raise UnknownTask(task_id) from error

    def get_attempt(self, run_id: str, attempt_id: str) -> AttemptRecord:
        run = self._run(run_id)
        try:
            return deepcopy(run.attempts[attempt_id])
        except KeyError as error:
            raise UnknownAttempt(attempt_id) from error

    def data_locations(self, run_id: str) -> tuple[DataLocation, ...]:
        run = self._run(run_id)
        return self._locations.locations(run.plan.id, run_id)

    def get_transfer(self, transfer_id: str, transfer_attempt_id: str) -> TransferRecord:
        try:
            return deepcopy(self._transfers[(transfer_id, transfer_attempt_id)])
        except KeyError as error:
            raise UnknownTransfer(f"{transfer_id}/{transfer_attempt_id}") from error

    def inspect_pending_operations(self) -> PendingRegistryView:
        """Return immutable correlation-bookkeeping diagnostics."""
        return self._pending_ops.view()

    def _ensure_outbox_capacity(self, worker: WorkerRecord, additional: int = 1) -> None:
        if additional < 0:
            raise ValueError("additional outbox capacity must be non-negative")
        if len(worker.outbox) + additional > self.outbox_limit:
            raise OutboundBackpressure(
                f"worker {worker.handle.worker_id} outbound control queue is full"
            )

    def _enqueue_message(self, worker: WorkerRecord, message: p.Message) -> bool:
        """Enqueue one typed message while keeping operational memory bounded.

        Heartbeat acknowledgements and full membership views are replaceable
        refresh messages, so only the newest undrained message of each type is
        retained. Correctness-critical task/transfer/preparation commands are
        never silently discarded.
        """
        if isinstance(message, (p.HeartbeatAck, p.MembershipUpdate)):
            for index in range(len(worker.outbox) - 1, -1, -1):
                if isinstance(worker.outbox[index], type(message)):
                    worker.outbox[index] = message
                    return True
            if len(worker.outbox) >= self.outbox_limit:
                return False
            worker.outbox.append(message)
            return True
        self._ensure_outbox_capacity(worker)
        worker.outbox.append(message)
        return True

    # ---------- workers / sessions ----------
    def _stage_membership_broadcast(
            self, *, recipients: Iterable[WorkerRecord],
            members: tuple[p.WorkerEndpoint, ...], revision: int
            ) -> _StagedMembershipBroadcast:
        messages: list[tuple[WorkerRecord, p.MembershipUpdate]] = []
        for record in sorted(recipients, key=lambda r: r.handle.worker_id):
            messages.append((record, p.MembershipUpdate(
                worker_id=record.handle.worker_id,
                session_id=record.handle.session_id,
                revision=revision,
                members=members,
                message_id=self._new("message"),
            )))
        return _StagedMembershipBroadcast(revision, tuple(messages))

    def _apply_membership_broadcast(self, staged: _StagedMembershipBroadcast) -> None:
        self._membership_revision = staged.revision
        for record, message in staged.messages:
            self._enqueue_message(record, message)

    def register_worker(self, hello: p.WorkerHello, *, now: float | None = None) -> SessionHandle:
        if not isinstance(hello, p.WorkerHello):
            raise InvalidWorkerMessage("WorkerHello required")
        if hello.worker.worker_id != hello.endpoint.worker_id:
            raise InvalidWorkerMessage("hello worker and endpoint identities differ")
        selected = p.negotiate_version(hello.supported_versions)
        worker_id = hello.worker.worker_id
        existing = self._workers.get(worker_id)
        if (worker_id not in self._generations
                and len(self._generations) >= self.operation_limits.max_known_worker_identities):
            raise OperationalLimitExceeded("known worker-identity limit reached")
        active_count = sum(1 for record in self._workers.values() if record.active)
        adding_member = existing is None or not existing.active
        if adding_member and active_count >= self.MAX_CLUSTER_MEMBERS:
            raise CapacityConflict(
                f"cluster membership limit is {self.MAX_CLUSTER_MEMBERS} workers"
            )

        # Resolve every fallible provider/protocol construction before retiring an
        # old authoritative generation or publishing the new one. Failed admission
        # must leave the old cluster state fully usable.
        generation = self._generations.get(worker_id, 0) + 1
        session_id = self._new("session")
        seen_at = self._now(now)
        handle = SessionHandle(worker_id, generation, session_id)
        record = WorkerRecord(handle, hello.endpoint, hello.worker, seen_at)

        post_records: list[WorkerRecord] = []
        replaced = False
        for wid, current in self._workers.items():
            if wid == worker_id:
                post_records.append(record)
                replaced = True
            elif current.active:
                post_records.append(current)
        if not replaced:
            post_records.append(record)
        post_members = tuple(r.endpoint for r in post_records if r.active)
        accepted = p.WorkerAccepted(
            worker_id=worker_id,
            session_id=session_id,
            selected_version=selected,
            members=post_members,
            message_id=self._new("message"),
            correlation_id=hello.message_id,
        )

        recipients = tuple(
            r for r in self._workers.values()
            if r.active and r.handle.worker_id != worker_id
        )
        base_revision = self._membership_revision
        loss_broadcast = None
        if existing is not None and existing.active:
            loss_members = tuple(
                r.endpoint for r in self._workers.values()
                if r.active and r.handle.worker_id != worker_id
            )
            loss_broadcast = self._stage_membership_broadcast(
                recipients=recipients, members=loss_members, revision=base_revision + 1,
            )
            base_revision += 1
        admission_broadcast = self._stage_membership_broadcast(
            recipients=recipients, members=post_members, revision=base_revision + 1,
        )

        if existing is not None and existing.active:
            self._lose_worker(
                existing, "worker reconnected",
                staged_membership=loss_broadcast, broadcast=True,
            )
        self._generations[worker_id] = generation
        self._workers[worker_id] = record
        self._enqueue_message(record, accepted)
        self._apply_membership_broadcast(admission_broadcast)
        self._touch()
        return handle

    def drain_outbox(self, session: SessionHandle) -> tuple[p.Message, ...]:
        record = self._session(session)
        result = tuple(record.outbox)
        record.outbox.clear()
        return result

    def disconnect_session(self, session: SessionHandle, *, reason: str = "control connection lost") -> bool:
        """Retire exactly ``session`` after transport loss.

        A replaced/stale connection is intentionally a no-op: its eventual socket
        teardown must never retire the newer generation that superseded it.
        """
        if not isinstance(session, SessionHandle):
            raise StaleWorkerSession("SessionHandle required")
        record = self._workers.get(session.worker_id)
        if record is None or not record.active or record.handle != session:
            return False
        self._lose_worker(record, reason)
        self._release_queued_transfers()
        return True

    def _broadcast_membership(self, *, exclude_worker_id: str | None = None) -> None:
        members = tuple(w.endpoint for w in sorted(
            (r for r in self._workers.values() if r.active),
            key=lambda r: r.handle.worker_id,
        ))
        recipients = tuple(
            record for record in self._workers.values()
            if record.active and record.handle.worker_id != exclude_worker_id
        )
        staged = self._stage_membership_broadcast(
            recipients=recipients, members=members, revision=self._membership_revision + 1,
        )
        self._apply_membership_broadcast(staged)

    def expire_workers(self, *, now: float | None = None) -> tuple[str, ...]:
        current = self._now(now)
        expired = []
        for record in list(self._workers.values()):
            if record.active and current - record.last_seen > self.heartbeat_timeout:
                expired.append(record.handle.worker_id)
                self._lose_worker(record, "heartbeat timeout")
        if expired:
            self._release_queued_transfers(current)
        return tuple(sorted(expired))

    def expire_operations(self, *, now: float | None = None) -> tuple[str, ...]:
        """Expire bounded control operations using injected time; never sleep.

        Preparation and transfer control operations can be retired directly.
        Dispatch/start/cancellation ambiguity can mean worker code still exists,
        so their expiry quarantines that worker's compute state instead of
        releasing physical capacity optimistically. Running task execution has no
        default deadline; callers may opt into one through OperationTimeouts.
        """
        current = self._now(now)
        expired: list[str] = []

        for request in tuple(self._pending_ops.active_requests()):
            if current - request.created_at <= self.operation_timeouts.preparation:
                continue
            self._pending_ops.complete(request.message_id)
            expired.append(f"preparation:{request.message_id}")

        workers_to_quarantine: dict[str, str] = {}
        for run in self._runs.values():
            for attempt in run.attempts.values():
                if not attempt.status.occupies_capacity:
                    continue
                age = current - attempt.phase_since
                timeout: float | None = None
                label = ""
                if attempt.status == AttemptStatus.DISPATCHED:
                    timeout, label = self.operation_timeouts.dispatch_ack, "dispatch"
                elif attempt.status == AttemptStatus.ACCEPTED:
                    timeout, label = self.operation_timeouts.start, "start"
                elif attempt.status == AttemptStatus.RUNNING:
                    timeout, label = self.operation_timeouts.execution, "execution"
                elif attempt.status in {AttemptStatus.CANCEL_REQUESTED, AttemptStatus.ORPHANED}:
                    timeout, label = self.operation_timeouts.cancellation, "cancellation"
                if timeout is not None and age > timeout:
                    workers_to_quarantine.setdefault(
                        attempt.worker_id,
                        f"{label} deadline expired for {attempt.identity.attempt_id}",
                    )
                    expired.append(f"{label}:{attempt.identity.attempt_id}")

        for worker_id, reason in workers_to_quarantine.items():
            worker = self._workers.get(worker_id)
            if worker is not None and worker.active and worker.reported.online:
                self._mark_worker_compute_unavailable(worker, reason)

        expired_transfer_records = [
            record for key, record in self._transfers.items()
            if (not record.status.terminal and key not in self._queued_transfers
                and current - record.phase_since
                > self.operation_timeouts.transfer_deadline(record.size_bytes))
        ]
        staged_transfer_failures: dict[tuple[str | None, str | None], _StagedRunFailure | None] = {}
        for record in expired_transfer_records:
            consumer_key = (record.consumer_run_id, record.consumer_attempt_id)
            if consumer_key not in staged_transfer_failures:
                staged_transfer_failures[consumer_key] = self._stage_consumer_transfer_failure(
                    record, "transfer control deadline expired"
                )

        # A logical transfer timeout is also an explicit physical cleanup event.
        # Stage every cancellation before mutating any transfer so ID generation,
        # protocol validation, or aggregate outbox pressure cannot leave a half-
        # expired batch. Consumer-associated transfers are grouped because failure
        # of one pending input abandons all sibling inputs for that same attempt.
        staged_cleanup_groups: dict[
            tuple[str, ...],
            tuple[tuple[tuple[str, str], str, WorkerRecord, p.CancelTransfer], ...],
        ] = {}
        for record in expired_transfer_records:
            consumer_key = (record.consumer_run_id, record.consumer_attempt_id)
            failure = staged_transfer_failures[consumer_key]
            if record.consumer_run_id is not None and record.consumer_attempt_id is not None:
                cleanup_key = ("consumer", record.consumer_run_id, record.consumer_attempt_id)
            else:
                cleanup_key = (
                    "transfer", record.identity.transfer_id,
                    record.identity.transfer_attempt_id,
                )
            if cleanup_key not in staged_cleanup_groups:
                staged_cleanup_groups[cleanup_key] = self._stage_consumer_transfer_cleanup(
                    record, "transfer control deadline expired",
                    staged_failure=failure, preflight=False,
                )

        combined_counts: dict[str, int] = {}
        seen_run_failures: set[int] = set()
        for failure in staged_transfer_failures.values():
            if failure is None or id(failure) in seen_run_failures:
                continue
            seen_run_failures.add(id(failure))
            for _, worker, _ in failure.cancellations:
                worker_id = worker.handle.worker_id
                combined_counts[worker_id] = combined_counts.get(worker_id, 0) + 1
            for _, worker, _ in failure.transfer_cancellations:
                worker_id = worker.handle.worker_id
                combined_counts[worker_id] = combined_counts.get(worker_id, 0) + 1
        for cleanup in staged_cleanup_groups.values():
            for _, _, worker, _ in cleanup:
                worker_id = worker.handle.worker_id
                combined_counts[worker_id] = combined_counts.get(worker_id, 0) + 1
        for worker_id, count in combined_counts.items():
            worker = self._workers.get(worker_id)
            if worker is not None:
                self._ensure_outbox_capacity(worker, count)

        applied_cleanup_groups: set[tuple[str, ...]] = set()
        for record in expired_transfer_records:
            detail = "transfer control deadline expired"
            record.status = TransferStatus.FAILED
            record.failure_detail = detail
            if record.consumer_run_id is not None and record.consumer_attempt_id is not None:
                cleanup_key = ("consumer", record.consumer_run_id, record.consumer_attempt_id)
            else:
                cleanup_key = (
                    "transfer", record.identity.transfer_id,
                    record.identity.transfer_attempt_id,
                )
            if cleanup_key not in applied_cleanup_groups:
                self._apply_attempt_transfer_cancellations(
                    staged_cleanup_groups.get(cleanup_key, ())
                )
                applied_cleanup_groups.add(cleanup_key)
            self._consumer_transfer_failed(
                record, detail,
                staged_failure=staged_transfer_failures[(record.consumer_run_id, record.consumer_attempt_id)],
            )
            expired.append(
                f"transfer:{record.identity.transfer_id}/{record.identity.transfer_attempt_id}"
            )

        if expired:
            self._touch()
        self._release_queued_transfers(current)
        return tuple(expired)

    def _now(self, value: float | None) -> float:
        current = self._clock() if value is None else value
        if type(current) not in (int, float):
            raise ValueError("clock value must be numeric")
        return float(current)

    def _worker(self, worker_id: str) -> WorkerRecord:
        record = self._workers.get(worker_id)
        if record is None or not record.active:
            raise UnknownWorker(worker_id)
        return record

    def _session(self, session: SessionHandle) -> WorkerRecord:
        if not isinstance(session, SessionHandle):
            raise StaleWorkerSession("SessionHandle required")
        record = self._workers.get(session.worker_id)
        if record is None or not record.active:
            raise StaleWorkerSession(f"inactive worker session: {session.worker_id}")
        if record.handle != session:
            raise StaleWorkerSession(f"stale session for {session.worker_id}")
        return record

    def _known_worker_load(self, worker_id: str) -> tuple[int, int]:
        running = reserved = 0
        for run in self._runs.values():
            for attempt in run.attempts.values():
                if attempt.worker_id != worker_id or not attempt.status.occupies_capacity:
                    continue
                if attempt.status == AttemptStatus.RUNNING:
                    running += 1
                else:
                    reserved += 1
        return running, reserved

    def _effective_worker_state(self, worker_id: str) -> WorkerState:
        record = self._worker(worker_id)
        raw = record.reported
        known_running, known_reserved = self._known_worker_load(worker_id)
        if known_running + known_reserved > raw.total_slots:
            raise CapacityConflict(f"coordinator reservations exceed {worker_id} capacity")
        # Preserve the most conservative free-slot view without double-counting a
        # heartbeat that already includes coordinator-known commitments.
        free = min(raw.free_slots, raw.total_slots - known_running - known_reserved)
        running = known_running
        reserved = raw.total_slots - free - running
        if reserved < known_reserved:
            raise CapacityConflict(f"invalid effective reservation count for {worker_id}")
        return WorkerState(
            worker_id=raw.worker_id,
            total_slots=raw.total_slots,
            running_slots=running,
            reserved_slots=reserved,
            online=raw.online and record.active and not record.compute_quarantined,
            accepting_work=raw.accepting_work and not record.compute_quarantined,
            cpu_percent=raw.cpu_percent,
            total_memory_bytes=raw.total_memory_bytes,
            available_memory_bytes=raw.available_memory_bytes,
            cpu_cores=raw.cpu_cores,
            environment_ids=raw.environment_ids,
            prepared_program_ids=raw.prepared_program_ids,
            supported_modes=raw.supported_modes,
        )

    def _effective_contexts(self, run: CoordinatorRun) -> tuple[WorkerContext, ...]:
        """Return scheduler-visible free context capacity after coordinator reservations."""
        users: dict[str, int] = {}
        for attempt in run.attempts.values():
            if attempt.context_id is None or not attempt.status.occupies_capacity:
                continue
            users[attempt.context_id] = users.get(attempt.context_id, 0) + 1
        result: list[WorkerContext] = []
        for context_id in sorted(run.contexts):
            context = run.contexts[context_id]
            used = users.get(context_id, 0)
            if used > context.available_slots:
                raise CapacityConflict(
                    f"context {context_id} reservations exceed capacity: {used}>{context.available_slots}"
                )
            result.append(replace(context, available_slots=context.available_slots - used))
        return tuple(result)

    @staticmethod
    def _validate_context_contract(plan: ExecutionPlan, affinities: dict[str, TaskAffinity],
                                   context_id: str, worker_id: str,
                                   task_ids: Iterable[str]) -> None:
        """Validate one context contract against the run's immutable task affinities."""
        for task_id in task_ids:
            if task_id not in plan.task_index:
                raise ContextConflict("context references unknown task")
            affinity = affinities.get(task_id)
            if affinity is None:
                continue
            if affinity.context_id is not None and affinity.context_id != context_id:
                raise ContextConflict("context conflicts with task context affinity")
            if affinity.required_worker is not None and affinity.required_worker != worker_id:
                raise ContextConflict("context owner conflicts with required worker")
            if affinity.allowed_workers is not None and worker_id not in affinity.allowed_workers:
                raise ContextConflict("context owner excluded by allowed workers")

    def _context_retention_usage(self) -> tuple[int, dict[str, int], dict[str, int]]:
        """Count authoritative contexts plus slots reserved by active preparations."""
        total = 0
        per_worker: dict[str, int] = {}
        per_run: dict[str, int] = {}

        def add(run_id: str, worker_id: str) -> None:
            nonlocal total
            total += 1
            per_worker[worker_id] = per_worker.get(worker_id, 0) + 1
            per_run[run_id] = per_run.get(run_id, 0) + 1

        for run_id, run in self._runs.items():
            for context in run.contexts.values():
                add(run_id, context.worker_id)
        for request in self._pending_ops.active_requests():
            if not isinstance(request, PendingContextPreparation):
                continue
            run = self._runs.get(request.run_id)
            if run is None or request.context_id in run.contexts:
                continue
            add(request.run_id, request.worker_id)
        return total, per_worker, per_run

    def _ensure_context_retention_capacity(self, run_id: str, worker_id: str) -> None:
        total, per_worker, per_run = self._context_retention_usage()
        if total >= self.operation_limits.max_retained_contexts_global:
            raise OperationalLimitExceeded("global retained-context limit reached")
        if per_worker.get(worker_id, 0) >= self.operation_limits.max_retained_contexts_per_worker:
            raise OperationalLimitExceeded(
                f"worker {worker_id} retained-context limit reached"
            )
        if per_run.get(run_id, 0) >= self.operation_limits.max_retained_contexts_per_run:
            raise OperationalLimitExceeded(f"run {run_id} retained-context limit reached")

    def _ensure_initial_context_retention_capacity(
            self, run_id: str, contexts: tuple[WorkerContext, ...]) -> None:
        total, per_worker, per_run = self._context_retention_usage()
        if total + len(contexts) > self.operation_limits.max_retained_contexts_global:
            raise OperationalLimitExceeded("global retained-context limit reached")
        if per_run.get(run_id, 0) + len(contexts) > self.operation_limits.max_retained_contexts_per_run:
            raise OperationalLimitExceeded(f"run {run_id} retained-context limit reached")
        additions: dict[str, int] = {}
        for context in contexts:
            additions[context.worker_id] = additions.get(context.worker_id, 0) + 1
        for worker_id, count in additions.items():
            if (per_worker.get(worker_id, 0) + count
                    > self.operation_limits.max_retained_contexts_per_worker):
                raise OperationalLimitExceeded(
                    f"worker {worker_id} retained-context limit reached"
                )

    # ---------- runs / snapshots / scheduling ----------
    def preflight_run_submission(self, run_id: str) -> None:
        """Validate run-identity/retention admission without mutating coordinator state.

        This narrow preflight exists for adapters that must reserve other bounded
        resources before calling :meth:`submit`.  ``submit`` repeats the same
        checks so callers cannot bypass coordinator authority.
        """
        if not isinstance(run_id, str) or not run_id.strip():
            raise ValueError("run_id must be nonempty text")
        if run_id in self._runs:
            raise InvalidRunTransition(f"run already exists: {run_id}")
        if len(self._runs) >= self.operation_limits.max_runs_in_memory:
            raise OperationalLimitExceeded(
                "in-memory run limit reached; prune terminal history before admitting more runs"
            )
        if self._history_store is not None and self._history_store.has_run(run_id):
            raise InvalidRunTransition(f"run identity already exists in durable history: {run_id}")

    def submit(self, plan: ExecutionPlan, *, run_id: str,
               affinities: Iterable[TaskAffinity] = (),
               contexts: Iterable[WorkerContext] = ()) -> str:
        if not isinstance(plan, ExecutionPlan):
            raise TypeError("ExecutionPlan required")
        self.preflight_run_submission(run_id)
        affinities = tuple(affinities)
        contexts = tuple(contexts)
        if len({a.task_id for a in affinities}) != len(affinities):
            raise ValueError("duplicate task affinity")
        if any(a.task_id not in plan.task_index for a in affinities):
            raise ValueError("affinity references unknown task")
        context_affinity_counts: dict[str, int] = {}
        for affinity in affinities:
            if affinity.context_id is not None:
                context_affinity_counts[affinity.context_id] = (
                    context_affinity_counts.get(affinity.context_id, 0) + 1
                )
        if any(count > p.MAX_COLLECTION_ITEMS for count in context_affinity_counts.values()):
            raise OperationalLimitExceeded(
                f"context task count exceeds protocol limit {p.MAX_COLLECTION_ITEMS}"
            )
        if len({c.context_id for c in contexts}) != len(contexts):
            raise ContextConflict("duplicate context_id in initial run contexts")
        affinities_by_task = {a.task_id: a for a in affinities}
        self._ensure_initial_context_retention_capacity(run_id, contexts)
        for context in contexts:
            owner = self._workers.get(context.worker_id)
            if owner is None or not owner.active:
                raise ContextConflict("initial context owner is not an active worker")
            self._validate_context_contract(
                plan, affinities_by_task, context.context_id, context.worker_id,
                context.prepared_task_ids,
            )
        run = CoordinatorRun.create(run_id, plan, affinities, contexts)
        self._runs[run_id] = run
        if run.status.terminal:
            self._archive_terminal_run(run)
        self._touch()
        return run_id

    def _run(self, run_id: str) -> CoordinatorRun:
        try:
            return self._runs[run_id]
        except KeyError as error:
            raise UnknownRun(run_id) from error

    def _remember_pruned_run(self, run_id: str) -> None:
        if run_id in self._pruned_run_tombstone_set:
            return
        self._pruned_run_tombstones.append(run_id)
        self._pruned_run_tombstone_set.add(run_id)
        while len(self._pruned_run_tombstones) > self.MAX_PRUNED_RUN_TOMBSTONES:
            expired = self._pruned_run_tombstones.popleft()
            self._pruned_run_tombstone_set.discard(expired)

    @staticmethod
    def _message_run_id(message: p.Message) -> str | None:
        attempt = getattr(message, "attempt", None)
        if isinstance(attempt, AttemptIdentity):
            return attempt.run_id
        data = getattr(message, "data", None)
        if isinstance(data, p.DataReference):
            return data.run_id
        transfer = getattr(message, "transfer", None)
        if isinstance(transfer, p.TransferIdentity):
            return transfer.data.run_id
        return None

    def build_snapshot(self, run_id: str) -> ClusterSnapshot:
        run = self._run(run_id)
        if run.status != RunStatus.RUNNING:
            raise InvalidRunTransition(f"cannot schedule run in {run.status.value}")
        snapshot_id = self._new("snapshot")
        ready = tuple(
            ReadyTask(t.task_id, t.ready_sequence if t.ready_sequence is not None else 0, t.wait_rounds)
            for t in sorted(run.tasks.values(), key=lambda x: (x.ready_sequence if x.ready_sequence is not None else 10**18, x.task_id))
            if t.status == TaskStatus.READY
        )
        workers = tuple(self._effective_worker_state(w) for w in sorted(self._workers)
                        if self._workers[w].active)
        commitments = []
        for attempt in run.attempts.values():
            if not attempt.status.active:
                continue
            phase = (CommitmentPhase.RUNNING if attempt.status == AttemptStatus.RUNNING
                     else CommitmentPhase.WAITING_TRANSFER if attempt.status == AttemptStatus.WAITING_TRANSFER
                     else CommitmentPhase.DISPATCHED)
            commitments.append(TaskCommitment(attempt.identity, attempt.worker_id, phase))
        blocked = frozenset(t.task_id for t in run.tasks.values() if t.status == TaskStatus.BLOCKED)
        completed = frozenset(t.task_id for t in run.tasks.values() if t.status == TaskStatus.COMMITTED)
        # F27: scheduling only consumes locations for tasks that are READY in
        # this snapshot.  Building every historical location on every schedule
        # call made a dependent chain quadratic as committed outputs accumulated.
        # These references originate from the validated plan/location index, so
        # construct scheduler-internal records directly rather than round-tripping
        # each one through protocol serialization validation again.
        needed_refs: dict[p.DataReference, None] = {}
        for ready_task in ready:
            for data in self._required_data_refs(run, ready_task.task_id):
                needed_refs[data] = None
        data = []
        for ref in sorted(
            needed_refs,
            key=lambda item: (item.value_id, item.object_state_id or "", item.form.value),
        ):
            worker_ids = self._locations.worker_ids(ref)
            if not worker_ids:
                continue
            replicas = tuple(
                Replica(worker_id, ReplicaStatus.AVAILABLE)
                for worker_id in sorted(worker_ids)
            )
            data.append(DataLocation(
                ref.value_id, ref.form, replicas, self._locations.size_bytes(ref),
                ref.object_state_id,
            ))

        snapshot = ClusterSnapshot(
            plan_id=run.plan.id,
            run_id=run_id,
            snapshot_id=snapshot_id,
            ready=ready,
            workers=workers,
            data=tuple(data),
            commitments=tuple(sorted(commitments, key=lambda c: c.attempt.task_id)),
            affinities=tuple(run.affinities[k] for k in sorted(run.affinities)),
            contexts=self._effective_contexts(run),
            blocked_task_ids=blocked,
            completed_task_ids=completed,
        )
        snapshot.validate(run.plan)
        run.last_snapshot_id = snapshot_id
        run.last_snapshot_revision = self._revision
        # A newly built snapshot invalidates any older proposal, even if the
        # coordinator revision has not changed.
        run.last_decision = None
        return snapshot

    def propose(self, run_id: str) -> SchedulingDecision:
        run = self._run(run_id)
        snapshot = self.build_snapshot(run_id)
        decision = run.scheduler.schedule(snapshot)
        run.last_decision = decision
        return decision

    def _required_data_ref_map(self, run: CoordinatorRun, task_id: str) -> dict[str, p.DataReference]:
        """Map logical task-input IDs to physical transferable representations.

        Immutable plain aliases reuse the producer's payload representation, but
        the logical alias ID remains the task input for binding/readiness semantics.
        Shared-reference snapshots keep their exact logical/value-version key.
        """
        manifest = run.plan.task_index[task_id]
        refs: dict[str, p.DataReference] = {}
        if manifest.mode != ExecutionMode.ISOLATED_CANDIDATE:
            # F58: the persistent namespace must import the latest transferable
            # bindings produced by isolated ancestors when those producers ran on
            # another worker.  The scheduler reports these logical IDs exactly as
            # it reports ordinary isolated inputs, so transfer staging can stay
            # placement-generic.
            for requirement in run.plan.context_seed_requirements(task_id):
                if requirement.kind == ValueKind.IMMUTABLE:
                    refs[requirement.id] = p.DataReference(
                        run.plan.id, run.run_id,
                        run.plan.immutable_representation_id(requirement.id),
                        DataForm.IMMUTABLE_VALUE,
                    )
                elif requirement.kind == ValueKind.SHARED_REFERENCE:
                    refs[requirement.id] = p.DataReference(
                        run.plan.id, run.run_id, requirement.id,
                        DataForm.OBJECT_SNAPSHOT, None,
                    )
            return refs
        objects = {obj.object_id: obj for obj in manifest.objects}
        for requirement in manifest.inputs:
            if requirement.kind == ValueKind.IMMUTABLE:
                refs[requirement.id] = p.DataReference(
                    run.plan.id, run.run_id,
                    run.plan.immutable_representation_id(requirement.id),
                    DataForm.IMMUTABLE_VALUE,
                )
            elif requirement.kind == ValueKind.SHARED_REFERENCE:
                obj = objects[requirement.value.object_id]
                if obj.access != ObjectAccess.SNAPSHOT_CANDIDATE or len(obj.state_inputs) > 1:
                    raise PlacementRejected(f"unsupported snapshot input: {requirement.id}")
                refs[requirement.id] = p.DataReference(
                    run.plan.id, run.run_id, requirement.id, DataForm.OBJECT_SNAPSHOT,
                    next(iter(obj.state_inputs), None),
                )
        return refs

    def _required_data_refs(self, run: CoordinatorRun, task_id: str) -> tuple[p.DataReference, ...]:
        return tuple(dict.fromkeys(self._required_data_ref_map(run, task_id).values()))

    def _planned_remote_inputs(self, run: CoordinatorRun, placement: Placement) -> tuple[tuple[p.DataReference, str], ...]:
        by_logical_input = self._required_data_ref_map(run, placement.task_id)
        planned: list[tuple[p.DataReference, str]] = []
        seen_routes: set[tuple[p.DataReference, str]] = set()
        for value_id in placement.preference.locality.remote_input_ids:
            ref = by_logical_input.get(value_id)
            if ref is None:
                raise PlacementRejected(f"scheduler reported unknown remote input: {value_id}")
            candidates = sorted(
                worker_id for worker_id in self._available_replica_workers(ref)
                if worker_id != placement.worker_id
            )
            if not candidates:
                raise PlacementRejected(f"remote input lost before acceptance: {value_id}")
            route = (ref, candidates[0])
            if route in seen_routes:
                continue
            seen_routes.add(route)
            planned.append(route)
        return tuple(planned)

    def accept_decision(self, run_id: str, decision: SchedulingDecision) -> tuple[AttemptIdentity, ...]:
        run = self._run(run_id)
        if run.status != RunStatus.RUNNING:
            raise PlacementRejected("run is not running")
        if decision.plan_id != run.plan.id or decision.run_id != run_id:
            raise PlacementRejected("foreign scheduling decision")
        if decision.snapshot_id != run.last_snapshot_id or run.last_snapshot_revision != self._revision:
            raise PlacementRejected("scheduling decision is stale")
        if run.last_decision is None or decision != run.last_decision:
            raise PlacementRejected("scheduling decision was not produced by the authoritative scheduler proposal")
        planned_inputs: dict[str, tuple[tuple[p.DataReference, str], ...]] = {}
        for placement in decision.placements:
            task = run.tasks.get(placement.task_id)
            if task is None or task.status != TaskStatus.READY:
                raise PlacementRejected(f"task no longer ready: {placement.task_id}")
            worker = self._workers.get(placement.worker_id)
            if worker is None or not worker.active:
                raise PlacementRejected(f"worker unavailable: {placement.worker_id}")
            planned_inputs[placement.task_id] = self._planned_remote_inputs(run, placement)
        counts: dict[str, int] = {}
        for placement in decision.placements:
            counts[placement.worker_id] = counts.get(placement.worker_id, 0) + 1
        for worker_id, count in counts.items():
            if self._effective_worker_state(worker_id).free_slots < count:
                raise PlacementRejected(f"insufficient current capacity: {worker_id}")

        staged: list[_StagedPlacement] = []
        new_attempt_ids: set[str] = set()
        new_transfer_keys: set[tuple[str, str]] = set()
        new_transfer_ids: set[str] = set()
        for placement in decision.placements:
            staged.append(self._stage_placement(
                run, placement, planned_inputs[placement.task_id],
                new_attempt_ids, new_transfer_keys, new_transfer_ids,
            ))

        staged_transfer_count = sum(len(prepared.transfers) for prepared in staged)
        if staged_transfer_count:
            self._ensure_transfer_record_capacity(run_id, staged_transfer_count)

        outbound_counts: dict[str, int] = {}
        for prepared in staged:
            if prepared.transfers:
                for transfer_record, _ in prepared.transfers:
                    wid = transfer_record.identity.destination_worker_id
                    outbound_counts[wid] = outbound_counts.get(wid, 0) + 1
            else:
                wid = prepared.placement.worker_id
                outbound_counts[wid] = outbound_counts.get(wid, 0) + 1
        for worker_id, count in outbound_counts.items():
            self._ensure_outbox_capacity(self._worker(worker_id), count)

        # All placements are fully staged before authoritative state changes.  If
        # staging raises, no task/attempt/transfer/outbox mutation has occurred.
        for prepared in staged:
            self._apply_staged_placement(run, prepared)
        placed_ids = {p.task_id for p in decision.placements}
        for task in run.tasks.values():
            if task.status == TaskStatus.READY and task.task_id not in placed_ids:
                task.wait_rounds += 1
        run.last_decision = None
        self._touch()
        return tuple(prepared.attempt.identity for prepared in staged)

    def _stage_placement(
            self, run: CoordinatorRun, placement: Placement,
            remote_inputs: tuple[tuple[p.DataReference, str], ...],
            new_attempt_ids: set[str],
            new_transfer_keys: set[tuple[str, str]],
            new_transfer_ids: set[str]) -> _StagedPlacement:
        manifest = run.plan.task_index[placement.task_id]
        attempt_id = self._new("attempt")
        if attempt_id in run.attempts or attempt_id in new_attempt_ids:
            raise CoordinatorError(f"attempt identity reused: {attempt_id}")
        new_attempt_ids.add(attempt_id)
        attempt = AttemptIdentity(run.plan.id, run.run_id, placement.task_id, attempt_id)
        worker = self._worker(placement.worker_id)

        if not remote_inputs:
            dispatch_id = self._new("message")
            record = AttemptRecord(
                attempt, placement.worker_id, worker.handle.generation, dispatch_id,
                status=AttemptStatus.DISPATCHED, context_id=placement.context_id,
                phase_since=self._now(None),
            )
            dispatch = p.TaskDispatch(
                worker_id=placement.worker_id, attempt=attempt,
                program_id=run.plan.program.id, mode=manifest.mode,
                context_id=placement.context_id, message_id=dispatch_id,
            )
            return _StagedPlacement(placement, record, dispatch, ())

        record = AttemptRecord(
            attempt, placement.worker_id, worker.handle.generation, None,
            status=AttemptStatus.WAITING_TRANSFER, context_id=placement.context_id,
            phase_since=self._now(None),
        )
        transfers: list[tuple[TransferRecord, p.PrepareReceive]] = []
        for data, source_worker_id in remote_inputs:
            transfer = p.TransferIdentity(
                data, self._new("transfer"), self._new("transfer-attempt"),
                source_worker_id, placement.worker_id,
            )
            transfer_key = (transfer.transfer_id, transfer.transfer_attempt_id)
            if (transfer_key in self._transfers or transfer_key in new_transfer_keys
                    or transfer.transfer_id in self._active_transfer_attempt
                    or transfer.transfer_id in new_transfer_ids):
                raise CoordinatorError(
                    f"generated transfer identity reused: "
                    f"{transfer.transfer_id}/{transfer.transfer_attempt_id}"
                )
            new_transfer_keys.add(transfer_key)
            new_transfer_ids.add(transfer.transfer_id)
            message_id = self._new("message")
            source_handle = self._worker(source_worker_id).handle
            authorization = self._new("transfer-authorization")
            transfer_record = TransferRecord(
                transfer, TransferStatus.DESTINATION_PREPARING, message_id,
                source_generation=source_handle.generation,
                destination_generation=worker.handle.generation,
                source_session_id=source_handle.session_id,
                destination_session_id=worker.handle.session_id,
                authorization=authorization,
                size_bytes=self._locations.size_bytes(data),
                consumer_run_id=run.run_id, consumer_attempt_id=attempt.attempt_id,
                phase_since=self._now(None),
            )
            prepare = p.PrepareReceive(
                transfer=transfer, size_bytes=transfer_record.size_bytes,
                source_session_id=transfer_record.source_session_id,
                destination_session_id=transfer_record.destination_session_id,
                authorization=transfer_record.authorization,
                message_id=message_id,
            )
            transfers.append((transfer_record, prepare))
            record.pending_transfers.add((transfer.transfer_id, transfer.transfer_attempt_id))
        return _StagedPlacement(placement, record, None, tuple(transfers))

    def _apply_staged_placement(self, run: CoordinatorRun, staged: _StagedPlacement) -> None:
        task = run.tasks[staged.placement.task_id]
        attempt = staged.attempt
        run.attempts[attempt.identity.attempt_id] = attempt
        task.attempt_ids.append(attempt.identity.attempt_id)
        task.current_attempt_id = attempt.identity.attempt_id

        if staged.transfers:
            task.status = TaskStatus.WAITING_TRANSFER
            for transfer_record, prepare in staged.transfers:
                key = (
                    transfer_record.identity.transfer_id,
                    transfer_record.identity.transfer_attempt_id,
                )
                self._transfers[key] = transfer_record
                self._active_transfer_attempt[transfer_record.identity.transfer_id] = (
                    transfer_record.identity.transfer_attempt_id
                )
                self._queued_transfers[key] = prepare
            self._release_queued_transfers()
            return

        task.status = TaskStatus.DISPATCHED
        assert staged.dispatch is not None
        self._enqueue_message(self._worker(staged.placement.worker_id), staged.dispatch)

    def schedule(self, run_id: str) -> ScheduleResult:
        decision = self.propose(run_id)
        attempts = self.accept_decision(run_id, decision)
        return ScheduleResult(attempts, tuple(u.task_id for u in decision.unplaced))

    # ---------- preparation helpers ----------
    def request_program_preparation(self, worker_id: str, plan: ExecutionPlan) -> p.PrepareProgram | None:
        worker = self._worker(worker_id)
        if plan.program.id in worker.reported.prepared_program_ids:
            return None

        contract = ProgramPreparationContract(
            worker_id, worker.handle.generation, plan.id, plan.program,
        )
        request = self._pending_ops.find_program(contract)
        if request is not None:
            return p.PrepareProgram(
                worker_id=request.worker_id, plan_id=request.plan_id,
                program=request.program, message_id=request.message_id,
            )

        message_id = self._pending_ops.new_message_id()
        message = p.PrepareProgram(
            worker_id=worker_id, plan_id=plan.id, program=plan.program,
            message_id=message_id,
        )
        self._ensure_outbox_capacity(worker)
        self._pending_ops.record(PendingProgramPreparation(
            message_id, worker_id, worker.handle.generation, plan.id, plan.program,
            self._now(None),
        ))
        self._enqueue_message(worker, message)
        self._touch()
        return message

    def request_context_preparation(self, run_id: str, worker_id: str,
                                    context_id: str, task_ids: Iterable[str]) -> p.PrepareContext | None:
        run = self._run(run_id)
        if run.status != RunStatus.RUNNING:
            raise InvalidRunTransition(f"cannot prepare context for {run.status.value} run")
        worker = self._worker(worker_id)
        task_ids = tuple(sorted(set(task_ids)))
        if len(task_ids) > p.MAX_COLLECTION_ITEMS:
            raise OperationalLimitExceeded(
                f"context task count exceeds protocol limit {p.MAX_COLLECTION_ITEMS}"
            )
        if any(t not in run.tasks for t in task_ids):
            raise UnknownTask("context task")
        self._validate_context_contract(
            run.plan, run.affinities, context_id, worker_id, task_ids,
        )

        existing = run.contexts.get(context_id)
        if existing is not None:
            if (existing.worker_id == worker_id
                    and existing.prepared_task_ids == frozenset(task_ids)):
                return None
            raise ContextConflict(
                f"context {context_id} already exists with a different contract"
            )

        contract = ContextPreparationContract(
            worker_id, worker.handle.generation, run.plan.id, run_id,
            run.plan.program.id, context_id, task_ids,
        )
        matching_pending = self._pending_ops.find_context(run_id, context_id)
        if matching_pending is not None and matching_pending.contract != contract:
            raise ContextConflict(
                f"context {context_id} already has a different pending contract"
            )

        if matching_pending is not None:
            return p.PrepareContext(
                worker_id=matching_pending.worker_id,
                plan_id=matching_pending.plan_id,
                run_id=matching_pending.run_id,
                program_id=matching_pending.program_id,
                context_id=matching_pending.context_id,
                task_ids=matching_pending.task_ids,
                message_id=matching_pending.message_id,
            )

        # Reserve retained-context admission before asking the worker to create
        # physical context state. Active preparations count toward this bound so
        # successful replies cannot overfill the retained collection.
        self._ensure_context_retention_capacity(run_id, worker_id)
        message_id = self._pending_ops.new_message_id()
        message = p.PrepareContext(
            worker_id=worker_id, plan_id=run.plan.id, run_id=run_id,
            program_id=run.plan.program.id, context_id=context_id, task_ids=task_ids,
            message_id=message_id,
        )
        self._ensure_outbox_capacity(worker)
        self._pending_ops.record(PendingContextPreparation(
            message_id, worker_id, worker.handle.generation, run.plan.id, run_id,
            run.plan.program.id, context_id, task_ids, self._now(None),
        ))
        self._enqueue_message(worker, message)
        self._touch()
        return message

    # ---------- inbound message routing ----------
    def handle_message(self, session: SessionHandle, message: p.Message, *, now: float | None = None) -> EventDisposition:
        worker = self._session(session)
        if not isinstance(message, p.Message):
            raise InvalidWorkerMessage("typed protocol Message required")
        claimed = self._message_worker_id(message)
        if claimed is not None and claimed != session.worker_id:
            raise InvalidWorkerMessage("message worker_id disagrees with session")
        run_id = self._message_run_id(message)
        if run_id is not None and run_id not in self._runs and run_id in self._pruned_run_tombstone_set:
            return EventDisposition.STALE
        seen_at = self._now(now)
        previous_seen = worker.last_seen
        worker.last_seen = seen_at
        try:
            disposition = self._dispatch_worker_message(worker, message)
        except Exception:
            # Message receipt time is part of the same authoritative transition.
            # If a staged provider/protocol operation aborts, do not refresh the
            # worker's liveness independently of the rejected control-plane event.
            if self._workers.get(session.worker_id) is worker and worker.active:
                worker.last_seen = previous_seen
            raise
        self._release_queued_transfers(seen_at)
        return disposition

    def _dispatch_worker_message(self, worker: WorkerRecord, message: p.Message) -> EventDisposition:
        if isinstance(message, p.Heartbeat):
            return self._handle_heartbeat(worker, message)
        if isinstance(message, p.WorkerGoodbye):
            self._lose_worker(worker, message.reason or "worker goodbye")
            return EventDisposition.APPLIED
        if isinstance(message, p.ProgramPrepared):
            return self._handle_program_prepared(worker, message)
        if isinstance(message, p.ProgramPreparationFailed):
            return self._handle_program_preparation_failed(worker, message)
        if isinstance(message, p.ProgramUnavailable):
            return self._handle_program_unavailable(worker, message)
        if isinstance(message, p.ContextPrepared):
            return self._handle_context_prepared(worker, message)
        if isinstance(message, p.ContextPreparationFailed):
            return self._handle_context_preparation_failed(worker, message)
        if isinstance(message, p.ContextUnavailable):
            return self._handle_context_unavailable(worker, message)
        if isinstance(message, p.TaskAccepted):
            return self._handle_task_accepted(worker, message)
        if isinstance(message, p.TaskRejected):
            return self._handle_task_rejected(worker, message)
        if isinstance(message, p.TaskStarted):
            return self._handle_task_started(worker, message)
        if isinstance(message, p.TaskSucceeded):
            return self._handle_task_succeeded(worker, message)
        if isinstance(message, p.TaskFailed):
            return self._handle_task_failed(worker, message)
        if isinstance(message, p.TaskCancellationResult):
            return self._handle_cancellation_result(worker, message)
        if isinstance(message, p.ObjectAvailable):
            return self._handle_object_available(worker, message)
        if isinstance(message, p.ObjectUnavailable):
            return self._handle_object_unavailable(worker, message)
        if isinstance(message, p.ObjectReleased):
            return self._handle_object_released(worker, message)
        if isinstance(message, p.ReceiveReady):
            return self._handle_receive_ready(worker, message)
        if isinstance(message, p.ReceivePreparationFailed):
            return self._handle_receive_preparation_failed(worker, message)
        if isinstance(message, p.TransferAccepted):
            return self._handle_transfer_accepted(worker, message)
        if isinstance(message, p.TransferStarted):
            return self._handle_transfer_started(worker, message)
        if isinstance(message, p.TransferCompleted):
            return self._handle_transfer_completed(worker, message)
        if isinstance(message, p.TransferFailed):
            return self._handle_transfer_failed(worker, message)
        raise InvalidWorkerMessage(
            f"unsupported coordinator inbound message: {type(message).__name__}"
        )

    @staticmethod
    def _message_worker_id(message: p.Message) -> str | None:
        if isinstance(message, p.Heartbeat):
            return message.worker.worker_id
        value = getattr(message, "worker_id", None)
        if value is None and isinstance(message, p.ContextPrepared):
            return message.context.worker_id
        if value is None and isinstance(message, (p.ReceiveReady, p.ReceivePreparationFailed,
                                                   p.TransferAccepted, p.TransferStarted,
                                                   p.TransferCompleted, p.TransferFailed)):
            return message.worker_id
        return value

    def _handle_heartbeat(self, worker: WorkerRecord, message: p.Heartbeat) -> EventDisposition:
        if message.sequence < worker.last_sequence:
            return EventDisposition.STALE
        duplicate = message.sequence == worker.last_sequence
        if not duplicate:
            known_running, known_reserved = self._known_worker_load(worker.handle.worker_id)
            if known_running + known_reserved > message.worker.total_slots:
                raise CapacityConflict(
                    f"heartbeat capacity for {worker.handle.worker_id} is below unresolved coordinator reservations"
                )
        # ACK construction is provider-dependent; resolve it before publishing the
        # heartbeat sequence/capability update so an injected ID failure is atomic.
        ack = p.HeartbeatAck(
            worker_id=worker.handle.worker_id, sequence=message.sequence,
            message_id=self._new("message"), correlation_id=message.message_id,
        )
        if not duplicate:
            was_online = worker.reported.online
            staged_failures = (
                self._stage_worker_unavailability_failures(worker)
                if was_online and not message.worker.online else None
            )
            worker.last_sequence = message.sequence
            worker.reported = message.worker
            if was_online and not message.worker.online:
                # The control session may still be alive, but the compute worker
                # explicitly says its execution state is unavailable. Reconcile
                # attempts/contexts/data exactly once without pretending that
                # heartbeats prove those operations are still alive.
                self._reconcile_compute_unavailable(
                    worker, f"worker {worker.handle.worker_id} reported offline",
                    staged_failures=staged_failures,
                )
            self._touch()
        self._enqueue_message(worker, ack)
        return EventDisposition.DUPLICATE if duplicate else EventDisposition.APPLIED

    def _invalidate_pending_for_session(self, worker: WorkerRecord) -> None:
        self._pending_ops.invalidate_session(worker.handle)

    def _invalidate_pending_for_run(self, run_id: str) -> None:
        self._pending_ops.invalidate_run(run_id)

    def _handle_program_prepared(self, worker: WorkerRecord, message: p.ProgramPrepared) -> EventDisposition:
        resolved = self._pending_ops.resolve(worker.handle, message.correlation_id, PendingProgramPreparation)
        pending, active = resolved.request, resolved.active
        if pending.plan_id != message.plan_id:
            raise InvalidWorkerMessage("program preparation plan mismatch")
        if message.program_id != pending.program.id:
            raise InvalidWorkerMessage("prepared program differs from requested program")
        if not active:
            if message.program_id in worker.reported.prepared_program_ids:
                return EventDisposition.DUPLICATE
            raise InvalidWorkerMessage("program preparation response arrived after request retirement")
        if message.program_id in worker.reported.prepared_program_ids:
            self._pending_ops.complete(message.correlation_id)
            self._touch()
            return EventDisposition.DUPLICATE
        worker.reported = replace(
            worker.reported,
            prepared_program_ids=frozenset((*worker.reported.prepared_program_ids, pending.program.id)),
        )
        self._pending_ops.complete(message.correlation_id)
        self._touch()
        return EventDisposition.APPLIED

    def _handle_program_preparation_failed(self, worker: WorkerRecord,
                                           message: p.ProgramPreparationFailed) -> EventDisposition:
        resolved = self._pending_ops.resolve(worker.handle, message.correlation_id, PendingProgramPreparation)
        pending, active = resolved.request, resolved.active
        if message.plan_id != pending.plan_id or message.program_id != pending.program.id:
            raise InvalidWorkerMessage("program preparation failure does not match requested program")
        if not active:
            return EventDisposition.DUPLICATE
        self._pending_ops.complete(message.correlation_id)
        self._touch()
        return EventDisposition.APPLIED

    def _handle_program_unavailable(self, worker: WorkerRecord, message: p.ProgramUnavailable) -> EventDisposition:
        if message.program_id not in worker.reported.prepared_program_ids:
            return EventDisposition.DUPLICATE
        worker.reported = replace(
            worker.reported,
            prepared_program_ids=frozenset(x for x in worker.reported.prepared_program_ids if x != message.program_id),
        )
        self._touch()
        return EventDisposition.APPLIED

    def _handle_context_prepared(self, worker: WorkerRecord, message: p.ContextPrepared) -> EventDisposition:
        resolved = self._pending_ops.resolve(worker.handle, message.correlation_id, PendingContextPreparation)
        pending, active = resolved.request, resolved.active
        if pending.run_id != message.run_id:
            raise InvalidWorkerMessage("context preparation run mismatch")
        run = self._run(pending.run_id)
        if message.plan_id != pending.plan_id or message.plan_id != run.plan.id:
            raise InvalidWorkerMessage("context plan mismatch")
        if message.context.context_id != pending.context_id:
            raise InvalidWorkerMessage("prepared context_id differs from requested context")
        if message.context.worker_id != pending.worker_id:
            raise InvalidWorkerMessage("prepared context owner differs from requested worker")
        if message.context.prepared_task_ids != frozenset(pending.task_ids):
            raise InvalidWorkerMessage("prepared context task set differs from requested task set")
        self._validate_context_contract(
            run.plan, run.affinities, message.context.context_id,
            message.context.worker_id, message.context.prepared_task_ids,
        )
        existing = run.contexts.get(pending.context_id)
        if not active:
            if existing == message.context:
                return EventDisposition.DUPLICATE
            raise InvalidWorkerMessage("context preparation response arrived after request retirement")
        if existing == message.context:
            self._pending_ops.complete(message.correlation_id)
            self._touch()
            return EventDisposition.DUPLICATE
        if existing is not None:
            # A context ID names one stable logical contract for the run.  Never
            # permit a later preparation response to replace it (last-write-wins).
            self._pending_ops.complete(message.correlation_id)
            self._touch()
            raise InvalidWorkerMessage("prepared context conflicts with existing authoritative context")
        run.contexts[pending.context_id] = message.context
        self._pending_ops.complete(message.correlation_id)
        self._touch()
        return EventDisposition.APPLIED

    def _handle_context_preparation_failed(self, worker: WorkerRecord,
                                           message: p.ContextPreparationFailed) -> EventDisposition:
        resolved = self._pending_ops.resolve(worker.handle, message.correlation_id, PendingContextPreparation)
        pending, active = resolved.request, resolved.active
        if (message.plan_id != pending.plan_id or message.run_id != pending.run_id
                or message.context_id != pending.context_id):
            raise InvalidWorkerMessage("context preparation failure does not match requested context")
        if not active:
            return EventDisposition.DUPLICATE
        self._pending_ops.complete(message.correlation_id)
        self._touch()
        return EventDisposition.APPLIED

    def _handle_context_unavailable(self, worker: WorkerRecord, message: p.ContextUnavailable) -> EventDisposition:
        run = self._run(message.run_id)
        if message.plan_id != run.plan.id:
            raise InvalidWorkerMessage("context unavailable plan mismatch")
        context = run.contexts.get(message.context_id)
        if context is None:
            return EventDisposition.DUPLICATE
        if context.worker_id != worker.handle.worker_id:
            raise InvalidWorkerMessage("context owner mismatch")
        capacity_users = any(
            attempt.context_id == message.context_id and attempt.status.occupies_capacity
            for attempt in run.attempts.values()
        )
        already_unavailable = message.context_id in run.unavailable_context_ids
        dependents = (self._unfinished_context_dependents(
            run, message.context_id, worker.handle.worker_id, context.prepared_task_ids
        ) if run.status == RunStatus.RUNNING else ())

        failure = None
        coordinator_failure = None
        staged_failure = None
        if dependents:
            reason = message.reason.strip() or "context became unavailable"
            failure = FailureInfo(
                FailureKind.EXECUTION_ERROR,
                f"native/shared context {message.context_id} unavailable: {reason}",
            )
            coordinator_failure = CoordinatorFailure(
                CoordinatorFailureCode.CONTEXT_LOST,
                f"context {message.context_id} on {worker.handle.worker_id} was lost: {reason}",
            )
            # Provider/backpressure work for terminal reconciliation must be
            # complete before publishing the context-loss fact itself.
            staged_failure = self._stage_run_failure(
                run, failure, coordinator_failure
            )

        run.unavailable_context_ids.add(message.context_id)
        if run.status != RunStatus.RUNNING and capacity_users:
            if already_unavailable:
                return EventDisposition.DUPLICATE
            self._touch()
            return EventDisposition.APPLIED

        if dependents:
            assert failure is not None and coordinator_failure is not None
            assert staged_failure is not None
            self._apply_run_failure(
                run, failure, coordinator_failure, staged_failure
            )
        self._retire_unavailable_contexts(run)
        self._touch()
        return EventDisposition.DUPLICATE if already_unavailable else EventDisposition.APPLIED

    def _retire_unavailable_contexts(self, run: CoordinatorRun) -> None:
        """Drop unavailable context tombstones only after physical users stop."""
        for context_id in tuple(run.unavailable_context_ids):
            if any(
                attempt.context_id == context_id and attempt.status.occupies_capacity
                for attempt in run.attempts.values()
            ):
                continue
            run.contexts.pop(context_id, None)
            run.unavailable_context_ids.discard(context_id)

    def _unfinished_context_dependents(self, run: CoordinatorRun, context_id: str,
                                       worker_id: str,
                                       prepared_task_ids: Iterable[str] = ()) -> tuple[str, ...]:
        prepared = frozenset(prepared_task_ids)
        context_object_ids = {
            obj.object_id
            for prepared_task_id in prepared
            if prepared_task_id in run.plan.task_index
            for obj in run.plan.task_index[prepared_task_id].objects
        }
        affected: list[str] = []
        for task_id, task in run.tasks.items():
            if task.status in {TaskStatus.COMMITTED, TaskStatus.FAILED, TaskStatus.CANCELLED}:
                continue
            manifest = run.plan.task_index[task_id]
            affinity = run.affinities.get(task_id)
            if task_id in prepared:
                affected.append(task_id)
                continue
            if affinity is None:
                pass
            elif affinity.context_id == context_id:
                affected.append(task_id)
                continue
            elif (manifest.mode in {ExecutionMode.SHARED_CONTEXT, ExecutionMode.NATIVE_REGION}
                  and affinity.required_worker == worker_id):
                affected.append(task_id)
                continue

            # A committed native mutation may leave an exact object version that
            # still needs snapshot materialization for an isolated downstream task.
            # If the owning context disappears before any certified snapshot exists,
            # there is no semantics-preserving reconstruction path to invent here.
            if context_object_ids and manifest.mode == ExecutionMode.ISOLATED_CANDIDATE:
                for ref in self._required_data_refs(run, task_id):
                    if ref.form != DataForm.OBJECT_SNAPSHOT or self._locations.worker_ids(ref):
                        continue
                    value = run.plan.value_index[ref.value_id]
                    if value.object_id in context_object_ids:
                        affected.append(task_id)
                        break
        return tuple(sorted(set(affected)))

    # ---------- attempts / commit ----------
    def _attempt_for(self, worker: WorkerRecord, attempt: AttemptIdentity,
                     correlation_id: str | None, *, terminal_observation: bool = False
                     ) -> tuple[CoordinatorRun, TaskRecord, AttemptRecord] | EventDisposition:
        run = self._run(attempt.run_id)
        if attempt.plan_id != run.plan.id:
            raise InvalidWorkerMessage("attempt plan mismatch")
        task = run.tasks.get(attempt.task_id)
        if task is None:
            raise UnknownTask(attempt.task_id)
        record = run.attempts.get(attempt.attempt_id)
        if record is None or record.identity != attempt:
            raise UnknownAttempt(attempt.attempt_id)
        if record.worker_id != worker.handle.worker_id or record.worker_generation != worker.handle.generation:
            raise InvalidWorkerMessage("attempt worker/session mismatch")
        if record.dispatch_message_id is None:
            raise InvalidWorkerMessage("task event received before dispatch")
        if correlation_id != record.dispatch_message_id:
            raise InvalidWorkerMessage("task event correlation mismatch")
        if record.status == AttemptStatus.COMMITTED:
            return EventDisposition.DUPLICATE
        if record.status == AttemptStatus.ORPHANED:
            # The result can never commit, but a terminal observation proves the
            # old worker process is no longer consuming this attempt's slot.
            if terminal_observation:
                record.status = AttemptStatus.STALE
                self._retire_unavailable_contexts(run)
                self._touch()
            return EventDisposition.STALE
        if task.current_attempt_id != attempt.attempt_id:
            return EventDisposition.STALE
        return run, task, record

    @staticmethod
    def _mark_attempt_accepted(task: TaskRecord, attempt: AttemptRecord, now: float) -> None:
        """Advance the paired task/attempt view together after validation."""
        attempt.status = AttemptStatus.ACCEPTED
        attempt.phase_since = now
        task.status = TaskStatus.ACCEPTED

    @staticmethod
    def _mark_attempt_running(task: TaskRecord, attempt: AttemptRecord, now: float) -> None:
        """Advance the paired task/attempt view together after validation."""
        attempt.status = AttemptStatus.RUNNING
        attempt.phase_since = now
        task.status = TaskStatus.RUNNING

    @staticmethod
    def _mark_attempt_dispatched(task: TaskRecord, attempt: AttemptRecord, now: float) -> None:
        """Advance a transfer-gated attempt once all inputs are confirmed."""
        attempt.status = AttemptStatus.DISPATCHED
        attempt.phase_since = now
        task.status = TaskStatus.DISPATCHED

    def _handle_task_accepted(self, worker: WorkerRecord, message: p.TaskAccepted) -> EventDisposition:
        resolved = self._attempt_for(worker, message.attempt, message.correlation_id)
        if isinstance(resolved, EventDisposition): return resolved
        run, task, attempt = resolved
        if attempt.status in {AttemptStatus.ACCEPTED, AttemptStatus.RUNNING, AttemptStatus.CANCEL_REQUESTED}:
            return EventDisposition.DUPLICATE
        if attempt.status != AttemptStatus.DISPATCHED:
            raise InvalidTaskTransition(f"cannot accept attempt in {attempt.status.value}")
        self._mark_attempt_accepted(task, attempt, worker.last_seen)
        self._touch()
        return EventDisposition.APPLIED

    def _handle_task_started(self, worker: WorkerRecord, message: p.TaskStarted) -> EventDisposition:
        resolved = self._attempt_for(worker, message.attempt, message.correlation_id)
        if isinstance(resolved, EventDisposition): return resolved
        run, task, attempt = resolved
        if attempt.status in {AttemptStatus.RUNNING, AttemptStatus.CANCEL_REQUESTED}:
            return EventDisposition.DUPLICATE
        if attempt.status != AttemptStatus.ACCEPTED:
            raise InvalidTaskTransition(f"cannot start attempt in {attempt.status.value}")
        self._mark_attempt_running(task, attempt, worker.last_seen)
        self._touch()
        return EventDisposition.APPLIED

    def _handle_task_rejected(self, worker: WorkerRecord, message: p.TaskRejected) -> EventDisposition:
        resolved = self._attempt_for(
            worker, message.attempt, message.correlation_id, terminal_observation=True
        )
        if isinstance(resolved, EventDisposition): return resolved
        run, task, attempt = resolved
        if attempt.status == AttemptStatus.REJECTED:
            return EventDisposition.DUPLICATE
        if attempt.status == AttemptStatus.CANCEL_REQUESTED:
            attempt.status = AttemptStatus.CANCELLED
            task.current_attempt_id = None
            task.status = TaskStatus.CANCELLED
            self._retire_unavailable_contexts(run)
            self._finish_cancellation_if_idle(run)
            self._touch()
            return EventDisposition.STALE
        if attempt.status != AttemptStatus.DISPATCHED:
            raise InvalidTaskTransition(f"cannot reject attempt in {attempt.status.value}")
        manifest = run.plan.task_index[task.task_id]
        failure = FailureInfo(
            FailureKind.EXECUTION_ERROR,
            f"worker rejected task: {message.code.value}: {message.detail}"
        )
        native_context_failure = (
            manifest.mode in {ExecutionMode.SHARED_CONTEXT, ExecutionMode.NATIVE_REGION}
            and message.code in {p.RejectionCode.CONTEXT_UNAVAILABLE, p.RejectionCode.INPUT_UNAVAILABLE}
        )
        retryable = message.code in {
            p.RejectionCode.BUSY, p.RejectionCode.PROGRAM_UNAVAILABLE,
            p.RejectionCode.INPUT_UNAVAILABLE, p.RejectionCode.CONTEXT_UNAVAILABLE,
        }
        can_retry = (
            not native_context_failure and retryable
            and len(task.attempt_ids) < self.retry_policy.max_attempts_per_task
            and run.status == RunStatus.RUNNING
        )

        coordinator_failure = None
        staged_failure = None
        if native_context_failure:
            coordinator_failure = CoordinatorFailure(
                CoordinatorFailureCode.CONTEXT_LOST,
                f"task {task.task_id} cannot safely reconstruct its native/shared context",
            )
            staged_failure = self._stage_run_failure(
                run, failure, coordinator_failure,
                terminal_attempt_ids=frozenset({attempt.identity.attempt_id}),
            )
        elif not can_retry:
            staged_failure = self._stage_run_failure(
                run, failure, terminal_attempt_ids=frozenset({attempt.identity.attempt_id}),
            )

        # Apply the terminal observation only after any run-failure cleanup has
        # been fully staged. Provider/backpressure failure above leaves this
        # attempt authoritative and retryable by the caller.
        attempt.status = AttemptStatus.REJECTED
        task.current_attempt_id = None
        attempt.failure = failure
        if staged_failure is not None:
            task.status = TaskStatus.FAILED
            task.failure = failure
            self._apply_run_failure(run, failure, coordinator_failure, staged_failure)
        else:
            self._retry_or_fail(run, task, retryable, failure)
        self._touch()
        return EventDisposition.APPLIED

    def _handle_task_succeeded(self, worker: WorkerRecord, message: p.TaskSucceeded) -> EventDisposition:
        result = message.result
        resolved = self._attempt_for(
            worker, result.attempt, message.correlation_id, terminal_observation=True
        )
        if isinstance(resolved, EventDisposition): return resolved
        run, task, attempt = resolved
        if run.status not in {RunStatus.RUNNING, RunStatus.CANCELLING}:
            return EventDisposition.STALE
        if attempt.status not in {AttemptStatus.RUNNING, AttemptStatus.CANCEL_REQUESTED}:
            raise InvalidTaskTransition(f"success in {attempt.status.value}")
        run.plan.validate_result(result, expected_attempt=attempt.identity)
        attempt.stdout_tail = result.stdout_tail
        attempt.stderr_tail = result.stderr_tail
        attempt.stdout_truncated = result.stdout_truncated
        attempt.stderr_truncated = result.stderr_truncated
        output_data = self._task_output_data(run, result)
        self._commit_task_success(run, task, attempt, output_data)
        if result.clean_exit:
            self._finish_clean_exit(run)
        self._touch()
        return EventDisposition.APPLIED

    def _commit_task_success(self, run: CoordinatorRun, task: TaskRecord,
                             attempt: AttemptRecord,
                             output_data: tuple[p.DataReference, ...]) -> None:
        """Commit a validated result exactly once and advance existing DAG readiness.

        Worker computation success is only evidence.  This is the coordinator's
        authoritative commit boundary after result and output representation
        validation have already succeeded.
        """
        run.unlock_after_commit(task.task_id)
        for data in output_data:
            self._locations.announce(
                run.plan, run.run_id, attempt.worker_id, attempt.worker_generation, data
            )
        task.status = TaskStatus.COMMITTED
        task.committed_attempt_id = attempt.identity.attempt_id
        task.current_attempt_id = None
        attempt.status = AttemptStatus.COMMITTED
        self._retire_unavailable_contexts(run)
        if run.status == RunStatus.RUNNING and len(run.readiness.completed) == len(run.tasks):
            run.status = RunStatus.SUCCEEDED
            self._invalidate_pending_for_run(run.run_id)
            self._archive_terminal_run(run)
        elif run.status == RunStatus.CANCELLING:
            self._finish_cancellation_if_idle(run)

    def _finish_clean_exit(self, run: CoordinatorRun) -> None:
        """Commit a clean SystemExit(0) as an early successful program stop.

        The current task has already committed its real outputs. Remaining source
        tasks are recorded as cancelled/not-run and readiness is advanced through
        them only for terminal-run accounting; no downstream task is dispatched.
        """
        self._invalidate_pending_for_run(run.run_id)
        for task in run.tasks.values():
            if task.current_attempt_id is not None:
                attempt = run.attempts[task.current_attempt_id]
                if attempt.status.active:
                    attempt.status = AttemptStatus.CANCELLED
                task.current_attempt_id = None
        dag = run.plan._as_dag()
        for task_id in dag.topological_order():
            if task_id in run.readiness.completed:
                continue
            task = run.tasks[task_id]
            if task.status != TaskStatus.COMMITTED:
                task.status = TaskStatus.CANCELLED
                task.failure = FailureInfo(FailureKind.EXECUTION_ERROR, "not run: program exited")
            # Mark only after all predecessors have been marked, preserving the
            # ReadinessState contract while preventing scheduler exposure.
            run.readiness.mark_completed(task_id)
        run.status = RunStatus.SUCCEEDED
        self._archive_terminal_run(run)

    def _task_output_data(self, run: CoordinatorRun, result: TaskSuccess) -> tuple[p.DataReference, ...]:
        """Infer only representations TaskSuccess can safely certify itself.

        Immutable isolated results reside on the reporting worker. Shared-reference
        snapshots require an explicit ObjectAvailable claim because TaskSuccess is
        only a logical acknowledgement and does not certify snapshot preparation.
        """
        manifest = run.plan.task_index[result.attempt.task_id]
        reported = set(result.output_ids)
        refs: list[p.DataReference] = []
        for requirement in manifest.outputs:
            if requirement.id not in reported or requirement.kind != ValueKind.IMMUTABLE:
                continue
            data = p.DataReference(run.plan.id, run.run_id, requirement.id, DataForm.IMMUTABLE_VALUE)
            self._locations.validate_reference(run.plan, run.run_id, data)
            refs.append(data)
        return tuple(refs)

    def _handle_task_failed(self, worker: WorkerRecord, message: p.TaskFailed) -> EventDisposition:
        result = message.result
        resolved = self._attempt_for(
            worker, result.attempt, message.correlation_id, terminal_observation=True
        )
        if isinstance(resolved, EventDisposition): return resolved
        run, task, attempt = resolved
        if attempt.status not in {AttemptStatus.RUNNING, AttemptStatus.CANCEL_REQUESTED}:
            raise InvalidTaskTransition(f"failure in {attempt.status.value}")
        if run.status == RunStatus.CANCELLING:
            attempt.status = AttemptStatus.FAILED
            attempt.failure = result.failure
            task.current_attempt_id = None
            task.status = TaskStatus.CANCELLED
            self._retire_unavailable_contexts(run)
            self._finish_cancellation_if_idle(run)
            self._touch()
            return EventDisposition.APPLIED
        if run.status != RunStatus.RUNNING:
            return EventDisposition.STALE
        run.plan.validate_result(result, expected_attempt=attempt.identity)
        attempt.stdout_tail = result.stdout_tail
        attempt.stderr_tail = result.stderr_tail
        attempt.stdout_truncated = result.stdout_truncated
        attempt.stderr_truncated = result.stderr_truncated
        manifest = run.plan.task_index[task.task_id]
        native_failure = manifest.mode in {ExecutionMode.SHARED_CONTEXT, ExecutionMode.NATIVE_REGION}
        retryable = result.failure.kind in self.retry_policy.retry_failure_kinds
        can_retry = (
            not native_failure and retryable
            and len(task.attempt_ids) < self.retry_policy.max_attempts_per_task
        )

        coordinator_failure = None
        staged_failure = None
        if native_failure:
            coordinator_failure = CoordinatorFailure(
                CoordinatorFailureCode.NATIVE_STATE_UNCERTAIN,
                f"{manifest.mode.value} task {task.task_id} failed after execution began; replay is unsafe",
            )
            staged_failure = self._stage_run_failure(
                run, result.failure, coordinator_failure,
                terminal_attempt_ids=frozenset({attempt.identity.attempt_id}),
            )
        elif not can_retry:
            staged_failure = self._stage_run_failure(
                run, result.failure, terminal_attempt_ids=frozenset({attempt.identity.attempt_id}),
            )

        attempt.status = AttemptStatus.FAILED
        attempt.failure = result.failure
        task.current_attempt_id = None
        if staged_failure is not None:
            task.status = TaskStatus.FAILED
            task.failure = result.failure
            self._apply_run_failure(run, result.failure, coordinator_failure, staged_failure)
        else:
            self._retry_or_fail(run, task, retryable, result.failure)
        self._touch()
        return EventDisposition.APPLIED

    def _retry_or_fail(self, run: CoordinatorRun, task: TaskRecord,
                       retryable: bool, failure: FailureInfo) -> None:
        if (retryable and len(task.attempt_ids) < self.retry_policy.max_attempts_per_task
                and run.status == RunStatus.RUNNING):
            task.failure = failure
            task.status = TaskStatus.READY
            if task.ready_sequence is None:
                task.ready_sequence = run.next_ready_sequence
                run.next_ready_sequence += 1
            return
        staged = self._stage_run_failure(run, failure)
        task.failure = failure
        task.status = TaskStatus.FAILED
        self._apply_run_failure(run, failure, None, staged)

    def _build_context_releases(
            self, run: CoordinatorRun, reason: str
            ) -> tuple[tuple[str, WorkerRecord, p.ReleaseContext], ...]:
        """Stage physical retirement for every still-authoritative run context."""
        staged: list[tuple[str, WorkerRecord, p.ReleaseContext]] = []
        for context_id, context in sorted(run.contexts.items()):
            if context_id in run.unavailable_context_ids:
                continue
            worker = self._workers.get(context.worker_id)
            if worker is None or not worker.active:
                continue
            message = p.ReleaseContext(
                worker_id=context.worker_id, plan_id=run.plan.id, run_id=run.run_id,
                context_id=context_id, reason=reason, message_id=self._new("message"),
            )
            staged.append((context_id, worker, message))
        return tuple(staged)

    def _stage_run_failure(
            self, run: CoordinatorRun, failure: FailureInfo,
            coordinator_failure: CoordinatorFailure | None = None, *,
            terminal_attempt_ids: frozenset[str] = frozenset(),
            ) -> _StagedRunFailure:
        """Resolve fallible failure-cleanup material before authoritative mutation."""
        if run.status.terminal:
            return _StagedRunFailure(None, (), (), ())
        orphan_candidates: list[AttemptRecord] = []
        for task in run.tasks.values():
            if task.current_attempt_id is None or task.current_attempt_id in terminal_attempt_ids:
                continue
            attempt = run.attempts[task.current_attempt_id]
            if attempt.status.active and attempt.status != AttemptStatus.WAITING_TRANSFER:
                orphan_candidates.append(attempt)
        orphaned_at = self._now(None) if orphan_candidates else None

        cancellations: list[tuple[str, WorkerRecord, p.CancelTask]] = []
        counts: dict[str, int] = {}
        for attempt in orphan_candidates:
            if attempt.cancel_message_id is not None:
                continue
            worker = self._workers.get(attempt.worker_id)
            if (worker is None or not worker.active
                    or worker.handle.generation != attempt.worker_generation):
                continue
            message = p.CancelTask(
                worker_id=attempt.worker_id, attempt=attempt.identity,
                reason="run failed", message_id=self._new("message"),
            )
            cancellations.append((attempt.identity.attempt_id, worker, message))
            counts[attempt.worker_id] = counts.get(attempt.worker_id, 0) + 1
        context_releases = self._build_context_releases(run, "run failed")
        for _, worker, _ in context_releases:
            wid = worker.handle.worker_id
            counts[wid] = counts.get(wid, 0) + 1
        transfer_cancellations: list[tuple[str, WorkerRecord, p.CancelTransfer]] = []
        for record in self._transfers.values():
            if (record.status.terminal or record.identity.data.run_id != run.run_id
                    or record.identity.data.plan_id != run.plan.id):
                continue
            transfer_cancellations.extend(
                self._build_transfer_cancellations(record, "run failed")
            )
        self._preflight_transfer_cancellations(
            transfer_cancellations, extra_counts=counts
        )
        return _StagedRunFailure(
            orphaned_at, tuple(cancellations), tuple(transfer_cancellations),
            tuple(context_releases)
        )

    def _apply_run_failure(
            self, run: CoordinatorRun, failure: FailureInfo,
            coordinator_failure: CoordinatorFailure | None, staged: _StagedRunFailure
            ) -> None:
        if run.status.terminal:
            return
        cancellation_by_attempt = {aid: (worker, message) for aid, worker, message in staged.cancellations}
        transfer_cancellation_by_key: dict[
            tuple[str, str], list[tuple[str, WorkerRecord, p.CancelTransfer]]
        ] = {}
        for participant, worker, message in staged.transfer_cancellations:
            key = (message.transfer.transfer_id, message.transfer.transfer_attempt_id)
            transfer_cancellation_by_key.setdefault(key, []).append(
                (participant, worker, message)
            )
        run.status = RunStatus.FAILED
        run.failure = coordinator_failure
        run.last_decision = None
        self._invalidate_pending_for_run(run.run_id)
        for task in run.tasks.values():
            if task.current_attempt_id is not None:
                attempt = run.attempts[task.current_attempt_id]
                if attempt.status == AttemptStatus.WAITING_TRANSFER:
                    self._abandon_attempt_transfers(run, attempt, "run failed")
                    attempt.status = AttemptStatus.STALE
                elif attempt.status.active:
                    attempt.status = AttemptStatus.ORPHANED
                    if staged.orphaned_at is None:
                        raise AssertionError("staged run failure lacks orphan timestamp")
                    attempt.phase_since = staged.orphaned_at
                    prepared = cancellation_by_attempt.get(attempt.identity.attempt_id)
                    if prepared is not None:
                        worker, message = prepared
                        attempt.cancel_message_id = message.message_id
                        self._enqueue_message(worker, message)
                task.current_attempt_id = None
            if task.status != TaskStatus.COMMITTED:
                task.status = TaskStatus.FAILED
                # F22: a task that never had an attempt did not itself fail. Keep
                # causal diagnostics distinct from the originating failure.
                if not task.attempt_ids:
                    task.failure = FailureInfo(
                        FailureKind.INPUT_UNAVAILABLE, "not run: upstream failure"
                    )
                else:
                    task.failure = failure
        # Task cancellation commands were enqueued first.  Context retirement then
        # safely cleans idle contexts and follows cancellation for active contexts.
        for _, worker, message in staged.context_releases:
            self._enqueue_message(worker, message)
        # Transfers of attempts abandoned above are already FAILED, but their
        # participants still hold state: their staged cleanup is sent all the same.
        for key, transfer in self._transfers.items():
            if (transfer.identity.data.run_id != run.run_id
                    or transfer.identity.data.plan_id != run.plan.id):
                continue
            staged_cleanup = transfer_cancellation_by_key.get(key)
            if transfer.status.terminal and staged_cleanup is None:
                continue
            if not transfer.status.terminal:
                transfer.status = TransferStatus.FAILED
                transfer.failure_detail = "run failed"
            self._apply_transfer_cancellations(transfer, staged_cleanup or ())
        self._archive_terminal_run(run)

    def _fail_run(self, run: CoordinatorRun, failure: FailureInfo,
                  coordinator_failure: CoordinatorFailure | None = None, *,
                  staged: _StagedRunFailure | None = None) -> None:
        if run.status.terminal:
            return
        prepared = staged or self._stage_run_failure(run, failure, coordinator_failure)
        self._apply_run_failure(run, failure, coordinator_failure, prepared)

    def fail_run_unpreparable(self, run_id: str, detail: str) -> None:
        """Terminally fail a run when no current worker generation can prepare it."""
        run = self._run(run_id)
        if run.status.terminal:
            return
        failure = FailureInfo(FailureKind.EXECUTION_ERROR, f"PROGRAM_UNPREPARABLE: {detail}")
        # Also record a run-level cause: without it `dpr status` and the archived
        # history show a bare "failed" with no reason, which is the most likely
        # production failure (a package no worker can install).
        self._fail_run(run, failure, CoordinatorFailure(
            CoordinatorFailureCode.PLACEMENT_CONSTRAINT_LOST,
            f"no eligible worker could prepare the program: {detail}",
        ))
        self._touch()

    def fail_run_resource_limit(self, run_id: str, detail: str) -> None:
        run = self._run(run_id)
        if run.status.terminal:
            return
        self._fail_run(run, FailureInfo(FailureKind.EXECUTION_ERROR, detail),
                       CoordinatorFailure(CoordinatorFailureCode.PLACEMENT_CONSTRAINT_LOST, detail))
        self._touch()

    # ---------- cancellation ----------
    def cancel_run(self, run_id: str, *, reason: str = "run cancelled") -> tuple[p.CancelTask, ...]:
        run = self._run(run_id)
        if run.status == RunStatus.CANCELLED:
            return ()
        if run.status.terminal:
            raise InvalidRunTransition(f"cannot cancel {run.status.value} run")

        # Stage every outbound cancellation command before changing authoritative
        # run/task/attempt state. Validation/ID/backpressure failure is atomic.
        staged: dict[str, p.CancelTask] = {}
        cancel_counts: dict[str, int] = {}
        for task in run.tasks.values():
            if task.current_attempt_id is None:
                continue
            attempt = run.attempts[task.current_attempt_id]
            if (attempt.status.active and attempt.status != AttemptStatus.WAITING_TRANSFER
                    and attempt.cancel_message_id is None):
                message_id = self._new("message")
                message = p.CancelTask(
                    worker_id=attempt.worker_id, attempt=attempt.identity,
                    reason=reason, message_id=message_id,
                )
                staged[attempt.identity.attempt_id] = message
                cancel_counts[attempt.worker_id] = cancel_counts.get(attempt.worker_id, 0) + 1
        staged_context_releases = self._build_context_releases(run, reason)
        for _, worker, _ in staged_context_releases:
            wid = worker.handle.worker_id
            cancel_counts[wid] = cancel_counts.get(wid, 0) + 1
        staged_transfer_cancellations: list[
            tuple[str, WorkerRecord, p.CancelTransfer]
        ] = []
        for record in self._transfers.values():
            if (record.status.terminal or record.identity.data.run_id != run.run_id
                    or record.identity.data.plan_id != run.plan.id):
                continue
            staged_transfer_cancellations.extend(
                self._build_transfer_cancellations(record, reason)
            )
        self._preflight_transfer_cancellations(
            staged_transfer_cancellations, extra_counts=cancel_counts
        )
        # The injected clock is fallible by contract. Resolve it before any
        # authoritative cancellation mutation so a local provider failure is
        # retryable and leaves the run unchanged.
        now = self._now(None)

        run.status = RunStatus.CANCELLING
        self._invalidate_pending_for_run(run.run_id)
        messages: list[p.CancelTask] = []
        for task in run.tasks.values():
            if task.current_attempt_id is None:
                if task.status != TaskStatus.COMMITTED:
                    task.status = TaskStatus.CANCELLED
                continue
            attempt = run.attempts[task.current_attempt_id]
            if not attempt.status.active:
                continue
            if attempt.status == AttemptStatus.WAITING_TRANSFER:
                self._abandon_attempt_transfers(run, attempt, "run cancelled")
                attempt.status = AttemptStatus.CANCELLED
                task.current_attempt_id = None
                task.status = TaskStatus.CANCELLED
                continue
            message = staged.get(attempt.identity.attempt_id)
            if message is not None:
                attempt.cancel_message_id = message.message_id
                attempt.status = AttemptStatus.CANCEL_REQUESTED
                attempt.phase_since = now
                task.status = TaskStatus.CANCELLING
                self._enqueue_message(self._worker(attempt.worker_id), message)
                messages.append(message)
        transfer_cancellation_by_key: dict[
            tuple[str, str], list[tuple[str, WorkerRecord, p.CancelTransfer]]
        ] = {}
        for participant, worker, message in staged_transfer_cancellations:
            key = (message.transfer.transfer_id, message.transfer.transfer_attempt_id)
            transfer_cancellation_by_key.setdefault(key, []).append(
                (participant, worker, message)
            )
        # As for a failed run: transfers of abandoned attempts are FAILED already,
        # and their staged participant cleanup must still go out.
        for key, record in self._transfers.items():
            if (record.identity.data.run_id != run.run_id
                    or record.identity.data.plan_id != run.plan.id):
                continue
            staged_cleanup = transfer_cancellation_by_key.get(key)
            if record.status.terminal and staged_cleanup is None:
                continue
            if not record.status.terminal:
                record.status = TransferStatus.FAILED
                record.failure_detail = "run cancelled"
            self._apply_transfer_cancellations(record, staged_cleanup or ())
        for _, worker, message in staged_context_releases:
            self._enqueue_message(worker, message)
        self._finish_cancellation_if_idle(run)
        self._touch()
        self._release_queued_transfers(now)
        return tuple(messages)

    def _handle_cancellation_result(self, worker: WorkerRecord,
                                    message: p.TaskCancellationResult) -> EventDisposition:
        run = self._run(message.attempt.run_id)
        task = run.tasks.get(message.attempt.task_id)
        attempt = run.attempts.get(message.attempt.attempt_id)
        if task is None or attempt is None or attempt.identity != message.attempt:
            raise UnknownAttempt(message.attempt.attempt_id)
        if attempt.worker_id != worker.handle.worker_id or attempt.worker_generation != worker.handle.generation:
            raise InvalidWorkerMessage("cancellation worker/session mismatch")
        if message.correlation_id != attempt.cancel_message_id:
            raise InvalidWorkerMessage("cancellation correlation mismatch")
        if attempt.status in {AttemptStatus.CANCELLED, AttemptStatus.LOST}:
            return EventDisposition.DUPLICATE
        if attempt.status == AttemptStatus.ORPHANED:
            if message.outcome in {p.CancellationOutcome.CANCELLED, p.CancellationOutcome.NOT_FOUND}:
                attempt.status = AttemptStatus.CANCELLED
                self._retire_unavailable_contexts(run)
                if run.status.terminal:
                    self._archive_terminal_run(run)
                self._touch()
            return EventDisposition.STALE
        if task.current_attempt_id != attempt.identity.attempt_id:
            return EventDisposition.STALE
        if message.outcome in {p.CancellationOutcome.CANCELLED, p.CancellationOutcome.NOT_FOUND}:
            attempt.status = AttemptStatus.CANCELLED
            task.current_attempt_id = None
            task.status = TaskStatus.CANCELLED
            self._retire_unavailable_contexts(run)
            self._finish_cancellation_if_idle(run)
            self._touch()
            return EventDisposition.APPLIED
        # TOO_LATE/UNSUPPORTED leave the attempt active; a real result may still arrive.
        return EventDisposition.APPLIED

    def _finish_cancellation_if_idle(self, run: CoordinatorRun) -> None:
        if run.status != RunStatus.CANCELLING:
            return
        if not any(a.status.active for a in run.attempts.values()):
            run.status = RunStatus.CANCELLED
            for task in run.tasks.values():
                if task.status not in {TaskStatus.COMMITTED, TaskStatus.FAILED}:
                    task.status = TaskStatus.CANCELLED
                    task.current_attempt_id = None
            self._archive_terminal_run(run)

    def _abandon_attempt_transfers(self, run: CoordinatorRun, attempt: AttemptRecord, detail: str) -> None:
        for key in tuple(attempt.pending_transfers):
            transfer = self._transfers.get(key)
            if transfer is not None and not transfer.status.terminal:
                transfer.status = TransferStatus.FAILED
                transfer.failure_detail = detail
        attempt.pending_transfers.clear()

    def _current_dispatch_requirements_hold(
            self, run: CoordinatorRun, attempt: AttemptRecord,
            *, additionally_available: frozenset[p.DataReference] = frozenset()) -> bool:
        worker = self._workers.get(attempt.worker_id)
        if (worker is None or not worker.active or worker.compute_quarantined
                or worker.handle.generation != attempt.worker_generation):
            return False
        state = worker.reported
        manifest = run.plan.task_index[attempt.identity.task_id]
        if (not state.online or not state.accepting_work
                or run.plan.program.environment_id not in state.environment_ids
                or run.plan.program.id not in state.prepared_program_ids
                or manifest.mode not in state.supported_modes):
            return False
        if attempt.context_id is not None:
            context = run.contexts.get(attempt.context_id)
            if (context is None or context.worker_id != attempt.worker_id
                    or attempt.identity.task_id not in context.prepared_task_ids):
                return False
        return all(
            ref in additionally_available
            or attempt.worker_id in self._available_replica_workers(ref)
            for ref in self._required_data_refs(run, attempt.identity.task_id)
        )

    def _stage_dispatch_after_final_transfer(
            self, run: CoordinatorRun, attempt: AttemptRecord,
            record: TransferRecord) -> tuple[WorkerRecord, p.TaskDispatch] | None:
        """Prebuild the final TaskDispatch before consuming completion state.

        This is used only when ``record`` is the attempt's final pending input.
        ID generation and protocol construction are fallible, so staging them
        before transfer/location mutation prevents a completed transfer from
        stranding a WAITING_TRANSFER attempt with no continuation.
        """
        key = (record.identity.transfer_id, record.identity.transfer_attempt_id)
        if attempt.pending_transfers != {key}:
            return None
        if not self._current_dispatch_requirements_hold(
                run, attempt, additionally_available=frozenset({record.identity.data})):
            return None
        worker = self._worker(attempt.worker_id)
        try:
            self._ensure_outbox_capacity(worker)
        except OutboundBackpressure:
            # Preserve existing behavior: completion is still authoritative; the
            # normal post-completion path will quarantine/reconcile the worker.
            return None
        manifest = run.plan.task_index[attempt.identity.task_id]
        message_id = self._new("message")
        dispatch = p.TaskDispatch(
            worker_id=attempt.worker_id, attempt=attempt.identity,
            program_id=run.plan.program.id, mode=manifest.mode,
            context_id=attempt.context_id, message_id=message_id,
        )
        return worker, dispatch

    def _dispatch_waiting_attempt(self, run: CoordinatorRun, attempt: AttemptRecord) -> None:
        task = run.tasks[attempt.identity.task_id]
        if task.current_attempt_id != attempt.identity.attempt_id or attempt.status != AttemptStatus.WAITING_TRANSFER:
            return
        if attempt.pending_transfers:
            return
        if not self._current_dispatch_requirements_hold(run, attempt):
            failure = FailureInfo(
                FailureKind.INPUT_UNAVAILABLE,
                "worker/task requirements changed while waiting for input transfer",
            )
            can_retry = (
                len(task.attempt_ids) < self.retry_policy.max_attempts_per_task
                and run.status == RunStatus.RUNNING
            )
            staged_failure = None
            if not can_retry:
                staged_failure = self._stage_run_failure(
                    run, failure,
                    terminal_attempt_ids=frozenset({attempt.identity.attempt_id}),
                )
            self._abandon_attempt_transfers(run, attempt, failure.message)
            attempt.status = AttemptStatus.FAILED
            attempt.failure = failure
            task.current_attempt_id = None
            if staged_failure is None:
                self._retry_or_fail(run, task, True, failure)
            else:
                task.status = TaskStatus.FAILED
                task.failure = failure
                self._apply_run_failure(run, failure, None, staged_failure)
            return
        worker = self._worker(attempt.worker_id)
        try:
            self._ensure_outbox_capacity(worker)
        except OutboundBackpressure:
            self._mark_worker_compute_unavailable(
                worker, "outbound queue full before task dispatch"
            )
            return
        manifest = run.plan.task_index[attempt.identity.task_id]
        message_id = self._new("message")
        dispatch = p.TaskDispatch(
            worker_id=attempt.worker_id, attempt=attempt.identity, program_id=run.plan.program.id,
            mode=manifest.mode, context_id=attempt.context_id, message_id=message_id,
        )
        attempt.dispatch_message_id = message_id
        self._mark_attempt_dispatched(task, attempt, worker.last_seen)
        self._enqueue_message(worker, dispatch)

    def _stage_consumer_transfer_failure(
            self, record: TransferRecord, detail: str) -> _StagedRunFailure | None:
        if record.consumer_run_id is None or record.consumer_attempt_id is None:
            return None
        run = self._runs.get(record.consumer_run_id)
        if run is None or run.status != RunStatus.RUNNING:
            return None
        attempt = run.attempts.get(record.consumer_attempt_id)
        if attempt is None or attempt.status != AttemptStatus.WAITING_TRANSFER:
            return None
        task = run.tasks[attempt.identity.task_id]
        if task.current_attempt_id != attempt.identity.attempt_id:
            return None
        if len(task.attempt_ids) < self.retry_policy.max_attempts_per_task:
            return None
        failure = FailureInfo(FailureKind.INPUT_UNAVAILABLE, detail)
        return self._stage_run_failure(
            run, failure,
            terminal_attempt_ids=frozenset({attempt.identity.attempt_id}),
        )

    def _stage_consumer_transfer_cleanup(
            self, record: TransferRecord, detail: str, *,
            staged_failure: _StagedRunFailure | None,
            skip_source: bool = False, skip_destination: bool = False,
            preflight: bool = True,
            ) -> tuple[
                tuple[tuple[str, str], str, WorkerRecord, p.CancelTransfer], ...
            ]:
        """Stage physical cleanup for a failed consumer transfer attempt.

        If the failure is already going to terminate the run, _stage_run_failure
        has staged cancellation for every live transfer and must remain the single
        source of cleanup commands. Otherwise all pending input transfers for the
        same attempt are abandoned together and need explicit participant cleanup.
        """
        if staged_failure is not None:
            return ()
        if record.consumer_run_id is not None and record.consumer_attempt_id is not None:
            run = self._runs.get(record.consumer_run_id)
            attempt = None if run is None else run.attempts.get(record.consumer_attempt_id)
            if attempt is not None and attempt.status == AttemptStatus.WAITING_TRANSFER:
                key = (record.identity.transfer_id, record.identity.transfer_attempt_id)
                ignored: set[str] = set()
                if skip_source:
                    ignored.add("source")
                if skip_destination:
                    ignored.add("destination")
                return self._stage_attempt_transfer_cancellations(
                    attempt, detail,
                    skip={key: frozenset(ignored)} if ignored else None,
                    preflight=preflight,
                )
        staged = self._build_transfer_cancellations(
            record, detail,
            skip_source=skip_source, skip_destination=skip_destination,
        )
        if preflight:
            self._preflight_transfer_cancellations(staged)
        key = (record.identity.transfer_id, record.identity.transfer_attempt_id)
        return tuple((key, participant, worker, message)
                     for participant, worker, message in staged)

    def _consumer_transfer_failed(
            self, record: TransferRecord, detail: str, *,
            staged_failure: _StagedRunFailure | None = None) -> None:
        if record.consumer_run_id is None or record.consumer_attempt_id is None:
            return
        run = self._runs.get(record.consumer_run_id)
        if run is None or run.status != RunStatus.RUNNING:
            return
        attempt = run.attempts.get(record.consumer_attempt_id)
        if attempt is None or attempt.status != AttemptStatus.WAITING_TRANSFER:
            return
        task = run.tasks[attempt.identity.task_id]
        if task.current_attempt_id != attempt.identity.attempt_id:
            return
        failure = FailureInfo(FailureKind.INPUT_UNAVAILABLE, detail)
        can_retry = len(task.attempt_ids) < self.retry_policy.max_attempts_per_task
        prepared = staged_failure
        if not can_retry and prepared is None:
            prepared = self._stage_run_failure(
                run, failure,
                terminal_attempt_ids=frozenset({attempt.identity.attempt_id}),
            )
        self._abandon_attempt_transfers(run, attempt, detail)
        attempt.status = AttemptStatus.FAILED
        attempt.failure = failure
        task.current_attempt_id = None
        if can_retry:
            self._retry_or_fail(run, task, True, failure)
        else:
            assert prepared is not None
            task.status = TaskStatus.FAILED
            task.failure = failure
            self._apply_run_failure(run, failure, None, prepared)

    def _consumer_transfer_completed(
            self, record: TransferRecord,
            staged_dispatch: tuple[WorkerRecord, p.TaskDispatch] | None = None) -> None:
        if record.consumer_run_id is None or record.consumer_attempt_id is None:
            return
        run = self._runs.get(record.consumer_run_id)
        if run is None or run.status != RunStatus.RUNNING:
            return
        attempt = run.attempts.get(record.consumer_attempt_id)
        if attempt is None or attempt.status != AttemptStatus.WAITING_TRANSFER:
            return
        key = (record.identity.transfer_id, record.identity.transfer_attempt_id)
        attempt.pending_transfers.discard(key)
        if attempt.pending_transfers:
            return
        if staged_dispatch is not None:
            worker, dispatch = staged_dispatch
            task = run.tasks[attempt.identity.task_id]
            attempt.dispatch_message_id = dispatch.message_id
            self._mark_attempt_dispatched(task, attempt, worker.last_seen)
            self._enqueue_message(worker, dispatch)
            return
        self._dispatch_waiting_attempt(run, attempt)

    # ---------- physical object release ----------
    def _stage_object_release(self, worker: WorkerRecord, data: p.DataReference, reason: str) -> bool:
        key = (worker.handle.worker_id, worker.handle.generation, data)
        if key in self._pending_object_releases:
            return False
        self._ensure_outbox_capacity(worker)
        message_id = self._new("message")
        command = p.ReleaseObject(
            worker_id=worker.handle.worker_id, data=data, reason=reason, message_id=message_id,
        )
        self._pending_object_releases[key] = message_id
        self._enqueue_message(worker, command)
        return True

    def request_terminal_object_releases(self) -> int:
        """Stage deletion of every live replica owned only by terminal runs.

        The service calls this from maintenance so terminal transitions themselves
        never fail merely because a worker socket queue is temporarily full.
        """
        staged = 0
        for run in tuple(self._runs.values()):
            if not run.status.terminal:
                continue
            for location in self._locations.locations(run.plan.id, run.run_id):
                data = p.DataReference(
                    run.plan.id, run.run_id, location.value_id, location.form, location.object_state_id
                )
                for replica in location.replicas:
                    worker = self._workers.get(replica.worker_id)
                    if worker is None or not worker.active:
                        continue
                    try:
                        if self._stage_object_release(
                            worker, data, f"terminal run {run.run_id} data release"
                        ):
                            staged += 1
                    except OutboundBackpressure:
                        continue
        if staged:
            self._touch()
        return staged

    def _handle_object_released(self, worker: WorkerRecord, message: p.ObjectReleased) -> EventDisposition:
        run = self._data_run(message.data)
        key = (worker.handle.worker_id, worker.handle.generation, message.data)
        expected = self._pending_object_releases.get(key)
        if expected is None:
            if not self._locations.has(message.data, worker.handle.worker_id, worker.handle.generation):
                return EventDisposition.DUPLICATE
            raise InvalidWorkerMessage("unsolicited object release acknowledgement")
        if message.correlation_id != expected:
            raise InvalidWorkerMessage("object release correlation mismatch")
        self._pending_object_releases.pop(key, None)
        self._locations.unavailable(
            run.plan, run.run_id, worker.handle.worker_id, worker.handle.generation, message.data
        )
        # A failed transfer can leave a destination-side physical copy that was
        # intentionally never certified as an available replica. Its cleanup is
        # proven only by this returning ObjectReleased acknowledgement.
        for record in self._transfers.values():
            if (record.status == TransferStatus.FAILED
                    and record.identity.data == message.data
                    and record.identity.destination_worker_id == worker.handle.worker_id
                    and record.destination_generation == worker.handle.generation):
                record.destination_cleanup_confirmed = True
        self._touch()
        return EventDisposition.APPLIED

    # ---------- locations ----------
    def _data_run(self, data: p.DataReference) -> CoordinatorRun:
        run = self._run(data.run_id)
        if data.plan_id != run.plan.id:
            raise InvalidDataLocation("data plan mismatch")
        return run

    def _committed_producer_session(self, run: CoordinatorRun, value_id: str) -> tuple[str, int] | None:
        """Return the exact worker generation that committed a logical value."""
        value = run.plan.value_index.get(value_id)
        producer = None if value is None else value.producer
        if producer is None:
            return None
        task = run.tasks.get(producer)
        if task is None or task.committed_attempt_id is None:
            return None
        attempt = run.attempts.get(task.committed_attempt_id)
        if attempt is None:
            return None
        return (attempt.worker_id, attempt.worker_generation)

    def _object_claim_has_provenance(self, run: CoordinatorRun, worker: WorkerRecord,
                                     data: p.DataReference) -> bool:
        # Existing certified replicas may refine size metadata idempotently. Old
        # session replicas are removed on worker loss before a replacement
        # generation is installed.
        if self._locations.has(data, worker.handle.worker_id, worker.handle.generation):
            return True
        reporter = (worker.handle.worker_id, worker.handle.generation)
        if data.form == DataForm.IMMUTABLE_VALUE:
            return self._committed_producer_session(run, data.value_id) == reporter
        if data.object_state_id is None:
            if self._committed_producer_session(run, data.value_id) == reporter:
                return True
        else:
            if self._committed_producer_session(run, data.object_state_id) == reporter:
                return True
        # The current protocol has no separate coordinator-issued "snapshot now"
        # authorization. Merely having a prepared native context is not proof of
        # possession: ContextPrepared explicitly does not certify readiness/state.
        # Future snapshot orchestration can add another provenance source here.
        return False

    def _handle_object_available(self, worker: WorkerRecord, message: p.ObjectAvailable) -> EventDisposition:
        run = self._data_run(message.data)
        if run.status.terminal:
            return EventDisposition.STALE
        if not self._object_claim_has_provenance(run, worker, message.data):
            raise InvalidDataLocation(
                "worker has no coordinator-certified provenance for this data representation"
            )
        already = self._locations.has(
            message.data, worker.handle.worker_id, worker.handle.generation
        )
        self._locations.announce(
            run.plan, run.run_id, worker.handle.worker_id, worker.handle.generation,
            message.data, message.size_bytes,
        )
        if not already:
            self._touch()
        return EventDisposition.DUPLICATE if already else EventDisposition.APPLIED

    def _handle_object_unavailable(self, worker: WorkerRecord, message: p.ObjectUnavailable) -> EventDisposition:
        run = self._data_run(message.data)
        if run.status.terminal:
            return EventDisposition.STALE
        existed = self._locations.has(
            message.data, worker.handle.worker_id, worker.handle.generation
        )

        staged_failure = None
        failure = None
        coordinator_failure = None
        if existed and run.status == RunStatus.RUNNING:
            remaining = self._available_replica_workers(message.data) - {worker.handle.worker_id}
            affected = self._unfinished_tasks_requiring_data(run, message.data)
            if not remaining and affected:
                detail = (
                    f"last valid replica of {message.data.value_id}"
                    f"{('/' + message.data.object_state_id) if message.data.object_state_id else ''} "
                    f"lost: worker reported data unavailable; required by {', '.join(affected)}"
                )
                failure = FailureInfo(FailureKind.INPUT_UNAVAILABLE, detail)
                coordinator_failure = CoordinatorFailure(
                    CoordinatorFailureCode.DATA_LOST, detail
                )
                staged_failure = self._stage_run_failure(
                    run, failure, coordinator_failure
                )

        lost_last = self._locations.unavailable(
            run.plan, run.run_id, worker.handle.worker_id, worker.handle.generation, message.data
        )
        if lost_last:
            if staged_failure is not None:
                assert failure is not None and coordinator_failure is not None
                self._apply_run_failure(
                    run, failure, coordinator_failure, staged_failure
                )
            else:
                self._fail_if_required_data_lost(
                    run, message.data, "worker reported data unavailable"
                )
        if existed:
            self._touch()
        return EventDisposition.APPLIED if existed else EventDisposition.DUPLICATE

    def _unfinished_tasks_requiring_data(self, run: CoordinatorRun,
                                         data: p.DataReference) -> tuple[str, ...]:
        affected: list[str] = []
        for task_id, task in run.tasks.items():
            if task.status in {TaskStatus.COMMITTED, TaskStatus.FAILED, TaskStatus.CANCELLED}:
                continue
            try:
                required = self._required_data_refs(run, task_id)
            except PlacementRejected:
                continue
            if data in required:
                affected.append(task_id)
        return tuple(sorted(affected))

    def _fail_if_required_data_lost(
            self, run: CoordinatorRun, data: p.DataReference, reason: str, *,
            staged_failure: _StagedRunFailure | None = None) -> None:
        if run.status != RunStatus.RUNNING or self._available_replica_workers(data):
            return
        affected = self._unfinished_tasks_requiring_data(run, data)
        if not affected:
            return
        detail = (
            f"last valid replica of {data.value_id}"
            f"{('/' + data.object_state_id) if data.object_state_id else ''} lost: {reason}; "
            f"required by {', '.join(affected)}"
        )
        self._fail_run(
            run,
            FailureInfo(FailureKind.INPUT_UNAVAILABLE, detail),
            CoordinatorFailure(CoordinatorFailureCode.DATA_LOST, detail),
            staged=staged_failure,
        )

    def _available_replica_workers(self, data: p.DataReference) -> frozenset[str]:
        return frozenset(
            worker_id
            for worker_id in self._locations.worker_ids(data)
            if worker_id in self._workers
            and self._workers[worker_id].active
            and self._workers[worker_id].reported.online
            and not self._workers[worker_id].compute_quarantined
            and self._locations.has(
                data, worker_id, self._workers[worker_id].handle.generation
            )
        )

    # ---------- transfers ----------
    def _build_transfer_cancellations(
            self, record: TransferRecord, reason: str, *,
            skip_source: bool = False, skip_destination: bool = False,
            ) -> tuple[tuple[str, WorkerRecord, p.CancelTransfer], ...]:
        """Stage exact participant cleanup commands without mutating transfer state.

        A destination may own preparation resources as soon as PrepareReceive is
        issued. The source can only own transfer resources after TransferRequest
        exists.  Session generation is checked here so a reconnect never receives
        cleanup intended for the replaced physical session.
        """
        staged: list[tuple[str, WorkerRecord, p.CancelTransfer]] = []
        if (record.identity.transfer_id, record.identity.transfer_attempt_id) in self._queued_transfers:
            return ()

        if (not skip_source and record.source_request_id is not None
                and not record.source_cleanup_confirmed
                and not record.source_cancel_requested):
            worker = self._workers.get(record.identity.source_worker_id)
            if (worker is not None and worker.active
                    and worker.handle.generation == record.source_generation):
                staged.append((
                    "source", worker,
                    p.CancelTransfer(
                        worker_id=record.identity.source_worker_id,
                        transfer=record.identity,
                        reason=reason,
                        message_id=self._new("message"),
                    ),
                ))

        if (not skip_destination and not record.destination_cleanup_confirmed
                and not record.destination_cancel_requested):
            worker = self._workers.get(record.identity.destination_worker_id)
            if (worker is not None and worker.active
                    and worker.handle.generation == record.destination_generation):
                staged.append((
                    "destination", worker,
                    p.CancelTransfer(
                        worker_id=record.identity.destination_worker_id,
                        transfer=record.identity,
                        reason=reason,
                        message_id=self._new("message"),
                    ),
                ))
        return tuple(staged)

    def _preflight_transfer_cancellations(
            self, staged: Iterable[tuple[str, WorkerRecord, p.CancelTransfer]],
            *, extra_counts: dict[str, int] | None = None) -> dict[str, int]:
        counts = dict(extra_counts or {})
        for _, worker, _ in staged:
            worker_id = worker.handle.worker_id
            counts[worker_id] = counts.get(worker_id, 0) + 1
        for worker_id, count in counts.items():
            worker = self._workers.get(worker_id)
            if worker is not None:
                self._ensure_outbox_capacity(worker, count)
        return counts

    def _apply_transfer_cancellations(
            self, record: TransferRecord,
            staged: Iterable[tuple[str, WorkerRecord, p.CancelTransfer]]) -> None:
        for participant, worker, message in staged:
            if participant == "source":
                if record.source_cleanup_confirmed:
                    continue  # confirmed after staging, e.g. by the event being handled
                record.source_cancel_requested = True
            elif participant == "destination":
                if record.destination_cleanup_confirmed:
                    continue
                record.destination_cancel_requested = True
            else:  # pragma: no cover - internal invariant
                raise AssertionError(f"unknown transfer participant {participant!r}")
            self._enqueue_message(worker, message)

    def _stage_attempt_transfer_cancellations(
            self, attempt: AttemptRecord, reason: str, *,
            skip: dict[tuple[str, str], frozenset[str]] | None = None,
            preflight: bool = True,
            ) -> tuple[
                tuple[tuple[str, str], str, WorkerRecord, p.CancelTransfer], ...
            ]:
        staged: list[
            tuple[tuple[str, str], str, WorkerRecord, p.CancelTransfer]
        ] = []
        skip = skip or {}
        for key in tuple(attempt.pending_transfers):
            record = self._transfers.get(key)
            if record is None or record.status.terminal:
                continue
            ignored = skip.get(key, frozenset())
            for participant, worker, message in self._build_transfer_cancellations(
                    record, reason,
                    skip_source="source" in ignored,
                    skip_destination="destination" in ignored):
                staged.append((key, participant, worker, message))
        if preflight:
            self._preflight_transfer_cancellations(
                (participant, worker, message)
                for _, participant, worker, message in staged
            )
        return tuple(staged)

    def _apply_attempt_transfer_cancellations(
            self, staged: Iterable[
                tuple[tuple[str, str], str, WorkerRecord, p.CancelTransfer]
            ]) -> None:
        grouped: dict[
            tuple[str, str], list[tuple[str, WorkerRecord, p.CancelTransfer]]
        ] = {}
        for key, participant, worker, message in staged:
            grouped.setdefault(key, []).append((participant, worker, message))
        for key, messages in grouped.items():
            record = self._transfers.get(key)
            if record is not None:
                self._apply_transfer_cancellations(record, messages)

    def _transfer_record_count_for_run(self, run_id: str) -> int:
        return sum(1 for record in self._transfers.values() if record.identity.data.run_id == run_id)

    def _ensure_transfer_record_capacity(self, run_id: str, additional: int = 1) -> None:
        if additional < 0:
            raise ValueError("additional transfer count must be non-negative")
        if len(self._transfers) + additional > self.operation_limits.max_transfer_records_global:
            raise OperationalLimitExceeded("global transfer-record limit reached")
        if (self._transfer_record_count_for_run(run_id) + additional
                > self.operation_limits.max_transfer_records_per_run):
            raise OperationalLimitExceeded(f"transfer-record limit reached for run {run_id}")

    def start_transfer(self, transfer: p.TransferIdentity, *, size_bytes: int | None = None) -> p.PrepareReceive:
        if not isinstance(transfer, p.TransferIdentity):
            raise TypeError("TransferIdentity required")
        run = self._data_run(transfer.data)
        if run.status != RunStatus.RUNNING:
            raise InvalidTransferTransition("new transfers require a running run")
        self._worker(transfer.source_worker_id)
        destination = self._worker(transfer.destination_worker_id)
        if transfer.source_worker_id == transfer.destination_worker_id:
            raise InvalidTransferTransition("source and destination must differ")
        if transfer.source_worker_id not in self._available_replica_workers(transfer.data):
            raise InvalidTransferTransition("source does not hold requested data")
        active_attempt = self._active_transfer_attempt.get(transfer.transfer_id)
        if active_attempt is not None:
            previous = self._transfers[(transfer.transfer_id, active_attempt)]
            if not previous.status.terminal:
                raise InvalidTransferTransition("logical transfer already has active attempt")
            if previous.status == TransferStatus.COMPLETED:
                raise InvalidTransferTransition("logical transfer already completed")
            if active_attempt == transfer.transfer_attempt_id:
                raise InvalidTransferTransition("transfer attempt identity already used")
        key = (transfer.transfer_id, transfer.transfer_attempt_id)
        if key in self._transfers:
            raise InvalidTransferTransition("transfer attempt already exists")
        self._ensure_transfer_record_capacity(run.run_id)
        known_size = self._locations.size_bytes(transfer.data)
        if known_size is not None and size_bytes is not None and known_size != size_bytes:
            raise InvalidTransferTransition("expected transfer size conflicts with source representation")
        self._ensure_outbox_capacity(destination)
        message_id = self._new("message")
        source_handle = self._worker(transfer.source_worker_id).handle
        authorization = self._new("transfer-authorization")
        record = TransferRecord(
            transfer, TransferStatus.DESTINATION_PREPARING, message_id,
            source_generation=source_handle.generation,
            destination_generation=destination.handle.generation,
            source_session_id=source_handle.session_id,
            destination_session_id=destination.handle.session_id,
            authorization=authorization,
            size_bytes=size_bytes, phase_since=self._now(None),
        )
        # Construct/validate the wire command before mutating authoritative
        # transfer state. Protocol validation failure must leave no ghost attempt.
        message = p.PrepareReceive(
            transfer=transfer, size_bytes=size_bytes,
            source_session_id=record.source_session_id,
            destination_session_id=record.destination_session_id,
            authorization=record.authorization, message_id=message_id,
        )
        self._transfers[key] = record
        self._active_transfer_attempt[transfer.transfer_id] = transfer.transfer_attempt_id
        self._enqueue_message(destination, message)
        self._issued_transfers.add(key)
        self._touch()
        return message

    def _release_queued_transfers(self, now: float | None = None) -> None:
        """Send queued transfers whose two workers have room, oldest first.

        Called after every event that can finish a transfer.  Never raises: a
        transfer that cannot be sent yet simply stays queued, and one that
        ended while queued (run failed or cancelled, worker lost) is dropped
        here, with nothing to clean up on any worker.
        """
        for key in tuple(self._issued_transfers):
            record = self._transfers.get(key)
            if record is None or record.status.terminal:
                self._issued_transfers.discard(key)
        if not self._queued_transfers:
            return
        limit = self.operation_limits.max_transfers_per_worker
        sending: dict[str, int] = {}
        receiving: dict[str, int] = {}
        for key in self._issued_transfers:
            identity = self._transfers[key].identity
            sending[identity.source_worker_id] = sending.get(identity.source_worker_id, 0) + 1
            receiving[identity.destination_worker_id] = (
                receiving.get(identity.destination_worker_id, 0) + 1)
        current: float | None = None
        for key, prepare in tuple(self._queued_transfers.items()):
            record = self._transfers.get(key)
            if record is None or record.status.terminal:
                del self._queued_transfers[key]
                if record is not None:
                    record.destination_cleanup_confirmed = True
                continue
            source = record.identity.source_worker_id
            target = record.identity.destination_worker_id
            if sending.get(source, 0) >= limit or receiving.get(target, 0) >= limit:
                continue
            destination = self._workers.get(target)
            if (destination is None or not destination.active
                    or destination.handle.generation != record.destination_generation
                    or len(destination.outbox) >= self.outbox_limit):
                continue  # worker loss fails it; a full outbox drains
            if current is None:
                try:
                    current = self._now(now)
                except Exception:
                    return
            del self._queued_transfers[key]
            self._issued_transfers.add(key)
            record.phase_since = current
            self._enqueue_message(destination, prepare)
            sending[source] = sending.get(source, 0) + 1
            receiving[target] = receiving.get(target, 0) + 1
        if current is not None:
            self._touch()

    def _transfer_for(
            self, transfer: p.TransferIdentity, *, allow_stale_failed_cleanup: bool = False
            ) -> TransferRecord | EventDisposition:
        key = (transfer.transfer_id, transfer.transfer_attempt_id)
        record = self._transfers.get(key)
        if record is None or record.identity != transfer or key in self._queued_transfers:
            # A queued transfer has not been sent to any worker yet.
            raise UnknownTransfer(f"{transfer.transfer_id}/{transfer.transfer_attempt_id}")
        if self._active_transfer_attempt.get(transfer.transfer_id) != transfer.transfer_attempt_id:
            if allow_stale_failed_cleanup and record.status == TransferStatus.FAILED:
                return record
            return EventDisposition.STALE
        return record

    def _handle_receive_ready(self, worker: WorkerRecord, message: p.ReceiveReady) -> EventDisposition:
        resolved = self._transfer_for(message.transfer)
        if isinstance(resolved, EventDisposition): return resolved
        record = resolved
        self._validate_destination_transfer_event(worker, message.correlation_id, record)
        if record.status == TransferStatus.FAILED:
            return EventDisposition.STALE
        if record.status in {TransferStatus.SOURCE_REQUESTED, TransferStatus.SOURCE_ACCEPTED,
                             TransferStatus.TRANSFERRING, TransferStatus.COMPLETED}:
            return EventDisposition.DUPLICATE
        if record.status != TransferStatus.DESTINATION_PREPARING:
            raise InvalidTransferTransition(f"ReceiveReady in {record.status.value}")
        if record.identity.source_worker_id not in self._available_replica_workers(record.identity.data):
            detail = "source data became unavailable before send"
            staged_failure = self._stage_consumer_transfer_failure(record, detail)
            staged_cleanup = self._stage_consumer_transfer_cleanup(
                record, detail, staged_failure=staged_failure
            )
            record.status = TransferStatus.FAILED
            record.failure_detail = detail
            self._apply_attempt_transfer_cancellations(staged_cleanup)
            self._consumer_transfer_failed(record, detail, staged_failure=staged_failure)
            self._touch()
            return EventDisposition.APPLIED
        source = self._worker(record.identity.source_worker_id)
        destination = self._worker(record.identity.destination_worker_id)
        try:
            self._ensure_outbox_capacity(source)
        except OutboundBackpressure:
            detail = "source outbound queue is full"
            staged_failure = self._stage_consumer_transfer_failure(record, detail)
            staged_cleanup = self._stage_consumer_transfer_cleanup(
                record, detail, staged_failure=staged_failure
            )
            staged_source_failures = self._stage_worker_unavailability_failures(source)
            record.status = TransferStatus.FAILED
            record.failure_detail = detail
            self._apply_attempt_transfer_cancellations(staged_cleanup)
            self._consumer_transfer_failed(record, detail, staged_failure=staged_failure)
            self._mark_worker_compute_unavailable(
                source, detail, staged_failures=staged_source_failures
            )
            self._touch()
            return EventDisposition.APPLIED
        source_request = self._new("message")
        request = p.TransferRequest(
            transfer=record.identity, destination=destination.endpoint,
            source_session_id=record.source_session_id,
            destination_session_id=record.destination_session_id,
            authorization=record.authorization,
            message_id=source_request,
        )
        record.source_request_id = source_request
        record.status = TransferStatus.SOURCE_REQUESTED
        record.phase_since = worker.last_seen
        self._enqueue_message(source, request)
        self._touch()
        return EventDisposition.APPLIED

    def _handle_receive_preparation_failed(self, worker: WorkerRecord,
                                           message: p.ReceivePreparationFailed) -> EventDisposition:
        resolved = self._transfer_for(message.transfer, allow_stale_failed_cleanup=True)
        if isinstance(resolved, EventDisposition): return resolved
        record = resolved
        self._validate_destination_transfer_event(worker, message.correlation_id, record)
        if record.status == TransferStatus.FAILED:
            if record.destination_cleanup_confirmed:
                return EventDisposition.DUPLICATE
            record.destination_cleanup_confirmed = True
            self._touch()
            return EventDisposition.APPLIED
        if record.status != TransferStatus.DESTINATION_PREPARING:
            raise InvalidTransferTransition("receive-preparation failure after readiness")
        detail = message.detail or message.code.value
        staged_failure = self._stage_consumer_transfer_failure(record, detail)
        staged_cleanup = self._stage_consumer_transfer_cleanup(
            record, detail, staged_failure=staged_failure,
            skip_destination=True,
        )
        record.status = TransferStatus.FAILED
        record.destination_cleanup_confirmed = True
        record.failure_detail = detail
        self._apply_attempt_transfer_cancellations(staged_cleanup)
        self._consumer_transfer_failed(record, detail, staged_failure=staged_failure)
        self._touch()
        return EventDisposition.APPLIED

    def _handle_transfer_accepted(self, worker: WorkerRecord, message: p.TransferAccepted) -> EventDisposition:
        resolved = self._transfer_for(message.transfer)
        if isinstance(resolved, EventDisposition): return resolved
        record = resolved
        self._validate_source_transfer_event(worker, message.correlation_id, record)
        if record.status == TransferStatus.FAILED:
            return EventDisposition.STALE
        if record.status in {TransferStatus.SOURCE_ACCEPTED, TransferStatus.TRANSFERRING,
                             TransferStatus.COMPLETED}:
            return EventDisposition.DUPLICATE
        if record.status != TransferStatus.SOURCE_REQUESTED:
            raise InvalidTransferTransition(f"TransferAccepted in {record.status.value}")
        record.status = TransferStatus.SOURCE_ACCEPTED
        record.phase_since = worker.last_seen
        self._touch()
        return EventDisposition.APPLIED

    def _handle_transfer_started(self, worker: WorkerRecord, message: p.TransferStarted) -> EventDisposition:
        resolved = self._transfer_for(message.transfer)
        if isinstance(resolved, EventDisposition): return resolved
        record = resolved
        self._validate_source_transfer_event(worker, message.correlation_id, record)
        if record.status == TransferStatus.FAILED:
            return EventDisposition.STALE
        if record.status in {TransferStatus.TRANSFERRING, TransferStatus.COMPLETED}:
            return EventDisposition.DUPLICATE
        if record.status != TransferStatus.SOURCE_ACCEPTED:
            raise InvalidTransferTransition(f"TransferStarted in {record.status.value}")
        record.status = TransferStatus.TRANSFERRING
        record.phase_since = worker.last_seen
        self._touch()
        return EventDisposition.APPLIED

    @staticmethod
    def _validate_source_transfer_event(worker: WorkerRecord, correlation_id: str | None,
                                        record: TransferRecord) -> None:
        if worker.handle.worker_id != record.identity.source_worker_id:
            raise InvalidWorkerMessage("source transfer event from wrong worker")
        if worker.handle.generation != record.source_generation:
            raise InvalidWorkerMessage("source transfer event belongs to a different worker session")
        if correlation_id != record.source_request_id:
            raise InvalidWorkerMessage("source transfer correlation mismatch")

    @staticmethod
    def _validate_destination_transfer_event(worker: WorkerRecord, correlation_id: str | None,
                                             record: TransferRecord) -> None:
        if worker.handle.worker_id != record.identity.destination_worker_id:
            raise InvalidWorkerMessage("destination transfer event from wrong worker")
        if worker.handle.generation != record.destination_generation:
            raise InvalidWorkerMessage("destination transfer event belongs to a different worker session")
        if correlation_id != record.destination_request_id:
            raise InvalidWorkerMessage("destination transfer correlation mismatch")

    def _handle_transfer_completed(self, worker: WorkerRecord, message: p.TransferCompleted) -> EventDisposition:
        resolved = self._transfer_for(message.transfer, allow_stale_failed_cleanup=True)
        if isinstance(resolved, EventDisposition): return resolved
        record = resolved
        self._validate_destination_transfer_event(worker, message.correlation_id, record)
        if record.status == TransferStatus.COMPLETED:
            return EventDisposition.DUPLICATE
        if record.status == TransferStatus.FAILED:
            if record.source_request_id is None:
                raise InvalidTransferTransition("TransferCompleted before source authorization")
            # F6: late receiver completion proves bytes may have been published on
            # the destination. Do not certify cleanup from that success message;
            # explicitly delete the physical copy and wait for ObjectReleased.
            key = (worker.handle.worker_id, worker.handle.generation, record.identity.data)
            if record.destination_cleanup_confirmed and key not in self._pending_object_releases:
                return EventDisposition.DUPLICATE
            record.source_cleanup_confirmed = True
            if key not in self._pending_object_releases:
                self._stage_object_release(
                    worker, record.identity.data, "failed transfer destination cleanup"
                )
            self._touch()
            return EventDisposition.APPLIED
        if record.status not in {TransferStatus.SOURCE_REQUESTED, TransferStatus.SOURCE_ACCEPTED,
                                  TransferStatus.TRANSFERRING}:
            raise InvalidTransferTransition(f"TransferCompleted in {record.status.value}")
        final_size = message.size_bytes if message.size_bytes is not None else record.size_bytes
        known_size = self._locations.size_bytes(record.identity.data)
        if known_size is not None and final_size is not None and known_size != final_size:
            raise InvalidDataLocation("completed transfer size conflicts with source representation")
        run = self._data_run(record.identity.data)

        staged_dispatch = None
        if record.consumer_run_id is not None and record.consumer_attempt_id is not None:
            consumer_run = self._runs.get(record.consumer_run_id)
            if consumer_run is not None and consumer_run.status == RunStatus.RUNNING:
                attempt = consumer_run.attempts.get(record.consumer_attempt_id)
                if attempt is not None and attempt.status == AttemptStatus.WAITING_TRANSFER:
                    staged_dispatch = self._stage_dispatch_after_final_transfer(
                        consumer_run, attempt, record
                    )

        # Everything fallible that is required for the final dispatch has now
        # been staged. Apply the authoritative completion and continuation.
        self._locations.announce(
            run.plan, run.run_id, worker.handle.worker_id, worker.handle.generation,
            record.identity.data, final_size,
        )
        record.status = TransferStatus.COMPLETED
        record.source_cleanup_confirmed = True
        record.destination_cleanup_confirmed = True
        self._consumer_transfer_completed(record, staged_dispatch)
        self._touch()
        return EventDisposition.APPLIED

    def _handle_transfer_failed(self, worker: WorkerRecord, message: p.TransferFailed) -> EventDisposition:
        resolved = self._transfer_for(message.transfer, allow_stale_failed_cleanup=True)
        if isinstance(resolved, EventDisposition): return resolved
        record = resolved
        source_event = worker.handle.worker_id == record.identity.source_worker_id
        destination_event = worker.handle.worker_id == record.identity.destination_worker_id
        if source_event:
            self._validate_source_transfer_event(worker, message.correlation_id, record)
        elif destination_event:
            self._validate_destination_transfer_event(worker, message.correlation_id, record)
        else:
            raise InvalidWorkerMessage("transfer failure from non-participant")

        if record.status == TransferStatus.COMPLETED:
            return EventDisposition.STALE
        if record.status == TransferStatus.FAILED:
            # Preserve late participant cleanup evidence even after the logical
            # transfer outcome is already FAILED. Do not re-run consumer failure.
            if source_event:
                if record.source_request_id is None:
                    raise InvalidTransferTransition("source failure before source request")
                changed = not record.source_cleanup_confirmed
                record.source_cleanup_confirmed = True
            else:
                changed = not record.destination_cleanup_confirmed
                record.destination_cleanup_confirmed = True
            if changed:
                self._touch()
                return EventDisposition.APPLIED
            return EventDisposition.DUPLICATE

        if source_event:
            if record.status not in {TransferStatus.SOURCE_REQUESTED, TransferStatus.SOURCE_ACCEPTED,
                                     TransferStatus.TRANSFERRING}:
                raise InvalidTransferTransition("source failure before source request")
        else:
            if record.status == TransferStatus.DESTINATION_PREPARING:
                raise InvalidTransferTransition("use ReceivePreparationFailed before readiness")
        detail = message.detail or message.code.value
        # Stage every fallible continuation before recording even participant
        # cleanup evidence. A provider failure must leave the transfer exactly as
        # it was so the same terminal observation can be retried safely.
        staged_failure = self._stage_consumer_transfer_failure(record, detail)
        staged_cleanup = self._stage_consumer_transfer_cleanup(
            record, detail, staged_failure=staged_failure,
            skip_source=source_event, skip_destination=destination_event,
        )
        if source_event:
            record.source_cleanup_confirmed = True
        else:
            record.destination_cleanup_confirmed = True
        record.status = TransferStatus.FAILED
        record.failure_detail = detail
        self._apply_attempt_transfer_cancellations(staged_cleanup)
        self._consumer_transfer_failed(record, detail, staged_failure=staged_failure)
        self._touch()
        return EventDisposition.APPLIED

    # ---------- worker loss ----------
    def _run_will_fail_after_worker_loss(self, run: CoordinatorRun, worker: WorkerRecord) -> bool:
        """Predict whether current worker-loss reconciliation reaches _fail_run.

        This mirrors the reconciliation decision tree only to pre-stage fallible
        clock/ID work before any worker/run/location mutation. It does not mutate
        scheduling state or substitute for the authoritative reconciliation.
        """
        if run.status != RunStatus.RUNNING:
            return False
        worker_id = worker.handle.worker_id
        active_attempts = self._active_attempts_on_worker(run, worker_id)
        if any(
            run.plan.task_index[task.task_id].mode
            in {ExecutionMode.SHARED_CONTEXT, ExecutionMode.NATIVE_REGION}
            for task, _ in active_attempts
        ):
            return True
        owned_contexts = {
            context_id: context for context_id, context in run.contexts.items()
            if context.worker_id == worker_id
        }
        if self._context_dependents_after_worker_loss(run, worker_id, owned_contexts):
            return True
        for task_id, affinity in run.affinities.items():
            task = run.tasks[task_id]
            unfinished = task.status not in {TaskStatus.COMMITTED, TaskStatus.FAILED, TaskStatus.CANCELLED}
            if not unfinished:
                continue
            if affinity.required_worker == worker_id:
                return True
            if affinity.allowed_workers is not None and worker_id in affinity.allowed_workers:
                if not frozenset(w for w in affinity.allowed_workers if w != worker_id):
                    return True
        for task, attempt in active_attempts:
            if run.plan.task_index[task.task_id].mode != ExecutionMode.ISOLATED_CANDIDATE:
                continue
            can_retry = (
                self.retry_policy.retry_worker_loss
                and len(task.attempt_ids) < self.retry_policy.max_attempts_per_task
            )
            if not can_retry:
                return True
        for data in self._locations.data_for_worker(worker_id, worker.handle.generation):
            if self._locations.worker_ids(data) != frozenset({worker_id}):
                continue
            if data.plan_id == run.plan.id and data.run_id == run.run_id:
                if self._unfinished_tasks_requiring_data(run, data):
                    return True
        return False

    def _stage_worker_unavailability_failures(
            self, worker: WorkerRecord) -> dict[str, _StagedRunFailure]:
        staged: dict[str, _StagedRunFailure] = {}
        for run in self._runs.values():
            if not self._run_will_fail_after_worker_loss(run, worker):
                continue
            terminal_ids = frozenset(
                attempt.identity.attempt_id
                for _, attempt in self._active_attempts_on_worker(
                    run, worker.handle.worker_id
                )
            )
            staged[run.run_id] = self._stage_run_failure(
                run,
                FailureInfo(FailureKind.EXECUTION_ERROR, "worker-loss reconciliation preflight"),
                terminal_attempt_ids=terminal_ids,
            )
        # A lost transfer participant can also exhaust a consumer task's retry
        # policy even when the run has no direct attempt/affinity on this worker.
        # Stage that terminal continuation before worker/session state is changed.
        for record in self._transfers.values():
            participant = (
                (record.identity.source_worker_id == worker.handle.worker_id
                 and record.source_generation == worker.handle.generation)
                or
                (record.identity.destination_worker_id == worker.handle.worker_id
                 and record.destination_generation == worker.handle.generation)
            )
            if not participant or record.status.terminal or record.consumer_run_id in staged:
                continue
            prepared = self._stage_consumer_transfer_failure(
                record, "worker-loss transfer reconciliation preflight"
            )
            if prepared is not None and record.consumer_run_id is not None:
                staged[record.consumer_run_id] = prepared
        return staged

    def _mark_worker_compute_unavailable(
            self, worker: WorkerRecord, reason: str, *,
            staged_failures: dict[str, _StagedRunFailure] | None = None) -> None:
        if not worker.active or worker.compute_quarantined:
            return
        prepared_failures = (
            self._stage_worker_unavailability_failures(worker)
            if staged_failures is None else staged_failures
        )
        # A coordinator-imposed quarantine represents uncertainty about physical
        # execution. Heartbeats on this same session cannot clear that uncertainty;
        # only a fresh generation may become schedulable again.
        worker.compute_quarantined = True
        if worker.reported.online or worker.reported.accepting_work:
            worker.reported = replace(worker.reported, online=False, accepting_work=False)
        self._reconcile_compute_unavailable(
            worker, reason, staged_failures=prepared_failures
        )

    def _reconcile_compute_unavailable(
            self, worker: WorkerRecord, reason: str, *,
            staged_failures: dict[str, _StagedRunFailure] | None = None) -> None:
        """Reconcile compute state while retaining the live control session."""
        prepared_failures = (
            self._stage_worker_unavailability_failures(worker)
            if staged_failures is None else staged_failures
        )
        self._invalidate_pending_for_session(worker)
        self._retire_orphans_for_worker(worker)
        lost_data = self._locations.remove_worker(worker.handle.worker_id, worker.handle.generation)
        for key in tuple(self._pending_object_releases):
            if key[0] == worker.handle.worker_id and key[1] == worker.handle.generation:
                self._pending_object_releases.pop(key, None)
        for run in self._runs.values():
            if run.status not in {RunStatus.RUNNING, RunStatus.CANCELLING}:
                continue
            self._reconcile_run_after_worker_loss(
                run, worker, reason, lost_data,
                staged_failure=prepared_failures.get(run.run_id),
            )
        self._fail_transfers_for_lost_worker(
            worker, reason, staged_failures=prepared_failures
        )

    def _lose_worker(self, worker: WorkerRecord, reason: str, *,
                     staged_membership: _StagedMembershipBroadcast | None = None,
                     broadcast: bool = True) -> None:
        if not worker.active:
            return
        staged_failures = self._stage_worker_unavailability_failures(worker)
        if broadcast and staged_membership is None:
            members = tuple(
                r.endpoint for r in sorted(
                    (r for r in self._workers.values()
                     if r.active and r.handle.worker_id != worker.handle.worker_id),
                    key=lambda r: r.handle.worker_id,
                )
            )
            recipients = tuple(
                r for r in self._workers.values()
                if r.active and r.handle.worker_id != worker.handle.worker_id
            )
            staged_membership = self._stage_membership_broadcast(
                recipients=recipients, members=members,
                revision=self._membership_revision + 1,
            )
        worker.active = False
        self._invalidate_pending_for_session(worker)
        self._retire_orphans_for_worker(worker)
        lost_data = self._locations.remove_worker(worker.handle.worker_id, worker.handle.generation)
        for key in tuple(self._pending_object_releases):
            if key[0] == worker.handle.worker_id and key[1] == worker.handle.generation:
                self._pending_object_releases.pop(key, None)

        for run in self._runs.values():
            if run.status not in {RunStatus.RUNNING, RunStatus.CANCELLING}:
                continue
            self._reconcile_run_after_worker_loss(
                run, worker, reason, lost_data,
                staged_failure=staged_failures.get(run.run_id),
            )

        self._fail_transfers_for_lost_worker(
            worker, reason, staged_failures=staged_failures
        )
        if broadcast:
            assert staged_membership is not None
            self._apply_membership_broadcast(staged_membership)
        # The inactive WorkerRecord/outbox is operational session state, not
        # durable history. Generation epochs remain so a future reconnect keeps
        # monotonic session identity, while dead session records are reclaimed.
        if self._workers.get(worker.handle.worker_id) is worker:
            self._workers.pop(worker.handle.worker_id, None)
        self._touch()

    def _retire_orphans_for_worker(self, worker: WorkerRecord) -> None:
        for run in self._runs.values():
            changed = False
            for attempt in run.attempts.values():
                if (attempt.worker_id == worker.handle.worker_id
                        and attempt.worker_generation == worker.handle.generation
                        and attempt.status == AttemptStatus.ORPHANED):
                    attempt.status = AttemptStatus.LOST
                    changed = True
            if changed:
                self._retire_unavailable_contexts(run)
                if run.status.terminal:
                    self._archive_terminal_run(run)

    def _reconcile_run_after_worker_loss(self, run: CoordinatorRun, worker: WorkerRecord,
                                         reason: str,
                                         lost_data: tuple[p.DataReference, ...], *,
                                         staged_failure: _StagedRunFailure | None = None) -> None:
        worker_id = worker.handle.worker_id
        owned_contexts = {
            context_id: context
            for context_id, context in run.contexts.items()
            if context.worker_id == worker_id
        }
        active_attempts = self._active_attempts_on_worker(run, worker_id)
        native_uncertain = tuple(
            task.task_id
            for task, _ in active_attempts
            if run.plan.task_index[task.task_id].mode
            in {ExecutionMode.SHARED_CONTEXT, ExecutionMode.NATIVE_REGION}
        )

        # Retire attempts before any terminal run transition. A lost worker may
        # still have executed work even when its final acknowledgement vanished.
        for task, attempt in active_attempts:
            attempt.status = AttemptStatus.LOST
            task.current_attempt_id = None
        for context_id in owned_contexts:
            del run.contexts[context_id]
            run.unavailable_context_ids.discard(context_id)

        if run.status == RunStatus.CANCELLING:
            for task, _ in active_attempts:
                task.status = TaskStatus.CANCELLED
            self._finish_cancellation_if_idle(run)
            return

        if native_uncertain:
            detail = (
                f"worker {worker_id} was lost while native/shared "
                f"work may have executed: {', '.join(sorted(native_uncertain))}"
            )
            self._fail_run(
                run,
                FailureInfo(FailureKind.EXECUTION_ERROR, detail),
                CoordinatorFailure(CoordinatorFailureCode.NATIVE_STATE_UNCERTAIN, detail),
                staged=staged_failure,
            )
            return

        context_affected = self._context_dependents_after_worker_loss(
            run, worker_id, owned_contexts,
        )
        if context_affected:
            detail = (
                f"native/shared context owner {worker_id} was lost; "
                f"affected tasks: {', '.join(sorted(context_affected))}"
            )
            self._fail_run(
                run,
                FailureInfo(FailureKind.EXECUTION_ERROR, detail),
                CoordinatorFailure(CoordinatorFailureCode.CONTEXT_LOST, detail),
                staged=staged_failure,
            )
            return

        if not self._reconcile_affinities_after_worker_loss(
                run, worker_id, staged_failure=staged_failure):
            return
        self._retry_isolated_attempts_after_worker_loss(
            run, active_attempts, reason, staged_failure=staged_failure
        )
        if run.status != RunStatus.RUNNING:
            return
        for data in lost_data:
            if data.plan_id != run.plan.id or data.run_id != run.run_id:
                continue
            self._fail_if_required_data_lost(
                run, data, f"worker {worker_id} lost", staged_failure=staged_failure
            )
            if run.status != RunStatus.RUNNING:
                break

    @staticmethod
    def _active_attempts_on_worker(run: CoordinatorRun, worker_id: str
                                   ) -> tuple[tuple[TaskRecord, AttemptRecord], ...]:
        active: list[tuple[TaskRecord, AttemptRecord]] = []
        for task in run.tasks.values():
            if task.current_attempt_id is None:
                continue
            attempt = run.attempts[task.current_attempt_id]
            if attempt.worker_id == worker_id and attempt.status.active:
                active.append((task, attempt))
        return tuple(active)

    def _context_dependents_after_worker_loss(
            self, run: CoordinatorRun, worker_id: str,
            owned_contexts: dict[str, WorkerContext]) -> set[str]:
        affected: set[str] = set()
        for context_id, context in owned_contexts.items():
            affected.update(self._unfinished_context_dependents(
                run, context_id, worker_id, context.prepared_task_ids,
            ))

        # Native/shared affinity is unreconstructable even if the context was not
        # materialized in run.contexts when the owner disappeared.
        for task_id, affinity in run.affinities.items():
            if run.tasks[task_id].status in {TaskStatus.COMMITTED, TaskStatus.FAILED, TaskStatus.CANCELLED}:
                continue
            mode = run.plan.task_index[task_id].mode
            if (mode in {ExecutionMode.SHARED_CONTEXT, ExecutionMode.NATIVE_REGION}
                    and affinity.required_worker == worker_id):
                affected.add(task_id)
        return affected

    def _reconcile_affinities_after_worker_loss(
            self, run: CoordinatorRun, worker_id: str, *,
            staged_failure: _StagedRunFailure | None = None) -> bool:
        """Remove dead optional affinity members or fail unrecoverable hard affinity."""
        for task_id, affinity in tuple(run.affinities.items()):
            task = run.tasks[task_id]
            unfinished = task.status not in {TaskStatus.COMMITTED, TaskStatus.FAILED, TaskStatus.CANCELLED}
            if affinity.required_worker == worker_id:
                if unfinished:
                    detail = f"task {task_id} requires unavailable worker {worker_id}"
                    self._fail_run(
                        run, FailureInfo(FailureKind.EXECUTION_ERROR, detail),
                        CoordinatorFailure(CoordinatorFailureCode.PLACEMENT_CONSTRAINT_LOST, detail),
                        staged=staged_failure,
                    )
                    return False
                run.affinities[task_id] = replace(affinity, required_worker=None)
                affinity = run.affinities[task_id]
            if affinity.allowed_workers is not None and worker_id in affinity.allowed_workers:
                remaining = frozenset(w for w in affinity.allowed_workers if w != worker_id)
                if unfinished and not remaining:
                    detail = f"task {task_id} has no surviving allowed worker after losing {worker_id}"
                    self._fail_run(
                        run, FailureInfo(FailureKind.EXECUTION_ERROR, detail),
                        CoordinatorFailure(CoordinatorFailureCode.PLACEMENT_CONSTRAINT_LOST, detail),
                        staged=staged_failure,
                    )
                    return False
                run.affinities[task_id] = replace(affinity, allowed_workers=remaining)
        return True

    def _retry_isolated_attempts_after_worker_loss(
            self, run: CoordinatorRun,
            active_attempts: tuple[tuple[TaskRecord, AttemptRecord], ...],
            reason: str, *, staged_failure: _StagedRunFailure | None = None) -> None:
        # Only isolated work can be replayed after worker loss: it cannot have
        # mutated persistent native/shared state whose effects would be duplicated.
        for task, attempt in active_attempts:
            if run.plan.task_index[task.task_id].mode != ExecutionMode.ISOLATED_CANDIDATE:
                continue
            failure = FailureInfo(FailureKind.EXECUTION_ERROR, f"worker lost: {reason}")
            attempt.failure = failure
            can_retry = (
                self.retry_policy.retry_worker_loss
                and len(task.attempt_ids) < self.retry_policy.max_attempts_per_task
                and run.status == RunStatus.RUNNING
            )
            if can_retry:
                task.failure = failure
                task.status = TaskStatus.READY
                if task.ready_sequence is None:
                    task.ready_sequence = run.next_ready_sequence
                    run.next_ready_sequence += 1
            else:
                task.failure = failure
                task.status = TaskStatus.FAILED
                prepared = staged_failure or self._stage_run_failure(run, failure)
                self._apply_run_failure(run, failure, None, prepared)
            if run.status != RunStatus.RUNNING:
                break

    def _fail_transfers_for_lost_worker(
            self, worker: WorkerRecord, reason: str, *,
            staged_failures: dict[str, _StagedRunFailure] | None = None) -> None:
        worker_id = worker.handle.worker_id
        generation = worker.handle.generation
        prepared_failures = staged_failures or {}
        for record in self._transfers.values():
            source_match = (
                worker_id == record.identity.source_worker_id
                and generation == record.source_generation
            )
            destination_match = (
                worker_id == record.identity.destination_worker_id
                and generation == record.destination_generation
            )
            if not source_match and not destination_match:
                continue
            if source_match:
                record.source_cleanup_confirmed = True
            if destination_match:
                record.destination_cleanup_confirmed = True
            if record.status.terminal:
                continue
            record.status = TransferStatus.FAILED
            record.failure_detail = f"worker lost: {reason}"
            self._consumer_transfer_failed(
                record, record.failure_detail,
                staged_failure=(
                    prepared_failures.get(record.consumer_run_id)
                    if record.consumer_run_id is not None else None
                ),
            )

    # ---------- durable terminal history ----------
    def _archive_terminal_run(self, run: CoordinatorRun) -> bool:
        if self._history_store is None or not run.status.terminal:
            return False
        try:
            self._history_store.archive_run(
                run, self._transfers.values(), archived_at=self._now(None)
            )
        except Exception as error:  # queue/snapshot failure must never interrupt live reconciliation
            self._history_archive_errors[run.run_id] = f"{type(error).__name__}: {error}"
            return False
        # SQLiteRunHistoryStore queues the write off the coordinator path; custom
        # stores may still be synchronous. Only claim durability when the store
        # can prove the queued generation completed, or when it has legacy sync
        # semantics with no confirmation API.
        # A successfully accepted refresh supersedes any coordinator-side
        # synchronous queue/snapshot error from the previous attempt.  The store's
        # own async error channel will repopulate an error if this new generation
        # later fails.  Do not clear such an error merely because an *older*
        # generation is still durably present.
        self._history_archive_errors.pop(run.run_id, None)
        confirmed = getattr(self._history_store, "archive_confirmed", None)
        if confirmed is None:
            return True
        return bool(confirmed(run.run_id))

    def history_archive_confirmed(self, run_id: str) -> bool:
        store = self._history_store
        if store is None:
            return False
        confirmed = getattr(store, "archive_confirmed", None)
        if confirmed is None:
            try:
                return store.has_run(run_id)
            except Exception:
                return False
        return bool(confirmed(run_id))

    def history_archive_error(self, run_id: str) -> str | None:
        """Return the latest terminal-history archival error, if any."""
        # Coordinator-side errors happen before a new store generation exists
        # (for example a synchronous identity/read/queue failure).  An older
        # confirmed generation must not erase that newer refresh failure.
        local_error = self._history_archive_errors.get(run_id)
        if local_error is not None:
            return local_error
        store = self._history_store
        if store is not None:
            waiter = getattr(store, "wait_for_archive", None)
            if waiter is not None:
                waiter(run_id, timeout=0.08)
            error_reader = getattr(store, "archive_error", None)
            if error_reader is not None:
                error = error_reader(run_id)
                if error is not None:
                    self._history_archive_errors[run_id] = error
                    return error
        return self._history_archive_errors.get(run_id)

    def retry_failed_archives(self) -> int:
        store = self._history_store
        error_reader = None if store is None else getattr(store, "archive_error", None)
        if error_reader is None:
            return 0
        retried = 0
        for run in tuple(self._runs.values()):
            if run.status.terminal and error_reader(run.run_id) is not None:
                self._archive_terminal_run(run)
                retried += 1
        return retried

    def load_historical_run(self, run_id: str):
        """Return a durably archived run bundle when the live run is absent.

        The generic history protocol predates read-side status recovery; SQLite
        already provides ``load_run``. Keep the coordinator as the ownership
        boundary and degrade cleanly for write-only history implementations.
        """
        if not isinstance(run_id, str) or not run_id:
            raise ValueError("run_id must be nonempty text")
        store = self._history_store
        if store is None:
            return None
        loader = getattr(store, "load_run", None)
        if loader is None:
            return None
        return loader(run_id)

    def _require_terminal_run_releasable(self, run: CoordinatorRun) -> None:
        if not run.status.terminal:
            raise InvalidRunTransition("only terminal runs may be released")
        if any(attempt.status.occupies_capacity for attempt in run.attempts.values()):
            raise InvalidRunTransition("terminal run still has physically unresolved attempts")
        if self._locations.locations(run.plan.id, run.run_id):
            raise InvalidRunTransition("terminal run still has physically retained data replicas")
        if any(
            data.plan_id == run.plan.id and data.run_id == run.run_id
            for (_worker_id, _generation, data) in self._pending_object_releases
        ):
            raise InvalidRunTransition("terminal run still has pending object releases")
        if any(
            not transfer.cleanup_confirmed
            and key not in self._queued_transfers
            and transfer.identity.data.plan_id == run.plan.id
            and transfer.identity.data.run_id == run.run_id
            for key, transfer in self._transfers.items()
        ):
            raise InvalidRunTransition("terminal run still has unresolved transfer operations")

    def _drop_run_from_memory(self, run: CoordinatorRun) -> None:
        del self._runs[run.run_id]
        self._remember_pruned_run(run.run_id)
        self._locations.remove_run(run.plan.id, run.run_id)
        for key, transfer in tuple(self._transfers.items()):
            if (transfer.identity.data.plan_id == run.plan.id
                    and transfer.identity.data.run_id == run.run_id):
                del self._transfers[key]
                self._queued_transfers.pop(key, None)
                self._issued_transfers.discard(key)
                if self._active_transfer_attempt.get(transfer.identity.transfer_id) == key[1]:
                    self._active_transfer_attempt.pop(transfer.identity.transfer_id, None)
        self._pending_ops.invalidate_run(run.run_id)
        for key in tuple(self._pending_object_releases):
            if key[2].plan_id == run.plan.id and key[2].run_id == run.run_id:
                self._pending_object_releases.pop(key, None)
        self._history_archive_errors.pop(run.run_id, None)
        self._touch()

    def prune_terminal_run(self, run_id: str) -> None:
        """Release terminal in-memory state only after durable archival."""
        run = self._run(run_id)
        self._require_terminal_run_releasable(run)
        if self._history_store is None:
            raise InvalidRunTransition("terminal run is not durably archived")
        # Async history stores must confirm the newest queued terminal snapshot.
        # A coordinator-side refresh error takes precedence over an older durable
        # generation: retry even when archive_confirmed() is true for that older
        # snapshot, and refuse pruning until the refresh itself succeeds.
        error = self.history_archive_error(run_id)
        if error is not None or not self.history_archive_confirmed(run_id):
            self._archive_terminal_run(run)
            waiter = getattr(self._history_store, "wait_for_archive", None)
            if waiter is not None:
                waiter(run_id, timeout=0.2)
        error = self.history_archive_error(run_id)
        if error is not None or not self.history_archive_confirmed(run_id):
            suffix = f": {error}" if error else ""
            raise InvalidRunTransition(
                f"current terminal run state could not be durably archived{suffix}"
            )
        if not self._history_store.has_run(run_id):
            raise InvalidRunTransition("terminal run is not durably archived")
        self._drop_run_from_memory(run)

    def discard_terminal_run(self, run_id: str) -> None:
        """Explicitly discard safe terminal history when durable storage is disabled."""
        run = self._run(run_id)
        self._require_terminal_run_releasable(run)
        self._drop_run_from_memory(run)

    def prune_releasable_terminal_runs(self) -> tuple[str, ...]:
        """Bound retained run state after physical data cleanup and archival.

        F4 is deliberately wired only after F8 tombstones and F5/F6 physical
        release acknowledgements exist. Async history is pruned only after the
        newest queued archive has confirmed success.
        """
        pruned: list[str] = []
        for run_id in tuple(self._runs):
            run = self._runs.get(run_id)
            if run is None or not run.status.terminal:
                continue
            try:
                self._require_terminal_run_releasable(run)
            except InvalidRunTransition:
                continue
            if self._history_store is None:
                # F4/F59: without a durable history store, dropping a terminal run
                # erases the only record of its outcome, so `run_status` would answer
                # UnknownRun moments after the run finished.  Retain terminal runs
                # until admission pressure requires reclaiming them, then drop the
                # oldest first (self._runs preserves submission order).
                # Retention must always stay strictly below the admission limit,
                # otherwise a small `max_runs_in_memory` (e.g. 1) would keep the
                # only slot occupied and block every later submission.
                limit = self.operation_limits.max_runs_in_memory
                retain = min(max(1, (limit * 3) // 4), max(0, limit - 1))
                if len(self._runs) <= retain:
                    continue
                self._drop_run_from_memory(run)
                pruned.append(run_id)
                continue
            if not self.history_archive_confirmed(run_id):
                continue
            try:
                if not self._history_store.has_run(run_id):
                    continue
            except Exception:
                continue
            self._drop_run_from_memory(run)
            pruned.append(run_id)
        return tuple(pruned)

    # ---------- validation ----------
    def validate_state(self) -> None:
        if len(self._runs) > self.operation_limits.max_runs_in_memory:
            raise AssertionError("in-memory run count exceeds admission limit")
        if len(self._generations) > self.operation_limits.max_known_worker_identities:
            raise AssertionError("known worker-identity count exceeds admission limit")
        if len(self._transfers) > self.operation_limits.max_transfer_records_global:
            raise AssertionError("transfer records exceed global admission limit")
        transfer_counts: dict[str, int] = {}
        for transfer in self._transfers.values():
            run_id = transfer.identity.data.run_id
            transfer_counts[run_id] = transfer_counts.get(run_id, 0) + 1
        if any(count > self.operation_limits.max_transfer_records_per_run
               for count in transfer_counts.values()):
            raise AssertionError("transfer records exceed per-run admission limit")
        context_total, contexts_per_worker, contexts_per_run = self._context_retention_usage()
        if context_total > self.operation_limits.max_retained_contexts_global:
            raise AssertionError("retained contexts exceed global admission limit")
        if any(count > self.operation_limits.max_retained_contexts_per_worker
               for count in contexts_per_worker.values()):
            raise AssertionError("retained contexts exceed per-worker admission limit")
        if any(count > self.operation_limits.max_retained_contexts_per_run
               for count in contexts_per_run.values()):
            raise AssertionError("retained contexts exceed per-run admission limit")
        active_workers = {wid for wid, worker in self._workers.items() if worker.active}
        current_generations = {
            worker_id: worker.handle.generation
            for worker_id, worker in self._workers.items()
            if worker.active
        }
        self._locations.validate(current_generations)
        self._pending_ops.validate(current_generations)
        self._validate_pending_contexts_against_runs()
        for run in self._runs.values():
            self._validate_run_state(run, active_workers)
        self._validate_worker_capacity(active_workers)
        self._validate_active_transfers()

    def _validate_pending_contexts_against_runs(self) -> None:
        for request in self._pending_ops.active_requests():
            if not isinstance(request, PendingContextPreparation):
                continue
            run = self._runs.get(request.run_id)
            if run is None:
                continue
            existing = run.contexts.get(request.context_id)
            if existing is not None and (
                    existing.worker_id != request.worker_id
                    or existing.prepared_task_ids != frozenset(request.task_ids)):
                raise AssertionError("pending context conflicts with authoritative context")
            try:
                self._validate_context_contract(
                    run.plan, run.affinities, request.context_id, request.worker_id,
                    request.task_ids,
                )
            except ContextConflict as error:
                raise AssertionError("pending context conflicts with task affinity") from error

    def _validate_run_state(self, run: CoordinatorRun, active_workers: set[str]) -> None:
        if not run.unavailable_context_ids <= run.contexts.keys():
            raise AssertionError("unavailable context tombstone is missing context identity")
        for unavailable_id in run.unavailable_context_ids:
            if not any(
                attempt.context_id == unavailable_id and attempt.status.occupies_capacity
                for attempt in run.attempts.values()
            ):
                raise AssertionError("unavailable context retained without physical reservation")
        for context_id, context in run.contexts.items():
            if context_id != context.context_id:
                raise AssertionError("context dictionary key disagrees with context identity")
            if not context.prepared_task_ids <= run.plan.task_index.keys():
                raise AssertionError("context references unknown task")
            if run.status == RunStatus.RUNNING and context.worker_id not in active_workers:
                raise AssertionError("running context belongs to inactive worker")
            for task_id in context.prepared_task_ids:
                affinity = run.affinities.get(task_id)
                if affinity is None:
                    continue
                if affinity.context_id is not None and affinity.context_id != context.context_id:
                    raise AssertionError("context conflicts with task context affinity")
                if affinity.required_worker is not None and affinity.required_worker != context.worker_id:
                    raise AssertionError("context owner conflicts with required worker")
                if affinity.allowed_workers is not None and context.worker_id not in affinity.allowed_workers:
                    raise AssertionError("context owner excluded by allowed workers")
        if run.failure is not None and run.status != RunStatus.FAILED:
            raise AssertionError("coordinator failure attached to non-failed run")

        current_attempts: set[str] = set()
        for task in run.tasks.values():
            self._validate_task_state(run, task, current_attempts, active_workers)

        context_use: dict[str, int] = {}
        for attempt in run.attempts.values():
            if attempt.context_id is not None and attempt.status.occupies_capacity:
                context_use[attempt.context_id] = context_use.get(attempt.context_id, 0) + 1
        for context_id, used in context_use.items():
            context = run.contexts.get(context_id)
            if context is None:
                raise AssertionError("capacity-consuming attempt references missing context")
            if used > context.available_slots:
                raise AssertionError("context capacity is oversubscribed")
        for attempt in run.attempts.values():
            if attempt.status == AttemptStatus.ORPHANED and not run.status.terminal:
                raise AssertionError("orphaned attempt belongs to non-terminal run")

        if run.status == RunStatus.SUCCEEDED and len(run.readiness.completed) != len(run.tasks):
            raise AssertionError("succeeded run is incomplete")
        if run.status.terminal:
            if any(attempt.status.active for attempt in run.attempts.values()):
                raise AssertionError("terminal run retains active attempt")
            terminal_forbidden = {
                TaskStatus.READY, TaskStatus.BLOCKED, TaskStatus.WAITING_TRANSFER,
                TaskStatus.DISPATCHED, TaskStatus.ACCEPTED, TaskStatus.RUNNING,
                TaskStatus.CANCELLING,
            }
            if any(task.status in terminal_forbidden for task in run.tasks.values()):
                raise AssertionError("terminal run retains schedulable/active task")

    @staticmethod
    def _validate_task_state(run: CoordinatorRun, task: TaskRecord,
                             current_attempts: set[str], active_workers: set[str]) -> None:
        if task.current_attempt_id is not None:
            if task.current_attempt_id in current_attempts:
                raise AssertionError("attempt is current for two tasks")
            current_attempts.add(task.current_attempt_id)
            attempt = run.attempts.get(task.current_attempt_id)
            if attempt is None or attempt.identity.task_id != task.task_id:
                raise AssertionError("current attempt link is invalid")
            if not attempt.status.active:
                raise AssertionError("current attempt is not active")
            if attempt.worker_id not in active_workers:
                raise AssertionError("active attempt belongs to inactive worker")
            expected_task_status = {
                AttemptStatus.WAITING_TRANSFER: TaskStatus.WAITING_TRANSFER,
                AttemptStatus.DISPATCHED: TaskStatus.DISPATCHED,
                AttemptStatus.ACCEPTED: TaskStatus.ACCEPTED,
                AttemptStatus.RUNNING: TaskStatus.RUNNING,
                AttemptStatus.CANCEL_REQUESTED: TaskStatus.CANCELLING,
            }[attempt.status]
            if task.status != expected_task_status:
                raise AssertionError("task/attempt active status mismatch")
            if (attempt.status == AttemptStatus.WAITING_TRANSFER
                    and attempt.dispatch_message_id is not None):
                raise AssertionError("transfer-gated attempt already has dispatch identity")
            if (attempt.status == AttemptStatus.WAITING_TRANSFER
                    and not attempt.pending_transfers):
                raise AssertionError("transfer-gated attempt has no pending transfer or dispatch continuation")
            if (attempt.status != AttemptStatus.WAITING_TRANSFER
                    and attempt.dispatch_message_id is None):
                raise AssertionError("dispatched attempt is missing dispatch identity")

        if task.status == TaskStatus.COMMITTED:
            if task.committed_attempt_id is None or task.task_id not in run.readiness.completed:
                raise AssertionError("committed task/readiness mismatch")
            if task.current_attempt_id is not None:
                raise AssertionError("committed task has current attempt")
        if task.status == TaskStatus.READY and task.task_id not in run.readiness.ready:
            raise AssertionError("coordinator READY task not ready in DAG")

    def _validate_worker_capacity(self, active_workers: set[str]) -> None:
        for worker_id in active_workers:
            state = self._effective_worker_state(worker_id)
            if state.running_slots < 0 or state.reserved_slots < 0 or state.free_slots < 0:
                raise AssertionError("negative worker capacity")

    def _validate_active_transfers(self) -> None:
        for record in self._transfers.values():
            if record.status.terminal:
                continue
            source = self._workers.get(record.identity.source_worker_id)
            destination = self._workers.get(record.identity.destination_worker_id)
            if (source is None or not source.active or source.handle.generation != record.source_generation
                    or destination is None or not destination.active
                    or destination.handle.generation != record.destination_generation):
                raise AssertionError("active transfer belongs to stale worker session")
