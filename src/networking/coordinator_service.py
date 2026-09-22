"""Asyncio adapter that exposes the synchronous Coordinator over real mutual TLS."""
from __future__ import annotations

import asyncio
import ast
from contextlib import suppress
import hashlib
import ipaddress
import logging
import os
from pathlib import Path, PurePosixPath
import ssl
import tempfile
import time
from dataclasses import dataclass, field
from typing import Callable

import protocol as p
from coordinator import (
    Coordinator, CoordinatorError, InvalidWorkerMessage, SessionHandle, StaleWorkerSession,
)
from dag_runtime.dag_engine import analyze_source
from execution import ExecutionMode, ValueKind, lower_dag
from scheduler import TaskAffinity
from runtime_security import AuthChallenge, AuthProof, AuthenticationError, NodeAuthenticator, TlsPolicy
from program_package import (
    PackageArtifact, PackageCache, PackageLimits, PackageRepository,
)

from .common import FramedProtocolStream, close_writer, new_transport_id, write_message
from .config import TransportLimits
LOG = logging.getLogger(__name__)

from .errors import (
    AdmissionError,
    ApplicationAuthenticationError,
    BackpressureError,
    ProtocolTransportError,
    TlsAuthenticationError,
    TransportIOError,
)


@dataclass(frozen=True, slots=True)
class _PackageDelivery:
    prepare: p.PrepareProgram
    artifact: PackageArtifact



@dataclass(slots=True)
class _ClientSubmission:
    start: p.RunSubmitStart
    path: Path
    handle: object
    hasher: object
    received: int = 0
    last_activity: float = field(default_factory=time.monotonic)
    absolute_deadline: float = 0.0
    idle_timeout: float = 0.0
    timeout_task: asyncio.Task[None] | None = None


@dataclass(frozen=True, slots=True)
class _ManagedRun:
    plan: object
    context_id: str | None = None
    context_worker_id: str | None = None


_PREAUTH_TYPES = (
    p.AuthenticationRequest,
    p.AuthenticationChallenge,
    p.AuthenticationProof,
    p.AuthenticationAccepted,
)


def _requires_persistent_process(plan, task) -> bool:
    """Return whether an isolated-labelled task still needs module-process identity.

    F16 can distribute isolated work only when the execution contract is actually
    process-independent. Shared-reference inputs/outputs may preserve alias identity
    or hidden mutation that cannot be merged back under the current immutable snapshot
    identity. Likewise CPython's ``hash``/``id`` are process-local by design, so two
    logical calls in one module must not be split across fresh interpreter processes.
    Keep those narrow cases in the prepared persistent context; ordinary immutable
    isolated work remains freely placeable.
    """
    if task.mode is not ExecutionMode.ISOLATED_CANDIDATE:
        return True
    if any(
        req.kind is ValueKind.SHARED_REFERENCE
        for req in (*task.inputs, *task.outputs)
    ):
        return True
    sources = [task.task.source]
    for definition_id in task.code.definition_ids:
        definition = plan.definition_index.get(definition_id)
        if definition is not None:
            sources.append(definition.source)
    for source in sources:
        try:
            tree = ast.parse(source)
        except SyntaxError:
            return True
        if any(
            isinstance(node, ast.Name)
            and isinstance(node.ctx, ast.Load)
            and node.id in {"hash", "id"}
            for node in ast.walk(tree)
        ):
            return True
    return False


def _peer_certificate_identities(writer: asyncio.StreamWriter) -> frozenset[str]:
    ssl_object = writer.get_extra_info("ssl_object")
    if ssl_object is None:
        raise TlsAuthenticationError("TLS is required for control connections")
    certificate = ssl_object.getpeercert()
    if not certificate:
        raise TlsAuthenticationError("peer certificate is unavailable")
    # RFC-style identity precedence: once the certificate carries a supported
    # subjectAltName, do not also treat the legacy Common Name as another
    # authorization identity.  This prevents one certificate from gaining an
    # unintended second cluster identity via a mismatched CN.
    san_identities = {
        value for kind, value in certificate.get("subjectAltName", ())
        if kind in {"DNS", "URI"} and isinstance(value, str)
    }
    if san_identities:
        return frozenset(san_identities)
    common_names: set[str] = set()
    for rdn in certificate.get("subject", ()):
        for key, value in rdn:
            if key == "commonName" and isinstance(value, str):
                common_names.add(value)
    return frozenset(common_names)


def _is_loopback_peer(writer: asyncio.StreamWriter) -> bool:
    peer = writer.get_extra_info("peername")
    try:
        return ipaddress.ip_address(peer[0]).is_loopback
    except (TypeError, ValueError, IndexError):
        return False


async def _cancel_owned_tasks(tasks: set[asyncio.Task[None]], timeout: float) -> None:
    if not tasks:
        return
    for task in tasks:
        if not task.done():
            task.cancel()
    done, pending = await asyncio.wait(tasks, timeout=timeout)
    for task in done:
        with suppress(BaseException):
            task.exception()
    for task in pending:
        task.cancel()
        task.add_done_callback(lambda finished: finished.exception() if not finished.cancelled() else None)


class CoordinatorNetworkService:
    """Own listening/TLS I/O while serializing all Coordinator mutations.

    Socket readers/writers are concurrent asyncio tasks, but every authoritative
    coordinator call is made while holding ``_coordinator_lock``. The synchronous
    Coordinator therefore retains a single serialized ownership boundary.
    """

    def __init__(
        self,
        coordinator: Coordinator,
        *,
        tls_policy: TlsPolicy,
        authenticator: NodeAuthenticator,
        host: str | list[str] = "127.0.0.1",
        port: int = 0,
        limits: TransportLimits | None = None,
        clock: Callable[[], float] = time.monotonic,
        package_repository: PackageRepository | None = None,
        package_chunk_bytes: int = 32 * 1024,
        package_limits: PackageLimits | None = None,
        max_active_submissions: int = 8,
        client_node_ids: frozenset[str] = frozenset(),
        worker_node_ids: frozenset[str] | None = None,
        operators_on_loopback_only: bool = False,
    ) -> None:
        if not isinstance(coordinator, Coordinator):
            raise TypeError("coordinator must be Coordinator")
        if not isinstance(tls_policy, TlsPolicy):
            raise TypeError("tls_policy must be TlsPolicy")
        if not isinstance(authenticator, NodeAuthenticator):
            raise TypeError("authenticator must be NodeAuthenticator")
        # One address, or several (for example a LAN address plus loopback).
        hosts = [host] if isinstance(host, str) else list(host) if isinstance(host, (list, tuple)) else []
        if not hosts or not all(isinstance(item, str) and item for item in hosts):
            raise ValueError("host must be nonempty text or a list of it")
        if type(port) is not int or not 0 <= port <= 65535:
            raise ValueError("port must be in 0..65535")
        self.coordinator = coordinator
        self.tls_policy = tls_policy
        self.authenticator = authenticator
        self.host = hosts[0] if len(hosts) == 1 else hosts
        # When the operator always sits on the coordinator's own machine, operator
        # access from anywhere else is refused even with valid credentials.
        self.operators_on_loopback_only = bool(operators_on_loopback_only)
        self.port = port
        self.limits = limits or TransportLimits()
        if package_repository is not None and not isinstance(package_repository, PackageRepository):
            raise TypeError("package_repository must be PackageRepository or None")
        if type(package_chunk_bytes) is not int or not 1 <= package_chunk_bytes <= 32 * 1024:
            raise ValueError("package_chunk_bytes must be in 1..32768")
        if type(max_active_submissions) is not int or max_active_submissions < 1:
            raise ValueError("max_active_submissions must be a positive integer")
        if not isinstance(client_node_ids, frozenset) or any(
            type(node_id) is not str or not node_id.strip() for node_id in client_node_ids
        ):
            raise ValueError("client_node_ids must be a frozenset of nonempty node IDs")
        if worker_node_ids is not None and (
            not isinstance(worker_node_ids, frozenset) or any(
                type(node_id) is not str or not node_id.strip() for node_id in worker_node_ids
            )
        ):
            raise ValueError("worker_node_ids must be None or a frozenset of nonempty node IDs")
        if worker_node_ids is not None and client_node_ids.intersection(worker_node_ids):
            raise ValueError("client_node_ids and worker_node_ids must be disjoint")
        self._clock = clock
        self.package_repository = package_repository
        self.package_chunk_bytes = package_chunk_bytes
        self.package_limits = package_limits or PackageLimits()
        self.max_active_submissions = max_active_submissions
        self.client_node_ids = client_node_ids
        # F41: production callers provide an explicit role allow-list. ``None`` is
        # retained only as a backwards-compatible embedding/test mode; the stock
        # CLI always supplies a concrete set and therefore fails closed by role.
        self.worker_node_ids = worker_node_ids
        self._active_submissions = 0
        # F37: admitted operator sockets are not immortal. Submission leases are
        # much shorter; this broader idle bound covers authenticated client
        # connections that never start or resume any operation.
        self.client_idle_timeout = max(60.0, self.limits.write_timeout * 12.0)
        self._managed_runs: dict[str, _ManagedRun] = {}
        # F15: deterministic preparation failure is fenced to the exact worker
        # generation/program pair. Reconnect (new generation) permits one retry.
        self._program_preparation_failures: dict[tuple[str, int, str], str] = {}
        self._server: asyncio.AbstractServer | None = None
        self._coordinator_lock = asyncio.Lock()
        self._connections: set[_ServerConnection] = set()
        self._connection_tasks: set[asyncio.Task[None]] = set()
        self._maintenance_task: asyncio.Task[None] | None = None
        # Wake maintenance as soon as a writer frees outbound capacity. This
        # keeps coordinator-owned backlog moving without waiting a full tick.
        self._outbox_wakeup = asyncio.Event()
        self._stopping = False

    @property
    def listening_port(self) -> int:
        if self._server is None or not self._server.sockets:
            raise RuntimeError("service is not listening")
        return int(self._server.sockets[0].getsockname()[1])

    @property
    def connection_count(self) -> int:
        return len(self._connections)

    def set_worker_identities(self, identities: frozenset[str]) -> None:
        """Replace the worker allow-list while serving.

        A connected worker that is no longer on it is disconnected at once; when it
        reconnects it is told it is not a member.  Call from the event loop.
        """
        self.worker_node_ids = frozenset(identities)
        for connection in tuple(self._connections):
            session = connection.session
            if session is not None and session.worker_id not in self.worker_node_ids:
                connection.request_close("removed from the cluster")

    async def start(self) -> None:
        if self._server is not None:
            raise RuntimeError("service already started")
        self._stopping = False
        context = self.tls_policy.build_server_context()
        self._server = await asyncio.start_server(
            self._client_connected,
            self.host,
            self.port,
            ssl=context,
            ssl_handshake_timeout=self.limits.handshake_timeout,
            limit=self.limits.stream_buffer_limit,
        )
        self._maintenance_task = asyncio.create_task(
            self._maintenance_loop(), name="coordinator-network-maintenance"
        )

    async def stop(self) -> None:
        if self._server is None:
            return
        self._stopping = True
        server = self._server
        server.close()
        for connection in tuple(self._connections):
            connection.request_close("coordinator service shutdown")
        tasks = tuple(self._connection_tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await server.wait_closed()
        self._server = None
        if self._maintenance_task is not None:
            self._maintenance_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._maintenance_task
            self._maintenance_task = None

    async def __aenter__(self) -> "CoordinatorNetworkService":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.stop()

    def _client_connected(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        if len(self._connections) >= self.limits.max_connections:
            transport = getattr(writer, "transport", None)
            if transport is not None:
                transport.abort()
            else:
                writer.close()
            return
        connection = _ServerConnection(self, reader, writer)
        self._connections.add(connection)
        task = asyncio.create_task(connection.run(), name="coordinator-control-connection")
        self._connection_tasks.add(task)
        task.add_done_callback(self._connection_tasks.discard)

    async def _admit(self, connection: "_ServerConnection", hello: p.WorkerHello) -> SessionHandle:
        async with self._coordinator_lock:
            # The allow-list may have changed while this handshake waited for the lock;
            # checking again here, where admission itself happens, leaves no gap.
            if (self.worker_node_ids is not None
                    and hello.worker.worker_id not in self.worker_node_ids):
                raise AdmissionError("authenticated node is not authorized for worker access")
            previous = None
            try:
                view = self.coordinator.inspect_worker(hello.worker.worker_id)
                if view.state.online:
                    previous = view.endpoint
            except CoordinatorError:
                previous = None
            try:
                session = self.coordinator.register_worker(hello, now=self._clock())
            except Exception as error:
                raise AdmissionError("worker admission rejected") from error
            connection.session = session
            LOG.info("worker admitted worker=%s generation=%d session=%s",
                     session.worker_id, session.generation, session.session_id)
            if previous is not None and (previous.host, previous.port) != (hello.endpoint.host, hello.endpoint.port):
                # Displacing a live session is normal after a crash/restart, but a
                # still-online incumbent on a *different* endpoint usually means two
                # processes share one --id.  They then displace each other
                # indefinitely, each clearing the other's session data.  Admission is
                # unchanged: refusing the newcomer would block recovery from a hung
                # worker, so surface the cause instead of leaving silent flapping.
                LOG.warning(
                    "worker %s reconnected from %s:%s while a live session was "
                    "registered at %s:%s; if both processes are running they share "
                    "one worker id",
                    session.worker_id, hello.endpoint.host, hello.endpoint.port,
                    previous.host, previous.port,
                )
            self._flush_outboxes_locked()
            return session

    @staticmethod
    def _log_worker_event(session: SessionHandle, message: p.Message) -> None:
        """Log bounded control-plane milestones without secrets or user payloads."""
        if isinstance(message, p.TaskStarted):
            attempt = message.attempt
            LOG.info("task started worker=%s generation=%d run=%s task=%s attempt=%s",
                     session.worker_id, session.generation, attempt.run_id,
                     attempt.task_id, attempt.attempt_id)
        elif isinstance(message, p.TaskSucceeded):
            attempt = message.result.attempt
            LOG.info("task succeeded worker=%s generation=%d run=%s task=%s attempt=%s",
                     session.worker_id, session.generation, attempt.run_id,
                     attempt.task_id, attempt.attempt_id)
        elif isinstance(message, p.TaskFailed):
            attempt = message.result.attempt
            LOG.info("task failed worker=%s generation=%d run=%s task=%s attempt=%s kind=%s",
                     session.worker_id, session.generation, attempt.run_id,
                     attempt.task_id, attempt.attempt_id, message.result.failure.kind.value)
        elif isinstance(message, p.ContextPrepared):
            LOG.info("context prepared worker=%s generation=%d context=%s",
                     session.worker_id, session.generation, message.context.context_id)
        elif isinstance(message, p.ContextUnavailable):
            log = LOG.info if message.reason == "prepared context work completed" else LOG.warning
            log("context unavailable worker=%s generation=%d context=%s reason=%s",
                session.worker_id, session.generation, message.context_id, message.reason)
        elif isinstance(message, p.TransferStarted):
            transfer = message.transfer
            LOG.info("transfer started worker=%s generation=%d transfer=%s attempt=%s",
                     session.worker_id, session.generation,
                     transfer.transfer_id, transfer.transfer_attempt_id)
        elif isinstance(message, p.TransferCompleted):
            transfer = message.transfer
            LOG.info("transfer completed worker=%s generation=%d transfer=%s attempt=%s",
                     session.worker_id, session.generation,
                     transfer.transfer_id, transfer.transfer_attempt_id)
        elif isinstance(message, p.TransferFailed):
            transfer = message.transfer
            LOG.warning("transfer failed worker=%s generation=%d transfer=%s attempt=%s code=%s",
                        session.worker_id, session.generation,
                        transfer.transfer_id, transfer.transfer_attempt_id, message.code.value)

    async def _handle_message(self, connection: "_ServerConnection", message: p.Message) -> None:
        if connection.client_id is not None:
            await self._handle_client_message(connection, message)
            return
        session = connection.session
        if session is None:
            raise AdmissionError("application message before session admission")
        if isinstance(message, _PREAUTH_TYPES) or isinstance(message, (p.WorkerHello, p.ClientHello)):
            raise AdmissionError("handshake message is invalid after admission")
        self._log_worker_event(session, message)
        async with self._coordinator_lock:
            try:
                self.coordinator.handle_message(session, message, now=self._clock())
                if isinstance(message, p.ProgramPreparationFailed):
                    self._program_preparation_failures[(
                        session.worker_id, session.generation, message.program_id
                    )] = message.failure.message or message.failure.kind.value
                elif isinstance(message, p.ProgramPrepared):
                    self._program_preparation_failures.pop((
                        session.worker_id, session.generation, message.program_id
                    ), None)
            except (StaleWorkerSession, InvalidWorkerMessage):
                # Session fencing and identity/correlation violations remain
                # fail-closed: they indicate a stale or invalid peer message.
                raise
            except CoordinatorError as error:
                # Fix-guide F3: a current authenticated worker can race ordinary
                # coordinator lifecycle transitions (late reject/transfer/etc.).
                # Those state-machine rejections are message-local and must not
                # retire the whole worker session or collateral runs.
                LOG.warning(
                    "dropping coordinator-rejected worker message worker=%s generation=%d "
                    "message=%s error=%s",
                    session.worker_id, session.generation, type(message).__name__, error,
                )
                return
            # F27: commits and transfer completion can make the next task ready.
            # Advance only that affected managed run immediately instead of
            # waiting for the global maintenance tick.  Accepted/Started and
            # other chatter intentionally do not reschedule, which bounds this
            # to one advancement pass per progress event rather than per message.
            affected_run = None
            if isinstance(message, p.TaskSucceeded):
                affected_run = message.result.attempt.run_id
            elif isinstance(message, p.TransferCompleted):
                affected_run = message.transfer.data.run_id
            if affected_run is not None and affected_run in self._managed_runs:
                self._drive_managed_run_locked(affected_run)
            self._flush_outboxes_locked()

    def _client_response(self, connection: "_ServerConnection", message: p.Message) -> None:
        if not connection.enqueue(message):
            raise BackpressureError("client outbound queue exhausted")

    def _client_failure(
        self, connection: "_ServerConnection", request: p.Message, operation: str,
        code: str, detail: str,
    ) -> None:
        bounded = detail[: p.MAX_DETAIL_BYTES]
        self._client_response(connection, p.ClientOperationFailed(
            operation=operation, code=code, detail=bounded,
            message_id=new_transport_id("client-failure"), correlation_id=request.message_id,
        ))

    async def _handle_client_message(
        self, connection: "_ServerConnection", message: p.Message
    ) -> None:
        if isinstance(message, _PREAUTH_TYPES) or isinstance(message, (p.WorkerHello, p.ClientHello)):
            raise AdmissionError("handshake message is invalid after admission")
        if isinstance(message, p.RunSubmitStart):
            await self._client_submit_start(connection, message)
            return
        if isinstance(message, p.RunSubmitChunk):
            await self._client_submit_chunk(connection, message)
            return
        if isinstance(message, p.RunSubmitEnd):
            await self._client_submit_end(connection, message)
            return
        if isinstance(message, p.RunStatusRequest):
            await self._client_run_status(connection, message)
            return
        if isinstance(message, p.ClusterStatusRequest):
            await self._client_cluster_status(connection, message)
            return
        if isinstance(message, p.CancelRunRequest):
            await self._client_cancel_run(connection, message)
            return
        raise AdmissionError("worker/control message is invalid on a client session")

    def _check_client_identity(self, connection: "_ServerConnection", client_id: str) -> None:
        if connection.client_id is None or client_id != connection.client_id:
            raise AdmissionError("client message identity disagrees with authenticated node")

    async def _client_submit_start(
        self, connection: "_ServerConnection", message: p.RunSubmitStart
    ) -> None:
        self._check_client_identity(connection, message.client_id)
        if self.package_repository is None:
            self._client_failure(connection, message, "run_submit", "package_repository_unavailable",
                                 "coordinator package repository is not configured")
            return
        if connection.submission is not None:
            self._client_failure(connection, message, "run_submit", "submission_in_progress",
                                 "this client connection already has an active submission")
            return
        if message.archive_size > self.package_limits.max_archive_bytes:
            self._client_failure(connection, message, "run_submit", "package_too_large",
                                 "package archive exceeds configured coordinator limit")
            return
        async with self._coordinator_lock:
            # F48: a client that lost the final acknowledgement must be allowed to
            # restage the exact same immutable submission. The full archive is still
            # verified before the end handler returns the existing committed result.
            if message.run_id in self.coordinator.run_ids():
                if self.coordinator.run_plan_id(message.run_id) != message.plan_id:
                    self._client_failure(connection, message, "run_submit", "run_exists",
                                         f"run already exists with a different plan: {message.run_id}")
                    return
            else:
                archived = self.coordinator.load_historical_run(message.run_id)
                if archived is not None and archived.run.plan_id != message.plan_id:
                    self._client_failure(connection, message, "run_submit", "run_exists",
                                         f"archived run already exists with a different plan: {message.run_id}")
                    return
            if self._active_submissions >= self.max_active_submissions:
                self._client_failure(connection, message, "run_submit", "backpressure",
                                     "active submission limit reached")
                return
            self._active_submissions += 1
        try:
            fd, raw_path = tempfile.mkstemp(prefix="dpr-submit-", suffix=".zip")
            handle = os.fdopen(fd, "wb")
        except Exception:
            async with self._coordinator_lock:
                self._active_submissions -= 1
            raise
        now = self._clock()
        expected_chunks = max(1, (message.archive_size + self.package_chunk_bytes - 1) // self.package_chunk_bytes)
        idle_timeout = max(self.limits.write_timeout * 2.0, 1.0)
        absolute_budget = min(60.0, max(idle_timeout, self.limits.write_timeout * (expected_chunks + 1)))
        submission = _ClientSubmission(
            message, Path(raw_path), handle, hashlib.sha256(), 0,
            last_activity=now, absolute_deadline=now + absolute_budget,
            idle_timeout=idle_timeout,
        )
        connection.submission = submission
        submission.timeout_task = asyncio.create_task(
            connection.expire_submission(message.message_id), name="client-submission-timeout"
        )
        self._client_response(connection, p.RunSubmitReady(
            run_id=message.run_id, package_id=message.package_id,
            message_id=new_transport_id("run-submit-ready"),
            correlation_id=message.message_id,
        ))

    async def _client_submit_chunk(
        self, connection: "_ServerConnection", message: p.RunSubmitChunk
    ) -> None:
        self._check_client_identity(connection, message.client_id)
        submission = connection.submission
        if submission is None:
            self._client_failure(connection, message, "run_submit", "no_submission",
                                 "no active package submission")
            return
        start = submission.start
        if (message.correlation_id != start.message_id or message.run_id != start.run_id
                or message.package_id != start.package_id or message.offset != submission.received):
            await connection.discard_submission()
            self._client_failure(connection, message, "run_submit", "submission_mismatch",
                                 "package chunk does not match the active submission")
            return
        try:
            chunk = bytes.fromhex(message.data_hex)
        except ValueError:
            await connection.discard_submission()
            self._client_failure(connection, message, "run_submit", "invalid_chunk",
                                 "package chunk is not valid hexadecimal")
            return
        if submission.received + len(chunk) > start.archive_size:
            await connection.discard_submission()
            self._client_failure(connection, message, "run_submit", "package_too_large",
                                 "package bytes exceed the declared archive size")
            return
        submission.handle.write(chunk)
        submission.hasher.update(chunk)
        submission.received += len(chunk)
        submission.last_activity = self._clock()

    def _stage_client_submission(
        self, path: Path, start: p.RunSubmitStart
    ) -> tuple[PackageArtifact, object]:
        with tempfile.TemporaryDirectory(prefix="dpr-submit-verify-") as temp_root:
            # The cache holds an open lock file for the lifetime of the object.  On
            # Windows an open file cannot be deleted, so the temp directory cleanup
            # fails with WinError 32 unless the cache is closed first.
            cache = PackageCache(temp_root, limits=self.package_limits)
            try:
                return self._verify_submission_with(cache, path, start)
            finally:
                cache.close()

    def _verify_submission_with(
        self, cache: PackageCache, path: Path, start: p.RunSubmitStart
    ) -> tuple[PackageArtifact, object]:
        manifest = cache.install_archive(path, start.package_id)
        manifest_files = {item.path: item for item in manifest.files}
        entry = manifest_files.get(start.entrypoint)
        if entry is None:
            raise ValueError("entrypoint is not present in the submitted package")
        source_path = cache.content_path(start.package_id).joinpath(*PurePosixPath(start.entrypoint).parts)
        with source_path.open("rb") as source_file:
            source_bytes = source_file.read(entry.size + 1)
        if len(source_bytes) != entry.size:
            raise ValueError("entrypoint changed while validating submission")
        try:
            # utf-8-sig: CPython accepts a UTF-8 BOM in source files (editors on
            # Windows add one), so a BOM must not make an otherwise valid program
            # unanalyzable.  The CLI decodes identically, keeping plan identity
            # byte-for-byte consistent across both sides (F43).
            source = source_bytes.decode("utf-8-sig", errors="strict")
        except UnicodeDecodeError as error:
            raise ValueError("entrypoint must be UTF-8 Python source") from error
        dag = analyze_source(source, filename=start.entrypoint)
        plan = lower_dag(
            dag, environment_id=start.environment_id, package_id=start.package_id
        )
        if plan.id != start.plan_id:
            raise ValueError("submitted plan identity does not match verified source/package")
        with path.open("rb") as archive_file:
            archive_bytes = archive_file.read(self.package_limits.max_archive_bytes + 1)
        if len(archive_bytes) != start.archive_size:
            raise ValueError("submitted archive size changed during verification")
        artifact = PackageArtifact(
            start.package_id, archive_bytes, start.archive_sha256, manifest
        )
        return artifact, plan

    async def _client_submit_end(
        self, connection: "_ServerConnection", message: p.RunSubmitEnd
    ) -> None:
        self._check_client_identity(connection, message.client_id)
        submission = connection.submission
        if submission is None:
            self._client_failure(connection, message, "run_submit", "no_submission",
                                 "no active package submission")
            return
        start = submission.start
        if (message.correlation_id != start.message_id or message.run_id != start.run_id
                or message.package_id != start.package_id
                or message.archive_size != start.archive_size
                or message.archive_sha256 != start.archive_sha256):
            await connection.discard_submission()
            self._client_failure(connection, message, "run_submit", "submission_mismatch",
                                 "submission terminator does not match its start record")
            return
        timeout_task = submission.timeout_task
        submission.timeout_task = None
        submission.last_activity = self._clock()
        if timeout_task is not None and timeout_task is not asyncio.current_task() and not timeout_task.done():
            timeout_task.cancel()
        committed = False
        post_commit_error: BaseException | None = None
        response: p.RunSubmitted | None = None
        try:
            submission.handle.flush()
            os.fsync(submission.handle.fileno())
            submission.handle.close()
            if submission.received != start.archive_size:
                raise ValueError("submitted archive is truncated")
            if submission.hasher.hexdigest() != start.archive_sha256:
                raise ValueError("submitted archive SHA-256 mismatch")
            artifact, plan = await asyncio.to_thread(
                self._stage_client_submission, submission.path, start
            )
            async with self._coordinator_lock:
                # F48: a retry after a lost acknowledgement is idempotent when it
                # names the same immutable execution. Do not require workers to be
                # available again just to acknowledge an already-committed submit.
                if start.run_id in self.coordinator.run_ids():
                    if self.coordinator.run_plan_id(start.run_id) != plan.id:
                        raise ValueError(f"run already exists with a different plan: {start.run_id}")
                    status = self.coordinator.inspect_run_summary(start.run_id)[0].value
                    committed = True
                else:
                    archived = self.coordinator.load_historical_run(start.run_id)
                    if archived is not None:
                        if (archived.run.plan_id != plan.id
                                or archived.run.program_id != plan.program.id):
                            raise ValueError(
                                f"archived run already exists with a different execution: {start.run_id}"
                            )
                        status = archived.run.status
                        committed = True
                    else:
                        active = [
                            self.coordinator.inspect_worker(worker_id)
                            for worker_id in self.coordinator.worker_ids(active_only=True)
                        ]
                        compatible = [
                            view for view in active
                            if plan.program.environment_id in view.state.environment_ids
                        ]
                        context_tasks = tuple(
                            task for task in plan.tasks
                            if _requires_persistent_process(plan, task)
                        )
                        needs_context = bool(context_tasks)
                        context_id = context_worker = None
                        affinities: tuple[TaskAffinity, ...] = ()
                        if needs_context:
                            if len(context_tasks) > p.MAX_COLLECTION_ITEMS:
                                raise ValueError(
                                    f"context task count exceeds protocol limit {p.MAX_COLLECTION_ITEMS}"
                                )
                            context_modes = {task.mode for task in context_tasks}
                            candidates = [
                                view for view in compatible
                                if context_modes <= view.state.supported_modes
                            ]
                            if not candidates:
                                raise ValueError(
                                    "no active worker can host the required shared/native context"
                                )
                            owner = max(
                                candidates, key=lambda view: (view.state.free_slots, view.handle.worker_id)
                            )
                            context_worker = owner.handle.worker_id
                            context_id = "cli-ctx-" + hashlib.sha256(
                                start.run_id.encode("utf-8")
                            ).hexdigest()[:24]
                            affinities = tuple(
                                TaskAffinity(task.task_id, required_worker=context_worker,
                                             context_id=context_id)
                                for task in context_tasks
                            )
                        # F4: terminal runs are reclaimed by the maintenance tick,
                        # so a burst of submissions can otherwise be rejected while
                        # reclaimable runs are still resident.  Reclaim first, then
                        # apply the (non-mutating) admission preflight.
                        self.coordinator.prune_releasable_terminal_runs()
                        self.coordinator.preflight_run_submission(start.run_id)
                        self.package_repository.preflight_add(artifact)
                        self.coordinator.submit(plan, run_id=start.run_id, affinities=affinities)
                        self.package_repository.add(artifact)
                        self.package_repository.pin(artifact.package_id)
                        self._managed_runs[start.run_id] = _ManagedRun(
                            plan, context_id=context_id, context_worker_id=context_worker
                        )
                        status = self.coordinator.inspect_run(start.run_id).status.value
                        committed = True
                        LOG.info("run submitted run=%s plan=%s client=%s",
                                 start.run_id, plan.id, connection.client_id)
                # Once committed is True, outbox flushing is transport work only.
                # It may force reconnect, but must never produce ClientOperationFailed.
                try:
                    self._flush_outboxes_locked()
                except BaseException as error:
                    post_commit_error = error
                response = p.RunSubmitted(
                    run_id=start.run_id, plan_id=plan.id, status=status,
                    message_id=new_transport_id("run-submitted"),
                    correlation_id=message.message_id,
                )
            if post_commit_error is not None:
                LOG.warning("post-commit submit flush failed run=%s error=%s",
                            start.run_id, post_commit_error)
                connection.request_close("post-commit submit transport failure")
                return
            try:
                assert response is not None
                self._client_response(connection, response)
            except BaseException as error:
                LOG.warning("post-commit submit response failed run=%s error=%s",
                            start.run_id, error)
                connection.request_close("post-commit submit response failure")
        except Exception as error:
            if committed:
                LOG.warning("post-commit submit handling failed run=%s error=%s",
                            start.run_id, error)
                connection.request_close("post-commit submit handling failure")
            else:
                self._client_failure(connection, message, "run_submit",
                                     type(error).__name__.lower(), str(error))
        finally:
            await connection.discard_submission()

    async def _client_run_status(
        self, connection: "_ServerConnection", message: p.RunStatusRequest
    ) -> None:
        self._check_client_identity(connection, message.client_id)
        try:
            async with self._coordinator_lock:
                if message.run_id not in self.coordinator.run_ids():
                    # FIXES F59 names coordinator/coordinator.py for the status path,
                    # but this tree builds the client-facing RunStatusResponse in the
                    # network service. The coordinator exposes the history lookup; the
                    # service must translate that durable bundle into the wire response.
                    archived = self.coordinator.load_historical_run(message.run_id)
                    if archived is None:
                        # Preserve the existing client-visible unknownrun code.
                        from coordinator import UnknownRun
                        raise UnknownRun(message.run_id)
                    run = archived.run
                    if not message.include_tasks:
                        response = p.RunStatusResponse(
                            run_id=run.run_id, plan_id=run.plan_id, status=run.status,
                            tasks=(), task_count=len(archived.tasks), contexts=(),
                            tasks_truncated=bool(archived.tasks),
                            failure_kind=run.failure_code,
                            failure_detail=(run.failure_detail or "")[: p.MAX_DETAIL_BYTES],
                            message_id=new_transport_id("run-status"),
                            correlation_id=message.message_id,
                        )
                    else:
                        attempts = {attempt.attempt_id: attempt for attempt in archived.attempts}
                        by_task: dict[str, list[object]] = {}
                        for attempt in archived.attempts:
                            by_task.setdefault(attempt.task_id, []).append(attempt)
                        views: list[p.RunTaskView] = []
                        for task in archived.tasks[:1024]:
                            attempt = None
                            if task.committed_attempt_id is not None:
                                attempt = attempts.get(task.committed_attempt_id)
                            if attempt is None and by_task.get(task.task_id):
                                attempt = by_task[task.task_id][-1]
                            failure_kind = task.failure_kind
                            detail = task.failure_message or ""
                            exception_type = task.exception_type
                            if failure_kind is None and attempt is not None:
                                failure_kind = attempt.failure_kind
                                detail = attempt.failure_message or ""
                                exception_type = attempt.exception_type
                            views.append(p.RunTaskView(
                                task_id=task.task_id, status=task.status,
                                attempt_id=None if attempt is None else attempt.attempt_id,
                                worker_id=None if attempt is None else attempt.worker_id,
                                failure_kind=failure_kind,
                                detail=detail[: p.MAX_DETAIL_BYTES], output_ids=(),
                                exception_type=exception_type,
                                stdout_tail="" if attempt is None else attempt.stdout_tail[: p.MAX_DETAIL_BYTES],
                                stderr_tail="" if attempt is None else attempt.stderr_tail[: p.MAX_DETAIL_BYTES],
                                stdout_truncated=False if attempt is None else bool(attempt.stdout_truncated),
                                stderr_truncated=False if attempt is None else bool(attempt.stderr_truncated),
                            ))
                        response = p.RunStatusResponse(
                            run_id=run.run_id, plan_id=run.plan_id, status=run.status,
                            tasks=tuple(views), task_count=len(archived.tasks), contexts=(),
                            tasks_truncated=len(archived.tasks) > len(views),
                            failure_kind=run.failure_code,
                            failure_detail=(run.failure_detail or "")[: p.MAX_DETAIL_BYTES],
                            message_id=new_transport_id("run-status"),
                            correlation_id=message.message_id,
                        )
                else:
                    plan_id = self.coordinator.run_plan_id(message.run_id)
                    if not message.include_tasks:
                        status, task_count, failure = self.coordinator.inspect_run_summary(message.run_id)
                        response = p.RunStatusResponse(
                            run_id=message.run_id, plan_id=plan_id, status=status.value,
                            tasks=(), task_count=task_count, contexts=(),
                            tasks_truncated=task_count > 0,
                            failure_kind=None if failure is None else failure.code.value,
                            failure_detail="" if failure is None else failure.detail[: p.MAX_DETAIL_BYTES],
                            message_id=new_transport_id("run-status"),
                            correlation_id=message.message_id,
                        )
                    else:
                        snapshot = self.coordinator.inspect_run(message.run_id)
                        views: list[p.RunTaskView] = []
                        for task_id, status in snapshot.tasks[:1024]:
                            task = self.coordinator.get_task(message.run_id, task_id)
                            attempt_id = task.committed_attempt_id or task.current_attempt_id
                            if attempt_id is None and task.attempt_ids:
                                attempt_id = task.attempt_ids[-1]
                            worker_id = failure_kind = exception_type = None
                            detail = ""
                            if attempt_id is not None:
                                attempt = self.coordinator.get_attempt(message.run_id, attempt_id)
                                worker_id = attempt.worker_id
                                if attempt.failure is not None:
                                    failure_kind = attempt.failure.kind.value
                                    exception_type = attempt.failure.exception_type
                                    detail = attempt.failure.message
                            if task.failure is not None:
                                failure_kind = task.failure.kind.value
                                exception_type = task.failure.exception_type
                                detail = task.failure.message
                            output_ids = tuple(output.id for output in self.coordinator.get_task_manifest(message.run_id, task_id).outputs) if status.value == "committed" else ()
                            views.append(p.RunTaskView(
                                task_id=task_id, status=status.value, attempt_id=attempt_id,
                                worker_id=worker_id, failure_kind=failure_kind,
                                detail=detail[: p.MAX_DETAIL_BYTES], output_ids=output_ids,
                                exception_type=exception_type,
                                stdout_tail="" if attempt_id is None else attempt.stdout_tail[: p.MAX_DETAIL_BYTES],
                                stderr_tail="" if attempt_id is None else attempt.stderr_tail[: p.MAX_DETAIL_BYTES],
                                stdout_truncated=False if attempt_id is None else attempt.stdout_truncated,
                                stderr_truncated=False if attempt_id is None else attempt.stderr_truncated,
                            ))
                        contexts = tuple(
                            p.ClientContextView(
                                context.context_id, context.worker_id, context.available_slots,
                                len(context.prepared_task_ids),
                            )
                            for context in self.coordinator.inspect_contexts(message.run_id)[:1024]
                        )
                        run_failure_kind = None
                        run_failure_detail = ""
                        if snapshot.failure is not None:
                            run_failure_kind = snapshot.failure.code.value
                            run_failure_detail = snapshot.failure.detail
                        response = p.RunStatusResponse(
                            run_id=message.run_id, plan_id=plan_id, status=snapshot.status.value,
                            tasks=tuple(views), task_count=len(snapshot.tasks), contexts=contexts,
                            tasks_truncated=len(snapshot.tasks) > len(views),
                            failure_kind=run_failure_kind,
                            failure_detail=run_failure_detail[: p.MAX_DETAIL_BYTES],
                            message_id=new_transport_id("run-status"),
                            correlation_id=message.message_id,
                        )
            self._client_response(connection, response)
        except Exception as error:
            self._client_failure(connection, message, "run_status",
                                 type(error).__name__.lower(), str(error))

    async def _client_cluster_status(
        self, connection: "_ServerConnection", message: p.ClusterStatusRequest
    ) -> None:
        self._check_client_identity(connection, message.client_id)
        try:
            async with self._coordinator_lock:
                workers = tuple(
                    p.ClientWorkerView(
                        view.handle.worker_id, view.handle.generation, view.handle.session_id,
                        view.state.online, view.state.accepting_work, view.state.total_slots,
                        view.state.running_slots, view.state.reserved_slots,
                        view.endpoint.host, view.endpoint.port,
                    )
                    for worker_id in self.coordinator.worker_ids()
                    for view in (self.coordinator.inspect_worker(worker_id),)
                )
                response = p.ClusterStatusResponse(
                    workers=workers, run_ids=self.coordinator.run_ids()[:1024],
                    message_id=new_transport_id("cluster-status"),
                    correlation_id=message.message_id,
                )
            self._client_response(connection, response)
        except Exception as error:
            self._client_failure(connection, message, "cluster_status",
                                 type(error).__name__.lower(), str(error))

    async def _client_cancel_run(
        self, connection: "_ServerConnection", message: p.CancelRunRequest
    ) -> None:
        self._check_client_identity(connection, message.client_id)
        committed = False
        response: p.CancelRunResponse | None = None
        post_commit_error: BaseException | None = None
        try:
            async with self._coordinator_lock:
                if message.run_id in self.coordinator.run_ids():
                    status = self.coordinator.inspect_run_summary(message.run_id)[0]
                    if status.value in {"cancelling", "cancelled"}:
                        # F48 idempotent retry after an acknowledgement loss.
                        committed = True
                    else:
                        self.coordinator.cancel_run(message.run_id, reason=message.reason)
                        status = self.coordinator.inspect_run_summary(message.run_id)[0]
                        committed = True
                else:
                    archived = self.coordinator.load_historical_run(message.run_id)
                    if archived is None or archived.run.status != "cancelled":
                        from coordinator import UnknownRun
                        raise UnknownRun(message.run_id)
                    status = archived.run.status
                    committed = True
                try:
                    self._flush_outboxes_locked()
                except BaseException as error:
                    post_commit_error = error
                status_value = status.value if hasattr(status, "value") else str(status)
                response = p.CancelRunResponse(
                    run_id=message.run_id, status=status_value,
                    message_id=new_transport_id("cancel-run"),
                    correlation_id=message.message_id,
                )
                LOG.info("run cancellation requested run=%s client=%s",
                         message.run_id, connection.client_id)
            if post_commit_error is not None:
                LOG.warning("post-commit cancel flush failed run=%s error=%s",
                            message.run_id, post_commit_error)
                connection.request_close("post-commit cancel transport failure")
                return
            try:
                assert response is not None
                self._client_response(connection, response)
            except BaseException as error:
                LOG.warning("post-commit cancel response failed run=%s error=%s",
                            message.run_id, error)
                connection.request_close("post-commit cancel response failure")
        except Exception as error:
            if committed:
                LOG.warning("post-commit cancel handling failed run=%s error=%s",
                            message.run_id, error)
                connection.request_close("post-commit cancel handling failure")
            else:
                self._client_failure(connection, message, "cancel_run",
                                     type(error).__name__.lower(), str(error))

    def _drive_managed_run_locked(self, run_id: str) -> None:
        """Advance one CLI-owned run once. Caller holds ``_coordinator_lock``."""
        managed = self._managed_runs.get(run_id)
        if managed is None:
            return
        try:
            status, _task_count, _failure = self.coordinator.inspect_run_summary(run_id)
            if status.terminal:
                # This adapter state is needed only while driving a live CLI
                # run. Retaining terminal entries would make every future
                # maintenance pass scan total historical CLI runs forever.
                self._managed_runs.pop(run_id, None)
                if managed.plan.program.package_id is not None:
                    self.package_repository.unpin(managed.plan.program.package_id)
                return
            workers = [
                self.coordinator.inspect_worker(worker_id)
                for worker_id in self.coordinator.worker_ids(active_only=True)
            ]
            eligible = [
                view for view in workers
                if managed.plan.program.environment_id in view.state.environment_ids
            ]
            usable_for_program = []
            preparation_failures = []
            for view in eligible:
                if managed.plan.program.id in view.state.prepared_program_ids:
                    usable_for_program.append(view)
                    continue
                failure = self._program_preparation_failures.get((
                    view.handle.worker_id, view.handle.generation, managed.plan.program.id
                ))
                if failure is not None:
                    preparation_failures.append((view.handle.worker_id, failure))
                    continue
                usable_for_program.append(view)
                try:
                    self.coordinator.request_program_preparation(
                        view.handle.worker_id, managed.plan
                    )
                except CoordinatorError:
                    # Capacity/backpressure/stale-state can change on every
                    # advancement; another worker or later pass may progress.
                    continue
            if eligible and not usable_for_program and preparation_failures:
                detail = "; ".join(
                    f"{worker_id}: {failure}" for worker_id, failure in preparation_failures
                )
                self.coordinator.fail_run_unpreparable(run_id, detail)
                return
            if managed.context_id is not None and managed.context_worker_id is not None:
                try:
                    owner = self.coordinator.inspect_worker(managed.context_worker_id)
                    if managed.plan.program.id in owner.state.prepared_program_ids:
                        # FIXES F16 names the affinity construction above, but
                        # the physical context contract must match it: including
                        # isolated task IDs here would make the context wait for
                        # work intentionally scheduled on other workers.
                        self.coordinator.request_context_preparation(
                            run_id, managed.context_worker_id, managed.context_id,
                            tuple(
                                task.task_id for task in managed.plan.tasks
                                if _requires_persistent_process(managed.plan, task)
                            ),
                        )
                except (p.ValidationError, p.ResourceLimitExceeded, ValueError) as error:
                    self.coordinator.fail_run_resource_limit(
                        run_id, f"context preparation rejected: {error}"
                    )
                    return
                except CoordinatorError:
                    pass
            try:
                self.coordinator.schedule(run_id)
            except (p.ValidationError, p.ResourceLimitExceeded, ValueError) as error:
                self.coordinator.fail_run_resource_limit(
                    run_id, f"scheduling rejected: {error}"
                )
            except CoordinatorError:
                pass
        except CoordinatorError:
            if run_id not in self.coordinator.run_ids():
                self._managed_runs.pop(run_id, None)
                if managed.plan.program.package_id is not None:
                    self.package_repository.unpin(managed.plan.program.package_id)
        except Exception:
            LOG.exception("managed run advancement failed run=%s", run_id)

    def _drive_managed_runs_locked(self) -> None:
        """Advance every live CLI-owned run once from maintenance."""
        for run_id in tuple(self._managed_runs):
            self._drive_managed_run_locked(run_id)

    async def _connection_closed(self, connection: "_ServerConnection", reason: str) -> None:
        self._connections.discard(connection)
        session = connection.session
        if session is None:
            if connection.client_id is not None:
                LOG.debug("client connection closed client=%s reason=%s", connection.client_id, reason)
            return
        LOG.info("worker connection closed worker=%s generation=%d reason=%s",
                 session.worker_id, session.generation, reason)
        async with self._coordinator_lock:
            try:
                self.coordinator.disconnect_session(session, reason=reason)
            finally:
                self._flush_outboxes_locked()

    def _flush_outboxes_locked(self) -> None:
        """Move only what each bounded socket queue can currently accept.

        Fix-guide F26 assumes the Coordinator exposes a partial-drain API. In
        this tree it exposes only drain-all, so while already holding the sole
        coordinator adapter lock we consume a prefix of the current session's
        private outbox and deliberately leave the remainder authoritative there.
        This avoids duplicating the backlog in an unbounded adapter queue.
        """
        for connection in tuple(self._connections):
            session = connection.session
            if session is None or connection._closing:
                continue
            try:
                record = self.coordinator._session(session)
            except StaleWorkerSession:
                continue

            moved = 0
            while moved < len(record.outbox):
                free_space = connection.outbound.maxsize - connection.outbound.qsize()
                if free_space <= 0:
                    break
                message = record.outbox[moved]
                artifact = None
                needs_package_slot = False
                if isinstance(message, p.PrepareProgram) and message.program.package_id is not None:
                    artifact = (
                        None if self.package_repository is None
                        else self.package_repository.get(message.program.package_id)
                    )
                    needs_package_slot = (
                        artifact is not None
                        and artifact.package_id not in connection._package_delivery_owner
                    )
                required_slots = 1 + int(needs_package_slot)
                if required_slots > free_space:
                    break
                if not connection.enqueue(message):
                    break
                if artifact is not None and not connection.enqueue_package_delivery(message, artifact):
                    # Capacity was preflighted above, so this can only happen if
                    # the connection entered closing state. Leave the authoritative
                    # message queued; session teardown will reconcile it.
                    break
                moved += 1
            if moved:
                del record.outbox[:moved]


    async def _maintenance_loop(self) -> None:
        try:
            while True:
                try:
                    await asyncio.wait_for(
                        self._outbox_wakeup.wait(), timeout=self.limits.maintenance_interval
                    )
                except asyncio.TimeoutError:
                    pass
                self._outbox_wakeup.clear()
                async with self._coordinator_lock:
                    now = self._clock()
                    # F7: pending dispatch/start/cancel/transfer/preparation deadlines
                    # are authoritative lifecycle timers and must be driven live.
                    # Use the coordinator's injected clock here: operation timestamps
                    # are created by Coordinator._now(), while worker heartbeat
                    # liveness is intentionally stamped with the service clock.
                    self.coordinator.expire_operations()
                    self.coordinator.retry_failed_archives()
                    expired = self.coordinator.expire_workers(now=now)
                    if expired:
                        expired_set = set(expired)
                        for connection in tuple(self._connections):
                            if (connection.session is not None
                                    and connection.session.worker_id in expired_set):
                                connection.request_close("heartbeat timeout")
                    self._drive_managed_runs_locked()
                    self.coordinator.request_terminal_object_releases()
                    self.coordinator.prune_releasable_terminal_runs()
                    self._flush_outboxes_locked()
        except asyncio.CancelledError:
            raise


class _ServerConnection:
    def __init__(
        self,
        service: CoordinatorNetworkService,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        self.service = service
        self.reader = reader
        self.writer = writer
        self.stream = FramedProtocolStream(
            reader, read_chunk_size=service.limits.read_chunk_size
        )
        self.inbound: asyncio.Queue[p.Message] = asyncio.Queue(
            maxsize=service.limits.inbound_queue_messages
        )
        self.outbound: asyncio.Queue[p.Message | _PackageDelivery | None] = asyncio.Queue(
            maxsize=service.limits.outbound_queue_messages
        )
        self.session: SessionHandle | None = None
        self.client_id: str | None = None
        self.client_session_id: str | None = None
        self.submission: _ClientSubmission | None = None
        self._package_delivery_owner: dict[str, str] = {}
        self._prepare_package: dict[str, str] = {}
        self._close_reason = "control connection closed"
        self._closing = False
        self._outstanding_auth_challenge: AuthChallenge | None = None

    async def expire_submission(self, start_message_id: str) -> None:
        try:
            while True:
                submission = self.submission
                if submission is None or submission.start.message_id != start_message_id:
                    return
                now = self.service._clock()
                deadline = min(
                    submission.absolute_deadline,
                    submission.last_activity + submission.idle_timeout,
                )
                delay = deadline - now
                if delay > 0:
                    await asyncio.sleep(delay)
                    continue
                await self.discard_submission()
                LOG.warning(
                    "expired idle client submission client=%s run=%s received=%d expected=%d",
                    self.client_id, submission.start.run_id, submission.received,
                    submission.start.archive_size,
                )
                return
        except asyncio.CancelledError:
            raise

    async def discard_submission(self) -> None:
        submission = self.submission
        if submission is None:
            return
        self.submission = None
        timeout_task = submission.timeout_task
        if timeout_task is not None and timeout_task is not asyncio.current_task() and not timeout_task.done():
            timeout_task.cancel()
        with suppress(Exception):
            submission.handle.close()
        with suppress(FileNotFoundError):
            submission.path.unlink()
        async with self.service._coordinator_lock:
            self.service._active_submissions = max(0, self.service._active_submissions - 1)

    def enqueue(self, message: p.Message | _PackageDelivery) -> bool:
        if self._closing:
            return False
        try:
            self.outbound.put_nowait(message)
            return True
        except asyncio.QueueFull:
            return False

    def enqueue_package_delivery(self, prepare: p.PrepareProgram, artifact: PackageArtifact) -> bool:
        """Single-flight package bytes per connection/package identity.

        Every PrepareProgram is still delivered.  While one preparation for an
        immutable package is awaiting a worker response, later preparations for
        the same package share those bytes and the worker's own single-flight
        installation instead of retransmitting the archive N times.
        """
        package_id = artifact.package_id
        self._prepare_package[prepare.message_id] = package_id
        if package_id in self._package_delivery_owner:
            return True
        if not self.enqueue(_PackageDelivery(prepare, artifact)):
            self._prepare_package.pop(prepare.message_id, None)
            return False
        self._package_delivery_owner[package_id] = prepare.message_id
        return True

    def note_package_response(self, message: p.Message) -> None:
        if not isinstance(message, (p.ProgramPrepared, p.ProgramPreparationFailed)):
            return
        correlation = message.correlation_id
        if correlation is None:
            return
        package_id = self._prepare_package.pop(correlation, None)
        if package_id is not None:
            self._package_delivery_owner.pop(package_id, None)

    def request_close(self, reason: str) -> None:
        if self._closing:
            return
        self._closing = True
        self._close_reason = reason
        with suppress(asyncio.QueueFull):
            self.outbound.put_nowait(None)
        # This path is used for fail-closed retirement/service shutdown, not the
        # peer's graceful goodbye. Abort wakes a reader blocked on TLS I/O and
        # prevents an uncooperative peer from pinning service shutdown.
        transport = getattr(self.writer, "transport", None)
        if transport is not None:
            transport.abort()
        else:
            self.writer.close()

    async def run(self) -> None:
        tasks: set[asyncio.Task[None]] = set()
        try:
            await self._authenticate_and_admit()
            tasks = {
                asyncio.create_task(self._reader_loop(), name="control-reader"),
                asyncio.create_task(self._processor_loop(), name="control-processor"),
                asyncio.create_task(self._writer_loop(), name="control-writer"),
            }
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            failure: BaseException | None = None
            for task in done:
                try:
                    task.result()
                except asyncio.CancelledError:
                    pass
                except BaseException as error:
                    if failure is None:
                        failure = error
            await _cancel_owned_tasks(set(pending), self.service.limits.write_timeout)
            if failure is not None:
                if isinstance(failure, (StaleWorkerSession, AdmissionError)):
                    self._close_reason = "stale or invalid admitted session"
                elif isinstance(failure, BackpressureError):
                    self._close_reason = "transport queue exhausted"
                elif isinstance(failure, ProtocolTransportError):
                    self._close_reason = "protocol input rejected"
                    LOG.warning(
                        "control protocol rejected role=%s identity=%s",
                        "client" if self.client_id is not None else "worker",
                        self.client_id or (self.session.worker_id if self.session is not None else "unadmitted"),
                    )
                elif isinstance(failure, (EOFError, TransportIOError)):
                    self._close_reason = "control connection lost"
                else:
                    self._close_reason = "control connection failed"
                    LOG.error(
                        "control connection task failed role=%s identity=%s error_type=%s",
                        "client" if self.client_id is not None else "worker",
                        self.client_id or (self.session.worker_id if self.session is not None else "unadmitted"),
                        type(failure).__name__,
                    )
        except (EOFError, TransportIOError):
            self._close_reason = "control connection lost during admission"
        except (TlsAuthenticationError, ApplicationAuthenticationError, ProtocolTransportError):
            self._close_reason = "connection authentication rejected"
        except AdmissionError:
            self._close_reason = "worker admission rejected"
        finally:
            await _cancel_owned_tasks(tasks, self.service.limits.write_timeout)
            self._closing = True
            challenge = self._outstanding_auth_challenge
            if challenge is not None:
                # F34: a peer that vanishes or times out before proof must not
                # retain scarce verifier capacity for the full replay window.
                self.service.authenticator.abandon(
                    challenge.node_id, challenge.session_id, challenge.nonce
                )
                self._outstanding_auth_challenge = None
            with suppress(Exception):
                await close_writer(self.writer, timeout=self.service.limits.write_timeout)
            await self.discard_submission()
            await self.service._connection_closed(self, self._close_reason)

    async def _next_handshake_message(self) -> p.Message:
        try:
            return await asyncio.wait_for(
                self.stream.read_message(), timeout=self.service.limits.handshake_timeout
            )
        except asyncio.TimeoutError as error:
            raise ApplicationAuthenticationError("authentication handshake timed out") from error

    async def _authenticate_and_admit(self) -> None:
        request = await self._next_handshake_message()
        if not isinstance(request, p.AuthenticationRequest):
            raise ApplicationAuthenticationError("first application message must request authentication")
        identities = _peer_certificate_identities(self.writer)
        if request.node_id not in identities:
            raise TlsAuthenticationError("client certificate identity does not match node identity")

        try:
            challenge = self.service.authenticator.issue(request.node_id, request.session_id)
        except (AuthenticationError, ValueError) as error:
            raise ApplicationAuthenticationError("authentication challenge rejected") from error
        self._outstanding_auth_challenge = challenge
        challenge_message = p.AuthenticationChallenge(
            node_id=challenge.node_id,
            session_id=challenge.session_id,
            nonce=challenge.nonce,
            issued_at=challenge.issued_at,
            message_id=new_transport_id("auth-challenge"),
            correlation_id=request.message_id,
        )
        await write_message(
            self.writer, challenge_message, timeout=self.service.limits.write_timeout
        )

        proof_message = await self._next_handshake_message()
        if not isinstance(proof_message, p.AuthenticationProof):
            raise ApplicationAuthenticationError("authentication proof required")
        if proof_message.correlation_id != challenge_message.message_id:
            raise ApplicationAuthenticationError("authentication proof correlation mismatch")
        proof = AuthProof(
            proof_message.node_id,
            proof_message.session_id,
            proof_message.nonce,
            proof_message.issued_at,
            proof_message.mac_hex,
        )
        try:
            self.service.authenticator.verify(
                proof,
                expected_node_id=request.node_id,
                expected_session_id=request.session_id,
            )
        except AuthenticationError as error:
            raise ApplicationAuthenticationError("authentication proof rejected") from error
        # Verification consumed the entry into a replay tombstone; it must no
        # longer be abandoned on connection teardown.
        self._outstanding_auth_challenge = None
        accepted = p.AuthenticationAccepted(
            node_id=request.node_id,
            session_id=request.session_id,
            message_id=new_transport_id("auth-accepted"),
            correlation_id=proof_message.message_id,
        )
        await write_message(self.writer, accepted, timeout=self.service.limits.write_timeout)

        hello = await self._next_handshake_message()
        if isinstance(hello, p.ClientHello):
            if hello.client_id != request.node_id:
                raise AdmissionError("ClientHello identity disagrees with authenticated node")
            if hello.client_id not in self.service.client_node_ids:
                raise AdmissionError("authenticated node is not authorized for operator access")
            if self.service.operators_on_loopback_only and not _is_loopback_peer(self.writer):
                raise AdmissionError("operator access is only allowed from this machine")
            if (self.service.worker_node_ids is not None
                    and hello.client_id in self.service.worker_node_ids):
                raise AdmissionError("worker identities cannot use operator access")
            self.client_id = hello.client_id
            self.client_session_id = request.session_id
            LOG.info("client admitted client=%s", hello.client_id)
            accepted_client = p.ClientAccepted(
                client_id=hello.client_id, session_id=request.session_id,
                message_id=new_transport_id("client-accepted"),
                correlation_id=hello.message_id,
            )
            await write_message(
                self.writer, accepted_client, timeout=self.service.limits.write_timeout
            )
            return
        if not isinstance(hello, p.WorkerHello):
            raise AdmissionError("WorkerHello or ClientHello required after authentication")
        if hello.worker.worker_id != request.node_id or hello.endpoint.worker_id != request.node_id:
            raise AdmissionError("WorkerHello identity disagrees with authenticated node")
        if (self.service.worker_node_ids is not None
                and request.node_id not in self.service.worker_node_ids):
            # The peer has proven who it is, so it may be told plainly that it is not
            # a member: a removed worker stops retrying instead of knocking for ever.
            with suppress(Exception):
                await write_message(self.writer, p.WorkerRejected(
                    worker_id=request.node_id, code=p.RejectionCode.NOT_ADMITTED,
                    detail="not a member of this cluster",
                    message_id=new_transport_id("worker-rejected"),
                    correlation_id=hello.message_id,
                ), timeout=self.service.limits.write_timeout)
            raise AdmissionError("authenticated node is not authorized for worker access")
        await self.service._admit(self, hello)

    async def _reader_loop(self) -> None:
        while True:
            if self.client_id is not None:
                try:
                    message = await asyncio.wait_for(
                        self.stream.read_message(), timeout=self.service.client_idle_timeout
                    )
                except asyncio.TimeoutError as error:
                    raise TransportIOError("admitted client connection idle timeout") from error
            else:
                message = await self.stream.read_message()
            # A full queue pauses reading: the peer is slowed by TCP flow control
            # until this connection's messages are processed.  Memory stays bounded
            # either way, and closing the connection instead dropped workers whenever
            # they answered a burst of requests faster than it was handled (e.g. the
            # object releases after a large run).
            await self.inbound.put(message)

    async def _processor_loop(self) -> None:
        while True:
            message = await self.inbound.get()
            try:
                await self.service._handle_message(self, message)
                self.note_package_response(message)
            finally:
                self.inbound.task_done()

    async def _write_package_delivery(self, delivery: _PackageDelivery) -> None:
        prepare, artifact = delivery.prepare, delivery.artifact
        assert prepare.program.package_id == artifact.package_id
        common = dict(correlation_id=prepare.message_id)
        start = p.PackageTransferStart(
            worker_id=prepare.worker_id, plan_id=prepare.plan_id,
            program_id=prepare.program.id, package_id=artifact.package_id,
            size_bytes=len(artifact.archive_bytes), archive_sha256=artifact.archive_sha256,
            message_id=new_transport_id("package-start"), **common,
        )
        await write_message(self.writer, start, timeout=self.service.limits.write_timeout)
        offset = 0
        data = artifact.archive_bytes
        while offset < len(data):
            chunk = data[offset:offset + self.service.package_chunk_bytes]
            message = p.PackageTransferChunk(
                worker_id=prepare.worker_id, package_id=artifact.package_id, offset=offset,
                data_hex=chunk.hex(), message_id=new_transport_id("package-chunk"), **common,
            )
            await write_message(self.writer, message, timeout=self.service.limits.write_timeout)
            offset += len(chunk)
        end = p.PackageTransferEnd(
            worker_id=prepare.worker_id, package_id=artifact.package_id,
            size_bytes=len(data), archive_sha256=artifact.archive_sha256,
            message_id=new_transport_id("package-end"), **common,
        )
        await write_message(self.writer, end, timeout=self.service.limits.write_timeout)

    async def _writer_loop(self) -> None:
        while True:
            message = await self.outbound.get()
            try:
                if message is None:
                    return
                if isinstance(message, _PackageDelivery):
                    await self._write_package_delivery(message)
                else:
                    await write_message(
                        self.writer, message, timeout=self.service.limits.write_timeout
                    )
            finally:
                self.outbound.task_done()
                # A queue slot is now available; promptly drain another prefix
                # from the coordinator-owned backlog instead of waiting a tick.
                self.service._outbox_wakeup.set()
