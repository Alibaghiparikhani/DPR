"""Real worker control client; user code remains isolated in worker runtime processes."""
from __future__ import annotations

import asyncio
from contextlib import suppress
from dataclasses import dataclass
import secrets
import logging
from typing import Callable, TYPE_CHECKING

import protocol as p
from runtime_security import AuthChallenge, NodeAuthenticator, TlsPolicy
from scheduler import WorkerState

from networking.common import FramedProtocolStream, close_writer, new_transport_id, write_message
from networking.config import TransportLimits
if TYPE_CHECKING:
    from .runtime import WorkerExecutionRuntime

LOG = logging.getLogger(__name__)

from networking.errors import (
    AdmissionError,
    ApplicationAuthenticationError,
    BackpressureError,
    ProtocolTransportError,
    TransportIOError,
    WorkerNotAdmitted,
)


@dataclass(frozen=True, slots=True)
class WorkerReconnectPolicy:
    # None is the deployment-safe default: retry indefinitely. Tests and callers
    # that need a bounded failure path can still pass an explicit integer.
    max_attempts: int | None = None
    delay_seconds: float = 0.25
    max_delay_seconds: float = 30.0

    def __post_init__(self) -> None:
        if self.max_attempts is not None and (type(self.max_attempts) is not int or self.max_attempts < 0):
            raise ValueError("max_attempts must be None or a non-negative integer")
        if type(self.delay_seconds) not in (int, float) or self.delay_seconds < 0:
            raise ValueError("delay_seconds must be non-negative")
        if type(self.max_delay_seconds) not in (int, float) or self.max_delay_seconds < 0:
            raise ValueError("max_delay_seconds must be non-negative")


@dataclass(frozen=True, slots=True)
# Callers may still opt into a bounded retry budget explicitly. The CLI mirrors
# this module's None default; tests pass small integer limits when they need a
# deterministic terminal failure path.


class WorkerControlConfig:
    node_id: str
    secret: bytes
    coordinator_host: str
    coordinator_port: int
    server_hostname: str
    endpoint: p.WorkerEndpoint
    initial_state: WorkerState
    tls_policy: TlsPolicy
    heartbeat_interval: float = 1.0
    reconnect: WorkerReconnectPolicy = WorkerReconnectPolicy()
    limits: TransportLimits = TransportLimits()

    def __post_init__(self) -> None:
        if not isinstance(self.node_id, str) or not self.node_id:
            raise ValueError("node_id must be nonempty text")
        if not isinstance(self.secret, bytes) or len(self.secret) < 32:
            raise ValueError("secret must contain at least 32 bytes")
        if self.endpoint.worker_id != self.node_id or self.initial_state.worker_id != self.node_id:
            raise ValueError("worker endpoint/state must match node_id")
        if type(self.coordinator_port) is not int or not 1 <= self.coordinator_port <= 65535:
            raise ValueError("coordinator_port must be in 1..65535")
        if not isinstance(self.coordinator_host, str) or not self.coordinator_host:
            raise ValueError("coordinator_host must be nonempty text")
        if not isinstance(self.server_hostname, str) or not self.server_hostname:
            raise ValueError("server_hostname must be nonempty text")
        if type(self.heartbeat_interval) not in (int, float) or self.heartbeat_interval <= 0:
            raise ValueError("heartbeat_interval must be positive")


@dataclass(frozen=True, slots=True)
class WorkerControlSession:
    worker_id: str
    session_id: str


class WorkerControlClient:
    """TLS/authenticated worker control loop with an optional Batch-2 runtime."""

    def __init__(
        self,
        config: WorkerControlConfig,
        *,
        state_provider: Callable[[], WorkerState] | None = None,
        runtime: "WorkerExecutionRuntime | None" = None,
    ) -> None:
        self.config = config
        self._state_provider = state_provider or (lambda: config.initial_state)
        self.runtime = runtime
        self._stop = asyncio.Event()
        self.active = asyncio.Event()
        self.session: WorkerControlSession | None = None
        self.members: tuple[p.WorkerEndpoint, ...] = ()
        self.last_ack_sequence = -1
        self.last_error: BaseException | None = None
        # Set once the coordinator has refused this worker; final, unlike last_error,
        # which later background failures may overwrite.
        self.not_admitted: WorkerNotAdmitted | None = None
        self.received_commands: asyncio.Queue[p.Message] = asyncio.Queue(
            maxsize=config.limits.inbound_queue_messages
        )
        self._outbound: asyncio.Queue[p.Message] | None = None
        self._session_ended: asyncio.Event | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._sequence = 0
        self._established_sessions = 0

    async def run(self) -> None:
        failures = 0
        while not self._stop.is_set():
            established_before = self._established_sessions
            try:
                await self._connect_once()
                failures = 0
            except asyncio.CancelledError:
                raise
            except WorkerNotAdmitted as error:
                self.last_error = self.not_admitted = error
                LOG.error("worker %s is not a member of this cluster", self.config.node_id)
                break
            except BaseException as error:
                self.last_error = error
                if self._stop.is_set():
                    break
                if self._established_sessions != established_before:
                    # F33: a session that reached WorkerAccepted was healthy; a
                    # later transport drop starts a fresh reconnect budget rather
                    # than consuming a lifetime failure counter.
                    failures = 0
                else:
                    failures += 1
                limit = self.config.reconnect.max_attempts
                limit_text = "unbounded" if limit is None else str(limit + 1)
                LOG.warning(
                    "worker control connection failed worker=%s attempt=%d/%s error=%s",
                    self.config.node_id, failures + 1, limit_text, type(error).__name__,
                )
                if limit is not None and failures > limit:
                    break
                if failures <= 1:
                    delay = self.config.reconnect.delay_seconds
                else:
                    delay = min(
                        self.config.reconnect.delay_seconds * (2 ** (failures - 1)),
                        self.config.reconnect.max_delay_seconds,
                    )
                if delay:
                    await asyncio.sleep(delay)
        self.active.clear()

    async def stop(self) -> None:
        """Request a clean goodbye from the active session, or close a pending connect."""
        self._stop.set()
        if not self.active.is_set() and self._writer is not None:
            with suppress(Exception):
                await close_writer(self._writer, timeout=self.config.limits.write_timeout)

    async def _connect_once(self) -> None:
        context = self.config.tls_policy.build_client_context()
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(
                    self.config.coordinator_host,
                    self.config.coordinator_port,
                            ssl=context,
                    server_hostname=self.config.server_hostname,
                    ssl_handshake_timeout=self.config.limits.handshake_timeout,
                    limit=self.config.limits.stream_buffer_limit,
                ),
                timeout=self.config.limits.handshake_timeout,
            )
        except (OSError, asyncio.TimeoutError) as error:
            raise TransportIOError("TLS control connection failed") from error
        self._writer = writer
        stream = FramedProtocolStream(reader, read_chunk_size=self.config.limits.read_chunk_size)
        try:
            accepted = await self._authenticate_and_register(stream, writer)
            self.session = WorkerControlSession(self.config.node_id, accepted.session_id)
            LOG.info("worker session active worker=%s session=%s",
                     self.config.node_id, accepted.session_id)
            self.members = accepted.members
            self._sequence = 0
            self.last_ack_sequence = -1
            self._outbound = asyncio.Queue(maxsize=self.config.limits.outbound_queue_messages)
            self._session_ended = asyncio.Event()
            if self.runtime is not None:
                await self.runtime.session_started(
                    accepted.session_id, self._queue_outbound, self._abort_runtime_session
                )
            self.active.set()
            self._established_sessions += 1
            await self._active_loop(stream, writer)
        finally:
            self.active.clear()
            # Release anything waiting for room in the outbound queue first: its
            # writer is gone, and cleanup below may wait for those senders.
            if self._session_ended is not None:
                self._session_ended.set()
            if self.session is not None:
                LOG.info("worker session closing worker=%s session=%s",
                         self.config.node_id, self.session.session_id)
            if self.runtime is not None and self.session is not None:
                with suppress(Exception):
                    await self.runtime.session_lost(self.session.session_id)
            self._outbound = None
            with suppress(Exception):
                await close_writer(writer, timeout=self.config.limits.write_timeout)
            if self._writer is writer:
                self._writer = None


    def _abort_runtime_session(self, error: BaseException) -> None:
        """Fail closed when a background runtime cannot publish a control event."""
        self.last_error = error
        writer = self._writer
        if writer is None:
            return
        transport = getattr(writer, "transport", None)
        if transport is not None:
            transport.abort()
        else:
            writer.close()

    async def _read_handshake(self, stream: FramedProtocolStream) -> p.Message:
        try:
            return await asyncio.wait_for(
                stream.read_message(), timeout=self.config.limits.handshake_timeout
            )
        except asyncio.TimeoutError as error:
            raise ApplicationAuthenticationError("worker handshake timed out") from error

    async def _authenticate_and_register(
        self, stream: FramedProtocolStream, writer: asyncio.StreamWriter
    ) -> p.WorkerAccepted:
        intent = f"intent-{secrets.token_hex(16)}"
        request = p.AuthenticationRequest(
            node_id=self.config.node_id,
            session_id=intent,
            message_id=new_transport_id("auth-request"),
        )
        await write_message(writer, request, timeout=self.config.limits.write_timeout)
        challenge_message = await self._read_handshake(stream)
        if not isinstance(challenge_message, p.AuthenticationChallenge):
            raise ApplicationAuthenticationError("coordinator did not issue authentication challenge")
        if (challenge_message.correlation_id != request.message_id
                or challenge_message.node_id != self.config.node_id
                or challenge_message.session_id != intent):
            raise ApplicationAuthenticationError("authentication challenge binding mismatch")
        challenge = AuthChallenge(
            challenge_message.node_id,
            challenge_message.session_id,
            challenge_message.nonce,
            challenge_message.issued_at,
        )
        prover = NodeAuthenticator({self.config.node_id: self.config.secret})
        proof = prover.prove(challenge)
        proof_message = p.AuthenticationProof(
            node_id=proof.node_id,
            session_id=proof.session_id,
            nonce=proof.nonce,
            issued_at=proof.issued_at,
            mac_hex=proof.mac_hex,
            message_id=new_transport_id("auth-proof"),
            correlation_id=challenge_message.message_id,
        )
        await write_message(writer, proof_message, timeout=self.config.limits.write_timeout)
        auth_accepted = await self._read_handshake(stream)
        if not isinstance(auth_accepted, p.AuthenticationAccepted):
            raise ApplicationAuthenticationError("coordinator did not accept authentication")
        if (auth_accepted.correlation_id != proof_message.message_id
                or auth_accepted.node_id != self.config.node_id
                or auth_accepted.session_id != intent):
            raise ApplicationAuthenticationError("authentication acceptance binding mismatch")

        if self.runtime is not None:
            await self.runtime.refresh_prepared_cache()
            await self.runtime.ensure_data_plane_listener(self.config.endpoint)
        state = self._current_state()
        hello = p.WorkerHello(
            worker=state,
            endpoint=self.config.endpoint,
            message_id=new_transport_id("worker-hello"),
        )
        await write_message(writer, hello, timeout=self.config.limits.write_timeout)
        accepted = await self._read_handshake(stream)
        if (isinstance(accepted, p.WorkerRejected)
                and accepted.code == p.RejectionCode.NOT_ADMITTED
                and accepted.worker_id == self.config.node_id
                and accepted.correlation_id == hello.message_id):
            raise WorkerNotAdmitted(accepted.detail or "not a member of this cluster")
        if not isinstance(accepted, p.WorkerAccepted):
            raise AdmissionError("coordinator did not accept WorkerHello")
        if accepted.worker_id != self.config.node_id or accepted.correlation_id != hello.message_id:
            raise AdmissionError("WorkerAccepted binding mismatch")
        return accepted

    def _current_state(self) -> WorkerState:
        state = self._state_provider()
        if not isinstance(state, WorkerState) or state.worker_id != self.config.node_id:
            raise ValueError("state_provider returned invalid worker identity")
        if self.runtime is not None:
            state = self.runtime.decorate_state(state)
        return state

    async def _queue_outbound(self, message: p.Message) -> None:
        queue, ended = self._outbound, self._session_ended
        if queue is None or ended is None or ended.is_set():
            raise TransportIOError("worker control session is not active")
        try:
            queue.put_nowait(message)
            return
        except asyncio.QueueFull:
            pass
        # Wait for room rather than fail: the writer drains this queue as fast as
        # the coordinator reads, and the coordinator slows its own sending in turn.
        # Failing here dropped the connection on an ordinary burst of replies.
        put = asyncio.ensure_future(queue.put(message))
        stop = asyncio.ensure_future(ended.wait())
        try:
            await asyncio.wait({put, stop}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            stop.cancel()
            if not put.done():
                put.cancel()
        if put.cancelled() or not put.done():
            raise TransportIOError("worker control session ended")
        put.result()

    async def _active_loop(
        self, stream: FramedProtocolStream, writer: asyncio.StreamWriter
    ) -> None:
        tasks = {
            asyncio.create_task(self._reader_loop(stream), name="worker-control-reader"),
            asyncio.create_task(self._writer_loop(writer), name="worker-control-writer"),
            asyncio.create_task(self._heartbeat_loop(), name="worker-heartbeats"),
            asyncio.create_task(self._stop_loop(), name="worker-stop"),
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
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        if failure is not None:
            raise failure

    async def _reader_loop(self, stream: FramedProtocolStream) -> None:
        while True:
            message = await stream.read_message()
            if isinstance(message, p.HeartbeatAck):
                if message.worker_id != self.config.node_id:
                    raise AdmissionError("heartbeat acknowledgement worker mismatch")
                if message.sequence <= self._sequence - 1:
                    self.last_ack_sequence = max(self.last_ack_sequence, message.sequence)
                continue
            if isinstance(message, p.MembershipUpdate):
                if self.session is None or message.worker_id != self.config.node_id:
                    raise AdmissionError("membership update worker mismatch")
                if message.session_id != self.session.session_id:
                    raise AdmissionError("membership update session mismatch")
                self.members = message.members
                continue
            if isinstance(message, (p.AuthenticationRequest, p.AuthenticationChallenge,
                                    p.AuthenticationProof, p.AuthenticationAccepted,
                                    p.WorkerAccepted, p.WorkerRejected)):
                raise ProtocolTransportError("handshake message received in active session")
            if self.runtime is not None and self.session is not None:
                # F35: WorkerExecutionRuntime hands CancelTask physical cleanup to
                # a bounded background job, so awaiting this dispatcher never
                # head-of-line blocks unrelated control messages on kill waits.
                if await self.runtime.handle_message(message, session_id=self.session.session_id):
                    continue
            try:
                self.received_commands.put_nowait(message)
            except asyncio.QueueFull as error:
                raise BackpressureError("worker command queue exhausted") from error

    async def _writer_loop(self, writer: asyncio.StreamWriter) -> None:
        assert self._outbound is not None
        while True:
            message = await self._outbound.get()
            try:
                await write_message(writer, message, timeout=self.config.limits.write_timeout)
            finally:
                self._outbound.task_done()

    async def _heartbeat_loop(self) -> None:
        assert self._outbound is not None
        while True:
            # F36: package verification is intentionally off the heartbeat hot path.
            # Preparation verifies at install time and dispatch verifies before use.
            heartbeat = p.Heartbeat(
                worker=self._current_state(),
                sequence=self._sequence,
                message_id=new_transport_id("heartbeat"),
            )
            self._sequence += 1
            # A full queue means messages are flowing, and every message proves the
            # worker alive; this beat is skipped rather than ending the session.
            with suppress(asyncio.QueueFull):
                self._outbound.put_nowait(heartbeat)
            await asyncio.sleep(self.config.heartbeat_interval)

    async def _stop_loop(self) -> None:
        await self._stop.wait()
        # A graceful goodbye must not advertise physical worker loss before this
        # session's isolated children have actually been terminated and reaped.
        # Abrupt transport loss cannot provide that ordering, but clean shutdown can.
        if self.runtime is not None and self.session is not None:
            cleaned = await self.runtime.session_lost(self.session.session_id)
            if not cleaned:
                self._abort_runtime_session(
                    TransportIOError("isolated child cleanup could not be established during shutdown")
                )
                raise TransportIOError(
                    "isolated child cleanup could not be established during shutdown"
                )
        if self._outbound is not None:
            goodbye = p.WorkerGoodbye(
                worker_id=self.config.node_id,
                reason="worker shutdown",
                message_id=new_transport_id("worker-goodbye"),
            )
            try:
                self._outbound.put_nowait(goodbye)
            except asyncio.QueueFull as error:
                raise BackpressureError("worker outbound queue exhausted during shutdown") from error
            await asyncio.wait_for(self._outbound.join(), timeout=self.config.limits.write_timeout)
