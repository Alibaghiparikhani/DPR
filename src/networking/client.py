"""Authenticated operator client for submission, inspection and cancellation.

This is a control-plane client.  It never accepts worker task results or relays
runtime object payloads; package bytes are the only bulk-ish content it sends,
and those use the bounded deterministic package submission records.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
import hashlib
import secrets

import protocol as p
from runtime_security import AuthChallenge, NodeAuthenticator, TlsPolicy

from .common import FramedProtocolStream, close_writer, new_transport_id, write_message
from .config import TransportLimits
from .errors import AdmissionError, ApplicationAuthenticationError, TransportIOError


@dataclass(frozen=True, slots=True)
class CoordinatorClientConfig:
    node_id: str
    secret: bytes
    coordinator_host: str
    coordinator_port: int
    server_hostname: str
    tls_policy: TlsPolicy
    limits: TransportLimits = TransportLimits()
    operation_timeout: float = 30.0

    def __post_init__(self) -> None:
        if not isinstance(self.node_id, str) or not self.node_id:
            raise ValueError("node_id must be nonempty text")
        if not isinstance(self.secret, bytes) or len(self.secret) < 32:
            raise ValueError("secret must contain at least 32 bytes")
        if not isinstance(self.coordinator_host, str) or not self.coordinator_host:
            raise ValueError("coordinator_host must be nonempty text")
        if type(self.coordinator_port) is not int or not 1 <= self.coordinator_port <= 65535:
            raise ValueError("coordinator_port must be in 1..65535")
        if not isinstance(self.server_hostname, str) or not self.server_hostname:
            raise ValueError("server_hostname must be nonempty text")
        if not isinstance(self.operation_timeout, (int, float)) or isinstance(self.operation_timeout, bool) or self.operation_timeout <= 0:
            raise ValueError("operation_timeout must be a positive number")


class ClientOperationError(RuntimeError):
    def __init__(self, operation: str, code: str, detail: str) -> None:
        super().__init__(f"{operation}: {code}: {detail}")
        self.operation = operation
        self.code = code
        self.detail = detail


class CoordinatorClient:
    """One sequential authenticated operator session.

    Operations are intentionally serialized on a connection.  This avoids a
    second client-side correlation dispatcher and keeps queue/resource ownership
    explicit.  Callers needing concurrency can open multiple authenticated
    client sessions.
    """

    def __init__(self, config: CoordinatorClientConfig) -> None:
        self.config = config
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._stream: FramedProtocolStream | None = None
        self.session_id: str | None = None
        # Each operation is a request/response (or a multi-frame submit) exchange on
        # one stream.  Without this lock, concurrent callers interleave frames and
        # asyncio raises "read() called while another coroutine is already waiting",
        # leaving the session unusable.  Serializing here makes the documented
        # single-flight contract real instead of advisory.
        self._operation_lock = asyncio.Lock()

    async def connect(self) -> None:
        if self._writer is not None:
            raise RuntimeError("client is already connected")
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
            raise TransportIOError("TLS client control connection failed") from error
        self._reader, self._writer = reader, writer
        self._stream = FramedProtocolStream(
            reader, read_chunk_size=self.config.limits.read_chunk_size
        )
        try:
            await self._authenticate()
        except BaseException:
            await self.close()
            raise

    async def close(self) -> None:
        writer = self._writer
        self._reader = None
        self._writer = None
        self._stream = None
        self.session_id = None
        if writer is not None:
            await close_writer(writer, timeout=self.config.limits.write_timeout)

    async def __aenter__(self) -> "CoordinatorClient":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def _read(self, *, timeout: float | None = None) -> p.Message:
        if self._stream is None:
            raise TransportIOError("client is not connected")
        effective_timeout = self.config.operation_timeout if timeout is None else timeout
        try:
            return await asyncio.wait_for(
                self._stream.read_message(), timeout=effective_timeout
            )
        except asyncio.TimeoutError as error:
            raise TransportIOError("coordinator response timed out") from error

    async def _write(self, message: p.Message) -> None:
        if self._writer is None:
            raise TransportIOError("client is not connected")
        await write_message(
            self._writer, message, timeout=self.config.limits.write_timeout
        )

    async def _authenticate(self) -> None:
        intent = f"client-intent-{secrets.token_hex(16)}"
        request = p.AuthenticationRequest(
            node_id=self.config.node_id,
            session_id=intent,
            message_id=new_transport_id("client-auth-request"),
        )
        await self._write(request)
        challenge_message = await self._read(timeout=self.config.limits.handshake_timeout)
        if not isinstance(challenge_message, p.AuthenticationChallenge):
            raise ApplicationAuthenticationError("coordinator did not issue authentication challenge")
        if (
            challenge_message.correlation_id != request.message_id
            or challenge_message.node_id != self.config.node_id
            or challenge_message.session_id != intent
        ):
            raise ApplicationAuthenticationError("authentication challenge binding mismatch")
        challenge = AuthChallenge(
            challenge_message.node_id,
            challenge_message.session_id,
            challenge_message.nonce,
            challenge_message.issued_at,
        )
        proof = NodeAuthenticator({self.config.node_id: self.config.secret}).prove(challenge)
        proof_message = p.AuthenticationProof(
            node_id=proof.node_id,
            session_id=proof.session_id,
            nonce=proof.nonce,
            issued_at=proof.issued_at,
            mac_hex=proof.mac_hex,
            message_id=new_transport_id("client-auth-proof"),
            correlation_id=challenge_message.message_id,
        )
        await self._write(proof_message)
        accepted = await self._read(timeout=self.config.limits.handshake_timeout)
        if not isinstance(accepted, p.AuthenticationAccepted):
            raise ApplicationAuthenticationError("coordinator rejected client authentication")
        if (
            accepted.correlation_id != proof_message.message_id
            or accepted.node_id != self.config.node_id
            or accepted.session_id != intent
        ):
            raise ApplicationAuthenticationError("authentication acceptance binding mismatch")
        hello = p.ClientHello(
            client_id=self.config.node_id,
            message_id=new_transport_id("client-hello"),
        )
        await self._write(hello)
        admitted = await self._read(timeout=self.config.limits.handshake_timeout)
        if not isinstance(admitted, p.ClientAccepted):
            raise AdmissionError("coordinator did not admit client session")
        if (
            admitted.correlation_id != hello.message_id
            or admitted.client_id != self.config.node_id
            or admitted.session_id != intent
        ):
            raise AdmissionError("client admission binding mismatch")
        self.session_id = admitted.session_id

    @staticmethod
    def _raise_if_failed(message: p.Message) -> None:
        if isinstance(message, p.ClientOperationFailed):
            raise ClientOperationError(message.operation, message.code, message.detail)

    async def submit_package(
        self,
        *,
        run_id: str,
        environment_id: str,
        entrypoint: str,
        plan_id: str,
        package_id: str,
        archive_bytes: bytes,
        chunk_bytes: int = 32 * 1024,
    ) -> p.RunSubmitted:
        async with self._operation_lock:
            if type(archive_bytes) is not bytes:
                raise TypeError("archive_bytes must be bytes")
            if type(chunk_bytes) is not int or not 1 <= chunk_bytes <= 32 * 1024:
                raise ValueError("chunk_bytes must be in 1..32768")
            digest = hashlib.sha256(archive_bytes).hexdigest()
            start = p.RunSubmitStart(
                client_id=self.config.node_id,
                run_id=run_id,
                environment_id=environment_id,
                entrypoint=entrypoint,
                plan_id=plan_id,
                package_id=package_id,
                archive_size=len(archive_bytes),
                archive_sha256=digest,
                message_id=new_transport_id("run-submit-start"),
            )
            await self._write(start)
            ready = await self._read()
            self._raise_if_failed(ready)
            if (not isinstance(ready, p.RunSubmitReady)
                    or ready.correlation_id != start.message_id
                    or ready.run_id != run_id
                    or ready.package_id != package_id):
                raise AdmissionError("run submission readiness correlation mismatch")
            offset = 0
            while offset < len(archive_bytes):
                chunk = archive_bytes[offset : offset + chunk_bytes]
                await self._write(p.RunSubmitChunk(
                    client_id=self.config.node_id,
                    run_id=run_id,
                    package_id=package_id,
                    offset=offset,
                    data_hex=chunk.hex(),
                    message_id=new_transport_id("run-submit-chunk"),
                    correlation_id=start.message_id,
                ))
                offset += len(chunk)
            end = p.RunSubmitEnd(
                client_id=self.config.node_id,
                run_id=run_id,
                package_id=package_id,
                archive_size=len(archive_bytes),
                archive_sha256=digest,
                message_id=new_transport_id("run-submit-end"),
                correlation_id=start.message_id,
            )
            await self._write(end)
            # Start/chunk failures can be queued before the end response.  Stop at
            # the first explicit failure or the terminal submission acknowledgement.
            while True:
                response = await self._read()
                self._raise_if_failed(response)
                if isinstance(response, p.RunSubmitted):
                    if response.correlation_id != end.message_id or response.run_id != run_id:
                        raise AdmissionError("run submission response correlation mismatch")
                    return response
                raise AdmissionError("unexpected coordinator response during submission")

    async def run_status(self, run_id: str, *, include_tasks: bool = True) -> p.RunStatusResponse:
        async with self._operation_lock:
            request = p.RunStatusRequest(
                client_id=self.config.node_id,
                run_id=run_id,
                include_tasks=include_tasks,
                message_id=new_transport_id("run-status-request"),
            )
            await self._write(request)
            response = await self._read()
            self._raise_if_failed(response)
            if not isinstance(response, p.RunStatusResponse) or response.correlation_id != request.message_id:
                raise AdmissionError("run status response correlation mismatch")
            return response

    async def run_status_resilient(
        self, run_id: str, *, include_tasks: bool = True
    ) -> p.RunStatusResponse:
        """Query status, reconnecting through a coordinator restart if needed."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.config.operation_timeout
        last_error: BaseException | None = None
        while True:
            try:
                if self._writer is None:
                    await self.connect()
                return await self.run_status(run_id, include_tasks=include_tasks)
            except (EOFError, TransportIOError) as error:
                last_error = error
                await self.close()
                if loop.time() >= deadline:
                    raise TransportIOError(
                        "coordinator unavailable while recovering run status"
                    ) from last_error
                await asyncio.sleep(min(0.1, max(0.0, deadline - loop.time())))

    async def cluster_status(self) -> p.ClusterStatusResponse:
        async with self._operation_lock:
            request = p.ClusterStatusRequest(
                client_id=self.config.node_id,
                message_id=new_transport_id("cluster-status-request"),
            )
            await self._write(request)
            response = await self._read()
            self._raise_if_failed(response)
            if not isinstance(response, p.ClusterStatusResponse) or response.correlation_id != request.message_id:
                raise AdmissionError("cluster status response correlation mismatch")
            return response

    async def cancel_run(self, run_id: str, *, reason: str = "run cancelled by client") -> p.CancelRunResponse:
        async with self._operation_lock:
            request = p.CancelRunRequest(
                client_id=self.config.node_id,
                run_id=run_id,
                reason=reason,
                message_id=new_transport_id("cancel-run-request"),
            )
            await self._write(request)
            response = await self._read()
            self._raise_if_failed(response)
            if not isinstance(response, p.CancelRunResponse) or response.correlation_id != request.message_id:
                raise AdmissionError("cancel response correlation mismatch")
            return response
