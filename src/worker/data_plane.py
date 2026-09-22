"""Authenticated direct worker-to-worker runtime data transfer.

This is a byte plane beneath the existing coordinator TransferIdentity state
machine.  It deliberately has no task retry or logical transfer policy of its
own.  The control plane authorizes an exact transfer attempt; this module only
moves and verifies the corresponding opaque runtime representation.
"""
from __future__ import annotations

import asyncio
from contextlib import suppress
from dataclasses import dataclass, field
import hashlib
import json
import os
from pathlib import Path
import socket
import ssl
import struct
import time
from typing import Awaitable, Callable

import protocol as p
from runtime_security import TlsPolicy
from scheduler import DataForm

from .data_store import DataStoreError, DataStoreFull, LocalDataStore, StoredData


class DataPlaneError(RuntimeError):
    pass


class DataPlaneAuthenticationError(DataPlaneError):
    pass


class DataPlaneAuthorizationError(DataPlaneError):
    pass


class DataPlaneIntegrityError(DataPlaneError):
    pass


class DataPlaneResourceError(DataPlaneError):
    pass


@dataclass(frozen=True, slots=True)
class DataPlaneLimits:
    max_transfer_bytes: int = 256 * 1024 * 1024
    chunk_bytes: int = 64 * 1024
    header_bytes: int = 64 * 1024
    max_incoming: int = 4
    # Preparations are bookkeeping (a dict entry and a timeout task), while
    # `max_incoming` bounds concurrent byte streams.  Capping preparations at the
    # stream limit made an ordinary fan-in -- N results converging on one task --
    # fail with "incoming transfer capacity exhausted" instead of queueing.
    max_prepared_receives: int = 64
    max_outgoing: int = 4
    connect_timeout: float = 5.0
    handshake_timeout: float = 5.0
    idle_timeout: float = 10.0
    total_timeout: float | None = 120.0
    cleanup_timeout: float = 2.0
    # The same holds for the sending side: `max_outgoing` bounds concurrent byte
    # streams (the send semaphore), while up to `max_prepared_sends` requests may
    # wait their turn.  Admitting only as many as could stream at once made a
    # fan-out of more than four values from one worker fail instead of queueing.
    # (Last, so positional construction of the earlier fields is unchanged.)
    max_prepared_sends: int = 64
    # `total_timeout` is extended by the value's size at this rate, so a large
    # value on a slow network is not cut off while it is still moving; one that
    # stops moving is caught by `idle_timeout` either way.
    min_bytes_per_second: int = 256 * 1024

    def __post_init__(self) -> None:
        for name in ("max_transfer_bytes", "chunk_bytes", "header_bytes", "max_incoming",
                     "max_outgoing", "max_prepared_receives", "max_prepared_sends",
                     "min_bytes_per_second"):
            value = getattr(self, name)
            if type(value) is not int or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        if self.chunk_bytes > self.max_transfer_bytes:
            raise ValueError("chunk_bytes cannot exceed max_transfer_bytes")
        for name in ("connect_timeout", "handshake_timeout", "idle_timeout", "cleanup_timeout"):
            value = getattr(self, name)
            if type(value) not in (int, float) or value <= 0:
                raise ValueError(f"{name} must be positive")
        if self.total_timeout is not None and (
            type(self.total_timeout) not in (int, float) or self.total_timeout <= 0
        ):
            raise ValueError("total_timeout must be positive or None")

    def deadline_for(self, size_bytes: int | None) -> float | None:
        """Whole-transfer deadline for a value of this size (None: unbounded)."""
        if self.total_timeout is None:
            return None
        return self.total_timeout + (size_bytes or 0) / self.min_bytes_per_second


@dataclass(frozen=True, slots=True)
class WorkerDataPlaneConfig:
    worker_id: str
    host: str
    port: int
    tls_policy: TlsPolicy
    limits: DataPlaneLimits = DataPlaneLimits()

    def __post_init__(self) -> None:
        if not isinstance(self.worker_id, str) or not self.worker_id:
            raise ValueError("worker_id must be nonempty text")
        if not isinstance(self.host, str) or not self.host:
            raise ValueError("host must be nonempty text")
        if type(self.port) is not int or not 0 <= self.port <= 65535:
            raise ValueError("port must be in 0..65535")
        if not isinstance(self.tls_policy, TlsPolicy):
            raise TypeError("tls_policy must be TlsPolicy")


@dataclass(slots=True)
class PreparedReceive:
    command: p.PrepareReceive
    session_id: str
    correlation_id: str
    created_at: float = field(default_factory=time.monotonic)
    ready_sent: bool = False
    cancelled: bool = False
    task: asyncio.Task[None] | None = None
    connection_task: asyncio.Task[None] | None = None
    writer: asyncio.StreamWriter | None = None
    temp_path: Path | None = None
    cleanup_reported: bool = False
    cancel_detail: str = "transfer cancelled"


@dataclass(slots=True)
class ActiveSend:
    command: p.TransferRequest
    session_id: str
    correlation_id: str
    created_at: float = field(default_factory=time.monotonic)
    cancelled: bool = False
    task: asyncio.Task[None] | None = None
    writer: asyncio.StreamWriter | None = None
    cleanup_reported: bool = False
    cancel_detail: str = "transfer cancelled"


TransferCallback = Callable[[p.TransferIdentity, int], Awaitable[None]]
FailureCallback = Callable[[p.TransferIdentity, bool, str], Awaitable[None]]
StartedCallback = Callable[[p.TransferIdentity], Awaitable[None]]
SendFinishedCallback = Callable[[p.TransferIdentity], Awaitable[None]]


_HEADER = struct.Struct("!I")
_VERSION = 1


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _peer_identities(writer: asyncio.StreamWriter) -> frozenset[str]:
    ssl_object = writer.get_extra_info("ssl_object")
    if ssl_object is None:
        raise DataPlaneAuthenticationError("TLS is required for runtime data transfer")
    cert = ssl_object.getpeercert()
    if not cert:
        raise DataPlaneAuthenticationError("peer certificate unavailable")
    # RFC-style identity precedence: once the certificate carries a supported
    # subjectAltName, do not also treat the legacy Common Name as another
    # authorization identity.  This prevents one certificate from gaining an
    # unintended second cluster identity via a mismatched CN.
    san_identities = {
        value for kind, value in cert.get("subjectAltName", ())
        if kind in {"DNS", "URI"} and isinstance(value, str)
    }
    if san_identities:
        return frozenset(san_identities)
    common_names: set[str] = set()
    for rdn in cert.get("subject", ()):
        for key, value in rdn:
            if key == "commonName" and isinstance(value, str):
                common_names.add(value)
    return frozenset(common_names)


def _transfer_key(transfer: p.TransferIdentity) -> tuple[str, str]:
    return transfer.transfer_id, transfer.transfer_attempt_id


def _header_record(command: p.TransferRequest, entry: StoredData) -> dict[str, object]:
    t = command.transfer
    return {
        "version": _VERSION,
        "plan_id": t.data.plan_id,
        "run_id": t.data.run_id,
        "value_id": t.data.value_id,
        "form": t.data.form.value,
        "object_state_id": t.data.object_state_id,
        "transfer_id": t.transfer_id,
        "transfer_attempt_id": t.transfer_attempt_id,
        "source_worker_id": t.source_worker_id,
        "destination_worker_id": t.destination_worker_id,
        "source_session_id": command.source_session_id,
        "destination_session_id": command.destination_session_id,
        "authorization": command.authorization,
        "size_bytes": entry.size_bytes,
        "sha256": entry.sha256,
        "serialization": entry.serialization,
    }


class WorkerDataPlane:
    """One bounded TLS listener plus exact authorized transfer attempts."""

    def __init__(self, config: WorkerDataPlaneConfig, store: LocalDataStore) -> None:
        self.config = config
        self.store = store
        self._server: asyncio.AbstractServer | None = None
        self._session_id: str | None = None
        self._receives: dict[tuple[str, str], PreparedReceive] = {}
        self._sends: dict[tuple[str, str], ActiveSend] = {}
        self._connection_tasks: set[asyncio.Task[None]] = set()
        # F46: cap accepted TLS connections before allocating an unbounded number
        # of per-connection tasks/sockets. Keep a small handshake backlog above
        # the payload concurrency limit so idle handshakes cannot occupy every
        # receive slot.
        # Accepted sockets wait for a payload slot after authenticating; admit as
        # many as there may be prepared receives, so a wide fan-in queues rather
        # than having its senders hung up on.
        self._accepted_connection_limit = max(8, config.limits.max_incoming * 4,
                                              config.limits.max_prepared_receives)
        self._incoming_sem = asyncio.Semaphore(config.limits.max_incoming)
        self._outgoing_sem = asyncio.Semaphore(config.limits.max_outgoing)
        self._on_completed: TransferCallback | None = None
        self._on_failed: FailureCallback | None = None
        self._on_started: StartedCallback | None = None
        self._on_send_finished: SendFinishedCallback | None = None

    @property
    def listening_port(self) -> int:
        if self._server is None or not self._server.sockets:
            raise RuntimeError("data plane is not listening")
        return int(self._server.sockets[0].getsockname()[1])

    @property
    def active_receive_count(self) -> int:
        return len(self._receives)

    @property
    def active_send_count(self) -> int:
        return len(self._sends)

    async def listen(self) -> None:
        """Open the authenticated byte-plane listener before control admission.

        No transfer can be accepted until ``start`` binds a control session and a
        coordinator-issued PrepareReceive exists, so early listening does not grant
        data authority.
        """
        if self._server is not None:
            return
        context = self.config.tls_policy.build_server_context()
        self._server = await asyncio.start_server(
            self._connected,
            self.config.host,
            self.config.port,
            ssl=context,
            ssl_handshake_timeout=self.config.limits.handshake_timeout,
            limit=max(self.config.limits.chunk_bytes * 2, 64 * 1024),
        )

    async def start(
        self,
        session_id: str,
        *,
        on_completed: TransferCallback,
        on_failed: FailureCallback,
        on_started: StartedCallback,
        on_send_finished: SendFinishedCallback | None = None,
    ) -> None:
        if not session_id:
            raise ValueError("session_id must be nonempty")
        if self._session_id is not None and self._session_id != session_id:
            raise RuntimeError("data plane already belongs to another session")
        await self.listen()
        self._session_id = session_id
        self._on_completed = on_completed
        self._on_failed = on_failed
        self._on_started = on_started
        self._on_send_finished = on_send_finished

    async def stop_session(self, session_id: str) -> bool:
        if self._session_id != session_id:
            return True
        self._session_id = None
        receives = tuple(self._receives.values())
        sends = tuple(self._sends.values())
        for item in receives:
            item.cancelled = True
            item.cancel_detail = "worker control session lost"
            if item.writer is not None:
                self._abort_writer(item.writer)
            if item.task is not None and not item.task.done():
                item.task.cancel()
            if item.connection_task is not None and not item.connection_task.done():
                item.connection_task.cancel()
        for item in sends:
            item.cancelled = True
            item.cancel_detail = "worker control session lost"
            if item.writer is not None:
                self._abort_writer(item.writer)
            if item.task is not None and not item.task.done():
                item.task.cancel()
        tasks: list[asyncio.Task[None]] = []
        for item in receives:
            if item.connection_task is not None:
                tasks.append(item.connection_task)
            elif item.task is not None:
                tasks.append(item.task)
        tasks.extend(item.task for item in sends if item.task is not None)
        if tasks:
            done, pending = await asyncio.wait(tasks, timeout=self.config.limits.cleanup_timeout)
            for task in done:
                with suppress(BaseException):
                    task.result()
            for task in pending:
                task.cancel()
        for item in receives:
            relevant = item.connection_task if item.connection_task is not None else item.task
            if relevant is None or relevant.done():
                self._cleanup_receive(item)
                self._receives.pop(_transfer_key(item.command.transfer), None)
        for item in sends:
            if item.task is None or item.task.done():
                self._sends.pop(_transfer_key(item.command.transfer), None)
        return not any(not task.done() for task in tasks)

    async def close(self) -> None:
        if self._session_id is not None:
            await self.stop_session(self._session_id)
        if self._server is not None:
            server = self._server
            server.close()
            await server.wait_closed()
            self._server = None
        tasks = tuple(self._connection_tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        for item in tuple(self._receives.values()):
            self._cleanup_receive(item)
        self._receives.clear()
        self._sends.clear()

    async def prepare_receive(self, command: p.PrepareReceive, *, session_id: str) -> None:
        self._require_live_session(session_id)
        self._require_authorized_command(command, destination=True, session_id=session_id)
        key = _transfer_key(command.transfer)
        existing = self._receives.get(key)
        if existing is not None:
            if existing.command == command and existing.session_id == session_id:
                return
            raise DataPlaneAuthorizationError("conflicting receive preparation for transfer attempt")
        if len(self._receives) >= self.config.limits.max_prepared_receives:
            raise DataPlaneResourceError(
                f"too many transfers prepared at once (limit "
                f"{self.config.limits.max_prepared_receives})")
        if command.size_bytes is not None and command.size_bytes > self.config.limits.max_transfer_bytes:
            raise DataPlaneResourceError("expected transfer exceeds configured byte limit")
        correlation = command.message_id
        record = PreparedReceive(command, session_id, correlation)
        self._receives[key] = record
        record.task = asyncio.create_task(self._receive_timeout(record), name="p2p-receive-timeout")

    def mark_ready_sent(self, transfer: p.TransferIdentity) -> None:
        item = self._receives.get(_transfer_key(transfer))
        if item is not None:
            item.ready_sent = True

    async def send(self, command: p.TransferRequest, *, session_id: str) -> None:
        self._require_live_session(session_id)
        self._require_authorized_command(command, destination=False, session_id=session_id)
        key = _transfer_key(command.transfer)
        existing = self._sends.get(key)
        if existing is not None:
            if existing.command == command and existing.session_id == session_id:
                return
            raise DataPlaneAuthorizationError("conflicting source transfer request")
        # _send_transfer takes a stream slot before it opens a socket, so admitted
        # requests beyond the stream limit simply wait their turn.
        capacity = max(self.config.limits.max_outgoing, self.config.limits.max_prepared_sends)
        if len(self._sends) >= capacity:
            raise DataPlaneResourceError(f"too many transfers queued at once (limit {capacity})")
        record = ActiveSend(command, session_id, command.message_id)
        self._sends[key] = record
        record.task = asyncio.create_task(self._send_transfer(record), name="p2p-send")

    async def cancel(self, transfer: p.TransferIdentity, *, session_id: str) -> tuple[bool, bool]:
        """Request exact participant cleanup.

        The return value only says whether participant state existed. Physical
        cleanup evidence is emitted through ``on_failed`` *after* the corresponding
        sender/receiver coroutine has actually stopped. A missing participant is
        already physically clean and can be acknowledged by the caller directly.
        """
        self._require_live_session(session_id)
        key = _transfer_key(transfer)
        source = self._sends.get(key)
        destination = self._receives.get(key)
        if source is not None:
            source.cancelled = True
            source.cancel_detail = "transfer cancelled by coordinator"
            if source.writer is not None:
                self._abort_writer(source.writer)
            if source.task is not None and not source.task.done():
                source.task.cancel()
        if destination is not None:
            destination.cancelled = True
            destination.cancel_detail = "transfer cancelled by coordinator"
            if destination.writer is not None:
                self._abort_writer(destination.writer)
            if destination.task is not None and not destination.task.done():
                destination.task.cancel()
            if (destination.connection_task is not None
                    and not destination.connection_task.done()):
                destination.connection_task.cancel()
        tasks: list[asyncio.Task[None]] = []
        if source is not None and source.task is not None:
            tasks.append(source.task)
        if destination is not None:
            if destination.connection_task is not None:
                tasks.append(destination.connection_task)
            elif destination.task is not None:
                tasks.append(destination.task)
        if tasks:
            done, pending = await asyncio.wait(tasks, timeout=self.config.limits.cleanup_timeout)
            for task in done:
                with suppress(BaseException): task.result()
            for task in pending:
                task.cancel()
        # A send task cancelled before its first step never runs its `finally`, so
        # its record would stay queued for ever and never report its cleanup.
        if (source is not None and (source.task is None or source.task.done())
                and self._sends.get(key) is source):
            self._sends.pop(key, None)
            with suppress(Exception):
                await self._report_cancelled_cleanup(source, destination=False)
            finished = self._on_send_finished
            if finished is not None:
                with suppress(Exception):
                    await finished(source.command.transfer)
        # A prepared receiver with no accepted P2P socket has no remaining physical
        # byte-transfer resource after its timeout task is cancelled.
        if destination is not None and destination.connection_task is None:
            self._receives.pop(key, None)
            self._cleanup_receive(destination)
            await self._report_cancelled_cleanup(destination, destination=True)
        return source is not None, destination is not None

    def _connected(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        if len(self._connection_tasks) >= self._accepted_connection_limit:
            # The TLS handshake already completed before asyncio invokes this
            # callback. Refuse excess accepted sockets immediately, before creating
            # a task or consuming a payload receive slot.
            writer.close()
            return
        task = asyncio.create_task(self._receive_connection(reader, writer), name="p2p-receive")
        self._connection_tasks.add(task)
        task.add_done_callback(self._connection_tasks.discard)

    async def _receive_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        item: PreparedReceive | None = None
        try:
            # F38: authenticate the TLS identity against the set of workers that
            # currently have an exact coordinator-authorized receive before reading
            # application bytes or taking a payload slot. This is stronger than a
            # generic cluster-CA check: an operator certificate or an unrelated
            # worker is rejected even though its certificate chains to the CA.
            identities = _peer_identities(writer)
            expected_sources = {
                record.command.transfer.source_worker_id
                for record in self._receives.values()
                if not record.cancelled and record.session_id == self._session_id
            }
            if not identities.intersection(expected_sources):
                raise DataPlaneAuthenticationError(
                    "peer certificate identity is not an authorized live transfer source"
                )

            # Header parsing/authorization has its own short handshake budget and
            # does not consume max_incoming payload capacity.
            try:
                header = await asyncio.wait_for(
                    self._read_header(reader), self.config.limits.handshake_timeout
                )
            except asyncio.TimeoutError as error:
                raise DataPlaneAuthenticationError("runtime transfer header timed out") from error
            transfer = self._transfer_from_header(header)
            key = _transfer_key(transfer)
            candidate = self._receives.get(key)
            if candidate is None or candidate.cancelled:
                raise DataPlaneAuthorizationError("destination has no live preparation for transfer")
            self._validate_incoming_header(candidate, header, writer)

            async with self._incoming_sem:
                # Re-check after waiting for capacity: another connection/cancel may
                # have consumed or retired this exact transfer meanwhile.
                candidate = self._receives.get(key)
                if candidate is None or candidate.cancelled:
                    raise DataPlaneAuthorizationError("destination receive expired before payload admission")
                if candidate.connection_task not in (None, asyncio.current_task()):
                    raise DataPlaneAuthorizationError("destination transfer already has a live connection")
                self._validate_incoming_header(candidate, header, writer)
                item = candidate
                item.connection_task = asyncio.current_task()
                item.writer = writer
                size = header["size_bytes"]
                assert type(size) is int
                if size > self.config.limits.max_transfer_bytes:
                    raise DataPlaneResourceError("runtime payload exceeds configured transfer limit")
                if item.command.size_bytes is not None and size != item.command.size_bytes:
                    raise DataPlaneIntegrityError("runtime payload size differs from coordinator expectation")
                await self._write_header(writer, {"status": "ready", "version": _VERSION})
                temp = self.store.staging / f"receive-{os.urandom(16).hex()}.part"
                item.temp_path = temp
                digest = hashlib.sha256()
                received = 0
                with temp.open("xb") as handle:
                    os.chmod(temp, 0o600)
                    while received < size:
                        if item.cancelled:
                            raise asyncio.CancelledError
                        want = min(self.config.limits.chunk_bytes, size - received)
                        chunk = await asyncio.wait_for(reader.read(want), self.config.limits.idle_timeout)
                        if not chunk:
                            raise DataPlaneIntegrityError("runtime payload truncated")
                        handle.write(chunk)
                        digest.update(chunk)
                        received += len(chunk)
                    handle.flush(); os.fsync(handle.fileno())
                # F47: the authenticated size is the complete payload frame, not
                # merely a prefix length.  The source closes its one-transfer TLS
                # connection immediately after sending the declared bytes; require
                # that close here before publishing.  Any extra byte is trailing
                # data and therefore an integrity failure.
                try:
                    trailing = await asyncio.wait_for(
                        reader.read(1), self.config.limits.cleanup_timeout
                    )
                except asyncio.TimeoutError as error:
                    raise DataPlaneIntegrityError(
                        "runtime payload sender did not close after declared length"
                    ) from error
                if trailing:
                    raise DataPlaneIntegrityError("runtime payload has trailing bytes")
                if digest.hexdigest() != header["sha256"]:
                    raise DataPlaneIntegrityError("runtime payload digest mismatch")
                if item.cancelled or self._session_id != item.session_id:
                    raise asyncio.CancelledError
                self.store.publish_file(
                    transfer.data, temp, size_bytes=size, sha256=header["sha256"],
                    session_id=item.session_id, serialization=header["serialization"],
                )
                if item.cancelled or self._session_id != item.session_id:
                    self.store.release(transfer.data, session_id=item.session_id)
                    raise asyncio.CancelledError
                callback = self._on_completed
                if callback is None:
                    raise DataPlaneError("completion callback unavailable")
                await callback(transfer, size)
                self._receives.pop(key, None)
                self._cleanup_receive(item)
        except asyncio.CancelledError:
            raise
        except BaseException as error:
            if item is not None and not item.cancelled:
                callback = self._on_failed
                if callback is not None:
                    with suppress(Exception):
                        await callback(item.command.transfer, True, self._detail(error))
                self._receives.pop(_transfer_key(item.command.transfer), None)
                self._cleanup_receive(item)
        finally:
            with suppress(Exception):
                writer.close()
                await asyncio.wait_for(writer.wait_closed(), self.config.limits.cleanup_timeout)
            if item is not None and item.cancelled:
                self._receives.pop(_transfer_key(item.command.transfer), None)
                self._cleanup_receive(item)
                with suppress(Exception):
                    await self._report_cancelled_cleanup(item, destination=True)

    async def _send_transfer(self, item: ActiveSend) -> None:
        command = item.command
        key = _transfer_key(command.transfer)
        try:
            async with self._outgoing_sem:
                # The request may have waited behind earlier transfers; do not open
                # a connection for one that was cancelled or outlived its session.
                if item.cancelled or self._session_id != item.session_id:
                    raise asyncio.CancelledError
                entry = self.store.get(command.transfer.data, session_id=item.session_id)
                if entry is None:
                    raise DataPlaneIntegrityError("authorized source representation is unavailable")
                if entry.size_bytes > self.config.limits.max_transfer_bytes:
                    raise DataPlaneResourceError("runtime payload exceeds configured transfer limit")
                context = self.config.tls_policy.build_client_context()
                try:
                    reader, writer = await asyncio.wait_for(
                        asyncio.open_connection(
                            command.destination.host, command.destination.port,
                            ssl=context,
                            server_hostname=command.destination.worker_id,
                            ssl_handshake_timeout=self.config.limits.handshake_timeout,
                            limit=max(self.config.limits.chunk_bytes * 2, 64 * 1024),
                        ), self.config.limits.connect_timeout,
                    )
                except (OSError, ssl.SSLError, asyncio.TimeoutError) as error:
                    raise DataPlaneError("destination TLS connection failed") from error
                item.writer = writer
                identities = _peer_identities(writer)
                if command.destination.worker_id not in identities:
                    raise DataPlaneAuthenticationError("destination certificate identity mismatch")
                await self._write_header(writer, _header_record(command, entry))
                response = await self._read_header(reader)
                if response != {"status": "ready", "version": _VERSION}:
                    raise DataPlaneAuthorizationError("destination rejected transfer authorization")
                if item.cancelled or self._session_id != item.session_id:
                    raise asyncio.CancelledError
                started = self._on_started
                if started is None:
                    raise DataPlaneError("start callback unavailable")
                await started(command.transfer)
                started_at = time.monotonic()
                deadline = self.config.limits.deadline_for(entry.size_bytes)
                with entry.path.open("rb") as handle:
                    remaining = entry.size_bytes
                    while remaining:
                        if item.cancelled or self._session_id != item.session_id:
                            raise asyncio.CancelledError
                        if deadline is not None and time.monotonic() - started_at > deadline:
                            raise DataPlaneError("runtime transfer total deadline exceeded")
                        chunk = handle.read(min(self.config.limits.chunk_bytes, remaining))
                        if not chunk:
                            raise DataPlaneIntegrityError("source representation truncated while sending")
                        writer.write(chunk)
                        await asyncio.wait_for(writer.drain(), self.config.limits.idle_timeout)
                        remaining -= len(chunk)
                # Half-close is not portable over asyncio TLS.  Exact length in the
                # authenticated header is the byte-plane framing boundary.
        except asyncio.CancelledError:
            raise
        except BaseException as error:
            if not item.cancelled:
                callback = self._on_failed
                if callback is not None:
                    with suppress(Exception):
                        await callback(command.transfer, False, self._detail(error))
        finally:
            writer = item.writer
            if writer is not None:
                with suppress(Exception):
                    writer.close()
                    await asyncio.wait_for(writer.wait_closed(), self.config.limits.cleanup_timeout)
            self._sends.pop(key, None)
            if item.cancelled:
                with suppress(Exception):
                    await self._report_cancelled_cleanup(item, destination=False)
            callback = self._on_send_finished
            if callback is not None:
                with suppress(Exception):
                    await callback(command.transfer)

    async def _receive_timeout(self, item: PreparedReceive) -> None:
        try:
            deadline = self.config.limits.deadline_for(item.command.size_bytes)
            await asyncio.sleep(deadline if deadline is not None else self.config.limits.idle_timeout)
            if item.cancelled or _transfer_key(item.command.transfer) not in self._receives:
                return
            item.cancelled = True
            item.cancel_detail = "destination receive preparation expired"
            if item.writer is not None:
                self._abort_writer(item.writer)
            if item.connection_task is not None and not item.connection_task.done():
                item.connection_task.cancel()
                return
            self._cleanup_receive(item)
            self._receives.pop(_transfer_key(item.command.transfer), None)
            await self._report_cancelled_cleanup(item, destination=True)
        except asyncio.CancelledError:
            raise

    async def _report_cancelled_cleanup(
        self, item: PreparedReceive | ActiveSend, *, destination: bool
    ) -> None:
        if item.cleanup_reported:
            return
        item.cleanup_reported = True
        callback = self._on_failed
        if callback is not None:
            await callback(item.command.transfer, destination, item.cancel_detail)

    def _validate_incoming_header(
        self, item: PreparedReceive, header: dict[str, object], writer: asyncio.StreamWriter
    ) -> None:
        command = item.command
        identities = _peer_identities(writer)
        if command.transfer.source_worker_id not in identities:
            raise DataPlaneAuthenticationError("source certificate identity mismatch")
        expected = {
            "version": _VERSION,
            "plan_id": command.transfer.data.plan_id,
            "run_id": command.transfer.data.run_id,
            "value_id": command.transfer.data.value_id,
            "form": command.transfer.data.form.value,
            "object_state_id": command.transfer.data.object_state_id,
            "transfer_id": command.transfer.transfer_id,
            "transfer_attempt_id": command.transfer.transfer_attempt_id,
            "source_worker_id": command.transfer.source_worker_id,
            "destination_worker_id": command.transfer.destination_worker_id,
            "source_session_id": command.source_session_id,
            "destination_session_id": command.destination_session_id,
            "authorization": command.authorization,
        }
        for name, value in expected.items():
            if header.get(name) != value:
                raise DataPlaneAuthorizationError(f"incoming transfer binding mismatch: {name}")
        if header.get("serialization") not in {"pickle-v1", "dpr-json-v1"}:
            raise DataPlaneIntegrityError("unsupported runtime serialization")
        size = header.get("size_bytes")
        digest = header.get("sha256")
        if type(size) is not int or size < 0:
            raise DataPlaneIntegrityError("invalid runtime payload size")
        if type(digest) is not str or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
            raise DataPlaneIntegrityError("invalid runtime payload digest")

    def _transfer_from_header(self, header: dict[str, object]) -> p.TransferIdentity:
        required = {
            "version", "plan_id", "run_id", "value_id", "form", "object_state_id",
            "transfer_id", "transfer_attempt_id", "source_worker_id", "destination_worker_id",
            "source_session_id", "destination_session_id", "authorization", "size_bytes",
            "sha256", "serialization",
        }
        if set(header) != required:
            raise DataPlaneAuthorizationError("runtime transfer header schema mismatch")
        try:
            data = p.DataReference(
                str(header["plan_id"]), str(header["run_id"]), str(header["value_id"]),
                DataForm(str(header["form"])),
                None if header["object_state_id"] is None else str(header["object_state_id"]),
            )
            return p.TransferIdentity(
                data, str(header["transfer_id"]), str(header["transfer_attempt_id"]),
                str(header["source_worker_id"]), str(header["destination_worker_id"]),
            )
        except (ValueError, TypeError) as error:
            raise DataPlaneAuthorizationError("invalid runtime transfer identity") from error

    def _require_authorized_command(
        self, command: p.PrepareReceive | p.TransferRequest, *, destination: bool, session_id: str
    ) -> None:
        if command.source_session_id is None or command.destination_session_id is None or command.authorization is None:
            raise DataPlaneAuthorizationError("control command lacks byte-plane authorization")
        if destination:
            if command.transfer.destination_worker_id != self.config.worker_id:
                raise DataPlaneAuthorizationError("receive command names another destination")
            if command.destination_session_id != session_id:
                raise DataPlaneAuthorizationError("receive command belongs to another destination session")
        else:
            if command.transfer.source_worker_id != self.config.worker_id:
                raise DataPlaneAuthorizationError("send command names another source")
            if command.source_session_id != session_id:
                raise DataPlaneAuthorizationError("send command belongs to another source session")

    def _require_live_session(self, session_id: str) -> None:
        if self._session_id != session_id:
            raise DataPlaneAuthorizationError("byte-plane command belongs to stale worker session")

    async def _read_header(self, reader: asyncio.StreamReader) -> dict[str, object]:
        try:
            raw_length = await asyncio.wait_for(reader.readexactly(_HEADER.size), self.config.limits.idle_timeout)
            (length,) = _HEADER.unpack(raw_length)
            if length < 2 or length > self.config.limits.header_bytes:
                raise DataPlaneResourceError("runtime transfer header length out of bounds")
            payload = await asyncio.wait_for(reader.readexactly(length), self.config.limits.idle_timeout)
            result = json.loads(payload.decode("utf-8"), object_pairs_hook=_unique_object)
        except (asyncio.IncompleteReadError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
            raise DataPlaneIntegrityError("malformed runtime transfer header") from error
        if type(result) is not dict:
            raise DataPlaneIntegrityError("runtime transfer header must be an object")
        return result

    async def _write_header(self, writer: asyncio.StreamWriter, record: dict[str, object]) -> None:
        payload = json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")
        if len(payload) > self.config.limits.header_bytes:
            raise DataPlaneResourceError("runtime transfer header exceeds configured bound")
        writer.write(_HEADER.pack(len(payload)) + payload)
        await asyncio.wait_for(writer.drain(), self.config.limits.idle_timeout)

    def _cleanup_receive(self, item: PreparedReceive) -> None:
        if item.task is not None and item.task is not asyncio.current_task() and not item.task.done():
            item.task.cancel()
        if item.temp_path is not None:
            item.temp_path.unlink(missing_ok=True)
            item.temp_path = None
        item.writer = None

    @staticmethod
    def _abort_writer(writer: asyncio.StreamWriter) -> None:
        transport = getattr(writer, "transport", None)
        if transport is not None:
            transport.abort()
        else:
            writer.close()

    @staticmethod
    def _detail(error: BaseException) -> str:
        text = f"{type(error).__name__}: {error}".replace("\x00", "")
        data = text.encode("utf-8", "replace")
        if len(data) <= p.MAX_DETAIL_BYTES:
            return data.decode("utf-8", "replace")
        return data[: p.MAX_DETAIL_BYTES - 16].decode("utf-8", "ignore") + "...[truncated]"
