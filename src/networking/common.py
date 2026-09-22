"""Shared bounded framed-stream utilities for the control transport."""
from __future__ import annotations

import asyncio
from collections import deque
import secrets
from typing import Deque

import protocol as p

from .errors import ProtocolTransportError, TransportIOError


def new_transport_id(prefix: str) -> str:
    return f"{prefix}-{secrets.token_hex(16)}"


def encode_frame(message: p.Message) -> bytes:
    return p.frame_payload(p.encode_message(message))


class FramedProtocolStream:
    """Incrementally decode the existing bounded protocol framing from a stream."""

    def __init__(self, reader: asyncio.StreamReader, *, read_chunk_size: int = 65536) -> None:
        if type(read_chunk_size) is not int or not 1 <= read_chunk_size <= p.MAX_FRAME_PAYLOAD:
            raise ValueError("read_chunk_size must be in 1..MAX_FRAME_PAYLOAD")
        self.reader = reader
        self.read_chunk_size = read_chunk_size
        self.decoder = p.FrameDecoder()
        self._pending: Deque[p.Message] = deque()

    async def read_message(self) -> p.Message:
        if self._pending:
            return self._pending.popleft()
        while True:
            try:
                chunk = await self.reader.read(self.read_chunk_size)
            except (ConnectionError, OSError) as error:
                raise TransportIOError("control connection read failed") from error
            if not chunk:
                try:
                    self.decoder.finish()
                except p.ProtocolError as error:
                    raise ProtocolTransportError("truncated control frame") from error
                raise EOFError("control connection closed")
            try:
                payloads = self.decoder.feed(chunk)
                for payload in payloads:
                    self._pending.append(p.decode_message(payload))
            except p.ProtocolError as error:
                raise ProtocolTransportError("invalid control protocol input") from error
            if self._pending:
                return self._pending.popleft()




async def close_writer(writer: asyncio.StreamWriter, *, timeout: float) -> None:
    """Bound TLS close so an uncooperative peer cannot hang shutdown."""
    writer.close()
    try:
        await asyncio.wait_for(writer.wait_closed(), timeout=timeout)
    except (Exception, asyncio.CancelledError):
        transport = getattr(writer, "transport", None)
        if transport is not None:
            transport.abort()
        if asyncio.current_task() is not None and asyncio.current_task().cancelling():
            raise

async def write_message(
    writer: asyncio.StreamWriter,
    message: p.Message,
    *,
    timeout: float,
) -> None:
    """Write one frame; StreamWriter/drain handles partial socket writes internally."""
    frame = encode_frame(message)
    try:
        writer.write(frame)
        await asyncio.wait_for(writer.drain(), timeout=timeout)
    except (ConnectionError, OSError, asyncio.TimeoutError) as error:
        raise TransportIOError("control connection write failed") from error
