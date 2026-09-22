"""Four-byte unsigned big-endian length framing, independent of JSON parsing."""

from __future__ import annotations

import struct

from .errors import DecoderStateError, FrameTooLarge, FramingError, TruncatedFrame
from .limits import MAX_FRAME_PAYLOAD

HEADER_SIZE = 4
_HEADER = struct.Struct("!I")


def _limit(value: int) -> int:
    if type(value) is not int or not 1 <= value <= MAX_FRAME_PAYLOAD:
        raise FramingError("max_payload must be in 1..MAX_FRAME_PAYLOAD")
    return value


def _length(value: int, maximum: int) -> None:
    if value == 0:
        raise FramingError("zero-length frame is forbidden")
    if value > maximum:
        raise FrameTooLarge("declared frame exceeds maximum payload")


def frame_payload(payload: bytes, *, max_payload: int = MAX_FRAME_PAYLOAD) -> bytes:
    """Frame already encoded bytes. This function does not validate JSON."""
    maximum = _limit(max_payload)
    if type(payload) is not bytes:
        raise FramingError("payload must be bytes")
    _length(len(payload), maximum)
    return _HEADER.pack(len(payload)) + payload


class FrameDecoder:
    """Bounded incremental parser, with explicit open/failed/closed states.

    A framing failure clears retained bytes and poisons the decoder until reset.
    The raised FramingError.completed_frames contains valid prefix payloads
    completed during that feed call. No frames are silently lost or duplicated.
    Returned batches cost O(input bytes); retained incomplete state is at most
    max_payload bytes plus the header. Instances require single-owner access.
    """

    def __init__(self, *, max_payload: int = MAX_FRAME_PAYLOAD) -> None:
        self._maximum = _limit(max_payload)
        self._header = bytearray()
        self._body = bytearray()
        self._expected: int | None = None
        self._failed = False
        self._closed = False

    @property
    def max_payload(self) -> int:
        return self._maximum

    @property
    def buffered_bytes(self) -> int:
        return len(self._header) + len(self._body)

    @property
    def failed(self) -> bool:
        return self._failed

    @property
    def closed(self) -> bool:
        return self._closed

    def _clear(self) -> None:
        self._header.clear()
        self._body.clear()
        self._expected = None

    def reset(self) -> None:
        """Explicitly start a NEW stream, abandoning any old partial frame."""
        self._clear()
        self._failed = self._closed = False

    def _open(self) -> None:
        if self._failed or self._closed:
            raise DecoderStateError(
                "decoder is failed or closed; reset for a new stream"
            )

    def feed(self, chunk: bytes | bytearray | memoryview) -> list[bytes]:
        """Consume a chunk immediately. Never allocate a peer-declared body upfront."""
        self._open()
        if type(chunk) not in (bytes, bytearray, memoryview):
            raise FramingError("chunk must be bytes, bytearray or byte memoryview")
        try:
            view = memoryview(chunk)
        except ValueError:
            raise FramingError("chunk is a released memoryview") from None
        if view.ndim != 1 or not view.c_contiguous or view.itemsize != 1:
            raise FramingError("chunk must be a contiguous one-dimensional byte view")
        view = view.cast("B")
        complete: list[bytes] = []
        offset = 0
        try:
            while offset < len(view):
                if self._expected is None:
                    count = min(HEADER_SIZE - len(self._header), len(view) - offset)
                    self._header.extend(view[offset : offset + count])
                    offset += count
                    if len(self._header) != HEADER_SIZE:
                        break
                    declared = _HEADER.unpack(self._header)[0]
                    _length(declared, self._maximum)
                    self._expected = declared
                    self._header.clear()
                count = min(self._expected - len(self._body), len(view) - offset)
                self._body.extend(view[offset : offset + count])
                offset += count
                if len(self._body) == self._expected:
                    complete.append(bytes(self._body))
                    self._body.clear()
                    self._expected = None
        except FramingError as error:
            self._clear()
            self._failed = True
            error.completed_frames = tuple(complete)
            raise
        return complete

    def finish(self) -> None:
        """Signal EOF. Idempotent after clean EOF; truncation is a terminal failure."""
        if self._closed:
            return
        self._open()
        if self._header or self._expected is not None:
            self._clear()
            self._failed = True
            raise TruncatedFrame("EOF in frame header or payload")
        self._closed = True
