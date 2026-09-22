"""Configuration records for the coordinator control service."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class TransportLimits:
    inbound_queue_messages: int = 64
    outbound_queue_messages: int = 64
    read_chunk_size: int = 65536
    stream_buffer_limit: int = 131072
    max_connections: int = 1024
    handshake_timeout: float = 5.0
    write_timeout: float = 5.0
    maintenance_interval: float = 0.1

    def __post_init__(self) -> None:
        for name in ("inbound_queue_messages", "outbound_queue_messages", "read_chunk_size", "stream_buffer_limit", "max_connections"):
            value = getattr(self, name)
            if type(value) is not int or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        for name in ("handshake_timeout", "write_timeout", "maintenance_interval"):
            value = getattr(self, name)
            if type(value) not in (int, float) or value <= 0:
                raise ValueError(f"{name} must be positive")
