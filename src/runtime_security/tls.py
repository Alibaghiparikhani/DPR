"""Strict TLS policy construction for live coordinator/worker/operator and P2P transport."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import ssl


@dataclass(frozen=True, slots=True)
class TlsCredentials:
    certificate: str | Path
    private_key: str | Path
    ca_certificate: str | Path

    def __post_init__(self) -> None:
        for name in ("certificate", "private_key", "ca_certificate"):
            value = Path(getattr(self, name))
            if not str(value):
                raise ValueError(f"{name} path must be nonempty")


@dataclass(frozen=True, slots=True)
class TlsPolicy:
    """Mutual-TLS policy: TLS 1.3 minimum and mandatory certificate validation."""

    credentials: TlsCredentials

    def build_server_context(self) -> ssl.SSLContext:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        self._harden(context)
        context.verify_mode = ssl.CERT_REQUIRED
        context.load_verify_locations(cafile=str(self.credentials.ca_certificate))
        context.load_cert_chain(
            certfile=str(self.credentials.certificate),
            keyfile=str(self.credentials.private_key),
        )
        return context

    def build_client_context(self) -> ssl.SSLContext:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        self._harden(context)
        context.check_hostname = True
        context.verify_mode = ssl.CERT_REQUIRED
        context.load_verify_locations(cafile=str(self.credentials.ca_certificate))
        context.load_cert_chain(
            certfile=str(self.credentials.certificate),
            keyfile=str(self.credentials.private_key),
        )
        return context

    @staticmethod
    def _harden(context: ssl.SSLContext) -> None:
        context.minimum_version = ssl.TLSVersion.TLSv1_3
        context.options |= ssl.OP_NO_COMPRESSION
