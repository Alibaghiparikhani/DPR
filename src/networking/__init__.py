"""Real TLS coordinator/worker control transport; no user-code execution."""
from .config import TransportLimits
from .coordinator_service import CoordinatorNetworkService
from .client import ClientOperationError, CoordinatorClient, CoordinatorClientConfig
from .errors import (
    AdmissionError,
    ApplicationAuthenticationError,
    BackpressureError,
    CleanShutdown,
    ProtocolTransportError,
    TlsAuthenticationError,
    TransportError,
    TransportIOError,
    WorkerNotAdmitted,
)

__all__ = [
    "CoordinatorNetworkService", "CoordinatorClient", "CoordinatorClientConfig", "ClientOperationError",
    "TransportLimits", "TransportError",
    "TlsAuthenticationError", "ApplicationAuthenticationError",
    "ProtocolTransportError", "AdmissionError", "TransportIOError",
    "BackpressureError", "CleanShutdown", "WorkerNotAdmitted",
]
