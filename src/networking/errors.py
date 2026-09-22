"""Transport-specific failures; never coordinator domain failures."""

class TransportError(RuntimeError):
    """Base class for coordinator/worker control transport failures."""

class TlsAuthenticationError(TransportError):
    pass

class ApplicationAuthenticationError(TransportError):
    pass

class ProtocolTransportError(TransportError):
    pass

class AdmissionError(TransportError):
    pass

class WorkerNotAdmitted(AdmissionError):
    """The coordinator authenticated this worker and then refused it: it is not (or
    no longer) a member.  Unlike a network failure, retrying cannot help."""

class TransportIOError(TransportError):
    pass

class BackpressureError(TransportError):
    pass

class CleanShutdown(TransportError):
    pass
