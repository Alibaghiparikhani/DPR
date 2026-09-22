"""Coordinator lifecycle errors, distinct from wire/protocol decoding errors."""

class CoordinatorError(RuntimeError):
    """Base class for control-plane state errors."""

class UnknownRun(CoordinatorError): pass
class UnknownWorker(CoordinatorError): pass
class StaleWorkerSession(CoordinatorError): pass
class UnknownTask(CoordinatorError): pass
class UnknownAttempt(CoordinatorError): pass
class StaleAttempt(CoordinatorError): pass
class InvalidTaskTransition(CoordinatorError): pass
class InvalidRunTransition(CoordinatorError): pass
class InvalidTransferTransition(CoordinatorError): pass
class CapacityConflict(CoordinatorError): pass
class PlacementRejected(CoordinatorError): pass
class InvalidWorkerMessage(CoordinatorError): pass
class UnknownTransfer(CoordinatorError): pass
class InvalidDataLocation(CoordinatorError): pass
class ContextConflict(CoordinatorError): pass

class OutboundBackpressure(CoordinatorError): pass
class OperationalLimitExceeded(CoordinatorError): pass
