"""Static execution contracts for dag_runtime; no scheduler or worker executor."""
from .model import (
    AttemptIdentity, CodeRequirement, ExecutionMode, ExecutionPlan,
    ExecutionValidationError, FailureInfo, FailureKind, ObjectAccess,
    ObjectRequirement, ProgramIdentity, TaskFailure, TaskManifest, TaskSuccess,
    ValueKind, ValueRequirement,
)


def __getattr__(name: str):
    """Lazily expose analyzer-facing helpers without polluting protocol imports.

    Protocol/network consumers depend only on frozen execution model records.
    Importing :mod:`execution` therefore must not import the AST analyzer.  The
    first explicit access to ``execution.lower_dag`` resolves and caches the real
    function, preserving the historical public identity contract.
    """
    if name == "lower_dag":
        from .lowering import lower_dag as resolved
        globals()[name] = resolved
        return resolved
    raise AttributeError(name)

__all__ = [
    "lower_dag", "AttemptIdentity", "CodeRequirement", "ExecutionMode", "ExecutionPlan",
    "ExecutionValidationError", "FailureInfo", "FailureKind", "ObjectAccess",
    "ObjectRequirement", "ProgramIdentity", "TaskFailure", "TaskManifest", "TaskSuccess",
    "ValueKind", "ValueRequirement",
]
