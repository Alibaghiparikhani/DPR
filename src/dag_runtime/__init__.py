"""DAG analysis subsystem for the distributed Python runtime."""
from __future__ import annotations

from typing import TypeVar

_F = TypeVar("_F")


def task(function: _F) -> _F:
    """Declare an explicit distributed-task contract.

    The decorator is intentionally a runtime no-op.  Static analysis recognizes
    only this marker imported from :mod:`dag_runtime`; arbitrary decorators named
    ``task`` are not trusted.  The decorated function promises that observable
    runtime data flows through its arguments/return value and that independent
    calls do not mutate hidden shared state.
    """
    return function


__all__ = ["task"]
