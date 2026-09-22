"""Pending coordinator command correlation and bounded completion tombstones.

This module owns only request/correlation bookkeeping.  It deliberately does
not own worker, run, task, context, or scheduling state; the Coordinator remains
the single authority for those semantics.
"""
from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from typing import Callable, Generic, Mapping, TypeVar, cast

from .errors import CoordinatorError, InvalidWorkerMessage, OperationalLimitExceeded
from .model import (
    ContextPreparationContract,
    PendingContextPreparation,
    PendingProgramPreparation,
    PendingRequest,
    ProgramPreparationContract,
    SessionHandle,
)

PendingT = TypeVar("PendingT", PendingProgramPreparation, PendingContextPreparation)


@dataclass(frozen=True, slots=True)
class PendingResolution(Generic[PendingT]):
    request: PendingT
    active: bool


@dataclass(frozen=True, slots=True)
class PendingRegistryView:
    """Immutable diagnostic summary; no mutable request mappings escape."""

    active_count: int
    retired_count: int
    active_message_ids: tuple[str, ...]
    retired_message_ids: tuple[str, ...]


class PendingOperationRegistry:
    """Typed active-request registry plus bounded completed-correlation history.

    Correlations are bound to the exact worker generation that received a
    command.  Completed requests become tombstones so recent duplicates can be
    identified idempotently; the tombstones are bounded and oldest-first.
    """

    def __init__(self, *, history_limit: Callable[[], int],
                 new_base_message_id: Callable[[], str],
                 active_global_limit: Callable[[], int] | None = None,
                 active_per_worker_limit: Callable[[], int] | None = None,
                 active_per_run_limit: Callable[[], int] | None = None) -> None:
        self._history_limit = history_limit
        self._new_base_message_id = new_base_message_id
        self._active_global_limit = active_global_limit
        self._active_per_worker_limit = active_per_worker_limit
        self._active_per_run_limit = active_per_run_limit
        self._active: dict[str, PendingRequest] = {}
        self._history: OrderedDict[str, PendingRequest] = OrderedDict()
        self._program_index: dict[ProgramPreparationContract, str] = {}
        self._context_index: dict[tuple[str, str], str] = {}
        self._worker_counts: dict[str, int] = {}
        self._run_counts: dict[str, int] = {}
        self._sequence = 0

    def new_message_id(self) -> str:
        # History is bounded, so a monotonic local suffix prevents an ancient,
        # evicted correlation from becoming an ABA match if an injected ID source
        # later returns the same base value again.
        self._sequence += 1
        return f"{self._new_base_message_id()}~p{self._sequence}"

    def record(self, request: PendingRequest) -> None:
        if request.message_id in self._active or request.message_id in self._history:
            raise CoordinatorError(f"correlation ID reused: {request.message_id}")
        self._ensure_admission(request)
        if isinstance(request, PendingProgramPreparation):
            if request.contract in self._program_index:
                raise CoordinatorError("duplicate active program-preparation contract")
            self._program_index[request.contract] = request.message_id
        else:
            identity = (request.run_id, request.context_id)
            if identity in self._context_index:
                raise CoordinatorError(
                    f"duplicate active context-preparation identity: "
                    f"{request.run_id}/{request.context_id}"
                )
            self._context_index[identity] = request.message_id
        self._active[request.message_id] = request
        self._worker_counts[request.worker_id] = self._worker_counts.get(request.worker_id, 0) + 1
        run_id = self._request_run_id(request)
        if run_id is not None:
            self._run_counts[run_id] = self._run_counts.get(run_id, 0) + 1

    def _ensure_admission(self, request: PendingRequest) -> None:
        if self._active_global_limit is not None and len(self._active) >= self._positive_limit(
                self._active_global_limit(), "active pending global"):
            raise OperationalLimitExceeded("global active pending-operation limit reached")
        worker_limit = None if self._active_per_worker_limit is None else self._positive_limit(
            self._active_per_worker_limit(), "active pending per-worker")
        if worker_limit is not None and self._worker_counts.get(request.worker_id, 0) >= worker_limit:
            raise OperationalLimitExceeded(
                f"active pending-operation limit reached for worker {request.worker_id}"
            )
        run_id = self._request_run_id(request)
        run_limit = None if self._active_per_run_limit is None else self._positive_limit(
            self._active_per_run_limit(), "active pending per-run")
        if run_id is not None and run_limit is not None and self._run_counts.get(run_id, 0) >= run_limit:
            raise OperationalLimitExceeded(
                f"active pending-operation limit reached for run {run_id}"
            )

    @staticmethod
    def _request_run_id(request: PendingRequest) -> str | None:
        return request.run_id if isinstance(request, PendingContextPreparation) else None

    @staticmethod
    def _positive_limit(value: int, name: str) -> int:
        if type(value) is not int or value < 1:
            raise CoordinatorError(f"{name} limit must be a positive integer")
        return value

    def find_program(self, contract: ProgramPreparationContract) -> PendingProgramPreparation | None:
        message_id = self._program_index.get(contract)
        if message_id is None:
            return None
        request = self._active[message_id]
        assert isinstance(request, PendingProgramPreparation)
        return request

    def find_context(self, run_id: str, context_id: str) -> PendingContextPreparation | None:
        message_id = self._context_index.get((run_id, context_id))
        if message_id is None:
            return None
        request = self._active[message_id]
        assert isinstance(request, PendingContextPreparation)
        return request

    def resolve(self, session: SessionHandle, correlation_id: str | None,
                kind: type[PendingT]) -> PendingResolution[PendingT]:
        ident = correlation_id or ""
        request = self._active.get(ident)
        active = True
        if request is None:
            request = self._history.get(ident)
            active = False
        if request is None or not isinstance(request, kind):
            raise InvalidWorkerMessage(f"unknown {kind.__name__} correlation")
        if (request.worker_id != session.worker_id
                or request.worker_generation != session.generation):
            raise InvalidWorkerMessage("pending command belongs to a different worker session")
        return PendingResolution(cast(PendingT, request), active)

    def complete(self, correlation_id: str | None) -> PendingRequest | None:
        request = self._active.pop(correlation_id or "", None)
        if request is not None:
            self._drop_index(request)
            self._archive(request)
        return request

    def invalidate_session(self, session: SessionHandle) -> tuple[PendingRequest, ...]:
        return self._retire_matching(
            lambda request: (
                request.worker_id == session.worker_id
                and request.worker_generation == session.generation
            )
        )

    def invalidate_run(self, run_id: str) -> tuple[PendingRequest, ...]:
        return self._retire_matching(
            lambda request: (
                isinstance(request, PendingContextPreparation)
                and request.run_id == run_id
            )
        )

    def active_requests(self) -> tuple[PendingRequest, ...]:
        return tuple(self._active.values())

    def view(self) -> PendingRegistryView:
        return PendingRegistryView(
            active_count=len(self._active),
            retired_count=len(self._history),
            active_message_ids=tuple(self._active),
            retired_message_ids=tuple(self._history),
        )

    def validate(self, current_generations: Mapping[str, int]) -> None:
        if len(self._history) > self._max_history():
            raise AssertionError("completed pending history exceeds configured bound")
        if (self._active_global_limit is not None
                and len(self._active) > self._positive_limit(
                    self._active_global_limit(), "active pending global")):
            raise AssertionError("active pending operations exceed global admission limit")
        if self._active.keys() & self._history.keys():
            raise AssertionError("pending correlation is both active and retired")

        program_contracts: set[ProgramPreparationContract] = set()
        context_contracts: dict[tuple[str, str], ContextPreparationContract] = {}
        expected_program_index: dict[ProgramPreparationContract, str] = {}
        expected_context_index: dict[tuple[str, str], str] = {}
        for request in self._active.values():
            if current_generations.get(request.worker_id) != request.worker_generation:
                raise AssertionError("active pending command belongs to stale worker session")
            if isinstance(request, PendingProgramPreparation):
                if request.contract in program_contracts:
                    raise AssertionError("duplicate active program-preparation contract")
                program_contracts.add(request.contract)
                expected_program_index[request.contract] = request.message_id
            else:
                identity = (request.run_id, request.context_id)
                if identity in context_contracts:
                    raise AssertionError("conflicting pending contracts share a context identity")
                context_contracts[identity] = request.contract
                expected_context_index[identity] = request.message_id
        if self._program_index != expected_program_index:
            raise AssertionError("program preparation index disagrees with active requests")
        expected_worker_counts: dict[str, int] = {}
        expected_run_counts: dict[str, int] = {}
        for request in self._active.values():
            expected_worker_counts[request.worker_id] = expected_worker_counts.get(request.worker_id, 0) + 1
            run_id = self._request_run_id(request)
            if run_id is not None:
                expected_run_counts[run_id] = expected_run_counts.get(run_id, 0) + 1
        if self._worker_counts != expected_worker_counts:
            raise AssertionError("pending worker-count index disagrees with active requests")
        if self._run_counts != expected_run_counts:
            raise AssertionError("pending run-count index disagrees with active requests")
        if self._active_per_worker_limit is not None:
            limit = self._positive_limit(self._active_per_worker_limit(), "active pending per-worker")
            if any(count > limit for count in self._worker_counts.values()):
                raise AssertionError("active pending operations exceed per-worker admission limit")
        if self._active_per_run_limit is not None:
            limit = self._positive_limit(self._active_per_run_limit(), "active pending per-run")
            if any(count > limit for count in self._run_counts.values()):
                raise AssertionError("active pending operations exceed per-run admission limit")
        if self._context_index != expected_context_index:
            raise AssertionError("context preparation index disagrees with active requests")

    def _retire_matching(self, predicate: Callable[[PendingRequest], bool]) -> tuple[PendingRequest, ...]:
        retired: list[PendingRequest] = []
        for message_id, request in tuple(self._active.items()):
            if not predicate(request):
                continue
            del self._active[message_id]
            self._drop_index(request)
            self._archive(request)
            retired.append(request)
        return tuple(retired)

    def _archive(self, request: PendingRequest) -> None:
        self._history[request.message_id] = request
        self._history.move_to_end(request.message_id)
        while len(self._history) > self._max_history():
            self._history.popitem(last=False)

    def _drop_index(self, request: PendingRequest) -> None:
        if isinstance(request, PendingProgramPreparation):
            self._program_index.pop(request.contract, None)
        else:
            self._context_index.pop((request.run_id, request.context_id), None)
        worker_count = self._worker_counts.get(request.worker_id, 0) - 1
        if worker_count > 0:
            self._worker_counts[request.worker_id] = worker_count
        else:
            self._worker_counts.pop(request.worker_id, None)
        run_id = self._request_run_id(request)
        if run_id is not None:
            run_count = self._run_counts.get(run_id, 0) - 1
            if run_count > 0:
                self._run_counts[run_id] = run_count
            else:
                self._run_counts.pop(run_id, None)

    def _max_history(self) -> int:
        limit = self._history_limit()
        if type(limit) is not int or limit < 0:
            raise CoordinatorError("pending history limit must be a non-negative integer")
        return limit
