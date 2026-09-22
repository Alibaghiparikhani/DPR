"""Coordinator-owned transferable representation location index."""
from __future__ import annotations

from collections.abc import Mapping

from execution import ExecutionPlan, ValueKind, ValueRequirement
from protocol import DataReference
from scheduler import DataForm, DataLocation, Replica, ReplicaStatus

from .errors import InvalidDataLocation
from .model import LocationRecord


class ObjectLocationIndex:
    def __init__(self) -> None:
        self._records: dict[DataReference, LocationRecord] = {}

    @staticmethod
    def validate_reference(plan: ExecutionPlan, run_id: str, data: DataReference) -> None:
        if data.plan_id != plan.id or data.run_id != run_id:
            raise InvalidDataLocation("foreign plan/run data reference")
        value = plan.value_index.get(data.value_id)
        if value is None:
            raise InvalidDataLocation(f"unknown value: {data.value_id}")
        kind = ValueRequirement(value).kind
        expected = {
            ValueKind.IMMUTABLE: DataForm.IMMUTABLE_VALUE,
            ValueKind.SHARED_REFERENCE: DataForm.OBJECT_SNAPSHOT,
        }.get(kind)
        if expected is None or data.form != expected:
            raise InvalidDataLocation("data reference is not a transferable representation")
        if data.object_state_id is not None:
            state = plan.value_index.get(data.object_state_id)
            if (state is None or ValueRequirement(state).kind != ValueKind.OBJECT_STATE
                    or state.object_id != value.object_id):
                raise InvalidDataLocation("object snapshot state does not match value object")
        elif data.form == DataForm.IMMUTABLE_VALUE:
            pass

    def announce(self, plan: ExecutionPlan, run_id: str, worker_id: str,
                 worker_generation: int, data: DataReference,
                 size_bytes: int | None = None) -> None:
        self.validate_reference(plan, run_id, data)
        if type(worker_generation) is not int or worker_generation < 1:
            raise InvalidDataLocation("worker_generation must be a positive integer")
        if size_bytes is not None and (type(size_bytes) is not int or size_bytes < 0):
            raise InvalidDataLocation("size_bytes must be a non-negative integer or None")
        record = self._records.get(data)
        if record is None:
            record = LocationRecord(data, {}, size_bytes)
            self._records[data] = record
        elif record.size_bytes is not None and size_bytes is not None and record.size_bytes != size_bytes:
            raise InvalidDataLocation("conflicting size for same data representation")
        elif record.size_bytes is None and size_bytes is not None:
            record.size_bytes = size_bytes
        existing_generation = record.replicas.get(worker_id)
        if existing_generation is not None and existing_generation != worker_generation:
            raise InvalidDataLocation("replica certification belongs to a different worker generation")
        record.replicas[worker_id] = worker_generation

    def unavailable(self, plan: ExecutionPlan, run_id: str, worker_id: str,
                    worker_generation: int, data: DataReference) -> bool:
        """Remove one exact-session replica and report final known-replica loss."""
        self.validate_reference(plan, run_id, data)
        record = self._records.get(data)
        if record is None:
            return False
        existed = record.replicas.get(worker_id) == worker_generation
        if existed:
            del record.replicas[worker_id]
        if existed and not record.replicas:
            del self._records[data]
            return True
        return False

    def remove_worker(self, worker_id: str, worker_generation: int | None = None) -> tuple[DataReference, ...]:
        lost: list[DataReference] = []
        for data, record in list(self._records.items()):
            generation = record.replicas.get(worker_id)
            had_replica = generation is not None and (
                worker_generation is None or generation == worker_generation
            )
            if had_replica:
                del record.replicas[worker_id]
            if had_replica and not record.replicas:
                lost.append(data)
                del self._records[data]
        return tuple(lost)

    def has(self, data: DataReference, worker_id: str,
            worker_generation: int | None = None) -> bool:
        record = self._records.get(data)
        if record is None:
            return False
        generation = record.replicas.get(worker_id)
        return generation is not None and (
            worker_generation is None or generation == worker_generation
        )

    def locations(self, plan_id: str, run_id: str) -> tuple[DataLocation, ...]:
        result: list[DataLocation] = []
        records = sorted(
            (r for r in self._records.values()
             if r.data.plan_id == plan_id and r.data.run_id == run_id),
            key=lambda r: (r.data.value_id, r.data.object_state_id or "", r.data.form.value),
        )
        for record in records:
            replicas = tuple(Replica(w, ReplicaStatus.AVAILABLE) for w in sorted(record.replicas))
            result.append(record.data.to_location(replicas, record.size_bytes))
        return tuple(result)

    def size_bytes(self, data: DataReference) -> int | None:
        record = self._records.get(data)
        return None if record is None else record.size_bytes

    def worker_ids(self, data: DataReference) -> frozenset[str]:
        record = self._records.get(data)
        return frozenset() if record is None else frozenset(record.replicas)

    def data_for_worker(self, worker_id: str, worker_generation: int | None = None) -> tuple[DataReference, ...]:
        return tuple(sorted(
            (data for data, record in self._records.items()
             if self.has(data, worker_id, worker_generation)),
            key=lambda data: (
                data.plan_id, data.run_id, data.value_id,
                data.object_state_id or "", data.form.value,
            ),
        ))

    def remove_run(self, plan_id: str, run_id: str) -> int:
        """Drop in-memory location metadata for one released terminal run."""
        removed = 0
        for data in tuple(self._records):
            if data.plan_id == plan_id and data.run_id == run_id:
                del self._records[data]
                removed += 1
        return removed

    def validate(self, active_generations: Mapping[str, int]) -> None:
        for record in self._records.values():
            if not record.replicas:
                raise AssertionError("empty location record retained")
            for worker_id, generation in record.replicas.items():
                if active_generations.get(worker_id) != generation:
                    raise AssertionError("location references inactive or stale worker generation")
