"""Run-owned DAG readiness and lifecycle state."""
from __future__ import annotations

from dataclasses import dataclass, field

from dag_runtime.dag_model import ReadinessState
from execution import ExecutionPlan
from scheduler import Scheduler, SchedulingDecision, TaskAffinity, WorkerContext

from .model import AttemptRecord, CoordinatorFailure, RunStatus, TaskRecord, TaskStatus


@dataclass(slots=True)
class CoordinatorRun:
    run_id: str
    plan: ExecutionPlan
    scheduler: Scheduler
    readiness: ReadinessState
    status: RunStatus
    tasks: dict[str, TaskRecord]
    attempts: dict[str, AttemptRecord] = field(default_factory=dict)
    affinities: dict[str, TaskAffinity] = field(default_factory=dict)
    contexts: dict[str, WorkerContext] = field(default_factory=dict)
    unavailable_context_ids: set[str] = field(default_factory=set)
    next_ready_sequence: int = 0
    last_snapshot_id: str | None = None
    last_snapshot_revision: int | None = None
    last_decision: SchedulingDecision | None = None
    failure: CoordinatorFailure | None = None

    @classmethod
    def create(cls, run_id: str, plan: ExecutionPlan,
               affinities: tuple[TaskAffinity, ...] = (),
               contexts: tuple[WorkerContext, ...] = ()) -> "CoordinatorRun":
        # ExecutionPlan deliberately retains the exact DAG records and validator.
        dag = plan._as_dag()
        readiness = dag.new_readiness()
        ready_ids = set(readiness.ready)
        tasks: dict[str, TaskRecord] = {}
        seq = 0
        for manifest in plan.tasks:
            if manifest.task_id in ready_ids:
                tasks[manifest.task_id] = TaskRecord(manifest.task_id, TaskStatus.READY, seq)
                seq += 1
            else:
                tasks[manifest.task_id] = TaskRecord(manifest.task_id, TaskStatus.BLOCKED)
        status = RunStatus.SUCCEEDED if not tasks else RunStatus.RUNNING
        return cls(
            run_id, plan, Scheduler(plan), readiness, status, tasks,
            affinities={a.task_id: a for a in affinities},
            contexts={c.context_id: c for c in contexts},
            next_ready_sequence=seq,
        )

    def unlock_after_commit(self, task_id: str) -> tuple[str, ...]:
        unlocked = self.readiness.mark_completed(task_id)
        for child in unlocked:
            record = self.tasks[child]
            if self.status == RunStatus.RUNNING:
                record.status = TaskStatus.READY
                record.ready_sequence = self.next_ready_sequence
                record.wait_rounds = 0
                self.next_ready_sequence += 1
        return unlocked
