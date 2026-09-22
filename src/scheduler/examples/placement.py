"""Three-worker placement demonstration. No submitted computation is executed.

Run from src/: python -m scheduler.examples.placement
"""
from dag_runtime.dag_engine import analyze_source
from execution import ExecutionMode, lower_dag
from scheduler import ClusterSnapshot, DataForm, DataLocation, ReadyTask, Replica, WorkerState, schedule


def example():
    dag = analyze_source("u=10\nv=20\nw=30\na=u+1\nb=v+1\nc=w+1\n")
    plan = lower_dag(dag, environment_id="pinned-env-v1", package_id="project-v1")
    # Illustrative coherent coordinator facts: roots have already completed.
    tasks = plan.tasks[3:]
    common = dict(environment_ids={plan.program.environment_id}, prepared_program_ids={plan.program.id},
                  supported_modes={ExecutionMode.ISOLATED_CANDIDATE})
    gib = 1024**3
    workers = (
        WorkerState("W1", 4, running_slots=2, reserved_slots=1, cpu_percent=70,
                    total_memory_bytes=16*gib, available_memory_bytes=2*gib, cpu_cores=8, **common),
        WorkerState("W2", 2, cpu_percent=20, total_memory_bytes=8*gib,
                    available_memory_bytes=6*gib, cpu_cores=4, **common),
        WorkerState("W3", 1, cpu_percent=45, total_memory_bytes=4*gib,
                    available_memory_bytes=3*gib, cpu_cores=2, **common),
    )
    data = tuple(DataLocation(plan.final_bindings[name], DataForm.IMMUTABLE_VALUE, (Replica(owner),), size)
                 # Illustrative reported representation sizes, not inferred from AST.
                 for name, owner, size in (("u", "W1", 128), ("v", "W2", 64), ("w", "W3", None)))
    state = ClusterSnapshot(plan.id, "example-run", "snapshot-7", workers=workers, data=data,
                            ready=tuple(ReadyTask(t.task_id, i) for i, t in enumerate(tasks)),
                            completed_task_ids={t.task_id for t in plan.tasks[:3]})
    return plan, state, schedule(plan, state)


if __name__ == "__main__":
    plan, state, decision = example()
    print(f"Proposal for {decision.run_id}, {decision.snapshot_id}")
    for placement in decision.placements:
        print(f"{placement.task_id} ({plan.task_index[placement.task_id].task.source}) -> {placement.worker_id}; "
              f"all inputs local; {placement.preference.free_slots_before} free slots before proposal")
