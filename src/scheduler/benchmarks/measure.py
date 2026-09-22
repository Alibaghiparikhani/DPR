"""Synthetic scheduler cost measurement; no user computation or timing prediction.

From src/: python -m scheduler.benchmarks.measure --tasks 1000 5000 --repeats 5
"""
import argparse
import json
import platform
from statistics import median
from time import perf_counter

from dag_runtime.dag_engine import analyze_source
from execution import ExecutionMode, lower_dag
from scheduler import ClusterSnapshot, ReadyTask, Scheduler, WorkerState


def measure(task_count: int, repeats: int) -> dict:
    source = "\n".join(f"v{i}={i}" for i in range(task_count)) + "\n"
    # Setup is outside the measured placement/preprocessing sections.
    dag = analyze_source(source)
    plan = lower_dag(dag, environment_id="synthetic-benchmark-env")
    assert len(plan.tasks) == task_count, "Analysis budget changed the synthetic fixture"
    workers = tuple(WorkerState(f"W{i}", (task_count + 2) // 3,
                               environment_ids={plan.program.environment_id}, prepared_program_ids={plan.program.id},
                               supported_modes={ExecutionMode.ISOLATED_CANDIDATE}) for i in range(3))
    state = ClusterSnapshot(plan.id, "benchmark-run", "benchmark-snapshot", workers=workers,
                            ready=tuple(ReadyTask(t.task_id, i) for i, t in enumerate(plan.tasks)))
    preparation, placement = [], []
    for _ in range(repeats):
        start = perf_counter()
        scheduler = Scheduler(plan)
        preparation.append(perf_counter() - start)
        start = perf_counter()
        decision = scheduler.schedule(state)
        placement.append(perf_counter() - start)
        assert len(decision.placements) == task_count
    return {"tasks": task_count, "workers": len(workers), "repeats": repeats,
            "preprocess_median_seconds": median(preparation), "schedule_median_seconds": median(placement),
            "exact_descendant_counts": next(iter(scheduler.structure.values())).descendant_count is not None}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tasks", type=int, nargs="+", default=[1000, 5000])
    parser.add_argument("--repeats", type=int, default=5)
    args = parser.parse_args()
    if args.repeats < 1 or any(n < 1 for n in args.tasks):
        parser.error("tasks and repeats must be positive")
    print(json.dumps({"python": platform.python_version(), "platform": platform.platform(),
                      "fixture": "Independent scalar roots; no payloads; all slots initially free",
                      "measurements": [measure(n, args.repeats) for n in args.tasks]}, indent=2))
