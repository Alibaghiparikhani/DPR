"""Realistic cross-layer simulations for DAG -> execution -> scheduler.

These tests simulate coordinator-owned readiness, worker snapshots, successful
completion and value materialization. They intentionally do not execute user
code or model networking/dispatch; those are later runtime layers.
"""
from dataclasses import replace
from pathlib import Path

import pytest

from dag_runtime.dag_engine import analyze_file, analyze_source
from execution import ExecutionMode, ValueKind, lower_dag
from scheduler import (
    ClusterSnapshot,
    DataForm,
    DataLocation,
    ReadyTask,
    Replica,
    Scheduler,
    UnplacedReason,
    WorkerState,
)

ROOT = Path(__file__).parents[2]


def make_plan(example: str):
    dag = analyze_file(ROOT / "dag_runtime" / "examples" / f"{example}.py")
    plan = lower_dag(dag, environment_id="brain-env", package_id="brain-package")
    return dag, plan


def worker(plan, worker_id: str, *, slots=1, online=True, accepting=True,
           env_ok=True, program_ok=True, cpu=20.0, memory=8_000_000_000):
    return WorkerState(
        worker_id,
        total_slots=slots,
        online=online,
        accepting_work=accepting,
        cpu_percent=cpu,
        total_memory_bytes=memory,
        available_memory_bytes=memory * 3 // 4,
        cpu_cores=max(1, slots * 2),
        environment_ids={plan.program.environment_id} if env_ok else {"wrong-env"},
        prepared_program_ids={plan.program.id} if program_ok else frozenset(),
        supported_modes=set(ExecutionMode),
    )


def snapshot(plan, ready_ids, workers, *, sid, completed=(), data=()):
    return ClusterSnapshot(
        plan_id=plan.id,
        run_id="brain-run",
        snapshot_id=sid,
        ready=tuple(ReadyTask(task_id, i) for i, task_id in enumerate(ready_ids)),
        workers=tuple(workers),
        completed_task_ids=frozenset(completed),
        data=tuple(data),
    )


def output_locations(plan, placements, *, size=8):
    """Pretend each completed task's immutable output now lives on its worker."""
    result = []
    for placement in placements:
        manifest = plan.task_index[placement.task_id]
        for value in manifest.outputs:
            if value.kind == ValueKind.IMMUTABLE:
                result.append(DataLocation(
                    value.id,
                    DataForm.IMMUTABLE_VALUE,
                    (Replica(placement.worker_id),),
                    size,
                ))
    return result


def run_two_wave_example(example: str):
    dag, plan = make_plan(example)
    sched = Scheduler(plan)
    readiness = dag.new_readiness()
    workers = tuple(worker(plan, f"W{i}") for i in (1, 2, 3))

    roots = readiness.ready
    assert len(roots) == 3
    first = sched.schedule(snapshot(plan, roots, workers, sid="wave-1"))
    assert not first.unplaced
    assert {p.task_id for p in first.placements} == set(roots)
    assert {p.worker_id for p in first.placements} == {"W1", "W2", "W3"}
    assert all(plan.task_index[p.task_id].mode == ExecutionMode.ISOLATED_CANDIDATE
               for p in first.placements)

    data = output_locations(plan, first.placements)
    completed = set()
    for placement in first.placements:
        readiness.mark_completed(placement.task_id)
        completed.add(placement.task_id)

    join_ready = readiness.ready
    assert len(join_ready) == 1
    join_id = join_ready[0]
    assert plan.task_index[join_id].dependencies == frozenset(completed)

    second_state = snapshot(plan, join_ready, workers, sid="wave-2",
                            completed=completed, data=data)
    second = sched.schedule(second_state)
    assert len(second.placements) == 1 and not second.unplaced
    join = second.placements[0]
    assert join.task_id == join_id
    # One root result is local to the selected worker; the other two are remote.
    assert len(join.preference.locality.local_input_ids) == 1
    assert len(join.preference.locality.remote_input_ids) == 2
    assert join.preference.locality.known_remote_bytes == 16

    readiness.mark_completed(join_id)
    completed.add(join_id)
    assert not readiness.ready
    assert completed == set(plan.task_index)
    plan.validate_against(dag)
    return first, second


@pytest.mark.parametrize("example", ["prime_workload", "collatz_workload"])
def test_realistic_cpu_examples_complete_in_two_scheduler_waves(example):
    run_two_wave_example(example)


def test_capacity_shortage_defers_one_root_then_recovers_next_snapshot():
    dag, plan = make_plan("prime_workload")
    sched = Scheduler(plan)
    readiness = dag.new_readiness()
    workers = (worker(plan, "W1"), worker(plan, "W2"))

    first = sched.schedule(snapshot(plan, readiness.ready, workers, sid="capacity-1"))
    assert len(first.placements) == 2
    assert len(first.unplaced) == 1
    assert first.unplaced[0].reason == UnplacedReason.NO_CAPACITY

    completed = set()
    data = output_locations(plan, first.placements)
    for p in first.placements:
        readiness.mark_completed(p.task_id)
        completed.add(p.task_id)

    assert len(readiness.ready) == 1
    second = sched.schedule(snapshot(plan, readiness.ready, workers, sid="capacity-2",
                                     completed=completed, data=data))
    assert len(second.placements) == 1 and not second.unplaced
    data += output_locations(plan, second.placements)
    readiness.mark_completed(second.placements[0].task_id)
    completed.add(second.placements[0].task_id)

    # The join becomes READY only after the deferred root completes.
    assert len(readiness.ready) == 1
    third = sched.schedule(snapshot(plan, readiness.ready, workers, sid="capacity-3",
                                    completed=completed, data=data))
    assert len(third.placements) == 1 and not third.unplaced


def test_worker_loss_makes_join_unplaceable_until_missing_result_is_replicated():
    dag, plan = make_plan("collatz_workload")
    sched = Scheduler(plan)
    readiness = dag.new_readiness()
    workers = tuple(worker(plan, f"W{i}") for i in (1, 2, 3))
    first = sched.schedule(snapshot(plan, readiness.ready, workers, sid="loss-1"))
    data = output_locations(plan, first.placements)
    completed = set()
    for p in first.placements:
        readiness.mark_completed(p.task_id)
        completed.add(p.task_id)

    # Simulate losing whichever worker owns one root result.
    lost = first.placements[-1].worker_id
    degraded_workers = tuple(replace(w, online=False, accepting_work=False)
                             if w.worker_id == lost else w for w in workers)
    blocked = sched.schedule(snapshot(plan, readiness.ready, degraded_workers,
                                      sid="loss-2", completed=completed, data=data))
    assert not blocked.placements
    assert blocked.unplaced[0].reason == UnplacedReason.NO_ELIGIBLE_WORKER
    assert all(UnplacedReason.INPUT_UNAVAILABLE in rejection.reasons
               for rejection in blocked.unplaced[0].workers
               if rejection.worker_id != lost)

    # Coordinator later reports an AVAILABLE replica of the lost result on W1/W2.
    surviving = next(w.worker_id for w in degraded_workers if w.online)
    repaired = []
    for location in data:
        if any(r.worker_id == lost for r in location.replicas):
            repaired.append(replace(location, replicas=location.replicas + (Replica(surviving),)))
        else:
            repaired.append(location)
    recovered = sched.schedule(snapshot(plan, readiness.ready, degraded_workers,
                                        sid="loss-3", completed=completed, data=repaired))
    assert len(recovered.placements) == 1 and not recovered.unplaced
    assert recovered.placements[0].worker_id != lost


def test_hard_compatibility_filters_cannot_be_overridden_by_load_or_capacity():
    dag, plan = make_plan("prime_workload")
    ready = dag.new_readiness().ready
    workers = (
        worker(plan, "W1", slots=8, env_ok=False, cpu=0.0),
        worker(plan, "W2", slots=8, program_ok=False, cpu=0.0),
        worker(plan, "W3", slots=3, cpu=95.0),
    )
    decision = Scheduler(plan).schedule(snapshot(plan, ready, workers, sid="compat"))
    assert not decision.unplaced
    assert {p.worker_id for p in decision.placements} == {"W3"}


def test_realistic_snapshot_is_deterministic_under_worker_input_permutation():
    dag, plan = make_plan("prime_workload")
    ready = dag.new_readiness().ready
    workers = tuple(worker(plan, f"W{i}") for i in (1, 2, 3))
    sched = Scheduler(plan)
    a = sched.schedule(snapshot(plan, ready, workers, sid="det"))
    b = sched.schedule(snapshot(plan, ready, tuple(reversed(workers)), sid="det"))
    assert a == b


INTEGER_WORKLOADS = {
    "matrix_checksum": '''
def work(size, offset):
    total = 0
    for i in range(size):
        for j in range(size):
            cell = 0
            for k in range(size):
                cell += (i + k + offset) * (k + j + 1)
            total += cell
    return total

a = work(20, 1)
b = work(20, 2)
c = work(20, 3)
total = a + b + c
''',
    "lcg_monte_carlo": '''
def work(seed, samples):
    state = seed
    inside = 0
    for i in range(samples):
        state = (1103515245 * state + 12345) % 2147483648
        x = state % 1000000
        state = (1103515245 * state + 12345) % 2147483648
        y = state % 1000000
        if x * x + y * y <= 1000000 * 1000000:
            inside += 1
    return inside

a = work(1, 1000)
b = work(2, 1000)
c = work(3, 1000)
total = a + b + c
''',
    "iterative_fibonacci": '''
def work(start, end):
    total = 0
    for n in range(start, end):
        a = 0
        b = 1
        i = 0
        while i < n:
            t = a + b
            a = b
            b = t
            i += 1
        total += a
    return total

a = work(1, 20)
b = work(20, 30)
c = work(30, 35)
total = a + b + c
''',
}


@pytest.mark.parametrize("name,source", INTEGER_WORKLOADS.items())
def test_integer_heavy_additional_workloads_parallelize_end_to_end(name, source):
    dag = analyze_source(source, filename=f"{name}.py")
    plan = lower_dag(dag, environment_id="brain-env", package_id="brain-package")
    ready = dag.new_readiness().ready
    assert len(ready) == 3
    assert all(plan.task_index[t].mode == ExecutionMode.ISOLATED_CANDIDATE for t in ready)
    workers = tuple(worker(plan, f"W{i}") for i in (1, 2, 3))
    decision = Scheduler(plan).schedule(snapshot(plan, ready, workers, sid=name))
    assert not decision.unplaced
    assert {p.worker_id for p in decision.placements} == {"W1", "W2", "W3"}


def test_explicit_task_fallback_distributes_float_work_even_when_body_is_not_auto_proven():
    source = '''
from dag_runtime import task

@task
def integrate(start, end, steps):
    h = (end - start) / steps
    total = 0.0
    for i in range(steps):
        x = start + (i + 0.5) * h
        total += 4.0 / (1.0 + x * x)
    return total * h

a = integrate(0.0, 0.33, 1000)
b = integrate(0.33, 0.66, 1000)
c = integrate(0.66, 1.0, 1000)
total = a + b + c
'''
    dag = analyze_source(source, filename="float_task_fallback.py")
    plan = lower_dag(dag, environment_id="brain-env", package_id="brain-package")
    ready = dag.new_readiness().ready
    assert len(ready) == 3
    assert all(plan.task_index[t].mode == ExecutionMode.ISOLATED_CANDIDATE for t in ready)
    workers = tuple(worker(plan, f"W{i}") for i in (1, 2, 3))
    decision = Scheduler(plan).schedule(snapshot(plan, ready, workers, sid="float-task"))
    assert not decision.unplaced
    assert {p.worker_id for p in decision.placements} == {"W1", "W2", "W3"}


def _assert_new_precision_workload_completes_two_waves(source: str, name: str):
    dag = analyze_source(source, filename=f"{name}.py")
    plan = lower_dag(dag, environment_id="brain-env", package_id="brain-package")
    sched = Scheduler(plan)
    readiness = dag.new_readiness()
    workers = tuple(worker(plan, f"W{i}") for i in (1, 2, 3))

    roots = readiness.ready
    assert len(roots) == 3
    assert all(plan.task_index[t].mode == ExecutionMode.ISOLATED_CANDIDATE for t in roots)

    first = sched.schedule(snapshot(plan, roots, workers, sid=f"{name}-1"))
    assert not first.unplaced
    assert {p.worker_id for p in first.placements} == {"W1", "W2", "W3"}

    completed = set()
    data = output_locations(plan, first.placements)
    for placement in first.placements:
        readiness.mark_completed(placement.task_id)
        completed.add(placement.task_id)

    assert len(readiness.ready) == 1
    join_id = readiness.ready[0]
    assert plan.task_index[join_id].mode == ExecutionMode.ISOLATED_CANDIDATE
    assert plan.task_index[join_id].dependencies == frozenset(completed)

    second = sched.schedule(snapshot(
        plan,
        readiness.ready,
        workers,
        sid=f"{name}-2",
        completed=completed,
        data=data,
    ))
    assert len(second.placements) == 1 and not second.unplaced
    assert second.placements[0].task_id == join_id


def test_auto_proven_float_numerical_workload_parallelizes_end_to_end():
    source = '''
def integrate(start, end, steps):
    h = (end - start) / steps
    total = 0.0
    for i in range(steps):
        x = start + (i + 0.5) * h
        total += 4.0 / (1.0 + x * x)
    return total * h

a = integrate(0.0, 0.33, 1000)
b = integrate(0.33, 0.66, 1000)
c = integrate(0.66, 1.0, 1000)
total = a + b + c
'''
    _assert_new_precision_workload_completes_two_waves(source, "float-integration-auto")


def test_auto_proven_local_list_workload_parallelizes_end_to_end():
    source = '''
def work(n):
    values = []
    for i in range(n):
        values.append(i * i)
    total = 0
    for x in values:
        total += x
    return total

a = work(100)
b = work(200)
c = work(300)
total = a + b + c
'''
    _assert_new_precision_workload_completes_two_waves(source, "local-list-auto")
