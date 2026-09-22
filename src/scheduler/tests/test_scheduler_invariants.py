"""Independent legality oracle, hash-seed checks and repeated-call invariants."""
from collections import Counter
from dataclasses import replace
import json
import os
from pathlib import Path
import random
import subprocess
import sys

import pytest

from execution import AttemptIdentity, ExecutionMode
from scheduler import (
    CommitmentPhase, DataForm, DataLocation, ReadyTask, Replica, ReplicaStatus,
    Scheduler, TaskAffinity, TaskCommitment, WorkerContext,
)


def legal_scalar_pair(plan, state, task_id, worker_id, free):
    """Test oracle from the public contracts, independent of matcher/ranking helpers."""
    worker = next(w for w in state.workers if w.worker_id == worker_id)
    task = plan.task_index[task_id]
    if not worker.online or not worker.accepting_work or free <= 0:
        return False
    if plan.program.environment_id not in worker.environment_ids or plan.program.id not in worker.prepared_program_ids:
        return False
    if task.mode not in worker.supported_modes:
        return False
    affinity = next((a for a in state.affinities if a.task_id == task_id), None)
    if affinity:
        if affinity.required_worker is not None and affinity.required_worker != worker_id:
            return False
        if affinity.allowed_workers is not None and worker_id not in affinity.allowed_workers:
            return False
    online = {w.worker_id for w in state.workers if w.online}
    for value in task.inputs:
        locations = [d for d in state.data if d.value_id == value.id]
        if not any(r.status == ReplicaStatus.AVAILABLE and r.worker_id in online for d in locations for r in d.replicas):
            return False
    return True


@pytest.mark.parametrize("seed", range(16))
def test_randomized_legality_and_immutability(build_plan, worker, snapshot, seed):
    """400 snapshots: every proposal legal, no overbooking/duplicates, no avoidable idle pair."""
    source = "\n".join([f"x{i}={i}" for i in range(6)] + [f"y{i}=x{i}+1" for i in range(6)]
                       + ["z=y0+1", "zz=z+1"])
    dag, plan = build_plan(source)
    readiness = dag.new_readiness()
    completed = set(dag.sources())
    for task_id in dag.sources():
        readiness.mark_completed(task_id)
    graph_before = dag.to_dict()
    ready_before = readiness.ready
    plan_before = (plan.id, plan.tasks, plan.values, plan.edges, plan.bindings)
    scheduler = Scheduler(plan)
    rng = random.Random(seed)
    for round_id in range(25):
        workers, occupied_slots = [], []
        for i in range(rng.randrange(5)):
            total = rng.randrange(5)
            running = rng.randrange(total + 1)
            reserved = rng.randrange(total - running + 1)
            worker_id = f"W{i}"
            workers.append(worker(plan, worker_id, total_slots=total, running_slots=running, reserved_slots=reserved,
                                  online=rng.random() > .15, accepting_work=rng.random() > .15,
                                  cpu_percent=rng.randrange(101), available_memory_bytes=rng.randrange(16_001),
                                  cpu_cores=rng.choice((1, 2, 8)),
                                  environment_ids={plan.program.environment_id} if rng.random() > .1 else set(),
                                  prepared_program_ids={plan.program.id} if rng.random() > .1 else set()))
            occupied_slots.extend((worker_id, CommitmentPhase.RUNNING) for _ in range(running))
            occupied_slots.extend((worker_id, rng.choice((CommitmentPhase.RESERVED, CommitmentPhase.DISPATCHED,
                                                         CommitmentPhase.WAITING_TRANSFER))) for _ in range(reserved))
        rng.shuffle(occupied_slots)
        ready, committed, blocked, affinities = [], [], set(), []
        worker_ids = [w.worker_id for w in workers]
        for sequence, task_id in enumerate(ready_before):
            choice = rng.randrange(6)
            if choice == 0 and occupied_slots:
                owner, phase = occupied_slots.pop()
                committed.append(TaskCommitment(AttemptIdentity(plan.id, "run-1", task_id, f"a-{round_id}"), owner, phase))
            elif choice == 1:
                blocked.add(task_id)
            else:
                ready.append(ReadyTask(task_id, sequence, rng.randrange(12)))
            if worker_ids and rng.random() < .6:
                allowed = frozenset(w for w in worker_ids if rng.random() < .6)
                affinities.append(TaskAffinity(task_id, allowed_workers=allowed))
        data = []
        for value in (plan.value_index[plan.final_bindings[f"x{i}"]] for i in range(6)):
            if rng.random() < .1:
                continue
            replicas = tuple(Replica(w, ReplicaStatus.AVAILABLE if rng.random() > .25 else ReplicaStatus.IN_FLIGHT)
                             for w in worker_ids if rng.random() < .65)
            data.append(DataLocation(value.id, DataForm.IMMUTABLE_VALUE, replicas, rng.choice((None, 0, 100, 10**12))))
        state = snapshot(plan, workers=workers, ready=ready, commitments=committed, data=data,
                         affinities=affinities, blocked_task_ids=blocked, completed_task_ids=completed,
                         snapshot_id=f"{seed}-{round_id}")
        before = (state.workers, state.ready, state.commitments, state.data, state.affinities)
        decision = scheduler.schedule(state)
        used = Counter()
        placed = set()
        for proposal in decision.placements:
            assert proposal.task_id in {r.task_id for r in ready}
            assert proposal.task_id not in placed
            assert proposal.task_id not in {c.attempt.task_id for c in committed}
            original = next(w for w in workers if w.worker_id == proposal.worker_id)
            assert legal_scalar_pair(plan, state, proposal.task_id, proposal.worker_id,
                                     original.free_slots - used[proposal.worker_id])
            placed.add(proposal.task_id)
            used[proposal.worker_id] += 1
        assert placed | {u.task_id for u in decision.unplaced} == {r.task_id for r in ready}
        assert not placed.intersection(u.task_id for u in decision.unplaced)
        assert all(used[w.worker_id] <= w.free_slots for w in workers)
        for unplaced in decision.unplaced:
            assert not any(legal_scalar_pair(plan, state, unplaced.task_id, w.worker_id,
                                            w.free_slots - used[w.worker_id]) for w in workers)
        shuffled = replace(state, workers=tuple(reversed(state.workers)), ready=tuple(reversed(state.ready)),
                           data=tuple(replace(d, replicas=tuple(reversed(d.replicas))) for d in reversed(state.data)),
                           affinities=tuple(reversed(state.affinities)), commitments=tuple(reversed(state.commitments)))
        assert decision == scheduler.schedule(state) == scheduler.schedule(shuffled)
        assert before == (state.workers, state.ready, state.commitments, state.data, state.affinities)
    assert dag.to_dict() == graph_before
    assert readiness.ready == ready_before
    assert plan_before == (plan.id, plan.tasks, plan.values, plan.edges, plan.bindings)


HASH_PROGRAM = '''
import json
from dataclasses import asdict
from dag_runtime.dag_engine import analyze_source
from execution import ExecutionMode, lower_dag
from scheduler import ClusterSnapshot, ReadyTask, WorkerState, schedule
plan=lower_dag(analyze_source("a=1\\nb=2\\nc=3\\nd=4\\n"),environment_id="env")
workers=tuple(WorkerState(w,2,environment_ids={"env"},prepared_program_ids={plan.program.id},
                          supported_modes={ExecutionMode.ISOLATED_CANDIDATE}) for w in {"W1","W2","W3"})
ready=tuple(ReadyTask(t,int(t[1:])) for t in set(plan.task_index))
state=ClusterSnapshot(plan.id,"run","snapshot",workers=workers,ready=ready)
print(json.dumps(asdict(schedule(plan,state)),sort_keys=True))
'''


def test_package_imports_and_hash_seed_independence():
    """Separate processes with permuted set order produce byte-identical proposals."""
    root = Path(__file__).parents[2]
    outputs = []
    for seed in ("1", "932", "78127"):
        environment = dict(os.environ, PYTHONHASHSEED=seed)
        result = subprocess.run([sys.executable, "-c", HASH_PROGRAM], cwd=root, env=environment,
                                text=True, capture_output=True, check=True)
        outputs.append(result.stdout)
    assert len(set(outputs)) == 1
    assert len(json.loads(outputs[0])["placements"]) == 4


def test_disconnect_between_snapshots_requires_new_proposal(roots, worker, snapshot):
    """Old decision stays an old proposal; a new offline snapshot never selects that worker."""
    dag, plan = roots
    state = snapshot(plan, dag.sources()[:1], (worker(plan),))
    scheduler = Scheduler(plan)
    old = scheduler.schedule(state)
    assert old.placements
    fresh = replace(state, snapshot_id="snapshot-2", workers=(replace(state.workers[0], online=False),))
    new = scheduler.schedule(fresh)
    assert new.snapshot_id != old.snapshot_id and not new.placements
    assert scheduler.schedule(state) == old  # no hidden worker connection state


def test_followup_snapshot_reservation_prevents_duplicate_dispatch(roots, worker, snapshot):
    dag, plan = roots
    a, b, *_ = dag.sources()
    scheduler = Scheduler(plan)
    state = snapshot(plan, (a, b), (worker(plan, total_slots=2),))
    result = scheduler.schedule(state)
    commitments = tuple(TaskCommitment(AttemptIdentity(plan.id, "run-1", p.task_id, "attempt-1"),
                                       p.worker_id, CommitmentPhase.WAITING_TRANSFER) for p in result.placements)
    reserved = replace(state, snapshot_id="snapshot-2", ready=(), commitments=commitments,
                       workers=(replace(state.workers[0], reserved_slots=2),))
    assert not scheduler.schedule(reserved).placements
    assert state.workers[0].reserved_slots == 0


def test_production_import_boundary():
    """Scheduler production has no execution, network, process or analysis imports."""
    import ast
    root = Path(__file__).parents[1]
    forbidden = {"socket", "subprocess", "multiprocessing", "threading", "asyncio", "time",
                 "random", "pickle", "sqlite3", "dag_runtime.dag_engine", "dag_runtime.dag_static"}
    for filename in ("__init__.py", "model.py", "scheduler.py"):
        tree = ast.parse((root / filename).read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                assert not {a.name for a in node.names}.intersection(forbidden)
            elif isinstance(node, ast.ImportFrom):
                assert node.module not in forbidden


@pytest.mark.parametrize("seed", range(5))
def test_randomized_context_and_worker_shadow_capacity(build_plan, worker, snapshot, seed):
    """125 snapshots jointly constrain worker slots and prepared native-context entries."""
    dag, plan = build_plan("\n".join([f"a{i}=[{i}]" for i in range(6)] + [f"a{i}.append(10)" for i in range(6)]))
    tasks = plan.tasks[6:]
    assert all(t.mode == ExecutionMode.SHARED_CONTEXT for t in tasks)
    assert all(not t.dependencies.intersection(m.task_id for m in tasks) for t in tasks)
    scheduler = Scheduler(plan)
    rng = random.Random(seed)
    for round_id in range(25):
        workers = tuple(worker(plan, f"W{i}", total_slots=rng.randrange(5)) for i in range(3))
        contexts = []
        affinity = []
        for i in range(3):
            ids = {t.task_id for t in tasks[i::3]}
            owner = rng.choice(workers).worker_id
            context_id = f"C{i}"
            contexts.append(WorkerContext(context_id, owner, {t for t in sorted(ids) if rng.random() > .2}, rng.randrange(4)))
            affinity.extend(TaskAffinity(t, context_id=context_id) for t in sorted(ids))
        state = snapshot(plan, (t.task_id for t in tasks), workers, contexts=contexts, affinities=affinity)
        decision = scheduler.schedule(state)
        used_workers, used_contexts = Counter(), Counter()
        context_by_id = {c.context_id: c for c in contexts}
        for placement in decision.placements:
            context = context_by_id[placement.context_id]
            assert context.worker_id == placement.worker_id
            assert placement.task_id in context.prepared_task_ids
            used_workers[placement.worker_id] += 1
            used_contexts[placement.context_id] += 1
        assert all(used_workers[w.worker_id] <= w.free_slots for w in workers)
        assert all(used_contexts[c.context_id] <= c.available_slots for c in contexts)
        for unplaced in decision.unplaced:
            context_id = next(a.context_id for a in affinity if a.task_id == unplaced.task_id)
            context = context_by_id[context_id]
            w = next(w for w in workers if w.worker_id == context.worker_id)
            if (unplaced.task_id in context.prepared_task_ids
                    and used_contexts[context_id] < context.available_slots
                    and used_workers[w.worker_id] < w.free_slots):
                # F58 adds transferable namespace-seed requirements. A randomly
                # prepared context may still be unplaceable when this synthetic
                # snapshot intentionally omitted the required producer replica.
                rejection = next(r for r in unplaced.workers if r.worker_id == w.worker_id)
                assert UnplacedReason.INPUT_UNAVAILABLE in rejection.reasons
        assert scheduler.schedule(state) == decision


def test_three_worker_documented_example():
    from scheduler.examples.placement import example
    plan, state, decision = example()
    assert [(p.task_id, p.worker_id) for p in decision.placements] == [
        ("T000004", "W1"), ("T000005", "W2"), ("T000006", "W3")]
    assert len(state.ready) == len(state.workers) == 3
    assert all(not p.preference.locality.remote_input_ids for p in decision.placements)
    assert state.data[-1].size_bytes is None
