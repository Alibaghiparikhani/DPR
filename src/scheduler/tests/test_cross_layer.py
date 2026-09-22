"""Integration uses real DAG readiness in the test harness, never in the scheduler.

The harness simulates successful completions/input preparation; it does not run
user code or claim to be an execution adapter.
"""
from dataclasses import replace
from pathlib import Path

import pytest

from dag_runtime.dag_engine import analyze_file, analyze_source
from dag_runtime.dag_model import DAG
from execution import ExecutionMode, ValueKind, lower_dag
from execution.tests.test_full_stack_torture import SOURCE as TORTURE_SOURCE
from scheduler import (
    DataForm, DataLocation, Replica, Scheduler, TaskAffinity, UnplacedReason,
    WorkerContext, schedule,
)


def prepared_inputs(plan, tasks, workers):
    """Explicit test-coordinator attestations for every required data/version key."""
    locations = {}
    for task_id in tasks:
        manifest = plan.task_index[task_id]
        requirements = (manifest.inputs if manifest.mode == ExecutionMode.ISOLATED_CANDIDATE
                        else plan.context_seed_requirements(task_id))
        states = {o.object_id: next(iter(o.state_inputs), None) for o in manifest.objects}
        for value in requirements:
            if value.kind not in {ValueKind.IMMUTABLE, ValueKind.SHARED_REFERENCE}:
                continue
            shared = value.kind == ValueKind.SHARED_REFERENCE
            state_id = (states.get(value.value.object_id) if shared and manifest.mode == ExecutionMode.ISOLATED_CANDIDATE else None)
            physical = value.id if shared else plan.immutable_representation_id(value.id)
            owner = workers[int(value.id[1:]) % len(workers)].worker_id
            locations[physical, state_id] = DataLocation(
                physical, DataForm.OBJECT_SNAPSHOT if shared else DataForm.IMMUTABLE_VALUE,
                (Replica(owner),), None if shared else 64, state_id)
    return tuple(locations.values())


@pytest.mark.parametrize("fixture", ["diamond", "stress_dag", "full_stack_torture"])
def test_real_plan_to_scheduler_over_all_readiness_waves(worker, snapshot, fixture):
    """All real modes and every original edge survive; only harness advances readiness."""
    if fixture == "full_stack_torture":
        dag = analyze_source(TORTURE_SOURCE, filename="full_stack_torture.py")
    else:
        dag = analyze_file(Path(__file__).parents[2] / "dag_runtime" / "examples" / f"{fixture}.py")
    plan = lower_dag(dag, environment_id="integration-env", package_id="project")
    before = dag.to_dict()
    scheduler = Scheduler(plan)
    workers = tuple(worker(plan, f"W{i}", total_slots=i) for i in (1, 2, 3))
    readiness = dag.new_readiness()
    completed, seen_modes, seen_tokens = set(), set(), set()
    count = 0
    while readiness.ready:
        ready = readiness.ready
        native = {t for t in ready if plan.task_index[t].mode != ExecutionMode.ISOLATED_CANDIDATE}
        state = snapshot(plan, ready, workers, snapshot_id=f"wave-{count}", completed_task_ids=completed,
                         data=prepared_inputs(plan, ready, workers),
                         contexts=(WorkerContext("native-scope", "W2", native),),
                         affinities=tuple(TaskAffinity(t, context_id="native-scope") for t in sorted(native)))
        decision = scheduler.schedule(state)
        assert decision.placements, decision.unplaced
        assert decision == scheduler.schedule(state)
        for placement in decision.placements:
            manifest = plan.task_index[placement.task_id]
            assert manifest.dependencies <= completed
            assert {edge.source for edge in manifest.prerequisites} <= completed
            seen_modes.add(manifest.mode)
            seen_tokens.update(v.kind for v in manifest.state_inputs if v.is_state_token)
            if manifest.mode != ExecutionMode.ISOLATED_CANDIDATE:
                assert placement.worker_id == "W2" and placement.context_id == "native-scope"
                # F58/F16: context namespace seeds may legitimately be remote;
                # scheduler locality records the transfer rather than pretending
                # the prepared context already owns isolated ancestor values.
                accounted = set(placement.preference.locality.local_input_ids +
                                placement.preference.locality.remote_input_ids)
                assert accounted == {v.id for v in plan.context_seed_requirements(manifest.task_id)}
            counted = set(placement.preference.locality.local_input_ids + placement.preference.locality.remote_input_ids)
            assert not counted.intersection(v.id for v in manifest.inputs if v.is_state_token)
        # Coordinator simulation, deliberately outside Scheduler.schedule().
        for placement in decision.placements:
            readiness.mark_completed(placement.task_id)
            completed.add(placement.task_id)
        count += 1
        assert count <= len(plan.tasks)
    assert completed == set(plan.task_index)
    assert dag.to_dict() == before
    plan.validate_against(dag)
    if fixture != "diamond":
        assert seen_modes == set(ExecutionMode)
        assert seen_tokens == {ValueKind.NAMESPACE_STATE, ValueKind.COMPLETION_STATE, ValueKind.OBJECT_STATE}


def test_post_append_alias_reader_needs_exact_prepared_version(build_plan, worker, snapshot):
    """An old copy is not usable after append; exact state snapshot through alias is."""
    dag, plan = build_plan("a=[1,2]\nalias=a\nb=sum(alias)\na.append(3)\nc=sum(alias)")
    tasks={m.task.source:m for m in plan.tasks}
    make, before, append, after=(tasks[src] for src in ('a=[1,2]','b=sum(alias)','a.append(3)','c=sum(alias)'))
    obj = after.objects[0]
    alias_id, = obj.input_ids
    version_id, = obj.state_inputs
    assert append.task_id in after.dependencies
    assert before.task_id in append.dependencies
    assert obj.object_id == append.objects[0].object_id
    initial = DataLocation(alias_id, DataForm.OBJECT_SNAPSHOT, (Replica("W1"),), 24)
    state = snapshot(plan, (after.task_id,), (worker(plan, "W1"), worker(plan, "W2")), data=(initial,))
    assert schedule(plan, state).unplaced[0].reason == UnplacedReason.INPUT_UNAVAILABLE
    current = replace(initial, object_state_id=version_id)
    chosen = schedule(plan, replace(state, data=(initial, current))).placements[0]
    assert chosen.worker_id == "W1"
    assert chosen.preference.locality.local_input_ids == (alias_id,)
    # A new snapshot does not satisfy the original pre-mutation reader, either.
    old_read = replace(state, ready=(type(state.ready[0])(before.task_id, 0),), data=(current,))
    assert not schedule(plan, old_read).placements
    assert schedule(plan, replace(old_read, data=(initial,))).placements


def test_alias_location_not_invented_from_other_binding(build_plan, worker, snapshot):
    """Coordinator must provide the logical alias representation; scheduler does not redo alias lowering."""
    _, plan = build_plan("a=[1]\nalias=a\nb=sum(alias)")
    task = plan.tasks[-1]
    original = DataLocation(plan.final_bindings["a"], DataForm.OBJECT_SNAPSHOT, (Replica("W1"),))
    state = snapshot(plan, (task.task_id,), (worker(plan),), data=(original,))
    assert not schedule(plan, state).placements
    alias = replace(original, value_id=plan.final_bindings["alias"])
    assert schedule(plan, replace(state, data=(alias,))).placements


def test_live_mutator_context_does_not_follow_snapshot_replica(build_plan, worker, snapshot):
    """A copy on W1 cannot authorize mutation; live alias context is on W2."""
    _, plan = build_plan("a=[1]\nalias=a\nalias.append(2)")
    task = plan.tasks[-1]
    stale_alias = DataLocation(plan.final_bindings["alias"], DataForm.OBJECT_SNAPSHOT, (Replica("W1"),), 0)
    seed = next(v for v in plan.context_seed_requirements(task.task_id) if v.value.name == "a")
    live_seed = DataLocation(seed.id, DataForm.OBJECT_SNAPSHOT, (Replica("W2"),), 0)
    state = snapshot(plan, (task.task_id,), (worker(plan, "W1"), worker(plan, "W2")), data=(stale_alias, live_seed),
                     contexts=(WorkerContext("C", "W2", {task.task_id}),),
                     affinities=(TaskAffinity(task.task_id, context_id="C"),))
    assert schedule(plan, state).placements[0].worker_id == "W2"


@pytest.mark.parametrize("source,token_kind", [
    ("getattr(obj,name)\n1+2\n3*4", ValueKind.NAMESPACE_STATE),
    ("a=1\nb=10//a\n1+2\n3*4", ValueKind.COMPLETION_STATE),
])
def test_fence_tokens_need_no_locations_and_recovered_siblings_place(build_plan, worker, snapshot, source, token_kind):
    dag, plan = build_plan(source)
    a, b = plan.tasks[-2:]
    assert a.mode == b.mode == ExecutionMode.ISOLATED_CANDIDATE
    assert a.state_inputs[0].kind == token_kind
    # Native earlier operation not offered as READY again; only post-fence siblings.
    state = snapshot(plan, (a.task_id, b.task_id), (worker(plan, "W1"), worker(plan, "W2")))
    chosen = schedule(plan, state).placements
    assert len(chosen) == 2 and {p.worker_id for p in chosen} == {"W1", "W2"}
    assert all(p.preference.locality.known_remote_bytes == p.preference.locality.unknown_remote_inputs == 0 for p in chosen)


def test_absent_native_names_do_not_become_missing_payloads(build_plan, worker, snapshot):
    _, plan = build_plan("if condition:\n    x=possibly_missing\nelse:\n    x=1")
    task = plan.tasks[0]
    assert task.mode == ExecutionMode.NATIVE_REGION
    state = snapshot(plan, (task.task_id,), (worker(plan),),
                     contexts=(WorkerContext("C", "W1", {task.task_id}),),
                     affinities=(TaskAffinity(task.task_id, context_id="C"),))
    assert schedule(plan, state).placements  # native Python owns conditional lookup/error timing


def test_scheduler_does_not_call_analyzer_or_readiness(roots, worker, snapshot, monkeypatch):
    dag, plan = roots
    state = snapshot(plan, dag.sources(), (worker(plan, total_slots=4),))
    def forbidden(*args, **kwargs):
        raise AssertionError("Scheduler crossed into DAG analysis/readiness")
    monkeypatch.setattr("dag_runtime.dag_engine.analyze_source", forbidden)
    monkeypatch.setattr(DAG, "new_readiness", forbidden)
    monkeypatch.setattr(DAG, "mark_completed", forbidden)
    monkeypatch.setattr(DAG, "initial_ready_tasks", forbidden)
    assert len(schedule(plan, state).placements) == 4


def test_multiple_outputs_feed_join_by_value_id(build_plan, worker, snapshot):
    dag, plan = build_plan("x,y=(1,2)\nz=x+y")
    task = plan.tasks[-1]
    assert len(task.inputs) == 2
    ws = (worker(plan),)
    state = snapshot(plan, (task.task_id,), ws, data=prepared_inputs(plan, (task.task_id,), ws))
    chosen = schedule(plan, state).placements[0]
    assert set(chosen.preference.locality.local_input_ids) == set(task.task.inputs)
