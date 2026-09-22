import textwrap

import pytest

from dag_runtime.dag_engine import analyze_source
from execution import ExecutionMode, ValueKind, lower_dag
from scheduler import ClusterSnapshot, DataForm, DataLocation, ReadyTask, Replica, WorkerState


@pytest.fixture
def build_plan():
    def build(source):
        dag = analyze_source(textwrap.dedent(source).strip() + "\n")
        return dag, lower_dag(dag, environment_id="test-env", package_id="test-package")
    return build


@pytest.fixture
def roots(build_plan):
    return build_plan("a=1\nb=2\nc=3\nd=4\n")


@pytest.fixture
def worker():
    def make(plan, worker_id="W1", **changes):
        fields = dict(total_slots=1, cpu_percent=30, total_memory_bytes=16_000,
                      available_memory_bytes=12_000, cpu_cores=4,
                      environment_ids={plan.program.environment_id},
                      prepared_program_ids={plan.program.id}, supported_modes=set(ExecutionMode))
        fields.update(changes)
        return WorkerState(worker_id, **fields)
    return make


@pytest.fixture
def snapshot():
    def make(plan, task_ids=(), workers=(), **changes):
        fields = dict(plan_id=plan.id, run_id="run-1", snapshot_id="snapshot-1",
                      ready=tuple(ReadyTask(t, i) for i, t in enumerate(task_ids)), workers=workers)
        # Phase 4/F58: prepared contexts may need latest transferable isolated
        # ancestor bindings even when the current source does not name them.
        # Unit snapshots that are testing context/capacity (and did not provide
        # explicit data facts) model the coordinator by attesting those seeds on
        # the context owner. Tests that need missing-data behavior pass data=...
        # explicitly and therefore bypass this convenience.
        if "data" not in changes and changes.get("contexts"):
            locations = {}
            for context in changes["contexts"]:
                for task_id in context.prepared_task_ids:
                    for requirement in plan.context_seed_requirements(task_id):
                        if requirement.kind == ValueKind.IMMUTABLE:
                            physical = plan.immutable_representation_id(requirement.id)
                            form = DataForm.IMMUTABLE_VALUE
                        elif requirement.kind == ValueKind.SHARED_REFERENCE:
                            physical = requirement.id
                            form = DataForm.OBJECT_SNAPSHOT
                        else:
                            continue
                        locations.setdefault((physical, None),
                            DataLocation(physical, form, (Replica(context.worker_id),), 64))
            if locations:
                fields["data"] = tuple(locations.values())
        fields.update(changes)
        return ClusterSnapshot(**fields)
    return make
