"""Bad contracts fail before dispatch; validation delegates graph invariants to DAG."""
from dataclasses import replace

import pytest

from dag_runtime.dag_engine import analyze_source
from dag_runtime.dag_model import Certainty, EffectKind
from execution import (
    CodeRequirement, ExecutionValidationError, ObjectAccess, ObjectRequirement,
    ProgramIdentity, ValueRequirement, lower_dag,
)


@pytest.mark.parametrize("source", ["x=", "return 1", "break", "nonlocal x"])
def test_diagnostic_dag_is_rejected(source):
    """Structurally valid diagnostics are not executable manifests."""
    dag = analyze_source(source)
    dag.validate()
    with pytest.raises(ExecutionValidationError, match="non-runnable"):
        lower_dag(dag, environment_id="env")


@pytest.mark.parametrize("environment", ["", " ", None, 1])
def test_environment_identity_must_be_explicit(environment):
    """Missing/empty environment is rejected; no host environment is silently assumed."""
    with pytest.raises(ExecutionValidationError, match="environment_id"):
        lower_dag(analyze_source("a=1"), environment_id=environment)


def test_lowering_rejects_mutated_input_dag(lowered):
    """DAG validation runs at the lowering boundary, not only at original construction."""
    dag, _ = lowered("a=1")
    dag.final_bindings = {"a": "missing"}
    with pytest.raises(ExecutionValidationError, match="Invalid input DAG"):
        lower_dag(dag, environment_id="env")


@pytest.mark.parametrize("field", ["tasks", "values", "edges", "definitions"])
def test_duplicate_plan_records_are_rejected(lowered, field):
    """Duplicate task/value/edge/definition records cannot be hidden by dictionary construction."""
    _, plan = lowered("def f(x): return x+1\na=1\nb=f(a)")
    records = getattr(plan, field)
    with pytest.raises(ExecutionValidationError, match="Duplicate"):
        replace(plan, **{field: records + records[:1]})


@pytest.mark.parametrize("field", ["tasks", "values", "edges", "definitions"])
def test_missing_plan_records_are_rejected(lowered, field):
    """Removing a required record fails DAG ownership/reference validation or provenance matching."""
    _, plan = lowered("def f(x): return x+1\na=1\nb=f(a)")
    with pytest.raises(ExecutionValidationError):
        replace(plan, **{field: ()})


@pytest.mark.parametrize("field,value", [
    ("source", "other=2"), ("dag_digest", "0" * 64),
    ("bindings", ()), ("assumptions", ()), ("final_bindings", {"bad": "missing"}),
    ("final_namespace", "missing"), ("final_object_states", {"other": "missing"}),
])
def test_plan_provenance_and_state_cannot_be_dropped(lowered, field, value):
    """Original source, bindings, assumptions, final namespace and object states are contract data, not optional decoration."""
    _, plan = lowered("a=[1]\na.append(2)\nunknown()")
    with pytest.raises(ExecutionValidationError):
        replace(plan, **{field: value})


def test_dependency_reason_cannot_be_silently_changed(lowered):
    """Matching predecessor IDs do not suffice if the manifest has lost the actual DATA/STATE/ORDER reasons."""
    _, plan = lowered("a=1\nunknown(a)")
    before, after = plan.tasks
    edge, = after.prerequisites
    altered = replace(edge, reasons=edge.reasons[:1])
    manifest = replace(after, prerequisites=(altered,))
    with pytest.raises(ExecutionValidationError, match="Dependency reasons"):
        replace(plan, tasks=(before, manifest))


def test_even_consistent_edge_removal_changes_graph_provenance(lowered):
    """Removing a valueless WAR edge and both adjacency entries still differs from the analyzed DAG."""
    _, plan = lowered("a=[1]\nx=sum(a)\na.append(2)")
    create, reader, writer = plan.tasks
    removed = (reader.task_id, writer.task_id)
    new_edges = tuple(e for e in plan.edges if (e.source, e.target) != removed)
    new_reader = replace(reader, task=replace(reader.task, dependents=frozenset()))
    new_writer = replace(writer,
                         task=replace(writer.task, dependencies=writer.dependencies - {reader.task_id}),
                         prerequisites=tuple(e for e in writer.prerequisites if e.source != reader.task_id))
    with pytest.raises(ExecutionValidationError, match="provenance"):
        replace(plan, tasks=(create, new_reader, new_writer), edges=new_edges)


def test_manifest_inputs_outputs_and_code_must_match_task(lowered):
    """A manifest cannot omit actual inputs/outputs or required definition identities."""
    _, plan = lowered("def f(x): return x+1\na=1\nb=f(a)")
    _, task = plan.tasks
    for changes in ({"inputs": ()}, {"outputs": ()}, {"prerequisites": ()},
                    {"code": CodeRequirement(plan.program.id)}):
        with pytest.raises(ExecutionValidationError):
            replace(task, **changes)


@pytest.mark.parametrize("changes", [
    {"object_id": "invented"}, {"storage": "native_namespace"}, {"projection": (99,)},
    {"alias_of": "missing"}, {"type_hint": "str"}, {"may_be_unbound": True},
])
def test_native_value_requirements_are_cross_checked_with_registry(lowered, changes):
    """Even structurally valid per-task values cannot disagree with the plan's authoritative Value records."""
    _, plan = lowered("a=[1]\nunknown(a)")
    before, task = plan.tasks
    inputs = tuple(ValueRequirement(replace(v.value, **changes)) if v.value.name == "a" else v for v in task.inputs)
    manifest = replace(task, inputs=inputs)
    with pytest.raises(ExecutionValidationError, match="Value requirement"):
        replace(plan, tasks=(before, manifest))


@pytest.mark.parametrize("source", [
    "unknown()", "n=0\nx=1//n", "a=[1]\na.append(2)", "g=globals()\nx=1",
])
def test_forced_isolation_rejects_unsafe_task_metadata(lowered, source):
    """Conservative/native/mutating/may-raise nodes cannot become candidates by changing placement alone."""
    _, plan = lowered(source)
    manifest = plan.tasks[-1]
    with pytest.raises(ExecutionValidationError, match="isolation"):
        replace(manifest, task=replace(manifest.task, placement="isolated_candidate"))


@pytest.mark.parametrize("changes", [
    {"certainty": Certainty.CONSERVATIVE}, {"effect": EffectKind.NAMESPACE},
    {"mutated_objects": ("V000001",)}, {"region_scope": "tail"}, {"proof": ""},
    {"placement": "remote_worker"}, {"runnable": False},
])
def test_candidate_consistency_checks_fail_closed(lowered, changes):
    """A candidate needs the original positive DAG isolation evidence, not just a certain-looking label."""
    _, plan = lowered("a=1")
    with pytest.raises(ExecutionValidationError):
        replace(plan.tasks[0], task=replace(plan.tasks[0].task, **changes))


@pytest.mark.parametrize("field", ["may_raise", "possible_side_effects", "unknown_calls"])
def test_candidate_characteristics_must_not_contradict_placement(lowered, field):
    """Exception/effect/call uncertainty contradicts isolated_candidate and is rejected."""
    _, plan = lowered("a=1")
    manifest = plan.tasks[0]
    stats = replace(manifest.task.characteristics, **{field: True})
    with pytest.raises(ExecutionValidationError):
        replace(manifest, task=replace(manifest.task, characteristics=stats))


def test_alias_identity_and_output_ownership_remain_dag_checks(lowered):
    """Changing a value's producer or alias group triggers the original DAG validator, not a second alias analysis."""
    _, plan = lowered("a=1\nb=a\nc=2")
    alias = next(v for v in plan.values if v.origin == 'alias')
    for changes in ({"object_id": "wrong"}, {"producer": "missing"}):
        values = tuple(replace(v, **changes) if v.id == alias.id else v for v in plan.values)
        with pytest.raises(ExecutionValidationError, match="Invalid execution graph"):
            replace(plan, values=values)


def test_unknown_state_and_value_schema_fail_closed(lowered):
    """Unknown token/origin/storage cannot silently degrade into transferable data."""
    _, plan = lowered("unknown()")
    token = next(v for v in plan.values if v.origin == "state")
    with pytest.raises(ExecutionValidationError, match="state token"):
        ValueRequirement(replace(token, name="@future_state"))
    value = plan.values[0]
    for changes in ({"origin": "future_origin"}, {"storage": "copied_object"}):
        with pytest.raises(ExecutionValidationError):
            ValueRequirement(replace(value, **changes))


def test_code_and_plan_must_refer_to_same_program(lowered):
    """A task cannot use code from a different environment/package/source identity."""
    _, plan = lowered("a=1")
    task = replace(plan.tasks[0], code=CodeRequirement("0" * 64))
    with pytest.raises(ExecutionValidationError, match="different program"):
        replace(plan, tasks=(task,))
    with pytest.raises(ExecutionValidationError, match="Duplicate code"):
        CodeRequirement(plan.program.id, ("D1", "D1"))


def test_invalid_identity_fields_fail_early(lowered):
    """Malformed hashes and empty package/filename fields cannot masquerade as usable code identity."""
    _, plan = lowered("a=1")
    p = plan.program
    for changes in ({"source_sha256": "not-hash"}, {"filename": ""}, {"package_id": ""}):
        with pytest.raises(ExecutionValidationError):
            replace(p, **changes)
    with pytest.raises(ExecutionValidationError):
        ProgramIdentity("A" * 64, "file.py", "env")


def test_plan_validation_against_wrong_graph_and_mutable_input_snapshot(lowered):
    """The plan owns its records even if the caller later reassigns DAG attributes; comparison then rejects that DAG."""
    dag, plan = lowered("a=1")
    with pytest.raises(ExecutionValidationError, match="supplied DAG"):
        plan.validate_against(analyze_source("a=2\n"))
    dag.source = "a=3\n"
    dag.final_bindings = {}
    plan.validate()
    assert plan.source == "a=1\n" and "a" in plan.final_bindings
    with pytest.raises(ExecutionValidationError, match="supplied DAG"):
        plan.validate_against(dag)


def test_mutable_constructor_collections_are_copied(lowered):
    """Changing a caller-owned list or final-binding dictionary cannot mutate the validated plan."""
    _, plan = lowered("a=1")
    tasks, bindings = list(plan.tasks), dict(plan.final_bindings)
    copied = replace(plan, tasks=tasks, final_bindings=bindings)
    tasks.clear()
    bindings.clear()
    assert len(copied.tasks) == 1 and "a" in copied.final_bindings
    copied.validate()


@pytest.mark.parametrize("field", ["tasks", "values", "edges", "definitions", "bindings"])
def test_plan_rejects_untyped_records(lowered, field):
    """Loose dictionaries cannot bypass typed contracts by standing in for graph records."""
    _, plan = lowered("a=1")
    with pytest.raises(ExecutionValidationError, match="record type"):
        replace(plan, **{field: ({"id": "made-up"},)})


def test_native_values_cannot_claim_immutable_storage(lowered):
    """A namespace view or token cannot be marked transferable by a conflicting storage field."""
    _, plan = lowered("unknown()\nx=1")
    for value in plan.values:
        if value.origin in {"namespace", "state", "external"}:
            with pytest.raises(ExecutionValidationError, match="non-native storage"):
                ValueRequirement(replace(value, storage="immutable_value"))


def test_object_requirement_is_immutable_and_requires_real_mutation_obligations():
    """A mutator needs a reference and a resulting state; constructor lists cannot change afterward."""
    references = ["V1"]
    req = ObjectRequirement("O1", references, [], ["S1"], ObjectAccess.MUTATE)
    references.clear()
    assert req.input_ids == ("V1",)
    for changes in ({"input_ids": ()}, {"state_outputs": ()}, {"access": "remote"}, {"object_id": ""}):
        with pytest.raises(ExecutionValidationError):
            replace(req, **changes)


def test_large_plan_keeps_linear_record_counts_and_exact_edges(lowered):
    """A 2001-node chain lowers one-for-one with 2000 edges, not a transitive closure or duplicated analyzer."""
    dag, plan = lowered("x=0\n" + "x=x+1\n" * 2000)
    assert len(plan.tasks) == len(plan.values) == 2001
    assert len(plan.edges) == sum(len(t.prerequisites) for t in plan.tasks) == 2000
    assert all(len(t.dependencies) == 1 for t in plan.tasks[1:])
    assert plan.edges == tuple(dag.edges.values())
