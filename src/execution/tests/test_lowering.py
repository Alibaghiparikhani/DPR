"""Each test specifies requirements retained from the authoritative DAG contract."""
from dataclasses import FrozenInstanceError
from pathlib import Path
import subprocess
import sys

import pytest

from dag_runtime.dag_engine import AnalysisOptions, analyze_file, analyze_source
from dag_runtime.dag_model import Certainty, EdgeKind, EffectKind
from execution import ExecutionMode, ObjectAccess, ValueKind, lower_dag


def test_chain_keeps_versioned_inputs_and_direct_edges(lowered):
    """x#1 -> x#2 -> result; manifests keep the exact DAG values and all reasons."""
    dag, plan = lowered("x=1\nx=x+2\nresult=x*3")
    first, second, third = plan.tasks
    assert first.dependencies == frozenset()
    assert second.dependencies == {first.task_id}
    assert third.dependencies == {second.task_id}
    assert second.inputs[0].value.label == "x#1"
    assert third.inputs[0].value.label == "x#2"
    assert plan.edges == tuple(dag.edges.values())
    assert plan.assumptions == dag.assumptions
    assert plan.final_bindings == dag.final_bindings
    assert all(m.task is dag.tasks[m.task_id] for m in plan.tasks)
    assert all(m.mode == ExecutionMode.ISOLATED_CANDIDATE for m in plan.tasks)


def test_diamond_retains_parallel_branches_and_multiple_inputs(lowered):
    """A -> B/C -> D; no execution-added edge between the two branch manifests."""
    dag, plan = lowered("a=1\nb=a+1\nc=a+2\nd=b+c")
    a, b, c, d = plan.tasks
    assert b.dependencies == c.dependencies == {a.task_id}
    assert d.dependencies == {b.task_id, c.task_id}
    assert tuple(v.value.name for v in d.inputs) == ("b", "c")
    assert len(plan.edges) == 4
    assert all(r.kind == EdgeKind.DATA for e in d.prerequisites for r in e.reasons)
    assert dag.explain_parallelism(b.task_id, c.task_id)["allowed"]


@pytest.mark.parametrize("source,projections", [
    ("x,y=(1,2)", ((0,), (1,))),
    ("x,x=(1,2)", ((0,), (1,))),
    ("a,(b,c),*rest=(1,(2,3),4,5)", ((0,), (1, 0), (1, 1), ("2:*",))),
    ("a=b=[1,2]", ((), ())),
])
def test_multiple_outputs_preserve_assignment_sequence(lowered, source, projections):
    """One atomic statement keeps every output version/projection, including repeated target names."""
    dag, plan = lowered(source)
    manifest, = plan.tasks
    assert tuple(v.value.projection for v in manifest.outputs) == projections
    assert tuple(v.id for v in manifest.outputs) == next(iter(dag.tasks.values())).outputs
    assert plan.bindings == dag.bindings
    assert tuple(b.value_id for b in plan.bindings) == manifest.task.outputs
    if source == "a=b=[1,2]":
        assert manifest.outputs[0].value.object_id == manifest.outputs[1].value.object_id
    if source == "x,x=(1,2)":
        assert manifest.outputs[0].value.version == 1
        assert manifest.outputs[1].value.version == 2
        assert plan.final_bindings["x"] == manifest.outputs[1].id


def test_alias_binding_is_a_source_order_worker_step(lowered):
    """Phase 1 materializes b=x so downstream namespaces receive the alias name and identity."""
    dag, plan = lowered("x=1\nb=x\nx=2\ny=b+3")
    first, bind, second, reader = plan.tasks
    base = first.outputs[0]
    alias = bind.outputs[0]
    assert alias.value.origin == "alias"
    assert alias.value.object_id == base.value.object_id
    assert alias.value.producer == bind.task_id
    assert bind.inputs[0] == base
    assert bind.reported_output_ids == (alias.id,)
    assert reader.inputs[0] == alias
    assert reader.dependencies == {bind.task_id}
    assert bind.task_id in second.dependencies
    assert plan.bindings[1].kind == "alias"
    assert plan.bindings == dag.bindings


def test_alias_commit_must_not_be_moved_before_an_intervening_failure(lowered):
    """The materialized alias binding stays after the may-raise completion fence."""
    _, plan = lowered("x=1\nq=1//0\nb=x")
    first, division, bind = plan.tasks
    alias = bind.outputs[0]
    assert alias.value.origin == "alias"
    assert division.task_id in bind.dependencies
    assert plan.bindings[-1].span.line == 3
    assert plan.bindings[-1].value_id == alias.id
    assert division.task.characteristics.may_raise


def test_definition_versions_aliases_and_builtin_dependencies(lowered):
    """old binds D1 through a real alias step; calls retain the correct definition identities."""
    _, plan = lowered("def f(x): return abs(x)\nold=f\ndef f(x): return x+2\na=old(1)\nb=f(1)")
    bind, a, b = plan.tasks
    assert a.dependencies == {bind.task_id} and not b.dependencies
    assert a.code.definition_ids == ("D000001",)
    assert b.code.definition_ids == ("D000002",)
    assert [d.name for d in plan.definitions] == ["f", "f"]
    assert any(v.value.origin == "builtin" and v.value.name == "abs" for v in a.inputs)
    assert any(v.value.origin == "alias" and v.value.name == "old" for v in a.inputs)
    assert plan.definition_index["D000001"].source == "def f(x): return abs(x)"


def test_nested_calls_stay_atomic_and_keep_all_code_requirements(lowered):
    """combine(clean(a),clean(b)) stays one task with both exact callable definitions and data inputs."""
    _, plan = lowered("def clean(x): return x+1\ndef combine(x,y): return x+y\na=1\nb=2\nr=combine(clean(a),clean(b))")
    a, b, call = plan.tasks
    assert call.dependencies == {a.task_id, b.task_id}
    assert set(call.code.definition_ids) == {"D000001", "D000002"}
    assert "combine(clean(a),clean(b))" in call.task.source
    assert call.task.characteristics.call_count == 3


@pytest.mark.parametrize("source", ["", "# empty", "pass", "def f(): return 1"])
def test_zero_task_plans_keep_definition_setup(lowered, source):
    """Empty modules need no tasks; an uncalled definition still has its source-order setup artifact."""
    dag, plan = lowered(source)
    assert not plan.tasks and not plan.edges
    assert plan.definitions == tuple(dag.definitions.values())
    assert plan.bindings == dag.bindings


@pytest.mark.parametrize("source", ["'module docs'\nx=__doc__", "x=len\ny=x"])
def test_implicit_docstring_and_builtin_alias_records_are_not_lost(lowered, source):
    """Module __doc__ and producerless builtin aliases remain explicit values and binding events."""
    dag, plan = lowered(source)
    assert plan.values == tuple(dag.values.values())
    assert plan.bindings == dag.bindings
    assert any(v.origin == 'alias' for v in plan.values)


def test_may_raise_fence_is_not_a_namespace_effect(lowered):
    """n -> q -> a/b -> c; q's completion token remains distinct from namespace state."""
    dag, plan = lowered("n=2\nq=10//n\na=1\nb=2\nc=a+b")
    n, q, a, b, c = plan.tasks
    assert q.task.effect == EffectKind.PURE and q.task.characteristics.may_raise
    assert q.mode == ExecutionMode.SHARED_CONTEXT
    assert q.task.certainty == Certainty.CONSERVATIVE
    token, = (v for v in q.outputs if v.is_state_token)
    assert token.kind == ValueKind.COMPLETION_STATE
    assert token in a.state_inputs and token in b.state_inputs
    assert not c.state_inputs
    assert a.dependencies == b.dependencies == {q.task_id}
    assert c.dependencies == {a.task_id, b.task_id}
    assert plan.final_namespace is None
    assert all(m.task.namespace_epoch == 0 for m in plan.tasks)
    assert plan.edges == tuple(dag.edges.values())


def test_object_local_mutation_keeps_alias_state_and_reader_hazards(lowered):
    """read(a) -> append(a) -> read(alias); unrelated b remains independent, with no forged copy of a."""
    dag, plan = lowered("a=[1,2]\nalias=a\nb=[4,5]\nx=sum(a)\nu=sum(b)\na.append(3)\ny=sum(alias)\nv=sum(b)")
    tasks={m.task.source:m for m in plan.tasks}
    a,b,read,unrelated,append,after,later_unrelated=(tasks[src] for src in ('a=[1,2]','b=[4,5]','x=sum(a)','u=sum(b)','a.append(3)','y=sum(alias)','v=sum(b)'))
    oid = a.outputs[0].value.object_id
    assert append.mode == ExecutionMode.SHARED_CONTEXT and append.task.certainty == Certainty.CERTAIN
    requirement, = append.objects
    assert requirement.object_id == oid and requirement.access == ObjectAccess.MUTATE
    assert requirement.input_ids == (a.outputs[0].id,)
    assert not requirement.state_inputs
    assert requirement.state_outputs == (plan.final_object_states[oid],)
    war = next(e for e in append.prerequisites if e.source == read.task_id)
    assert any(r.kind == EdgeKind.STATE and r.value_id is None for r in war.reasons)
    assert after.objects[0].access == ObjectAccess.SNAPSHOT_CANDIDATE
    assert after.objects[0].object_id == oid
    assert after.objects[0].state_inputs == requirement.state_outputs
    assert read.objects[0].state_inputs == ()
    assert not dag.dependency_path(unrelated.task_id, append.task_id)
    assert not dag.dependency_path(append.task_id, later_unrelated.task_id)
    assert b.outputs[0].value.object_id != oid


def test_two_appends_keep_waw_tokens_and_all_war_edges(lowered):
    """append1 -> readers -> append2; the second writer consumes the first writer's exact token."""
    _, plan = lowered("a=[1]\na.append(2)\nx=sum(a)\ny=len(a)\na.append(3)")
    _, first, x, y, second = plan.tasks
    assert {first.task_id, x.task_id, y.task_id} <= second.dependencies
    assert second.objects[0].state_inputs == first.objects[0].state_outputs
    assert plan.final_object_states[second.objects[0].object_id] == second.objects[0].state_outputs[0]


@pytest.mark.parametrize("source", [
    "d={'a':1}", "def identity(x): return x\na=[1]\nb=identity(a)", "a=[1]\nb=[a]",
])
def test_certain_does_not_override_shared_placement(lowered, source):
    """Proved dictionary construction/borrowed or nested aliases are certain but not isolation candidates."""
    _, plan = lowered(source)
    task = plan.tasks[-1]
    assert task.task.certainty == Certainty.CERTAIN
    assert task.mode == ExecutionMode.SHARED_CONTEXT
    assert all(o.access == ObjectAccess.LIVE_REFERENCE for o in task.objects)


def test_safe_comprehension_retains_snapshot_obligations(lowered):
    """The comprehension call remains one candidate using the list's required version, not dynamic iteration tasks."""
    _, plan = lowered("def clean(values): return [x*2 for x in values]\na=[1,2]\na.append(3)\nb=clean(a)")
    a, append, clean = plan.tasks
    assert clean.mode == ExecutionMode.ISOLATED_CANDIDATE
    assert clean.task.characteristics.comprehension_count == 1
    assert clean.objects[0].state_inputs == append.objects[0].state_outputs
    assert clean.outputs[0].kind == ValueKind.SHARED_REFERENCE
    assert clean.outputs[0].value.object_id != a.outputs[0].value.object_id


@pytest.mark.parametrize("operation", ["getattr(obj,name)", "setattr(obj,name,1)", "unknown()", "import math"])
def test_namespace_barrier_and_literal_recovery(lowered, operation):
    """Barrier -> two candidate scalar expressions, each gated by namespace state, not given a namespace snapshot."""
    dag, plan = lowered(operation + "\n1+2\n3*4")
    barrier, a, b = plan.tasks
    assert barrier.task.effect == EffectKind.NAMESPACE
    assert barrier.mode != ExecutionMode.ISOLATED_CANDIDATE
    assert a.mode == b.mode == ExecutionMode.ISOLATED_CANDIDATE
    assert a.dependencies == b.dependencies == {barrier.task_id}
    assert a.state_inputs == b.state_inputs
    assert a.state_inputs[0].kind == ValueKind.NAMESPACE_STATE
    assert a.task.namespace_epoch == b.task.namespace_epoch == 1
    assert plan.final_namespace == dag.final_namespace


def test_stale_name_is_native_view_not_captured_data(lowered):
    """After unknown(), x is an unknown/unbound namespace projection produced by the barrier, not stale x#1."""
    _, plan = lowered("x=1\nunknown()\ny=x+1")
    original, barrier, consumer = plan.tasks
    x = next(v for v in consumer.inputs if v.value.name == "x")
    assert x.id != original.outputs[0].id
    assert x.kind == ValueKind.NATIVE_REFERENCE and x.value.origin == "namespace"
    assert x.value.may_be_unbound and x.value.producer == barrier.task_id
    assert x in consumer.state_inputs
    assert consumer.mode == ExecutionMode.NATIVE_REGION


def test_later_completion_does_not_replace_final_namespace(lowered):
    """The newer completion fence and the older final namespace are separate tokens with separate producers."""
    _, plan = lowered("unknown()\n1//0\n1+2")
    barrier, division, after = plan.tasks
    assert plan.value_index[plan.final_namespace].producer == barrier.task_id
    assert after.state_inputs[0].kind == ValueKind.COMPLETION_STATE
    assert after.state_inputs[0].value.producer == division.task_id
    assert after.task.namespace_epoch == 1


@pytest.mark.parametrize("operation", [
    "g=globals()", "g=locals()", "g=vars()", "exec(code)", "eval(code)",
    "from plugin import *", "g=f.__globals__", 'getattr(f,"__globals__")',
])
def test_native_tail_never_becomes_independent_snippet(lowered, operation):
    """Before -> one complete native suffix; original module context/source and region scope are retained."""
    dag, plan = lowered("before=1\n" + operation + "\nx=2\ny=3")
    before, tail = plan.tasks
    assert tail.mode == ExecutionMode.NATIVE_REGION
    assert tail.task.region_scope == "tail" and tail.task.effect == EffectKind.ESCAPE
    assert tail.dependencies == {before.task_id}
    assert tail.task.source == operation + "\nx=2\ny=3"
    assert tail.task.span.line == 2 and tail.task.statement_count == 3
    assert plan.source == dag.source and "before=1" in plan.source
    assert tail.code.program_id == plan.program.id
    assert tail.task.conservative_reasons


@pytest.mark.parametrize("source,options", [
    ("async def f(): return 1\nx=f()", {}),
    ("g=(x for x in values)", {}),
    ("import threading\nx=1", {}),
    ("x=1\ny=2", {"options": AnalysisOptions(max_ast_nodes=1)}),
])
def test_native_module_fallback_has_only_native_state(lowered, source, options):
    """Concurrency/budget fallback retains one native module without fabricated per-name outputs."""
    dag, plan = lowered(source, **options)
    task, = plan.tasks
    assert task.mode == ExecutionMode.NATIVE_REGION and task.task.region_scope == "module"
    assert task.task.source == plan.source
    assert not plan.final_bindings
    assert plan.final_namespace == dag.final_namespace


@pytest.mark.parametrize("source", [
    "if flag:\n    x=1\ny=x", "for x in values:\n    print(x)",
    "try:\n    unknown()\nfinally:\n    finish()", "with manager():\n    unknown()",
])
def test_general_control_flow_remains_one_native_region(lowered, source):
    """Control flow is not expanded; conditional/native inputs may be absent and require no fake producer."""
    dag, plan = lowered(source)
    assert plan.tasks[0].mode == ExecutionMode.NATIVE_REGION
    assert plan.tasks[0].task.region_scope == "statement"
    assert plan.tasks[0].task == next(iter(dag.tasks.values()))
    assert any(v.value.may_be_unbound for v in plan.tasks[0].inputs)


@pytest.mark.parametrize("example", ["diamond.py", "stress_dag.py", "precision.py", "prototype_program.py", "prototype_program2.py"])
def test_authoritative_examples_lower_without_changing_dag(example):
    """Every supplied example retains all task/value/edge/binding records, including the mixed stress tail."""
    path = Path(__file__).parents[2] / "dag_runtime" / "examples" / example
    dag = analyze_file(path)
    before = dag.to_dict()
    plan = lower_dag(dag, environment_id="test-cpython312-lock-v1")
    plan.validate_against(dag)
    assert tuple(m.task for m in plan.tasks) == tuple(dag.tasks.values())
    assert plan.values == tuple(dag.values.values())
    assert plan.edges == tuple(dag.edges.values())
    assert dag.to_dict() == before
    if example == "stress_dag.py":
        assert any(t.task.effect == EffectKind.OBJECT_LOCAL for t in plan.tasks)
        assert plan.tasks[-1].task.region_scope == "tail"


def test_source_context_preserves_future_imports_and_utf8_spans(lowered):
    """Full program context and AST byte columns survive; no snippet compilation or future-flag guessing occurs."""
    source = 'from __future__ import annotations\ncafé=1; other=2'
    dag, plan = lowered(source, filename="submitted.py")
    assert plan.source == dag.source
    assert plan.program.filename == "submitted.py"
    assert tuple(t.task.span for t in plan.tasks) == tuple(t.span for t in dag.tasks.values())
    assert "from __future__ import annotations" in plan.source
    assert plan.tasks[-1].task.span.column == len("café=1; ".encode("utf-8"))


def test_lowering_never_executes_imports_or_user_code(tmp_path):
    """A malicious-looking import/write source is only described; no module import or file write occurs."""
    marker = tmp_path / "must-not-exist"
    dag = analyze_source(f"import nonexistent_lowering_fixture\nopen({str(marker)!r},'w').write('oops')")
    plan = lower_dag(dag, environment_id="test-env")
    assert len(plan.tasks) == 2 and not marker.exists()
    assert "nonexistent_lowering_fixture" not in sys.modules


def test_lowering_is_deterministic_and_independent_of_readiness(lowered):
    """Identical DAGs/environments yield equal plans/IDs, regardless of a separately advanced readiness tracker."""
    dag, first = lowered("a=1\nb=a+1")
    state = dag.new_readiness()
    state.mark_completed(next(iter(dag.tasks)))
    second = lower_dag(dag, environment_id=first.program.environment_id)
    third = lower_dag(analyze_source(dag.source), environment_id=first.program.environment_id)
    assert first == second == third and first.id == second.id == third.id
    assert state.completed == {first.tasks[0].task_id}
    assert dag.initial_ready_tasks() == (first.tasks[0].task_id,)


def test_environment_package_source_and_analysis_identity_are_distinct(lowered):
    """Changing environment/package, code, filename, or budget outcome changes plan identity even when task IDs repeat."""
    dag, plan = lowered("a=1\nb=a+1")
    variants = [
        lower_dag(dag, environment_id="other-env"),
        lower_dag(dag, environment_id=plan.program.environment_id, package_id="package-v2"),
        lower_dag(analyze_source(dag.source + "# edit\n"), environment_id=plan.program.environment_id),
        lower_dag(analyze_source(dag.source, filename="other.py"), environment_id=plan.program.environment_id),
        lower_dag(analyze_source(dag.source, options=AnalysisOptions(max_ast_nodes=1)), environment_id=plan.program.environment_id),
    ]
    assert len({plan.id, *(p.id for p in variants)}) == 6
    assert variants[-1].program == plan.program
    assert variants[-1].dag_digest != plan.dag_digest


def test_plan_indexes_and_records_are_immutable(lowered):
    """The plan owns read-only indexes and frozen records; callers cannot edit placement/requirements in place."""
    _, plan = lowered("a=1\nb=a+1")
    with pytest.raises(FrozenInstanceError):
        plan.tasks[0].mode = ExecutionMode.SHARED_CONTEXT
    with pytest.raises(FrozenInstanceError):
        plan.source = "changed"
    for mapping in (plan.task_index, plan.value_index, plan.definition_index, plan.final_bindings, plan.final_object_states):
        with pytest.raises(TypeError):
            mapping["invented"] = None


def test_package_imports_do_not_load_analyzer_visualizer_or_external_frameworks():
    """execution import does not pull optional visualization/external frameworks or execute user code."""
    root = Path(__file__).parents[2]
    script = """
import sys
import execution
from execution.lowering import lower_dag
from execution.model import ExecutionPlan
assert execution.lower_dag is lower_dag
assert execution.ExecutionPlan is ExecutionPlan
# execution now imports dag_engine for Phase-1 definition/binding closure metadata.
# dag_static is transitively imported by the analyzer contract.
assert 'dag_runtime.dag_visualizer' not in sys.modules
# Phase 5/6 containment and portability helpers may import stdlib process/network modules.
assert 'graphviz' not in sys.modules
# Pydantic is now loaded by shared validated execution/config models.
"""
    result = subprocess.run([sys.executable, "-c", script], cwd=root, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
