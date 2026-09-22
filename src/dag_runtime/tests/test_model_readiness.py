"""Graph validation rejects corrupt records; readiness counts dependency pairs once."""
from dataclasses import replace
import json

import pytest

from dag_runtime.dag_engine import analyze_source
from dag_runtime.dag_model import DAG, DependencyEdge, EdgeReason, EdgeKind, Certainty, GraphValidationError, ReadinessError


def rebuild(dag, *, tasks=None, values=None, edges=None, **kwargs):
    return DAG(dag.tasks.values() if tasks is None else tasks,
               dag.values.values() if values is None else values,
               dag.edges.values() if edges is None else edges, **kwargs)


def test_readiness_diamond_transitions(analyze,nodes):
    """Complete A→{B,C}; completing only B does not unlock D; C then unlocks D once."""
    dag=analyze('a=1\nb=a+1\nc=a+2\nd=b+c')
    a,b,c,d=nodes(dag)
    state=dag.new_readiness()
    assert state.ready==(a.id,)
    assert set(state.mark_completed(a.id))=={b.id,c.id}
    assert state.mark_completed(b.id)==()
    assert state.mark_completed(c.id)==(d.id,)
    assert state.mark_completed(d.id)==()
    assert not state.ready and len(state.completed)==4


def test_independent_run_states(analyze,nodes):
    """Completing a root in one tracker does not advance another tracker or the graph's helper."""
    dag=analyze('a=1\nb=a+1')
    a,b=nodes(dag)
    first,second=dag.new_readiness(),dag.new_readiness()
    assert first.mark_completed(a.id)==(b.id,)
    assert second.ready==(a.id,)
    assert dag.mark_completed(a.id)==(b.id,)


def test_bad_completion_rejected(analyze,nodes):
    """Unknown, premature, and duplicate completions never decrement counters."""
    dag=analyze('a=1\nb=a+1')
    a,b=nodes(dag)
    state=dag.new_readiness()
    with pytest.raises(KeyError): state.mark_completed('typo')
    with pytest.raises(ReadinessError,match='incomplete'): state.mark_completed(b.id)
    assert state.mark_completed(a.id)==(b.id,)
    with pytest.raises(ReadinessError,match='already'): state.mark_completed(a.id)
    assert state.ready==(b.id,)


def test_multiple_edge_reasons_count_one_predecessor(analyze,nodes):
    """A call reading two A outputs plus a barrier reason still needs exactly one A completion."""
    dag=analyze('a,b=(1,2)\nunknown(a,b)')
    a,call=nodes(dag)
    assert len(dag.edges[(a.id,call.id)].reasons)>=3
    assert dag.new_readiness().mark_completed(a.id)==(call.id,)


def test_invalid_source_cannot_be_completed():
    """A structurally valid diagnostic graph has no runnable tasks or successful transitions."""
    dag=analyze_source('return 1')
    with pytest.raises(ReadinessError,match='invalid'):
        dag.mark_completed(next(iter(dag.tasks)))


def test_empty_graph(analyze):
    """An empty/comment-only module has empty topology, values, ready tasks, and sinks."""
    dag=analyze('# no computation')
    assert not dag.tasks and not dag.values and not dag.edges
    assert dag.topological_order()==dag.initial_ready_tasks()==dag.sinks()==()


def test_deterministic_ids_and_json(analyze):
    """The same source/options produce byte-identical JSON and stable invocation IDs."""
    source='def f(x): return x+1\na=1\nb=f(a)\nc=f(a)'
    a,b=analyze(source),analyze(source)
    assert json.dumps(a.to_dict(),sort_keys=True)==json.dumps(b.to_dict(),sort_keys=True)


def test_mapping_indexes_are_read_only(analyze):
    """Public task/value/edge indexes reject mutation that would bypass validation."""
    dag=analyze('a=1')
    for mapping in (dag.tasks,dag.values,dag.edges,dag.final_bindings):
        with pytest.raises(TypeError): mapping['invented']=None


def test_duplicate_task_value_edge_rejected(analyze,nodes):
    """Constructing a graph with duplicate IDs or duplicate dependency pairs fails explicitly."""
    dag=analyze('a=1\nb=a+1')
    with pytest.raises(GraphValidationError,match='Duplicate task'):
        rebuild(dag,tasks=[*nodes(dag),nodes(dag)[0]])
    with pytest.raises(GraphValidationError,match='Duplicate value'):
        rebuild(dag,values=[*dag.values.values(),next(iter(dag.values.values()))])
    with pytest.raises(GraphValidationError,match='Duplicate edge'):
        rebuild(dag,edges=[*dag.edges.values(),next(iter(dag.edges.values()))])


def test_missing_task_reference_rejected(analyze):
    """An edge to a task absent from the graph is rejected."""
    dag=analyze('a=1\nb=a+1')
    edge=next(iter(dag.edges.values()))
    with pytest.raises(GraphValidationError,match='Missing task'):
        rebuild(dag,edges=[replace(edge,target='missing')])


def test_missing_value_rejected(analyze):
    """A missing consumed/produced value invalidates the graph."""
    dag=analyze('a=1\nb=a+1')
    with pytest.raises(GraphValidationError):
        rebuild(dag,values=[])


def test_reverse_index_mismatch_rejected(analyze,nodes):
    """A parent missing its reverse dependent index is rejected."""
    dag=analyze('a=1\nb=a+1')
    a,b=nodes(dag)
    with pytest.raises(GraphValidationError,match='adjacency'):
        rebuild(dag,tasks=[replace(a,dependents=frozenset()),b])


def test_missing_data_edge_rejected(analyze,nodes):
    """Matching empty adjacency is insufficient if a produced input lost its RAW edge."""
    dag=analyze('a=1\nb=a+1')
    a,b=nodes(dag)
    with pytest.raises(GraphValidationError,match='Missing data/state'):
        rebuild(dag,tasks=[replace(a,dependents=frozenset()),replace(b,dependencies=frozenset())],edges=[])


def test_cycle_rejected(analyze,nodes):
    """Even internally consistent forward/reverse references cannot form A↔B."""
    dag=analyze('a=1\nb=2')
    a,b=nodes(dag)
    a=replace(a,dependencies=frozenset({b.id}),dependents=frozenset({b.id}))
    b=replace(b,dependencies=frozenset({a.id}),dependents=frozenset({a.id}))
    reason=EdgeReason(EdgeKind.ORDER,Certainty.CONSERVATIVE,'test cycle')
    with pytest.raises(GraphValidationError,match='Cycle'):
        rebuild(dag,tasks=[a,b],edges=[DependencyEdge(a.id,b.id,(reason,)),DependencyEdge(b.id,a.id,(reason,))])


def test_conservative_reason_required(analyze,nodes):
    """Conservative task records without an explanation fail validation."""
    dag=analyze('unknown()')
    [task]=nodes(dag)
    with pytest.raises(GraphValidationError,match='Missing conservative reason'):
        rebuild(dag,tasks=[replace(task,conservative_reasons=())])


def test_alias_identity_mismatch_rejected(analyze):
    """An alias must agree with its source producer and reference identity."""
    dag=analyze('a=1\nb=a')
    values=[replace(v,object_id='wrong') if v.origin=='alias' else v for v in dag.values.values()]
    with pytest.raises(GraphValidationError,match='alias identity'):
        rebuild(dag,values=values)


def test_explanations_for_transitive_order(analyze,nodes):
    """A→B→C is reported as an ordering path even without a direct A→C edge."""
    dag=analyze('a=1\nb=a+1\nc=b+1')
    a,b,c=nodes(dag)
    assert dag.explain_dependency(a.id,c.id) is None
    assert dag.dependency_path(a.id,c.id)==(a.id,b.id,c.id)
    assert not dag.explain_parallelism(a.id,c.id)['allowed']
    with pytest.raises(KeyError): dag.explain_dependency(a.id,'typo')


def test_required_values_include_namespace_tokens(analyze,nodes):
    """A task following an effect barrier requires the live namespace token as well as data."""
    dag=analyze('unknown()\nx=1')
    barrier,x=nodes(dag)
    requirements=dag.required_values(x.id)
    assert any(v.origin=='state' and v.producer==barrier.id for v in requirements)
