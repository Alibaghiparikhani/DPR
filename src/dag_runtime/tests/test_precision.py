"""Precision is tested together with the unsafe neighbor for every new rule.

Every docstring states the expected partial order or the reason order must stay.
No test infers independence merely from the absence of a direct edge.
"""
from dataclasses import replace

import pytest

from dag_runtime.dag_engine import AnalysisOptions
from dag_runtime.dag_model import Certainty, EffectKind, EdgeKind, GraphValidationError


def by_source(dag, source):
    return next(t for t in dag.tasks.values() if t.source == source)


def independent(dag, a, b):
    assert not dag.dependency_path(a.id, b.id)
    assert not dag.dependency_path(b.id, a.id)
    assert dag.explain_parallelism(a.id, b.id)['allowed']


@pytest.mark.parametrize('operation', [
    'unknown()', 'getattr(obj,name)', 'setattr(obj,name,1)',
    'delattr(obj,name)', '__import__("external_module")', 'vars(obj)',
    'import math', 'import numpy as np', 'from package import item',
])
def test_recover_structure_and_literal_work(analyze, nodes, operation):
    """A/B→namespace barrier→C/D: fresh literal computations can overlap after any recoverable barrier."""
    dag = analyze('a=1\nb=2\n'+operation+'\n1+2\n3*4')
    a,b,barrier,c,d = nodes(dag)
    assert barrier.effect == EffectKind.NAMESPACE and barrier.certainty == Certainty.CONSERVATIVE
    assert barrier.dependencies == {a.id,b.id}
    assert c.dependencies == d.dependencies == {barrier.id}
    independent(dag,c,d)
    assert c.namespace_epoch == d.namespace_epoch == 1
    assert dag.metrics()['whole_tail_collapses'] == 0


@pytest.mark.parametrize('operation', ['unknown()', 'getattr(obj,name)', 'import math', 'vars(obj)'])
def test_recoverable_barrier_does_not_restore_old_callables_or_targets(analyze,nodes,operation):
    """Old f/a become unknown, and new x/y targets remain guarded against injected finalizers."""
    dag=analyze('def f(x): return x+1\na=[1]\n'+operation+'\nx=f(a[0])\ny=2')
    create,barrier,x,y=nodes(dag)
    assert x.certainty == y.certainty == Certainty.CONSERVATIVE
    assert barrier.id in x.dependencies and x.id in y.dependencies
    old_views=[dag.values[v] for v in x.inputs if dag.values[v].name in {'f','a'}]
    assert all(v.origin=='namespace' and v.producer==barrier.id and v.may_be_unbound for v in old_views)
    assert dag.metrics()['whole_tail_collapses']==0


@pytest.mark.parametrize('operation', [
    'g=globals()', 'g=locals()', 'g=vars()', 'exec(code)', 'eval(code)',
    'exec(code,{})', 'eval(code,{"__builtins__":{}})', 'from package import *',
    'g=f.__globals__', 'g=m.__dict__', 'g=frame.f_locals',
    'g=getattr(f,"__globals__")', 'g=getattr(frame,"f_globals")',
])
def test_true_capability_and_dynamic_code_keep_native_tail(analyze,nodes,operation):
    """Prior work→one tail including escaped capability and later work; no remote tail candidate."""
    dag=analyze('a=1\n'+operation+'\nx=2\ny=3')
    before,tail=nodes(dag)
    assert tail.dependencies == {before.id}
    assert tail.effect == EffectKind.ESCAPE and tail.region_scope=='tail'
    assert tail.placement=='shared_namespace' and operation in tail.source and 'y=3' in tail.source
    assert tail.source==operation+'\nx=2\ny=3' and tail.span.line==2
    assert tail.statement_count==3 and dag.metrics()['hidden_statements']==2
    assert dag.metrics()['whole_tail_collapses']==1 and tail.conservative_reasons


def test_namespace_defaults_execute_now_but_function_bodies_do_not(analyze,nodes):
    """globals() in a default starts a tail; an uncalled function's body does not execute."""
    dag=analyze('def f(): return globals()\na=1\nb=2')
    assert len(nodes(dag))==2 and dag.metrics()['whole_tail_collapses']==0
    dag=analyze('a=1\ndef f(g=globals()): return g\nb=2')
    assert nodes(dag)[-1].region_scope=='tail'


def test_tail_never_repeats_already_analyzed_source(analyze,nodes):
    """The native suffix starts at the escape, never at preceding side effects or definitions."""
    dag=analyze('before=side_effect()\ng=globals()\nx=2')
    before,tail=nodes(dag)
    assert tail.span.line==2 and tail.source=='g=globals()\nx=2'
    assert 'side_effect' not in tail.source and tail.dependencies=={before.id}


def test_tail_preserves_escape_inside_a_decorator(analyze,nodes):
    """An escape in a decorator retains that decorator line as the start of the native suffix."""
    dag=analyze('a=1\n@decorate(globals())\ndef f(): return 1\nx=2')
    before,tail=nodes(dag)
    assert tail.source=='@decorate(globals())\ndef f(): return 1\nx=2'
    assert tail.span.line==2 and before.id in tail.dependencies


def test_exception_only_barrier_recovers_definitions_and_fresh_bindings(analyze,nodes):
    """n→division→{f(2),f(3)}→join; division may raise but cannot rebind f or inject finalizers."""
    dag=analyze('def f(x): return x+1\nn=2\nq=10//n\na=f(2)\nb=f(3)\nc=a+b')
    n,q,a,b,c=nodes(dag)
    assert q.effect==EffectKind.PURE and q.characteristics.may_raise
    assert not q.characteristics.possible_side_effects and q.certainty==Certainty.CONSERVATIVE
    assert a.dependencies==b.dependencies=={q.id}
    independent(dag,a,b)
    assert c.dependencies=={a.id,b.id} and (q.id,c.id) not in dag.edges
    assert dag.final_namespace is None and all(t.namespace_epoch==0 for t in nodes(dag))
    assert dag.values[dag.final_bindings['n']].producer==n.id


def test_completion_fence_is_not_repeated_down_a_chain(analyze,nodes):
    """q→a→b→c carries one completion fence; no redundant q→b/c state inputs."""
    dag=analyze('n=2\nq=10//n\na=1\nb=a+1\nc=b+1')
    n,q,a,b,c=nodes(dag)
    assert a.dependencies=={q.id} and b.dependencies=={a.id} and c.dependencies=={b.id}
    assert any(dag.values[v].name=='@completion' for v in a.inputs)
    assert not any(dag.values[v].origin=='state' for v in b.inputs+c.inputs)


def test_exception_after_namespace_barrier_keeps_the_correct_two_states(analyze,nodes):
    """Unknown namespace state remains final; later computation waits for the newer completion fence."""
    dag=analyze('unknown()\n1//0\n1+2\n3+4')
    unknown,division,a,b=nodes(dag)
    assert a.dependencies==b.dependencies=={division.id}
    assert division.dependencies=={unknown.id}
    assert dag.values[dag.final_namespace].producer==unknown.id
    assert division.namespace_epoch==a.namespace_epoch==b.namespace_epoch==1


@pytest.mark.parametrize('expression', ['[]', '(1,2)', '{}', '[x*2 for x in (1,2)]', '([1,2])[0]'])
def test_post_namespace_allocation_may_run_pending_finalizers(analyze,nodes,expression):
    """Unknown code can leave GC callbacks/cycles; container allocation stays native and ordered before later work."""
    dag=analyze('unknown()\n'+expression+'\n1+2')
    before,allocation,after=nodes(dag)
    assert allocation.effect==EffectKind.NAMESPACE and allocation.certainty==Certainty.CONSERVATIVE
    assert before.id in allocation.dependencies and allocation.id in after.dependencies
    assert any('GC/finalization' in reason for reason in allocation.conservative_reasons)


def test_pure_may_raise_does_not_unlock_work_on_failure(analyze,nodes):
    """Only successfully completing division unlocks its branches; merely completing n leaves them blocked."""
    dag=analyze('n=0\nq=1//n\na=1\nb=2')
    n,q,a,b=nodes(dag)
    state=dag.new_readiness()
    assert state.mark_completed(n.id)==(q.id,)
    assert state.ready==(q.id,)
    assert a.id not in state.ready and b.id not in state.ready


@pytest.mark.parametrize('op', ['/', '//', '%'])
def test_integer_exception_classification_is_exact_type_only(analyze,nodes,op):
    """Exact integer arithmetic is exception-only; unknown overloaded operands remain namespace effects."""
    exact=analyze('a=1\nx=a'+op+'0\ny=2')
    assert nodes(exact)[1].effect==EffectKind.PURE
    unknown=analyze('x=obj'+op+'other\ny=2')
    assert nodes(unknown)[0].effect==EffectKind.NAMESPACE
    assert nodes(unknown)[0].id in nodes(unknown)[1].dependencies


def test_intermediate_exception_proof_cannot_stand_in_for_function_return(analyze,nodes):
    """Unproved intermediate division must not report its integer type as the function's string return."""
    dag=analyze('def f(x):\n    y=1//x\n    return "text"\na=f(1)\nb=a+1')
    a,b=nodes(dag)
    assert a.effect==EffectKind.NAMESPACE and b.certainty==Certainty.CONSERVATIVE
    assert dag.values[dag.final_bindings['a']].type_hint=='unknown'
    assert a.id in b.dependencies


@pytest.mark.parametrize('container,index', [
    ('(10,20,30)','1'), ('[10,20,30]','-1'), ('(10,20)','True'),
    ('[10,20]','+0'), ('(10,20)','-2'), ('((1,2),(3,4))','0'),
])
def test_constant_in_bounds_indexing_is_proved(analyze,nodes,container,index):
    """Container→read, with unrelated scalar branch independent; exact immutable result needs no barrier."""
    dag=analyze('a='+container+'\nx=a['+index+']\ny=2+3')
    a,x,y=nodes(dag)
    assert x.dependencies=={a.id} and x.certainty==Certainty.CERTAIN
    independent(dag,x,y)


@pytest.mark.parametrize('expression', ['a[3]','a[-4]','a[index]','a[1:2]','obj[0]','a.__getitem__(0)'])
def test_indexing_unsafe_neighbors_keep_barriers(analyze,nodes,expression):
    """Unknown indices/protocols and invalid bounds preserve order before the following statement."""
    dag=analyze('a=[1,2,3]\nx='+expression+'\ny=4')
    a,x,y=nodes(dag)
    assert x.certainty==Certainty.CONSERVATIVE and x.id in y.dependencies
    assert x.effect== (EffectKind.PURE if expression in {'a[3]','a[-4]'} else EffectKind.NAMESPACE)


def test_mutable_subscript_result_is_not_assigned_fake_allocation_identity(analyze,nodes):
    """Index returning a mutable child stays conservative; no disjointness is inferred from a new value ID."""
    dag=analyze('a=[[1]]\nx=a[0]\nx.append(2)')
    assert all(t.certainty==Certainty.CONSERVATIVE for t in nodes(dag)[1:])


def test_exact_append_orders_alias_state_and_leaves_other_object_independent(analyze,nodes):
    """read(a)→append(a)→read(alias); readers of fresh b have no path to the mutation."""
    dag=analyze('a=[1,2]\nalias=a\nb=[4,5]\nx=sum(a)\nu=sum(b)\na.append(3)\ny=sum(alias)\nv=sum(b)')
    tasks={t.source:t for t in nodes(dag)}
    a,b,x,u,write,y,v=(tasks[src] for src in ('a=[1,2]','b=[4,5]','x=sum(a)','u=sum(b)','a.append(3)','y=sum(alias)','v=sum(b)'))
    assert x.id in write.dependencies and write.id in y.dependencies
    independent(dag,write,u)
    independent(dag,write,v)
    assert write.effect==EffectKind.OBJECT_LOCAL and write.placement=='shared_namespace'
    assert write.characteristics.possible_side_effects and not write.characteristics.may_raise
    oid=dag.values[dag.final_bindings['alias']].object_id
    assert write.mutated_objects==(oid,)
    state=dag.values[dag.final_object_states[oid]]
    assert state.id in y.inputs and state.producer==write.id
    assert all(r.kind==EdgeKind.STATE and r.certainty==Certainty.CERTAIN for r in dag.edges[(x.id,write.id)].reasons)
    assert dag.final_namespace is None


def test_two_mutations_have_waw_and_all_intervening_readers_have_war(analyze,nodes):
    """append1→both readers→append2; both reads of the mutated list precede its next append."""
    dag=analyze('a=[1]\na.append(2)\nx=len(a)\ny=sum(a)\na.append(3)\nz=a[-1]')
    a,w1,x,y,w2,z=nodes(dag)
    assert w1.id in x.dependencies and w1.id in y.dependencies
    assert {x.id,y.id,w1.id} <= w2.dependencies and w2.id in z.dependencies
    assert z.certainty==Certainty.CERTAIN
    independent(dag,x,y)


def test_append_argument_reads_another_object(analyze,nodes):
    """append(a,b[0]) also reads b; a later append(b) must wait for that cross-object read."""
    dag=analyze('a=[1]\nb=[2]\na.append(b[0])\nb.append(3)')
    a,b,wa,wb=nodes(dag)
    assert wa.id in wb.dependencies
    assert all(t.effect==EffectKind.OBJECT_LOCAL for t in (wa,wb))


@pytest.mark.parametrize('operation', [
    'a.append(unknown())','a.append([])','a.append(a)','a.append(value=2)',
    'a.append(2,3)','a.extend([2])','a.clear()','a.pop()', 'a[0]=2',
])
def test_append_rule_unsafe_neighbors_remain_broad(analyze,nodes,operation):
    """Unproved signature, mutable argument, removal, or another mutator keeps the frontier barrier."""
    dag=analyze('a=[1]\nx=1\ny=2\n'+operation+'\nz=3')
    a,x,y,write,z=nodes(dag)
    assert write.effect==EffectKind.NAMESPACE and write.certainty==Certainty.CONSERVATIVE
    assert {a.id,x.id,y.id} <= write.dependencies and write.id in z.dependencies


@pytest.mark.parametrize('escape', ['b=identity(a)', 'b=[a]', 'b=(a,)', 'b={"a":a}'])
def test_unindexed_alias_disables_local_mutation(analyze,nodes,escape):
    """Borrowed return/container escape retires exact-list mutation eligibility; later append is broad."""
    dag=analyze('def identity(x): return x\na=[1]\n'+escape+'\nx=2\na.append(3)')
    write=nodes(dag)[-1]
    assert write.effect==EffectKind.NAMESPACE and write.certainty==Certainty.CONSERVATIVE
    assert by_source(dag,'x=2').id in write.dependencies


def test_function_cache_does_not_turn_borrowed_results_into_fresh_allocations(analyze,nodes):
    """Same-type identity specializations must not transfer caller-literal freshness to an aliased return."""
    dag=analyze('def identity(x): return x\nw=identity([1])\na=[1]\nb=identity(a)\nb.append(2)\nx=a[-1]')
    write=by_source(dag,'b.append(2)')
    assert write.effect==EffectKind.NAMESPACE
    assert by_source(dag,'b=identity(a)').placement=='shared_namespace'
    assert nodes(dag)[-1].certainty==Certainty.CONSERVATIVE


def test_mutation_updates_alias_type_facts_before_negative_indexing(analyze,nodes):
    """Appending str changes alias[-1] to str; later +1 must not retain a stale integer proof."""
    dag=analyze('a=[1]\nb=a\na.append("s")\nx=b[-1]\ny=x+1')
    tasks={t.source:t for t in nodes(dag)}
    a,write,x,y=(tasks[src] for src in ('a=[1]','a.append("s")','x=b[-1]','y=x+1'))
    assert write.effect==EffectKind.OBJECT_LOCAL and x.certainty==Certainty.CERTAIN
    assert dag.values[x.outputs[0]].type_hint=='str'
    assert y.certainty==Certainty.CONSERVATIVE and x.id in y.dependencies


def test_namespace_barrier_retires_all_object_facts(analyze,nodes):
    """External code may replace a's type/aliases; append after it is never object-local."""
    dag=analyze('a=[1]\na.append(2)\nexternal(a)\na.append(3)')
    assert nodes(dag)[1].effect==EffectKind.OBJECT_LOCAL
    assert nodes(dag)[-1].effect==EffectKind.NAMESPACE
    assert dag.final_object_states=={}


@pytest.mark.parametrize('body', [
    '[x*2 for x in values]', '[x+1 for x in values]',
    '[x*2 for x in values if x>0]', '[abs(x) for x in values]',
    '[~x for x in values]',
])
def test_safe_comprehension_functions_are_isolated_branches(analyze,nodes,body):
    """Two exact-data loaders→two independent fresh-result comprehension invocations; no iteration tasks."""
    dag=analyze('def clean(values): return '+body+'\na=[1,2]\nb=[3,4]\nx=clean(a)\ny=clean(b)')
    a,b,x,y=nodes(dag)
    independent(dag,x,y)
    assert x.dependencies=={a.id} and y.dependencies=={b.id}
    assert x.placement==y.placement=='isolated_candidate'
    assert x.characteristics.comprehension_count==y.characteristics.comprehension_count==1


@pytest.mark.parametrize('body', [
    '[unknown(x) for x in values]', '[x.attr for x in values]',
    '[x/0 for x in values]', '[x for x in values if unknown(x)]',
    '[x for x in values if x.__bool__()]', '[x for x,y in values]',
    '[x+y for x in values for y in values]', '[x for x in arbitrary]',
    '[[] for x in values]', '{x:x*2 for x in values}',
    '[sum([1]) for sum in values]', '[x for x in values if (y:=x)]',
])
def test_comprehension_unsafe_neighbors_remain_atomic_barriers(analyze,nodes,body):
    """Unproved body/filter/iteration/scope or unsupported output retains one conservative call before z."""
    dag=analyze('def clean(values): return '+body+'\na=[1,2]\nx=clean(a)\nz=3')
    a,x,z=nodes(dag)
    assert x.certainty==Certainty.CONSERVATIVE and x.placement=='shared_namespace'
    assert x.id in z.dependencies and len(nodes(dag))==3


def test_comprehension_shadowing_does_not_hide_external_iterable(analyze,nodes):
    """The iterable is read before its identically named target becomes a comprehension-local name."""
    dag=analyze('x=[1,2]\na=[x*2 for x in x]\nb=x[0]')
    x,a,b=nodes(dag)
    assert a.dependencies==b.dependencies=={x.id}
    independent(dag,a,b)


def test_boolean_inversion_cannot_hide_a_warning_inside_a_comprehension(analyze,nodes):
    """~bool can warn/raise in the supported Python version; that comprehension remains a barrier."""
    dag=analyze('values=[True,False]\nx=[~i for i in values]\ny=1')
    values,x,y=nodes(dag)
    assert x.certainty==Certainty.CONSERVATIVE and x.id in y.dependencies
    assert any('warning' in reason for reason in x.conservative_reasons)


def test_comprehension_budget_fails_closed(analyze,nodes):
    """Exhausting the common proof budget never turns a partially inspected comprehension into an isolated task."""
    dag=analyze('a=[1,2]\nx=[i*2 for i in a]\ny=3',options=AnalysisOptions(max_proof_steps=5))
    assert nodes(dag)[1].certainty==Certainty.CONSERVATIVE
    assert nodes(dag)[1].id in nodes(dag)[2].dependencies


@pytest.mark.parametrize('method,operation', [
    ('__getattribute__','obj.value'), ('__getattr__','getattr(obj,"value")'),
    ('__setattr__','setattr(obj,"value",1)'), ('__delattr__','delattr(obj,"value")'),
    ('__getitem__','obj[0]'), ('__setitem__','obj[0]=1'),
    ('__iter__','[x*2 for x in obj]'), ('__next__','next(obj)'),
    ('__len__','len(obj)'), ('__bool__','not obj'), ('__add__','obj+1'),
    ('__del__','obj=1'),
])
def test_custom_protocols_are_never_mistaken_for_exact_builtins(analyze,nodes,method,operation):
    """User protocol bodies may rebind global state; each operation remains a barrier before later work."""
    source='class C:\n    def '+method+'(self,*args):\n        global changed\n        changed=1\n        return 1\nobj=C()\n'
    statement=operation if '=' in operation and operation not in {'obj==1'} else 'result='+operation
    dag=analyze(source+statement+'\nafter=2')
    operation_task,after=nodes(dag)[-2:]
    assert operation_task.effect==EffectKind.NAMESPACE
    assert operation_task.certainty==Certainty.CONSERVATIVE and operation_task.id in after.dependencies


def test_descriptor_and_builtin_rebinding_keep_barriers(analyze,nodes):
    """A descriptor's globals write and a rebound builtin prevent using exact-looking syntax as a proof."""
    dag=analyze('class C:\n    @property\n    def value(self):\n        global len\n        len=3\n        return 1\nobj=C()\nx=obj.value\ny=len([1])')
    assert all(t.certainty==Certainty.CONSERVATIVE for t in nodes(dag))
    dag=analyze('len=3\na=[1,2]\nx=[len(x) for x in a]\ny=4')
    assert nodes(dag)[-2].certainty==Certainty.CONSERVATIVE


def test_metrics_and_object_state_validation(analyze,nodes):
    """Metrics count proved object state and pair kinds; malformed object-local metadata is rejected."""
    from dag_runtime.dag_model import DAG
    dag=analyze('a=[1]\nx=sum(a)\na.append(2)\ny=sum(a)')
    metrics=dag.metrics()
    assert metrics['certain_tasks']==4 and metrics['effects']['object_local']==1
    assert metrics['edges_by_kind']['state']==2
    assert dag.to_dict()['final_object_states']==dict(dag.final_object_states)
    tasks=[replace(t,placement='isolated_candidate') if t.effect==EffectKind.OBJECT_LOCAL else t for t in nodes(dag)]
    with pytest.raises(GraphValidationError,match='object effect'):
        DAG(tasks,dag.values.values(),dag.edges.values())
