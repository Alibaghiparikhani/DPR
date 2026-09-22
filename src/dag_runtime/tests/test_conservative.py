"""Safety regressions: uncertainty must create order, never fabricated independence."""
import pytest

from dag_runtime.dag_engine import AnalysisOptions, analyze_source
from dag_runtime.dag_model import Certainty, EdgeKind


@pytest.mark.parametrize('operation',[
    'values.append(x)', 'values[0]=x', 'obj.field=x', 'dictionary["x"]=x',
],ids=['method-mutation','subscript-mutation','attribute-mutation','dictionary-mutation'])
def test_mutation_joins_prior_readers_and_blocks_later(analyze,nodes,operation):
    """Generic mutation joins a/b; proved append has no hazard with those unrelated scalar branches."""
    dag=analyze('values=[1]\nobj=values\ndictionary=values\nx=3\na=x+1\nb=x+2\n'+operation+'\ny=values')
    tasks=nodes(dag)
    mutation=next(t for t in tasks if t.source==operation)
    if operation == 'values.append(x)':
        # The old full-frontier expectation serialized unrelated immutable work.
        # Exact append has no protocol, removal/finalizer, or ordinary exception.
        from dag_runtime.dag_model import EffectKind
        a,b=(next(t for t in tasks if t.source==s) for s in ('a=x+1','b=x+2'))
        assert mutation.certainty==Certainty.CERTAIN and mutation.effect==EffectKind.OBJECT_LOCAL
        assert not dag.dependency_path(a.id,mutation.id) and not dag.dependency_path(b.id,mutation.id)
        assert dag.values[dag.final_bindings['y']].object_id==dag.values[dag.final_bindings['values']].object_id
        return
    a,b=tasks[-4:-2]
    assert mutation.certainty==Certainty.CONSERVATIVE
    assert {a.id,b.id} <= mutation.dependencies
    later=tasks[-1]
    assert dag.dependency_path(mutation.id,later.id)
    assert any(r.kind==EdgeKind.ORDER for e in dag.edges.values() if e.target==mutation.id for r in e.reasons)


def test_alias_mutation_refreshes_all_alias_views(analyze,nodes):
    """Exact append advances shared object state; c=a remains an alias, not a namespace-invalidating read."""
    dag=analyze('a=[1]\nb=a\nb.append(2)\nc=a')
    create=next(t for t in nodes(dag) if t.source=='a=[1]')
    mutate=next(t for t in nodes(dag) if t.source=='b.append(2)')
    bind=next(t for t in nodes(dag) if t.source=='b=a')
    assert create.id in bind.dependencies and bind.id in mutate.dependencies
    aliases=[dag.values[dag.final_bindings[name]] for name in ('a','b','c')]
    assert len({v.object_id for v in aliases})==1
    state=dag.values[dag.final_object_states[aliases[0].object_id]]
    assert state.producer==mutate.id and aliases[-1].origin=='alias'


def test_barrier_preserves_write_after_read(analyze,nodes):
    """Both readers precede in-place mutation, preventing WAR violations through shared objects."""
    dag=analyze('a=[1,2]\nx=len(a)\ny=sum(a)\na.append(3)')
    a,x,y,write=nodes(dag)
    assert x.dependencies==y.dependencies=={a.id}
    assert {x.id,y.id} <= write.dependencies


def test_unknown_calls_are_serialized(analyze,nodes):
    """load→save_file→database.write→unknown_library_call→print, all conservative."""
    dag=analyze('x=load()\nsave_file(x)\ndatabase.write(x)\nunknown_library_call(x)\nprint(x)')
    tasks=nodes(dag)
    assert all(t.certainty==Certainty.CONSERVATIVE for t in tasks)
    for before,after in zip(tasks,tasks[1:]):
        assert before.id in after.dependencies
        assert after.conservative_reasons


def test_unknown_code_can_inject_future_assignment_targets(analyze,nodes):
    """Unknown code can install a/b with finalizers; later namespace writes must remain ordered."""
    dag=analyze('unknown()\na=1\nb=2')
    barrier,a,b=nodes(dag)
    assert barrier.id in a.dependencies and a.id in b.dependencies
    assert not dag.explain_parallelism(a.id,b.id)['allowed']
    assert any('introduced' in reason for reason in a.conservative_reasons)


def test_proved_literal_evaluations_can_overlap_after_barrier(analyze,nodes):
    """Pure literal evaluations without namespace rebinding can still overlap after a barrier."""
    dag=analyze('unknown()\n1+2\n3*4')
    barrier,a,b=nodes(dag)
    assert a.dependencies==b.dependencies=={barrier.id}
    assert dag.explain_parallelism(a.id,b.id)['allowed']


def test_alias_after_unknown_namespace_write_needs_binding_guard(analyze,nodes):
    """The alias is guarded when opaque code may already have created its target with a finalizer."""
    dag=analyze('a=load()\nb=a\nc=process(b)')
    load,bind,process=nodes(dag)
    assert load.id in bind.dependencies and bind.id in process.dependencies
    assert bind.certainty==Certainty.CONSERVATIVE
    assert any('introduced' in reason for reason in bind.conservative_reasons)


def test_multiple_targets_with_setter_may_erase_earlier_binding(analyze,nodes):
    """A setter can delete earlier target x; the following alias must not silently skip a possible NameError."""
    dag=analyze('x=holder.field=1\ny=x\nz=2')
    assign,read,z=nodes(dag)
    assert assign.certainty==read.certainty==Certainty.CONSERVATIVE
    assert assign.id in read.dependencies and read.id in z.dependencies
    xv=next(dag.values[v] for v in read.inputs if dag.values[v].name=='x')
    assert xv.may_be_unbound


@pytest.mark.parametrize('source',[
    '@decorate\ndef f(): return 1',
    '@(\n    decorate\n)\ndef f(): return 1',
    '@decorate\nclass C:\n    pass',
])
def test_decorator_source_is_preserved(analyze,nodes,source):
    """Opaque definition source includes its leading decorator, including parenthesized multiline form."""
    dag=analyze(source)
    [task]=nodes(dag)
    assert task.source==source and task.span.line==1


def test_globals_read_at_call_time(analyze,nodes):
    """The function call reads latest global x#2, never the x#1 present at definition time."""
    dag=analyze('x=1\ndef f(): return x\nx=2\ny=f()')
    x1,x2,call=nodes(dag)
    reads=[dag.values[v] for v in call.inputs if dag.values[v].name=='x']
    assert len(reads)==1 and reads[0].label=='x#2'
    assert reads[0].producer==x2.id
    assert call.certainty==Certainty.CONSERVATIVE
    assert x1.id in call.dependencies  # conservative barrier also preserves earlier commit


def test_global_write_invalidates_known_values(analyze,nodes):
    """set_x is a barrier; y's x input is a new namespace projection, not stale integer x#1."""
    dag=analyze('x=1\ndef set_x():\n    global x\n    x="changed"\n    return 0\nset_x()\ny=x+1')
    x,call,y=nodes(dag)
    assert call.certainty==y.certainty==Certainty.CONSERVATIVE
    assert any('global/nonlocal' in r for r in call.conservative_reasons)
    xv=next(dag.values[v] for v in y.inputs if dag.values[v].name=='x')
    assert xv.producer==call.id and xv.origin=='namespace'


def test_builtins_environment_replacement(analyze,nodes):
    """Replacing __builtins__ disables later builtin proofs, including in newly defined functions."""
    for prefix in ('__builtins__={}', 'def __builtins__(): return 1'):
        dag=analyze(prefix+'\ndef f(): return len([])\nx=f()\ny=1')
        tasks=nodes(dag)
        assert all(t.certainty==Certainty.CONSERVATIVE for t in tasks)
        assert any('__builtins__' in r for r in tasks[0].conservative_reasons)


def test_unknown_result_binding_may_be_changed_reentrantly(analyze,nodes):
    """Unknown RHS/finalization may change the result binding; a later bare read cannot be declared certain."""
    dag=analyze('x=unknown()\nx\nprint("after")')
    call,read,last=nodes(dag)
    assert read.certainty==Certainty.CONSERVATIVE
    assert call.id in read.dependencies and read.id in last.dependencies
    assert dag.final_namespace is not None


def test_unknown_call_can_replace_function(analyze,nodes):
    """After external(), previously pure f is no longer a trusted callable."""
    dag=analyze('def f(): return 1\na=f()\nexternal()\nb=f()')
    a,e,b=nodes(dag)
    assert a.certainty==Certainty.CERTAIN
    assert b.certainty==Certainty.CONSERVATIVE
    f=next(dag.values[v] for v in b.inputs if dag.values[v].name=='f')
    assert f.producer==e.id and f.origin=='namespace'


def test_operator_overloading_is_not_assumed_pure(analyze,nodes):
    """Unknown a+b is a barrier: __add__ may mutate globals or raise."""
    dag=analyze('a=load()\nx=a+10\ny=1')
    load,op,y=nodes(dag)
    assert op.certainty==Certainty.CONSERVATIVE
    assert any('operator' in r for r in op.conservative_reasons)
    assert load.id in op.dependencies and op.id in y.dependencies


@pytest.mark.parametrize('expression', ['obj.value','items[index]','config["threshold"]'])
def test_attribute_and_subscript_dependencies(analyze,nodes,expression):
    """Underlying objects and index are inputs; read protocols remain an ordering barrier."""
    dag=analyze('obj=1\nitems=[1]\nindex=0\nconfig={"threshold":1}\nx='+expression)
    task=nodes(dag)[-1]
    assert task.certainty==Certainty.CONSERVATIVE
    names={dag.values[v].name for v in task.inputs}
    expected={'obj'} if expression.startswith('obj') else {'items','index'} if expression.startswith('items') else {'config'}
    assert expected <= names


@pytest.mark.parametrize('code,kind',[
    ('if condition:\n    x=foo()\nelse:\n    x=bar()', 'If'),
    ('for item in items:\n    x=process(item)', 'For'),
    ('while condition:\n    x=step()', 'While'),
    ('try:\n    x=foo()\nexcept Exception:\n    x=bar()\nfinally:\n    finish()', 'Try'),
    ('with manager() as resource:\n    x=resource.read()', 'With'),
    ('match data:\n    case {"x": x}:\n        pass', 'Match'),
],ids=['if','for','while','try-finally','with','match'])
def test_control_flow_one_region(analyze,nodes,code,kind):
    """Setup→one opaque control-flow region→use(x), with no branch execution or back edges."""
    dag=analyze('seed=1\n'+code+'\ny=use(x)')
    seed,region,use=nodes(dag)
    assert region.ast_type==kind and region.kind=='opaque_region'
    assert seed.id in region.dependencies and region.id in use.dependencies
    assert len(dag.topological_order())==3
    assert any(dag.values[v].name=='x' for v in region.outputs)


def test_conditional_output_preserves_prior_value(analyze,nodes):
    """Region consumes previous x for the skipped branch; its x output may be conditionally bound."""
    dag=analyze('x=1\nif flag:\n    x=2\ny=x')
    old,region,y=nodes(dag)
    old_x=next(v for v in dag.values.values() if v.name=='x' and v.producer==old.id)
    assert old_x.id in region.inputs
    new_x=next(dag.values[v] for v in y.inputs if dag.values[v].name=='x')
    assert new_x.producer==region.id and new_x.may_be_unbound


def test_loop_local_not_external_requirement(analyze,nodes):
    """for target item and intermediate x are region-local, not required external inputs."""
    dag=analyze('values=[1,2]\nfor item in values:\n    x=item+1\n    print(x)')
    values,region=nodes(dag)
    assert values.id in region.dependencies
    assert not any(v.name in {'item','x'} and v.origin=='external' for v in dag.values.values())


def test_comprehension_kept_atomic_with_correct_scope(analyze,nodes):
    """values→one comprehension task; x is lexical-local and no per-iteration tasks exist."""
    dag=analyze('values=[1,2]\nresult=[process(x) for x in values]')
    values,result=nodes(dag)
    assert values.id in result.dependencies
    assert result.certainty==Certainty.CONSERVATIVE
    assert result.characteristics.comprehension_count==1
    assert 'x' not in {dag.values[v].name for v in result.inputs}


def test_comprehension_walrus_exports_binding(analyze,nodes):
    """Comprehension walrus y is an opaque-region output; later use waits for that region."""
    dag=analyze('values=[1,2]\nr=[(y:=x) for x in values]\nz=y')
    values,region,z=nodes(dag)
    assert any(dag.values[v].name=='y' for v in region.outputs)
    assert region.id in z.dependencies


def test_lambda_capture_and_late_rebind(analyze,nodes):
    """Lambda creation records capture x; invocation after x rebind stays conservative."""
    dag=analyze('x=1\nf=lambda a: a+x\nx=2\ny=f(3)')
    x1,closure,x2,call=nodes(dag)
    assert x1.id in closure.dependencies
    assert closure.certainty==call.certainty==Certainty.CONSERVATIVE
    assert x2.id in call.dependencies
    assert not any(v.name=='a' and v.origin=='external' for v in dag.values.values())


def test_nonlocal_nested_function_is_not_flattened(analyze,nodes):
    """A closure factory with nonlocal mutation is one conservative call, with no nested tasks."""
    dag=analyze('def outer():\n    x=0\n    def inner():\n        nonlocal x\n        x+=1\n    inner()\n    return x\ny=outer()')
    [task]=nodes(dag)
    assert task.certainty==Certainty.CONSERVATIVE


@pytest.mark.parametrize('statement',[
    'exec("x=3")','x=eval("1+2")','state=globals()','state=locals()',
    'x=getattr(obj,name)','from unknown import *',
])
def test_reflection_retains_tail_as_native_region(analyze,nodes,statement):
    """Explicit escape retains the tail; generic getattr is a local barrier with stale bindings invalidated."""
    dag=analyze('before=1\n'+statement+'\na=2\nb=3')
    if statement=='x=getattr(obj,name)':
        before,operation,a,b=nodes(dag)
        assert before.id in operation.dependencies and operation.certainty==Certainty.CONSERVATIVE
        assert operation.id in a.dependencies and a.id in b.dependencies
        assert not dag.metrics()['whole_tail_collapses']
        return
    before,tail=nodes(dag)
    assert tail.kind=='opaque_region' and before.id in tail.dependencies
    assert statement in tail.source and 'b=3' in tail.source
    assert any('dynamic namespace' in r for r in tail.conservative_reasons)


@pytest.mark.parametrize('source',[
    'async def f():\n    await unknown()\nx=f()',
    'def f():\n    yield unknown()\nx=f()',
    'values=[1,2]\ng=(x for x in values)',
    'import threading\nthreading.Thread(target=work).start()\nx=1',
    'from concurrent.futures import ThreadPoolExecutor\nx=1',
])
def test_concurrency_requires_whole_native_context(analyze,nodes,source):
    """Async/generator/concurrency scope becomes one native module node, with no parallel tasks."""
    dag=analyze(source)
    [task]=nodes(dag)
    assert task.kind=='opaque_module' and task.certainty==Certainty.CONSERVATIVE
    assert task.placement=='shared_namespace'


def test_import_binding_and_external_call(analyze,nodes):
    """Import→library call; imported symbol and argument remain explicit inputs."""
    dag=analyze('import math as m\nx=1\ny=m.sqrt(x)')
    imp,x,call=nodes(dag)
    assert imp.id in x.dependencies
    assert call.dependencies=={x.id}
    assert dag.dependency_path(imp.id,call.id)==(imp.id,x.id,call.id)
    module_view=next(dag.values[v] for v in call.inputs if dag.values[v].name=='m')
    assert module_view.origin=='namespace' and module_view.producer==x.id
    assert {'m','x'} <= {dag.values[v].name for v in call.inputs}


@pytest.mark.parametrize('definition',[
    'def f(x=side_effect()): return x',
    '@decorate\ndef f(): return 1',
    'def f(x: annotate()) -> result_type(): return x',
    'class C(base()):\n    value=side_effect()',
])
def test_definition_time_effects_are_real_nodes(analyze,nodes,definition):
    """Definition-time decorators/defaults/annotations/class body form an opaque setup node."""
    dag=analyze('a=1\n'+definition+'\nb=2')
    a,setup,b=nodes(dag)
    assert setup.certainty==Certainty.CONSERVATIVE
    assert a.id in setup.dependencies and setup.id in b.dependencies


@pytest.mark.parametrize('statement',[
    'x=1/0','a,b=(1,)','a,*b,c=()', 'assert flag', 'raise RuntimeError("stop")',
    'x=sum([None])', 'x=sum([1,None])', 'x=len(1)', 'x=abs("bad")',
])
def test_possible_exceptions_block_following_work(analyze,nodes,statement):
    """Potential ordinary exception is an ordering barrier before all subsequent work."""
    dag=analyze('before=1\n'+statement+'\nafter=2')
    before,fail,after=nodes(dag)
    assert fail.certainty==Certainty.CONSERVATIVE and fail.characteristics.may_raise
    assert before.id in fail.dependencies and fail.id in after.dependencies


@pytest.mark.parametrize('call', ['f()','f(1,2)','f(x=1,z=2)','f(1,x=2)'])
def test_call_binding_errors_preserve_order(analyze,nodes,call):
    """Invalid signature call must be conservative and block later literal assignment."""
    dag=analyze('def f(x): return x+1\na='+call+'\nb=2')
    call_node,b=nodes(dag)
    assert call_node.certainty==Certainty.CONSERVATIVE
    assert call_node.id in b.dependencies


def test_parameter_shadowing_builtin(analyze,nodes):
    """A parameter named sum must never be mistaken for the standard builtin."""
    dag=analyze('def f(sum): return sum([1])\nx=f(3)\ny=1')
    call,y=nodes(dag)
    assert call.certainty==Certainty.CONSERVATIVE
    assert call.id in y.dependencies


def test_local_shadowing_builtin_before_assignment(analyze,nodes):
    """Function-wide local scope makes early sum(...) unbound, despite a later assignment."""
    dag=analyze('def f():\n    x=sum([1])\n    sum=3\n    return x\ny=f()')
    [call]=nodes(dag)
    assert call.certainty==Certainty.CONSERVATIVE


def test_builtin_rebinding(analyze,nodes):
    """A global integer named len disables the builtin proof and makes len([]) conservative."""
    dag=analyze('len=3\nx=len([])')
    definition,call=nodes(dag)
    assert call.certainty==Certainty.CONSERVATIVE
    assert definition.id in call.dependencies


def test_unknown_object_rebinding_finalizer(analyze,nodes):
    """Dropping the last namespace binding to an unknown object is a conservative write barrier."""
    dag=analyze('x=load()\ny=1\nx=2')
    load,y,rebind=nodes(dag)
    assert rebind.certainty==Certainty.CONSERVATIVE
    assert y.id in rebind.dependencies
    assert any('finalization' in r for r in rebind.conservative_reasons)


def test_short_circuit_not_eagerly_split(analyze,nodes):
    """One opaque RHS retains short circuit; side_effect() never becomes a separate runnable node."""
    dag=analyze('x=False and side_effect()\ny=1')
    expr,y=nodes(dag)
    assert expr.certainty==Certainty.CONSERVATIVE
    assert expr.id in y.dependencies and len(dag.tasks)==2


@pytest.mark.parametrize('source', ['x =','return 3','break','nonlocal x','await f()','\x00'])
def test_malformed_source_still_returns_valid_diagnostic_graph(source):
    """Invalid syntax/scope produces one non-runnable diagnostic node and no ready tasks."""
    dag=analyze_source(source)
    dag.validate()
    assert len(dag.tasks)==1 and not dag.execution_permitted
    assert not dag.initial_ready_tasks() and dag.diagnostics


def test_analysis_budget_falls_back(analyze,nodes):
    """Exhausted AST budget produces one native module node retaining complete source."""
    source='a=1\nb=2\nc=a+b'
    dag=analyze(source,options=AnalysisOptions(max_ast_nodes=3))
    [task]=nodes(dag)
    assert task.kind=='opaque_module' and 'c=a+b' in task.source


def test_function_proof_budget_fails_closed(analyze,nodes):
    """A function exceeding the bounded proof size becomes conservative, not an analysis failure."""
    dag=analyze('def f(x): return x+1\na=f(2)',options=AnalysisOptions(max_function_ast_nodes=2))
    [call]=nodes(dag)
    assert call.certainty==Certainty.CONSERVATIVE
    assert any('budget' in r for r in call.conservative_reasons)


def test_no_user_code_is_executed(analyze,tmp_path):
    """Analysis records the file-writing call but never runs it or creates its side effect."""
    path=tmp_path/'should_not_exist'
    dag=analyze(f'open({str(path)!r},"w").write("unsafe to evaluate")')
    assert not path.exists()
    assert all(t.certainty==Certainty.CONSERVATIVE for t in dag.tasks.values())
