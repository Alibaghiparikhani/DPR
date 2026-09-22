"""Small graph expectations are explicit in each test's docstring or parameter ID."""
from dag_runtime.dag_model import Certainty, EdgeKind


def test_independent_calls(analyze, nodes):
    """Two known constant-return invocations are distinct, initially ready roots."""
    dag = analyze('def f(): return 3\na=f()\nb=f()')
    a, b = nodes(dag)
    assert a.id != b.id and a.callable_repr == b.callable_repr == 'f'
    assert dag.edges == {}
    assert dag.initial_ready_tasks() == (a.id, b.id)
    assert dag.explain_parallelism(a.id, b.id)['allowed']


def test_diamond(analyze, nodes):
    """Expected A→B, A→C, B→D, C→D; B and C are independent."""
    dag = analyze('a=1\nb=a+2\nc=a*3\nd=b+c')
    a, b, c, d = nodes(dag)
    assert set(dag.edges) == {(a.id,b.id),(a.id,c.id),(b.id,d.id),(c.id,d.id)}
    assert dag.sources() == (a.id,)
    assert dag.sinks() == (d.id,)
    assert dag.explain_parallelism(b.id,c.id)['allowed']


def test_reassignment_chain(analyze, nodes):
    """x#1→x#2→x#3→result#1; no self-edge or final-producer substitution."""
    dag = analyze('def step(x): return x+1\nx=1\nx=step(x)\nx=step(x)\nresult=step(x)')
    a,b,c,d = nodes(dag)
    assert set(dag.edges) == {(a.id,b.id),(b.id,c.id),(c.id,d.id)}
    assert [v.label for v in dag.values.values() if v.name == 'x'] == ['x#1','x#2','x#3']
    assert dag.values[dag.final_bindings['x']].producer == c.id


def test_simple_alias_binding_is_materialized_in_source_order(analyze, nodes):
    """Phase 1: load→alias-bind→process preserves identity and source-order visibility."""
    dag = analyze('def load(): return [1,2]\na=load()\nb=a\nc=process(b)')
    load, bind, process = nodes(dag)
    alias = next(v for v in dag.values.values() if v.origin == 'alias' and v.name == 'b')
    base = dag.values[bind.inputs[0]]
    assert alias.producer == bind.id and base.producer == load.id
    assert alias.object_id == base.object_id
    assert alias.id in process.inputs
    assert dag.dependency_path(load.id, process.id) == (load.id, bind.id, process.id)


def test_alias_survives_rebinding(analyze, nodes):
    """c reads the materialized old-x alias even after x is rebound."""
    dag = analyze('x=1\nb=x\nx=2\nc=b+3')
    old, bind, new, c = nodes(dag)
    assert bind.dependencies == {old.id}
    assert c.dependencies == {bind.id}
    assert bind.id in new.dependencies  # source-order namespace commit fence
    alias = dag.values[dag.final_bindings['b']]
    assert alias.object_id == dag.values[old.outputs[0]].object_id


def test_war_waw_eliminated_only_by_immutable_versions(analyze, nodes):
    """Old reader and later immutable rebind may overlap; each uses its own version."""
    dag = analyze('x=1\ny=x+1\nx=3\nz=x+1')
    a,b,c,d = nodes(dag)
    assert set(dag.edges) == {(a.id,b.id),(b.id,c.id),(c.id,d.id)}
    # Assignment commits are source ordered even when value versions are immutable.
    assert not dag.explain_parallelism(b.id,c.id)['allowed']


def test_constants_do_not_create_argument_tasks(analyze, nodes):
    """Only data producer→call; constants and keyword literals have no value dependencies."""
    dag = analyze('def f(data, n, text, flag, threshold): return data+n\ndata=4\nx=f(data,10,"hello",True,threshold=0.8)')
    data,call = nodes(dag)
    assert call.dependencies == {data.id}
    assert {dag.values[v].name for v in call.inputs} == {'f','data'}
    assert call.certainty == Certainty.CERTAIN


def test_keyword_value_dependency(analyze, nodes):
    """Both data and config producers feed the keyword-argument call."""
    dag = analyze('def f(data, threshold, config): return data+config\ndata=2\nconfig=3\nx=f(data,threshold=0.8,config=config)')
    data,config,call = nodes(dag)
    assert call.dependencies == {data.id,config.id}
    assert call.certainty == Certainty.CERTAIN


def test_nested_arithmetic(analyze, nodes):
    """a, scale, offset all feed one containing call; there are no fake expression tasks."""
    dag = analyze('def f(x): return x\na=2\nscale=3\noffset=4\nx=f(a*scale+offset)')
    a,scale,offset,call = nodes(dag)
    assert call.dependencies == {a.id,scale.id,offset.id}
    assert call.characteristics.call_count == 1


def test_containers(analyze, nodes):
    """Every referenced element/key/value feeds its containing container expression."""
    dag = analyze('a=1\nb=2\nc=3\nx=[a,b]\ny={"first":a,"second":c}\nz=(a,b,c)')
    a,b,c,x,y,z=nodes(dag)
    assert x.dependencies == {a.id,b.id}
    assert y.dependencies == {a.id,c.id}
    assert z.dependencies == {a.id,b.id,c.id}
    assert all(t.certainty == Certainty.CERTAIN for t in nodes(dag))


def test_nested_calls_are_atomic(analyze, nodes):
    """clean and analyze remain inside the combine statement, which consumes a and b."""
    dag = analyze('def clean(x): return x+1\ndef analyze(x): return x*2\ndef combine(x,y): return x+y\na=1\nb=2\nresult=combine(clean(a),analyze(b))')
    a,b,result=nodes(dag)
    assert len(dag.tasks)==3
    assert result.dependencies == {a.id,b.id}
    assert result.characteristics.call_count == 3
    assert result.certainty == Certainty.CERTAIN


def test_unknown_nested_calls_not_flattened(analyze, nodes):
    """Unknown nested calls stay in one opaque expression, preserving Python evaluation order."""
    dag=analyze('a=1\nb=2\nresult=combine(clean(a),analyze(b))')
    a,b,result=nodes(dag)
    assert result.dependencies == {a.id,b.id}
    assert result.certainty == Certainty.CONSERVATIVE
    assert 'combine(clean(a),analyze(b))' in result.source


def test_tuple_unpacking(analyze, nodes):
    """One split task publishes x and y projections; both readers depend on that task."""
    dag=analyze('def split(data): return (data,data+1)\ndata=3\nx,y=split(data)\na=x+10\nb=y+20')
    data,split,a,b=nodes(dag)
    assert split.dependencies == {data.id}
    assert a.dependencies == b.dependencies == {split.id}
    outputs=[dag.values[v] for v in split.outputs]
    assert [(v.name,v.projection) for v in outputs] == [('x',(0,)),('y',(1,))]
    assert split.certainty == Certainty.CERTAIN


def test_nested_and_starred_unpacking(analyze, nodes):
    """Nested known shapes and starred rest are projections from one successful assignment."""
    dag=analyze('a,(b,c),*rest=(1,(2,3),4,5)\nx=a+b+c')
    unpack,x=nodes(dag)
    assert unpack.certainty == Certainty.CERTAIN
    assert x.dependencies == {unpack.id}
    values={dag.values[v].name:dag.values[v] for v in unpack.outputs}
    assert values['c'].projection==(1,1)
    assert values['rest'].projection==('2:*',)
    assert values['rest'].type_hint=='list'


def test_chained_assignment_same_reference(analyze,nodes):
    """a=b=[] produces two bindings to one reference group, with one task."""
    dag=analyze('a=b=[]')
    [task]=nodes(dag)
    a,b=[dag.values[v] for v in task.outputs]
    assert a.object_id == b.object_id and a.id != b.id


def test_repeated_unpack_target_versioning(analyze,nodes):
    """x,x=(1,2) publishes both successive bindings; final x is the second projection."""
    dag=analyze('x,x=(1,2)')
    [task]=nodes(dag)
    x1,x2=[dag.values[v] for v in task.outputs]
    assert (x1.label,x2.label)==('x#1','x#2')
    assert dag.final_bindings['x']==x2.id


def test_function_versions_and_alias(analyze,nodes):
    """Both definitions retain identity; old() resolves through its materialized alias binding."""
    dag=analyze('def f(): return 1\nold=f\ndef f(): return 2\na=old()\nb=f()')
    bind,a,b=nodes(dag)
    assert len(dag.definitions)==2
    assert a.dependencies == {bind.id} and not b.dependencies
    assert all(t.certainty==Certainty.CERTAIN for t in (bind,a,b))
    assert {dag.values[v].name for v in a.inputs}=={'old'}


def test_plain_definitions_are_not_body_executions(analyze):
    """Defining f with an effectful body creates only a definition, never a print task."""
    dag=analyze('def f():\n    print("not executed")\n    return 1')
    assert not dag.tasks and len(dag.definitions)==1


def test_module_docstring_binding(analyze,nodes):
    """The module docstring produces __doc__; the alias is materialized after it."""
    dag=analyze('"documentation"\nx=__doc__')
    task, bind = nodes(dag)
    assert task.kind=='module_docstring'
    assert bind.dependencies == {task.id}
    assert dag.values[dag.final_bindings['x']].producer==bind.id


def test_final_bindings_sinks_and_edge_reasons(analyze,nodes):
    """Two final branch results are both sinks and direct RAW reasons name their inputs."""
    dag=analyze('a=1\nb=a+2\nc=a+3')
    a,b,c=nodes(dag)
    assert dag.sinks()==(b.id,c.id)
    assert set(dag.final_bindings)=={'a','b','c'}
    edge=dag.explain_dependency(a.id,b.id)
    assert edge.reasons[0].kind==EdgeKind.DATA
    assert edge.reasons[0].certainty==Certainty.CERTAIN
    assert 'a#1' in edge.reasons[0].text
