"""Test-only semantic oracle for small, effect-free fixtures.

This is not a runtime or scheduler. Exhaustively replay every legal order of
tiny pure graphs using value IDs, and compare final bindings to ordinary Python.
Only the literal source fixtures below are executed; analyzed user files are not.
"""
import ast
import textwrap

import pytest

from dag_runtime.dag_model import Certainty


def all_orders(dag, done=()):
    if len(done)==len(dag.tasks):
        yield done
        return
    completed=set(done)
    for task in dag.tasks.values():
        if task.id not in completed and task.dependencies <= completed:
            yield from all_orders(dag,done+(task.id,))


def replay_pure_fixture(dag, order):
    results={}
    definition_values={}
    for definition in dag.definitions.values():
        scope={}
        exec(definition.source,scope)
        for value in dag.values.values():
            if value.origin=='definition' and value.name==definition.name:
                # These fixtures use one definition per name. Dedicated structural
                # tests separately cover redefinition/late-binding boundaries.
                definition_values[value.id]=scope[definition.name]

    def value(ident):
        record=dag.values[ident]
        if record.alias_of:
            return value(record.alias_of)
        if ident in definition_values:
            return definition_values[ident]
        if record.origin=='builtin':
            import builtins
            return getattr(builtins,record.name)
        return results[ident]

    for ident in order:
        task=dag.tasks[ident]
        assert task.certainty==Certainty.CERTAIN
        scope={dag.values[v].name:value(v) for v in task.inputs}
        exec(task.source,scope)
        for out in task.outputs:
            record=dag.values[out]
            # Phase 1 materializes alias bindings as real source-order tasks, so
            # their value is produced by executing the binding just like any
            # other task rather than reconstructed from alias_of metadata.
            results[out]=scope[record.name]
    return {name:value(ident) for name,ident in dag.final_bindings.items()
            if dag.values[ident].origin!='definition'}


@pytest.mark.parametrize('source',[
    'a=1\nb=2\nc=a+b',
    'a=2\nb=a+1\nc=a*3\nd=b+c',
    'x=1\ny=x+2\nx=3\nz=x+y',
    'x=1\ny=x\nx=3\nz=y+4',
    'def f(x): return x+1\na=1\nb=f(a)\nc=f(a)\nd=b+c',
    'def f(x=3,*,y=2): return x*y\na=f()\nb=f(4,y=5)\nc=a+b',
    'def f(x,/): return x+1\na=f(1)\nb=f(2)',
    'def f(x):\n    y=x+1\n    z=y*2\n    return z\na=f(2)\nb=f(3)',
    'def f(x): return x+1\ndef g(x,y): return x*y\na=g(f(2),f(3))\nb=g(f(4),f(5))',
    'a=[1,2,3]\nb=sum(a)\nc=len(a)\nd=b+c',
    'a,b=(1,2)\nc=a+b',
    'x=1.5\ny=x*2.0\nz=x+1.0',
],ids=['independent','diamond','reassignment','alias','calls','defaults-keywords','positional-only',
        'body-locals','nested-calls','builtins','unpacking','exact-floats'])
def test_every_allowed_order_matches_python(analyze,source):
    """Every legal topological order must produce the same final values as sequential Python."""
    source=textwrap.dedent(source)
    dag=analyze(source)
    expected={}
    exec(source,expected)
    orders=list(all_orders(dag))
    assert orders
    for order in orders:
        actual=replay_pure_fixture(dag,order)
        assert actual=={name:expected[name] for name in actual}


@pytest.mark.parametrize('source',[
    'def f(x:int): return x+1\na=f(data)\nb=1',
    'def f(x): return x/0\na=f(1)\nb=2',
    'def f(): return missing\na=f()\nb=2',
    'def f():\n    x=unknown()\n    return 1\na=f()\nb=2',
    'def f():\n    return f()\na=f()\nb=2',
    'def f(*args): return 1\na=f(2)\nb=3',
    'def f(x,/): return x\na=f(x=1)\nb=2',
    'def f():\n    x=x+1\n    return x\na=f()\nb=2',
],ids=['annotation-not-proof','division-error','unbound-global','unknown-body-call','recursion',
        'variadic','invalid-posonly-keyword','unbound-local'])
def test_unproved_call_is_never_parallel_with_following_work(analyze,source):
    """Each invalid/uncertain function call is a barrier with a path to the following statement."""
    dag=analyze(source)
    tasks=list(dag.tasks.values())
    call,last=tasks[-2:]
    assert call.certainty==Certainty.CONSERVATIVE
    assert dag.dependency_path(call.id,last.id)


def test_large_alias_type_shape_is_bounded(analyze):
    """Repeated x=(x,x) keeps 81 tasks and bounded type facts, rather than exponential expansion."""
    dag=analyze('x=1\n'+'x=(x,x)\n'*80)
    assert len(dag.tasks)==81
    assert all(t.certainty==Certainty.CERTAIN for t in dag.tasks.values())
    assert len(dag.edges)==80


def test_nested_singleton_type_hashing_is_bounded(analyze):
    """Nested singleton containers must keep specialization-key hashing bounded as well as type traversal."""
    source='def identity(x): return x\nx=1\n'+'x=[x]\n'*200+'y=identity(x)'
    dag=analyze(source)
    assert len(dag.tasks)==202 and len(dag.edges)==201
    assert all(t.certainty==Certainty.CERTAIN for t in dag.tasks.values())


def test_large_linear_graph_and_readiness(analyze):
    """A 2,001-node chain has exactly 2,000 dependency pairs and unlocks one successor at a time."""
    dag=analyze('x=0\n'+'x=x+1\n'*2000)
    assert len(dag.tasks)==2001 and len(dag.edges)==2000
    state=dag.new_readiness()
    for ident in dag.topological_order():
        assert state.ready==(ident,)
        state.mark_completed(ident)
    assert not state.ready


def test_many_roots_join_in_one_barrier(analyze):
    """One barrier joins 1,000 independent roots with 1,000 edges, not an all-pairs graph."""
    dag=analyze('\n'.join(f'x{i}={i}' for i in range(1000))+'\nunknown()')
    tasks=list(dag.tasks.values())
    assert len(tasks[-1].dependencies)==1000
    assert len(dag.edges)==1000


def test_long_expression_recursion_fallback_is_valid():
    """Deep source either analyzes normally or becomes an explicit valid fallback, never a broken partial graph."""
    from dag_runtime.dag_engine import analyze_source
    dag=analyze_source('x='+('+'.join(['1']*1500)))
    dag.validate()
    assert dag.tasks


def test_prototype_files_are_regressions_not_proof_contract():
    """Both prototypes remain valid; their exact arithmetic comprehensions now pass the bounded proof."""
    from pathlib import Path
    from dag_runtime.dag_engine import analyze_file
    for path in (Path(__file__).resolve().parents[1]/'examples').glob('prototype_*.py'):
        dag=analyze_file(path)
        dag.validate()
        assert dag.execution_permitted
        cleaning=[t for t in dag.tasks.values() if t.callable_repr in {'clean_users','clean_orders'}]
        assert len(cleaning)==2
        assert all(t.certainty==Certainty.CERTAIN and t.characteristics.comprehension_count for t in cleaning)
        assert dag.explain_parallelism(cleaning[0].id,cleaning[1].id)['allowed']
