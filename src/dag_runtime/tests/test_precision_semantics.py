"""Fixed-fixture semantic oracles; these helpers are not runtime execution code."""
import pytest

from dag_runtime.dag_model import Certainty
from test_semantic_contract import all_orders, replay_pure_fixture


@pytest.mark.parametrize('source', [
    'a=(10,20,30)\nb=a[-1]\nc=a[0]\nd=b+c',
    'a=[1,2]\nb=[x*2 for x in a]\nc=[x+1 for x in a]\nd=sum(b)+sum(c)',
    'def clean(values): return [x*2 for x in values if x>0]\na=[-1,2]\nb=clean(a)\nc=clean((3,4))\nd=sum(b)+sum(c)',
    'def f(values): return values[1]+1\na=(1,2)\nb=f(a)\nc=f((3,4))\nd=b+c',
    'a=(1,2)\nb=[x*2 for x in a]\nc=b[-1]\nd=a[0]+1',
])
def test_every_new_pure_order_matches_sequential_python(analyze,source):
    """Every permitted order of exact reads/comprehensions produces the same final values as native Python."""
    dag=analyze(source)
    expected={}
    exec(source,expected)
    for order in all_orders(dag):
        actual=replay_pure_fixture(dag,order)
        assert actual=={name:expected[name] for name in actual}


@pytest.mark.parametrize('source', [
    'a=[1]\nb=a\nx=sum(a)\na.append(2)\ny=sum(b)\nz=10+20',
    'a=[1]\nb=[2]\na.append(3)\nb.append(4)\nx=sum(a)\ny=sum(b)',
    'a=[1]\nb=[2]\na.append(b[0])\nb.append(3)\nx=sum(a)\ny=sum(b)',
    'a=[1]\na.append(2)\nx=a[-1]\na.append(3)\ny=a[-1]\nz=20+30',
    'a=b=[1]\nx=len(a)\nb.append(2)\ny=sum(a)\nz=4+5',
])
def test_every_object_state_order_preserves_native_values_and_aliases(analyze,source):
    """Every legal interleaving of bounded appends and reads preserves both list contents and reader results."""
    dag=analyze(source)
    assert all(t.certainty==Certainty.CERTAIN for t in dag.tasks.values())
    expected={}
    exec(source,expected)
    order_count=0
    for order in all_orders(dag):
        actual={}
        for ident in order:
            exec(dag.tasks[ident].source,actual)
            # Phase 1 materializes direct alias/rebind events as source-order
            # tasks, so executing task.source above already installs the binding.
        assert {n:actual[n] for n in dag.final_bindings}=={n:expected[n] for n in dag.final_bindings}
        for left in dag.final_bindings:
            for right in dag.final_bindings:
                if isinstance(expected[left],list) and isinstance(expected[right],list):
                    assert (actual[left] is actual[right])==(expected[left] is expected[right])
        order_count+=1
    assert order_count>1


def test_injected_finalizer_really_can_replace_a_fresh_assignment(analyze):
    """CPython's overwrite can call __del__ after storing 10; the graph must not assume x stays integer."""
    source='''
class Bomb:
    def __del__(self):
        global x
        x = "replaced by finalizer"
x = Bomb()
x = 10
'''
    scope={}
    exec(source,scope)
    assert scope['x']=='replaced by finalizer'
    dag=analyze(source)
    x=dag.values[dag.final_bindings['x']]
    assert x.type_hint=='unknown' and x.may_be_unbound
    assert list(dag.tasks.values())[-1].certainty==Certainty.CONSERVATIVE


def test_shape_budget_loses_precision_without_stale_index_proofs(analyze):
    """After shape truncation, a later negative index is ordered, never inferred from an old shorter list."""
    source='a=[1]\n'+'a.append(2)\n'*33+'x=a[-1]\ny=2'
    dag=analyze(source)
    x,y=list(dag.tasks.values())[-2:]
    assert x.certainty==Certainty.CONSERVATIVE and x.id in y.dependencies
