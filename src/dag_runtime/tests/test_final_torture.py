import random

from dag_runtime.dag_model import Certainty, EffectKind
from test_semantic_contract import all_orders, replay_pure_fixture


def random_topological_order(dag, rng):
    remaining = {
        tid: set(task.dependencies)
        for tid, task in dag.tasks.items()
    }

    ready = [
        tid for tid, deps in remaining.items()
        if not deps
    ]

    order = []

    while ready:
        choice = rng.choice(ready)
        ready.remove(choice)
        order.append(choice)

        for child in dag.tasks[choice].dependents:
            deps = remaining[child]
            deps.discard(choice)

            if (
                not deps
                and child not in order
                and child not in ready
            ):
                ready.append(child)

    assert len(order) == len(dag.tasks)
    return tuple(order)


def test_final_complex_pure_dag_random_interleavings(analyze):
    source = '''
def double(values): return [x*2 for x in values]
def bump(values): return [x+1 for x in values]

left=[1,2,3]
right=[4,5,6]

left2=double(left)
right2=bump(right)

l0=left2[0]
l2=left2[-1]

r0=right2[0]
r2=right2[-1]

a=l0+l2
b=r0+r2

c=sum(left2)
d=sum(right2)

e=a+c
f=b+d

g=e+f

alias=left2
h=alias[1]

result=g+h
'''

    dag = analyze(source)

    assert len(dag.tasks) == 18
    assert all(
        t.certainty == Certainty.CERTAIN
        for t in dag.tasks.values()
    )

    assert len(dag.initial_ready_tasks()) == 2

    expected = {}
    exec(source, expected)

    expected_result = expected['result']

    # This DAG has millions of possible legal schedules.
    # Instead of materializing all of them, test 10,000
    # deterministic random topological schedules.
    rng = random.Random(0xDADA)

    seen = set()

    for _ in range(10000):
        order = random_topological_order(dag, rng)

        seen.add(order)

        actual = replay_pure_fixture(dag, order)

        assert actual['result'] == expected_result
        assert actual['alias'] == expected['alias']

    # Make sure we actually exercised many distinct schedules.
    assert len(seen) > 5000


def test_final_complex_object_alias_dag_all_interleavings(analyze):
    source = '''
a=[1,2]
b=a

c=[10,20]
d=c

pre_a=sum(a)
pre_c=sum(c)

a.append(3)
c.append(30)

a_mid=a[-1]
c_mid=c[-1]

a.append(4)
c.append(40)

post_a=sum(b)
post_c=sum(d)

x=pre_a+post_a
y=pre_c+post_c

result=x+y
'''

    dag = analyze(source)

    assert len(dag.tasks) == 17

    assert all(
        t.certainty == Certainty.CERTAIN
        for t in dag.tasks.values()
    )

    expected = {}
    exec(source, expected)

    order_count = 0

    # This one is small enough to exhaustively execute
    # EVERY legal topological ordering.
    for order in all_orders(dag):
        actual = {}

        for ident in order:
            exec(dag.tasks[ident].source, actual)

            # Phase 1 materializes alias bindings as source-order tasks;
            # executing the task source above already applies them.

        # Values must match ordinary sequential Python.
        assert actual['result'] == expected['result']

        assert actual['pre_a'] == expected['pre_a']
        assert actual['post_a'] == expected['post_a']

        assert actual['pre_c'] == expected['pre_c']
        assert actual['post_c'] == expected['post_c']

        # Even object identity must survive.
        assert actual['a'] is actual['b']
        assert actual['c'] is actual['d']

        assert actual['a'] == expected['a']
        assert actual['c'] == expected['c']

        order_count += 1

    # On the current graph there are 3,432 legal schedules.
    assert order_count > 3000


def test_final_barriers_recovery_and_true_escape(analyze):
    source = '''
def f(x): return x+1

left=1
right=2
divisor=2

q=100//divisor

a=f(left)
b=f(right)

joined=a+b

getattr([1,2], "append")

1+2
3*4

namespace=globals()

tail_a=10
tail_b=20
tail_result=tail_a+tail_b
'''

    dag = analyze(source)
    tasks = list(dag.tasks.values())

    # --------------------------------------------------
    # 1. Exception-only fence
    # --------------------------------------------------

    division = next(
        t for t in tasks
        if '100//divisor' in t.source.replace(' ', '')
    )

    assert division.certainty == Certainty.CONSERVATIVE

    # It may raise, but it is not a namespace effect.
    assert division.effect == EffectKind.PURE

    a_task = next(
        t for t in tasks
        if t.source.strip() == 'a=f(left)'
    )

    b_task = next(
        t for t in tasks
        if t.source.strip() == 'b=f(right)'
    )

    # Both must wait for successful division completion.
    assert dag.dependency_path(division.id, a_task.id)
    assert dag.dependency_path(division.id, b_task.id)

    # But the two branches remain independent afterward.
    assert not dag.dependency_path(a_task.id, b_task.id)
    assert not dag.dependency_path(b_task.id, a_task.id)

    # --------------------------------------------------
    # 2. Recoverable reflection
    # --------------------------------------------------

    reflection = next(
        t for t in tasks
        if t.callable_repr == 'getattr'
    )

    assert reflection.certainty == Certainty.CONSERVATIVE
    assert reflection.effect == EffectKind.NAMESPACE

    post = [
        t for t in tasks
        if t.source.strip() in {'1+2', '3*4'}
    ]

    assert len(post) == 2

    assert all(
        t.certainty == Certainty.CERTAIN
        for t in post
    )

    # Both happen after getattr...
    assert all(
        dag.dependency_path(reflection.id, t.id)
        for t in post
    )

    # ...but neither depends on the other.
    assert not dag.dependency_path(post[0].id, post[1].id)
    assert not dag.dependency_path(post[1].id, post[0].id)

    # --------------------------------------------------
    # 3. Real namespace escape
    # --------------------------------------------------

    tail = tasks[-1]

    assert tail.region_scope == 'tail'
    assert tail.effect == EffectKind.ESCAPE

    assert 'globals()' in tail.source
    assert 'tail_result' in tail.source

    # globals() + three statements after it
    assert tail.statement_count == 4