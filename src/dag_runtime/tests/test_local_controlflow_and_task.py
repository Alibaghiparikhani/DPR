"""Precision and explicit-task contract tests for realistic CPU functions."""
from __future__ import annotations

from dag_runtime.dag_model import Certainty, EffectKind
from execution import ExecutionMode, ValueKind, lower_dag


PRIME_BODY = '''
def count_primes(start, end):
    count = 0
    for n in range(max(2, start), end + 1):
        is_prime = True
        d = 2
        while d * d <= n:
            if n % d == 0:
                is_prime = False
                break
            d += 1
        if is_prime:
            count += 1
    return count
'''.strip()


def test_realistic_prime_loop_is_automatically_parallel(analyze, nodes):
    dag = analyze(PRIME_BODY + '''

a=count_primes(1, 5_000_000)
b=count_primes(5_000_001, 10_000_000)
c=count_primes(10_000_001, 15_000_000)
total=a+b+c
''')
    a, b, c, total = nodes(dag)
    assert all(t.certainty == Certainty.CERTAIN for t in (a, b, c, total))
    assert all(t.effect == EffectKind.PURE for t in (a, b, c, total))
    assert all(t.placement == 'isolated_candidate' for t in (a, b, c, total))
    assert a.dependencies == b.dependencies == c.dependencies == frozenset()
    assert total.dependencies == {a.id, b.id, c.id}
    assert not dag.dependency_path(a.id, b.id)
    assert not dag.dependency_path(b.id, c.id)
    assert dag.metrics()['max_generation_width'] >= 3


def test_prime_loop_lowers_to_independent_execution_manifests(analyze):
    dag = analyze(PRIME_BODY + '''

a=count_primes(1, 1000)
b=count_primes(1001, 2000)
c=count_primes(2001, 3000)
total=a+b+c
''')
    plan = lower_dag(dag, environment_id='cpu-test', package_id='prime-v1')
    plan.validate_against(dag)
    assert [m.mode for m in plan.tasks] == [ExecutionMode.ISOLATED_CANDIDATE] * 4
    assert plan.tasks[0].dependencies == plan.tasks[1].dependencies == plan.tasks[2].dependencies == frozenset()
    assert plan.tasks[3].dependencies == {m.task_id for m in plan.tasks[:3]}


def test_local_control_flow_does_not_mean_external_mutation(analyze, nodes):
    dag = analyze('''
def f(limit):
    total=0
    i=0
    while i < limit:
        if i > 2:
            total += i
        i += 1
    return total

a=f(10)
b=f(20)
c=a+b
''')
    a, b, c = nodes(dag)
    assert a.certainty == b.certainty == c.certainty == Certainty.CERTAIN
    assert a.dependencies == b.dependencies == frozenset()
    assert c.dependencies == {a.id, b.id}


def test_global_mutation_remains_conservative(analyze, nodes):
    dag = analyze('''
x=0
def f(n):
    global x
    x += n
    return x

a=f(1)
b=f(2)
''')
    x, a, b = nodes(dag)
    assert a.certainty == Certainty.CONSERVATIVE
    assert b.certainty == Certainty.CONSERVATIVE
    assert a.effect == EffectKind.NAMESPACE
    assert a.id in b.dependencies


def test_argument_object_mutation_remains_conservative(analyze, nodes):
    dag = analyze('''
def f(values):
    values.append(1)
    return values

x=[1]
a=f(x)
b=2
''')
    x, a, b = nodes(dag)
    assert a.certainty == Certainty.CONSERVATIVE
    assert a.effect == EffectKind.NAMESPACE
    assert a.id in b.dependencies


def test_unknown_nested_call_remains_conservative(analyze, nodes):
    dag = analyze('''
def f(x):
    y=helper(x)
    return y

a=f(1)
b=2
''')
    a, b = nodes(dag)
    assert a.certainty == Certainty.CONSERVATIVE
    assert a.id in b.dependencies


def test_runtime_task_marker_is_explicit_contract_and_lowers_isolated(analyze, nodes):
    dag = analyze('''
from dag_runtime import task
@task
def opaque_work(x):
    hidden_library_call(x)
    return x

a=opaque_work(1)
b=opaque_work(2)
''')
    a, b = nodes(dag)
    assert a.certainty == b.certainty == Certainty.CERTAIN
    assert a.effect == b.effect == EffectKind.PURE
    assert a.placement == b.placement == 'isolated_candidate'
    assert a.dependencies == b.dependencies == frozenset()
    plan = lower_dag(dag, environment_id='cpu-test')
    assert [m.mode for m in plan.tasks] == [ExecutionMode.ISOLATED_CANDIDATE] * 2
    assert all(v.kind != ValueKind.NATIVE_REFERENCE for m in plan.tasks for v in (*m.inputs, *m.outputs))


def test_task_marker_alias_import_is_recognized(analyze, nodes):
    dag = analyze('''
from dag_runtime import task as distributed_task
@distributed_task
def opaque(x):
    mystery(x)
    return x

a=opaque(1)
b=opaque(2)
''')
    a, b = nodes(dag)
    assert a.placement == b.placement == 'isolated_candidate'
    assert a.dependencies == b.dependencies == frozenset()


def test_arbitrary_local_decorator_named_task_is_not_trusted(analyze, nodes):
    dag = analyze('''
def task(fn):
    side_effect()
    return fn
@task
def f(x):
    return x+1

a=f(1)
''')
    definition_region, call = nodes(dag)
    assert definition_region.kind == 'opaque_region'
    assert definition_region.certainty == Certainty.CONSERVATIVE
    assert call.certainty == Certainty.CONSERVATIVE


def test_bare_task_name_without_runtime_import_is_not_trusted(analyze, nodes):
    dag = analyze('''
@task
def f(x):
    return x+1

a=f(1)
''')
    definition_region, call = nodes(dag)
    assert definition_region.certainty == Certainty.CONSERVATIVE
    assert call.certainty == Certainty.CONSERVATIVE


def test_rebinding_runtime_task_marker_disables_special_treatment(analyze, nodes):
    dag = analyze('''
from dag_runtime import task
task=decorate
@task
def f(x):
    return x+1

a=f(1)
''')
    assign, definition_region, call = nodes(dag)
    assert assign.certainty == Certainty.CONSERVATIVE
    assert definition_region.kind == 'opaque_region'
    assert definition_region.certainty == Certainty.CONSERVATIVE
    assert call.certainty == Certainty.CONSERVATIVE


def test_later_loop_iteration_zero_divisor_is_not_falsely_proved(analyze, nodes):
    dag = analyze('''
def f(limit):
    d=2
    out=0
    i=0
    while i < limit:
        out = 10 % d
        d = 0
        i += 1
    return out

a=f(2)
b=1
''')
    a, b = nodes(dag)
    assert a.certainty == Certainty.CONSERVATIVE
    assert a.id in b.dependencies


def test_nested_external_mutation_inside_loop_stays_conservative(analyze, nodes):
    dag = analyze('''
def f(values, limit):
    i=0
    while i < limit:
        if i > 1:
            values.append(i)
        i += 1
    return i

x=[1]
a=f(x, 3)
b=2
''')
    x, a, b = nodes(dag)
    assert a.certainty == Certainty.CONSERVATIVE
    assert a.id in b.dependencies


COLLATZ_BODY = '''
def collatz_score(start, end):
    checksum = 0
    longest = 0
    for n in range(start, end + 1):
        x = n
        steps = 0
        while x != 1:
            if x % 2 == 0:
                x = x // 2
            else:
                x = 3 * x + 1
            steps += 1
        checksum += steps
        if steps > longest:
            longest = steps
    return checksum + longest
'''.strip()


def test_collatz_style_branching_loop_is_automatically_parallel(analyze, nodes):
    dag = analyze(COLLATZ_BODY + '''

a=collatz_score(1, 300_000)
b=collatz_score(300_001, 600_000)
c=collatz_score(600_001, 900_000)
total=a+b+c
''')
    a, b, c, total = nodes(dag)
    assert all(t.certainty == Certainty.CERTAIN for t in (a, b, c, total))
    assert all(t.effect == EffectKind.PURE for t in (a, b, c, total))
    assert all(t.placement == 'isolated_candidate' for t in (a, b, c, total))
    assert a.dependencies == b.dependencies == c.dependencies == frozenset()
    assert total.dependencies == {a.id, b.id, c.id}
    assert dag.initial_ready_tasks() == (a.id, b.id, c.id)


def test_collatz_style_result_type_propagates_through_execution(analyze):
    dag = analyze(COLLATZ_BODY + '''

a=collatz_score(1, 100)
b=collatz_score(101, 200)
c=collatz_score(201, 300)
total=a+b+c
''')
    plan = lower_dag(dag, environment_id='cpu-test', package_id='collatz-v1')
    plan.validate_against(dag)
    assert [m.mode for m in plan.tasks] == [ExecutionMode.ISOLATED_CANDIDATE] * 4
    assert all(m.outputs[0].kind == ValueKind.IMMUTABLE for m in plan.tasks)
    assert plan.tasks[3].dependencies == {m.task_id for m in plan.tasks[:3]}


def test_explicit_task_keeps_proved_collatz_integer_return_type(analyze, nodes):
    dag = analyze('''
from dag_runtime import task
@task
def collatz_score(start, end):
    checksum = 0
    longest = 0
    for n in range(start, end + 1):
        x = n
        steps = 0
        while x != 1:
            if x % 2 == 0:
                x = x // 2
            else:
                x = 3 * x + 1
            steps += 1
        checksum += steps
        if steps > longest:
            longest = steps
    return checksum + longest

a=collatz_score(1, 100)
b=collatz_score(101, 200)
c=collatz_score(201, 300)
total=a+b+c
''')
    a, b, c, total = nodes(dag)
    assert a.dependencies == b.dependencies == c.dependencies == frozenset()
    assert total.dependencies == {a.id, b.id, c.id}
    assert all(t.certainty == Certainty.CERTAIN for t in (a, b, c, total))
    assert all(t.placement == 'isolated_candidate' for t in (a, b, c, total))


def test_nonzero_literal_floor_division_is_total_for_exact_ints(analyze, nodes):
    dag = analyze('''
def half(x):
    y=x//2
    return y

a=half(11)
b=half(22)
c=a+b
''')
    a, b, c = nodes(dag)
    assert a.certainty == b.certainty == c.certainty == Certainty.CERTAIN
    assert a.dependencies == b.dependencies == frozenset()
    assert c.dependencies == {a.id, b.id}


def test_variable_floor_divisor_keeps_historical_completion_safety(analyze, nodes):
    dag = analyze('''
def f(x):
    y=1//x
    return "text"

a=f(1)
b=a+1
''')
    a, b = nodes(dag)
    assert a.certainty == Certainty.CONSERVATIVE
    assert a.effect == EffectKind.NAMESPACE
    assert b.certainty == Certainty.CONSERVATIVE
    assert a.id in b.dependencies


def test_zero_literal_floor_division_is_not_falsely_proved(analyze, nodes):
    dag = analyze('''
def f(x):
    y=x//0
    return y

a=f(10)
b=1
''')
    a, b = nodes(dag)
    assert a.certainty == Certainty.CONSERVATIVE
    assert a.id in b.dependencies
