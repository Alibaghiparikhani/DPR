"""Precision tests for exact built-in floats and uniquely local list mutation.

These tests deliberately pair useful positive cases with near-neighbour cases
that must remain conservative.  The goal is more parallelism without widening
the trust boundary to user protocols, borrowed mutable state, or uncertain
exceptions.
"""
from __future__ import annotations

from dag_runtime.dag_model import Certainty, EffectKind
from execution import ExecutionMode, ValueKind, lower_dag


INTEGRATION_BODY = '''
def integrate(start, end, steps):
    h = (end - start) / steps
    total = 0.0
    for i in range(steps):
        x = start + (i + 0.5) * h
        total += 4.0 / (1.0 + x * x)
    return total * h
'''.strip()


def test_float_heavy_numerical_integration_is_automatically_parallel(analyze, nodes):
    dag = analyze(INTEGRATION_BODY + '''

a=integrate(0.0, 0.33, 1000)
b=integrate(0.33, 0.66, 1000)
c=integrate(0.66, 1.0, 1000)
total=a+b+c
''')
    a, b, c, total = nodes(dag)
    assert dag.initial_ready_tasks() == (a.id, b.id, c.id)
    assert all(t.certainty == Certainty.CERTAIN for t in (a, b, c, total))
    assert all(t.effect == EffectKind.PURE for t in (a, b, c, total))
    assert all(t.placement == 'isolated_candidate' for t in (a, b, c, total))
    assert a.dependencies == b.dependencies == c.dependencies == frozenset()
    assert total.dependencies == {a.id, b.id, c.id}


def test_float_integration_lowers_to_isolated_execution_manifests(analyze):
    dag = analyze(INTEGRATION_BODY + '''

a=integrate(0.0, 0.5, 100)
b=integrate(0.5, 1.0, 100)
total=a+b
''')
    plan = lower_dag(dag, environment_id='cpu-test', package_id='float-integration-v1')
    plan.validate_against(dag)
    assert [m.mode for m in plan.tasks] == [ExecutionMode.ISOLATED_CANDIDATE] * 3
    assert all(m.outputs[0].kind == ValueKind.IMMUTABLE for m in plan.tasks)


def test_float_division_with_possible_zero_divisor_remains_conservative(analyze, nodes):
    dag = analyze('''
def reciprocal(x):
    return 1.0 / x

a=reciprocal(0.0)
b=2
''')
    a, b = nodes(dag)
    assert a.certainty == Certainty.CONSERVATIVE
    assert a.effect == EffectKind.PURE
    assert a.characteristics.may_raise
    assert a.id in b.dependencies


def test_nonzero_float_literal_divisor_is_total(analyze, nodes):
    dag = analyze('''
def scale(x):
    return x / 2.0

a=scale(3.5)
b=scale(7.0)
c=a+b
''')
    a, b, c = nodes(dag)
    assert a.certainty == b.certainty == c.certainty == Certainty.CERTAIN
    assert a.dependencies == b.dependencies == frozenset()
    assert c.dependencies == {a.id, b.id}


def test_huge_int_mixed_with_float_is_not_assumed_conversion_safe(analyze, nodes):
    huge = '1' + '0' * 400
    dag = analyze(f'''
def mix(x):
    return x + 1.0

a=mix({huge})
b=2
''')
    a, b = nodes(dag)
    assert a.certainty == Certainty.CONSERVATIVE
    assert a.id in b.dependencies


def test_exact_float_comparison_abs_min_max_stay_in_pure_subset(analyze, nodes):
    dag = analyze('''
def score(x):
    y=abs(x)
    low=min(y, 4.0)
    high=max(low, 1.0)
    if high > 2.0:
        high = high * 2.0
    return high

a=score(-3.0)
b=score(0.5)
c=a+b
''')
    a, b, c = nodes(dag)
    assert all(t.certainty == Certainty.CERTAIN for t in (a, b, c))
    assert a.dependencies == b.dependencies == frozenset()
    assert c.dependencies == {a.id, b.id}


LOCAL_LIST_BODY = '''
def work(n):
    values=[]
    for i in range(n):
        values.append(i*i)
    total=0
    for x in values:
        total += x
    return total
'''.strip()


def test_locally_owned_list_build_and_iteration_is_automatically_parallel(analyze, nodes):
    dag = analyze(LOCAL_LIST_BODY + '''

a=work(100)
b=work(200)
c=work(300)
total=a+b+c
''')
    a, b, c, total = nodes(dag)
    assert dag.initial_ready_tasks() == (a.id, b.id, c.id)
    assert all(t.certainty == Certainty.CERTAIN for t in (a, b, c, total))
    assert all(t.placement == 'isolated_candidate' for t in (a, b, c, total))
    assert total.dependencies == {a.id, b.id, c.id}


def test_locally_owned_flat_list_can_be_returned_as_fresh_transferable_value(analyze, nodes):
    dag = analyze('''
def squares(n):
    values=[]
    for i in range(n):
        values.append(i*i)
    return values

a=squares(3)
b=squares(4)
sa=sum(a)
sb=sum(b)
total=sa+sb
''')
    a, b, sa, sb, total = nodes(dag)
    assert a.placement == b.placement == 'isolated_candidate'
    assert a.dependencies == b.dependencies == frozenset()
    assert sa.dependencies == {a.id}
    assert sb.dependencies == {b.id}
    assert total.dependencies == {sa.id, sb.id}
    plan = lower_dag(dag, environment_id='cpu-test', package_id='local-list-v1')
    assert all(m.mode == ExecutionMode.ISOLATED_CANDIDATE for m in plan.tasks)


def test_local_float_list_accumulation_and_sum_is_supported(analyze, nodes):
    dag = analyze('''
def work(n):
    values=[]
    for i in range(n):
        values.append((i + 0.5) * 0.25)
    return sum(values)

a=work(10)
b=work(20)
c=a+b
''')
    a, b, c = nodes(dag)
    assert a.certainty == b.certainty == c.certainty == Certainty.CERTAIN
    assert a.dependencies == b.dependencies == frozenset()
    assert c.dependencies == {a.id, b.id}


def test_borrowed_list_mutation_still_fails_closed(analyze, nodes):
    dag = analyze('''
def work(values, n):
    for i in range(n):
        values.append(i)
    return len(values)

values=[]
a=work(values, 3)
b=2
''')
    values, a, b = nodes(dag)
    assert a.certainty == Certainty.CONSERVATIVE
    assert a.effect == EffectKind.NAMESPACE
    assert a.id in b.dependencies


def test_aliasing_locally_owned_mutable_list_stays_conservative(analyze, nodes):
    dag = analyze('''
def work(n):
    values=[]
    alias=values
    for i in range(n):
        alias.append(i)
    return len(values)

a=work(3)
b=2
''')
    a, b = nodes(dag)
    assert a.certainty == Certainty.CONSERVATIVE
    assert a.id in b.dependencies


def test_appending_mutable_value_to_local_list_is_not_promoted(analyze, nodes):
    dag = analyze('''
def work(n):
    values=[]
    child=[]
    for i in range(n):
        values.append(child)
    return len(values)

a=work(3)
b=2
''')
    a, b = nodes(dag)
    assert a.certainty == Certainty.CONSERVATIVE
    assert a.id in b.dependencies


def test_heterogeneous_local_list_iteration_loses_precision_safely(analyze, nodes):
    dag = analyze('''
def work():
    values=[]
    values.append(1)
    values.append(1.5)
    total=0.0
    for x in values:
        total += x
    return total

a=work()
b=2
''')
    a, b = nodes(dag)
    assert a.certainty == Certainty.CONSERVATIVE
    assert a.id in b.dependencies


def test_local_container_cache_does_not_promote_later_borrowed_alias(analyze, nodes):
    dag = analyze('''
def identity(x):
    return x

fresh=identity([1])
base=[1]
borrowed=identity(base)
borrowed.append(2)
reader=base[-1]
''')
    tasks = {t.source.strip(): t for t in nodes(dag)}
    mutation = tasks['borrowed.append(2)']
    reader = tasks['reader=base[-1]']
    assert mutation.certainty == Certainty.CONSERVATIVE
    assert mutation.effect == EffectKind.NAMESPACE
    assert mutation.id in reader.dependencies


def test_float_specialization_cache_keeps_zero_divisor_facts_separate(analyze, nodes):
    dag = analyze('''
def reciprocal(x):
    return 1.0 / x

safe=reciprocal(2.0)
unsafe=reciprocal(0.0)
after=3
''')
    safe, unsafe, after = nodes(dag)
    assert safe.certainty == Certainty.CERTAIN
    assert unsafe.certainty == Certainty.CONSERVATIVE
    assert unsafe.id in after.dependencies


def test_fresh_list_comprehension_can_remain_locally_owned_for_append(analyze, nodes):
    dag = analyze('''
def work():
    values=[x*x for x in (1,2,3)]
    values.append(16)
    return sum(values)

a=work()
b=work()
c=a+b
''')
    a, b, c = nodes(dag)
    assert a.certainty == b.certainty == c.certainty == Certainty.CERTAIN
    assert a.dependencies == b.dependencies == frozenset()
    assert c.dependencies == {a.id, b.id}


def test_multi_target_assignment_of_fresh_mutable_list_is_not_treated_as_unique(analyze, nodes):
    dag = analyze('''
def work():
    left=right=[]
    left.append(1)
    return sum(right)

a=work()
b=2
''')
    a, b = nodes(dag)
    assert a.certainty == Certainty.CONSERVATIVE
    assert a.id in b.dependencies


def test_range_specialization_cache_keeps_cardinality_for_zero_division(analyze, nodes):
    dag = analyze('''
def f(r):
    return 1 % (len(r) - 6)

safe=f(range(11))
unsafe=f(range(0, 11, 2))
after=2
''')
    safe, unsafe, after = nodes(dag)
    assert safe.certainty == Certainty.CERTAIN
    assert unsafe.certainty == Certainty.CONSERVATIVE
    assert unsafe.characteristics.may_raise
    assert unsafe.id in after.dependencies


def test_range_specialization_cache_keeps_cardinality_for_len_overflow(analyze, nodes):
    dag = analyze('''
def f(r):
    return len(r)

safe=f(range(0, 9223372036854775809, 9223372036854775808))
unsafe=f(range(0, 9223372036854775809))
after=2
''')
    safe, unsafe, after = nodes(dag)
    assert safe.certainty == Certainty.CERTAIN
    assert unsafe.certainty == Certainty.CONSERVATIVE
    assert unsafe.characteristics.may_raise
    assert unsafe.id in after.dependencies


def test_range_specialization_cache_is_order_independent(analyze, nodes):
    dag = analyze('''
def f(r):
    return 1 % (len(r) - 6)

unsafe=f(range(0, 11, 2))
safe=f(range(11))
after=2
''')
    unsafe, safe, after = nodes(dag)
    assert unsafe.certainty == Certainty.CONSERVATIVE
    assert unsafe.characteristics.may_raise
    assert safe.certainty == Certainty.CERTAIN
    assert unsafe.id in safe.dependencies
    assert unsafe.id in after.dependencies


def test_equal_range_element_bounds_with_different_steps_do_not_share_specialization(analyze, nodes):
    dag = analyze('''
def f(r):
    return 1 % (len(r) - 6)

safe=f(range(0, 11))
unsafe=f(range(0, 11, 2))
after=2
''')
    safe, unsafe, after = nodes(dag)
    assert safe.certainty == Certainty.CERTAIN
    assert unsafe.certainty == Certainty.CONSERVATIVE
    assert unsafe.characteristics.may_raise
    assert unsafe.id in after.dependencies


def test_all_bool_minmax_preserves_bool_for_inversion_guard(analyze, nodes):
    dag = analyze('''
x=~min(True, False)
y=2
''')
    x, y = nodes(dag)
    assert x.certainty == Certainty.CONSERVATIVE
    assert x.characteristics.may_raise
    assert x.id in y.dependencies


def test_all_bool_max_preserves_bool_for_inversion_guard(analyze, nodes):
    dag = analyze('''
x=~max(False, True)
y=2
''')
    x, y = nodes(dag)
    assert x.certainty == Certainty.CONSERVATIVE
    assert x.characteristics.may_raise
    assert x.id in y.dependencies


def test_mixed_bool_int_minmax_does_not_claim_exact_int(analyze, nodes):
    for expr in (
        'min(True, 0)',
        'min(0, True)',
        'max(False, 1)',
        'max(1, False)',
        'max(True, 1)',
        'max(1, True)',
    ):
        dag = analyze(f'''\nx=~{expr}\ny=2\n''')
        x, y = nodes(dag)
        assert x.certainty == Certainty.CONSERVATIVE, expr
        assert x.characteristics.may_raise, expr
        assert x.id in y.dependencies, expr


def test_plain_int_and_float_minmax_precision_is_preserved(analyze, nodes):
    dag = analyze('''
a=min(7, 3)
b=max(1, 9)
c=min(7.0, 3.0)
d=max(1.0, 9.0)
total=a+b+c+d
''')
    a, b, c, d, total = nodes(dag)
    assert all(t.certainty == Certainty.CERTAIN for t in (a, b, c, d))
    assert a.dependencies == b.dependencies == c.dependencies == d.dependencies == frozenset()
    # The mixed int/float aggregate is outside this regression's scope; the
    # four builtin calls themselves must retain their existing precision.
    assert total.dependencies == {a.id, b.id, c.id, d.id}
