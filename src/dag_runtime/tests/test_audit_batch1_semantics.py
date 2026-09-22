from __future__ import annotations

import sys

import pytest

from dag_runtime.dag_model import Certainty, EffectKind


@pytest.mark.parametrize("body", [
    """
def f():
    x = 0
    for i in range(1):
        break
    else:
        x = 1
    return 1 % x
""",
    """
def f():
    x = 1
    for i in range(1):
        x = 0
        continue
        x = 1
    return 1 % x
""",
    """
def f():
    x = 1
    for i in range(1):
        x = 0
        break
        x = 1
    return 1 % x
""",
    """
def f():
    for i in range(1):
        break
    else:
        x = 1
    return x
""",
])
def test_local_loop_control_cannot_prove_exception_free(body, analyze, nodes):
    dag = analyze(body + "\nr=f()\ny=2")
    call, later = nodes(dag)
    assert call.certainty == Certainty.CONSERVATIVE
    assert call.effect == EffectKind.PURE
    assert call.characteristics.may_raise
    assert call.placement != "isolated_candidate"
    assert call.id in later.dependencies


@pytest.mark.parametrize("expression", ["min(1)", "max(1)", "min(1.0)", "max(1.0)"])
def test_scalar_single_argument_min_max_preserve_typeerror_order(expression, analyze, nodes):
    dag = analyze(f"x={expression}\ny=2")
    call, later = nodes(dag)
    assert call.certainty == Certainty.CONSERVATIVE
    assert call.effect == EffectKind.PURE
    assert call.characteristics.may_raise
    assert call.id in later.dependencies


@pytest.mark.parametrize("expression", ["min(1, 2)", "max(1, 2)", "min(1.0, 2.0)", "max(1.0, 2.0)"])
def test_valid_two_scalar_min_max_remain_precise(expression, analyze, nodes):
    dag = analyze(f"x={expression}\ny=2")
    call, later = nodes(dag)
    assert call.certainty == Certainty.CERTAIN
    assert not call.characteristics.may_raise
    assert call.placement == "isolated_candidate"
    assert call.id not in later.dependencies


def test_len_range_overflow_preserves_exception_order(analyze, nodes):
    dag = analyze(f"x=len(range({sys.maxsize + 1}))\ny=2")
    call, later = nodes(dag)
    assert call.certainty == Certainty.CONSERVATIVE
    assert call.effect == EffectKind.PURE
    assert call.characteristics.may_raise
    assert call.id in later.dependencies


@pytest.mark.parametrize("n", [0, 1, 10, sys.maxsize])
def test_len_range_representable_cardinality_remains_precise(n, analyze, nodes):
    dag = analyze(f"x=len(range({n}))\ny=2")
    call, later = nodes(dag)
    assert call.certainty == Certainty.CERTAIN
    assert not call.characteristics.may_raise
    assert call.placement == "isolated_candidate"
    assert call.id not in later.dependencies


def test_supported_continue_flow_remains_provable(analyze, nodes):
    dag = analyze('''
def f(limit):
    total = 0
    for i in range(limit):
        if i % 2 == 0:
            continue
        total += i
    return total

a=f(10)
b=f(20)
c=a+b
''')
    a, b, c = nodes(dag)
    assert a.certainty == b.certainty == c.certainty == Certainty.CERTAIN
    assert a.placement == b.placement == c.placement == 'isolated_candidate'
    assert a.dependencies == b.dependencies == frozenset()
    assert c.dependencies == {a.id, b.id}
