import textwrap

import pytest

from coordinator import Coordinator, RetryPolicy
from dag_runtime.dag_engine import analyze_source
from execution import lower_dag


@pytest.fixture
def build_plan():
    def build(source):
        dag = analyze_source(textwrap.dedent(source).strip() + "\n")
        plan = lower_dag(dag, environment_id="test-env", package_id="test-package")
        return dag, plan
    return build


@pytest.fixture
def diamond(build_plan):
    # A feeds B/C; both feed D through ordinary scalar data dependencies.
    return build_plan("""
        a = 1
        b = a + 1
        c = a + 2
        d = b + c
    """)


@pytest.fixture
def coordinator():
    return Coordinator(retry_policy=RetryPolicy(max_attempts_per_task=3))
