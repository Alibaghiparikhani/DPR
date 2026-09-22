import textwrap

import pytest

from dag_runtime.dag_engine import analyze_source
from execution import lower_dag


@pytest.fixture
def lowered():
    def build(source, **options):
        dag = analyze_source(textwrap.dedent(source).strip() + "\n", **options)
        plan = lower_dag(dag, environment_id="test-cpython312-lock-v1")
        plan.validate_against(dag)
        return dag, plan
    return build
