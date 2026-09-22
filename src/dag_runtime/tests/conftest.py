import textwrap

import pytest

from dag_runtime.dag_engine import analyze_source


@pytest.fixture
def analyze():
    def build(source, **kwargs):
        dag = analyze_source(textwrap.dedent(source).strip()+'\n', **kwargs)
        assert dag.execution_permitted, dag.diagnostics
        dag.validate()
        return dag
    return build


@pytest.fixture
def nodes():
    return lambda dag: list(dag.tasks.values())
