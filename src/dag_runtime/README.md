# Conservative Python DAG engine

A static-analysis component for a moderate-scope LAN Python runtime. It builds
versioned dataflow and ordering dependencies, explains conservative choices,
validates the graph, and provides incremental readiness bookkeeping. It does
**not** execute analyzed code or implement a scheduler.

Start with **[DAG_REPORT.md](DAG_REPORT.md)** for the analysis and execution
contract. Arbitrary Python cannot always be automatically parallelized.

The precision revision preserves the architecture and adds effect scopes,
exception-only completion fences, exact-list object state, bounded indexing,
list-comprehension proofs, bounded local `if`/`for`/`while` function analysis,
exact built-in float/int numeric propagation, uniquely local exact-list
construction/append proofs, and a narrowly recognized explicit `@task` contract.
Unknown effects still invalidate facts. Explicit
namespace escape retains a native tail. See **Precision and Conservatism** in
the report for the audit and the deliberately retained import/finalizer limits.

## Quick start

Use Python 3.12 (verified with Python 3.12.14). Run commands from this directory.
The engine and visualizer use only the standard library; pytest is a development
dependency. Keep `dag_engine.py`, `dag_model.py`, and `dag_static.py` together on
your Python import path.

```bash
python -m pip install -r requirements-dev.txt
python -m pytest -q

python -m dag_runtime.dag_engine dag_runtime/examples/diamond.py
python -m dag_runtime.dag_engine dag_runtime/examples/diamond.py --json graph.json

python -m dag_runtime.dag_visualizer dag_runtime/examples/diamond.py --dot graph.dot
python -m dag_runtime.dag_visualizer dag_runtime/examples/diamond.py --dot detailed.dot --mode detailed
python -m dag_runtime.dag_visualizer dag_runtime/examples/diamond.py --dot graph.dot --render graph.svg
python -m dag_runtime.dag_visualizer dag_runtime/examples/diamond.py --dot graph.dot --render graph.png

python -m dag_runtime.dag_engine dag_runtime/examples/precision.py --json precision.json
python -m dag_runtime.dag_visualizer dag_runtime/examples/precision.py --dot precision.dot --mode detailed
python benchmarks/measure.py --output benchmarks/local.json --repeats 5
```

Rendering uses the optional Graphviz **`dot` executable**, installed separately.
The Python `graphviz` package is unnecessary. If `dot` is absent, the CLI still
writes DOT and explains why it skipped rendering. Actual renderer failures,
invalid formats, and timeouts remain errors.

Replace `examples/diamond.py` with your own Python file to analyze it. Invalid
Python produces a diagnostic graph and CLI exit code 2; file access/encoding
errors are ordinary I/O errors. A valid graph does not certify a program is
error-free or suitable for arbitrary worker placement.

## Realistic CPU functions and `@task`

Local functions can now be proved through a bounded control-flow subset without
turning loops into DAG nodes. Local loop proofs track fallthrough, `break`, and
`continue` exits separately so unreachable statements and loop `else` suites cannot
be used to prove away ordinary Python exceptions. For example, independent calls to a pure
prime-counting, Collatz-style, numerical-integration, or local-list-processing
function can become sibling isolated candidates automatically. Exact built-in
float arithmetic remains precise through supported `+`, `-`, `*`, comparisons,
`abs`/`min`/`max`, and true division only when a zero divisor and unsafe int-to-float
conversion are ruled out. Scalar `min`/`max` proofs require at least two scalar
arguments; the one-argument iterable form is not mistaken for scalar totality.
`len(range(...))` is exception-free only when the exact range cardinality is known
to fit `sys.maxsize`. Exact integer floor division by a non-zero literal divisor
(such as `x // 2`) remains supported.

A list created inside the analyzed function may be mutated through the narrow
`local_name.append(immutable_value)` subset and iterated later when its element
type remains exact and immutable. Borrowed lists, aliases of the local mutable
list, nested mutable values, other methods, and heterogeneous element types fail
closed. Dict/set mutation and recursion deliberately remain conservative.

Try the realistic precision examples:

```bash
python -m dag_runtime.dag_engine dag_runtime/examples/numerical_integration_workload.py
python -m dag_runtime.dag_engine dag_runtime/examples/local_list_workload.py
```

External/global mutation, unknown method calls, user-defined numeric protocols,
uncertain division, and unsupported control flow still fail closed.

For code that cannot be inferred automatically, the runtime also exposes an
explicit contract marker:

```python
from dag_runtime import task

@task
def expensive_work(x):
    # The programmer promises that observable data flows through args/return
    # and independent calls do not mutate hidden shared state.
    ...
```

Only the marker imported from `dag_runtime` is trusted. A locally defined or
rebound decorator named `task` is treated as an ordinary decorator.

## Programmatic use

```python
from dag_runtime.dag_engine import analyze_source

dag = analyze_source("a=1\nb=a+2\nc=a*3\nd=b+c\n")
print(dag.initial_ready_tasks())
print(dag.topological_order())
print(dag.sinks())
print(dag.metrics())  # structural diagnostics, never runtime estimates

run = dag.new_readiness()  # one independent tracker per run
root = dag.initial_ready_tasks()[0]
newly_ready = run.mark_completed(root)  # only after actual successful completion
print(newly_ready)
print(dag.explain_parallelism(*newly_ready))

for edge in dag.edges.values():
    for reason in edge.reasons:
        print(edge.source, "->", edge.target, reason.kind.value, reason.text)
```

`dag.mark_completed(id)` is also available as a convenience tracker. The caller
owns dispatch/running status, input residency, failures, and source-order binding
commits. Structural readiness alone does not establish those conditions.

## Files

| File | Role |
| --- | --- |
| `dag_engine.py` | Public API, source analysis, barriers, symbol versions, CLI |
| `dag_model.py` | Frozen records, graph indexes/validation, readiness |
| `dag_static.py` | Lexical facts and bounded exact-type/local-function proofs |
| `dag_visualizer.py` | DOT export and optional Graphviz executable adapter |
| `DAG_REPORT.md` | Detailed design, worked examples, limits, integration contract |
| `tests/` | 311 pytest cases, including precision/safety, semantic interleaving, realistic integer/float/local-list control flow, explicit `@task`, readiness, CLI, and final torture tests |
| `examples/diamond.py` | Automatically proved parallel diamond |
| `examples/prototype_program*.py` | Original example programs copied from the ZIP |
| `examples/precision.py` | Safe comprehension branches and object-local append |
| `examples/numerical_integration_workload.py` | Automatically proved float-heavy numerical workload |
| `examples/local_list_workload.py` | Automatically proved uniquely local list build/iterate workload |
| `examples/stress_dag.py` | Manual mixed-feature stress example used during final validation |
| `benchmarks/measure.py` | Reproducible static analyzer benchmark and graph diagnostics |
| `benchmarks/before.json`, `benchmarks/after.json` | Five raw timing samples per workload and representative graph metrics |

JSON exports now use schema version 2, adding task effect/epoch/object metadata
and `final_object_states`. Existing analysis, topology, explanation, and
readiness APIs remain available. Exact mutations require shared placement;
their object-state tokens are execution requirements, not serializable copies.

The original prototype DAG and scheduler scripts were inspected during development but are not
dependencies of this implementation or part of the active source tree. No networking, multiprocessing, profiling,
distributed storage, scheduling algorithm, or Graphviz dependency enters the
runtime engine.
