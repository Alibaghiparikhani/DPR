# Execution Contract

This package lowers the existing `dag_runtime` DAG into typed execution
manifests. It does not execute submitted code. The authoritative 275-test DAG
implementation, its tests, examples, and `DAG_REPORT.md` are unchanged.

## Quick Start

Run from the supplied `src/` directory, with Python 3.12 and pytest installed:

```bash
python -m pip install -r dag_runtime/requirements-dev.txt
python -m pytest -q
python -m pytest -q execution/tests
```

```python
from dag_runtime.dag_engine import analyze_file
from execution import lower_dag

dag = analyze_file("dag_runtime/examples/diamond.py")
plan = lower_dag(
    dag,
    environment_id="my-pinned-python-build-and-dependency-lock-v1",
    package_id="my-submitted-project-revision",  # optional
)
plan.validate()
plan.validate_against(dag)

for manifest in plan.tasks:
    print(manifest.task_id, manifest.mode.value, manifest.task.source)
    print("inputs:", [(v.id, v.kind.value) for v in manifest.inputs])
    print("outputs:", [v.id for v in manifest.outputs])
    print("state:", [v.id for v in manifest.state_inputs])
    print("objects:", manifest.objects)
    print("definitions:", manifest.code.definition_ids)
    for edge in manifest.prerequisites:
        print(edge.source, "->", edge.target, edge.reasons)
```

An environment ID is an explicit compatibility requirement supplied by the
caller. In a deployed system it should identify a pinned Python
implementation/version/build and dependency environment. It is not inferred
from this analyzer's host and is not an assertion that any worker has it.
Likewise, `package_id` is a reference, not a package builder or dependency
resolver. Neither ID contains locations or makes network requests.

## Architecture

| File | Responsibility |
| --- | --- |
| `model.py` | Frozen execution records, requirement classification and validation |
| `lowering.py` | One-for-one DAG-to-manifest projection, preserving source order and indexes |
| `__init__.py` | Public API exports |
| `tests/` | Semantic preservation, negative validation and result-contract tests |

There are no additional runtime dependencies. Production code imports
`dag_runtime.dag_model`, never `dag_engine`, `dag_static`, or Graphviz.
There is no second AST analyzer, alias analysis, topological algorithm, or
readiness implementation. Original frozen `TaskNode`, `Value`, `Definition`,
`BindingEvent` and `DependencyEdge` records are retained, not approximated by
replacement dictionaries.

| Record/Enum | Why It Exists |
| --- | --- |
| `ProgramIdentity` | Binds exact source content and filename to required environment/package identities |
| `CodeRequirement` | References that original program and the exact local definition IDs consumed by a task |
| `ValueRequirement` / `ValueKind` | Distinguishes data, code, native references, discarded expressions and three kinds of state token |
| `ObjectRequirement` / `ObjectAccess` | Records known shared-reference/version obligations without asserting arbitrary objects are disjoint |
| `TaskManifest` / `ExecutionMode` | Describes one DAG computation and preserves its conditional isolation or native/shared restriction |
| `ExecutionPlan` | Holds an immutable, validated module contract and lookup indexes |
| `AttemptIdentity` | Correlates a result with its plan, run, task and caller-assigned attempt |
| `TaskSuccess` | Describes successful computation and its logical output acknowledgments, not completed binding commits |
| `TaskFailure`, `FailureInfo` / `FailureKind` | Describe Python or preparation/execution failures without storing live exceptions or managing retries |

## Code Is a Requirement, Not Generated Worker Code

`manifest.task` retains source, span, AST kind, callable representation,
characteristics, proof, effect, certainty, namespace epoch, region scope and
statement count. `manifest.code` points to the submitted program and exact
definition versions. A callable's display name is never used as its identity.
Aliased/redefined functions keep their distinct `definition_id` values.
Builtin bindings also remain explicit code-valued inputs, including alias chains.

`plan.source` retains the complete original module. A future adapter must use
its compilation context, including future imports, filename, native namespace,
and definition setup order. It must not blindly `exec(task.source, {})`, execute
the entire module again to retrieve a function, or run every definition up
front. Native tails must not replay their already-executed prefixes.

This version produces the requested **code reference and execution contract**,
not bytecode, transformed ASTs, compiled call wrappers, or RPC payloads. When a
later adapter cannot preserve a native scope, the existing DAG report's safe
route remains original-module execution in one native context. This package
does not invent a way to pause/reconstruct Python frames.

Program identity hashes the exact UTF-8 encoding of `dag.source` (the decoded
source string, not original encoded file bytes), plus filename and environment/
package identities. `dag_digest` fingerprints the full canonical DAG export,
including values, edges/reasons, setup, assumptions and analysis outcomes.
`plan.id` binds both identities to execution schema 1. Consequently:

- Equal source, filename, environment and analysis records give equal plans.
- Changed source/environment/package gives a different identity.
- Different analysis-budget outcomes also give different plan identities even
  when source is identical.
- Task/value IDs are scoped by the plan; `T000001` alone is not a global identity.

These digests detect accidental mismatches, not forged proof records. The
trusted input is an analyzed DAG. This is not a loader for untrusted serialized
graphs, code authentication system, or environment verifier.

## Isolation, Values and State

`ISOLATED_CANDIDATE` is carried forward only from the DAG's matching placement,
with consistent pure/certain/non-raising metadata and no native-reference inputs
or state writes. No new type inference or remote-safety proof is performed.
`CERTAIN` alone never enables it. The original bounded proof and all
`plan.assumptions` still apply.

Candidates require a future code adapter and appropriately prepared versioned
inputs. This is not an unconditional `remote_executable=True` or a dispatch
decision. Proved list input readers get `SNAPSHOT_CANDIDATE`, not permission for
arbitrary deep/shallow copies. Snapshots must be obtained only after all
dependencies/state requirements are satisfied, and must preserve the original
alias/binding contract at result commitment. Reader-before-writer edges remain
in force even if a future adapter could potentially release them earlier.

`SHARED_CONTEXT` covers native/shared computations, including exact append and
exception-only expressions. `NATIVE_REGION` covers the DAG's statement, tail or
module regions. Their code must retain the original native context. Shared
placement does not itself add ordering edges: unrelated branches retain the
original DAG's legal computation order.

| Requirement | Interpretation |
| --- | --- |
| `IMMUTABLE` | Logical versioned value, not a mutable module-name slot |
| `SHARED_REFERENCE` | Established reference group whose identity/state must survive |
| `CODE_BINDING` | Definition or builtin binding; aliases retain their source value ID |
| `NATIVE_REFERENCE` | Native lookup/view or native-owned result, not an eager object capture |
| `NAMESPACE_STATE` | Namespace-effect context token (`@namespace`) |
| `COMPLETION_STATE` | Successful completion fence without namespace invalidation (`@completion`) |
| `OBJECT_STATE` | Version token for a known mutated object, independent of its reference value |
| `DISCARDED_RESULT` | Expression result is discarded; do not invent an `@discard` namespace lookup |

All task input/output IDs are preserved, including state tokens. State views
are separately discoverable through `state_inputs` and `state_outputs`.
The complete original prerequisite edges remain authoritative, including
valueless STATE/WAR edges and ORDER fences. No transitive closure or redundant
barrier edges are added. `task.dependencies` and `task.dependents` are unchanged.

Native inputs may be absent or conditional. Do not wait indefinitely for a
producerless external name, eagerly evaluate an unselected branch's lookup, or
raise an error before native Python would read it. Do not retain extra strong
references to unknown objects: doing so can change finalizer timing. Native
state requirements are not requests to serialize a globals dictionary.

Object requirements group only established shared-reference inputs and explicit
object-state tokens. Unknown native references stay native and may alias
anything permitted by the DAG contract. Different `object_id` values alone do
not imply disjointness. A mutator requires its live reference and produced
state token; readers keep the input state version already recorded by the DAG.
An empty object `state_inputs` tuple is not permission to use arbitrary current
state: producer and prerequisite constraints still apply.
Old `Value.type_hint` strings are descriptive evidence, not a fresh proof of
the contents after mutation. The current object-state requirement remains
authoritative; lowering does not perform new inference from those strings.

## Binding and Result Semantics

`plan.bindings` is the original source-ordered `BindingEvent` tuple. It includes
plain definition setup and aliases without computation tasks, assignments,
repeated targets, and opaque-region binding views. It must not be reordered
according to whichever remote computation finishes first. Region events are
descriptions of native binding behavior, not instructions to execute those
assignments again after the region.

For example, `x,x=(1,2)` has two output IDs and ordered projections, not a
dictionary that overwrites the first output. Starred/nested projections remain
descriptive Python unpacking requirements; this package does not reinterpret
them as arbitrary indexing instructions.

Another important example is:

```python
x = 1
q = 1 // 0
b = x
```

A plain immutable alias does not create another physical immutable payload. The
`ExecutionPlan.immutable_representation_id()` resolver follows only immutable alias chains
that preserve the same object/projection identity. Shared references, object-state versions,
native references, code bindings, and state tokens are never collapsed by this resolver.
Logical binding/readiness still uses the alias value ID and source-ordered `BindingEvent`s.

The alias value for `b` belongs to x's original producer, but the binding event
occurs **after** the division. Completing x cannot commit b early, nor can x
wait for that later alias commit before enabling q. The manifest retains the
alias derivation, but excludes it from `reported_output_ids`. Definition/setup
events likewise retain their original position instead of becoming fake tasks.

`TaskSuccess.output_ids` acknowledges exactly `reported_output_ids`: all declared
outputs other than derived aliases and discarded results. State and native-view
IDs are included as logical acknowledgments, not Python payloads. A possibly
unbound native view remains a view; success does not assert that every potential
binding exists or eagerly look it up. Output acknowledgment order is irrelevant;
binding/target order is retained separately in the plan.

```python
from execution import AttemptIdentity, TaskSuccess

task = plan.tasks[0]
attempt = AttemptIdentity(plan.id, "run-1", task.task_id, "attempt-1")
report = TaskSuccess(attempt, task.reported_output_ids)
plan.validate_result(report, expected_attempt=attempt)
```

This example validates a contract record; it does not execute the task or
certify that the reported work actually happened. The future caller must
perform required commits before telling DAG readiness that the task completed.
No execution API calls `mark_completed`. `TaskFailure` cannot supply successful
outputs, advance readiness, imply rollback, or authorize retry. Native failures
may leave partial mutations or bindings. Python exception descriptions use
qualified type names and optional traceback **text**, never live frame objects.
Infrastructure/preparation failures have separate enum categories. Attempt IDs
are caller-owned; validation is stateless and does not deduplicate reports or
manage a run/attempt lifecycle.

## Difficult Cases and Invariants

| Existing Case | Preserved Execution Meaning |
| --- | --- |
| Pure diamond | Same roots, sibling candidates, exact input IDs and join edges |
| Alias + list append | One live alias group, reader WAR edges, writer state output, later reader's required version |
| Exact integer division | Shared may-raise computation with a completion fence; no namespace epoch invented |
| Generic getattr | Namespace barrier and unknown native views; later proved scalar expressions remain sibling candidates |
| globals/exec/wildcard import | Complete original native tail, with no remote eligibility or statement expansion |
| Async/generator/budget fallback | One native module manifest and native final state |
| Conditional output | A possibly absent native binding view, never a fabricated available data blob |
| Imported library call | Unknown native behavior and original prerequisites, without a library whitelist |

Plan construction validates automatically. `validate()` can be called again;
`validate_against(dag)` additionally checks the supplied analysis identity.
Graph validation delegates duplicates, missing references, output ownership,
alias consistency/cycles, dependency/dependent indexes, state outputs and
topology to `DAG`. Execution checks source/program identity, full DAG provenance,
exact manifest inputs/outputs, code definitions, full edge reasons, supported
value/placement classifications and candidate consistency. Input lists and
maps owned by callers are copied; public indexes are read-only. Invalid source
diagnostic DAGs are rejected, while runnable native fallback DAGs are accepted.

The final audit checked that no edge was added/removed, no certainty was raised,
no native/mutating/may-raise node was promoted, and no namespace token was
flattened into transport data. It also checked alias commitment across failure,
unbound native views, definition rebinding, UTF-8 source columns, future-import
context, conservative input lifetime, and package-only imports. Existing
adversarial DAG tests remain unchanged. Manifest checks do not re-prove arbitrary
Python or compensate for a maliciously forged DAG.

## Complexity and Deliberate Limits

Lowering is output-sensitive: approximately O(T + V + E + R + S) time and
space, where R counts input/output incidences and edge reasons, and S includes
retained source/definition/record text. Original frozen records are reused.
A few full graph passes perform validation and canonical hashing; there is no
whole-AST reanalysis, points-to search, recursive proof, or per-task whole-graph
scan. Task/value/definition indexes provide O(1) lookup. Result validation costs
the number of reported outputs. There is no dynamic readiness state in a plan.

Deliberately absent: code generation/executor, a wire serialization format,
object payloads/locations/transfers, package installation, worker selection,
networking, multiprocessing, retries, rollback, cancellation, resource monitoring,
and persistent runtime state. Native scope/frame continuation and source-order
commit mechanics remain obligations for the future execution adapter/coordinator,
not invented capabilities of this contract layer.

## Verification

Verified with Python 3.12.3 and pytest 9.1.1:

- Baseline before implementation: **275 passed**.
- Execution suite: **138 passed**.
- Combined suite: **413 passed**.
- All 26 original non-generated project files match the supplied ZIP byte for
  byte; no DAG source, tests, examples or report were modified.
- Separate-process plan IDs and object requirements matched with hash seeds
  `1` and `932`.
- Three lowering-only samples on already analyzed chains gave medians of
  **0.108 s** for 1,000 tasks and **0.596 s** for 5,000 tasks. These include plan
  validation and hashing, not analyzed-program execution, and are environment-
  specific observations rather than timing guarantees.

The combined test configuration is `src/pytest.ini`. The original DAG-only
test configuration remains intact. The new suite covers chains, diamonds,
multiple/repeated/unpacked outputs, alias/setup events, definition versions,
object-state RAW/WAR/WAW requirements, may-raise fences, namespace recovery and
escape, native fallbacks, source/context fidelity, malformed contracts,
deterministic identity, imports, failures, and exact result correlation.
