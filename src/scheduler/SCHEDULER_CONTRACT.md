# Scheduler contract

This revision adds a deterministic worker-placement engine to the supplied
repository. The coordinator remains control-only: user computation can be
placed only on explicitly enrolled workers. With no valid worker, a task stays
unplaced. Neither this package nor the coordinator runs user computation.

The input ZIP's **416 tests passed before changes**. All 36 non-generated files
under `dag_runtime/` and `execution/` were compared with that ZIP and remain
byte-for-byte unchanged, including their tests and historical reports. The only
modified existing file is the root `pytest.ini`, which adds scheduler test
discovery. This document describes the new subsystem, not a DAG revision.

## Running and importing

From the supplied `src/` directory, using the existing Python 3.12 environment:

```bash
python -m pytest -q
python -m pytest -q scheduler/tests
python -m scheduler.examples.placement
python -m scheduler.benchmarks.measure --tasks 1000 5000 --repeats 5
```

```python
from scheduler import Scheduler, schedule

# plan is the existing execution.ExecutionPlan.
# snapshot contains coherent, coordinator-owned facts for this plan/run.
decision = schedule(plan, snapshot)

# Reuse immutable structural preprocessing for repeated snapshots:
placement_logic = Scheduler(plan)
decision = placement_logic.schedule(snapshot)
```

`Scheduler` stores only a fixed plan, policy, structural metadata and input
requirement indexes. It stores no worker state, readiness, previous decision,
clock, random generator, reservation or attempt counter. A call-local index and
shadow capacities are discarded after each decision.

| File | Responsibility |
| --- | --- |
| `model.py` | Frozen coordinator facts, validation and typed proposals |
| `scheduler.py` | Structural preprocessing, hard eligibility, ranking and matching |
| `__init__.py` | Package public API |
| `tests/` | Legality, semantics, determinism and cross-layer verification |
| `examples/placement.py` | Runnable three-worker decision example; no task execution |
| `benchmarks/measure.py` | Synthetic scheduler-cost measurement, outside production |

There are no new third-party production dependencies. Benchmark/test utilities
may measure time or launch separate test interpreters; scheduler production
does neither. No networking, dispatcher, task executor, object store, coordinator
loop or empty future subsystem is included.

## What the scheduler trusts and preserves

The scheduler consumes `ExecutionPlan` and its original `TaskNode`, dependency
and value records. It does not inspect ASTs, reproduce the bounded proof,
reclassify effects, infer alias disjointness, or promote an execution mode.
Certainty is never treated as remote eligibility.

The execution plan has no public topological-order method. For structural
preprocessing, the scheduler makes a temporary `DAG` view of the plan's **same
original task/value/edge/definition records**, using the existing DAG validator
and topological-order implementation. It infers no edges and retains no second
semantic graph. There is no second readiness algorithm.

The coordinator supplies READY tasks after the original DAG prerequisites,
successful completion fences and required binding/state commits are satisfied.
The scheduler does not verify readiness by reconstructing completed ancestors.
It rejects explicit contradictory lifecycle facts, but an incorrectly certified
READY list cannot be repaired by this layer. Tests use the existing
`DAG.new_readiness()` in their coordinator simulation.

Original source-order `BindingEvent` semantics, namespace epochs, alias identity,
native frame/scope requirements and exception ordering remain obligations of
the future worker adapter and coordinator. A placement does not authorize
`exec(task.source, {})`, eager capture of native references, replay of a native
tail's prefix, or early result/binding commitment.

## Snapshot and proposal records

| Record or enum | Purpose |
| --- | --- |
| `ClusterSnapshot` | One coherent plan/run/snapshot identity plus current facts |
| `ReadyTask` | Coordinator-authorized task, ready sequence and waiting rounds |
| `WorkerState` | Enrolled worker status, slots, CPU/RAM, cores and explicit compatibility |
| `DataLocation`, `DataForm` | Prepared immutable value or certified object snapshot and optional size |
| `Replica`, `ReplicaStatus` | An AVAILABLE or IN_FLIGHT representation at an enrolled worker |
| `TaskCommitment`, `CommitmentPhase` | Existing attempt and RESERVED, DISPATCHED, WAITING_TRANSFER or RUNNING placement |
| `TaskAffinity` | Optional required worker, allowed worker set and exact context ID |
| `WorkerContext` | Context owner, tasks prepared there and currently available entry capacity |
| `SchedulerPolicy` | Fairness threshold and bounded exact-descendant storage budget |
| `StructuralPriority`, `TaskPriority` | Unweighted graph importance and actual task-selection facts |
| `Locality`, `WorkerPreference` | Input residency/uncertainty and current selection facts |
| `Placement` | Proposed task/worker/context with selection facts |
| `UnplacedTask`, `WorkerRejection`, `UnplacedReason` | Typed failure summary and each worker's rejection facts |
| `SchedulingDecision` | Scoped immutable proposal containing placements and unplaced READY tasks |

All task/value/context references are scoped by `plan_id` **and** `run_id` on
the snapshot. `snapshot_id` identifies the observation. Task IDs alone are not
global IDs. Attempt records reuse the existing execution `AttemptIdentity` and
must agree with both plan and run. The scheduler creates no attempt IDs.

`workers` accepts only `WorkerState` records. There is no coordinator hardware
field, role switch or fallback target. Enrollment of actual worker devices is
the coordinator's responsibility; an arbitrary string ID cannot independently
prove a physical device's role, just as this static component cannot verify
reported installations or CPU readings.

Worker running/reserved counters include commitments from **all** plans. The
snapshot's explicit task commitments are scoped to this plan/run and may cover
only part of those aggregate counters. Their counts must fit the reported
running/reserved occupancy. Non-running commitments, including waiting for
transfer, occupy reserved slots. Do not subtract these entries a second time.

`blocked_task_ids` and `completed_task_ids` are optional known lifecycle facts,
useful for contradiction validation. Unlisted tasks never become READY merely
because they are absent from those sets. A READY/committed overlap is corrupt
input and raises `SnapshotValidationError`; committed tasks alone produce no
new proposal.

## Compatibility and native context

A worker must advertise the exact `plan.program.environment_id`, exact
`manifest.code.program_id` in `prepared_program_ids`, and the manifest's
`ExecutionMode` in `supported_modes`. The program identity already binds source,
filename, environment and package identity. Compatible environment alone is
insufficient; package names and familiar libraries are not whitelisted.

Advertising a supported mode is an explicit assertion that a future compliant
adapter exists. Advertising a prepared program includes its code/definition
artifacts and compilation context; it is not a request to run the module to
retrieve a function. These sets default empty, so missing preparation fails
closed. The scheduler verifies supplied keys, never installs or ships code.

`ISOLATED_CANDIDATE` can use eligible workers with correctly prepared logical
inputs. Optional affinity/context restrictions are still enforced. State-token
prerequisites remain satisfied coordinator facts, not a request to copy a
namespace onto the selected worker.

`SHARED_CONTEXT` and `NATIVE_REGION` additionally require:

1. Task affinity naming an exact context.
2. A known worker owning that context.
3. This exact task ID in the context's `prepared_task_ids`.
4. A free worker slot and a free context entry.

A mere worker pin or a context prepared for a different task is insufficient.
Preparation certifies this task's original scope, definitions, live references,
aliases, object-state versions and exception behavior. Native input resolution
belongs to that prepared context: a name absent in an unselected branch is not
a missing transferable payload. Native tasks do not add artificial input-copy
requirements or transfer byte estimates.

`WorkerContext.available_slots` is a current coordinator fact **after** existing
active/reserved context use. Its default is one, suitable for non-reentrant
native entry. More entries require an adapter that actually supports them.
The scheduler shadows context capacity as well as worker slots. Two independent
list mutations may share a worker yet require separate decisions if their one
native context admits only one entry. This is an execution-context constraint;
the DAG's independence remains unchanged.

An unavailable context is unplaced, including when all user code must remain
inside a native module. Context creation/preparation, when eventually implemented,
must happen on workers. It is not coordinator computation.

## Data, aliases and state versions

For isolated candidates, only these existing kinds enter locality accounting:

| Execution input kind | Scheduler requirement |
| --- | --- |
| `IMMUTABLE` | Matching `DataLocation` with `IMMUTABLE_VALUE` form |
| `SHARED_REFERENCE` | Matching `OBJECT_SNAPSHOT`, under existing `SNAPSHOT_CANDIDATE` object access |
| `CODE_BINDING` | Correct prepared program/definition artifacts; not payload locality |
| `NAMESPACE_STATE`, `COMPLETION_STATE`, `OBJECT_STATE` | Original semantic prerequisite/version; never transferable bytes |
| `NATIVE_REFERENCE` | Native context resolution, never arbitrary serialization |
| `DISCARDED_RESULT` | Not a retained transferable input representation |

Locations for code, native references, discarded results or tokens are rejected.
Unknown future input kinds fail closed. The scheduler does not infer transfer
safety from `Value.type_hint` or invent locations for producerless native names.

An object snapshot is a **coordinator-certified prepared representation**, not a
permission to shallow/deep-copy any live Python object. Certification includes
the execution contract's alias, lifetime and commitment obligations. Location
metadata must refer to exactly `(value_id, object_state_id)`. `None` means the
initial version established by the producer and prerequisite order; it is not
the current object state by default.

Example: after `a.append(3)`, an alias reader still consumes the original alias
binding ID but requires the append's object-state token. An older snapshot of
that same alias does not satisfy it. A current snapshot cannot stand in for an
earlier version either. An unrelated object's state token is invalid metadata.
If a future manifest requires several simultaneous versions for one input in a
way this contract cannot represent, that input remains unavailable.

Aliases retain their logical input IDs in locality and diagnostic results. For
immutable plain aliases only, location lookup uses the execution plan's certified
backing representation ID because the alias binding does not create new payload
bytes. Shared references and object-state snapshots keep their exact logical and
versioned keys; the scheduler does not infer object alias sharing. Source-ordered
alias binding/readiness remains separate from data residency. Known transfer bytes
are still accounted per logical input; no object-ID deduplication is inferred.

Only AVAILABLE replicas on online workers are usable sources. A worker that is
not accepting new computation may still hold a usable source. IN_FLIGHT is
never local availability; with another online AVAILABLE source, the destination
still requires a remote input. With no usable source anywhere, the pair is
ineligible. Actual routes, serialization, transfer bandwidth and transfer
scheduling are outside this model.

## Eligibility, ordering and matching

Hard eligibility checks precede every preference. Offline/not-accepting workers,
no free slot, unsupported mode, missing program/environment, affinity/context
failure or missing input representation cannot be compensated by a score.

Task order is lexicographic:

1. Tasks waiting at least eight supplied scheduling rounds are promoted to FIFO
   precedence using `ready_sequence`, then waiting rounds.
2. Other tasks prioritize fewer currently feasible workers.
3. Greater downstream edge depth, then more distinct descendants when counted,
   then more immediate dependents.
4. Earlier ready sequence, waiting rounds and finally task ID.

Promoted tasks use scarcity and structure only after their FIFO fields. Fairness
therefore overrides structural importance once the bounded wait threshold is
reached. The coordinator must preserve waiting rounds and monotonic ready order;
identical repeated snapshots do not secretly age. Under continuing legal
capacity and finite earlier arrivals, a continually offered eligible task cannot
be bypassed forever by newer structurally important tasks. No policy can promise
progress when its required worker/context/data never becomes available.

Depth and counts include existing DATA, ORDER and STATE edges equally. Counts
are logical DAG nodes, not hidden loop iterations or timing estimates. Exact
descendant bitsets count a diamond's shared descendant once. They have a default
8 MiB conservative storage-estimate budget. Above that bound, **all** exact
counts are `None` and that tie-break is omitted; depth and immediate dependents
remain available. No approximate count is presented as exact.

Worker order is lexicographic:

1. All required transferable inputs local.
2. Fewer unknown-size remote inputs.
3. Fewer known remote bytes.
4. Fewer remote logical input IDs.
5. More currently free slots, including same-call shadow consumption.
6. Lower RAM pressure band, then lower CPU pressure band (10-percentage-point bands).
7. More currently available RAM bytes, then more CPU cores as a weak static tie-break.
8. Worker ID.

Unknown transfers are not free. This deliberately prefers a known large transfer
over an unknown transfer when earlier criteria tie; it makes no claim that the
known transfer will finish sooner. CPU 41% and 42% use the same band. Band
boundaries can still break a tie, but load never overrides earlier locality.
There is no giant weighted score, task RAM/CPU estimate or runtime prediction.

The matcher evaluates ready-task/worker pairs once, puts task priorities in a
heap, chooses a task and its best eligible worker, then decreases local worker
and context capacity. On exhaustion, only affected feasible pairs are removed
and their task priorities refreshed. Stale heap entries are ignored. Each pair
can be removed at most once, including when worker and context fill together.
Input snapshots are never mutated.

This preserves W1 for a task that can use only W1 before assigning a flexible
sibling to W2, unless explicit FIFO fairness promotion takes precedence. Once
W1 fills, a W1/W2 task immediately becomes constrained to W2. It does not retain
its original two-worker scarcity count.

An idle remote worker is used when the local worker has no legal capacity. The
scheduler does not wait for perfect locality or forecast when a worker will
finish. This small greedy matcher is not a global optimum solver; matching
optimality across complex constraints is deliberately not claimed.

## Failures, proposals and stale observations

Invalid metadata raises `SnapshotValidationError` before placement: duplicate
records, foreign plan/run IDs, unknown tasks/values/workers, impossible slots,
non-finite/out-of-range CPU, negative/excess memory, conflicting lifecycle or
affinity facts, wrong object-state versions, and payload locations for tokens.
An unknown required worker is an invalid reference; a known offline worker is
a valid observation that yields unplaced work. An as-yet absent named context
also yields unplaced work.

An unplaced task includes typed worker rejections. A uniform single cause is
reported directly; mixed failures use `NO_ELIGIBLE_WORKER` while retaining every
worker's actual causes. Specific reasons are `OFFLINE`, `NOT_ACCEPTING`,
`NO_CAPACITY`, `ENVIRONMENT_MISMATCH`, `PROGRAM_UNAVAILABLE`, `MODE_UNSUPPORTED`,
`AFFINITY_MISMATCH`, `CONTEXT_UNAVAILABLE`, and `INPUT_UNAVAILABLE`. Missing input
IDs are retained. Human text is not authoritative state.

Every READY task appears exactly once in placements or unplaced results.
Other tasks do not appear. A decision preserves plan, run and snapshot identity.
It does not mark tasks reserved, dispatched, running or completed. Identical
snapshots intentionally yield identical proposals; that is not permission to
dispatch both results. The coordinator must atomically revalidate and reserve
before dispatch, then include commitments/counters in subsequent snapshots.

If W2 disconnects after a snapshot, the old proposal stays a proposal for that
old snapshot. The coordinator rejects/revalidates it and supplies a new snapshot.
No scheduler locks, timeouts, worker checks or retry lifecycle are introduced.
Explicit worker-observation TTLs and route freshness are not modeled in v1;
snapshot coherence and current AVAILABLE facts are coordinator obligations.

## Three heterogeneous workers

The runnable example supplies three READY siblings, after their input-producing
tasks have completed. Representation sizes below are illustrative coordinator
facts; they are not inferred from source syntax or interpreted as task RAM.

| Worker | Total / running / reserved slots | CPU | RAM available / total | Cores | Local input |
| --- | --- | --- | --- | --- | --- |
| W1 | 4 / 2 / 1 | 70% | 2 / 16 GiB | 8 | u, 128 bytes |
| W2 | 2 / 0 / 0 | 20% | 6 / 8 GiB | 4 | v, 64 bytes |
| W3 | 1 / 0 / 0 | 45% | 3 / 4 GiB | 2 | w, size unknown |

| Task | Computation | Proposed worker | Reason |
| --- | --- | --- | --- |
| T000004 | `a = u + 1` | W1 | u is local and W1 still has one free slot |
| T000005 | `b = v + 1` | W2 | v is local, with two free slots before this proposal |
| T000006 | `c = w + 1` | W3 | w is local; unknown size does not imply a free remote transfer |

W1's higher CPU pressure cannot cancel its locality advantage while a legal slot
exists. All proposals target workers. The example prints decisions only.

## Complexity and measurement

Let T/V/E describe the existing plan, R be READY tasks, W workers, L replica
records, and I the number of transferable input incidences across READY tasks.
Basic structural preprocessing delegates validation/topology and does a reverse
pass: approximately O(T+V+E+all input incidences). When enabled, exact descendant
bitsets add up to O(E*T/b) machine-word work and O(T*T/b) bitset words, bounded
by the stated budget; masks are released after counts are computed. For large
plans, counts are omitted and the linear depth pass remains.

Per snapshot, validation/indexing costs its fact volume, pair evaluation costs
O(W*(R+I)), and matching costs roughly O(R*W*log(R*W) + R*W). There is no
per-pair graph traversal. Pair explanations retain O(W*(R+I)) data in the worst
case. The design favors the initial small cluster, approximately three workers.

Measured on Python 3.12.14, Linux x86-64, five samples per case:

| Independent READY roots / workers | Median preprocessing | Median scheduling |
| --- | --- | --- |
| 1,000 / 3 | 0.00452 s | 0.03110 s |
| 5,000 / 3 | 0.02804 s | 0.20518 s |

The fixture has no transferable inputs and enough total slots. Measurements
exclude AST analysis, execution lowering and submitted task execution; they
include scheduler validation and proposal construction. They are observations
for this environment, not general latency guarantees or runtime profiles.
`benchmarks/results.json` retains the measured numbers and fixture description.

## Verification and final audit

The scheduler suite contains **169 passing tests**, with **585 repository tests
passing** in total (416 unchanged baseline tests). `verification.txt` retains
the complete pytest summary. Coverage includes 400 randomized scalar snapshots,
125 randomized native-context snapshots, three distinct hash-seed processes,
and full scheduling waves for the real diamond, stress and full-stack torture
fixtures. Integration simulations advance original DAG readiness outside the
scheduler and do not execute submitted computations.

| Threat | Check or preserved obligation |
| --- | --- |
| Duplicate dispatch / already running | Explicit commitments excluded; READY overlap rejected; coordinator atomically accepts proposals |
| Worker overcommit / ignored reservations | Aggregate counters and same-call shadow worker slots; represented commitments must fit counters |
| Native context overcommit | Current context entry capacity plus same-call shadow consumption |
| Stale snapshot / worker loss | Decision preserves snapshot/run/plan identity; new offline snapshot cannot place there |
| Foreign task/plan/run/value | Membership and exact identity validation before pair evaluation |
| State token treated as bytes | Token/native/code locations rejected; pure post-fence siblings require no token payloads |
| Stale shared snapshot / aliases | Exact value + object-state key required; wrong object/version and missing alias representation fail closed |
| Shared/native placed arbitrarily | Exact prepared context and its owner required, even for CERTAIN mutation |
| Coordinator used as worker | Worker-only typed inventory and no fallback/execution path |
| In-flight/offline replica considered available | AVAILABLE status and online source required |
| Fake CPU/RAM/runtime prediction | Only reported worker facts, logical input sizes and unweighted structure used |
| Unknown size treated as zero | Explicit None and separate unknown-remote-input count precede byte ranking |
| Greedy choice blocks constrained task | Feasible-worker priority refreshed as capacity disappears |
| Starvation | Supplied waiting-round threshold promotes FIFO over structural priority |
| Waiting forever for locality | Any presently legal remote candidate may be selected |
| Hash/set-order nondeterminism | Stable heap keys, worker/task IDs and canonical rejection order; shuffled-input/hash-seed tests |
| Hidden live scheduler state | Reused static Scheduler produces identical results for old/new/old snapshots |
| DAG/plan/readiness mutation | Original records unchanged; analyzer/readiness APIs forbidden during scheduler tests |
| Coordinator/worker work crossing boundary | No production dispatch, execution, transfers, installation, lifecycle or networking imports/code |

Deliberate limits: no global optimal matching, duration estimates, bandwidth
model, task-specific memory admission model, cross-plan fairness, observation
TTL, code/definition preparation, alias artifact deduplication, worker retry
lifecycle, serialization protocol, or native-frame execution adapter. All
preparation/compatibility facts are trusted coordinator attestations, not remote
measurements performed by the scheduler. If the future runtime cannot establish
them safely, it must leave the corresponding mode/context/input unavailable.
