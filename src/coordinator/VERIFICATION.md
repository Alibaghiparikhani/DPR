# Coordinator verification

Verified on Python 3.13.5 after a behavior-preserving maintainability pass over
`src/coordinator/`.

## Behavioral baseline

Before production refactoring, the exact input ZIP produced:

- Coordinator: **150 passed**
- Complete repository: **1324 passed, 1 failed**

The sole failure was the already-known environment-sensitive execution
import-isolation assertion that expects `socket` to be absent from
`sys.modules` in a fresh subprocess. In this environment it is already loaded.
No production change in this pass attempts to hide that unrelated condition.

The baseline already contained the adversarial corrections for native/shared
retry safety, context loss, session-generation correlation, exact program and
context preparation identity, `DATA_LOST`, immutable inspection, stable context
contracts, bounded retired-correlation history, and idempotent active program
preparation. Those semantics remain regression-tested and unchanged.

## Maintainability changes

### Pending-operation registry

Preparation correlation bookkeeping was extracted from `Coordinator` into the
small `PendingOperationRegistry`. It owns only:

- active typed program/context preparation requests;
- frozen program/context preparation contract equality;
- exact worker-generation correlation resolution;
- idempotent active request indexes;
- completion/session/run retirement;
- bounded oldest-first completed tombstones;
- ABA-safe preparation correlation generation.

It does **not** own worker state, runs, tasks, attempts, contexts, scheduling,
transfers, result commit, or message delivery. `Coordinator` remains the sole
semantic/mutable runtime authority.

The registry now keeps O(1) secondary indexes for exact program contracts and
`(run_id, context_id)`. Its invariant checker verifies that those indexes exactly
match the authoritative active-request map.

`inspect_pending_operations()` exposes only a frozen count/message-ID view; no
mutable registry dictionaries are returned.

### Explicit semantic transitions

Paired task/attempt transitions for dispatch, acceptance, and start now use small
semantic helpers so one record cannot be advanced accidentally without the other.
Validated task success enters one explicit `_commit_task_success()` boundary,
which preserves the existing distinction between worker computation success and
coordinator logical commit.

### Atomic placement staging

`accept_decision()` still has exactly the same scheduling/revalidation behavior,
but all attempt/transfer/control records are now staged before any authoritative
state or outbox mutation. The application phase is separate and short. A focused
regression proves a multi-placement staging failure leaves every task unmodified
and emits no task dispatch.

### Worker-loss readability

The previous 112-line worker-loss handler was decomposed into named semantic
phases while keeping mutation ownership in `Coordinator`: per-run reconciliation,
active-attempt discovery, context-dependent loss determination, isolated retry,
and transfer failure. Native/context/data-loss behavior is unchanged.

### Invariant structure and typing

`validate_state()` now delegates focused checks rather than embedding one dense
second state machine. It additionally verifies the already-required relationship
between each current task and active attempt, including transfer gating,
cancellation, and dispatch-message presence.

All production coordinator function boundaries are annotated. The worker outbox
is typed as `list[protocol.Message]` rather than `list[object]`. Program/context
preparation equality is exposed through frozen contract dataclasses.

## Additional tests added

This pass adds direct tests for the extracted pending subsystem, including:

- ABA-safe correlation generation;
- exact program-contract indexing;
- one-active-operation context identity;
- session-generation-bound resolution;
- bounded tombstones and deterministic eviction;
- session/run invalidation;
- stale-generation invariant detection;
- explicit contract equality;
- immutable pending diagnostic views.

It also adds coordinator regressions for:

- task/attempt active-status invariant mismatches;
- atomic multi-placement staging failure.

All prior adversarial coordinator tests remain in place.

## Static / compile checks

- `python -m compileall`: passes.
- Runtime resolution of production type annotations: passes.
- AST audit: every production coordinator function/method has parameter and
  return annotations.
- AST import-use audit: no unused production imports found in the coordinator
  modules.
- Broad `except Exception` / bare `except` audit: none found in production
  coordinator modules.
- Ruff, MyPy, Pyright, and Bandit are not installed in the supplied environment;
  this pass did not add tool dependencies solely for cleanup.

## Complexity comparison

Line count was deliberately not optimized. Named helpers and explicit contracts
add lines while reducing dense branching.

For `coordinator/coordinator.py`:

| Metric | Baseline | Refactored |
| --- | ---: | ---: |
| Physical LOC | 1657 | 1720 |
| Functions/methods | 80 | 92 |
| Longest method | `_lose_worker`, 112 lines | `_reconcile_run_after_worker_loss`, 67 lines |
| `accept_decision()` | 106 lines | 44 lines |
| Methods > 20 in a simple AST branch-count metric | 4 | 1 |
| Maximum simple AST branch-count | 54 (`validate_state`) | 27 (`handle_message`) |
| Sum of simple AST branch-counts | 525 | 501 |

The branch-count metric is an internal deterministic AST sanity measure, not a
claim of formal cyclomatic-complexity tooling. The new `pending.py` is a cohesive
standalone 205-line bookkeeping module rather than a second state authority.

## Behavior deliberately preserved

The pass does not change:

- scheduler policy, ranking, inputs, outputs, or proposal semantics;
- DAG readiness or execution contracts;
- execution modes or native/shared retry restrictions;
- `CONTEXT_LOST`, `NATIVE_STATE_UNCERTAIN`, or `DATA_LOST` policy;
- stale-attempt / duplicate-result commit rules;
- worker-generation semantics;
- transfer protocol/lifecycle;
- cancellation semantics;
- capacity accounting;
- protocol wire contracts;
- the invariant that the coordinator never executes user code.

No genuine new distributed-semantic defect was discovered during this refactor.
The new invariant/staging tests protect existing intended behavior rather than
changing it.

## Final working-tree results

- Coordinator: **162 passed**
- Complete repository: **1336 passed, 1 known environment-sensitive failure**

The final delivery procedure must clean generated caches, build the ZIP, extract
that exact ZIP to a brand-new directory, rerun the coordinator suite and complete
repository suite there, and verify the frozen non-coordinator production sources
remain byte-for-byte unchanged. The delivery report records those fresh-extraction
results for the exact returned archive.

## Adversarial audit correction pass (2026-09-14)

Verified regressions now cover:

- context capacity across repeated scheduling rounds and release paths;
- physical capacity retained for logically orphaned sibling attempts;
- hard/allowed affinity reconciliation after worker loss;
- exact scheduler-decision authority at placement acceptance;
- forged `ObjectAvailable` claims and premature native state-output claims;
- exact `ContextUnavailable` plan scope;
- `online=False` compute reconciliation and generation-scoped quarantine;
- injected-time preparation/dispatch/start/execution/transfer/cancellation deadlines;
- bounded/coalesced worker outboxes and atomic critical-message backpressure;
- membership-limit rejection before registration mutation;
- invalid generated protocol identifiers before registration mutation;
- inactive worker-session record reclamation while preserving generation epochs.

The repository root now places `.` on pytest's import path, so both
`pytest -q` and `python -m pytest -q` collect the same suite after fresh extraction.
The known environment-sensitive execution test asserting that `socket` is absent
from a pristine subprocess may still fail in environments whose Python startup
already imports `socket`; no production behavior is changed to mask that case.

## Batch-2 independent-audit regressions

Focused regressions now cover valid cross-stream transfer completion reordering, persistence failure during multi-run worker loss (including a real locked SQLite database), unresolved-transfer pruning, atomic heartbeat capacity shrink rejection, context-loss tombstone accounting, cancellation/manual-transfer failure atomicity, initial-context validation, delayed-dispatch requirement revalidation, and durable run-ID uniqueness across pruning/restart. The dedicated `test_audit_batch2.py` suite is required in release verification.

## Batch-3 independent-audit regressions

Batch 3 closes the remaining audit findings around authentication replay retention,
worker-generation data provenance, operational-state bounds, and clean release
packaging. Regression coverage includes replay-cache pressure that cannot shorten the
validity-window replay guarantee, verifier-issued challenge enforcement, replacement
worker sessions attempting to reassert historical producer data, active pending and
transfer admission limits (including scheduler-created transfers), retained-run and
worker-identity limits, explicit non-durable terminal-run discard, and deterministic
cache-free release ZIP construction via `python -m tools.build_release`.

## Final post-audit state-hygiene regressions

The focused `test_audit_phase3_state_hygiene.py` suite verifies context loss while
cancellation is in flight, eventual tombstone retirement (including owner-session
loss), dynamic context affinity rejection before enqueue, current-state archival as a
precondition for pruning, retained-context admission/reservation bounds, and safe
release restoring admission. `runtime_security/tests/test_auth.py` also verifies that
expiry reclamation is independent of verification/LRU ordering while unexpired replay
state remains fail-closed.
