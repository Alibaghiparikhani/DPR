# Coordinator control-plane contract

`coordinator` is the authoritative **in-memory control plane** for one process.
It owns distributed run state but performs no user computation and opens no
network connections. The real networking adapter decodes `protocol.Message`
records and passes them into this synchronous state-machine core.

## Scope and dependency direction

The dependency direction is intentionally one way:

```text
dag_runtime  -> execution -> scheduler
                    \          \
                     \          -> coordinator
                      -> protocol -> coordinator
```

The existing DAG, execution, scheduler, and protocol packages remain semantic
sources of truth. The coordinator does not re-analyze Python, reproduce the DAG
proof, recreate scheduler ranking, serialize objects, or execute task source.

This phase implements:

- worker enrollment, session generations, heartbeats, expiry and membership views;
- run/task/attempt state;
- real `Scheduler.schedule()` integration;
- revision-stamped proposal acceptance and capacity reservations;
- typed `TaskDispatch`/result handling;
- retries and stale-attempt rejection;
- exactly-once logical commit;
- transferable-representation location metadata;
- two-sided transfer orchestration;
- cancellation and worker-loss handling;
- deterministic clocks/IDs and invariant validation.

Those physical concerns remain outside this package: sockets/asyncio servers,
worker and context processes, package/cache installation, runtime serialization,
direct P2P byte movement, TLS/authentication, discovery and coordinator
replication are implemented by or remain responsibilities of adjacent layers.
The coordinator orchestrates them only through typed control messages.

## Deterministic core

`Coordinator` accepts an injected ID source and clock. The default ID source is
a per-coordinator sequential source (`attempt-1`, `message-1`, ...), and the
default clock is constant zero. Production networking injects a monotonic clock. No state transition calls random UUID generators or wall-clock time.

The processing model is synchronous:

```text
network transport
        |
        v
typed protocol message
        |
        v
Coordinator.handle_message(...)
        |
        +--> validated state transition
        +--> typed outbound message(s) in session outbox
```

Draining an outbox is transport delivery bookkeeping, not coordinator-state
mutation, so it does not invalidate a scheduling proposal.

### Internal ownership boundary

`Coordinator` remains the single authority for worker, run, task, attempt,
context, transfer, retry, cancellation, and commit semantics. The small
`PendingOperationRegistry` is deliberately narrower: it owns only preparation
command correlation bookkeeping (active operations, exact immutable contracts,
session-generation binding, bounded completed tombstones, and correlation-ID
generation). It cannot schedule work, mutate runs, publish data, or send messages.

Program and context preparation equality is represented by frozen contract
objects rather than repeated loose field comparisons. The registry maintains
secondary indexes for exact program contracts and `(run_id, context_id)` so
idempotent request lookup is O(1); invariant validation cross-checks those
indexes against active requests.

## Worker sessions and membership

A `WorkerHello` creates a `SessionHandle(worker_id, generation, session_id)`.
Generations are coordinator-internal and increase whenever the same worker ID
reconnects. The previous session is treated as lost before the new session is
accepted. A message routed through an old handle raises `StaleWorkerSession` and
cannot mutate the new session.

Worker state supplied by hello/heartbeat remains a claim. Coordinator-known
active attempts are overlaid conservatively when constructing effective
`WorkerState`; this prevents a stale heartbeat from freeing a locally reserved
slot and avoids double-counting a heartbeat that already includes that work.

New joins and departures produce full `MembershipUpdate` views for remaining
sessions. The coordinator is the membership/endpoint authority, but this phase
does not establish peer connections.

Heartbeat expiry is explicit/injectable (`expire_workers(now=...)`); tests never
sleep.

## Run lifecycle

```text
RUNNING ---------------------> SUCCEEDED
   |                              ^
   | terminal task failure        | all tasks committed
   v                              |
 FAILED                           |
   |
   +-- terminal                   |

RUNNING -> CANCELLING -> CANCELLED
```

An empty execution plan is immediately `SUCCEEDED`. Terminal runs never regain
READY work or active attempts.

## Task and attempt lifecycle

The normal lifecycle is:

```text
BLOCKED -> READY -> [WAITING_TRANSFER] -> DISPATCHED -> ACCEPTED -> RUNNING
                                                               |
                                                               v
                                                            COMMITTED
```

A placement with remote inputs allocates an attempt and reserves the destination
slot but enters `WAITING_TRANSFER`. `TaskDispatch` is not emitted until every
required transfer is destination-confirmed.

Failure branches create a new attempt only when both the failure category **and
the execution mode** make replay semantically safe, and the per-task attempt
bound has not been exhausted. Attempt history is retained; only
`current_attempt_id` is authoritative.

`AttemptIdentity` from `execution` is reused unchanged:

```text
plan_id + run_id + task_id + attempt_id
```

The coordinator never infers identity from formatting.

## Scheduler integration and atomic acceptance

`build_snapshot()` produces real `scheduler.ClusterSnapshot` records containing:

- READY tasks and persistent `ready_sequence`/`wait_rounds`;
- enrolled worker state;
- certified `DataLocation`s;
- active `TaskCommitment`s;
- affinities and prepared contexts;
- blocked/completed task facts.

The existing `Scheduler` returns a proposal. `propose()` records the coordinator
revision represented by that snapshot. `accept_decision()` rejects the entire
proposal if any scheduling-relevant state changed before acceptance. This avoids
copying scheduler eligibility/ranking logic into the coordinator.

Before mutation, every placement and required remote input/source is validated.
All resulting attempt/transfer/control records are then staged before any
authoritative task, attempt, transfer, or outbox mutation. The accepted placement
then logically commits together:

- one new attempt;
- one current-attempt link;
- one worker-slot reservation;
- task transition away from READY;
- either a `TaskDispatch` or all required `PrepareReceive` commands.

No intermediate coordinator API call exposes a task as both READY and reserved.
If staging any placement fails, none of the staged placements are applied.

## Capacity accounting

Coordinator-known `WAITING_TRANSFER`, `DISPATCHED`, `ACCEPTED`, and cancellation
work consumes reserved capacity. `RUNNING` consumes running capacity. Terminal
attempts consume neither.

The effective worker free-slot count is conservative relative to both the latest
worker report and coordinator-known commitments. Duplicate results, retries,
worker loss, and cancellation therefore cannot release a reservation twice.

## Task result and exactly-once commit

`TaskSucceeded` is **not** a commit. The coordinator:

1. resolves the current session/run/task/attempt;
2. checks correlation to the exact dispatch command;
3. requires a legal attempt lifecycle state;
4. calls `ExecutionPlan.validate_result()`;
5. validates any coordinator-inferred transferable immutable output metadata;
6. advances the existing DAG `ReadinessState` exactly once;
7. records the logical commit and newly READY children;
8. determines run completion.

The coordinator never calls user code while committing. In this in-memory phase,
"commit" means authoritative acceptance of the already-computed logical result
and the execution contract's output/state acknowledgements. Worker/native
adapters added later remain responsible for preserving the execution contract's
actual Python binding/context semantics before reporting success.

A task may logically commit at most once. If attempt A1 becomes obsolete and A2
is current, A1 cannot commit. Replaying the already committed success is harmless
and does not advance DAG readiness or release capacity again.

The validated-result commit path is one explicit semantic coordinator operation.
Paired task/attempt transitions for dispatch, acceptance, and start likewise use
paired helpers so future code cannot accidentally advance one side without the
other.

Strict task event order is enforced: dispatch -> accepted -> started -> result.
Cancellation races are the exception: once cancellation is requested, a real
late result may still be authoritative because cancellation may have been too
late to prevent execution.

## Retry and failure policy

`RetryPolicy` owns a deliberately small policy surface:

- bounded `max_attempts_per_task`;
- optional worker-loss retry;
- a set of retryable `FailureKind`s.

Python exceptions are never accepted as an automatic retry category. For
`ISOLATED_CANDIDATE` work, explicitly configured infrastructure-style execution,
input, or environment failures may be retried. Once `SHARED_CONTEXT` or
`NATIVE_REGION` work has been dispatched/executed, failure or worker loss is
treated conservatively: native/shared state may already have changed, so the run
fails with coordinator cause `NATIVE_STATE_UNCERTAIN` rather than replaying the
task. A worker rejection that proves execution did not begin may still be
handled separately; a native/context-unavailable rejection is terminal context
loss rather than invented reconstruction.

The scheduler does not decide retry policy.

If a task becomes terminally failed, the run becomes `FAILED`; remaining
non-committed work is marked failed and all waiting-transfer work is abandoned.
Committed work is never rolled back.

Coordinator-only terminal causes are typed independently from worker
`FailureKind`: `CONTEXT_LOST`, `NATIVE_STATE_UNCERTAIN`, and `DATA_LOST`. They
are exposed through `RunSnapshot.failure` and do not require changing the frozen
execution/protocol contracts.

## Context ownership and loss

Prepared native/shared contexts are persistent execution state, not disposable
scheduler hints. If their authoritative worker disappears or explicitly reports
the context unavailable, the coordinator does not put dependent native work back
into ordinary READY state. If reconstruction has not been explicitly proven, the
run fails with `CONTEXT_LOST`.

This includes a committed native mutation whose exact object version still needs
to be materialized as an object snapshot for an isolated downstream task. If the
owning context disappears before any certified snapshot exists, the coordinator
does not leave that consumer READY forever.

The resulting terminal run is never passed back to the scheduler, so stale
`TaskAffinity`/`WorkerContext` facts cannot form an internally invalid scheduler
snapshot.

## Locations and transferable data

`ObjectLocationIndex` stores metadata only:

```text
DataReference(plan/run/value/form/object-state-version)
    -> available worker IDs + optional size
```

It stores no object bytes. Every announcement is checked against the exact
`ExecutionPlan` and can represent only:

- immutable values; or
- certified shared-reference object snapshots.

Native references, code bindings, state tokens, and discarded results cannot be
published as ordinary data.

A successful immutable task result is sufficient for the coordinator to know
that immutable result resides on its reporting worker. A shared-reference result
is **not** automatically promoted to `OBJECT_SNAPSHOT`: snapshot preparation is
stronger than logical task success, so a worker must explicitly send
`ObjectAvailable` for that exact snapshot/version.

Worker loss removes all replica claims owned by that worker. Reconnecting with
the same `worker_id` does not restore old replicas. If that removal destroys the
last valid replica of an exact transferable representation that unfinished work
requires, and no semantics-preserving recomputation is explicitly implemented,
the run fails with `DATA_LOST`. Multiple surviving replicas keep the run viable;
offline/stale replicas never count as surviving availability.

## Automatic input-transfer gating

For an isolated task, the coordinator derives the same transferable input keys
from the frozen execution contract that the scheduler consumed. If a placement's
locality reports remote inputs, the coordinator chooses a deterministic active
AVAILABLE source and creates one control transfer per remote representation.

The task is reserved but not dispatched until all transfers complete:

```text
READY
  |
  | scheduler placement accepted
  v
WAITING_TRANSFER
  |
  | every destination confirms receipt
  v
DISPATCHED -> ACCEPTED -> RUNNING
```

A failed required transfer fails that attempt as infrastructure/input failure and
enters the ordinary bounded retry path. Sibling transfers for the obsolete
attempt are abandoned, so late events cannot trigger dispatch.

If the failed transfer also lost the **last** source replica of a committed
required representation, ordinary retry is not possible: the run terminates as
`DATA_LOST` rather than cycling a permanently impossible READY task.

## Transfer state machine

The corrected two-leg protocol is used verbatim:

```text
Coordinator              Source                 Destination
    |                       |                         |
    | PrepareReceive(T) ---------------------------->|
    |<-------------------------- ReceiveReady(T) ----|
    |                       |                         |
    | TransferRequest(T) -->|                         |
    |<-- TransferAccepted --|                         |
    |<-- TransferStarted ---|                         |
    |                       |===== direct data =====>|
    |<-------------------- TransferCompleted(T) -----|
```

Coordinator transfer states are:

```text
DESTINATION_PREPARING
  -> DESTINATION_READY
  -> SOURCE_REQUESTED
  -> SOURCE_ACCEPTED
  -> TRANSFERRING
  -> COMPLETED
```

with terminal `FAILED` branches.

`message_id`/`correlation_id` identify each control-channel command/reply;
`TransferIdentity` identifies the logical transfer plus transfer attempt.
Destination completion must correlate to the destination-visible
`PrepareReceive`, never the source-only `TransferRequest`.

Each transfer record also captures the source and destination **session
generation** that received its commands. A reconnect using the same worker ID
cannot acknowledge, start, fail, or complete a transfer issued to the previous
session generation.

Only valid destination completion publishes a destination replica. Source start
or send acceptance does not. Conflicting representation sizes are rejected
before transfer state is marked complete.

A failed logical transfer may use a new transfer-attempt identity. A completed
logical transfer is not retried under the same transfer ID.

## Cancellation

`cancel_run()` prevents all new scheduling. READY/BLOCKED work is cancelled
locally. Attempts already sent to workers receive exactly one `CancelTask`.
Attempts still waiting for input transfer have never been dispatched, so their
transfers are abandoned locally and no fake cancellation command is sent.

`TaskCancellationResult(CANCELLED/NOT_FOUND)` releases the attempt. `TOO_LATE`
or `UNSUPPORTED` leaves it active because a real result may still arrive.
If a task result wins the race after cancellation request, the result may commit,
but newly unlocked work is not scheduled and the run finishes `CANCELLED` once
no active attempts remain. No rollback of native/external effects is invented.

## Idempotency and stale events

Events are deliberately classified as applied, duplicate, stale, or invalid.
Examples:

- duplicate heartbeat: harmless, acknowledged again;
- duplicate TaskAccepted after start: harmless;
- duplicate TaskSucceeded after commit: harmless;
- success from an obsolete attempt: stale;
- old-session message after reconnect: rejected;
- duplicate ObjectAvailable: set-like and harmless;
- duplicate ReceiveReady: never emits a second source send command;
- duplicate TransferCompleted: never publishes a second logical replica;
- completion from an old transfer attempt after retry: stale.

No duplicate can create a second commit, retry, capacity release, or DAG
advancement.

## Preparation request correlation and context identity

Program/context preparation requests are stored as typed pending records, not
stringly tuples. Each pending request binds:

- exact worker ID and session generation;
- exact plan/run scope where applicable;
- exact `ProgramIdentity` (whose ID covers environment/package identity);
- exact context ID and requested task set for context preparation.

Only the session that received a command may satisfy it. Worker reconnect/loss
retires old pending commands; a replacement session must receive a new command
and correlation ID. A response claiming a different program, plan, run, context,
or task set is rejected.

The pending records expose frozen semantic contract objects. The registry, not
individual handlers, owns active correlation uniqueness, exact-contract lookup,
generation-scoped resolution, completion, session/run invalidation, tombstone
eviction, and ABA-safe preparation message IDs. Coordinator handlers still own
the semantic decision about what a valid response means.

Program preparation is request-idempotent on one authoritative worker session.
The active request contract is the worker/session generation, plan ID, and exact
`ProgramIdentity` (which includes environment/package identity). Repeating that
exact request reuses the existing pending correlation and emits no second
`PrepareProgram`. If the current worker state already certifies the program ID as
prepared, the request is already satisfied and emits no command. Different
workers/sessions, plans, or program identities remain distinct operations.

Within one run, `context_id` names one stable logical context contract. The
contract is scoped by the immutable run/plan and includes the owning worker and
exact prepared-task set. `WorkerContext.available_slots` is runtime capacity
metadata rather than context identity because `PrepareContext` does not request a
slot count; however, once a concrete `WorkerContext` has been accepted, a later
`ContextPrepared` response with any non-identical record (including a changed
slot count) cannot replace it. Identical duplicate requests reuse the existing
pending command; an identical request for an already prepared context is already
satisfied and emits no new command. A different contract for the same
`(run_id, context_id)` is rejected before dispatch. The response path independently
rejects any attempted last-write-wins replacement.

Completed or retired preparation correlations are retained as bounded tombstones
so recent exact duplicates remain idempotent while stale replies cannot satisfy a
newer operation. `Coordinator.MAX_PENDING_HISTORY` is 1024 entries; eviction is
oldest-first. Active pending operations are stored separately and are never
evicted by this bound. Preparation message IDs include a coordinator-local
monotonic suffix, so eviction of an ancient tombstone cannot permit ABA reuse even
when an injected ID source repeats a base string. A response whose tombstone has
been evicted is treated as an unknown/invalid correlation and cannot mutate state.

Cancellation/failure retires pending context preparation for that run. Attempt
dispatch/cancellation correlation is likewise bound to the attempt's worker
generation.

## Inspection state ownership

The coordinator is the sole mutable-state owner. `get_task()`, `get_attempt()`,
and `get_transfer()` return defensive copies for inspection; mutating those
objects cannot bypass transition checks, revision tracking, or invariants.
`inspect_pending_operations()` returns a frozen summary containing only counts and
message-ID tuples; it does not expose the registry's mutable mappings.

## Invariants

`validate_state()` checks important cross-record invariants, including:

- at most one current attempt per task;
- current attempts exist, belong to the task, are active, and reference active workers;
- each current task status matches its active attempt status, including
  cancellation and transfer gating, and dispatch identities exist only after
  dispatch;
- committed tasks agree with the original DAG readiness state;
- READY tasks are genuinely ready in the original DAG state;
- succeeded runs have completed every task;
- terminal runs have no active attempts or schedulable/active tasks;
- effective worker capacity is never negative;
- location records reference only active workers;
- active pending commands belong to the currently authoritative worker session;
- pending registry secondary indexes exactly match the active typed requests;
- active and retired pending correlations are disjoint and retired history stays
  within its configured bound;
- pending context contracts cannot disagree for the same `(run_id, context_id)`
  and cannot contradict an already authoritative context;
- context dictionary keys agree with each context's own identity;
- active transfers belong to the source/destination generations that received
  their commands.

A completed transfer is historical evidence that destination receipt was once
confirmed; it does **not** assert that the destination replica must exist forever
after a later eviction or worker loss.

Adversarial tests call these checks throughout failure sequences.

## Physical-layer boundary and remaining limitations

The surrounding runtime now provides real TLS coordinator/worker transport,
verified package distribution/cache, isolated execution, persistent shared/native
contexts, worker-local representation storage, and authenticated direct
worker-to-worker byte transfer. None of those physical mechanisms are reimplemented
inside `coordinator`; this package remains the synchronous authoritative control
state machine.

Still intentionally absent are active-run coordinator crash recovery/replication,
polished product/CLI workflow and cluster-discovery UX. Terminal SQLite history is
diagnostic history, not a distributed write-ahead log.

## Post-audit capacity, liveness, provenance, and queue guarantees

The coordinator presents **effective** context capacity to the scheduler: every
attempt that is reserved/running (including transfer-gated work) consumes one
entry slot from its `WorkerContext`. Repeated scheduling rounds therefore cannot
oversubscribe a non-reentrant native/shared context.

Logical run failure does not prove physical worker termination. Sibling attempts
that may still execute become `ORPHANED`: they lose commit authority but keep
occupying worker capacity until a terminal worker observation, cancellation
confirmation, session loss, or conservative operation-deadline quarantine proves
that generation can no longer be scheduled. A coordinator-imposed quarantine
cannot be cleared by a heartbeat on the same session; a fresh worker generation
is required.

A scheduling decision is accepted only when it is the exact decision most
recently produced by this run's scheduler for the still-current snapshot and
coordinator revision. The coordinator does not duplicate scheduler scoring; it
prevents callers from substituting a different placement at the acceptance
boundary.

Hard worker affinity is never silently weakened. Loss of a `required_worker`, or
loss of the final member of `allowed_workers`, fails unfinished work with
`PLACEMENT_CONSTRAINT_LOST`. Surviving members of a multi-worker allow-list may
be normalized after loss, so snapshots never contain references to unknown
required workers.

`ObjectAvailable` is a claim, not authority. The coordinator accepts it only for
an already-certified replica or a representation whose producer has committed
on that worker. Destination-confirmed transfer publishes a replica directly.
`ContextPrepared` alone is deliberately insufficient provenance and cannot be
used to announce future/uncommitted object-state outputs. The current protocol does not let `ContextPrepared` itself authorize an arbitrary
future snapshot. Physical shared/native outputs become available only through
the exact task/result and transfer provenance rules already enforced here.

`ContextUnavailable` is checked against the exact plan/run/context owner scope.
A worker heartbeat transitioning to `online=False` reconciles its active compute
state, contexts, replicas, and transfers instead of leaving immortal running
attempts.

Control-operation liveness is governed by injected-time `OperationTimeouts`.
Program/context preparation, dispatch acknowledgement, accepted-before-start,
transfer control, and cancellation have finite configurable bounds. User task
execution remains unbounded by default (`execution=None`); callers may opt into
an execution deadline. Ambiguous dispatch/start/cancellation expiry quarantines
the worker generation rather than pretending its process stopped.

Worker control outboxes are bounded by `outbox_limit`. Replaceable heartbeat ACK
and membership-refresh messages are coalesced; correctness-critical task,
preparation, transfer, and cancellation commands are never silently discarded
and apply explicit backpressure. Cluster membership is capped at the protocol's
collection limit and registration rejects the boundary before installing a new
worker session.

Inactive `WorkerRecord` session state is reclaimed on loss. Generation epochs,
retained runs/transfers, prepared contexts, and pending operations all have explicit
admission bounds; terminal runs are reclaimed only through the safe durable-prune or
explicit non-durable-discard paths described below. Completed pending-operation
tombstones remain separately bounded as documented above.

## Durable terminal history

The live state machine remains single-owner and in-memory. When an optional
`RunHistoryStore` is configured, every terminal run is archived with its task,
attempt, and run-scoped transfer metadata. The supplied
`SQLiteRunHistoryStore` performs an atomic SQLite transaction and stores metadata
only; user-code objects and payload bytes are not persisted.

This archive does **not** claim active-run crash recovery. `prune_terminal_run()`
refuses active runs, unarchived runs, and terminal runs that still contain
physically unresolved capacity-owning attempts. Immediately before pruning, the
terminal archive is refreshed so late orphan termination observations are not
lost. Pruning requires that refresh itself to succeed; the existence of an older
durable row is never treated as proof that the current terminal state is archived.

## Batch-2 adversarial corrections (F04-F07, F10-F14)

The coordinator additionally guarantees the following failure-ordering and persistence rules:

- Source and destination transfer control streams are independent. Once the coordinator has authorized the source send (`SOURCE_REQUESTED`), a valid destination `TransferCompleted` may arrive before source `TransferAccepted`/`TransferStarted`; late source notifications are idempotent.
- Terminal-history I/O is diagnostic only. Archive failures are recorded and never interrupt authoritative worker-loss/cancellation reconciliation; pruning retries archival and remains forbidden until durable history exists.
- A terminal run cannot be pruned while any transfer lacks an authoritative terminal cleanup observation. New manual transfers are not admitted for terminal runs.
- Heartbeat capacity changes are validated against all coordinator-known physical reservations before becoming authoritative; rejected heartbeats do not refresh liveness.
- Context loss during unresolved execution retains the immutable context identity/capacity tombstone required to account for orphaned physical work.
- Public cancellation and manual-transfer APIs construct and validate outbound protocol records before authoritative mutation.
- Initial context facts are accepted only for active owners, known tasks, and affinity-compatible ownership; `validate_state()` enforces the same accepted-state invariants.
- Transfer-gated dispatch revalidates current worker generation, online state, environment, prepared program, execution mode, context contract, and required input residency immediately before `TaskDispatch`.
- A `run_id` is a durable execution identity when a history store is configured. Archived IDs cannot be silently reused, and the SQLite store refuses a conflicting execution under an existing ID.

## Batch-3 operational bounds and generation-bound provenance

The coordinator now treats worker identity reuse as a session-generation boundary for
all data residency authority. A committed producer authorizes `ObjectAvailable` only
from the exact `(worker_id, worker_generation)` that committed the value. Internal
location records also retain the generation that certified each replica; scheduler
snapshots continue to expose worker IDs only. Worker loss removes only the matching
generation's certification, so a replacement session must establish fresh possession
through a valid transfer/materialization path.

Operational state is bounded through explicit `OperationLimits`. The coordinator
fails closed before mutation when any of these admission bounds is exhausted:

- active pending preparation operations globally, per worker, and per run;
- retained transfer records globally and per run;
- authoritative plus reserved prepared-context state globally, per worker, and per run;
- runs retained in memory;
- remembered worker identities/generation epochs.

These limits do not evict live work. Completed pending-operation tombstones remain
separately bounded by `MAX_PENDING_HISTORY`, and worker outboxes remain bounded by
`outbox_limit`. Scheduler-created transfers and manual transfers use the same transfer
record limits before authoritative state mutation.

Terminal history is never silently discarded. With a `RunHistoryStore`, callers use
`prune_terminal_run()` after durable archival. When durable storage is intentionally
disabled, `discard_terminal_run()` provides an explicit irreversible reclamation
operation; it is allowed only for terminal runs with no unresolved capacity-owning
attempts or transfer cleanup. If the in-memory run limit is reached, new submissions
are backpressured until the caller safely prunes/discards retained terminal history.

Durable SQLite history itself is intentionally not bounded by these operational
limits. It is user-visible diagnostic history and has an explicit deletion API rather
than an automatic in-memory eviction policy.

## Post-audit state-hygiene corrections (F20-F23, residual F15)

Context contracts are validated by one affinity rule at initial submission, dynamic
preparation admission, and response installation. `ContextUnavailable` records an
unavailable tombstone while physical attempts still occupy the context and retires
that tombstone only after the last reservation is conclusively terminal.

Retained context state is admission-bounded globally, per worker, and per run. Active
context preparations reserve a retained-context slot before a worker is asked to
prepare anything, so successful replies cannot overfill the completed collection. A
safely unused context may be retired by authoritative `ContextUnavailable` evidence,
which restores admission capacity; live users are never evicted to satisfy a bound.

Terminal pruning requires a successful archive of the current in-memory state. If a
late terminal observation cannot be persisted, the run and its archival error remain
in memory until a later refresh succeeds.
