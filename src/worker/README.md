# Worker runtime — isolated, shared/native contexts, and direct data plane

`worker.WorkerControlClient` owns the authenticated coordinator control
connection, exact coordinator session identity, heartbeats, bounded queues,
reconnect policy and clean shutdown. `WorkerExecutionRuntime` owns package
preparation, local runtime representations, isolated child processes, persistent
execution contexts, and the worker-to-worker byte plane. User Python never runs
inside the coordinator, networking callbacks, or the worker controller process.

## Execution modes

Ordinary `ISOLATED_CANDIDATE` work retains the Batch-2 one-shot subprocess
model. A `TaskDispatch` carrying an exact prepared `context_id` is instead routed
to that persistent context process. `SHARED_CONTEXT` and `NATIVE_REGION` require
such a context; they are never downgraded to isolated execution when the context
is absent.

Dispatch is validated against the exact worker/session, program, plan, run,
task, attempt, execution mode, context binding, locally available input
representations and physical capacity. The coordinator remains authoritative for
logical task/attempt state and retry policy.

## Package preparation

`PrepareProgram` requires the exact content-addressed package and environment.
Package bytes arrive over the authenticated control connection using bounded
`PackageTransferStart/Chunk/End` records correlated to that `PrepareProgram`.
Installation uses `program_package.PackageCache`; a cache hit is reverified. The
worker checks the entrypoint hash, reconstructs the execution plan, and requires
the exact program and plan identities before emitting `ProgramPrepared`.

Package preparation is single-flight by immutable package identity. Partial
transfers/installations remain staged and are removed on failure or session loss.
The package cache can survive reconnects, but its contents are always reverified
before use.

## Isolated child model

Isolated user code runs in a separate Python process created with
`create_subprocess_exec`, never through shell interpolation. Parent/child control
metadata is bounded and structurally validated before becoming a protocol result.
stdout and stderr are drained concurrently and retained only up to configured
limits.

There is no universal execution timeout. When a task deadline is configured,
monotonic timing terminates and reaps the exact child. Cancellation is
attempt-scoped and idempotent: terminate, bounded grace wait, force-kill if
needed, then bounded reap. Physical capacity is retained until exit/reap is
established. POSIX execution uses a process group for obvious descendants; Linux
also uses a parent-death signal. Complete arbitrary process-tree cleanup on all
platforms is not claimed.

## Persistent shared/native contexts

A prepared context is a real long-lived Python subprocess with exact
`plan_id/run_id/context_id/session_id` ownership and the coordinator-provided
prepared task set. The controller retains a physical runtime handle and permits
only one active context task at a time in the current implementation, matching
the advertised context slot capacity.

Tasks assigned to the same context execute in the same interpreter and therefore
observe the same in-process mutable/native state. Controller-to-context requests
and context-to-controller results are bounded and carry the exact attempt and
context identity. Replies from retired/replaced contexts are not accepted as
current execution.

Uncertainty is handled conservatively. A context exception, cancellation,
process crash, worker-session loss, or explicit `ReleaseContext` retires the
physical context rather than silently rebuilding or replaying mutable/native
work. `NATIVE_REGION`/shared work is never automatically rebound to a reconnect.
A context can be reported unavailable only for the exact owning run/plan/context.

## Worker-local runtime data store

Task outputs that are transferable are published into `LocalDataStore` before
success/availability is reported. Entries are keyed by the complete
`DataReference`, including `DataForm` and object-state version, and by the exact
worker control session. Immutable identity collisions with different bytes or
provenance are rejected.

Publication is staged and atomic. The store verifies declared size and SHA-256
before installation, has explicit item/value/aggregate byte limits, and removes
corrupt entries rather than treating them as available. Runtime data authority is
intentionally session/process scoped: on worker-controller startup the store
clears unindexed old runtime data, so reconnecting under the same worker name
does not resurrect stale possession from a previous process/session.

The controller keeps payloads as opaque verified bytes. Python-value
deserialization happens only inside user-code subprocesses. The runtime uses the
internal `pickle-v1` representation for Python values produced by trusted worker
execution and a bounded JSON-safe representation where appropriate; arbitrary
network-selected pickle is never deserialized by the worker controller.

## Direct worker-to-worker data plane

Every worker may expose a separate mTLS data-plane listener. Bulk runtime bytes
do not travel through the coordinator control connection. A destination accepts
bytes only after the coordinator has issued the exact `PrepareReceive`; a source
sends only after the exact `TransferRequest`.

The coordinator-issued authorization is bound to the complete transfer identity,
source and destination worker IDs, source and destination control-session IDs,
and a one-use authorization token. Both data-plane peers validate TLS peer
certificates against the expected worker identity. Reconnect therefore does not
inherit an old transfer authorization.

The sender streams bounded chunks from the already-published local
representation. The authenticated transfer header includes exact data/transfer
identity, session bindings, expected size, SHA-256 and serialization. The
receiver writes to temporary storage, verifies identity, length and digest, then
atomically publishes the representation. Only after publication can it emit
`TransferCompleted`. A TCP/TLS close by itself is never completion evidence.

Incoming/outgoing transfer counts, transfer bytes, chunk/header sizes and
connect/handshake/idle/total/cleanup times are bounded/configurable.
`CancelTransfer` aborts the exact sender/receiver attempt; a cancelled receiver
cannot publish late bytes. Old transfer attempts, object-state versions or
sessions cannot satisfy a newer attempt.

## Session loss and shutdown

All execution, contexts, transfer authorization and runtime-data provenance are
owned by the exact coordinator session. On session loss/replacement the worker
fences results, terminates/fences active isolated/context work, cancels P2P
state, clears session-local runtime data and never translates old work into the
new session. A clean `WorkerGoodbye` is withheld until required physical cleanup
has been established; otherwise the transport fails closed.

## User workflow and remaining scope

The `dpr` CLI now starts real workers and the coordinator and submits/statuses/cancels runs through the authenticated operator channel. Active-run coordinator crash recovery and automatic cluster discovery remain outside the current architecture. The runtime does not claim physical exactly-once execution: the guarantee remains at-most-one authoritative logical commit; physical work may have run before failure, and shared/native state uncertainty is handled by conservative context loss rather than replay.
