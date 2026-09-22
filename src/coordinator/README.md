# coordinator

Synchronous, deterministic control-plane state for the same-LAN Python runtime.
It integrates the existing execution plan, scheduler, and typed protocol but
opens no sockets and executes no user code.

Core API:

```python
from coordinator import Coordinator

c = Coordinator()
handle = c.register_worker(worker_hello)
c.submit(plan, run_id="run-1")
result = c.schedule("run-1")

# Real networking adapter feeds typed protocol messages:
c.handle_message(handle, worker_message)

# Real networking adapter sends these typed messages:
outbound = c.drain_outbox(handle)

# Immutable operational diagnostics:
pending = c.inspect_pending_operations()

c.validate_state()
```

For abrupt socket/TLS loss, the adapter calls `disconnect_session(handle)`. It
retires only an exact current handle; stale/replaced connection teardown is a
no-op and therefore cannot retire a newer generation. The coordinator itself
remains socket-free and synchronous.

See `COORDINATOR_CONTRACT.md` for lifecycle, commit, execution-mode-aware retry,
session-scoped correlation, idempotent program/context preparation, stable run-scoped
context contracts, bounded completed-correlation tombstones, context/data-loss failure,
location/transfer, and cancellation semantics. Inspection getters return defensive
copies; coordinator state remains single-owner mutable state.

Internal ownership is intentionally narrow rather than service-oriented:

- `coordinator.py` remains the sole owner of live worker/run/task/attempt/transfer
  semantics and all authoritative state transitions;
- `pending.py` owns only preparation correlation IDs, exact request contracts,
  session-generation binding, idempotent active lookup, and bounded retired
  tombstones;
- `locations.py` owns transferable-representation location metadata only;
- `runs.py` wraps the existing DAG readiness state for one coordinator run.

This separation is meant to make state-machine mistakes harder without creating
independent "manager" objects that could disagree about runtime truth.

### Audit-hardening notes

Recent adversarial testing added effective native-context capacity accounting,
physical-capacity retention for orphaned attempts, exact scheduler-proposal
authority, strict `ObjectAvailable` provenance, plan-scoped context loss,
worker-generation quarantine, deterministic operation deadlines, bounded
control outboxes, membership-limit atomicity, and inactive-session reclamation.
Running user tasks still have no default wall-clock deadline; configure
`OperationTimeouts(execution=...)` only when the application has a valid task
execution policy.

## Optional durable history

`SQLiteRunHistoryStore` can be supplied to `Coordinator(history_store=...)` to
archive terminal run/task/attempt/transfer metadata transactionally using the
Python standard library. This is diagnostic history, not active-run crash
recovery: live execution state remains authoritative in memory. A terminal run
may be removed with `prune_terminal_run()` only after durable archival and only
when no physically unresolved attempt still occupies worker capacity.

Task execution remains unbounded by default. Set `OperationTimeouts(execution=...)`
when an application explicitly wants a wall-clock execution deadline; control
operations retain their normal bounded deadlines.

### Additional failure-safety notes

Transfer source/destination notifications are not assumed to share a global receive order. Persistence errors do not stop live reconciliation. Context and worker capacity are never reduced beneath unresolved physical reservations. Initial contexts and delayed dispatches are revalidated at their authority boundaries, and durable run IDs are not reusable after archival.

### Operational admission limits

`Coordinator(operation_limits=OperationLimits(...))` bounds live/retained in-memory
operational structures without evicting live work. Defaults cover active preparation
requests, transfer records, retained prepared contexts (globally/per worker/per run),
retained runs, and remembered worker identities. Active context preparation reserves
its future retained slot before worker-side creation. Hitting a limit raises
`OperationalLimitExceeded` before authoritative mutation. Terminal runs
without a durable history store can be explicitly reclaimed with
`discard_terminal_run()` after all physical work/transfer cleanup is resolved.

Replica certification is worker-generation scoped internally. A restarted worker with
the same `worker_id` does not inherit a previous session's producer/data authority.
