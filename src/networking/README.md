# Coordinator/worker/operator control networking

`networking` is the real TCP/TLS adapter around the existing synchronous
`Coordinator`. It does **not** execute Python tasks or relay runtime object
payloads. Batch 3 keeps bulk worker-to-worker bytes on the separate worker-owned
mTLS data plane; this package continues to carry only authoritative control
messages and bounded program-package transfer records.

## Ownership and concurrency

The service uses `asyncio` for sockets, but all calls that can mutate
`Coordinator` state are serialized through one service-owned lock. Socket
readers/writers may progress concurrently; authoritative coordinator transitions
do not. The coordinator remains the single source of truth for membership,
session generations, heartbeat semantics, retries, loss reconciliation, task
state, and transfer orchestration.

## Admission sequence

Every control connection is TLS from the first byte. `TlsPolicy` enforces TLS
1.3 minimum, CA validation, a server certificate, mandatory client certificate,
and client hostname validation.

After TLS:

1. worker sends `AuthenticationRequest(node_id, session_id)` where `session_id`
   is a fresh one-use connection/admission intent;
2. coordinator verifies that `node_id` exactly matches an identity in the
   validated client certificate (SAN DNS/URI or common name);
3. the existing `NodeAuthenticator` issues a bounded HMAC challenge;
4. worker returns `AuthenticationProof` bound to node, intent, nonce and issue
   time;
5. verifier checks freshness, MAC and replay state and returns
   `AuthenticationAccepted`;
6. exactly one `WorkerHello` is accepted on that same authenticated TLS
   connection, and its worker/endpoint identity must equal the authenticated
   node identity;
7. `Coordinator.register_worker()` creates the authoritative `SessionHandle` and
   its `WorkerAccepted` response is drained through the normal coordinator
   outbox.

The authentication-intent ID is not the coordinator session ID. The security
binding is one-to-one and transactional: a verified proof authorizes exactly one
`WorkerHello` on that exact TLS connection; the resulting `SessionHandle` is
stored on that connection. A proof cannot be moved to a different connection,
node, intent, or later reconnect.

## Session replacement and disconnect

Each admitted socket retains the exact `SessionHandle` returned for that
admission. Reconnecting the same worker ID creates the normal next coordinator
generation. An older socket can remain physically alive, but any later message
is delivered with its old handle and is rejected by the existing stale-session
logic. It is never rewritten to the current generation.

Transport teardown calls `Coordinator.disconnect_session()`. That method retires
only an exact current handle; delayed teardown of a replaced socket is a no-op,
so it cannot accidentally remove the replacement generation.

## Framing, bounds and backpressure

The network layer reuses `protocol.FrameDecoder`, `decode_message`,
`encode_message`, and `frame_payload`; there is no second application codec.
Reads are incremental and support split headers/bodies and multiple frames per
read. Frame size remains capped by the protocol's 1 MiB limit.

Each admitted connection has explicit bounded inbound and outbound message
queues. `asyncio.StreamWriter.write()` plus `drain()` is used so partial kernel
writes are handled by the stream transport rather than assuming one `send()` can
write a frame. Writes and admission have timeouts. Queue exhaustion is
fail-closed: the affected session is disconnected/reconciled instead of silently
dropping a critical control message.

Malformed framing/JSON/type/version input is contained to the offending
connection. Pre-authentication application messages never reach coordinator
authority.

## Heartbeats and time

The adapter supplies monotonic runtime timestamps to existing coordinator APIs.
Heartbeat state, duplicate/stale sequence semantics, capacity checks, offline
reconciliation, and timeout loss remain coordinator semantics. A small
maintenance task only calls `expire_workers(now=...)` and drains coordinator
outboxes.

## Shutdown

Service shutdown first stops accepting new work, then retires/cancels owned
connections, and finally waits for the listening server to close. TLS close is
bounded so a peer that stops cooperating cannot pin shutdown forever. A clean
worker shutdown sends `WorkerGoodbye` through the normal framed connection.

## Batch-3 control/data-plane boundary

Shared/native context execution and direct runtime-object transfer are now worker
responsibilities, but their authority still comes from this control channel.
`PrepareContext`/`ReleaseContext`, `PrepareReceive`, `TransferRequest`, and
`CancelTransfer` remain coordinator-issued commands. Transfer authorization is
bound to the exact source/destination control-session IDs and transfer attempt,
so a reconnect cannot inherit an older physical data-plane operation.

The coordinator network service never opens a bulk object stream and never sees
the runtime payload bytes. Workers maintain a separate mTLS listener for the
actual data plane. Control traffic therefore remains responsive while large
values are chunked directly between workers.

The authenticated operator channel used by the `dpr` CLI shares this TLS/HMAC/framing stack but is capability-limited to package submission, run/cluster inspection, and run cancellation. Operator identities require an explicit allowlist and cannot inject worker messages. Active-run coordinator crash recovery and broad cluster discovery remain out of scope.

## Package delivery extension

Program-package acquisition remains on this same authenticated control connection;
it does not introduce a second unauthenticated socket protocol. When the
coordinator emits an existing `PrepareProgram` whose immutable package is in the
bounded `PackageRepository`, the adapter follows it with correlated typed
`PackageTransferStart`, ordered `PackageTransferChunk`, and `PackageTransferEnd`
records. Package chunks remain within normal protocol/frame limits and writes
remain subject to the Batch-1 timeout/backpressure policy.

A connection single-flights package bytes by immutable package identity while a
preparation is outstanding. Later `PrepareProgram` commands for that same
package still reach the worker (so their control correlations remain intact),
but do not enqueue duplicate archive streams. The worker independently verifies
all bytes and is the authority on whether physical package preparation actually
succeeded; transport delivery is never treated as `ProgramPrepared`.
