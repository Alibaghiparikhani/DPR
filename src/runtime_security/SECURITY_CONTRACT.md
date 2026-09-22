# Runtime Security Contract

`runtime_security` contains transport-independent primitives used by the real coordinator/worker
networking layer. It deliberately opens no sockets itself.

## Node authentication

`NodeAuthenticator` uses a distinct caller-provisioned secret for each node and
HMAC-SHA256 proofs over the exact node ID, one-use authentication session intent,
challenge nonce, and issuance time. Verification uses constant-time MAC
comparison, an expiry window, and a bounded replay cache. The real transport
verifies a proof **before** admitting the peer's `WorkerHello` into coordinator
state, then binds the resulting coordinator `SessionHandle` to that exact TLS
connection.

Secrets must be generated outside this package, stored with filesystem/secret
manager protections, and rotated operationally. Sharing one secret between all
nodes defeats per-node identity.

## TLS

`TlsPolicy` constructs TLS contexts with:

- TLS 1.3 as the minimum version;
- peer certificate verification required;
- client hostname verification enabled;
- local certificate/private-key loading;
- configured cluster CA trust;
- TLS compression disabled.

The intended network transport is mutual TLS. Certificate issuance, rotation,
revocation, and endpoint-to-certificate naming remain deployment responsibilities.

## Layering

Authentication and TLS do not modify protocol records and do not weaken the
protocol decoder. The `networking` socket transport performs TLS and peer authentication before
forwarding admitted typed protocol messages into coordinator state.

## Replay-window capacity semantics

Authentication replay protection is verifier-issued-challenge based. Every unexpired
issued challenge remains in bounded verifier state for its entire acceptance window.
A successfully verified proof marks that challenge consumed, but its replay tombstone
is retained until expiry. Capacity pressure therefore **never evicts still-valid
replay state**: once `replay_limit` unexpired challenges are retained, `issue()` fails
closed with `AuthenticationCapacityExceeded` until entries expire.

`verify()` accepts only a proof for an exact current challenge previously issued by
that verifier, bound to node ID, session ID, nonce, and issuance time. Replays of a
consumed challenge are rejected even while the verifier is at capacity. Reusing a
nonce for the same node/session inside the live security window is also rejected.
Transport admission should treat capacity exhaustion as authentication backpressure,
not as permission to weaken replay guarantees. Expiry reclamation scans the bounded
challenge set by issuance timestamp rather than assuming verification/LRU order is an
expiry order, so legitimate out-of-order completion cannot strand expired records.
