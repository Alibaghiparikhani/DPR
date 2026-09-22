# Runtime Security

This package contains the security primitives used by the real coordinator/worker/operator control transport and worker data plane:

- `NodeAuthenticator`: per-node HMAC-SHA256 challenge/response, session-bound,
  expiring, and replay-protected with a bounded cache;
- `TlsPolicy`: mutual-TLS context construction with TLS 1.3 minimum, mandatory
  peer certificate verification, hostname verification on clients, configured
  cluster CA trust, and TLS compression disabled.

It intentionally opens no sockets itself. `networking` applies these primitives to every real coordinator/worker/operator control connection, while the worker data plane uses the same TLS policy for direct peer authentication. TLS plus application authentication completes before worker or operator admission.

Replay protection is fail-closed under pressure: the authenticator never evicts an
unexpired accepted/issued challenge merely to make room. When its bounded security
window is full, new challenge issuance is rejected until older entries expire.
