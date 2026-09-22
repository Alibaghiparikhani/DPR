# Control protocol

`protocol` provides immutable control messages, strict deterministic UTF-8 JSON,
and bounded incremental framing. It uses only the standard library and the
existing execution/scheduler models. It does not open sockets or run tasks. Batch 1 networking reuses this codec/framing directly, including typed pre-admission authentication records. Batch 2 adds bounded package-transfer records. Batch 3 adds only the control records/bindings needed for physical context retirement and direct P2P transfer (`ReleaseContext`, `CancelTransfer`, and transfer/session authorization fields). TLS, package/cache logic, context processes, runtime serialization, and byte movement remain worker/network responsibilities rather than codec behavior.

Run from `src/` with Python 3.12 and the existing pytest development dependency:

```bash
python -m pytest -q protocol/tests
python -m pytest -q
python -m protocol.examples.control_flow
python -m protocol.benchmarks.measure --messages 10000 --chunk-size 8192
```

```python
from execution import AttemptIdentity
from protocol import (
    TaskStarted,
    encode_message,
    decode_message,
    frame_payload,
    FrameDecoder,
)

attempt = AttemptIdentity("a" * 64, "run-1", "T000001", "attempt-1")
message = TaskStarted(
    "worker-1", attempt, message_id="started-1", correlation_id="dispatch-1"
)
frame = frame_payload(encode_message(message))
decoder = FrameDecoder()
messages = []
for chunk in (frame[:2], frame[2:11], frame[11:]):
    messages.extend(decode_message(payload) for payload in decoder.feed(chunk))
decoder.finish()
assert messages == [message]
```

Every response to a command carries that command's `message_id` as its
`correlation_id`. Commands and unsolicited notices use `None`; no ID or timestamp
is generated automatically. Frozen records require exact typed enum/record
instances and tuple/frozenset collections. Wire arrays are strictly validated.

Transfers first use `PrepareReceive` → `ReceiveReady` on the destination's
control channel, then `TransferRequest` → `TransferAccepted` → `TransferStarted`
on the source's channel. Destination `TransferCompleted` correlates to
`PrepareReceive`; it never requires the source-only request ID. Both legs
retain the same caller-assigned `TransferIdentity`. In Batch 3 the coordinator
also binds those two commands to exact source/destination control-session IDs and
a transfer-scoped authorization token. `CancelTransfer` requests physical
participant cleanup without changing task-retry semantics. The protocol still
contains no runtime payload bytes. The runnable control-flow example covers the
control sequence through fragmented framing.

Read [PROTOCOL_CONTRACT.md](PROTOCOL_CONTRACT.md) before integration, especially
the prefix-delivery rule on a framing error, native-context obligations,
version bootstrap, and the difference between success and commitment.

## Authenticated operator/control client

The final runtime CLI uses typed `client.*` records on the same TLS/HMAC/framed control service. A successful HMAC proof does not itself grant operator authority: after authentication, `ClientHello.client_id` must match the authenticated certificate/HMAC node and the coordinator's explicit operator allowlist. Operator sessions can only submit verified program packages, inspect run/cluster state, and request real coordinator cancellation. They cannot send worker lifecycle/task/transfer records.

Run submission is bounded and staged: `RunSubmitStart` first reserves a submission slot and receives `RunSubmitReady`; only then are bounded hex chunks accepted, followed by `RunSubmitEnd`. The coordinator verifies archive size/SHA-256, package identity, entrypoint source and reconstructed execution-plan identity before run admission. Operator package transfer is not a runtime-object data path.
