# Transfer preparation correction — verification

Verified on 2026-09-14 with Python 3.12.14 and pytest 9.1.1 (Linux).
Input: `src-protocol-verified(2).zip`.

## Baseline before modifications

- Protocol: **442 passed in 1.73s**.
- Complete repository: **1,075 passed in 7.56s**.
- The environment-sensitive execution/import-isolation `socket` failure did not
  occur. No workaround, skipped test, or lower-layer change was introduced.

## Focused correction

The destination was required to correlate completion/failure to a source-only
TransferRequest. It now receives its own PrepareReceive, responds ReceiveReady
(or ReceivePreparationFailed), and retains that preparation's message ID for
its subsequent completion/failure reports. The coordinator waits for matching
readiness before instructing the source. Source events retain correlation to
TransferRequest. The existing full TransferIdentity connects both legs.

New immutable/slotted records and wire types:

- PrepareReceive — `transfer.prepare_receive`.
- ReceiveReady — `transfer.receive_ready`.
- ReceivePreparationFailed — `transfer.receive_preparation_failed`.

PrepareReceive uses existing optional representation-size metadata. The two
new reports require the destination worker ID. Existing TransferFailed retains
its explicit reporter worker ID, which distinguishes source and destination.
No new identity fields, execution modes, DataForm variants, checksum format,
serializer, retry policy, listener, or runtime state machine were introduced.

This is an explicit undeployed-v1 correction. Versioning implementation and
all existing wire shapes remain unchanged; older decoders reject the new types.
The protocol validates local structure, not command-delivery history. Future
callers must match full identities, recipient-visible request IDs, and lifecycle
order. Tests explicitly exercise that boundary without claiming the codec knows
which commands a particular worker received.

## Test results and fresh archive verification

Each suite was run independently, followed by the full repository suite. The
same commands were then run from a brand-new extraction of the final archive:

| Command from extracted `src/` | Working tree | Fresh extraction |
| --- | --- | --- |
| `python -m pytest -q protocol/tests` | 542 passed | 542 passed |
| `python -m pytest -q dag_runtime/tests` | 311 passed | 311 passed |
| `python -m pytest -q execution/tests` | 141 passed | 141 passed |
| `python -m pytest -q scheduler/tests` | 181 passed | 181 passed |
| `python -m pytest -q` | 1,175 passed | 1,175 passed |

All runs had zero failures/skips/xfails. **100 test cases were added**:
88 focused cases in test_receive_lifecycle.py and 12 cases automatically added
by the existing per-message parametrizations for the three new types. All nine
existing protocol test-function files remain byte-for-byte unchanged. Five
transfer sample fixtures were updated to distinguish source/destination command
IDs; three receive-preparation samples were added. Existing missing/unknown-field,
immutability, codec, split-position and fuzz-style tests now also cover new types.

Focused coverage includes successful two-leg transfer, preparation failure with
no source instruction, source rejection, source/destination failures after start,
destination completion without the source command, changed source request IDs,
two interleaved transfers of the same value/version to different destinations,
old attempts and conflicting scopes/routes, malformed/duplicate identities,
wrong reporters, correlation structure, invalid enum/version/size fields,
non-transferable forms, bounds, frozen records and every frame split.

`python -m protocol.examples.control_flow` passed through encode → frame →
seven-byte fragmented decoding → decode: **15 messages, 6,247 framed bytes**.
Its final six messages are PrepareReceive, ReceiveReady, TransferRequest,
TransferAccepted, TransferStarted, TransferCompleted. Assertions verify equal
TransferIdentity on both legs, separate request correlations, and destination
completion correlating only to preparation. No computation or data transfer runs.

The existing benchmark also passed at 10,000 messages/8,192-byte chunks and
1,000 messages/single-byte chunks, without wall-clock test thresholds. Isolated
`python -S` checks confirm protocol/execution/scheduler/dag_runtime imports all
resolve inside the fresh extraction, with all **36** message types registered.

## Complete diff audit

Exactly these eight paths were added/changed, all under `protocol/`:

- `messages.py`: three records, three explicit registry entries, one destination
  reporter validator, and clarified transfer identity/correlation docstrings.
- `__init__.py`: public exports for the three new messages.
- `tests/samples.py`: corrected transfer correlations and new sample records.
- `tests/test_receive_lifecycle.py`: new focused regression cases.
- `examples/control_flow.py`: destination preparation and complete transfer
  sequence; destination completion correlates to preparation.
- `PROTOCOL_CONTRACT.md`: corrected lifecycle, sender/recipient/correlation table,
  sequence diagram, failure semantics, identity rules and undeployed-v1 note.
- `README.md`: concise corrected lifecycle guidance.
- `VERIFICATION.md`: this correction-specific verification record.

Byte comparison against the supplied ZIP confirms:

- All **58** original DAG/execution/scheduler files are unchanged.
- `codec.py`, `framing.py`, `validation.py`, `version.py`, `limits.py`, and
  `errors.py` are unchanged.
- Root `pytest.ini` and all existing test-function files are unchanged.

AST comparison confirms all **33** existing registry entries and all existing
transfer/envelope validators are unchanged. The existing TransferIdentity and
DataReference fields are unchanged; no arbitrary identity generation was added.
Every transfer message was audited for sender, recipient, identity and request
visibility. The contract table records the result. Ruff F/E9 and formatting
checks passed for changed Python files; no development or production dependency
was added to the repository.

The ZIP contains the complete updated `src/` tree (84 files) and excludes
__pycache__, .pytest_cache, .ruff_cache, bytecode, temporary verification tooling,
and external links. Archive content is compared byte-for-byte with the release
source before fresh tests. Original-workspace PYTHONPATH is removed for those
tests. Final delivery is the exact archive whose fresh extraction passed.

Scope ends at the protocol correction: no coordinator, worker daemon, sockets,
package/object data transfer, object storage, scheduling changes or runtime
execution was implemented.
