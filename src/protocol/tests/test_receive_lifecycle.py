"""Recipient-visible correlation across both legs, without a runtime state engine."""

from dataclasses import FrozenInstanceError, replace
import json

import pytest

import protocol as p
from execution import ValueKind
from scheduler import DataForm
from .samples import DATA, DESTINATION, TRANSFER


def exchange(messages):
    wire = b"".join(p.frame_payload(p.encode_message(m)) for m in messages)
    decoder = p.FrameDecoder()
    result = []
    for offset in range(0, len(wire), 3):
        result.extend(
            p.decode_message(b) for b in decoder.feed(wire[offset : offset + 3])
        )
    decoder.finish()
    assert tuple(result) == tuple(messages)
    return result


def assert_reply(event, command):
    """Test the caller's matching obligation using an actually delivered command."""
    recipient = (
        command.transfer.destination_worker_id
        if type(command) is p.PrepareReceive
        else command.transfer.source_worker_id
    )
    assert event.worker_id == recipient
    assert event.transfer == command.transfer
    assert event.correlation_id == command.message_id


def success(transfer=TRANSFER, destination=DESTINATION):
    prepare = p.PrepareReceive(transfer, 512, message_id="destination-request-A")
    ready = p.ReceiveReady(
        transfer.destination_worker_id,
        transfer,
        message_id="ready",
        correlation_id=prepare.message_id,
    )
    send = p.TransferRequest(transfer, destination, message_id="source-request-B")
    accepted = p.TransferAccepted(
        transfer.source_worker_id,
        transfer,
        message_id="accepted",
        correlation_id=send.message_id,
    )
    started = p.TransferStarted(
        transfer.source_worker_id,
        transfer,
        message_id="started",
        correlation_id=send.message_id,
    )
    completed = p.TransferCompleted(
        transfer.destination_worker_id,
        transfer,
        512,
        message_id="completed",
        correlation_id=prepare.message_id,
    )
    return prepare, ready, send, accepted, started, completed


def test_successful_lifecycle_retains_identity_and_recipient_visible_correlations():
    prepare, ready, send, accepted, started, completed = exchange(success())
    for event in (ready, completed):
        assert_reply(event, prepare)
    for event in (accepted, started):
        assert_reply(event, send)
    assert prepare.message_id != send.message_id
    for message in (prepare, ready, send, accepted, started, completed):
        assert message.transfer == TRANSFER
        assert message.transfer.transfer_id == "transfer-1"
        assert message.transfer.transfer_attempt_id == "transfer-attempt-1"
        assert message.transfer.data == DATA
    assert completed.worker_id == TRANSFER.destination_worker_id


def test_destination_preparation_failure_needs_no_source_request():
    prepare = success()[0]
    failed = p.ReceivePreparationFailed(
        "W2",
        TRANSFER,
        p.TransferFailureCode.IO_ERROR,
        "could not prepare reception",
        message_id="failed",
        correlation_id=prepare.message_id,
    )
    messages = exchange((prepare, failed))
    assert_reply(messages[1], messages[0])
    assert not any(isinstance(m, p.TransferRequest) for m in messages)


def test_source_rejection_after_destination_readiness():
    prepare, ready, send = success()[:3]
    failed = p.TransferFailed(
        "W1",
        TRANSFER,
        p.TransferFailureCode.DATA_UNAVAILABLE,
        "source has no requested version",
        message_id="rejected",
        correlation_id=send.message_id,
    )
    decoded = exchange((prepare, ready, send, failed))
    assert_reply(decoded[1], decoded[0])
    assert_reply(decoded[-1], decoded[2])
    assert not any(isinstance(m, p.TransferStarted) for m in decoded)


@pytest.mark.parametrize(
    "worker,code",
    [
        ("W1", p.TransferFailureCode.IO_ERROR),
        ("W2", p.TransferFailureCode.INTEGRITY_ERROR),
    ],
)
def test_source_or_destination_failure_after_start(worker, code):
    prefix = success()[:-1]
    prepare, _, send = prefix[:3]
    command = prepare if worker == "W2" else send
    failed = p.TransferFailed(
        worker, TRANSFER, code, message_id="failed", correlation_id=command.message_id
    )
    decoded = exchange((*prefix, failed))
    assert type(decoded[-2]) is p.TransferStarted
    assert_reply(decoded[-1], command)
    assert not any(isinstance(m, p.TransferCompleted) for m in decoded)


def test_destination_completion_can_be_built_without_ever_receiving_source_command():
    # Destination's entire control inbox consists of this one preparation.
    prepare = exchange((success()[0],))[0]
    destination_inbox = {prepare.message_id: prepare}
    completed = p.TransferCompleted(
        prepare.transfer.destination_worker_id,
        prepare.transfer,
        prepare.size_bytes,
        message_id="received",
        correlation_id=prepare.message_id,
    )
    completed = exchange((completed,))[0]
    assert_reply(completed, destination_inbox[completed.correlation_id])
    assert "source-request-B" not in destination_inbox


def test_changing_source_message_id_does_not_change_destination_events():
    prepare, ready, send, accepted, started, completed = success()
    changed_send = replace(send, message_id="unseen-source-command-C")
    messages = exchange(
        (
            prepare,
            ready,
            changed_send,
            replace(accepted, correlation_id=changed_send.message_id),
            replace(started, correlation_id=changed_send.message_id),
            completed,
        )
    )
    assert messages[-1] == completed
    assert messages[1] == ready


@pytest.mark.parametrize(
    "event_index,wrong_command_index", [(1, 2), (3, 0), (4, 0), (5, 2)]
)
def test_caller_matching_detects_invisible_command_correlations(
    event_index, wrong_command_index
):
    messages = success()
    event = messages[event_index]
    actual_command = messages[0] if event.worker_id == "W2" else messages[2]
    wrong = replace(event, correlation_id=messages[wrong_command_index].message_id)
    # The stateless schema cannot know an arbitrary ID was never delivered.
    # The caller must compare against its sent/received command and full identity.
    decoded = exchange((wrong,))[0]
    with pytest.raises(AssertionError):
        assert_reply(decoded, actual_command)


def test_same_value_version_to_two_destinations_is_distinct():
    second = replace(TRANSFER, transfer_id="transfer-2", destination_worker_id="W3")
    second_endpoint = replace(DESTINATION, worker_id="W3", port=9002)
    first_flow = success()
    second_flow = tuple(
        replace(
            m,
            message_id=m.message_id + "-second",
            correlation_id=m.correlation_id + "-second" if m.correlation_id else None,
        )
        for m in success(second, second_endpoint)
    )
    # Interleave both operations on one framed control stream with unique message IDs.
    decoded = exchange(tuple(m for pair in zip(first_flow, second_flow) for m in pair))
    one, two = decoded[::2], decoded[1::2]
    assert len({m.message_id for m in decoded}) == len(decoded)
    assert_reply(one[-1], one[0])
    assert_reply(two[-1], two[0])
    assert one[-1].transfer.data == two[-1].transfer.data
    assert one[-1].transfer != two[-1].transfer
    assert one[-1].transfer.transfer_id != two[-1].transfer.transfer_id
    assert one[-1].worker_id != two[-1].worker_id


@pytest.mark.parametrize(
    "field,value",
    [
        ("transfer_attempt_id", "new-attempt"),
        ("destination_worker_id", "W3"),
        ("source_worker_id", "W0"),
        ("data", replace(DATA, run_id="other-run")),
        ("data", replace(DATA, plan_id="c" * 64)),
        ("data", replace(DATA, value_id="other-value")),
        ("data", replace(DATA, object_state_id=None)),
    ],
)
def test_same_transfer_id_does_not_hide_attempt_route_or_scope_collisions(field, value):
    old_prepare = success()[0]
    changed = replace(TRANSFER, **{field: value})
    event = p.ReceiveReady(
        changed.destination_worker_id,
        changed,
        message_id="ready",
        correlation_id=old_prepare.message_id,
    )
    event = exchange((event,))[0]
    assert event.transfer.transfer_id == old_prepare.transfer.transfer_id
    assert event.transfer != old_prepare.transfer
    with pytest.raises(AssertionError):
        assert_reply(event, old_prepare)


NEW_MESSAGES = (
    success()[0],
    success()[1],
    p.ReceivePreparationFailed(
        "W2",
        TRANSFER,
        p.TransferFailureCode.IO_ERROR,
        message_id="failed",
        correlation_id="destination-request-A",
    ),
)


@pytest.mark.parametrize("message", NEW_MESSAGES, ids=lambda m: type(m).__name__)
def test_new_types_are_frozen_slotted_and_roundtrip_at_every_frame_split(message):
    with pytest.raises(FrozenInstanceError):
        message.transfer = replace(TRANSFER, transfer_id="changed")
    assert not hasattr(message, "__dict__")
    frame = p.frame_payload(p.encode_message(message))
    for split in range(len(frame) + 1):
        decoder = p.FrameDecoder()
        payloads = decoder.feed(frame[:split]) + decoder.feed(frame[split:])
        decoder.finish()
        assert [p.decode_message(payload) for payload in payloads] == [message]


@pytest.mark.parametrize("cls", [p.ReceiveReady, p.ReceivePreparationFailed])
@pytest.mark.parametrize("worker", ["W1", "not-a-participant"])
def test_preparation_reports_reject_non_destination_senders(cls, worker):
    extra = (
        {"code": p.TransferFailureCode.IO_ERROR}
        if cls is p.ReceivePreparationFailed
        else {}
    )
    with pytest.raises(p.ValidationError):
        cls(worker, TRANSFER, message_id="m", correlation_id="A", **extra)
    good = cls("W2", TRANSFER, message_id="m", correlation_id="A", **extra)
    wire = json.loads(p.encode_message(good))
    wire["payload"]["worker_id"] = worker
    with pytest.raises(p.ValidationError):
        p.decode_message(json.dumps(wire).encode())


@pytest.mark.parametrize(
    "value", [None, "", " ", 1, True, [], "x" * (p.MAX_IDENTIFIER_BYTES + 1)]
)
@pytest.mark.parametrize("field", ["transfer_id", "transfer_attempt_id"])
def test_bad_transfer_identifiers_both_boundaries(field, value):
    with pytest.raises(p.ValidationError):
        p.PrepareReceive(replace(TRANSFER, **{field: value}), message_id="A")
    wire = json.loads(p.encode_message(NEW_MESSAGES[0]))
    wire["payload"]["transfer"][field] = value
    with pytest.raises(p.ValidationError):
        p.decode_message(json.dumps(wire).encode())


@pytest.mark.parametrize("field", ["source_worker_id", "destination_worker_id"])
def test_missing_participant_or_same_participant_rejected(field):
    wire = json.loads(p.encode_message(NEW_MESSAGES[0]))
    del wire["payload"]["transfer"][field]
    with pytest.raises(p.ValidationError):
        p.decode_message(json.dumps(wire).encode())
    with pytest.raises(p.ValidationError):
        replace(
            TRANSFER,
            **{
                field: TRANSFER.destination_worker_id
                if field == "source_worker_id"
                else TRANSFER.source_worker_id
            },
        )


@pytest.mark.parametrize(
    "field,value",
    [
        ("plan_id", "not-a-hash"),
        ("run_id", None),
        ("value_id", ""),
        ("value_id", 7),
        ("object_state_id", ""),
        ("object_state_id", 4),
        ("form", "native_reference"),
        ("form", "code_binding"),
        ("form", "object_state"),
        ("form", "immutable_value"),
    ],
)
def test_receive_preserves_data_reference_validation(field, value):
    wire = json.loads(p.encode_message(NEW_MESSAGES[0]))
    wire["payload"]["transfer"]["data"][field] = value
    with pytest.raises(p.ValidationError):
        p.decode_message(json.dumps(wire).encode())


@pytest.mark.parametrize("kind", [k for k in ValueKind if k is not ValueKind.IMMUTABLE])
def test_receive_does_not_make_non_data_enums_transferable(kind):
    with pytest.raises(p.ValidationError):
        p.PrepareReceive(
            replace(TRANSFER, data=replace(DATA, form=kind)), message_id="A"
        )


@pytest.mark.parametrize(
    "data",
    [
        replace(DATA, form=DataForm.IMMUTABLE_VALUE, object_state_id=None),
        replace(DATA, object_state_id=None),
        DATA,
    ],
)
@pytest.mark.parametrize("size", [None, 0, p.MAX_INTEGER])
def test_preparation_supports_only_existing_forms_and_optional_expected_size(
    data, size
):
    message = p.PrepareReceive(replace(TRANSFER, data=data), size, message_id="A")
    assert exchange((message,))[0].size_bytes == size


@pytest.mark.parametrize("size", [-1, True, 1.5, "512", p.MAX_INTEGER + 1])
def test_bad_expected_size_both_boundaries(size):
    with pytest.raises(p.ValidationError):
        replace(NEW_MESSAGES[0], size_bytes=size)
    obj = json.loads(p.encode_message(NEW_MESSAGES[0]))
    obj["payload"]["size_bytes"] = size
    with pytest.raises(p.ValidationError):
        p.decode_message(json.dumps(obj).encode())


@pytest.mark.parametrize("message", NEW_MESSAGES, ids=lambda m: type(m).__name__)
def test_new_message_schema_correlation_version_and_duplicate_keys(message):
    wrong = "unrequested" if type(message) is p.PrepareReceive else None
    with pytest.raises(p.ValidationError):
        replace(message, correlation_id=wrong)
    for version in (True, "1", 999):
        obj = json.loads(p.encode_message(message))
        obj["protocol_version"] = version
        with pytest.raises(p.ValidationError):
            p.decode_message(json.dumps(obj).encode())
    obj = json.loads(p.encode_message(message))
    obj["payload"]["unknown_field"] = True
    with pytest.raises(p.ValidationError):
        p.decode_message(json.dumps(obj).encode())
    payload = p.encode_message(message).replace(
        b'"transfer_id":"transfer-1"',
        b'"transfer_id":"transfer-1","transfer_id":"conflicting-transfer"',
        1,
    )
    with pytest.raises(p.ValidationError, match="duplicate"):
        p.decode_message(payload)


@pytest.mark.parametrize("code", list(p.TransferFailureCode))
def test_preparation_failures_reuse_existing_codes(code):
    message = replace(NEW_MESSAGES[2], code=code)
    assert exchange((message,))[0].code is code


@pytest.mark.parametrize("code", ["io_error", "unknown", 1, None])
def test_wrong_preparation_failure_enum_type(code):
    with pytest.raises(p.ValidationError):
        replace(NEW_MESSAGES[2], code=code)
    if code != "io_error":
        obj = json.loads(p.encode_message(NEW_MESSAGES[2]))
        obj["payload"]["code"] = code
        with pytest.raises(p.ValidationError):
            p.decode_message(json.dumps(obj).encode())


def test_failure_detail_limit_and_deterministic_encoding():
    message = replace(NEW_MESSAGES[2], detail="x" * p.MAX_DETAIL_BYTES)
    assert p.encode_message(exchange((message,))[0]) == p.encode_message(message)
    with pytest.raises(p.ResourceLimitExceeded):
        replace(message, detail=message.detail + "x")
