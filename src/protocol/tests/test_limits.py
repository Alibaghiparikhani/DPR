from dataclasses import replace
import json

import pytest
import protocol as p
from execution import FailureInfo, FailureKind, TaskFailure, TaskSuccess
from .samples import ATTEMPT, MESSAGES, PROGRAM, WORKER


@pytest.mark.parametrize(
    "text",
    [
        "a" * p.MAX_IDENTIFIER_BYTES,
        "ü" * (p.MAX_IDENTIFIER_BYTES // 2),
        "🛰" * (p.MAX_IDENTIFIER_BYTES // 4),
        "x space /:..#? 🐍",
    ],
)
def test_maximum_and_opaque_unicode_identifiers(text):
    message = p.WorkerGoodbye(text, message_id="m")
    assert p.decode_message(p.encode_message(message)) == message


@pytest.mark.parametrize(
    "text",
    [
        "a" * (p.MAX_IDENTIFIER_BYTES + 1),
        "ü" * (p.MAX_IDENTIFIER_BYTES // 2 + 1),
        "\ud800",
        "x\x00",
        "x\n",
        "x\x7f",
        "x\x85",
    ],
)
def test_invalid_identifiers_at_both_boundaries(text):
    with pytest.raises(p.ValidationError):
        p.WorkerGoodbye(text, message_id="m")
    obj = json.loads(p.encode_message(MESSAGES[6]))
    obj["payload"]["worker_id"] = text
    with pytest.raises(p.ValidationError):
        p.decode_message(json.dumps(obj).encode())


def test_text_controls_and_unicode_preserved_without_normalization():
    for text in ("\x00\n\t\x1b[31m", "e\u0301", "é", "🐍\u2028\u2029"):
        m = p.WorkerGoodbye("W", text, message_id="m")
        assert p.decode_message(p.encode_message(m)).reason == text
    assert p.encode_message(
        p.WorkerGoodbye("W", "é", message_id="m")
    ) != p.encode_message(p.WorkerGoodbye("W", "e\u0301", message_id="m"))


def test_traceback_and_detail_boundaries():
    info = FailureInfo(
        FailureKind.EXECUTION_ERROR,
        "x" * p.MAX_DETAIL_BYTES,
        traceback_text="t" * p.MAX_TRACEBACK_BYTES,
    )
    message = p.TaskFailed(
        "W1", TaskFailure(ATTEMPT, info), message_id="m", correlation_id="c"
    )
    assert p.decode_message(p.encode_message(message)) == message
    for changed in (
        replace(info, message=info.message + "x"),
        replace(info, traceback_text=info.traceback_text + "t"),
    ):
        with pytest.raises(p.ResourceLimitExceeded):
            replace(message, result=TaskFailure(ATTEMPT, changed))


def test_filename_metadata_boundary():
    program = replace(PROGRAM, filename="f" * p.MAX_METADATA_BYTES)
    message = replace(MESSAGES[7], program=program)
    assert p.decode_message(p.encode_message(message)) == message
    with pytest.raises(p.ResourceLimitExceeded):
        replace(message, program=replace(program, filename=program.filename + "f"))


def test_collection_boundaries_before_frozenset_normalization():
    ids = tuple(f"v{i}" for i in range(p.MAX_COLLECTION_ITEMS))
    message = p.TaskSucceeded(
        "W1", TaskSuccess(ATTEMPT, ids), message_id="m", correlation_id="c"
    )
    assert p.decode_message(p.encode_message(message)) == message
    with pytest.raises(p.ResourceLimitExceeded):
        replace(message, result=TaskSuccess(ATTEMPT, ids + ("extra",)))
    obj = json.loads(p.encode_message(message))
    obj["payload"]["result"]["output_ids"].append("extra")
    with pytest.raises(p.ResourceLimitExceeded):
        p.decode_message(json.dumps(obj).encode())


def test_aggregate_encoding_budget_and_direct_payload_budget():
    ids = frozenset(f"{i:04}" + "x" * 252 for i in range(p.MAX_COLLECTION_ITEMS))
    message = p.Heartbeat(replace(WORKER, environment_ids=ids), 1, message_id="m")
    with pytest.raises(p.ResourceLimitExceeded):
        p.encode_message(message)
    with pytest.raises(p.ResourceLimitExceeded):
        p.decode_message(b" " * (p.MAX_FRAME_PAYLOAD + 1))
    # An exact maximum payload is permitted even when noncanonical whitespace is present.
    encoded = p.encode_message(MESSAGES[6])
    assert (
        p.decode_message(encoded + b" " * (p.MAX_FRAME_PAYLOAD - len(encoded)))
        == MESSAGES[6]
    )


@pytest.mark.parametrize(
    "payload",
    [
        b"[" * (p.MAX_JSON_DEPTH + 1) + b"0" + b"]" * (p.MAX_JSON_DEPTH + 1),
        b'"' + b"a" * (p.MAX_TRACEBACK_BYTES + 1) + b'"',
        b"[" + b"0," * p.MAX_COLLECTION_ITEMS + b"0]",
        b"{" + b",".join(b'"k%d":0' % i for i in range(p.MAX_OBJECT_FIELDS + 1)) + b"}",
        b"9" * (p.MAX_NUMBER_CHARS + 1),
        b"0." + b"1" * p.MAX_NUMBER_CHARS,
        str(p.MAX_INTEGER + 1).encode(),
        str(-p.MAX_INTEGER - 2).encode(),
    ],
    ids=lambda payload: f"payload_len_{len(payload)}",
)
def test_json_resource_guards(payload):
    with pytest.raises(p.ResourceLimitExceeded):
        p.decode_message(payload)


def test_json_node_budget_across_many_small_containers():
    payload = b"[" + b",".join(b"[" + b"0," * 63 + b"0]" for _ in range(1100)) + b"]"
    assert len(payload) < p.MAX_FRAME_PAYLOAD
    with pytest.raises(p.ResourceLimitExceeded):
        p.decode_message(payload)


def test_max_integer_and_zero_counts():
    for sequence in (0, p.MAX_INTEGER):
        message = p.Heartbeat(WORKER, sequence, message_id="m")
        assert p.decode_message(p.encode_message(message)) == message
    with pytest.raises(p.ValidationError):
        p.Heartbeat(WORKER, p.MAX_INTEGER + 1, message_id="m")
