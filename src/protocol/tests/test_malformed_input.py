from copy import deepcopy
import json

import pytest
import protocol as p
from .samples import MESSAGES, WORKER


def raw(obj):
    return json.dumps(obj, ensure_ascii=True, separators=(",", ":")).encode()


def objects(value):
    if type(value) is dict:
        yield value
        for child in value.values():
            yield from objects(child)
    elif type(value) is list:
        for child in value:
            yield from objects(child)


@pytest.mark.parametrize("message", MESSAGES, ids=lambda m: type(m).__name__)
def test_all_nested_schema_objects_reject_unknown_and_missing_fields(message):
    original = json.loads(p.encode_message(message))
    count = len(list(objects(original)))
    for index in range(count):
        obj = deepcopy(original)
        list(objects(obj))[index]["__unknown_typo__"] = 1
        with pytest.raises(p.ValidationError):
            p.decode_message(raw(obj))
        for key in list(list(objects(original))[index]):
            obj = deepcopy(original)
            del list(objects(obj))[index][key]
            with pytest.raises(p.ValidationError):
                p.decode_message(raw(obj))


@pytest.mark.parametrize(
    "payload",
    [
        b"",
        b" \t\n",
        b"\xff",
        b"\xc0\xaf",
        b"\xed\xa0\x80",
        b"\xef\xbb\xbf{}",
        b"null",
        b"[]",
        b"1",
        b"true",
        b'"text"',
        b"{",
        b"}",
        b"[}",
        b"{}{}",
        b"{} trailing",
        b'{"x":}',
        b"{'x': 1}",
        b'{"x":1,}',
        b'{"x":"raw\x00control"}',
        b'{"x":1,"x":2}',
        b'{"x":1,"\\u0078":2}',
        b'{"x":{"y":1,"y":2}}',
        b'{"x":NaN}',
        b'{"x":Infinity}',
        b'{"x":-Infinity}',
        b'{"x":1e9999}',
        b'{"x":-1e9999}',
        b'{"x":"\\ud800"}',
        b'{"x":"\\udfff"}',
        b"\x80\x04cos\nsystem\n(S'bad'\ntR.",
    ],
)
def test_malformed_payloads_fail_as_protocol_errors(payload):
    with pytest.raises(p.DecodingError):
        p.decode_message(payload)


@pytest.mark.parametrize(
    "payload", ["{}", bytearray(b"{}"), memoryview(b"{}"), None, {}, 0]
)
def test_codec_requires_bytes(payload):
    with pytest.raises(p.DecodingError):
        p.decode_message(payload)


@pytest.mark.parametrize(
    "kind",
    ["unknown", "tas_started", "os.system", "__import__", "protocol.WorkerGoodbye"],
)
def test_unknown_wire_type(kind):
    obj = json.loads(p.encode_message(MESSAGES[0]))
    obj["message_type"] = kind
    with pytest.raises(p.UnknownMessageType):
        p.decode_message(raw(obj))


@pytest.mark.parametrize("kind", ["", " ", None, [], 1, True])
def test_invalid_wire_type(kind):
    obj = json.loads(p.encode_message(MESSAGES[0]))
    obj["message_type"] = kind
    with pytest.raises(p.ValidationError):
        p.decode_message(raw(obj))


@pytest.mark.parametrize(
    "field,invalid",
    [
        ("total_slots", [-4, "4", 1.0, True, None]),
        ("running_slots", [-1, 1000]),
        ("reserved_slots", [-1, 1000]),
        ("cpu_percent", [-1, 101, "40", True, None, float("nan"), float("inf")]),
        ("available_memory_bytes", [-1, 8193, "0", 0.0]),
        ("cpu_cores", [0, -1, True, "4", p.MAX_CAPACITY + 1]),
        ("online", [1, 0, "true", None]),
        ("accepting_work", [1, "false"]),
        ("environment_ids", ["env", ["x", "x"], [1], {}, None]),
        ("prepared_program_ids", [["not-a-hash"], ["a" * 64, "a" * 64]]),
        (
            "supported_modes",
            [
                ["shared_namespace"],
                ["native_region", "native_region"],
                [1],
                "isolated_candidate",
            ],
        ),
    ],
)
def test_bad_worker_fields(field, invalid):
    for value in invalid:
        obj = json.loads(p.encode_message(MESSAGES[0]))
        obj["payload"]["worker"][field] = value
        with pytest.raises(p.DecodingError):
            p.decode_message(raw(obj))


@pytest.mark.parametrize("field", ["plan_id", "run_id", "task_id", "attempt_id"])
def test_malformed_attempt_identity_fields(field):
    for value in (
        None,
        "",
        " ",
        1,
        True,
        [],
        {},
        "bad" if field == "plan_id" else "x\x00",
    ):
        obj = json.loads(p.encode_message(MESSAGES[18]))
        obj["payload"]["attempt"][field] = value
        with pytest.raises(p.ValidationError):
            p.decode_message(raw(obj))


def test_attempt_is_never_null_or_string():
    for value in (None, "attempt", []):
        obj = json.loads(p.encode_message(MESSAGES[18]))
        obj["payload"]["attempt"] = value
        with pytest.raises(p.ValidationError):
            p.decode_message(raw(obj))


def test_program_hash_identity_must_match_all_components():
    obj = json.loads(p.encode_message(MESSAGES[7]))
    for field in ("id", "source_sha256", "environment_id", "package_id", "filename"):
        changed = deepcopy(obj)
        changed["payload"]["program"][field] = (
            "c" * 64 if field in ("id", "source_sha256") else "different"
        )
        with pytest.raises(p.ValidationError):
            p.decode_message(raw(changed))


@pytest.mark.parametrize(
    "path,value",
    [
        (("result", "output_ids"), ["a", "a"]),
        (("result", "output_ids"), "abc"),
        (("result", "output_ids"), [1]),
        (("result", "attempt"), None),
    ],
)
def test_bad_success_record(path, value):
    obj = json.loads(p.encode_message(MESSAGES[19]))
    obj["payload"][path[0]][path[1]] = value
    with pytest.raises(p.ValidationError):
        p.decode_message(raw(obj))


@pytest.mark.parametrize(
    "field,value",
    [
        ("kind", "wire_failure"),
        ("kind", None),
        ("exception_type", None),
        ("exception_type", ""),
        ("traceback_text", []),
        ("traceback_text", {"frame": "arbitrary"}),
        ("message", 0),
    ],
)
def test_bad_failure_record(field, value):
    obj = json.loads(p.encode_message(MESSAGES[20]))
    obj["payload"]["result"]["failure"][field] = value
    with pytest.raises(p.ValidationError):
        p.decode_message(raw(obj))


def test_failure_is_not_implicitly_a_protocol_error():
    assert not issubclass(p.ProtocolError, RuntimeError)
    obj = json.loads(p.encode_message(MESSAGES[20]))
    obj["message_type"] = "protocol.error"
    with pytest.raises(p.ValidationError):
        p.decode_message(raw(obj))


def test_local_type_coercions_are_rejected():
    for value in (1, ["x"], b"x", None, " ", "a\n"):
        with pytest.raises(p.ValidationError):
            p.WorkerGoodbye(value, message_id="m")
    with pytest.raises(p.ValidationError):
        p.Heartbeat(WORKER, True, message_id="m")
