from dataclasses import fields, replace
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

import protocol as p
from protocol.codec import _build_registry
from protocol.messages import DEFINITIONS, SCHEMAS
from .samples import MESSAGES, WORKER, ENDPOINT


@pytest.mark.parametrize("message", MESSAGES, ids=lambda m: type(m).__name__)
def test_round_trip_and_deterministic_bytes(message):
    encoded = p.encode_message(message)
    decoded = p.decode_message(encoded)
    assert type(decoded) is type(message)
    assert decoded == message
    assert p.encode_message(decoded) == encoded == p.encode_message(replace(message))
    assert isinstance(encoded, bytes)


@pytest.mark.parametrize("message", MESSAGES, ids=lambda m: type(m).__name__)
def test_every_message_fragmented_round_trip(message):
    frame = p.frame_payload(p.encode_message(message))
    decoder = p.FrameDecoder()
    result = []
    for byte in frame:
        result.extend(p.decode_message(b) for b in decoder.feed(bytes([byte])))
    decoder.finish()
    assert result == [message]


def test_exact_golden_wire_and_network_byte_order():
    message = p.WorkerGoodbye("W1", message_id="m1")
    golden = (
        b'{"correlation_id":null,"message_id":"m1","message_type":"worker.goodbye",'
        b'"payload":{"reason":"","worker_id":"W1"},"protocol_version":1}'
    )
    assert p.encode_message(message) == golden
    assert p.frame_payload(golden) == len(golden).to_bytes(4, "big") + golden


def test_keys_and_set_order_do_not_affect_encoding():
    message = p.WorkerHello(WORKER, ENDPOINT, message_id="m")
    obj = json.loads(p.encode_message(message))
    obj["payload"]["worker"]["supported_modes"].reverse()
    obj["payload"]["worker"]["environment_ids"].reverse()
    reversed_keys = json.dumps(dict(reversed(list(obj.items()))), indent=2).encode()
    assert p.encode_message(p.decode_message(reversed_keys)) == p.encode_message(
        message
    )


@pytest.mark.parametrize("cpu", [0, -0.0, 0.0])
def test_equivalent_numeric_values_are_canonical(cpu):
    one = p.Heartbeat(replace(WORKER, cpu_percent=cpu), 1, message_id="m")
    other = replace(one, worker=replace(WORKER, cpu_percent=0.0))
    assert one == other
    assert p.encode_message(one) == p.encode_message(other)


def test_complete_closed_registry_and_schema_field_alignment():
    assert {type(m) for m in MESSAGES} == set(p.MESSAGE_TYPES.values())
    assert len(MESSAGES) == len(p.MESSAGE_TYPES)
    for cls, schema in SCHEMAS.items():
        assert {f.name for f in fields(cls)} == {name for name, _ in schema.fields}
    with pytest.raises(TypeError):
        p.MESSAGE_TYPES["bad"] = p.WorkerGoodbye
    with pytest.raises(TypeError):
        SCHEMAS[p.WorkerGoodbye] = None


@pytest.mark.parametrize("change", ["wire", "class", "same", "correlation"])
def test_duplicate_or_invalid_registry_rejected(change):
    first, second = DEFINITIONS[:2]
    bad = {
        "wire": replace(second, wire_type=first.wire_type),
        "class": replace(second, cls=first.cls),
        "same": first,
        "correlation": replace(second, correlation="anything"),
    }[change]
    with pytest.raises(p.RegistryError):
        _build_registry((first, bad))


@pytest.mark.parametrize(
    "obj", [None, {}, [], "worker.goodbye", 2, object(), ValueError("user failure")]
)
def test_unsupported_python_objects(obj):
    with pytest.raises(p.EncodingError):
        p.encode_message(obj)


def test_hash_seed_determinism_in_separate_interpreters():
    script = "from protocol.tests.samples import MESSAGES; from protocol import encode_message; print(b'\\n'.join(map(encode_message, MESSAGES)).hex())"
    outputs = [
        subprocess.check_output(
            [sys.executable, "-c", script],
            cwd=Path(__file__).resolve().parents[2],
            env={**os.environ, "PYTHONHASHSEED": seed},
        )
        for seed in ("1", "917", "0")
    ]
    assert outputs[0] == outputs[1] == outputs[2]
