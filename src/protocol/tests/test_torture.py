import ast
from dataclasses import replace
import json
from pathlib import Path
import random
import subprocess
import sys

import pytest
import protocol as p
from .samples import MESSAGES, WORKER


def test_ten_thousand_valid_frames_in_one_chunk():
    message = MESSAGES[6]
    encoded = p.encode_message(message)
    decoder = p.FrameDecoder()
    result = decoder.feed(p.frame_payload(encoded) * 10_000)
    assert len(result) == 10_000
    assert all(p.decode_message(payload) == message for payload in result)
    assert decoder.buffered_bytes == 0
    decoder.finish()


@pytest.mark.parametrize("seed", [8, 1729, 40961])
def test_generated_valid_message_sequences(seed):
    rng = random.Random(seed)
    messages = []
    for i in range(250):
        total = rng.randrange(1, 500)
        running = rng.randrange(total + 1)
        reserved = rng.randrange(total - running + 1)
        worker = replace(
            WORKER,
            running_slots=running,
            reserved_slots=reserved,
            total_slots=total,
            cpu_percent=rng.uniform(0, 100),
            worker_id=f"worker-{rng.randrange(40)}-ü",
        )
        messages.append(p.Heartbeat(worker, i, message_id=f"{seed}-{i}"))
    wire = b"".join(p.frame_payload(p.encode_message(m)) for m in messages)
    decoder = p.FrameDecoder()
    offset = 0
    result = []
    while offset < len(wire):
        count = rng.randrange(1, 800)
        result.extend(
            p.decode_message(b) for b in decoder.feed(wire[offset : offset + count])
        )
        offset += count
    decoder.finish()
    assert result == messages


def test_seeded_random_non_message_bytes_fail_with_typed_errors():
    rng = random.Random(6773)
    for _ in range(1500):
        data = rng.randbytes(rng.randrange(0, 800))
        with pytest.raises(p.ProtocolError):
            p.decode_message(data)
        decoder = p.FrameDecoder()
        try:
            decoder.feed(data)
            decoder.finish()
        except p.FramingError:
            assert decoder.failed


def test_seeded_byte_mutations_either_reject_or_roundtrip_to_a_known_type():
    rng = random.Random(117)
    for message in MESSAGES:
        original = p.encode_message(message)
        for _ in range(30):
            payload = bytearray(original)
            for _ in range(rng.randrange(1, 5)):
                payload[rng.randrange(len(payload))] = rng.randrange(256)
            try:
                result = p.decode_message(bytes(payload))
            except p.ProtocolError:
                continue
            assert type(result) in p.MESSAGE_TYPES.values()
            assert p.decode_message(p.encode_message(result)) == result


def test_delimiters_and_escapes_inside_text_do_not_consume_structure_budget():
    reason = ("[[{{" * 500) + '\\"' * 500 + ("}}]]" * 500)
    message = p.WorkerGoodbye("W1", reason, message_id="m")
    assert p.decode_message(p.encode_message(message)) == message
    escaped = json.dumps(
        json.loads(p.encode_message(message)), ensure_ascii=True
    ).encode()
    assert p.decode_message(escaped) == message


def test_invalid_wire_values_never_trigger_imports_or_execution(tmp_path):
    marker = tmp_path / "should-never-exist"
    text = f"__import__('pathlib').Path({str(marker)!r}).touch()"
    message = p.WorkerGoodbye("W1", text, message_id="m")
    assert p.decode_message(p.encode_message(message)).reason == text
    obj = json.loads(p.encode_message(message))
    obj["payload"]["__class__"] = "pathlib.Path"
    with pytest.raises(p.ValidationError):
        p.decode_message(json.dumps(obj).encode())
    assert not marker.exists()


def test_import_and_static_production_boundary():
    root = Path(__file__).resolve().parents[1]
    forbidden = {
        "pickle",
        "marshal",
        "socket",
        "subprocess",
        "multiprocessing",
        "asyncio",
        "threading",
        "importlib",
        "random",
        "time",
        "uuid",
        "dag_runtime.dag_engine",
        "dag_runtime.dag_static",
    }
    for filename in root.glob("*.py"):
        tree = ast.parse(filename.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                assert not {n.name for n in node.names}.intersection(forbidden), (
                    filename
                )
            elif isinstance(node, ast.ImportFrom):
                assert node.module not in forbidden, filename
            elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                assert node.func.id not in {
                    "eval",
                    "exec",
                    "compile",
                    "__import__",
                    "open",
                }, filename
    # Compare import delta so interpreter startup's own modules cannot cause false failures.
    script = """import sys
before = set(sys.modules)
import protocol
added = set(sys.modules) - before
assert not added.intersection({'socket', 'pickle', 'subprocess', 'multiprocessing', 'asyncio', 'dag_runtime.dag_engine', 'dag_runtime.dag_static'})
assert protocol.PROTOCOL_VERSION == 1
"""
    subprocess.run([sys.executable, "-S", "-c", script], cwd=root.parent, check=True)


def test_missing_local_attributes_raise_typed_errors():
    with pytest.raises(p.ValidationError):
        p.encode_message(object.__new__(p.WorkerGoodbye))


def test_released_memoryview_is_a_predictable_caller_error():
    chunk = memoryview(b"data")
    chunk.release()
    decoder = p.FrameDecoder()
    with pytest.raises(p.FramingError):
        decoder.feed(chunk)
    assert not decoder.failed
