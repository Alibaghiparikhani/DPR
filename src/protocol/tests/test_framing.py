import array
import random

import pytest
import protocol as p
from .samples import MESSAGES


def collect(parts):
    decoder = p.FrameDecoder()
    result = []
    for part in parts:
        result.extend(decoder.feed(part))
        assert decoder.buffered_bytes <= decoder.max_payload + p.HEADER_SIZE
    decoder.finish()
    return result


def test_every_split_position_for_every_message():
    for message in MESSAGES:
        payload = p.encode_message(message)
        frame = p.frame_payload(payload)
        for split in range(len(frame) + 1):
            assert collect((frame[:split], frame[split:])) == [payload], (
                type(message),
                split,
            )


@pytest.mark.parametrize("seed", range(12))
def test_concatenated_frames_random_chunking_and_coalescing(seed):
    rng = random.Random(seed)
    payloads = [p.encode_message(m) for m in MESSAGES]
    rng.shuffle(payloads)
    data = b"".join(p.frame_payload(b) for b in payloads)
    chunks = []
    offset = 0
    while offset < len(data):
        length = rng.randint(1, 3000)
        chunks.append(data[offset : offset + length])
        offset += length
    assert collect(chunks) == payloads
    assert collect((data,)) == payloads


def test_partial_header_body_and_one_and_half_frames():
    decoder = p.FrameDecoder()
    frame = p.frame_payload(b"abcdefgh")
    assert decoder.feed(b"") == []
    assert decoder.feed(frame[:1]) == []
    assert decoder.buffered_bytes == 1
    assert decoder.feed(frame[1:4]) == []
    assert decoder.buffered_bytes == 0
    assert decoder.feed(frame[4:7]) == []
    assert decoder.buffered_bytes == 3
    assert decoder.feed(frame[7:] + frame + frame[:6]) == [b"abcdefgh"] * 2
    assert decoder.buffered_bytes == 2
    assert decoder.feed(frame[6:]) == [b"abcdefgh"]
    decoder.finish()
    decoder.finish()
    assert decoder.closed
    with pytest.raises(p.DecoderStateError):
        decoder.feed(b"")
    decoder.reset()
    assert not decoder.closed and not decoder.failed
    assert decoder.feed(frame) == [b"abcdefgh"]


@pytest.mark.parametrize("length", [0, 1, 2, 3, 4, 5, 8, 9])
def test_finish_detects_truncation_and_reset_abandons_old_stream(length):
    frame = p.frame_payload(b"123456")
    decoder = p.FrameDecoder()
    decoder.feed(frame[:length])
    if length:
        with pytest.raises(p.TruncatedFrame):
            decoder.finish()
        assert decoder.failed and decoder.buffered_bytes == 0
        with pytest.raises(p.DecoderStateError):
            decoder.finish()
        with pytest.raises(p.DecoderStateError):
            decoder.feed(frame)
    else:
        decoder.finish()
        assert decoder.closed
    decoder.reset()
    assert decoder.feed(frame) == [b"123456"]
    decoder.finish()


def test_reset_while_partial():
    decoder = p.FrameDecoder()
    decoder.feed(p.frame_payload(b"old")[:5])
    decoder.reset()
    assert decoder.buffered_bytes == 0
    assert decoder.feed(p.frame_payload(b"new")) == [b"new"]


@pytest.mark.parametrize("declared", [0, p.MAX_FRAME_PAYLOAD + 1, 2**32 - 1])
def test_bad_header_rejected_immediately_and_valid_prefix_preserved(declared):
    decoder = p.FrameDecoder()
    prefix = p.frame_payload(b"first") + p.frame_payload(b"second")
    bad = declared.to_bytes(4, "big")
    expected = p.FramingError if declared == 0 else p.FrameTooLarge
    with pytest.raises(expected) as caught:
        decoder.feed(prefix + bad + p.frame_payload(b"must not parse"))
    assert caught.value.completed_frames == (b"first", b"second")
    assert decoder.failed and decoder.buffered_bytes == 0
    with pytest.raises(p.DecoderStateError):
        decoder.feed(p.frame_payload(b"next"))
    decoder.reset()
    assert decoder.feed(p.frame_payload(b"new stream")) == [b"new stream"]


def test_oversize_header_rejection_at_last_header_byte():
    header = (2**32 - 1).to_bytes(4, "big")
    decoder = p.FrameDecoder()
    assert decoder.feed(header[:3]) == []
    with pytest.raises(p.FrameTooLarge):
        decoder.feed(header[3:])
    assert decoder.buffered_bytes == 0


def test_maximum_legal_frame_and_no_declared_size_allocation():
    body = b"x" * p.MAX_FRAME_PAYLOAD
    frame = p.frame_payload(body)
    decoder = p.FrameDecoder()
    assert decoder.feed(frame[:4]) == []
    assert decoder.buffered_bytes == 0
    assert decoder.feed(frame[4:5]) == []
    assert decoder.buffered_bytes == 1
    assert decoder.feed(frame[5:]) == [body]
    decoder.finish()
    with pytest.raises(p.FrameTooLarge):
        p.frame_payload(body + b"x")


@pytest.mark.parametrize(
    "maximum", [0, -1, True, 1.0, "12", None, p.MAX_FRAME_PAYLOAD + 1]
)
def test_invalid_configured_limit(maximum):
    with pytest.raises(p.FramingError):
        p.FrameDecoder(max_payload=maximum)
    with pytest.raises(p.FramingError):
        p.frame_payload(b"x", max_payload=maximum)


def test_smaller_local_limit_and_zero_payload():
    assert p.FrameDecoder(max_payload=1).feed(p.frame_payload(b"x", max_payload=1)) == [
        b"x"
    ]
    with pytest.raises(p.FrameTooLarge):
        p.FrameDecoder(max_payload=1).feed(p.frame_payload(b"xy"))
    with pytest.raises(p.FramingError):
        p.frame_payload(b"")


@pytest.mark.parametrize(
    "chunk",
    ["x", 4, [1], None, memoryview(b"1234")[::2], memoryview(array.array("I", [1, 2]))],
)
def test_invalid_chunk_types_do_not_damage_stream(chunk):
    decoder = p.FrameDecoder()
    with pytest.raises(p.FramingError):
        decoder.feed(chunk)
    assert not decoder.failed
    assert decoder.feed(p.frame_payload(b"good")) == [b"good"]


def test_mutable_input_is_copied_not_retained():
    data = bytearray(p.frame_payload(b"abcdef"))
    decoder = p.FrameDecoder()
    assert decoder.feed(memoryview(data)[:6]) == []
    data[4:6] = b"XX"
    assert decoder.feed(memoryview(data)[6:]) == [b"abcdef"]


def test_framing_does_not_claim_payload_is_valid_json():
    decoder = p.FrameDecoder()
    parts = decoder.feed(
        p.frame_payload(p.encode_message(MESSAGES[0])) + p.frame_payload(b"not json")
    )
    assert p.decode_message(parts[0]) == MESSAGES[0]
    with pytest.raises(p.DecodingError):
        p.decode_message(parts[1])
    assert not decoder.failed  # framing and message policy are separate
    assert decoder.feed(p.frame_payload(b"next")) == [b"next"]
