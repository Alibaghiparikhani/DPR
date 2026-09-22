from __future__ import annotations

import pytest

import protocol as p


def test_authentication_wire_records_round_trip_through_existing_codec_and_framing():
    messages = (
        p.AuthenticationRequest("W1", "intent-1", message_id="r"),
        p.AuthenticationChallenge("W1", "intent-1", "nonce", 100, message_id="c", correlation_id="r"),
        p.AuthenticationProof("W1", "intent-1", "nonce", 100, "ab" * 32, message_id="p", correlation_id="c"),
        p.AuthenticationAccepted("W1", "intent-1", message_id="a", correlation_id="p"),
    )
    decoder = p.FrameDecoder()
    encoded = b"".join(p.frame_payload(p.encode_message(m)) for m in messages)
    decoded = [p.decode_message(payload) for payload in decoder.feed(encoded)]
    decoder.finish()
    assert decoded == list(messages)


def test_authentication_responses_require_correlation():
    with pytest.raises(p.ValidationError):
        p.AuthenticationChallenge("W1", "intent", "nonce", 1, message_id="c")
    with pytest.raises(p.ValidationError):
        p.AuthenticationProof("W1", "intent", "nonce", 1, "ab" * 32, message_id="p")
    with pytest.raises(p.ValidationError):
        p.AuthenticationAccepted("W1", "intent", message_id="a")


def test_authentication_request_is_pre_admission_unsolicited_message():
    with pytest.raises(p.ValidationError):
        p.AuthenticationRequest("W1", "intent", message_id="r", correlation_id="something")
