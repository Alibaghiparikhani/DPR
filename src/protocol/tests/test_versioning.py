from dataclasses import replace
import json

import pytest
import protocol as p
from .samples import MESSAGES


def test_exact_supported_version_and_explicit_negotiation():
    assert p.PROTOCOL_VERSION == 1
    assert p.SUPPORTED_VERSIONS == (1,)
    assert p.negotiate_version((4, 1, 2)) == 1
    assert p.negotiate_version((1,)) == 1
    p.validate_version(1)


@pytest.mark.parametrize(
    "version", [0, -1, 2, 65535, 65536, 999999, True, False, "1", 1.0, None, [], {}]
)
def test_invalid_versions_on_constructor_and_wire(version):
    with pytest.raises(p.ValidationError):
        replace(MESSAGES[-1], protocol_version=version)
    wire = json.loads(p.encode_message(MESSAGES[-1]))
    wire["protocol_version"] = version
    with pytest.raises(p.ValidationError):
        p.decode_message(json.dumps(wire).encode())


@pytest.mark.parametrize(
    "versions",
    [
        (),
        (2,),
        (65535,),
        (1, 1),
        (True,),
        (1.0,),
        ("1",),
        (0,),
        (-1,),
        (65536,),
        [1],
        {1},
        None,
        tuple(range(1, 34)),
    ],
)
def test_reject_bad_version_offers(versions):
    with pytest.raises(p.ValidationError):
        p.negotiate_version(versions)


def test_version_error_is_typed_and_precedes_unknown_type():
    obj = json.loads(p.encode_message(MESSAGES[-1]))
    obj.update(protocol_version=2, message_type="future.type")
    with pytest.raises(p.UnsupportedProtocolVersion):
        p.decode_message(json.dumps(obj).encode())
    obj.pop("protocol_version")
    with pytest.raises(p.ValidationError):
        p.decode_message(json.dumps(obj).encode())
