"""Deterministic UTF-8 JSON control codec with a frozen local class allowlist."""

from __future__ import annotations

import json
import math
from types import MappingProxyType
from typing import Mapping

from .errors import (
    DecodingError,
    EncodingError,
    RegistryError,
    ResourceLimitExceeded,
    UnknownMessageType,
    ValidationError,
)
from .limits import (
    MAX_COLLECTION_ITEMS,
    MAX_FRAME_PAYLOAD,
    MAX_INTEGER,
    MAX_JSON_DEPTH,
    MAX_JSON_NODES,
    MAX_NUMBER_CHARS,
    MAX_OBJECT_FIELDS,
    MAX_TRACEBACK_BYTES,
)
from .messages import DEFINITIONS, SCHEMAS, Message, MessageDefinition
from .validation import decode_record, encode_record, exact_object, text_value
from .version import validate_version


def _build_registry(
    definitions: tuple[MessageDefinition, ...],
) -> tuple[Mapping, Mapping]:
    by_wire, by_class = {}, {}
    for definition in definitions:
        text_value(
            definition.wire_type, "wire_type", 128, nonempty=True, identifier=True
        )
        if definition.wire_type in by_wire or definition.cls in by_class:
            raise RegistryError("duplicate wire type or message class")
        if definition.cls not in SCHEMAS or definition.correlation not in (
            "required",
            "none",
            "optional",
        ):
            raise RegistryError("unregistered message schema or correlation rule")
        by_wire[definition.wire_type] = definition
        by_class[definition.cls] = definition
    return MappingProxyType(by_wire), MappingProxyType(by_class)


_BY_WIRE, _BY_CLASS = _build_registry(DEFINITIONS)
MESSAGE_TYPES: Mapping[str, type[Message]] = MappingProxyType(
    {k: d.cls for k, d in _BY_WIRE.items()}
)
_ENVELOPE = (
    "protocol_version",
    "message_type",
    "message_id",
    "correlation_id",
    "payload",
)


def encode_message(message: Message) -> bytes:
    """Revalidate exact registered records and produce deterministic wire-v1 bytes."""
    definition = _BY_CLASS.get(type(message))
    if definition is None:
        raise EncodingError("unsupported Python message class")
    fields = encode_record(message, definition.cls, SCHEMAS)
    envelope = {
        "protocol_version": fields.pop("protocol_version"),
        "message_type": definition.wire_type,
        "message_id": fields.pop("message_id"),
        "correlation_id": fields.pop("correlation_id"),
        "payload": fields,
    }
    encoder = json.JSONEncoder(
        ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":")
    )
    output = bytearray()
    for piece in encoder.iterencode(envelope):
        encoded = piece.encode("utf-8")
        if len(output) + len(encoded) > MAX_FRAME_PAYLOAD:
            raise ResourceLimitExceeded("encoded message exceeds maximum payload")
        output.extend(encoded)
    return bytes(output)


def _guard_structure(text: str) -> None:
    """Linear lexical budgets BEFORE json.loads can allocate a nested tree.

    This is deliberately not another JSON grammar; json.loads remains the
    syntax authority. Delimiters inside strings do not count toward nesting.
    """
    stack: list[list] = []
    in_string = escaped = in_atom = False
    nodes = 0
    for char in text:
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue
        if char in " \t\r\n":
            in_atom = False
            continue
        if char in '[{"' or (char not in ",:]}" and not in_atom):
            nodes += 1
            if nodes > MAX_JSON_NODES:
                raise ResourceLimitExceeded("JSON node budget exceeded")
        if char == '"':
            in_string = True
            in_atom = False
        elif char in "[{":
            stack.append([char, 1])
            if len(stack) > MAX_JSON_DEPTH:
                raise ResourceLimitExceeded("JSON nesting budget exceeded")
            in_atom = False
        elif char in "]}":
            if not stack or stack[-1][0] != ("[" if char == "]" else "{"):
                raise DecodingError("mismatched JSON delimiters")
            stack.pop()
            in_atom = False
        elif char == ",":
            if stack:
                stack[-1][1] += 1
                limit = (
                    MAX_COLLECTION_ITEMS if stack[-1][0] == "[" else MAX_OBJECT_FIELDS
                )
                if stack[-1][1] > limit:
                    raise ResourceLimitExceeded("JSON container budget exceeded")
            in_atom = False
        elif char == ":":
            in_atom = False
        else:
            in_atom = True


def _integer(token: str) -> int:
    if len(token) > MAX_NUMBER_CHARS:
        raise ResourceLimitExceeded("JSON numeric token budget exceeded")
    value = int(token)
    if not -MAX_INTEGER - 1 <= value <= MAX_INTEGER:
        raise ResourceLimitExceeded("JSON integer exceeds signed 64-bit range")
    return value


def _real(token: str) -> float:
    if len(token) > MAX_NUMBER_CHARS:
        raise ResourceLimitExceeded("JSON numeric token budget exceeded")
    value = float(token)
    if not math.isfinite(value):
        raise ValidationError("non-finite JSON number")
    return value


def _constant(token: str) -> None:
    raise ValidationError("non-finite JSON constant")


def _object(pairs: list[tuple[str, object]]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValidationError("duplicate JSON object key")
        result[key] = value
    return result


def _check_texts(value: object) -> None:
    pending = [value]
    while pending:
        current = pending.pop()
        if type(current) is str:
            text_value(current, "JSON text", MAX_TRACEBACK_BYTES)
        elif type(current) is dict:
            pending.extend(current.keys())
            pending.extend(current.values())
        elif type(current) is list:
            pending.extend(current)


def decode_message(payload: bytes) -> Message:
    """Decode untrusted bounded bytes without executing/deserializing user code."""
    if type(payload) is not bytes:
        raise DecodingError("payload must be bytes")
    if not payload:
        raise DecodingError("empty message payload")
    if len(payload) > MAX_FRAME_PAYLOAD:
        raise ResourceLimitExceeded("message exceeds maximum payload")
    try:
        text = payload.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        raise DecodingError("invalid UTF-8") from None
    _guard_structure(text)
    try:
        obj = json.loads(
            text,
            object_pairs_hook=_object,
            parse_int=_integer,
            parse_float=_real,
            parse_constant=_constant,
        )
    except (json.JSONDecodeError, RecursionError):
        raise DecodingError("invalid JSON message") from None
    _check_texts(obj)
    envelope = exact_object(obj, _ENVELOPE, "envelope")
    validate_version(envelope["protocol_version"])
    wire_type = text_value(
        envelope["message_type"], "message_type", 128, nonempty=True, identifier=True
    )
    definition = _BY_WIRE.get(wire_type)
    if definition is None:
        raise UnknownMessageType("unknown message_type")
    body = exact_object(
        envelope["payload"], tuple(n for n, _ in definition.fields), "payload"
    )
    fields = dict(body)
    fields.update(
        {
            name: envelope[name]
            for name in ("message_id", "correlation_id", "protocol_version")
        }
    )
    return decode_record(fields, definition.cls, SCHEMAS)
