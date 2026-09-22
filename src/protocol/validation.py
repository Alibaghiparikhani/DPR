"""Explicit local schema machinery shared by constructors and codec.

Only classes present in the supplied immutable schema table can be rebuilt.
No annotation evaluation, arbitrary dataclass traversal, or peer type loading.
"""

from __future__ import annotations

from dataclasses import dataclass
import math
from types import MappingProxyType
from typing import Callable, Mapping

from execution import ExecutionValidationError
from scheduler import SnapshotValidationError

from .errors import RegistryError, ResourceLimitExceeded, ValidationError
from .limits import MAX_COLLECTION_ITEMS, MAX_INTEGER

_MISSING = object()


def require(condition: bool, path: str, reason: str) -> None:
    if not condition:
        raise ValidationError(f"{path}: {reason}")


@dataclass(frozen=True, slots=True)
class Spec:
    kind: str
    cls: type | None = None
    item: Spec | None = None
    minimum: int = 0
    maximum: int = MAX_INTEGER
    unique: bool = False
    identifier: bool = False


@dataclass(frozen=True, slots=True)
class Schema:
    cls: type
    fields: tuple[tuple[str, Spec], ...]
    check: Callable[[object], None] | None = None
    derived: frozenset[str] = frozenset()


def schema_table(entries: tuple[Schema, ...]) -> Mapping[type, Schema]:
    table: dict[type, Schema] = {}
    for entry in entries:
        names = [name for name, _ in entry.fields]
        if entry.cls in table or len(set(names)) != len(names):
            raise RegistryError("duplicate local record class or field")
        if not entry.derived <= set(names):
            raise RegistryError("unknown derived field")
        table[entry.cls] = entry
    return MappingProxyType(table)


def text_value(
    value: object,
    path: str,
    maximum: int,
    *,
    nonempty: bool = False,
    identifier: bool = False,
) -> str:
    require(type(value) is str, path, "expected text")
    if len(value) > maximum:
        raise ResourceLimitExceeded(f"{path}: text byte limit exceeded")
    try:
        length = len(value.encode("utf-8", errors="strict"))
    except UnicodeEncodeError:
        raise ValidationError(f"{path}: Unicode surrogate is forbidden") from None
    if length > maximum:
        raise ResourceLimitExceeded(f"{path}: text byte limit exceeded")
    require(not nonempty or bool(value.strip()), path, "expected nonempty text")
    if identifier:
        require(
            not any(ord(c) < 32 or 127 <= ord(c) <= 159 for c in value),
            path,
            "control character in identifier",
        )
    return value


def convert(
    value: object,
    spec: Spec,
    schemas: Mapping[type, Schema],
    *,
    decode: bool,
    path: str,
) -> object:
    """Validate and convert one field. Wire arrays are never arbitrary iterables."""
    kind = spec.kind
    if kind == "optional":
        return (
            None
            if value is None
            else convert(value, spec.item, schemas, decode=decode, path=path)
        )
    if kind in ("text", "hash"):
        result = text_value(
            value,
            path,
            spec.maximum,
            nonempty=bool(spec.minimum),
            identifier=spec.identifier,
        )
        if kind == "hash":
            require(
                len(result) == 64 and all(c in "0123456789abcdef" for c in result),
                path,
                "expected lowercase SHA-256",
            )
        return result
    if kind == "int":
        require(type(value) is int, path, "expected integer")
        require(spec.minimum <= value <= spec.maximum, path, "integer out of range")
        return value
    if kind == "real":
        require(type(value) in (int, float), path, "expected finite number")
        require(
            spec.minimum <= value <= spec.maximum and math.isfinite(value),
            path,
            "number out of range",
        )
        # 0, -0.0 and 0.0 (likewise 1 and 1.0) have one deterministic encoding.
        return 0.0 if value == 0 else float(value)
    if kind == "bool":
        require(type(value) is bool, path, "expected boolean")
        return value
    if kind == "enum":
        if decode:
            require(type(value) is str, path, "expected enum string")
            try:
                return spec.cls(value)
            except ValueError:
                raise ValidationError(f"{path}: unknown enum value") from None
        require(type(value) is spec.cls, path, "expected exact enum type")
        return value.value
    if kind in ("tuple", "set"):
        expected = list if decode else (tuple if kind == "tuple" else frozenset)
        require(type(value) is expected, path, f"expected {expected.__name__}")
        if len(value) > min(spec.maximum, MAX_COLLECTION_ITEMS):
            raise ResourceLimitExceeded(f"{path}: collection limit exceeded")
        require(len(value) >= spec.minimum, path, "too few collection entries")
        converted = [
            convert(v, spec.item, schemas, decode=decode, path=f"{path}[{i}]")
            for i, v in enumerate(value)
        ]
        # Uniqueness is checked before a wire array could become a frozenset.
        if spec.unique or kind == "set":
            candidates = converted if decode else value
            require(
                len(set(candidates)) == len(value), path, "duplicate collection entries"
            )
        if decode:
            return tuple(converted) if kind == "tuple" else frozenset(converted)
        return converted if kind == "tuple" else sorted(converted)
    if kind == "record":
        return (
            decode_record(value, spec.cls, schemas, path)
            if decode
            else encode_record(value, spec.cls, schemas, path)
        )
    raise RegistryError("unknown local schema kind")


def exact_object(value: object, fields: tuple[str, ...], path: str) -> dict:
    require(type(value) is dict, path, "expected object")
    require(set(value) == set(fields), path, "missing or unknown fields")
    return value


def encode_record(
    value: object, cls: type, schemas: Mapping[type, Schema], path: str = "payload"
) -> dict:
    require(type(value) is cls, path, "expected exact registered record type")
    schema = schemas[cls]
    result = {
        name: convert(
            getattr(value, name, _MISSING),
            spec,
            schemas,
            decode=False,
            path=f"{path}.{name}",
        )
        for name, spec in schema.fields
    }
    if schema.check is not None:
        schema.check(value)
    return result


def decode_record(
    value: object, cls: type, schemas: Mapping[type, Schema], path: str = "payload"
) -> object:
    schema = schemas[cls]
    obj = exact_object(value, tuple(name for name, _ in schema.fields), path)
    kwargs = {
        name: convert(obj[name], spec, schemas, decode=True, path=f"{path}.{name}")
        for name, spec in schema.fields
    }
    try:
        result = cls(**{k: v for k, v in kwargs.items() if k not in schema.derived})
    except (ExecutionValidationError, SnapshotValidationError) as error:
        # Deliberate boundary: parent contracts retain their own exception types.
        raise ValidationError(f"{path}: {error}") from None
    for name in schema.derived:
        require(
            getattr(result, name) == kwargs[name], path, "derived identity mismatch"
        )
    if schema.check is not None:
        schema.check(result)
    return result
