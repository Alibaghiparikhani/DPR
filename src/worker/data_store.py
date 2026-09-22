"""Bounded worker-local storage for exact runtime data representations.

The controller never deserializes Python payloads.  Values are stored as opaque
verified bytes keyed by the exact protocol DataReference and owning control
session.  Child/context processes are the only components that deserialize the
``pickle-v1`` representation.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import shutil
from typing import Iterable

import protocol as p


class DataStoreError(RuntimeError):
    pass


class DataStoreFull(DataStoreError):
    pass


class DataStoreConflict(DataStoreError):
    pass


class DataStoreIntegrityError(DataStoreError):
    pass

_DATA_MARKER = ".dpr-data-store"
_DATA_LOCK = ".dpr-data-store.lock"
_DATA_MARKER_BYTES = b"dpr-data-store-v1\n"


def _write_marker_atomically(marker: Path, payload: bytes) -> None:
    """Create an ownership marker that cannot be observed half-written.

    The previous create-then-write sequence left a zero-byte marker behind if the
    process was killed in between (a force-kill during shutdown is enough), and every
    later start then rejected the directory as foreign.
    """
    temporary = marker.with_name(marker.name + ".tmp")
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        os.write(fd, payload)
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(temporary, marker)


def _acquire_directory_lock(path: Path) -> int:
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        if os.name == "posix":
            import fcntl
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        elif os.name == "nt":
            import msvcrt
            if os.fstat(fd).st_size == 0:
                os.write(fd, b"0")
                os.lseek(fd, 0, os.SEEK_SET)
            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
        return fd
    except BaseException:
        os.close(fd)
        raise


def _initialize_owned_root(root: Path, *, reset_if_unusable: bool = False) -> tuple[int, Path]:
    if root.exists() and (root.is_symlink() or not root.is_dir()):
        raise DataStoreIntegrityError("data store root must be a real directory")
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(root, 0o700)
    marker = root / _DATA_MARKER
    existing = tuple(root.iterdir())
    if existing and not marker.is_file():
        sample = ", ".join(sorted(entry.name for entry in existing)[:3])
        raise DataStoreIntegrityError(
            f"refusing to use {root} as a data directory: it already contains files this worker "
            f"did not create ({sample}...). Point --data-dir at an empty or dpr-owned directory; "
            "each worker needs its own"
        )
    if marker.exists():
        if marker.is_symlink():
            raise DataStoreIntegrityError(f"data-store ownership marker is a symlink: {marker}")
        content = marker.read_bytes()
        if content == b"":
            _write_marker_atomically(marker, _DATA_MARKER_BYTES)
        elif content != _DATA_MARKER_BYTES:
            if not reset_if_unusable:
                raise DataStoreIntegrityError(
                    f"invalid DPR data-store ownership marker at {marker}; delete {root} "
                    "and start again"
                )
            # Session-scoped values only: safe to rebuild rather than block startup.
            for child in list(root.iterdir()):
                if child.is_dir() and not child.is_symlink():
                    shutil.rmtree(child, ignore_errors=True)
                else:
                    child.unlink(missing_ok=True)
            _write_marker_atomically(marker, _DATA_MARKER_BYTES)
    else:
        _write_marker_atomically(marker, _DATA_MARKER_BYTES)
    try:
        lock_fd = _acquire_directory_lock(root / _DATA_LOCK)
    except (OSError, BlockingIOError) as error:
        raise DataStoreError(
            f"data directory {root} is locked by another running worker; give each worker its own "
            "--data-dir (and its own --cache-dir)"
        ) from error
    return lock_fd, marker


@dataclass(frozen=True, slots=True)
class DataStoreLimits:
    max_bytes: int = 256 * 1024 * 1024
    max_items: int = 65536
    max_value_bytes: int = 64 * 1024 * 1024
    verify_chunk_bytes: int = 256 * 1024

    def __post_init__(self) -> None:
        for name in ("max_bytes", "max_items", "max_value_bytes", "verify_chunk_bytes"):
            value = getattr(self, name)
            if type(value) is not int or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        if self.max_value_bytes > self.max_bytes:
            raise ValueError("max_value_bytes cannot exceed max_bytes")


@dataclass(frozen=True, slots=True)
class StoredData:
    data: p.DataReference
    path: Path
    size_bytes: int
    sha256: str
    serialization: str
    session_id: str
    context_id: str | None = None


def data_reference_record(data: p.DataReference) -> dict[str, object]:
    return {
        "plan_id": data.plan_id,
        "run_id": data.run_id,
        "value_id": data.value_id,
        "form": data.form.value,
        "object_state_id": data.object_state_id,
    }


def data_reference_token(data: p.DataReference) -> str:
    raw = json.dumps(
        data_reference_record(data), sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("ascii")
    return hashlib.sha256(raw).hexdigest()


class LocalDataStore:
    """Session-scoped immutable publication of verified opaque value bytes."""

    def __init__(self, root: Path | str, *, limits: DataStoreLimits | None = None,
                 reset_if_unusable: bool = False) -> None:
        self.root = Path(root)
        self.limits = limits or DataStoreLimits()
        # F52: never mutate an arbitrary pre-existing directory. A DPR marker is
        # the ownership proof; the held lock prevents a second worker from purging
        # or writing the same store concurrently.
        self._lock_fd, self._marker_path = _initialize_owned_root(
            self.root, reset_if_unusable=reset_if_unusable)
        self.content = self.root / "content"
        self.staging = self.root / "staging"
        self.content.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.staging.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(self.content, 0o700)
        os.chmod(self.staging, 0o700)
        os.chmod(self._marker_path, 0o600)
        os.chmod(self.root / _DATA_LOCK, 0o600)
        # Runtime data authority is intentionally session/process scoped. Once the
        # marker and exclusive lock prove ownership, stale unindexed content and
        # incomplete staging from the previous process may be discarded safely.
        self._purge_directory(self.content)
        self._purge_directory(self.staging)
        self._entries: dict[p.DataReference, StoredData] = {}
        self._bytes = 0

    def close(self) -> None:
        fd = getattr(self, "_lock_fd", None)
        if fd is None:
            return
        self._lock_fd = None
        try:
            os.close(fd)
        except OSError:
            pass

    def __del__(self):
        self.close()

    @property
    def size_bytes(self) -> int:
        return self._bytes

    @property
    def item_count(self) -> int:
        return len(self._entries)

    def entries(self) -> tuple[StoredData, ...]:
        return tuple(self._entries.values())

    def get(self, data: p.DataReference, *, session_id: str) -> StoredData | None:
        # F10: publication verifies the complete file once, and every child input
        # descriptor carries the certified size/digest which the child verifies
        # again while reading. Re-hashing the whole file on every metadata lookup
        # stalls the worker event loop and turns repeated transfers into O(bytes)
        # control-plane work.
        entry = self._entries.get(data)
        if entry is None or entry.session_id != session_id:
            return None
        return entry

    def has(self, data: p.DataReference, *, session_id: str) -> bool:
        return self.get(data, session_id=session_id) is not None

    def publish_file(
        self,
        data: p.DataReference,
        source: Path | str,
        *,
        size_bytes: int,
        sha256: str,
        session_id: str,
        serialization: str = "pickle-v1",
        context_id: str | None = None,
    ) -> StoredData:
        if type(size_bytes) is not int or size_bytes < 0 or size_bytes > self.limits.max_value_bytes:
            raise DataStoreIntegrityError("value size exceeds configured bound")
        if not self._valid_digest(sha256):
            raise DataStoreIntegrityError("invalid value SHA-256")
        if not session_id:
            raise DataStoreIntegrityError("session_id must be nonempty")
        if serialization not in {"pickle-v1", "dpr-json-v1"}:
            raise DataStoreIntegrityError("unsupported runtime serialization")
        source_path = Path(source)
        actual_size, actual_digest = self._hash_file(source_path, self.limits.max_value_bytes)
        if actual_size != size_bytes or actual_digest != sha256:
            raise DataStoreIntegrityError("runtime value file does not match declared integrity")

        existing = self._entries.get(data)
        if existing is not None:
            if (
                existing.size_bytes == size_bytes
                and existing.sha256 == sha256
                and existing.serialization == serialization
                and existing.session_id == session_id
                and self._verify_entry(existing)
            ):
                return existing
            raise DataStoreConflict("exact data identity is already published with different bytes/provenance")

        if len(self._entries) >= self.limits.max_items:
            raise DataStoreFull("worker local data store item limit reached")
        if self._bytes + size_bytes > self.limits.max_bytes:
            raise DataStoreFull("worker local data store byte limit reached")

        token = data_reference_token(data)
        destination = self.content / f"{token}.bin"
        staged = self.staging / f"publish-{os.urandom(16).hex()}.part"
        try:
            with source_path.open("rb") as src, staged.open("xb") as dst:
                os.chmod(staged, 0o600)
                remaining = size_bytes
                while remaining:
                    chunk = src.read(min(self.limits.verify_chunk_bytes, remaining))
                    if not chunk:
                        raise DataStoreIntegrityError("runtime value file truncated during publication")
                    dst.write(chunk)
                    remaining -= len(chunk)
                if src.read(1):
                    raise DataStoreIntegrityError("runtime value file grew during publication")
                dst.flush()
                os.fsync(dst.fileno())
            # Another physical file under the same exact key must never be replaced
            # silently.  The in-memory authority above is single-owner, but keep the
            # filesystem rule equally strict.
            if destination.exists():
                existing_size, existing_hash = self._hash_file(destination, self.limits.max_value_bytes)
                if existing_size != size_bytes or existing_hash != sha256:
                    raise DataStoreConflict("on-disk runtime value identity collision")
                staged.unlink(missing_ok=True)
            else:
                os.replace(staged, destination)
        finally:
            staged.unlink(missing_ok=True)

        entry = StoredData(data, destination, size_bytes, sha256, serialization, session_id, context_id)
        self._entries[data] = entry
        self._bytes += size_bytes
        return entry

    def publish_bytes(
        self,
        data: p.DataReference,
        payload: bytes,
        *,
        session_id: str,
        serialization: str = "pickle-v1",
        context_id: str | None = None,
    ) -> StoredData:
        if not isinstance(payload, bytes):
            raise TypeError("payload must be bytes")
        size_bytes = len(payload)
        if size_bytes > self.limits.max_value_bytes:
            raise DataStoreFull("runtime value exceeds configured bound")
        if serialization not in {"pickle-v1", "dpr-json-v1"}:
            raise DataStoreIntegrityError("unsupported runtime serialization")
        if not session_id:
            raise DataStoreIntegrityError("session_id must be nonempty")
        digest = hashlib.sha256(payload).hexdigest()
        existing = self._entries.get(data)
        if existing is not None:
            if (existing.size_bytes == size_bytes and existing.sha256 == digest
                    and existing.serialization == serialization
                    and existing.session_id == session_id and self._verify_entry(existing)):
                return existing
            raise DataStoreConflict("exact data identity is already published with different bytes/provenance")
        if len(self._entries) >= self.limits.max_items:
            raise DataStoreFull("worker local data store item limit reached")
        if self._bytes + size_bytes > self.limits.max_bytes:
            raise DataStoreFull("worker local data store byte limit reached")

        token = data_reference_token(data)
        destination = self.content / f"{token}.bin"
        staged = self.staging / f"bytes-{os.urandom(16).hex()}.part"
        try:
            with staged.open("xb") as handle:
                os.chmod(staged, 0o600)
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            if destination.exists():
                existing_size, existing_hash = self._hash_file(destination, self.limits.max_value_bytes)
                if existing_size != size_bytes or existing_hash != digest:
                    raise DataStoreConflict("on-disk runtime value identity collision")
                staged.unlink(missing_ok=True)
            else:
                # F60/F10: publish the staging file directly; do not copy through
                # a second full-size staging file and double physical disk demand.
                os.replace(staged, destination)
        finally:
            staged.unlink(missing_ok=True)
        entry = StoredData(data, destination, size_bytes, digest, serialization, session_id, context_id)
        self._entries[data] = entry
        self._bytes += size_bytes
        return entry

    def release(self, data: p.DataReference, *, session_id: str) -> bool:
        entry = self._entries.get(data)
        if entry is None or entry.session_id != session_id:
            return False
        self._drop_entry(data)
        return True

    def remove_session(self, session_id: str) -> tuple[p.DataReference, ...]:
        removed: list[p.DataReference] = []
        for data, entry in tuple(self._entries.items()):
            if entry.session_id == session_id:
                removed.append(data)
                self._drop_entry(data)
        return tuple(removed)

    def remove_context(self, context_id: str, *, session_id: str) -> tuple[p.DataReference, ...]:
        removed: list[p.DataReference] = []
        for data, entry in tuple(self._entries.items()):
            if entry.session_id == session_id and entry.context_id == context_id:
                removed.append(data)
                self._drop_entry(data)
        return tuple(removed)

    def clear(self) -> None:
        for data in tuple(self._entries):
            self._drop_entry(data)
        for path in self.staging.iterdir():
            if path.is_file() or path.is_symlink():
                path.unlink(missing_ok=True)
            elif path.is_dir():
                shutil.rmtree(path, ignore_errors=True)

    def _drop_entry(self, data: p.DataReference) -> None:
        entry = self._entries.get(data)
        if entry is None:
            return
        # F45: filesystem deletion is the fallible step. Do it first so EBUSY,
        # permission errors, or Windows sharing violations leave the value visible
        # and fully accounted instead of creating an invisible on-disk orphan.
        entry.path.unlink(missing_ok=True)
        self._entries.pop(data, None)
        self._bytes -= entry.size_bytes
        if self._bytes < 0:  # defensive invariant; never mask accounting bugs
            self._bytes = 0

    def _verify_entry(self, entry: StoredData) -> bool:
        try:
            size, digest = self._hash_file(entry.path, self.limits.max_value_bytes)
        except (OSError, DataStoreError):
            return False
        return size == entry.size_bytes and digest == entry.sha256

    def _hash_file(self, path: Path, max_bytes: int) -> tuple[int, str]:
        hasher = hashlib.sha256()
        total = 0
        with path.open("rb") as handle:
            while True:
                chunk = handle.read(self.limits.verify_chunk_bytes)
                if not chunk:
                    break
                total += len(chunk)
                if total > max_bytes:
                    raise DataStoreIntegrityError("runtime value exceeds configured bound")
                hasher.update(chunk)
        return total, hasher.hexdigest()

    @staticmethod
    def _purge_directory(root: Path) -> None:
        for path in root.iterdir():
            if path.is_symlink() or path.is_file():
                path.unlink(missing_ok=True)
            elif path.is_dir():
                shutil.rmtree(path, ignore_errors=True)

    @staticmethod
    def _valid_digest(value: str) -> bool:
        return (
            type(value) is str
            and len(value) == 64
            and all(char in "0123456789abcdef" for char in value)
        )
