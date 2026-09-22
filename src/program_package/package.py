"""Deterministic program packages and a bounded, integrity-checked worker cache."""
from __future__ import annotations

import re

from dataclasses import dataclass
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import secrets
import shutil
import tempfile
import zipfile

FORMAT_VERSION = 1
MANIFEST_NAME = "DPR-PACKAGE.json"
_FIXED_TIME = (1980, 1, 1, 0, 0, 0)
_EXCLUDED_DIRS = {"__pycache__", ".pytest_cache", ".git", ".mypy_cache", ".ruff_cache",
                  ".dpr-local", ".dpr-runtime", ".dpr-state", ".dpr", ".venv", "venv",
                  "node_modules", "dpr-results",
                  # Credential stores: a package goes to every worker, and no program
                  # needs the sender's keys to run.
                  ".ssh", ".gnupg", ".aws", ".azure", ".kube", ".docker", ".password-store"}
_EXCLUDED_FILES = {".netrc", "_netrc", ".pypirc", ".git-credentials", ".npmrc",
                   "id_rsa", "id_dsa", "id_ecdsa", "id_ed25519", "id_ecdsa_sk", "id_ed25519_sk"}
_EXCLUDED_SUFFIXES = {".pyc", ".pyo", ".swp", ".swo"}
# Run results written beside a program (`dpr-results/`, and the per-run folders of
# earlier releases).  Packaging them would change the package on every run: a new
# package id, a re-upload, and a new cache entry on every worker for the same program.
_RESULT_DIR = re.compile(r"^.+-dpr-\d{8}-\d{6}$")


class PackageError(ValueError):
    pass


class PackageIntegrityError(PackageError):
    pass


class PackageResourceError(PackageError):
    pass


class PackageCacheFull(PackageResourceError):
    pass

_CACHE_MARKER = ".dpr-package-cache"
_CACHE_LOCK = ".dpr-package-cache.lock"
_CACHE_MARKER_BYTES = b"dpr-package-cache-v1\n"


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


def _reset_directory(root: Path) -> None:
    """Empty a directory we own, keeping the directory itself (it may be a mount)."""
    for child in root.iterdir():
        if child.is_dir() and not child.is_symlink():
            shutil.rmtree(child, ignore_errors=True)
        else:
            child.unlink(missing_ok=True)


def _acquire_cache_lock(path: Path) -> int:
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


def _initialize_cache_root(root: Path, *, reset_if_unusable: bool = False) -> int:
    if root.exists() and (root.is_symlink() or not root.is_dir()):
        raise PackageIntegrityError("package cache root must be a real directory")
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(root, 0o700)
    marker = root / _CACHE_MARKER
    existing = tuple(root.iterdir())
    if existing and not marker.is_file():
        if not reset_if_unusable:
            raise PackageIntegrityError(
                f"refusing to use {root} as a package cache: it holds files this runtime "
                "did not create. Point --cache-dir at an empty directory"
            )
        _reset_directory(root)
    if marker.exists():
        if marker.is_symlink():
            raise PackageIntegrityError(f"package-cache ownership marker is a symlink: {marker}")
        content = marker.read_bytes()
        if content == b"":
            # Interrupted initialization, not a foreign directory: finish the job.
            _write_marker_atomically(marker, _CACHE_MARKER_BYTES)
        elif content != _CACHE_MARKER_BYTES:
            if not reset_if_unusable:
                raise PackageIntegrityError(
                    f"invalid DPR package-cache ownership marker at {marker}; delete {root} "
                    "and start again (it only holds cached packages)"
                )
            # A cache is disposable and this directory belongs to us, so repair it
            # instead of making the user delete files by hand.
            _reset_directory(root)
            _write_marker_atomically(marker, _CACHE_MARKER_BYTES)
    else:
        _write_marker_atomically(marker, _CACHE_MARKER_BYTES)
    try:
        return _acquire_cache_lock(root / _CACHE_LOCK)
    except (OSError, BlockingIOError) as error:
        raise PackageResourceError("package cache is already in use by another worker") from error


@dataclass(frozen=True, slots=True)
class PackageLimits:
    max_archive_bytes: int = 16 * 1024 * 1024
    max_unpacked_bytes: int = 64 * 1024 * 1024
    max_file_bytes: int = 8 * 1024 * 1024
    max_files: int = 2048
    # Windows-safe relative-path budget: leaves ~120 chars for cache prefixes
    # under legacy MAX_PATH while remaining conservative on other platforms.
    max_path_bytes: int = 120
    max_cache_bytes: int = 256 * 1024 * 1024
    chunk_bytes: int = 32 * 1024

    def __post_init__(self) -> None:
        for name in self.__dataclass_fields__:
            value = getattr(self, name)
            if type(value) is not int or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        if self.max_file_bytes > self.max_unpacked_bytes:
            raise ValueError("max_file_bytes cannot exceed max_unpacked_bytes")
        # Package bytes are hex-encoded inside the JSON control protocol. The
        # global decoder caps every JSON string at 64 KiB, so one binary chunk
        # must not exceed 32 KiB.
        if self.chunk_bytes > 32 * 1024:
            raise ValueError("chunk_bytes is too large for the control-protocol JSON string limit")


@dataclass(frozen=True, slots=True)
class PackageFile:
    path: str
    size: int
    sha256: str


@dataclass(frozen=True, slots=True)
class PackageManifest:
    package_id: str
    files: tuple[PackageFile, ...]
    format_version: int = FORMAT_VERSION

    @property
    def unpacked_bytes(self) -> int:
        return sum(item.size for item in self.files)


@dataclass(frozen=True, slots=True)
class PackageArtifact:
    package_id: str
    archive_bytes: bytes
    archive_sha256: str
    manifest: PackageManifest


def _canonical_json(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, ensure_ascii=True, separators=(",", ":")).encode("utf-8")


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _read_bounded(path: Path, limit: int) -> bytes:
    """Read at most limit bytes plus one sentinel byte before rejecting."""
    with path.open("rb") as handle:
        data = handle.read(limit + 1)
    if len(data) > limit:
        raise PackageResourceError(f"package file too large: {path.name}")
    return data


def _verify_file_stream(path: Path, item: PackageFile, limits: PackageLimits) -> None:
    """Verify one regular file without allocating its whole contents."""
    try:
        if path.stat().st_size != item.size:
            raise PackageIntegrityError("cached package file size mismatch")
        digest = hashlib.sha256()
        read = 0
        with path.open("rb") as handle:
            while True:
                chunk = handle.read(65536)
                if not chunk:
                    break
                read += len(chunk)
                if read > item.size or read > limits.max_file_bytes:
                    raise PackageResourceError("cached package file exceeded configured limit")
                digest.update(chunk)
        if read != item.size or digest.hexdigest() != item.sha256:
            raise PackageIntegrityError("cached package file integrity mismatch")
    except OSError as error:
        raise PackageIntegrityError("cached package file could not be verified") from error


def _safe_relative(path: str, limits: PackageLimits) -> str:
    if not isinstance(path, str) or not path or "\x00" in path or "\\" in path:
        raise PackageIntegrityError("package path is not a portable relative POSIX path")
    try:
        raw = path.encode("utf-8", "strict")
    except UnicodeEncodeError:
        raise PackageIntegrityError("package path contains invalid Unicode") from None
    if len(raw) > limits.max_path_bytes:
        raise PackageResourceError(f"path longer than {limits.max_path_bytes} bytes: {path}")
    pure = PurePosixPath(path)
    if pure.is_absolute() or any(part in {"", ".", ".."} for part in pure.parts):
        raise PackageIntegrityError("package path escapes package root")
    # F61: validate against Windows path semantics even when the coordinator is
    # running on Linux/macOS; otherwise a valid package can become unextractable
    # or alias a device when dispatched to a Windows worker.
    reserved = {"con", "prn", "aux", "nul", *(f"com{i}" for i in range(1, 10)), *(f"lpt{i}" for i in range(1, 10))}
    for part in pure.parts:
        if part.endswith((" ", ".")):
            raise PackageIntegrityError("package path component has a trailing dot or space")
        if any(char in '<>:"|?*' for char in part):
            raise PackageIntegrityError("package path contains a Windows-forbidden character")
        stem = part.split(".", 1)[0].casefold()
        if stem in reserved:
            raise PackageIntegrityError("package path uses a reserved Windows device name")
    if pure.parts and len(pure.parts[0]) >= 2 and pure.parts[0][1] == ":":
        raise PackageIntegrityError("drive-qualified package paths are forbidden")
    normalized = pure.as_posix()
    if normalized == MANIFEST_NAME:
        raise PackageIntegrityError(f"{MANIFEST_NAME} is reserved")
    return normalized


def _identity_record(files: tuple[PackageFile, ...]) -> dict:
    return {
        "format_version": FORMAT_VERSION,
        "files": [
            {"path": item.path, "size": item.size, "sha256": item.sha256}
            for item in files
        ],
    }


def _manifest_bytes(manifest: PackageManifest) -> bytes:
    record = _identity_record(manifest.files)
    record["package_id"] = manifest.package_id
    return _canonical_json(record)


def _reject_path_collisions(paths: set[str]) -> None:
    """Reject layouts unsafe on either case-sensitive or case-insensitive hosts."""
    folded: dict[str, str] = {}
    for path in paths:
        key = path.casefold()
        previous = folded.get(key)
        if previous is not None and previous != path:
            raise PackageIntegrityError("package contains case-colliding paths")
        folded[key] = path
        parts = PurePosixPath(path).parts
        for index in range(1, len(parts)):
            parent = PurePosixPath(*parts[:index]).as_posix()
            if parent in paths:
                raise PackageIntegrityError("package contains a file/directory path collision")


def _remove_tree_or_link(path: Path) -> None:
    """Remove a cache/staging path without ever traversing a symlink target."""
    try:
        if path.is_symlink():
            path.unlink()
        elif path.is_dir():
            shutil.rmtree(path)
        elif path.exists():
            path.unlink()
    except FileNotFoundError:
        pass



def _unique_json_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result

def _parse_manifest(data: bytes, limits: PackageLimits) -> PackageManifest:
    if len(data) > 1024 * 1024:
        raise PackageResourceError("package manifest is too large")
    try:
        raw = json.loads(data.decode("utf-8"), object_pairs_hook=_unique_json_object)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError, RecursionError) as error:
        raise PackageIntegrityError("invalid package manifest") from error
    if type(raw) is not dict or set(raw) != {"format_version", "package_id", "files"}:
        raise PackageIntegrityError("package manifest schema mismatch")
    if raw["format_version"] != FORMAT_VERSION:
        raise PackageIntegrityError("unsupported package format version")
    if type(raw["package_id"]) is not str or len(raw["package_id"]) != 64:
        raise PackageIntegrityError("invalid package identity")
    if type(raw["files"]) is not list or len(raw["files"]) > limits.max_files:
        raise PackageResourceError("package file count limit exceeded")
    files: list[PackageFile] = []
    seen: set[str] = set()
    total = 0
    for row in raw["files"]:
        if type(row) is not dict or set(row) != {"path", "size", "sha256"}:
            raise PackageIntegrityError("invalid package file manifest entry")
        path = _safe_relative(row["path"], limits)
        if path in seen:
            raise PackageIntegrityError("duplicate package path")
        seen.add(path)
        size = row["size"]
        digest = row["sha256"]
        if type(size) is not int or size < 0 or size > limits.max_file_bytes:
            raise PackageResourceError("package file size limit exceeded")
        if type(digest) is not str or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
            raise PackageIntegrityError("invalid package file digest")
        total += size
        if total > limits.max_unpacked_bytes:
            raise PackageResourceError("package unpacked-size limit exceeded")
        files.append(PackageFile(path, size, digest))
    _reject_path_collisions(seen)
    files_tuple = tuple(files)
    expected = _sha(_canonical_json(_identity_record(files_tuple)))
    if raw["package_id"] != expected:
        raise PackageIntegrityError("package identity does not match manifest")
    return PackageManifest(expected, files_tuple)


def _included_files(root: Path, limits: PackageLimits,
                    exclude: tuple[Path, ...] = ()) -> tuple[tuple[str, bytes], ...]:
    root = root.resolve()
    if not root.is_dir():
        raise PackageError("package root must be a directory")
    selected: list[tuple[str, bytes]] = []
    total = 0
    skip = tuple(p.resolve() for p in exclude)
    for path in root.rglob("*"):
        rel = path.relative_to(root)
        if any(part in _EXCLUDED_DIRS for part in rel.parts):
            continue
        if any(_RESULT_DIR.match(part) for part in rel.parts):
            continue
        if path.name in _EXCLUDED_FILES:
            continue
        # Never package runtime state (credentials, logs, worker data), even when a
        # cluster happens to live inside the program directory.
        if any(path == item or item in path.parents for item in skip):
            continue
        if path.is_symlink():
            raise PackageIntegrityError(f"symlinks are unsupported: {rel.as_posix()}")
        if path.is_dir():
            continue
        if not path.is_file():
            raise PackageIntegrityError(f"unsupported filesystem entry: {rel.as_posix()}")
        if path.suffix.lower() in _EXCLUDED_SUFFIXES or path.name.endswith("~") or path.name.startswith(".#"):
            continue
        portable = _safe_relative(rel.as_posix(), limits)
        try:
            data = _read_bounded(path, limits.max_file_bytes)
        except OSError as error:
            # Name the file: a bare "[Errno 13] Permission denied" gives the user
            # nothing to act on, and the usual cause is an unreadable or in-use file
            # sitting inside the package root.
            raise PackageError(f"cannot read {rel.as_posix()}: {error.strerror or error}") from error
        total += len(data)
        if total > limits.max_unpacked_bytes:
            raise PackageResourceError(
                f"folder is larger than {limits.max_unpacked_bytes // (1024 * 1024)} MiB")
        selected.append((portable, data))
        if len(selected) > limits.max_files:
            raise PackageResourceError(f"folder has more than {limits.max_files} files")
    selected.sort(key=lambda item: item[0])
    return tuple(selected)


def build_package(root: str | Path, *, limits: PackageLimits | None = None, exclude: tuple[Path, ...] = ()) -> PackageArtifact:
    limits = limits or PackageLimits()
    files_with_data = _included_files(Path(root), limits, exclude)
    _reject_path_collisions({path for path, _data in files_with_data})
    files = tuple(PackageFile(path, len(data), _sha(data)) for path, data in files_with_data)
    package_id = _sha(_canonical_json(_identity_record(files)))
    manifest = PackageManifest(package_id, files)
    with tempfile.SpooledTemporaryFile(max_size=limits.max_archive_bytes + 1) as buffer:
        with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_STORED) as archive:
            info = zipfile.ZipInfo(MANIFEST_NAME, date_time=_FIXED_TIME)
            info.compress_type = zipfile.ZIP_STORED
            info.external_attr = 0o100644 << 16
            archive.writestr(info, _manifest_bytes(manifest))
            for path, data in files_with_data:
                info = zipfile.ZipInfo(path, date_time=_FIXED_TIME)
                info.compress_type = zipfile.ZIP_STORED
                info.external_attr = 0o100644 << 16
                archive.writestr(info, data)
        buffer.seek(0)
        data = buffer.read(limits.max_archive_bytes + 1)
    if len(data) > limits.max_archive_bytes:
        raise PackageResourceError("package archive size limit exceeded")
    return PackageArtifact(package_id, data, _sha(data), manifest)


class PackageRepository:
    """Bounded immutable coordinator-side package source."""

    def __init__(self, *, max_bytes: int = 256 * 1024 * 1024) -> None:
        if type(max_bytes) is not int or max_bytes < 1:
            raise ValueError("max_bytes must be positive")
        self.max_bytes = max_bytes
        self._packages: dict[str, PackageArtifact] = {}
        self._bytes = 0
        self._lru_clock = 0
        self._last_used: dict[str, int] = {}
        self._pins: dict[str, int] = {}

    def preflight_add(self, artifact: PackageArtifact) -> bool:
        """Validate immutable identity/capacity without mutating repository state.

        Returns ``True`` when publication would add a new entry and ``False``
        for an already-present identical immutable package.  Callers that need
        multi-resource failure atomicity can stage this check before committing
        another authoritative resource, then call :meth:`add` inside the same
        serialized ownership boundary.
        """
        if not isinstance(artifact, PackageArtifact):
            raise TypeError("artifact must be PackageArtifact")
        if (artifact.package_id != artifact.manifest.package_id
                or artifact.package_id != _sha(_canonical_json(_identity_record(artifact.manifest.files)))):
            raise PackageIntegrityError("artifact manifest identity mismatch")
        if artifact.archive_sha256 != _sha(artifact.archive_bytes):
            raise PackageIntegrityError("artifact archive digest mismatch")
        existing = self._packages.get(artifact.package_id)
        if existing is not None:
            # F18: package identity is the verified canonical manifest/content,
            # not one compressor's byte stream. A differently compressed archive
            # with the same validated manifest is an idempotent publication.
            if existing.manifest != artifact.manifest:
                raise PackageIntegrityError("immutable package identity collision")
            return False
        if len(artifact.archive_bytes) > self.max_bytes:
            raise PackageCacheFull("package exceeds coordinator repository byte budget")
        # F17: add() can evict older immutable packages. Preflight remains
        # mutation-free and therefore checks only whether this artifact can ever fit.
        return True

    def _touch(self, package_id: str) -> None:
        self._lru_clock += 1
        self._last_used[package_id] = self._lru_clock

    def pin(self, package_id: str) -> None:
        if package_id not in self._packages:
            raise KeyError(package_id)
        self._pins[package_id] = self._pins.get(package_id, 0) + 1
        self._touch(package_id)

    def unpin(self, package_id: str) -> None:
        count = self._pins.get(package_id, 0)
        if count <= 1:
            self._pins.pop(package_id, None)
        else:
            self._pins[package_id] = count - 1

    def _evict_until_fits(self, incoming_bytes: int, *, protect: frozenset[str] = frozenset()) -> None:
        while self._bytes + incoming_bytes > self.max_bytes:
            candidates = [
                pid for pid in self._packages
                if pid not in protect and self._pins.get(pid, 0) == 0
            ]
            if not candidates:
                raise PackageCacheFull("coordinator package repository is full")
            victim = min(candidates, key=lambda pid: self._last_used.get(pid, 0))
            old = self._packages.pop(victim)
            self._last_used.pop(victim, None)
            self._pins.pop(victim, None)
            self._bytes -= len(old.archive_bytes)

    def add(self, artifact: PackageArtifact) -> None:
        if not self.preflight_add(artifact):
            self._touch(artifact.package_id)
            return
        self._evict_until_fits(len(artifact.archive_bytes))
        self._packages[artifact.package_id] = artifact
        self._bytes += len(artifact.archive_bytes)
        self._touch(artifact.package_id)

    def get(self, package_id: str) -> PackageArtifact | None:
        artifact = self._packages.get(package_id)
        if artifact is not None:
            self._touch(package_id)
        return artifact


class PackageCache:
    """Content-addressed worker cache with atomic verified publication and no eviction."""

    def __init__(self, root: str | Path, *, limits: PackageLimits | None = None,
                 reset_if_unusable: bool = False) -> None:
        self.root = Path(root).resolve()
        self.limits = limits or PackageLimits()
        # F52: stale staging cleanup is destructive, so perform it only after a
        # marker proves DPR owns the directory and an exclusive lock proves no
        # other worker is using it.
        self._lock_fd = _initialize_cache_root(
            self.root, reset_if_unusable=reset_if_unusable)
        self.entries = self.root / "entries"
        self.staging = self.root / "staging"
        self.entries.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.staging.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(self.entries, 0o700)
        os.chmod(self.staging, 0o700)
        os.chmod(self.root / _CACHE_MARKER, 0o600)
        os.chmod(self.root / _CACHE_LOCK, 0o600)
        if self.entries.is_symlink() or self.staging.is_symlink():
            raise PackageIntegrityError("cache control directories must not be symlinks")
        self.cleanup_stale_staging()

    def close(self) -> None:
        fd = getattr(self, "_lock_fd", None)
        if fd is None:
            return
        self._lock_fd = None
        with contextlib_suppress(OSError):
            os.close(fd)

    def __del__(self):
        self.close()

    def cleanup_stale_staging(self) -> None:
        for child in tuple(self.staging.iterdir()):
            with contextlib_suppress(OSError):
                _remove_tree_or_link(child)

    def entry_path(self, package_id: str) -> Path:
        if not isinstance(package_id, str) or len(package_id) != 64 or any(c not in "0123456789abcdef" for c in package_id):
            raise PackageIntegrityError("invalid package identity")
        return self.entries / package_id

    def content_path(self, package_id: str) -> Path:
        return self.entry_path(package_id) / "content"

    def _cache_usage(self) -> int:
        total = 0
        for path in self.entries.rglob("*"):
            if path.is_file() and not path.is_symlink():
                total += path.stat().st_size
        return total

    def _entry_usage(self, entry: Path) -> int:
        total = 0
        for path in entry.rglob("*"):
            if path.is_file() and not path.is_symlink():
                total += path.stat().st_size
        return total

    def _evict_until_fits(self, incoming_bytes: int, *, protect: frozenset[str] = frozenset()) -> None:
        while self._cache_usage() + incoming_bytes > self.limits.max_cache_bytes:
            candidates = []
            for entry in self.entries.iterdir():
                if not entry.is_dir() or entry.name in protect:
                    continue
                try:
                    stamp = entry.stat().st_mtime_ns
                except OSError:
                    continue
                candidates.append((stamp, entry.name, entry))
            if not candidates:
                raise PackageCacheFull("worker package cache is full")
            _stamp, _name, victim = min(candidates)
            _remove_tree_or_link(victim)

    def verify(self, package_id: str) -> PackageManifest | None:
        entry = self.entry_path(package_id)
        if entry.is_symlink():
            _remove_tree_or_link(entry)
            return None
        manifest_path = entry / "manifest.json"
        content = entry / "content"
        if manifest_path.is_symlink() or content.is_symlink() or not manifest_path.is_file() or not content.is_dir():
            if entry.exists() or entry.is_symlink():
                _remove_tree_or_link(entry)
            return None
        try:
            manifest = _parse_manifest(_read_bounded(manifest_path, 1024 * 1024), self.limits)
            if manifest.package_id != package_id:
                raise PackageIntegrityError("cache path identity mismatch")
            expected = {item.path: item for item in manifest.files}
            actual: set[str] = set()
            for path in content.rglob("*"):
                rel_path = path.relative_to(content)
                # F32 belt-and-suspenders: children run with -B, but bytecode
                # from a prior/manual import is non-authoritative cache debris and
                # must never invalidate immutable source content.
                if "__pycache__" in rel_path.parts or path.suffix == ".pyc":
                    continue
                if path.is_symlink():
                    raise PackageIntegrityError("cached package contains symlink")
                if path.is_dir():
                    continue
                rel = rel_path.as_posix()
                actual.add(rel)
                item = expected.get(rel)
                if item is None:
                    raise PackageIntegrityError("cached package contains unexpected file")
                _verify_file_stream(path, item, self.limits)
            if actual != set(expected):
                raise PackageIntegrityError("cached package is incomplete")
            with contextlib_suppress(OSError):
                os.utime(entry, None)
            return manifest
        except (OSError, PackageError):
            _remove_tree_or_link(entry)
            return None

    def install_archive(
        self, archive_path: str | Path, expected_package_id: str, *,
        protected_package_ids: frozenset[str] = frozenset(),
    ) -> PackageManifest:
        archive_path = Path(archive_path)
        size = archive_path.stat().st_size
        if size > self.limits.max_archive_bytes:
            raise PackageResourceError("package archive size limit exceeded")
        existing = self.verify(expected_package_id)
        if existing is not None:
            return existing
        stage = self.staging / f"install-{secrets.token_hex(16)}"
        content = stage / "content"
        content.mkdir(parents=True, mode=0o700)
        os.chmod(stage, 0o700)
        os.chmod(content, 0o700)
        try:
            try:
                archive_context = zipfile.ZipFile(archive_path, "r")
            except (OSError, zipfile.BadZipFile) as error:
                raise PackageIntegrityError("invalid package ZIP archive") from error
            with archive_context as archive:
                infos = archive.infolist()
                if len(infos) > self.limits.max_files + 1:
                    raise PackageResourceError("package ZIP member count limit exceeded")
                names = [info.filename for info in infos]
                if len(names) != len(set(names)):
                    raise PackageIntegrityError("duplicate ZIP member")
                if MANIFEST_NAME not in names:
                    raise PackageIntegrityError("package manifest is missing")
                manifest_info = archive.getinfo(MANIFEST_NAME)
                manifest_mode = (manifest_info.external_attr >> 16) & 0o170000
                if manifest_info.is_dir() or manifest_mode == 0o120000:
                    raise PackageIntegrityError("package manifest must be a regular file")
                if manifest_mode not in (0, 0o100000):
                    raise PackageIntegrityError("package manifest has invalid filesystem type")
                if manifest_info.file_size > 1024 * 1024:
                    raise PackageResourceError("package manifest is too large")
                with archive.open(manifest_info, "r") as manifest_stream:
                    manifest_bytes = manifest_stream.read(1024 * 1024 + 1)
                if len(manifest_bytes) > 1024 * 1024:
                    raise PackageResourceError("package manifest is too large")
                manifest = _parse_manifest(manifest_bytes, self.limits)
                if manifest.package_id != expected_package_id:
                    raise PackageIntegrityError("received package identity mismatch")
                expected = {item.path: item for item in manifest.files}
                _reject_path_collisions(set(expected))
                if set(names) != {MANIFEST_NAME, *expected}:
                    raise PackageIntegrityError("ZIP members differ from manifest")
                # F17: reclaim least-recently-used immutable cache entries before
                # admitting a distinct package. The package being installed is
                # protected from eviction if an older copy already exists.
                manifest_payload = _manifest_bytes(manifest)
                incoming = manifest.unpacked_bytes + len(manifest_payload)
                if incoming > self.limits.max_cache_bytes:
                    raise PackageCacheFull("package exceeds worker cache byte budget")
                self._evict_until_fits(
                    incoming, protect=frozenset({expected_package_id, *protected_package_ids})
                )
                for info in infos:
                    if info.filename == MANIFEST_NAME:
                        continue
                    rel = _safe_relative(info.filename, self.limits)
                    item = expected[rel]
                    mode = (info.external_attr >> 16) & 0o170000
                    if mode == 0o120000:
                        raise PackageIntegrityError("package symlinks are forbidden")
                    if mode not in (0, 0o100000):
                        raise PackageIntegrityError("package contains a non-regular filesystem entry")
                    if info.is_dir() or info.file_size != item.size:
                        raise PackageIntegrityError("ZIP metadata differs from manifest")
                    if info.file_size > self.limits.max_file_bytes:
                        raise PackageResourceError("package file size limit exceeded")
                    target = content.joinpath(*PurePosixPath(rel).parts)
                    target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                    # mkdir is umask-sensitive for already-created parents; force the
                    # entire extracted parent chain under content to owner-only.
                    current = target.parent
                    while current != content.parent:
                        os.chmod(current, 0o700)
                        if current == content:
                            break
                        current = current.parent
                    h = hashlib.sha256()
                    written = 0
                    with archive.open(info, "r") as src, target.open("xb") as dst:
                        os.chmod(target, 0o600)
                        while True:
                            chunk = src.read(65536)
                            if not chunk:
                                break
                            written += len(chunk)
                            if written > item.size or written > self.limits.max_file_bytes:
                                raise PackageResourceError("decompressed file exceeded declared limit")
                            h.update(chunk)
                            dst.write(chunk)
                    if written != item.size or h.hexdigest() != item.sha256:
                        raise PackageIntegrityError("package content digest mismatch")
                manifest_path = stage / "manifest.json"
                fd = os.open(manifest_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                try:
                    os.write(fd, manifest_payload)
                    os.fsync(fd)
                finally:
                    os.close(fd)
            # Verify the fully extracted stage before publication.
            self._verify_stage(stage, manifest)
            destination = self.entry_path(expected_package_id)
            if destination.exists() or destination.is_symlink():
                current = self.verify(expected_package_id)
                if current is not None:
                    return current
                _remove_tree_or_link(destination)
            os.replace(stage, destination)
            os.chmod(destination, 0o700)
            verified = self.verify(expected_package_id)
            if verified is None:
                raise PackageIntegrityError("published cache entry failed verification")
            return verified
        finally:
            if stage.exists() or stage.is_symlink():
                _remove_tree_or_link(stage)

    def _verify_stage(self, stage: Path, manifest: PackageManifest) -> None:
        content = stage / "content"
        expected = {item.path: item for item in manifest.files}
        actual: set[str] = set()
        for path in content.rglob("*"):
            if path.is_symlink():
                raise PackageIntegrityError("staged package contains symlink")
            if path.is_dir():
                continue
            rel = path.relative_to(content).as_posix()
            actual.add(rel)
            item = expected.get(rel)
            if item is None:
                raise PackageIntegrityError("staged package contains extra file")
            _verify_file_stream(path, item, self.limits)
        if actual != set(expected):
            raise PackageIntegrityError("staged package missing file")


class contextlib_suppress:
    """Tiny local suppress to keep this module's import surface minimal."""
    def __init__(self, *exceptions): self.exceptions = exceptions
    def __enter__(self): return None
    def __exit__(self, typ, value, tb): return typ is not None and issubclass(typ, self.exceptions)
