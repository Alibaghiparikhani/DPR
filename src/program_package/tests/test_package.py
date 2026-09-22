from __future__ import annotations

import hashlib
import os
from pathlib import Path
import zipfile

import pytest

from program_package import (
    MANIFEST_NAME, PackageCache, PackageCacheFull, PackageIntegrityError,
    PackageLimits, PackageRepository, PackageResourceError, build_package,
)


def make_source(root: Path, text: str = "a = 20 + 22\n") -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / "main.py").write_text(text, encoding="utf-8")
    (root / "pkg").mkdir(exist_ok=True)
    (root / "pkg" / "helper.py").write_text("VALUE = 7\n", encoding="utf-8")


def test_package_build_is_byte_deterministic_and_path_independent(tmp_path):
    a, b = tmp_path / "a", tmp_path / "other" / "b"
    make_source(a); make_source(b)
    first = build_package(a)
    os.utime(a / "main.py", (1000000000, 1000000000))
    second = build_package(a)
    third = build_package(b)
    assert first.package_id == second.package_id == third.package_id
    assert first.archive_bytes == second.archive_bytes == third.archive_bytes
    assert first.archive_sha256 == hashlib.sha256(first.archive_bytes).hexdigest()


def test_content_changes_add_delete_change_identity(tmp_path):
    root = tmp_path / "src"; make_source(root)
    initial = build_package(root)
    (root / "main.py").write_text("a = 20 + 23\n")
    changed = build_package(root)
    assert changed.package_id != initial.package_id
    (root / "extra.py").write_text("x=1\n")
    added = build_package(root)
    assert added.package_id != changed.package_id
    (root / "extra.py").unlink()
    assert build_package(root).package_id == changed.package_id


def test_generated_junk_is_excluded(tmp_path):
    root = tmp_path / "src"; make_source(root)
    baseline = build_package(root)
    (root / "__pycache__").mkdir()
    (root / "__pycache__" / "main.cpython.pyc").write_bytes(b"junk")
    (root / ".pytest_cache").mkdir(); (root / ".pytest_cache" / "x").write_text("junk")
    (root / "temp.py~").write_text("junk")
    assert build_package(root).package_id == baseline.package_id


def test_symlink_rejected(tmp_path):
    root = tmp_path / "src"; make_source(root)
    target = tmp_path / "secret"; target.write_text("secret")
    try:
        (root / "escape").symlink_to(target)
    except (OSError, NotImplementedError):
        pytest.skip("symlinks unavailable")
    with pytest.raises(PackageIntegrityError, match="symlinks"):
        build_package(root)


def test_cache_install_verify_and_corruption_removal(tmp_path):
    source = tmp_path / "src"; make_source(source)
    artifact = build_package(source)
    archive = tmp_path / "package.zip"; archive.write_bytes(artifact.archive_bytes)
    cache = PackageCache(tmp_path / "cache")
    manifest = cache.install_archive(archive, artifact.package_id)
    assert manifest.package_id == artifact.package_id
    assert cache.verify(artifact.package_id) is not None
    (cache.content_path(artifact.package_id) / "main.py").write_text("corrupt")
    assert cache.verify(artifact.package_id) is None
    assert not cache.entry_path(artifact.package_id).exists()


def test_cache_rejects_truncated_wrong_identity_and_extra_members(tmp_path):
    source = tmp_path / "src"; make_source(source)
    artifact = build_package(source)
    cache = PackageCache(tmp_path / "cache")
    truncated = tmp_path / "truncated.zip"; truncated.write_bytes(artifact.archive_bytes[:50])
    with pytest.raises((zipfile.BadZipFile, PackageIntegrityError, OSError)):
        cache.install_archive(truncated, artifact.package_id)
    archive = tmp_path / "package.zip"; archive.write_bytes(artifact.archive_bytes)
    with pytest.raises(PackageIntegrityError, match="identity"):
        cache.install_archive(archive, "0" * 64)
    evil = tmp_path / "evil.zip"
    with zipfile.ZipFile(evil, "w") as z:
        z.writestr(MANIFEST_NAME, b'{}')
        z.writestr("../escape.py", b"x")
    with pytest.raises(PackageIntegrityError):
        cache.install_archive(evil, artifact.package_id)
    assert not (tmp_path / "escape.py").exists()


def test_archive_budgets_and_cache_full_are_enforced(tmp_path):
    source = tmp_path / "src"; make_source(source)
    artifact = build_package(source)
    archive = tmp_path / "package.zip"; archive.write_bytes(artifact.archive_bytes)
    limits = PackageLimits(max_cache_bytes=1)
    cache = PackageCache(tmp_path / "cache", limits=limits)
    with pytest.raises(PackageCacheFull):
        cache.install_archive(archive, artifact.package_id)


def test_repository_is_bounded_lru_and_identity_immutable(tmp_path):
    source = tmp_path / "src"; make_source(source)
    artifact = build_package(source)
    repo = PackageRepository(max_bytes=len(artifact.archive_bytes))
    repo.add(artifact); repo.add(artifact)
    assert repo.get(artifact.package_id) is artifact
    (source / "main.py").write_text("b=9\n")
    other = build_package(source)

    # F17: an idle immutable package is reclaimable instead of permanently
    # consuming the repository's lifetime byte budget.
    repo.add(other)
    assert repo.get(other.package_id) is other
    assert repo.get(artifact.package_id) is None


def test_repository_lru_never_evicts_pinned_live_package(tmp_path):
    source = tmp_path / "src"; make_source(source)
    artifact = build_package(source)
    repo = PackageRepository(max_bytes=len(artifact.archive_bytes))
    repo.add(artifact)
    repo.pin(artifact.package_id)
    (source / "main.py").write_text("b=9\n")
    other = build_package(source)
    with pytest.raises(PackageCacheFull):
        repo.add(other)
    assert repo.get(artifact.package_id) is artifact
    repo.unpin(artifact.package_id)
    repo.add(other)
    assert repo.get(other.package_id) is other


def _manifest_for(rows):
    import json
    record = {"format_version": 1, "files": rows}
    canonical = json.dumps(record, sort_keys=True, ensure_ascii=True, separators=(",", ":")).encode()
    package_id = hashlib.sha256(canonical).hexdigest()
    full = dict(record, package_id=package_id)
    return package_id, json.dumps(full, sort_keys=True, ensure_ascii=True, separators=(",", ":")).encode()


def test_cache_rejects_path_traversal_and_file_directory_collision(tmp_path):
    cache = PackageCache(tmp_path / "cache")
    traversal_rows = [{"path": "../escape.py", "size": 1, "sha256": hashlib.sha256(b"x").hexdigest()}]
    package_id, manifest = _manifest_for(traversal_rows)
    bad = tmp_path / "traversal.zip"
    with zipfile.ZipFile(bad, "w") as z:
        z.writestr(MANIFEST_NAME, manifest)
        z.writestr("../escape.py", b"x")
    with pytest.raises(PackageIntegrityError, match="path|root"):
        cache.install_archive(bad, package_id)
    assert not (tmp_path / "escape.py").exists()

    rows = [
        {"path": "pkg", "size": 1, "sha256": hashlib.sha256(b"x").hexdigest()},
        {"path": "pkg/a.py", "size": 1, "sha256": hashlib.sha256(b"y").hexdigest()},
    ]
    collision_id, collision_manifest = _manifest_for(rows)
    collision = tmp_path / "collision.zip"
    with zipfile.ZipFile(collision, "w") as z:
        z.writestr(MANIFEST_NAME, collision_manifest)
        z.writestr("pkg", b"x")
        z.writestr("pkg/a.py", b"y")
    with pytest.raises(PackageIntegrityError, match="collision"):
        cache.install_archive(collision, collision_id)


def test_symlinked_cache_entry_is_removed_without_following_target(tmp_path):
    source = tmp_path / "src"; make_source(source)
    artifact = build_package(source)
    cache = PackageCache(tmp_path / "cache")
    outside = tmp_path / "outside"; outside.mkdir()
    marker = outside / "keep.txt"; marker.write_text("keep")
    link = cache.entry_path(artifact.package_id)
    try:
        link.symlink_to(outside, target_is_directory=True)
    except (OSError, NotImplementedError):
        pytest.skip("symlinks unavailable")
    assert cache.verify(artifact.package_id) is None
    assert not link.exists() and not link.is_symlink()
    assert marker.read_text() == "keep"


def test_malformed_zip_is_normalized_to_package_integrity_error(tmp_path):
    cache = PackageCache(tmp_path / "cache")
    bad = tmp_path / "bad.zip"; bad.write_bytes(b"not-a-zip")
    with pytest.raises(PackageIntegrityError, match="ZIP"):
        cache.install_archive(bad, "0" * 64)


def test_repository_rejects_forged_artifact_digest(tmp_path):
    from dataclasses import replace
    source = tmp_path / "src"; make_source(source)
    artifact = build_package(source)
    repo = PackageRepository()
    with pytest.raises(PackageIntegrityError, match="digest"):
        repo.add(replace(artifact, archive_sha256="0" * 64))


def test_compressed_oversized_manifest_is_rejected_before_unbounded_read(tmp_path):
    cache = PackageCache(tmp_path / "cache")
    bomb = tmp_path / "manifest-bomb.zip"
    with zipfile.ZipFile(bomb, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(MANIFEST_NAME, b"x" * (1024 * 1024 + 1))
    with pytest.raises(PackageResourceError, match="manifest"):
        cache.install_archive(bomb, "0" * 64)


def test_runtime_generated_local_credentials_are_never_packaged(tmp_path):
    (tmp_path / "main.py").write_text("x=1\n", encoding="utf-8")
    secrets_dir = tmp_path / ".dpr-local"
    secrets_dir.mkdir()
    (secrets_dir / "worker.key").write_text("PRIVATE", encoding="ascii")
    (secrets_dir / "admin.secret").write_text("SECRET", encoding="ascii")
    runtime_dir = tmp_path / ".dpr-runtime"
    runtime_dir.mkdir()
    (runtime_dir / "object.bin").write_bytes(b"runtime-payload")
    artifact = build_package(tmp_path)
    paths = {item.path for item in artifact.manifest.files}
    assert paths == {"main.py"}


def test_package_control_chunk_limit_matches_protocol_json_string_budget():
    assert PackageLimits().chunk_bytes == 32 * 1024
    with pytest.raises(ValueError, match="JSON string limit"):
        PackageLimits(chunk_bytes=32 * 1024 + 1)

def test_cache_verify_rejects_oversized_manifest_without_unbounded_read(tmp_path, monkeypatch):
    """A corrupted cache manifest is bounded before JSON parsing/allocation."""
    from program_package import PackageCache, PackageLimits, build_package
    import program_package.package as package_module

    root = tmp_path / "src-oversized-manifest"
    root.mkdir()
    (root / "main.py").write_text("x = 1\n", encoding="utf-8")
    artifact = build_package(root)
    cache = PackageCache(tmp_path / "cache", limits=PackageLimits())
    archive = tmp_path / "package.zip"
    archive.write_bytes(artifact.archive_bytes)
    cache.install_archive(archive, artifact.package_id)
    manifest_path = cache.entry_path(artifact.package_id) / "manifest.json"
    manifest_path.write_bytes(b"x" * (1024 * 1024 + 1))

    original_open = package_module.Path.open
    max_requested = 0

    class Guarded:
        def __init__(self, handle):
            self.handle = handle
        def __enter__(self): return self
        def __exit__(self, *args): self.handle.close()
        def read(self, size=-1):
            nonlocal max_requested
            max_requested = max(max_requested, size)
            assert 0 <= size <= 1024 * 1024 + 1
            return self.handle.read(size)

    def guarded_open(path, *args, **kwargs):
        handle = original_open(path, *args, **kwargs)
        if path == manifest_path and (not args or "r" in args[0]):
            return Guarded(handle)
        return handle

    monkeypatch.setattr(package_module.Path, "open", guarded_open)
    assert cache.verify(artifact.package_id) is None
    assert max_requested <= 1024 * 1024 + 1
    assert not cache.entry_path(artifact.package_id).exists()


def test_interrupted_cache_marker_initialization_is_repaired(tmp_path):
    """Same contract as the data store: an empty marker means we were killed mid-init."""
    from program_package.package import _CACHE_MARKER, _CACHE_MARKER_BYTES

    root = tmp_path / "cache"
    cache = PackageCache(root)
    cache.close()
    marker = root / _CACHE_MARKER
    marker.write_bytes(b"")

    repaired = PackageCache(root)
    repaired.close()
    assert marker.read_bytes() == _CACHE_MARKER_BYTES

    marker.write_bytes(b"not ours\n")
    with pytest.raises(PackageIntegrityError) as excinfo:
        PackageCache(root)
    assert str(root) in str(excinfo.value)
