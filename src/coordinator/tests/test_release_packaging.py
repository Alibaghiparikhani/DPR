from __future__ import annotations

from pathlib import Path
import zipfile

from tools.build_release import build_release


def test_release_builder_excludes_generated_caches_and_has_one_src_root(tmp_path):
    root = Path(__file__).resolve().parents[2]
    output = tmp_path / "release.zip"
    build_release(root, output)
    with zipfile.ZipFile(output) as archive:
        names = archive.namelist()
    assert names
    assert all(name.startswith("src/") for name in names)
    assert "src/pytest.ini" in names
    assert "src/tools/build_release.py" in names
    assert not any("/__pycache__/" in name for name in names)
    assert not any("/.pytest_cache/" in name for name in names)
    assert not any(name.endswith((".pyc", ".pyo")) for name in names)


def test_release_builder_is_deterministic(tmp_path):
    root = Path(__file__).resolve().parents[2]
    first = tmp_path / "first.zip"
    second = tmp_path / "second.zip"
    build_release(root, first)
    build_release(root, second)
    assert first.read_bytes() == second.read_bytes()
