from pathlib import Path
import zipfile

from tools.build_release import build_release


def test_release_builder_never_packages_generated_runtime_credentials_or_state(tmp_path: Path):
    root = tmp_path / "src"
    root.mkdir()
    (root / "main.py").write_text("x = 1\n", encoding="utf-8")
    for dirname, filename in ((".dpr-local", "admin.secret"), (".dpr-runtime", "object.bin")):
        directory = root / dirname
        directory.mkdir()
        (directory / filename).write_bytes(b"must-not-ship")
    output = tmp_path / "release.zip"
    build_release(root, output)
    with zipfile.ZipFile(output) as archive:
        names = set(archive.namelist())
    assert names == {"src/main.py"}


def test_release_builder_rejects_symlink_escape(tmp_path: Path):
    root = tmp_path / "src-link"
    root.mkdir()
    outside = tmp_path / "private.key"
    outside.write_text("secret", encoding="ascii")
    link = root / "innocent.txt"
    try:
        link.symlink_to(outside)
    except (OSError, NotImplementedError):
        import pytest
        pytest.skip("symlinks unavailable on this platform/test account")
    import pytest
    with pytest.raises(ValueError, match="symlinks are unsupported"):
        build_release(root, tmp_path / "bad-release.zip")


def test_source_audit_companion_matches_release_source_inventory():
    from tools.build_source_companion import render_source_companion

    root = Path(__file__).resolve().parents[2]
    assert (root / "SOURCE-CODE.md").read_text(encoding="utf-8") == render_source_companion(root)
