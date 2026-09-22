"""Build a deterministic clean source release ZIP.

Usage from the repository root::

    python -m tools.build_release /path/to/output.zip

The archive has a single ``src/`` root and excludes generated Python/pytest
caches, VCS metadata, temporary files, and the output archive itself.
"""
from __future__ import annotations

from pathlib import Path
import argparse
import zipfile

_EXCLUDED_DIRS = {"__pycache__", ".pytest_cache", ".git", ".mypy_cache", ".ruff_cache", ".dpr-local", ".dpr-runtime"}
_EXCLUDED_SUFFIXES = {".pyc", ".pyo"}
_FIXED_TIME = (1980, 1, 1, 0, 0, 0)


def iter_release_files(root: Path, *, output: Path | None = None) -> tuple[Path, ...]:
    root = root.resolve()
    output = None if output is None else output.resolve()
    files: list[Path] = []
    for path in root.rglob("*"):
        relative = path.relative_to(root)
        if any(part in _EXCLUDED_DIRS for part in relative.parts):
            continue
        if path.is_symlink():
            raise ValueError(f"symlinks are unsupported in release source: {relative.as_posix()}")
        if not path.is_file():
            continue
        resolved = path.resolve()
        if output is not None and resolved == output:
            continue
        if path.suffix.lower() in _EXCLUDED_SUFFIXES:
            continue
        # Common editor/temporary leftovers are never release source.
        if path.name.endswith("~") or path.name.startswith(".#"):
            continue
        files.append(path)
    return tuple(sorted(files, key=lambda item: item.relative_to(root).as_posix()))


def build_release(root: Path, output: Path) -> Path:
    root = root.resolve()
    output = output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for path in iter_release_files(root, output=output):
            relative = path.relative_to(root).as_posix()
            info = zipfile.ZipInfo(f"src/{relative}", date_time=_FIXED_TIME)
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o100644 << 16
            archive.writestr(info, path.read_bytes())
    return output


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    args = parser.parse_args()
    build_release(args.root, args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
