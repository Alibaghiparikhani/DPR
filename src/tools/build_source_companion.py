"""Render the deterministic human-readable source audit companion.

The release ZIP is authoritative.  ``SOURCE-CODE.md`` is a convenience mirror of
all UTF-8 release-source files except itself, ordered exactly like the release
builder.  Generated caches/runtime credentials are excluded by the same
``iter_release_files`` policy.
"""
from __future__ import annotations

from pathlib import Path
import argparse

from .build_release import iter_release_files

_HEADER = "# SOURCE CODE AUDIT COMPANION\n\nGenerated deterministically from the final repository. The actual files are authoritative.\n"
_SEPARATOR = "=" * 80


def render_source_companion(root: Path) -> str:
    root = root.resolve()
    chunks = [_HEADER.rstrip(), ""]
    for path in iter_release_files(root):
        relative = path.relative_to(root).as_posix()
        if relative == "SOURCE-CODE.md":
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError as error:
            raise ValueError(f"release source is not UTF-8 text: {relative}") from error
        chunks.extend((_SEPARATOR, f"FILE: {relative}", _SEPARATOR, text.rstrip("\n"), ""))
    return "\n".join(chunks).rstrip() + "\n"


def build_source_companion(root: Path, output: Path) -> Path:
    output.write_text(render_source_companion(root), encoding="utf-8", newline="\n")
    return output


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args()
    output = args.output or args.root / "SOURCE-CODE.md"
    build_source_companion(args.root, output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
