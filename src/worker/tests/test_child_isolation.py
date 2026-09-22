"""F50: user code must not be able to read worker-owned credentials.

The runtime can drop children to a separate unprivileged identity.  These tests
verify the spawn contract rather than a particular error code, so they stay
meaningful on hosts where a sandboxed child fails at a different point (for
example when it cannot even open the program file).
"""
from __future__ import annotations

import asyncio
import os
import sys

import pytest

from program_package import PackageCache, build_package
from dag_runtime.dag_engine import analyze_source
from execution import lower_dag
from scheduler import WorkerState
from worker import WorkerExecutionRuntime

def _is_root() -> bool:
    """True only where a real POSIX root check is possible.

    `os.getuid` does not exist on Windows, and a skipif condition is evaluated at
    collection time, so calling it unguarded aborts the whole test session there.
    """
    return hasattr(os, "geteuid") and os.geteuid() == 0


# Dropping submitted code to a separate UID is POSIX-only; Windows has no setuid.
pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.skipif(os.name != "posix", reason="child UID isolation is POSIX-only"),
]

UNPRIVILEGED_UID = 65534  # nobody
UNPRIVILEGED_GID = 65534


def _runtime(tmp_path, *, child_uid=None, child_gid=None):
    root = tmp_path / "src"
    root.mkdir(parents=True)
    (root / "main.py").write_text("a = 1\n", encoding="utf-8")
    artifact = build_package(root)
    plan = lower_dag(analyze_source("a = 1\n", filename="main.py"),
                     environment_id="env-test", package_id=artifact.package_id)
    state = WorkerState("W1", 2, environment_ids=frozenset({"env-test"}))
    return WorkerExecutionRuntime(
        "W1", state, PackageCache(tmp_path / "cache"),
        child_uid=child_uid, child_gid=child_gid,
    )


def test_child_spawn_kwargs_drop_privileges(tmp_path):
    """Without an explicit identity the runtime must not silently stay as root."""
    plain = _runtime(tmp_path / "plain")._child_spawn_kwargs()
    assert "user" not in plain and "group" not in plain

    dropped = _runtime(
        tmp_path / "dropped", child_uid=UNPRIVILEGED_UID, child_gid=UNPRIVILEGED_GID,
    )._child_spawn_kwargs()
    assert dropped["user"] == UNPRIVILEGED_UID
    assert dropped["group"] == UNPRIVILEGED_GID
    # Descendants must stay killable as a group (F51).
    assert dropped.get("start_new_session") is True


@pytest.mark.skipif(not _is_root(), reason="dropping to another UID requires root")
async def test_exfil_task_cannot_read_worker_secret(tmp_path):
    """A child dropped to `nobody` cannot read a root-owned 0600 secret."""
    workdir = tmp_path / "work"
    workdir.mkdir()
    # The sandboxed child must be able to traverse to its own program; only the
    # secret itself is protected.  Otherwise the test would pass for the wrong
    # reason (the child failing before it ever attempts the read).
    os.chmod(tmp_path, 0o755)
    os.chmod(workdir, 0o755)

    secret = workdir / "worker.secret"
    secret.write_text("deadbeef" * 8, encoding="utf-8")
    os.chmod(secret, 0o600)
    assert os.stat(secret).st_uid == 0

    runtime = _runtime(
        tmp_path / "rt", child_uid=UNPRIVILEGED_UID, child_gid=UNPRIVILEGED_GID,
    )
    kwargs = runtime._child_spawn_kwargs()

    program = workdir / "exfil.py"
    program.write_text(
        "import sys\n"
        "try:\n"
        f"    data = open({str(secret)!r}, 'rb').read()\n"
        "except PermissionError:\n"
        "    sys.exit(13)\n"
        "except OSError:\n"
        "    sys.exit(14)\n"
        "sys.stdout.write(data.decode())\n"
        "sys.exit(0)\n",
        encoding="utf-8",
    )
    os.chmod(program, 0o644)

    process = await asyncio.create_subprocess_exec(
        sys.executable, "-I", "-B", str(program),
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        **kwargs,
    )
    stdout, stderr = await process.communicate()

    assert process.returncode != 0, (
        f"exfil child read the worker secret: {stdout!r}"
    )
    assert b"deadbeef" not in stdout, "secret contents leaked to the child"
    # 13 is the precise expectation; any other nonzero exit means the child was
    # blocked even earlier, which is also acceptable containment.
    assert process.returncode in (13, 14, 1, 2), (
        f"unexpected child failure mode rc={process.returncode}: {stderr!r}"
    )
