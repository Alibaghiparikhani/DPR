"""Real coordinator and worker processes (`python -m dpr.node`) driven through dpr.runs."""
from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time

import pytest

from dpr import credentials, runs
from dpr.node import DEFAULT_CHILD_USER, _detect_advertise_host, build_parser, machine_limits, \
    resolve_child_identity
from networking import CoordinatorClient, CoordinatorClientConfig
from runtime_security import TlsCredentials, TlsPolicy

from .sessions import SRC, scale

needs_openssl = pytest.mark.skipif(credentials.openssl() is None, reason="needs openssl")


def _port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class Cluster:
    """A coordinator plus workers on loopback, each a real node process."""

    def __init__(self, root: Path, workers: list[str], *, slots: int = 1,
                 node_args: tuple[str, ...] = ()) -> None:
        self.root = root
        self.creds = root / "creds"
        credentials.create(self.creds, workers=workers, client="admin")
        self.port = _port()
        self.processes: list[subprocess.Popen] = []
        self.env = dict(os.environ, PYTHONPATH=os.pathsep.join(
            filter(None, [str(SRC), os.environ.get("PYTHONPATH")])))
        self.start_node([
            "coordinator", "--bind", "127.0.0.1", "--port", str(self.port),
            "--auth-file", str(self.creds / "auth.json"), "--client-id", "admin",
            "--history-db", str(root / "history.sqlite3"), *self.tls("coordinator"), *node_args,
        ])
        for worker in workers:
            self.start_worker(worker, slots=slots, extra=node_args)
        self.config = CoordinatorClientConfig(
            node_id="admin", secret=bytes.fromhex((self.creds / "admin.secret").read_text().strip()),
            coordinator_host="127.0.0.1", coordinator_port=self.port,
            server_hostname=credentials.SERVER_NAME,
            tls_policy=TlsPolicy(TlsCredentials(*self.tls("admin")[1::2])))
        self.wait_online(len(workers))

    def tls(self, stem: str) -> list[str]:
        return ["--cert", str(self.creds / f"{stem}.pem"), "--key", str(self.creds / f"{stem}.key"),
                "--ca", str(self.creds / "ca.pem")]

    def start_node(self, args: list[str]) -> subprocess.Popen:
        process = subprocess.Popen([sys.executable, "-m", "dpr.node", *args, "--log-level", "ERROR"],
                                   env=self.env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.processes.append(process)
        return process

    def start_worker(self, worker: str, *, slots: int = 1, extra: tuple[str, ...] = ()):
        return self.start_node([
            "worker", "--id", worker, "--coordinator", f"127.0.0.1:{self.port}",
            "--secret-file", str(self.creds / f"{worker}.secret"), *self.tls(worker),
            "--slots", str(slots), "--cache-dir", str(self.root / f"cache-{worker}"),
            "--data-dir", str(self.root / f"data-{worker}"), "--data-port", str(_port()),
            *(["--allow-unprivileged-child-execution"] if os.name == "posix" else []), *extra,
        ])

    async def status(self):
        async with CoordinatorClient(self.config) as client:
            return await client.cluster_status()

    async def run_status(self, run_id: str):
        async with CoordinatorClient(self.config) as client:
            return await client.run_status(run_id)

    def wait_online(self, count: int, timeout: float = 15.0):
        deadline = time.monotonic() + timeout * scale()
        while time.monotonic() < deadline:
            try:
                cluster = asyncio.run(self.status())
            except Exception:
                cluster = None
            if cluster is not None and sum(w.online for w in cluster.workers) == count:
                return cluster
            time.sleep(0.1)
        raise AssertionError(f"{count} worker(s) did not come online")

    def close(self) -> None:
        for process in reversed(self.processes):
            if process.poll() is None:
                process.terminate()
        for process in self.processes:
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)


@pytest.fixture
def cluster_factory(tmp_path: Path):
    made: list[Cluster] = []

    def make(workers: list[str], **kwargs) -> Cluster:
        made.append(Cluster(tmp_path, workers, **kwargs))
        return made[-1]

    yield make
    for cluster in made:
        cluster.close()


# ---------------------------------------------------------------------- credentials

@needs_openssl
def test_credentials_layout(tmp_path: Path):
    target = tmp_path / "creds"
    summary = credentials.create(target, workers=["w1", "w2"], client="admin")
    assert summary["workers"] == ["w1", "w2"] and summary["expires"] > time.time() + 700 * 86400
    auth = json.loads((target / "auth.json").read_text())
    assert set(auth) == {"w1", "w2", "admin"}
    assert all(len(bytes.fromhex(secret)) == 32 for secret in auth.values())
    for name in ("ca.pem", "coordinator.pem", "coordinator.key", "w1.pem", "w1.key", "admin.pem"):
        assert (target / name).is_file()
    assert not (target / "ca.key").exists(), "nobody can mint identities later"
    assert not list(target.glob("*.csr")) and not list(target.glob("*.ext"))
    assert not (tmp_path / "creds.partial").exists()


def test_credentials_validate_before_writing(tmp_path: Path):
    target = tmp_path / "creds"
    for workers, client in ((["../escape"], "admin"), (["w1\nsubjectAltName=DNS:evil"], "admin"),
                            (["w1"], "../escape"), (["w1", "w1"], "admin"), (["admin"], "admin"),
                            (["coordinator"], "admin")):
        with pytest.raises(ValueError):
            credentials.create(target, workers=workers, client=client)
    assert not target.exists() and not (tmp_path / "escape").exists()


# ------------------------------------------------------------------------ real runs

@needs_openssl
def test_independent_tasks_spread_across_workers(cluster_factory, tmp_path: Path):
    cluster = cluster_factory(["w1", "w2", "w3"])
    program = tmp_path / "project" / "independent.py"
    program.parent.mkdir()
    program.write_text("a=20+22\nb=30+12\nc=40+2\n")
    outcome = asyncio.run(runs.run(cluster.config, program))
    assert outcome.succeeded, outcome.status
    assert {task.worker_id for task in outcome.status.tasks} == {"w1", "w2", "w3"}
    assert sum(task.status == "committed" for task in outcome.status.tasks) == 3


@needs_openssl
def test_concurrent_runs_isolate_failure_and_reuse_packages(cluster_factory, tmp_path: Path):
    cluster = cluster_factory(["w1", "w2"], slots=2)
    good = tmp_path / "good-project" / "good.py"
    bad = tmp_path / "bad-project" / "bad.py"
    for path, text in ((good, "a=20+22\nb=30+12\n"), (bad, "a=1/0\n")):
        path.parent.mkdir()
        path.write_text(text)

    async def both():
        return await asyncio.gather(runs.run(cluster.config, good, run_id="good-1"),
                                    runs.run(cluster.config, bad, run_id="bad-1"))

    good_outcome, bad_outcome = asyncio.run(both())
    assert good_outcome.succeeded
    assert bad_outcome.status.status == "failed"
    assert bad_outcome.error == "ZeroDivisionError: division by zero"

    entries = lambda: {w: len(list((tmp_path / f"cache-{w}" / "entries").glob("*")))  # noqa: E731
                       for w in ("w1", "w2")}
    before = entries()
    assert asyncio.run(runs.run(cluster.config, good, run_id="good-2")).succeeded
    assert entries() == before, "the same program must reuse its cached package"
    for run_id, expected in (("good-1", "succeeded"), ("bad-1", "failed"), ("good-2", "succeeded")):
        assert asyncio.run(cluster.run_status(run_id)).status == expected


@needs_openssl
def test_cancelling_the_run_reaches_the_running_task(cluster_factory, tmp_path: Path):
    cluster = cluster_factory(["w1"])
    program = tmp_path / "project" / "long.py"
    program.parent.mkdir()
    program.write_text("import time\ntime.sleep(60)\n")

    async def start_then_cancel():
        task = asyncio.create_task(runs.run(cluster.config, program, run_id="long-1"))
        deadline = time.monotonic() + 10 * scale()
        while time.monotonic() < deadline:
            await asyncio.sleep(0.1)
            try:
                if (await cluster.run_status("long-1")).status == "running":
                    break
            except Exception:
                pass
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(start_then_cancel())
    deadline = time.monotonic() + 10 * scale()
    while asyncio.run(cluster.run_status("long-1")).status != "cancelled":
        assert time.monotonic() < deadline, "run was not cancelled"
        time.sleep(0.1)
    busy = lambda: sum(w.running_slots for w in asyncio.run(cluster.status()).workers)  # noqa: E731
    while busy():
        assert time.monotonic() < deadline + 10, "worker still busy"
        time.sleep(0.1)


@needs_openssl
def test_reconnecting_worker_gets_a_new_generation(cluster_factory):
    cluster = cluster_factory(["w1"])
    worker = cluster.processes[-1]
    generations = []
    for _ in range(3):
        online = [w for w in cluster.wait_online(1).workers if w.online]
        generations.append(online[0].generation)
        worker.terminate()
        worker.wait(timeout=10)
        cluster.processes.remove(worker)
        deadline = time.monotonic() + 10 * scale()
        while any(w.online for w in asyncio.run(cluster.status()).workers):
            assert time.monotonic() < deadline
            time.sleep(0.05)
        worker = cluster.start_worker("w1", extra=("--reconnect-attempts", "0"))
    assert generations == [1, 2, 3]


@needs_openssl
def test_supervised_node_exits_when_its_pipe_closes(tmp_path: Path):
    """The session's hold on a node is a pipe: closing it (or dying) stops the node."""
    from dpr.processes import spawn

    creds = tmp_path / "creds"
    credentials.create(creds, workers=["w1"], client="admin")
    child = spawn("coordinator", [
        "coordinator", "--bind", "127.0.0.1", "--port", str(_port()),
        "--auth-file", str(creds / "auth.json"), "--client-id", "admin",
        "--cert", str(creds / "coordinator.pem"), "--key", str(creds / "coordinator.key"),
        "--ca", str(creds / "ca.pem"), "--ready-file", str(tmp_path / "ready"),
    ], pid_dir=tmp_path / "pids")
    deadline = time.monotonic() + 20 * scale()
    while not (tmp_path / "ready").exists():
        assert child.alive() and time.monotonic() < deadline
        time.sleep(0.05)
    started = time.monotonic()
    child.popen.stdin.close()
    assert child.popen.wait(timeout=15) == 0
    assert time.monotonic() - started < 10
    assert not (tmp_path / "ready").exists()


# --------------------------------------------------------------------------- limits

def test_child_identity_requires_root_unless_explicitly_waived():
    if os.name != "posix":
        pytest.skip("POSIX-only behaviour")
    if os.geteuid() == 0:
        uid, gid = resolve_child_identity("nobody")
        assert uid not in (None, 0) and gid is not None
        assert resolve_child_identity("nobody", allow_unprivileged=True)[0] == uid
    else:
        with pytest.raises(RuntimeError, match="root"):
            resolve_child_identity("nobody")
        assert resolve_child_identity("nobody", allow_unprivileged=True) == (None, None)
    with pytest.raises(RuntimeError, match="different UID"):
        resolve_child_identity(str(os.geteuid()))


def test_child_user_default_does_not_break_windows_workers(monkeypatch):
    monkeypatch.setattr(os, "name", "nt")
    assert resolve_child_identity(None) == (None, None)
    assert resolve_child_identity(DEFAULT_CHILD_USER) == (None, None)
    with pytest.raises(RuntimeError, match="only on POSIX"):
        resolve_child_identity("some-other-account")


def test_execution_bounds_are_sized_to_the_machine(tmp_path: Path):
    args = build_parser().parse_args([
        "worker", "--id", "w1", "--coordinator", "127.0.0.1:1", "--secret-file", "s",
        "--cert", "c", "--key", "k", "--ca", "a", "--cache-dir", str(tmp_path / "cache"),
        "--data-dir", str(tmp_path / "data"), "--data-port", "1"])
    shared = machine_limits(args, shared_account=True)
    dedicated = machine_limits(args, shared_account=False)
    assert shared["task_timeout_seconds"] >= 3600
    assert shared["rlimit_cpu_seconds"] >= 3600
    assert shared["rlimit_fsize_bytes"] >= 1024 ** 3
    assert shared["rlimit_nproc"] is None and dedicated["rlimit_nproc"] >= 1024
    if shared["rlimit_as_bytes"] is not None:
        assert shared["rlimit_as_bytes"] >= 2 * 1024 ** 3
    explicit = build_parser().parse_args([
        "worker", "--id", "w1", "--coordinator", "127.0.0.1:1", "--secret-file", "s",
        "--cert", "c", "--key", "k", "--ca", "a", "--cache-dir", "x", "--data-dir", "y",
        "--data-port", "1", "--task-timeout", "5", "--task-processes", "7"])
    assert machine_limits(explicit, shared_account=True)["task_timeout_seconds"] == 5
    assert machine_limits(explicit, shared_account=True)["rlimit_nproc"] == 7


def test_detect_advertise_host_prefers_the_route_to_the_coordinator():
    assert _detect_advertise_host("127.0.0.1", 8800) == "127.0.0.1"
    assert _detect_advertise_host("no-such-host.invalid", 8800) == "127.0.0.1"


# ---------------------------------------------------------------------- overload

_WIDE = 160


def _overload_program() -> str:
    """A wide fan-in, and a value well over 1 MiB carried to another task."""
    lines = [
        "from dag_runtime import task",
        "@task",
        "def tile(i):",
        "    import hashlib",
        "    return i, hashlib.sha256(str(i).encode()).digest() * 2048",
        "@task",
        "def gather(*tiles):",
        "    return len(tiles), sum([len(data) for _, data in tiles])",
        "@task",
        "def big():",
        "    return b'x' * 3000000",
        "@task",
        "def size(value):",
        "    return len(value)",
    ]
    lines += [f"t{i:03d} = tile({i})" for i in range(_WIDE)]
    lines.append("g = gather(" + ", ".join(f"t{i:03d}" for i in range(_WIDE)) + ")")
    lines += ["b = big()", "n = size(b)", "print(g, n)"]
    return "\n".join(lines) + "\n"


@needs_openssl
def test_wide_fan_in_large_values_and_back_to_back_runs(cluster_factory, tmp_path: Path):
    """Each of these once failed a run or dropped the workers:

    - more values gathered onto one task than a worker sends or receives at once;
    - a task output over 1 MiB;
    - the burst of replies when a large run ends, which disconnected every worker
      so that the next run found no machines.
    """
    # Small control queues make every burst overflow them, so the test does not
    # depend on how fast this machine happens to handle messages.
    cluster = cluster_factory(["w1", "w2"], slots=2,
                              node_args=("--inbound-queue", "4", "--outbound-queue", "4"))
    program = tmp_path / "project" / "wide.py"
    program.parent.mkdir()
    program.write_text(_overload_program())
    first = asyncio.run(runs.run(cluster.config, program, run_id="wide-1"))
    assert first.succeeded, (first.status.status, first.error)
    assert first.output == f"({_WIDE}, {_WIDE * 65536}) 3000000"
    workers = {task.worker_id for task in first.status.tasks if task.worker_id}
    assert workers == {"w1", "w2"}

    simple = tmp_path / "project2" / "simple.py"
    simple.parent.mkdir()
    simple.write_text("a = 20 + 22\nb = a * 2\n")
    second = asyncio.run(runs.run(cluster.config, simple, run_id="wide-2"))
    assert second.succeeded, (second.status.status, second.error)
    time.sleep(2)  # let the end-of-run cleanup of both runs finish
    status = asyncio.run(cluster.status())
    assert sorted((w.worker_id, w.generation, w.online) for w in status.workers) == [
        ("w1", 1, True), ("w2", 1, True)], "a worker was disconnected"


def test_value_budget_is_sized_to_the_disk(tmp_path: Path):
    from dpr.node import data_budget
    budget = data_budget(tmp_path / "data")
    assert 256 * 1024 * 1024 <= budget <= 64 * 1024 ** 3
    args = build_parser().parse_args([
        "worker", "--id", "w", "--coordinator", "127.0.0.1:1", "--secret-file", "s",
        "--cache-dir", "c", "--data-dir", "d", "--data-port", "1",
        "--cert", "c", "--key", "k", "--ca", "a"])
    assert args.data_bytes is None and args.max_value_bytes == 256 * 1024 * 1024
