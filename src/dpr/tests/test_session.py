"""The session, end to end: real `dpr start` processes hosting, joining and running."""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import time

import pytest

from dpr.credentials import openssl

from .sessions import Session, process_alive, wait_gone

needs_openssl = pytest.mark.skipif(openssl() is None, reason="hosting needs openssl")

COMMAND_WORDS = ("host", "join <code>", "run <file>", "status", "approve <name>", "kick <name>",
                 "exit")


def approve_join(host: Session, worker: Session, code: str) -> str:
    """`join` on the worker, `approve` on the host: the numbers both screens show match."""
    worker.write(f"join {code}")
    notice = host.read(30)
    number = re.search(r"wants to join \((\d{6})\)", notice).group(1)
    assert host.send(f"approve {number}").startswith("approved ")
    joined = worker.read(30).splitlines()
    assert joined == [f"waiting for approval ({number})", joined[1]]
    assert joined[1].startswith("joined ")
    return number


def _programs(root: Path) -> Path:
    folder = root / "project"
    folder.mkdir()
    (folder / "good.py").write_text(
        "def square(n):\n    return n * n\n\na = square(3)\nb = square(4)\nprint('sum', a + b)\n")
    (folder / "bad.py").write_text("x = 6 * 7\nprint('answer', x)\ny = 1 / 0\n")
    (folder / "slow.py").write_text("import time\ntime.sleep(60)\nprint('never')\n")
    return folder


def test_banner_lists_every_command_and_nothing_else(tmp_path: Path):
    session = Session(tmp_path / "home", tmp_path)
    try:
        lines = [line for line in session.banner.splitlines() if line.strip()]
        assert lines[0].startswith("dpr ")
        assert [re.split(r"\s{2,}", line.strip())[0] for line in lines[1:]] == list(COMMAND_WORDS)
        assert session.send("status").strip() == "not in a cluster"
        assert session.send("run nothing.py").strip() == "no such file: nothing.py"
        (tmp_path / "x.py").write_text("x = 1\n")
        assert session.send("run x.py").strip() == "not hosting"
        assert session.send("join").strip() == "usage: join <code>"
        assert session.send("join dpr3_nonsense").strip() == "invalid code"
        unknown = session.send("frobnicate").splitlines()
        assert unknown[0] == "unknown command: frobnicate" and len(unknown) == 1 + len(COMMAND_WORDS)
        assert session.send("").strip() == ""
    finally:
        assert session.close() == 0


def test_one_session_per_machine(tmp_path: Path):
    first = Session(tmp_path / "home", tmp_path)
    try:
        second = Session(tmp_path / "home", tmp_path)
        assert second.banner.strip() == "dpr is already running on this machine"
        assert second.close() == 1
    finally:
        first.close()


@needs_openssl
def test_host_join_run_status_and_leave(tmp_path: Path):
    project = _programs(tmp_path)
    host = Session(tmp_path / "host-home", project)
    worker = Session(tmp_path / "worker-home", project)
    try:
        hosting = host.send("host", timeout=90).splitlines()
        assert hosting[0].startswith("hosting ") and hosting[1].startswith("code ")
        code = re.search(r"dpr3_\S+", hosting[1]).group(0)
        assert host.send("run good.py").strip() == "no machines joined"

        approve_join(host, worker, code)
        assert worker.send("status").split() == ["joined", hosting[0].split()[1], "state", "connected"]
        assert worker.send("run good.py").strip() == "only the host runs programs"

        status = host.send("status").splitlines()
        assert status[2].startswith("machines ") and "idle" in status[2]

        good = host.send("run good.py").splitlines()
        assert good[0] == "sum 25"
        assert re.fullmatch(r"done in \d+\.\ds, saved to dpr-results[/\\]good-\d{8}-\d{6}\.txt", good[1])
        saved = project / good[1].split("saved to ")[1]
        assert "result   succeeded" in saved.read_text()

        bad = host.send("run bad.py").splitlines()
        assert bad[:2] == ["answer 42", "ZeroDivisionError: division by zero"]
        assert bad[2].startswith("failed after ")

        # Leaving stops what the session started, and nothing else is left behind.
        worker_nodes = worker.node_pids()
        assert worker_nodes and worker.close() == 0
        assert wait_gone(worker_nodes)
        assert not list((tmp_path / "worker-home" / "state" / "pids").glob("*.pid"))

        # The machine rejoins without a code, and keeps its identity.
        worker = Session(tmp_path / "worker-home", project)
        assert worker.send("join").strip().startswith("joined ")
        assert len(re.findall(r" (idle|busy) ", host.send("status"))) == 1
        claims = json.loads((tmp_path / "host-home" / "host" / "claims.json").read_text())
        assert len(claims["claims"]) == 1
    finally:
        host_nodes = host.node_pids()
        worker.close()
        assert host.close() == 0
        assert wait_gone(host_nodes)


@needs_openssl
@pytest.mark.skipif(os.name != "posix", reason="Ctrl-C is delivered as SIGINT")
def test_ctrl_c_cancels_the_run_and_frees_the_machine(tmp_path: Path):
    project = _programs(tmp_path)
    host = Session(tmp_path / "host-home", project)
    worker = Session(tmp_path / "worker-home", project)
    try:
        code = re.search(r"dpr3_\S+", host.send("host", timeout=90)).group(0)
        approve_join(host, worker, code)
        host.write("run slow.py")
        deadline = time.monotonic() + 30
        time.sleep(2)
        host.interrupt()
        assert re.fullmatch(r"cancelled after \d+\.\ds", host.read().strip().split("\r")[-1].strip())
        while time.monotonic() < deadline:
            if re.search(r"idle\s+0/", host.send("status")):
                break
            time.sleep(0.2)
        else:
            raise AssertionError("worker still busy after cancellation")
    finally:
        worker.close()
        host.close()


@needs_openssl
@pytest.mark.skipif(os.name != "posix", reason="kills the session process abruptly")
def test_nodes_stop_when_the_session_dies(tmp_path: Path):
    project = _programs(tmp_path)
    host = Session(tmp_path / "host-home", project)
    host.send("host", timeout=90)
    nodes = host.node_pids()
    assert nodes and all(process_alive(pid) for pid in nodes)
    host.process.kill()
    host.process.wait(timeout=10)
    assert wait_gone(nodes), "a node outlived its session"
    # The next session starts clean, on the same port, with the same credentials.
    again = Session(tmp_path / "host-home", project)
    try:
        assert again.send("host", timeout=90).startswith("hosting ")
    finally:
        again.close()


def test_earlier_layout_is_cleared_on_start(tmp_path: Path):
    home = tmp_path / "home"
    (home / "cred").mkdir(parents=True)
    (home / "cred" / "cluster.json").write_text('{"version": 1, "credentials_dir": "x"}')
    (home / "state" / "w1" / "cache").mkdir(parents=True)
    (home / "state" / "logs").mkdir(parents=True)
    (home / "state" / "logs" / "coordinator.log").write_text("old")
    session = Session(home, tmp_path)
    session.close()
    assert not (home / "cred").exists()
    assert not (home / "state" / "w1").exists()
    assert not (home / "state" / "logs" / "coordinator.log").exists()


@needs_openssl
def test_the_host_decides_who_joins_and_who_stays(tmp_path: Path):
    project = _programs(tmp_path)
    host = Session(tmp_path / "host-home", project)
    first = Session(tmp_path / "first-home", project)
    second = Session(tmp_path / "second-home", project)
    try:
        code = re.search(r"dpr3_\S+", host.send("host", timeout=90)).group(0)
        assert host.send("approve anyone").strip() == "no machine named anyone is waiting"
        approve_join(host, first, code)

        # A second machine with the same name: the number tells them apart.
        second.write(f"join {code}")
        number = re.search(r"\((\d{6})\)", host.read(30)).group(1)
        status = host.send("status")
        assert re.search(rf"^waiting\s+\S+\s+{number}$", status, re.M)
        assert host.send(f"kick {number}").startswith("removed ")
        assert second.read(30).splitlines()[-1] == "refused by the host"

        # Removing a member cuts it off; its saved identity is gone with it.
        assert host.send("kick w1").startswith("removed ")
        deadline = time.monotonic() + 20
        while True:
            reply = first.send("status").strip()
            if reply.startswith("removed by the host"):
                break
            assert time.monotonic() < deadline, reply
            time.sleep(0.3)
        assert first.send("join").strip() == "usage: join <code>"
        assert first.send(f"join {code}", timeout=30).strip() == "refused by the host"
        assert "machines  none" in host.send("status")
    finally:
        for session in (first, second, host):
            session.close()
