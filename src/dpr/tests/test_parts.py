"""The pieces behind the session, one at a time."""
from __future__ import annotations

from pathlib import Path
import subprocess
import sys
import time

import pytest

import protocol as p
from dpr import cli, credentials, enroll, runs
from dpr.home import Home, read_json, write_json
from dpr.processes import SessionLock, clear_leftovers, free_port
from dpr.session import COMMANDS

from .sessions import process_alive, wait_gone

needs_openssl = pytest.mark.skipif(credentials.openssl() is None, reason="needs openssl")


# ------------------------------------------------------------------------------ cli

def test_only_start_and_uninstall_exist_outside_the_session(capsys):
    assert cli.main([]) == 0
    shown = capsys.readouterr().out.split()
    assert [word for word in shown if word in ("start", "uninstall")] == ["start", "uninstall"]
    for retired in ("run", "status", "join", "owner", "host", "stop", "coordinator", "worker"):
        assert cli.main([retired]) == 2
        assert capsys.readouterr().out.startswith(f"unknown command: {retired}")


def test_welcome_prints_every_command_once():
    text = cli.welcome()
    for name, description in COMMANDS:
        assert text.count(f"{name} ") == 1 and description in text
    assert "dpr start" in text and "dpr uninstall" in text
    assert "python -m dpr start" in cli.welcome("python -m dpr")


# ---------------------------------------------------------------------- join codes

MACHINE_A, MACHINE_B, MACHINE_C = "a" * 32, "b" * 32, "c" * 32


def test_join_code_round_trip_and_rejects():
    token, pin = enroll.new_token(), enroll.fingerprint(b"certificate")
    code = enroll.encode("192.168.1.10", 8750, token, pin)
    assert enroll.decode(f"  {code}\n") == {"host": "192.168.1.10", "port": 8750,
                                           "token": token, "fingerprint": pin}
    for bad in ("", "hello", code.replace("dpr3_", "dpr2_"), code.replace(":8750:", ":port:"),
                code.replace(":8750:", ":70000:"), code.rsplit(":", 1)[0], code + ":extra",
                code.replace(token, token[:-1]), code.replace(pin, pin + "x"),
                code.replace("192.168.1.10", "-bad-host"), code.replace("192.168.1.10", "a b")):
        with pytest.raises(enroll.JoinError, match="invalid code"):
            enroll.decode(bad)


def admit(admissions: enroll.Admissions, machine: str, name: str) -> str:
    """Ask as `machine`, then approve it as the host would."""
    decision, _ = admissions.request(machine, name)
    if decision == "waiting":
        ref = next(entry["ref"] for entry in admissions._waiting.values() if entry["name"] == name)
        assert admissions.approve(ref) == "approved"
    decision, identity = admissions.request(machine, name)
    assert decision == "approved"
    return identity


def test_members_keep_one_identity_per_machine(tmp_path: Path):
    admissions = enroll.Admissions(tmp_path / "claims.json", ["w1", "w2"])
    assert admit(admissions, MACHINE_A, "laptop") == "w1"
    assert admit(admissions, MACHINE_B, "desk") == "w2"
    assert admissions.request(MACHINE_A, "laptop") == ("approved", "w1")
    assert admissions.request(MACHINE_C, "third") == ("full", None)
    # Survives a restart of the host.
    again = enroll.Admissions(tmp_path / "claims.json", ["w1", "w2"])
    assert again.request(MACHINE_B, "desk") == ("approved", "w2")
    assert enroll.member_names(tmp_path / "claims.json") == {"w1": "laptop", "w2": "desk"}


@needs_openssl
def test_enrollment_pins_the_host_and_checks_the_token(tmp_path: Path):
    import ssl
    creds = tmp_path / "creds"
    credentials.create(creds, workers=["w1"], client="owner")
    token = enroll.new_token()
    admissions = enroll.Admissions(creds / "claims.json", ["w1"])
    admit(admissions, MACHINE_A, "one")
    server = enroll.serve(credentials=creds, admissions=admissions, token=token,
                          coordinator_port=8740, host="127.0.0.1", port=0)
    try:
        port = server.server_address[1]
        pin = enroll.fingerprint(ssl.PEM_cert_to_DER_cert((creds / "coordinator.pem").read_text()))
        code = {"host": "127.0.0.1", "port": port, "token": token, "fingerprint": pin}
        payload = enroll.fetch(code, machine=MACHINE_A, name="one")
        assert payload["id"] == "w1" and payload["port"] == 8740
        assert payload["cert"] == (creds / "w1.pem").read_text()
        assert enroll.fetch(code, machine=MACHINE_A, name="one")["id"] == "w1"
        with pytest.raises(enroll.JoinError, match="cluster is full"):
            enroll.fetch(code, machine=MACHINE_B, name="two", patience=0)
        with pytest.raises(enroll.JoinError, match="code no longer valid"):
            enroll.fetch({**code, "token": enroll.new_token()}, machine=MACHINE_A, name="one")
        with pytest.raises(enroll.JoinError, match="does not match the host"):
            enroll.fetch({**code, "fingerprint": enroll.fingerprint(b"other")},
                         machine=MACHINE_A, name="one")
    finally:
        server.shutdown()
    with pytest.raises(enroll.JoinError, match="cannot reach"):
        enroll.fetch({**code, "port": free_port(0)}, machine=MACHINE_A, name="one")


# ------------------------------------------------------------------------ processes

def test_session_lock_is_exclusive(tmp_path: Path):
    first, second = SessionLock(tmp_path / "lock"), SessionLock(tmp_path / "lock")
    assert first.acquire()
    assert not second.acquire()
    first.release()
    assert second.acquire()
    second.release()


def test_leftovers_are_cleared_but_only_dpr_processes(tmp_path: Path):
    sleeper = "import time; time.sleep(120)"
    ours = subprocess.Popen([sys.executable, "-c", sleeper, "-m", "dpr.node"])
    theirs = subprocess.Popen([sys.executable, "-c", sleeper])
    try:
        pids = tmp_path / "pids"
        pids.mkdir()
        (pids / "worker.pid").write_text(str(ours.pid))
        (pids / "host.pid").write_text(str(theirs.pid))
        (pids / "stale.pid").write_text("not a pid")
        clear_leftovers(pids)
        ours.wait(timeout=10)
        assert wait_gone([ours.pid])
        assert process_alive(theirs.pid), "a recycled pid must never be killed"
        assert not list(pids.glob("*.pid"))
    finally:
        for process in (ours, theirs):
            process.kill()
            process.wait(timeout=10)


def test_free_port_prefers_the_usual_port():
    port = free_port(0)
    assert free_port(port) == port
    import socket
    with socket.socket() as holder:
        holder.bind(("0.0.0.0", port))
        holder.listen()
        assert free_port(port) != port


# ----------------------------------------------------------------------------- home

def test_damaged_state_reads_as_absent(tmp_path: Path):
    path = tmp_path / "profile.json"
    assert read_json(path) is None
    path.write_text("{not json")
    assert read_json(path) is None
    path.write_text('{"version": 1}')
    assert read_json(path) is None
    write_json(path, {"port": 1})
    assert read_json(path) == {"version": 2, "port": 1}


def test_machine_id_is_stable_and_repaired(tmp_path: Path):
    home = Home(tmp_path)
    first = home.machine_id()
    assert home.machine_id() == first and len(first) == 32
    (tmp_path / "machine-id").write_text("garbage")
    assert home.machine_id() != "garbage"


# ----------------------------------------------------------------------------- runs

def _outcome(program: Path, status: str, *, detail: str = "", kind: str | None = None):
    task = p.RunTaskView(task_id="T1", status="failed" if detail else "committed", worker_id="w1",
                         failure_kind="python_exception" if detail else None, detail=detail,
                         exception_type=kind, stdout_tail="hello\n")
    response = p.RunStatusResponse(run_id="run-1", plan_id="0" * 64, status=status, tasks=(task,),
                                   task_count=1, message_id="m", correlation_id="c")
    return runs.Outcome(program, "run-1", response, 1.25)


def test_outcome_names_the_error_plainly(tmp_path: Path):
    failed = _outcome(tmp_path / "x.py", "failed", detail="division by zero",
                      kind="builtins.ZeroDivisionError")
    assert failed.error == "ZeroDivisionError: division by zero"
    assert failed.output == "hello"
    assert _outcome(tmp_path / "x.py", "succeeded").error == ""


def test_results_are_saved_beside_the_program_and_pruned(tmp_path: Path):
    program = tmp_path / "job.py"
    program.write_text("x = 1\n")
    results = tmp_path / runs.RESULTS_DIR
    results.mkdir()
    for index in range(runs.RESULTS_KEPT + 5):
        (results / f"job-20260101-{index:06d}.txt").write_text("old")
    (results / "other-20260101-000000.txt").write_text("keep")
    (results / "notes.txt").write_text("keep")
    saved = runs.save(_outcome(program, "succeeded")).saved
    assert saved is not None and saved.parent == results
    assert "result   succeeded in 1.2s" in saved.read_text()
    assert len(list(results.glob("job-*.txt"))) == runs.RESULTS_KEPT
    assert (results / "other-20260101-000000.txt").exists() and (results / "notes.txt").exists()


def test_results_folder_is_never_packaged(tmp_path: Path):
    from program_package import build_package
    (tmp_path / "job.py").write_text("x = 1\n")
    before = build_package(tmp_path).package_id
    (tmp_path / runs.RESULTS_DIR).mkdir()
    (tmp_path / runs.RESULTS_DIR / "job-20260101-000000.txt").write_text("output")
    assert build_package(tmp_path).package_id == before


def test_history_keeps_only_recent_runs(tmp_path: Path):
    from coordinator import SQLiteRunHistoryStore
    store = SQLiteRunHistoryStore(tmp_path / "history.sqlite3")
    with store._connect() as db:
        for index in range(5):
            db.execute("INSERT INTO runs VALUES(?, 'p', 'g', 'succeeded', NULL, NULL, ?)",
                       (f"run-{index}", time.time() + index))
    assert store.prune(2) == 3
    assert [store.has_run(f"run-{index}") for index in range(5)] == [False] * 3 + [True] * 2


def test_a_program_that_cannot_run_is_explained_before_submission(tmp_path: Path):
    (tmp_path / "broken.py").write_text("x = (1,\n")
    with pytest.raises(RuntimeError, match=r"SyntaxError: .* \(broken.py, line 1\)"):
        runs.package(tmp_path / "broken.py")
    (tmp_path / "latin.py").write_bytes(b"s = '\xe9'\n")
    with pytest.raises(RuntimeError, match="not UTF-8"):
        runs.package(tmp_path / "latin.py")
