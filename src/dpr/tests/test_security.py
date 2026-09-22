"""Regression tests for the hardening: each one pins down a way the system must not fail."""
from __future__ import annotations

import json
import os
from pathlib import Path
import socket
import ssl
import sys
import time

import pytest

from dpr import credentials, enroll, runs
from dpr.home import Home, HomeError, write_text
from dpr.processes import _DPR_COMMAND
from dpr.text import printable

needs_openssl = pytest.mark.skipif(credentials.openssl() is None, reason="needs openssl")
MACHINE = "d" * 32


# ------------------------------------------------------------------- enrollment

@pytest.fixture
def enrollment(tmp_path: Path):
    creds = tmp_path / "creds"
    credentials.create(creds, workers=["w1", "w2"], client="owner")
    token = enroll.new_token()
    admissions = enroll.Admissions(creds / "claims.json", ["w1", "w2"])
    server = enroll.serve(credentials=creds, admissions=admissions, token=token,
                          coordinator_port=8740, host="127.0.0.1", port=0)
    pin = enroll.fingerprint(ssl.PEM_cert_to_DER_cert((creds / "coordinator.pem").read_text()))
    yield {"host": "127.0.0.1", "port": server.server_address[1], "token": token,
           "fingerprint": pin, "creds": creds, "admissions": admissions}
    server.shutdown()


def _tls_socket(code: dict) -> ssl.SSLSocket:
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    raw = socket.create_connection((code["host"], code["port"]), timeout=10)
    return context.wrap_socket(raw)


def _raw_request(code: dict, head: str, body: bytes = b"") -> bytes:
    with _tls_socket(code) as tls:
        tls.sendall(head.encode("utf-8") + b"\r\n\r\n" + body)
        chunks = []
        while True:
            try:
                chunk = tls.recv(65536)
            except (OSError, ssl.SSLError):
                break
            if not chunk:
                break
            chunks.append(chunk)
    return b"".join(chunks)


@needs_openssl
def test_join_secrets_are_long_enough():
    assert len(enroll.new_token()) == 16            # 96 random bits
    assert len(enroll.fingerprint(b"x")) == 22      # 128 bits of SHA-256


@needs_openssl
def test_wrong_tokens_are_answered_slowly(enrollment):
    started = time.monotonic()
    with pytest.raises(enroll.JoinError, match="code no longer valid"):
        enroll.fetch({**enrollment, "token": enroll.new_token()}, machine=MACHINE, name="x")
    assert time.monotonic() - started >= enroll.FAILURE_DELAY * 0.9


@needs_openssl
@pytest.mark.parametrize("length", ["-5", "999999999", "abc", "", "١٢"])
def test_body_length_is_checked_before_reading(enrollment, length):
    head = f"POST {enroll.PATH} HTTP/1.1\r\nHost: x"
    if length:
        head += f"\r\nContent-Length: {length}"
    started = time.monotonic()
    reply = _raw_request(enrollment, head, b'{"token": "x"}')
    assert b" 400 " in reply.split(b"\r\n", 1)[0]
    assert time.monotonic() - started < 5, "the server must not wait for a body it refused"


@needs_openssl
def test_client_fields_are_validated_and_names_made_inert(enrollment):
    body = json.dumps({"token": enrollment["token"], "machine": "../../etc", "name": "x"}).encode()
    reply = _raw_request(enrollment, f"POST {enroll.PATH} HTTP/1.1\r\nContent-Length: {len(body)}",
                         body)
    assert b" 400 " in reply.split(b"\r\n", 1)[0]
    hostile = "evil\x1b]0;owned\x07\x1b[2J\u202egnp.exe" + "x" * 100
    seen = []
    with pytest.raises(enroll.JoinError, match="not approved in time"):
        enroll.fetch(enrollment, machine=MACHINE, name=hostile, waiting=seen.append, patience=0)
    waiting = list(enrollment["admissions"]._waiting.values())
    assert waiting[0]["name"] == ("evil0owned2Jgnp.exe" + "x" * 100)[:32]
    assert enrollment["admissions"].approve(waiting[0]["ref"]) == "approved"
    assert enroll.fetch(enrollment, machine=MACHINE, name=hostile)["id"] == "w1"
    names = enroll.member_names(enrollment["creds"] / "claims.json")
    assert names["w1"] == ("evil0owned2Jgnp.exe" + "x" * 100)[:32]
    assert all(ch.isalnum() or ch in "._-" for ch in names["w1"])


@needs_openssl
def test_server_banner_does_not_reveal_versions(enrollment):
    reply = _raw_request(enrollment, "GET / HTTP/1.1\r\nHost: x")
    assert b"Python" not in reply and b"BaseHTTP" not in reply


@needs_openssl
def test_old_tls_is_refused(enrollment):
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    context.maximum_version = ssl.TLSVersion.TLSv1_2
    with socket.create_connection(("127.0.0.1", enrollment["port"]), timeout=10) as raw:
        with pytest.raises((ssl.SSLError, OSError)):
            context.wrap_socket(raw).do_handshake()


@needs_openssl
def test_a_flood_cannot_hold_the_service(enrollment, monkeypatch):
    # More idle connections than the service serves at once, then a real join.
    monkeypatch.setattr(enroll, "REQUEST_DEADLINE", 2.0)
    from .test_parts import admit
    admit(enrollment["admissions"], MACHINE, "real")
    idle = []
    for _ in range(enroll.MAX_CONCURRENT + 4):
        try:
            idle.append(_tls_socket(enrollment))
        except (OSError, ssl.SSLError):
            pass  # beyond the limit the service simply hangs up
    assert len(idle) >= enroll.MAX_CONCURRENT
    try:
        time.sleep(0.3)
        deadline = time.monotonic() + enroll.REQUEST_DEADLINE + 15
        while True:
            try:
                assert enroll.fetch(enrollment, machine=MACHINE, name="real")["id"] == "w1"
                break
            except enroll.JoinError:
                assert time.monotonic() < deadline, "idle clients held the service past its deadline"
                time.sleep(0.5)
    finally:
        for sock in idle:
            sock.close()


def test_payloads_that_are_not_identity_bundles_are_refused():
    good = {"port": 8740, "id": "w1", "ca": "-----BEGIN CERTIFICATE-----\n",
            "cert": "-----BEGIN CERTIFICATE-----\n", "key": "-----BEGIN PRIVATE KEY-----\n",
            "secret": "a" * 64}
    assert enroll._checked(dict(good)) == good
    for change in ({"port": 0}, {"port": True}, {"port": "8740"}, {"id": "../w1"},
                   {"id": "w1\n"}, {"ca": "not pem"}, {"key": "nothing"}, {"secret": "zz"},
                   {"cert": "-----BEGIN CERTIFICATE-----" + "x" * 70000}):
        with pytest.raises(enroll.JoinError):
            enroll._checked({**good, **change})
    with pytest.raises(enroll.JoinError):
        enroll._checked(["not", "a", "dict"])


# ------------------------------------------------------------------ terminal text

@pytest.mark.parametrize("hostile, expected", [
    ("\x1b]0;title\x07after", "after"),                  # retitle the terminal
    ("\x1b]52;c;ZXZpbA==\x07x", "x"),                    # write the clipboard
    ("a\x1b[2J\x1b[Hb", "ab"),                           # clear and move
    ("\x1bP+q544e\x1b\\ok", "ok"),                       # device control string
    ("hid\rden", "hid\nden"),                            # overwrite a line
    ("\u202egnp.exe", "gnp.exe"),                        # reverse text direction
    ("nul\x00bell\x07c1\x9b", "nulbellc1"),
])
def test_remote_text_cannot_drive_the_terminal(hostile, expected):
    assert printable(hostile) == expected


def test_colour_survives_on_screen_but_not_in_files():
    assert printable("\x1b[31mred") == "\x1b[31mred\x1b[0m"
    assert printable("\x1b[31mred", colour=False) == "red"


# ------------------------------------------------------------------ home folder

def test_a_folder_with_other_files_is_never_adopted_or_erased(tmp_path: Path):
    precious = tmp_path / "documents"
    (precious / "state").mkdir(parents=True)
    (precious / "state" / "thesis.txt").write_text("years of work")
    (precious / "notes.txt").write_text("keep")
    home = Home(precious)
    with pytest.raises(HomeError):
        home.claim()
    home.erase()
    assert (precious / "state" / "thesis.txt").read_text() == "years of work"
    assert (precious / "notes.txt").exists()


def test_erase_removes_only_what_dpr_made(tmp_path: Path):
    home = Home(tmp_path / "home")
    home.claim()
    home.prepare()
    home.machine_id()
    (home.path / "mine.txt").write_text("added later by hand")
    home.erase()
    assert (home.path / "mine.txt").exists()
    assert sorted(p.name for p in home.path.iterdir()) == ["mine.txt"]
    (home.path / "mine.txt").unlink()
    Home(tmp_path / "fresh").claim()
    Home(tmp_path / "fresh").erase()
    assert not (tmp_path / "fresh").exists()


def test_earlier_layout_is_adopted(tmp_path: Path):
    home = tmp_path / "home"
    (home / "cred").mkdir(parents=True)
    (home / "cred" / "cluster.json").write_text('{"version": 1, "credentials_dir": "x"}')
    (home / "state" / "pids").mkdir(parents=True)
    assert Home(home).owned()
    Home(home).claim()
    assert (home / ".dpr-home").is_file()


def test_familiar_names_alone_never_make_a_folder_dpr_s(tmp_path: Path):
    # A folder whose entries merely share dpr's names ("state", "host", ...) is not
    # dpr's until their contents say so.
    folder = tmp_path / "project"
    (folder / "state").mkdir(parents=True)
    (folder / "state" / "thesis.txt").write_text("years of work")
    (folder / "host").mkdir()
    home = Home(folder)
    assert not home.owned()
    with pytest.raises(HomeError):
        home.claim()
    home.erase()
    assert (folder / "state" / "thesis.txt").read_text() == "years of work"


@pytest.mark.skipif(os.name != "posix", reason="POSIX permissions")
def test_home_is_private(tmp_path: Path):
    home = Home(tmp_path / "home")
    home.path.mkdir(mode=0o755)
    home.claim()
    home.prepare()
    for path in (home.path, home.state, home.logs, home.pids):
        assert path.stat().st_mode & 0o077 == 0, path
    write_text(home.path / "secret", "x")
    assert (home.path / "secret").stat().st_mode & 0o077 == 0


def test_writes_never_follow_a_planted_temporary_file(tmp_path: Path, monkeypatch):
    import secrets as _secrets
    monkeypatch.setattr(_secrets, "token_hex", lambda n=16: "f" * (2 * n))
    target = tmp_path / "outside"
    target.write_text("untouched")
    planted = tmp_path / f".state.json.{'f' * 16}.tmp"
    try:
        planted.symlink_to(target)
    except (OSError, NotImplementedError):
        pytest.skip("symlinks unavailable")
    with pytest.raises(FileExistsError):
        write_text(tmp_path / "state.json", "new")
    assert target.read_text() == "untouched"


# ---------------------------------------------------------------- processes

@pytest.mark.parametrize("command, ours", [
    ("/usr/bin/python3 -m dpr.node coordinator --port 1", True),
    ("python -m dpr --profile x worker", True),                        # earlier release
    ('"C:\\Python\\python.exe" -m dpr.node worker ', True),
    ("python -m dprint fmt", False),
    ("python -m dpr_tools", False),
    ("python evil.py --note=-m dpr", False),
    ("vim notes-m dpr", False),
])
def test_only_dpr_processes_are_recognised(command, ours):
    assert (_DPR_COMMAND.search(command + " ") is not None) is ours


# ------------------------------------------------------------------- packaging

def test_credential_stores_are_never_packaged(tmp_path: Path):
    from program_package import build_package
    (tmp_path / "job.py").write_text("x = 1\n")
    for secret in (".ssh/id_ed25519", ".aws/credentials", ".gnupg/private.key", "id_rsa",
                   ".netrc", ".git-credentials", "sub/.kube/config"):
        path = tmp_path / secret
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("secret")
    (tmp_path / "data.csv").write_text("a,b\n")
    files = {item.path for item in build_package(tmp_path).manifest.files}
    assert files == {"job.py", "data.csv"}


def test_home_folder_and_drive_root_are_never_sent(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))
    program = tmp_path / "job.py"
    program.write_text("x = 1\n")
    with pytest.raises(RuntimeError, match="folder of its own"):
        runs.package(program)
    project = tmp_path / "project"
    project.mkdir()
    (project / "job.py").write_text("x = 1\n")
    assert runs.package(project / "job.py")[2] == "job.py"


def test_results_never_follow_links_or_overwrite(tmp_path: Path):
    from .test_parts import _outcome
    program = tmp_path / "job.py"
    program.write_text("x = 1\n")
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    try:
        (tmp_path / runs.RESULTS_DIR).symlink_to(elsewhere, target_is_directory=True)
    except (OSError, NotImplementedError):
        pytest.skip("symlinks unavailable")
    assert runs.save(_outcome(program, "succeeded")).saved is None
    assert not list(elsewhere.iterdir())
    (tmp_path / runs.RESULTS_DIR).unlink()
    first = runs.save(_outcome(program, "succeeded")).saved
    second = runs.save(_outcome(program, "succeeded")).saved
    assert first and second and first != second and first.exists() and second.exists()


def test_saved_results_are_inert(tmp_path: Path):
    from .test_parts import _outcome
    program = tmp_path / "job.py"
    program.write_text("x = 1\n")
    outcome = _outcome(program, "failed", detail="boom\x1b]0;x\x07", kind="Evil\x1b[2J")
    text = runs.save(outcome).saved.read_text()
    assert "\x1b" not in text and "\x07" not in text


# ---------------------------------------------------------------- the host node

@needs_openssl
@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="reads /proc")
def test_join_token_stays_out_of_argv_and_environment(tmp_path: Path, monkeypatch):
    from dpr.cluster import Hosting
    monkeypatch.setenv("DPR_HOME", str(tmp_path / "home"))
    home = Home.default()
    home.claim()
    home.prepare()
    hosting = Hosting.start(home)
    try:
        token = enroll.decode(hosting.code)["token"]
        proc = Path(f"/proc/{hosting.child.pid}")
        assert token.encode() not in (proc / "cmdline").read_bytes()
        assert token.encode() not in (proc / "environ").read_bytes()
    finally:
        hosting.stop()


@needs_openssl
def test_coordinator_listens_only_where_told(tmp_path: Path):
    from .test_nodes import Cluster
    address = enroll.local_address()
    if address.startswith("127."):
        pytest.skip("no non-loopback address on this machine")
    cluster = Cluster(tmp_path, ["w1"])
    try:
        with pytest.raises(OSError):
            socket.create_connection((address, cluster.port), timeout=2).close()
    finally:
        cluster.close()


def test_windows_children_get_their_system_folder(monkeypatch):
    from worker.runtime import WorkerExecutionRuntime
    monkeypatch.setattr(os, "name", "nt")
    monkeypatch.setenv("SYSTEMROOT", "C:\\Windows")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "leak")
    env = WorkerExecutionRuntime._child_environment(None)
    assert env["SYSTEMROOT"] == "C:\\Windows" and "System32" in env["PATH"]
    assert "AWS_SECRET_ACCESS_KEY" not in env and "HOME" not in env


@needs_openssl
def test_operator_access_only_from_the_host_itself(tmp_path: Path):
    import asyncio
    from dataclasses import replace
    from networking import CoordinatorClient
    from .test_nodes import Cluster, _port

    address = enroll.local_address()
    if address.startswith("127."):
        pytest.skip("no non-loopback address on this machine")
    cluster = Cluster.__new__(Cluster)
    cluster.root, cluster.creds = tmp_path, tmp_path / "creds"
    credentials.create(cluster.creds, workers=["w1"], client="admin")
    cluster.port, cluster.processes = _port(), []
    cluster.env = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[2]))
    cluster.start_node([
        "coordinator", "--bind", address, "--bind", "127.0.0.1", "--port", str(cluster.port),
        "--local-operators", "--auth-file", str(cluster.creds / "auth.json"),
        "--client-id", "admin", *cluster.tls("coordinator")])
    try:
        from runtime_security import TlsCredentials, TlsPolicy
        from networking import CoordinatorClientConfig
        local = CoordinatorClientConfig(
            node_id="admin", secret=bytes.fromhex((cluster.creds / "admin.secret").read_text().strip()),
            coordinator_host="127.0.0.1", coordinator_port=cluster.port,
            server_hostname=credentials.SERVER_NAME, operation_timeout=5,
            tls_policy=TlsPolicy(TlsCredentials(*cluster.tls("admin")[1::2])))

        async def status(config):
            async with CoordinatorClient(config) as client:
                return await client.cluster_status()

        deadline = time.monotonic() + 20
        while True:
            try:
                asyncio.run(status(local))
                break
            except Exception:
                assert time.monotonic() < deadline
                time.sleep(0.1)
        with pytest.raises(Exception):
            asyncio.run(status(replace(local, coordinator_host=address)))
    finally:
        cluster.close()
