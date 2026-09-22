"""The two roles a machine takes in a session: host a cluster, or work for one.

Both run as supervised background nodes (dpr.processes).  Credentials, ports and
leftovers are handled here so the person at the prompt never deals with them:

- the host's credentials are created on first use and renewed before they expire;
- busy ports are swapped for free ones;
- a machine that joins again reuses its identity, and can rejoin without a code.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import secrets
import socket
import ssl
import time

from dpr import credentials, enroll
from dpr.home import Home, read_json, remove, write_json, write_text
from dpr.node import REMOVED
from dpr.processes import Child, free_port, spawn, wait_for
from networking import CoordinatorClient, CoordinatorClientConfig
from runtime_security import TlsCredentials, TlsPolicy

CLIENT = "owner"
JOINABLE = 32
HOST_PORT = 8740
ENROLL_PORT = 8750
DATA_PORT = 8841
RENEW_BEFORE = 30 * 86400
START_TIMEOUT = 20.0


def _fresh_log(log: Path) -> Path:
    """Start a node's log afresh, keeping the previous one as `.1`."""
    try:
        os.replace(log, log.with_name(log.name + ".1"))
    except OSError:
        pass
    return log


def _last_error(log: Path) -> str:
    """The most recent error a node logged, without its timestamp."""
    try:
        lines = log.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return ""
    for line in reversed(lines):
        if " ERROR " in line:
            return line.split(": ", 1)[-1].strip()
    return ""


# ----------------------------------------------------------------------------- host

class Hosting:
    def __init__(self, home: Home, child: Child, port: int, address: str, code: str) -> None:
        self.home = home
        self.child = child
        self.port = port
        self.address = address
        self.code = code

    @classmethod
    def start(cls, home: Home) -> "Hosting":
        profile = _host_profile(home)
        directory = home.host
        ready_file = home.state / "host.ready"
        ready_file.unlink(missing_ok=True)
        (home.state / "board.json").unlink(missing_ok=True)
        log = _fresh_log(home.logs / "host.log")
        port = free_port(int(profile.get("port") or HOST_PORT))
        token = enroll.new_token()
        # Listen on the LAN address the code names, plus loopback for this session;
        # never on every interface (VPNs, public networks).
        address = enroll.local_address()
        binds = ["--bind", address] + (["--bind", "127.0.0.1"] if address != "127.0.0.1" else [])
        child = spawn("host", [
            "coordinator", *binds, "--port", str(port), "--local-operators",
            "--enroll-port", str(free_port(ENROLL_PORT)),
            "--auth-file", str(directory / "auth.json"), "--client-id", CLIENT,
            "--cert", str(directory / "coordinator.pem"),
            "--key", str(directory / "coordinator.key"), "--ca", str(directory / "ca.pem"),
            "--history-db", str(home.state / "history.sqlite3"),
            "--ready-file", str(ready_file), "--board-file", str(home.state / "board.json"),
            "--log-file", str(log),
        ], pid_dir=home.pids, stdin_line=token)
        try:
            wait_for(lambda: read_json(ready_file) is not None or not child.alive(), START_TIMEOUT)
        except BaseException:
            child.stop()
            raise
        ready = read_json(ready_file)
        if ready is None:
            child.stop()
            raise RuntimeError(f"cannot start: {_last_error(log) or 'no response'}")
        if ready["port"] != profile.get("port"):
            write_json(directory / "host.json", {**_strip(profile), "port": ready["port"]})
        certificate = ssl.PEM_cert_to_DER_cert(
            (directory / "coordinator.pem").read_text(encoding="ascii"))
        code = enroll.encode(address, ready["enroll_port"], token, enroll.fingerprint(certificate))
        return cls(home, child, ready["port"], address, code)

    def alive(self) -> bool:
        return self.child.alive()

    def stop(self) -> None:
        self.child.stop()

    def failure(self) -> str:
        return _last_error(self.home.logs / "host.log")

    def config(self) -> CoordinatorClientConfig:
        directory = self.home.host
        return CoordinatorClientConfig(
            node_id=CLIENT,
            secret=bytes.fromhex((directory / f"{CLIENT}.secret").read_text("ascii").strip()),
            coordinator_host="127.0.0.1",
            coordinator_port=self.port,
            server_hostname=credentials.SERVER_NAME,
            tls_policy=TlsPolicy(TlsCredentials(directory / f"{CLIENT}.pem",
                                                directory / f"{CLIENT}.key",
                                                directory / "ca.pem")),
            operation_timeout=15.0,
        )

    async def machines(self):
        async with CoordinatorClient(self.config()) as client:
            return await client.cluster_status()

    def names(self) -> dict[str, str]:
        """Approved machines: identity -> name."""
        return enroll.member_names(self.home.host / "claims.json")

    def waiting(self) -> list[dict]:
        """Machines asking to join: [{ref, name, code}]."""
        board = read_json(self.home.state / "board.json") or {}
        return [entry for entry in board.get("waiting", [])
                if isinstance(entry, dict) and all(isinstance(entry.get(key), str)
                                                   for key in ("ref", "name", "code"))]

    def decide(self, action: str, value: str) -> str:
        """Send approve/deny/kick to the node and wait for its answer."""
        command = secrets.token_hex(8)
        if not self.child.send(json.dumps({"id": command, action: value})):
            return "host stopped"
        answer: list[str] = []

        def answered() -> bool:
            results = (read_json(self.home.state / "board.json") or {}).get("results", {})
            if isinstance(results, dict) and isinstance(results.get(command), str):
                answer.append(results[command])
                return True
            return False

        wait_for(answered, 5.0)
        return answer[0] if answer else "no answer"


def _strip(profile: dict) -> dict:
    return {key: value for key, value in profile.items() if key != "version"}


def _host_profile(home: Home) -> dict:
    """Usable host credentials: the existing ones, or a fresh set when they are
    missing, damaged or close to expiry."""
    directory = home.host
    profile = read_json(directory / "host.json")
    needed = ["ca.pem", "auth.json", "coordinator.pem", "coordinator.key",
              f"{CLIENT}.pem", f"{CLIENT}.key", f"{CLIENT}.secret"]
    if (profile is not None
            and isinstance(profile.get("workers"), list) and profile["workers"]
            and float(profile.get("expires", 0)) - time.time() > RENEW_BEFORE
            and all((directory / name).is_file() for name in needed)):
        return profile
    remove(directory)
    summary = credentials.create(
        directory, workers=[f"w{index}" for index in range(1, JOINABLE + 1)], client=CLIENT)
    profile = {"port": HOST_PORT, "workers": summary["workers"], "expires": summary["expires"]}
    write_json(directory / "host.json", profile)
    return read_json(directory / "host.json") or profile


# --------------------------------------------------------------------------- worker

class Joined:
    def __init__(self, home: Home, child: Child, host: str, port: int) -> None:
        self.home = home
        self.child = child
        self.host = host
        self.port = port

    @property
    def address(self) -> str:
        return f"{self.host}:{self.port}"

    @staticmethod
    def enroll(home: Home, code: str, waiting=None) -> None:
        """Claim an identity with a join code and keep it for later rejoins.  Blocks
        until the host approves this machine; `waiting(number)` is told the pairing
        number to compare with the host's screen."""
        decoded = enroll.decode(code)
        payload = enroll.fetch(decoded, machine=home.machine_id(), name=socket.gethostname(),
                               waiting=waiting)
        identity = credentials.validate_identity(payload["id"])
        directory = home.worker
        remove(directory)
        for name, key in (("ca.pem", "ca"), (f"{identity}.pem", "cert"),
                          (f"{identity}.key", "key"), (f"{identity}.secret", "secret")):
            write_text(directory / name, payload[key] + ("\n" if key == "secret" else ""))
        try:
            # Keep the identity only if the CA parses and the certificate matches its key.
            TlsPolicy(TlsCredentials(directory / f"{identity}.pem", directory / f"{identity}.key",
                                     directory / "ca.pem")).build_client_context()
        except (OSError, ValueError, ssl.SSLError):
            remove(directory)
            raise enroll.JoinError("join refused") from None
        write_json(directory / "worker.json",
                   {"id": identity, "host": decoded["host"], "port": int(payload["port"])})

    @staticmethod
    def enrolled(home: Home) -> bool:
        return read_json(home.worker / "worker.json") is not None

    @classmethod
    def start(cls, home: Home) -> "Joined":
        directory = home.worker
        profile = read_json(directory / "worker.json")
        if profile is None:
            raise RuntimeError("usage: join <code>")
        identity, host, port = profile["id"], profile["host"], int(profile["port"])

        status_file = home.state / "worker.state"
        status_file.unlink(missing_ok=True)
        log = _fresh_log(home.logs / "worker.log")
        arguments = [
            "worker", "--id", identity, "--coordinator", f"{host}:{port}",
            "--secret-file", str(directory / f"{identity}.secret"),
            "--cert", str(directory / f"{identity}.pem"),
            "--key", str(directory / f"{identity}.key"), "--ca", str(directory / "ca.pem"),
            "--cache-dir", str(home.state / "cache"), "--data-dir", str(home.state / "data"),
            "--data-port", str(free_port(DATA_PORT)),
            "--status-file", str(status_file), "--log-file", str(log),
        ]
        if os.name == "posix" and os.geteuid() != 0:
            arguments.append("--allow-unprivileged-child-execution")
        child = spawn("worker", arguments, pid_dir=home.pids)
        joined = cls(home, child, host, port)
        try:
            wait_for(lambda: joined.state() != "connecting", START_TIMEOUT)
        except BaseException:
            child.stop()
            raise
        if joined.state() == "removed":
            joined.forget()
            raise RuntimeError("removed by the host")
        if joined.state() != "connected":
            reason = _last_error(log)
            child.stop()
            raise RuntimeError(f"cannot connect to {joined.address}"
                               + (f": {reason}" if reason else ""))
        return joined

    def state(self) -> str:
        if not self.child.alive():
            return "removed" if self.child.exit_code == REMOVED else "stopped"
        status = read_json(self.home.state / "worker.state") or {}
        return "connected" if status.get("state") == "connected" else "connecting"

    def alive(self) -> bool:
        return self.child.alive()

    def stop(self) -> None:
        self.child.stop()

    def failure(self) -> str:
        if self.child.exit_code == REMOVED:
            return "removed by the host"
        return _last_error(self.home.logs / "worker.log")

    def forget(self) -> None:
        """Drop this machine's credentials: the host no longer accepts them."""
        remove(self.home.worker)
