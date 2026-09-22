"""Join codes, and the small HTTPS service that hands out worker identities.

A code is `dpr3_HOST:PORT:TOKEN:PIN`.  The joining machine connects to HOST:PORT,
checks that the certificate it is shown hashes to PIN (so nothing is sent to an
impostor), presents TOKEN, and receives one pre-made worker identity.

- PIN is 128 bits of SHA-256 over the host certificate; TOKEN is 96 random bits.
- Wrong tokens are answered slowly and only a few requests are served at once, so
  guessing is hopeless and a flood cannot exhaust the host.
- Everything a client sends is size-limited and validated before use, and a name
  shown to the host is reduced to plain characters.
- Nothing is handed out until the person at the host approves the machine,
  after checking the pairing number both screens show.  An approved machine that
  joins again gets its identity back; a removed one never does.
"""
from __future__ import annotations

import base64
import hashlib
import http.client
import http.server
import json
from pathlib import Path
import re
import secrets
import socket
import ssl
import threading
import time
from typing import Callable

from dpr import home as _home

PREFIX = "dpr3_"
PATH = "/enroll"
MAX_BODY = 4096
MAX_RESPONSE = 1 << 20
MAX_CONCURRENT = 8
FAILURE_DELAY = 1.0
REQUEST_DEADLINE = 20.0
MAX_WAITING = 8          # machines waiting for approval at once
WAITING_IDLE = 30.0      # a waiting machine that stops asking is forgotten
WAITING_MAX = 600.0      # and nobody waits longer than this
POLL_INTERVAL = 2.0      # how often a waiting machine asks again
PATIENCE = 300.0         # how long a joining machine waits for approval

# Status codes carry the outcome; the joining side maps them to its own words, so
# nothing the host sends is ever printed.
_REFUSALS = {"refused": (410, {"error": "refused"}), "full": (409, {"error": "full"}),
             "busy": (429, {"error": "busy"})}
_JOIN_ERRORS = {403: "code no longer valid", 409: "cluster is full",
                410: "refused by the host", 429: "too many machines waiting; try again"}

_HOST = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9.-]{0,251}[A-Za-z0-9])?\Z")
_TOKEN = re.compile(r"[A-Za-z0-9_-]{16}\Z")
_PIN = re.compile(r"[A-Za-z0-9_-]{22}\Z")
_MACHINE = re.compile(r"[0-9a-f]{32}\Z")
_SECRET = re.compile(r"[0-9a-f]{64}\Z")
_NAME_DROP = re.compile(r"[^A-Za-z0-9._-]")


class JoinError(RuntimeError):
    pass


def fingerprint(certificate_der: bytes) -> str:
    digest = hashlib.sha256(certificate_der).digest()[:16]
    return base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")


def new_token() -> str:
    return secrets.token_urlsafe(12)


def encode(host: str, port: int, token: str, pin: str) -> str:
    return f"{PREFIX}{host}:{port}:{token}:{pin}"


def decode(code: str) -> dict:
    text = code.strip()
    parts = text[len(PREFIX):].split(":") if text.startswith(PREFIX) else []
    if (len(parts) != 4 or not _HOST.fullmatch(parts[0])
            or not (parts[1].isascii() and parts[1].isdigit()) or not 1 <= int(parts[1]) <= 65535
            or not _TOKEN.fullmatch(parts[2]) or not _PIN.fullmatch(parts[3])):
        raise JoinError("invalid code")
    return {"host": parts[0], "port": int(parts[1]), "token": parts[2], "fingerprint": parts[3]}


def clean_name(value: object) -> str:
    """A machine name safe to store and print: letters, digits, '.', '_' and '-'."""
    return _NAME_DROP.sub("", value if isinstance(value, str) else "")[:32] or "machine"


def local_address() -> str:
    """The address other machines on the LAN most likely reach this one by."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
            probe.connect(("8.8.8.8", 53))  # routing lookup only; nothing is sent
            address = probe.getsockname()[0]
            if not address.startswith("127."):
                return address
    except OSError:
        pass
    try:
        for info in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET):
            if not info[4][0].startswith("127."):
                return info[4][0]
    except OSError:
        pass
    return "127.0.0.1"


class Admissions:
    """Who may join this host, decided by the person at the host.

    members  approved machines and the identity each holds (persisted)
    retired  identities taken back from removed machines; never handed out again
    waiting  machines asking to join, each with a pairing number shown on both screens
    refused  machines turned away while this host runs

    A machine gets an identity only after `approve`.  Requests are keyed by the
    machine's secret id, so an approval can only ever be collected by the machine
    that asked.  The node owns this state; the session reads the board file it
    publishes and sends decisions over the node's private stdin.
    """

    def __init__(self, path: Path, identities: list[str], *, board: Path | None = None,
                 clock=time.monotonic) -> None:
        self.path = path
        self.board = board
        self.identities = list(identities)
        self._clock = clock
        self._lock = threading.RLock()
        self._waiting: dict[str, dict] = {}
        self._refused: set[str] = set()
        self._results: dict[str, str] = {}
        self._publish()

    # -- persisted: members and retired identities

    def _load(self) -> tuple[dict[str, dict], set[str]]:
        return read_members(self.path, self.identities)

    def _save(self, members: dict[str, dict], retired: set[str]) -> None:
        _home.write_json(self.path, {"claims": members,
                                     "retired": sorted(retired, key=_natural)})

    def _free(self, members: dict[str, dict], retired: set[str]) -> list[str]:
        taken = {entry["id"] for entry in members.values()} | retired
        return [identity for identity in self.identities if identity not in taken]

    def members(self) -> frozenset[str]:
        """The identities allowed to work right now."""
        with self._lock:
            return frozenset(entry["id"] for entry in self._load()[0].values())

    # -- requests from joining machines (enrollment threads)

    def request(self, machine: str, name: str) -> tuple[str, str | None]:
        """("approved", identity) | ("waiting", number) | ("refused" | "full" | "busy", None)"""
        if not _MACHINE.fullmatch(machine):
            raise ValueError("invalid machine identity")
        with self._lock:
            if machine in self._refused:
                return "refused", None
            members, retired = self._load()
            entry = members.get(machine)
            if entry is not None:
                return "approved", entry["id"]
            self._expire()
            now = self._clock()
            waiting = self._waiting.get(machine)
            if waiting is None:
                if not self._free(members, retired):
                    return "full", None
                if len(self._waiting) >= MAX_WAITING:
                    return "busy", None
                waiting = {"ref": secrets.token_hex(8), "name": clean_name(name),
                           "code": f"{secrets.randbelow(10 ** 6):06d}", "since": now}
                self._waiting[machine] = waiting
                self._publish()
            waiting["seen"] = now
            return "waiting", waiting["code"]

    def _expire(self) -> bool:
        now = self._clock()
        gone = [machine for machine, entry in self._waiting.items()
                if now - entry["seen"] > WAITING_IDLE or now - entry["since"] > WAITING_MAX]
        for machine in gone:
            del self._waiting[machine]
        return bool(gone)

    def tick(self) -> None:
        """Drop requests whose machine stopped asking; call periodically."""
        with self._lock:
            if self._expire():
                self._publish()

    # -- decisions from the host (the node's event loop)

    def _waiting_by_ref(self, ref: str) -> str | None:
        return next((machine for machine, entry in self._waiting.items()
                     if entry["ref"] == ref), None)

    def approve(self, ref: str) -> str:
        """Give a waiting machine an identity: "approved", "full" or "gone"."""
        with self._lock:
            machine = self._waiting_by_ref(ref)
            if machine is None:
                return "gone"
            members, retired = self._load()
            free = self._free(members, retired)
            if not free:
                return "full"
            members[machine] = {"id": free[0], "name": self._waiting[machine]["name"]}
            self._save(members, retired)
            del self._waiting[machine]
            self._publish()
            return "approved"

    def deny(self, ref: str) -> str:
        with self._lock:
            machine = self._waiting_by_ref(ref)
            if machine is None:
                return "gone"
            del self._waiting[machine]
            self._refused.add(machine)
            self._publish()
            return "denied"

    def kick(self, identity: str) -> str:
        """Take a member's identity away for good: "removed" or "gone"."""
        with self._lock:
            members, retired = self._load()
            machine = next((m for m, entry in members.items() if entry["id"] == identity), None)
            if machine is None:
                return "gone"
            del members[machine]
            retired.add(identity)
            self._save(members, retired)
            self._refused.add(machine)
            self._publish()
            return "removed"

    def record(self, command: str, result: str) -> None:
        with self._lock:
            self._results[command] = result
            while len(self._results) > 16:
                self._results.pop(next(iter(self._results)))
            self._publish()

    def _publish(self) -> None:
        if self.board is None:
            return
        waiting = [{"ref": entry["ref"], "name": entry["name"], "code": entry["code"]}
                   for entry in self._waiting.values()]
        try:
            _home.write_json(self.board, {"waiting": waiting, "results": dict(self._results)})
        except OSError:
            pass


def read_members(path: Path, identities: list[str] | None = None) -> tuple[dict[str, dict], set[str]]:
    """Approved machines {machine: {id, name}} and retired identities, as stored."""
    data = _home.read_json(path) or {}
    claims = data.get("claims")
    retired = {item for item in data.get("retired", []) if isinstance(item, str)
               and (identities is None or item in identities)}
    members = {}
    if isinstance(claims, dict):
        for machine, entry in claims.items():
            if (_MACHINE.fullmatch(str(machine)) and isinstance(entry, dict)
                    and isinstance(entry.get("id"), str) and entry["id"] not in retired
                    and (identities is None or entry["id"] in identities)):
                members[machine] = {"id": entry["id"], "name": clean_name(entry.get("name"))}
    return members, retired


def member_names(path: Path) -> dict[str, str]:
    """identity -> machine name, for display."""
    return {entry["id"]: entry["name"] for entry in read_members(path)[0].values()}


def _natural(identity: str) -> tuple:
    return tuple(int(part) if part.isdigit() else part for part in re.split(r"(\d+)", identity))


def serve(*, credentials: Path, admissions: Admissions, token: str, coordinator_port: int,
          host: str, port: int) -> http.server.ThreadingHTTPServer:
    """Start the enrollment service on a daemon thread; returns the running server."""
    if not _TOKEN.fullmatch(token):
        raise ValueError("invalid join token")
    expected = token.encode("ascii")
    slots = threading.BoundedSemaphore(MAX_CONCURRENT)

    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"
        server_version = "dpr"
        sys_version = ""
        timeout = 10

        def setup(self) -> None:
            # Per-read timeouts alone would let a client that trickles bytes hold a
            # slot for ever; the whole exchange gets one deadline.
            self._deadline = threading.Timer(REQUEST_DEADLINE, self._abort)
            self._deadline.daemon = True
            self._deadline.start()
            try:
                self.request.settimeout(self.timeout)
                self.request.do_handshake()
                super().setup()
            except BaseException:
                self._deadline.cancel()
                raise

        def finish(self) -> None:
            try:
                super().finish()
            finally:
                self._deadline.cancel()

        def _abort(self) -> None:
            try:
                self.request.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

        def log_message(self, *args) -> None:
            return

        def _reply(self, status: int, body: dict) -> None:
            payload = json.dumps(body).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(payload)
            self.close_connection = True

        def do_POST(self) -> None:  # noqa: N802 - http.server API
            if self.path != PATH:
                self._reply(404, {"error": "not found"})
                return
            length = self.headers.get("Content-Length", "")
            if not (length.isascii() and length.isdigit()) or not 0 < int(length) <= MAX_BODY:
                self._reply(400, {"error": "bad request"})
                return
            try:
                request = json.loads(self.rfile.read(int(length)))
            except (ValueError, OSError):
                request = None
            if not isinstance(request, dict):
                self._reply(400, {"error": "bad request"})
                return
            presented = request.get("token")
            if (not isinstance(presented, str)
                    or not secrets.compare_digest(presented.encode("utf-8"), expected)):
                time.sleep(FAILURE_DELAY)
                self._reply(403, {"error": "code no longer valid"})
                return
            machine = request.get("machine")
            if not isinstance(machine, str) or not _MACHINE.fullmatch(machine):
                self._reply(400, {"error": "bad request"})
                return
            decision, value = admissions.request(machine, clean_name(request.get("name")))
            if decision == "waiting":
                self._reply(202, {"status": "waiting", "code": value})
                return
            if decision != "approved":
                self._reply(*_REFUSALS[decision])
                return
            identity = value

            def read(file_name: str) -> str:
                return (credentials / file_name).read_text(encoding="utf-8")

            self._reply(200, {
                "port": coordinator_port, "id": identity, "ca": read("ca.pem"),
                "cert": read(f"{identity}.pem"), "key": read(f"{identity}.key"),
                "secret": read(f"{identity}.secret").strip(),
            })

    class Server(http.server.ThreadingHTTPServer):
        daemon_threads = True
        request_queue_size = 16

        def process_request(self, request, client_address) -> None:
            # A fixed number of requests at once; anything beyond is dropped, so a
            # flood of connections costs the host nothing but closed sockets.
            if not slots.acquire(blocking=False):
                self.shutdown_request(request)
                return
            try:
                super().process_request(request, client_address)
            except BaseException:
                slots.release()
                raise

        def process_request_thread(self, request, client_address) -> None:
            try:
                super().process_request_thread(request, client_address)
            finally:
                slots.release()

        def handle_error(self, request, client_address) -> None:
            return

    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_3
    context.load_cert_chain(credentials / "coordinator.pem", credentials / "coordinator.key")
    server = Server((host, port), Handler)
    # Handshakes happen in the per-request thread, so a stalled client cannot block
    # everyone else.
    server.socket = context.wrap_socket(server.socket, server_side=True,
                                        do_handshake_on_connect=False)
    threading.Thread(target=server.serve_forever, name="dpr-enroll", daemon=True).start()
    return server


def fetch(code: dict, *, machine: str, name: str,
          waiting: Callable[[str], None] | None = None, patience: float = PATIENCE) -> dict:
    """Claim a worker identity from the host named in a decoded join code.

    Until the host approves this machine, the host answers "waiting" with a pairing
    number; `waiting` is called once with it, and the request is repeated until
    the host decides or `patience` runs out.
    """
    deadline = time.monotonic() + patience
    announced = None
    while True:
        status, payload = _ask(code, machine=machine, name=name)
        if status == 200:
            return _checked(payload)
        if status != 202:
            raise JoinError(_JOIN_ERRORS.get(status, "join refused"))
        number = payload.get("code") if isinstance(payload, dict) else None
        if not isinstance(number, str) or not re.fullmatch(r"\d{6}", number):
            raise JoinError("join refused")
        if number != announced and waiting is not None:
            waiting(number)
        announced = number
        if time.monotonic() >= deadline:
            raise JoinError("not approved in time")
        time.sleep(POLL_INTERVAL)


def _ask(code: dict, *, machine: str, name: str) -> tuple[int, object]:
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.minimum_version = ssl.TLSVersion.TLSv1_3
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE  # pinned by fingerprint below, before anything is sent
    connection = http.client.HTTPSConnection(code["host"], code["port"], timeout=10,
                                             context=context)
    try:
        try:
            connection.connect()
        except OSError:
            raise JoinError(f"cannot reach {code['host']}") from None
        presented = connection.sock.getpeercert(binary_form=True) or b""
        if not secrets.compare_digest(fingerprint(presented), code["fingerprint"]):
            raise JoinError("code does not match the host")
        body = json.dumps({"token": code["token"], "machine": machine, "name": clean_name(name)})
        try:
            connection.request("POST", PATH, body=body,
                               headers={"Content-Type": "application/json"})
            response = connection.getresponse()
            raw = response.read(MAX_RESPONSE + 1)
            payload = json.loads(raw) if len(raw) <= MAX_RESPONSE else None
        except (OSError, ValueError, http.client.HTTPException):
            raise JoinError(f"cannot reach {code['host']}") from None
    finally:
        connection.close()
    return response.status, payload


def _checked(payload: object) -> dict:
    """Refuse anything that is not exactly the identity bundle a host sends."""
    if not isinstance(payload, dict):
        raise JoinError("join refused")
    port, identity = payload.get("port"), payload.get("id")
    texts = [payload.get(key) for key in ("ca", "cert", "key", "secret")]
    if (type(port) is not int or not 1 <= port <= 65535
            or not isinstance(identity, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", identity)
            or not all(isinstance(text, str) and len(text) < 65536 for text in texts)
            or not texts[0].lstrip().startswith("-----BEGIN CERTIFICATE-----")
            or not texts[1].lstrip().startswith("-----BEGIN CERTIFICATE-----")
            or "PRIVATE KEY-----" not in texts[2]
            or not _SECRET.fullmatch(texts[3])):
        raise JoinError("join refused")
    return payload
