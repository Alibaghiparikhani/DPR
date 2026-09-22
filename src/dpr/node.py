"""The long-running nodes: `python -m dpr.node coordinator|worker ...`.

These are internal.  The session starts them with every path spelled out and stops
them again; nobody is expected to type these commands.  A node started with
`--supervised` lives exactly as long as the pipe on its stdin (see dpr.processes).
"""
from __future__ import annotations

import argparse
import asyncio
from contextlib import suppress
import json
import logging
import logging.handlers
import os
from pathlib import Path
import re
import shutil
import signal
import socket
import sys
import threading
import time

import protocol as p
from coordinator import Coordinator, OperationLimits, SQLiteRunHistoryStore
from dpr import home as _home
from dpr.credentials import SERVER_NAME
from networking import CoordinatorNetworkService, TransportLimits
from program_package import PackageCache, PackageLimits, PackageRepository
from runtime_security import NodeAuthenticator, TlsCredentials, TlsPolicy
from scheduler import WorkerState
from worker import (
    DataPlaneLimits,
    DataStoreLimits,
    IsolatedExecutionLimits,
    LocalDataStore,
    WorkerControlClient,
    WorkerControlConfig,
    WorkerDataPlane,
    WorkerDataPlaneConfig,
    WorkerExecutionRuntime,
    WorkerReconnectPolicy,
)

LOG = logging.getLogger("dpr")

DEFAULT_CHILD_USER = "nobody"
HISTORY_KEEP = 1000
MAX_COMMAND = 4096
REMOVED = 3          # worker exit status: the host no longer admits this machine
_GIB = 1024 ** 3


# --------------------------------------------------------------------------- parsing

def _positive_int(value: str) -> int:
    result = int(value)
    if result < 1:
        raise argparse.ArgumentTypeError("expected a positive integer")
    return result


def _nonnegative_int(value: str) -> int:
    result = int(value)
    if result < 0:
        raise argparse.ArgumentTypeError("expected a non-negative integer")
    return result


def _positive_float(value: str) -> float:
    result = float(value)
    if result <= 0:
        raise argparse.ArgumentTypeError("expected a positive number")
    return result


def _nonnegative_float(value: str) -> float:
    result = float(value)
    if result < 0:
        raise argparse.ArgumentTypeError("expected a non-negative number")
    return result


def _split_host_port(value: str) -> tuple[str, int]:
    if value.count(":") != 1:
        raise argparse.ArgumentTypeError("expected HOST:PORT")
    host, raw_port = value.rsplit(":", 1)
    if not host or not raw_port.isdigit() or not 1 <= int(raw_port) <= 65535:
        raise argparse.ArgumentTypeError("expected HOST:PORT")
    return host, int(raw_port)


def _add_common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--cert", required=True)
    parser.add_argument("--key", required=True)
    parser.add_argument("--ca", required=True)
    parser.add_argument("--log-level", choices=("DEBUG", "INFO", "WARNING", "ERROR"), default="INFO")
    parser.add_argument("--log-file", help="rotating log file (default: stderr)")
    parser.add_argument("--supervised", action="store_true",
                        help="exit when stdin closes (set by the session)")
    parser.add_argument("--inbound-queue", type=_positive_int, default=64)
    parser.add_argument("--outbound-queue", type=_positive_int, default=64)
    parser.add_argument("--read-chunk-bytes", type=_positive_int, default=65536)
    parser.add_argument("--stream-buffer-bytes", type=_positive_int, default=131072)
    parser.add_argument("--handshake-timeout", type=_positive_float, default=5.0)
    parser.add_argument("--write-timeout", type=_positive_float, default=5.0)
    parser.add_argument("--maintenance-interval", type=_positive_float, default=0.1)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m dpr.node", description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="role", required=True)

    coord = sub.add_parser("coordinator")
    coord.add_argument("--bind", action="append", default=None,
                       help="address to listen on; repeatable (default: 127.0.0.1)")
    coord.add_argument("--port", type=_nonnegative_int, default=8740)
    coord.add_argument("--auth-file", required=True)
    coord.add_argument("--client-id", action="append", required=True)
    coord.add_argument("--worker-id", action="append", default=None,
                       help="default: every auth-file identity that is not a client")
    coord.add_argument("--history-db")
    coord.add_argument("--heartbeat-timeout", type=_positive_float, default=30.0)
    coord.add_argument("--coordinator-outbox", type=_positive_int, default=4096)
    coord.add_argument("--max-runs", type=_positive_int, default=2048)
    coord.add_argument("--package-repository-bytes", type=_positive_int, default=256 * 1024 * 1024)
    coord.add_argument("--max-package-bytes", type=_positive_int, default=16 * 1024 * 1024)
    coord.add_argument("--max-submissions", type=_positive_int, default=8)
    coord.add_argument("--max-connections", type=_positive_int, default=1024)
    coord.add_argument("--auth-max-age", type=_positive_int, default=60)
    coord.add_argument("--auth-challenge-limit", type=_positive_int, default=4096)
    coord.add_argument("--enroll-port", type=_nonnegative_int, default=None,
                       help="serve join requests on the first --bind address and this port; "
                            "the join token is the first line on stdin")
    coord.add_argument("--ready-file", help="written once the node is serving")
    coord.add_argument("--board-file",
                       help="with --enroll-port: where machines waiting for approval are listed")
    coord.add_argument("--local-operators", action="store_true",
                       help="accept operator (run/status/cancel) connections only from this machine")
    _add_common(coord)

    worker = sub.add_parser("worker")
    worker.add_argument("--id", required=True)
    worker.add_argument("--coordinator", type=_split_host_port, metavar="HOST:PORT", required=True)
    worker.add_argument("--server-hostname", default=SERVER_NAME)
    worker.add_argument("--secret-file", required=True)
    worker.add_argument("--cache-dir", required=True)
    worker.add_argument("--data-dir", required=True)
    worker.add_argument("--data-port", type=_positive_int, required=True)
    worker.add_argument("--data-host", default=None)
    worker.add_argument("--advertise-host", default=None)
    worker.add_argument("--slots", type=_positive_int, default=max(1, os.cpu_count() or 1))
    worker.add_argument("--environment", action="append", default=None)
    worker.add_argument("--cache-bytes", type=_positive_int, default=256 * 1024 * 1024)
    worker.add_argument("--data-bytes", type=_positive_int, default=None,
                        help="room for task values (default: sized to the free disk)")
    worker.add_argument("--data-items", type=_positive_int, default=65536)
    worker.add_argument("--max-value-bytes", type=_positive_int, default=256 * 1024 * 1024)
    worker.add_argument("--max-contexts", type=_positive_int, default=128)
    worker.add_argument("--child-user", default=None)
    worker.add_argument("--allow-unprivileged-child-execution", action="store_true")
    # Execution bounds default to what this machine can actually sustain.
    worker.add_argument("--task-timeout", type=_positive_float, default=None)
    worker.add_argument("--task-memory-bytes", type=_positive_int, default=None)
    worker.add_argument("--task-processes", type=_positive_int, default=None)
    worker.add_argument("--task-cpu-seconds", type=_positive_int, default=None)
    worker.add_argument("--task-file-bytes", type=_positive_int, default=None)
    worker.add_argument("--stdout-bytes", type=_positive_int, default=64 * 1024)
    worker.add_argument("--stderr-bytes", type=_positive_int, default=64 * 1024)
    worker.add_argument("--result-bytes", type=_positive_int, default=1024 * 1024)
    worker.add_argument("--max-transfer-bytes", type=_positive_int, default=256 * 1024 * 1024)
    worker.add_argument("--transfer-chunk-bytes", type=_positive_int, default=64 * 1024)
    worker.add_argument("--max-incoming-transfers", type=_positive_int, default=4)
    worker.add_argument("--max-outgoing-transfers", type=_positive_int, default=4)
    worker.add_argument("--transfer-connect-timeout", type=_positive_float, default=5.0)
    worker.add_argument("--transfer-idle-timeout", type=_positive_float, default=10.0)
    worker.add_argument("--transfer-total-timeout", type=_positive_float, default=120.0)
    worker.add_argument("--transfer-cleanup-timeout", type=_positive_float, default=2.0)
    worker.add_argument("--heartbeat-interval", type=_positive_float, default=1.0)
    worker.add_argument("--reconnect-attempts", type=_nonnegative_int, default=None)
    worker.add_argument("--reconnect-delay", type=_nonnegative_float, default=.25)
    worker.add_argument("--status-file", help="kept up to date with the connection state")
    _add_common(worker)
    return parser


# ------------------------------------------------------------------------- building

def _read_secret(path: str | Path) -> bytes:
    raw = Path(path).read_text(encoding="ascii").strip()
    try:
        secret = bytes.fromhex(raw)
    except ValueError:
        raise ValueError(f"secret file {path!s} must contain hexadecimal bytes") from None
    if len(secret) < 32:
        raise ValueError(f"secret file {path!s} must contain at least 32 bytes")
    return secret


def _read_auth_map(path: str | Path) -> dict[str, bytes]:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    if type(raw) is not dict or not raw:
        raise ValueError("authentication file must be a nonempty JSON object")
    result: dict[str, bytes] = {}
    for node_id, value in raw.items():
        if type(node_id) is not str or not node_id.strip() or type(value) is not str:
            raise ValueError("authentication map must contain node-id -> hex-secret strings")
        try:
            secret = bytes.fromhex(value)
        except ValueError:
            raise ValueError(f"authentication secret for {node_id!r} is not hexadecimal") from None
        if len(secret) < 32:
            raise ValueError(f"authentication secret for {node_id!r} is shorter than 32 bytes")
        result[node_id] = secret
    return result


def _tls(args) -> TlsPolicy:
    return TlsPolicy(TlsCredentials(args.cert, args.key, args.ca))


def _transport_limits(args) -> TransportLimits:
    return TransportLimits(
        inbound_queue_messages=args.inbound_queue,
        outbound_queue_messages=args.outbound_queue,
        read_chunk_size=args.read_chunk_bytes,
        stream_buffer_limit=args.stream_buffer_bytes,
        max_connections=getattr(args, "max_connections", 1024),
        handshake_timeout=args.handshake_timeout,
        write_timeout=args.write_timeout,
        maintenance_interval=args.maintenance_interval,
    )


def _roles(args, auth: dict[str, bytes]) -> tuple[frozenset[str], frozenset[str]]:
    """Operator identities, and every identity that could ever be a worker."""
    client_ids = frozenset(args.client_id)
    worker_ids = (frozenset(args.worker_id) if args.worker_id
                  else frozenset(auth).difference(client_ids))
    if not worker_ids:
        raise ValueError("no worker identities in the authentication file")
    if client_ids & worker_ids:
        raise ValueError("client and worker identities must be disjoint")
    unknown = (client_ids | worker_ids).difference(auth)
    if unknown:
        raise ValueError(f"identities missing from the authentication file: {sorted(unknown)}")
    return client_ids, worker_ids


async def start_coordinator_service(args, members: frozenset[str] | None = None
                                    ) -> CoordinatorNetworkService:
    """`members`, when given, is the subset of worker identities admitted right now
    (the host's approved machines); otherwise every worker identity is."""
    auth = _read_auth_map(args.auth_file)
    client_ids, worker_ids = _roles(args, auth)
    if members is not None:
        worker_ids = worker_ids & members
    history = None
    if args.history_db:
        history = SQLiteRunHistoryStore(args.history_db)
        history.prune(HISTORY_KEEP)
    coordinator = Coordinator(
        heartbeat_timeout=args.heartbeat_timeout,
        operation_limits=OperationLimits(max_runs_in_memory=args.max_runs),
        outbox_limit=args.coordinator_outbox,
        history_store=history,
        clock=time.monotonic,
    )
    binds = list(dict.fromkeys(args.bind or ["127.0.0.1"]))
    service = CoordinatorNetworkService(
        coordinator,
        tls_policy=_tls(args),
        authenticator=NodeAuthenticator(
            auth, max_age_seconds=args.auth_max_age, replay_limit=args.auth_challenge_limit),
        host=binds[0] if len(binds) == 1 else binds,
        port=args.port,
        limits=_transport_limits(args),
        package_repository=PackageRepository(max_bytes=args.package_repository_bytes),
        package_limits=PackageLimits(max_archive_bytes=args.max_package_bytes),
        max_active_submissions=args.max_submissions,
        client_node_ids=client_ids,
        worker_node_ids=worker_ids,
        operators_on_loopback_only=args.local_operators,
    )
    await service.start()
    LOG.info("coordinator listening on %s port %d", ", ".join(binds), service.listening_port)
    return service


def _detect_advertise_host(coordinator_host: str, coordinator_port: int) -> str:
    """The local address peers can reach this worker on: the one that routes to the
    coordinator.  Loopback would silently break worker-to-worker transfer."""
    probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        probe.connect((coordinator_host, coordinator_port))
        return probe.getsockname()[0]
    except OSError:
        return "127.0.0.1"
    finally:
        probe.close()


def resolve_child_identity(value: str | None, *, allow_unprivileged: bool = False
                           ) -> tuple[int | None, int | None]:
    """The dedicated account submitted code runs as on POSIX (none on Windows)."""
    if os.name != "posix":
        if value not in (None, "", DEFAULT_CHILD_USER):
            raise RuntimeError("--child-user is available only on POSIX")
        return None, None
    if value is None:
        value = DEFAULT_CHILD_USER
    if allow_unprivileged and os.geteuid() != 0:
        return None, None
    if not value:
        raise RuntimeError("a dedicated --child-user is required on POSIX workers")
    import pwd
    try:
        record = pwd.getpwnam(value) if not value.isdigit() else pwd.getpwuid(int(value))
    except (KeyError, ValueError) as error:
        raise RuntimeError(f"unknown child user: {value}") from error
    if record.pw_uid == os.geteuid():
        raise RuntimeError(f"--child-user {value} must be a different UID from the worker")
    if os.geteuid() != 0:
        raise RuntimeError("running submitted code as --child-user requires starting the "
                           "worker as root, or --allow-unprivileged-child-execution")
    return record.pw_uid, record.pw_gid


def _physical_memory() -> int | None:
    try:
        return os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    except (AttributeError, ValueError, OSError):
        return None


def machine_limits(args, *, shared_account: bool) -> dict:
    """Execution bounds sized to this machine rather than fixed guesses.

    Each bound still exists -- a runaway task cannot take the machine down -- but a
    real workload is not cut off at a few minutes or a gigabyte.
    """
    cpus = max(1, os.cpu_count() or 1)
    memory = _physical_memory()
    try:
        free_disk = shutil.disk_usage(Path(args.data_dir).parent).free
    except OSError:
        free_disk = 0
    day = 24 * 3600
    return {
        "task_timeout_seconds": args.task_timeout or float(day),
        "rlimit_as_bytes": args.task_memory_bytes or (max(2 * _GIB, memory) if memory else None),
        # RLIMIT_NPROC counts every process of the account.  When tasks share the
        # user's own account, any fixed number would count their desktop too.
        "rlimit_nproc": args.task_processes or (None if shared_account else 4096),
        "rlimit_cpu_seconds": args.task_cpu_seconds or day * cpus,
        "rlimit_fsize_bytes": args.task_file_bytes or max(_GIB, free_disk // 2),
    }


def data_budget(data_dir: str | Path) -> int:
    """Room for task values: a quarter of the free disk, from 256 MiB up to 64 GiB.

    Values live in files, so disk rather than memory is what bounds them; a fixed
    small budget failed ordinary runs that simply produced a lot of data.
    """
    try:
        free = shutil.disk_usage(Path(data_dir).expanduser().resolve().parent).free
    except OSError:
        free = 0
    return min(64 * _GIB, max(256 * 1024 * 1024, free // 4))


def _owned(path: str | Path) -> bool:
    """True for directories dpr created itself, which may be rebuilt when damaged."""
    try:
        Path(path).expanduser().resolve().relative_to(_home.Home.default().path.resolve())
        return True
    except (ValueError, OSError):
        return False


async def start_worker_client(args):
    """Build and start a worker; returns (client, control task, data plane)."""
    coordinator_host, coordinator_port = args.coordinator
    advertise_host = args.advertise_host or _detect_advertise_host(coordinator_host, coordinator_port)
    data_host = args.data_host or advertise_host
    child_uid, child_gid = resolve_child_identity(
        args.child_user, allow_unprivileged=args.allow_unprivileged_child_execution)
    for credential in (args.secret_file, args.key):
        if not Path(credential).is_file():
            raise RuntimeError(f"worker credential is not a regular file: {credential}")
        os.chmod(credential, 0o600)
    base_state = WorkerState(
        args.id, args.slots, cpu_cores=max(1, os.cpu_count() or 1),
        environment_ids=frozenset(args.environment or ["default"]),
    )
    cache = PackageCache(args.cache_dir, limits=PackageLimits(max_cache_bytes=args.cache_bytes),
                         reset_if_unusable=_owned(args.cache_dir))
    data_bytes = args.data_bytes or data_budget(args.data_dir)
    data_store = LocalDataStore(
        args.data_dir,
        reset_if_unusable=_owned(args.data_dir),
        limits=DataStoreLimits(max_bytes=data_bytes, max_items=args.data_items,
                               max_value_bytes=min(args.max_value_bytes, data_bytes)),
    )
    data_plane = WorkerDataPlane(
        WorkerDataPlaneConfig(
            args.id, data_host, args.data_port, _tls(args),
            DataPlaneLimits(
                max_transfer_bytes=args.max_transfer_bytes,
                chunk_bytes=args.transfer_chunk_bytes,
                max_incoming=args.max_incoming_transfers,
                max_outgoing=args.max_outgoing_transfers,
                connect_timeout=args.transfer_connect_timeout,
                handshake_timeout=args.handshake_timeout,
                idle_timeout=args.transfer_idle_timeout,
                total_timeout=args.transfer_total_timeout,
                cleanup_timeout=args.transfer_cleanup_timeout,
            ),
        ),
        data_store,
    )
    runtime = WorkerExecutionRuntime(
        args.id, base_state, cache,
        limits=IsolatedExecutionLimits(
            stdout_bytes=args.stdout_bytes,
            stderr_bytes=args.stderr_bytes,
            result_bytes=args.result_bytes,
            local_value_bytes=data_bytes,
            local_value_items=args.data_items,
            max_contexts=args.max_contexts,
            **machine_limits(args, shared_account=child_uid is None),
        ),
        data_store=data_store,
        data_plane=data_plane,
        child_uid=child_uid,
        child_gid=child_gid,
    )
    config = WorkerControlConfig(
        node_id=args.id,
        secret=_read_secret(args.secret_file),
        coordinator_host=coordinator_host,
        coordinator_port=coordinator_port,
        server_hostname=args.server_hostname,
        endpoint=p.WorkerEndpoint(args.id, advertise_host, args.data_port),
        initial_state=base_state,
        tls_policy=_tls(args),
        heartbeat_interval=args.heartbeat_interval,
        reconnect=WorkerReconnectPolicy(args.reconnect_attempts, args.reconnect_delay,
                                        max_delay_seconds=5.0),
        limits=_transport_limits(args),
    )
    client = WorkerControlClient(config, runtime=runtime)
    task = asyncio.create_task(client.run(), name=f"worker-{args.id}-control")
    LOG.info("worker %s connecting to %s:%d; data plane %s:%d",
             args.id, coordinator_host, coordinator_port, data_host, args.data_port)
    return client, task, data_plane


# -------------------------------------------------------------------------- running

def _configure_logging(args) -> None:
    if args.log_file:
        Path(args.log_file).parent.mkdir(parents=True, exist_ok=True)
        handler: logging.Handler = logging.handlers.RotatingFileHandler(
            args.log_file, maxBytes=1_000_000, backupCount=1, encoding="utf-8", delay=True)
    else:
        handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(getattr(logging, args.log_level))


def _stop_event(supervised: bool, on_line=None) -> asyncio.Event:
    """Set on SIGINT/SIGTERM, and when a supervised node's stdin closes.

    Lines arriving on a supervised node's stdin are handed to `on_line` on the event
    loop: the session's private channel for decisions.  Only the session holds the
    other end of that pipe.
    """
    event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for name in ("SIGINT", "SIGTERM"):
        sig = getattr(signal, name, None)
        if sig is not None:
            with suppress(NotImplementedError, RuntimeError, ValueError):
                loop.add_signal_handler(sig, event.set)
    if supervised:
        def watch() -> None:
            with suppress(Exception):
                for line in STDIN.lines():
                    if on_line is not None:
                        loop.call_soon_threadsafe(on_line, line)
            loop.call_soon_threadsafe(event.set)
            # Backstop: a clean shutdown takes seconds, never this long.
            timer = threading.Timer(20.0, os._exit, args=(0,))
            timer.daemon = True
            timer.start()
        threading.Thread(target=watch, name="dpr-supervisor", daemon=True).start()
    return event


class _StdinLines:
    """Lines from the session's pipe, read straight from the file descriptor.

    A thread parked in the buffered sys.stdin holds its lock, and interpreter exit
    then aborts the process; a raw read holds nothing.  Over-long lines are dropped.
    """

    def __init__(self) -> None:
        self._pending = b""
        self._lock = threading.Lock()

    def readline(self) -> bytes | None:
        """The next line without its newline, or None at end of input."""
        with self._lock:
            while True:
                end = self._pending.find(b"\n")
                if end >= 0:
                    line, self._pending = self._pending[:end], self._pending[end + 1:]
                    if len(line) <= MAX_COMMAND:
                        return line
                    continue
                if len(self._pending) > MAX_COMMAND:
                    self._pending = b""  # an endless line: discard it
                chunk = os.read(0, 65536)
                if not chunk:
                    return None
                self._pending += chunk

    def lines(self):
        while (line := self.readline()) is not None:
            yield line


STDIN = _StdinLines()
_COMMAND = re.compile(r"[0-9a-f]{16}")


def _decide(line: bytes, admissions, service: CoordinatorNetworkService) -> None:
    """Apply one decision from the session: approve or deny a waiting machine, or
    remove a member.  Anything malformed is ignored."""
    try:
        command = json.loads(line)
    except ValueError:
        return
    if not isinstance(command, dict) or len(command) != 2 or not _COMMAND.fullmatch(
            str(command.get("id", ""))):
        return
    action = next((key for key in ("approve", "deny", "kick") if key in command), None)
    value = command.get(action) if action else None
    if not isinstance(value, str):
        return
    if action in ("approve", "deny") and not _COMMAND.fullmatch(value):
        return
    if action == "kick" and value not in admissions.identities:
        return
    result = getattr(admissions, action)(value)
    # Approve: the identity is admitted before this callback returns, and a worker's
    # hello is handled on this same loop, so it can never arrive in between.  Kick:
    # the claim is already gone, so the member is disconnected now.
    service.set_worker_identities(admissions.members())
    admissions.record(command["id"], result)
    LOG.info("%s %s: %s", action, value, result)


async def _tick(admissions) -> None:
    while True:
        await asyncio.sleep(1.0)
        admissions.tick()


async def _coordinator_main(args) -> int:
    admissions = None
    if args.enroll_port is not None:
        # Hosting for people: only machines the host approved may work.
        from dpr import enroll as _enroll
        _clients, workers = _roles(args, _read_auth_map(args.auth_file))
        admissions = _enroll.Admissions(
            Path(args.cert).parent / "claims.json", sorted(workers, key=_natural),
            board=Path(args.board_file) if args.board_file else None)
    service = await start_coordinator_service(
        args, members=None if admissions is None else admissions.members())
    stop = _stop_event(args.supervised, None if admissions is None
                       else lambda line: _decide(line, admissions, service))
    enroll = ticker = None
    try:
        ready = {"port": service.listening_port}
        if admissions is not None:
            enroll = _enroll.serve(
                credentials=Path(args.cert).parent, admissions=admissions,
                token=args.join_token, coordinator_port=service.listening_port,
                host=(args.bind or ["127.0.0.1"])[0], port=args.enroll_port)
            ticker = asyncio.create_task(_tick(admissions))
            ready["enroll_port"] = enroll.server_address[1]
        if args.ready_file:
            _home.write_json(Path(args.ready_file), {**ready, "pid": os.getpid()})
        await stop.wait()
    finally:
        LOG.info("coordinator shutting down")
        if ticker is not None:
            ticker.cancel()
        if enroll is not None:
            enroll.shutdown()
        if args.board_file:
            Path(args.board_file).unlink(missing_ok=True)
        await service.stop()
        if args.ready_file:
            Path(args.ready_file).unlink(missing_ok=True)
    return 0


def _natural(identity: str) -> tuple:
    return tuple(int(part) if part.isdigit() else part for part in re.split(r"(\d+)", identity))


async def _publish_state(client: WorkerControlClient, path: Path) -> None:
    last = None
    while True:
        state = "connected" if client.active.is_set() else "connecting"
        if state != last:
            with suppress(OSError):
                _home.write_json(path, {"state": state, "pid": os.getpid()})
                last = state
        await asyncio.sleep(0.2)


async def _worker_main(args) -> int:
    stop = _stop_event(args.supervised)
    client, task, data_plane = await start_worker_client(args)
    publisher = None
    if args.status_file:
        publisher = asyncio.create_task(_publish_state(client, Path(args.status_file)))
    stopper = asyncio.create_task(stop.wait())
    try:
        await asyncio.wait({task, stopper}, return_when=asyncio.FIRST_COMPLETED)
        await client.stop()
        await task
        if client.not_admitted is not None:
            return REMOVED
        if client.last_error is not None and not stop.is_set():
            LOG.error("worker control loop ended: %s", client.last_error)
            return 2
        return 0
    finally:
        stopper.cancel()
        if publisher is not None:
            publisher.cancel()
        await client.stop()
        if not task.done():
            await task
        await data_plane.close()
        if args.status_file:
            Path(args.status_file).unlink(missing_ok=True)


def _raise_descriptor_limit() -> None:
    """Headroom for many peers and task pipes; the default soft limit is often 1024."""
    try:
        import resource
    except ImportError:
        return
    try:
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        target = 65536 if hard == resource.RLIM_INFINITY else min(hard, 65536)
        if target > soft:
            resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))
    except (OSError, ValueError):
        pass


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    _configure_logging(args)
    _raise_descriptor_limit()
    if getattr(args, "enroll_port", None) is not None:
        # The join token never appears in argv or the environment, where other
        # processes could read it; the session hands it over on stdin.
        args.join_token = (STDIN.readline() or b"").strip().decode("ascii", "replace")
    try:
        return asyncio.run(_coordinator_main(args) if args.role == "coordinator"
                           else _worker_main(args))
    except KeyboardInterrupt:
        return 130
    except Exception as error:
        LOG.error("%s", error)
        LOG.debug("node failed", exc_info=True)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
