"""Worker package preparation plus isolated and persistent-context execution."""
from __future__ import annotations

import asyncio
import errno
from contextlib import suppress
from dataclasses import dataclass, field, replace
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import signal
import sys
import tempfile
import time
from typing import Awaitable, Callable

from dag_runtime.dag_engine import analyze_source
from execution import (
    ExecutionMode, FailureInfo, FailureKind, ObjectAccess, ProgramIdentity, TaskFailure, TaskSuccess,
    ValueKind, lower_dag,
)
import protocol as p
from program_package import (
    PackageCache, PackageError, PackageIntegrityError, PackageResourceError,
)
from scheduler import DataForm, WorkerContext, WorkerState
from networking.common import new_transport_id
from .data_store import (
    DataStoreError, DataStoreFull, DataStoreIntegrityError, DataStoreLimits,
    LocalDataStore,
)
from .data_plane import (
    DataPlaneAuthorizationError, DataPlaneError, DataPlaneResourceError,
    WorkerDataPlane,
)

SendMessage = Callable[[p.Message], Awaitable[None]]
AbortSession = Callable[[BaseException], None]



def _unique_json_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _validate_inert_value(node: object, *, depth: int = 0) -> None:
    """Validate child-produced value metadata without reconstructing Python objects."""
    if depth > 32 or type(node) is not dict or set(node) != {"t", "v"}:
        raise ValueError("invalid isolated value encoding")
    kind = node["t"]
    value = node["v"]
    if kind == "none":
        if value is not None: raise ValueError("invalid none encoding")
        return
    if kind == "bool":
        if type(value) is not bool: raise ValueError("invalid bool encoding")
        return
    if kind == "int":
        if (type(value) is not str or len(value.lstrip("-")) > 4096
                or not value.lstrip("-").isdigit()):
            raise ValueError("invalid int encoding")
        return
    if kind == "float":
        if type(value) is not str or len(value) > 128:
            raise ValueError("invalid float encoding")
        if value not in {"nan", "inf", "-inf"}:
            float.fromhex(value)
        return
    if kind == "str":
        if type(value) is not str: raise ValueError("invalid str encoding")
        return
    if kind == "bytes":
        if (type(value) is not str or len(value) % 2
                or any(char not in "0123456789abcdef" for char in value)):
            raise ValueError("invalid bytes encoding")
        return
    if kind in {"tuple", "frozenset"}:
        if type(value) is not list: raise ValueError("invalid sequence encoding")
        for item in value:
            _validate_inert_value(item, depth=depth + 1)
        return
    raise ValueError("unsupported isolated value encoding")

@dataclass(frozen=True, slots=True)
class IsolatedExecutionLimits:
    stdout_bytes: int = 64 * 1024
    stderr_bytes: int = 64 * 1024
    result_bytes: int = 1024 * 1024
    request_bytes: int = 1024 * 1024
    local_value_bytes: int = 64 * 1024 * 1024
    local_value_items: int = 65536
    terminal_attempt_history: int = 4096
    max_prepared_programs: int = 4096
    cancel_grace_seconds: float = 0.5
    kill_wait_seconds: float = 1.0
    shutdown_wait_seconds: float = 2.0
    preparation_timeout_seconds: float = 30.0
    # F51: executions are bounded by default. ``None`` remains accepted for
    # embedders/tests that deliberately opt out, but the production CLI always
    # supplies these defaults.
    task_timeout_seconds: float | None = 300.0
    rlimit_as_bytes: int | None = 1024 * 1024 * 1024
    rlimit_nproc: int | None = 64
    rlimit_cpu_seconds: int | None = 300
    rlimit_fsize_bytes: int | None = 128 * 1024 * 1024
    retained_diagnostics: int = 256
    max_contexts: int = 128
    context_result_poll_seconds: float = 0.01

    def __post_init__(self) -> None:
        for name in ("stdout_bytes", "stderr_bytes", "result_bytes", "request_bytes", "local_value_bytes", "local_value_items", "terminal_attempt_history", "max_prepared_programs", "retained_diagnostics", "max_contexts"):
            value = getattr(self, name)
            if type(value) is not int or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        for name in ("cancel_grace_seconds", "kill_wait_seconds", "shutdown_wait_seconds", "preparation_timeout_seconds"):
            value = getattr(self, name)
            if type(value) not in (int, float) or value <= 0:
                raise ValueError(f"{name} must be positive")
        if self.task_timeout_seconds is not None and (type(self.task_timeout_seconds) not in (int, float) or self.task_timeout_seconds <= 0):
            raise ValueError("task_timeout_seconds must be positive or None")
        for name in ("rlimit_as_bytes", "rlimit_nproc", "rlimit_cpu_seconds", "rlimit_fsize_bytes"):
            value = getattr(self, name)
            if value is not None and (type(value) is not int or value < 1):
                raise ValueError(f"{name} must be a positive integer or None")
        if type(self.context_result_poll_seconds) not in (int, float) or self.context_result_poll_seconds <= 0:
            raise ValueError("context_result_poll_seconds must be positive")


@dataclass(frozen=True, slots=True)
class ExecutionDiagnostics:
    attempt_id: str
    stdout: str
    stderr: str
    stdout_truncated: bool
    stderr_truncated: bool
    exit_code: int | None
    outcome: str


@dataclass(slots=True)
class _Preparation:
    command: p.PrepareProgram
    timeout_task: asyncio.Task[None] | None = None
    path: Path | None = None
    handle: object | None = None
    hasher: object | None = None
    expected_size: int | None = None
    expected_hash: str | None = None
    received: int = 0
    wait_for: str | None = None


@dataclass(slots=True)
class _ActiveExecution:
    dispatch: p.TaskDispatch
    session_id: str
    plan: object
    process: asyncio.subprocess.Process | None = None
    task: asyncio.Task[None] | None = None
    cancel_message: p.CancelTask | None = None
    terminal: bool = False
    started_at: float = field(default_factory=time.monotonic)


@dataclass(slots=True)
class _WorkerContextRuntime:
    command: p.PrepareContext
    session_id: str
    plan: object
    process: asyncio.subprocess.Process
    # F53: POSIX contexts use a dedicated inherited control pipe, never fd 0.
    # Windows' asyncio subprocess API has no pass_fds equivalent, so it captures
    # the bootstrap stdin pipe in the child and immediately replaces user-facing
    # sys.stdin with DEVNULL; Phase 6 can replace that compatibility path with a
    # native named-pipe/handle implementation without changing this protocol.
    control_fd: int | None
    control_stream: asyncio.StreamWriter | None
    stdout_task: asyncio.Task[tuple[bytes, bool]] | None
    stderr_task: asyncio.Task[tuple[bytes, bool]] | None
    # F50: task requests must point at the same child-readable package view used
    # to launch the persistent context, never back at the worker-owned cache.
    package_root: Path
    sandbox_root: Path | None = None
    active_attempt_id: str | None = None
    completed_task_ids: set[str] = field(default_factory=set)
    retired: bool = False
    created_at: float = field(default_factory=time.monotonic)


@dataclass(slots=True)
class _ActiveContextExecution:
    dispatch: p.TaskDispatch
    session_id: str
    plan: object
    context_id: str
    task: asyncio.Task[None] | None = None
    cancel_message: p.CancelTask | None = None
    terminal: bool = False
    started_at: float = field(default_factory=time.monotonic)


class WorkerExecutionRuntime:
    """Own package/cache/process state for exactly one worker control client."""

    def __init__(
        self,
        worker_id: str,
        base_state: WorkerState,
        cache: PackageCache,
        *,
        limits: IsolatedExecutionLimits | None = None,
        data_store: LocalDataStore | None = None,
        data_plane: WorkerDataPlane | None = None,
        clock: Callable[[], float] = time.monotonic,
        child_uid: int | None = None,
        child_gid: int | None = None,
    ) -> None:
        if base_state.worker_id != worker_id:
            raise ValueError("base worker state must match worker_id")
        self.worker_id = worker_id
        self.base_state = base_state
        self.cache = cache
        self.limits = limits or IsolatedExecutionLimits()
        self.data_store = data_store or LocalDataStore(
            cache.root.parent / "runtime-data",
            limits=DataStoreLimits(
                max_bytes=self.limits.local_value_bytes,
                max_items=self.limits.local_value_items,
                max_value_bytes=min(self.limits.local_value_bytes, 64 * 1024 * 1024),
            ),
        )
        if data_plane is not None and data_plane.store is not self.data_store:
            raise ValueError("data_plane must use the runtime's LocalDataStore")
        self.data_plane = data_plane
        self._clock = clock
        # F50: a production worker may execute submitted code under a dedicated
        # unprivileged OS identity. Direct embedders can omit this for tests, but
        # the CLI resolves a separate account and refuses same-UID execution.
        if (child_uid is None) != (child_gid is None):
            raise ValueError("child_uid and child_gid must be supplied together")
        for name, value in (("child_uid", child_uid), ("child_gid", child_gid)):
            if value is not None and (type(value) is not int or value < 0):
                raise ValueError(f"{name} must be a non-negative integer or None")
        if child_uid is not None and os.name != "posix":
            raise ValueError("separate child UID execution is supported only on POSIX")
        if child_uid is not None and child_uid == os.geteuid():
            raise ValueError("child_uid must differ from the worker effective UID")
        self._child_uid = child_uid
        self._child_gid = child_gid
        self._session_id: str | None = None
        self._send: SendMessage | None = None
        self._abort_session: AbortSession | None = None
        self._plans: dict[tuple[str, str], object] = {}
        self._programs: dict[str, ProgramIdentity] = {}
        self._preparations: dict[str, _Preparation] = {}
        self._package_owner: dict[str, str] = {}
        self._active: dict[str, _ActiveExecution] = {}
        self._contexts: dict[tuple[str, str, str], _WorkerContextRuntime] = {}
        self._active_context: dict[str, _ActiveContextExecution] = {}
        # F35: cancellation can spend grace+kill time waiting on an uncooperative
        # child. Keep that physical cleanup off the control reader while bounding
        # concurrent kill/reap work so a cancel storm cannot spawn without limit.
        self._cancel_tasks: dict[str, tuple[str, asyncio.Task[None]]] = {}
        self._cancel_semaphore = asyncio.Semaphore(min(4, max(1, base_state.total_slots)))
        self._receive_commands: dict[tuple[str, str], p.PrepareReceive] = {}
        self._send_commands: dict[tuple[str, str], p.TransferRequest] = {}
        self._terminal_attempts: set[str] = set()
        self._terminal_order: list[str] = []
        self._prepared_program_ids: set[str] = set()
        self._diagnostics: dict[str, ExecutionDiagnostics] = {}
        self._windows_jobs: dict[int, int] = {}
        self._raise_macos_nofile_limit()

    @staticmethod
    def _raise_macos_nofile_limit() -> None:
        """Best-effort headroom for per-child pipes on macOS workers."""
        if sys.platform != "darwin":
            return
        try:
            import resource
            soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
            target = max(soft, 4096)
            if hard != resource.RLIM_INFINITY:
                target = min(target, hard)
            if target > soft:
                resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))
        except (OSError, ValueError):
            # Worker startup remains usable when the host policy forbids raising
            # the descriptor limit; documentation/CI still exercise the path.
            pass

    def _child_environment(self) -> dict[str, str]:
        """Return the deliberately tiny environment exposed to submitted code."""
        env = {
            "PATH": os.defpath,
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONNOUSERSITE": "1",
        }
        # Locale/timezone affect ordinary Python semantics but carry no credential
        # paths. Everything else (HOME, *_TOKEN, *_KEY, cloud credentials, etc.)
        # is intentionally absent.
        for name in ("LANG", "LC_ALL", "LC_CTYPE", "TZ"):
            value = os.environ.get(name)
            if value and len(value) <= 4096:
                env[name] = value
        if os.name == "nt":
            # Windows needs its system folder for sockets, crypto and subprocesses;
            # these name only the OS installation, never anything of the user's.
            root = os.environ.get("SYSTEMROOT") or os.environ.get("WINDIR")
            if root and len(root) <= 260:
                env["SYSTEMROOT"] = env["WINDIR"] = root
                env["PATH"] = os.pathsep.join((os.path.join(root, "System32"), root))
        return env

    def _resource_preexec(self):
        if os.name != "posix":
            return None
        # Imported here, in the parent: the function below runs between fork and
        # exec, where taking the import lock can deadlock a threaded worker.
        import resource
        limits = self.limits
        def apply_limits() -> None:
            mapping = (
                (resource.RLIMIT_AS, limits.rlimit_as_bytes),
                (resource.RLIMIT_NPROC, limits.rlimit_nproc),
                (resource.RLIMIT_CPU, limits.rlimit_cpu_seconds),
                (resource.RLIMIT_FSIZE, limits.rlimit_fsize_bytes),
            )
            for resource_id, value in mapping:
                if value is None:
                    continue
                soft, hard = resource.getrlimit(resource_id)
                target = int(value)
                if hard != resource.RLIM_INFINITY:
                    target = min(target, hard)
                resource.setrlimit(resource_id, (target, target))
        return apply_limits

    def _child_spawn_kwargs(self) -> dict[str, object]:
        kwargs: dict[str, object] = {"env": self._child_environment()}
        if os.name == "posix":
            kwargs["start_new_session"] = True
            preexec = self._resource_preexec()
            if preexec is not None:
                kwargs["preexec_fn"] = preexec
            if self._child_uid is not None:
                # subprocess applies group/extra_groups/user after preexec_fn. The
                # worker therefore needs root/CAP_SETUID at launch, while the child
                # receives no supplementary groups from the privileged worker.
                kwargs["user"] = self._child_uid
                kwargs["group"] = self._child_gid
                kwargs["extra_groups"] = ()
                kwargs["umask"] = 0o077
        elif os.name == "nt":
            kwargs["creationflags"] = getattr(__import__("subprocess"), "CREATE_NEW_PROCESS_GROUP", 0)
        return kwargs

    def _make_child_workdir(self, prefix: str) -> Path:
        # A separate-UID child cannot traverse the worker-owned cache/data roots
        # once F55 tightens them to 0700. Secure children therefore get a small
        # execution view under the system temp directory, itself 0700 and owned by
        # the child identity. Non-isolated embedders retain the historical staging
        # location.
        base = None if self._child_uid is not None else str(self.cache.staging)
        path = Path(tempfile.mkdtemp(prefix=prefix, dir=base))
        os.chmod(path, 0o700)
        if self._child_uid is not None:
            os.chown(path, self._child_uid, self._child_gid)
        return path

    def _copy_package_view(self, package_root: Path, sandbox_root: Path) -> Path:
        if self._child_uid is None:
            return package_root
        view = sandbox_root / "package"
        __import__("shutil").copytree(package_root, view, symlinks=False)
        # Package source is immutable for the child. Keep it child-owned so a 0700
        # worker cache root is never exposed merely to make imports work.
        for path in [view, *view.rglob("*")]:
            if path.is_symlink():
                raise ValueError("execution package view unexpectedly contains a symlink")
            os.chown(path, self._child_uid, self._child_gid)
            os.chmod(path, 0o500 if path.is_dir() else 0o400)
        return view

    def _child_entry_descriptor(self, entry, workdir: Path, ordinal: int) -> dict[str, object]:
        descriptor = self._entry_descriptor(entry)
        if self._child_uid is None:
            return descriptor
        target = workdir / f"input-{ordinal}.bin"
        with entry.path.open("rb") as src:
            fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            try:
                with os.fdopen(fd, "wb", closefd=False) as dst:
                    while True:
                        chunk = src.read(256 * 1024)
                        if not chunk:
                            break
                        dst.write(chunk)
                    dst.flush()
                    os.fsync(dst.fileno())
            finally:
                os.close(fd)
        os.chown(target, self._child_uid, self._child_gid)
        descriptor = dict(descriptor)
        descriptor["path"] = str(target)
        return descriptor


    def _publish_child_request(self, path: Path, payload: bytes) -> None:
        # Parent writes first, then hands the immutable request to the child UID.
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            with os.fdopen(fd, "wb", closefd=False) as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
        finally:
            os.close(fd)
        if self._child_uid is not None:
            os.chown(path, self._child_uid, self._child_gid)

    def _attach_windows_job(self, pid: int) -> None:
        if os.name != "nt":
            return
        import ctypes
        from ctypes import wintypes

        class IO_COUNTERS(ctypes.Structure):
            _fields_ = [(name, ctypes.c_ulonglong) for name in (
                "ReadOperationCount", "WriteOperationCount", "OtherOperationCount",
                "ReadTransferCount", "WriteTransferCount", "OtherTransferCount",
            )]

        class BASIC_LIMITS(ctypes.Structure):
            _fields_ = [
                ("PerProcessUserTimeLimit", ctypes.c_longlong),
                ("PerJobUserTimeLimit", ctypes.c_longlong),
                ("LimitFlags", wintypes.DWORD),
                ("MinimumWorkingSetSize", ctypes.c_size_t),
                ("MaximumWorkingSetSize", ctypes.c_size_t),
                ("ActiveProcessLimit", wintypes.DWORD),
                ("Affinity", ctypes.c_size_t),
                ("PriorityClass", wintypes.DWORD),
                ("SchedulingClass", wintypes.DWORD),
            ]

        class EXTENDED_LIMITS(ctypes.Structure):
            _fields_ = [
                ("BasicLimitInformation", BASIC_LIMITS),
                ("IoInfo", IO_COUNTERS),
                ("ProcessMemoryLimit", ctypes.c_size_t),
                ("JobMemoryLimit", ctypes.c_size_t),
                ("PeakProcessMemoryUsed", ctypes.c_size_t),
                ("PeakJobMemoryUsed", ctypes.c_size_t),
            ]

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        job = kernel32.CreateJobObjectW(None, None)
        if not job:
            raise OSError(ctypes.get_last_error(), "CreateJobObjectW failed")
        info = EXTENDED_LIMITS()
        flags = 0x00002000  # JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        if self.limits.rlimit_nproc is not None:
            flags |= 0x00000008  # JOB_OBJECT_LIMIT_ACTIVE_PROCESS
            info.BasicLimitInformation.ActiveProcessLimit = self.limits.rlimit_nproc
        if self.limits.rlimit_as_bytes is not None:
            flags |= 0x00000100  # JOB_OBJECT_LIMIT_PROCESS_MEMORY
            info.ProcessMemoryLimit = self.limits.rlimit_as_bytes
        if self.limits.rlimit_cpu_seconds is not None:
            flags |= 0x00000002  # JOB_OBJECT_LIMIT_PROCESS_TIME
            info.BasicLimitInformation.PerProcessUserTimeLimit = self.limits.rlimit_cpu_seconds * 10_000_000
        info.BasicLimitInformation.LimitFlags = flags
        if not kernel32.SetInformationJobObject(job, 9, ctypes.byref(info), ctypes.sizeof(info)):
            error = ctypes.get_last_error()
            kernel32.CloseHandle(job)
            raise OSError(error, "SetInformationJobObject failed")
        process_handle = kernel32.OpenProcess(0x0101, False, pid)  # TERMINATE | SET_QUOTA
        if not process_handle:
            error = ctypes.get_last_error()
            kernel32.CloseHandle(job)
            raise OSError(error, "OpenProcess for job assignment failed")
        try:
            if not kernel32.AssignProcessToJobObject(job, process_handle):
                error = ctypes.get_last_error()
                kernel32.CloseHandle(job)
                raise OSError(error, "AssignProcessToJobObject failed")
        finally:
            kernel32.CloseHandle(process_handle)
        self._windows_jobs[pid] = int(job)

    def _close_windows_job(self, pid: int, *, terminate: bool = False) -> None:
        if os.name != "nt":
            return
        handle = self._windows_jobs.pop(pid, None)
        if handle is None:
            return
        import ctypes
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        if terminate:
            kernel32.TerminateJobObject(handle, 1)
        kernel32.CloseHandle(handle)

    @staticmethod
    def _linux_descendant_pids(root_pid: int) -> tuple[int, ...]:
        if not sys.platform.startswith("linux"):
            return ()
        children: dict[int, list[int]] = {}
        proc = Path("/proc")
        for entry in proc.iterdir():
            if not entry.name.isdigit():
                continue
            try:
                raw = (entry / "stat").read_text("utf-8", errors="replace")
                tail = raw.rsplit(")", 1)[1].strip().split()
                ppid = int(tail[1])
                pid = int(entry.name)
            except (OSError, ValueError, IndexError):
                continue
            children.setdefault(ppid, []).append(pid)
        found: list[int] = []
        stack = list(children.get(root_pid, ()))
        while stack:
            pid = stack.pop()
            if pid in found:
                continue
            found.append(pid)
            stack.extend(children.get(pid, ()))
        # Deepest/newest children first is friendlier to process trees that react
        # to parent death by immediately spawning replacements.
        return tuple(reversed(found))

    @staticmethod
    def _signal_pid(pid: int, sig: int) -> None:
        with suppress(ProcessLookupError, PermissionError):
            os.kill(pid, sig)

    async def _terminate_child_process(self, process: asyncio.subprocess.Process) -> bool:
        if process.returncode is not None:
            return True
        descendants = self._linux_descendant_pids(process.pid)
        if os.name == "posix":
            for pid in descendants:
                self._signal_pid(pid, signal.SIGTERM)
            with suppress(ProcessLookupError, PermissionError):
                os.killpg(process.pid, signal.SIGTERM)
        else:
            self._close_windows_job(process.pid, terminate=True)
            if process.returncode is None:
                process.terminate()
        try:
            await asyncio.wait_for(process.wait(), self.limits.cancel_grace_seconds)
        except asyncio.TimeoutError:
            pass
        if process.returncode is None:
            if os.name == "posix":
                # Re-scan before SIGKILL to catch descendants created during the
                # grace window, including start_new_session() escape attempts.
                descendants = tuple(dict.fromkeys((*descendants, *self._linux_descendant_pids(process.pid))))
                for pid in descendants:
                    self._signal_pid(pid, signal.SIGKILL)
                with suppress(ProcessLookupError, PermissionError):
                    os.killpg(process.pid, signal.SIGKILL)
            else:
                self._close_windows_job(process.pid, terminate=True)
                process.kill()
            try:
                await asyncio.wait_for(process.wait(), self.limits.kill_wait_seconds)
            except asyncio.TimeoutError:
                return False
        if os.name == "posix":
            # The direct child can exit before detached descendants. Retained PIDs
            # are still safe to signal here because they were observed as descendants
            # while the child was alive.
            for pid in descendants:
                self._signal_pid(pid, signal.SIGKILL)
            deadline = self._clock() + self.limits.kill_wait_seconds
            while self._clock() < deadline:
                alive = []
                for pid in descendants:
                    try:
                        os.kill(pid, 0)
                    except (ProcessLookupError, PermissionError):
                        continue
                    alive.append(pid)
                if not alive:
                    break
                await asyncio.sleep(0.02)
        return process.returncode is not None

    @property
    def prepared_program_ids(self) -> frozenset[str]:
        return frozenset(self._prepared_program_ids)

    @property
    def active_attempt_ids(self) -> frozenset[str]:
        return frozenset((*self._active, *self._active_context))

    @property
    def context_ids(self) -> frozenset[str]:
        return frozenset(key[2] for key in self._contexts)

    def diagnostics(self, attempt_id: str) -> ExecutionDiagnostics | None:
        return self._diagnostics.get(attempt_id)

    def _live_package_ids(self) -> frozenset[str]:
        """Packages whose files may still be in use by a live child/context."""
        program_ids = {item.dispatch.program_id for item in self._active.values()}
        program_ids.update(item.dispatch.program_id for item in self._active_context.values())
        program_ids.update(context.command.program_id for context in self._contexts.values())
        package_ids = {
            program.package_id
            for program_id in program_ids
            if (program := self._programs.get(program_id)) is not None
            and program.package_id is not None
        }
        return frozenset(package_ids)

    def _drop_evicted_prepared_metadata(self) -> None:
        """Invalidate idle prepared plans whose LRU cache entry was reclaimed."""
        for program_id in tuple(self._prepared_program_ids):
            program = self._programs.get(program_id)
            if program is None or program.package_id is None:
                self._prepared_program_ids.discard(program_id)
                continue
            if self.cache.entry_path(program.package_id).exists():
                continue
            self._prepared_program_ids.discard(program_id)
            for key in tuple(self._plans):
                if key[0] == program_id:
                    self._plans.pop(key, None)

    async def refresh_prepared_cache(self) -> None:
        """Revalidate idle prepared packages before advertising them in a heartbeat/hello."""
        active_programs = {item.dispatch.program_id for item in self._active.values()}
        active_programs.update(item.dispatch.program_id for item in self._active_context.values())
        active_programs.update(context.command.program_id for context in self._contexts.values())
        for program_id in tuple(self._prepared_program_ids):
            if program_id in active_programs:
                continue
            program = self._programs.get(program_id)
            if program is None or program.package_id is None:
                self._prepared_program_ids.discard(program_id)
                continue
            manifest = await asyncio.to_thread(self.cache.verify, program.package_id)
            if manifest is None:
                self._prepared_program_ids.discard(program_id)
                for key in tuple(self._plans):
                    if key[0] == program_id:
                        self._plans.pop(key, None)

    def decorate_state(self, state: WorkerState) -> WorkerState:
        if state.worker_id != self.worker_id:
            raise ValueError("worker state identity mismatch")
        running = sum(1 for item in self._active.values() if item.process is not None and item.process.returncode is None)
        running += sum(
            1 for item in self._active_context.values()
            if (context := self._context_for_dispatch(item.dispatch)) is not None
            and context.process.returncode is None
        )
        reserved = len(self._active) + len(self._active_context) - running
        if running + reserved > state.total_slots:
            # Never advertise an impossible heartbeat even under an internal bug.
            running = min(running, state.total_slots)
            reserved = min(reserved, state.total_slots - running)
        return replace(
            state,
            running_slots=running,
            reserved_slots=reserved,
            prepared_program_ids=frozenset(self._prepared_program_ids),
            supported_modes=frozenset({
                ExecutionMode.ISOLATED_CANDIDATE,
                ExecutionMode.SHARED_CONTEXT,
                ExecutionMode.NATIVE_REGION,
            }),
        )

    async def ensure_data_plane_listener(self, endpoint: p.WorkerEndpoint) -> None:
        if self.data_plane is None:
            return
        if endpoint.worker_id != self.worker_id:
            raise ValueError("data-plane endpoint worker identity mismatch")
        await self.data_plane.listen()
        if self.data_plane.listening_port != endpoint.port:
            raise ValueError("advertised worker endpoint port differs from data-plane listener")
        configured_hosts = {self.data_plane.config.host, "0.0.0.0"}
        if endpoint.host not in configured_hosts and self.data_plane.config.host not in {"0.0.0.0", endpoint.host}:
            raise ValueError("advertised worker endpoint host differs from data-plane listener")

    async def session_started(
        self, session_id: str, send: SendMessage, abort_session: AbortSession | None = None
    ) -> None:
        if self._session_id is not None and self._session_id != session_id:
            await self.session_lost(self._session_id)
        self._session_id = session_id
        self._send = send
        self._abort_session = abort_session
        # Attempt identities are only meaningful inside the control session that
        # issued them.  A restarted coordinator starts a fresh identity sequence,
        # so retaining the previous session's retired attempt IDs would reject its
        # first dispatches as `stale_attempt` and fail otherwise healthy runs.
        # Duplicate-dispatch protection is per session, which this preserves.
        self._terminal_attempts.clear()
        self._terminal_order.clear()
        if self.data_plane is not None:
            await self.data_plane.start(
                session_id,
                on_completed=self._p2p_completed,
                on_failed=self._p2p_failed,
                on_started=self._p2p_started,
                on_send_finished=self._p2p_send_finished,
            )

    async def session_lost(self, session_id: str) -> bool:
        if self._session_id != session_id:
            return not any(item.session_id == session_id for item in self._active.values()) and not any(
                item.session_id == session_id for item in self._active_context.values()
            )
        self._session_id = None
        self._send = None
        self._abort_session = None
        cancel_jobs = [
            task for job_session, task in self._cancel_tasks.values()
            if job_session == session_id and not task.done()
        ]
        for task in cancel_jobs:
            task.cancel()
        if cancel_jobs:
            await asyncio.gather(*cancel_jobs, return_exceptions=True)
        data_plane_clean = True
        if self.data_plane is not None:
            data_plane_clean = await self.data_plane.stop_session(session_id)
        self._receive_commands.clear()
        self._send_commands.clear()
        for prep in tuple(self._preparations.values()):
            self._cleanup_preparation(prep)
        self._preparations.clear()
        self._package_owner.clear()
        executions = [item for item in self._active.values() if item.session_id == session_id]
        results = await asyncio.gather(
            *(self._terminate(item) for item in executions), return_exceptions=True
        )
        unresolved: list[_ActiveExecution] = []
        for item, result in zip(executions, results):
            cleaned = result is True or (item.process is not None and item.process.returncode is not None)
            if not cleaned:
                unresolved.append(item)
                continue
            if item.task is not None and item.task is not asyncio.current_task() and not item.task.done():
                item.task.cancel()
        cleaned_tasks = [
            item.task for item in executions if item not in unresolved
            and item.task is not None and item.task is not asyncio.current_task()
        ]
        if cleaned_tasks:
            await asyncio.gather(*cleaned_tasks, return_exceptions=True)
        for item in executions:
            if item not in unresolved:
                self._active.pop(item.dispatch.attempt.attempt_id, None)
        context_results = await asyncio.gather(
            *(self._retire_context(context, notify=False, reason="worker control session lost")
              for context in tuple(self._contexts.values()) if context.session_id == session_id),
            return_exceptions=True,
        )
        context_clean = all(result is True for result in context_results)
        self._active_context = {
            attempt_id: item for attempt_id, item in self._active_context.items()
            if item.session_id != session_id
        }
        # Data residency is session-scoped. A reconnect must not silently resurrect
        # old execution outputs as current-session inputs. Unreaped old-session
        # children remain in _active solely to retain physical capacity until they die.
        self.data_store.remove_session(session_id)
        return not unresolved and context_clean and data_plane_clean

    async def shutdown(self) -> bool:
        if self._session_id is not None:
            cleaned = await self.session_lost(self._session_id)
            if self.data_plane is not None:
                await self.data_plane.close()
            return cleaned
        executions = tuple(self._active.values())
        results = await asyncio.gather(
            *(self._terminate(item) for item in executions), return_exceptions=True
        )
        cleaned_tasks: list[asyncio.Task[None]] = []
        for item, result in zip(executions, results):
            if result is True or (item.process is not None and item.process.returncode is not None):
                if item.task is not None and item.task is not asyncio.current_task() and not item.task.done():
                    item.task.cancel()
                if item.task is not None and item.task is not asyncio.current_task():
                    cleaned_tasks.append(item.task)
        if cleaned_tasks:
            await asyncio.gather(*cleaned_tasks, return_exceptions=True)
        for item, result in zip(executions, results):
            if result is True or (item.process is not None and item.process.returncode is not None):
                self._active.pop(item.dispatch.attempt.attempt_id, None)
        contexts = tuple(self._contexts.values())
        context_results = await asyncio.gather(
            *(self._retire_context(context, notify=False, reason="worker runtime shutdown")
              for context in contexts), return_exceptions=True,
        )
        if self.data_plane is not None:
            await self.data_plane.close()
        return not self._active and not self._active_context and all(
            result is True for result in context_results
        )

    async def handle_message(self, message: p.Message, *, session_id: str) -> bool:
        if self._session_id != session_id:
            return False
        if isinstance(message, p.PrepareProgram):
            await self._prepare_program(message)
            return True
        if isinstance(message, p.PackageTransferStart):
            await self._package_start(message)
            return True
        if isinstance(message, p.PackageTransferChunk):
            await self._package_chunk(message)
            return True
        if isinstance(message, p.PackageTransferEnd):
            await self._package_end(message)
            return True
        if isinstance(message, p.PrepareContext):
            await self._prepare_context(message, session_id)
            return True
        if isinstance(message, p.ReleaseContext):
            await self._release_context(message, session_id)
            return True
        if isinstance(message, p.TaskDispatch):
            await self._dispatch(message, session_id)
            return True
        if isinstance(message, p.CancelTask):
            attempt_id = message.attempt.attempt_id
            if attempt_id in self._active or attempt_id in self._active_context:
                self._handoff_cancel(message, session_id)
            else:
                # No physical child/context can block here; preserve the prompt
                # NOT_FOUND/TOO_LATE idempotency response for duplicate cancels.
                await self._cancel(message)
            return True
        if isinstance(message, p.PrepareReceive):
            await self._prepare_receive(message, session_id)
            return True
        if isinstance(message, p.TransferRequest):
            await self._transfer_request(message, session_id)
            return True
        if isinstance(message, p.CancelTransfer):
            await self._cancel_transfer(message, session_id)
            return True
        if isinstance(message, p.ReleaseObject):
            await self._release_object(message, session_id)
            return True
        return False

    async def _emit(self, message: p.Message) -> None:
        send = self._send
        if send is None:
            raise ConnectionError("worker session is not active")
        try:
            await send(message)
        except BaseException as error:
            # Background executor tasks are not directly awaited by the control
            # loop. A failed authoritative publication must therefore fail the
            # owning session closed instead of leaving the coordinator waiting
            # forever on an event that was dropped locally.
            abort = self._abort_session
            if abort is not None:
                with suppress(Exception):
                    abort(error)
            raise

    async def _prepare_program(self, command: p.PrepareProgram) -> None:
        if command.worker_id != self.worker_id:
            return
        program = command.program
        if program.package_id is None:
            await self._preparation_failed(command, FailureKind.EXECUTION_ERROR, "program has no content-addressed package identity")
            return
        if program.environment_id not in self.base_state.environment_ids:
            await self._preparation_failed(command, FailureKind.ENVIRONMENT_MISMATCH, "worker environment does not satisfy program identity")
            return
        if not self._portable_program_filename(program.filename):
            await self._preparation_failed(command, FailureKind.EXECUTION_ERROR, "program filename is not a safe package-relative path")
            return
        manifest = await asyncio.to_thread(self.cache.verify, program.package_id)
        if manifest is not None:
            await self._finish_preparation(command)
            return
        existing_owner = self._package_owner.get(program.package_id)
        prep = _Preparation(command, wait_for=existing_owner)
        self._preparations[command.message_id] = prep
        if existing_owner is None:
            self._package_owner[program.package_id] = command.message_id
        prep.timeout_task = asyncio.create_task(self._preparation_timeout(command.message_id))

    async def _preparation_timeout(self, message_id: str) -> None:
        await asyncio.sleep(self.limits.preparation_timeout_seconds)
        prep = self._preparations.get(message_id)
        if prep is None:
            return
        await self._fail_preparation_object(prep, FailureKind.EXECUTION_ERROR, "program package delivery timed out")

    async def _package_start(self, message: p.PackageTransferStart) -> None:
        prep = self._preparations.get(message.correlation_id or "")
        if prep is None:
            return
        command = prep.command
        if prep.wait_for is not None:
            return
        package_id = command.program.package_id
        if (message.worker_id != self.worker_id or message.plan_id != command.plan_id
                or message.program_id != command.program.id or message.package_id != package_id):
            await self._fail_preparation_object(prep, FailureKind.EXECUTION_ERROR, "package transfer identity mismatch")
            return
        if message.size_bytes > self.cache.limits.max_archive_bytes:
            await self._fail_preparation_object(prep, FailureKind.EXECUTION_ERROR, "package archive exceeds worker limit")
            return
        # Protocol identifiers are opaque correlation values, not filesystem names.
        # Keep them out of paths even though this message came from the authenticated
        # coordinator: custom ID providers are allowed and must not create traversal.
        path = self.cache.staging / f"download-{__import__('secrets').token_hex(16)}.part"
        try:
            handle = path.open("xb")
        except OSError as error:
            await self._fail_preparation_object(prep, FailureKind.EXECUTION_ERROR, f"cannot stage package download: {error}")
            return
        prep.path, prep.handle = path, handle
        prep.hasher = hashlib.sha256()
        prep.expected_size = message.size_bytes
        prep.expected_hash = message.archive_sha256
        prep.received = 0

    async def _package_chunk(self, message: p.PackageTransferChunk) -> None:
        prep = self._preparations.get(message.correlation_id or "")
        if prep is None or prep.wait_for is not None:
            return
        if prep.handle is None or prep.hasher is None or message.package_id != prep.command.program.package_id:
            await self._fail_preparation_object(prep, FailureKind.EXECUTION_ERROR, "package chunk arrived outside active transfer")
            return
        if message.offset != prep.received:
            await self._fail_preparation_object(prep, FailureKind.EXECUTION_ERROR, "package chunk offset mismatch")
            return
        data = bytes.fromhex(message.data_hex)
        if prep.expected_size is None or prep.received + len(data) > prep.expected_size:
            await self._fail_preparation_object(prep, FailureKind.EXECUTION_ERROR, "package transfer exceeded declared size")
            return
        try:
            prep.handle.write(data)
        except OSError as error:
            await self._fail_preparation_object(prep, FailureKind.EXECUTION_ERROR, f"package staging write failed: {error}")
            return
        prep.hasher.update(data)
        prep.received += len(data)

    async def _package_end(self, message: p.PackageTransferEnd) -> None:
        prep = self._preparations.get(message.correlation_id or "")
        if prep is None:
            return
        if prep.wait_for is not None:
            return
        if prep.handle is None or prep.hasher is None or prep.path is None:
            await self._fail_preparation_object(prep, FailureKind.EXECUTION_ERROR, "package transfer ended before start")
            return
        if (message.package_id != prep.command.program.package_id
                or message.size_bytes != prep.expected_size or message.archive_sha256 != prep.expected_hash
                or prep.received != prep.expected_size or prep.hasher.hexdigest() != prep.expected_hash):
            await self._fail_preparation_object(prep, FailureKind.EXECUTION_ERROR, "package transfer integrity mismatch")
            return
        try:
            prep.handle.flush()
            os.fsync(prep.handle.fileno())
            prep.handle.close()
            prep.handle = None
            # FIXES F17 lists only the cache module, but that layer cannot know
            # whether a child currently has files open. Protect live package roots
            # and invalidate metadata for any idle prepared package evicted by LRU.
            await asyncio.to_thread(
                self.cache.install_archive, prep.path, message.package_id,
                protected_package_ids=self._live_package_ids(),
            )
            self._drop_evicted_prepared_metadata()
        except (OSError, PackageError) as error:
            await self._fail_preparation_object(prep, FailureKind.EXECUTION_ERROR, f"package installation failed: {error}")
            return
        owner_id = prep.command.message_id
        waiters = [item for item in self._preparations.values() if item.wait_for == owner_id]
        await self._complete_preparation_object(prep)
        for waiter in waiters:
            await self._complete_preparation_object(waiter)

    async def _complete_preparation_object(self, prep: _Preparation) -> None:
        try:
            await self._finish_preparation(prep.command)
        except Exception as error:
            await self._preparation_failed(prep.command, FailureKind.EXECUTION_ERROR, f"program verification failed: {error}")
        finally:
            self._remove_preparation(prep)

    async def _finish_preparation(self, command: p.PrepareProgram) -> None:
        program = command.program
        assert program.package_id is not None
        manifest = await asyncio.to_thread(self.cache.verify, program.package_id)
        if manifest is None:
            raise PackageIntegrityError("package cache verification failed")
        source_path = self.cache.content_path(program.package_id).joinpath(*PurePosixPath(program.filename).parts)
        source_entry = next((item for item in manifest.files if item.path == program.filename), None)
        if source_entry is None:
            raise PackageIntegrityError("program entrypoint is absent from package manifest")
        with source_path.open("rb") as source_handle:
            data = source_handle.read(source_entry.size + 1)
        if len(data) != source_entry.size:
            raise PackageIntegrityError("program entrypoint size changed after cache verification")
        # utf-8-sig on all three sites (CLI, coordinator, worker) so a leading BOM —
        # which CPython accepts and editors on Windows add — yields the same decoded
        # source everywhere.  The raw packaged bytes remain covered by the package
        # manifest digest, so tampering is still detected.
        source = data.decode("utf-8-sig")
        if hashlib.sha256(source.encode("utf-8")).hexdigest() != program.source_sha256:
            raise PackageIntegrityError("entrypoint source digest differs from ProgramIdentity")
        dag = analyze_source(source, filename=program.filename)
        plan = lower_dag(dag, environment_id=program.environment_id, package_id=program.package_id)
        if plan.program != program or plan.id != command.plan_id:
            raise PackageIntegrityError("reconstructed execution plan identity mismatch")
        if program.id not in self._prepared_program_ids and len(self._prepared_program_ids) >= self.limits.max_prepared_programs:
            raise PackageResourceError("prepared-program retention limit reached")
        response = p.ProgramPrepared(
            worker_id=self.worker_id, plan_id=command.plan_id, program_id=program.id,
            message_id=new_transport_id("program-prepared"), correlation_id=command.message_id,
        )
        # Publish worker-local preparation only after every fallible verification/message construction step.
        self._plans[(program.id, plan.id)] = plan
        self._programs[program.id] = program
        self._prepared_program_ids.add(program.id)
        await self._emit(response)

    async def _preparation_failed(self, command: p.PrepareProgram, kind: FailureKind, detail: str) -> None:
        failure = FailureInfo(kind, self._bound_text(detail, p.MAX_DETAIL_BYTES))
        await self._emit(p.ProgramPreparationFailed(
            worker_id=self.worker_id, plan_id=command.plan_id, program_id=command.program.id,
            failure=failure, message_id=new_transport_id("program-preparation-failed"),
            correlation_id=command.message_id,
        ))

    async def _fail_preparation_object(self, prep: _Preparation, kind: FailureKind, detail: str) -> None:
        owner_id = prep.command.message_id
        waiters = [item for item in self._preparations.values() if item.wait_for == owner_id]
        self._cleanup_preparation(prep)
        self._remove_preparation(prep)
        await self._preparation_failed(prep.command, kind, detail)
        for waiter in waiters:
            self._remove_preparation(waiter)
            await self._preparation_failed(waiter.command, kind, detail)

    def _remove_preparation(self, prep: _Preparation) -> None:
        self._preparations.pop(prep.command.message_id, None)
        if prep.command.program.package_id is not None and self._package_owner.get(prep.command.program.package_id) == prep.command.message_id:
            self._package_owner.pop(prep.command.program.package_id, None)
        self._cleanup_preparation(prep)

    def _cleanup_preparation(self, prep: _Preparation) -> None:
        if prep.timeout_task is not None and prep.timeout_task is not asyncio.current_task():
            prep.timeout_task.cancel()
        if prep.handle is not None:
            with suppress(Exception): prep.handle.close()
            prep.handle = None
        if prep.path is not None:
            with suppress(OSError): prep.path.unlink()
            prep.path = None

    # ---------- persistent worker contexts ----------
    def _context_key(self, plan_id: str, run_id: str, context_id: str) -> tuple[str, str, str]:
        return plan_id, run_id, context_id

    def _context_for_dispatch(self, dispatch: p.TaskDispatch) -> _WorkerContextRuntime | None:
        if dispatch.context_id is None:
            return None
        return self._contexts.get(self._context_key(
            dispatch.attempt.plan_id, dispatch.attempt.run_id, dispatch.context_id
        ))

    async def _prepare_context(self, command: p.PrepareContext, session_id: str) -> None:
        if command.worker_id != self.worker_id or self._session_id != session_id:
            return
        plan = self._plans.get((command.program_id, command.plan_id))
        if plan is None:
            await self._context_preparation_failed(
                command, FailureKind.EXECUTION_ERROR, "exact program/plan is not prepared"
            )
            return
        if any(task_id not in plan.task_index for task_id in command.task_ids):
            await self._context_preparation_failed(
                command, FailureKind.EXECUTION_ERROR, "context references unknown task"
            )
            return
        key = self._context_key(command.plan_id, command.run_id, command.context_id)
        existing = self._contexts.get(key)
        if existing is not None:
            if (
                existing.session_id == session_id
                and existing.command.worker_id == command.worker_id
                and existing.command.plan_id == command.plan_id
                and existing.command.run_id == command.run_id
                and existing.command.program_id == command.program_id
                and existing.command.context_id == command.context_id
                and existing.command.task_ids == command.task_ids
                and existing.process.returncode is None
            ):
                await self._emit(p.ContextPrepared(
                    plan_id=command.plan_id, run_id=command.run_id,
                    context=__import__("scheduler").WorkerContext(
                        command.context_id, self.worker_id,
                        frozenset(command.task_ids), 1,
                    ),
                    message_id=new_transport_id("context-prepared"),
                    correlation_id=command.message_id,
                ))
                return
            await self._context_preparation_failed(
                command, FailureKind.EXECUTION_ERROR, "conflicting physical context identity"
            )
            return
        if len(self._contexts) >= self.limits.max_contexts:
            await self._context_preparation_failed(
                command, FailureKind.EXECUTION_ERROR, "worker context capacity exhausted"
            )
            return
        program = self._programs.get(command.program_id)
        if program is None or program.package_id is None:
            await self._context_preparation_failed(
                command, FailureKind.EXECUTION_ERROR, "prepared program metadata is unavailable"
            )
            return
        if await asyncio.to_thread(self.cache.verify, program.package_id) is None:
            await self._context_preparation_failed(
                command, FailureKind.EXECUTION_ERROR, "program package cache is missing or corrupt"
            )
            return
        package_root = self.cache.content_path(program.package_id)
        context_sandbox: Path | None = None
        if self._child_uid is not None:
            context_sandbox = self._make_child_workdir("dpr-context-")
            try:
                package_root = self._copy_package_view(package_root, context_sandbox)
            except Exception as error:
                await asyncio.to_thread(__import__("shutil").rmtree, context_sandbox, True)
                await self._context_preparation_failed(
                    command, FailureKind.EXECUTION_ERROR, f"cannot build child package view: {error}"
                )
                return
        child = Path(__file__).with_name("context_child.py")
        kwargs = self._child_spawn_kwargs()
        control_read_fd: int | None = None
        control_write_fd: int | None = None
        child_args = [sys.executable, "-I", "-B", str(child)]
        child_stdin = asyncio.subprocess.DEVNULL
        if os.name == "posix":
            control_read_fd, control_write_fd = os.pipe()
            kwargs["pass_fds"] = (control_read_fd,)
            child_args.extend(("--control-fd", str(control_read_fd)))
        elif os.name == "nt":
            # FIXES F53 names a Windows named pipe/socket.  The current asyncio
            # child API cannot pass an arbitrary inherited fd, so preserve Windows
            # compatibility by capturing this bootstrap pipe inside context_child
            # and replacing sys.stdin with DEVNULL before any user source runs.
            child_args.append("--control-stdin")
            child_stdin = asyncio.subprocess.PIPE
        try:
            process = await asyncio.create_subprocess_exec(
                *child_args, cwd=str(package_root),
                stdin=child_stdin, stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE, **kwargs,
            )
            self._attach_windows_job(process.pid)
        except (OSError, ValueError) as error:
            for fd in (control_read_fd, control_write_fd):
                if fd is not None:
                    with suppress(OSError):
                        os.close(fd)
            if context_sandbox is not None:
                await asyncio.to_thread(__import__("shutil").rmtree, context_sandbox, True)
            await self._context_preparation_failed(
                command, FailureKind.EXECUTION_ERROR, f"cannot create context process: {error}"
            )
            return
        finally:
            if os.name == "posix" and control_read_fd is not None:
                with suppress(OSError):
                    os.close(control_read_fd)
        await asyncio.sleep(0)
        control_stream = process.stdin if os.name == "nt" else None
        if process.returncode is not None or (control_write_fd is None and control_stream is None):
            if control_write_fd is not None:
                with suppress(OSError):
                    os.close(control_write_fd)
            with suppress(Exception):
                await process.wait()
            if context_sandbox is not None:
                await asyncio.to_thread(__import__("shutil").rmtree, context_sandbox, True)
            await self._context_preparation_failed(
                command, FailureKind.EXECUTION_ERROR, "context process exited during preparation"
            )
            return
        context = _WorkerContextRuntime(
            command, session_id, plan, process, control_write_fd, control_stream,
            asyncio.create_task(self._drain(process.stdout, self.limits.stdout_bytes)),
            asyncio.create_task(self._drain(process.stderr, self.limits.stderr_bytes)),
            package_root=package_root,
            sandbox_root=context_sandbox,
        )
        self._contexts[key] = context
        try:
            await self._emit(p.ContextPrepared(
                plan_id=command.plan_id, run_id=command.run_id,
                context=__import__("scheduler").WorkerContext(
                    command.context_id, self.worker_id, frozenset(command.task_ids), 1
                ),
                message_id=new_transport_id("context-prepared"),
                correlation_id=command.message_id,
            ))
        except BaseException:
            await self._retire_context(context, notify=False, reason="context preparation publication failed")
            raise

    async def _release_context(self, command: p.ReleaseContext, session_id: str) -> None:
        if command.worker_id != self.worker_id or self._session_id != session_id:
            return
        key = self._context_key(command.plan_id, command.run_id, command.context_id)
        context = self._contexts.get(key)
        if context is not None and context.session_id == session_id:
            await self._retire_context(
                context, notify=True, reason=command.reason or "context released by coordinator"
            )
            return
        # Idempotent cleanup evidence: absence is already physical retirement.
        await self._emit(p.ContextUnavailable(
            worker_id=self.worker_id, plan_id=command.plan_id, run_id=command.run_id,
            context_id=command.context_id,
            reason=self._bound_text(command.reason or "context already unavailable", p.MAX_DETAIL_BYTES),
            message_id=new_transport_id("context-unavailable"),
        ))

    async def _context_preparation_failed(
        self, command: p.PrepareContext, kind: FailureKind, detail: str
    ) -> None:
        await self._emit(p.ContextPreparationFailed(
            worker_id=self.worker_id, plan_id=command.plan_id, run_id=command.run_id,
            context_id=command.context_id,
            failure=FailureInfo(kind, self._bound_text(detail, p.MAX_DETAIL_BYTES)),
            message_id=new_transport_id("context-preparation-failed"),
            correlation_id=command.message_id,
        ))

    async def _retire_context(
        self, context: _WorkerContextRuntime, *, notify: bool, reason: str
    ) -> bool:
        if context.retired and context.process.returncode is not None:
            return True
        context.retired = True
        process = context.process
        if context.control_stream is not None:
            context.control_stream.close()
            with suppress(Exception):
                await context.control_stream.wait_closed()
            context.control_stream = None
        if context.control_fd is not None:
            with suppress(OSError):
                os.close(context.control_fd)
            context.control_fd = None
        cleaned = await self._terminate_process(process)
        if not cleaned and process.returncode is None:
            return False
        if process.returncode is not None:
            self._close_windows_job(process.pid, terminate=False)
        if context.stdout_task is not None and not context.stdout_task.done():
            context.stdout_task.cancel()
        if context.stderr_task is not None and not context.stderr_task.done():
            context.stderr_task.cancel()
        await asyncio.gather(
            *(task for task in (context.stdout_task, context.stderr_task) if task is not None),
            return_exceptions=True,
        )
        key = self._context_key(
            context.command.plan_id, context.command.run_id, context.command.context_id
        )
        self._contexts.pop(key, None)
        if context.active_attempt_id is not None:
            active = self._active_context.pop(context.active_attempt_id, None)
            context.active_attempt_id = None
            if (active is not None and active.task is not None
                    and active.task is not asyncio.current_task() and not active.task.done()):
                active.task.cancel()
                await asyncio.gather(active.task, return_exceptions=True)
        self.data_store.remove_context(context.command.context_id, session_id=context.session_id)
        if context.sandbox_root is not None:
            await asyncio.to_thread(__import__("shutil").rmtree, context.sandbox_root, True)
            context.sandbox_root = None
        if notify and self._session_id == context.session_id:
            await self._emit(p.ContextUnavailable(
                worker_id=self.worker_id, plan_id=context.command.plan_id,
                run_id=context.command.run_id, context_id=context.command.context_id,
                reason=self._bound_text(reason, p.MAX_DETAIL_BYTES),
                message_id=new_transport_id("context-unavailable"),
            ))
        return True

    async def _terminate_process(self, process: asyncio.subprocess.Process) -> bool:
        return await self._terminate_child_process(process)

    # ---------- direct runtime data plane ----------
    async def _prepare_receive(self, command: p.PrepareReceive, session_id: str) -> None:
        if command.transfer.destination_worker_id != self.worker_id:
            return
        if self.data_plane is None:
            await self._emit(p.ReceivePreparationFailed(
                worker_id=self.worker_id, transfer=command.transfer,
                code=p.TransferFailureCode.DESTINATION_UNAVAILABLE,
                detail="worker has no configured runtime data plane",
                message_id=new_transport_id("receive-preparation-failed"),
                correlation_id=command.message_id,
            ))
            return
        key = (command.transfer.transfer_id, command.transfer.transfer_attempt_id)
        try:
            await self.data_plane.prepare_receive(command, session_id=session_id)
        except (DataPlaneError, DataStoreError, ValueError) as error:
            await self._emit(p.ReceivePreparationFailed(
                worker_id=self.worker_id, transfer=command.transfer,
                code=p.TransferFailureCode.DESTINATION_UNAVAILABLE,
                detail=self._bound_text(str(error), p.MAX_DETAIL_BYTES),
                message_id=new_transport_id("receive-preparation-failed"),
                correlation_id=command.message_id,
            ))
            return
        self._receive_commands[key] = command
        await self._emit(p.ReceiveReady(
            worker_id=self.worker_id, transfer=command.transfer,
            message_id=new_transport_id("receive-ready"), correlation_id=command.message_id,
        ))
        self.data_plane.mark_ready_sent(command.transfer)

    async def _transfer_request(self, command: p.TransferRequest, session_id: str) -> None:
        if command.transfer.source_worker_id != self.worker_id:
            return
        if self.data_plane is None:
            await self._emit(p.TransferFailed(
                worker_id=self.worker_id, transfer=command.transfer,
                code=p.TransferFailureCode.SOURCE_UNAVAILABLE,
                detail="worker has no configured runtime data plane",
                message_id=new_transport_id("transfer-failed"), correlation_id=command.message_id,
            ))
            return
        entry = self.data_store.get(command.transfer.data, session_id=session_id)
        if entry is None:
            await self._emit(p.TransferFailed(
                worker_id=self.worker_id, transfer=command.transfer,
                code=p.TransferFailureCode.DATA_UNAVAILABLE,
                detail="exact source representation is unavailable in current session",
                message_id=new_transport_id("transfer-failed"), correlation_id=command.message_id,
            ))
            return
        key = (command.transfer.transfer_id, command.transfer.transfer_attempt_id)
        self._send_commands[key] = command
        await self._emit(p.TransferAccepted(
            worker_id=self.worker_id, transfer=command.transfer,
            message_id=new_transport_id("transfer-accepted"), correlation_id=command.message_id,
        ))
        try:
            await self.data_plane.send(command, session_id=session_id)
        except (DataPlaneError, DataStoreError, ValueError) as error:
            self._send_commands.pop(key, None)
            await self._emit(p.TransferFailed(
                worker_id=self.worker_id, transfer=command.transfer,
                code=p.TransferFailureCode.IO_ERROR,
                detail=self._bound_text(str(error), p.MAX_DETAIL_BYTES),
                message_id=new_transport_id("transfer-failed"), correlation_id=command.message_id,
            ))

    async def _p2p_started(self, transfer: p.TransferIdentity) -> None:
        command = self._send_commands.get((transfer.transfer_id, transfer.transfer_attempt_id))
        if command is None or self._session_id != command.source_session_id:
            return
        await self._emit(p.TransferStarted(
            worker_id=self.worker_id, transfer=transfer,
            message_id=new_transport_id("transfer-started"), correlation_id=command.message_id,
        ))

    async def _p2p_send_finished(self, transfer: p.TransferIdentity) -> None:
        """Retire local source correlation after physical write cleanup only.

        This is deliberately not coordinator completion evidence; only the
        destination can publish TransferCompleted after verified receipt.
        """
        key = (transfer.transfer_id, transfer.transfer_attempt_id)
        command = self._send_commands.get(key)
        if command is not None and self._session_id == command.source_session_id:
            self._send_commands.pop(key, None)

    async def _p2p_completed(self, transfer: p.TransferIdentity, size_bytes: int) -> None:
        key = (transfer.transfer_id, transfer.transfer_attempt_id)
        command = self._receive_commands.get(key)
        if command is None or self._session_id != command.destination_session_id:
            return
        await self._emit(p.TransferCompleted(
            worker_id=self.worker_id, transfer=transfer, size_bytes=size_bytes,
            message_id=new_transport_id("transfer-completed"), correlation_id=command.message_id,
        ))
        self._receive_commands.pop(key, None)

    async def _p2p_failed(
        self, transfer: p.TransferIdentity, destination: bool, detail: str
    ) -> None:
        key = (transfer.transfer_id, transfer.transfer_attempt_id)
        command = self._receive_commands.get(key) if destination else self._send_commands.get(key)
        if command is None:
            return
        expected_session = command.destination_session_id if destination else command.source_session_id
        if self._session_id != expected_session:
            return
        lowered = detail.lower()
        if "cancel" in lowered:
            code = p.TransferFailureCode.CANCELLED
        elif any(word in lowered for word in ("integrity", "digest", "size", "binding", "authorization")):
            code = p.TransferFailureCode.INTEGRITY_ERROR
        else:
            code = p.TransferFailureCode.IO_ERROR
        await self._emit(p.TransferFailed(
            worker_id=self.worker_id, transfer=transfer, code=code,
            detail=self._bound_text(detail, p.MAX_DETAIL_BYTES),
            message_id=new_transport_id("transfer-failed"), correlation_id=command.message_id,
        ))
        if destination:
            self._receive_commands.pop(key, None)
        else:
            self._send_commands.pop(key, None)

    async def _cancel_transfer(self, command: p.CancelTransfer, session_id: str) -> None:
        if command.worker_id != self.worker_id or self.data_plane is None:
            return
        key = (command.transfer.transfer_id, command.transfer.transfer_attempt_id)
        source_command = self._send_commands.get(key)
        destination_command = self._receive_commands.get(key)
        source_had, destination_had = await self.data_plane.cancel(
            command.transfer, session_id=session_id
        )
        # Existing participant state reports through the data-plane failure callback
        # only after its physical socket/coroutine is actually stopped. If no state
        # exists, cleanup is already established and can be acknowledged directly.
        if not source_had and source_command is not None:
            await self._emit(p.TransferFailed(
                worker_id=self.worker_id, transfer=command.transfer,
                code=p.TransferFailureCode.CANCELLED,
                detail=self._bound_text(command.reason or "transfer cancelled", p.MAX_DETAIL_BYTES),
                message_id=new_transport_id("transfer-failed"),
                correlation_id=source_command.message_id,
            ))
            self._send_commands.pop(key, None)
        if not destination_had and destination_command is not None:
            await self._emit(p.TransferFailed(
                worker_id=self.worker_id, transfer=command.transfer,
                code=p.TransferFailureCode.CANCELLED,
                detail=self._bound_text(command.reason or "transfer cancelled", p.MAX_DETAIL_BYTES),
                message_id=new_transport_id("transfer-failed"),
                correlation_id=destination_command.message_id,
            ))
            self._receive_commands.pop(key, None)

    async def _release_object(self, command: p.ReleaseObject, session_id: str) -> None:
        if command.worker_id != self.worker_id:
            return
        self.data_store.release(command.data, session_id=session_id)
        await self._emit(p.ObjectReleased(
            worker_id=self.worker_id, data=command.data,
            message_id=new_transport_id("object-released"), correlation_id=command.message_id,
        ))

    def _immutable_data_ref(self, plan, run_id: str, value_id: str) -> p.DataReference:
        return p.DataReference(
            plan.id, run_id, plan.immutable_representation_id(value_id),
            DataForm.IMMUTABLE_VALUE,
        )

    @staticmethod
    def _snapshot_data_ref(plan, run_id: str, value_id: str, object_state_id: str | None) -> p.DataReference:
        return p.DataReference(
            plan.id, run_id, value_id, DataForm.OBJECT_SNAPSHOT, object_state_id
        )

    @staticmethod
    def _entry_descriptor(entry) -> dict[str, object]:
        return {
            "path": str(entry.path), "size_bytes": entry.size_bytes,
            "sha256": entry.sha256, "serialization": entry.serialization,
        }

    def _definitions_for(self, plan, manifest) -> list[str]:
        # FIXES F30 discrepancy: this implementation iterated direct inputs,
        # not manifest.code.definition_ids as the guide assumed.
        definitions: list[str] = []
        for ident in manifest.code.definition_ids:
            definition = plan.definition_index.get(ident)
            if definition is None:
                raise ValueError("required code definition is missing")
            definitions.append(definition.source)

        for req in manifest.inputs:
            if req.kind is ValueKind.CODE_BINDING and req.value.definition_id is not None:
                name = plan.definition_index[req.value.definition_id].name
                if req.value.name != name:
                    definitions.append(f"{req.value.name} = {name}")
        return definitions

    @staticmethod
    def _publication_failure(error: BaseException, prefix: str) -> FailureInfo:
        resource = isinstance(error, DataStoreFull) or (
            isinstance(error, OSError) and error.errno in {errno.ENOSPC, errno.EDQUOT, errno.EFBIG}
        )
        kind = FailureKind.RESOURCE_EXHAUSTED if resource else FailureKind.EXECUTION_ERROR
        return FailureInfo(kind, f"{prefix}: {error}")

    @staticmethod
    def _context_bindings_for(plan, manifest):
        events = []
        for event in manifest.binding_events:
            value = plan.value_index[event.value_id]
            if event.kind in {"definition", "task_definition"}:
                events.append({"id": event.value_id, "kind": "definition", "name": event.name,
                               "source": plan.definition_index[value.definition_id].source})
            elif event.kind == "runtime_task_import":
                events.append({"id": event.value_id, "kind": "runtime_task_import", "name": event.name})
            elif event.kind == "alias":
                # The binding task contains only a simple name-to-name assignment.
                import ast
                source = ast.parse(manifest.task.source).body[0].value.id
                events.append({"id": event.value_id, "kind": "alias", "name": event.name,
                               "source_name": source})
        return events

    def _snapshot_requirement_ref(self, plan, run_id: str, manifest, requirement) -> p.DataReference:
        matches = [obj for obj in manifest.objects if requirement.id in obj.input_ids]
        if len(matches) != 1 or matches[0].access is not ObjectAccess.SNAPSHOT_CANDIDATE:
            raise ValueError("shared reference is not a transferable snapshot candidate")
        obj = matches[0]
        if len(obj.state_inputs) > 1:
            raise ValueError("ambiguous object-state snapshot requirement")
        return self._snapshot_data_ref(plan, run_id, requirement.id, next(iter(obj.state_inputs), None))

    async def _dispatch(self, dispatch: p.TaskDispatch, session_id: str) -> None:
        if dispatch.worker_id != self.worker_id:
            return
        plan = self._plans.get((dispatch.program_id, dispatch.attempt.plan_id))
        if plan is None:
            await self._reject(dispatch, p.RejectionCode.PROGRAM_UNAVAILABLE, "exact program/plan is not prepared")
            return
        manifest = plan.task_index.get(dispatch.attempt.task_id)
        if manifest is None:
            await self._reject(dispatch, p.RejectionCode.INVALID_REQUEST, "task manifest is missing")
            return
        if manifest.mode is not dispatch.mode:
            await self._reject(dispatch, p.RejectionCode.MODE_UNSUPPORTED, "task dispatch mode differs from the execution plan")
            return
        program = self._programs[dispatch.program_id]
        if program.package_id is None or await asyncio.to_thread(self.cache.verify, program.package_id) is None:
            self._prepared_program_ids.discard(program.id)
            self._plans.pop((program.id, plan.id), None)
            await self._emit(p.ProgramUnavailable(
                worker_id=self.worker_id, program_id=program.id,
                reason="package cache integrity verification failed",
                message_id=new_transport_id("program-unavailable"),
            ))
            await self._reject(dispatch, p.RejectionCode.PROGRAM_UNAVAILABLE, "package cache is missing or corrupt")
            return
        attempt_id = dispatch.attempt.attempt_id
        if attempt_id in self._active or attempt_id in self._active_context or attempt_id in self._terminal_attempts:
            await self._reject(dispatch, p.RejectionCode.STALE_ATTEMPT, "duplicate/stale attempt identity")
            return
        if len(self._active) + len(self._active_context) >= self.base_state.total_slots:
            await self._reject(dispatch, p.RejectionCode.BUSY, "worker execution capacity is full")
            return

        # Any coordinator-selected physical context is authoritative placement.
        # Even an otherwise-isolated candidate may be assigned to the context so
        # that it creates/observes the same live object state as later native work.
        if dispatch.context_id is not None:
            context = self._context_for_dispatch(dispatch)
            if context is None or context.session_id != session_id or context.retired:
                await self._reject(dispatch, p.RejectionCode.CONTEXT_UNAVAILABLE, "exact physical context is unavailable")
                return
            if dispatch.attempt.task_id not in context.command.task_ids:
                await self._reject(dispatch, p.RejectionCode.INVALID_REQUEST, "task is not prepared in this context")
                return
            if context.process.returncode is not None:
                await self._retire_context(context, notify=True, reason="context process exited")
                await self._reject(dispatch, p.RejectionCode.CONTEXT_UNAVAILABLE, "context process is not alive")
                return
            if context.active_attempt_id is not None:
                await self._reject(dispatch, p.RejectionCode.BUSY, "context execution capacity is full")
                return
            active = _ActiveContextExecution(
                dispatch=dispatch, session_id=session_id, plan=plan,
                context_id=dispatch.context_id,
            )
            self._active_context[attempt_id] = active
            context.active_attempt_id = attempt_id
            active.task = asyncio.create_task(self._run_context_attempt(active, context, manifest))
            return

        if dispatch.mode is not ExecutionMode.ISOLATED_CANDIDATE:
            await self._reject(
                dispatch, p.RejectionCode.CONTEXT_UNAVAILABLE,
                "shared/native execution requires an exact prepared physical context",
            )
            return

        inputs: dict[str, object] = {}
        try:
            definitions = self._definitions_for(plan, manifest)
            for req in manifest.inputs:
                if req.kind is ValueKind.CODE_BINDING or req.is_state_token:
                    continue
                if req.kind is ValueKind.IMMUTABLE:
                    ref = self._immutable_data_ref(plan, dispatch.attempt.run_id, req.id)
                    entry = self.data_store.get(ref, session_id=session_id)
                    if entry is None:
                        raise LookupError("required immutable input is not local")
                    inputs[req.value.name] = self._entry_descriptor(entry)
                    continue
                if req.kind is ValueKind.SHARED_REFERENCE:
                    ref = self._snapshot_requirement_ref(
                        plan, dispatch.attempt.run_id, manifest, req
                    )
                    entry = self.data_store.get(ref, session_id=session_id)
                    if entry is None:
                        raise LookupError("required object snapshot is not local")
                    inputs[req.value.name] = self._entry_descriptor(entry)
                    continue
                raise LookupError("required input is not transferable to isolated execution")
        except ValueError as error:
            await self._reject(dispatch, p.RejectionCode.INVALID_REQUEST, str(error))
            return
        except LookupError as error:
            await self._reject(dispatch, p.RejectionCode.INPUT_UNAVAILABLE, str(error))
            return

        active = _ActiveExecution(dispatch=dispatch, session_id=session_id, plan=plan)
        self._active[attempt_id] = active
        active.task = asyncio.create_task(self._run_attempt(active, manifest, inputs, definitions))

    async def _run_context_attempt(self, active: _ActiveContextExecution, context: _WorkerContextRuntime, manifest) -> None:
        dispatch = active.dispatch
        attempt_id = dispatch.attempt.attempt_id
        workdir: Path | None = None
        outcome = "internal_error"
        try:
            if active.cancel_message is not None:
                await self._cancel_context_active(active, context, active.cancel_message)
                outcome = "cancelled_before_start"
                return
            if self._session_id != active.session_id or context.session_id != active.session_id:
                return
            program = self._programs[dispatch.program_id]
            assert program.package_id is not None
            package_root = context.package_root
            workdir = self._make_child_workdir("dpr-context-task-")
            request_path = workdir / "request.json"
            result_path = workdir / "result.json"

            try:
                definitions = self._definitions_for(active.plan, manifest)
            except ValueError as error:
                await self._context_task_failed(active, FailureInfo(FailureKind.EXECUTION_ERROR, str(error)))
                outcome = "invalid_definition"
                return

            # F58: a persistent context owns values produced by earlier native
            # tasks, but isolated ancestors may have run on a different worker.
            # Materialize the latest still-live transferable binding per name from
            # the authenticated local data store after scheduler/coordinator transfer.
            seed_requirements = active.plan.context_seed_requirements(manifest.task_id)
            inputs: dict[str, object] = {}
            for req in seed_requirements:
                if req.kind is ValueKind.IMMUTABLE:
                    ref = self._immutable_data_ref(active.plan, dispatch.attempt.run_id, req.id)
                elif req.kind is ValueKind.SHARED_REFERENCE:
                    ref = self._snapshot_data_ref(
                        active.plan, dispatch.attempt.run_id, req.id, None
                    )
                else:  # context_seed_requirements is intentionally transferable-only
                    continue
                entry = self.data_store.get(ref, session_id=active.session_id)
                if entry is None:
                    await self._context_task_failed(
                        active, FailureInfo(
                            FailureKind.INPUT_UNAVAILABLE,
                            f"required context seed is not local: {req.id}",
                        )
                    )
                    outcome = "input_unavailable"
                    return
                inputs[req.value.name] = self._child_entry_descriptor(entry, workdir, len(inputs))

            output_refs: dict[str, p.DataReference] = {}
            outputs: list[dict[str, str]] = []
            for req in manifest.outputs:
                if req.id not in manifest.reported_output_ids:
                    continue
                ref: p.DataReference | None = None
                if req.kind is ValueKind.IMMUTABLE:
                    ref = self._immutable_data_ref(active.plan, dispatch.attempt.run_id, req.id)
                elif req.kind is ValueKind.SHARED_REFERENCE:
                    ref = self._snapshot_data_ref(active.plan, dispatch.attempt.run_id, req.id, None)
                if ref is None:
                    continue
                if not req.value.name.isidentifier():
                    await self._context_task_failed(
                        active, FailureInfo(FailureKind.EXECUTION_ERROR, "transferable context output has invalid binding name")
                    )
                    outcome = "invalid_output"
                    return
                path = workdir / f"output-{len(outputs)}.bin"
                output_refs[req.id] = ref
                outputs.append({"value_id": req.id, "name": req.value.name, "path": str(path)})

            snapshots: list[dict[str, str]] = []
            snapshot_refs: dict[tuple[str, str], p.DataReference] = {}
            for obj in manifest.objects:
                if not obj.state_outputs:
                    continue
                if not obj.input_ids:
                    await self._context_task_failed(
                        active, FailureInfo(FailureKind.EXECUTION_ERROR, "object-state output has no live reference input")
                    )
                    outcome = "invalid_snapshot"
                    return
                reference_id = obj.input_ids[0]
                reference = active.plan.value_index.get(reference_id)
                if reference is None or not reference.name.isidentifier():
                    await self._context_task_failed(
                        active, FailureInfo(FailureKind.EXECUTION_ERROR, "object-state live reference is invalid")
                    )
                    outcome = "invalid_snapshot"
                    return
                for state_id in obj.state_outputs:
                    ref = self._snapshot_data_ref(
                        active.plan, dispatch.attempt.run_id, reference_id, state_id
                    )
                    path = workdir / f"snapshot-{len(snapshots)}.bin"
                    snapshot_refs[(reference_id, state_id)] = ref
                    snapshots.append({
                        "value_id": reference_id, "object_state_id": state_id,
                        "name": reference.name, "path": str(path),
                    })

            request = {
                "package_root": str(package_root), "source": manifest.task.source,
                "definitions": [], "inputs": inputs, "outputs": outputs,
                "binding_events": self._context_bindings_for(active.plan, manifest),
                "filename": active.plan.program.filename,
                "input_ids": {
                    r.value.name: r.id for r in seed_requirements if r.value.name in inputs
                },
                "snapshots": snapshots, "result_path": str(result_path),
                "result_limit": self.limits.result_bytes,
                "value_limit": self.data_store.limits.max_value_bytes,
                "stdout_limit": self.limits.stdout_bytes,
                "stderr_limit": self.limits.stderr_bytes,
            }
            raw = json.dumps(request, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
            if len(raw) > self.limits.request_bytes:
                await self._context_task_failed(
                    active, FailureInfo(FailureKind.EXECUTION_ERROR, "context executor request exceeds configured bound")
                )
                outcome = "request_too_large"
                return
            self._publish_child_request(request_path, raw)

            # Request publication into the persistent child precedes Accepted/Started.
            # F53 keeps this control channel separate from the user's sys.stdin.
            control_message = (str(request_path) + "\n").encode("utf-8")
            try:
                if context.control_fd is not None:
                    await asyncio.to_thread(os.write, context.control_fd, control_message)
                elif context.control_stream is not None:
                    context.control_stream.write(control_message)
                    await context.control_stream.drain()
                else:
                    raise BrokenPipeError("context control channel is closed")
            except (BrokenPipeError, ConnectionError, OSError) as error:
                await self._context_task_failed(
                    active, FailureInfo(FailureKind.EXECUTION_ERROR, f"context IPC failed before start: {error}")
                )
                await self._retire_context(context, notify=True, reason="context IPC failed")
                outcome = "context_crash"
                return

            if active.cancel_message is not None:
                await self._cancel_context_active(active, context, active.cancel_message)
                outcome = "cancelled_during_start"
                return
            await self._emit(p.TaskAccepted(
                worker_id=self.worker_id, attempt=dispatch.attempt,
                message_id=new_transport_id("task-accepted"), correlation_id=dispatch.message_id,
            ))
            await self._emit(p.TaskStarted(
                worker_id=self.worker_id, attempt=dispatch.attempt,
                message_id=new_transport_id("task-started"), correlation_id=dispatch.message_id,
            ))

            deadline = None if self.limits.task_timeout_seconds is None else self._clock() + self.limits.task_timeout_seconds
            while not result_path.is_file():
                if active.cancel_message is not None:
                    await self._cancel_context_active(active, context, active.cancel_message)
                    outcome = "cancelled"
                    return
                if context.process.returncode is not None:
                    await self._context_task_failed(
                        active, FailureInfo(FailureKind.EXECUTION_ERROR, f"context process exited during task (exit={context.process.returncode})")
                    )
                    await self._retire_context(context, notify=True, reason="context process crashed during task")
                    outcome = "context_crash"
                    return
                if self._session_id != active.session_id:
                    return
                if deadline is not None and self._clock() >= deadline:
                    await self._context_task_failed(
                        active, FailureInfo(FailureKind.EXECUTION_ERROR, "context execution deadline exceeded")
                    )
                    await self._retire_context(context, notify=True, reason="context task timed out")
                    outcome = "timeout"
                    return
                await asyncio.sleep(self.limits.context_result_poll_seconds)

            if result_path.stat().st_size > self.limits.result_bytes:
                await self._context_task_failed(active, FailureInfo(FailureKind.EXECUTION_ERROR, "context child result exceeded configured bound"))
                await self._retire_context(context, notify=True, reason="malformed context result")
                outcome = "malformed_result"
                return
            try:
                result = json.loads(result_path.read_text("utf-8"), object_pairs_hook=_unique_json_object)
            except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError, RecursionError):
                await self._context_task_failed(active, FailureInfo(FailureKind.EXECUTION_ERROR, "context child returned malformed result metadata"))
                await self._retire_context(context, notify=True, reason="malformed context result")
                outcome = "malformed_result"
                return
            if type(result) is not dict or "status" not in result:
                await self._context_task_failed(active, FailureInfo(FailureKind.EXECUTION_ERROR, "context child returned invalid result schema"))
                await self._retire_context(context, notify=True, reason="invalid context result")
                outcome = "malformed_result"
                return
            result_stdout = result.get("stdout", "") if type(result.get("stdout", "")) is str else ""
            result_stderr = result.get("stderr", "") if type(result.get("stderr", "")) is str else ""
            result_stdout_truncated = bool(result.get("stdout_truncated", False))
            result_stderr_truncated = bool(result.get("stderr_truncated", False))
            if result["status"] == "python_exception":
                exc_type, message, tb = result.get("exception_type"), result.get("message"), result.get("traceback")
                if not all(type(x) is str for x in (exc_type, message, tb)):
                    failure = FailureInfo(FailureKind.EXECUTION_ERROR, "context exception metadata was malformed")
                else:
                    failure = FailureInfo(
                        FailureKind.PYTHON_EXCEPTION,
                        self._bound_text(message, p.MAX_DETAIL_BYTES),
                        self._bound_text(exc_type, p.MAX_IDENTIFIER_BYTES),
                        self._bound_text(tb, p.MAX_TRACEBACK_BYTES),
                    )
                await self._context_task_failed(
                    active, failure, stdout_tail=result_stdout, stderr_tail=result_stderr,
                    stdout_truncated=result_stdout_truncated, stderr_truncated=result_stderr_truncated,
                )
                # Python exceptions can leave arbitrary mutable state changed before
                # raising.  Retire the whole context; coordinator then applies its
                # existing native-state-uncertain no-replay policy.
                await self._retire_context(context, notify=True, reason="context task raised")
                outcome = "python_exception"
                return
            if (result.get("status") != "success" or type(result.get("clean_exit")) is not bool
                    or type(result.get("outputs")) is not dict or type(result.get("snapshots")) is not list):
                await self._context_task_failed(active, FailureInfo(FailureKind.EXECUTION_ERROR, "context child reported malformed success metadata"))
                await self._retire_context(context, notify=True, reason="malformed context success")
                outcome = "malformed_result"
                return
            if set(result["outputs"]) != set(output_refs):
                await self._context_task_failed(active, FailureInfo(FailureKind.EXECUTION_ERROR, "context output identities differ from manifest"))
                await self._retire_context(context, notify=True, reason="context output identity mismatch")
                outcome = "malformed_result"
                return
            returned_snapshots: dict[tuple[str, str], dict[str, object]] = {}
            for item in result["snapshots"]:
                if type(item) is not dict or set(item) != {"value_id", "object_state_id", "size_bytes", "sha256", "serialization"}:
                    await self._context_task_failed(active, FailureInfo(FailureKind.EXECUTION_ERROR, "context snapshot metadata is malformed"))
                    await self._retire_context(context, notify=True, reason="malformed context snapshot")
                    outcome = "malformed_result"
                    return
                returned_snapshots[(item["value_id"], item["object_state_id"])] = item
            if set(returned_snapshots) != set(snapshot_refs):
                await self._context_task_failed(active, FailureInfo(FailureKind.EXECUTION_ERROR, "context snapshot identities differ from manifest"))
                await self._retire_context(context, notify=True, reason="context snapshot identity mismatch")
                outcome = "malformed_result"
                return

            # Stage and verify all physical publications before exposing TaskSucceeded.
            published: list[tuple[p.DataReference, int]] = []
            newly_published: list[p.DataReference] = []
            try:
                output_paths = {item["value_id"]: Path(item["path"]) for item in outputs}
                for value_id, ref in output_refs.items():
                    metadata = result["outputs"][value_id]
                    if type(metadata) is not dict or set(metadata) != {"size_bytes", "sha256", "serialization"}:
                        raise DataStoreIntegrityError("context output metadata is malformed")
                    existed = self.data_store.get(ref, session_id=active.session_id) is not None
                    entry = await asyncio.to_thread(
                        self.data_store.publish_file, ref, output_paths[value_id],
                        size_bytes=metadata["size_bytes"], sha256=metadata["sha256"],
                        session_id=active.session_id, serialization=metadata["serialization"],
                    )
                    if not existed:
                        newly_published.append(ref)
                    published.append((ref, entry.size_bytes))
                snapshot_paths = {
                    (item["value_id"], item["object_state_id"]): Path(item["path"])
                    for item in snapshots
                }
                for key, ref in snapshot_refs.items():
                    metadata = returned_snapshots[key]
                    existed = self.data_store.get(ref, session_id=active.session_id) is not None
                    entry = await asyncio.to_thread(
                        self.data_store.publish_file, ref, snapshot_paths[key],
                        size_bytes=metadata["size_bytes"], sha256=metadata["sha256"],
                        session_id=active.session_id, serialization=metadata["serialization"],
                    )
                    if not existed:
                        newly_published.append(ref)
                    published.append((ref, entry.size_bytes))
            except (DataStoreError, OSError, ValueError, TypeError) as error:
                for ref in newly_published:
                    self.data_store.release(ref, session_id=active.session_id)
                failure = self._publication_failure(error, "context output publication failed")
                await self._context_task_failed(
                    active, FailureInfo(failure.kind, self._bound_text(failure.message, p.MAX_DETAIL_BYTES)),
                    stdout_tail=result_stdout, stderr_tail=result_stderr,
                    stdout_truncated=result_stdout_truncated, stderr_truncated=result_stderr_truncated,
                )
                await self._retire_context(context, notify=True, reason="context output publication failed")
                outcome = "publication_failed"
                return

            if active.cancel_message is not None or active.terminal or self._session_id != active.session_id:
                for ref in newly_published:
                    self.data_store.release(ref, session_id=active.session_id)
                outcome = "cancelled_race"
                return
            active.terminal = True
            await self._emit(p.TaskSucceeded(
                worker_id=self.worker_id,
                result=TaskSuccess(
                    dispatch.attempt, manifest.reported_output_ids, clean_exit=result["clean_exit"],
                    stdout_tail=self._bound_text(result_stdout, p.MAX_DETAIL_BYTES),
                    stderr_tail=self._bound_text(result_stderr, p.MAX_DETAIL_BYTES),
                    stdout_truncated=result_stdout_truncated,
                    stderr_truncated=result_stderr_truncated,
                ),
                message_id=new_transport_id("task-succeeded"), correlation_id=dispatch.message_id,
            ))
            # Control-stream ordering guarantees coordinator commit is observed before
            # these availability refinements/snapshot claims.
            for ref, size in published:
                await self._emit(p.ObjectAvailable(
                    worker_id=self.worker_id, data=ref, size_bytes=size,
                    message_id=new_transport_id("object-available"),
                ))
            context.completed_task_ids.add(dispatch.attempt.task_id)
            if context.completed_task_ids.issuperset(context.command.task_ids):
                await self._retire_context(
                    context, notify=True, reason="prepared context work completed"
                )
            outcome = "success"
            self._remember_diagnostics(ExecutionDiagnostics(
                attempt_id, result_stdout, result_stderr,
                bool(result.get("stdout_truncated", False)), bool(result.get("stderr_truncated", False)),
                context.process.returncode, outcome,
            ))
        except asyncio.CancelledError:
            # Session loss owns context retirement; never rebind/re-report here.
            raise
        except Exception as error:
            if self._session_id == active.session_id and not active.terminal:
                with suppress(Exception):
                    await self._context_task_failed(
                        active, FailureInfo(FailureKind.EXECUTION_ERROR, self._bound_text(f"worker context executor failure: {error}", p.MAX_DETAIL_BYTES))
                    )
                with suppress(Exception):
                    await self._retire_context(context, notify=True, reason="worker context executor failure")
            outcome = "internal_error"
        finally:
            if workdir is not None:
                await asyncio.to_thread(__import__("shutil").rmtree, workdir, True)
            self._remember_terminal_attempt(attempt_id)
            self._active_context.pop(attempt_id, None)
            if context.active_attempt_id == attempt_id:
                context.active_attempt_id = None
            if attempt_id not in self._diagnostics:
                self._remember_diagnostics(ExecutionDiagnostics(
                    attempt_id, "", "", False, False, context.process.returncode, outcome
                ))

    async def _context_task_failed(
        self, active: _ActiveContextExecution, failure: FailureInfo, *,
        stdout_tail: str = "", stderr_tail: str = "",
        stdout_truncated: bool = False, stderr_truncated: bool = False,
    ) -> None:
        if active.terminal or self._session_id != active.session_id:
            return
        active.terminal = True
        await self._emit(p.TaskFailed(
            worker_id=self.worker_id, result=TaskFailure(
                active.dispatch.attempt, failure,
                self._bound_text(stdout_tail, p.MAX_DETAIL_BYTES),
                self._bound_text(stderr_tail, p.MAX_DETAIL_BYTES),
                stdout_truncated, stderr_truncated,
            ),
            message_id=new_transport_id("task-failed"), correlation_id=active.dispatch.message_id,
        ))

    async def _cancel_context_active(
        self, active: _ActiveContextExecution, context: _WorkerContextRuntime, command: p.CancelTask
    ) -> bool:
        if active.cancel_message is None:
            active.cancel_message = command
        cleaned = await self._retire_context(
            context, notify=False, reason="context task cancelled"
        )
        if not cleaned:
            return False
        if not active.terminal and self._session_id == active.session_id:
            active.terminal = True
            self._remember_terminal_attempt(active.dispatch.attempt.attempt_id)
            await self._emit(p.TaskCancellationResult(
                worker_id=self.worker_id, attempt=active.dispatch.attempt,
                outcome=p.CancellationOutcome.CANCELLED,
                detail="persistent context terminated and reaped",
                message_id=new_transport_id("task-cancel-result"),
                correlation_id=command.message_id,
            ))
            await self._emit(p.ContextUnavailable(
                worker_id=self.worker_id, plan_id=active.dispatch.attempt.plan_id,
                run_id=active.dispatch.attempt.run_id, context_id=active.context_id,
                reason="context retired after task cancellation",
                message_id=new_transport_id("context-unavailable"),
            ))
        return True

    async def _reject(self, dispatch: p.TaskDispatch, code: p.RejectionCode, detail: str) -> None:
        await self._emit(p.TaskRejected(
            worker_id=self.worker_id, attempt=dispatch.attempt, code=code,
            detail=self._bound_text(detail, p.MAX_DETAIL_BYTES), message_id=new_transport_id("task-rejected"),
            correlation_id=dispatch.message_id,
        ))

    async def _run_attempt(self, active: _ActiveExecution, manifest, inputs: dict[str, object], definitions: list[str]) -> None:
        dispatch = active.dispatch
        attempt_id = dispatch.attempt.attempt_id
        workdir = None
        stdout_task = stderr_task = None
        outcome = "internal_error"
        exit_code = None
        captured_stdout = b""
        captured_stderr = b""
        stdout_trunc = False
        stderr_trunc = False
        try:
            if active.cancel_message is not None:
                await self._report_cancelled(active)
                outcome = "cancelled_before_start"
                return
            program = self._programs[dispatch.program_id]
            assert program.package_id is not None
            original_package_root = self.cache.content_path(program.package_id)
            workdir = self._make_child_workdir("dpr-exec-")
            package_root = self._copy_package_view(original_package_root, workdir)
            request_path = workdir / "request.json"
            result_path = workdir / "result.json"
            outputs = [[req.id, req.value.name] for req in manifest.outputs if req.id in manifest.reported_output_ids]
            if self._child_uid is not None:
                localized: dict[str, object] = {}
                for index, (name, descriptor) in enumerate(inputs.items()):
                    class _EntryProxy:
                        pass
                    proxy = _EntryProxy()
                    proxy.path = Path(descriptor["path"])
                    proxy.size_bytes = descriptor["size_bytes"]
                    proxy.sha256 = descriptor["sha256"]
                    proxy.serialization = descriptor["serialization"]
                    localized[name] = self._child_entry_descriptor(proxy, workdir, index)
                inputs = localized
            request = {
                "package_root": str(package_root),
                "source": manifest.task.source,
                "filename": active.plan.program.filename,
                "definitions": definitions,
                "inputs": inputs,
                "outputs": outputs,
                "result_limit": self.limits.result_bytes,
                "value_limit": self.data_store.limits.max_value_bytes,
            }
            request_bytes = json.dumps(request, sort_keys=True, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
            if len(request_bytes) > self.limits.request_bytes:
                await self._reject_and_retire(active, p.RejectionCode.INVALID_REQUEST, "executor request exceeds configured bound")
                outcome = "request_too_large"
                return
            self._publish_child_request(request_path, request_bytes)
            child = Path(__file__).with_name("isolated_child.py")
            kwargs = self._child_spawn_kwargs()
            try:
                process = await asyncio.create_subprocess_exec(
                    sys.executable, "-I", "-B", str(child), str(request_path), str(result_path),
                    cwd=str(package_root), stdin=asyncio.subprocess.DEVNULL,
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, **kwargs,
                )
                self._attach_windows_job(process.pid)
            except (OSError, ValueError) as error:
                await self._reject_and_retire(active, p.RejectionCode.BUSY, f"cannot create isolated child: {error}")
                outcome = "spawn_failed"
                return
            active.process = process
            if active.cancel_message is not None or active.terminal:
                await self._terminate(active)
                if active.cancel_message is not None:
                    await self._report_cancelled(active)
                outcome = "cancelled_during_start"
                return
            accepted = p.TaskAccepted(
                worker_id=self.worker_id, attempt=dispatch.attempt,
                message_id=new_transport_id("task-accepted"), correlation_id=dispatch.message_id,
            )
            started = p.TaskStarted(
                worker_id=self.worker_id, attempt=dispatch.attempt,
                message_id=new_transport_id("task-started"), correlation_id=dispatch.message_id,
            )
            await self._emit(accepted)
            await self._emit(started)
            stdout_task = asyncio.create_task(self._drain(process.stdout, self.limits.stdout_bytes))
            stderr_task = asyncio.create_task(self._drain(process.stderr, self.limits.stderr_bytes))
            timed_out = False
            try:
                if self.limits.task_timeout_seconds is None:
                    await process.wait()
                else:
                    await asyncio.wait_for(process.wait(), self.limits.task_timeout_seconds)
            except asyncio.TimeoutError:
                timed_out = True
                await self._terminate(active)
            exit_code = process.returncode
            if exit_code is not None:
                self._close_windows_job(process.pid, terminate=False)
            captured_stdout, stdout_trunc = await stdout_task
            captured_stderr, stderr_trunc = await stderr_task
            stdout_task = stderr_task = None
            stdout_text = self._bound_text(captured_stdout.decode("utf-8", "replace"), p.MAX_DETAIL_BYTES)
            stderr_text = self._bound_text(captured_stderr.decode("utf-8", "replace"), p.MAX_DETAIL_BYTES)
            if active.cancel_message is not None:
                await self._report_cancelled(active)
                outcome = "cancelled"
                return
            if timed_out:
                await self._task_failed(active, FailureInfo(FailureKind.EXECUTION_ERROR, "isolated execution deadline exceeded"))
                outcome = "timeout"
                return
            if not result_path.is_file():
                await self._task_failed(active, FailureInfo(FailureKind.EXECUTION_ERROR, f"isolated child exited without a valid result (exit={exit_code})"))
                outcome = "child_crash"
                return
            if result_path.stat().st_size > self.limits.result_bytes:
                await self._task_failed(active, FailureInfo(FailureKind.RESOURCE_EXHAUSTED, "isolated child result exceeded configured bound"))
                outcome = "malformed_result"
                return
            try:
                result = json.loads(
                    result_path.read_text("utf-8"), object_pairs_hook=_unique_json_object
                )
            except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError, RecursionError):
                await self._task_failed(active, FailureInfo(FailureKind.EXECUTION_ERROR, "isolated child returned malformed result metadata"))
                outcome = "malformed_result"
                return
            if type(result) is not dict or "status" not in result:
                await self._task_failed(active, FailureInfo(FailureKind.EXECUTION_ERROR, "isolated child returned invalid result schema"))
                outcome = "malformed_result"
                return
            if result["status"] == "python_exception":
                exc_type = result.get("exception_type")
                message = result.get("message")
                tb = result.get("traceback")
                if not all(type(x) is str for x in (exc_type, message, tb)):
                    await self._task_failed(active, FailureInfo(FailureKind.EXECUTION_ERROR, "isolated exception metadata was malformed"))
                    outcome = "malformed_result"
                    return
                await self._task_failed(active, FailureInfo(
                    FailureKind.PYTHON_EXCEPTION, self._bound_text(message, p.MAX_DETAIL_BYTES),
                    self._bound_text(exc_type, p.MAX_IDENTIFIER_BYTES), self._bound_text(tb, p.MAX_TRACEBACK_BYTES),
                ), stdout_tail=stdout_text, stderr_tail=stderr_text,
                   stdout_truncated=stdout_trunc, stderr_truncated=stderr_trunc)
                outcome = "python_exception"
                return
            if (result.get("status") == "internal_error"
                    and result.get("message") == "executor result exceeded configured bound"):
                # F11: this is a deterministic local resource bound, not a transient
                # executor error. RESOURCE_EXHAUSTED is outside the automatic retry set.
                await self._task_failed(active, FailureInfo(
                    FailureKind.RESOURCE_EXHAUSTED,
                    "isolated child result exceeded configured bound",
                ))
                outcome = "resource_exhausted"
                return
            if (result["status"] != "success" or set(result) != {"status", "clean_exit", "outputs"}
                    or type(result["clean_exit"]) is not bool or type(result["outputs"]) is not dict):
                await self._task_failed(active, FailureInfo(FailureKind.EXECUTION_ERROR, "isolated child reported internal/malformed result"))
                outcome = "malformed_result"
                return
            expected = set(manifest.reported_output_ids)
            if set(result["outputs"]) != expected:
                await self._task_failed(active, FailureInfo(FailureKind.EXECUTION_ERROR, "isolated child output identities differ from manifest"))
                outcome = "malformed_result"
                return
            # Values remain files: the controller validates metadata and digest,
            # and never unpickles user objects.
            published: list[tuple[p.DataReference, int]] = []
            newly_published: list[p.DataReference] = []
            by_id = {req.id: req for req in manifest.outputs}
            try:
                for value_id, metadata in result["outputs"].items():
                    requirement = by_id[value_id]
                    if requirement.kind not in {ValueKind.IMMUTABLE, ValueKind.SHARED_REFERENCE}:
                        raise DataStoreIntegrityError("isolated output requires a persistent context")
                    if (type(metadata) is not dict
                            or set(metadata) != {"file", "size_bytes", "sha256", "serialization"}
                            or metadata["file"] != f"output-{value_id}.bin"
                            or metadata["serialization"] != "pickle-v1"):
                        raise DataStoreIntegrityError("isolated output descriptor is malformed")
                    ref = (self._immutable_data_ref(active.plan, dispatch.attempt.run_id, value_id)
                           if requirement.kind is ValueKind.IMMUTABLE else
                           self._snapshot_data_ref(active.plan, dispatch.attempt.run_id, value_id, None))
                    existed = self.data_store.get(ref, session_id=active.session_id) is not None
                    entry = await asyncio.to_thread(
                        self.data_store.publish_file, ref, workdir / metadata["file"],
                        size_bytes=metadata["size_bytes"], sha256=metadata["sha256"],
                        session_id=active.session_id, serialization=metadata["serialization"],
                    )
                    if not existed:
                        newly_published.append(ref)
                    published.append((ref, entry.size_bytes))
            except (DataStoreError, OSError) as error:
                for ref in newly_published:
                    self.data_store.release(ref, session_id=active.session_id)
                failure = self._publication_failure(error, "worker-local output publication failed")
                await self._task_failed(active, FailureInfo(
                    failure.kind, self._bound_text(failure.message, p.MAX_DETAIL_BYTES),
                ), stdout_tail=stdout_text, stderr_tail=stderr_text,
                   stdout_truncated=stdout_trunc, stderr_truncated=stderr_trunc)
                outcome = "value_store_full"
                return
            if active.cancel_message is not None or active.terminal or self._session_id != active.session_id:
                for ref in newly_published:
                    self.data_store.release(ref, session_id=active.session_id)
                outcome = "cancelled_race"
                return
            success = p.TaskSucceeded(
                worker_id=self.worker_id, result=TaskSuccess(
                    dispatch.attempt, manifest.reported_output_ids, clean_exit=result["clean_exit"],
                    stdout_tail=stdout_text, stderr_tail=stderr_text,
                    stdout_truncated=stdout_trunc, stderr_truncated=stderr_trunc,
                ),
                message_id=new_transport_id("task-succeeded"), correlation_id=dispatch.message_id,
            )
            active.terminal = True
            await self._emit(success)
            for ref, size in published:
                await self._emit(p.ObjectAvailable(
                    worker_id=self.worker_id, data=ref, size_bytes=size,
                    message_id=new_transport_id("object-available"),
                ))
            outcome = "success"
        except asyncio.CancelledError:
            await self._terminate(active)
            outcome = "session_lost"
            raise
        except Exception as error:
            if self._session_id == active.session_id:
                with suppress(Exception):
                    await self._task_failed(active, FailureInfo(FailureKind.EXECUTION_ERROR, self._bound_text(f"worker executor failure: {error}", p.MAX_DETAIL_BYTES)))
            outcome = "internal_error"
        finally:
            if stdout_task is not None: stdout_task.cancel()
            if stderr_task is not None: stderr_task.cancel()
            if stdout_task is not None or stderr_task is not None:
                await asyncio.gather(*(t for t in (stdout_task, stderr_task) if t is not None), return_exceptions=True)
            if active.process is not None and active.process.returncode is None:
                cleaned = await self._terminate(active)
                if not cleaned and active.process.returncode is None:
                    # Do not publish physical cleanup merely because terminate/kill was
                    # requested.  This background execution task remains the reaper and
                    # therefore retains _active capacity until the OS proves child exit.
                    await active.process.wait()
            if workdir is not None:
                await asyncio.to_thread(__import__("shutil").rmtree, workdir, True)
            self._remember_terminal_attempt(attempt_id)
            self._active.pop(attempt_id, None)
            # diagnostics are populated by _drain only on the normal wait path below if available
            if attempt_id not in self._diagnostics:
                self._remember_diagnostics(ExecutionDiagnostics(
                    attempt_id, captured_stdout.decode("utf-8", "replace"),
                    captured_stderr.decode("utf-8", "replace"),
                    stdout_trunc, stderr_trunc, exit_code, outcome,
                ))

    async def _reject_and_retire(self, active: _ActiveExecution, code: p.RejectionCode, detail: str) -> None:
        await self._reject(active.dispatch, code, detail)
        active.terminal = True

    async def _task_failed(
        self, active: _ActiveExecution, failure: FailureInfo, *,
        stdout_tail: str = "", stderr_tail: str = "",
        stdout_truncated: bool = False, stderr_truncated: bool = False,
    ) -> None:
        if active.terminal or self._session_id != active.session_id:
            return
        active.terminal = True
        await self._emit(p.TaskFailed(
            worker_id=self.worker_id, result=TaskFailure(
                active.dispatch.attempt, failure,
                self._bound_text(stdout_tail, p.MAX_DETAIL_BYTES),
                self._bound_text(stderr_tail, p.MAX_DETAIL_BYTES),
                stdout_truncated, stderr_truncated,
            ),
            message_id=new_transport_id("task-failed"), correlation_id=active.dispatch.message_id,
        ))

    def _handoff_cancel(self, message: p.CancelTask, session_id: str) -> None:
        """Schedule one bounded cancellation job and return to the control reader."""
        attempt_id = message.attempt.attempt_id
        existing = self._cancel_tasks.get(attempt_id)
        if existing is not None and not existing[1].done():
            return
        task = asyncio.create_task(
            self._run_cancel_job(message, session_id),
            name=f"worker-cancel-{attempt_id}",
        )
        self._cancel_tasks[attempt_id] = (session_id, task)

        def _done(done: asyncio.Task[None], *, key: str = attempt_id) -> None:
            current = self._cancel_tasks.get(key)
            if current is not None and current[1] is done:
                self._cancel_tasks.pop(key, None)
            if done.cancelled():
                return
            error = done.exception()
            if error is not None:
                abort = self._abort_session
                if abort is not None:
                    with suppress(Exception):
                        abort(error)

        task.add_done_callback(_done)

    async def _run_cancel_job(self, message: p.CancelTask, session_id: str) -> None:
        async with self._cancel_semaphore:
            if self._session_id != session_id:
                return
            await self._cancel(message)

    async def _cancel(self, message: p.CancelTask) -> None:
        if message.worker_id != self.worker_id:
            return
        context_active = self._active_context.get(message.attempt.attempt_id)
        if context_active is not None and context_active.dispatch.attempt == message.attempt:
            context = self._context_for_dispatch(context_active.dispatch)
            if context is None:
                outcome = p.CancellationOutcome.TOO_LATE
                await self._emit(p.TaskCancellationResult(
                    worker_id=self.worker_id, attempt=message.attempt, outcome=outcome,
                    detail="context is already physically unavailable",
                    message_id=new_transport_id("task-cancel-result"), correlation_id=message.message_id,
                ))
                return
            if context_active.cancel_message is None:
                context_active.cancel_message = message
            await self._cancel_context_active(context_active, context, message)
            return
        active = self._active.get(message.attempt.attempt_id)
        if active is None or active.dispatch.attempt != message.attempt:
            outcome = p.CancellationOutcome.TOO_LATE if message.attempt.attempt_id in self._terminal_attempts else p.CancellationOutcome.NOT_FOUND
            await self._emit(p.TaskCancellationResult(
                worker_id=self.worker_id, attempt=message.attempt, outcome=outcome,
                detail="attempt is not physically active", message_id=new_transport_id("task-cancel-result"),
                correlation_id=message.message_id,
            ))
            return
        if active.cancel_message is None:
            active.cancel_message = message
        cleaned = await self._terminate(active)
        if cleaned:
            await self._report_cancelled(active, command=message)
        # If kill/reap could not be established within the bounded cleanup window,
        # retain this active attempt/capacity. _run_attempt remains the reaper and
        # reports cancellation only when process.wait() eventually proves exit.

    async def _report_cancelled(self, active: _ActiveExecution, command: p.CancelTask | None = None) -> None:
        command = command or active.cancel_message
        if command is None or active.terminal or self._session_id != active.session_id:
            return
        active.terminal = True
        await self._emit(p.TaskCancellationResult(
            worker_id=self.worker_id, attempt=active.dispatch.attempt,
            outcome=p.CancellationOutcome.CANCELLED, detail="isolated child terminated and reaped",
            message_id=new_transport_id("task-cancel-result"), correlation_id=command.message_id,
        ))

    async def _terminate(self, active: _ActiveExecution) -> bool:
        process = active.process
        if process is None or process.returncode is not None:
            return True
        return await self._terminate_child_process(process)

    async def _drain(self, stream: asyncio.StreamReader | None, limit: int) -> tuple[bytes, bool]:
        if stream is None:
            return b"", False
        kept = bytearray()
        truncated = False
        while True:
            chunk = await stream.read(8192)
            if not chunk:
                break
            remaining = limit - len(kept)
            if remaining > 0:
                kept.extend(chunk[:remaining])
            if len(chunk) > remaining:
                truncated = True
        return bytes(kept), truncated

    def _remember_terminal_attempt(self, attempt_id: str) -> None:
        if attempt_id in self._terminal_attempts:
            return
        self._terminal_attempts.add(attempt_id)
        self._terminal_order.append(attempt_id)
        while len(self._terminal_order) > self.limits.terminal_attempt_history:
            old = self._terminal_order.pop(0)
            self._terminal_attempts.discard(old)

    def _remember_diagnostics(self, item: ExecutionDiagnostics) -> None:
        self._diagnostics[item.attempt_id] = item
        while len(self._diagnostics) > self.limits.retained_diagnostics:
            self._diagnostics.pop(next(iter(self._diagnostics)))

    @staticmethod
    def _portable_program_filename(filename: str) -> bool:
        if not isinstance(filename, str) or not filename or "\\" in filename or "\x00" in filename:
            return False
        pure = PurePosixPath(filename)
        return not pure.is_absolute() and all(part not in {"", ".", ".."} for part in pure.parts)

    @staticmethod
    def _bound_text(text: str, byte_limit: int) -> str:
        data = str(text).encode("utf-8", "replace")
        if len(data) <= byte_limit:
            return data.decode("utf-8", "replace")
        marker = b"...[truncated]"
        return (data[:max(0, byte_limit - len(marker))] + marker).decode("utf-8", "ignore")
