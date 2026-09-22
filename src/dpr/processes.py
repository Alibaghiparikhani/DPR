"""Background nodes started by the session, and everything needed to clean up after them.

Each node is a child of the session with a pipe on its stdin, and exits when that pipe
closes.  The session closes it to stop a node, and the operating system closes it when
the session dies for any reason, so nothing outlives the session.  Pid files exist only
to clear a node that was hung when its session vanished.
"""
from __future__ import annotations

import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import sys
import time


# `python -m dpr.node ...` now, `python -m dpr ...` in earlier releases -- and nothing
# that merely starts with those letters.
_DPR_COMMAND = re.compile(r"\s-m\s+dpr(?:\.node)?(?:\s|$)")


class Child:
    def __init__(self, name: str, popen: subprocess.Popen, pid_file: Path) -> None:
        self.name = name
        self.popen = popen
        self.pid_file = pid_file

    @property
    def pid(self) -> int:
        return self.popen.pid

    def alive(self) -> bool:
        return self.popen.poll() is None

    @property
    def exit_code(self) -> int | None:
        return self.popen.poll()

    def send(self, line: str) -> bool:
        """One line down the node's private stdin; False if the node is gone."""
        try:
            self.popen.stdin.write(line.encode("ascii") + b"\n")
            self.popen.stdin.flush()
            return True
        except (OSError, ValueError, AttributeError):
            return False

    def stop(self, timeout: float = 10.0) -> None:
        """Ask the node to shut down cleanly; force it only if it does not."""
        try:
            if self.popen.stdin is not None:
                self.popen.stdin.close()
        except OSError:
            pass
        try:
            self.popen.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            kill_tree(self.pid)
            try:
                self.popen.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass
        self.pid_file.unlink(missing_ok=True)


def spawn(name: str, args: list[str], *, pid_dir: Path, stdin_line: str | None = None) -> Child:
    """Start `python -m dpr.node ...` supervised by this process.

    `stdin_line` is handed to the node as its first line of input: the way secrets
    reach a node without appearing in its arguments or environment.
    """
    command = [sys.executable, "-m", "dpr.node", *args, "--supervised"]
    environment = dict(os.environ)
    environment["PYTHONPATH"] = os.pathsep.join(
        filter(None, [str(Path(__file__).resolve().parents[1]), environment.get("PYTHONPATH")]))
    kwargs: dict = {"stdin": subprocess.PIPE, "stdout": subprocess.DEVNULL,
                    "stderr": subprocess.DEVNULL, "env": environment}
    if os.name == "posix":
        # Own session: Ctrl-C in the session terminal must not reach the nodes.
        kwargs["start_new_session"] = True
    else:
        # Own process group and a hidden console, which the node's own children
        # inherit: no console windows flash up when tasks run.
        kwargs["creationflags"] = (subprocess.CREATE_NEW_PROCESS_GROUP
                                   | subprocess.CREATE_NO_WINDOW)
    popen = subprocess.Popen(command, **kwargs)
    if stdin_line is not None:
        try:
            popen.stdin.write(stdin_line.encode("ascii") + b"\n")
            popen.stdin.flush()
        except OSError:
            pass  # the node already failed; the caller sees it exit
    pid_dir.mkdir(parents=True, exist_ok=True)
    pid_file = pid_dir / f"{name}.pid"
    pid_file.write_text(str(popen.pid), encoding="ascii")
    return Child(name, popen, pid_file)


def clear_leftovers(pid_dir: Path) -> None:
    """Stop dpr nodes recorded by an earlier session that did not exit cleanly."""
    if not pid_dir.is_dir():
        return
    for pid_file in pid_dir.glob("*.pid"):
        try:
            pid = int(pid_file.read_text(encoding="ascii").strip())
        except (OSError, ValueError):
            pid = 0
        if pid > 0 and pid != os.getpid() and _alive(pid) and _is_dpr(pid):
            kill_tree(pid)
        pid_file.unlink(missing_ok=True)


def kill_tree(pid: int) -> None:
    try:
        if os.name == "posix":
            # Nodes lead their own process group; anything else is killed alone so
            # a stray pid can never take its neighbours (or this session) with it.
            if os.getpgid(pid) == pid:
                os.killpg(pid, signal.SIGKILL)
            else:
                os.kill(pid, signal.SIGKILL)
        else:
            subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"],
                           capture_output=True, check=False)
    except (ProcessLookupError, PermissionError, OSError):
        pass


def _alive(pid: int) -> bool:
    if os.name == "posix":
        try:
            os.kill(pid, 0)
        except PermissionError:
            return True
        except OSError:
            return False
        return True
    # os.kill(pid, 0) would terminate the process on Windows.
    result = subprocess.run(["tasklist", "/FI", f"PID eq {pid}", "/NH", "/FO", "CSV"],
                            capture_output=True, text=True, check=False)
    return f'"{pid}"' in result.stdout


def _is_dpr(pid: int) -> bool:
    """Guard against a recycled pid: only ever kill a process running dpr."""
    command = ""
    try:
        if sys.platform.startswith("linux"):
            command = Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\0", b" ").decode(
                "utf-8", "replace")
        elif os.name == "posix":
            command = subprocess.run(["ps", "-p", str(pid), "-o", "command="],
                                     capture_output=True, text=True, check=False).stdout
        else:
            command = subprocess.run(
                ["powershell", "-NoProfile", "-Command",
                 f"(Get-CimInstance Win32_Process -Filter 'ProcessId={pid}').CommandLine"],
                capture_output=True, text=True, check=False).stdout
    except OSError:
        return False
    return _DPR_COMMAND.search(command) is not None


class SessionLock:
    """One session per machine: two would fight over the same ports and files."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._fd: int | None = None

    def acquire(self) -> bool:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            if os.name == "posix":
                import fcntl
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            else:
                import msvcrt
                if os.fstat(fd).st_size == 0:
                    os.write(fd, b"0")
                os.lseek(fd, 0, os.SEEK_SET)
                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
        except OSError:
            os.close(fd)
            return False
        self._fd = fd
        return True

    def release(self) -> None:
        if self._fd is not None:
            try:
                os.close(self._fd)
            except OSError:
                pass
            self._fd = None


def free_port(preferred: int) -> int:
    """`preferred` when nothing holds it, otherwise any free port."""
    for candidate in (preferred, 0):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            if os.name == "posix":
                probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                probe.bind(("0.0.0.0", candidate))
            except OSError:
                continue
            return probe.getsockname()[1]
    raise OSError("no free port")


def wait_for(predicate, timeout: float, interval: float = 0.1) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()
