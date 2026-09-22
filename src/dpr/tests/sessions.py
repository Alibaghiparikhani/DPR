"""Drive real `dpr start` sessions through a pipe, the way a person types at them."""
from __future__ import annotations

import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time

SRC = Path(__file__).resolve().parents[2]
PROMPT = "dpr> "


def scale() -> float:
    return max(1.0, float(os.environ.get("DPR_TEST_TIMEOUT_SCALE", "1")))


class Session:
    def __init__(self, home: Path, cwd: Path) -> None:
        env = dict(os.environ, DPR_HOME=str(home), PYTHONUNBUFFERED="1",
                   PYTHONPATH=os.pathsep.join(filter(None, [str(SRC), os.environ.get("PYTHONPATH")])))
        env.pop("DPR_DEBUG", None)
        kwargs: dict = {}
        if os.name == "posix":
            kwargs["start_new_session"] = True
        else:
            kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
        self.home = home
        self.process = subprocess.Popen(
            [sys.executable, "-m", "dpr", "start"], cwd=cwd, env=env, text=True, bufsize=1,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, **kwargs)
        self._buffer = ""
        self._lock = threading.Lock()
        threading.Thread(target=self._pump, daemon=True).start()
        self.banner = self.read()

    def _pump(self) -> None:
        while True:
            character = self.process.stdout.read(1)
            if not character:
                return
            with self._lock:
                self._buffer += character

    def read(self, timeout: float = 60.0) -> str:
        """Everything printed up to the next prompt (or until the session ends)."""
        deadline = time.monotonic() + timeout * scale()
        while time.monotonic() < deadline:
            with self._lock:
                if self._buffer.endswith(PROMPT):
                    text, self._buffer = self._buffer[: -len(PROMPT)], ""
                    return text
            if self.process.poll() is not None:
                time.sleep(0.2)
                with self._lock:
                    text, self._buffer = self._buffer, ""
                return text
            time.sleep(0.02)
        raise AssertionError(f"no prompt; output so far: {self._buffer!r}")

    def send(self, line: str, timeout: float = 60.0) -> str:
        self.write(line)
        return self.read(timeout)

    def write(self, line: str) -> None:
        self.process.stdin.write(line + "\n")
        self.process.stdin.flush()

    def interrupt(self) -> None:
        if os.name == "posix":
            os.kill(self.process.pid, signal.SIGINT)
        else:
            os.kill(self.process.pid, signal.CTRL_BREAK_EVENT)

    def close(self, timeout: float = 30.0) -> int:
        if self.process.poll() is None:
            try:
                self.write("exit")
            except OSError:
                pass
        try:
            return self.process.wait(timeout=timeout * scale())
        except subprocess.TimeoutExpired:
            self.process.kill()
            return self.process.wait(timeout=10)

    def node_pids(self) -> list[int]:
        pids = []
        for pid_file in (self.home / "state" / "pids").glob("*.pid"):
            try:
                pids.append(int(pid_file.read_text().strip()))
            except (OSError, ValueError):
                pass
        return pids


def process_alive(pid: int) -> bool:
    if os.name == "posix":
        try:
            os.kill(pid, 0)
        except OSError:
            return False
        try:  # a zombie has already exited
            with open(f"/proc/{pid}/stat", encoding="ascii") as handle:
                return handle.read().split(")")[-1].split()[0] != "Z"
        except OSError:
            return True
    result = subprocess.run(["tasklist", "/FI", f"PID eq {pid}", "/NH", "/FO", "CSV"],
                            capture_output=True, text=True, check=False)
    return f'"{pid}"' in result.stdout


def wait_gone(pids: list[int], timeout: float = 30.0) -> bool:
    deadline = time.monotonic() + timeout * scale()
    while time.monotonic() < deadline:
        if not any(process_alive(pid) for pid in pids):
            return True
        time.sleep(0.1)
    return False
