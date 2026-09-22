"""Where dpr keeps its files on this machine.

    ~/.dpr                 ($DPR_HOME overrides)
      .dpr-home            marks the folder as dpr's own
      session.lock         held by the open session
      machine-id           stable identity this machine presents when joining
      host/                cluster credentials and host.json    (this machine hosts)
      worker/              worker credentials and worker.json   (this machine joined)
      state/               logs, caches, run history -- all disposable

Nothing in here is edited by hand.  Anything unreadable is treated as absent and
rebuilt, so a crash or a half-written file never needs manual cleanup.

dpr only ever deletes the entries listed above, and only in a folder it has marked
as its own; a $DPR_HOME that already holds other files is refused, so a mistyped
path can never cost anyone their data.  The folder is private to the user.
"""
from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import subprocess
import time

VERSION = 2
ENV_HOME = "DPR_HOME"
MARKER = ".dpr-home"
# Everything dpr may create in its folder (cred/ and state/ also cover the layout of
# earlier releases).  Nothing outside this set is ever touched.
OWNED = frozenset({MARKER, "session.lock", "machine-id", "host", "host.partial", "worker",
                   "state", "cred"})
# Metadata the operating system may drop into any folder; never a reason to refuse one.
SYSTEM_LITTER = frozenset({".DS_Store", "desktop.ini", "Thumbs.db"})
MAX_STATE_FILE = 1 << 20


class HomeError(RuntimeError):
    pass


@dataclass(frozen=True)
class Home:
    path: Path

    @classmethod
    def default(cls) -> "Home":
        override = os.environ.get(ENV_HOME)
        path = Path(override).expanduser() if override else Path.home() / ".dpr"
        return cls(path.absolute())

    @property
    def lock(self) -> Path:
        return self.path / "session.lock"

    @property
    def host(self) -> Path:
        return self.path / "host"

    @property
    def worker(self) -> Path:
        return self.path / "worker"

    @property
    def state(self) -> Path:
        return self.path / "state"

    @property
    def logs(self) -> Path:
        return self.state / "logs"

    @property
    def pids(self) -> Path:
        return self.state / "pids"

    def owned(self) -> bool:
        """True when the folder is dpr's: marked, empty, or unmistakably an earlier
        release's -- names alone prove nothing, so the contents are checked."""
        if (self.path / MARKER).is_file():
            return True
        try:
            names = [entry.name for entry in self.path.iterdir() if entry.name not in SYSTEM_LITTER]
        except FileNotFoundError:
            return True
        except OSError:
            return False
        if not names:
            return True
        if not all(name in OWNED or _is_temporary(name) for name in names):
            return False
        return _earlier_release(self.path)

    def claim(self) -> None:
        """Make the folder dpr's (refusing one that holds anything else) and private."""
        if self.path.is_symlink():
            raise HomeError(f"{self.path} is a link; dpr needs a real folder")
        if not self.owned():
            raise HomeError(f"{self.path} holds other files; point {ENV_HOME} at an empty folder")
        new = not (self.path / MARKER).is_file()
        self.path.mkdir(parents=True, exist_ok=True, mode=0o700)
        if new:
            write_text(self.path / MARKER, "dpr\n")
            _restrict_windows(self.path)
        _private(self.path)

    def prepare(self) -> None:
        # Earlier releases kept credentials under cred/ with per-worker state beside
        # them.  None of it is usable now, so it goes (their processes are already
        # stopped by the time this runs).
        if (self.path / "cred").exists():
            remove(self.path / "cred")
            remove(self.state)
        for directory in (self.state, self.logs, self.pids):
            directory.mkdir(parents=True, exist_ok=True, mode=0o700)
            _private(directory)
        for directory in (self.host, self.worker):
            if directory.is_dir():
                _private(directory)
        for leftover in self.path.glob(".*.tmp"):
            remove(leftover)

    def erase(self) -> None:
        """Remove everything dpr keeps here, and the folder itself once it is empty."""
        if not self.owned():
            return
        for name in OWNED | SYSTEM_LITTER:
            remove(self.path / name)
        for leftover in self.path.glob(".*.tmp"):
            remove(leftover)
        try:
            self.path.rmdir()
        except OSError:
            pass  # something that is not dpr's lives here; leave it be

    def machine_id(self) -> str:
        path = self.path / "machine-id"
        try:
            value = path.read_text(encoding="ascii").strip()
            if re.fullmatch(r"[0-9a-f]{32}", value):
                return value
        except (OSError, UnicodeDecodeError):
            pass
        value = secrets.token_hex(16)
        write_text(path, value + "\n")
        return value


def _earlier_release(path: Path) -> bool:
    """A folder written by an earlier dpr: its own files, with their own contents."""
    def json_version(file: Path) -> object:
        try:
            with open(file, "rb") as handle:
                data = json.loads(handle.read(MAX_STATE_FILE))
            return data.get("version") if isinstance(data, dict) else None
        except (OSError, ValueError):
            return None

    try:
        machine = (path / "machine-id").read_text(encoding="ascii").strip()
    except (OSError, UnicodeDecodeError):
        machine = ""
    try:
        lock = (path / "session.lock").stat().st_size <= 1  # dpr's lock file: 0 or 1 byte
    except OSError:
        lock = False
    return (re.fullmatch(r"[0-9a-f]{32}", machine) is not None or lock
            or json_version(path / "cred" / "cluster.json") == 1
            or json_version(path / "host" / "host.json") == VERSION
            or json_version(path / "worker" / "worker.json") == VERSION)


def _is_temporary(name: str) -> bool:
    return name.startswith(".") and name.endswith(".tmp")


def _private(path: Path) -> None:
    if os.name == "posix":
        try:
            os.chmod(path, 0o700)
        except OSError:
            pass


def _restrict_windows(path: Path) -> None:
    """Give a folder outside the user profile the profile's privacy: only this user,
    SYSTEM and Administrators.  Inside the profile that is already the case."""
    if os.name != "nt":
        return
    try:
        profile = Path(os.environ.get("USERPROFILE", "")).resolve()
        if profile != Path(".").resolve() and path.resolve().is_relative_to(profile):
            return
        found = subprocess.run(["whoami", "/user", "/fo", "csv", "/nh"], capture_output=True,
                               text=True, check=False, timeout=10).stdout
        sid = re.search(r"S-1-5-[0-9-]+", found)
        if sid is None:
            return
        subprocess.run(["icacls", str(path), "/inheritance:r",
                        "/grant:r", f"*{sid.group(0)}:(OI)(CI)F",
                        "/grant:r", "*S-1-5-18:(OI)(CI)F", "/grant:r", "*S-1-5-32-544:(OI)(CI)F",
                        "/Q"], capture_output=True, check=False, timeout=30)
    except (OSError, ValueError, subprocess.SubprocessError):
        pass


def read_json(path: Path) -> dict | None:
    """A profile or state file, or None when it is missing, unreadable or outdated."""
    try:
        with open(path, "rb") as handle:
            raw = handle.read(MAX_STATE_FILE + 1)
        data = json.loads(raw) if len(raw) <= MAX_STATE_FILE else None
    except (OSError, ValueError):
        return None
    if not isinstance(data, dict) or data.get("version") != VERSION:
        return None
    return data


def write_json(path: Path, data: dict) -> None:
    write_text(path, json.dumps({"version": VERSION, **data}, indent=2, sort_keys=True) + "\n")


def write_text(path: Path, text: str) -> None:
    """Replace a small file atomically, readable only by this user."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(f".{path.name}.{secrets.token_hex(8)}.tmp")
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        view = memoryview(text.encode("utf-8"))
        while view:
            view = view[os.write(fd, view):]
        os.fsync(fd)
    finally:
        os.close(fd)
    for attempt in range(40):
        try:
            os.replace(temporary, path)
            return
        except PermissionError:
            # Windows refuses to replace a file another process has open for a moment.
            if attempt == 39:
                temporary.unlink(missing_ok=True)
                raise
            time.sleep(0.05)


def remove(path: Path) -> None:
    path = Path(path)
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path, ignore_errors=True)
    else:
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass
