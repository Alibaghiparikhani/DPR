"""The interactive session opened by `dpr start`.

Everything happens here: hosting, joining, running, and leaving.  Whatever the session
starts, it stops -- on `exit`, on end of input, and (through dpr.processes) even when
the terminal is simply closed.
"""
from __future__ import annotations

import asyncio
import contextlib
import ipaddress
import os
from pathlib import Path
import re
import sys
import threading
import time

import dpr
from dpr import runs
from dpr.cluster import Hosting, Joined
from dpr.home import Home, HomeError
from dpr.processes import SessionLock, clear_leftovers
from dpr.text import printable

COMMANDS = (
    ("host", "start a cluster on this machine"),
    ("join <code>", "add this machine to a cluster"),
    ("run <file>", "run a Python program on the cluster"),
    ("status", "show the cluster"),
    ("approve <name>", "let a waiting machine join"),
    ("kick <name>", "remove a machine from the cluster"),
    ("exit", "stop everything and leave"),
)
PROMPT = "dpr> "


def table(rows, indent: str = "  ", width: int | None = None) -> str:
    width = width or max(len(name) for name, _ in rows) + 3
    return "\n".join(f"{indent}{name:<{width}}{text}" for name, text in rows)


def command_table(width: int | None = None) -> str:
    return table(COMMANDS, width=width)


def _rows(pairs: list[tuple[str, str]]) -> str:
    return "\n".join(f"{key:<10}{value}" for key, value in pairs)


def _user_path(text: str) -> Path:
    """A path as typed: quotes, `~`, and Git Bash's /c/... form on Windows."""
    text = text.strip().strip('"').strip("'")
    if os.name == "nt":
        match = re.match(r"^/([A-Za-z])/(.*)$", text)
        if match:
            text = f"{match.group(1).upper()}:\\" + match.group(2).replace("/", "\\")
    return Path(text).expanduser()


def _shown(path: Path) -> str:
    try:
        return str(path.relative_to(Path.cwd()))
    except ValueError:
        return str(path)


class Session:
    def __init__(self, home: Home) -> None:
        self.home = home
        self.hosting: Hosting | None = None
        self.joined: Joined | None = None
        # Join requests are announced as they arrive: at once while the prompt is
        # idle, otherwise just before the next prompt, never inside a command's output.
        self._screen = threading.Lock()
        self._idle = False
        self._announced: set[str] = set()
        self._queued: list[str] = []
        self._closing = threading.Event()
        threading.Thread(target=self._watch_requests, name="dpr-requests", daemon=True).start()

    # ------------------------------------------------------------------ loop

    def loop(self) -> None:
        try:
            import readline  # noqa: F401 - line editing and history where available
        except ImportError:
            pass
        while True:
            with self._screen:
                for notice in self._queued:
                    print(notice)
                self._queued.clear()
                self._idle = True
            try:
                line = self._read()
            except KeyboardInterrupt:
                print()
                continue
            except EOFError:
                # Windows can report Ctrl-C at the prompt as end of input, with the
                # interrupt arriving a moment later; only a real EOF ends the session.
                try:
                    time.sleep(0.1)
                except KeyboardInterrupt:
                    print()
                    continue
                print()
                return
            command, _, rest = line.strip().partition(" ")
            if command in ("exit", "quit"):
                return
            try:
                self.dispatch(command, rest.strip())
            except KeyboardInterrupt:
                print()
            except Exception as error:  # every failure is one line, never a traceback
                if os.environ.get("DPR_DEBUG"):
                    raise
                print(printable(str(error)) or type(error).__name__)

    def _read(self) -> str:
        try:
            return input(PROMPT)
        finally:
            with self._screen:
                self._idle = False

    def dispatch(self, command: str, rest: str) -> None:
        if not command:
            return
        self._check()
        handler = {"host": self.host, "join": self.join, "run": self.run,
                   "status": self.status, "approve": self.approve, "kick": self.kick,
                   "help": lambda _: print(command_table())}.get(command)
        if handler is None:
            print(f"unknown command: {command}")
            print(command_table())
            return
        handler(rest)

    def _check(self) -> None:
        """Notice a node that stopped on its own, and say why once."""
        for role in ("hosting", "joined"):
            node = getattr(self, role)
            if node is not None and not node.alive():
                setattr(self, role, None)
                if role == "joined" and node.state() == "removed":
                    node.forget()
                    print("removed by the host")
                    continue
                reason = printable(node.failure())
                print(f"{'host' if role == 'hosting' else 'worker'} stopped"
                      + (f": {reason}" if reason else ""))

    def _watch_requests(self) -> None:
        while not self._closing.wait(0.5):
            hosting = self.hosting
            if hosting is None:
                continue
            try:
                waiting = hosting.waiting()
            except Exception:
                continue
            for entry in waiting:
                if entry["ref"] in self._announced:
                    continue
                self._announced.add(entry["ref"])
                notice = f"{printable(entry['name'])} wants to join ({entry['code']})"
                with self._screen:
                    if self._idle:
                        _interject(notice)
                    else:
                        self._queued.append(notice)

    def close(self) -> None:
        self._closing.set()
        for node in (self.hosting, self.joined):
            if node is not None:
                node.stop()
        self.hosting = self.joined = None

    # -------------------------------------------------------------- commands

    def host(self, rest: str) -> None:
        if rest:
            raise RuntimeError("usage: host")
        if self.hosting is None:
            if self.joined is not None:
                self.joined.stop()
                self.joined = None
                print("left the cluster")
            self.hosting = Hosting.start(self.home)
        rows = [("hosting", f"{self.hosting.address}:{self.hosting.port}"),
                ("code", self.hosting.code)]
        if ipaddress.ip_address(self.hosting.address).is_global:
            rows.append(("note", "this address is reachable from the internet"))
        print(_rows(rows))

    def join(self, rest: str) -> None:
        if " " in rest:
            raise RuntimeError("usage: join <code>")
        # Settle the code first: a bad one must not cost the current role.
        if rest:
            def waiting(number: str) -> None:
                print(f"waiting for approval ({number})", flush=True)
            Joined.enroll(self.home, rest, waiting=waiting)
        elif not Joined.enrolled(self.home):
            raise RuntimeError("usage: join <code>")
        if self.hosting is not None:
            self.hosting.stop()
            self.hosting = None
            print("stopped hosting")
        if self.joined is not None:
            self.joined.stop()
            self.joined = None
        self.joined = Joined.start(self.home)
        print(_rows([("joined", self.joined.address)]))

    def run(self, rest: str) -> None:
        if not rest:
            raise RuntimeError("usage: run <file>")
        program = _user_path(rest).resolve()
        if not program.is_file():
            raise RuntimeError(f"no such file: {_shown(program)}")
        if self.hosting is None:
            raise RuntimeError("only the host runs programs" if self.joined else "not hosting")
        cluster = asyncio.run(self.hosting.machines())
        if not cluster.workers:
            raise RuntimeError("no machines joined")
        if not any(worker.online and worker.accepting_work for worker in cluster.workers):
            raise RuntimeError("no machines available")

        progress = _Progress()
        started = time.monotonic()
        try:
            outcome = asyncio.run(runs.run(self.hosting.config(), program,
                                           exclude=(self.home.path,), tick=progress.update))
        except KeyboardInterrupt:
            progress.clear()
            print(f"cancelled after {time.monotonic() - started:.1f}s")
            return
        progress.clear()
        outcome = runs.save(outcome)
        # Output and errors come from other machines: printed only once made inert.
        if outcome.output:
            print(printable(outcome.output))
        if outcome.error:
            print(printable(outcome.error))
        verb = "done in" if outcome.succeeded else (
            "cancelled after" if outcome.status.status == "cancelled" else "failed after")
        saved = f", saved to {_shown(outcome.saved)}" if outcome.saved else ""
        print(f"{verb} {outcome.seconds:.1f}s{saved}")

    def status(self, rest: str) -> None:
        if rest:
            raise RuntimeError("usage: status")
        if self.hosting is not None:
            rows = [("hosting", f"{self.hosting.address}:{self.hosting.port}"),
                    ("code", self.hosting.code)]
            cluster = asyncio.run(self.hosting.machines())
            views = {worker.worker_id: worker for worker in cluster.workers}
            members = _labels(self.hosting.names())
            machines = sorted(members.items(), key=lambda item: item[1].lower())
            waiting = [(printable(entry["name"]), entry["code"]) for entry in self.hosting.waiting()]
            width = max((len(label) for label in [*members.values(), *(n for n, _ in waiting)]),
                        default=0) + 3
            if not machines:
                rows.append(("machines", "none"))
            for index, (identity, label) in enumerate(machines):
                rows.append(("machines" if index == 0 else "",
                             f"{label:<{width}}{_state(views.get(identity))}"))
            for index, (name, number) in enumerate(waiting):
                rows.append(("waiting" if index == 0 else "", f"{name:<{width}}{number}"))
            print(_rows(rows))
        elif self.joined is not None:
            print(_rows([("joined", self.joined.address), ("state", self.joined.state())]))
        else:
            print("not in a cluster")

    def approve(self, rest: str) -> None:
        if not rest or " " in rest:
            raise RuntimeError("usage: approve <name>")
        hosting = self._hosting()
        matches = [entry for entry in hosting.waiting() if _matches(rest, entry["name"], entry["code"])]
        if not matches:
            raise RuntimeError(f"no machine named {rest} is waiting")
        if len(matches) > 1:
            raise RuntimeError(f"more than one {rest} is waiting; use its number")
        entry = matches[0]
        result = hosting.decide("approve", entry["ref"])
        if result != "approved":
            raise RuntimeError({"full": "cluster is full",
                                "gone": f"{rest} stopped waiting"}.get(result, result))
        print(_rows([("approved", printable(entry["name"]))]))

    def kick(self, rest: str) -> None:
        if not rest or " " in rest:
            raise RuntimeError("usage: kick <name>")
        hosting = self._hosting()
        labels = _labels(hosting.names())
        candidates = [("kick", identity, label) for identity, label in labels.items()
                      if _matches(rest, label, identity) or _matches(rest, label.split(" (")[0])]
        candidates += [("deny", entry["ref"], printable(entry["name"])) for entry in hosting.waiting()
                       if _matches(rest, entry["name"], entry["code"])]
        if not candidates:
            raise RuntimeError(f"no machine named {rest}")
        if len(candidates) > 1:
            raise RuntimeError(f"more than one {rest}; use its number or id")
        action, value, label = candidates[0]
        result = hosting.decide(action, value)
        if result not in ("removed", "denied"):
            raise RuntimeError(f"no machine named {rest}" if result == "gone" else result)
        print(_rows([("removed", label)]))

    def _hosting(self) -> Hosting:
        if self.hosting is None:
            raise RuntimeError("not hosting")
        return self.hosting


def _matches(typed: str, *names: str) -> bool:
    return any(typed.casefold() == name.casefold() for name in names)


def _labels(names: dict[str, str]) -> dict[str, str]:
    """identity -> what to call it; a name two machines share gets its id attached."""
    shown = {identity: printable(name) for identity, name in names.items()}
    counts: dict[str, int] = {}
    for name in shown.values():
        counts[name.casefold()] = counts.get(name.casefold(), 0) + 1
    return {identity: f"{name} ({identity})" if counts[name.casefold()] > 1 else name
            for identity, name in shown.items()}


def _state(worker) -> str:
    if worker is None or not worker.online:
        return "offline"
    if not worker.accepting_work:
        return "paused"
    return (f"{'busy' if worker.running_slots else 'idle':<7}"
            f"{worker.running_slots}/{worker.total_slots} slots")


def _interject(line: str) -> None:
    """Print a line while someone may be typing at the prompt, then put the prompt
    (and whatever they had typed) back."""
    typed = None
    if sys.stdin.isatty():
        try:
            import readline
            typed = readline.get_line_buffer()
        except (ImportError, AttributeError):
            pass
    if typed is None:
        # No line editor to redraw: finish the prompt's line and start a new one.
        sys.stdout.write(f"\n{line}\n{PROMPT}")
    else:
        sys.stdout.write("\r" + " " * (len(PROMPT) + len(typed)) + "\r" + f"{line}\n{PROMPT}{typed}")
    sys.stdout.flush()


class _Progress:
    """A single self-erasing `running 12s` line, shown only on a terminal."""

    def __init__(self) -> None:
        self._last = -1
        self._shown = False
        self._tty = sys.stdout.isatty()

    def update(self, seconds: float) -> None:
        whole = int(seconds)
        if self._tty and whole >= 2 and whole != self._last:
            sys.stdout.write(f"\rrunning {whole}s")
            sys.stdout.flush()
            self._shown = True
            self._last = whole

    def clear(self) -> None:
        if self._shown:
            sys.stdout.write("\r" + " " * 20 + "\r")
            sys.stdout.flush()
            self._shown = False


def start() -> int:
    home = Home.default()
    try:
        home.claim()
    except HomeError as error:
        print(error)
        return 1
    lock = SessionLock(home.lock)
    if not lock.acquire():
        print("dpr is already running on this machine")
        return 1
    with contextlib.suppress(AttributeError, ValueError):
        sys.stdout.reconfigure(errors="replace")  # program output never crashes the prompt
    session = Session(home)
    try:
        clear_leftovers(home.pids)
        home.prepare()
        print(f"dpr {dpr.__version__}\n\n{command_table()}\n")
        session.loop()
    finally:
        session.close()
        lock.release()
    return 0
