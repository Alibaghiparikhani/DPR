"""The `dpr` command.  It has two subcommands; everything else lives in the session."""
from __future__ import annotations

import subprocess
import sys

import dpr
from dpr.home import Home
from dpr.processes import SessionLock, clear_leftovers
from dpr.session import COMMANDS, command_table, start, table

PACKAGE = "dpr-runtime"


def _rows(launcher: str) -> tuple[tuple[str, str], ...]:
    return ((f"{launcher} start", "open a session"),
            (f"{launcher} uninstall", "remove dpr and its data from this machine"))


def usage(launcher: str = "dpr") -> str:
    return table(_rows(launcher))


def welcome(launcher: str = "dpr") -> str:
    """Every command and what it does; printed once dpr is installed."""
    rows = _rows(launcher)
    width = max(len(name) for name, _ in rows + COMMANDS) + 3
    return (f"dpr {dpr.__version__} installed\n\n{table(rows, width=width)}\n\n"
            f"in a session\n{command_table(width)}")


def uninstall() -> int:
    try:
        answer = input("remove dpr and all its data from this machine? [y/N] ")
    except (EOFError, KeyboardInterrupt):
        print()
        return 1
    if answer.strip().lower() not in ("y", "yes"):
        return 1
    home = Home.default()
    if home.path.is_dir() and home.owned():
        lock = SessionLock(home.lock)
        if not lock.acquire():
            print("close the open dpr session first")
            return 1
        clear_leftovers(home.pids)
        lock.release()
        home.erase()
    result = subprocess.run([sys.executable, "-m", "pip", "uninstall", "-y", "-q", PACKAGE],
                            capture_output=True, text=True, check=False)
    if result.returncode != 0:
        lines = (result.stderr or result.stdout).strip().splitlines()
        print(f"data removed; the package was not: {lines[-1] if lines else result.returncode}")
        return 2
    print("removed")
    return 0


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else list(argv)
    if args == ["start"]:
        return start()
    if args == ["uninstall"]:
        return uninstall()
    if args and args[0] not in ("-h", "--help", "help"):
        print(f"unknown command: {' '.join(args)}")
        print(usage())
        return 2
    print(usage())
    return 0


def cli_entry() -> None:
    """Console-script entry point installed as the `dpr` command."""
    raise SystemExit(main())
