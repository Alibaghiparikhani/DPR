"""Running a program on the cluster: package, submit, wait, report, keep the result.

Each finished run is written to one text file in `dpr-results/` beside the program.
Only the newest results per program are kept; older ones are removed automatically.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
import datetime
import os
from pathlib import Path
import re
import secrets
import time
from typing import Callable

import protocol as p
from dag_runtime.dag_engine import analyze_source
from execution import lower_dag
from networking import CoordinatorClient, CoordinatorClientConfig
from program_package import PackageError, build_package

from dpr.text import printable

RESULTS_DIR = "dpr-results"
RESULTS_KEPT = 20
ENVIRONMENT = "default"
TERMINAL = frozenset({"succeeded", "failed", "cancelled"})


@dataclass(frozen=True)
class Outcome:
    program: Path
    run_id: str
    status: p.RunStatusResponse
    seconds: float
    saved: Path | None = None

    @property
    def succeeded(self) -> bool:
        return self.status.status == "succeeded"

    @property
    def output(self) -> str:
        return "\n".join(task.stdout_tail for task in self.status.tasks
                         if task.stdout_tail).rstrip("\n")

    @property
    def error(self) -> str:
        """One line naming what went wrong, or '' when nothing did."""
        if self.status.status == "succeeded":
            return ""
        for task in self.status.tasks:
            if task.failure_kind and task.detail and "not run" not in task.detail:
                return _describe(task.exception_type or task.failure_kind, task.detail)
        if self.status.failure_detail:
            return _describe(self.status.failure_kind or "", self.status.failure_detail)
        return ""


def _describe(kind: str, detail: str) -> str:
    kind = kind.removeprefix("builtins.")
    return f"{kind}: {detail}" if kind else detail


def package(program: Path, *, exclude: tuple[Path, ...] = ()):
    """Package the program's folder and plan its execution.

    The whole folder goes to every worker, so a home folder or a drive root -- where
    anything personal might sit beside the program -- is never sent.
    """
    root = program.parent
    relative = program.relative_to(root).as_posix()
    if root.resolve().parent == root.resolve() or root.resolve() == Path.home().resolve():
        raise RuntimeError(f"move {relative} into a folder of its own; "
                           "its folder is sent to every machine")
    try:
        artifact = build_package(root, exclude=exclude)
    except PackageError as error:
        raise RuntimeError(f"cannot package {root}: {error}") from None
    # Analyze the exact bytes that are shipped; utf-8-sig tolerates a leading BOM.
    try:
        source = program.read_bytes().decode("utf-8-sig")
    except UnicodeDecodeError:
        raise RuntimeError(f"{relative} is not UTF-8 text") from None
    dag = analyze_source(source, filename=relative)
    if not dag.execution_permitted:
        raise RuntimeError(dag.diagnostics[0] if dag.diagnostics else f"{relative} cannot run")
    plan = lower_dag(dag, environment_id=ENVIRONMENT, package_id=artifact.package_id)
    return artifact, plan, relative


async def submit(client: CoordinatorClient, program: Path, *, run_id: str | None = None,
                 exclude: tuple[Path, ...] = ()) -> str:
    artifact, plan, relative = package(program, exclude=exclude)
    run_id = run_id or f"run-{secrets.token_hex(6)}"
    await client.submit_package(
        run_id=run_id, environment_id=ENVIRONMENT, entrypoint=relative,
        plan_id=plan.id, package_id=artifact.package_id, archive_bytes=artifact.archive_bytes)
    return run_id


async def wait(client: CoordinatorClient, run_id: str, *, poll: float = 0.2,
               tick: Callable[[], None] | None = None) -> p.RunStatusResponse:
    while True:
        status = await client.run_status_resilient(run_id, include_tasks=False)
        if status.status in TERMINAL:
            return await client.run_status_resilient(run_id, include_tasks=True)
        if tick is not None:
            tick()
        await asyncio.sleep(poll)


async def run(config: CoordinatorClientConfig, program: Path, *,
              exclude: tuple[Path, ...] = (), tick: Callable[[float], None] | None = None,
              run_id: str | None = None) -> Outcome:
    """Run `program` to completion.  Cancelling this coroutine cancels the run."""
    started = time.monotonic()
    run_id = run_id or f"run-{secrets.token_hex(6)}"
    try:
        async with CoordinatorClient(config) as client:
            await submit(client, program, run_id=run_id, exclude=exclude)
            status = await wait(client, run_id, tick=None if tick is None
                                else lambda: tick(time.monotonic() - started))
    except asyncio.CancelledError:
        await asyncio.shield(_cancel(config, run_id))
        raise
    return Outcome(program, run_id, status, time.monotonic() - started)


async def _cancel(config: CoordinatorClientConfig, run_id: str) -> None:
    try:
        async with CoordinatorClient(config) as client:
            await asyncio.wait_for(client.cancel_run(run_id, reason="cancelled"), timeout=10)
    except Exception:
        pass


def save(outcome: Outcome) -> Outcome:
    """Write the run's result file and prune old ones; the outcome gains its path.

    Nothing is written through a link or over an existing file.
    """
    directory = outcome.program.parent / RESULTS_DIR
    stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    data = report(outcome).encode("utf-8")
    try:
        if directory.is_symlink():
            return outcome
        directory.mkdir(exist_ok=True)
        if directory.is_symlink() or not directory.is_dir():
            return outcome
        for attempt in range(100):
            path = directory / f"{outcome.program.stem}-{stamp}{f'-{attempt}' if attempt else ''}.txt"
            try:
                fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
            except FileExistsError:
                continue
            with os.fdopen(fd, "wb") as handle:
                handle.write(data)
            break
        else:
            return outcome
        _prune(directory, outcome.program.stem)
    except OSError:
        return outcome
    return Outcome(outcome.program, outcome.run_id, outcome.status, outcome.seconds, path)


def _prune(directory: Path, stem: str) -> None:
    pattern = re.compile(re.escape(stem) + r"-\d{8}-\d{6}(?:-\d+)?\.txt")
    mine = sorted((path for path in directory.iterdir() if pattern.fullmatch(path.name)),
                  key=lambda path: path.name)
    for old in mine[:-RESULTS_KEPT]:
        old.unlink(missing_ok=True)


def report(outcome: Outcome) -> str:
    status = outcome.status
    lines = [
        f"program  {outcome.program}",
        f"run      {outcome.run_id}",
        f"result   {status.status} in {outcome.seconds:.1f}s",
    ]
    if outcome.error:
        lines.append(f"error    {outcome.error}")
    lines += ["", "output", outcome.output or "(none)", "", "tasks"]
    for task in status.tasks:
        where = f"  {task.worker_id}" if task.worker_id else ""
        lines.append(f"  {task.task_id}  {task.status}{where}")
        if task.failure_kind:
            lines.append(f"    {_describe(task.exception_type or task.failure_kind, task.detail)}")
        if task.stderr_tail:
            lines.extend(f"    {line}" for line in task.stderr_tail.rstrip("\n").splitlines())
    if status.tasks_truncated:
        lines.append(f"  ... {status.task_count - len(status.tasks)} more")
    return printable("\n".join(lines), colour=False) + "\n"
