"""Optional SQLite-backed terminal coordinator history.

The live coordinator remains in-memory.  This store archives terminal runs and
related task/attempt/transfer metadata for diagnostics and later pruning; it is
not crash-recovery state for active runs.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os
import queue
import sqlite3
import threading
from typing import Iterable, Protocol

from .model import AttemptRecord, TaskRecord, TransferRecord
from .runs import CoordinatorRun

_SCHEMA_VERSION = 1


@dataclass(frozen=True, slots=True)
class StoredRun:
    run_id: str
    plan_id: str
    program_id: str
    status: str
    failure_code: str | None
    failure_detail: str | None
    archived_at: float


@dataclass(frozen=True, slots=True)
class StoredTask:
    run_id: str
    task_id: str
    status: str
    committed_attempt_id: str | None
    failure_kind: str | None
    failure_message: str | None
    exception_type: str | None = None


@dataclass(frozen=True, slots=True)
class StoredAttempt:
    run_id: str
    attempt_id: str
    task_id: str
    worker_id: str
    worker_generation: int
    status: str
    context_id: str | None
    failure_kind: str | None
    failure_message: str | None
    stdout_tail: str = ""
    stderr_tail: str = ""
    stdout_truncated: bool = False
    stderr_truncated: bool = False
    exception_type: str | None = None


@dataclass(frozen=True, slots=True)
class StoredTransfer:
    run_id: str
    transfer_id: str
    transfer_attempt_id: str
    value_id: str
    source_worker_id: str
    destination_worker_id: str
    status: str
    failure_detail: str | None


@dataclass(frozen=True, slots=True)
class StoredRunBundle:
    run: StoredRun
    tasks: tuple[StoredTask, ...]
    attempts: tuple[StoredAttempt, ...]
    transfers: tuple[StoredTransfer, ...]


@dataclass(frozen=True, slots=True)
class _ArchiveSnapshot:
    run: tuple[object, ...]
    tasks: tuple[tuple[object, ...], ...]
    attempts: tuple[tuple[object, ...], ...]
    transfers: tuple[tuple[object, ...], ...]


@dataclass(frozen=True, slots=True)
class _ArchiveJob:
    run_id: str
    generation: int
    snapshot: _ArchiveSnapshot


class RunHistoryStore(Protocol):
    def archive_run(
        self, run: CoordinatorRun, transfers: Iterable[TransferRecord], *, archived_at: float
    ) -> None: ...

    def has_run(self, run_id: str) -> bool: ...


class SQLiteRunHistoryStore:
    """Small stdlib SQLite archive for terminal coordinator history.

    Writes are transactional and idempotent by run/attempt/transfer identity.
    The database stores metadata only; it never stores user-code objects or task
    payload bytes.
    """

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        if not str(self.path):
            raise ValueError("history database path must be nonempty")
        parent_existed = self.path.parent.exists()
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if not parent_existed:
            os.chmod(self.path.parent, 0o700)
        if self.path.exists():
            if self.path.is_symlink() or not self.path.is_file():
                raise ValueError("history database must be a regular file")
            os.chmod(self.path, 0o600)
        else:
            fd = os.open(self.path, os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600)
            os.close(fd)
        self._initialize()
        self._archive_queue: queue.Queue[_ArchiveJob] = queue.Queue()
        self._archive_lock = threading.Lock()
        self._archive_generation: dict[str, int] = {}
        self._archive_events: dict[str, threading.Event] = {}
        self._archive_errors: dict[str, str] = {}
        self._archive_thread = threading.Thread(
            target=self._archive_worker, name="dpr-history-archive", daemon=True
        )
        self._archive_thread.start()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=0.05)
        os.chmod(self.path, 0o600)
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    def _secure_sidecars(self) -> None:
        for suffix in ("", "-journal", "-wal", "-shm"):
            path = Path(str(self.path) + suffix)
            if path.exists() and not path.is_symlink():
                with __import__("contextlib").suppress(OSError):
                    os.chmod(path, 0o600)

    def _initialize(self) -> None:
        with self._connect() as db:
            db.executescript(
                """
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    plan_id TEXT NOT NULL,
                    program_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    failure_code TEXT,
                    failure_detail TEXT,
                    archived_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS tasks (
                    run_id TEXT NOT NULL,
                    task_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    committed_attempt_id TEXT,
                    failure_kind TEXT,
                    failure_message TEXT,
                    PRIMARY KEY (run_id, task_id),
                    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
                );
                CREATE TABLE IF NOT EXISTS attempts (
                    run_id TEXT NOT NULL,
                    attempt_id TEXT NOT NULL,
                    task_id TEXT NOT NULL,
                    worker_id TEXT NOT NULL,
                    worker_generation INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    context_id TEXT,
                    failure_kind TEXT,
                    failure_message TEXT,
                    stdout_tail TEXT NOT NULL DEFAULT '',
                    stderr_tail TEXT NOT NULL DEFAULT '',
                    stdout_truncated INTEGER NOT NULL DEFAULT 0,
                    stderr_truncated INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (run_id, attempt_id),
                    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
                );
                CREATE TABLE IF NOT EXISTS transfers (
                    run_id TEXT NOT NULL,
                    transfer_id TEXT NOT NULL,
                    transfer_attempt_id TEXT NOT NULL,
                    value_id TEXT NOT NULL,
                    source_worker_id TEXT NOT NULL,
                    destination_worker_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    failure_detail TEXT,
                    PRIMARY KEY (run_id, transfer_id, transfer_attempt_id),
                    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
                );
                """
            )
            existing = db.execute(
                "SELECT value FROM metadata WHERE key = 'schema_version'"
            ).fetchone()
            if existing is None:
                db.execute(
                    "INSERT INTO metadata(key, value) VALUES('schema_version', ?)",
                    (str(_SCHEMA_VERSION),),
                )
            elif existing[0] != str(_SCHEMA_VERSION):
                raise RuntimeError(
                    f"unsupported coordinator history schema version: {existing[0]}"
                )
            # F20: keep schema-v1 databases readable while adding bounded attempt
            # diagnostics. Older binaries ignore these additive columns.
            columns = {row[1] for row in db.execute("PRAGMA table_info(attempts)")}
            additions = {
                "stdout_tail": "TEXT NOT NULL DEFAULT ''",
                "stderr_tail": "TEXT NOT NULL DEFAULT ''",
                "stdout_truncated": "INTEGER NOT NULL DEFAULT 0",
                "stderr_truncated": "INTEGER NOT NULL DEFAULT 0",
            }
            additions["exception_type"] = "TEXT"
            for name, declaration in additions.items():
                if name not in columns:
                    db.execute(f"ALTER TABLE attempts ADD COLUMN {name} {declaration}")
            if "exception_type" not in {row[1] for row in db.execute("PRAGMA table_info(tasks)")}:
                db.execute("ALTER TABLE tasks ADD COLUMN exception_type TEXT")
        self._secure_sidecars()

    def archive_run(
        self, run: CoordinatorRun, transfers: Iterable[TransferRecord], *, archived_at: float
    ) -> None:
        """Queue an immutable terminal snapshot; SQLite I/O happens off-thread."""
        if not run.status.terminal:
            raise ValueError("only terminal runs may be archived")
        # Preserve durable run-identity rejection synchronously. This bounded
        # read uses the short SQLite timeout and never inherits the old 5s stall.
        self.wait_for_archive(run.run_id, timeout=0.1)
        with self._connect() as db:
            existing = db.execute(
                "SELECT plan_id, program_id FROM runs WHERE run_id = ?", (run.run_id,),
            ).fetchone()
        if existing is not None and existing != (run.plan.id, run.plan.program.id):
            raise ValueError(
                f"durable run identity {run.run_id!r} already belongs to a different execution"
            )
        failure_code = run.failure.code.value if run.failure is not None else None
        failure_detail = run.failure.detail if run.failure is not None else None
        run_transfers = tuple(
            transfer for transfer in transfers
            if transfer.identity.data.run_id == run.run_id
            and transfer.identity.data.plan_id == run.plan.id
        )
        snapshot = _ArchiveSnapshot(
            run=(run.run_id, run.plan.id, run.plan.program.id, run.status.value,
                 failure_code, failure_detail, float(archived_at)),
            tasks=tuple(self._task_row(run.run_id, task) for task in run.tasks.values()),
            attempts=tuple(self._attempt_row(run.run_id, attempt) for attempt in run.attempts.values()),
            transfers=tuple(self._transfer_row(run.run_id, transfer) for transfer in run_transfers),
        )
        with self._archive_lock:
            generation = self._archive_generation.get(run.run_id, 0) + 1
            self._archive_generation[run.run_id] = generation
            self._archive_events[run.run_id] = threading.Event()
            self._archive_errors.pop(run.run_id, None)
        self._archive_queue.put(_ArchiveJob(run.run_id, generation, snapshot))

    def _archive_worker(self) -> None:
        while True:
            job = self._archive_queue.get()
            try:
                try:
                    self._write_snapshot(job.snapshot)
                except Exception as error:
                    with self._archive_lock:
                        if self._archive_generation.get(job.run_id) == job.generation:
                            self._archive_errors[job.run_id] = f"{type(error).__name__}: {error}"
                            self._archive_events[job.run_id].set()
                else:
                    with self._archive_lock:
                        if self._archive_generation.get(job.run_id) == job.generation:
                            self._archive_errors.pop(job.run_id, None)
                            self._archive_events[job.run_id].set()
            finally:
                self._archive_queue.task_done()

    def _write_snapshot(self, snapshot: _ArchiveSnapshot) -> None:
        run_id, plan_id, program_id, _status, _failure_code, _failure_detail, _archived_at = snapshot.run
        with self._connect() as db:
            existing = db.execute(
                "SELECT plan_id, program_id FROM runs WHERE run_id = ?", (run_id,),
            ).fetchone()
            if existing is not None and existing != (plan_id, program_id):
                raise ValueError(
                    f"durable run identity {run_id!r} already belongs to a different execution"
                )
            db.execute(
                """INSERT INTO runs(run_id, plan_id, program_id, status, failure_code,
                       failure_detail, archived_at)
                   VALUES(?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(run_id) DO UPDATE SET
                     plan_id=excluded.plan_id,
                     program_id=excluded.program_id,
                     status=excluded.status,
                     failure_code=excluded.failure_code,
                     failure_detail=excluded.failure_detail,
                     archived_at=excluded.archived_at""",
                snapshot.run,
            )
            db.execute("DELETE FROM tasks WHERE run_id = ?", (run_id,))
            db.execute("DELETE FROM attempts WHERE run_id = ?", (run_id,))
            db.execute("DELETE FROM transfers WHERE run_id = ?", (run_id,))
            db.executemany(
                """INSERT INTO tasks(run_id, task_id, status, committed_attempt_id,
                       failure_kind, failure_message, exception_type)
                   VALUES(?, ?, ?, ?, ?, ?, ?)""",
                snapshot.tasks,
            )
            db.executemany(
                """INSERT INTO attempts(run_id, attempt_id, task_id, worker_id,
                       worker_generation, status, context_id, failure_kind, failure_message,
                       stdout_tail, stderr_tail, stdout_truncated, stderr_truncated,
                       exception_type)
                   VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                snapshot.attempts,
            )
            db.executemany(
                """INSERT INTO transfers(run_id, transfer_id, transfer_attempt_id, value_id,
                       source_worker_id, destination_worker_id, status, failure_detail)
                   VALUES(?, ?, ?, ?, ?, ?, ?, ?)""",
                snapshot.transfers,
            )
        self._secure_sidecars()

    def wait_for_archive(self, run_id: str, timeout: float = 0.2) -> bool:
        with self._archive_lock:
            event = self._archive_events.get(run_id)
        if event is None:
            return False
        event.wait(timeout)
        return self.archive_confirmed(run_id)

    def archive_confirmed(self, run_id: str) -> bool:
        with self._archive_lock:
            event = self._archive_events.get(run_id)
            return bool(event is not None and event.is_set() and run_id not in self._archive_errors)

    def archive_error(self, run_id: str) -> str | None:
        with self._archive_lock:
            return self._archive_errors.get(run_id)

    @staticmethod
    def _task_row(run_id: str, task: TaskRecord) -> tuple[object, ...]:
        return (
            run_id, task.task_id, task.status.value, task.committed_attempt_id,
            task.failure.kind.value if task.failure else None,
            task.failure.message if task.failure else None,
            task.failure.exception_type if task.failure else None,
        )

    @staticmethod
    def _attempt_row(run_id: str, attempt: AttemptRecord) -> tuple[object, ...]:
        return (
            run_id, attempt.identity.attempt_id, attempt.identity.task_id,
            attempt.worker_id, attempt.worker_generation, attempt.status.value,
            attempt.context_id,
            attempt.failure.kind.value if attempt.failure else None,
            attempt.failure.message if attempt.failure else None,
            attempt.stdout_tail, attempt.stderr_tail,
            int(attempt.stdout_truncated), int(attempt.stderr_truncated),
            attempt.failure.exception_type if attempt.failure else None,
        )

    @staticmethod
    def _transfer_row(run_id: str, transfer: TransferRecord) -> tuple[object, ...]:
        identity = transfer.identity
        return (
            run_id, identity.transfer_id, identity.transfer_attempt_id,
            identity.data.value_id, identity.source_worker_id,
            identity.destination_worker_id, transfer.status.value,
            transfer.failure_detail,
        )

    def has_run(self, run_id: str) -> bool:
        self.wait_for_archive(run_id, timeout=0.1)
        with self._connect() as db:
            return db.execute(
                "SELECT 1 FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone() is not None

    def load_run(self, run_id: str) -> StoredRunBundle | None:
        self.wait_for_archive(run_id, timeout=1.0)
        with self._connect() as db:
            row = db.execute(
                "SELECT run_id, plan_id, program_id, status, failure_code, failure_detail, archived_at "
                "FROM runs WHERE run_id = ?", (run_id,),
            ).fetchone()
            if row is None:
                return None
            tasks = tuple(StoredTask(*item) for item in db.execute(
                "SELECT run_id, task_id, status, committed_attempt_id, failure_kind, failure_message, "
                "exception_type FROM tasks WHERE run_id = ? ORDER BY task_id", (run_id,),
            ))
            attempts = tuple(StoredAttempt(*item) for item in db.execute(
                "SELECT run_id, attempt_id, task_id, worker_id, worker_generation, status, context_id, "
                "failure_kind, failure_message, stdout_tail, stderr_tail, "
                "stdout_truncated, stderr_truncated, exception_type FROM attempts "
                "WHERE run_id = ? ORDER BY attempt_id",
                (run_id,),
            ))
            transfers = tuple(StoredTransfer(*item) for item in db.execute(
                "SELECT run_id, transfer_id, transfer_attempt_id, value_id, source_worker_id, "
                "destination_worker_id, status, failure_detail FROM transfers WHERE run_id = ? "
                "ORDER BY transfer_id, transfer_attempt_id", (run_id,),
            ))
            return StoredRunBundle(StoredRun(*row), tasks, attempts, transfers)

    def delete_run(self, run_id: str) -> bool:
        self.wait_for_archive(run_id, timeout=1.0)
        with self._connect() as db:
            cursor = db.execute("DELETE FROM runs WHERE run_id = ?", (run_id,))
            return cursor.rowcount > 0

    def prune(self, keep: int) -> int:
        """Keep only the `keep` most recently archived runs; returns how many went.

        History is diagnostic metadata, so a long-lived host bounds it rather than
        letting the database grow for ever.
        """
        if type(keep) is not int or keep < 0:
            raise ValueError("keep must be a non-negative integer")
        with self._connect() as db:
            cursor = db.execute(
                "DELETE FROM runs WHERE run_id NOT IN "
                "(SELECT run_id FROM runs ORDER BY archived_at DESC, run_id LIMIT ?)", (keep,),
            )
            return cursor.rowcount
