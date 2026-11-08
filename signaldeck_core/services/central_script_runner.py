"""Multi-worker script execution with a single elected runner and a durable queue.

All workers share one SQLite database. A non-blocking OS file lock elects exactly
one worker to execute scripts; all other workers enqueue work and read snapshots.
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from signaldeck_sdk import Cmd

LOG = logging.getLogger(__name__)


def utcnow():
    return datetime.now(timezone.utc).isoformat()


def script_db_path(config_path: str) -> Path:
    return Path(config_path).resolve().parent / "script_jobs.sqlite"


@dataclass
class RunSnapshot:
    run_id: str
    script_name: str
    status: str
    started_at: datetime | None
    finished_at: datetime | None
    result: SimpleNamespace
    secret_variable_names: frozenset[str]
    origin: str = "manual"


def _date(value):
    return datetime.fromisoformat(value).astimezone() if value else None


class ScriptJobStore:
    def __init__(self, path: Path | str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connection() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("""CREATE TABLE IF NOT EXISTS script_schedules (
                id TEXT PRIMARY KEY, script_name TEXT NOT NULL,
                cron TEXT NOT NULL, enabled INTEGER NOT NULL DEFAULT 1,
                variables TEXT NOT NULL DEFAULT '{}',
                overlap_policy TEXT NOT NULL DEFAULT 'skip'
            )""")
            db.execute("""CREATE TABLE IF NOT EXISTS script_jobs (
                run_id TEXT PRIMARY KEY, script_name TEXT NOT NULL,
                status TEXT NOT NULL, requested_at TEXT NOT NULL,
                started_at TEXT, finished_at TEXT,
                variables TEXT NOT NULL, secret_names TEXT NOT NULL DEFAULT '[]',
                state TEXT NOT NULL DEFAULT '[]', origin TEXT NOT NULL,
                schedule_id TEXT, overlap_policy TEXT NOT NULL DEFAULT 'allow',
                stop_requested INTEGER NOT NULL DEFAULT 0
            )""")
            db.execute("CREATE INDEX IF NOT EXISTS script_jobs_recent ON script_jobs(script_name, requested_at DESC)")
            db.execute("CREATE INDEX IF NOT EXISTS script_jobs_pending ON script_jobs(status, requested_at)")
        try:
            os.chmod(self.path, 0o600)
        except OSError:
            pass

    def connection(self):
        db = sqlite3.connect(self.path, timeout=15.0)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA busy_timeout=15000")
        return db

    def list_schedules(self, script_name: str) -> list[dict]:
        with self.connection() as db:
            rows = db.execute(
                "SELECT * FROM script_schedules WHERE script_name=? ORDER BY id",
                (script_name,),
            ).fetchall()
        return [self._schedule(row) for row in rows]

    def schedule(self, schedule_id: str) -> dict | None:
        with self.connection() as db:
            row = db.execute(
                "SELECT * FROM script_schedules WHERE id=?", (schedule_id,)
            ).fetchone()
        return self._schedule(row) if row else None

    @staticmethod
    def _schedule(row):
        return dict(id=row["id"], script_name=row["script_name"],
                    cron=row["cron"], enabled=bool(row["enabled"]),
                    variables=json.loads(row["variables"]),
                    overlap_policy=row["overlap_policy"])

    def put_schedule(self, schedule: dict):
        with self.connection() as db:
            db.execute("""INSERT INTO script_schedules
                (id, script_name, cron, enabled, variables, overlap_policy)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                  script_name=excluded.script_name, cron=excluded.cron,
                  enabled=excluded.enabled, variables=excluded.variables,
                  overlap_policy=excluded.overlap_policy""",
                (schedule["id"], schedule["script_name"], schedule["cron"],
                 int(schedule["enabled"]), json.dumps(schedule["variables"]),
                 schedule["overlap_policy"]))

    def remove_schedule(self, schedule_id: str):
        with self.connection() as db:
            db.execute("DELETE FROM script_schedules WHERE id=?", (schedule_id,))

    def enqueue(self, script_name: str, *, variables=None, origin="manual",
                schedule_id=None, overlap_policy="allow",
                secret_names=()) -> str:
        run_id = uuid.uuid4().hex
        with self.connection() as db:
            db.execute("""INSERT INTO script_jobs
                (run_id, script_name, status, requested_at, variables,
                 secret_names, origin, schedule_id, overlap_policy)
                VALUES (?, ?, 'pending', ?, ?, ?, ?, ?, ?)""",
                (run_id, script_name, utcnow(), json.dumps(variables or {}),
                 json.dumps(sorted(secret_names)), origin, schedule_id, overlap_policy))
        return run_id

    def enqueue_schedule(self, schedule_id: str) -> str:
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM script_schedules WHERE id=? AND enabled=1",
                             (schedule_id,)).fetchone()
            if row is None:
                raise ValueError("Unknown or disabled schedule")
            run_id = uuid.uuid4().hex
            db.execute("""INSERT INTO script_jobs
                (run_id, script_name, status, requested_at, variables,
                 origin, schedule_id, overlap_policy)
                VALUES (?, ?, 'pending', ?, ?, 'cron', ?, ?)""",
                (run_id, row["script_name"], utcnow(), row["variables"],
                 schedule_id, row["overlap_policy"]))
        return run_id

    def claim(self):
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("""SELECT * FROM script_jobs WHERE status='pending'
                               ORDER BY requested_at, rowid LIMIT 1""").fetchone()
            if row is None:
                return None
            if row["overlap_policy"] == "skip":
                running = db.execute("""SELECT 1 FROM script_jobs
                    WHERE script_name=? AND status IN ('running', 'stopping')
                    LIMIT 1""", (row["script_name"],)).fetchone()
                if running:
                    db.execute("""UPDATE script_jobs SET status='skipped', finished_at=?
                                  WHERE run_id=?""", (utcnow(), row["run_id"]))
                    return None
            db.execute("""UPDATE script_jobs SET status='running', started_at=?
                          WHERE run_id=? AND status='pending'""",
                       (utcnow(), row["run_id"]))
            return dict(row)

    def update(self, run_id: str, *, status: str, state=None, finished=False,
               secret_names=None):
        parts = ["status=?"]
        values = [status]
        if state is not None:
            parts.append("state=?")
            values.append(json.dumps(state, ensure_ascii=False))
        if secret_names is not None:
            parts.append("secret_names=?")
            values.append(json.dumps(sorted(secret_names)))
        if finished:
            parts.append("finished_at=?")
            values.append(utcnow())
        values.append(run_id)
        with self.connection() as db:
            db.execute("UPDATE script_jobs SET " + ", ".join(parts) +
                       " WHERE run_id=?", values)

    def cancel(self, run_id: str) -> bool:
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT status FROM script_jobs WHERE run_id=?",
                             (run_id,)).fetchone()
            if row is None or row["status"] not in ("pending", "running", "stopping"):
                return False
            if row["status"] == "pending":
                db.execute("""UPDATE script_jobs SET status='stopped',
                    stop_requested=1, finished_at=? WHERE run_id=?""",
                           (utcnow(), run_id))
            else:
                db.execute("""UPDATE script_jobs SET stop_requested=1,
                    status='stopping' WHERE run_id=?""", (run_id,))
        return True

    def cancellations(self) -> set[str]:
        with self.connection() as db:
            rows = db.execute("""SELECT run_id FROM script_jobs
                WHERE stop_requested=1 AND status='stopping'""").fetchall()
        return {row["run_id"] for row in rows}

    def interrupt_running(self):
        with self.connection() as db:
            db.execute("""UPDATE script_jobs SET status='failed',
                finished_at=? WHERE status IN ('running','stopping')""", (utcnow(),))

    def get_run(self, run_id: str) -> RunSnapshot | None:
        with self.connection() as db:
            row = db.execute("SELECT * FROM script_jobs WHERE run_id=?",
                             (run_id,)).fetchone()
        return self._run(row) if row else None

    def list_runs(self, script_name: str, include_finished=True):
        where = "" if include_finished else " AND status IN ('pending','running','stopping')"
        with self.connection() as db:
            rows = db.execute(
                "SELECT * FROM script_jobs WHERE script_name=?" + where +
                " ORDER BY requested_at DESC, rowid DESC LIMIT 50", (script_name,)
            ).fetchall()
        return [self._run(row) for row in rows]

    @staticmethod
    def _run(row):
        states = json.loads(row["state"])
        for state in states:
            state["date"] = _date(state.get("date"))
        return RunSnapshot(
            run_id=row["run_id"], script_name=row["script_name"],
            status=row["status"], started_at=_date(row["started_at"]),
            finished_at=_date(row["finished_at"]),
            result=SimpleNamespace(state=states, variables=json.loads(row["variables"])),
            secret_variable_names=frozenset(json.loads(row["secret_names"])),
            origin=row["origin"],
        )


class CentralCmd(Cmd):
    """Cmd with shared script scheduling; other registered commands stay local."""

    def __init__(self, loop, *, config_path, scheduler, script_repository, alias_repository):
        super().__init__(loop, script_repository=script_repository,
                         alias_repository=alias_repository)
        self.store = ScriptJobStore(script_db_path(config_path))
        self.scheduler = scheduler
        self.config_path = str(Path(config_path).resolve())
        self._stop_runner = threading.Event()
        self._runner_thread = None
        self._leader_file = None
        self._active = {}

    def getScript(self, name):
        # Scripts saved by a different Gunicorn worker must be visible immediately.
        if self.script_repository and name:
            saved = self.script_repository.get(name)
            if saved is not None:
                return saved
        return super().getScript(name)

    def listScripts(self):
        merged = {s.name: s for s in super().listScripts()}
        if self.script_repository:
            merged.update({s.name: s for s in self.script_repository.list()})
        return [merged[name] for name in sorted(merged)]

    def startScript(self, scriptName, **variables):
        script = self.getScript(scriptName)
        if script is None:
            raise ValueError(f"Unknown script: {scriptName}")
        secret_names = {v.name for v in script.variables if v.type == "secret"}
        rid = self.store.enqueue(scriptName, variables=variables,
                                 secret_names=secret_names)
        return self.store.get_run(rid)

    def runScript(self, scriptName, **variables):
        return self.startScript(scriptName, **variables)

    def getScriptRuns(self, scriptName, *, include_finished=True):
        return self.store.list_runs(scriptName, include_finished=include_finished)

    def getScriptRun(self, run_id):
        return self.store.get_run(run_id)

    def stopRun(self, run_id):
        return self.store.cancel(run_id)

    def stop(self, name):
        for run in self.getScriptRuns(name, include_finished=False):
            self.stopRun(run.run_id)

    def listScriptSchedules(self, script_name):
        return self.store.list_schedules(script_name)

    def saveScriptSchedule(self, script_name, cron, *, schedule_id=None,
                           enabled=True, variables=None, overlap_policy="skip"):
        if self.getScript(script_name) is None:
            raise ValueError("Unknown script")
        if overlap_policy not in ("skip", "allow"):
            raise ValueError("Invalid overlap policy")
        if not isinstance(variables, dict):
            raise ValueError("Schedule variables must be an object")
        script = self.getScript(script_name)
        if any(v.type == "secret" and v.name in variables for v in script.variables):
            raise ValueError("Do not store secret variables in cron schedules")
        schedule_id = schedule_id or uuid.uuid4().hex
        old = self.store.schedule(schedule_id)
        if old and old["script_name"] != script_name:
            raise ValueError("Schedule belongs to another script")
        schedule = dict(id=schedule_id, script_name=script_name, cron=cron,
                        enabled=bool(enabled), variables=variables,
                        overlap_policy=overlap_policy)
        # Validate and install before updating the DB, so invalid cron
        # expressions cannot replace a previously working schedule.
        self.scheduler.set_script_schedule(schedule, config_path=self.config_path)
        try:
            self.store.put_schedule(schedule)
        except Exception:
            if old:
                self.scheduler.set_script_schedule(old, config_path=self.config_path)
            else:
                self.scheduler.remove_script_schedule(schedule_id)
            raise
        return schedule

    def deleteScriptSchedule(self, script_name, schedule_id):
        old = self.store.schedule(schedule_id)
        if old is None or old["script_name"] != script_name:
            raise ValueError("Unknown schedule for script")
        self.scheduler.remove_script_schedule(schedule_id)
        self.store.remove_schedule(schedule_id)

    def start_runner(self):
        if self._runner_thread is not None:
            return
        self._runner_thread = threading.Thread(
            target=self._run_runner, name="SignalDeckScriptRunner", daemon=True)
        self._runner_thread.start()

    def shutdown_runner(self):
        self._stop_runner.set()
        if self._runner_thread:
            self._runner_thread.join(timeout=3)
        if self._leader_file:
            self._unlock()
            self._leader_file.close()
            self._leader_file = None

    def _try_leadership(self):
        if self._leader_file is None:
            self._leader_file = open(str(self.store.path) + ".lock", "a+b")
        try:
            if os.name == "nt":
                import msvcrt
                self._leader_file.seek(0)
                msvcrt.locking(self._leader_file.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self._leader_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (OSError, BlockingIOError):
            return False
        return True

    def _unlock(self):
        try:
            if os.name == "nt":
                import msvcrt
                self._leader_file.seek(0)
                msvcrt.locking(self._leader_file.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(self._leader_file.fileno(), fcntl.LOCK_UN)
        except OSError:
            pass

    def _run_runner(self):
        leader = False
        while not self._stop_runner.is_set():
            try:
                if not leader:
                    leader = self._try_leadership()
                    if leader:
                        LOG.info("This worker is the central Script Runner")
                        self.store.interrupt_running()
                if leader:
                    self._poll()
            except Exception:
                LOG.exception("Central Script Runner iteration failed")
            self._stop_runner.wait(0.4)
        if leader:
            # Mark local runs as interrupted before another worker takes over.
            self.store.interrupt_running()

    @staticmethod
    def _states(run):
        result = []
        for item in run.result.state[-40:]:
            dt = item.get("date")
            result.append({"date": dt.isoformat() if hasattr(dt, "isoformat") else None,
                           "msg": str(item.get("msg", ""))})
        return result

    def _poll(self):
        for external_id, run in list(self._active.items()):
            if external_id in self.store.cancellations() and not run.future.done():
                super().stopRun(run.run_id)
            finished = run.future.done()
            status = run.status
            self.store.update(external_id, status=status, state=self._states(run),
                              finished=finished)
            if finished:
                self._active.pop(external_id, None)

        # Start at most one new queued job per iteration to maintain
        # responsiveness to stop requests and changing schedules.
        job = self.store.claim()
        if job is None:
            return
        try:
            script = self.getScript(job["script_name"])
            if script is None:
                raise ValueError(f"Script {job['script_name']} no longer exists")
            variables = json.loads(job["variables"])
            run = super().startScript(job["script_name"], **variables)
            secrets = {v.name for v in script.variables if v.type == "secret"}
            self.store.update(job["run_id"], status="running", secret_names=secrets)
            self._active[job["run_id"]] = run
        except Exception:
            LOG.exception("Unable to start queued script %s", job["script_name"])
            self.store.update(job["run_id"], status="failed", finished=True)
