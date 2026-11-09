# signaldeck_core/services/scheduler_service.py
import logging
import os
import tempfile
from contextlib import contextmanager
from pathlib import Path

from crontab import CronTab


@contextmanager
def _crontab_lock():
    """Serialize crontab read-modify-write across all web workers.

    Locking is per OS user, matching CronTab(user=True). It covers both
    script schedules and legacy ActionHash-based jobs.
    """
    user_id = os.getuid() if hasattr(os, "getuid") else os.environ.get("USERNAME", "user")
    path = Path(tempfile.gettempdir()) / f"signaldeck-crontab-{user_id}.lock"
    flags = os.O_RDWR | os.O_CREAT
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = os.open(path, flags, 0o600)
    try:
        if os.name == "nt":
            import msvcrt
            os.lseek(fd, 0, os.SEEK_SET)
            # msvcrt's non-blocking lock needs retry. Linux cron normally
            # runs on POSIX, where flock blocks until the writer finishes.
            import time
            while True:
                try:
                    msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
                    break
                except OSError:
                    time.sleep(0.05)
        else:
            import fcntl
            fcntl.flock(fd, fcntl.LOCK_EX)
        try:
            yield
        finally:
            if os.name == "nt":
                os.lseek(fd, 0, os.SEEK_SET)
                msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)

class _LockedScriptScheduleEditor:
    """Only usable while SchedulerService.schedule_transaction holds the OS lock."""

    def __init__(self, service):
        self._service = service

    def set_script_schedule(self, schedule: dict, *, config_path: str) -> None:
        self._service._write_script_schedule(schedule, config_path=config_path)

    def remove_script_schedule(self, schedule_id: str) -> None:
        self._service._delete_script_schedule(schedule_id)


class SchedulerService:
    def __init__(self, logger: logging.Logger | None = None) -> None:
        self.logger = logger or logging.getLogger(__name__)

    def get_crons_for_actions(self, actions) -> list[str]:
        cron = CronTab(user=True)
        res: list[str] = []

        for act in actions:
            self.logger.info(f"Search jobs for action {act}")
            jobs = list(cron.find_comment(act.getHash()))

            if len(jobs) > 1:
                self.logger.warning(f"There are multiple cronjobs for action {act.getHash()}")

            if not jobs:
                res.append("")
                continue

            sl = [str(s) for s in jobs[0].slices]
            res.append(" ".join(sl))

        return res

    def set_cron_job(self, action_hash: str, crondef: str | None, base_url: str = "localhost:5000") -> None:
        with _crontab_lock():
            cron = CronTab(user=True)
            cron.remove_all(comment=action_hash)

            job = cron.new(
                command=f"curl {base_url}/run?actionhash={action_hash}",
                comment=action_hash,
            )

            set_time = True
            try:
                job.setall(crondef)
            except Exception:
                set_time = False

            if not crondef or crondef == "" or not set_time or not job.is_valid():
                self.logger.info(f"Disable crontab for action {action_hash}")
                cron.remove_all(comment=action_hash)
                cron.write_to_user(user=True)
                return

            self.logger.info(f"Set crontab for action {action_hash}: {crondef}")
            cron.write_to_user(user=True)

        # Script schedules do not use view ActionHashes. A cron invocation writes a
        # durable queue entry; it never needs access to the HTTP server or its auth.
    @staticmethod
    def _schedule_comment(schedule_id: str) -> str:
        import uuid
        if uuid.UUID(schedule_id).hex != schedule_id:
            raise ValueError("Expected canonical hexadecimal schedule ID")
        return f"signaldeck-script:{schedule_id}"

    @contextmanager
    def schedule_transaction(self):
        """Protect a schedule's SQLite read/write and crontab update together.

        The editor must not outlive this context. Other cron writers (including
        legacy action-based schedules) use the exact same per-user lock.
        """
        with _crontab_lock():
            yield _LockedScriptScheduleEditor(self)

    def _write_script_schedule(self, schedule: dict, *, config_path: str) -> None:
        """Internal implementation; only called under schedule_transaction."""
        import shlex
        import sys

        marker = self._schedule_comment(schedule["id"])
        expression = str(schedule["cron"]).strip()
        if len(expression.split()) != 5:
            raise ValueError("Cron schedule must contain five fields")
        cron = CronTab(user=True)
        command = " ".join(shlex.quote(part) for part in (
            sys.executable, "-m", "signaldeck_core.script_jobs_cli",
            "--config", str(Path(config_path).resolve()),
            "--schedule-id", schedule["id"],
        ))
        job = cron.new(command=command, comment=marker)
        try:
            job.setall(expression)
            if not job.is_valid():
                raise ValueError("Invalid cron expression")
        except Exception as exc:
            raise ValueError(f"Invalid cron expression: {expression}") from exc
        cron.remove_all(comment=marker)
        if schedule["enabled"]:
            cron.new(command=command, comment=marker).setall(expression)
        cron.write_to_user(user=True)

    def _delete_script_schedule(self, schedule_id: str) -> None:
        """Internal implementation; only called under schedule_transaction."""
        marker = self._schedule_comment(schedule_id)
        cron = CronTab(user=True)
        cron.remove_all(comment=marker)
        cron.write_to_user(user=True)

    def set_script_schedule(self, schedule: dict, *, config_path: str) -> None:
        with self.schedule_transaction() as editor:
            editor.set_script_schedule(schedule, config_path=config_path)

    def remove_script_schedule(self, schedule_id: str) -> None:
        with self.schedule_transaction() as editor:
            editor.remove_script_schedule(schedule_id)

    def reconcile_script_schedules(self, store, *, config_path: str) -> None:
        """Reconcile a single consistent snapshot against the user's crontab."""
        with self.schedule_transaction() as editor:
            for schedule in store.list_all_schedules():
                editor.set_script_schedule(schedule, config_path=config_path)
