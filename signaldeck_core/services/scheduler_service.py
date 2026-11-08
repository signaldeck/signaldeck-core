# signaldeck_core/services/scheduler_service.py
import logging
from crontab import CronTab

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

    def set_script_schedule(self, schedule: dict, *, config_path: str) -> None:
        import shlex
        import sys
        from pathlib import Path

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
        # Preserve all unrelated jobs, including legacy ActionHash cronjobs.
        cron.remove_all(comment=marker)
        if schedule["enabled"]:
            cron.new(command=command, comment=marker).setall(expression)
        cron.write_to_user(user=True)

    def remove_script_schedule(self, schedule_id: str) -> None:
        marker = self._schedule_comment(schedule_id)
        cron = CronTab(user=True)
        cron.remove_all(comment=marker)
        cron.write_to_user(user=True)

    def reconcile_script_schedules(self, store, *, config_path: str) -> None:
        """Restore saved schedules after OS crontab replacement if needed."""
        # Reconciliation is explicit. Normal startup must not rewrite the
        # crontab in every Gunicorn worker.
        for schedule in store.list_all_schedules():
            self.set_script_schedule(schedule, config_path=config_path)
