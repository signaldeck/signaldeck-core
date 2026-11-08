import asyncio
import json
import tempfile
import threading
import time
import unittest
from pathlib import Path

from signaldeck_core.services.central_script_runner import (
    CentralCmd, ScriptJobStore, script_db_path,
)


class FakeScheduler:
    def __init__(self):
        self.schedules = {}

    def set_script_schedule(self, schedule, *, config_path):
        if len(schedule["cron"].split()) != 5:
            raise ValueError("Invalid cron")
        self.schedules[schedule["id"]] = schedule

    def remove_script_schedule(self, schedule_id):
        self.schedules.pop(schedule_id, None)


class CentralScriptRunnerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.config_path = str(Path(self.temp.name) / "config.json")
        Path(self.config_path).write_text(
            json.dumps({"cmd": {"jobs_db": "state/jobs.sqlite"}}),
            encoding="utf-8",
        )
        self.db_path = script_db_path(self.config_path)
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self.loop.run_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.stop_loop)

    def stop_loop(self):
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(timeout=3)
        self.loop.close()

    def test_sqlite_store_persists_schedules_across_instances(self):
        first = ScriptJobStore(self.db_path)
        schedule = {
            "id": "test-id", "script_name": "hello", "cron": "0 7 * * *",
            "enabled": True, "variables": {"x": 42},
            "overlap_policy": "skip",
        }
        first.put_schedule(schedule)
        second = ScriptJobStore(self.db_path)
        self.assertEqual(second.schedule("test-id"), schedule)
        rid = second.enqueue_schedule("test-id")
        self.assertEqual(first.get_run(rid).status, "pending")

    def test_disabled_schedule_cannot_be_enqueued(self):
        store = ScriptJobStore(self.db_path)
        store.put_schedule({
            "id": "disabled", "script_name": "hello", "cron": "0 7 * * *",
            "enabled": False, "variables": {}, "overlap_policy": "skip",
        })
        with self.assertRaises(ValueError):
            store.enqueue_schedule("disabled")

    def test_only_one_leader_executes_shared_job(self):
        scheduler = FakeScheduler()
        workers = [
            CentralCmd(self.loop, config_path=self.config_path, scheduler=scheduler,
                       script_repository=None, alias_repository=None)
            for _ in range(2)
        ]
        for worker in workers:
            worker.registerScript({"name": "hello", "commands": ["echo hello"]})
        for worker in workers:
            worker.start_runner()
        try:
            queued = workers[1].startScript("hello")
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                run = workers[0].getScriptRun(queued.run_id)
                if run.status in ("completed", "failed", "stopped"):
                    break
                time.sleep(0.1)
            self.assertEqual(run.status, "completed")
            self.assertEqual(len(run.result.state), 1)
            self.assertIn("hello", run.result.state[0]["msg"])
            self.assertEqual(len(workers[1].getScriptRuns("hello")), 1)
        finally:
            for worker in workers:
                worker.shutdown_runner()

    def test_queued_jobs_survive_runner_handover(self):
        scheduler = FakeScheduler()
        first = CentralCmd(self.loop, config_path=self.config_path,
                           scheduler=scheduler, script_repository=None, alias_repository=None)
        second = CentralCmd(self.loop, config_path=self.config_path,
                            scheduler=scheduler, script_repository=None, alias_repository=None)
        for worker in (first, second):
            worker.registerScript({"name": "hello", "commands": ["echo takeover"]})
        first.start_runner()
        try:
            # First worker has time to acquire the file lock.
            time.sleep(0.7)
            second.start_runner()
            first.shutdown_runner()
            queued = second.startScript("hello")
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                run = second.getScriptRun(queued.run_id)
                if run.status in ("completed", "failed", "stopped"):
                    break
                time.sleep(0.1)
            self.assertEqual(run.status, "completed")
        finally:
            first.shutdown_runner()
            second.shutdown_runner()

    def test_schedules_are_per_script_and_reject_secret_variables(self):
        scheduler = FakeScheduler()
        cmd = CentralCmd(self.loop, config_path=self.config_path,
                         scheduler=scheduler, script_repository=None, alias_repository=None)
        cmd.registerScript({"name": "sensitive", "commands": ["echo test"],
                            "variables": [{"name": "token", "type": "secret"}]})
        with self.assertRaises(ValueError):
            cmd.saveScriptSchedule("sensitive", "0 7 * * *", variables={"token": "secret"})
        schedule = cmd.saveScriptSchedule("sensitive", "0 7 * * *")
        self.assertEqual(len(schedule["id"]), 32)
        self.assertEqual(len(cmd.listScriptSchedules("sensitive")), 1)
        cmd.deleteScriptSchedule("sensitive", schedule["id"])
        self.assertEqual(cmd.listScriptSchedules("sensitive"), [])


if __name__ == "__main__":
    unittest.main()
