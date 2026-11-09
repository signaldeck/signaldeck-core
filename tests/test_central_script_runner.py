import asyncio
from contextlib import contextmanager
import json
import tempfile
import threading
import time
import unittest
from concurrent.futures import Future
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from pathlib import Path
from unittest.mock import patch

from crontab import CronTab
from signaldeck_core.services.scheduler_service import SchedulerService

from signaldeck_core.services.central_script_runner import (
    CentralCmd, ScriptJobStore, script_db_path,
)


class FakeScheduler:
    def __init__(self):
        self.schedules = {}
        self._lock = threading.RLock()

    @contextmanager
    def schedule_transaction(self):
        with self._lock:
            yield self

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

    def test_scheduler_writes_local_cli_cron_command(self):
        memory_cron = CronTab(tab="")
        schedule = {
            "id": "0123456789abcdef0123456789abcdef",
            "script_name": "hello", "cron": "0 7 * * *",
            "enabled": True, "variables": {}, "overlap_policy": "skip",
        }
        service = SchedulerService()
        with patch("signaldeck_core.services.scheduler_service.CronTab",
                   return_value=memory_cron), patch.object(memory_cron, "write_to_user"):
            service.set_script_schedule(schedule, config_path=self.config_path)
            jobs = list(memory_cron.find_comment("signaldeck-script:" + schedule["id"]))
            self.assertEqual(len(jobs), 1)
            self.assertIn("signaldeck_core.script_jobs_cli", jobs[0].command)
            self.assertIn("--schedule-id", jobs[0].command)
            self.assertNotIn("/run", jobs[0].command)
            schedule["enabled"] = False
            service.set_script_schedule(schedule, config_path=self.config_path)
            self.assertEqual(
                list(memory_cron.find_comment("signaldeck-script:" + schedule["id"])),
                [])

    def test_stop_request_survives_stale_running_status_update(self):
        store = ScriptJobStore(self.db_path)
        run_id = store.enqueue("hello")
        store.claim()
        self.assertTrue(store.cancel(run_id))
        # The runner still holds a snapshot from before the stop request.
        store.update(run_id, status="running", state=[])
        self.assertEqual(store.get_run(run_id).status, "stopping")
        self.assertIn(run_id, store.cancellations())
        # After the actual future has stopped, the final state is terminal.
        store.update(run_id, status="stopped", finished=True)
        self.assertEqual(store.get_run(run_id).status, "stopped")

    def test_runner_drain_waits_for_real_execution_before_handover(self):
        from unittest.mock import patch
        from signaldeck_sdk import Cmd

        scheduler = FakeScheduler()
        leader = CentralCmd(self.loop, config_path=self.config_path,
                            scheduler=scheduler, script_repository=None,
                            alias_repository=None)
        follower = CentralCmd(self.loop, config_path=self.config_path,
                              scheduler=scheduler, script_repository=None,
                              alias_repository=None)
        self.assertTrue(leader._try_leadership())
        job_id = leader.store.enqueue("hello")
        leader.store.claim()

        future = Future()
        run = SimpleNamespace(
            run_id="local-job", script_name="hello", future=future,
            status="running", result=SimpleNamespace(state=[]),
        )
        leader._active[job_id] = run
        cancel_seen = threading.Event()
        drain_done = threading.Event()

        def cancel_run(_cmd, run_id):
            self.assertEqual(run_id, "local-job")
            cancel_seen.set()

        def drain():
            try:
                leader._drain_active()
            finally:
                drain_done.set()

        with patch.object(Cmd, "stopRun", cancel_run):
            thread = threading.Thread(target=drain, daemon=True)
            thread.start()
            try:
                self.assertTrue(cancel_seen.wait(timeout=2))
                self.assertFalse(drain_done.wait(timeout=0.15))
                self.assertFalse(follower._try_leadership())
                run.status = "stopped"
                future.set_result(None)
                self.assertTrue(drain_done.wait(timeout=2))
                # Leadership has not been released by draining itself.
                self.assertFalse(follower._try_leadership())
            finally:
                future.cancel()
                thread.join(timeout=2)
                leader._unlock()
                leader._leader_file.close()
                follower._leader_file.close()

    def test_stop_all_cancels_more_than_fifty_pending_runs(self):
        scheduler = FakeScheduler()
        cmd = CentralCmd(self.loop, config_path=self.config_path,
                         scheduler=scheduler, script_repository=None,
                         alias_repository=None)
        cmd.registerScript({"name": "bulk", "commands": ["echo test"]})
        job_ids = [cmd.store.enqueue("bulk") for _ in range(75)]
        # UI history remains deliberately limited; stop(name) must not be.
        self.assertEqual(len(cmd.getScriptRuns("bulk", include_finished=False)), 50)
        cmd.stop("bulk")
        self.assertTrue(all(cmd.store.get_run(job_id).status == "stopped"
                            for job_id in job_ids))

    def test_concurrent_schedule_updates_preserve_both_cron_entries(self):
        """Two concurrent writers cannot overwrite each other's crontab."""
        from signaldeck_core.services import scheduler_service
        from threading import Event, Thread

        state = {"text": ""}
        first_writer_started = Event()
        release_first_writer = Event()
        second_writer_finished = Event()
        count = [0]
        original_crontab = CronTab

        def fake_crontab(*args, **kwargs):
            assert kwargs.get("user") is True
            count[0] += 1
            index = count[0]
            cron = original_crontab(tab=state["text"])

            def save(*, user=True):
                if index == 1:
                    first_writer_started.set()
                    if not release_first_writer.wait(timeout=3):
                        raise TimeoutError("First writer never released")
                state["text"] = str(cron)

            cron.write_to_user = save
            return cron

        service = SchedulerService()
        errors = []
        def save_schedule(schedule_id):
            try:
                service.set_script_schedule(
                    {"id": schedule_id, "cron": "0 7 * * *", "enabled": True},
                    config_path=self.config_path,
                )
            except Exception as exc:
                errors.append(exc)
            finally:
                if schedule_id == "b" * 32:
                    second_writer_finished.set()

        with patch.object(scheduler_service, "CronTab", side_effect=fake_crontab):
            first = Thread(target=save_schedule, args=("a" * 32,))
            second = Thread(target=save_schedule, args=("b" * 32,))
            first.start()
            try:
                self.assertTrue(first_writer_started.wait(timeout=3))
                second.start()
                self.assertFalse(second_writer_finished.wait(timeout=0.15))
            finally:
                release_first_writer.set()
                first.join(timeout=3)
                if second.ident is not None:
                    second.join(timeout=3)
        self.assertFalse(errors, errors)
        self.assertEqual(count[0], 2)
        written = original_crontab(tab=state["text"])
        self.assertEqual(len(list(written.find_comment("signaldeck-script:" + "a" * 32))), 1)
        self.assertEqual(len(list(written.find_comment("signaldeck-script:" + "b" * 32))), 1)

    def test_long_running_completion_is_not_immediately_pruned(self):
        store = ScriptJobStore(self.db_path)
        long_running_id = store.enqueue("hello")
        store.claim()
        # Many newer requests finish before the oldest running job completes.
        for _ in range(220):
            run_id = store.enqueue("hello")
            job = store.claim()
            self.assertEqual(job["run_id"], run_id)
            store.update(run_id, status="completed", finished=True)
        store.update(long_running_id, status="completed", finished=True)
        store.prune_history("hello", keep=200)
        self.assertIsNotNone(store.get_run(long_running_id))

    def test_subscriber_missing_result_finishes_with_error(self):
        worker = CentralCmd(self.loop, config_path=self.config_path,
                            scheduler=FakeScheduler(), script_repository=None,
                            alias_repository=None)
        missing_id = "missing"
        future = Future()
        worker._subscribers[missing_id] = future
        worker._poll_subscribers()
        self.assertTrue(future.done())
        with self.assertRaisesRegex(RuntimeError, "no longer exists"):
            future.result()
        self.assertNotIn(missing_id, worker._subscribers)

    def test_schedule_persistence_and_crontab_share_one_lock(self):
        class BlockingScheduler(FakeScheduler):
            def __init__(self):
                super().__init__()
                self.first_cron_written = threading.Event()
                self.allow_first_persist = threading.Event()
                self.first = None

            def set_script_schedule(self, schedule, *, config_path):
                super().set_script_schedule(schedule, config_path=config_path)
                if schedule["cron"] == "0 1 * * *":
                    self.first_cron_written.set()
                    if not self.allow_first_persist.wait(timeout=3):
                        raise TimeoutError("Concurrent schedule test failed")

        scheduler = BlockingScheduler()
        first = CentralCmd(self.loop, config_path=self.config_path,
                           scheduler=scheduler, script_repository=None,
                           alias_repository=None)
        second = CentralCmd(self.loop, config_path=self.config_path,
                            scheduler=scheduler, script_repository=None,
                            alias_repository=None)
        for worker in (first, second):
            worker.registerScript({"name": "hello", "commands": ["echo hello"]})
        existing = first.saveScriptSchedule("hello", "0 7 * * *")
        schedule_id = existing["id"]
        errors = []
        second_done = threading.Event()

        def save(worker, cron):
            try:
                worker.saveScriptSchedule("hello", cron, schedule_id=schedule_id)
            except Exception as exc:
                errors.append(exc)
            finally:
                if cron == "0 2 * * *":
                    second_done.set()

        a = threading.Thread(target=save, args=(first, "0 1 * * *"))
        b = threading.Thread(target=save, args=(second, "0 2 * * *"))
        a.start()
        try:
            self.assertTrue(scheduler.first_cron_written.wait(timeout=3))
            b.start()
            self.assertFalse(second_done.wait(timeout=0.15))
        finally:
            scheduler.allow_first_persist.set()
            a.join(timeout=3)
            if b.ident is not None:
                b.join(timeout=3)
        self.assertFalse(errors, errors)
        self.assertEqual(first.store.schedule(schedule_id)["cron"], "0 2 * * *")
        self.assertEqual(scheduler.schedules[schedule_id]["cron"], "0 2 * * *")

    def test_disabled_schedule_cannot_be_enqueued(self):
        store = ScriptJobStore(self.db_path)
        store.put_schedule({
            "id": "disabled", "script_name": "hello", "cron": "0 7 * * *",
            "enabled": False, "variables": {}, "overlap_policy": "skip",
        })
        with self.assertRaises(ValueError):
            store.enqueue_schedule("disabled")

    def test_shutdown_settles_finished_and_queued_subscribers(self):
        cmd = CentralCmd(self.loop, config_path=self.config_path,
                         scheduler=FakeScheduler(), script_repository=None,
                         alias_repository=None)
        completed_id = cmd.store.enqueue("test")
        cmd.store.claim()
        cmd.store.update(completed_id, status="completed", finished=True)
        pending_id = cmd.store.enqueue("test")
        finished_future = Future()
        waiting_future = Future()
        cmd._subscribers[completed_id] = finished_future
        cmd._subscribers[pending_id] = waiting_future

        # A non-leader worker can be reinitialized while another worker
        # is responsible for the pending execution.
        cmd.shutdown_runner()
        self.assertTrue(finished_future.done())
        self.assertTrue(finished_future.result().isFinished())
        self.assertTrue(waiting_future.done())
        with self.assertRaisesRegex(RuntimeError, "shut down"):
            waiting_future.result()
        self.assertEqual(cmd.store.get_run(pending_id).status, "pending")
        self.assertEqual(cmd._subscribers, {})

    def test_runner_shutdown_drains_and_settles_active_subscriber(self):
        from unittest.mock import patch
        from signaldeck_sdk import Cmd
        cmd = CentralCmd(self.loop, config_path=self.config_path,
                         scheduler=FakeScheduler(), script_repository=None,
                         alias_repository=None)
        run_id = cmd.store.enqueue("test")
        cmd.store.claim()
        local_future = Future()
        local_run = SimpleNamespace(
            run_id="local", script_name="test", future=local_future,
            status="running", result=SimpleNamespace(state=[]),
        )
        cmd._active[run_id] = local_run
        subscriber = Future()
        cmd._subscribers[run_id] = subscriber

        def stop_local(_cmd, local_id):
            self.assertEqual(local_id, "local")
            local_run.status = "stopped"
            local_future.set_result(None)

        with patch.object(Cmd, "stopRun", stop_local):
            cmd._drain_active()
            cmd._settle_shutdown_subscribers()
        self.assertTrue(subscriber.done())
        self.assertTrue(subscriber.cancelled())
        self.assertEqual(cmd._subscribers, {})

    def test_shutting_down_rejects_new_subscribers(self):
        cmd = CentralCmd(self.loop, config_path=self.config_path,
                         scheduler=FakeScheduler(), script_repository=None,
                         alias_repository=None)
        cmd.registerScript({"name": "test", "commands": ["echo test"]})
        cmd.shutdown_runner()
        with self.assertRaisesRegex(RuntimeError, "shutting down"):
            cmd.runScript("test")
        self.assertEqual(cmd.store.list_runs("test"), [])

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
