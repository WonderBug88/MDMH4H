import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

APP_ROOT = ROOT / "MDMH4H"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from app.fulcrum.scheduler import (
    register_fulcrum_jobs,
    run_fulcrum_daily_sync,
    run_fulcrum_weekly_generation,
)


class _FakeLogger:
    def __init__(self):
        self.info_calls = []
        self.exception_calls = []
        self.warning_calls = []

    def info(self, message, *args):
        self.info_calls.append((message, args))

    def exception(self, message, *args):
        self.exception_calls.append((message, args))

    def warning(self, message, *args):
        self.warning_calls.append((message, args))


class _FakeScheduler:
    def __init__(self):
        self.jobs = []

    def add_job(self, **kwargs):
        self.jobs.append(kwargs)


class FulcrumSchedulerTests(unittest.TestCase):
    def test_run_fulcrum_daily_sync_continues_after_store_error(self):
        logger = _FakeLogger()
        sync_calls = []

        def _sync_catalog(store_hash, initiated_by=None):
            sync_calls.append((store_hash, initiated_by))
            if store_hash == "store-b":
                raise RuntimeError("boom")
            return {"synced_products": 4, "synced_categories": 2}

        run_fulcrum_daily_sync(
            allowed_stores=["store-a", "store-b", "store-c"],
            gsc_refresh=lambda: 12,
            sync_catalog_profiles=_sync_catalog,
            job_logger=logger,
        )

        self.assertEqual(
            sync_calls,
            [
                ("store-a", "fulcrum-scheduler"),
                ("store-b", "fulcrum-scheduler"),
                ("store-c", "fulcrum-scheduler"),
            ],
        )
        self.assertEqual(len(logger.exception_calls), 1)
        self.assertTrue(any("daily GSC refresh completed" in call[0] for call in logger.info_calls))

    def test_run_fulcrum_weekly_generation_queues_all_stores(self):
        logger = _FakeLogger()
        queue_calls = []

        def _queue_run(**kwargs):
            queue_calls.append(kwargs)
            return {"run_id": 99, "status": "queued"}

        run_fulcrum_weekly_generation(
            allowed_stores=["store-a", "store-b"],
            queue_run=_queue_run,
            job_logger=logger,
        )

        self.assertEqual(
            queue_calls,
            [
                {
                    "store_hash": "store-a",
                    "initiated_by": "fulcrum-scheduler",
                    "cluster": None,
                    "run_source": "scheduler",
                },
                {
                    "store_hash": "store-b",
                    "initiated_by": "fulcrum-scheduler",
                    "cluster": None,
                    "run_source": "scheduler",
                },
            ],
        )
        self.assertEqual(len(logger.exception_calls), 0)

    def test_register_fulcrum_jobs_uses_config_schedule_values(self):
        class _Config:
            FULCRUM_ALLOWED_STORES = ["alpha", "beta"]
            FULCRUM_DAILY_SYNC_HOUR = 1
            FULCRUM_DAILY_SYNC_MINUTE = 15
            FULCRUM_WEEKLY_GENERATION_DAY = "sun"
            FULCRUM_WEEKLY_GENERATION_HOUR = 2
            FULCRUM_WEEKLY_GENERATION_MINUTE = 45

        scheduler = _FakeScheduler()
        register_fulcrum_jobs(scheduler, config=_Config, job_logger=_FakeLogger())

        self.assertEqual(len(scheduler.jobs), 2)
        self.assertEqual(scheduler.jobs[0]["id"], "fulcrum_daily_sync")
        self.assertEqual(scheduler.jobs[0]["hour"], 1)
        self.assertEqual(scheduler.jobs[0]["minute"], 15)
        self.assertEqual(scheduler.jobs[1]["id"], "fulcrum_weekly_generation")
        self.assertEqual(scheduler.jobs[1]["day_of_week"], "sun")
        self.assertEqual(scheduler.jobs[1]["hour"], 2)
        self.assertEqual(scheduler.jobs[1]["minute"], 45)


if __name__ == "__main__":
    unittest.main()
