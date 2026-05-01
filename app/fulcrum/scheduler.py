"""Fulcrum scheduled job orchestration."""

from __future__ import annotations

import logging
from typing import Any, Callable, Iterable

from app.fulcrum.config import Config
from app.fulcrum.jobs import run_gsc_refresh


logger = logging.getLogger(__name__)

CatalogSyncFn = Callable[..., dict[str, Any]]
QueueRunFn = Callable[..., dict[str, Any]]
GscRefreshFn = Callable[[], Any]


def run_fulcrum_daily_sync(
    *,
    allowed_stores: Iterable[str],
    gsc_refresh: GscRefreshFn = run_gsc_refresh,
    sync_catalog_profiles: CatalogSyncFn | None = None,
    job_logger: logging.Logger | None = None,
) -> None:
    job_logger = job_logger or logger
    if sync_catalog_profiles is None:
        from app.fulcrum.services import sync_store_catalog_profiles as default_sync_catalog_profiles

        sync_catalog_profiles = default_sync_catalog_profiles

    try:
        rows = gsc_refresh()
        job_logger.info("Fulcrum daily GSC refresh completed with %s rows.", rows)
    except Exception:  # noqa: BLE001
        job_logger.exception("Fulcrum daily GSC refresh failed.")

    for store_hash in tuple(allowed_stores):
        try:
            result = sync_catalog_profiles(store_hash, initiated_by="fulcrum-scheduler")
            job_logger.info(
                "Fulcrum daily catalog sync completed for %s: %s products, %s categories.",
                store_hash,
                result.get("synced_products"),
                result.get("synced_categories"),
            )
        except Exception:  # noqa: BLE001
            job_logger.exception("Fulcrum daily catalog sync failed for %s.", store_hash)


def run_fulcrum_weekly_generation(
    *,
    allowed_stores: Iterable[str],
    queue_run: QueueRunFn | None = None,
    job_logger: logging.Logger | None = None,
) -> None:
    job_logger = job_logger or logger
    if queue_run is None:
        from app.fulcrum.services import queue_candidate_run as default_queue_run

        queue_run = default_queue_run

    for store_hash in tuple(allowed_stores):
        try:
            result = queue_run(
                store_hash=store_hash,
                initiated_by="fulcrum-scheduler",
                cluster=None,
                run_source="scheduler",
            )
            job_logger.info(
                "Fulcrum weekly generation queued for %s: run %s (%s).",
                store_hash,
                result.get("run_id"),
                result.get("status"),
            )
        except Exception:  # noqa: BLE001
            job_logger.exception("Fulcrum weekly generation failed for %s.", store_hash)


def register_fulcrum_jobs(
    scheduler: Any,
    *,
    config: type[Config] = Config,
    gsc_refresh: GscRefreshFn = run_gsc_refresh,
    sync_catalog_profiles: CatalogSyncFn | None = None,
    queue_run: QueueRunFn | None = None,
    job_logger: logging.Logger | None = None,
) -> Any:
    job_logger = job_logger or logger
    allowed_stores = tuple(config.FULCRUM_ALLOWED_STORES or [])

    scheduler.add_job(
        func=lambda: run_fulcrum_daily_sync(
            allowed_stores=allowed_stores,
            gsc_refresh=gsc_refresh,
            sync_catalog_profiles=sync_catalog_profiles,
            job_logger=job_logger,
        ),
        trigger="cron",
        hour=config.FULCRUM_DAILY_SYNC_HOUR,
        minute=config.FULCRUM_DAILY_SYNC_MINUTE,
        id="fulcrum_daily_sync",
        replace_existing=True,
    )
    scheduler.add_job(
        func=lambda: run_fulcrum_weekly_generation(
            allowed_stores=allowed_stores,
            queue_run=queue_run,
            job_logger=job_logger,
        ),
        trigger="cron",
        day_of_week=config.FULCRUM_WEEKLY_GENERATION_DAY,
        hour=config.FULCRUM_WEEKLY_GENERATION_HOUR,
        minute=config.FULCRUM_WEEKLY_GENERATION_MINUTE,
        id="fulcrum_weekly_generation",
        replace_existing=True,
    )
    return scheduler


def create_embedded_scheduler(*, config: type[Config] = Config, job_logger: logging.Logger | None = None):
    from apscheduler.schedulers.background import BackgroundScheduler

    scheduler = BackgroundScheduler()
    return register_fulcrum_jobs(scheduler, config=config, job_logger=job_logger)


def start_embedded_scheduler(*, config: type[Config] = Config, job_logger: logging.Logger | None = None):
    scheduler = create_embedded_scheduler(config=config, job_logger=job_logger)
    scheduler.start()
    return scheduler


def run_standalone_scheduler(*, config: type[Config] = Config, job_logger: logging.Logger | None = None) -> int:
    job_logger = job_logger or logger
    if not config.ENABLE_SCHEDULER:
        job_logger.warning("Fulcrum scheduler is disabled via ENABLE_SCHEDULER=0.")
        return 0

    from apscheduler.schedulers.blocking import BlockingScheduler

    scheduler = BlockingScheduler()
    register_fulcrum_jobs(scheduler, config=config, job_logger=job_logger)
    job_logger.info("Starting standalone Fulcrum scheduler.")
    scheduler.start()
    return 0
