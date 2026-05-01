"""Process queued Route Authority Google integration sync runs."""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main() -> int:
    parser = argparse.ArgumentParser(description="Process Fulcrum GA4/GSC integration sync queue.")
    parser.add_argument("--once", action="store_true", help="Process the current queue once and exit.")
    parser.add_argument("--limit", type=int, default=5, help="Maximum runs to process per polling cycle.")
    parser.add_argument("--sleep", type=float, default=10.0, help="Seconds to wait between empty polling cycles.")
    parser.add_argument("--store-hash", default="", help="Optional store hash filter.")
    parser.add_argument("--integration-key", choices=["gsc", "ga4"], default=None, help="Optional provider filter.")
    parser.add_argument("--expire-running-after-minutes", type=int, default=30, help="Mark abandoned running syncs failed after this many minutes.")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    logger = logging.getLogger("fulcrum.integration_sync_worker")
    os.environ.setdefault("FULCRUM_RUN_EMBEDDED_SCHEDULER", "0")

    from app.fulcrum.services import apply_runtime_schema, process_queued_integration_syncs

    apply_runtime_schema()
    logger.info("Starting Fulcrum integration sync worker.")
    while True:
        result = process_queued_integration_syncs(
            limit=args.limit,
            store_hash=args.store_hash or None,
            integration_key=args.integration_key,
            expire_running_after_minutes=args.expire_running_after_minutes,
        )
        processed_count = int(result.get("processed_count") or 0)
        expired_count = int(result.get("expired_count") or 0)
        if expired_count:
            logger.warning("Expired %s stale running integration sync run(s).", expired_count)
        if processed_count:
            logger.info("Processed %s integration sync run(s).", processed_count)
        if args.once:
            return 0
        time.sleep(1.0 if processed_count else max(args.sleep, 1.0))


if __name__ == "__main__":
    raise SystemExit(main())
