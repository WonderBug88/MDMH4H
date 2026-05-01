import logging
import os

from app.fulcrum.scheduler import run_standalone_scheduler


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    os.environ["FULCRUM_RUN_EMBEDDED_SCHEDULER"] = "0"
    return run_standalone_scheduler(job_logger=logging.getLogger("fulcrum.scheduler"))


if __name__ == "__main__":
    raise SystemExit(main())
