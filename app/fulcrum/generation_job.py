import argparse
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from app.fulcrum.services import apply_runtime_schema, execute_candidate_run  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Execute one queued Fulcrum generation run.")
    parser.add_argument("--run-id", type=int, required=True, help="Run id from app_runtime.link_runs")
    args = parser.parse_args()

    apply_runtime_schema()
    execute_candidate_run(args.run_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
