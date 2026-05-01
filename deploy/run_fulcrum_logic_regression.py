import argparse
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from app.fulcrum.logic_regression import (
    format_logic_regression_json,
    format_logic_regression_report,
    record_regression_against_logic_changelog,
    run_logic_regression,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Fulcrum deterministic logic regression checks.")
    parser.add_argument("--store-hash", required=True, help="BigCommerce store hash")
    parser.add_argument("--run-id", type=int, default=None, help="Optional gate run id")
    parser.add_argument(
        "--case-id",
        action="append",
        dest="case_ids",
        default=None,
        help="Optional case id filter; may be passed multiple times",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of text")
    parser.add_argument("--strict", action="store_true", help="Exit non-zero when any case fails")
    parser.add_argument(
        "--record-changelog",
        action="store_true",
        help="Write the regression outcome back onto each changelog entry using its affected queries",
    )
    args = parser.parse_args()

    payload = run_logic_regression(
        store_hash=args.store_hash,
        run_id=args.run_id,
        case_ids=args.case_ids,
    )
    changelog_result = None
    if args.record_changelog:
        changelog_result = record_regression_against_logic_changelog(payload)
        payload["changelog_recording"] = changelog_result
    if args.json:
        print(format_logic_regression_json(payload))
    else:
        print(format_logic_regression_report(payload))
        if changelog_result:
            print("")
            print(
                f"Changelog recording: {changelog_result.get('status')} "
                f"({changelog_result.get('updated_count', 0)} updated)"
            )
    if args.strict and payload.get("status") != "ok":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
