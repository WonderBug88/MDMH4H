from __future__ import annotations

import argparse
import json

from app.fulcrum import services


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the Fulcrum AI reviewer against query-family gate rows.",
    )
    parser.add_argument("--store-hash", required=True, help="BigCommerce store hash, for example 99oa2tso")
    parser.add_argument(
        "--disposition",
        default="all",
        choices=["all", "pass", "hold", "reject"],
        help="Which gate disposition to review. Defaults to all.",
    )
    parser.add_argument("--run-id", type=int, default=None, help="Optional gate run id. Defaults to latest completed run.")
    parser.add_argument("--limit", type=int, default=40, help="Maximum gate rows to review. Defaults to 40.")
    parser.add_argument("--json", action="store_true", help="Emit the full JSON result.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    disposition = None if args.disposition == "all" else args.disposition

    result = services.run_query_gate_agent_review(
        store_hash=args.store_hash,
        run_id=args.run_id,
        disposition=disposition,
        limit=max(int(args.limit or 40), 1),
        initiated_by="cli-agent-review",
    )

    if args.json:
        print(json.dumps(result, indent=2, default=str))
        return 0

    print("Fulcrum Gate AI Review")
    print(f"store_hash: {args.store_hash}")
    print(f"run_id: {result.get('run_id') or 'n/a'}")
    print(f"status: {result.get('status') or 'unknown'}")
    if result.get("reason"):
        print(f"reason: {result['reason']}")
    print(f"reviewed_count: {result.get('reviewed_count') or 0}")
    print(f"stored_count: {result.get('stored_count') or 0}")

    summary = result.get("summary") or {}
    print(
        "summary: "
        f"correct={summary.get('correct') or 0}, "
        f"incorrect={summary.get('incorrect') or 0}, "
        f"unclear={summary.get('unclear') or 0}"
    )
    clusters = result.get("clusters") or []
    if clusters:
        print("")
        print("top_clusters:")
        for cluster in clusters[:5]:
            print(
                "- "
                f"{cluster.get('cluster_label') or ''} "
                f"({cluster.get('review_count') or 0})"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
