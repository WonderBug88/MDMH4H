import argparse
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from app.fulcrum.services import (  # noqa: E402
    apply_runtime_schema,
    build_operational_snapshot,
    list_query_gate_review_requests,
    list_runs,
    refresh_store_readiness,
    summarize_live_publications,
    summarize_query_gate_dispositions,
)


def _format_watchdog_text(payload: dict) -> str:
    lines = [
        f"Store: {payload.get('store_hash')}",
        f"Status: {payload.get('overall_status_label')} ({payload.get('overall_status')})",
        (
            "Counts: "
            f"urgent={payload.get('counts', {}).get('urgent', 0)} "
            f"watch={payload.get('counts', {}).get('watch', 0)} "
            f"healthy={payload.get('counts', {}).get('healthy', 0)}"
        ),
        "",
        "Alerts:",
    ]
    for alert in payload.get("alerts", []):
        lines.append(f"- [{(alert.get('severity') or '').upper()}] {alert.get('title')}: {alert.get('detail')}")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Fulcrum operational watchdog checks for one store.")
    parser.add_argument("--store-hash", required=True, help="BigCommerce store hash")
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of text")
    parser.add_argument("--strict", action="store_true", help="Exit non-zero on watch or urgent conditions")
    args = parser.parse_args()

    apply_runtime_schema()
    store_hash = args.store_hash
    runs = list_runs(store_hash, limit=10)
    readiness = refresh_store_readiness(store_hash)
    publication_summary = summarize_live_publications(store_hash)
    edge_case_requests = list_query_gate_review_requests(store_hash, request_status="requested", limit=20)
    gate_summary = summarize_query_gate_dispositions(store_hash)
    payload = build_operational_snapshot(
        store_hash,
        runs=runs,
        readiness=readiness,
        publication_summary=publication_summary,
        edge_case_requests=edge_case_requests,
        gate_summary=gate_summary,
    )
    payload["store_hash"] = store_hash

    if args.json:
        print(json.dumps(payload, indent=2, default=str))
    else:
        print(_format_watchdog_text(payload))

    status = (payload.get("overall_status") or "").strip().lower()
    if status == "urgent":
        return 2
    if args.strict and status == "watch":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
