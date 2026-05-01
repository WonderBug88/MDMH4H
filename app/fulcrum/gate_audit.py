from __future__ import annotations

import argparse
import json
from typing import Any

from app.fulcrum import services


def _signal_summary(signal_list: list[dict[str, Any]], limit: int = 3) -> list[str]:
    summaries: list[str] = []
    for signal in signal_list[:limit]:
        label = (signal.get("label") or signal.get("normalized_label") or "").strip()
        source = (signal.get("source") or "unknown").strip()
        matched_tokens = ", ".join(signal.get("matched_tokens") or [])
        bits = [label, f"[{source}]"]
        if matched_tokens:
            bits.append(f"tokens: {matched_tokens}")
        summaries.append(" ".join(bit for bit in bits if bit))
    return summaries


def _render_text_row(index: int, row: dict[str, Any]) -> str:
    metadata = row.get("metadata") or {}
    signals = (metadata.get("resolved_signals") or {}) if isinstance(metadata, dict) else {}
    winner = row.get("suggested_target") or {}
    alternate = row.get("second_option") or {}
    override = row.get("target_override") or {}

    lines = [
        f"[{index}] {row.get('representative_query') or '(unknown query)'} [{row.get('disposition')}]",
        f"family: {row.get('normalized_query_key') or ''}",
        f"source: {(row.get('current_page_type') or row.get('source_entity_type') or 'unknown')} | {row.get('source_name') or ''} | {row.get('source_url') or ''}",
        f"page-type: current={row.get('current_page_type') or row.get('source_entity_type') or 'unknown'} preferred={row.get('preferred_entity_type') or 'unknown'} intent={row.get('query_intent_scope') or 'mixed_or_unknown'}",
        f"scores: opportunity={row.get('opportunity_score')} demand={row.get('demand_score')} intent={row.get('intent_clarity_score')} noise={row.get('noise_penalty')}",
        f"reason: {row.get('reason_summary') or ''}",
    ]

    for label, key in (
        ("brand", "brand_signals"),
        ("hard", "hard_attribute_signals"),
        ("soft", "soft_attribute_signals"),
        ("collection", "collection_signals"),
        ("topic", "topic_signals"),
        ("sku", "sku_signals"),
    ):
        summary = _signal_summary(list(signals.get(key) or []))
        if summary:
            lines.append(f"{label}: " + " | ".join(summary))

    if winner:
        lines.append(
            "winner: "
            f"{winner.get('entity_type') or 'unknown'} | "
            f"{winner.get('name') or ''} | "
            f"{winner.get('url') or ''} | "
            f"score={winner.get('score')}"
        )
    if alternate:
        lines.append(
            "alternate: "
            f"{alternate.get('entity_type') or 'unknown'} | "
            f"{alternate.get('name') or ''} | "
            f"{alternate.get('url') or ''} | "
            f"score={alternate.get('score')}"
        )
    if override:
        metadata = override.get("metadata") or {}
        lines.append(
            "override: "
            f"{override.get('target_entity_type') or 'unknown'} | "
            f"{metadata.get('target_name') or ''} | "
            f"{metadata.get('target_url') or ''}"
        )

    return "\n".join(lines)


def _build_audit_rows(
    store_hash: str,
    disposition: str | None,
    run_id: int | None,
    limit: int,
    query_filter: str | None,
) -> tuple[int | None, list[dict[str, Any]]]:
    resolved_run_id = run_id or services._latest_gate_run_id(store_hash)
    requested_limit = max(int(limit or 25), 1)
    fetch_limit = max(requested_limit * 4, 100)
    gate_rows = services.list_query_gate_records(
        store_hash,
        disposition=disposition,
        limit=fetch_limit,
        run_id=resolved_run_id,
    )
    annotated_rows = services._annotate_query_gate_rows_with_suggestions(store_hash, gate_rows)

    if query_filter:
        lowered = query_filter.strip().lower()
        annotated_rows = [
            row
            for row in annotated_rows
            if lowered in (row.get("representative_query") or "").lower()
            or lowered in (row.get("normalized_query_key") or "").lower()
            or lowered in (row.get("source_name") or "").lower()
            or lowered in (((row.get("suggested_target") or {}).get("name")) or "").lower()
        ]

    return (annotated_rows[0].get("run_id") if annotated_rows else resolved_run_id), annotated_rows[:requested_limit]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Inspect Fulcrum query-family gate decisions for hold/reject auditing.",
    )
    parser.add_argument("--store-hash", required=True, help="BigCommerce store hash, for example 99oa2tso")
    parser.add_argument(
        "--disposition",
        default="reject",
        choices=["reject", "hold", "pass", "all"],
        help="Gate disposition to inspect. Defaults to reject.",
    )
    parser.add_argument("--run-id", type=int, default=None, help="Optional gate run id. Defaults to the latest completed run.")
    parser.add_argument("--limit", type=int, default=25, help="Maximum rows to print. Defaults to 25.")
    parser.add_argument("--query", default=None, help="Optional substring filter for the representative query/family/source/winner.")
    parser.add_argument("--json", action="store_true", help="Emit structured JSON instead of text.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    disposition = None if args.disposition == "all" else args.disposition
    run_id, rows = _build_audit_rows(
        store_hash=args.store_hash,
        disposition=disposition,
        run_id=args.run_id,
        limit=args.limit,
        query_filter=args.query,
    )

    if args.json:
        payload = {
            "store_hash": args.store_hash,
            "run_id": run_id,
            "disposition": args.disposition,
            "row_count": len(rows),
            "rows": rows,
        }
        print(json.dumps(payload, indent=2, default=str))
        return 0

    header = [
        "Fulcrum Gate Audit",
        f"store_hash: {args.store_hash}",
        f"run_id: {run_id or 'n/a'}",
        f"disposition: {args.disposition}",
        f"row_count: {len(rows)}",
    ]
    print("\n".join(header))
    print("")

    if not rows:
        print("No matching gate rows found.")
        if args.disposition == "reject":
            print("Tip: try --disposition hold if the latest run has no rejections.")
        return 0

    for idx, row in enumerate(rows, start=1):
        print(_render_text_row(idx, row))
        print("")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
