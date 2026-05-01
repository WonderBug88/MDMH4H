"""Presentation helpers for Fulcrum review and publication rows."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Callable
from urllib.parse import urlencode


def merge_fresh_gate_context_into_review_row(row: dict[str, Any], gate_row: dict[str, Any] | None) -> None:
    if not isinstance(gate_row, dict) or not gate_row:
        return
    for key in (
        "reason_summary",
        "current_page_type",
        "query_intent_scope",
        "preferred_entity_type",
        "source_name",
        "source_url",
        "source_entity_type",
        "source_entity_id",
        "representative_query",
        "normalized_query_key",
    ):
        if gate_row.get(key) is not None:
            row[key] = gate_row.get(key)


def build_query_gate_human_review_mailto(
    store_hash: str,
    *,
    app_base_url: str,
    get_store_owner_email_fn: Callable[[str], str | None],
    normalize_store_hash_fn: Callable[[str], str],
    gate_row: dict[str, Any] | None = None,
    request_row: dict[str, Any] | None = None,
    review: dict[str, Any] | None = None,
    email_to: str | None = None,
) -> str:
    recipient = (email_to or get_store_owner_email_fn(store_hash) or "").strip()
    if not recipient:
        return ""

    gate_record_id = int((request_row or {}).get("gate_record_id") or (gate_row or {}).get("gate_record_id") or 0)
    representative_query = (request_row or {}).get("representative_query") or (gate_row or {}).get("representative_query") or ""
    normalized_query_key = (request_row or {}).get("normalized_query_key") or (gate_row or {}).get("normalized_query_key") or ""
    current_name = (request_row or {}).get("source_name") or (gate_row or {}).get("source_name") or ""
    current_url = (request_row or {}).get("source_url") or (gate_row or {}).get("source_url") or ""
    current_type = (
        (request_row or {}).get("current_page_type")
        or (gate_row or {}).get("current_page_type")
        or (gate_row or {}).get("source_entity_type")
        or ""
    )
    review_dict = review if isinstance(review, dict) else {}
    review_metadata = review_dict.get("metadata") if isinstance(review_dict.get("metadata"), dict) else {}
    review_winner = dict(review_metadata.get("winner") or {}) if isinstance(review_metadata, dict) else {}
    winner = review_winner or (((gate_row or {}).get("suggested_target") or {}) if gate_row else {})
    recommended_action = (review_dict.get("recommended_action") or "").strip().lower()
    target_name = (request_row or {}).get("target_name") or winner.get("name") or ""
    target_url = (request_row or {}).get("target_url") or winner.get("url") or ""
    target_type = (request_row or {}).get("target_entity_type") or winner.get("entity_type") or ""
    if recommended_action == "use_original":
        target_name = current_name or target_name
        target_url = current_url or target_url
        target_type = current_type or target_type
    elif recommended_action == "keep_winner" and winner:
        target_name = winner.get("name") or target_name
        target_url = winner.get("url") or target_url
        target_type = winner.get("entity_type") or target_type
    reason_summary = (gate_row or {}).get("reason_summary") or (request_row or {}).get("reason_summary") or ""
    admin_url = f"{app_base_url.rstrip('/')}/fulcrum/admin?store_hash={normalize_store_hash_fn(store_hash)}"
    subject = f"Fulcrum human review needed: Gate #{gate_record_id} {representative_query}".strip()
    body_lines = [
        f"Store: {normalize_store_hash_fn(store_hash)}",
        f"Gate ID: #{gate_record_id}" if gate_record_id else "Gate ID: unknown",
        f"Query: {representative_query or 'unknown'}",
        f"Family: {normalized_query_key or 'unknown'}",
        "",
        "Current GSC page:",
        f"- {current_name or 'unknown'}",
        f"- {current_type or 'unknown'}",
        f"- {current_url or 'unknown'}",
        "",
        "Fulcrum route:",
        f"- {target_name or 'unknown'}",
        f"- {target_type or 'unknown'}",
        f"- {target_url or 'unknown'}",
        "",
        f"Routing reason: {reason_summary or 'unknown'}",
    ]
    if review:
        body_lines.extend(
            [
                "",
                "AI review:",
                f"- verdict: {review_dict.get('verdict') or 'unknown'}",
                f"- issue: {review_dict.get('issue_type') or 'unknown'}",
                f"- action: {review_dict.get('recommended_action') or 'unknown'}",
                f"- rationale: {review_dict.get('rationale') or 'none'}",
            ]
        )
    body_lines.extend(["", f"Admin page: {admin_url}"])
    return "mailto:" + recipient + "?" + urlencode({"subject": subject, "body": "\n".join(body_lines)})


def publication_posting_label(row: dict[str, Any]) -> str:
    source_type = (row.get("source_entity_type") or "").strip().lower()
    metafield_key = (row.get("metafield_key") or "internal_links_html").strip().lower()
    if source_type == "product" and metafield_key == "internal_links_html":
        return "Product page posting related links"
    if source_type == "category" and metafield_key == "internal_product_links_html":
        return "Category page posting product families"
    if source_type == "category" and metafield_key == "internal_category_links_html":
        return "Category page posting related categories"
    return f"{source_type or 'unknown'} page posting {metafield_key or 'links'}"


def summarize_edge_case_requests(
    rows: list[dict[str, Any]],
    *,
    format_timestamp_display_fn: Callable[[Any], str | None],
    format_relative_time_fn: Callable[[Any], str | None],
) -> dict[str, Any]:
    request_rows = list(rows or [])
    paused_live_blocks = 0
    needs_human_review_count = 0
    audited_count = 0
    oldest_request_at: datetime | None = None
    newest_request_at: datetime | None = None
    for row in request_rows:
        metadata = dict(row.get("metadata") or {})
        review = row.get("agent_review") if isinstance(row.get("agent_review"), dict) else {}
        issue_type = ((review or {}).get("issue_type") or "").strip().lower()
        recommended_action = ((review or {}).get("recommended_action") or "").strip().lower()
        audit_status = (metadata.get("audit_status") or "").strip().lower()
        if metadata.get("live_block_paused"):
            paused_live_blocks += 1
        if review or audit_status == "ok":
            audited_count += 1
        if issue_type == "needs_human_review" or recommended_action == "manual_review":
            needs_human_review_count += 1
        created_at = row.get("created_at")
        if isinstance(created_at, datetime):
            if oldest_request_at is None or created_at < oldest_request_at:
                oldest_request_at = created_at
            if newest_request_at is None or created_at > newest_request_at:
                newest_request_at = created_at
    return {
        "open_count": len(request_rows),
        "paused_live_blocks": paused_live_blocks,
        "audited_count": audited_count,
        "pending_audit_count": max(len(request_rows) - audited_count, 0),
        "needs_human_review_count": needs_human_review_count,
        "oldest_request_display": format_timestamp_display_fn(oldest_request_at),
        "oldest_request_relative": format_relative_time_fn(oldest_request_at),
        "newest_request_display": format_timestamp_display_fn(newest_request_at),
        "newest_request_relative": format_relative_time_fn(newest_request_at),
    }


def apply_review_target_display(
    row: dict[str, Any],
    *,
    normalize_storefront_path_fn: Callable[[Any], str],
) -> None:
    review = row.get("agent_review") if isinstance(row.get("agent_review"), dict) else {}
    recommended_action = ((review or {}).get("recommended_action") or "").strip().lower()
    review_metadata = review.get("metadata") if isinstance(review.get("metadata"), dict) else {}
    review_winner = dict(review_metadata.get("winner") or {}) if isinstance(review_metadata, dict) else {}
    source_url = normalize_storefront_path_fn(row.get("source_url"))
    gate_target_name = row.get("gate_suggested_target_name") or review_winner.get("name") or row.get("target_name")
    gate_target_url = normalize_storefront_path_fn(
        row.get("gate_suggested_target_url") or review_winner.get("url") or row.get("target_url")
    )
    gate_target_live_url = row.get("gate_suggested_target_live_url") or row.get("target_live_url")

    if recommended_action == "use_original":
        row["display_target_name"] = row.get("source_name") or gate_target_name or "No target stored"
        row["display_target_url"] = source_url or gate_target_url or ""
        row["display_target_live_url"] = row.get("source_live_url") or gate_target_live_url
    elif recommended_action == "keep_winner" and gate_target_url:
        row["display_target_name"] = gate_target_name or "No target stored"
        row["display_target_url"] = gate_target_url or ""
        row["display_target_live_url"] = gate_target_live_url
    else:
        row["display_target_name"] = row.get("target_name") or gate_target_name or "No target stored"
        row["display_target_url"] = normalize_storefront_path_fn(row.get("target_url")) or gate_target_url or ""
        row["display_target_live_url"] = row.get("target_live_url") or gate_target_live_url

    display_url = normalize_storefront_path_fn(row.get("display_target_url"))
    row["target_matches_current"] = bool(source_url and display_url and source_url == display_url)
    row["allow_live_approval"] = not row["target_matches_current"]


__all__ = [
    "apply_review_target_display",
    "build_query_gate_human_review_mailto",
    "merge_fresh_gate_context_into_review_row",
    "publication_posting_label",
    "summarize_edge_case_requests",
]
