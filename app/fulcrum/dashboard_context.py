"""Dashboard context assembly helpers for Fulcrum."""

from __future__ import annotations

from datetime import datetime
import math
from typing import Any, Callable


def _gate_status_label(value: str | None) -> str:
    status = (value or "hold").strip().lower() or "hold"
    labels = {
        "pass": "Pass",
        "hold": "Hold",
        "reject": "Reject",
    }
    return labels.get(status, status.title() or "Hold")


def _normalized_path(value: Any) -> str:
    path = str(value or "").strip()
    if not path:
        return ""
    if "://" in path:
        path = path.split("://", 1)[1]
        path = "/" + path.split("/", 1)[1] if "/" in path else "/"
    path = path.split("?", 1)[0].split("#", 1)[0].strip().lower()
    if not path:
        return ""
    return path if path == "/" else path.rstrip("/") + "/"


def _is_same_page_winner(row: dict[str, Any]) -> bool:
    suggested_target = dict(row.get("suggested_target") or {})
    if not suggested_target:
        return False

    source_type = (row.get("source_entity_type") or row.get("current_page_type") or "").strip().lower()
    target_type = (suggested_target.get("entity_type") or "").strip().lower()
    source_id = int(row.get("source_entity_id") or 0)
    target_id = int(suggested_target.get("entity_id") or 0)
    if source_type and target_type and source_id and target_id:
        return source_type == target_type and source_id == target_id

    source_path = _normalized_path(row.get("source_url"))
    target_path = _normalized_path(suggested_target.get("url"))
    return bool(source_path and target_path and source_path == target_path)


def _source_row_key(row: dict[str, Any]) -> tuple[str, int, str]:
    source_type = (row.get("source_entity_type") or row.get("current_page_type") or "").strip().lower()
    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    source_id = int(row.get("source_entity_id") or row.get("source_product_id") or 0)
    if source_type == "category":
        source_id = int(metadata.get("bc_category_id") or metadata.get("source_bc_entity_id") or source_id or 0)
    elif source_type == "product":
        source_id = int(metadata.get("bc_product_id") or metadata.get("source_bc_entity_id") or source_id or 0)
    elif metadata.get("source_bc_entity_id"):
        source_id = int(metadata.get("source_bc_entity_id") or source_id or 0)
    return (
        source_type,
        source_id,
        _normalized_path(row.get("source_url")),
    )


def _is_review_blocked(row: dict[str, Any], blocked_source_keys: set[tuple[str, int, str]]) -> bool:
    row_key = _source_row_key(row)
    if row_key in blocked_source_keys:
        return True

    row_type, row_id, row_path = row_key
    if row_path and any(source_path == row_path for _, _, source_path in blocked_source_keys):
        return True
    if row_type and row_id and any(source_type == row_type and source_id == row_id for source_type, source_id, _ in blocked_source_keys):
        return True
    return False


def _is_top_ten_rank_hold(row: dict[str, Any]) -> bool:
    if (row.get("gate_status") or "").strip().lower() != "hold":
        return False
    reason_summary = str(row.get("reason_summary") or "").strip().lower()
    return "query already ranks in the top 10" in reason_summary


def _is_noise_gate_reason(row: dict[str, Any]) -> bool:
    reason_summary = str(row.get("reason_summary") or "").strip().lower()
    return "too noisy or ambiguous" in reason_summary


def _is_low_clarity_gate_reason(row: dict[str, Any]) -> bool:
    reason_summary = str(row.get("reason_summary") or "").strip().lower()
    return (
        "does not have enough demand to justify routing" in reason_summary
        or "google already aligns this query to the current page" in reason_summary
        or "query should be monitored until demand or clarity improves" in reason_summary
    )


def _is_published_family_row(row: dict[str, Any]) -> bool:
    return bool(
        row.get("is_live_result")
        and (row.get("gate_status") or "").strip().lower() == "pass"
        and _has_surfaced_target(row)
        and not row.get("is_same_page_winner")
    )


def _gate_reason_for_row(row: dict[str, Any]) -> tuple[str | None, str | None]:
    gate_status = (row.get("gate_status") or "").strip().lower()
    if gate_status == "pass":
        return None, None
    if _is_top_ten_rank_hold(row):
        return "gating_top_ten", "Gating - Top-10"
    if _is_noise_gate_reason(row):
        return "gating_noise", "Gating - Noise"
    if _is_low_clarity_gate_reason(row):
        return "gating_low_clarity", "Gating - Low Clarity"
    return "gating_low_clarity", "Gating - Low Clarity"


def _routing_reason_for_row(row: dict[str, Any]) -> tuple[str | None, str | None]:
    if row.get("is_same_page_winner"):
        return "routing_same_page_winner", "Routing - Same Page Winner"
    if not _has_surfaced_target(row):
        return "routing_no_target", "Routing - No Target"
    return None, None


def _publish_reason_for_row(
    row: dict[str, Any],
    *,
    blocked_source_keys: set[tuple[str, int, str]],
    category_publishing_enabled: bool,
) -> tuple[str, str]:
    if _is_published_family_row(row):
        return "published", "Published"
    if _is_review_blocked(row, blocked_source_keys):
        return "blocked_by_review", "Blocked by review"
    gate_reason_key, gate_reason_label = _gate_reason_for_row(row)
    routing_reason_key, routing_reason_label = _routing_reason_for_row(row)
    if row.get("gate_status") == "pass":
        if routing_reason_key and routing_reason_label:
            return routing_reason_key, routing_reason_label

        source_type = (row.get("source_entity_type") or row.get("current_page_type") or "").strip().lower()
        if source_type == "category" and not category_publishing_enabled:
            return "category_publishing_off", "Category publishing off"
        if source_type not in {"product", "category"}:
            return "source_type_not_publishable", "Source type not publishable"
        return "awaiting_publish", "Awaiting publish"
    if gate_reason_key and gate_reason_label:
        return gate_reason_key, gate_reason_label
    return "gating_low_clarity", "Gating - Low Clarity"


def _raw_query_variant_count(row: dict[str, Any]) -> int:
    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    query_variants = metadata.get("query_variants")
    if isinstance(query_variants, list) and query_variants:
        return len(query_variants)
    return 1


def _has_surfaced_target(row: dict[str, Any]) -> bool:
    suggested_target = row.get("suggested_target")
    return bool(isinstance(suggested_target, dict) and suggested_target)


def admin_context_defaults(
    *,
    pending_count: int,
    changed_route_search: str | None,
    changed_route_sort: str,
    changed_route_page: int,
    changed_route_page_size: int,
) -> dict[str, Any]:
    return {
        "approved_candidates": [],
        "approved_sources": [],
        "pending_candidates": [],
        "rejected_candidates": [],
        "pending_count": pending_count,
        "pending_mapping_reviews": [],
        "publication_summary": {},
        "logic_change_summary": [],
        "gate_agent_review_summary": {},
        "gate_agent_review_clusters": [],
        "edge_case_requests": [],
        "edge_case_summary": {},
        "resolved_edge_case_requests": [],
        "resolved_edge_case_summary": {},
        "changed_route_results": [],
        "changed_route_summary": {},
        "changed_route_review_summary": {},
        "changed_route_review_reasoning": {},
        "changed_route_search": (changed_route_search or "").strip(),
        "changed_route_sort": changed_route_sort,
        "changed_route_page": max(int(changed_route_page or 1), 1),
        "changed_route_page_size": max(int(changed_route_page_size or 25), 1),
        "changed_route_total_count": 0,
        "changed_route_filtered_count": 0,
        "changed_route_page_count": 0,
        "blocked_gate_summary": {},
        "gsc_routing_coverage": {},
        "gsc_alignment_summary": {},
        "gsc_performance_summary": {},
        "operational_snapshot": {},
    }


def populate_changed_route_admin_context(
    admin_context: dict[str, Any],
    *,
    store_hash: str,
    latest_gate_run_id: int | None,
    publications: list[dict[str, Any]],
    changed_route_search: str | None,
    changed_route_sort: str,
    changed_route_page: int,
    changed_route_page_size: int,
    get_cached_changed_route_results_fn: Callable[..., list[dict[str, Any]]],
    summarize_changed_route_rows_fn: Callable[[list[dict[str, Any]]], dict[str, Any]],
    gate_review_map_for_ids_fn: Callable[..., dict[int, dict[str, Any]]],
    attach_changed_route_agent_reviews_fn: Callable[[list[dict[str, Any]], dict[int, dict[str, Any]]], list[dict[str, Any]]],
    summarize_changed_route_agent_reviews_fn: Callable[[list[dict[str, Any]], dict[int, dict[str, Any]]], dict[str, Any]],
    get_cached_changed_route_review_reasoning_fn: Callable[..., dict[str, Any]],
    build_query_gate_human_review_mailto_fn: Callable[..., str],
    matches_changed_route_search_fn: Callable[[dict[str, Any], str], bool],
    sorted_changed_route_rows_fn: Callable[[list[dict[str, Any]], str], list[dict[str, Any]]],
) -> dict[str, Any]:
    changed_route_rows = get_cached_changed_route_results_fn(store_hash, run_id=latest_gate_run_id, limit=2000)
    admin_context["changed_route_summary"] = summarize_changed_route_rows_fn(changed_route_rows)
    changed_route_review_map = gate_review_map_for_ids_fn(
        store_hash,
        {
            int(row.get("gate_record_id") or 0)
            for row in changed_route_rows
            if int(row.get("gate_record_id") or 0) > 0
        },
        run_id=latest_gate_run_id,
    )
    changed_route_rows = attach_changed_route_agent_reviews_fn(changed_route_rows, changed_route_review_map)
    admin_context["changed_route_review_summary"] = summarize_changed_route_agent_reviews_fn(
        changed_route_rows,
        changed_route_review_map,
    )
    admin_context["changed_route_review_reasoning"] = get_cached_changed_route_review_reasoning_fn(
        store_hash,
        run_id=latest_gate_run_id,
        rows=changed_route_rows,
        review_map=changed_route_review_map,
    )

    active_publication_keys = {
        (
            (publication.get("source_entity_type") or "").strip().lower(),
            int(publication.get("source_entity_id") or publication.get("source_product_id") or 0),
            (publication.get("source_url") or "").strip(),
        )
        for publication in publications
    }
    for row in changed_route_rows:
        live_key = (
            (row.get("source_entity_type") or row.get("current_page_type") or "").strip().lower(),
            int(row.get("source_entity_id") or 0),
            (row.get("source_url") or "").strip(),
        )
        row["is_live_result"] = live_key in active_publication_keys
        row["live_status_label"] = "Published live" if row["is_live_result"] else "Not published yet"
        row["human_review_mailto"] = build_query_gate_human_review_mailto_fn(
            store_hash,
            gate_row=row,
            review=row.get("agent_review"),
        )

    filtered_changed_routes = [
        row for row in changed_route_rows if matches_changed_route_search_fn(row, changed_route_search or "")
    ]
    filtered_changed_routes = sorted_changed_route_rows_fn(filtered_changed_routes, changed_route_sort)
    safe_page_size = max(int(changed_route_page_size or 25), 1)
    safe_page = max(int(changed_route_page or 1), 1)
    total_filtered = len(filtered_changed_routes)
    page_count = max(int(math.ceil(total_filtered / safe_page_size)) if total_filtered else 1, 1)
    if safe_page > page_count:
        safe_page = page_count
    start_index = (safe_page - 1) * safe_page_size
    end_index = start_index + safe_page_size

    admin_context["changed_route_results"] = filtered_changed_routes[start_index:end_index]
    admin_context["changed_route_total_count"] = len(changed_route_rows)
    admin_context["changed_route_filtered_count"] = total_filtered
    admin_context["changed_route_page"] = safe_page
    admin_context["changed_route_page_size"] = safe_page_size
    admin_context["changed_route_page_count"] = page_count
    return admin_context


def populate_edge_case_admin_context(
    admin_context: dict[str, Any],
    *,
    store_hash: str,
    list_query_gate_review_requests_fn: Callable[..., list[dict[str, Any]]],
    gate_review_map_for_ids_fn: Callable[..., dict[int, dict[str, Any]]],
    query_gate_record_map_for_ids_fn: Callable[..., dict[int, dict[str, Any]]],
    extract_storefront_channel_id_fn: Callable[..., int | None],
    build_storefront_url_fn: Callable[..., str | None],
    merge_fresh_gate_context_into_review_row_fn: Callable[[dict[str, Any], dict[str, Any] | None], None],
    build_query_gate_human_review_mailto_fn: Callable[..., str],
    apply_review_target_display_fn: Callable[[dict[str, Any]], None],
    format_timestamp_display_fn: Callable[[Any], str | None],
    format_relative_time_fn: Callable[[Any], str | None],
    summarize_edge_case_requests_fn: Callable[[list[dict[str, Any]]], dict[str, Any]],
) -> dict[str, Any]:
    admin_context["edge_case_requests"] = list_query_gate_review_requests_fn(store_hash, request_status="requested", limit=25)
    admin_context["resolved_edge_case_requests"] = list_query_gate_review_requests_fn(store_hash, request_status="resolved", limit=10)
    all_rows = admin_context["edge_case_requests"] + admin_context["resolved_edge_case_requests"]
    edge_case_review_map = gate_review_map_for_ids_fn(
        store_hash,
        {
            int(row.get("gate_record_id") or 0)
            for row in all_rows
            if int(row.get("gate_record_id") or 0) > 0
        },
        run_ids={
            int(row.get("run_id") or 0)
            for row in all_rows
            if int(row.get("run_id") or 0) > 0
        },
    )
    edge_case_gate_row_map = query_gate_record_map_for_ids_fn(
        store_hash,
        {
            int(row.get("gate_record_id") or 0)
            for row in all_rows
            if int(row.get("gate_record_id") or 0) > 0
        },
        run_ids={
            int(row.get("run_id") or 0)
            for row in all_rows
            if int(row.get("run_id") or 0) > 0
        },
        fresh_suggestions=True,
    )

    def decorate_edge_case_row(row: dict[str, Any], *, resolved: bool) -> datetime | None:
        source_channel_id = extract_storefront_channel_id_fn(row)
        target_channel_id = extract_storefront_channel_id_fn(
            row.get("metadata") if isinstance(row.get("metadata"), dict) else {},
            row,
        )
        row["source_live_url"] = build_storefront_url_fn(store_hash, row.get("source_url"), channel_id=source_channel_id)
        row["target_live_url"] = build_storefront_url_fn(store_hash, row.get("target_url"), channel_id=target_channel_id)
        gate_row = edge_case_gate_row_map.get(int(row.get("gate_record_id") or 0)) or {}
        merge_fresh_gate_context_into_review_row_fn(row, gate_row)
        gate_target = (gate_row.get("suggested_target") or {}) if isinstance(gate_row, dict) else {}
        review = edge_case_review_map.get(int(row.get("gate_record_id") or 0))
        review_metadata = review.get("metadata") if isinstance((review or {}).get("metadata"), dict) else {}
        review_winner = dict(review_metadata.get("winner") or {}) if isinstance(review_metadata, dict) else {}
        gate_target_url = gate_target.get("url") or review_winner.get("url")
        row["gate_suggested_target_name"] = gate_target.get("name") or review_winner.get("name")
        row["gate_suggested_target_url"] = gate_target_url
        row["gate_suggested_target_live_url"] = (
            build_storefront_url_fn(store_hash, gate_target_url, channel_id=target_channel_id) if gate_target_url else None
        )
        row_metadata = dict(row.get("metadata") or {})
        row["agent_review"] = review
        audited = bool(row["agent_review"] or row_metadata.get("audit_status") == "ok")
        row["requested_at_display"] = format_timestamp_display_fn(row.get("created_at"))
        row["requested_at_relative"] = format_relative_time_fn(row.get("created_at"))
        row["human_review_mailto"] = build_query_gate_human_review_mailto_fn(
            store_hash,
            gate_row=gate_row,
            request_row=row,
            review=row.get("agent_review"),
        )
        apply_review_target_display_fn(row)

        if not resolved:
            row["applied_status_label"] = "Live block paused for review" if row_metadata.get("live_block_paused") else "Not applied"
            row["review_status_label"] = (
                "Agent recommends current page"
                if audited and row.get("target_matches_current")
                else ("Agent diagnosis ready for support" if audited else "Agent diagnosis pending")
            )
            row["resolution_status_label"] = (
                "Support fix applied live"
                if row_metadata.get("live_block_restored")
                else ("Waiting for support confirmation" if row.get("target_matches_current") else "Waiting for support investigation or live fix")
            )
            return None

        row["applied_status_label"] = (
            "Live fix applied"
            if row_metadata.get("live_block_restored")
            else ("Investigation complete; live block remains paused" if row_metadata.get("live_block_paused") else "Investigation complete; no live block")
        )
        resolved_at_raw = row_metadata.get("resolved_at")
        resolved_at_dt: datetime | None = None
        if isinstance(resolved_at_raw, datetime):
            resolved_at_dt = resolved_at_raw
        else:
            resolved_text = (str(resolved_at_raw or "")).strip()
            if resolved_text:
                try:
                    resolved_at_dt = datetime.fromisoformat(resolved_text)
                except ValueError:
                    resolved_at_dt = None
        row["resolved_at_display"] = format_timestamp_display_fn(resolved_at_dt or resolved_at_raw)
        row["resolved_at_relative"] = format_relative_time_fn(resolved_at_dt)
        row["review_status_label"] = (
            "Agent confirmed current page"
            if audited and row.get("target_matches_current")
            else ("Agent diagnosis ready for support" if audited else "Agent diagnosis unavailable")
        )
        row["resolution_status_label"] = (
            "Support fix applied live"
            if row_metadata.get("live_block_restored")
            else (
                "Investigation complete; no live fix needed"
                if row_metadata.get("live_approval_completed")
                else ("Investigation confirmed current page" if row.get("target_matches_current") else "Investigation complete")
            )
        )
        row["can_approve_live"] = (
            bool(row.get("allow_live_approval"))
            and not bool(row_metadata.get("live_block_restored"))
            and not bool(row_metadata.get("live_approval_completed"))
        )
        return resolved_at_dt

    for row in admin_context["edge_case_requests"]:
        decorate_edge_case_row(row, resolved=False)
    admin_context["edge_case_summary"] = summarize_edge_case_requests_fn(admin_context["edge_case_requests"])

    latest_resolved_at: datetime | None = None
    latest_pending_approval_at: datetime | None = None
    approved_live_count = 0
    pending_approval_rows: list[dict[str, Any]] = []
    for row in admin_context["resolved_edge_case_requests"]:
        resolved_at_dt = decorate_edge_case_row(row, resolved=True)
        row_metadata = dict(row.get("metadata") or {})
        if row_metadata.get("live_block_restored"):
            approved_live_count += 1
        if row.get("can_approve_live"):
            pending_approval_rows.append(row)
            if resolved_at_dt and (latest_pending_approval_at is None or resolved_at_dt > latest_pending_approval_at):
                latest_pending_approval_at = resolved_at_dt
        if resolved_at_dt and (latest_resolved_at is None or resolved_at_dt > latest_resolved_at):
            latest_resolved_at = resolved_at_dt

    admin_context["resolved_edge_case_requests"] = pending_approval_rows
    admin_context["resolved_edge_case_summary"] = {
        "recent_count": len(admin_context["resolved_edge_case_requests"]),
        "total_recent_count": approved_live_count + len(admin_context["resolved_edge_case_requests"]),
        "pending_approval_count": len(admin_context["resolved_edge_case_requests"]),
        "approved_live_count": approved_live_count,
        "latest_resolved_display": format_timestamp_display_fn(latest_resolved_at),
        "latest_resolved_relative": format_relative_time_fn(latest_resolved_at),
        "latest_pending_approval_display": format_timestamp_display_fn(latest_pending_approval_at),
        "latest_pending_approval_relative": format_relative_time_fn(latest_pending_approval_at),
    }
    return admin_context


def build_public_dashboard_data(
    *,
    store_hash: str,
    include_admin: bool,
    latest_gate_run_id: int | None,
    latest_gate_cluster: str | None,
    publications: list[dict[str, Any]],
    review_bucket_requests: list[dict[str, Any]],
    category_publishing_enabled: bool = True,
    list_query_gate_records_fn: Callable[..., list[dict[str, Any]]],
    annotate_query_gate_rows_with_suggestions_fn: Callable[..., list[dict[str, Any]]],
    attach_cached_query_gate_suggestions_fn: Callable[[list[dict[str, Any]]], list[dict[str, Any]]],
    extract_storefront_channel_id_fn: Callable[..., int | None],
    build_storefront_url_fn: Callable[..., str | None],
    summarize_suggested_target_types_fn: Callable[[list[dict[str, Any]]], dict[str, int]],
    publication_posting_label_fn: Callable[[dict[str, Any]], str],
    format_timestamp_display_fn: Callable[[Any], str | None],
    format_relative_time_fn: Callable[[Any], str | None],
    summarize_edge_case_requests_fn: Callable[[list[dict[str, Any]]], dict[str, Any]],
) -> dict[str, Any]:
    query_family_review: list[dict[str, Any]] = []
    suggested_target_type_summary: dict[str, int] = {}
    pass_count = 0
    hold_count = 0
    reject_count = 0
    published_count = 0
    not_published_count = 0
    published_query_count = 0
    not_published_query_count = 0
    pass_published_query_count = 0
    pass_not_published_query_count = 0
    review_bucket_query_count = 0
    raw_query_count = 0

    if not include_admin:
        queued_gate_ids = {
            int(row.get("gate_record_id") or 0)
            for row in review_bucket_requests
            if int(row.get("gate_record_id") or 0) > 0
        }
        raw_gate_rows = [
            row
            for row in list_query_gate_records_fn(store_hash, disposition=None, limit=2000, run_id=latest_gate_run_id)
            if int(row.get("gate_record_id") or 0) not in queued_gate_ids
        ]
        query_family_review = attach_cached_query_gate_suggestions_fn(raw_gate_rows)

        missing_snapshot_ids = {
            int(row.get("gate_record_id") or 0)
            for row in query_family_review
            if int(row.get("gate_record_id") or 0) > 0
            and not any(
                key in dict(row.get("metadata") or {})
                for key in ("suggested_target_snapshot", "second_option_snapshot", "target_override_snapshot")
            )
        }
        if missing_snapshot_ids:
            refreshed_rows = annotate_query_gate_rows_with_suggestions_fn(
                store_hash,
                [
                    row
                    for row in raw_gate_rows
                    if int(row.get("gate_record_id") or 0) in missing_snapshot_ids
                ],
                cluster=latest_gate_cluster,
            )
            refreshed_map = {
                int(row.get("gate_record_id") or 0): row
                for row in refreshed_rows
                if int(row.get("gate_record_id") or 0) > 0
            }
            query_family_review = [
                refreshed_map.get(int(row.get("gate_record_id") or 0), row)
                for row in query_family_review
            ]

        active_publication_keys = {
            _source_row_key(publication)
            for publication in publications
        }
        blocked_source_keys = {
            _source_row_key(row)
            for row in review_bucket_requests
            if _normalized_path(row.get("source_url")) or int(row.get("source_entity_id") or row.get("source_product_id") or 0)
        }
        query_family_review = [
            row
            for row in query_family_review
            if not _is_review_blocked(row, blocked_source_keys)
        ]
        for row in query_family_review:
            source_channel_id = extract_storefront_channel_id_fn(row)
            row["source_live_url"] = build_storefront_url_fn(store_hash, row.get("source_url"), channel_id=source_channel_id)
            suggested_target = row.get("suggested_target") or {}
            if suggested_target:
                suggested_channel_id = extract_storefront_channel_id_fn(suggested_target, row)
                suggested_target["live_url"] = build_storefront_url_fn(
                    store_hash,
                    suggested_target.get("url"),
                    channel_id=suggested_channel_id,
                )
            second_option = row.get("second_option") or {}
            if second_option:
                second_channel_id = extract_storefront_channel_id_fn(second_option, row)
                second_option["live_url"] = build_storefront_url_fn(
                    store_hash,
                    second_option.get("url"),
                    channel_id=second_channel_id,
                )
            live_key = _source_row_key(row)
            row["is_live_result"] = live_key in active_publication_keys
            row["gate_status"] = (row.get("disposition") or "hold").strip().lower() or "hold"
            row["gate_status_label"] = _gate_status_label(row["gate_status"])
            row["is_same_page_winner"] = row["gate_status"] != "reject" and _is_same_page_winner(row)
            row["publish_status"] = "published" if _is_published_family_row(row) else "not_published"
            row["publish_reason_key"], row["publish_status_label"] = _publish_reason_for_row(
                row,
                blocked_source_keys=blocked_source_keys,
                category_publishing_enabled=category_publishing_enabled,
            )
            row["live_status_label"] = row["publish_status_label"]
            row["raw_query_count"] = _raw_query_variant_count(row)

            if row["gate_status"] == "pass":
                pass_count += 1
            elif row["gate_status"] == "reject":
                reject_count += 1
            else:
                hold_count += 1

            has_target = _has_surfaced_target(row)
            is_publishable_query = row["gate_status"] == "pass" and has_target

            if row["publish_status"] == "published":
                published_count += 1
            else:
                not_published_count += 1

            if is_publishable_query:
                published_query_count += row["raw_query_count"]
                if row["publish_status"] == "published":
                    pass_published_query_count += row["raw_query_count"]
                else:
                    pass_not_published_query_count += row["raw_query_count"]
            elif row["gate_status"] in {"hold", "reject"}:
                not_published_query_count += row["raw_query_count"]
            raw_query_count += row["raw_query_count"]
        suggested_target_type_summary = summarize_suggested_target_types_fn(query_family_review)

    for row in publications:
        source_channel_id = extract_storefront_channel_id_fn(row)
        row["source_live_url"] = build_storefront_url_fn(store_hash, row.get("source_url"), channel_id=source_channel_id)
        row["posting_label"] = publication_posting_label_fn(row)

    if not include_admin:
        for row in review_bucket_requests:
            source_channel_id = extract_storefront_channel_id_fn(row)
            target_channel_id = extract_storefront_channel_id_fn(
                row.get("metadata") if isinstance(row.get("metadata"), dict) else {},
                row,
            )
            row["source_live_url"] = build_storefront_url_fn(store_hash, row.get("source_url"), channel_id=source_channel_id)
            row["target_live_url"] = build_storefront_url_fn(store_hash, row.get("target_url"), channel_id=target_channel_id)
            review_metadata = dict(row.get("metadata") or {})
            row["review_status_label"] = (
                "Agent diagnosis ready for support"
                if review_metadata.get("audit_status") == "ok"
                else "Agent diagnosis pending"
            )
            row["applied_status_label"] = "Live block paused for review" if review_metadata.get("live_block_paused") else "Not live yet"
            row["requested_at_display"] = format_timestamp_display_fn(row.get("created_at"))
            row["requested_at_relative"] = format_relative_time_fn(row.get("created_at"))
            row["raw_query_count"] = _raw_query_variant_count(row)
            review_bucket_query_count += row["raw_query_count"]

    review_bucket_summary = summarize_edge_case_requests_fn(review_bucket_requests) if not include_admin else {}
    return {
        "query_family_review": query_family_review,
        "suggested_target_type_summary": suggested_target_type_summary,
        "pass_count": pass_count,
        "hold_count": hold_count,
        "reject_count": reject_count,
        "published_count": published_count,
        "not_published_count": not_published_count,
        "publications": publications,
        "review_bucket_requests": review_bucket_requests,
        "review_bucket_summary": review_bucket_summary,
        "results_report": {
            "published_query_count": published_query_count,
            "not_published_query_count": not_published_query_count,
            "pass_published_query_count": pass_published_query_count,
            "pass_not_published_query_count": pass_not_published_query_count,
            "review_bucket_query_count": review_bucket_query_count,
            "family_count": len(query_family_review),
            "raw_query_count": raw_query_count,
        },
    }


def build_dashboard_context(
    store_hash: str,
    *,
    include_admin: bool = False,
    include_quality: bool = False,
    changed_route_search: str | None = None,
    changed_route_sort: str = "score_desc",
    changed_route_page: int = 1,
    changed_route_page_size: int = 25,
    generation_active_statuses: set[str],
    list_runs_fn: Callable[..., list[dict[str, Any]]],
    get_store_profile_summary_fn: Callable[[str], dict[str, Any]],
    refresh_store_readiness_fn: Callable[[str], dict[str, Any]],
    list_query_gate_review_requests_fn: Callable[..., list[dict[str, Any]]],
    summarize_query_gate_dispositions_fn: Callable[[str], dict[str, Any]],
    list_publications_fn: Callable[..., list[dict[str, Any]]],
    summarize_live_publications_fn: Callable[[str], dict[str, Any]],
    build_public_dashboard_data_fn: Callable[..., dict[str, Any]],
    count_pending_candidates_fn: Callable[[str], int],
    admin_context_defaults_fn: Callable[..., dict[str, Any]],
    populate_edge_case_admin_context_fn: Callable[..., dict[str, Any]],
    summarize_gsc_routing_coverage_fn: Callable[..., dict[str, Any]],
    populate_changed_route_admin_context_fn: Callable[..., dict[str, Any]],
    summarize_blocked_gate_families_fn: Callable[..., dict[str, Any]],
    get_cached_live_gsc_performance_fn: Callable[[str], dict[str, Any]],
    build_operational_snapshot_fn: Callable[..., dict[str, Any]],
    get_logic_change_summary_fn: Callable[..., dict[str, Any]],
    summarize_query_gate_agent_reviews_fn: Callable[..., dict[str, Any]],
    list_query_gate_agent_review_clusters_fn: Callable[..., list[dict[str, Any]]],
    count_query_gate_review_requests_fn: Callable[..., int],
    count_publications_fn: Callable[..., int],
    theme_hook_present_fn: Callable[[], bool],
    category_theme_hook_present_fn: Callable[[], bool],
    format_timestamp_display_fn: Callable[[Any], str | None],
    format_relative_time_fn: Callable[[Any], str | None],
) -> dict[str, Any]:
    runs = list_runs_fn(store_hash)
    active_run = next((run for run in runs if (run.get("status") or "").strip().lower() in generation_active_statuses), None)
    if include_admin:
        profile_summary = get_store_profile_summary_fn(store_hash)
        readiness = refresh_store_readiness_fn(store_hash)
        product_theme_hook_ready = theme_hook_present_fn()
        category_theme_hook_ready = category_theme_hook_present_fn()
    else:
        profile_summary = {}
        readiness = {}
        product_theme_hook_ready = False
        category_theme_hook_ready = False
    review_bucket_requests = [] if include_admin else list_query_gate_review_requests_fn(store_hash, request_status="requested", limit=60)
    gate_summary = summarize_query_gate_dispositions_fn(store_hash)
    latest_gate_run_id = gate_summary.get("run_id")
    latest_gate_run = next((run for run in runs if int(run.get("run_id") or 0) == int(latest_gate_run_id or 0)), None)
    latest_gate_filters = (latest_gate_run or {}).get("filters") or {}
    latest_gate_cluster = latest_gate_filters.get("cluster") if isinstance(latest_gate_filters, dict) else None
    publications = list_publications_fn(store_hash, active_only=True, limit=40 if include_admin else 1000)
    publication_summary = summarize_live_publications_fn(store_hash) if include_admin else {}
    public_context = build_public_dashboard_data_fn(
        store_hash=store_hash,
        include_admin=include_admin,
        latest_gate_run_id=latest_gate_run_id,
        latest_gate_cluster=latest_gate_cluster,
        publications=publications,
        review_bucket_requests=review_bucket_requests,
    )
    query_family_review = public_context["query_family_review"]
    suggested_target_type_summary = public_context["suggested_target_type_summary"]
    pass_count = int(public_context.get("pass_count") or 0)
    hold_count = int(public_context.get("hold_count") or 0)
    reject_count = int(public_context.get("reject_count") or 0)
    published_count = int(public_context.get("published_count") or 0)
    not_published_count = int(public_context.get("not_published_count") or 0)
    results_report = public_context.get("results_report") or {}
    publications = public_context["publications"]
    review_bucket_requests = public_context["review_bucket_requests"]
    review_bucket_summary = public_context["review_bucket_summary"]
    gsc_performance_summary = get_cached_live_gsc_performance_fn(store_hash)
    pending_count = count_pending_candidates_fn(store_hash) if include_admin else 0
    admin_context: dict[str, Any] = admin_context_defaults_fn(
        pending_count=pending_count,
        changed_route_search=changed_route_search,
        changed_route_sort=changed_route_sort,
        changed_route_page=changed_route_page,
        changed_route_page_size=changed_route_page_size,
    )
    admin_context["gsc_performance_summary"] = gsc_performance_summary
    if include_admin:
        admin_context["publication_summary"] = publication_summary
        admin_context = populate_edge_case_admin_context_fn(admin_context, store_hash=store_hash)
        admin_context["gsc_routing_coverage"] = summarize_gsc_routing_coverage_fn(store_hash, run_id=latest_gate_run_id)
        admin_context = populate_changed_route_admin_context_fn(
            admin_context,
            store_hash=store_hash,
            latest_gate_run_id=latest_gate_run_id,
            publications=publications,
            changed_route_search=changed_route_search,
            changed_route_sort=changed_route_sort,
            changed_route_page=changed_route_page,
            changed_route_page_size=changed_route_page_size,
        )
        admin_context["blocked_gate_summary"] = summarize_blocked_gate_families_fn(store_hash, run_id=latest_gate_run_id)
        admin_context["operational_snapshot"] = build_operational_snapshot_fn(
            store_hash,
            runs=runs,
            active_run=active_run,
            readiness=readiness,
            publication_summary=publication_summary,
            edge_case_requests=admin_context["edge_case_requests"],
            gate_summary=gate_summary,
        )
        if include_quality:
            admin_context.update(
                {
                    "logic_change_summary": get_logic_change_summary_fn(limit=6),
                    "gate_agent_review_summary": summarize_query_gate_agent_reviews_fn(store_hash, run_id=latest_gate_run_id),
                    "gate_agent_review_clusters": list_query_gate_agent_review_clusters_fn(store_hash, run_id=latest_gate_run_id, limit=8),
                }
            )
    return {
        "store_hash": store_hash,
        "runs": runs,
        "active_run": active_run,
        "review_bucket_requests": review_bucket_requests,
        "review_bucket_summary": review_bucket_summary,
        "review_bucket_count": count_query_gate_review_requests_fn(store_hash, request_status="requested"),
        "publications": publications,
        "publication_count": count_publications_fn(store_hash, active_only=True),
        "theme_hook_present": product_theme_hook_ready,
        "category_theme_hook_present": category_theme_hook_ready,
        "profile_summary": profile_summary,
        "readiness": readiness,
        "last_checked_display": format_timestamp_display_fn(readiness.get("updated_at")) if isinstance(readiness, dict) else None,
        "last_checked_relative": format_relative_time_fn(readiness.get("updated_at")) if isinstance(readiness, dict) else None,
        "feature_flags": (readiness.get("metadata") or {}).get("feature_flags", {}),
        "gate_summary": gate_summary,
        "latest_gate_run_id": latest_gate_run_id,
        "query_family_review": query_family_review,
        "suggested_target_type_summary": suggested_target_type_summary,
        "pass_count": pass_count,
        "hold_count": hold_count,
        "reject_count": reject_count,
        "published_count": published_count,
        "not_published_count": not_published_count,
        "results_report": results_report,
        "gsc_performance_summary": gsc_performance_summary,
        **admin_context,
    }


__all__ = [
    "admin_context_defaults",
    "build_dashboard_context",
    "build_public_dashboard_data",
    "populate_edge_case_admin_context",
    "populate_changed_route_admin_context",
]
