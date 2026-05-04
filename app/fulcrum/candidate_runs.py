"""Candidate-run orchestration helpers for Fulcrum."""

from __future__ import annotations

import json
import re
from typing import Any, Callable

from psycopg2.extras import RealDictCursor, execute_batch


def _candidate_text_tokens(value: str | None) -> set[str]:
    return {token for token in re.split(r"[^a-z0-9]+", str(value or "").lower()) if token}


def _candidate_target_tokens(row: dict[str, Any]) -> set[str]:
    return _candidate_text_tokens(f"{row.get('target_name') or ''} {row.get('target_url') or ''}")


def _brand_navigation_category_allowed(row: dict[str, Any], metadata: dict[str, Any]) -> bool:
    semantics = metadata.get("semantics_analysis") if isinstance(metadata.get("semantics_analysis"), dict) else {}
    target_tokens = _candidate_target_tokens(row)
    query_target_tokens = {
        token
        for value in list(metadata.get("query_target_tokens") or [])
        for token in _candidate_text_tokens(str(value))
    }
    if query_target_tokens and query_target_tokens & target_tokens:
        return True

    if semantics.get("thin_brand_family_category_fallback"):
        family_tokens: set[str] = set()
        head_term = str(semantics.get("head_term") or "").strip().lower()
        if head_term:
            family_tokens.add(head_term)
            family_tokens.add(f"{head_term}s")
        for rule in semantics.get("constraint_rules") or []:
            if (rule.get("kind") or "").strip().lower() == "thin_brand_family_prefer_category":
                family_tokens.update(str(token).strip().lower() for token in (rule.get("family_tokens") or []) if token)
        return bool(family_tokens & target_tokens)

    return False


def queue_candidate_run(
    store_hash: str,
    initiated_by: str | None = None,
    cluster: str | None = None,
    max_links_per_product: int = 4,
    min_hit_count: int = 3,
    limit_total: int = 300,
    run_source: str = "manual",
    *,
    normalize_store_hash_fn: Callable[[str | None], str],
    find_active_run_fn: Callable[[str], dict[str, Any] | None],
    create_run_fn: Callable[..., int],
    start_generation_worker_fn: Callable[[int], dict[str, Any]],
    complete_run_fn: Callable[..., Any],
) -> dict[str, Any]:
    normalized_hash = normalize_store_hash_fn(store_hash)
    active_run = find_active_run_fn(normalized_hash)
    if active_run:
        return {
            "queued": False,
            "duplicate": True,
            "run_id": int(active_run["run_id"]),
            "status": active_run.get("status") or "running",
        }

    run_id = create_run_fn(
        store_hash=normalized_hash,
        initiated_by=initiated_by,
        run_source=run_source,
        status="queued",
        filters={
            "cluster": cluster,
            "max_links_per_product": max_links_per_product,
            "min_hit_count": min_hit_count,
            "limit_total": limit_total,
        },
    )
    start_result = start_generation_worker_fn(run_id)
    if not start_result.get("started"):
        complete_run_fn(run_id, "failed", notes=start_result.get("reason") or "Failed to start generation worker.")
        return {
            "queued": False,
            "duplicate": False,
            "run_id": run_id,
            "status": "failed",
            "reason": start_result.get("reason") or "Failed to start generation worker.",
        }

    return {
        "queued": True,
        "duplicate": False,
        "run_id": run_id,
        "status": "queued",
        "worker_pid": start_result.get("pid"),
    }


def eligible_auto_publish_candidates(
    store_hash: str,
    run_id: int,
    *,
    refresh_store_readiness_fn: Callable[[str], dict[str, Any]],
    get_pg_conn_fn: Callable[[], Any],
    normalize_store_hash_fn: Callable[[str | None], str],
    category_publishing_enabled_for_store_fn: Callable[[str], bool],
    auto_publish_min_score: float,
    auto_publish_max_links_per_source: int,
) -> list[dict[str, Any]]:
    readiness = refresh_store_readiness_fn(store_hash)
    if not readiness.get("auto_publish_ready"):
        return []

    sql = """
        SELECT
            candidate_id,
            source_entity_type,
            target_entity_type,
            source_entity_id,
            target_entity_id,
            source_product_id,
            target_product_id,
            source_name,
            target_name,
            anchor_label,
            score,
            metadata
        FROM app_runtime.link_candidates
        WHERE store_hash = %s
          AND run_id = %s
          AND review_status = 'pending'
        ORDER BY source_product_id, score DESC, candidate_id;
    """
    with get_pg_conn_fn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (normalize_store_hash_fn(store_hash), run_id))
            rows = [dict(row) for row in cur.fetchall()]

    selected: list[dict[str, Any]] = []
    selected_by_source: dict[int, int] = {}
    seen_pairs: set[tuple[int, int]] = set()
    seen_labels: dict[int, set[str]] = {}
    min_score = float(auto_publish_min_score)
    max_links_per_source = max(1, int(auto_publish_max_links_per_source))
    category_enabled = category_publishing_enabled_for_store_fn(store_hash)

    for row in rows:
        source_entity_type = row.get("source_entity_type") or "product"
        if source_entity_type not in {"product", "category"}:
            continue
        if source_entity_type == "category" and not category_enabled:
            continue
        target_entity_type = row.get("target_entity_type") or "product"
        metadata = row.get("metadata") or {}
        if candidate_publish_block_reason(row, category_enabled):
            continue
        if float(row.get("score") or 0) < min_score:
            continue

        source_id = int(row["source_product_id"])
        target_id = int(row["target_product_id"])
        if source_id == target_id:
            continue
        if selected_by_source.get(source_id, 0) >= max_links_per_source:
            continue
        if (source_id, target_id) in seen_pairs:
            continue

        shared_tokens = list(metadata.get("shared_tokens") or [])
        query_target_tokens = list(metadata.get("query_target_tokens") or [])
        fuzzy_signal = metadata.get("fuzzy_signal") or {}
        if not shared_tokens and not query_target_tokens and not (
            fuzzy_signal.get("active") and float(fuzzy_signal.get("score") or 0.0) >= 82.0
        ):
            continue
        if target_entity_type == "brand" and not query_target_tokens:
            continue

        label_key = (row.get("anchor_label") or "").strip().lower()
        source_labels = seen_labels.setdefault(source_id, set())
        if not label_key or label_key in source_labels:
            continue

        selected.append(row)
        selected_by_source[source_id] = selected_by_source.get(source_id, 0) + 1
        seen_pairs.add((source_id, target_id))
        source_labels.add(label_key)

    return selected


def auto_approve_and_publish_run(
    store_hash: str,
    run_id: int,
    *,
    auto_publish_enabled: bool,
    refresh_store_readiness_fn: Callable[[str], dict[str, Any]],
    publish_all_current_results_fn: Callable[[str, str | None], dict[str, Any]],
) -> dict[str, Any]:
    if not auto_publish_enabled:
        return {
            "auto_publish_enabled": False,
            "auto_publish_ready": False,
            "auto_approved_count": 0,
            "auto_published_count": 0,
            "published_entities": [],
        }

    readiness = refresh_store_readiness_fn(store_hash)
    if not readiness.get("auto_publish_ready"):
        return {
            "auto_publish_enabled": True,
            "auto_publish_ready": False,
            "auto_approved_count": 0,
            "auto_published_count": 0,
            "published_entities": [],
        }

    result = publish_all_current_results_fn(store_hash, "fulcrum-auto")
    return {
        "auto_publish_enabled": True,
        "auto_publish_ready": True,
        "auto_approved_count": int(result.get("approved_count") or 0),
        "auto_published_count": int(result.get("published_count") or 0),
        "published_entities": list(result.get("publications") or []),
        "blocked_source_count": int(result.get("blocked_source_count") or 0),
        "publishable_pending_count": int(result.get("publishable_pending_count") or 0),
        "pending_row_count": int(result.get("pending_row_count") or 0),
        "approved_source_count": int(result.get("approved_source_count") or 0),
        "unresolved_approved_source_count": int(result.get("unresolved_approved_source_count") or 0),
    }


def execute_candidate_run_impl(
    run_id: int,
    *,
    store_hash: str,
    cluster: str | None = None,
    max_links_per_product: int = 4,
    min_hit_count: int = 3,
    limit_total: int = 300,
    refresh_store_readiness_fn: Callable[[str], dict[str, Any]],
    build_unified_entity_index_fn: Callable[..., dict[str, Any]],
    build_query_gate_records_fn: Callable[..., list[dict[str, Any]]],
    annotate_query_gate_rows_with_suggestions_fn: Callable[..., list[dict[str, Any]]],
    store_query_gate_records_fn: Callable[[int, str, list[dict[str, Any]]], Any],
    load_query_target_overrides_fn: Callable[[str], dict[str, Any]],
    load_review_feedback_maps_fn: Callable[[str], dict[str, Any]],
    build_store_signal_library_fn: Callable[[str], dict[str, Any]],
    direct_route_candidates_from_gsc_fn: Callable[..., list[dict[str, Any]]],
    rank_source_rows_fn: Callable[[list[dict[str, Any]]], list[dict[str, Any]]],
    get_pg_conn_fn: Callable[[], Any],
    auto_approve_and_publish_run_fn: Callable[[str, int], dict[str, Any]],
    complete_run_fn: Callable[..., Any],
) -> dict[str, Any]:
    refresh_store_readiness_fn(store_hash)

    try:
        entity_index = build_unified_entity_index_fn(store_hash, cluster=cluster)
        gate_rows = build_query_gate_records_fn(
            store_hash=store_hash,
            source_profiles=entity_index["sources"],
            target_entities=entity_index["targets"],
            min_hit_count=min_hit_count,
            limit_total=max(limit_total * 2, 80),
        )
        gate_rows = annotate_query_gate_rows_with_suggestions_fn(
            store_hash,
            gate_rows,
            cluster=cluster,
            source_profiles=entity_index["sources"],
            target_entities=entity_index["targets"],
            overrides=load_query_target_overrides_fn(store_hash),
            review_feedback_maps=load_review_feedback_maps_fn(store_hash),
            signal_library=build_store_signal_library_fn(store_hash),
            cache_snapshots=True,
        )
        store_query_gate_records_fn(run_id, store_hash, gate_rows)
        gate_counts = {
            "pass": sum(1 for row in gate_rows if (row.get("disposition") or "hold") == "pass"),
            "hold": sum(1 for row in gate_rows if (row.get("disposition") or "hold") == "hold"),
            "reject": sum(1 for row in gate_rows if (row.get("disposition") or "hold") == "reject"),
        }
        candidate_rows = direct_route_candidates_from_gsc_fn(
            store_hash=store_hash,
            cluster=cluster,
            min_hit_count=min_hit_count,
            limit_total=max(limit_total * 2, 80),
            entity_index=entity_index,
            gate_rows=gate_rows,
        )
        grouped_rows: dict[tuple[str, int], list[dict[str, Any]]] = {}
        for row in candidate_rows:
            key = (
                row.get("source_entity_type") or "product",
                int(row.get("source_product_id") or 0),
            )
            grouped_rows.setdefault(key, []).append(row)

        rows: list[dict[str, Any]] = []
        for (source_entity_type, _), source_rows in grouped_rows.items():
            rows.extend(rank_source_rows_fn(source_rows, source_entity_type=source_entity_type))
        rows.sort(key=lambda item: (-float(item["score"]), item["source_product_id"], item["target_product_id"]))
        rows = rows[:limit_total]

        with get_pg_conn_fn() as conn:
            records = []
            for row in rows:
                records.append(
                    (
                        run_id,
                        store_hash,
                        row["source_product_id"],
                        row["source_name"],
                        row["source_url"],
                        row["target_product_id"],
                        row["target_name"],
                        row["target_url"],
                        row["relation_type"],
                        row["example_query"],
                        row["anchor_label"],
                        row["hit_count"],
                        row["score"],
                        row.get("source_entity_type", "product"),
                        row.get("target_entity_type", "product"),
                        row["source_product_id"],
                        row["target_product_id"],
                        json.dumps(row["metadata"]),
                    )
                )

            insert_sql = """
                INSERT INTO app_runtime.link_candidates (
                    run_id,
                    store_hash,
                    source_product_id,
                    source_name,
                    source_url,
                    target_product_id,
                    target_name,
                    target_url,
                    relation_type,
                    example_query,
                    anchor_label,
                    hit_count,
                    score,
                    source_entity_type,
                    target_entity_type,
                    source_entity_id,
                    target_entity_id,
                    metadata
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb);
            """

            with conn.cursor() as cur:
                if records:
                    execute_batch(cur, insert_sql, records, page_size=200)
            conn.commit()

        auto_publish = auto_approve_and_publish_run_fn(store_hash, run_id)
        note_bits = [
            f"Gate: {gate_counts['pass']} pass, {gate_counts['hold']} hold, {gate_counts['reject']} reject.",
            f"Generated {len(rows)} intentional candidates from direct GSC entity routing.",
        ]
        if auto_publish["auto_approved_count"]:
            note_bits.append(
                f"Auto-approved {auto_publish['auto_approved_count']} candidate(s) and published {auto_publish['auto_published_count']} source block(s)."
            )
        if auto_publish.get("unresolved_approved_source_count"):
            note_bits.append(
                f"{auto_publish['unresolved_approved_source_count']} approved source(s) still were not live after publish."
            )
        complete_run_fn(run_id, "completed", notes=" ".join(note_bits))
        return {
            "run_id": run_id,
            "candidate_count": len(rows),
            "gate_counts": gate_counts,
            **auto_publish,
        }
    except Exception as exc:
        complete_run_fn(run_id, "failed", notes=str(exc))
        raise


def execute_candidate_run(
    run_id: int,
    *,
    get_run_fn: Callable[[int], dict[str, Any] | None],
    normalize_store_hash_fn: Callable[[str | None], str],
    mark_run_running_fn: Callable[[int], Any],
    execute_candidate_run_impl_fn: Callable[..., dict[str, Any]],
) -> dict[str, Any]:
    run = get_run_fn(run_id)
    if not run:
        raise ValueError(f"Run {run_id} does not exist.")

    filters = dict(run.get("filters") or {})
    store_hash = normalize_store_hash_fn(run.get("store_hash"))
    cluster = (filters.get("cluster") or None) if isinstance(filters, dict) else None
    max_links_per_product = int(filters.get("max_links_per_product", 4)) if isinstance(filters, dict) else 4
    min_hit_count = int(filters.get("min_hit_count", 3)) if isinstance(filters, dict) else 3
    limit_total = int(filters.get("limit_total", 300)) if isinstance(filters, dict) else 300

    mark_run_running_fn(run_id)
    return execute_candidate_run_impl_fn(
        run_id,
        store_hash=store_hash,
        cluster=cluster,
        max_links_per_product=max_links_per_product,
        min_hit_count=min_hit_count,
        limit_total=limit_total,
    )


def generate_candidate_run(
    store_hash: str,
    initiated_by: str | None = None,
    cluster: str | None = None,
    max_links_per_product: int = 4,
    min_hit_count: int = 3,
    limit_total: int = 300,
    *,
    create_run_fn: Callable[..., int],
    execute_candidate_run_impl_fn: Callable[..., dict[str, Any]],
) -> dict[str, Any]:
    run_id = create_run_fn(
        store_hash=store_hash,
        initiated_by=initiated_by,
        filters={
            "cluster": cluster,
            "max_links_per_product": max_links_per_product,
            "min_hit_count": min_hit_count,
            "limit_total": limit_total,
        },
        status="running",
    )
    return execute_candidate_run_impl_fn(
        run_id,
        store_hash=store_hash,
        cluster=cluster,
        max_links_per_product=max_links_per_product,
        min_hit_count=min_hit_count,
        limit_total=limit_total,
    )


def review_request_source_key(row: dict[str, Any]) -> tuple[str, int]:
    return (
        (row.get("source_entity_type") or "product").strip().lower() or "product",
        int(row.get("source_entity_id") or row.get("source_product_id") or 0),
    )


def candidate_source_key(row: dict[str, Any]) -> tuple[str, int]:
    return (
        (row.get("source_entity_type") or "product").strip().lower() or "product",
        int(row.get("source_entity_id") or row.get("source_product_id") or 0),
    )


def candidate_publish_block_reason(row: dict[str, Any], category_enabled: bool) -> str:
    source_entity_type = (row.get("source_entity_type") or "product").strip().lower() or "product"
    target_entity_type = (row.get("target_entity_type") or "product").strip().lower() or "product"
    metadata = row.get("metadata") or {}
    query_intent_scope = (metadata.get("query_intent_scope") or "").strip().lower()
    preferred_entity_type = (metadata.get("preferred_entity_type") or "").strip().lower()

    if source_entity_type == "category" and not category_enabled:
        return "category source publishing is disabled"
    if target_entity_type not in {"product", "category", "brand"}:
        return f"unsupported target entity type `{target_entity_type}`"
    if target_entity_type == "brand":
        if query_intent_scope != "brand_navigation" or preferred_entity_type != "brand":
            return "brand targets require brand-navigation intent"
        if not list(metadata.get("query_target_tokens") or []):
            return "brand target did not preserve query target tokens"
    if target_entity_type == "category":
        if preferred_entity_type == "category":
            return ""
        if query_intent_scope == "brand_navigation" and preferred_entity_type == "brand":
            if _brand_navigation_category_allowed(row, metadata):
                return ""
            return "brand-navigation category target did not preserve brand or family intent"
        if preferred_entity_type != "category":
            return "category target does not match preferred entity type"
    return ""


def publish_all_current_results(
    store_hash: str,
    initiated_by: str | None = None,
    *,
    normalize_store_hash_fn: Callable[[str | None], str],
    category_publishing_enabled_for_store_fn: Callable[[str], bool],
    list_query_gate_review_requests_fn: Callable[..., list[dict[str, Any]]],
    latest_candidate_rows_for_store_fn: Callable[..., list[dict[str, Any]]],
    include_dashboard_candidate_fn: Callable[[dict[str, Any], str, bool], bool],
    review_candidates_fn: Callable[..., int],
    publish_approved_entities_fn: Callable[..., list[dict[str, Any]]],
) -> dict[str, Any]:
    normalized_store_hash = normalize_store_hash_fn(store_hash)
    category_enabled = category_publishing_enabled_for_store_fn(normalized_store_hash)
    blocked_sources = {
        review_request_source_key(row)
        for row in list_query_gate_review_requests_fn(normalized_store_hash, request_status="requested", limit=1000)
    }

    pending_rows = [
        row
        for row in latest_candidate_rows_for_store_fn(normalized_store_hash, review_status="pending", limit=None)
        if include_dashboard_candidate_fn(row, "pending", category_enabled)
    ]
    pending_policy_blocked_rows = [
        row
        for row in pending_rows
        if candidate_source_key(row) not in blocked_sources
        and candidate_publish_block_reason(row, category_enabled)
    ]
    publishable_pending_rows = [
        row
        for row in pending_rows
        if candidate_source_key(row) not in blocked_sources
        and not candidate_publish_block_reason(row, category_enabled)
    ]
    pending_candidate_ids = [
        int(row.get("candidate_id") or 0)
        for row in publishable_pending_rows
        if int(row.get("candidate_id") or 0) > 0
    ]

    approved_count = 0
    if pending_candidate_ids:
        approved_count = review_candidates_fn(
            pending_candidate_ids,
            "approved",
            reviewed_by=initiated_by or "fulcrum",
            note="Bulk approved during Publish All Results.",
        )

    approved_rows = [
        row
        for row in latest_candidate_rows_for_store_fn(normalized_store_hash, review_status="approved", limit=None)
        if candidate_source_key(row) not in blocked_sources
        and not candidate_publish_block_reason(row, category_enabled)
    ]
    approved_policy_blocked_rows = [
        row
        for row in latest_candidate_rows_for_store_fn(normalized_store_hash, review_status="approved", limit=None)
        if candidate_source_key(row) not in blocked_sources
        and candidate_publish_block_reason(row, category_enabled)
    ]
    approved_source_keys = {
        candidate_source_key(row)
        for row in approved_rows
        if int(row.get("source_product_id") or row.get("source_entity_id") or 0)
    }
    source_entity_ids = sorted(
        {
            int(row.get("source_product_id") or row.get("source_entity_id") or 0)
            for row in approved_rows
            if int(row.get("source_product_id") or row.get("source_entity_id") or 0)
        }
    )
    publications = publish_approved_entities_fn(
        normalized_store_hash,
        source_entity_ids=source_entity_ids or None,
    ) if source_entity_ids else []
    published_source_keys = {
        (
            (publication.get("source_entity_type") or "product").strip().lower() or "product",
            int(publication.get("source_product_id") or publication.get("source_entity_id") or 0),
        )
        for publication in publications
        if int(publication.get("source_product_id") or publication.get("source_entity_id") or 0)
        and not str(publication.get("status") or "").strip().lower().startswith("skipped")
    }
    approved_source_count = len(approved_source_keys)
    published_source_count = len(published_source_keys)
    unresolved_approved_source_count = max(approved_source_count - published_source_count, 0)

    return {
        "status": "ok",
        "blocked_source_count": len(blocked_sources),
        "policy_blocked_candidate_count": len(pending_policy_blocked_rows) + len(approved_policy_blocked_rows),
        "pending_row_count": len(pending_rows),
        "approved_count": approved_count,
        "publishable_pending_count": len(publishable_pending_rows),
        "approved_source_count": approved_source_count,
        "published_count": published_source_count,
        "published_source_count": published_source_count,
        "unresolved_approved_source_count": unresolved_approved_source_count,
        "publications": publications,
    }


__all__ = [
    "auto_approve_and_publish_run",
    "candidate_publish_block_reason",
    "candidate_source_key",
    "eligible_auto_publish_candidates",
    "execute_candidate_run",
    "execute_candidate_run_impl",
    "generate_candidate_run",
    "publish_all_current_results",
    "queue_candidate_run",
    "review_request_source_key",
]
