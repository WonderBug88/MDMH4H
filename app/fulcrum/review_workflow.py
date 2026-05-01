"""Review request workflow helpers for Fulcrum."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Callable

from psycopg2.extras import RealDictCursor

from app.fulcrum.platform import get_pg_conn, normalize_store_hash


def request_query_gate_review(
    store_hash: str,
    gate_record_id: int,
    target_entity_type: str | None,
    target_entity_id: int | None,
    target_name: str | None,
    target_url: str | None,
    reason_summary: str | None,
    requested_by: str | None,
    note: str | None = None,
    *,
    get_query_gate_record_by_id_fn: Callable[[str, int], dict[str, Any] | None],
) -> dict[str, Any] | None:
    gate_row = get_query_gate_record_by_id_fn(store_hash, gate_record_id)
    if not gate_row:
        return None

    normalized_store_hash = normalize_store_hash(store_hash)
    metadata = {
        "requested_from": "dashboard",
        "current_disposition": gate_row.get("disposition"),
        "query_intent_scope": gate_row.get("query_intent_scope"),
        "preferred_entity_type": gate_row.get("preferred_entity_type"),
    }
    sql = """
        INSERT INTO app_runtime.query_gate_review_requests (
            gate_record_id,
            run_id,
            store_hash,
            normalized_query_key,
            representative_query,
            source_url,
            source_name,
            source_entity_type,
            source_entity_id,
            current_page_type,
            target_entity_type,
            target_entity_id,
            target_name,
            target_url,
            reason_summary,
            request_status,
            request_note,
            metadata,
            requested_by
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'requested', %s, %s::jsonb, %s
        )
        ON CONFLICT (gate_record_id) DO UPDATE
        SET
            target_entity_type = EXCLUDED.target_entity_type,
            target_entity_id = EXCLUDED.target_entity_id,
            target_name = EXCLUDED.target_name,
            target_url = EXCLUDED.target_url,
            reason_summary = EXCLUDED.reason_summary,
            request_status = 'requested',
            request_note = EXCLUDED.request_note,
            metadata = EXCLUDED.metadata,
            requested_by = EXCLUDED.requested_by,
            updated_at = NOW()
        RETURNING
            request_id,
            gate_record_id,
            run_id,
            store_hash,
            normalized_query_key,
            representative_query,
            source_url,
            source_name,
            source_entity_type,
            source_entity_id,
            current_page_type,
            target_entity_type,
            target_entity_id,
            target_name,
            target_url,
            reason_summary,
            request_status,
            request_note,
            metadata,
            requested_by,
            created_at,
            updated_at;
    """
    params = (
        int(gate_row.get("gate_record_id") or 0),
        int(gate_row.get("run_id") or 0),
        normalized_store_hash,
        gate_row.get("normalized_query_key") or "",
        gate_row.get("representative_query") or "",
        gate_row.get("source_url") or "",
        gate_row.get("source_name") or "",
        gate_row.get("source_entity_type") or "product",
        int(gate_row.get("source_entity_id") or 0) or None,
        gate_row.get("current_page_type") or "",
        (target_entity_type or "").strip().lower() or None,
        int(target_entity_id or 0) or None,
        target_name or "",
        target_url or "",
        reason_summary or gate_row.get("reason_summary") or "",
        note or "User requested admin and agent review from the results table.",
        json.dumps(metadata),
        requested_by or "fulcrum",
    )
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
        conn.commit()
    return dict(row) if row else None


def update_query_gate_review_request_metadata(
    store_hash: str,
    request_id: int,
    metadata_updates: dict[str, Any],
) -> dict[str, Any] | None:
    if not request_id:
        return None
    sql = """
        UPDATE app_runtime.query_gate_review_requests
        SET metadata = COALESCE(metadata, '{}'::jsonb) || %s::jsonb,
            updated_at = NOW()
        WHERE store_hash = %s
          AND request_id = %s
        RETURNING
            request_id,
            gate_record_id,
            run_id,
            store_hash,
            normalized_query_key,
            representative_query,
            source_url,
            source_name,
            source_entity_type,
            source_entity_id,
            current_page_type,
            target_entity_type,
            target_entity_id,
            target_name,
            target_url,
            reason_summary,
            request_status,
            request_note,
            metadata,
            requested_by,
            created_at,
            updated_at;
    """
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (json.dumps(metadata_updates or {}), normalize_store_hash(store_hash), int(request_id)))
            row = cur.fetchone()
        conn.commit()
    return dict(row) if row else None



def candidate_source_bc_entity_id(
    row: dict[str, Any],
    *,
    entity_bc_id_fn: Callable[[str, int | None], int | None],
) -> int | None:
    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    raw_bc_id = metadata.get("source_bc_entity_id")
    try:
        bc_id = abs(int(raw_bc_id))
    except (TypeError, ValueError):
        bc_id = 0
    if bc_id > 0:
        return bc_id

    try:
        storage_id = int(row.get("source_product_id") or row.get("source_entity_id") or 0)
    except (TypeError, ValueError):
        storage_id = 0
    if storage_id == 0:
        return None

    entity_type = (row.get("source_entity_type") or "product").strip().lower() or "product"
    if entity_type == "product" and storage_id > 0:
        return storage_id

    decoded_bc_id = entity_bc_id_fn(entity_type, storage_id)
    if isinstance(decoded_bc_id, int) and decoded_bc_id > 0:
        return decoded_bc_id

    return abs(storage_id)


def candidate_matches_review_source(
    row: dict[str, Any],
    *,
    source_entity_type: str | None = None,
    source_entity_id: int | None = None,
    source_url: str | None = None,
    normalize_storefront_path_fn: Callable[[str | None], str],
    entity_bc_id_fn: Callable[[str, int | None], int | None],
) -> bool:
    normalized_source_type = (source_entity_type or "").strip().lower()
    candidate_source_type = (row.get("source_entity_type") or "product").strip().lower()
    if normalized_source_type and candidate_source_type != normalized_source_type:
        return False

    requested_bc_id = int(source_entity_id or 0)
    candidate_bc_id = candidate_source_bc_entity_id(row, entity_bc_id_fn=entity_bc_id_fn)
    if requested_bc_id > 0 and candidate_bc_id == requested_bc_id:
        return True

    normalized_source_url = normalize_storefront_path_fn(source_url)
    candidate_source_url = normalize_storefront_path_fn(row.get("source_url"))
    if normalized_source_url and candidate_source_url and candidate_source_url == normalized_source_url:
        return True

    if requested_bc_id > 0 or normalized_source_url:
        return False
    return True


def pause_source_for_review(
    store_hash: str,
    source_entity_id: int | None,
    source_entity_type: str = "product",
    reviewed_by: str | None = None,
    note: str | None = None,
    *,
    latest_candidate_rows_for_store: Callable[..., list[dict[str, Any]]],
    normalize_storefront_path_fn: Callable[[str | None], str],
    entity_bc_id_fn: Callable[[str, int | None], int | None],
    review_candidates_fn: Callable[[list[int], str, str | None, str | None], int],
    unpublish_entities_fn: Callable[[str, list[int]], list[dict[str, Any]]],
) -> dict[str, Any]:
    if source_entity_id in {None, 0}:
        return {
            "approved_candidate_count": 0,
            "review_reset_count": 0,
            "publication_count": 0,
            "live_block_paused": False,
            "publications": [],
        }

    normalized_store_hash = normalize_store_hash(store_hash)
    normalized_source_type = (source_entity_type or "product").strip().lower()
    source_rows = [
        row
        for row in latest_candidate_rows_for_store(normalized_store_hash, review_status="approved", limit=None)
        if candidate_matches_review_source(
            row,
            source_entity_type=normalized_source_type,
            source_entity_id=source_entity_id,
            source_url=None,
            normalize_storefront_path_fn=normalize_storefront_path_fn,
            entity_bc_id_fn=entity_bc_id_fn,
        )
    ]
    candidate_ids = [int(row.get("candidate_id") or 0) for row in source_rows if int(row.get("candidate_id") or 0)]
    review_reset_count = 0
    if candidate_ids:
        review_reset_count = review_candidates_fn(
            candidate_ids,
            "pending",
            reviewed_by or "fulcrum",
            note or "Paused for review from the results table.",
        )

    publication_source_ids = sorted(
        {
            int(row.get("source_product_id") or row.get("source_entity_id") or 0)
            for row in source_rows
            if int(row.get("source_product_id") or row.get("source_entity_id") or 0) != 0
        }
    )
    if not publication_source_ids:
        publication_source_ids = [int(source_entity_id)]

    publications = unpublish_entities_fn(normalized_store_hash, publication_source_ids)
    return {
        "approved_candidate_count": len(candidate_ids),
        "review_reset_count": review_reset_count,
        "publication_count": len(publications),
        "live_block_paused": bool(publications),
        "publications": publications,
    }


def list_query_gate_review_requests(
    store_hash: str,
    request_status: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    sql = """
        SELECT
            request_id,
            gate_record_id,
            run_id,
            store_hash,
            normalized_query_key,
            representative_query,
            source_url,
            source_name,
            source_entity_type,
            source_entity_id,
            current_page_type,
            target_entity_type,
            target_entity_id,
            target_name,
            target_url,
            reason_summary,
            request_status,
            request_note,
            metadata,
            requested_by,
            created_at,
            updated_at
        FROM app_runtime.query_gate_review_requests
        WHERE store_hash = %s
          AND (%s IS NULL OR request_status = %s)
        ORDER BY updated_at DESC, created_at DESC
        LIMIT %s;
    """
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (normalize_store_hash(store_hash), request_status, request_status, limit))
            return [dict(row) for row in cur.fetchall()]


def get_query_gate_review_request_by_id(store_hash: str, request_id: int) -> dict[str, Any] | None:
    sql = """
        SELECT
            request_id,
            gate_record_id,
            run_id,
            store_hash,
            normalized_query_key,
            representative_query,
            source_url,
            source_name,
            source_entity_type,
            source_entity_id,
            current_page_type,
            target_entity_type,
            target_entity_id,
            target_name,
            target_url,
            reason_summary,
            request_status,
            request_note,
            metadata,
            requested_by,
            created_at,
            updated_at
        FROM app_runtime.query_gate_review_requests
        WHERE store_hash = %s
          AND request_id = %s
        LIMIT 1;
    """
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (normalize_store_hash(store_hash), int(request_id)))
            row = cur.fetchone()
    return dict(row) if row else None


def count_query_gate_review_requests(store_hash: str, request_status: str | None = None) -> int:
    sql = """
        SELECT COUNT(*)
        FROM app_runtime.query_gate_review_requests
        WHERE store_hash = %s
          AND (%s IS NULL OR request_status = %s);
    """
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (normalize_store_hash(store_hash), request_status, request_status))
            row = cur.fetchone()
    return int((row or [0])[0] or 0)


def resolve_query_gate_review_request(
    store_hash: str,
    request_id: int,
    *,
    resolved_by: str | None = None,
    resolution_note: str | None = None,
    metadata_updates: dict[str, Any] | None = None,
    get_query_gate_review_request_by_id_fn: Callable[[str, int], dict[str, Any] | None],
) -> dict[str, Any] | None:
    request_row = get_query_gate_review_request_by_id_fn(store_hash, request_id)
    if not request_row:
        return None

    metadata = dict(request_row.get("metadata") or {})
    metadata.update(metadata_updates or {})
    metadata["resolved_at"] = datetime.now().astimezone().isoformat()
    metadata["resolved_by"] = resolved_by or "fulcrum"
    if resolution_note:
        metadata["resolution_note"] = resolution_note

    sql = """
        UPDATE app_runtime.query_gate_review_requests
        SET
            request_status = 'resolved',
            request_note = %s,
            metadata = %s::jsonb,
            updated_at = NOW()
        WHERE store_hash = %s
          AND request_id = %s
        RETURNING
            request_id,
            gate_record_id,
            run_id,
            store_hash,
            normalized_query_key,
            representative_query,
            source_url,
            source_name,
            source_entity_type,
            source_entity_id,
            current_page_type,
            target_entity_type,
            target_entity_id,
            target_name,
            target_url,
            reason_summary,
            request_status,
            request_note,
            metadata,
            requested_by,
            created_at,
            updated_at;
    """
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                sql,
                (
                    resolution_note or request_row.get("request_note") or "Investigation completed.",
                    json.dumps(metadata),
                    normalize_store_hash(store_hash),
                    int(request_id),
                ),
            )
            row = cur.fetchone()
        conn.commit()
    return dict(row) if row else None


def review_all_edge_cases(
    store_hash: str,
    initiated_by: str | None = None,
    *,
    list_query_gate_review_requests_fn: Callable[[str, str | None, int], list[dict[str, Any]]],
    run_query_gate_agent_review_fn: Callable[..., dict[str, Any]],
) -> dict[str, Any]:
    review_requests = list_query_gate_review_requests_fn(store_hash, request_status="requested", limit=1000)
    if not review_requests:
        return {
            "status": "skipped",
            "reason": "No edge cases are waiting for review.",
            "request_count": 0,
            "reviewed_count": 0,
            "stored_count": 0,
            "summary": {"correct": 0, "incorrect": 0, "unclear": 0},
            "results": [],
        }

    run_groups: dict[int, list[int]] = {}
    for row in review_requests:
        gate_record_id = int(row.get("gate_record_id") or 0)
        run_id = int(row.get("run_id") or 0)
        if gate_record_id <= 0:
            continue
        run_groups.setdefault(run_id, []).append(gate_record_id)

    if not run_groups:
        return {
            "status": "skipped",
            "reason": "The open edge cases do not have usable query-family rows yet.",
            "request_count": len(review_requests),
            "reviewed_count": 0,
            "stored_count": 0,
            "summary": {"correct": 0, "incorrect": 0, "unclear": 0},
            "results": [],
        }

    aggregate_summary = {"correct": 0, "incorrect": 0, "unclear": 0}
    aggregate_results: list[dict[str, Any]] = []
    overall_status = "skipped"
    reviewed_count = 0
    stored_count = 0
    reasons: list[str] = []

    for run_id, gate_record_ids in sorted(run_groups.items()):
        result = run_query_gate_agent_review_fn(
            store_hash=store_hash,
            run_id=run_id or None,
            gate_record_ids=gate_record_ids,
            limit=max(len(gate_record_ids), 1),
            initiated_by=initiated_by,
        )
        aggregate_results.append(result)
        reviewed_count += int(result.get("reviewed_count") or 0)
        stored_count += int(result.get("stored_count") or 0)
        summary = dict(result.get("summary") or {})
        for key in aggregate_summary:
            aggregate_summary[key] += int(summary.get(key) or 0)
        status = (result.get("status") or "").strip().lower()
        if status == "ok":
            overall_status = "ok"
        elif overall_status != "ok" and status:
            overall_status = status
        reason = (result.get("reason") or "").strip()
        if reason:
            reasons.append(reason)

    return {
        "status": overall_status,
        "reason": "; ".join(dict.fromkeys(reasons)) if reasons else "",
        "request_count": len(review_requests),
        "reviewed_count": reviewed_count,
        "stored_count": stored_count,
        "summary": aggregate_summary,
        "results": aggregate_results,
    }


def candidate_target_bc_entity_id(
    row: dict[str, Any],
    *,
    entity_bc_id_fn: Callable[[str, int | None], int | None],
) -> int | None:
    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    raw_bc_id = metadata.get("target_bc_entity_id")
    try:
        bc_id = abs(int(raw_bc_id))
    except (TypeError, ValueError):
        bc_id = 0
    if bc_id > 0:
        return bc_id

    try:
        storage_id = int(row.get("target_entity_id") or row.get("target_product_id") or 0)
    except (TypeError, ValueError):
        storage_id = 0
    if storage_id == 0:
        return None

    entity_type = (row.get("target_entity_type") or "product").strip().lower() or "product"
    if entity_type == "product" and storage_id > 0:
        return storage_id

    decoded_bc_id = entity_bc_id_fn(entity_type, storage_id)
    if isinstance(decoded_bc_id, int) and decoded_bc_id > 0:
        return decoded_bc_id

    return abs(storage_id)


def candidate_matches_review_target(
    row: dict[str, Any],
    *,
    target_entity_type: str | None = None,
    target_entity_id: int | None = None,
    target_url: str | None = None,
    normalize_storefront_path_fn: Callable[[str | None], str],
    entity_bc_id_fn: Callable[[str, int | None], int | None],
) -> bool:
    normalized_target_type = (target_entity_type or "").strip().lower()
    candidate_target_type = (row.get("target_entity_type") or "product").strip().lower()
    if normalized_target_type and candidate_target_type != normalized_target_type:
        return False

    requested_bc_id = int(target_entity_id or 0)
    candidate_bc_id = candidate_target_bc_entity_id(row, entity_bc_id_fn=entity_bc_id_fn)
    if requested_bc_id > 0 and candidate_bc_id == requested_bc_id:
        return True

    normalized_target_url = normalize_storefront_path_fn(target_url)
    candidate_target_url = normalize_storefront_path_fn(row.get("target_url"))
    if normalized_target_url and candidate_target_url and candidate_target_url == normalized_target_url:
        return True

    if requested_bc_id > 0 or normalized_target_url:
        return False
    return True


def restore_source_after_review(
    store_hash: str,
    source_entity_id: int | None,
    source_entity_type: str,
    *,
    target_entity_type: str | None = None,
    target_entity_id: int | None = None,
    target_url: str | None = None,
    reviewed_by: str | None = None,
    note: str | None = None,
    latest_candidate_rows_for_store: Callable[..., list[dict[str, Any]]],
    normalize_storefront_path_fn: Callable[[str | None], str],
    entity_bc_id_fn: Callable[[str, int | None], int | None],
    review_candidates_fn: Callable[[list[int], str, str | None, str | None], int],
    publish_approved_entities_fn: Callable[[str, list[int] | None], list[dict[str, Any]]],
) -> dict[str, Any]:
    if source_entity_id in {None, 0}:
        return {
            "approved_candidate_count": 0,
            "publication_count": 0,
            "live_block_restored": False,
            "publications": [],
        }

    normalized_store_hash = normalize_store_hash(store_hash)
    normalized_source_type = (source_entity_type or "product").strip().lower()
    normalized_target_type = (target_entity_type or "").strip().lower()
    source_id = int(source_entity_id)
    target_id = int(target_entity_id or 0)
    normalized_target_url = normalize_storefront_path_fn(target_url)

    source_rows = [
        row
        for row in latest_candidate_rows_for_store(normalized_store_hash, review_status=None, limit=None)
        if candidate_matches_review_source(
            row,
            source_entity_type=normalized_source_type,
            source_entity_id=source_id,
            source_url=None,
            normalize_storefront_path_fn=normalize_storefront_path_fn,
            entity_bc_id_fn=entity_bc_id_fn,
        )
    ]
    if normalized_target_type or target_id or normalized_target_url:
        source_rows = [
            row
            for row in source_rows
            if candidate_matches_review_target(
                row,
                target_entity_type=normalized_target_type,
                target_entity_id=target_id or None,
                target_url=normalized_target_url or None,
                normalize_storefront_path_fn=normalize_storefront_path_fn,
                entity_bc_id_fn=entity_bc_id_fn,
            )
        ]

    pending_candidate_ids = [
        int(row.get("candidate_id") or 0)
        for row in source_rows
        if (row.get("review_status") or "").strip().lower() == "pending" and int(row.get("candidate_id") or 0)
    ]
    approved_count = 0
    if pending_candidate_ids:
        approved_count = review_candidates_fn(
            pending_candidate_ids,
            "approved",
            reviewed_by or "fulcrum",
            note or "Restored after support investigation.",
        )

    publish_source_ids = sorted(
        {
            int(row.get("source_product_id") or row.get("source_entity_id") or 0)
            for row in source_rows
            if int(row.get("source_product_id") or row.get("source_entity_id") or 0) != 0
        }
    )
    if not publish_source_ids:
        publish_source_ids = [source_id]

    publications = publish_approved_entities_fn(normalized_store_hash, publish_source_ids)
    return {
        "approved_candidate_count": approved_count,
        "publication_count": len(publications),
        "live_block_restored": bool(publications),
        "publications": publications,
    }


__all__ = [
    "candidate_matches_review_target",
    "candidate_target_bc_entity_id",
    "count_query_gate_review_requests",
    "get_query_gate_review_request_by_id",
    "list_query_gate_review_requests",
    "pause_source_for_review",
    "request_query_gate_review",
    "resolve_query_gate_review_request",
    "restore_source_after_review",
    "review_all_edge_cases",
    "update_query_gate_review_request_metadata",
]
