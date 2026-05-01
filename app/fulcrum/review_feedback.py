"""Review-feedback aggregation helpers for Fulcrum routing."""

from __future__ import annotations

from typing import Any, Callable

from psycopg2.extras import RealDictCursor

from app.fulcrum.platform import get_pg_conn, normalize_store_hash


def increment_review_feedback_bucket(
    buckets: dict[Any, dict[str, int]],
    key: Any,
    review_status: str | None,
) -> None:
    if key in {None, ""}:
        return
    normalized_status = (review_status or "").strip().lower()
    if normalized_status not in {"approved", "reject"}:
        return
    bucket = buckets.setdefault(key, {"approved_count": 0, "rejected_count": 0})
    if normalized_status == "approved":
        bucket["approved_count"] += 1
    elif normalized_status == "reject":
        bucket["rejected_count"] += 1


def load_review_feedback_maps(
    store_hash: str,
    *,
    normalize_query_family_key_fn: Callable[[str | None], str],
) -> dict[str, dict[Any, dict[str, int]]]:
    normalized_hash = normalize_store_hash(store_hash)
    sql = """
        WITH latest_reviews AS (
            SELECT DISTINCT ON (r.candidate_id)
                r.candidate_id,
                r.review_status
            FROM app_runtime.link_reviews r
            JOIN app_runtime.link_candidates c
              ON c.candidate_id = r.candidate_id
            WHERE c.store_hash = %s
              AND r.review_status IN ('approved', 'reject')
            ORDER BY r.candidate_id, r.reviewed_at DESC, r.review_id DESC
        )
        SELECT
            c.source_entity_type,
            COALESCE(c.source_entity_id, c.source_product_id) AS source_entity_id,
            c.target_entity_type,
            COALESCE(c.target_entity_id, c.target_product_id) AS target_entity_id,
            c.example_query,
            lr.review_status
        FROM latest_reviews lr
        JOIN app_runtime.link_candidates c
          ON c.candidate_id = lr.candidate_id;
    """
    feedback_maps: dict[str, dict[Any, dict[str, int]]] = {
        "pair": {},
        "family_target": {},
        "target": {},
    }
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (normalized_hash,))
            rows = [dict(row) for row in cur.fetchall()]

    for row in rows:
        source_entity_type = (row.get("source_entity_type") or "product").strip().lower() or "product"
        target_entity_type = (row.get("target_entity_type") or "product").strip().lower() or "product"
        source_entity_id = int(row.get("source_entity_id") or 0)
        target_entity_id = int(row.get("target_entity_id") or 0)
        review_status = row.get("review_status")
        if not source_entity_id or not target_entity_id:
            continue
        family_key = normalize_query_family_key_fn(row.get("example_query"))
        increment_review_feedback_bucket(
            feedback_maps["pair"],
            (source_entity_type, source_entity_id, target_entity_type, target_entity_id),
            review_status,
        )
        if family_key:
            increment_review_feedback_bucket(
                feedback_maps["family_target"],
                (family_key, target_entity_type, target_entity_id),
                review_status,
            )
        increment_review_feedback_bucket(
            feedback_maps["target"],
            (target_entity_type, target_entity_id),
            review_status,
        )

    override_sql = """
        SELECT
            normalized_query_key,
            source_entity_type,
            source_entity_id,
            target_entity_type,
            target_entity_id
        FROM app_runtime.query_target_overrides
        WHERE store_hash = %s;
    """
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(override_sql, (normalized_hash,))
            override_rows = [dict(row) for row in cur.fetchall()]

    for row in override_rows:
        source_entity_type = (row.get("source_entity_type") or "product").strip().lower() or "product"
        target_entity_type = (row.get("target_entity_type") or "product").strip().lower() or "product"
        source_entity_id = int(row.get("source_entity_id") or 0)
        target_entity_id = int(row.get("target_entity_id") or 0)
        family_key = normalize_query_family_key_fn(row.get("normalized_query_key"))
        if source_entity_id and target_entity_id:
            pair_bucket = feedback_maps["pair"].setdefault(
                (source_entity_type, source_entity_id, target_entity_type, target_entity_id),
                {"approved_count": 0, "rejected_count": 0},
            )
            pair_bucket["approved_count"] += 2
        if family_key and target_entity_id:
            family_bucket = feedback_maps["family_target"].setdefault(
                (family_key, target_entity_type, target_entity_id),
                {"approved_count": 0, "rejected_count": 0},
            )
            family_bucket["approved_count"] += 2
        if target_entity_id:
            target_bucket = feedback_maps["target"].setdefault(
                (target_entity_type, target_entity_id),
                {"approved_count": 0, "rejected_count": 0},
            )
            target_bucket["approved_count"] += 1
    return feedback_maps


__all__ = [
    "increment_review_feedback_bucket",
    "load_review_feedback_maps",
]
