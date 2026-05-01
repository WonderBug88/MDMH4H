"""Candidate read and review helpers for Fulcrum."""

from __future__ import annotations

from typing import Any, Callable

from psycopg2.extras import RealDictCursor, execute_batch

from app.fulcrum.platform import get_pg_conn


def latest_candidate_rows_for_store(
    store_hash: str,
    review_status: str | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    sql = """
        WITH latest_pairs AS (
            SELECT DISTINCT ON (source_entity_type, source_product_id, target_entity_type, target_product_id)
                candidate_id,
                run_id,
                source_entity_type,
                target_entity_type,
                source_entity_id,
                target_entity_id,
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
                review_status,
                metadata,
                created_at
            FROM app_runtime.link_candidates
            WHERE store_hash = %s
            ORDER BY
                source_entity_type,
                source_product_id,
                target_entity_type,
                target_product_id,
                candidate_id DESC
        )
        SELECT
            candidate_id,
            run_id,
            source_entity_type,
            target_entity_type,
            source_entity_id,
            target_entity_id,
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
            review_status,
            metadata,
            created_at
        FROM latest_pairs
        WHERE (%s IS NULL OR review_status = %s)
        ORDER BY score DESC, created_at DESC
        LIMIT COALESCE(%s, 1000000);
    """
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (store_hash, review_status, review_status, limit))
            return [dict(row) for row in cur.fetchall()]


def include_dashboard_candidate(row: dict[str, Any], review_status: str, category_enabled: bool) -> bool:
    source_entity_type = row.get("source_entity_type") or "product"
    metadata = row.get("metadata") or {}
    block_type = metadata.get("block_type")

    if review_status == "pending" and source_entity_type == "category" and not category_enabled:
        return False
    if review_status == "pending" and source_entity_type == "category" and block_type == "matching_products" and not category_enabled:
        return False
    return True


def count_pending_candidates(
    store_hash: str,
    *,
    category_publishing_enabled_for_store_fn: Callable[[str], bool],
) -> int:
    category_enabled = category_publishing_enabled_for_store_fn(store_hash)
    sql = """
        WITH latest_pairs AS (
            SELECT DISTINCT ON (source_entity_type, source_product_id, target_entity_type, target_product_id)
                review_status,
                source_entity_type
            FROM app_runtime.link_candidates
            WHERE store_hash = %s
            ORDER BY
                source_entity_type,
                source_product_id,
                target_entity_type,
                target_product_id,
                candidate_id DESC
        )
        SELECT COUNT(*)
        FROM latest_pairs
        WHERE review_status = 'pending'
          AND (%s OR source_entity_type <> 'category');
    """
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (store_hash, category_enabled))
            row = cur.fetchone()
    return int((row or [0])[0] or 0)


def count_candidates_by_statuses(
    store_hash: str,
    review_statuses: list[str],
    *,
    category_publishing_enabled_for_store_fn: Callable[[str], bool],
) -> int:
    normalized_statuses = [str(status or "").strip().lower() for status in review_statuses if str(status or "").strip()]
    if not normalized_statuses:
        return 0

    category_enabled = category_publishing_enabled_for_store_fn(store_hash)
    sql = """
        WITH latest_pairs AS (
            SELECT DISTINCT ON (source_entity_type, source_product_id, target_entity_type, target_product_id)
                review_status,
                source_entity_type
            FROM app_runtime.link_candidates
            WHERE store_hash = %s
            ORDER BY
                source_entity_type,
                source_product_id,
                target_entity_type,
                target_product_id,
                candidate_id DESC
        )
        SELECT COUNT(*)
        FROM latest_pairs
        WHERE review_status = ANY(%s::text[])
          AND (%s OR source_entity_type <> 'category');
    """
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (store_hash, normalized_statuses, category_enabled))
            row = cur.fetchone()
    return int((row or [0])[0] or 0)


def review_candidates(candidate_ids: list[int], review_status: str, reviewed_by: str | None, note: str | None = None) -> int:
    if not candidate_ids:
        return 0

    sql_update = """
        UPDATE app_runtime.link_candidates
        SET review_status = %s
        WHERE candidate_id = ANY(%s::bigint[]);
    """
    sql_insert = """
        INSERT INTO app_runtime.link_reviews (candidate_id, review_status, reviewed_by, review_note)
        VALUES (%s, %s, %s, %s);
    """

    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql_update, (review_status, candidate_ids))
            execute_batch(
                cur,
                sql_insert,
                [(candidate_id, review_status, reviewed_by, note) for candidate_id in candidate_ids],
                page_size=100,
            )
        conn.commit()

    return len(candidate_ids)


def get_approved_rows_for_source(
    store_hash: str,
    source_product_id: int,
    *,
    source_entity_type: str = "product",
    latest_candidate_rows_for_store_fn: Callable[..., list[dict[str, Any]]],
    rank_source_rows_fn: Callable[[list[dict[str, Any]], str], list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    rows = [
        row
        for row in latest_candidate_rows_for_store_fn(store_hash, review_status="approved", limit=None)
        if (row.get("source_entity_type") or "product") == source_entity_type
        and int(row.get("source_product_id") or 0) == int(source_product_id)
    ]
    return rank_source_rows_fn(rows, source_entity_type=source_entity_type)


def list_candidates(
    store_hash: str,
    review_status: str = "pending",
    limit: int = 200,
    *,
    latest_candidate_rows_for_store_fn: Callable[..., list[dict[str, Any]]],
    category_publishing_enabled_for_store_fn: Callable[[str], bool],
    include_dashboard_candidate_fn: Callable[[dict[str, Any], str, bool], bool],
    rank_source_rows_fn: Callable[[list[dict[str, Any]], str], list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    category_enabled = category_publishing_enabled_for_store_fn(store_hash)
    rows = [
        row
        for row in latest_candidate_rows_for_store_fn(store_hash, review_status=review_status, limit=None)
        if include_dashboard_candidate_fn(row, review_status, category_enabled)
    ]
    if review_status == "approved":
        grouped: dict[tuple[str, int], list[dict[str, Any]]] = {}
        for row in rows:
            key = (
                row.get("source_entity_type") or "product",
                int(row.get("source_product_id") or 0),
            )
            grouped.setdefault(key, []).append(row)

        flattened: list[dict[str, Any]] = []
        for (source_entity_type, _), source_rows in grouped.items():
            flattened.extend(rank_source_rows_fn(source_rows, source_entity_type=source_entity_type))
        flattened.sort(
            key=lambda row: (
                -float(row.get("score") or 0),
                row.get("created_at") or "",
            )
        )
        return flattened[:limit]

    rows.sort(
        key=lambda row: (
            -float(row.get("score") or 0),
            row.get("created_at") or "",
        )
    )
    return rows[:limit]


def list_approved_sources(
    store_hash: str,
    limit: int = 100,
    *,
    list_candidates_fn: Callable[..., list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    rows = list_candidates_fn(store_hash, "approved", limit=1000000)
    seen: set[tuple[str, int]] = set()
    sources: list[dict[str, Any]] = []
    for row in rows:
        key = (
            row.get("source_entity_type") or "product",
            int(row.get("source_product_id") or 0),
        )
        if key in seen:
            continue
        seen.add(key)
        sources.append(
            {
                "source_entity_type": row.get("source_entity_type") or "product",
                "source_product_id": row.get("source_product_id"),
                "source_name": row.get("source_name"),
                "source_url": row.get("source_url"),
            }
        )
        if len(sources) >= limit:
            break
    return sources


__all__ = [
    "count_candidates_by_statuses",
    "count_pending_candidates",
    "get_approved_rows_for_source",
    "include_dashboard_candidate",
    "latest_candidate_rows_for_store",
    "list_approved_sources",
    "list_candidates",
    "review_candidates",
]
