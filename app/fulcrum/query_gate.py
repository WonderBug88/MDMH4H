"""Query gate record helpers for Fulcrum."""

from __future__ import annotations

import json
from typing import Any, Callable

from psycopg2.extras import RealDictCursor, execute_batch

from app.fulcrum.platform import get_pg_conn


def build_query_gate_records(
    store_hash: str,
    source_profiles: dict[str, dict[str, Any]],
    target_entities: list[dict[str, Any]],
    min_hit_count: int = 3,
    limit_total: int = 300,
    *,
    build_store_signal_library: Callable[[str], dict[str, Any]],
    fetch_gsc_query_page_evidence: Callable[..., list[dict[str, Any]]],
    normalize_storefront_path: Callable[[str | None], str],
    normalize_query_family_key: Callable[[str | None], str],
    build_query_gate_record: Callable[..., dict[str, Any] | None],
) -> list[dict[str, Any]]:
    signal_library = build_store_signal_library(store_hash)
    evidence_rows = fetch_gsc_query_page_evidence(
        list(source_profiles.keys()),
        min_hit_count=min_hit_count,
        limit_total=limit_total,
    )
    grouped: dict[str, dict[str, Any]] = {}
    for evidence in evidence_rows:
        source_url = normalize_storefront_path(evidence.get("source_url"))
        source_profile = source_profiles.get(source_url)
        if not source_profile:
            continue
        if (source_profile.get("entity_type") or "product") not in {"product", "category"}:
            continue

        family_key = normalize_query_family_key(evidence.get("query"))
        if not family_key:
            continue
        group = grouped.setdefault(
            family_key,
            {
                "family_key": family_key,
                "variants": [],
            },
        )
        group["variants"].append({**evidence, "source_url": source_url})

    gate_rows: list[dict[str, Any]] = []
    for family_key, group in grouped.items():
        variants = group["variants"]
        representative_variant = max(
            variants,
            key=lambda row: (
                int(row.get("clicks_90d") or 0),
                int(row.get("impressions_90d") or 0),
                -float(row.get("avg_position_90d") or 999.0),
            ),
        )
        gate_row = build_query_gate_record(
            store_hash=store_hash,
            family_key=family_key,
            representative_query=representative_variant.get("query") or family_key,
            evidence_rows=variants,
            source_profiles=source_profiles,
            target_entities=target_entities,
            signal_library=signal_library,
        )
        if gate_row:
            gate_rows.append(gate_row)

    gate_rows.sort(
        key=lambda row: (
            {"pass": 0, "hold": 1, "reject": 2}.get(row.get("disposition") or "hold", 3),
            -float(row.get("opportunity_score") or 0.0),
            -float(row.get("demand_score") or 0.0),
            row.get("representative_query") or "",
        )
    )
    return gate_rows[:limit_total]


def store_query_gate_records(run_id: int, store_hash: str, gate_rows: list[dict[str, Any]]) -> None:
    if not gate_rows:
        return

    sql = """
        INSERT INTO app_runtime.query_gate_records (
            run_id,
            store_hash,
            normalized_query_key,
            representative_query,
            source_url,
            source_name,
            source_entity_type,
            source_entity_id,
            current_page_type,
            query_intent_scope,
            preferred_entity_type,
            clicks_28d,
            impressions_28d,
            ctr_28d,
            avg_position_28d,
            clicks_90d,
            impressions_90d,
            ctr_90d,
            avg_position_90d,
            demand_score,
            opportunity_score,
            intent_clarity_score,
            noise_penalty,
            freshness_context,
            disposition,
            reason_summary,
            metadata
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s::jsonb
        )
        ON CONFLICT (run_id, normalized_query_key, source_url) DO UPDATE SET
            source_name = EXCLUDED.source_name,
            source_entity_type = EXCLUDED.source_entity_type,
            source_entity_id = EXCLUDED.source_entity_id,
            current_page_type = EXCLUDED.current_page_type,
            query_intent_scope = EXCLUDED.query_intent_scope,
            preferred_entity_type = EXCLUDED.preferred_entity_type,
            clicks_28d = EXCLUDED.clicks_28d,
            impressions_28d = EXCLUDED.impressions_28d,
            ctr_28d = EXCLUDED.ctr_28d,
            avg_position_28d = EXCLUDED.avg_position_28d,
            clicks_90d = EXCLUDED.clicks_90d,
            impressions_90d = EXCLUDED.impressions_90d,
            ctr_90d = EXCLUDED.ctr_90d,
            avg_position_90d = EXCLUDED.avg_position_90d,
            demand_score = EXCLUDED.demand_score,
            opportunity_score = EXCLUDED.opportunity_score,
            intent_clarity_score = EXCLUDED.intent_clarity_score,
            noise_penalty = EXCLUDED.noise_penalty,
            freshness_context = EXCLUDED.freshness_context,
            disposition = EXCLUDED.disposition,
            reason_summary = EXCLUDED.reason_summary,
            metadata = EXCLUDED.metadata;
    """
    records = [
        (
            run_id,
            store_hash,
            row["normalized_query_key"],
            row["representative_query"],
            row["source_url"],
            row.get("source_name"),
            row.get("source_entity_type", "product"),
            row.get("source_entity_id"),
            row.get("current_page_type"),
            row.get("query_intent_scope"),
            row.get("preferred_entity_type"),
            row.get("clicks_28d", 0),
            row.get("impressions_28d", 0),
            row.get("ctr_28d", 0.0),
            row.get("avg_position_28d", 0.0),
            row.get("clicks_90d", 0),
            row.get("impressions_90d", 0),
            row.get("ctr_90d", 0.0),
            row.get("avg_position_90d", 0.0),
            row.get("demand_score", 0.0),
            row.get("opportunity_score", 0.0),
            row.get("intent_clarity_score", 0.0),
            row.get("noise_penalty", 0.0),
            json.dumps(row.get("freshness_context") or {}),
            row.get("disposition", "hold"),
            row.get("reason_summary"),
            json.dumps(row.get("metadata") or {}),
        )
        for row in gate_rows
    ]
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            execute_batch(cur, sql, records, page_size=100)
        conn.commit()


def list_runs(store_hash: str, limit: int = 10) -> list[dict[str, Any]]:
    sql = """
        SELECT run_id, store_hash, initiated_by, run_source, status, filters, notes, started_at, completed_at
        FROM app_runtime.link_runs
        WHERE store_hash = %s
        ORDER BY started_at DESC
        LIMIT %s;
    """
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (store_hash, limit))
            return [dict(row) for row in cur.fetchall()]


def latest_gate_run_id(store_hash: str) -> int | None:
    sql = """
        SELECT MAX(g.run_id)
        FROM app_runtime.query_gate_records g
        JOIN app_runtime.link_runs r
          ON r.run_id = g.run_id
        WHERE g.store_hash = %s
          AND r.status = 'completed';
    """
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (store_hash,))
            row = cur.fetchone()
    return int((row or [0])[0] or 0) or None


def summarize_query_gate_dispositions(
    store_hash: str,
    run_id: int | None = None,
    *,
    latest_gate_run_id_fn: Callable[[str], int | None] | None = None,
) -> dict[str, Any]:
    latest_gate_run_id_fn = latest_gate_run_id_fn or latest_gate_run_id
    run_id = run_id or latest_gate_run_id_fn(store_hash)
    if not run_id:
        return {"run_id": None, "pass": 0, "hold": 0, "reject": 0}

    sql = """
        SELECT disposition, COUNT(*)
        FROM app_runtime.query_gate_records
        WHERE store_hash = %s
          AND run_id = %s
        GROUP BY disposition;
    """
    counts = {"pass": 0, "hold": 0, "reject": 0}
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (store_hash, run_id))
            for disposition, count in cur.fetchall():
                counts[str(disposition)] = int(count or 0)
    return {"run_id": run_id, **counts}


def list_query_gate_records(
    store_hash: str,
    disposition: str | None = None,
    limit: int = 100,
    run_id: int | None = None,
    *,
    latest_gate_run_id_fn: Callable[[str], int | None] | None = None,
) -> list[dict[str, Any]]:
    latest_gate_run_id_fn = latest_gate_run_id_fn or latest_gate_run_id
    run_id = run_id or latest_gate_run_id_fn(store_hash)
    if not run_id:
        return []

    sql = """
        SELECT
            gate_record_id,
            run_id,
            normalized_query_key,
            representative_query,
            source_url,
            source_name,
            source_entity_type,
            source_entity_id,
            current_page_type,
            query_intent_scope,
            preferred_entity_type,
            clicks_28d,
            impressions_28d,
            ctr_28d,
            avg_position_28d,
            clicks_90d,
            impressions_90d,
            ctr_90d,
            avg_position_90d,
            demand_score,
            opportunity_score,
            intent_clarity_score,
            noise_penalty,
            freshness_context,
            disposition,
            reason_summary,
            metadata,
            created_at
        FROM app_runtime.query_gate_records
        WHERE store_hash = %s
          AND run_id = %s
          AND (%s IS NULL OR disposition = %s)
        ORDER BY
            CASE disposition
                WHEN 'pass' THEN 0
                WHEN 'hold' THEN 1
                ELSE 2
            END,
            opportunity_score DESC,
            demand_score DESC,
            created_at DESC
        LIMIT %s;
    """
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (store_hash, run_id, disposition, disposition, limit))
            return [dict(row) for row in cur.fetchall()]


def get_query_gate_record_by_id(store_hash: str, gate_record_id: int) -> dict[str, Any] | None:
    sql = """
        SELECT
            gate_record_id,
            run_id,
            normalized_query_key,
            representative_query,
            source_url,
            source_name,
            source_entity_type,
            source_entity_id,
            current_page_type,
            query_intent_scope,
            preferred_entity_type,
            clicks_28d,
            impressions_28d,
            ctr_28d,
            avg_position_28d,
            clicks_90d,
            impressions_90d,
            ctr_90d,
            avg_position_90d,
            demand_score,
            opportunity_score,
            intent_clarity_score,
            noise_penalty,
            freshness_context,
            disposition,
            reason_summary,
            metadata,
            created_at
        FROM app_runtime.query_gate_records
        WHERE store_hash = %s
          AND gate_record_id = %s
        LIMIT 1;
    """
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (store_hash, gate_record_id))
            row = cur.fetchone()
            return dict(row) if row else None


def query_gate_record_map_for_ids(
    store_hash: str,
    gate_record_ids: set[int],
    *,
    run_ids: set[int] | None = None,
    fresh_suggestions: bool = False,
    build_unified_entity_index: Callable[[str], dict[str, Any]] | None = None,
    load_query_target_overrides: Callable[[str], dict[Any, Any]] | None = None,
    load_review_feedback_maps: Callable[[str], dict[str, Any]] | None = None,
    build_store_signal_library: Callable[[str], dict[str, Any]] | None = None,
    annotate_query_gate_rows_with_suggestions: Callable[..., list[dict[str, Any]]] | None = None,
    attach_cached_query_gate_suggestions: Callable[[list[dict[str, Any]]], list[dict[str, Any]]] | None = None,
) -> dict[int, dict[str, Any]]:
    normalized_gate_ids = sorted({int(value) for value in gate_record_ids if int(value or 0) > 0})
    if not normalized_gate_ids:
        return {}
    normalized_run_ids = sorted({int(value) for value in (run_ids or set()) if int(value or 0) > 0})
    sql = """
        SELECT
            gate_record_id,
            run_id,
            normalized_query_key,
            representative_query,
            source_url,
            source_name,
            source_entity_type,
            source_entity_id,
            current_page_type,
            query_intent_scope,
            preferred_entity_type,
            clicks_28d,
            impressions_28d,
            ctr_28d,
            avg_position_28d,
            clicks_90d,
            impressions_90d,
            ctr_90d,
            avg_position_90d,
            demand_score,
            opportunity_score,
            intent_clarity_score,
            noise_penalty,
            freshness_context,
            disposition,
            reason_summary,
            metadata,
            created_at
        FROM app_runtime.query_gate_records
        WHERE store_hash = %s
          AND gate_record_id = ANY(%s::int[])
          AND (%s::int[] IS NULL OR run_id = ANY(%s::int[]));
    """
    run_id_array = normalized_run_ids or None
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (store_hash, normalized_gate_ids, run_id_array, run_id_array))
            rows = [dict(row) for row in cur.fetchall()]
    if fresh_suggestions and rows:
        if not all(
            (
                build_unified_entity_index,
                load_query_target_overrides,
                load_review_feedback_maps,
                build_store_signal_library,
                annotate_query_gate_rows_with_suggestions,
            )
        ):
            raise ValueError("Fresh gate suggestions require full annotation dependencies.")
        entity_index = build_unified_entity_index(store_hash)
        overrides = load_query_target_overrides(store_hash)
        review_feedback_maps = load_review_feedback_maps(store_hash)
        signal_library = build_store_signal_library(store_hash)
        rows = annotate_query_gate_rows_with_suggestions(
            store_hash,
            rows,
            source_profiles=entity_index.get("sources") or {},
            target_entities=entity_index.get("targets") or [],
            overrides=overrides,
            review_feedback_maps=review_feedback_maps,
            signal_library=signal_library,
        )
    elif attach_cached_query_gate_suggestions is not None:
        rows = attach_cached_query_gate_suggestions(rows)
    return {
        int(row.get("gate_record_id") or 0): row
        for row in rows
        if int(row.get("gate_record_id") or 0) > 0
    }


__all__ = [
    "build_query_gate_records",
    "get_query_gate_record_by_id",
    "latest_gate_run_id",
    "list_query_gate_records",
    "list_runs",
    "query_gate_record_map_for_ids",
    "store_query_gate_records",
    "summarize_query_gate_dispositions",
]
