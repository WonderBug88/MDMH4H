"""Query-target override and suggestion-cache helpers for Fulcrum."""

from __future__ import annotations

import json
from typing import Any, Callable

from psycopg2.extras import RealDictCursor

from app.fulcrum.platform import get_pg_conn


def query_target_override_key(
    normalized_query_key: str | None,
    source_url: str | None,
    *,
    normalize_query_family_key_fn: Callable[[str | None], str],
    normalize_storefront_path_fn: Callable[[Any], str],
) -> tuple[str, str]:
    return (
        normalize_query_family_key_fn(normalized_query_key or ""),
        normalize_storefront_path_fn(source_url) or "",
    )


def load_query_target_overrides(
    store_hash: str,
    *,
    query_target_override_key_fn: Callable[[str | None, str | None], tuple[str, str]],
) -> dict[tuple[str, str], dict[str, Any]]:
    sql = """
        SELECT
            override_id,
            store_hash,
            normalized_query_key,
            source_url,
            source_entity_type,
            source_entity_id,
            target_entity_type,
            target_entity_id,
            metadata,
            created_by,
            created_at,
            updated_at
        FROM app_runtime.query_target_overrides
        WHERE store_hash = %s;
    """
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (store_hash,))
            rows = [dict(row) for row in cur.fetchall()]
    return {
        query_target_override_key_fn(row.get("normalized_query_key"), row.get("source_url")): row
        for row in rows
    }


def set_query_target_override(
    store_hash: str,
    normalized_query_key: str,
    source_url: str,
    source_entity_type: str,
    source_entity_id: int | None,
    target_entity_type: str,
    target_entity_id: int,
    *,
    query_target_override_key_fn: Callable[[str | None, str | None], tuple[str, str]],
    created_by: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_key, normalized_source_url = query_target_override_key_fn(normalized_query_key, source_url)
    sql = """
        INSERT INTO app_runtime.query_target_overrides (
            store_hash,
            normalized_query_key,
            source_url,
            source_entity_type,
            source_entity_id,
            target_entity_type,
            target_entity_id,
            metadata,
            created_by,
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, NOW())
        ON CONFLICT (store_hash, normalized_query_key, source_url)
        DO UPDATE SET
            source_entity_type = EXCLUDED.source_entity_type,
            source_entity_id = EXCLUDED.source_entity_id,
            target_entity_type = EXCLUDED.target_entity_type,
            target_entity_id = EXCLUDED.target_entity_id,
            metadata = EXCLUDED.metadata,
            created_by = EXCLUDED.created_by,
            updated_at = NOW()
        RETURNING
            override_id,
            store_hash,
            normalized_query_key,
            source_url,
            source_entity_type,
            source_entity_id,
            target_entity_type,
            target_entity_id,
            metadata,
            created_by,
            created_at,
            updated_at;
    """
    params = (
        store_hash,
        normalized_key,
        normalized_source_url,
        (source_entity_type or "product").strip().lower() or "product",
        int(source_entity_id) if source_entity_id is not None else None,
        (target_entity_type or "product").strip().lower() or "product",
        int(target_entity_id),
        json.dumps(metadata or {}),
        created_by,
    )
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            row = dict(cur.fetchone())
        conn.commit()
    return row


def serialize_query_gate_target_snapshot(target: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(target, dict) or not target:
        return None
    snapshot: dict[str, Any] = {}
    for key in (
        "entity_type",
        "entity_id",
        "name",
        "url",
        "score",
        "anchor_label",
        "reason_summary",
        "type_fit_reason",
        "is_current_page",
        "manual_override",
    ):
        if key in target:
            snapshot[key] = target.get(key)
    return snapshot or None


def attach_cached_query_gate_suggestions(gate_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    annotated_rows: list[dict[str, Any]] = []
    for row in gate_rows:
        annotated = dict(row)
        metadata = dict(annotated.get("metadata") or {})
        annotated["suggested_target"] = dict(metadata.get("suggested_target_snapshot") or {}) or None
        annotated["second_option"] = dict(metadata.get("second_option_snapshot") or {}) or None
        annotated["target_override"] = dict(metadata.get("target_override_snapshot") or {}) or None
        annotated_rows.append(annotated)
    return annotated_rows


def annotate_query_gate_rows_with_suggestions(
    store_hash: str,
    gate_rows: list[dict[str, Any]],
    cluster: str | None = None,
    *,
    source_profiles: dict[str, dict[str, Any]] | None = None,
    target_entities: list[dict[str, Any]] | None = None,
    overrides: dict[tuple[str, str], dict[str, Any]] | None = None,
    review_feedback_maps: dict[str, dict[Any, dict[str, int]]] | None = None,
    signal_library: dict[str, Any] | None = None,
    cache_snapshots: bool = False,
    build_unified_entity_index_fn: Callable[..., dict[str, Any]],
    load_query_target_overrides_fn: Callable[[str], dict[tuple[str, str], dict[str, Any]]],
    load_review_feedback_maps_fn: Callable[[str], dict[str, dict[Any, dict[str, int]]]],
    build_store_signal_library_fn: Callable[[str], dict[str, Any]],
    refresh_query_gate_row_live_state_fn: Callable[..., dict[str, Any]],
    rank_target_options_for_gate_row_fn: Callable[..., list[dict[str, Any]]],
    query_target_override_key_fn: Callable[[str | None, str | None], tuple[str, str]],
    serialize_query_gate_target_snapshot_fn: Callable[[dict[str, Any] | None], dict[str, Any] | None],
) -> list[dict[str, Any]]:
    if not gate_rows:
        return gate_rows

    if source_profiles is None or target_entities is None:
        entity_index = build_unified_entity_index_fn(store_hash, cluster=cluster)
        source_profiles = entity_index["sources"]
        target_entities = entity_index["targets"]
    overrides = overrides if overrides is not None else load_query_target_overrides_fn(store_hash)
    review_feedback_maps = review_feedback_maps if review_feedback_maps is not None else load_review_feedback_maps_fn(store_hash)
    signal_library = signal_library if signal_library is not None else build_store_signal_library_fn(store_hash)

    annotated_rows: list[dict[str, Any]] = []
    for row in gate_rows:
        annotated = refresh_query_gate_row_live_state_fn(
            store_hash=store_hash,
            gate_row=row,
            source_profiles=source_profiles,
            target_entities=target_entities,
            signal_library=signal_library,
        )
        ranked_targets = rank_target_options_for_gate_row_fn(
            gate_row=annotated,
            source_profiles=source_profiles,
            target_entities=target_entities,
            overrides=overrides,
            review_feedback_maps=review_feedback_maps,
            limit=2,
        )
        annotated["suggested_target"] = ranked_targets[0] if ranked_targets else None
        annotated["second_option"] = ranked_targets[1] if len(ranked_targets) > 1 else None
        annotated["target_override"] = overrides.get(
            query_target_override_key_fn(annotated.get("normalized_query_key"), annotated.get("source_url"))
        )
        if cache_snapshots:
            metadata = dict(annotated.get("metadata") or {})
            metadata["suggested_target_snapshot"] = serialize_query_gate_target_snapshot_fn(annotated.get("suggested_target"))
            metadata["second_option_snapshot"] = serialize_query_gate_target_snapshot_fn(annotated.get("second_option"))
            metadata["target_override_snapshot"] = serialize_query_gate_target_snapshot_fn(annotated.get("target_override"))
            annotated["metadata"] = metadata
        annotated_rows.append(annotated)
    return annotated_rows


def refresh_query_gate_suggestion_cache(
    store_hash: str,
    *,
    run_id: int | None = None,
    gate_rows: list[dict[str, Any]] | None = None,
    latest_gate_run_id_fn: Callable[[str], int | None],
    list_runs_fn: Callable[..., list[dict[str, Any]]],
    list_query_gate_records_fn: Callable[..., list[dict[str, Any]]],
    build_unified_entity_index_fn: Callable[..., dict[str, Any]],
    build_store_signal_library_fn: Callable[[str], dict[str, Any]],
    load_query_target_overrides_fn: Callable[[str], dict[tuple[str, str], dict[str, Any]]],
    load_review_feedback_maps_fn: Callable[[str], dict[str, dict[Any, dict[str, int]]]],
    annotate_query_gate_rows_with_suggestions_fn: Callable[..., list[dict[str, Any]]],
    store_query_gate_records_fn: Callable[[int, str, list[dict[str, Any]]], None],
) -> dict[str, Any]:
    resolved_run_id = run_id or latest_gate_run_id_fn(store_hash)
    if not resolved_run_id:
        return {"status": "skipped", "run_id": None, "updated_count": 0}

    runs = list_runs_fn(store_hash, limit=20)
    latest_gate_run = next((run for run in runs if int(run.get("run_id") or 0) == int(resolved_run_id or 0)), None)
    latest_gate_filters = (latest_gate_run or {}).get("filters") or {}
    latest_gate_cluster = latest_gate_filters.get("cluster") if isinstance(latest_gate_filters, dict) else None

    rows_to_update = gate_rows or list_query_gate_records_fn(store_hash, disposition=None, limit=2000, run_id=resolved_run_id)
    if not rows_to_update:
        return {"status": "skipped", "run_id": resolved_run_id, "updated_count": 0}

    entity_index = build_unified_entity_index_fn(store_hash, cluster=latest_gate_cluster)
    signal_library = build_store_signal_library_fn(store_hash)
    overrides = load_query_target_overrides_fn(store_hash)
    review_feedback_maps = load_review_feedback_maps_fn(store_hash)
    annotated_rows = annotate_query_gate_rows_with_suggestions_fn(
        store_hash,
        rows_to_update,
        cluster=latest_gate_cluster,
        source_profiles=entity_index["sources"],
        target_entities=entity_index["targets"],
        overrides=overrides,
        review_feedback_maps=review_feedback_maps,
        signal_library=signal_library,
        cache_snapshots=True,
    )
    store_query_gate_records_fn(resolved_run_id, store_hash, annotated_rows)
    return {"status": "ok", "run_id": resolved_run_id, "updated_count": len(annotated_rows)}


__all__ = [
    "annotate_query_gate_rows_with_suggestions",
    "attach_cached_query_gate_suggestions",
    "load_query_target_overrides",
    "query_target_override_key",
    "refresh_query_gate_suggestion_cache",
    "serialize_query_gate_target_snapshot",
    "set_query_target_override",
]
