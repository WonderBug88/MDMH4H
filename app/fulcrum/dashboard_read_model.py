"""Dashboard read-model helpers for Fulcrum admin views."""

from __future__ import annotations

from typing import Any, Callable

from psycopg2.extras import RealDictCursor

from app.fulcrum.platform import get_pg_conn


def summarize_changed_route_rows(
    rows: list[dict[str, Any]],
    *,
    row_current_page_matches_winner_fn: Callable[[dict[str, Any]], tuple[bool, bool]],
) -> dict[str, Any]:
    changed_count = 0
    same_type_changes = 0
    wrong_type_changes = 0
    for row in rows:
        is_exact_match, same_type = row_current_page_matches_winner_fn(row)
        if is_exact_match:
            continue
        changed_count += 1
        if same_type:
            same_type_changes += 1
        else:
            wrong_type_changes += 1
    return {
        "changed_count": changed_count,
        "same_type_changes": same_type_changes,
        "wrong_type_changes": wrong_type_changes,
    }


def matches_changed_route_search(row: dict[str, Any], search_text: str) -> bool:
    normalized_search = (search_text or "").strip().lower()
    if not normalized_search:
        return True
    winner = row.get("suggested_target") or {}
    haystack = " ".join(
        [
            str(row.get("gate_record_id") or ""),
            str(row.get("representative_query") or ""),
            str(row.get("normalized_query_key") or ""),
            str(row.get("source_name") or ""),
            str(row.get("source_url") or ""),
            str(winner.get("name") or ""),
            str(winner.get("url") or ""),
            str((row.get("agent_review") or {}).get("verdict") or ""),
        ]
    ).lower()
    return normalized_search in haystack


def sorted_changed_route_rows(rows: list[dict[str, Any]], sort_key: str) -> list[dict[str, Any]]:
    normalized_sort = (sort_key or "score_desc").strip().lower()
    ordered = list(rows)
    if normalized_sort == "impressions_desc":
        ordered.sort(
            key=lambda row: (
                -float(row.get("impressions_90d") or 0.0),
                -float(((row.get("suggested_target") or {}).get("score") or 0.0)),
                row.get("representative_query") or "",
            )
        )
    elif normalized_sort == "position_desc":
        ordered.sort(
            key=lambda row: (
                -float(row.get("avg_position_90d") or 0.0),
                -float(row.get("impressions_90d") or 0.0),
                row.get("representative_query") or "",
            )
        )
    elif normalized_sort == "incorrect_first":
        verdict_rank = {"incorrect": 0, "unclear": 1, "correct": 2}
        ordered.sort(
            key=lambda row: (
                verdict_rank.get(((row.get("agent_review") or {}).get("verdict") or "").strip().lower(), 3),
                -float(row.get("impressions_90d") or 0.0),
                -float(((row.get("suggested_target") or {}).get("score") or 0.0)),
                row.get("representative_query") or "",
            )
        )
    elif normalized_sort == "query_asc":
        ordered.sort(key=lambda row: (row.get("representative_query") or "", -float(row.get("impressions_90d") or 0.0)))
    else:
        ordered.sort(
            key=lambda row: (
                -float(((row.get("suggested_target") or {}).get("score") or 0.0)),
                -float(row.get("impressions_90d") or 0.0),
                -float(row.get("opportunity_score") or 0.0),
                row.get("representative_query") or "",
            )
        )
    return ordered


def gate_review_map_for_ids(
    store_hash: str,
    gate_record_ids: set[int],
    *,
    run_id: int | None = None,
    run_ids: set[int] | None = None,
    query_gate_record_map_for_ids_fn: Callable[..., dict[int, dict[str, Any]]],
    list_query_gate_agent_reviews_fn: Callable[..., list[dict[str, Any]]],
    postprocess_gate_agent_reviews_fn: Callable[[list[dict[str, Any]], dict[int, dict[str, Any]]], list[dict[str, Any]]],
) -> dict[int, dict[str, Any]]:
    if not gate_record_ids:
        return {}
    gate_row_map = query_gate_record_map_for_ids_fn(
        store_hash,
        gate_record_ids,
        run_ids=run_ids or ({int(run_id)} if int(run_id or 0) > 0 else None),
    )
    normalized_run_ids = {int(value) for value in (run_ids or set()) if int(value or 0) > 0}
    if normalized_run_ids:
        sql = """
            SELECT
                review_id,
                gate_record_id,
                run_id,
                store_hash,
                normalized_query_key,
                representative_query,
                source_url,
                source_entity_type,
                source_entity_id,
                target_entity_type,
                target_entity_id,
                verdict,
                issue_type,
                recommended_action,
                confidence,
                cluster_key,
                cluster_label,
                rationale,
                model_name,
                metadata,
                created_by,
                created_at,
                updated_at
            FROM app_runtime.query_gate_agent_reviews
            WHERE store_hash = %s
              AND gate_record_id = ANY(%s::int[])
              AND run_id = ANY(%s::int[])
            ORDER BY updated_at DESC, review_id DESC;
        """
        with get_pg_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    sql,
                    (
                        store_hash,
                        sorted(gate_record_ids),
                        sorted(normalized_run_ids),
                    ),
                )
                rows = [dict(row) for row in cur.fetchall()]
        processed_rows = postprocess_gate_agent_reviews_fn(rows, gate_row_map)
        review_map: dict[int, dict[str, Any]] = {}
        for row in processed_rows:
            gate_record_id = int(row.get("gate_record_id") or 0)
            if gate_record_id > 0 and gate_record_id not in review_map:
                review_map[gate_record_id] = row
        return review_map
    rows = list_query_gate_agent_reviews_fn(
        store_hash,
        run_id=run_id,
        verdict=None,
        limit=max(len(gate_record_ids) * 2, 500),
    )
    processed_rows = postprocess_gate_agent_reviews_fn(rows, gate_row_map)
    review_map: dict[int, dict[str, Any]] = {}
    for row in processed_rows:
        gate_record_id = int(row.get("gate_record_id") or 0)
        if gate_record_id in gate_record_ids and gate_record_id not in review_map:
            review_map[gate_record_id] = row
    return review_map


def list_changed_route_results(
    store_hash: str,
    run_id: int | None = None,
    limit: int = 25,
    *,
    latest_gate_run_id_fn: Callable[[str], int | None],
    list_query_gate_records_fn: Callable[..., list[dict[str, Any]]],
    attach_cached_query_gate_suggestions_fn: Callable[[list[dict[str, Any]]], list[dict[str, Any]]],
    row_current_page_matches_winner_fn: Callable[[dict[str, Any]], tuple[bool, bool]],
    extract_storefront_channel_id_fn: Callable[..., int | None],
    build_storefront_url_fn: Callable[..., str | None],
) -> list[dict[str, Any]]:
    resolved_run_id = run_id or latest_gate_run_id_fn(store_hash)
    if not resolved_run_id:
        return []

    gate_rows = list_query_gate_records_fn(store_hash, disposition="pass", limit=2000, run_id=resolved_run_id)
    gate_rows = attach_cached_query_gate_suggestions_fn(gate_rows)
    changed_rows: list[dict[str, Any]] = []
    for row in gate_rows:
        winner = dict(row.get("suggested_target") or {})
        if not winner:
            continue
        is_exact_match, same_type = row_current_page_matches_winner_fn(row)
        if is_exact_match:
            continue
        annotated = dict(row)
        annotated["route_change_type"] = "same_page_type_new_page" if same_type else "new_page_type"
        annotated["route_change_label"] = "Same page type, new page" if same_type else "New page type"
        source_channel_id = extract_storefront_channel_id_fn(annotated)
        target_channel_id = extract_storefront_channel_id_fn(winner, annotated)
        annotated["source_live_url"] = build_storefront_url_fn(store_hash, annotated.get("source_url"), channel_id=source_channel_id)
        winner["live_url"] = build_storefront_url_fn(store_hash, winner.get("url"), channel_id=target_channel_id)
        annotated["suggested_target"] = winner
        changed_rows.append(annotated)

    changed_rows.sort(
        key=lambda row: (
            -float(((row.get("suggested_target") or {}).get("score") or 0.0)),
            -float(row.get("opportunity_score") or 0.0),
            row.get("representative_query") or "",
        )
    )
    return changed_rows[:limit]


def get_cached_changed_route_results(
    store_hash: str,
    *,
    run_id: int | None = None,
    limit: int = 25,
    force_refresh: bool = False,
    latest_gate_run_id_fn: Callable[[str], int | None],
    load_admin_metric_cache_fn: Callable[[str, str], dict[str, Any] | None],
    store_admin_metric_cache_fn: Callable[[str, str, dict[str, Any]], dict[str, Any]],
    list_changed_route_results_fn: Callable[..., list[dict[str, Any]]] | None = None,
) -> list[dict[str, Any]]:
    resolved_run_id = run_id or latest_gate_run_id_fn(store_hash)
    if not resolved_run_id:
        return []
    metric_key = f"changed_route_results_run_{int(resolved_run_id)}_limit_{int(limit)}"
    if not force_refresh:
        cached = load_admin_metric_cache_fn(store_hash, metric_key)
        if cached is not None:
            return list(cached.get("rows") or [])
    list_changed_route_results_fn = list_changed_route_results_fn or list_changed_route_results
    rows = list_changed_route_results_fn(store_hash, run_id=resolved_run_id, limit=limit)
    payload = {
        "run_id": int(resolved_run_id),
        "limit": int(limit),
        "rows": rows,
    }
    cached = store_admin_metric_cache_fn(store_hash, metric_key, payload)
    return list(cached.get("rows") or [])


__all__ = [
    "gate_review_map_for_ids",
    "get_cached_changed_route_results",
    "list_changed_route_results",
    "matches_changed_route_search",
    "sorted_changed_route_rows",
    "summarize_changed_route_rows",
]
