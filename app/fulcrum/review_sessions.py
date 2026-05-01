"""Persist user-submitted review batches from the public Fulcrum dashboard."""

from __future__ import annotations

import json
from typing import Any

from psycopg2.extras import RealDictCursor

from app.fulcrum.platform import get_pg_conn, normalize_store_hash


def _normalize_gate_record_ids(values: list[Any] | tuple[Any, ...] | set[Any] | None) -> list[int]:
    normalized_ids: list[int] = []
    seen_ids: set[int] = set()
    for value in values or []:
        try:
            gate_record_id = int(value)
        except (TypeError, ValueError):
            continue
        if gate_record_id <= 0 or gate_record_id in seen_ids:
            continue
        seen_ids.add(gate_record_id)
        normalized_ids.append(gate_record_id)
    return normalized_ids


def create_query_gate_review_submission(
    store_hash: str,
    *,
    run_id: int | None,
    submitted_by: str | None,
    all_gate_record_ids: list[Any] | tuple[Any, ...] | set[Any] | None,
    cleared_gate_record_ids: list[Any] | tuple[Any, ...] | set[Any] | None,
    review_bucket_gate_record_ids: list[Any] | tuple[Any, ...] | set[Any] | None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_hash = normalize_store_hash(store_hash)
    all_ids = _normalize_gate_record_ids(all_gate_record_ids)
    review_bucket_ids = _normalize_gate_record_ids(review_bucket_gate_record_ids)
    review_bucket_id_set = set(review_bucket_ids)
    cleared_ids = [
        gate_record_id
        for gate_record_id in _normalize_gate_record_ids(cleared_gate_record_ids)
        if gate_record_id not in review_bucket_id_set
    ]
    cleared_id_set = set(cleared_ids)

    if all_ids:
        all_id_set = set(all_ids)
        review_bucket_ids = [gate_record_id for gate_record_id in review_bucket_ids if gate_record_id in all_id_set]
        review_bucket_id_set = set(review_bucket_ids)
        cleared_ids = [
            gate_record_id
            for gate_record_id in cleared_ids
            if gate_record_id in all_id_set and gate_record_id not in review_bucket_id_set
        ]
        cleared_id_set = set(cleared_ids)
        remaining_ids = [
            gate_record_id
            for gate_record_id in all_ids
            if gate_record_id not in cleared_id_set and gate_record_id not in review_bucket_id_set
        ]
    else:
        remaining_ids = []
        all_ids = review_bucket_ids + [gate_record_id for gate_record_id in cleared_ids if gate_record_id not in review_bucket_id_set]

    payload = dict(metadata or {})
    payload.update(
        {
            "all_gate_record_ids": list(all_ids),
            "cleared_gate_record_ids": list(cleared_ids),
            "review_bucket_gate_record_ids": list(review_bucket_ids),
            "remaining_gate_record_ids": list(remaining_ids),
        }
    )

    sql = """
        INSERT INTO app_runtime.query_gate_review_submissions (
            store_hash,
            run_id,
            submitted_by,
            total_result_count,
            cleared_count,
            review_bucket_count,
            remaining_count,
            metadata
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)
        RETURNING
            submission_id,
            store_hash,
            run_id,
            submitted_by,
            total_result_count,
            cleared_count,
            review_bucket_count,
            remaining_count,
            metadata,
            created_at
    """
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                sql,
                (
                    normalized_hash,
                    int(run_id) if run_id else None,
                    (submitted_by or "").strip() or None,
                    len(all_ids),
                    len(cleared_ids),
                    len(review_bucket_ids),
                    len(remaining_ids),
                    json.dumps(payload),
                ),
            )
            row = dict(cur.fetchone() or {})
        conn.commit()
    return row


__all__ = ["create_query_gate_review_submission"]
