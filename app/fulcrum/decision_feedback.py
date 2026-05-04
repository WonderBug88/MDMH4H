"""Durable Clear/Review decision feedback for Route Authority."""

from __future__ import annotations

import json
from typing import Any

from psycopg2.extras import RealDictCursor, execute_batch

from app.fulcrum.platform import get_pg_conn, normalize_store_hash


FEEDBACK_ACTIONS = {"clear", "review"}


def _normalize_action(action: str | None) -> str:
    normalized = (action or "").strip().lower()
    return normalized if normalized in FEEDBACK_ACTIONS else ""


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


def _target_snapshot_from_inputs(
    gate_row: dict[str, Any],
    *,
    target_entity_type: str | None = None,
    target_entity_id: int | None = None,
    target_name: str | None = None,
    target_url: str | None = None,
    reason_summary: str | None = None,
) -> dict[str, Any]:
    metadata = gate_row.get("metadata") if isinstance(gate_row.get("metadata"), dict) else {}
    cached_target = metadata.get("suggested_target_snapshot") if isinstance(metadata, dict) else {}
    cached_target = cached_target if isinstance(cached_target, dict) else {}
    return {
        "entity_type": (target_entity_type or cached_target.get("entity_type") or "").strip().lower(),
        "entity_id": int(target_entity_id or cached_target.get("entity_id") or 0) or None,
        "name": target_name if target_name is not None else cached_target.get("name"),
        "url": target_url if target_url is not None else cached_target.get("url"),
        "reason_summary": reason_summary if reason_summary is not None else cached_target.get("reason_summary"),
        "score": cached_target.get("score"),
        "anchor_label": cached_target.get("anchor_label"),
    }


def decision_snapshot_from_gate_row(
    gate_row: dict[str, Any],
    *,
    target_entity_type: str | None = None,
    target_entity_id: int | None = None,
    target_name: str | None = None,
    target_url: str | None = None,
    reason_summary: str | None = None,
) -> dict[str, Any]:
    metadata = gate_row.get("metadata") if isinstance(gate_row.get("metadata"), dict) else {}
    return {
        "gate_record_id": int(gate_row.get("gate_record_id") or 0),
        "run_id": int(gate_row.get("run_id") or 0) or None,
        "representative_query": gate_row.get("representative_query") or "",
        "normalized_query_key": gate_row.get("normalized_query_key") or "",
        "source": {
            "entity_type": gate_row.get("source_entity_type") or gate_row.get("current_page_type") or "product",
            "entity_id": int(gate_row.get("source_entity_id") or 0) or None,
            "name": gate_row.get("source_name") or "",
            "url": gate_row.get("source_url") or "",
        },
        "gate": {
            "disposition": gate_row.get("disposition") or "",
            "query_intent_scope": gate_row.get("query_intent_scope") or "",
            "preferred_entity_type": gate_row.get("preferred_entity_type") or "",
            "demand_score": float(gate_row.get("demand_score") or 0.0),
            "opportunity_score": float(gate_row.get("opportunity_score") or 0.0),
            "intent_clarity_score": float(gate_row.get("intent_clarity_score") or 0.0),
            "noise_penalty": float(gate_row.get("noise_penalty") or 0.0),
            "reason_summary": gate_row.get("reason_summary") or "",
        },
        "target": _target_snapshot_from_inputs(
            gate_row,
            target_entity_type=target_entity_type,
            target_entity_id=target_entity_id,
            target_name=target_name,
            target_url=target_url,
            reason_summary=reason_summary,
        ),
        "semantics_analysis": metadata.get("semantics_analysis") or {},
        "resolved_signals": metadata.get("resolved_signals") or {},
    }


def record_query_gate_decision_feedback(
    store_hash: str,
    gate_record_id: int,
    action: str,
    *,
    submitted_by: str | None = None,
    request_id: int | None = None,
    feedback_status: str | None = None,
    diagnosis_category: str | None = None,
    recommended_action: str | None = None,
    admin_decision: str | None = None,
    target_entity_type: str | None = None,
    target_entity_id: int | None = None,
    target_name: str | None = None,
    target_url: str | None = None,
    reason_summary: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    normalized_action = _normalize_action(action)
    if not normalized_action or int(gate_record_id or 0) <= 0:
        return None

    normalized_hash = normalize_store_hash(store_hash)
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT *
                FROM app_runtime.query_gate_records
                WHERE store_hash = %s
                  AND gate_record_id = %s
                """,
                (normalized_hash, int(gate_record_id)),
            )
            gate_row = dict(cur.fetchone() or {})
            if not gate_row:
                return None
            snapshot = decision_snapshot_from_gate_row(
                gate_row,
                target_entity_type=target_entity_type,
                target_entity_id=target_entity_id,
                target_name=target_name,
                target_url=target_url,
                reason_summary=reason_summary,
            )
            cur.execute(
                """
                INSERT INTO app_runtime.query_gate_decision_feedback (
                    store_hash,
                    gate_record_id,
                    run_id,
                    action,
                    feedback_status,
                    request_id,
                    diagnosis_category,
                    recommended_action,
                    admin_decision,
                    decision_snapshot,
                    metadata,
                    submitted_by
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s
                )
                ON CONFLICT (store_hash, gate_record_id, action)
                DO UPDATE SET
                    run_id = EXCLUDED.run_id,
                    feedback_status = EXCLUDED.feedback_status,
                    request_id = COALESCE(EXCLUDED.request_id, app_runtime.query_gate_decision_feedback.request_id),
                    diagnosis_category = COALESCE(EXCLUDED.diagnosis_category, app_runtime.query_gate_decision_feedback.diagnosis_category),
                    recommended_action = COALESCE(EXCLUDED.recommended_action, app_runtime.query_gate_decision_feedback.recommended_action),
                    admin_decision = COALESCE(EXCLUDED.admin_decision, app_runtime.query_gate_decision_feedback.admin_decision),
                    decision_snapshot = EXCLUDED.decision_snapshot,
                    metadata = COALESCE(app_runtime.query_gate_decision_feedback.metadata, '{}'::jsonb) || EXCLUDED.metadata,
                    submitted_by = EXCLUDED.submitted_by,
                    updated_at = NOW()
                RETURNING feedback_id, store_hash, gate_record_id, run_id, action, feedback_status, request_id,
                          diagnosis_category, recommended_action, admin_decision, metadata, submitted_by, created_at, updated_at;
                """,
                (
                    normalized_hash,
                    int(gate_record_id),
                    int(gate_row.get("run_id") or 0) or None,
                    normalized_action,
                    (feedback_status or ("confirmed_correct" if normalized_action == "clear" else "diagnosis_pending")),
                    int(request_id or 0) or None,
                    (diagnosis_category or "").strip().lower() or None,
                    (recommended_action or "").strip().lower() or None,
                    (admin_decision or "").strip().lower() or None,
                    json.dumps(snapshot),
                    json.dumps(metadata or {}),
                    (submitted_by or "").strip() or None,
                ),
            )
            row = dict(cur.fetchone() or {})
        conn.commit()
    return row


def record_query_gate_decision_feedback_batch(
    store_hash: str,
    action: str,
    gate_record_ids: list[Any] | tuple[Any, ...] | set[Any] | None,
    *,
    submitted_by: str | None = None,
    feedback_status: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> int:
    normalized_action = _normalize_action(action)
    normalized_ids = _normalize_gate_record_ids(gate_record_ids)
    if not normalized_action or not normalized_ids:
        return 0
    normalized_hash = normalize_store_hash(store_hash)
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT *
                FROM app_runtime.query_gate_records
                WHERE store_hash = %s
                  AND gate_record_id = ANY(%s)
                """,
                (normalized_hash, normalized_ids),
            )
            rows = [dict(row) for row in cur.fetchall()]
        if not rows:
            return 0

        params = []
        resolved_status = feedback_status or ("confirmed_correct" if normalized_action == "clear" else "diagnosis_pending")
        for gate_row in rows:
            params.append(
                (
                    normalized_hash,
                    int(gate_row.get("gate_record_id") or 0),
                    int(gate_row.get("run_id") or 0) or None,
                    normalized_action,
                    resolved_status,
                    json.dumps(decision_snapshot_from_gate_row(gate_row)),
                    json.dumps(metadata or {}),
                    (submitted_by or "").strip() or None,
                )
            )
        with conn.cursor() as cur:
            execute_batch(
                cur,
                """
                INSERT INTO app_runtime.query_gate_decision_feedback (
                    store_hash,
                    gate_record_id,
                    run_id,
                    action,
                    feedback_status,
                    decision_snapshot,
                    metadata,
                    submitted_by
                ) VALUES (
                    %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s
                )
                ON CONFLICT (store_hash, gate_record_id, action)
                DO UPDATE SET
                    run_id = EXCLUDED.run_id,
                    feedback_status = EXCLUDED.feedback_status,
                    decision_snapshot = EXCLUDED.decision_snapshot,
                    metadata = COALESCE(app_runtime.query_gate_decision_feedback.metadata, '{}'::jsonb) || EXCLUDED.metadata,
                    submitted_by = EXCLUDED.submitted_by,
                    updated_at = NOW()
                """,
                params,
                page_size=100,
            )
        conn.commit()
    return len(params)


__all__ = [
    "decision_snapshot_from_gate_row",
    "record_query_gate_decision_feedback",
    "record_query_gate_decision_feedback_batch",
]
