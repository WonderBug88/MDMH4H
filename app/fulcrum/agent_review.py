"""Agent audit helpers for Fulcrum query-gate reviews."""

from __future__ import annotations

import json
import os
import re
from typing import Any, Callable

from psycopg2.extras import RealDictCursor, execute_batch

from app.fulcrum.constants import (
    GATE_REVIEW_AGENT_ACTIONS,
    GATE_REVIEW_AGENT_BATCH_SIZE,
    GATE_REVIEW_AGENT_ISSUE_TYPES,
    GATE_REVIEW_AGENT_VERDICTS,
)
from app.fulcrum.platform import get_pg_conn


def gate_review_cluster_values(
    verdict: str | None,
    issue_type: str | None,
    recommended_action: str | None,
) -> tuple[str, str]:
    normalized_verdict = ((verdict or "unclear").strip().lower()) or "unclear"
    normalized_issue = ((issue_type or "needs_human_review").strip().lower()) or "needs_human_review"
    normalized_action = ((recommended_action or "manual_review").strip().lower()) or "manual_review"
    cluster_key = f"{normalized_verdict}:{normalized_issue}:{normalized_action}"
    cluster_label = (
        f"{normalized_verdict.title()} / "
        f"{normalized_issue.replace('_', ' ').title()} / "
        f"{normalized_action.replace('_', ' ').title()}"
    )
    return cluster_key, cluster_label


def agent_review_signal_snapshot(signals: dict[str, Any]) -> dict[str, list[str]]:
    payload: dict[str, list[str]] = {}
    for key in (
        "brand_signals",
        "hard_attribute_signals",
        "soft_attribute_signals",
        "collection_signals",
        "topic_signals",
        "sku_signals",
    ):
        items = list(signals.get(key) or [])
        if items:
            payload[key] = [
                str(item.get("label") or item.get("normalized_label") or "").strip()
                for item in items[:4]
                if (item.get("label") or item.get("normalized_label"))
            ]
    return payload


def serialize_query_gate_row_for_agent_review(row: dict[str, Any]) -> dict[str, Any]:
    metadata = row.get("metadata") or {}
    signals = (metadata.get("resolved_signals") or {}) if isinstance(metadata, dict) else {}
    semantics = (metadata.get("semantics_analysis") or {}) if isinstance(metadata, dict) else {}
    payload = {
        "gate_record_id": int(row.get("gate_record_id") or 0),
        "representative_query": row.get("representative_query") or "",
        "normalized_query_key": row.get("normalized_query_key") or "",
        "source_page": {
            "entity_type": row.get("source_entity_type") or row.get("current_page_type") or "unknown",
            "name": row.get("source_name") or "",
            "url": row.get("source_url") or "",
        },
        "page_type_decision": {
            "current_page_type": row.get("current_page_type") or row.get("source_entity_type") or "unknown",
            "preferred_entity_type": row.get("preferred_entity_type") or "unknown",
            "query_intent_scope": row.get("query_intent_scope") or "mixed_or_unknown",
        },
        "gate_scores": {
            "opportunity": float(row.get("opportunity_score") or 0.0),
            "demand": float(row.get("demand_score") or 0.0),
            "intent": float(row.get("intent_clarity_score") or 0.0),
            "noise": float(row.get("noise_penalty") or 0.0),
        },
        "reason_summary": row.get("reason_summary") or "",
        "signals": agent_review_signal_snapshot(signals if isinstance(signals, dict) else {}),
        "semantics": {
            "query_shape": semantics.get("query_shape") or "",
            "head_term": semantics.get("head_term") or "",
            "head_family": semantics.get("head_family") or "",
            "eligible_page_types": list(semantics.get("eligible_page_types") or []),
            "blocked_page_types": list(semantics.get("blocked_page_types") or []),
            "negative_constraints": list(semantics.get("negative_constraints") or [])[:4],
            "token_roles": list(semantics.get("token_roles") or [])[:6],
            "brand_family_matching_product_count": int(semantics.get("brand_family_matching_product_count") or 0),
        },
        "query_variants": [
            {
                "query": variant.get("query") or "",
                "impressions_90d": int(variant.get("impressions_90d") or 0),
                "avg_position_90d": float(variant.get("avg_position_90d") or 0.0),
            }
            for variant in (metadata.get("query_variants") or [])[:5]
            if isinstance(variant, dict)
        ],
    }
    winner = row.get("suggested_target") or {}
    if winner:
        payload["winner"] = {
            "entity_type": winner.get("entity_type") or "unknown",
            "entity_id": int(winner.get("entity_id") or 0),
            "name": winner.get("name") or "",
            "url": winner.get("url") or "",
            "score": float(winner.get("score") or 0.0),
            "reason_summary": winner.get("reason_summary") or "",
            "type_fit_reason": winner.get("type_fit_reason") or "",
            "manual_override": bool(winner.get("manual_override")),
        }
    alternate = row.get("second_option") or {}
    if alternate:
        payload["alternate"] = {
            "entity_type": alternate.get("entity_type") or "unknown",
            "entity_id": int(alternate.get("entity_id") or 0),
            "name": alternate.get("name") or "",
            "url": alternate.get("url") or "",
            "score": float(alternate.get("score") or 0.0),
            "reason_summary": alternate.get("reason_summary") or "",
        }
    return payload


def parse_agent_json_list(raw_content: str | None) -> list[dict[str, Any]]:
    text = str(raw_content or "").strip()
    if not text:
        return []
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text, flags=re.IGNORECASE | re.DOTALL).strip()
    try:
        parsed = json.loads(text)
    except Exception:
        return []
    if isinstance(parsed, list):
        return [dict(item) for item in parsed if isinstance(item, dict)]
    if isinstance(parsed, dict):
        if isinstance(parsed.get("reviews"), list):
            return [dict(item) for item in parsed.get("reviews") if isinstance(item, dict)]
        return [parsed]
    return []


def normalize_gate_review_item(item: dict[str, Any]) -> dict[str, Any]:
    details = item.get("details") if isinstance(item.get("details"), dict) else {}
    gate_record_id = (
        item.get("gate_record_id")
        or details.get("gate_record_id")
        or item.get("row_id")
        or item.get("id")
        or 0
    )
    rationale = item.get("rationale") or item.get("reason") or details.get("reason") or ""
    review_notes = item.get("review_notes") or details.get("review_notes") or ""
    return {
        "gate_record_id": int(gate_record_id or 0),
        "verdict": ((item.get("verdict") or "").strip().lower()),
        "issue_type": ((item.get("issue_type") or "").strip().lower()),
        "recommended_action": ((item.get("recommended_action") or item.get("action") or "").strip().lower()),
        "confidence": float(item.get("confidence") or 0.0),
        "rationale": str(rationale).strip(),
        "review_notes": str(review_notes).strip(),
    }


def review_query_gate_rows_with_agent(
    store_hash: str,
    annotated_rows: list[dict[str, Any]],
    initiated_by: str | None = None,
) -> dict[str, Any]:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return {"status": "skipped", "reason": "OPENAI_API_KEY is not configured", "reviews": [], "model_name": None}
    eligible_rows = [row for row in annotated_rows if row.get("suggested_target")]
    if not eligible_rows:
        return {"status": "skipped", "reason": "No suggested targets were available to review", "reviews": [], "model_name": None}

    try:
        from openai import OpenAI

        client = OpenAI(api_key=api_key)
        model_name = os.getenv("FULCRUM_GATE_REVIEW_AGENT_MODEL", "gpt-4o-mini")
        reviews: list[dict[str, Any]] = []
        for batch_start in range(0, len(eligible_rows), GATE_REVIEW_AGENT_BATCH_SIZE):
            batch = eligible_rows[batch_start:batch_start + GATE_REVIEW_AGENT_BATCH_SIZE]
            prompt = {
                "task": "Audit Fulcrum routing decisions for obvious failures only.",
                "instructions": [
                    "Return JSON only.",
                    "Return an object with one top-level key: reviews.",
                    "reviews must be an array of objects.",
                    "For each row, judge the current Fulcrum winner as correct, incorrect, or unclear.",
                    "Use incorrect when the winner is obviously the wrong page type, too broad, too narrow, off-topic, or the original/alternate is clearly better.",
                    "Use unclear when the evidence is mixed or the right target is not obvious.",
                    "Do not over-penalize close sibling categories or close sibling products unless there is a clear error.",
                    "When the preferred page type is category and the winner is already a category, do not mark it incorrect just because an alternate PDP has a tighter lexical title match.",
                    "For broad_product_family and commercial_topic queries, category winners are often correct and should be treated conservatively.",
                    "Treat generic B2B words such as wholesale, bulk, supplier, vendor, manufacturer, distributor, procurement, and sourcing as low-signal commercial intent words unless they clearly change the product family.",
                    "Do not mark a category winner incorrect just because its title or URL does not repeat those generic B2B words.",
                    "If semantics says eligible_page_types is category-only, only call the winner incorrect when it is off-topic, the wrong page type, or it drops the actual product family or meaningful subtype.",
                    "Do not recommend use_original when the winner is a stronger canonical category than a narrower original source category unless the winner is clearly off-topic.",
                    "If the original landing page looks better than the winner, recommend use_original.",
                    "If the alternate looks better than the winner, recommend use_alternate.",
                    "Use tune_logic for systemic mistakes and manual_review when human judgment is genuinely needed.",
                    "Choose issue_type from the allowed list only.",
                    "Choose recommended_action from the allowed list only.",
                ],
                "allowed_verdicts": sorted(GATE_REVIEW_AGENT_VERDICTS),
                "allowed_issue_types": sorted(GATE_REVIEW_AGENT_ISSUE_TYPES),
                "allowed_actions": sorted(GATE_REVIEW_AGENT_ACTIONS),
                "required_review_fields": [
                    "gate_record_id",
                    "verdict",
                    "issue_type",
                    "recommended_action",
                    "confidence",
                    "rationale",
                ],
                "rows": [serialize_query_gate_row_for_agent_review(row) for row in batch],
            }
            response = client.chat.completions.create(
                model=model_name,
                temperature=0,
                response_format={"type": "json_object"},
                messages=[
                    {
                        "role": "system",
                        "content": "You audit ecommerce routing suggestions for obvious failures. Reply with a valid JSON object only.",
                    },
                    {"role": "user", "content": json.dumps(prompt)},
                ],
            )
            choice = response.choices[0] if getattr(response, "choices", None) else None
            content = getattr(getattr(choice, "message", None), "content", None)
            for raw_item in parse_agent_json_list(content):
                item = normalize_gate_review_item(raw_item)
                gate_record_id = int(item.get("gate_record_id") or 0)
                verdict = ((item.get("verdict") or "").strip().lower())
                issue_type = ((item.get("issue_type") or "").strip().lower())
                recommended_action = ((item.get("recommended_action") or "").strip().lower())
                if gate_record_id <= 0 or verdict not in GATE_REVIEW_AGENT_VERDICTS:
                    continue
                if issue_type not in GATE_REVIEW_AGENT_ISSUE_TYPES:
                    issue_type = "needs_human_review"
                if recommended_action not in GATE_REVIEW_AGENT_ACTIONS:
                    recommended_action = "manual_review"
                cluster_key, cluster_label = gate_review_cluster_values(verdict, issue_type, recommended_action)
                reviews.append(
                    {
                        "gate_record_id": gate_record_id,
                        "verdict": verdict,
                        "issue_type": issue_type,
                        "recommended_action": recommended_action,
                        "confidence": max(0.0, min(1.0, float(item.get("confidence") or 0.0))),
                        "cluster_key": cluster_key,
                        "cluster_label": cluster_label,
                        "rationale": (item.get("rationale") or "").strip(),
                        "metadata": {
                            "initiated_by": initiated_by or "fulcrum",
                            "review_notes": (item.get("review_notes") or "").strip(),
                        },
                    }
                )
        return {"status": "ok", "reason": "", "reviews": reviews, "model_name": model_name}
    except Exception as exc:
        return {"status": "error", "reason": str(exc), "reviews": [], "model_name": None}


def store_query_gate_agent_reviews(
    store_hash: str,
    run_id: int,
    reviews: list[dict[str, Any]],
    gate_rows: dict[int, dict[str, Any]],
    model_name: str | None = None,
    created_by: str | None = None,
) -> int:
    if not reviews:
        return 0

    sql = """
        INSERT INTO app_runtime.query_gate_agent_reviews (
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
            created_by
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s
        )
        ON CONFLICT (gate_record_id)
        DO UPDATE SET
            run_id = EXCLUDED.run_id,
            store_hash = EXCLUDED.store_hash,
            normalized_query_key = EXCLUDED.normalized_query_key,
            representative_query = EXCLUDED.representative_query,
            source_url = EXCLUDED.source_url,
            source_entity_type = EXCLUDED.source_entity_type,
            source_entity_id = EXCLUDED.source_entity_id,
            target_entity_type = EXCLUDED.target_entity_type,
            target_entity_id = EXCLUDED.target_entity_id,
            verdict = EXCLUDED.verdict,
            issue_type = EXCLUDED.issue_type,
            recommended_action = EXCLUDED.recommended_action,
            confidence = EXCLUDED.confidence,
            cluster_key = EXCLUDED.cluster_key,
            cluster_label = EXCLUDED.cluster_label,
            rationale = EXCLUDED.rationale,
            model_name = EXCLUDED.model_name,
            metadata = EXCLUDED.metadata,
            created_by = EXCLUDED.created_by,
            updated_at = NOW();
    """
    params: list[tuple[Any, ...]] = []
    for review in reviews:
        gate_record_id = int(review.get("gate_record_id") or 0)
        gate_row = gate_rows.get(gate_record_id)
        if not gate_row:
            continue
        winner = gate_row.get("suggested_target") or {}
        metadata = dict(review.get("metadata") or {})
        metadata["winner"] = {
            "entity_type": winner.get("entity_type"),
            "entity_id": winner.get("entity_id"),
            "name": winner.get("name"),
            "url": winner.get("url"),
            "score": winner.get("score"),
        }
        alternate = gate_row.get("second_option") or {}
        if alternate:
            metadata["alternate"] = {
                "entity_type": alternate.get("entity_type"),
                "entity_id": alternate.get("entity_id"),
                "name": alternate.get("name"),
                "url": alternate.get("url"),
                "score": alternate.get("score"),
            }
        params.append(
            (
                gate_record_id,
                run_id,
                store_hash,
                gate_row.get("normalized_query_key") or "",
                gate_row.get("representative_query") or "",
                gate_row.get("source_url") or "",
                gate_row.get("source_entity_type") or gate_row.get("current_page_type") or "product",
                int(gate_row.get("source_entity_id") or 0) or None,
                winner.get("entity_type"),
                int(winner.get("entity_id") or 0) or None,
                review.get("verdict") or "unclear",
                review.get("issue_type") or "needs_human_review",
                review.get("recommended_action") or "manual_review",
                float(review.get("confidence") or 0.0),
                review.get("cluster_key") or "",
                review.get("cluster_label") or "",
                review.get("rationale") or "",
                model_name or "",
                json.dumps(metadata),
                created_by or "fulcrum",
            )
        )
    if not params:
        return 0
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            execute_batch(cur, sql, params, page_size=50)
            feedback_params = [
                (
                    review.get("issue_type") or "needs_human_review",
                    review.get("recommended_action") or "manual_review",
                    {
                        "agent_verdict": review.get("verdict") or "unclear",
                        "agent_confidence": float(review.get("confidence") or 0.0),
                        "agent_cluster_key": review.get("cluster_key") or "",
                        "agent_rationale": review.get("rationale") or "",
                    },
                    store_hash,
                    int(review.get("gate_record_id") or 0),
                )
                for review in reviews
                if int(review.get("gate_record_id") or 0) > 0
            ]
            if feedback_params:
                execute_batch(
                    cur,
                    """
                    UPDATE app_runtime.query_gate_decision_feedback
                    SET feedback_status = 'agent_diagnosed',
                        diagnosis_category = %s,
                        recommended_action = %s,
                        metadata = COALESCE(metadata, '{}'::jsonb) || %s::jsonb,
                        updated_at = NOW()
                    WHERE store_hash = %s
                      AND gate_record_id = %s
                      AND action = 'review'
                    """,
                    [(category, action, json.dumps(metadata), store_hash, gate_record_id) for category, action, metadata, store_hash, gate_record_id in feedback_params],
                    page_size=50,
                )
        conn.commit()
    return len(params)


def list_query_gate_agent_reviews(
    store_hash: str,
    run_id: int | None = None,
    verdict: str | None = None,
    limit: int = 100,
    *,
    apply_runtime_schema_fn: Callable[[], None],
    latest_gate_run_id_fn: Callable[[str], int | None],
) -> list[dict[str, Any]]:
    apply_runtime_schema_fn()
    resolved_run_id = run_id or latest_gate_run_id_fn(store_hash)
    if not resolved_run_id:
        return []
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
          AND run_id = %s
          AND (%s IS NULL OR verdict = %s)
        ORDER BY
            CASE verdict
                WHEN 'incorrect' THEN 0
                WHEN 'unclear' THEN 1
                ELSE 2
            END,
            confidence DESC,
            updated_at DESC
        LIMIT %s;
    """
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (store_hash, resolved_run_id, verdict, verdict, limit))
            return [dict(row) for row in cur.fetchall()]


def summarize_query_gate_agent_reviews(
    store_hash: str,
    run_id: int | None = None,
    *,
    apply_runtime_schema_fn: Callable[[], None],
    latest_gate_run_id_fn: Callable[[str], int | None],
) -> dict[str, Any]:
    apply_runtime_schema_fn()
    resolved_run_id = run_id or latest_gate_run_id_fn(store_hash)
    if not resolved_run_id:
        return {"run_id": None, "correct": 0, "incorrect": 0, "unclear": 0}
    counts = {"correct": 0, "incorrect": 0, "unclear": 0}
    sql = """
        SELECT verdict, COUNT(*)
        FROM app_runtime.query_gate_agent_reviews
        WHERE store_hash = %s
          AND run_id = %s
        GROUP BY verdict;
    """
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (store_hash, resolved_run_id))
            for verdict_value, count in cur.fetchall():
                counts[str(verdict_value)] = int(count or 0)
    return {"run_id": resolved_run_id, **counts}


def list_query_gate_agent_review_clusters(
    store_hash: str,
    run_id: int | None = None,
    limit: int = 10,
    *,
    apply_runtime_schema_fn: Callable[[], None],
    latest_gate_run_id_fn: Callable[[str], int | None],
) -> list[dict[str, Any]]:
    apply_runtime_schema_fn()
    resolved_run_id = run_id or latest_gate_run_id_fn(store_hash)
    if not resolved_run_id:
        return []
    sql = """
        SELECT
            cluster_key,
            cluster_label,
            verdict,
            issue_type,
            recommended_action,
            COUNT(*) AS review_count
        FROM app_runtime.query_gate_agent_reviews
        WHERE store_hash = %s
          AND run_id = %s
        GROUP BY cluster_key, cluster_label, verdict, issue_type, recommended_action
        ORDER BY
            CASE verdict
                WHEN 'incorrect' THEN 0
                WHEN 'unclear' THEN 1
                ELSE 2
            END,
            COUNT(*) DESC,
            cluster_label ASC
        LIMIT %s;
    """
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (store_hash, resolved_run_id, limit))
            return [dict(row) for row in cur.fetchall()]


def load_query_gate_agent_review_map(
    store_hash: str,
    run_id: int | None = None,
    *,
    list_query_gate_agent_reviews_fn: Callable[..., list[dict[str, Any]]],
    query_gate_record_map_for_ids_fn: Callable[..., dict[int, dict[str, Any]]],
    gate_row_semantics_analysis_fn: Callable[[dict[str, Any], str], dict[str, Any]],
) -> dict[int, dict[str, Any]]:
    rows = list_query_gate_agent_reviews_fn(store_hash, run_id=run_id, verdict=None, limit=500)
    gate_row_map = query_gate_record_map_for_ids_fn(
        store_hash,
        {
            int(row.get("gate_record_id") or 0)
            for row in rows
            if int(row.get("gate_record_id") or 0) > 0
        },
        run_ids={int(run_id)} if int(run_id or 0) > 0 else None,
    )
    processed_rows = postprocess_gate_agent_reviews(
        rows,
        gate_row_map,
        gate_row_semantics_analysis_fn=gate_row_semantics_analysis_fn,
    )
    review_map: dict[int, dict[str, Any]] = {}
    for row in processed_rows:
        gate_record_id = int(row.get("gate_record_id") or 0)
        if gate_record_id > 0 and gate_record_id not in review_map:
            review_map[gate_record_id] = row
    return review_map


def postprocess_gate_agent_reviews(
    reviews: list[dict[str, Any]],
    gate_row_map: dict[int, dict[str, Any]],
    *,
    gate_row_semantics_analysis_fn: Callable[[dict[str, Any], str], dict[str, Any]],
) -> list[dict[str, Any]]:
    normalized_reviews: list[dict[str, Any]] = []
    for review in reviews:
        gate_record_id = int(review.get("gate_record_id") or 0)
        gate_row = gate_row_map.get(gate_record_id) or {}
        metadata = gate_row.get("metadata") if isinstance(gate_row.get("metadata"), dict) else {}
        winner = gate_row.get("suggested_target") or (metadata.get("suggested_target_snapshot") or {})
        semantics = (metadata.get("semantics_analysis") or {}) if isinstance(metadata, dict) else {}
        if not semantics:
            review_store_hash = (review.get("store_hash") or gate_row.get("store_hash") or "").strip()
            if review_store_hash and gate_row:
                try:
                    semantics = gate_row_semantics_analysis_fn(gate_row, review_store_hash)
                except Exception:
                    semantics = {}
        preferred_entity_type = ((gate_row.get("preferred_entity_type") or "").strip().lower())
        winner_entity_type = ((winner.get("entity_type") or "").strip().lower())
        query_intent_scope = ((gate_row.get("query_intent_scope") or "").strip().lower())
        brand_family_matching_product_count = int(semantics.get("brand_family_matching_product_count") or 0)

        normalized = dict(review)
        if (
            normalized.get("verdict") == "incorrect"
            and normalized.get("issue_type") == "wrong_page_type"
            and preferred_entity_type
            and winner_entity_type == preferred_entity_type
        ):
            normalized["verdict"] = "correct"
            normalized["issue_type"] = "looks_correct"
            normalized["recommended_action"] = "keep_winner"
            normalized["rationale"] = (
                "Fulcrum winner already matches the preferred page type, so the audit downgraded this from a false "
                "wrong-page-type warning."
            )
        elif (
            normalized.get("verdict") == "incorrect"
            and query_intent_scope in {"broad_product_family", "commercial_topic"}
            and preferred_entity_type == "category"
            and winner_entity_type == "category"
            and normalized.get("recommended_action") in {"use_alternate", "use_original"}
        ):
            normalized["verdict"] = "correct"
            normalized["issue_type"] = "looks_correct"
            normalized["recommended_action"] = "keep_winner"
            normalized["rationale"] = (
                "Broad category intent plus a category winner looks correct; the audit treated the alternate/original "
                "suggestion as reviewer noise."
            )
        elif (
            normalized.get("verdict") == "incorrect"
            and winner_entity_type == "brand"
            and brand_family_matching_product_count >= 2
            and normalized.get("recommended_action") in {"use_alternate", "use_original", "manual_review"}
        ):
            normalized["verdict"] = "correct"
            normalized["issue_type"] = "looks_correct"
            normalized["recommended_action"] = "keep_winner"
            normalized["rationale"] = (
                "This brand-plus-family query matches multiple products under the same brand, so Fulcrum kept the "
                "brand page instead of forcing a single product."
            )
        cluster_key, cluster_label = gate_review_cluster_values(
            normalized.get("verdict"),
            normalized.get("issue_type"),
            normalized.get("recommended_action"),
        )
        normalized["cluster_key"] = cluster_key
        normalized["cluster_label"] = cluster_label
        normalized_reviews.append(normalized)
    return normalized_reviews


def run_query_gate_agent_review(
    store_hash: str,
    run_id: int | None = None,
    disposition: str | None = None,
    limit: int = 40,
    cluster: str | None = None,
    initiated_by: str | None = None,
    gate_record_ids: list[int] | None = None,
    *,
    apply_runtime_schema_fn: Callable[[], None],
    get_query_gate_record_by_id_fn: Callable[[str, int], dict[str, Any] | None],
    latest_gate_run_id_fn: Callable[[str], int | None],
    list_query_gate_records_fn: Callable[..., list[dict[str, Any]]],
    annotate_query_gate_rows_with_suggestions_fn: Callable[..., list[dict[str, Any]]],
    summarize_query_gate_agent_reviews_fn: Callable[[str, int | None], dict[str, Any]],
    list_query_gate_agent_review_clusters_fn: Callable[[str, int | None, int], list[dict[str, Any]]],
    gate_row_semantics_analysis_fn: Callable[[dict[str, Any], str], dict[str, Any]],
    review_query_gate_rows_with_agent_fn: Callable[[str, list[dict[str, Any]], str | None], dict[str, Any]] | None = None,
    store_query_gate_agent_reviews_fn: Callable[..., int] | None = None,
) -> dict[str, Any]:
    apply_runtime_schema_fn()
    requested_gate_record_ids = sorted({int(value) for value in (gate_record_ids or []) if int(value or 0) > 0})
    gate_rows: list[dict[str, Any]] = []
    resolved_run_id = run_id
    if requested_gate_record_ids:
        for gate_record_id in requested_gate_record_ids:
            row = get_query_gate_record_by_id_fn(store_hash, gate_record_id)
            if not row:
                continue
            if resolved_run_id is None:
                resolved_run_id = int(row.get("run_id") or 0) or None
            if resolved_run_id is not None and int(row.get("run_id") or 0) != int(resolved_run_id):
                continue
            gate_rows.append(row)
    else:
        resolved_run_id = resolved_run_id or latest_gate_run_id_fn(store_hash)
        if not resolved_run_id:
            return {
                "status": "skipped",
                "reason": "No completed gate run is available yet",
                "run_id": None,
                "reviewed_count": 0,
                "stored_count": 0,
                "reviews": [],
                "summary": {"run_id": None, "correct": 0, "incorrect": 0, "unclear": 0},
                "clusters": [],
            }
        gate_rows = list_query_gate_records_fn(store_hash, disposition=disposition, limit=max(limit, 1), run_id=resolved_run_id)

    if not gate_rows:
        return {
            "status": "skipped",
            "reason": "No matching query-family rows were available to review.",
            "run_id": resolved_run_id,
            "reviewed_count": 0,
            "stored_count": 0,
            "reviews": [],
            "summary": summarize_query_gate_agent_reviews_fn(store_hash, resolved_run_id),
            "clusters": list_query_gate_agent_review_clusters_fn(store_hash, resolved_run_id, 10),
        }

    annotated_rows = annotate_query_gate_rows_with_suggestions_fn(store_hash, gate_rows, cluster=cluster)
    review_query_gate_rows_with_agent_fn = review_query_gate_rows_with_agent_fn or review_query_gate_rows_with_agent
    review_result = review_query_gate_rows_with_agent_fn(store_hash, annotated_rows, initiated_by=initiated_by)
    if review_result.get("status") != "ok":
        return {
            "status": review_result.get("status") or "skipped",
            "reason": review_result.get("reason") or "",
            "run_id": resolved_run_id,
            "reviewed_count": len(annotated_rows),
            "stored_count": 0,
            "reviews": [],
            "summary": summarize_query_gate_agent_reviews_fn(store_hash, resolved_run_id),
            "clusters": list_query_gate_agent_review_clusters_fn(store_hash, resolved_run_id, 10),
        }

    parsed_reviews = list(review_result.get("reviews") or [])
    if not parsed_reviews:
        return {
            "status": "skipped",
            "reason": review_result.get("reason") or "The AI reviewer returned no structured verdicts.",
            "run_id": resolved_run_id,
            "reviewed_count": len(annotated_rows),
            "stored_count": 0,
            "reviews": [],
            "model_name": review_result.get("model_name"),
            "summary": summarize_query_gate_agent_reviews_fn(store_hash, resolved_run_id),
            "clusters": list_query_gate_agent_review_clusters_fn(store_hash, resolved_run_id, 10),
        }

    gate_row_map = {int(row.get("gate_record_id") or 0): row for row in annotated_rows if int(row.get("gate_record_id") or 0) > 0}
    parsed_reviews = postprocess_gate_agent_reviews(
        parsed_reviews,
        gate_row_map,
        gate_row_semantics_analysis_fn=gate_row_semantics_analysis_fn,
    )
    store_query_gate_agent_reviews_fn = store_query_gate_agent_reviews_fn or store_query_gate_agent_reviews
    stored_count = store_query_gate_agent_reviews_fn(
        store_hash=store_hash,
        run_id=resolved_run_id,
        reviews=parsed_reviews,
        gate_rows=gate_row_map,
        model_name=review_result.get("model_name"),
        created_by=initiated_by or "fulcrum",
    )
    return {
        "status": "ok",
        "reason": "",
        "run_id": resolved_run_id,
        "reviewed_count": len(annotated_rows),
        "stored_count": stored_count,
        "model_name": review_result.get("model_name"),
        "reviews": parsed_reviews,
        "summary": summarize_query_gate_agent_reviews_fn(store_hash, resolved_run_id),
        "clusters": list_query_gate_agent_review_clusters_fn(store_hash, resolved_run_id, 10),
    }


__all__ = [
    "agent_review_signal_snapshot",
    "gate_review_cluster_values",
    "list_query_gate_agent_review_clusters",
    "list_query_gate_agent_reviews",
    "load_query_gate_agent_review_map",
    "normalize_gate_review_item",
    "parse_agent_json_list",
    "postprocess_gate_agent_reviews",
    "review_query_gate_rows_with_agent",
    "run_query_gate_agent_review",
    "serialize_query_gate_row_for_agent_review",
    "store_query_gate_agent_reviews",
    "summarize_query_gate_agent_reviews",
]
