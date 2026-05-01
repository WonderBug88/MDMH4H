"""Changed-route review summaries and reasoning for Fulcrum."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
import json
import os
from typing import Any, Callable


def attach_changed_route_agent_reviews(
    rows: list[dict[str, Any]],
    review_map: dict[int, dict[str, Any]],
) -> list[dict[str, Any]]:
    annotated_rows: list[dict[str, Any]] = []
    for row in rows:
        annotated = dict(row)
        gate_record_id = int(annotated.get("gate_record_id") or 0)
        review = review_map.get(gate_record_id)
        annotated["agent_review"] = dict(review) if review else None
        annotated_rows.append(annotated)
    return annotated_rows


def summarize_changed_route_agent_reviews(
    rows: list[dict[str, Any]],
    review_map: dict[int, dict[str, Any]],
) -> dict[str, Any]:
    summary = {
        "row_count": len(rows),
        "reviewed_count": 0,
        "unaudited_count": 0,
        "correct": 0,
        "incorrect": 0,
        "unclear": 0,
    }
    for row in rows:
        gate_record_id = int(row.get("gate_record_id") or 0)
        review = review_map.get(gate_record_id)
        if not review:
            summary["unaudited_count"] += 1
            continue
        summary["reviewed_count"] += 1
        verdict = ((review.get("verdict") or "").strip().lower()) or "unclear"
        if verdict not in {"correct", "incorrect", "unclear"}:
            verdict = "unclear"
        summary[verdict] += 1
    return summary


def changed_route_review_next_step_label(action: str | None) -> str:
    normalized = ((action or "").strip().lower()) or "manual_review"
    labels = {
        "keep_winner": "Keep the current Fulcrum route.",
        "use_original": "Keep Google's current page for now.",
        "use_alternate": "Try the alternate route instead.",
        "tune_logic": "Adjust deterministic routing logic.",
        "manual_review": "Needs manual admin review.",
    }
    return labels.get(normalized, "Needs manual admin review.")


def fallback_changed_route_review_reasoning(
    rows: list[dict[str, Any]],
    review_map: dict[int, dict[str, Any]],
) -> dict[str, Any]:
    summary = summarize_changed_route_agent_reviews(rows, review_map)
    problem_reviews: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for row in rows:
        review = review_map.get(int(row.get("gate_record_id") or 0))
        if not review:
            continue
        verdict = ((review.get("verdict") or "").strip().lower())
        if verdict in {"incorrect", "unclear"}:
            problem_reviews.append((row, review))

    buckets: dict[tuple[str, str], list[tuple[dict[str, Any], dict[str, Any]]]] = defaultdict(list)
    for row, review in problem_reviews:
        bucket_key = (
            ((review.get("issue_type") or "").strip().lower()) or "needs_human_review",
            ((review.get("recommended_action") or "").strip().lower()) or "manual_review",
        )
        buckets[bucket_key].append((row, review))

    patterns: list[dict[str, Any]] = []
    for (issue_type, action), items in sorted(
        buckets.items(),
        key=lambda item: (-len(item[1]), item[0][0], item[0][1]),
    ):
        sample_review = items[0][1]
        sample_rationale = (sample_review.get("rationale") or "").strip()
        gate_record_ids = [int(row.get("gate_record_id") or 0) for row, _ in items][:8]
        patterns.append(
            {
                "label": (sample_review.get("cluster_label") or issue_type.replace("_", " ").title() or "Needs review"),
                "why": sample_rationale or "These changed routes are clustering around the same review issue.",
                "gate_record_ids": gate_record_ids,
                "next_step": changed_route_review_next_step_label(action),
                "review_count": len(items),
                "confidence": round(
                    sum(float(review.get("confidence") or 0.0) for _, review in items) / max(len(items), 1),
                    2,
                ),
            }
        )

    if summary["reviewed_count"] == 0:
        summary_text = "Changed routes have not been audited yet."
    elif summary["incorrect"] or summary["unclear"]:
        summary_text = (
            f"{summary['incorrect']} changed route(s) look incorrect and "
            f"{summary['unclear']} look unclear. Review the top patterns before resolving them."
        )
    else:
        summary_text = "The audited changed routes currently look reasonable."

    return {
        "status": "fallback",
        "summary_text": summary_text,
        "patterns": patterns,
        "model_name": None,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def reason_about_changed_route_reviews_with_agent(
    rows: list[dict[str, Any]],
    review_map: dict[int, dict[str, Any]],
    *,
    initiated_by: str | None = None,
) -> dict[str, Any]:
    fallback = fallback_changed_route_review_reasoning(rows, review_map)
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return fallback

    reviewed_rows: list[dict[str, Any]] = []
    for row in rows:
        gate_record_id = int(row.get("gate_record_id") or 0)
        review = review_map.get(gate_record_id)
        if not review:
            continue
        verdict = ((review.get("verdict") or "").strip().lower())
        if verdict not in {"incorrect", "unclear"}:
            continue
        winner = row.get("suggested_target") or {}
        reviewed_rows.append(
            {
                "gate_record_id": gate_record_id,
                "query": row.get("representative_query") or "",
                "family": row.get("normalized_query_key") or "",
                "current_page": {
                    "entity_type": row.get("current_page_type") or row.get("source_entity_type") or "unknown",
                    "name": row.get("source_name") or "",
                    "url": row.get("source_url") or "",
                },
                "fulcrum_route": {
                    "entity_type": winner.get("entity_type") or "unknown",
                    "name": winner.get("name") or "",
                    "url": winner.get("url") or "",
                },
                "page_type_decision": {
                    "preferred_entity_type": row.get("preferred_entity_type") or "unknown",
                    "query_intent_scope": row.get("query_intent_scope") or "mixed_or_unknown",
                },
                "why_it_changed": row.get("reason_summary") or "",
                "review": {
                    "verdict": verdict,
                    "issue_type": review.get("issue_type") or "",
                    "recommended_action": review.get("recommended_action") or "",
                    "confidence": float(review.get("confidence") or 0.0),
                    "rationale": review.get("rationale") or "",
                },
            }
        )

    if not reviewed_rows:
        return fallback

    try:
        from openai import OpenAI

        client = OpenAI(api_key=api_key)
        model_name = os.getenv("FULCRUM_CHANGED_ROUTE_REASONING_MODEL", "gpt-4o-mini")
        prompt = {
            "task": "Summarize why Fulcrum's changed-route decisions look wrong or unclear before an admin resolves them.",
            "instructions": [
                "Return JSON only.",
                "Return an object with summary_text and patterns.",
                "patterns must be an array of objects.",
                "Group the rows into a few repeated logic patterns, not one pattern per row.",
                "Explain what Fulcrum likely over-inferred from the data.",
                "Keep each why and next_step concise and practical.",
                "Do not suggest manual route changes as the only answer when the pattern obviously points to a logic bug.",
            ],
            "required_pattern_fields": ["label", "why", "gate_record_ids", "next_step", "confidence"],
            "rows": reviewed_rows,
            "context": {
                "initiated_by": initiated_by or "fulcrum",
                "reviewed_changed_routes": len(reviewed_rows),
            },
        }
        response = client.chat.completions.create(
            model=model_name,
            temperature=0,
            response_format={"type": "json_object"},
            messages=[
                {
                    "role": "system",
                    "content": "You explain repeated ecommerce routing failures for admins. Reply with a valid JSON object only.",
                },
                {"role": "user", "content": json.dumps(prompt)},
            ],
        )
        choice = response.choices[0] if getattr(response, "choices", None) else None
        content = getattr(getattr(choice, "message", None), "content", None)
        parsed = json.loads(str(content or "{}"))
        patterns_payload = parsed.get("patterns") if isinstance(parsed, dict) else []
        patterns: list[dict[str, Any]] = []
        if isinstance(patterns_payload, list):
            for item in patterns_payload:
                if not isinstance(item, dict):
                    continue
                gate_record_ids = [
                    int(value)
                    for value in (item.get("gate_record_ids") or [])
                    if str(value).strip().isdigit()
                ]
                patterns.append(
                    {
                        "label": str(item.get("label") or "Needs review").strip() or "Needs review",
                        "why": str(item.get("why") or "").strip(),
                        "gate_record_ids": gate_record_ids[:8],
                        "next_step": str(item.get("next_step") or "").strip() or "Needs manual admin review.",
                        "confidence": max(0.0, min(1.0, float(item.get("confidence") or 0.0))),
                        "review_count": len(gate_record_ids),
                    }
                )
        return {
            "status": "ok",
            "summary_text": str(parsed.get("summary_text") or fallback.get("summary_text") or "").strip(),
            "patterns": patterns or fallback.get("patterns") or [],
            "model_name": model_name,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
    except Exception:
        return fallback


def get_cached_changed_route_review_reasoning(
    store_hash: str,
    *,
    run_id: int | None = None,
    rows: list[dict[str, Any]] | None = None,
    review_map: dict[int, dict[str, Any]] | None = None,
    force_refresh: bool = False,
    initiated_by: str | None = None,
    latest_gate_run_id_fn: Callable[[str], int | None],
    load_admin_metric_cache_fn: Callable[[str, str], dict[str, Any] | None],
    store_admin_metric_cache_fn: Callable[[str, str, dict[str, Any]], dict[str, Any]],
    gate_review_map_for_ids_fn: Callable[..., dict[int, dict[str, Any]]],
    reason_about_changed_route_reviews_with_agent_fn: Callable[..., dict[str, Any]] | None = None,
) -> dict[str, Any]:
    resolved_run_id = run_id or latest_gate_run_id_fn(store_hash)
    rows = list(rows or [])
    if not resolved_run_id or not rows:
        return {
            "status": "empty",
            "summary_text": "No changed routes are available in the latest run yet.",
            "patterns": [],
            "model_name": None,
            "generated_at": None,
        }

    metric_key = f"changed_route_review_reasoning_run_{int(resolved_run_id)}_limit_{len(rows)}"
    if not force_refresh:
        cached = load_admin_metric_cache_fn(store_hash, metric_key)
        if cached is not None:
            return cached

    review_map = review_map or gate_review_map_for_ids_fn(
        store_hash,
        {int(row.get('gate_record_id') or 0) for row in rows if int(row.get('gate_record_id') or 0) > 0},
        run_id=resolved_run_id,
    )
    if force_refresh:
        reason_about_changed_route_reviews_with_agent_fn = (
            reason_about_changed_route_reviews_with_agent_fn or reason_about_changed_route_reviews_with_agent
        )
        payload = reason_about_changed_route_reviews_with_agent_fn(rows, review_map, initiated_by=initiated_by)
        return store_admin_metric_cache_fn(store_hash, metric_key, payload)
    return fallback_changed_route_review_reasoning(rows, review_map)


def run_changed_route_agent_review(
    store_hash: str,
    *,
    run_id: int | None = None,
    limit: int = 25,
    initiated_by: str | None = None,
    latest_gate_run_id_fn: Callable[[str], int | None],
    list_changed_route_results_fn: Callable[[str, int | None, int], list[dict[str, Any]]],
    run_query_gate_agent_review_fn: Callable[..., dict[str, Any]],
    gate_review_map_for_ids_fn: Callable[..., dict[int, dict[str, Any]]],
    get_cached_changed_route_review_reasoning_fn: Callable[..., dict[str, Any]],
    summarize_changed_route_agent_reviews_fn: Callable[[list[dict[str, Any]], dict[int, dict[str, Any]]], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    resolved_run_id = run_id or latest_gate_run_id_fn(store_hash)
    rows = list_changed_route_results_fn(store_hash, resolved_run_id, limit)
    gate_record_ids = [int(row.get("gate_record_id") or 0) for row in rows if int(row.get("gate_record_id") or 0) > 0]
    if not gate_record_ids:
        return {
            "status": "skipped",
            "reason": "No changed routes were available to audit.",
            "run_id": resolved_run_id,
            "reviewed_count": 0,
            "stored_count": 0,
            "summary": {"correct": 0, "incorrect": 0, "unclear": 0, "reviewed_count": 0, "unaudited_count": 0},
            "reasoning": {
                "status": "empty",
                "summary_text": "No changed routes are available in the latest run yet.",
                "patterns": [],
                "model_name": None,
                "generated_at": None,
            },
        }

    result = run_query_gate_agent_review_fn(
        store_hash=store_hash,
        run_id=resolved_run_id,
        gate_record_ids=gate_record_ids,
        initiated_by=initiated_by,
    )
    review_map = gate_review_map_for_ids_fn(store_hash, set(gate_record_ids), run_id=resolved_run_id)
    reasoning = get_cached_changed_route_review_reasoning_fn(
        store_hash,
        run_id=resolved_run_id,
        rows=rows,
        review_map=review_map,
        force_refresh=(result.get("status") == "ok"),
        initiated_by=initiated_by,
    )
    summarize_changed_route_agent_reviews_fn = (
        summarize_changed_route_agent_reviews_fn or summarize_changed_route_agent_reviews
    )
    result["reasoning"] = reasoning
    result["changed_route_review_summary"] = summarize_changed_route_agent_reviews_fn(rows, review_map)
    return result


__all__ = [
    "attach_changed_route_agent_reviews",
    "changed_route_review_next_step_label",
    "fallback_changed_route_review_reasoning",
    "get_cached_changed_route_review_reasoning",
    "reason_about_changed_route_reviews_with_agent",
    "run_changed_route_agent_review",
    "summarize_changed_route_agent_reviews",
]
