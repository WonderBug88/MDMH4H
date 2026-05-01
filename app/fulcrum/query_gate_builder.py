"""Query gate record builder helpers for Fulcrum."""

from __future__ import annotations

import math
from typing import Any, Callable


WEAK_QUERY_TAIL_TOKENS = {
    "a",
    "an",
    "and",
    "best",
    "buy",
    "for",
    "in",
    "near",
    "online",
    "or",
    "sale",
    "shop",
    "the",
    "to",
}


def _normalize_query_token(token: str | None) -> str:
    value = str(token or "").strip().lower()
    if not value:
        return ""
    if len(value) > 4 and value.endswith("ies"):
        return value[:-3] + "y"
    if len(value) > 3 and value.endswith("es"):
        return value[:-2]
    if len(value) > 3 and value.endswith("s") and not value.endswith("ss"):
        return value[:-1]
    return value


def _ordered_meaningful_query_tokens(
    query: str | None,
    tokenize_intent_text_fn: Callable[[str | None], set[str]],
    query_noise_words: set[str],
) -> list[str]:
    allowed_tokens = {
        token
        for token in tokenize_intent_text_fn(query)
        if token and token not in query_noise_words and token not in WEAK_QUERY_TAIL_TOKENS
    }
    ordered_tokens: list[str] = []
    seen_tokens: set[str] = set()
    for raw_token in str(query or "").lower().replace("/", " ").replace("-", " ").split():
        token = "".join(ch for ch in raw_token if ch.isalnum())
        if not token or token not in allowed_tokens or token in seen_tokens:
            continue
        seen_tokens.add(token)
        ordered_tokens.append(token)
    return ordered_tokens


def _leading_query_qualifier_tokens(
    query: str | None,
    head_term: str | None,
    tokenize_intent_text_fn: Callable[[str | None], set[str]],
    query_noise_words: set[str],
) -> list[str]:
    ordered_tokens = _ordered_meaningful_query_tokens(query, tokenize_intent_text_fn, query_noise_words)
    if not ordered_tokens:
        return []
    normalized_head = _normalize_query_token(head_term)
    if not normalized_head:
        return ordered_tokens[:2]

    leading_tokens: list[str] = []
    for token in ordered_tokens:
        if _normalize_query_token(token) == normalized_head:
            break
        leading_tokens.append(token)
    return leading_tokens[:2]


def _clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, float(value)))


def current_page_gsc_trust_score(avg_position_90d: float, impressions_90d: int) -> float:
    position_score = _clamp((15.0 - float(avg_position_90d or 0.0)) / 15.0, 0.0, 1.0)
    impression_score = _clamp(math.log10(max(int(impressions_90d or 0), 0) + 1) / 3.0, 0.0, 1.0)
    return round((0.55 * position_score) + (0.45 * impression_score), 4)


def build_current_page_preservation_guard(
    representative_query: str | None,
    source_profile: dict[str, Any],
    semantics_analysis: dict[str, Any] | None,
    avg_position_90d: float,
    impressions_90d: int,
    *,
    tokenize_intent_text_fn: Callable[[str | None], set[str]],
    query_noise_words: set[str],
) -> dict[str, Any]:
    source_tokens = tokenize_intent_text_fn(
        f"{source_profile.get('name') or ''} {source_profile.get('url') or ''}"
    )
    ordered_tokens = _ordered_meaningful_query_tokens(
        representative_query,
        tokenize_intent_text_fn,
        query_noise_words,
    )
    head_term = (semantics_analysis or {}).get("head_term")
    leading_qualifiers = _leading_query_qualifier_tokens(
        representative_query,
        head_term,
        tokenize_intent_text_fn,
        query_noise_words,
    )
    normalized_source_tokens = {_normalize_query_token(token) for token in source_tokens if token}
    normalized_head = _normalize_query_token(head_term)
    preserves_head_term = bool(normalized_head and normalized_head in normalized_source_tokens)
    preserves_leading_qualifiers = bool(leading_qualifiers) and all(token in source_tokens for token in leading_qualifiers)
    trust_score = current_page_gsc_trust_score(avg_position_90d, impressions_90d)
    strong_gsc_alignment = bool(
        float(avg_position_90d or 0.0) > 0.0
        and float(avg_position_90d or 0.0) <= 12.0
        and int(impressions_90d or 0) >= 100
        and trust_score >= 0.55
    )
    return {
        "active": bool(strong_gsc_alignment and preserves_head_term and preserves_leading_qualifiers),
        "trust_score": trust_score,
        "strong_gsc_alignment": strong_gsc_alignment,
        "head_term": head_term or "",
        "ordered_query_tokens": ordered_tokens[:5],
        "leading_qualifiers": leading_qualifiers,
        "preserves_head_term": preserves_head_term,
        "preserves_leading_qualifiers": preserves_leading_qualifiers,
    }


def expected_ctr_for_position(avg_position: float) -> float:
    position = float(avg_position or 0.0)
    if position <= 0:
        return 0.0
    if position <= 3:
        return 0.16
    if position <= 5:
        return 0.09
    if position <= 10:
        return 0.045
    if position <= 20:
        return 0.02
    return 0.01


def build_freshness_context(
    clicks_28d: int,
    impressions_28d: int,
    clicks_90d: int,
    impressions_90d: int,
) -> dict[str, Any]:
    expected_clicks_28d = (float(clicks_90d or 0) * 28.0) / 90.0
    expected_impressions_28d = (float(impressions_90d or 0) * 28.0) / 90.0

    def _pct_delta(current: float, expected: float) -> float:
        baseline = max(expected, 1.0)
        return round(((current - expected) / baseline) * 100.0, 2)

    click_delta = _pct_delta(float(clicks_28d or 0), expected_clicks_28d)
    impression_delta = _pct_delta(float(impressions_28d or 0), expected_impressions_28d)
    if click_delta >= 20 or impression_delta >= 20:
        trend_label = "rising"
    elif click_delta <= -20 or impression_delta <= -20:
        trend_label = "softening"
    else:
        trend_label = "stable"
    return {
        "trend_label": trend_label,
        "click_delta_pct": click_delta,
        "impression_delta_pct": impression_delta,
        "expected_clicks_28d": round(expected_clicks_28d, 2),
        "expected_impressions_28d": round(expected_impressions_28d, 2),
    }


def build_query_gate_record(
    store_hash: str,
    family_key: str,
    representative_query: str,
    evidence_rows: list[dict[str, Any]],
    source_profiles: dict[str, dict[str, Any]],
    target_entities: list[dict[str, Any]],
    signal_library: dict[str, list[dict[str, Any]]] | None = None,
    *,
    normalize_storefront_path_fn: Callable[[Any], str],
    tokenize_intent_text_fn: Callable[[str | None], set[str]],
    resolve_query_signal_context_fn: Callable[..., dict[str, Any]],
    classify_query_intent_scope_fn: Callable[..., tuple[str, str]],
    build_query_semantics_analysis_fn: Callable[..., dict[str, Any]],
    fuzzy_match_score_fn: Callable[[str | None, str | None], float],
    expected_ctr_for_position_fn: Callable[[float], float],
    build_freshness_context_fn: Callable[[int, int, int, int], dict[str, Any]],
    query_noise_words: set[str],
) -> dict[str, Any] | None:
    del target_entities
    if not evidence_rows:
        return None

    dominant_row = max(
        evidence_rows,
        key=lambda row: (
            int(row.get("clicks_90d") or 0),
            int(row.get("impressions_90d") or 0),
            -float(row.get("avg_position_90d") or 999.0),
        ),
    )
    source_url = normalize_storefront_path_fn(dominant_row.get("source_url"))
    source_profile = source_profiles.get(source_url)
    if not source_profile:
        return None

    query = representative_query or dominant_row.get("query") or family_key
    variant_groups: dict[str, dict[str, Any]] = {}
    for row in evidence_rows:
        raw_query = (row.get("query") or "").strip()
        if not raw_query:
            continue
        group = variant_groups.setdefault(
            raw_query,
            {
                "query": raw_query,
                "clicks_28d": 0,
                "impressions_28d": 0,
                "clicks_90d": 0,
                "impressions_90d": 0,
                "position_weighted_28d": 0.0,
                "position_weighted_90d": 0.0,
                "position_impressions_28d": 0,
                "position_impressions_90d": 0,
            },
        )
        group["clicks_28d"] += int(row.get("clicks_28d") or 0)
        group["impressions_28d"] += int(row.get("impressions_28d") or 0)
        group["clicks_90d"] += int(row.get("clicks_90d") or 0)
        group["impressions_90d"] += int(row.get("impressions_90d") or 0)
        impressions_28d = max(int(row.get("impressions_28d") or 0), 1)
        impressions_90d = max(int(row.get("impressions_90d") or 0), 1)
        group["position_weighted_28d"] += float(row.get("avg_position_28d") or 0.0) * impressions_28d
        group["position_weighted_90d"] += float(row.get("avg_position_90d") or 0.0) * impressions_90d
        group["position_impressions_28d"] += impressions_28d
        group["position_impressions_90d"] += impressions_90d

    query_variants = []
    for variant in variant_groups.values():
        impressions_28d = max(int(variant["position_impressions_28d"] or 0), 1)
        impressions_90d = max(int(variant["position_impressions_90d"] or 0), 1)
        query_variants.append(
            {
                "query": variant["query"],
                "clicks_28d": int(variant["clicks_28d"]),
                "impressions_28d": int(variant["impressions_28d"]),
                "clicks_90d": int(variant["clicks_90d"]),
                "impressions_90d": int(variant["impressions_90d"]),
                "avg_position_28d": round(float(variant["position_weighted_28d"]) / impressions_28d, 2),
                "avg_position_90d": round(float(variant["position_weighted_90d"]) / impressions_90d, 2),
            }
        )
    query_variants.sort(
        key=lambda item: (
            -int(item.get("clicks_90d") or 0),
            -int(item.get("impressions_90d") or 0),
            float(item.get("avg_position_90d") or 999.0),
            item.get("query") or "",
        )
    )

    query_tokens = tokenize_intent_text_fn(query)
    resolved_signals = resolve_query_signal_context_fn(
        store_hash=None,
        example_query=query,
        signal_library=signal_library,
        source_profile=source_profile,
        target_profile=None,
    )
    raw_query_attrs = resolved_signals.get("query_attrs") or {}
    query_attrs = {bucket: set(values or []) for bucket, values in raw_query_attrs.items()}
    brand_signals = list(resolved_signals.get("brand_signals") or [])
    hard_signals = list(resolved_signals.get("hard_attribute_signals") or [])
    soft_signals = list(resolved_signals.get("soft_attribute_signals") or [])
    collection_signals = list(resolved_signals.get("collection_signals") or [])
    topic_signals = list(resolved_signals.get("topic_signals") or [])
    sku_signals = list(resolved_signals.get("sku_signals") or [])
    hard_attribute_signal = bool(hard_signals)
    soft_attribute_signal = bool(soft_signals)
    collection_signal = bool(collection_signals)
    has_model_signal = bool(sku_signals)
    query_brand_tokens = {
        token
        for signal in brand_signals
        for token in (signal.get("matched_tokens") or [])
    }

    query_intent_scope, preferred_entity_type = classify_query_intent_scope_fn(
        example_query=query,
        query_tokens=query_tokens,
        query_attrs=query_attrs,
        query_brand_tokens=query_brand_tokens,
        resolved_signals=resolved_signals,
    )
    semantics_analysis = build_query_semantics_analysis_fn(
        store_hash=store_hash,
        example_query=query,
        resolved_signals=resolved_signals,
        signal_library=signal_library,
    )
    semantic_query_scope = {
        "exact_product_like": "specific_product",
        "category_like": "broad_product_family",
        "attribute_refined_category": "commercial_topic",
        "brand_navigational": "brand_navigation",
        "broad_descriptive": "commercial_topic",
        "mixed_ambiguous": "mixed_or_unknown",
        "hold": "mixed_or_unknown",
    }.get(semantics_analysis.get("query_shape") or "", query_intent_scope)
    semantic_preferred_entity_type = {
        "exact_product_like": "product",
        "category_like": "category",
        "attribute_refined_category": "category",
        "brand_navigational": "brand",
        "broad_descriptive": "category",
        "mixed_ambiguous": "category",
        "hold": "category",
    }.get(semantics_analysis.get("query_shape") or "", preferred_entity_type)
    if semantic_preferred_entity_type == "category" and preferred_entity_type in {"product", "brand"}:
        query_intent_scope = semantic_query_scope
        preferred_entity_type = semantic_preferred_entity_type
    elif semantic_preferred_entity_type == "brand" and float(semantics_analysis.get("brand_confidence") or 0.0) >= 0.72:
        query_intent_scope = semantic_query_scope
        preferred_entity_type = semantic_preferred_entity_type
    elif semantic_preferred_entity_type == "product" and float(semantics_analysis.get("pdp_confidence") or 0.0) >= 0.72:
        query_intent_scope = semantic_query_scope
        preferred_entity_type = semantic_preferred_entity_type

    source_entity_type = (source_profile.get("entity_type") or "product").strip().lower()
    source_entity_id = int(source_profile.get("bc_entity_id") or 0)
    current_page_type = source_entity_type
    wrong_page_type = preferred_entity_type and current_page_type != preferred_entity_type
    if wrong_page_type:
        if preferred_entity_type == "product" and current_page_type == "category":
            page_mismatch_reason = "current landing page is too broad for the query"
        elif preferred_entity_type == "category" and current_page_type == "product":
            page_mismatch_reason = "current landing page is too narrow for the query"
        else:
            page_mismatch_reason = "current landing page type does not match query intent"
    else:
        page_mismatch_reason = ""

    clicks_28d = sum(int(row.get("clicks_28d") or 0) for row in evidence_rows)
    impressions_28d = sum(int(row.get("impressions_28d") or 0) for row in evidence_rows)
    clicks_90d = sum(int(row.get("clicks_90d") or 0) for row in evidence_rows)
    impressions_90d = sum(int(row.get("impressions_90d") or 0) for row in evidence_rows)
    ctr_28d = round((clicks_28d / impressions_28d) if impressions_28d else 0.0, 6)
    ctr_90d = round((clicks_90d / impressions_90d) if impressions_90d else 0.0, 6)

    avg_position_28d_num = sum(
        float(row.get("avg_position_28d") or 0.0) * max(int(row.get("impressions_28d") or 0), 1)
        for row in evidence_rows
    )
    avg_position_28d_den = sum(max(int(row.get("impressions_28d") or 0), 1) for row in evidence_rows if row.get("avg_position_28d") is not None)
    avg_position_28d = round((avg_position_28d_num / avg_position_28d_den) if avg_position_28d_den else 0.0, 4)

    avg_position_90d_num = sum(
        float(row.get("avg_position_90d") or 0.0) * max(int(row.get("impressions_90d") or 0), 1)
        for row in evidence_rows
    )
    avg_position_90d_den = sum(max(int(row.get("impressions_90d") or 0), 1) for row in evidence_rows if row.get("avg_position_90d") is not None)
    avg_position_90d = round((avg_position_90d_num / avg_position_90d_den) if avg_position_90d_den else 0.0, 4)
    routing_position = avg_position_28d or avg_position_90d or 999.0

    source_fuzzy_score = max(
        fuzzy_match_score_fn(query, source_profile.get("name")),
        fuzzy_match_score_fn(query, source_profile.get("url")),
    )

    demand_score = 0.0
    demand_score += min(math.log1p(max(impressions_90d, 0)) * 10.0, 60.0)
    demand_score += min(math.log1p(max(clicks_90d, 0)) * 12.0, 40.0)
    demand_score = max(0.0, min(100.0, round(demand_score, 2)))

    if routing_position > 25:
        opportunity_score = 92.0
    elif routing_position > 20:
        opportunity_score = 86.0
    elif routing_position > 15:
        opportunity_score = 78.0
    elif routing_position > 10:
        opportunity_score = 70.0
    else:
        opportunity_score = 36.0

    expected_ctr = expected_ctr_for_position_fn(routing_position)
    ctr_gap = max(expected_ctr - ctr_28d, 0.0)
    if expected_ctr and ctr_gap >= expected_ctr * 0.35:
        opportunity_score += min((ctr_gap / expected_ctr) * 10.0, 8.0)
    if wrong_page_type:
        opportunity_score = max(opportunity_score, 82.0)
    opportunity_score = max(0.0, min(100.0, round(opportunity_score, 2)))

    intent_clarity_score = {
        "specific_product": 82.0,
        "brand_navigation": 80.0,
        "informational": 78.0,
        "commercial_topic": 70.0,
        "broad_product_family": 68.0,
        "mixed_or_unknown": 44.0,
    }.get(query_intent_scope, 44.0)
    if brand_signals:
        intent_clarity_score += 10.0
    if has_model_signal:
        intent_clarity_score += 12.0
    if hard_attribute_signal:
        intent_clarity_score += 10.0
    elif soft_attribute_signal:
        intent_clarity_score += 6.0
    if collection_signal:
        intent_clarity_score += 8.0
    if topic_signals:
        intent_clarity_score += 4.0
    if source_fuzzy_score >= 82.0:
        intent_clarity_score += 8.0
    elif source_fuzzy_score >= 70.0:
        intent_clarity_score += 4.0
    intent_clarity_score = max(0.0, min(100.0, round(intent_clarity_score, 2)))

    noise_penalty = min(float(len(query_tokens & query_noise_words)) * 6.0, 24.0)
    if query_intent_scope == "mixed_or_unknown":
        noise_penalty += 12.0
    if len(query_tokens) <= 1 and not (brand_signals or has_model_signal or hard_attribute_signal or soft_attribute_signal or collection_signal):
        noise_penalty += 10.0
    if not topic_signals and not brand_signals and not has_model_signal and not hard_attribute_signal and not soft_attribute_signal and not collection_signal and query_intent_scope != "informational":
        noise_penalty += 8.0
    noise_penalty = max(0.0, min(100.0, round(noise_penalty, 2)))

    freshness_context = build_freshness_context_fn(clicks_28d, impressions_28d, clicks_90d, impressions_90d)
    current_page_preservation_guard = build_current_page_preservation_guard(
        query,
        source_profile,
        semantics_analysis,
        avg_position_90d,
        impressions_90d,
        tokenize_intent_text_fn=tokenize_intent_text_fn,
        query_noise_words=query_noise_words,
    )

    specific_override = bool(
        brand_signals
        or has_model_signal
        or hard_attribute_signal
        or collection_signal
        or source_fuzzy_score >= 82.0
    )
    strong_opportunity = opportunity_score >= 68.0
    strong_mismatch_override = bool(wrong_page_type and intent_clarity_score >= 60.0 and noise_penalty < 28.0)

    if noise_penalty >= 30.0 and not specific_override:
        disposition = "reject"
        gate_reason = "query is too noisy or ambiguous to route cleanly"
    elif demand_score < 12.0 and not specific_override:
        disposition = "reject"
        gate_reason = "query does not have enough demand to justify routing"
    elif routing_position <= 10.0 and not strong_mismatch_override:
        disposition = "hold"
        gate_reason = "query already ranks in the top 10, so routing stays on hold by default"
    elif (strong_opportunity or strong_mismatch_override) and intent_clarity_score >= 55.0 and noise_penalty < 28.0 and (demand_score >= 30.0 or specific_override):
        disposition = "pass"
        gate_reason = page_mismatch_reason if strong_mismatch_override and page_mismatch_reason else "rank position suggests routing upside"
    else:
        disposition = "hold"
        gate_reason = "query should be monitored until demand or clarity improves"

    reason_bits = [gate_reason]
    if routing_position:
        reason_bits.append(f"avg position {round(routing_position, 1)}")
    if query_intent_scope:
        reason_bits.append(f"{query_intent_scope.replace('_', ' ')}")
    if brand_signals:
        reason_bits.append("brand signal present")
    elif hard_attribute_signal:
        reason_bits.append("hard attribute signal present")
    elif collection_signal:
        reason_bits.append("collection signal present")
    elif source_fuzzy_score >= 82.0:
        reason_bits.append("strong source-page fuzzy match")

    return {
        "store_hash": store_hash,
        "normalized_query_key": family_key,
        "representative_query": query,
        "source_url": source_url,
        "source_name": source_profile.get("name") or dominant_row.get("source_name") or "",
        "source_entity_type": source_entity_type,
        "source_entity_id": source_entity_id,
        "current_page_type": current_page_type,
        "query_intent_scope": query_intent_scope,
        "preferred_entity_type": preferred_entity_type,
        "clicks_28d": clicks_28d,
        "impressions_28d": impressions_28d,
        "ctr_28d": ctr_28d,
        "avg_position_28d": avg_position_28d,
        "clicks_90d": clicks_90d,
        "impressions_90d": impressions_90d,
        "ctr_90d": ctr_90d,
        "avg_position_90d": avg_position_90d,
        "demand_score": demand_score,
        "opportunity_score": opportunity_score,
        "intent_clarity_score": intent_clarity_score,
        "noise_penalty": noise_penalty,
        "freshness_context": freshness_context,
        "disposition": disposition,
        "reason_summary": "; ".join(reason_bits[:3]),
        "metadata": {
            "current_page_type": current_page_type,
            "wrong_page_type": wrong_page_type,
            "page_mismatch_reason": page_mismatch_reason,
            "query_brand_tokens": sorted(query_brand_tokens),
            "query_tokens": sorted(query_tokens)[:8],
            "resolved_signals": resolved_signals,
            "semantics_analysis": semantics_analysis,
            "current_page_preservation_guard": current_page_preservation_guard,
            "source_fuzzy_score": round(source_fuzzy_score, 2),
            "specific_override": specific_override,
            "demand_floor_met": demand_score >= 30.0,
            "query_variants": query_variants[:8],
            "query_variant_count": len(query_variants),
        },
    }


__all__ = [
    "build_freshness_context",
    "build_current_page_preservation_guard",
    "build_query_gate_record",
    "current_page_gsc_trust_score",
    "expected_ctr_for_position",
]
