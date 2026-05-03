"""Routing target ranking helpers for Fulcrum."""

from __future__ import annotations

import math
from typing import Any, Callable


def current_page_gsc_trust_score(gate_row: dict[str, Any]) -> float:
    avg_position_90d = float(gate_row.get("avg_position_90d") or 0.0)
    impressions_90d = max(int(gate_row.get("impressions_90d") or 0), 0)
    position_score = max(0.0, min(1.0, (15.0 - avg_position_90d) / 15.0))
    impression_score = max(0.0, min(1.0, math.log10(impressions_90d + 1) / 3.0))
    return round((0.55 * position_score) + (0.45 * impression_score), 4)


def build_review_feedback_signal(
    query: str | None,
    source_entity_type: str,
    source_entity_id: int | None,
    target_entity_type: str,
    target_entity_id: int | None,
    *,
    feedback_maps: dict[str, dict[Any, dict[str, int]]] | None = None,
    normalize_query_family_key_fn: Callable[[str | None], str],
) -> dict[str, Any]:
    feedback_maps = feedback_maps or {}
    source_type = (source_entity_type or "product").strip().lower() or "product"
    target_type = (target_entity_type or "product").strip().lower() or "product"
    source_id = int(source_entity_id or 0)
    target_id = int(target_entity_id or 0)
    family_key = normalize_query_family_key_fn(query)
    if not source_id or not target_id:
        return {"active": False, "delta": 0.0, "reason": "", "summary": "", "metrics": {}}

    pair_bucket = dict((feedback_maps.get("pair") or {}).get((source_type, source_id, target_type, target_id)) or {})
    family_bucket = dict((feedback_maps.get("family_target") or {}).get((family_key, target_type, target_id)) or {})
    target_bucket = dict((feedback_maps.get("target") or {}).get((target_type, target_id)) or {})

    pair_approved = int(pair_bucket.get("approved_count") or 0)
    pair_rejected = int(pair_bucket.get("rejected_count") or 0)
    family_approved = int(family_bucket.get("approved_count") or 0)
    family_rejected = int(family_bucket.get("rejected_count") or 0)
    target_approved = int(target_bucket.get("approved_count") or 0)
    target_rejected = int(target_bucket.get("rejected_count") or 0)

    if not any((pair_approved, pair_rejected, family_approved, family_rejected, target_approved, target_rejected)):
        return {"active": False, "delta": 0.0, "reason": "", "summary": "", "metrics": {}}

    pair_delta = min(pair_approved * 2.5, 10.0) - min(pair_rejected * 3.0, 12.0)
    family_delta = min(family_approved * 0.75, 3.0) - min(family_rejected * 1.25, 3.75)
    target_delta = min(target_approved * 0.15, 1.0) - min(target_rejected * 0.25, 1.5)
    delta = max(-10.0, min(10.0, round(pair_delta + family_delta + target_delta, 2)))

    reason = ""
    summary = ""
    if delta > 0:
        if pair_approved > pair_rejected and pair_approved:
            reason = "past approvals rewarded this exact connection"
            summary = "Past approvals support this connection"
        elif family_approved > family_rejected and family_approved:
            reason = "past approvals rewarded this target for similar queries"
            summary = "Past approvals support this target"
        elif target_approved > target_rejected and target_approved:
            reason = "past approvals slightly rewarded this target"
            summary = "Past approvals slightly support this target"
    elif delta < 0:
        if pair_rejected >= pair_approved and pair_rejected:
            reason = "past rejections weakened this exact connection"
            summary = "Past rejections weaken this connection"
        elif family_rejected >= family_approved and family_rejected:
            reason = "past rejections weakened this target for similar queries"
            summary = "Past rejections weaken this target"
        elif target_rejected >= target_approved and target_rejected:
            reason = "past rejections slightly weakened this target"
            summary = "Past rejections slightly weaken this target"

    return {
        "active": abs(delta) >= 0.5,
        "delta": delta,
        "reason": reason,
        "summary": summary,
        "metrics": {
            "pair": {"approved_count": pair_approved, "rejected_count": pair_rejected},
            "query_family_target": {"approved_count": family_approved, "rejected_count": family_rejected},
            "target": {"approved_count": target_approved, "rejected_count": target_rejected},
        },
    }


def append_reason_summary(base_summary: str | None, extra_summary: str | None) -> str:
    base = (base_summary or "").strip()
    extra = (extra_summary or "").strip()
    if not extra:
        return base
    if not base:
        return extra
    if extra.lower() in base.lower():
        return base
    return f"{base}; {extra}"


def rank_target_options_for_gate_row(
    gate_row: dict[str, Any],
    source_profiles: dict[str, dict[str, Any]],
    target_entities: list[dict[str, Any]],
    overrides: dict[tuple[str, str], dict[str, Any]] | None = None,
    review_feedback_maps: dict[str, dict[Any, dict[str, int]]] | None = None,
    limit: int = 2,
    *,
    apply_semantics_control: bool = True,
    semantics_analysis: dict[str, Any] | None = None,
    target_entities_by_key: dict[tuple[str, int], dict[str, Any]] | None = None,
    normalize_storefront_path_fn: Callable[[Any], str],
    gate_row_query_signal_context_fn: Callable[[dict[str, Any]], dict[str, Any] | None],
    tokenize_intent_text_fn: Callable[[str | None], set[str]],
    gate_row_semantics_analysis_fn: Callable[..., dict[str, Any]],
    query_target_override_key_fn: Callable[[str | None, str | None], tuple[str, str]],
    semantics_target_block_reason_fn: Callable[[dict[str, Any], dict[str, Any] | None], str | None],
    target_prefilter_fn: Callable[[str, dict[str, Any]], bool],
    build_intent_profile_fn: Callable[..., dict[str, Any]],
    build_review_feedback_signal_fn: Callable[..., dict[str, Any]],
    entity_type_fit_adjustment_fn: Callable[..., tuple[float, str]],
    append_reason_summary_fn: Callable[[str | None, str | None], str],
    apply_semantics_control_to_ranked_targets_fn: Callable[..., tuple[list[dict[str, Any]], dict[str, Any]]],
) -> list[dict[str, Any]]:
    source_url = normalize_storefront_path_fn(gate_row.get("source_url"))
    source_profile = source_profiles.get(source_url)
    if not source_profile:
        return []

    source_entity_type = source_profile.get("entity_type") or gate_row.get("source_entity_type") or "product"
    source_bc_id = int(source_profile.get("bc_entity_id") or gate_row.get("source_entity_id") or 0)
    query = (gate_row.get("representative_query") or "").strip()
    if not query:
        return []
    query_signal_context = gate_row_query_signal_context_fn(gate_row)
    query_tokens = tokenize_intent_text_fn(query)
    has_brand_signal = bool((query_signal_context or {}).get("brand_signals"))
    has_collection_signal = bool((query_signal_context or {}).get("collection_signals"))
    has_sku_signal = bool((query_signal_context or {}).get("sku_signals"))
    broad_hotel_query = "hotel" in query_tokens
    narrowing_tokens = {"fabric", "vinyl", "hookless", "laminated", "polyester", "blackout", "thermal"}
    preferred_entity_type = (gate_row.get("preferred_entity_type") or "").strip().lower()
    query_intent_scope = (gate_row.get("query_intent_scope") or "").strip().lower()
    content_allowed = preferred_entity_type == "content" or query_intent_scope == "informational"
    target_entities_by_key = target_entities_by_key or {
        ((target.get("entity_type") or "product").strip().lower(), int(target.get("bc_entity_id") or 0)): target
        for target in target_entities
        if int(target.get("bc_entity_id") or 0)
    }
    semantics_analysis = semantics_analysis or gate_row_semantics_analysis_fn(
        gate_row,
        source_profile.get("store_hash") or gate_row.get("store_hash") or "",
        signal_library=None,
    )
    override = (overrides or {}).get(query_target_override_key_fn(gate_row.get("normalized_query_key"), source_url))

    ranked_matches: list[dict[str, Any]] = []
    for target_profile in target_entities:
        target_entity_type = target_profile.get("entity_type") or "product"
        target_bc_id = int(target_profile.get("bc_entity_id") or 0)
        if not target_bc_id:
            continue
        is_manual_override_target = bool(
            override
            and (override.get("target_entity_type") or "product") == target_entity_type
            and int(override.get("target_entity_id") or 0) == target_bc_id
        )
        if target_entity_type == "content" and not content_allowed and not is_manual_override_target:
            continue
        if not is_manual_override_target:
            block_reason = semantics_target_block_reason_fn(semantics_analysis, target_profile)
            if block_reason:
                continue
        if not is_manual_override_target and target_profile.get("url") != source_profile.get("url") and not target_prefilter_fn(query, target_profile):
            continue

        relation_type = target_entity_type if target_entity_type in {"category", "brand"} else "product"
        profile = build_intent_profile_fn(
            source_name=source_profile.get("name"),
            source_url=source_profile.get("url"),
            target_name=target_profile.get("name"),
            target_url=target_profile.get("url"),
            example_query=query,
            relation_type=relation_type,
            hit_count=int(gate_row.get("clicks_90d") or gate_row.get("impressions_90d") or 0),
            source_profile=source_profile,
            target_profile=target_profile,
            used_labels=set(),
            query_signal_context=query_signal_context,
        )
        if not profile.get("passes"):
            continue

        review_feedback_signal = build_review_feedback_signal_fn(
            query=query,
            source_entity_type=source_entity_type,
            source_entity_id=source_bc_id,
            target_entity_type=target_entity_type,
            target_entity_id=target_bc_id,
            feedback_maps=review_feedback_maps,
        )
        type_fit_delta, type_fit_reason = entity_type_fit_adjustment_fn(
            query=query,
            preferred_entity_type=profile.get("preferred_entity_type"),
            target_entity_type=target_entity_type,
            fuzzy_signal=profile.get("fuzzy_signal"),
            current_page=bool(source_entity_type == target_entity_type and source_bc_id == target_bc_id),
            source_query_topic_match_count=int(profile.get("source_query_topic_match_count") or 0),
            has_brand_signal=has_brand_signal,
            has_collection_signal=has_collection_signal,
            has_sku_signal=has_sku_signal,
        )
        review_feedback_delta = float(review_feedback_signal.get("delta") or 0.0)
        raw_final_score = round(float(profile.get("raw_score") or profile.get("score") or 0.0) + type_fit_delta + review_feedback_delta, 2)
        final_score = max(0.0, min(100.0, raw_final_score))
        name_url_tokens = tokenize_intent_text_fn(f"{target_profile.get('name') or ''} {target_profile.get('url') or ''}")
        target_name_tokens = tokenize_intent_text_fn(target_profile.get("name"))
        exact_brand_name_match = int(
            query_intent_scope == "brand_navigation"
            and target_entity_type == "brand"
            and bool(query_tokens)
            and target_name_tokens == query_tokens
        )
        hotel_name_match = 1 if broad_hotel_query and "hotel" in name_url_tokens else 0
        narrowing_penalty = 1 if broad_hotel_query and (name_url_tokens & narrowing_tokens) and not (query_tokens & narrowing_tokens) else 0
        score_for_sort = raw_final_score if query_intent_scope in {"specific_product", "brand_navigation"} else final_score
        candidate = {
            "entity_type": target_entity_type,
            "entity_id": target_bc_id,
            "name": target_profile.get("name"),
            "url": target_profile.get("url"),
            "score": max(final_score, 100.0) if is_manual_override_target else final_score,
            "raw_score": max(raw_final_score, 100.0) if is_manual_override_target else raw_final_score,
            "anchor_label": profile.get("anchor_label"),
            "reason_summary": append_reason_summary_fn(profile.get("reason_summary"), review_feedback_signal.get("summary")),
            "type_fit_reason": type_fit_reason,
            "fuzzy_signal": profile.get("fuzzy_signal") or {},
            "review_feedback_signal": review_feedback_signal,
            "is_current_page": source_entity_type == target_entity_type and source_bc_id == target_bc_id,
            "manual_override": is_manual_override_target,
            "exact_brand_name_match": bool(exact_brand_name_match),
            "name_url_overlap": len(query_tokens & name_url_tokens),
            "hotel_name_match": hotel_name_match,
            "narrowing_penalty": narrowing_penalty,
            "source_query_topic_match_count": int(profile.get("source_query_topic_match_count") or 0),
            "source_query_topic_missing_count": int(profile.get("source_query_topic_missing_count") or 0),
            "source_query_modifier_match_count": int(profile.get("source_query_modifier_match_count") or 0),
            "source_query_modifier_missing_count": int(profile.get("source_query_modifier_missing_count") or 0),
            "_sort_key": (
                1 if is_manual_override_target else 0,
                exact_brand_name_match,
                float(max(score_for_sort, 100.0) if is_manual_override_target else score_for_sort),
                int(profile.get("source_query_modifier_match_count") or 0),
                -int(profile.get("source_query_modifier_missing_count") or 0),
                int(profile.get("source_query_topic_match_count") or 0),
                -int(profile.get("source_query_topic_missing_count") or 0),
                int(hotel_name_match),
                -int(narrowing_penalty),
                int(source_entity_type == target_entity_type and source_bc_id == target_bc_id),
                int(len(query_tokens & name_url_tokens)),
                float((profile.get("fuzzy_signal") or {}).get("score") or 0.0),
            ),
        }
        ranked_matches.append(candidate)

    if query_intent_scope == "specific_product" and not (has_brand_signal or has_collection_signal or has_sku_signal):
        for candidate in ranked_matches:
            current_category_preserves_specific_query = (
                bool(candidate.get("is_current_page"))
                and candidate.get("entity_type") == "category"
                and int(candidate.get("source_query_topic_missing_count") or 0) == 0
                and int(candidate.get("source_query_modifier_missing_count") or 0) == 0
                and int(candidate.get("source_query_topic_match_count") or 0) >= 2
            )
            candidate["_sort_key"] = (
                candidate["_sort_key"][0],
                1 if current_category_preserves_specific_query else 0,
                *candidate["_sort_key"][1:],
            )

    if query_intent_scope in {"broad_product_family", "commercial_topic"}:
        current_page_trust_score = current_page_gsc_trust_score(gate_row)
        strong_current_page_alignment = bool(
            current_page_trust_score >= 0.55
            and float(gate_row.get("avg_position_90d") or 0.0) > 0.0
            and float(gate_row.get("avg_position_90d") or 0.0) <= 12.0
            and int(gate_row.get("impressions_90d") or 0) >= 100
        )
        for candidate in ranked_matches:
            category_preserves_full_query = (
                candidate.get("entity_type") == "category"
                and int(candidate.get("source_query_topic_missing_count") or 0) == 0
                and int(candidate.get("source_query_modifier_missing_count") or 0) == 0
                and int(candidate.get("source_query_topic_match_count") or 0) >= 1
            )
            current_page_preservation_guard = (
                strong_current_page_alignment
                and bool(candidate.get("is_current_page"))
                and int(candidate.get("source_query_topic_match_count") or 0) >= 1
                and int(candidate.get("source_query_topic_missing_count") or 0) == 0
                and int(candidate.get("source_query_modifier_match_count") or 0) >= 1
                and int(candidate.get("source_query_modifier_missing_count") or 0) == 0
            )
            current_page_preserves_modifier = (
                bool(candidate.get("is_current_page"))
                and int(candidate.get("source_query_modifier_match_count") or 0) >= 1
                and int(candidate.get("source_query_modifier_missing_count") or 0) == 0
            )
            candidate["_sort_key"] = (
                candidate["_sort_key"][0],
                1 if current_page_preservation_guard else 0,
                1 if category_preserves_full_query else 0,
                1 if current_page_preserves_modifier else 0,
                *candidate["_sort_key"][1:],
            )

    ranked_matches.sort(key=lambda candidate: candidate["_sort_key"], reverse=True)

    unique_matches: list[dict[str, Any]] = []
    seen_targets: set[tuple[str, int]] = set()
    for candidate in ranked_matches:
        target_key = (candidate["entity_type"], int(candidate["entity_id"]))
        if target_key in seen_targets:
            continue
        seen_targets.add(target_key)
        candidate.pop("_sort_key", None)
        unique_matches.append(candidate)
        if len(unique_matches) >= max(1, int(limit or 1)):
            break

    metadata = dict(gate_row.get("metadata") or {})
    metadata["semantics_analysis"] = semantics_analysis
    gate_row["metadata"] = metadata
    if not apply_semantics_control:
        return unique_matches

    controlled_matches, controlled_semantics = apply_semantics_control_to_ranked_targets_fn(
        gate_row,
        unique_matches,
        store_hash=source_profile.get("store_hash") or gate_row.get("store_hash") or "",
        source_profile=source_profile,
        target_entities_by_key=target_entities_by_key,
        source_profiles=source_profiles,
        target_entities=target_entities,
        overrides=overrides,
        review_feedback_maps=review_feedback_maps,
        signal_library=None,
    )
    metadata["semantics_analysis"] = controlled_semantics
    gate_row["metadata"] = metadata
    return controlled_matches[: max(1, int(limit or 1))]


def refresh_query_gate_row_live_state(
    store_hash: str,
    gate_row: dict[str, Any],
    source_profiles: dict[str, dict[str, Any]],
    target_entities: list[dict[str, Any]],
    signal_library: dict[str, Any] | None = None,
    *,
    normalize_storefront_path_fn: Callable[[Any], str],
    build_query_gate_record_fn: Callable[..., dict[str, Any] | None],
    build_store_signal_library_fn: Callable[[str], dict[str, Any]],
) -> dict[str, Any]:
    refreshed_row = dict(gate_row)
    metadata = dict(refreshed_row.get("metadata") or {})
    evidence_rows = [dict(item) for item in (metadata.get("query_variants") or [])]
    family_key = (refreshed_row.get("normalized_query_key") or "").strip()
    representative_query = (refreshed_row.get("representative_query") or family_key).strip()
    if not family_key or not representative_query or not evidence_rows:
        return refreshed_row
    source_url = normalize_storefront_path_fn(refreshed_row.get("source_url"))
    for evidence_row in evidence_rows:
        evidence_row.setdefault("source_url", source_url)

    rebuilt = build_query_gate_record_fn(
        store_hash=store_hash,
        family_key=family_key,
        representative_query=representative_query,
        evidence_rows=evidence_rows,
        source_profiles=source_profiles,
        target_entities=target_entities,
        signal_library=signal_library or build_store_signal_library_fn(store_hash),
    )
    if not rebuilt:
        return refreshed_row

    for key in (
        "current_page_type",
        "query_intent_scope",
        "preferred_entity_type",
        "clicks_28d",
        "impressions_28d",
        "ctr_28d",
        "avg_position_28d",
        "clicks_90d",
        "impressions_90d",
        "ctr_90d",
        "avg_position_90d",
        "demand_score",
        "opportunity_score",
        "intent_clarity_score",
        "noise_penalty",
        "freshness_context",
        "disposition",
        "reason_summary",
    ):
        refreshed_row[key] = rebuilt.get(key)

    rebuilt_metadata = dict(rebuilt.get("metadata") or {})
    metadata.update(rebuilt_metadata)
    refreshed_row["metadata"] = metadata
    return refreshed_row


__all__ = [
    "append_reason_summary",
    "build_review_feedback_signal",
    "current_page_gsc_trust_score",
    "rank_target_options_for_gate_row",
    "refresh_query_gate_row_live_state",
]
