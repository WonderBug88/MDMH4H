"""Direct-route generation helpers for Fulcrum."""

from __future__ import annotations

from typing import Any, Callable


def looks_informational_query(
    query: str | None,
    *,
    tokenize_intent_text_fn: Callable[[str | None], set[str]],
) -> bool:
    tokens = tokenize_intent_text_fn(query)
    informational_tokens = {
        "how",
        "what",
        "why",
        "guide",
        "vs",
        "difference",
        "compare",
        "comparison",
        "clean",
        "care",
        "wash",
        "washing",
        "faq",
    }
    return bool(tokens & informational_tokens)


def entity_type_fit_adjustment(
    query: str | None,
    preferred_entity_type: str | None,
    target_entity_type: str,
    fuzzy_signal: dict[str, Any] | None = None,
    current_page: bool = False,
    source_query_topic_match_count: int = 0,
    has_brand_signal: bool = False,
    has_collection_signal: bool = False,
    has_sku_signal: bool = False,
    *,
    looks_informational_query_fn: Callable[[str | None], bool],
) -> tuple[float, str | None]:
    preferred = (preferred_entity_type or "").strip().lower()
    target_type = (target_entity_type or "").strip().lower()
    fuzzy_signal = fuzzy_signal or {}
    fuzzy_kind = (fuzzy_signal.get("matched_kind") or "").strip().lower()
    fuzzy_score = float(fuzzy_signal.get("score") or 0.0)
    informational = looks_informational_query_fn(query)

    if informational and target_type == "content":
        return 18.0, "informational query fits content"
    if informational and target_type == "product":
        return -12.0, "informational query is too narrow for a product page"

    if preferred == "brand" and target_type == "brand":
        return 18.0, "brand-led query fits a brand page"
    if preferred == "brand" and target_type in {"product", "category", "content"}:
        return -18.0, "brand-led query leans away from non-brand pages"
    if preferred and preferred == target_type:
        return 16.0, "page type matches query intent"
    if target_type == "brand" and fuzzy_kind == "brand":
        return 18.0, "brand page aligns with the query"
    if target_type == "product" and fuzzy_kind in {"title", "collection"} and fuzzy_score >= 78.0:
        return 12.0, "product title/collection strongly matches the query"
    if preferred == "category" and target_type == "product" and fuzzy_score < 78.0:
        return -10.0, "broad query leans away from a specific product"
    if (
        preferred == "product"
        and target_type == "category"
        and current_page
        and source_query_topic_match_count >= 2
        and fuzzy_score >= 55.0
        and not (has_brand_signal or has_collection_signal or has_sku_signal)
    ):
        return 4.0, "current category strongly preserves the query topic"
    if preferred == "product" and target_type in {"category", "brand", "content"}:
        return -12.0, "query leans toward a product page"
    return 0.0, None


def target_prefilter(
    query: str,
    target_profile: dict[str, Any],
    *,
    tokenize_intent_text_fn: Callable[[str | None], set[str]],
    extract_attribute_terms_fn: Callable[[str | None], dict[str, set[str]]],
    fuzzy_match_score_fn: Callable[[str | None, str | None], float],
    generic_routing_tokens: set[str],
) -> bool:
    query_tokens = tokenize_intent_text_fn(query)
    target_tokens = set(target_profile.get("tokens") or set())
    if (query_tokens & target_tokens) - generic_routing_tokens:
        return True

    query_attrs = extract_attribute_terms_fn(query)
    target_attrs = dict(target_profile.get("attributes") or {})
    for bucket in ("size", "color", "material", "form", "pack_size"):
        if set(query_attrs.get(bucket) or set()) & set(target_attrs.get(bucket) or set()):
            return True

    query_brand_tokens = query_tokens & tokenize_intent_text_fn(target_profile.get("brand_name"))
    if query_brand_tokens:
        return True

    fuzzy_name = fuzzy_match_score_fn(query, target_profile.get("name"))
    fuzzy_url = fuzzy_match_score_fn(query, target_profile.get("url"))
    return max(fuzzy_name, fuzzy_url) >= 70.0


def direct_route_candidates_from_gsc(
    store_hash: str,
    cluster: str | None = None,
    min_hit_count: int = 3,
    limit_total: int = 300,
    entity_index: dict[str, Any] | None = None,
    gate_rows: list[dict[str, Any]] | None = None,
    *,
    build_unified_entity_index_fn: Callable[[str, str | None], dict[str, Any]],
    load_query_target_overrides_fn: Callable[[str], dict[tuple[str, str], dict[str, Any]]],
    load_review_feedback_maps_fn: Callable[[str], dict[str, dict[Any, dict[str, int]]]],
    build_query_gate_records_fn: Callable[..., list[dict[str, Any]]],
    normalize_storefront_path_fn: Callable[[Any], str],
    entity_storage_id_fn: Callable[[str | None, int | None], int | None],
    gate_row_query_signal_context_fn: Callable[[dict[str, Any]], dict[str, Any] | None],
    query_target_override_key_fn: Callable[[str | None, str | None], tuple[str, str]],
    target_prefilter_fn: Callable[[str, dict[str, Any]], bool],
    build_intent_profile_fn: Callable[..., dict[str, Any]],
    build_review_feedback_signal_fn: Callable[..., dict[str, Any]],
    entity_type_fit_adjustment_fn: Callable[..., tuple[float, str | None]],
    append_reason_summary_fn: Callable[[str | None, str | None], str],
) -> list[dict[str, Any]]:
    entity_index = entity_index or build_unified_entity_index_fn(store_hash, cluster)
    source_profiles = entity_index["sources"]
    target_entities = entity_index["targets"]
    target_entities_by_key = {
        ((target.get("entity_type") or "product").strip().lower(), int(target.get("bc_entity_id") or 0)): target
        for target in target_entities
        if int(target.get("bc_entity_id") or 0)
    }
    overrides = load_query_target_overrides_fn(store_hash)
    review_feedback_maps = load_review_feedback_maps_fn(store_hash)
    if gate_rows is None:
        gate_rows = build_query_gate_records_fn(
            store_hash=store_hash,
            source_profiles=source_profiles,
            target_entities=target_entities,
            min_hit_count=min_hit_count,
            limit_total=limit_total,
        )

    best_rows: dict[tuple[str, int, str, int], dict[str, Any]] = {}
    for gate_row in gate_rows:
        if (gate_row.get("disposition") or "hold") != "pass":
            continue

        source_url = normalize_storefront_path_fn(gate_row.get("source_url"))
        source_profile = source_profiles.get(source_url)
        if not source_profile:
            continue
        source_entity_type = source_profile.get("entity_type") or "product"
        if source_entity_type not in {"product", "category"}:
            continue

        source_bc_id = int(source_profile.get("bc_entity_id") or 0)
        source_storage_id = entity_storage_id_fn(source_entity_type, source_bc_id)
        if source_storage_id is None:
            continue

        query = gate_row.get("representative_query") or ""
        query_signal_context = gate_row_query_signal_context_fn(gate_row)
        used_labels: set[str] = set()
        gate_metadata = dict(gate_row.get("metadata") or {})
        controlled_target = gate_row.get("suggested_target") or dict(gate_metadata.get("suggested_target_snapshot") or {}) or None
        override = overrides.get(query_target_override_key_fn(gate_row.get("normalized_query_key"), source_url))
        controlled_target_key = (
            ((controlled_target or {}).get("entity_type") or "").strip().lower(),
            int((controlled_target or {}).get("entity_id") or 0),
        )
        current_target_key = (source_entity_type, source_bc_id)
        if controlled_target_key[0] and controlled_target_key[1]:
            if controlled_target_key == current_target_key:
                continue
            target_iterable = [target_entities_by_key[controlled_target_key]] if controlled_target_key in target_entities_by_key else []
        elif override:
            override_key = (
                (override.get("target_entity_type") or "product").strip().lower(),
                int(override.get("target_entity_id") or 0),
            )
            target_iterable = [target_entities_by_key[override_key]] if override_key in target_entities_by_key else []
        else:
            target_iterable = target_entities

        for target_profile in target_iterable:
            target_entity_type = target_profile.get("entity_type") or "product"
            if source_entity_type == "category" and target_entity_type == "content":
                continue
            target_bc_id = int(target_profile.get("bc_entity_id") or 0)
            if not target_bc_id:
                continue
            if source_entity_type == target_entity_type and source_bc_id == target_bc_id:
                continue
            if not target_prefilter_fn(query, target_profile):
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
                used_labels=used_labels,
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
                has_brand_signal=bool((profile.get("query_signals") or {}).get("brand_signals")),
                has_collection_signal=bool((profile.get("query_signals") or {}).get("collection_signals")),
                has_sku_signal=bool((profile.get("query_signals") or {}).get("sku_signals")),
            )
            review_feedback_delta = float(review_feedback_signal.get("delta") or 0.0)
            final_score = max(0.0, min(100.0, round(float(profile["score"]) + type_fit_delta + review_feedback_delta, 2)))
            target_storage_id = entity_storage_id_fn(target_entity_type, target_bc_id)
            if target_storage_id is None:
                continue
            row = {
                "source_product_id": source_storage_id,
                "source_name": source_profile.get("name"),
                "source_url": source_profile.get("url"),
                "target_product_id": target_storage_id,
                "target_name": target_profile.get("name"),
                "target_url": target_profile.get("url"),
                "relation_type": relation_type,
                "example_query": query,
                "anchor_label": profile["anchor_label"],
                "hit_count": int(gate_row.get("clicks_90d") or gate_row.get("impressions_90d") or 0),
                "score": final_score,
                "source_entity_type": source_entity_type,
                "target_entity_type": target_entity_type,
                "metadata": {
                    "routing_mode": "direct_gsc_entity_router",
                    "source_entity_type": source_entity_type,
                    "target_entity_type": target_entity_type,
                    "source_bc_entity_id": source_bc_id,
                    "target_bc_entity_id": target_bc_id,
                    "topic_key": profile["topic_key"],
                    "anchor_label_source": profile["anchor_label_source"],
                    "anchor_quality": profile["anchor_quality"],
                    "reason_summary": append_reason_summary_fn(
                        profile["reason_summary"],
                        review_feedback_signal.get("summary"),
                    ),
                    "reasons": [
                        *(profile.get("reasons") or []),
                        *([type_fit_reason] if type_fit_reason else []),
                        *([review_feedback_signal.get("reason")] if review_feedback_signal.get("active") else []),
                    ][:6],
                    "shared_tokens": profile["shared_tokens"],
                    "query_target_tokens": profile["query_target_tokens"],
                    "query_source_tokens": profile["query_source_tokens"],
                    "attributes": profile["attributes"],
                    "fuzzy_signal": profile.get("fuzzy_signal") or {},
                    "ga4_signal": profile.get("ga4_signal") or {},
                    "review_feedback_signal": review_feedback_signal,
                    "source_primary_cluster": profile["source_primary_cluster"],
                    "target_primary_cluster": profile["target_primary_cluster"],
                    "query_intent_scope": profile["query_intent_scope"],
                    "preferred_entity_type": profile["preferred_entity_type"],
                    "gsc_clicks_28d": int(gate_row.get("clicks_28d") or 0),
                    "gsc_impressions_28d": int(gate_row.get("impressions_28d") or 0),
                    "gsc_ctr_28d": float(gate_row.get("ctr_28d") or 0.0),
                    "gsc_avg_position_28d": float(gate_row.get("avg_position_28d") or 0.0),
                    "gsc_clicks_90d": int(gate_row.get("clicks_90d") or 0),
                    "gsc_impressions_90d": int(gate_row.get("impressions_90d") or 0),
                    "gsc_ctr_90d": float(gate_row.get("ctr_90d") or 0.0),
                    "gsc_avg_position_90d": float(gate_row.get("avg_position_90d") or 0.0),
                    "gate_disposition": gate_row.get("disposition"),
                    "gate_reason_summary": gate_row.get("reason_summary"),
                    "manual_target_override": bool(override),
                    "gate_scores": {
                        "demand_score": gate_row.get("demand_score"),
                        "opportunity_score": gate_row.get("opportunity_score"),
                        "intent_clarity_score": gate_row.get("intent_clarity_score"),
                        "noise_penalty": gate_row.get("noise_penalty"),
                    },
                    "freshness_context": gate_row.get("freshness_context") or {},
                    "semantics_analysis": gate_metadata.get("semantics_analysis") or {},
                },
            }

            key = (
                source_entity_type,
                int(source_storage_id or 0),
                target_entity_type,
                int(target_storage_id or 0),
            )
            existing = best_rows.get(key)
            if not existing or float(row["score"]) > float(existing["score"]):
                best_rows[key] = row
                used_labels.add((row.get("anchor_label") or "").strip().lower())

    rows = list(best_rows.values())
    rows.sort(key=lambda item: (-float(item["score"]), item["source_product_id"], item["target_product_id"]))
    return rows[:limit_total]


__all__ = [
    "direct_route_candidates_from_gsc",
    "entity_type_fit_adjustment",
    "looks_informational_query",
    "target_prefilter",
]
