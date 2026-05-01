"""Intent-profile scoring helpers for Fulcrum."""

from __future__ import annotations

import math
from typing import Any, Callable


def build_intent_profile(
    source_name: str | None,
    source_url: str | None,
    target_name: str | None,
    target_url: str,
    example_query: str | None,
    relation_type: str,
    hit_count: int,
    source_profile: dict[str, Any] | None = None,
    target_profile: dict[str, Any] | None = None,
    used_labels: set[str] | None = None,
    query_signal_context: dict[str, Any] | None = None,
    *,
    tokenize_intent_text_fn: Callable[[str | None], set[str]],
    extract_attribute_terms_fn: Callable[[str | None], dict[str, set[str]]],
    resolve_query_signal_context_fn: Callable[..., dict[str, Any]],
    build_fuzzy_signal_fn: Callable[..., dict[str, Any]],
    classify_query_intent_scope_fn: Callable[..., tuple[str, str]],
    select_anchor_label_fn: Callable[..., dict[str, Any]],
    build_ga4_signal_fn: Callable[..., dict[str, Any]],
    is_replacement_or_accessory_target_fn: Callable[[set[str], set[str], str | None], bool],
    attribute_sets_to_list_fn: Callable[[dict[str, set[str]]], dict[str, list[str]]],
    topic_priority: set[str],
    topic_display_map: dict[str, str],
    form_family_tokens: set[str],
    generic_routing_tokens: set[str],
    query_noise_words: set[str],
    intent_stopwords: set[str],
    context_keep_tokens: set[str],
    narrow_accessory_target_tokens: set[str],
    replacement_intent_tokens: set[str],
) -> dict[str, Any]:
    source_tokens = tokenize_intent_text_fn(f"{source_name or ''} {source_url or ''}")
    target_tokens = tokenize_intent_text_fn(f"{target_name or ''} {target_url or ''}")
    query_tokens = tokenize_intent_text_fn(example_query)
    target_entity_type = ((target_profile or {}).get("entity_type") or "").strip().lower() or (
        relation_type if relation_type in {"category", "brand", "content"} else "product"
    )
    source_attrs = dict(source_profile.get("attributes") or {}) if source_profile else extract_attribute_terms_fn(
        f"{source_name or ''} {source_url or ''}"
    )
    target_attrs = dict(target_profile.get("attributes") or {}) if target_profile else extract_attribute_terms_fn(
        f"{target_name or ''} {target_url or ''}"
    )
    resolved_signals = query_signal_context or resolve_query_signal_context_fn(
        store_hash=None,
        example_query=example_query,
        signal_library=None,
        source_profile=source_profile,
        target_profile=target_profile,
    )
    raw_query_attrs = resolved_signals.get("query_attrs") or {}
    query_attrs = {bucket: set(values or []) for bucket, values in raw_query_attrs.items()}
    fuzzy_signal = build_fuzzy_signal_fn(
        example_query=example_query,
        target_name=target_name,
        target_url=target_url,
        target_profile=target_profile,
    )
    source_brand_tokens = tokenize_intent_text_fn((source_profile or {}).get("brand_name"))
    target_brand_tokens = tokenize_intent_text_fn((target_profile or {}).get("brand_name"))
    brand_signals = list(resolved_signals.get("brand_signals") or [])
    hard_signals = list(resolved_signals.get("hard_attribute_signals") or [])
    soft_signals = list(resolved_signals.get("soft_attribute_signals") or [])
    collection_signals = list(resolved_signals.get("collection_signals") or [])
    topic_signals = list(resolved_signals.get("topic_signals") or [])
    sku_signals = list(resolved_signals.get("sku_signals") or [])
    query_brand_tokens = {
        token
        for signal in brand_signals
        for token in (signal.get("matched_tokens") or [])
    }
    query_intent_scope, preferred_entity_type = classify_query_intent_scope_fn(
        example_query=example_query,
        query_tokens=query_tokens,
        query_attrs=query_attrs,
        query_brand_tokens=query_brand_tokens,
        resolved_signals=resolved_signals,
    )
    if (
        preferred_entity_type == "category"
        and target_entity_type == "product"
        and float(fuzzy_signal.get("score") or 0.0) >= 78.0
        and fuzzy_signal.get("matched_kind") in {"title", "collection", "brand"}
    ):
        query_intent_scope = "specific_product"
        preferred_entity_type = "product"

    if source_profile:
        source_tokens |= set(source_profile.get("tokens") or set())
    if target_profile:
        target_tokens |= set(target_profile.get("tokens") or set())

    shared_tokens = sorted(source_tokens & target_tokens)
    query_target_tokens = sorted(query_tokens & target_tokens)
    query_source_tokens = sorted(query_tokens & source_tokens)
    matched_topic_tokens = {
        token
        for signal in topic_signals
        for token in (signal.get("matched_tokens") or [])
    }
    topic_tokens = [token for token in shared_tokens if token in topic_priority or token in matched_topic_tokens]
    topic_key = topic_tokens[0] if topic_tokens else (shared_tokens[0] if shared_tokens else "")

    anchor = select_anchor_label_fn(
        relation_type=relation_type,
        example_query=example_query,
        target_url=target_url,
        target_name=target_name,
        source_name=source_name,
        source_profile=source_profile,
        target_profile=target_profile,
        used_labels=used_labels,
    )

    score = 18.0
    score += min(math.log1p(max(hit_count, 0)) * 14, 28)
    score += min(len(shared_tokens) * 8, 24)
    score += min(len(query_target_tokens) * 8, 16)
    score += min(len(query_source_tokens) * 4, 8)
    score += min(anchor["quality"] * 0.35, 24)

    reasons: list[str] = []
    if hit_count:
        reasons.append(f"{hit_count} shared query hit{'s' if hit_count != 1 else ''}")
    if topic_key:
        reasons.append(f"same {topic_key} topic")
    if brand_signals and query_intent_scope == "brand_navigation":
        reasons.append("brand-led query")
    if query_intent_scope == "broad_product_family":
        reasons.append("broad product-family query")
    elif query_intent_scope == "specific_product" and fuzzy_signal.get("active"):
        reasons.append("strong fuzzy product match")
    elif query_intent_scope == "specific_product" and collection_signals:
        reasons.append("collection signal narrows the query")
    if query_target_tokens:
        reasons.append(f"query matches target terms: {', '.join(query_target_tokens[:2])}")
    if anchor["label_source"] == "target_name":
        reasons.append("anchor comes from the target product title")
    elif anchor["label_source"] == "target_fragment":
        reasons.append("anchor uses a clean target-title fragment")
    elif anchor["label_source"] == "query" and query_target_tokens:
        reasons.append("anchor keeps a high-intent query phrase")

    for bucket in ("size", "color", "material", "form", "pack_size"):
        query_bucket = query_attrs.get(bucket, set())
        target_bucket = target_attrs.get(bucket, set())
        source_bucket = source_attrs.get(bucket, set())
        matched_query_attrs = sorted(query_bucket & target_bucket)
        if matched_query_attrs:
            score += 12 if bucket in {"size", "form"} else (6 if bucket == "pack_size" else 8)
            reasons.append(f"shared {bucket}: {', '.join(matched_query_attrs[:2])}")
        elif query_bucket and target_bucket and query_bucket.isdisjoint(target_bucket):
            score -= 14 if bucket == "size" else (8 if bucket == "pack_size" else 10)
            reasons.append(f"query {bucket} does not match target")
        elif query_bucket and not target_bucket:
            score -= 6

        matched_source_attrs = sorted(source_bucket & target_bucket)
        if matched_source_attrs:
            score += 4

    if source_brand_tokens and target_brand_tokens and source_brand_tokens & target_brand_tokens:
        score += 4
        reasons.append("same brand family")
    if query_brand_tokens and target_brand_tokens and query_brand_tokens & target_brand_tokens:
        score += 8
        reasons.append("query brand matches target")
    if collection_signals:
        target_collection_tokens = tokenize_intent_text_fn(
            " ".join(signal.get("normalized_label") or "" for signal in collection_signals)
        )
        if target_collection_tokens & target_tokens:
            score += 10
            reasons.append("query collection matches target")

    if fuzzy_signal.get("active"):
        fuzzy_score = float(fuzzy_signal.get("score") or 0.0)
        if fuzzy_score >= 92:
            score += 14
            reasons.append("very strong fuzzy query-to-target match")
        elif fuzzy_score >= 82:
            score += 10
            reasons.append("strong fuzzy query-to-target match")
        elif fuzzy_score >= 68:
            score += 6
            reasons.append("fuzzy query-to-target match")

    ga4_signal = build_ga4_signal_fn(
        target_profile=target_profile,
        target_entity_type=((target_profile or {}).get("entity_type") or "product"),
        query_intent_scope=query_intent_scope,
    )
    ga4_delta = float(ga4_signal.get("delta") or 0.0)
    if ga4_delta:
        score += ga4_delta
        reasons.append(ga4_signal.get("reason") or "GA4 performance supports this target")

    source_primary_cluster = (((source_profile or {}).get("cluster_profile") or {}).get("primary") or "").strip().lower()
    target_primary_cluster = (((target_profile or {}).get("cluster_profile") or {}).get("primary") or "").strip().lower()
    accessory_query_intent = bool(query_tokens & narrow_accessory_target_tokens) or bool(
        query_tokens & replacement_intent_tokens
    )
    narrow_accessory_target = is_replacement_or_accessory_target_fn(query_tokens, target_tokens, target_name)
    source_query_topic_tokens = {
        token
        for token in (query_tokens & source_tokens)
        if token not in intent_stopwords and token not in context_keep_tokens
    }
    matched_source_query_topics = sorted(source_query_topic_tokens & target_tokens)
    missing_source_query_topics = sorted(source_query_topic_tokens - target_tokens)
    source_query_modifier_tokens = {
        token
        for token in source_query_topic_tokens
        if token not in topic_priority
        and token not in set(topic_display_map.keys())
        and token not in form_family_tokens
        and token not in generic_routing_tokens
        and token not in query_noise_words
    }
    matched_source_query_modifiers = sorted(source_query_modifier_tokens & target_tokens)
    missing_source_query_modifiers = sorted(source_query_modifier_tokens - target_tokens)
    current_page = bool(
        source_profile
        and target_profile
        and (source_profile.get("entity_type") or "").strip().lower()
        == (target_profile.get("entity_type") or "").strip().lower()
        and int(source_profile.get("bc_entity_id") or 0) == int(target_profile.get("bc_entity_id") or 0)
    )

    if (
        narrow_accessory_target
        and not accessory_query_intent
        and query_intent_scope in {"specific_product", "commercial_topic", "broad_product_family"}
    ):
        score -= 18 if query_intent_scope == "specific_product" else 12
        reasons.append("accessory target is narrower than the query")

    if len(source_query_topic_tokens) >= 2 and missing_source_query_topics:
        if query_intent_scope == "specific_product":
            score -= min(22 + (len(missing_source_query_topics) - 1) * 8, 32)
            reasons.append(
                f"target misses key source-aligned topic: {', '.join(missing_source_query_topics[:2])}"
            )
        elif query_intent_scope in {"commercial_topic", "broad_product_family"}:
            score -= min(8 + (len(missing_source_query_topics) - 1) * 4, 16)
            reasons.append("target misses some source-aligned topic terms")

    if len(source_query_topic_tokens) >= 2 and len(matched_source_query_topics) == len(source_query_topic_tokens):
        score += 6
        reasons.append("target preserves the full source-aligned topic")

    if source_query_modifier_tokens and missing_source_query_modifiers:
        if query_intent_scope == "specific_product":
            score -= min(18 + (len(missing_source_query_modifiers) - 1) * 6, 28)
            reasons.append(
                f"target misses key query modifier: {', '.join(missing_source_query_modifiers[:2])}"
            )
        elif query_intent_scope in {"commercial_topic", "broad_product_family"}:
            score -= min(16 + (len(missing_source_query_modifiers) - 1) * 6, 24)
            reasons.append(
                f"target misses key query modifier: {', '.join(missing_source_query_modifiers[:2])}"
            )
        elif query_intent_scope == "brand_navigation":
            score -= min(14 + (len(missing_source_query_modifiers) - 1) * 5, 20)
            reasons.append(
                f"target misses brand-side query modifier: {', '.join(missing_source_query_modifiers[:2])}"
            )

    if source_query_modifier_tokens and len(matched_source_query_modifiers) == len(source_query_modifier_tokens):
        score += 10
        reasons.append("target keeps the key query modifier")
        if current_page and query_intent_scope in {"commercial_topic", "broad_product_family"}:
            score += 8
            reasons.append("current page preserves the exact query wording")

    if query_intent_scope == "broad_product_family":
        hotel_context = "hotel" in query_tokens
        target_narrowing_tokens = target_tokens & {
            "fabric",
            "vinyl",
            "hookless",
            "laminated",
            "polyester",
            "blackout",
            "thermal",
        }
        query_has_matching_narrowing = bool(query_tokens & target_narrowing_tokens)
        if hotel_context and target_entity_type == "category":
            if "hotel" in target_tokens:
                score += 12
                reasons.append("hotel-level category matches the broad hotel query")
            elif target_narrowing_tokens and not query_has_matching_narrowing:
                score -= 24
                reasons.append("subcategory is narrower than the broad hotel query")
        elif hotel_context and target_entity_type == "product":
            score -= 8
            reasons.append("broad hotel query leans above a specific product")

        replacement_intent = bool(query_tokens & replacement_intent_tokens)
        narrow_accessory_target = is_replacement_or_accessory_target_fn(query_tokens, target_tokens, target_name)
        if narrow_accessory_target and not replacement_intent:
            score -= 22
            reasons.append("replacement/accessory target is too narrow for a broad family query")

    if not shared_tokens:
        score -= 20
        reasons.append("limited product-topic overlap")
    if not query_target_tokens:
        score -= 10
    if anchor["generic"]:
        score -= 12
        reasons.append("anchor is still too generic")
    if example_query and (tokenize_intent_text_fn(example_query) & query_noise_words):
        score -= 6
        reasons.append("query language looks more editorial than commercial")

    raw_score = round(score, 2)
    score = max(0.0, min(100.0, raw_score))
    passes = (
        score >= 58
        and len(shared_tokens) >= 1
        and anchor["quality"] >= 42
        and anchor["label"].lower() not in {label.lower() for label in (used_labels or set())}
    )
    if not passes and not reasons:
        reasons.append("insufficient confidence")

    summary_reasons: list[str] = []
    if hit_count:
        summary_reasons.append(f"{hit_count} shared query hit{'s' if hit_count != 1 else ''}")
    if topic_key:
        summary_reasons.append(f"same {topic_key} topic")
    if fuzzy_signal.get("active"):
        summary_reasons.append(f"fuzzy match to {fuzzy_signal.get('matched_kind') or 'entity'}")
    if query_intent_scope == "broad_product_family":
        summary_reasons.append("broad product-family query")
    elif query_intent_scope == "specific_product" and fuzzy_signal.get("active"):
        summary_reasons.append("strong fuzzy product match")
    elif query_intent_scope == "specific_product" and collection_signals:
        summary_reasons.append("collection signal narrows the query")
    elif query_intent_scope == "brand_navigation":
        summary_reasons.append("brand-led query")
    elif query_intent_scope == "informational":
        summary_reasons.append("informational query")
    if ga4_signal.get("active"):
        summary_reasons.append(ga4_signal.get("summary") or "GA4 supports this target")
    if query_target_tokens:
        summary_reasons.append(f"query matches target terms: {', '.join(query_target_tokens[:2])}")
    if anchor["label_source"] == "target_name":
        summary_reasons.append("anchor comes from the target product title")
    elif anchor["label_source"] == "target_fragment":
        summary_reasons.append("anchor uses a clean target-title fragment")
    elif anchor["label_source"] == "query" and query_target_tokens:
        summary_reasons.append("anchor keeps a high-intent query phrase")

    return {
        "raw_score": raw_score,
        "score": score,
        "anchor_label": anchor["label"],
        "anchor_label_source": anchor["label_source"],
        "anchor_quality": anchor["quality"],
        "reason_summary": "; ".join(summary_reasons[:3]) or "Related product relationship",
        "reasons": reasons,
        "shared_tokens": shared_tokens[:5],
        "query_target_tokens": query_target_tokens[:4],
        "query_source_tokens": query_source_tokens[:4],
        "attributes": {
            "query": attribute_sets_to_list_fn(query_attrs),
            "source": attribute_sets_to_list_fn(source_attrs),
            "target": attribute_sets_to_list_fn(target_attrs),
        },
        "fuzzy_signal": fuzzy_signal,
        "ga4_signal": ga4_signal,
        "topic_key": topic_key,
        "query_signals": resolved_signals,
        "passes": passes,
        "source_primary_cluster": source_primary_cluster,
        "target_primary_cluster": target_primary_cluster,
        "source_query_topic_match_count": len(matched_source_query_topics),
        "source_query_topic_missing_count": len(missing_source_query_topics),
        "source_query_modifier_match_count": len(matched_source_query_modifiers),
        "source_query_modifier_missing_count": len(missing_source_query_modifiers),
        "query_intent_scope": query_intent_scope,
        "preferred_entity_type": preferred_entity_type,
    }


__all__ = ["build_intent_profile"]
