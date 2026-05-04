"""Query-signal resolution and semantics analysis helpers for Fulcrum."""

from __future__ import annotations

from typing import Any, Callable


ROLLOWAY_MATTRESS_COMPONENT_RESTRICTORS = {
    "component",
    "components",
    "only",
    "part",
    "parts",
    "replace",
    "replacement",
    "replacing",
}


def is_rollaway_mattress_component_query(query_tokens: set[str]) -> bool:
    return {"rollaway", "bed", "mattress"} <= set(query_tokens) and bool(
        set(query_tokens) & ROLLOWAY_MATTRESS_COMPONENT_RESTRICTORS
    )


def match_store_signal_entries(
    query: str | None,
    query_tokens: set[str],
    entries: list[dict[str, Any]],
    signal_kind: str,
    *,
    normalize_signal_label_fn: Callable[[str | None], str],
    tokenize_intent_text_fn: Callable[[str | None], set[str]],
    non_generic_signal_tokens_fn: Callable[[set[str]], set[str]],
    signal_source_priority_fn: Callable[[str | None], int],
    topic_priority: set[str],
    topic_display_map: dict[str, str],
    form_family_tokens: set[str],
) -> list[dict[str, Any]]:
    normalized_query = normalize_signal_label_fn(query)
    query_text = f" {str(query or '').lower()} "
    matches: list[dict[str, Any]] = []
    for entry in entries:
        label = (entry.get("normalized_label") or entry.get("raw_label") or "").strip()
        label_tokens = set(entry.get("tokens") or tokenize_intent_text_fn(label))
        if not label and not label_tokens:
            continue

        matched_tokens = query_tokens & label_tokens
        core_match_tokens = non_generic_signal_tokens_fn(matched_tokens)
        matched = False
        if signal_kind == "sku_pattern":
            exact_like_sku = bool(label and (any(char.isdigit() for char in label) or "-" in label))
            matched = bool(
                label and exact_like_sku and (
                    f" {label.lower()} " in query_text
                    or label.lower() in normalized_query.replace(" ", "-")
                )
            )
        elif signal_kind == "topic_token":
            matched = bool(core_match_tokens)
        elif signal_kind == "brand_alias":
            label_core_tokens = non_generic_signal_tokens_fn(label_tokens)
            matched = bool(
                (len(label_tokens) == 1 and (core_match_tokens or matched_tokens))
                or (label and f" {label.lower()} " in f" {normalized_query.lower()} ")
                or (label_core_tokens and label_core_tokens <= query_tokens)
            )
        elif signal_kind == "collection":
            matched = bool(core_match_tokens or (len(label_tokens) == 1 and matched_tokens))
        else:
            label_core_tokens = non_generic_signal_tokens_fn(label_tokens)
            if signal_kind == "soft_attribute" and (entry.get("metadata") or {}).get("bucket_key") == "form":
                expanded_label_tokens: set[str] = set()
                for token in label_tokens:
                    expanded_label_tokens.add(token)
                    expanded_label_tokens |= {part for part in token.split("-") if part}
                label_core_tokens = {
                    token
                    for token in non_generic_signal_tokens_fn(expanded_label_tokens)
                    if token not in topic_priority
                    and token not in set(topic_display_map.keys())
                    and token not in form_family_tokens
                }
                core_match_tokens = label_core_tokens & query_tokens
            matched = bool(
                (label and f" {label.lower()} " in f" {normalized_query.lower()} ")
                or core_match_tokens
                or (label_core_tokens and label_core_tokens <= query_tokens)
            )
        if not matched:
            continue

        matches.append(
            {
                "label": entry.get("raw_label") or label,
                "normalized_label": label,
                "source": entry.get("source") or "deterministic",
                "confidence": float(entry.get("confidence") or 0.0),
                "matched_tokens": core_match_tokens or matched_tokens or label_tokens,
                "bucket_key": (entry.get("metadata") or {}).get("bucket_key"),
                "scope_kind": entry.get("scope_kind") or "",
                "entity_type": entry.get("entity_type") or "",
                "entity_id": int(entry.get("entity_id") or 0),
            }
        )
    deduped: dict[tuple[str, str], dict[str, Any]] = {}
    for match in matches:
        key = (signal_kind, match.get("normalized_label") or "")
        existing = deduped.get(key)
        if not existing:
            deduped[key] = match
            continue
        existing_priority = (signal_source_priority_fn(existing.get("source")), float(existing.get("confidence") or 0.0))
        match_priority = (signal_source_priority_fn(match.get("source")), float(match.get("confidence") or 0.0))
        if match_priority >= existing_priority:
            deduped[key] = match
    return list(deduped.values())


def match_semantic_signal_entries(
    query: str | None,
    query_tokens: set[str],
    entries: list[dict[str, Any]],
    *,
    normalize_signal_label_fn: Callable[[str | None], str],
    tokenize_intent_text_fn: Callable[[str | None], set[str]],
) -> list[dict[str, Any]]:
    normalized_query = normalize_signal_label_fn(query)
    query_text = f" {normalized_query.lower()} "
    matches: list[dict[str, Any]] = []
    for entry in entries:
        label = (entry.get("normalized_label") or entry.get("raw_label") or "").strip().lower()
        label_tokens = set(entry.get("tokens") or tokenize_intent_text_fn(label))
        if not label:
            continue
        if len(label_tokens) >= 2:
            matched = f" {label} " in query_text or label_tokens <= query_tokens
        else:
            matched = bool(label_tokens & query_tokens) or f" {label} " in query_text
        if not matched:
            continue
        matches.append(
            {
                "label": entry.get("raw_label") or label,
                "normalized_label": label,
                "source": entry.get("source") or "deterministic",
                "confidence": float(entry.get("confidence") or 0.0),
                "matched_tokens": sorted(label_tokens & query_tokens or label_tokens),
                "scope_kind": entry.get("scope_kind") or "",
                "metadata": dict(entry.get("metadata") or {}),
            }
        )
    return matches


def semantic_head_term_from_phrases(
    bound_phrase_matches: list[dict[str, Any]],
    *,
    normalize_signal_label_fn: Callable[[str | None], str],
) -> str:
    for match in bound_phrase_matches:
        head_term = normalize_signal_label_fn((match.get("metadata") or {}).get("head_term"))
        if head_term:
            return head_term
    return ""


def semantic_head_term(
    query: str | None,
    query_tokens: set[str],
    bound_phrase_matches: list[dict[str, Any]],
    resolved_signals: dict[str, Any],
    *,
    semantic_head_term_from_phrases_fn: Callable[[list[dict[str, Any]]], str],
    ordered_intent_tokens_fn: Callable[[str | None], list[str]],
    canonical_word_token_fn: Callable[[str], str],
    topic_priority: set[str],
    query_noise_words: set[str],
    generic_routing_tokens: set[str],
    context_keep_tokens: set[str],
) -> str:
    phrase_head = semantic_head_term_from_phrases_fn(bound_phrase_matches)
    if phrase_head == "bed" and is_rollaway_mattress_component_query(query_tokens):
        return "mattress"
    if phrase_head:
        return phrase_head

    ordered_tokens = ordered_intent_tokens_fn(query)
    topic_tokens = {
        token
        for signal in (resolved_signals.get("topic_signals") or [])
        for token in (signal.get("matched_tokens") or [])
    }
    for token in reversed(ordered_tokens):
        canonical = canonical_word_token_fn(token)
        if canonical in topic_priority or canonical in topic_tokens:
            return canonical
    for token in reversed(ordered_tokens):
        canonical = canonical_word_token_fn(token)
        if canonical not in query_noise_words and canonical not in generic_routing_tokens and canonical not in context_keep_tokens:
            return canonical
    return next(iter(query_tokens), "")


def semantic_head_family(
    head_term: str,
    query_tokens: set[str],
    bound_phrase_matches: list[dict[str, Any]],
    taxonomy_alias_matches: list[dict[str, Any]],
    *,
    normalize_signal_label_fn: Callable[[str | None], str],
    semantic_pluralize_fn: Callable[[str | None], str],
) -> str:
    for match in bound_phrase_matches:
        head_family = normalize_signal_label_fn((match.get("metadata") or {}).get("head_family"))
        if head_family:
            return head_family

    if head_term == "cart" and taxonomy_alias_matches:
        return "bellman carts"
    if head_term == "bed" and "rollaway" in query_tokens:
        return "rollaway beds"
    if head_term == "pillow" and "hotel" in query_tokens:
        return "hotel pillows"
    if head_term:
        return semantic_pluralize_fn(head_term)
    return ""


def semantic_family_candidate_tokens(
    head_term: str,
    head_family: str,
    taxonomy_alias_matches: list[dict[str, Any]],
    *,
    canonical_word_token_fn: Callable[[str], str],
    semantic_pluralize_fn: Callable[[str | None], str],
    tokenize_intent_text_fn: Callable[[str | None], set[str]],
    semantic_brand_family_aliases: dict[str, set[str]],
    generic_routing_tokens: set[str],
) -> set[str]:
    family_tokens: set[str] = set()
    canonical_head = canonical_word_token_fn(head_term)
    if canonical_head:
        family_tokens.add(canonical_head)
        family_tokens.add(semantic_pluralize_fn(canonical_head))
        family_tokens |= set(semantic_brand_family_aliases.get(canonical_head, set()))
    family_tokens |= tokenize_intent_text_fn(head_family)
    for match in taxonomy_alias_matches:
        metadata = match.get("metadata") or {}
        family_tokens |= {
            canonical_word_token_fn(token)
            for token in (metadata.get("canonical_tokens") or [])
            if canonical_word_token_fn(token)
        }
    return {token for token in family_tokens if token and token not in generic_routing_tokens}


def query_has_exact_brand_phrase(
    query: str | None,
    brand_signals: list[dict[str, Any]],
    *,
    normalize_signal_label_fn: Callable[[str | None], str],
    tokenize_intent_text_fn: Callable[[str | None], set[str]],
) -> float:
    normalized_query = f" {normalize_signal_label_fn(query).lower()} "
    best = 0.0
    for signal in brand_signals:
        label = normalize_signal_label_fn(signal.get("normalized_label") or signal.get("label"))
        label_tokens = tokenize_intent_text_fn(label)
        if len(label_tokens) < 2:
            continue
        if f" {label.lower()} " in normalized_query:
            best = max(best, max(float(signal.get("confidence") or 0.0), 0.9))
    return min(best, 1.0)


def semantic_token_roles(
    query: str | None,
    head_term: str,
    resolved_signals: dict[str, Any],
    taxonomy_alias_matches: list[dict[str, Any]],
    ambiguous_modifier_matches: list[dict[str, Any]],
    *,
    ordered_intent_tokens_fn: Callable[[str | None], list[str]],
    expand_signal_tokens_fn: Callable[[set[str] | list[str]], set[str]],
    canonical_word_token_fn: Callable[[str], str],
    context_keep_tokens: set[str],
    topic_priority: set[str],
    query_noise_words: set[str],
    generic_routing_tokens: set[str],
    size_tokens: set[str],
) -> list[dict[str, Any]]:
    ordered_tokens = ordered_intent_tokens_fn(query)
    query_brand_tokens = {
        token
        for signal in (resolved_signals.get("brand_signals") or [])
        for token in (signal.get("matched_tokens") or [])
    }
    hard_tokens = {
        token
        for signal in (resolved_signals.get("hard_attribute_signals") or [])
        for token in expand_signal_tokens_fn(set(signal.get("matched_tokens") or []))
    }
    soft_tokens = {
        token
        for signal in (resolved_signals.get("soft_attribute_signals") or [])
        for token in expand_signal_tokens_fn(set(signal.get("matched_tokens") or []))
    }
    taxonomy_tokens = {
        token
        for signal in taxonomy_alias_matches
        for token in set(signal.get("matched_tokens") or [])
    }
    ambiguous_tokens = {
        token
        for signal in ambiguous_modifier_matches
        for token in set(signal.get("matched_tokens") or [])
    }
    roles: list[dict[str, Any]] = []
    seen: set[str] = set()
    for token in ordered_tokens:
        canonical = canonical_word_token_fn(token)
        if not canonical or canonical in seen:
            continue
        seen.add(canonical)
        role = "low_signal_term"
        confidence = 0.4
        alternate_roles: list[str] = []
        if canonical == head_term:
            role = "head_product"
            confidence = 0.97
        elif canonical in context_keep_tokens:
            role = "context_modifier"
            confidence = 0.88
        elif canonical in taxonomy_tokens:
            role = "taxonomy_alias"
            confidence = 0.82
        elif canonical in hard_tokens and canonical in size_tokens:
            role = "size_attribute"
            confidence = 0.92
        elif canonical in hard_tokens or canonical in soft_tokens:
            role = "attribute"
            confidence = 0.76
        elif canonical in ambiguous_tokens:
            role = "ambiguous_modifier"
            confidence = 0.42
            alternate_roles = ["brand_candidate", "descriptive_modifier"]
        elif canonical in query_brand_tokens:
            role = "brand_candidate"
            confidence = 0.62
        elif canonical in topic_priority:
            role = "subtype_modifier"
            confidence = 0.72
        elif canonical in query_noise_words or canonical in generic_routing_tokens:
            role = "low_signal_term"
            confidence = 0.28
        roles.append(
            {
                "text": token,
                "role": role,
                "confidence": round(confidence, 2),
                "alternate_roles": alternate_roles,
            }
        )
    return roles


def query_is_broad_descriptive(
    query: str | None,
    query_tokens: set[str],
    resolved_signals: dict[str, Any],
    *,
    semantic_broad_descriptive_patterns: list[Any],
) -> bool:
    lowered = f" {str(query or '').strip().lower()} "
    if any(pattern.search(lowered) for pattern in semantic_broad_descriptive_patterns):
        return True
    if len(query_tokens) >= 5 and not (resolved_signals.get("sku_signals") or resolved_signals.get("collection_signals")):
        return True
    return False


def build_query_semantics_analysis(
    store_hash: str | None,
    example_query: str | None,
    resolved_signals: dict[str, Any],
    *,
    signal_library: dict[str, list[dict[str, Any]]] | None = None,
    build_store_signal_library_fn: Callable[[str], dict[str, list[dict[str, Any]]]],
    ordered_intent_tokens_fn: Callable[[str | None], list[str]],
    expand_signal_tokens_fn: Callable[[set[str] | list[str]], set[str]],
    tokenize_intent_text_fn: Callable[[str | None], set[str]],
    non_generic_signal_tokens_fn: Callable[[set[str]], set[str]],
    match_semantic_signal_entries_fn: Callable[[str | None, set[str], list[dict[str, Any]]], list[dict[str, Any]]],
    semantic_head_term_fn: Callable[[str | None, set[str], list[dict[str, Any]], dict[str, Any]], str],
    semantic_head_family_fn: Callable[[str, set[str], list[dict[str, Any]], list[dict[str, Any]]], str],
    query_has_exact_brand_phrase_fn: Callable[[str | None, list[dict[str, Any]]], float],
    query_is_broad_descriptive_fn: Callable[[str | None, set[str], dict[str, Any]], bool],
    semantic_family_candidate_tokens_fn: Callable[[str, str, list[dict[str, Any]]], set[str]],
    normalize_signal_label_fn: Callable[[str | None], str],
    brand_family_catalog_evidence_fn: Callable[[str, str, tuple[str, ...]], dict[str, Any]],
    semantic_head_term_from_phrases_fn: Callable[[list[dict[str, Any]]], str],
    semantic_token_roles_fn: Callable[[str | None, str, dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]], list[dict[str, Any]]],
    generic_brand_alias_tokens: set[str],
    semantic_allowed_page_types: set[str],
    semantic_accessory_block_rules: dict[str, dict[str, Any]],
    semantic_subtype_constraints: dict[str, dict[str, Any]],
    context_keep_tokens: set[str],
    query_noise_words: set[str],
    generic_routing_tokens: set[str],
) -> dict[str, Any]:
    signal_library = signal_library or (build_store_signal_library_fn(store_hash) if store_hash else {})
    query_tokens = set(resolved_signals.get("query_tokens") or ordered_intent_tokens_fn(example_query))
    brand_signals = list(resolved_signals.get("brand_signals") or [])
    hard_signals = list(resolved_signals.get("hard_attribute_signals") or [])
    soft_signals = list(resolved_signals.get("soft_attribute_signals") or [])
    collection_signals = list(resolved_signals.get("collection_signals") or [])
    topic_signals = list(resolved_signals.get("topic_signals") or [])
    sku_signals = list(resolved_signals.get("sku_signals") or [])
    soft_match_tokens = {
        token
        for signal in soft_signals
        for token in expand_signal_tokens_fn(set(signal.get("matched_tokens") or []))
    }
    filtered_brand_signals: list[dict[str, Any]] = []
    for signal in brand_signals:
        label_tokens = tokenize_intent_text_fn(signal.get("normalized_label") or signal.get("label"))
        matched_tokens = set(signal.get("matched_tokens") or [])
        if len(label_tokens) == 1 and label_tokens <= generic_brand_alias_tokens:
            continue
        if len(label_tokens) == 1 and matched_tokens and matched_tokens <= soft_match_tokens:
            continue
        filtered_brand_signals.append(signal)
    brand_signals = filtered_brand_signals
    brand_tokens = {
        token
        for signal in brand_signals
        for token in (signal.get("matched_tokens") or [])
    }
    bound_phrase_matches = match_semantic_signal_entries_fn(
        example_query,
        query_tokens,
        (signal_library or {}).get("protected_phrase", []),
    )
    taxonomy_alias_matches = match_semantic_signal_entries_fn(
        example_query,
        query_tokens,
        (signal_library or {}).get("taxonomy_alias", []),
    )
    ambiguous_modifier_matches = match_semantic_signal_entries_fn(
        example_query,
        query_tokens,
        (signal_library or {}).get("ambiguous_modifier", []),
    )
    head_term = semantic_head_term_fn(example_query, query_tokens, bound_phrase_matches, resolved_signals)
    head_family = semantic_head_family_fn(head_term, query_tokens, bound_phrase_matches, taxonomy_alias_matches)
    exact_category_topic_signal = any(
        (signal.get("entity_type") or "").strip().lower() == "category"
        and len(non_generic_signal_tokens_fn(set(signal.get("matched_tokens") or []))) >= 2
        and bool(set(signal.get("matched_tokens") or []) & brand_tokens)
        and bool(non_generic_signal_tokens_fn(set(signal.get("matched_tokens") or [])) - brand_tokens)
        and (
            not head_term
            or head_term in tokenize_intent_text_fn(signal.get("normalized_label") or signal.get("label"))
            or head_term in set(signal.get("matched_tokens") or [])
        )
        for signal in topic_signals
    )
    exact_brand_phrase_confidence = query_has_exact_brand_phrase_fn(example_query, brand_signals)
    broad_descriptive = query_is_broad_descriptive_fn(example_query, query_tokens, resolved_signals)
    family_candidate_tokens = semantic_family_candidate_tokens_fn(head_term, head_family, taxonomy_alias_matches)
    component_specificity = is_rollaway_mattress_component_query(query_tokens)
    primary_brand_label = normalize_signal_label_fn(
        (brand_signals[0].get("normalized_label") or brand_signals[0].get("label")) if brand_signals else ""
    )
    brand_family_catalog = (
        brand_family_catalog_evidence_fn(store_hash or "", primary_brand_label, tuple(sorted(family_candidate_tokens)))
        if store_hash and primary_brand_label and family_candidate_tokens
        else {"matching_product_count": 0, "matching_product_urls": []}
    )
    matching_brand_family_products = int(brand_family_catalog.get("matching_product_count") or 0)
    best_brand_category_share = float(brand_family_catalog.get("best_brand_category_share") or 0.0)
    thin_brand_family_category_fallback = bool(
        brand_signals
        and topic_signals
        and not exact_category_topic_signal
        and not (hard_signals or soft_signals or collection_signals or sku_signals)
        and matching_brand_family_products == 1
        and best_brand_category_share < 0.20
    )

    phrase_binding_confidence = (
        0.92
        if any(len(tokenize_intent_text_fn(match.get("normalized_label"))) >= 2 for match in bound_phrase_matches)
        else (0.65 if bound_phrase_matches else 0.42)
    )
    head_confidence = 0.9 if semantic_head_term_from_phrases_fn(bound_phrase_matches) else (0.78 if head_term else 0.38)

    ambiguous_brand_only = bool(
        brand_signals
        and not exact_brand_phrase_confidence
        and query_tokens
        and query_tokens <= {
            token
            for signal in ambiguous_modifier_matches
            for token in set(signal.get("matched_tokens") or [])
        } | {head_term} | context_keep_tokens
    )

    brand_confidence = 0.0
    if exact_brand_phrase_confidence:
        brand_confidence = max(brand_confidence, exact_brand_phrase_confidence)
    elif brand_signals and head_term and len(query_tokens) <= 3 and not (hard_signals or soft_signals or collection_signals or sku_signals) and not exact_category_topic_signal:
        brand_confidence = max(brand_confidence, 0.76)
    elif brand_signals:
        brand_confidence = 0.74 if len(query_tokens) <= 3 else 0.56
    if ambiguous_brand_only:
        brand_confidence = min(brand_confidence or 0.32, 0.22)
    if head_term and topic_signals and not exact_brand_phrase_confidence and not (
        brand_signals and len(query_tokens) <= 3 and not exact_category_topic_signal and not (soft_signals or collection_signals or sku_signals)
    ):
        brand_confidence = max(0.0, brand_confidence - 0.18)
    if brand_signals and topic_signals and not exact_category_topic_signal and not (hard_signals or soft_signals or collection_signals or sku_signals):
        if matching_brand_family_products >= 2:
            brand_confidence = max(brand_confidence, 0.9)
        elif matching_brand_family_products == 1:
            brand_confidence = min(brand_confidence, 0.46)

    pdp_confidence = 0.12
    if sku_signals:
        pdp_confidence += 0.58
    if hard_signals:
        pdp_confidence += 0.24
    if collection_signals:
        pdp_confidence += 0.22
        if len(query_tokens) <= 3:
            pdp_confidence += 0.18
    if brand_signals and head_term and len(query_tokens) <= 3 and (soft_signals or collection_signals):
        pdp_confidence += 0.28
        if soft_signals:
            pdp_confidence += 0.22
    if collection_signals and soft_signals and len(query_tokens) <= 3:
        pdp_confidence += 0.16
    if exact_brand_phrase_confidence and len(query_tokens) <= 4:
        pdp_confidence += 0.16
    if any(char.isdigit() for char in (example_query or "")) and brand_signals and head_term:
        pdp_confidence += 0.35
        if len(query_tokens) <= 5:
            pdp_confidence += 0.18
    if any((match.get("metadata") or {}).get("role") == "quality_modifier" for match in bound_phrase_matches):
        pdp_confidence += 0.24 if len(query_tokens) <= 4 else 0.12
    if broad_descriptive:
        pdp_confidence -= 0.25
    if ambiguous_modifier_matches:
        pdp_confidence -= 0.08
    if brand_signals and topic_signals and not exact_category_topic_signal and not (hard_signals or soft_signals or collection_signals or sku_signals):
        if matching_brand_family_products >= 2:
            pdp_confidence = max(0.0, pdp_confidence - 0.24)
        elif matching_brand_family_products == 1:
            pdp_confidence = max(pdp_confidence, 0.76)
    pdp_confidence = max(0.0, min(1.0, round(pdp_confidence, 2)))

    category_confidence = 0.24
    if head_term:
        category_confidence += 0.3
    if topic_signals:
        category_confidence += 0.2
    if taxonomy_alias_matches:
        category_confidence += 0.18
    if soft_signals or hard_signals:
        category_confidence += 0.08
    if "hotel" in query_tokens or "hospitality" in query_tokens:
        category_confidence += 0.08
    if broad_descriptive:
        category_confidence += 0.14
    category_confidence = max(0.0, min(1.0, round(category_confidence, 2)))
    if thin_brand_family_category_fallback:
        category_confidence = max(category_confidence, 0.78)
        pdp_confidence = min(pdp_confidence, 0.44)
        brand_confidence = min(brand_confidence, 0.46)

    ambiguity_confidence = 0.18
    if ambiguous_modifier_matches:
        ambiguity_confidence += 0.25
    if broad_descriptive:
        ambiguity_confidence += 0.35
    if brand_signals and head_term and not exact_brand_phrase_confidence:
        ambiguity_confidence += 0.14
    if len(query_tokens) >= 5 and not sku_signals:
        ambiguity_confidence += 0.08
    ambiguity_confidence = max(0.0, min(1.0, round(ambiguity_confidence, 2)))

    query_shape = "mixed_ambiguous"
    if broad_descriptive and ambiguity_confidence >= 0.78:
        query_shape = "hold"
    elif broad_descriptive:
        query_shape = "broad_descriptive"
    elif (
        brand_signals
        and topic_signals
        and not exact_category_topic_signal
        and not (hard_signals or soft_signals or collection_signals or sku_signals)
        and matching_brand_family_products >= 2
    ):
        query_shape = "brand_navigational"
    elif (
        brand_signals
        and topic_signals
        and not exact_category_topic_signal
        and not (hard_signals or soft_signals or collection_signals or sku_signals)
        and matching_brand_family_products == 1
    ):
        query_shape = "category_like" if thin_brand_family_category_fallback else "exact_product_like"
    elif exact_brand_phrase_confidence >= 0.82 and brand_confidence >= max(category_confidence, pdp_confidence):
        query_shape = "brand_navigational"
    elif brand_confidence >= 0.72 and not exact_category_topic_signal and not (hard_signals or soft_signals or collection_signals or sku_signals):
        query_shape = "brand_navigational"
    elif sku_signals or (pdp_confidence >= 0.72 and ambiguity_confidence < 0.5 and (hard_signals or collection_signals)):
        query_shape = "exact_product_like"
    elif pdp_confidence >= 0.62 and ambiguity_confidence < 0.5 and (brand_signals or collection_signals):
        query_shape = "exact_product_like"
    elif len(query_tokens) <= 3 and collection_signals and soft_signals and ambiguity_confidence < 0.45:
        query_shape = "exact_product_like"
    elif len(query_tokens) <= 3 and brand_signals and soft_signals and ambiguity_confidence < 0.45:
        query_shape = "exact_product_like"
    elif pdp_confidence >= 0.58 and ambiguity_confidence < 0.45 and len(query_tokens) <= 3 and (brand_signals or collection_signals) and (soft_signals or hard_signals):
        query_shape = "exact_product_like"
    elif component_specificity and topic_signals and not (brand_signals or hard_signals or soft_signals or collection_signals or sku_signals):
        query_shape = "exact_product_like"
    elif hard_signals and head_term and not sku_signals:
        query_shape = "attribute_refined_category"
    elif category_confidence >= 0.64 and ambiguity_confidence < 0.75:
        query_shape = "category_like"
    elif ambiguity_confidence >= 0.78:
        query_shape = "hold"

    blocked_brand_escalation = "suite" in query_tokens and not exact_brand_phrase_confidence
    if blocked_brand_escalation and query_shape == "brand_navigational" and topic_signals:
        query_shape = "category_like"

    if query_shape == "exact_product_like":
        eligible_page_types = ["product"]
        if category_confidence >= 0.72 and not sku_signals and not component_specificity:
            eligible_page_types.append("category")
    elif query_shape == "brand_navigational":
        eligible_page_types = ["brand"]
        if category_confidence >= 0.74 and head_term:
            eligible_page_types.append("category")
    elif query_shape == "attribute_refined_category":
        eligible_page_types = ["category"]
        if pdp_confidence >= 0.58:
            eligible_page_types.append("product")
    elif query_shape == "category_like":
        eligible_page_types = ["category"]
        if pdp_confidence >= 0.52 and (exact_brand_phrase_confidence or collection_signals or bound_phrase_matches or (brand_signals and len(query_tokens) <= 3)):
            eligible_page_types.append("product")
    elif query_shape == "broad_descriptive":
        eligible_page_types = ["category", "content"]
    else:
        eligible_page_types = ["category"]
    eligible_page_types = [page_type for page_type in eligible_page_types if page_type in semantic_allowed_page_types]
    blocked_page_types = sorted(semantic_allowed_page_types - set(eligible_page_types))

    negative_constraints: list[str] = []
    constraint_rules: list[dict[str, Any]] = []
    accessory_rule = semantic_accessory_block_rules.get(head_term)
    if accessory_rule and not (query_tokens & set(accessory_rule.get("unless_query_tokens") or [])):
        negative_constraints.append(accessory_rule["message"])
        constraint_rules.append(
            {
                "kind": "suppress_accessory_family",
                "head_term": head_term,
                "blocked_tokens": sorted(accessory_rule.get("blocked_tokens") or []),
                "unless_query_tokens": sorted(accessory_rule.get("unless_query_tokens") or []),
                "message": accessory_rule["message"],
            }
        )

    for match in taxonomy_alias_matches:
        for token in set(match.get("matched_tokens") or []):
            rule = semantic_subtype_constraints.get(token)
            if not rule or rule.get("head_term") != head_term:
                continue
            negative_constraints.append(rule["message"])
            constraint_rules.append(
                {
                    "kind": "require_taxonomy_tokens",
                    "query_token": token,
                    "allowed_target_tokens": sorted(rule.get("allowed_target_tokens") or []),
                    "blocked_target_tokens": sorted(rule.get("blocked_target_tokens") or []),
                    "message": rule["message"],
                }
            )

    if blocked_brand_escalation:
        negative_constraints.append("Do not escalate 'suite' to brand without corroboration")
        constraint_rules.append(
            {
                "kind": "block_brand_without_exact_phrase",
                "token": "suite",
                "message": "Do not escalate 'suite' to brand without corroboration",
            }
        )
        blocked_page_types = sorted(set(blocked_page_types) | {"brand"})
        eligible_page_types = [page_type for page_type in eligible_page_types if page_type != "brand"]

    if "king-size" in {match.get("normalized_label") for match in bound_phrase_matches} or "king" in query_tokens:
        if broad_descriptive:
            negative_constraints.append("King size should not alone trigger specific-product routing")
            constraint_rules.append(
                {
                    "kind": "block_pdp_without_identity_phrase",
                    "token": "king-size",
                    "message": "King size should not alone trigger specific-product routing",
                }
            )

    if head_term and float(head_confidence) >= 0.75 and query_shape not in {"brand_navigational", "hold"}:
        negative_constraints.append(f"The head product `{head_term}` must stay visible in any changed route")
        constraint_rules.append(
            {
                "kind": "require_head_term_presence",
                "head_term": head_term,
                "message": f"The head product `{head_term}` must stay visible in any changed route",
            }
        )
    if brand_signals and topic_signals and matching_brand_family_products >= 2 and query_shape == "brand_navigational":
        negative_constraints.append(
            "Brand plus family queries with multiple matching brand products should stay brand-led until a more specific product phrase is present"
        )
        constraint_rules.append(
            {
                "kind": "prefer_brand_when_family_has_multiple_products",
                "brand_label": primary_brand_label,
                "matching_product_count": matching_brand_family_products,
                "family_tokens": sorted(family_candidate_tokens),
                "message": "Brand plus family queries with multiple matching brand products should stay brand-led until a more specific product phrase is present",
            }
        )
    if thin_brand_family_category_fallback and query_shape == "category_like":
        negative_constraints.append(
            "Thin brand-family coverage blocks single-product routing; use the strongest matching family category instead"
        )
        constraint_rules.append(
            {
                "kind": "thin_brand_family_prefer_category",
                "brand_label": primary_brand_label,
                "matching_product_count": matching_brand_family_products,
                "brand_category_share": best_brand_category_share,
                "family_tokens": sorted(family_candidate_tokens),
                "message": "Thin brand-family coverage blocks single-product routing; use the strongest matching family category instead",
            }
        )

    ignored_modifier_tokens: set[str] = set()
    for match in bound_phrase_matches:
        role = ((match.get("metadata") or {}).get("role") or "").strip().lower()
        if role in {"quality_modifier", "brand_candidate"}:
            ignored_modifier_tokens |= tokenize_intent_text_fn(match.get("normalized_label") or match.get("label"))
    ambiguous_tokens = {
        token
        for match in ambiguous_modifier_matches
        for token in set(match.get("matched_tokens") or [])
    }
    taxonomy_tokens = {
        token
        for match in taxonomy_alias_matches
        for token in set(match.get("matched_tokens") or [])
    }
    modifier_tokens = sorted(
        {
            token
            for token in query_tokens
            if token
            and token != head_term
            and token not in context_keep_tokens
            and token not in query_noise_words
            and token not in generic_routing_tokens
            and token not in ignored_modifier_tokens
            and token not in ambiguous_tokens
            and token not in taxonomy_tokens
            and token not in brand_tokens
        }
    )
    if modifier_tokens and len(modifier_tokens) <= 2 and query_shape not in {"brand_navigational", "hold"}:
        negative_constraints.append("Key modifiers must stay visible in any changed route: " + ", ".join(modifier_tokens))
        constraint_rules.append(
            {
                "kind": "require_modifier_presence",
                "modifier_tokens": modifier_tokens,
                "message": "Key modifiers must stay visible in any changed route: " + ", ".join(modifier_tokens),
            }
        )

    token_roles = semantic_token_roles_fn(
        example_query,
        head_term,
        resolved_signals,
        taxonomy_alias_matches,
        ambiguous_modifier_matches,
    )
    ambiguity_level = "high" if ambiguity_confidence >= 0.72 else ("medium" if ambiguity_confidence >= 0.42 else "low")

    if "brand" in eligible_page_types and brand_confidence >= max(category_confidence, pdp_confidence):
        recommended_behavior = "allow brand-led routing"
    elif "product" in eligible_page_types and "category" not in eligible_page_types:
        recommended_behavior = "allow product-led routing"
    elif "category" in eligible_page_types and "product" in eligible_page_types:
        recommended_behavior = "prefer category unless exact product evidence is stronger"
    elif "category" in eligible_page_types:
        recommended_behavior = "keep category-led routing"
    else:
        recommended_behavior = "hold route change"

    reasoning_parts = []
    if head_term:
        reasoning_parts.append(f"Head product is {head_term}")
    if component_specificity:
        reasoning_parts.append("component-specific wording keeps the route product-led")
    if ambiguous_modifier_matches:
        reasoning_parts.append("ambiguous modifiers reduce specificity")
    if broad_descriptive:
        reasoning_parts.append("query reads broad or descriptive")
    elif exact_brand_phrase_confidence >= 0.82:
        reasoning_parts.append("exact brand phrase supports navigational intent")
    elif taxonomy_alias_matches:
        reasoning_parts.append("subtype alias narrows the family")
    elif hard_signals:
        reasoning_parts.append("attributes refine but do not fully identify the route")
    elif bound_phrase_matches:
        reasoning_parts.append("meaningful phrases were preserved before routing")

    return {
        "normalized_query": normalize_signal_label_fn(example_query),
        "bound_phrases": [match.get("normalized_label") or match.get("label") for match in bound_phrase_matches],
        "head_term": head_term,
        "head_family": head_family,
        "token_roles": token_roles,
        "query_shape": query_shape,
        "ambiguity_level": ambiguity_level,
        "eligible_page_types": eligible_page_types,
        "blocked_page_types": blocked_page_types,
        "negative_constraints": negative_constraints,
        "brand_confidence": round(brand_confidence, 2),
        "pdp_confidence": round(pdp_confidence, 2),
        "category_confidence": round(category_confidence, 2),
        "phrase_binding_confidence": round(phrase_binding_confidence, 2),
        "head_confidence": round(head_confidence, 2),
        "ambiguity_confidence": round(ambiguity_confidence, 2),
        "recommended_behavior": recommended_behavior,
        "reasoning_summary": "; ".join(reasoning_parts[:3]) or "Semantics control analyzed the query before routing.",
        "brand_family_matching_product_count": matching_brand_family_products,
        "brand_family_matching_product_urls": list(brand_family_catalog.get("matching_product_urls") or []),
        "brand_family_category_depth": list(brand_family_catalog.get("category_depth") or []),
        "best_brand_category_share": best_brand_category_share,
        "thin_brand_family_category_fallback": thin_brand_family_category_fallback,
        "judge_verdict": "hold" if query_shape == "hold" else "allow",
        "resolver_invoked": False,
        "constraint_rules": constraint_rules,
        "_bound_phrase_matches": bound_phrase_matches,
        "_taxonomy_alias_matches": taxonomy_alias_matches,
        "_ambiguous_modifier_matches": ambiguous_modifier_matches,
    }


def build_fallback_query_signal_context(
    example_query: str | None,
    query_tokens: set[str],
    source_profile: dict[str, Any] | None = None,
    target_profile: dict[str, Any] | None = None,
    *,
    extract_attribute_terms_fn: Callable[[str | None], dict[str, set[str]]],
    tokenize_intent_text_fn: Callable[[str | None], set[str]],
    fallback_signal_match_fn: Callable[..., dict[str, Any]],
    has_model_or_sku_signal_fn: Callable[[str | None], bool],
    serialize_query_signal_matches_fn: Callable[[list[dict[str, Any]]], list[dict[str, Any]]],
    signal_kind_from_bucket_fn: Callable[[str | None], str | None],
    expand_signal_tokens_fn: Callable[[set[str] | list[str]], set[str]],
    non_generic_signal_tokens_fn: Callable[[set[str]], set[str]],
    topic_priority: set[str],
    topic_display_map: dict[str, str],
    material_tokens: set[str],
    form_tokens: set[str],
) -> dict[str, Any]:
    query_attrs = extract_attribute_terms_fn(example_query)
    brand_tokens = query_tokens & (
        tokenize_intent_text_fn((source_profile or {}).get("brand_name"))
        | tokenize_intent_text_fn((target_profile or {}).get("brand_name"))
    )
    brand_signals = [
        fallback_signal_match_fn("brand_alias", token, matched_tokens={token}, confidence=0.54)
        for token in sorted(brand_tokens)
        if token not in topic_priority and token not in material_tokens and token not in form_tokens
    ]
    hard_signals: list[dict[str, Any]] = []
    soft_signals: list[dict[str, Any]] = []
    for bucket_key, values in query_attrs.items():
        signal_kind = signal_kind_from_bucket_fn(bucket_key)
        if signal_kind not in {"hard_attribute", "soft_attribute"}:
            continue
        for value in sorted(values or []):
            match_tokens = tokenize_intent_text_fn(value) or {value}
            if bucket_key == "form":
                match_tokens = expand_signal_tokens_fn(match_tokens)
            match = fallback_signal_match_fn(
                signal_kind,
                value,
                matched_tokens=match_tokens,
                confidence=0.48 if signal_kind == "soft_attribute" else 0.56,
                bucket_key=bucket_key,
            )
            if signal_kind == "hard_attribute":
                hard_signals.append(match)
            else:
                soft_signals.append(match)
    topic_tokens = query_tokens & (topic_priority | set(topic_display_map.keys()))
    topic_signals = [
        fallback_signal_match_fn("topic_token", token, matched_tokens={token}, confidence=0.5)
        for token in sorted(non_generic_signal_tokens_fn(topic_tokens) or topic_tokens)
    ]
    sku_signals = []
    if has_model_or_sku_signal_fn(example_query):
        sku_signals.append(
            fallback_signal_match_fn(
                "sku_pattern",
                example_query or "",
                normalized_label=example_query or "",
                matched_tokens=query_tokens,
                confidence=0.62,
            )
        )
    return {
        "brand_signals": serialize_query_signal_matches_fn(brand_signals),
        "hard_attribute_signals": serialize_query_signal_matches_fn(hard_signals),
        "soft_attribute_signals": serialize_query_signal_matches_fn(soft_signals),
        "collection_signals": [],
        "topic_signals": serialize_query_signal_matches_fn(topic_signals),
        "sku_signals": serialize_query_signal_matches_fn(sku_signals),
    }


def resolve_query_signal_context(
    store_hash: str | None,
    example_query: str | None,
    signal_library: dict[str, list[dict[str, Any]]] | None = None,
    source_profile: dict[str, Any] | None = None,
    target_profile: dict[str, Any] | None = None,
    *,
    tokenize_intent_text_fn: Callable[[str | None], set[str]],
    build_store_signal_library_fn: Callable[[str], dict[str, list[dict[str, Any]]]],
    match_store_signal_entries_fn: Callable[[str | None, set[str], list[dict[str, Any]], str], list[dict[str, Any]]],
    extract_attribute_terms_fn: Callable[[str | None], dict[str, set[str]]],
    query_has_explicit_attribute_intent_fn: Callable[[dict[str, set[str]], str, set[str]], bool],
    build_fallback_query_signal_context_fn: Callable[..., dict[str, Any]],
    match_has_specific_attribute_tokens_fn: Callable[[set[str], dict[str, Any], str], bool],
    expand_signal_tokens_fn: Callable[[set[str] | list[str]], set[str]],
    serialize_query_signal_matches_fn: Callable[[list[dict[str, Any]]], list[dict[str, Any]]],
) -> dict[str, Any]:
    query_tokens = tokenize_intent_text_fn(example_query)
    signal_library = signal_library or (build_store_signal_library_fn(store_hash) if store_hash else None)
    brand_matches = match_store_signal_entries_fn(example_query, query_tokens, (signal_library or {}).get("brand_alias", []), "brand_alias")
    hard_matches = match_store_signal_entries_fn(example_query, query_tokens, (signal_library or {}).get("hard_attribute", []), "hard_attribute")
    soft_matches = match_store_signal_entries_fn(example_query, query_tokens, (signal_library or {}).get("soft_attribute", []), "soft_attribute")
    collection_matches = match_store_signal_entries_fn(example_query, query_tokens, (signal_library or {}).get("collection", []), "collection")
    topic_matches = match_store_signal_entries_fn(example_query, query_tokens, (signal_library or {}).get("topic_token", []), "topic_token")
    sku_matches = match_store_signal_entries_fn(example_query, query_tokens, (signal_library or {}).get("sku_pattern", []), "sku_pattern")
    fallback_query_attrs = extract_attribute_terms_fn(example_query)
    explicit_hard_query_attrs = any(
        query_has_explicit_attribute_intent_fn(fallback_query_attrs, bucket, query_tokens)
        for bucket in ("size", "pack_size")
    )
    explicit_soft_query_attrs = any(
        query_has_explicit_attribute_intent_fn(fallback_query_attrs, bucket, query_tokens)
        for bucket in ("color", "material", "form")
    )

    fallback_context = build_fallback_query_signal_context_fn(
        example_query=example_query,
        query_tokens=query_tokens,
        source_profile=source_profile,
        target_profile=target_profile,
    )
    brand_matches = brand_matches or fallback_context["brand_signals"]
    hard_matches = hard_matches or fallback_context["hard_attribute_signals"]
    soft_matches = soft_matches or fallback_context["soft_attribute_signals"]
    topic_matches = topic_matches or fallback_context["topic_signals"]
    sku_matches = sku_matches or fallback_context["sku_signals"]
    if not explicit_hard_query_attrs:
        hard_matches = [
            match
            for match in hard_matches
            if match_has_specific_attribute_tokens_fn(query_tokens, match, "hard_attribute")
        ]
    if not explicit_soft_query_attrs:
        soft_matches = [
            match
            for match in soft_matches
            if match_has_specific_attribute_tokens_fn(query_tokens, match, "soft_attribute")
        ]
    soft_match_tokens = {
        token
        for signal in soft_matches
        for token in expand_signal_tokens_fn(set(signal.get("matched_tokens") or []))
    }
    brand_matches = [
        match
        for match in brand_matches
        if not (
            len(tokenize_intent_text_fn(match.get("normalized_label") or match.get("label"))) == 1
            and set(match.get("matched_tokens") or []) <= soft_match_tokens
        )
    ]
    brand_match_tokens = {
        token
        for match in brand_matches
        for token in (match.get("matched_tokens") or [])
    }
    collection_matches = [
        match
        for match in collection_matches
        if not (set(match.get("matched_tokens") or []) and set(match.get("matched_tokens") or []) <= brand_match_tokens)
    ]

    resolved_signals = {
        "query_tokens": sorted(query_tokens),
        "brand_signals": serialize_query_signal_matches_fn(brand_matches),
        "hard_attribute_signals": serialize_query_signal_matches_fn(hard_matches),
        "soft_attribute_signals": serialize_query_signal_matches_fn(soft_matches),
        "collection_signals": serialize_query_signal_matches_fn(collection_matches),
        "topic_signals": serialize_query_signal_matches_fn(topic_matches),
        "sku_signals": serialize_query_signal_matches_fn(sku_matches),
    }
    query_attrs = {
        "size": set(),
        "color": set(),
        "material": set(),
        "form": set(),
        "pack_size": set(),
    }
    for signal in resolved_signals["hard_attribute_signals"] + resolved_signals["soft_attribute_signals"]:
        bucket_key = (signal.get("bucket_key") or "").strip().lower()
        normalized_label = (signal.get("normalized_label") or "").strip()
        if bucket_key in query_attrs and normalized_label:
            query_attrs[bucket_key].add(normalized_label)
    resolved_signals["query_attrs"] = {bucket: sorted(values) for bucket, values in query_attrs.items() if values}
    return resolved_signals


def classify_query_intent_from_signals(
    example_query: str | None,
    resolved_signals: dict[str, Any],
    *,
    ordered_intent_tokens_fn: Callable[[str | None], list[str]],
    expand_signal_tokens_fn: Callable[[set[str] | list[str]], set[str]],
    tokenize_intent_text_fn: Callable[[str | None], set[str]],
    non_generic_signal_tokens_fn: Callable[[set[str]], set[str]],
    looks_informational_query_fn: Callable[[str | None], bool],
    generic_brand_alias_tokens: set[str],
) -> tuple[str, str]:
    query_tokens = set(resolved_signals.get("query_tokens") or ordered_intent_tokens_fn(example_query))
    brand_signals = list(resolved_signals.get("brand_signals") or [])
    hard_signals = list(resolved_signals.get("hard_attribute_signals") or [])
    soft_signals = list(resolved_signals.get("soft_attribute_signals") or [])
    collection_signals = list(resolved_signals.get("collection_signals") or [])
    topic_signals = list(resolved_signals.get("topic_signals") or [])
    sku_signals = list(resolved_signals.get("sku_signals") or [])
    component_specificity = is_rollaway_mattress_component_query(query_tokens)

    soft_match_tokens = {
        token
        for signal in soft_signals
        for token in expand_signal_tokens_fn(set(signal.get("matched_tokens") or []))
    }
    filtered_brand_signals: list[dict[str, Any]] = []
    for signal in brand_signals:
        label_tokens = tokenize_intent_text_fn(signal.get("normalized_label") or signal.get("label"))
        matched_tokens = set(signal.get("matched_tokens") or [])
        if len(label_tokens) == 1 and label_tokens <= generic_brand_alias_tokens:
            continue
        if len(label_tokens) == 1 and matched_tokens and matched_tokens <= soft_match_tokens:
            continue
        filtered_brand_signals.append(signal)
    brand_signals = filtered_brand_signals

    brand_tokens = {
        token
        for signal in brand_signals
        for token in (signal.get("matched_tokens") or [])
    }
    collection_signals = [
        signal
        for signal in collection_signals
        if not (set(signal.get("matched_tokens") or []) and set(signal.get("matched_tokens") or []) <= brand_tokens)
    ]
    topic_tokens = {
        token
        for signal in topic_signals
        for token in (signal.get("matched_tokens") or [])
    }
    core_topic_tokens = non_generic_signal_tokens_fn(topic_tokens)
    brand_free_query_tokens = non_generic_signal_tokens_fn(query_tokens - brand_tokens)
    exact_category_topic_signal = any(
        (signal.get("entity_type") or "").strip().lower() == "category"
        and len(non_generic_signal_tokens_fn(set(signal.get("matched_tokens") or []))) >= 2
        and bool(set(signal.get("matched_tokens") or []) & brand_tokens)
        and bool(non_generic_signal_tokens_fn(set(signal.get("matched_tokens") or [])) - brand_tokens)
        for signal in topic_signals
    )
    informational = looks_informational_query_fn(example_query)

    if informational and not hard_signals and not sku_signals and not collection_signals and not brand_signals:
        return "informational", "content"
    if brand_signals and topic_signals and exact_category_topic_signal and not hard_signals and not soft_signals and not sku_signals and not collection_signals:
        return "commercial_topic", "category"
    if brand_signals and not topic_signals and not hard_signals and not soft_signals and not sku_signals and not collection_signals and len(brand_free_query_tokens) >= 2:
        return "specific_product", "product"
    if collection_signals and topic_signals and not brand_signals and not hard_signals and not soft_signals and not sku_signals:
        return "commercial_topic", "category"
    if brand_signals and not topic_signals and not hard_signals and not soft_signals and not sku_signals and not collection_signals and len(query_tokens) <= max(4, len(brand_tokens) + 2):
        return "brand_navigation", "brand"
    if brand_signals and topic_signals and not exact_category_topic_signal and not hard_signals and not soft_signals and not sku_signals and not collection_signals and len(query_tokens) <= 4:
        return "brand_navigation", "brand"
    if brand_signals and topic_signals and soft_signals and len(query_tokens) <= 5 and not sku_signals and not hard_signals and not collection_signals:
        return "specific_product", "product"
    if soft_signals and topic_signals and not hard_signals and not sku_signals and not collection_signals:
        return "commercial_topic", "category"
    if component_specificity and topic_signals and not brand_signals and not hard_signals and not soft_signals and not sku_signals and not collection_signals:
        return "specific_product", "product"
    if hard_signals or sku_signals or (collection_signals and (topic_signals or core_topic_tokens)) or (brand_signals and hard_signals) or (brand_signals and collection_signals) or (brand_signals and topic_signals and len(query_tokens) <= 4):
        return "specific_product", "product"
    if topic_signals and not brand_signals and not hard_signals and not sku_signals and not collection_signals and len(query_tokens) <= 6:
        return "broad_product_family", "category"
    if topic_signals:
        return "commercial_topic", "category"
    return "mixed_or_unknown", "product"


def classify_query_intent_scope(
    example_query: str | None,
    query_tokens: set[str],
    query_attrs: dict[str, set[str]],
    query_brand_tokens: set[str] | None = None,
    resolved_signals: dict[str, Any] | None = None,
    *,
    classify_query_intent_from_signals_fn: Callable[[str | None, dict[str, Any]], tuple[str, str]],
    looks_informational_query_fn: Callable[[str | None], bool],
    has_model_or_sku_signal_fn: Callable[[str | None], bool],
    topic_priority: set[str],
    topic_display_map: dict[str, str],
) -> tuple[str, str]:
    if resolved_signals:
        return classify_query_intent_from_signals_fn(example_query, resolved_signals)
    query_brand_tokens = query_brand_tokens or set()
    hard_attribute_signal = any(query_attrs.get(bucket) for bucket in ("size", "pack_size"))
    soft_attribute_signal = any(query_attrs.get(bucket) for bucket in ("color", "material", "form"))
    has_attribute_signal = hard_attribute_signal or soft_attribute_signal
    has_model_signal = has_model_or_sku_signal_fn(example_query)
    topic_tokens = query_tokens & (topic_priority | set(topic_display_map.keys()))
    if looks_informational_query_fn(example_query) and not hard_attribute_signal and not query_brand_tokens and not has_model_signal:
        return "informational", "content"
    if query_brand_tokens and not has_attribute_signal and not has_model_signal and len(query_tokens) <= max(4, len(query_brand_tokens) + 2):
        return "brand_navigation", "brand"
    if topic_tokens and not hard_attribute_signal and not query_brand_tokens and not has_model_signal and len(query_tokens) <= 6:
        return "broad_product_family", "category"
    if hard_attribute_signal or query_brand_tokens or has_model_signal:
        return "specific_product", "product"
    if soft_attribute_signal and topic_tokens:
        return "commercial_topic", "category"
    if topic_tokens:
        return "commercial_topic", "category"
    return "mixed_or_unknown", "product"


__all__ = [
    "build_fallback_query_signal_context",
    "build_query_semantics_analysis",
    "classify_query_intent_from_signals",
    "classify_query_intent_scope",
    "match_semantic_signal_entries",
    "match_store_signal_entries",
    "query_has_exact_brand_phrase",
    "query_is_broad_descriptive",
    "resolve_query_signal_context",
    "semantic_family_candidate_tokens",
    "semantic_head_family",
    "semantic_head_term",
    "semantic_head_term_from_phrases",
    "semantic_token_roles",
]
