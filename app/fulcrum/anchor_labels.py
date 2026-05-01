"""Anchor-label helpers for Fulcrum."""

from __future__ import annotations

import re
from typing import Any, Callable


def title_case_anchor(text: str, *, anchor_small_words: set[str]) -> str:
    words = []
    for idx, word in enumerate(text.split()):
        if "-" in word:
            parts = [
                part.capitalize() if part and (idx == 0 or part not in anchor_small_words) else part
                for part in word.split("-")
            ]
            words.append("-".join(parts))
            continue
        if idx > 0 and word in anchor_small_words:
            words.append(word)
        else:
            words.append(word.capitalize())
    return " ".join(words)


def normalize_anchor_text(
    raw_text: str | None,
    *,
    title_case_anchor_fn: Callable[[str], str],
    anchor_phrase_replacements: list[tuple[str, str]],
) -> str:
    if not raw_text:
        return ""
    text = str(raw_text).strip().strip(".").lower()
    text = text.replace("&", " and ")
    text = re.sub(r"\bw\s*/\s*", " with ", text)
    text = text.replace("/", " ")
    text = text.replace("_", " ")
    text = re.sub(r"[^a-z0-9\s-]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    for source, target in anchor_phrase_replacements:
        text = text.replace(source, target)
    text = re.sub(r"^(a|an|the)\s+", "", text).strip()
    text = re.sub(r"(.+?)\s+(full-size|twin-size|queen-size|king-size)$", r"\2 \1", text)
    text = re.sub(r"\s+", " ", text).strip(" -")
    return title_case_anchor_fn(text)


def label_from_target_url(
    target_url: str,
    *,
    normalize_anchor_text_fn: Callable[[str | None], str],
) -> str:
    if not target_url:
        return "View Details"
    slug = target_url.strip("/").split("/")[-1].replace(".html", "").replace("-", " ")
    return normalize_anchor_text_fn(slug) or "View Details"


def legacy_fallback_anchor_label(
    relation_type: str | None,
    example_query: str | None,
    target_url: str,
    *,
    label_from_target_url_fn: Callable[[str], str],
    normalize_anchor_text_fn: Callable[[str | None], str],
) -> str | None:
    if (relation_type or "").lower() in {"category", "brand"}:
        return label_from_target_url_fn(target_url)
    normalized_query = normalize_anchor_text_fn(example_query)
    if normalized_query and len(normalized_query) >= 4:
        return normalized_query
    return label_from_target_url_fn(target_url)


def looks_generic_phrase(
    text: str,
    *,
    tokenize_intent_text_fn: Callable[[str | None], set[str]],
    topic_priority: set[str],
    anchor_generic_words: set[str],
) -> bool:
    tokens = tokenize_intent_text_fn(text)
    if not tokens:
        return True
    if len(tokens) == 1 and next(iter(tokens)) not in topic_priority:
        return True
    return all(token in anchor_generic_words for token in tokens)


def canonical_word_token(word: str) -> str:
    token = re.sub(r"[^a-z0-9-]", "", word.lower())
    if token.endswith("s") and len(token) > 4 and not token.endswith(("ss", "us", "is")):
        token = token[:-1]
    return token


def is_noise_fragment(
    text: str,
    *,
    normalize_anchor_text_fn: Callable[[str | None], str],
    anchor_small_words: set[str],
    fragment_noise_patterns: list[Any],
) -> bool:
    normalized = normalize_anchor_text_fn(text)
    if not normalized:
        return True
    lowered = normalized.lower()
    words = lowered.split()
    if lowered in {"wholesale", "made in usa", "made in the usa"}:
        return True
    if len(words) >= 4 and words[-1] in anchor_small_words:
        return True
    if lowered.endswith(" made in the") or lowered.endswith(" made in"):
        return True
    return any(pattern.match(normalized) for pattern in fragment_noise_patterns)


def trim_phrase_tokens(
    text: str,
    *,
    canonical_word_token_fn: Callable[[str], str],
    anchor_small_words: set[str],
    max_words: int = 5,
) -> str:
    words = [word for word in text.split() if word]
    trimmed = words if len(words) <= max_words else words[:max_words]
    while trimmed and canonical_word_token_fn(trimmed[-1]) in anchor_small_words:
        trimmed.pop()
    if trimmed and canonical_word_token_fn(trimmed[-1]) == "made":
        trimmed.pop()
    return " ".join(trimmed)


def ordered_focus_terms(
    fragment: str,
    *,
    canonical_word_token_fn: Callable[[str], str],
    topic_priority: set[str],
    anchor_generic_words: set[str],
    size_tokens: set[str],
    max_terms: int = 2,
) -> list[str]:
    words = [word for word in fragment.split() if word]
    preferred = [
        word
        for word in words
        if canonical_word_token_fn(word) in topic_priority and canonical_word_token_fn(word) not in size_tokens
    ]
    if preferred:
        return preferred[:max_terms]
    fallback = [
        word
        for word in words
        if canonical_word_token_fn(word) not in anchor_generic_words and canonical_word_token_fn(word) not in size_tokens
    ]
    return fallback[:max_terms]


def ordered_size_terms(
    fragment: str,
    *,
    canonical_word_token_fn: Callable[[str], str],
    size_tokens: set[str],
    max_terms: int = 1,
) -> list[str]:
    words = [word for word in fragment.split() if word]
    return [word for word in words if canonical_word_token_fn(word) in size_tokens][:max_terms]


def extract_label_candidates(
    target_name: str | None,
    target_url: str,
    example_query: str | None = None,
    target_profile: dict[str, Any] | None = None,
    *,
    normalize_anchor_text_fn: Callable[[str | None], str],
    trim_phrase_tokens_fn: Callable[[str], str],
    tokenize_intent_text_fn: Callable[[str | None], set[str]],
    is_noise_fragment_fn: Callable[[str], bool],
    ordered_size_terms_fn: Callable[[str], list[str]],
    ordered_focus_terms_fn: Callable[[str], list[str]],
    profile_topic_label_fn: Callable[[dict[str, Any] | None], str],
    profile_brand_label_fn: Callable[[dict[str, Any] | None], str],
    extract_attribute_terms_fn: Callable[[str | None], dict[str, set[str]]],
    label_from_target_url_fn: Callable[[str], str],
    looks_generic_phrase_fn: Callable[[str], bool],
    query_noise_words: set[str],
    size_tokens: set[str],
    topic_priority: set[str],
    form_tokens: set[str],
    topic_display_map: dict[str, str],
    form_display_map: dict[str, str],
) -> list[tuple[str, str]]:
    candidates: list[tuple[str, str]] = []

    def add_candidate(raw_value: str | None, source: str) -> None:
        if not raw_value:
            return
        normalized = normalize_anchor_text_fn(raw_value)
        if not normalized:
            return
        normalized = trim_phrase_tokens_fn(normalized)
        if len(normalized) < 4:
            return
        if any(existing[0].lower() == normalized.lower() for existing in candidates):
            return
        candidates.append((normalized, source))

    if target_name:
        clean_name = str(target_name).replace("Â®", "").replace("â„¢", "")
        raw_fragments = [fragment.strip() for fragment in re.split(r"\s*(?:\||/|,|\s-\s)\s*", clean_name) if fragment.strip()]
        normalized_fragments = [normalize_anchor_text_fn(fragment) for fragment in raw_fragments]
        normalized_fragments = [fragment for fragment in normalized_fragments if fragment and not is_noise_fragment_fn(fragment)]
        if len(normalized_fragments) <= 1:
            add_candidate(clean_name, "target_name")
        for fragment in normalized_fragments:
            add_candidate(fragment, "target_fragment")
        if len(normalized_fragments) >= 2:
            first = normalized_fragments[0]
            second = normalized_fragments[1]
            if tokenize_intent_text_fn(first) & topic_priority and tokenize_intent_text_fn(second) & {"twin-size", "full-size", "queen-size", "king-size", "replacement", "fold-up", "fold-out"}:
                add_candidate(f"{first} {second}", "target_variant_combo")
        size_fragments = [fragment for fragment in normalized_fragments if tokenize_intent_text_fn(fragment) & size_tokens]
        for size_fragment in size_fragments:
            size_terms = ordered_size_terms_fn(size_fragment)
            if not size_terms:
                continue
            for base_fragment in [fragment for fragment in normalized_fragments if fragment not in size_fragments]:
                focus_terms = ordered_focus_terms_fn(base_fragment)
                if focus_terms:
                    add_candidate(f"{' '.join(size_terms)} {' '.join(focus_terms)}", "target_variant_focus")

    if target_profile:
        profile_name = normalize_anchor_text_fn(target_profile.get("name") or "")
        brand_name = normalize_anchor_text_fn(target_profile.get("brand_name") or "")
        descriptor_source = profile_name[len(brand_name) + 1 :] if brand_name and profile_name.startswith(f"{brand_name} ") else profile_name
        name_fragments = [fragment.strip() for fragment in re.split(r"\s*(?:\||/|,|\s-\s)\s*", descriptor_source) if fragment.strip()]
        descriptor_labels = [" ".join(terms) for terms in (ordered_focus_terms_fn(fragment) for fragment in name_fragments) if terms]
        for size_value in sorted(target_profile.get("attributes", {}).get("size", set())):
            size_label = normalize_anchor_text_fn(size_value)
            for descriptor in descriptor_labels[:3]:
                add_candidate(f"{size_label} {descriptor}", "profile_variant_focus")
        for form_value in sorted(target_profile.get("attributes", {}).get("form", set())):
            add_candidate(normalize_anchor_text_fn(form_value), "profile_form")

        topic_label = profile_topic_label_fn(target_profile)
        brand_label = profile_brand_label_fn(target_profile)
        collection_fragments: list[str] = []
        for fragment in name_fragments:
            fragment_tokens = tokenize_intent_text_fn(fragment)
            if not fragment_tokens or (brand_label and fragment.startswith(brand_label)) or fragment_tokens & size_tokens:
                continue
            if looks_generic_phrase_fn(fragment) or fragment_tokens <= query_noise_words or all(token.isdigit() for token in fragment_tokens):
                continue
            if fragment.lower() in {topic_label.lower(), brand_label.lower()}:
                continue
            collection_fragments.append(fragment)

        for collection in collection_fragments[:3]:
            collection_tokens = tokenize_intent_text_fn(collection)
            if collection_tokens & (topic_priority | form_tokens | set(topic_display_map.keys())):
                add_candidate(collection, "profile_collection")
                if brand_label and not collection.startswith(brand_label):
                    add_candidate(f"{brand_label} {collection}", "profile_brand_collection")
            elif topic_label:
                add_candidate(f"{collection} {topic_label}", "profile_collection")
                if brand_label:
                    add_candidate(f"{brand_label} {collection} {topic_label}", "profile_brand_collection")
            elif brand_label:
                add_candidate(f"{brand_label} {collection}", "profile_brand_collection")

        if topic_label == "Towels":
            collection_core = collection_fragments[0] if collection_fragments else ""
            query_forms = extract_attribute_terms_fn(example_query).get("form", set())
            target_forms = sorted(target_profile.get("attributes", {}).get("form", set()))
            if collection_core:
                if query_forms:
                    for form_value in sorted(query_forms & set(target_forms)):
                        form_label = form_display_map.get(form_value, normalize_anchor_text_fn(form_value))
                        add_candidate(f"{collection_core} {form_label}", "profile_collection_form")
                        if brand_label:
                            add_candidate(f"{brand_label} {collection_core} {form_label}", "profile_brand_collection_form")
                else:
                    add_candidate(f"{collection_core} Towels", "profile_collection_form")
                    if brand_label:
                        add_candidate(f"{brand_label} {collection_core} Towels", "profile_brand_collection_form")

    add_candidate(example_query or "", "query")
    add_candidate(label_from_target_url_fn(target_url), "target_url")
    return candidates


def select_anchor_label(
    relation_type: str,
    example_query: str | None,
    target_url: str,
    target_name: str | None = None,
    source_name: str | None = None,
    source_profile: dict[str, Any] | None = None,
    target_profile: dict[str, Any] | None = None,
    used_labels: set[str] | None = None,
    *,
    legacy_fallback_anchor_label_fn: Callable[[str | None, str | None, str], str | None],
    label_from_target_url_fn: Callable[[str], str],
    looks_generic_phrase_fn: Callable[[str], bool],
    tokenize_intent_text_fn: Callable[[str | None], set[str]],
    extract_attribute_terms_fn: Callable[[str | None], dict[str, set[str]]],
    profile_topic_label_fn: Callable[[dict[str, Any] | None], str],
    profile_brand_label_fn: Callable[[dict[str, Any] | None], str],
    extract_label_candidates_fn: Callable[[str | None, str, str | None, dict[str, Any] | None], list[tuple[str, str]]],
    query_noise_words: set[str],
    size_tokens: set[str],
    topic_priority: set[str],
    form_family_tokens: set[str],
    generic_routing_tokens: set[str],
    topic_display_map: dict[str, str],
) -> dict[str, Any]:
    fallback_label = legacy_fallback_anchor_label_fn(relation_type, example_query, target_url)
    if (relation_type or "").lower() in {"category", "brand"}:
        label = label_from_target_url_fn(target_url)
        return {"label": label, "label_source": "target_url", "quality": 72.0, "generic": looks_generic_phrase_fn(label)}

    used_labels = {label.lower() for label in (used_labels or set())}
    source_tokens = tokenize_intent_text_fn(source_name) | set((source_profile or {}).get("tokens") or set())
    target_tokens = tokenize_intent_text_fn(target_name or target_url) | set((target_profile or {}).get("tokens") or set())
    query_tokens = tokenize_intent_text_fn(example_query)
    query_size_tokens = query_tokens & size_tokens
    target_size_tokens = target_tokens & size_tokens
    query_attrs = extract_attribute_terms_fn(example_query)
    target_attrs = dict(target_profile.get("attributes") or {}) if target_profile else extract_attribute_terms_fn(target_name or target_url)
    source_attrs = dict(source_profile.get("attributes") or {}) if source_profile else extract_attribute_terms_fn(source_name)
    target_topic_tokens = tokenize_intent_text_fn(profile_topic_label_fn(target_profile))
    core_query_topic_tokens = query_tokens & (topic_priority | target_topic_tokens)
    broad_query_without_attrs = not any(query_attrs.get(bucket) for bucket in ("size", "color", "material", "form", "pack_size"))

    ranked_choices: list[dict[str, Any]] = []
    for candidate, label_source in extract_label_candidates_fn(target_name, target_url, example_query, target_profile):
        candidate_tokens = tokenize_intent_text_fn(candidate)
        candidate_size_tokens = candidate_tokens & size_tokens
        candidate_attrs = extract_attribute_terms_fn(candidate)
        quality = 40.0 + min(len(candidate_tokens & target_tokens) * 10, 30) + min(len(candidate_tokens & query_tokens) * 5, 10) + min(len(candidate_tokens & source_tokens) * 3, 6)
        quality += 10 if 2 <= len(candidate.split()) <= 4 else -12
        quality += 8 if 8 <= len(candidate) <= 32 else -10
        quality += {"target_fragment": 8, "target_variant_combo": 8, "target_variant_focus": 14, "profile_variant_focus": 16, "profile_form": 10, "profile_collection": 14, "profile_collection_form": 18, "profile_brand_collection": 18, "profile_brand_collection_form": 22, "target_name": 4, "query": -6}.get(label_source, 0)
        if query_size_tokens and candidate_size_tokens & query_size_tokens:
            quality += 20
        elif query_size_tokens and not candidate_size_tokens:
            quality -= 18
        if target_size_tokens and candidate_size_tokens:
            quality += 6
        if query_size_tokens and label_source == "query":
            quality -= 4
        if target_topic_tokens and candidate_tokens & target_topic_tokens:
            quality += 12
        elif target_topic_tokens:
            quality -= 12
        if broad_query_without_attrs and core_query_topic_tokens and not (candidate_tokens & core_query_topic_tokens):
            quality -= 28
            if label_source in {"target_variant_focus", "profile_variant_focus"}:
                quality -= 12
        if not query_size_tokens and len(target_attrs.get("size", set())) > 1 and candidate_size_tokens:
            quality -= 14
        for bucket in ("size", "color", "material", "form", "pack_size"):
            query_bucket = query_attrs.get(bucket, set())
            target_bucket = target_attrs.get(bucket, set())
            candidate_bucket = candidate_attrs.get(bucket, set())
            source_bucket = source_attrs.get(bucket, set())
            if query_bucket and target_bucket and query_bucket & candidate_bucket & target_bucket:
                quality += 10 if bucket == "pack_size" else 12
            elif query_bucket and target_bucket and query_bucket.isdisjoint(target_bucket):
                quality -= 14 if bucket == "pack_size" else 18
            elif query_bucket and not target_bucket:
                quality -= 6 if bucket == "pack_size" else 8
            if source_bucket and target_bucket and source_bucket & target_bucket:
                quality += 4
        generic = looks_generic_phrase_fn(candidate)
        if generic:
            quality -= 16
        target_topic_label = profile_topic_label_fn(target_profile)
        target_brand_label = profile_brand_label_fn(target_profile)
        if target_brand_label and target_topic_label and candidate.lower() == f"{target_brand_label} {target_topic_label}".strip().lower():
            quality -= 18
        if re.match(r"^(made|full|queen|king|twin|100)\b", candidate.lower()) and not (candidate_tokens & target_topic_tokens):
            quality -= 18
        if re.search(r"\b20\d{2}\b", candidate):
            quality -= 14
        if candidate.lower() in used_labels:
            quality -= 24
        ranked_choices.append({"label": candidate, "label_source": label_source, "quality": max(0.0, round(quality, 2)), "generic": generic})

    if ranked_choices:
        ranked_choices.sort(key=lambda choice: (choice["label"].lower() in used_labels, choice["generic"], -choice["quality"], len(choice["label"])))
        return ranked_choices[0]

    label = fallback_label or label_from_target_url_fn(target_url)
    return {"label": label, "label_source": "fallback", "quality": 45.0, "generic": looks_generic_phrase_fn(label)}


def select_category_product_anchor_label(
    target_name: str | None,
    target_url: str,
    source_name: str | None = None,
    source_profile: dict[str, Any] | None = None,
    target_profile: dict[str, Any] | None = None,
    used_labels: set[str] | None = None,
    *,
    tokenize_intent_text_fn: Callable[[str | None], set[str]],
    profile_brand_label_fn: Callable[[dict[str, Any] | None], str],
    profile_topic_label_fn: Callable[[dict[str, Any] | None], str],
    extract_label_candidates_fn: Callable[[str | None, str, str | None, dict[str, Any] | None], list[tuple[str, str]]],
    looks_generic_phrase_fn: Callable[[str], bool],
    select_anchor_label_fn: Callable[..., dict[str, Any]],
    size_tokens: set[str],
) -> dict[str, Any]:
    preferred_source_order = {"target_variant_combo": 0, "target_fragment": 1, "target_name": 2, "profile_brand_collection_form": 3, "profile_brand_collection": 4, "profile_collection_form": 5, "profile_collection": 6, "profile_form": 7, "target_url": 8}
    target_brand_tokens = tokenize_intent_text_fn(profile_brand_label_fn(target_profile))
    target_topic_tokens = tokenize_intent_text_fn(profile_topic_label_fn(target_profile))
    target_name_tokens = tokenize_intent_text_fn(target_name or target_url)
    choices: list[dict[str, Any]] = []
    for candidate, label_source in extract_label_candidates_fn(target_name, target_url, target_name, target_profile):
        if label_source in {"query", "target_variant_focus", "profile_variant_focus", "fallback"}:
            continue
        candidate_tokens = tokenize_intent_text_fn(candidate)
        semantic_quality = int(bool(candidate_tokens & target_brand_tokens)) * 3 + int(bool(candidate_tokens & target_topic_tokens)) * 2 + int(bool(candidate_tokens & target_name_tokens))
        if candidate_tokens and target_brand_tokens and candidate_tokens <= target_brand_tokens and target_topic_tokens and not (candidate_tokens & target_topic_tokens):
            semantic_quality -= 6
        if candidate_tokens <= (size_tokens | {"portable"}):
            semantic_quality -= 3
        choices.append({"label": candidate, "label_source": label_source, "quality": (70.0 if label_source != "target_url" else 60.0) + semantic_quality, "generic": looks_generic_phrase_fn(candidate)})
    if choices:
        used_keys = {label.lower() for label in (used_labels or set())}
        choices.sort(key=lambda choice: (choice["label"].lower() in used_keys, choice["generic"], preferred_source_order.get(choice["label_source"], 99), -choice["quality"], len(choice["label"])))
        return choices[0]
    return select_anchor_label_fn(relation_type="canonical", example_query=target_name, target_url=target_url, target_name=target_name, source_name=source_name, source_profile=source_profile, target_profile=target_profile, used_labels=used_labels)


__all__ = ["canonical_word_token", "extract_label_candidates", "label_from_target_url", "legacy_fallback_anchor_label", "looks_generic_phrase", "normalize_anchor_text", "ordered_focus_terms", "ordered_size_terms", "select_anchor_label", "select_category_product_anchor_label", "title_case_anchor", "trim_phrase_tokens"]
