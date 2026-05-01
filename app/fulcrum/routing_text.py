"""Reusable text and fuzzy-matching helpers for Fulcrum routing."""

from __future__ import annotations

import re
from difflib import SequenceMatcher
from typing import Any, Callable


def tokenize_intent_text(
    raw_text: str | None,
    *,
    anchor_phrase_replacements: list[tuple[str, str]],
    context_token_aliases: dict[str, str],
    intent_stopwords: set[str],
    context_keep_tokens: set[str],
) -> set[str]:
    if not raw_text:
        return set()

    text = str(raw_text).lower()
    text = text.replace("&", " and ")
    text = text.replace("/", " ")
    text = text.replace("_", " ")
    text = text.replace("|", " ")
    text = re.sub(r"[^a-z0-9\s-]", " ", text)

    for source, target in anchor_phrase_replacements:
        text = text.replace(source, target)

    tokens: set[str] = set()
    for token in re.findall(r"[a-z0-9]+(?:-[a-z0-9]+)?", text):
        if token.endswith("s") and len(token) > 4 and not token.endswith(("ss", "us", "is")):
            token = token[:-1]
        token = context_token_aliases.get(token, token)
        if len(token) <= 1 or (token in intent_stopwords and token not in context_keep_tokens):
            continue
        tokens.add(token)
    return tokens


def ordered_intent_tokens(
    raw_text: str | None,
    *,
    anchor_phrase_replacements: list[tuple[str, str]],
    context_token_aliases: dict[str, str],
    intent_stopwords: set[str],
    context_keep_tokens: set[str],
) -> list[str]:
    if not raw_text:
        return []

    text = str(raw_text).lower()
    text = text.replace("&", " and ")
    text = text.replace("/", " ")
    text = text.replace("_", " ")
    text = text.replace("|", " ")
    text = re.sub(r"[^a-z0-9\s-]", " ", text)
    for source, target in anchor_phrase_replacements:
        text = text.replace(source, target)

    ordered: list[str] = []
    seen: set[str] = set()
    for token in re.findall(r"[a-z0-9]+(?:-[a-z0-9]+)?", text):
        if token.endswith("s") and len(token) > 4 and not token.endswith(("ss", "us", "is")):
            token = token[:-1]
        token = context_token_aliases.get(token, token)
        if len(token) <= 1 or (token in intent_stopwords and token not in context_keep_tokens):
            continue
        if token in seen:
            continue
        seen.add(token)
        ordered.append(token)
    return ordered


def normalize_signal_label(
    raw_label: str | None,
    *,
    ordered_intent_tokens_fn: Callable[[str | None], list[str]],
) -> str:
    return " ".join(ordered_intent_tokens_fn(raw_label))


def semantic_pluralize(term: str | None) -> str:
    normalized = (term or "").strip().lower()
    if not normalized:
        return ""
    if normalized.endswith("y") and len(normalized) > 3:
        return normalized[:-1] + "ies"
    if normalized.endswith("s"):
        return normalized
    return normalized + "s"


def profile_topic_label(
    profile: dict[str, Any] | None,
    *,
    form_display_map: dict[str, str],
    topic_display_map: dict[str, str],
    normalize_anchor_text_fn: Callable[[str], str],
    tokenize_intent_text_fn: Callable[[str | None], set[str]],
) -> str:
    if not profile:
        return ""

    attrs = profile.get("attributes") or {}
    form_values = set(attrs.get("form") or set())
    if form_values:
        towel_forms = {"bath-towel", "hand-towel", "pool-towel", "washcloth", "bath-mat", "bath-sheet"}
        if form_values & towel_forms:
            return "Towels"
        for form in sorted(form_values):
            if form in form_display_map:
                return form_display_map[form]

    name_text = " ".join(
        [
            profile.get("name") or "",
            " ".join(profile.get("option_labels") or []),
            " ".join(profile.get("option_display_names") or []),
        ]
    )
    tokens = tokenize_intent_text_fn(name_text)
    for token in ["curtain", "hookless", "towel", "blanket", "comforter", "bedspread", "scarf", "cover", "cart", "carrier", "rack", "sleeper", "bed", "mattress", "frame", "pillow"]:
        if token in tokens:
            return topic_display_map.get(token, normalize_anchor_text_fn(token))
    return ""


def profile_brand_label(
    profile: dict[str, Any] | None,
    *,
    normalize_anchor_text_fn: Callable[[str], str],
) -> str:
    if not profile:
        return ""
    brand = normalize_anchor_text_fn(profile.get("brand_name") or "")
    if brand.endswith(" Hospitality"):
        brand = brand[: -len(" Hospitality")]
    return brand


def normalize_phrase_for_match(
    text: str | None,
    *,
    normalize_anchor_text_fn: Callable[[str], str],
) -> str:
    normalized = normalize_anchor_text_fn(text or "")
    normalized = re.sub(r"\s+", " ", normalized).strip().lower()
    return normalized


def fuzzy_match_score(
    left: str | None,
    right: str | None,
    *,
    normalize_phrase_for_match_fn: Callable[[str | None], str],
    tokenize_intent_text_fn: Callable[[str | None], set[str]],
) -> float:
    left_norm = normalize_phrase_for_match_fn(left)
    right_norm = normalize_phrase_for_match_fn(right)
    if not left_norm or not right_norm:
        return 0.0

    ratio = SequenceMatcher(None, left_norm, right_norm).ratio() * 100
    left_tokens = tokenize_intent_text_fn(left_norm)
    right_tokens = tokenize_intent_text_fn(right_norm)
    overlap_ratio = 0.0
    if left_tokens:
        overlap_ratio = (len(left_tokens & right_tokens) / len(left_tokens)) * 100

    subset_bonus = 8.0 if left_tokens and left_tokens <= right_tokens else 0.0
    contains_bonus = 5.0 if left_norm in right_norm or right_norm in left_norm else 0.0
    return round(min(100.0, (ratio * 0.6) + (overlap_ratio * 0.4) + subset_bonus + contains_bonus), 2)


def fuzzy_candidate_kind(label_source: str) -> str:
    source = (label_source or "").strip().lower()
    if source in {"target_name", "target_fragment", "target_variant_combo", "target_variant_focus"}:
        return "title"
    if source in {"profile_collection", "profile_collection_form", "profile_brand_collection", "profile_brand_collection_form"}:
        return "collection"
    if source in {"profile_form", "target_url"}:
        return "category phrase"
    if source == "brand":
        return "brand"
    return "entity"


def build_fuzzy_signal(
    example_query: str | None,
    target_name: str | None,
    target_url: str,
    *,
    target_profile: dict[str, Any] | None = None,
    normalize_phrase_for_match_fn: Callable[[str | None], str],
    profile_brand_label_fn: Callable[[dict[str, Any] | None], str],
    extract_label_candidates_fn: Callable[[str | None, str, str | None, dict[str, Any] | None], list[tuple[str, str]]],
    fuzzy_match_score_fn: Callable[[str | None, str | None], float],
    normalize_anchor_text_fn: Callable[[str], str],
    fuzzy_candidate_kind_fn: Callable[[str], str],
) -> dict[str, Any]:
    query_phrase = normalize_phrase_for_match_fn(example_query)
    if not query_phrase:
        return {
            "active": False,
            "score": 0.0,
            "matched_text": "",
            "matched_kind": "",
            "matched_source": "",
        }

    candidates: list[tuple[str, str]] = []
    seen: set[str] = set()

    def add_candidate(raw_value: str | None, source: str) -> None:
        normalized = normalize_phrase_for_match_fn(raw_value)
        if not normalized or normalized == query_phrase or normalized in seen:
            return
        seen.add(normalized)
        candidates.append((normalized, source))

    add_candidate(target_name, "target_name")
    add_candidate(profile_brand_label_fn(target_profile), "brand")
    for label, source in extract_label_candidates_fn(target_name, target_url, example_query, target_profile):
        if source == "query":
            continue
        add_candidate(label, source)

    best_score = 0.0
    best_text = ""
    best_source = ""
    for candidate_text, source in candidates:
        score = fuzzy_match_score_fn(query_phrase, candidate_text)
        if score > best_score:
            best_score = score
            best_text = candidate_text
            best_source = source

    return {
        "active": best_score >= 55.0,
        "score": round(best_score, 2),
        "matched_text": normalize_anchor_text_fn(best_text),
        "matched_kind": fuzzy_candidate_kind_fn(best_source),
        "matched_source": best_source,
    }


def normalize_query_family_key(
    query: str | None,
    *,
    normalize_phrase_for_match_fn: Callable[[str | None], str],
    tokenize_intent_text_fn: Callable[[str | None], set[str]],
    query_noise_words: set[str],
) -> str:
    normalized_query = normalize_phrase_for_match_fn(query)
    tokens = sorted(tokenize_intent_text_fn(query) - query_noise_words)
    if tokens:
        return " ".join(tokens)
    return normalized_query or ""


__all__ = [
    "build_fuzzy_signal",
    "fuzzy_candidate_kind",
    "fuzzy_match_score",
    "normalize_phrase_for_match",
    "normalize_query_family_key",
    "normalize_signal_label",
    "ordered_intent_tokens",
    "profile_brand_label",
    "profile_topic_label",
    "semantic_pluralize",
    "tokenize_intent_text",
]
