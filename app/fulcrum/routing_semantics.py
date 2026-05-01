"""Routing semantics helpers for Fulcrum query-gate evaluation."""

from __future__ import annotations

from typing import Any, Callable


def gate_row_query_signal_context(gate_row: dict[str, Any]) -> dict[str, Any] | None:
    metadata = gate_row.get("metadata") or {}
    resolved_signals = metadata.get("resolved_signals") if isinstance(metadata, dict) else None
    return resolved_signals if isinstance(resolved_signals, dict) else None


def gate_row_semantics_analysis(
    gate_row: dict[str, Any],
    store_hash: str,
    *,
    signal_library: dict[str, list[dict[str, Any]]] | None = None,
    gate_row_query_signal_context_fn: Callable[[dict[str, Any]], dict[str, Any] | None],
    resolve_query_signal_context_fn: Callable[..., dict[str, Any] | None],
    build_query_semantics_analysis_fn: Callable[..., dict[str, Any]],
) -> dict[str, Any]:
    metadata = gate_row.get("metadata") or {}
    semantics = metadata.get("semantics_analysis") if isinstance(metadata, dict) else None
    if isinstance(semantics, dict) and semantics.get("normalized_query"):
        return dict(semantics)
    resolved_signals = gate_row_query_signal_context_fn(gate_row) or resolve_query_signal_context_fn(
        store_hash=store_hash,
        example_query=gate_row.get("representative_query"),
        signal_library=signal_library,
        source_profile=None,
        target_profile=None,
    )
    return build_query_semantics_analysis_fn(
        store_hash=store_hash,
        example_query=gate_row.get("representative_query"),
        resolved_signals=resolved_signals,
        signal_library=signal_library,
    )


def gate_row_current_page_snapshot(
    gate_row: dict[str, Any],
    *,
    source_profile: dict[str, Any] | None = None,
    normalize_storefront_path_fn: Callable[[Any], str],
) -> dict[str, Any] | None:
    source_profile = source_profile or {}
    entity_type = (
        (source_profile.get("entity_type") or gate_row.get("source_entity_type") or gate_row.get("current_page_type") or "product")
        .strip()
        .lower()
        or "product"
    )
    entity_id = int(source_profile.get("bc_entity_id") or gate_row.get("source_entity_id") or 0)
    source_url = normalize_storefront_path_fn(source_profile.get("url") or gate_row.get("source_url"))
    if not entity_id and not source_url:
        return None
    return {
        "entity_type": entity_type,
        "entity_id": entity_id,
        "name": source_profile.get("name") or gate_row.get("source_name") or "",
        "url": source_profile.get("url") or gate_row.get("source_url") or "",
        "score": 100.0,
        "raw_score": 100.0,
        "anchor_label": "",
        "reason_summary": "Current GSC landing page remains the safest route",
        "type_fit_reason": "current page preserved",
        "is_current_page": True,
        "manual_override": False,
    }


def semantics_target_block_reason(
    semantics_analysis: dict[str, Any],
    target_profile: dict[str, Any] | None,
    *,
    tokenize_intent_text_fn: Callable[[str | None], set[str]],
    normalize_signal_label_fn: Callable[[str | None], str],
    semantic_pluralize_fn: Callable[[str], str],
) -> str | None:
    target_profile = target_profile or {}
    target_type = (target_profile.get("entity_type") or "product").strip().lower() or "product"
    if target_type not in set(semantics_analysis.get("eligible_page_types") or []):
        return f"page type `{target_type}` is blocked by semantics"

    query_tokens = set(tokenize_intent_text_fn(semantics_analysis.get("normalized_query")))
    target_tokens = set(target_profile.get("tokens") or tokenize_intent_text_fn(f"{target_profile.get('name') or ''} {target_profile.get('url') or ''}"))
    core_target_tokens = tokenize_intent_text_fn(f"{target_profile.get('name') or ''} {target_profile.get('url') or ''}")
    for rule in semantics_analysis.get("constraint_rules") or []:
        kind = (rule.get("kind") or "").strip().lower()
        if kind == "suppress_accessory_family":
            unless_tokens = set(rule.get("unless_query_tokens") or [])
            blocked_tokens = set(rule.get("blocked_tokens") or [])
            if query_tokens & unless_tokens:
                continue
            if target_tokens & blocked_tokens:
                return rule.get("message") or "accessory family is suppressed for this query"
        elif kind == "require_taxonomy_tokens":
            allowed_target_tokens = set(rule.get("allowed_target_tokens") or [])
            blocked_target_tokens = set(rule.get("blocked_target_tokens") or [])
            if blocked_target_tokens and target_tokens & blocked_target_tokens:
                return rule.get("message") or "generic family is blocked by subtype semantics"
            if allowed_target_tokens and not (target_tokens & allowed_target_tokens):
                return rule.get("message") or "target does not preserve the subtype alias"
        elif kind == "block_brand_without_exact_phrase" and target_type == "brand":
            return rule.get("message") or "brand routing is blocked without corroboration"
        elif kind == "block_pdp_without_identity_phrase" and target_type == "product":
            return rule.get("message") or "product routing is blocked by semantics"
        elif kind == "prefer_brand_when_family_has_multiple_products":
            if target_type == "product":
                return rule.get("message") or "multiple brand-family products make product routing too specific"
        elif kind == "require_head_term_presence" and target_type in {"product", "category"}:
            head_term = normalize_signal_label_fn(rule.get("head_term"))
            if head_term and head_term not in core_target_tokens and semantic_pluralize_fn(head_term) not in core_target_tokens:
                return rule.get("message") or "target drifted away from the head product family"
        elif kind == "require_modifier_presence" and target_type in {"product", "category"}:
            modifier_tokens = set(rule.get("modifier_tokens") or [])
            if modifier_tokens and (
                (len(modifier_tokens) <= 2 and not modifier_tokens <= core_target_tokens)
                or (len(modifier_tokens) > 2 and not (core_target_tokens & modifier_tokens))
            ):
                return rule.get("message") or "target dropped the key query modifier"
    return None


def apply_semantics_control_to_ranked_targets(
    gate_row: dict[str, Any],
    ranked_targets: list[dict[str, Any]],
    *,
    store_hash: str,
    source_profile: dict[str, Any] | None = None,
    target_entities_by_key: dict[tuple[str, int], dict[str, Any]] | None = None,
    source_profiles: dict[str, dict[str, Any]] | None = None,
    target_entities: list[dict[str, Any]] | None = None,
    overrides: dict[tuple[str, str], dict[str, Any]] | None = None,
    review_feedback_maps: dict[str, dict[Any, dict[str, int]]] | None = None,
    signal_library: dict[str, list[dict[str, Any]]] | None = None,
    gate_row_semantics_analysis_fn: Callable[..., dict[str, Any]],
    gate_row_current_page_snapshot_fn: Callable[..., dict[str, Any] | None],
    semantics_target_block_reason_fn: Callable[[dict[str, Any], dict[str, Any] | None], str | None],
    rank_target_options_for_gate_row_fn: Callable[..., list[dict[str, Any]]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    source_profile = source_profile or {}
    semantics_analysis = gate_row_semantics_analysis_fn(gate_row, store_hash, signal_library=signal_library)
    current_page = gate_row_current_page_snapshot_fn(gate_row, source_profile=source_profile)
    current_key = (
        (current_page.get("entity_type") or "").strip().lower(),
        int(current_page.get("entity_id") or 0),
    ) if current_page else ("", 0)

    if not ranked_targets:
        semantics_analysis["judge_verdict"] = "hold"
        semantics_analysis["resolver_invoked"] = False
        if current_page:
            return [current_page], semantics_analysis
        return ranked_targets, semantics_analysis

    proposed = ranked_targets[0]
    proposed_key = (
        (proposed.get("entity_type") or "").strip().lower(),
        int(proposed.get("entity_id") or 0),
    )
    if current_page and proposed_key == current_key:
        semantics_analysis["judge_verdict"] = "allow"
        semantics_analysis["resolver_invoked"] = False
        return ranked_targets, semantics_analysis

    target_profile = {}
    if target_entities_by_key is not None:
        target_profile = dict(target_entities_by_key.get(proposed_key) or {})
    block_reason = semantics_target_block_reason_fn(semantics_analysis, target_profile)
    if not block_reason and semantics_analysis.get("ambiguity_level") == "high" and proposed.get("entity_type") in {"product", "brand"}:
        block_reason = "high ambiguity suppresses product and brand route changes"

    if not block_reason:
        semantics_analysis["judge_verdict"] = "allow"
        semantics_analysis["resolver_invoked"] = False
        return ranked_targets, semantics_analysis

    semantics_analysis["judge_verdict"] = "reject"
    semantics_analysis["resolver_invoked"] = True
    semantics_analysis.setdefault("negative_constraints", []).append(block_reason)

    fallback_candidates: list[dict[str, Any]] = []
    current_block_reason: str | None = None
    if current_page:
        current_block_reason = semantics_target_block_reason_fn(semantics_analysis, source_profile)
        if not current_block_reason and semantics_analysis.get("ambiguity_level") == "high" and current_page.get("entity_type") in {"product", "brand"}:
            current_block_reason = "high ambiguity suppresses product and brand route changes"
    if current_page and not current_block_reason:
        fallback_candidates.append(current_page)

    resolver_pool = ranked_targets[1:]
    if target_entities and source_profiles is not None:
        resolver_pool = rank_target_options_for_gate_row_fn(
            gate_row=gate_row,
            source_profiles=source_profiles,
            target_entities=target_entities,
            overrides=overrides,
            review_feedback_maps=review_feedback_maps,
            limit=max(25, len(ranked_targets)),
            apply_semantics_control=False,
            semantics_analysis=semantics_analysis,
            target_entities_by_key=target_entities_by_key,
        )
    for candidate in resolver_pool:
        if current_page and (
            (candidate.get("entity_type") or "").strip().lower(),
            int(candidate.get("entity_id") or 0),
        ) == current_key:
            continue
        candidate_profile = {}
        if target_entities_by_key is not None:
            candidate_profile = dict(
                target_entities_by_key.get(
                    (
                        (candidate.get("entity_type") or "").strip().lower(),
                        int(candidate.get("entity_id") or 0),
                    )
                )
                or {}
            )
        if semantics_target_block_reason_fn(semantics_analysis, candidate_profile):
            continue
        fallback_candidates.append(candidate)
        break

    if not fallback_candidates:
        semantics_analysis["judge_verdict"] = "hold"
        semantics_analysis["resolver_invoked"] = True
        return [], semantics_analysis

    unique_targets: list[dict[str, Any]] = []
    seen: set[tuple[str, int]] = set()
    for candidate in fallback_candidates + ranked_targets:
        key = ((candidate.get("entity_type") or "").strip().lower(), int(candidate.get("entity_id") or 0))
        if not all(key) or key in seen:
            continue
        seen.add(key)
        unique_targets.append(candidate)
        if len(unique_targets) >= 2:
            break
    return unique_targets, semantics_analysis


__all__ = [
    "apply_semantics_control_to_ranked_targets",
    "gate_row_current_page_snapshot",
    "gate_row_query_signal_context",
    "gate_row_semantics_analysis",
    "semantics_target_block_reason",
]
