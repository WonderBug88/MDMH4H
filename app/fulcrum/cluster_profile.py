"""Cluster-profile helpers for Fulcrum entity classification."""

from __future__ import annotations

from typing import Any, Callable


def build_cluster_profile(
    product_name: str | None,
    product_url: str | None,
    brand_name: str | None,
    search_keywords: str | None,
    attribute_profile: dict[str, set[str]],
    *,
    tokenize_intent_text_fn: Callable[[str | None], set[str]],
    shower_curtain_subcluster_map: dict[str, str],
    towel_subcluster_map: dict[str, str],
    topic_priority: set[str],
) -> dict[str, Any]:
    text = " ".join(filter(None, [product_name or "", product_url or "", brand_name or "", search_keywords or ""]))
    tokens = tokenize_intent_text_fn(text)
    clusters: list[str] = []
    subclusters: set[str] = set()

    if {"rollaway", "fold-up", "fold-out", "portable", "mobile", "sleeper"} & tokens:
        clusters.append("rollaway")
    if {"luggage", "bellman", "bellhop", "carrier", "cart", "rack"} & tokens:
        clusters.append("luggage")

    shower_forms = set(attribute_profile.get("form") or set()) & set(shower_curtain_subcluster_map.keys())
    shower_curtain_tokens = {"shower", "curtain", "hookless"} & tokens
    shower_material_tokens = {"vinyl", "polyester", "fabric", "laminated"} & (
        tokens | set(attribute_profile.get("material") or set())
    )
    if shower_forms or shower_curtain_tokens:
        clusters.append("shower-curtains")
        subclusters.update(shower_curtain_subcluster_map[form] for form in shower_forms)
        if "hookless" in tokens:
            subclusters.add("hookless-shower-curtains")
        if "fabric" in shower_material_tokens or "polyester" in shower_material_tokens:
            subclusters.add("fabric-shower-curtains")
        if "vinyl" in shower_material_tokens:
            subclusters.add("vinyl-shower-curtains")
        if "laminated" in shower_material_tokens:
            subclusters.add("laminated-shower-curtains")

    towel_forms = set(attribute_profile.get("form") or set()) & set(towel_subcluster_map.keys())
    towel_tokens = {"towel", "washcloth", "bath-mat", "pool-towel"} & tokens
    bath_only_signal = "bath" in tokens and not (shower_forms or shower_curtain_tokens)
    if towel_forms or towel_tokens or bath_only_signal:
        clusters.append("towels")
        subclusters.update(towel_subcluster_map[form] for form in towel_forms)

    if {"sheet", "duvet", "comforter", "blanket", "linen", "pillow", "bedspread"} & tokens:
        clusters.append("bedding")

    if not clusters:
        topic_tokens = [token for token in tokens if token in topic_priority]
        if topic_tokens:
            clusters.append(topic_tokens[0])

    primary_cluster = clusters[0] if clusters else ""
    return {
        "primary": primary_cluster,
        "clusters": list(dict.fromkeys(clusters)),
        "subclusters": sorted(subclusters),
    }


__all__ = ["build_cluster_profile"]
