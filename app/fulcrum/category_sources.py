"""Category-source and category-competition helpers for Fulcrum."""

from __future__ import annotations

from typing import Any, Callable


def store_category_competition_enabled(
    store_hash: str,
    cluster: str | None,
    *,
    normalize_store_hash_fn: Callable[[str | None], str],
    store_category_competition: dict[str, set[str]],
) -> bool:
    normalized_hash = normalize_store_hash_fn(store_hash)
    cluster_key = (cluster or "").strip().lower()
    return bool(cluster_key and cluster_key in store_category_competition.get(normalized_hash, set()))


def load_canonical_cluster_categories(
    store_hash: str,
    cluster: str | None,
    *,
    load_store_category_profiles_fn: Callable[..., dict[str, dict[str, Any]]],
    profile_matches_cluster_fn: Callable[[dict[str, Any] | None, str | None], bool],
    category_competition_url_hints: dict[str, tuple[str, ...]],
) -> list[dict[str, Any]]:
    if not cluster:
        return []
    profiles = load_store_category_profiles_fn(store_hash, canonical_only=True)
    rows = [profile for profile in profiles.values() if profile_matches_cluster_fn(profile, cluster)]
    hints = category_competition_url_hints.get((cluster or "").strip().lower(), ())
    if hints:
        hinted_rows = [
            profile
            for profile in rows
            if any(hint in (profile.get("url") or "").strip("/").lower() for hint in hints)
        ]
        if hinted_rows:
            rows = hinted_rows
    rows.sort(
        key=lambda profile: (
            not bool(profile.get("is_canonical_target")),
            next(
                (
                    idx
                    for idx, hint in enumerate(hints)
                    if hint in (profile.get("url") or "").strip("/").lower()
                ),
                999,
            ),
            len(profile.get("name") or ""),
            len(profile.get("url") or ""),
        )
    )
    return rows


def category_anchor_label_for_cluster(
    cluster: str | None,
    category_profile: dict[str, Any],
    *,
    category_competition_specific_hints: dict[str, tuple[dict[str, Any], ...]],
    category_cluster_labels: dict[str, str],
    normalize_anchor_text_fn: Callable[[str], str],
    label_from_target_url_fn: Callable[[str], str],
) -> str:
    cluster_key = (cluster or "").strip().lower()
    category_name = normalize_anchor_text_fn(category_profile.get("name") or "")
    category_url = (category_profile.get("url") or "").strip("/").lower()
    hints = category_competition_specific_hints.get(cluster_key, ())
    if hints:
        fallback_fragment = hints[-1]["fragment"]
        specific_fragments = [hint["fragment"] for hint in hints[:-1]]
        if any(fragment in category_url for fragment in specific_fragments):
            return category_name or label_from_target_url_fn(category_profile.get("url") or "")
        if fallback_fragment in category_url and category_name and len(category_name) <= 26:
            return category_name
    preferred = category_cluster_labels.get(cluster_key)
    if preferred:
        return preferred
    return category_name or label_from_target_url_fn(category_profile.get("url") or "") or "Shop Category"


def category_competition_specificity_bonus(
    cluster: str | None,
    source_row: dict[str, Any],
    source_profile: dict[str, Any],
    category_profile: dict[str, Any],
    *,
    category_competition_specific_hints: dict[str, tuple[dict[str, Any], ...]],
    tokenize_intent_text_fn: Callable[[str | None], set[str]],
) -> tuple[float, str | None]:
    cluster_key = (cluster or "").strip().lower()
    hints = category_competition_specific_hints.get(cluster_key, ())
    if not hints:
        return 0.0, None

    category_url = (category_profile.get("url") or "").strip("/").lower()
    query_tokens = tokenize_intent_text_fn(source_row.get("example_query"))
    source_tokens = set(source_profile.get("tokens") or set())
    source_subclusters = {
        str(value).strip().lower()
        for value in ((source_profile.get("cluster_profile") or {}).get("subclusters") or [])
        if value
    }
    combined_tokens = query_tokens | source_tokens

    preferred_fragment = None
    specific_match = False
    for hint in hints:
        token_hints = {str(value).strip().lower() for value in (hint.get("tokens") or set()) if value}
        subcluster_hints = {str(value).strip().lower() for value in (hint.get("subclusters") or set()) if value}
        if (token_hints and combined_tokens & token_hints) or (subcluster_hints and source_subclusters & subcluster_hints):
            preferred_fragment = hint["fragment"]
            specific_match = True
            break

    if not preferred_fragment:
        preferred_fragment = hints[-1]["fragment"]

    if preferred_fragment in category_url:
        return (18.0 if specific_match else 10.0), preferred_fragment

    hinted_urls = [hint["fragment"] for hint in hints if hint["fragment"] in category_url]
    if specific_match and hinted_urls:
        return -8.0, preferred_fragment
    if hinted_urls:
        return -3.0, preferred_fragment
    return 0.0, preferred_fragment


def build_pdp_category_competition_candidate(
    store_hash: str,
    cluster: str | None,
    source_row: dict[str, Any],
    source_profile: dict[str, Any],
    broad_query_profile: dict[str, Any],
    category_profile: dict[str, Any],
    *,
    build_intent_profile_fn: Callable[..., dict[str, Any]],
    entity_storage_id_fn: Callable[[str | None, int | None], int | None],
    category_competition_specificity_bonus_fn: Callable[..., tuple[float, str | None]],
    category_anchor_label_for_cluster_fn: Callable[[str | None, dict[str, Any]], str],
) -> dict[str, Any] | None:
    category_intent = build_intent_profile_fn(
        source_name=source_row.get("source_name"),
        source_url=source_row.get("source_url"),
        target_name=category_profile.get("name"),
        target_url=category_profile.get("url") or "",
        example_query=source_row.get("example_query"),
        relation_type="category",
        hit_count=int(source_row.get("hit_count") or 0),
        source_profile=source_profile,
        target_profile=category_profile,
        used_labels=set(),
    )
    if not category_intent.get("passes"):
        return None

    source_storage_id = entity_storage_id_fn("product", int(source_row["source_product_id"]))
    target_storage_id = entity_storage_id_fn("category", int(category_profile["bc_category_id"]))
    specificity_bonus, preferred_fragment = category_competition_specificity_bonus_fn(
        cluster=cluster,
        source_row=source_row,
        source_profile=source_profile,
        category_profile=category_profile,
    )
    score = min(
        100.0,
        max(float(category_intent["score"]), float(broad_query_profile["score"]) + 6.0) + specificity_bonus,
    )
    reason_summary = (
        "Broad product-family query prefers category competition; "
        f"{category_intent['reason_summary']}"
    )
    if preferred_fragment and preferred_fragment in ((category_profile.get("url") or "").strip("/").lower()):
        reason_summary += "; canonical subcategory aligns with the query"
    return {
        "source_product_id": source_storage_id,
        "source_name": source_row.get("source_name"),
        "source_url": source_row.get("source_url"),
        "target_product_id": target_storage_id,
        "target_name": category_profile.get("name"),
        "target_url": category_profile.get("url"),
        "relation_type": "category",
        "example_query": source_row.get("example_query"),
        "anchor_label": category_anchor_label_for_cluster_fn(cluster, category_profile),
        "hit_count": int(source_row.get("hit_count") or 0),
        "score": round(score, 2),
        "source_entity_type": "product",
        "target_entity_type": "category",
        "metadata": {
            "cluster": cluster or "all",
            "source_entity_type": "product",
            "target_entity_type": "category",
            "source_bc_entity_id": int(source_row["source_product_id"]),
            "target_bc_entity_id": int(category_profile["bc_category_id"]),
            "topic_key": broad_query_profile["topic_key"],
            "anchor_label_source": "cluster_category_label",
            "anchor_quality": category_intent["anchor_quality"],
            "reason_summary": reason_summary,
            "reasons": [
                "broad product-family query prefers category competition",
                *(
                    ["canonical subcategory aligns with the query"]
                    if preferred_fragment and preferred_fragment in ((category_profile.get("url") or "").strip("/").lower())
                    else []
                ),
                *(category_intent.get("reasons") or [])[:3],
            ],
            "shared_tokens": category_intent["shared_tokens"],
            "query_target_tokens": category_intent["query_target_tokens"],
            "query_source_tokens": category_intent["query_source_tokens"],
            "attributes": category_intent["attributes"],
            "source_primary_cluster": broad_query_profile["source_primary_cluster"],
            "target_primary_cluster": (category_profile.get("cluster_profile") or {}).get("primary", ""),
            "query_intent_scope": broad_query_profile["query_intent_scope"],
            "preferred_entity_type": broad_query_profile["preferred_entity_type"],
            "block_type": "pdp_category_competition",
            "category_competition_enabled": True,
        },
    }


def build_category_descendants(category_profiles: list[dict[str, Any]]) -> dict[int, set[int]]:
    children: dict[int, list[int]] = {}
    for profile in category_profiles:
        parent_id = profile.get("parent_category_id")
        category_id = profile.get("bc_category_id")
        if parent_id and category_id:
            children.setdefault(int(parent_id), []).append(int(category_id))

    descendants: dict[int, set[int]] = {}

    def walk(category_id: int) -> set[int]:
        if category_id in descendants:
            return descendants[category_id]
        result = {category_id}
        for child_id in children.get(category_id, []):
            result |= walk(child_id)
        descendants[category_id] = result
        return result

    for profile in category_profiles:
        category_id = profile.get("bc_category_id")
        if category_id:
            walk(int(category_id))
    return descendants


def shared_subclusters(source_profile: dict[str, Any], target_profile: dict[str, Any]) -> set[str]:
    return set((source_profile.get("cluster_profile") or {}).get("subclusters") or []) & set(
        (target_profile.get("cluster_profile") or {}).get("subclusters") or []
    )


def generate_category_source_candidates(
    store_hash: str,
    cluster: str | None = None,
    *,
    load_store_category_profiles_fn: Callable[..., dict[str, dict[str, Any]]],
    load_all_store_product_profiles_fn: Callable[[str, str | None], list[dict[str, Any]]],
    profile_matches_cluster_fn: Callable[[dict[str, Any] | None, str | None], bool],
    build_category_descendants_fn: Callable[[list[dict[str, Any]]], dict[int, set[int]]],
    entity_storage_id_fn: Callable[[str | None, int | None], int | None],
    shared_subclusters_fn: Callable[[dict[str, Any], dict[str, Any]], set[str]],
    build_intent_profile_fn: Callable[..., dict[str, Any]],
    select_category_product_anchor_label_fn: Callable[..., dict[str, Any]],
) -> list[dict[str, Any]]:
    category_profiles_by_url = load_store_category_profiles_fn(store_hash, canonical_only=True)
    category_profiles = [
        profile
        for profile in category_profiles_by_url.values()
        if profile_matches_cluster_fn(profile, cluster)
    ]
    if not category_profiles:
        return []

    product_profiles = load_all_store_product_profiles_fn(store_hash, cluster)
    descendant_map = build_category_descendants_fn(category_profiles)
    rows: list[dict[str, Any]] = []

    for source_profile in category_profiles:
        source_category_id = int(source_profile["bc_category_id"])
        source_storage_id = entity_storage_id_fn("category", source_category_id)
        used_labels: set[str] = set()

        related_categories: list[dict[str, Any]] = []
        for target_profile in category_profiles:
            target_category_id = int(target_profile["bc_category_id"])
            if target_category_id == source_category_id:
                continue

            relation_bonus = 0
            if target_profile.get("parent_category_id") == source_category_id:
                relation_bonus = 28
                hit_count = 4
            elif source_profile.get("parent_category_id") == target_category_id:
                relation_bonus = 18
                hit_count = 3
            elif (
                source_profile.get("parent_category_id")
                and source_profile.get("parent_category_id") == target_profile.get("parent_category_id")
            ):
                relation_bonus = 22
                hit_count = 3
            elif shared_subclusters_fn(source_profile, target_profile):
                relation_bonus = 14
                hit_count = 2
            else:
                relation_bonus = 8
                hit_count = 1

            profile = build_intent_profile_fn(
                source_name=source_profile.get("name"),
                source_url=source_profile.get("url"),
                target_name=target_profile.get("name"),
                target_url=target_profile.get("url"),
                example_query=target_profile.get("name") or source_profile.get("name"),
                relation_type="category",
                hit_count=hit_count,
                source_profile=source_profile,
                target_profile=target_profile,
                used_labels=used_labels,
            )
            score = min(100.0, round(profile["score"] + relation_bonus, 2))
            if score < 60:
                continue
            related_categories.append(
                {
                    "source_product_id": source_storage_id,
                    "source_name": source_profile.get("name"),
                    "source_url": source_profile.get("url"),
                    "target_product_id": entity_storage_id_fn("category", target_category_id),
                    "target_name": target_profile.get("name"),
                    "target_url": target_profile.get("url"),
                    "relation_type": "category",
                    "example_query": target_profile.get("name") or source_profile.get("name"),
                    "anchor_label": profile["anchor_label"],
                    "hit_count": hit_count,
                    "score": score,
                    "source_entity_type": "category",
                    "target_entity_type": "category",
                    "metadata": {
                        "cluster": cluster or "all",
                        "source_entity_type": "category",
                        "target_entity_type": "category",
                        "source_bc_entity_id": source_category_id,
                        "target_bc_entity_id": target_category_id,
                        "topic_key": profile["topic_key"],
                        "anchor_label_source": profile["anchor_label_source"],
                        "anchor_quality": profile["anchor_quality"],
                        "reason_summary": profile["reason_summary"],
                        "reasons": profile["reasons"],
                        "shared_tokens": profile["shared_tokens"],
                        "query_target_tokens": profile["query_target_tokens"],
                        "query_source_tokens": profile["query_source_tokens"],
                        "attributes": profile["attributes"],
                        "block_type": "related_categories",
                        "source_primary_cluster": profile["source_primary_cluster"],
                        "target_primary_cluster": profile["target_primary_cluster"],
                        "query_intent_scope": profile["query_intent_scope"],
                        "preferred_entity_type": profile["preferred_entity_type"],
                    },
                }
            )

        related_categories.sort(key=lambda row: (-float(row["score"]), row["target_name"] or ""))
        selected_categories = related_categories[:3]
        used_labels |= {row["anchor_label"] for row in selected_categories}

        matching_products: list[dict[str, Any]] = []
        eligible_category_ids = descendant_map.get(source_category_id, {source_category_id})
        for target_profile in product_profiles:
            target_product_id = int(target_profile["bc_product_id"])
            product_category_ids = {
                int(category_id)
                for category_id in (((target_profile.get("source_data") or {}).get("product") or {}).get("categories") or [])
            }
            direct_membership = bool(product_category_ids & eligible_category_ids)
            same_subcluster = bool(shared_subclusters_fn(source_profile, target_profile))
            if not direct_membership and not same_subcluster and not profile_matches_cluster_fn(target_profile, cluster):
                continue

            membership_bonus = 22 if direct_membership else (12 if same_subcluster else 6)
            hit_count = 4 if direct_membership else (3 if same_subcluster else 2)
            profile = build_intent_profile_fn(
                source_name=source_profile.get("name"),
                source_url=source_profile.get("url"),
                target_name=target_profile.get("name"),
                target_url=target_profile.get("url"),
                example_query=source_profile.get("name"),
                relation_type="canonical",
                hit_count=hit_count,
                source_profile=source_profile,
                target_profile=target_profile,
                used_labels=used_labels,
            )
            score = min(100.0, round(profile["score"] + membership_bonus, 2))
            if score < 58:
                continue
            category_product_anchor = select_category_product_anchor_label_fn(
                target_url=target_profile.get("url"),
                target_name=target_profile.get("name"),
                source_name=source_profile.get("name"),
                source_profile=source_profile,
                target_profile=target_profile,
                used_labels=used_labels,
            )
            matching_products.append(
                {
                    "source_product_id": source_storage_id,
                    "source_name": source_profile.get("name"),
                    "source_url": source_profile.get("url"),
                    "target_product_id": entity_storage_id_fn("product", target_product_id),
                    "target_name": target_profile.get("name"),
                    "target_url": target_profile.get("url"),
                    "relation_type": "canonical",
                    "example_query": source_profile.get("name"),
                    "anchor_label": category_product_anchor["label"],
                    "hit_count": hit_count,
                    "score": score,
                    "source_entity_type": "category",
                    "target_entity_type": "product",
                    "metadata": {
                        "cluster": cluster or "all",
                        "source_entity_type": "category",
                        "target_entity_type": "product",
                        "source_bc_entity_id": source_category_id,
                        "target_bc_entity_id": target_product_id,
                        "topic_key": profile["topic_key"],
                        "anchor_label_source": category_product_anchor["label_source"],
                        "anchor_quality": category_product_anchor["quality"],
                        "reason_summary": profile["reason_summary"],
                        "reasons": profile["reasons"],
                        "shared_tokens": profile["shared_tokens"],
                        "query_target_tokens": profile["query_target_tokens"],
                        "query_source_tokens": profile["query_source_tokens"],
                        "attributes": profile["attributes"],
                        "block_type": "matching_products",
                        "direct_membership": direct_membership,
                        "source_primary_cluster": profile["source_primary_cluster"],
                        "target_primary_cluster": profile["target_primary_cluster"],
                        "query_intent_scope": profile["query_intent_scope"],
                        "preferred_entity_type": profile["preferred_entity_type"],
                    },
                }
            )

        matching_products.sort(key=lambda row: (-float(row["score"]), row["target_name"] or ""))
        selected_products: list[dict[str, Any]] = []
        seen_product_labels: set[str] = set()
        for row in matching_products:
            anchor_key = (row.get("anchor_label") or "").strip().lower()
            if not anchor_key or anchor_key in seen_product_labels:
                continue
            seen_product_labels.add(anchor_key)
            selected_products.append(row)
            if len(selected_products) >= 4:
                break
        rows.extend(selected_categories)
        rows.extend(selected_products)

    return rows


__all__ = [
    "build_category_descendants",
    "build_pdp_category_competition_candidate",
    "category_anchor_label_for_cluster",
    "category_competition_specificity_bonus",
    "generate_category_source_candidates",
    "load_canonical_cluster_categories",
    "shared_subclusters",
    "store_category_competition_enabled",
]
