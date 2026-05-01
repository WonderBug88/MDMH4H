"""Entity-index helpers for Fulcrum routing."""

from __future__ import annotations

from typing import Any, Callable

from psycopg2.extras import RealDictCursor

from app.fulcrum.platform import get_pg_conn, normalize_store_hash


def profile_matches_cluster(profile: dict[str, Any] | None, cluster: str | None) -> bool:
    if not cluster:
        return True
    cluster_key = (cluster or "").strip().lower()
    if not profile:
        return False
    cluster_profile = profile.get("cluster_profile") or {}
    primary = (cluster_profile.get("primary") or "").strip().lower()
    clusters = {value.strip().lower() for value in (cluster_profile.get("clusters") or []) if value}
    return cluster_key == primary or cluster_key in clusters


def load_all_store_product_profiles(
    store_hash: str,
    cluster: str | None = None,
    *,
    profile_matches_cluster_fn: Callable[[dict[str, Any] | None, str | None], bool],
    tokenize_intent_text_fn: Callable[[str | None], set[str]],
    extract_attribute_terms_fn: Callable[[str | None], dict[str, set[str]]],
) -> list[dict[str, Any]]:
    sql = """
        SELECT
            bc_product_id,
            product_name,
            product_url,
            brand_name,
            search_keywords,
            source_data,
            attribute_profile,
            cluster_profile,
            canonical_group_key,
            is_canonical_target,
            is_visible,
            availability,
            is_price_hidden,
            eligible_for_routing
        FROM app_runtime.store_product_profiles
        WHERE store_hash = %s;
    """
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (normalize_store_hash(store_hash),))
            rows = [dict(row) for row in cur.fetchall()]

    profiles: list[dict[str, Any]] = []
    for row in rows:
        raw_attrs = row.get("attribute_profile") or {}
        attrs = {bucket: set(values or []) for bucket, values in raw_attrs.items()}
        source_data = row.get("source_data") or {}
        option_pairs = source_data.get("option_pairs") or []
        combined_text = " ".join(
            filter(
                None,
                [
                    row.get("product_name") or "",
                    row.get("brand_name") or "",
                    row.get("search_keywords") or "",
                    row.get("product_url") or "",
                    " ".join(pair.get("name") or "" for pair in option_pairs),
                    " ".join(pair.get("value") or "" for pair in option_pairs),
                ],
            )
        )
        profile = {
            "bc_product_id": row.get("bc_product_id"),
            "url": row.get("product_url") or "",
            "name": row.get("product_name") or "",
            "brand_name": row.get("brand_name") or "",
            "search_keywords": row.get("search_keywords") or "",
            "tokens": tokenize_intent_text_fn(combined_text),
            "attributes": attrs or extract_attribute_terms_fn(combined_text),
            "cluster_profile": row.get("cluster_profile") or {},
            "source_data": source_data,
            "canonical_group_key": row.get("canonical_group_key") or "",
            "is_canonical_target": bool(row.get("is_canonical_target", True)),
            "is_visible": bool(row.get("is_visible", True)),
            "availability": row.get("availability") or "",
            "is_price_hidden": bool(row.get("is_price_hidden", False)),
            "eligible_for_routing": bool(row.get("eligible_for_routing", True)),
        }
        if not cluster or profile_matches_cluster_fn(profile, cluster):
            profiles.append(profile)
    return profiles


def build_unified_entity_index(
    store_hash: str,
    cluster: str | None = None,
    *,
    load_all_store_product_profiles_fn: Callable[[str, str | None], list[dict[str, Any]]],
    load_store_category_profiles_fn: Callable[..., dict[str, dict[str, Any]]],
    load_store_brand_profiles_fn: Callable[[str], dict[str, dict[str, Any]]],
    load_store_content_profiles_fn: Callable[[str], dict[str, dict[str, Any]]],
    normalize_storefront_path_fn: Callable[[Any], str],
    profile_matches_cluster_fn: Callable[[dict[str, Any] | None, str | None], bool],
    load_ga4_page_metrics_fn: Callable[[list[str], int], dict[str, dict[str, Any]]],
) -> dict[str, Any]:
    product_profiles = load_all_store_product_profiles_fn(store_hash, cluster)
    eligible_product_sources = [profile for profile in product_profiles if profile.get("eligible_for_routing", True)]
    target_product_profiles = [
        profile
        for profile in eligible_product_sources
        if profile.get("is_canonical_target", True)
    ]
    category_profiles = [
        profile
        for profile in load_store_category_profiles_fn(store_hash, canonical_only=True).values()
        if profile.get("eligible_for_routing", True) and (not cluster or profile_matches_cluster_fn(profile, cluster))
    ]
    source_category_profiles = [
        profile
        for profile in load_store_category_profiles_fn(store_hash, canonical_only=False).values()
        if profile.get("eligible_for_routing", True) and (not cluster or profile_matches_cluster_fn(profile, cluster))
    ]
    brand_profiles = [
        profile
        for profile in load_store_brand_profiles_fn(store_hash).values()
        if profile.get("eligible_for_routing", True) and (not cluster or profile_matches_cluster_fn(profile, cluster))
    ]
    content_profiles = [
        profile
        for profile in load_store_content_profiles_fn(store_hash, include_backlog=True).values()
        if profile.get("eligible_for_routing", True) and (not cluster or profile_matches_cluster_fn(profile, cluster))
    ]

    source_profiles: dict[str, dict[str, Any]] = {}
    for profile in eligible_product_sources:
        source_profiles[normalize_storefront_path_fn(profile.get("url"))] = {
            **profile,
            "entity_type": "product",
            "bc_entity_id": int(profile.get("bc_product_id") or 0),
        }
    for profile in source_category_profiles:
        source_profiles[normalize_storefront_path_fn(profile.get("url"))] = {
            **profile,
            "entity_type": "category",
            "bc_entity_id": int(profile.get("bc_category_id") or 0),
        }
    for profile in brand_profiles + content_profiles:
        normalized_url = normalize_storefront_path_fn(profile.get("url"))
        if not normalized_url:
            continue
        source_profiles[normalized_url] = {
            **profile,
            "entity_type": profile.get("entity_type") or "content",
            "bc_entity_id": int(profile.get("bc_entity_id") or 0),
        }

    target_entities: list[dict[str, Any]] = []
    for profile in target_product_profiles:
        target_entities.append(
            {
                **profile,
                "entity_type": "product",
                "bc_entity_id": int(profile.get("bc_product_id") or 0),
            }
        )
    for profile in category_profiles:
        target_entities.append(
            {
                **profile,
                "entity_type": "category",
                "bc_entity_id": int(profile.get("bc_category_id") or 0),
            }
        )
    for profile in brand_profiles + content_profiles:
        target_entities.append(profile)

    ga4_metrics = load_ga4_page_metrics_fn(
        list(source_profiles.keys()) + [entity.get("url") for entity in target_entities],
        90,
    )
    for profile in source_profiles.values():
        profile["ga4_metrics"] = ga4_metrics.get(normalize_storefront_path_fn(profile.get("url")), {})
    for profile in target_entities:
        profile["ga4_metrics"] = ga4_metrics.get(normalize_storefront_path_fn(profile.get("url")), {})

    return {
        "sources": source_profiles,
        "targets": target_entities,
    }


__all__ = [
    "build_unified_entity_index",
    "load_all_store_product_profiles",
    "profile_matches_cluster",
]
