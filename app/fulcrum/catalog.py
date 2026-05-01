"""Catalog sync operations for Fulcrum."""

from __future__ import annotations

import concurrent.futures
import json
import re
from typing import Any, Callable

from psycopg2.extras import execute_batch

from app.fulcrum.platform import (
    _flatten_option_pairs,
    _list_bc_paginated,
    fetch_store_brand_map,
    fetch_store_product_options,
    get_pg_conn,
    list_store_categories,
    normalize_store_hash,
)


def _load_product_options_by_product(
    store_hash: str,
    products: list[dict[str, Any]],
) -> dict[int, list[dict[str, Any]]]:
    options_by_product: dict[int, list[dict[str, Any]]] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        future_map = {
            executor.submit(fetch_store_product_options, store_hash, int(product["id"])): int(product["id"])
            for product in products
            if product.get("id") is not None
        }
        for future in concurrent.futures.as_completed(future_map):
            product_id = future_map[future]
            try:
                options_by_product[product_id] = future.result()
            except Exception:
                options_by_product[product_id] = []
    return options_by_product


def sync_store_catalog_profiles(
    store_hash: str,
    initiated_by: str | None = None,
    *,
    seed_store_attribute_buckets: Callable[[str], None],
    seed_store_cluster_rules: Callable[[str], None],
    sync_store_storefront_sites: Callable[[str, str | None], dict[str, Any]],
    normalize_storefront_path: Callable[[str | None], str],
    pick_canonical_product_ids: Callable[[list[dict[str, Any]]], set[int]],
    pick_canonical_category_ids: Callable[[list[dict[str, Any]]], set[int]],
    infer_bucket_from_option_name: Callable[[str | None, list[str] | None], tuple[str | None, float]],
    canonicalize_attribute_value: Callable[[str, str | None], str],
    extract_attribute_terms: Callable[[str | None], dict[str, set[str]]],
    slugify_value: Callable[[str | None], str],
    build_cluster_profile: Callable[..., dict[str, Any]],
    canonical_product_group_key: Callable[[dict[str, Any], set[str]], str],
    product_eligible_for_routing: Callable[[dict[str, Any]], bool],
    canonical_category_group_key: Callable[[dict[str, Any]], str],
    category_eligible_for_routing: Callable[[dict[str, Any]], bool],
    serialize_attribute_profile: Callable[[dict[str, set[str]]], dict[str, list[str]]],
    refresh_store_intent_signal_enrichments: Callable[[str, str | None], dict[str, Any]],
    normalize_mapping_review_statuses: Callable[[str], dict[str, int]],
    refresh_store_readiness: Callable[[str], dict[str, Any]],
) -> dict[str, Any]:
    normalized_hash = normalize_store_hash(store_hash)
    seed_store_attribute_buckets(normalized_hash)
    seed_store_cluster_rules(normalized_hash)
    storefront_sync = sync_store_storefront_sites(normalized_hash, initiated_by=initiated_by)

    products = _list_bc_paginated(
        normalized_hash,
        "/catalog/products",
        {"include_fields": "id,name,custom_url,search_keywords,brand_id,categories,is_visible,availability,is_price_hidden"},
    )
    categories = list_store_categories(normalized_hash)
    brand_map = fetch_store_brand_map(normalized_hash)
    known_product_urls = {
        normalize_storefront_path(((item.get("custom_url") or {}).get("url")) or "")
        for item in products
        if ((item.get("custom_url") or {}).get("url") or "").strip()
    }
    canonical_product_ids = pick_canonical_product_ids(products)
    canonical_category_ids = pick_canonical_category_ids(categories)
    options_by_product = _load_product_options_by_product(normalized_hash, products)

    option_name_rows: list[tuple[Any, ...]] = []
    option_value_rows: list[tuple[Any, ...]] = []
    product_profile_rows: list[tuple[Any, ...]] = []
    category_profile_rows: list[tuple[Any, ...]] = []
    synced_count = 0
    synced_categories = 0
    towel_profile_count = 0

    for product in products:
        product_id = int(product["id"])
        product_name = product.get("name") or ""
        product_url = ((product.get("custom_url") or {}).get("url") or "").strip()
        if not product_url:
            continue

        brand_name = brand_map.get(int(product.get("brand_id") or 0), "")
        search_keywords = product.get("search_keywords") or ""
        product_categories = [int(category_id) for category_id in (product.get("categories") or [])]
        options = options_by_product.get(product_id, [])
        option_pairs = _flatten_option_pairs(options)
        attribute_profile = extract_attribute_terms(" ".join([product_name, brand_name, search_keywords, product_url]))
        option_name_mapping_meta: dict[str, dict[str, Any]] = {}

        for raw_option_name, raw_option_value in option_pairs:
            bucket_key, bucket_confidence = infer_bucket_from_option_name(raw_option_name, [raw_option_value])
            if bucket_key:
                option_name_mapping_meta.setdefault(raw_option_name, {"bucket_key": bucket_key, "confidence": bucket_confidence})
                canonical_value = canonicalize_attribute_value(bucket_key, raw_option_value)
                if canonical_value:
                    attribute_profile.setdefault(bucket_key, set()).add(canonical_value)
                    option_value_rows.append(
                        (
                            normalized_hash,
                            raw_option_name,
                            raw_option_value,
                            bucket_key,
                            canonical_value,
                            round(bucket_confidence, 2),
                            json.dumps({"initiated_by": initiated_by or "fulcrum", "product_id": product_id}),
                        )
                    )

        for raw_option_name, mapping in option_name_mapping_meta.items():
            option_name_rows.append(
                (
                    normalized_hash,
                    raw_option_name,
                    mapping["bucket_key"],
                    slugify_value(raw_option_name),
                    round(mapping["confidence"], 2),
                    json.dumps({"initiated_by": initiated_by or "fulcrum"}),
                )
            )

        cluster_profile = build_cluster_profile(
            product_name=product_name,
            product_url=product_url,
            brand_name=brand_name,
            search_keywords=search_keywords,
            attribute_profile=attribute_profile,
        )
        if "towels" in (cluster_profile.get("clusters") or []):
            towel_profile_count += 1

        source_data = {
            "product": {
                "id": product_id,
                "name": product_name,
                "custom_url": product_url,
                "search_keywords": search_keywords,
                "brand_name": brand_name,
                "categories": product_categories,
                "is_visible": bool(product.get("is_visible", True)),
                "availability": product.get("availability") or "",
                "is_price_hidden": bool(product.get("is_price_hidden", False)),
            },
            "option_pairs": [{"name": name, "value": value} for name, value in option_pairs],
        }
        product_is_visible = bool(product.get("is_visible", True))
        product_availability = (product.get("availability") or "").strip().lower()
        product_is_price_hidden = bool(product.get("is_price_hidden", False))
        canonical_group_key = canonical_product_group_key(product, known_product_urls)
        combined_text = " ".join(
            [
                product_name,
                brand_name,
                search_keywords,
                product_url,
                " ".join(f"{name} {value}" for name, value in option_pairs),
            ]
        )
        eligible_for_routing = product_eligible_for_routing({**product, "product_name": product_name})
        product_profile_rows.append(
            (
                normalized_hash,
                product_id,
                product_name,
                product_url,
                brand_name,
                search_keywords,
                json.dumps(source_data),
                json.dumps(serialize_attribute_profile(attribute_profile)),
                json.dumps(cluster_profile),
                canonical_group_key,
                product_id in canonical_product_ids,
                product_is_visible,
                product_availability,
                product_is_price_hidden,
                eligible_for_routing,
                "bigcommerce_api",
                combined_text,
            )
        )
        synced_count += 1

    for category in categories:
        category_id = int(category["id"])
        category_name = (category.get("name") or "").strip()
        category_url = ((category.get("custom_url") or {}).get("url") or "").strip()
        if not category_url:
            continue
        page_title = (category.get("page_title") or "").strip()
        description = category.get("description") or ""
        meta_keywords = [keyword.strip() for keyword in (category.get("meta_keywords") or []) if keyword]
        attribute_profile = extract_attribute_terms(" ".join([category_name, page_title, " ".join(meta_keywords), category_url]))
        cluster_profile = build_cluster_profile(
            product_name=category_name,
            product_url=category_url,
            brand_name="",
            search_keywords=" ".join(meta_keywords),
            attribute_profile=attribute_profile,
        )
        canonical_group_key = canonical_category_group_key(category)
        source_data = {
            "category": {
                "id": category_id,
                "name": category_name,
                "custom_url": category_url,
                "page_title": page_title,
                "description_length": len(re.sub(r"<[^>]+>", "", description or "").strip()),
                "meta_keywords": meta_keywords,
                "parent_id": category.get("parent_id"),
                "is_visible": bool(category.get("is_visible", True)),
            }
        }
        category_is_visible = bool(category.get("is_visible", True))
        category_routing_eligible = category_eligible_for_routing(category)
        category_profile_rows.append(
            (
                normalized_hash,
                category_id,
                category.get("parent_id"),
                category_name,
                category_url,
                page_title,
                description,
                json.dumps(meta_keywords),
                json.dumps(source_data),
                json.dumps(serialize_attribute_profile(attribute_profile)),
                json.dumps(cluster_profile),
                canonical_group_key,
                category_id in canonical_category_ids,
                category_is_visible,
                category_routing_eligible,
                "bigcommerce_api",
            )
        )
        synced_categories += 1

    name_sql = """
        INSERT INTO app_runtime.store_option_name_mappings (
            store_hash, raw_option_name, bucket_key, normalized_name, confidence, source, review_status, metadata, created_at, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, 'auto', 'auto_approved', %s::jsonb, NOW(), NOW())
        ON CONFLICT (store_hash, raw_option_name) DO UPDATE SET
            bucket_key = EXCLUDED.bucket_key,
            normalized_name = EXCLUDED.normalized_name,
            confidence = GREATEST(app_runtime.store_option_name_mappings.confidence, EXCLUDED.confidence),
            source = 'auto',
            review_status = app_runtime.store_option_name_mappings.review_status,
            metadata = app_runtime.store_option_name_mappings.metadata || EXCLUDED.metadata,
            updated_at = NOW();
    """
    value_sql = """
        INSERT INTO app_runtime.store_option_value_mappings (
            store_hash, raw_option_name, raw_option_value, bucket_key, canonical_value, confidence, source, review_status, metadata, created_at, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, 'auto', 'auto_approved', %s::jsonb, NOW(), NOW())
        ON CONFLICT (store_hash, raw_option_name, raw_option_value) DO UPDATE SET
            bucket_key = EXCLUDED.bucket_key,
            canonical_value = EXCLUDED.canonical_value,
            confidence = GREATEST(app_runtime.store_option_value_mappings.confidence, EXCLUDED.confidence),
            source = 'auto',
            review_status = app_runtime.store_option_value_mappings.review_status,
            metadata = app_runtime.store_option_value_mappings.metadata || EXCLUDED.metadata,
            updated_at = NOW();
    """
    profile_sql = """
        INSERT INTO app_runtime.store_product_profiles (
            store_hash, bc_product_id, product_name, product_url, brand_name, search_keywords, source_data,
            attribute_profile, cluster_profile, canonical_group_key, is_canonical_target, is_visible, availability,
            is_price_hidden, eligible_for_routing, sync_source, last_synced_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (store_hash, bc_product_id) DO UPDATE SET
            product_name = EXCLUDED.product_name,
            product_url = EXCLUDED.product_url,
            brand_name = EXCLUDED.brand_name,
            search_keywords = EXCLUDED.search_keywords,
            source_data = EXCLUDED.source_data,
            attribute_profile = EXCLUDED.attribute_profile,
            cluster_profile = EXCLUDED.cluster_profile,
            canonical_group_key = EXCLUDED.canonical_group_key,
            is_canonical_target = EXCLUDED.is_canonical_target,
            is_visible = EXCLUDED.is_visible,
            availability = EXCLUDED.availability,
            is_price_hidden = EXCLUDED.is_price_hidden,
            eligible_for_routing = EXCLUDED.eligible_for_routing,
            sync_source = EXCLUDED.sync_source,
            last_synced_at = NOW();
    """
    category_profile_sql = """
        INSERT INTO app_runtime.store_category_profiles (
            store_hash, bc_category_id, parent_category_id, category_name, category_url, page_title, description,
            meta_keywords, source_data, attribute_profile, cluster_profile, canonical_group_key, is_canonical_target,
            is_visible, eligible_for_routing, sync_source, last_synced_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (store_hash, bc_category_id) DO UPDATE SET
            parent_category_id = EXCLUDED.parent_category_id,
            category_name = EXCLUDED.category_name,
            category_url = EXCLUDED.category_url,
            page_title = EXCLUDED.page_title,
            description = EXCLUDED.description,
            meta_keywords = EXCLUDED.meta_keywords,
            source_data = EXCLUDED.source_data,
            attribute_profile = EXCLUDED.attribute_profile,
            cluster_profile = EXCLUDED.cluster_profile,
            canonical_group_key = EXCLUDED.canonical_group_key,
            is_canonical_target = EXCLUDED.is_canonical_target,
            is_visible = EXCLUDED.is_visible,
            eligible_for_routing = EXCLUDED.eligible_for_routing,
            sync_source = EXCLUDED.sync_source,
            last_synced_at = NOW();
    """

    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            if option_name_rows:
                execute_batch(cur, name_sql, option_name_rows, page_size=200)
            if option_value_rows:
                execute_batch(cur, value_sql, option_value_rows, page_size=500)
            if product_profile_rows:
                execute_batch(cur, profile_sql, [row[:-1] for row in product_profile_rows], page_size=200)
            if category_profile_rows:
                execute_batch(cur, category_profile_sql, category_profile_rows, page_size=200)
        conn.commit()

    signal_refresh = refresh_store_intent_signal_enrichments(normalized_hash, initiated_by=initiated_by)
    pending_mapping_counts = normalize_mapping_review_statuses(normalized_hash)
    readiness = refresh_store_readiness(normalized_hash)
    return {
        "store_hash": normalized_hash,
        "synced_products": synced_count,
        "synced_categories": synced_categories,
        "storefront_sync": storefront_sync,
        "mapped_option_names": len(option_name_rows),
        "mapped_option_values": len(option_value_rows),
        "towel_profiles": towel_profile_count,
        "pending_option_name_mappings": pending_mapping_counts["pending_name_count"],
        "pending_option_value_mappings": pending_mapping_counts["pending_value_count"],
        "intent_signal_refresh": signal_refresh,
        "readiness": readiness,
    }


__all__ = ["sync_store_catalog_profiles"]
