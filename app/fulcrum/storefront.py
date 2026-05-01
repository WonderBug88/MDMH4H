"""Storefront URL and profile summary helpers for Fulcrum."""

from __future__ import annotations

from functools import lru_cache
from typing import Any

from psycopg2.extras import RealDictCursor

from app.fulcrum.platform import get_pg_conn, normalize_store_hash


KNOWN_STOREFRONT_BASE_URLS = {
    "99oa2tso": "https://www.hotels4humanity.com",
    "pdwzti0dpv": "https://www.hotels4humanity.com",
}


def _known_storefront_base_url(store_hash: str) -> str:
    return (KNOWN_STOREFRONT_BASE_URLS.get(normalize_store_hash(store_hash)) or "").rstrip("/")


def _coerce_channel_id(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def clear_storefront_site_caches() -> None:
    load_storefront_site_rows.cache_clear()
    get_storefront_base_url_from_db.cache_clear()


@lru_cache(maxsize=64)
def load_storefront_site_rows(store_hash: str) -> list[dict[str, Any]]:
    sql = """
        SELECT
            channel_id,
            site_id,
            channel_name,
            channel_platform,
            channel_type,
            channel_status,
            is_channel_enabled,
            site_url,
            primary_url,
            canonical_url,
            checkout_url,
            metadata,
            last_synced_at
        FROM app_runtime.store_storefront_sites
        WHERE store_hash = %s
        ORDER BY
            CASE WHEN lower(channel_type) = 'storefront' THEN 0 ELSE 1 END,
            CASE WHEN is_channel_enabled THEN 0 ELSE 1 END,
            CASE WHEN channel_id = 1 THEN 0 ELSE 1 END,
            channel_id,
            site_id;
    """
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (normalize_store_hash(store_hash),))
            return [dict(row) for row in cur.fetchall()]


def select_storefront_site_row(
    site_rows: list[dict[str, Any]],
    channel_id: int | None = None,
) -> dict[str, Any] | None:
    if channel_id is not None:
        for row in site_rows:
            if int(row.get("channel_id") or 0) == int(channel_id):
                return row
    storefront_rows = [
        row
        for row in site_rows
        if (row.get("channel_type") or "").strip().lower() == "storefront"
    ]
    if storefront_rows:
        return storefront_rows[0]
    return site_rows[0] if site_rows else None


@lru_cache(maxsize=128)
def get_storefront_base_url_from_db(store_hash: str, channel_id: int | None = None) -> str:
    site_rows = load_storefront_site_rows(store_hash)
    selected_row = select_storefront_site_row(site_rows, channel_id=channel_id)
    if not selected_row:
        return ""
    return (
        (selected_row.get("primary_url") or "").strip().rstrip("/")
        or (selected_row.get("site_url") or "").strip().rstrip("/")
        or (selected_row.get("canonical_url") or "").strip().rstrip("/")
        or (selected_row.get("checkout_url") or "").strip().rstrip("/")
    )


def extract_storefront_channel_id(*objects: Any) -> int | None:
    candidate_keys = (
        "channel_id",
        "source_channel_id",
        "target_channel_id",
        "bc_channel_id",
        "storefront_channel_id",
    )
    for item in objects:
        if not isinstance(item, dict):
            continue
        for key in candidate_keys:
            channel_id = _coerce_channel_id(item.get(key))
            if channel_id is not None:
                return channel_id
        metadata = item.get("metadata")
        if isinstance(metadata, dict):
            for key in candidate_keys:
                channel_id = _coerce_channel_id(metadata.get(key))
                if channel_id is not None:
                    return channel_id
    return None


def get_storefront_base_url(store_hash: str, channel_id: int | None = None) -> str:
    normalized_hash = normalize_store_hash(store_hash)
    site_url = get_storefront_base_url_from_db(normalized_hash, channel_id=channel_id)
    if site_url:
        return site_url
    known_url = _known_storefront_base_url(normalized_hash)
    if known_url:
        return known_url
    return f"https://store-{normalized_hash}.mybigcommerce.com"


def list_storefront_base_urls(store_hash: str) -> list[str]:
    normalized_hash = normalize_store_hash(store_hash)
    urls: list[str] = []
    seen: set[str] = set()
    for row in load_storefront_site_rows(normalized_hash):
        for key in ("primary_url", "site_url", "canonical_url", "checkout_url"):
            raw = (row.get(key) or "").strip().rstrip("/")
            if not raw or raw in seen:
                continue
            seen.add(raw)
            urls.append(raw)
    known_url = _known_storefront_base_url(normalized_hash)
    if known_url and known_url not in seen:
        seen.add(known_url)
        urls.append(known_url)
    fallback = get_storefront_base_url(normalized_hash).rstrip("/")
    if fallback and fallback not in seen:
        urls.append(fallback)
    return urls


def get_store_profile_summary(store_hash: str) -> dict[str, Any]:
    sql = """
        SELECT
            (SELECT COUNT(*) FROM app_runtime.store_product_profiles WHERE store_hash = %s) AS profile_count,
            (SELECT COUNT(*) FROM app_runtime.store_category_profiles WHERE store_hash = %s) AS category_profile_count,
            (SELECT COUNT(*) FROM app_runtime.store_option_name_mappings WHERE store_hash = %s) AS option_name_mapping_count,
            (SELECT COUNT(*) FROM app_runtime.store_option_value_mappings WHERE store_hash = %s) AS option_value_mapping_count,
            (SELECT COUNT(*) FROM app_runtime.store_storefront_sites WHERE store_hash = %s) AS storefront_site_count,
            (
                SELECT COUNT(*)
                FROM app_runtime.store_storefront_sites
                WHERE store_hash = %s
                  AND lower(channel_type) = 'storefront'
            ) AS storefront_channel_count,
            (
                SELECT MAX(last_synced_at)
                FROM (
                    SELECT MAX(last_synced_at) AS last_synced_at
                    FROM app_runtime.store_product_profiles
                    WHERE store_hash = %s
                    UNION ALL
                    SELECT MAX(last_synced_at) AS last_synced_at
                    FROM app_runtime.store_category_profiles
                    WHERE store_hash = %s
                    UNION ALL
                    SELECT MAX(last_synced_at) AS last_synced_at
                    FROM app_runtime.store_storefront_sites
                    WHERE store_hash = %s
                ) synced
            ) AS last_synced_at;
    """
    cluster_sql = """
        SELECT
            COALESCE(cluster_profile->>'primary', '') AS primary_cluster,
            COUNT(*) AS profile_count
        FROM app_runtime.store_product_profiles
        WHERE store_hash = %s
        GROUP BY 1
        ORDER BY COUNT(*) DESC, 1
        LIMIT 8;
    """
    normalized_hash = normalize_store_hash(store_hash)
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                sql,
                (
                    normalized_hash,
                    normalized_hash,
                    normalized_hash,
                    normalized_hash,
                    normalized_hash,
                    normalized_hash,
                    normalized_hash,
                    normalized_hash,
                    normalized_hash,
                ),
            )
            summary = dict(cur.fetchone() or {})
            cur.execute(cluster_sql, (normalized_hash,))
            summary["cluster_counts"] = [dict(row) for row in cur.fetchall()]
    summary["default_storefront_base_url"] = get_storefront_base_url(normalized_hash)
    return summary


__all__ = [
    "clear_storefront_site_caches",
    "extract_storefront_channel_id",
    "get_store_profile_summary",
    "get_storefront_base_url",
    "get_storefront_base_url_from_db",
    "list_storefront_base_urls",
    "load_storefront_site_rows",
    "select_storefront_site_row",
]
