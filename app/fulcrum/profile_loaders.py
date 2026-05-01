"""Profile-loading helpers for Fulcrum."""

from __future__ import annotations

import hashlib
import re
from typing import Any, Callable

from psycopg2.errors import UndefinedTable
from psycopg2.extras import RealDictCursor


def load_product_profiles(
    product_urls: list[str],
    *,
    get_pg_conn_fn: Callable[[], Any],
    extract_attribute_terms_fn: Callable[[str | None], dict[str, set[str]]],
    tokenize_intent_text_fn: Callable[[str | None], set[str]],
    build_cluster_profile_fn: Callable[..., dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    normalized_urls = sorted({url for url in product_urls if url})
    if not normalized_urls:
        return {}
    sql = """
        SELECT p.custom_url, p.name, p.search_keywords, b.name AS brand_name,
               ARRAY_REMOVE(ARRAY_AGG(DISTINCT m.option_label), NULL) AS option_labels,
               ARRAY_REMOVE(ARRAY_AGG(DISTINCT m.option_display_name), NULL) AS option_display_names
        FROM h4h_import2.products p
        LEFT JOIN h4h_import2.brands b ON b.id = p.brand_id
        LEFT JOIN h4h_import2.migration_flattened_options m ON m.product_id = p.id
        WHERE p.custom_url = ANY(%s)
        GROUP BY p.custom_url, p.name, p.search_keywords, b.name;
    """
    profiles: dict[str, dict[str, Any]] = {}
    with get_pg_conn_fn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (normalized_urls,))
            rows = [dict(row) for row in cur.fetchall()]
    for row in rows:
        option_labels = [label for label in (row.get("option_labels") or []) if label]
        option_display_names = [label for label in (row.get("option_display_names") or []) if label]
        token_text = " ".join([row.get("name") or "", row.get("brand_name") or "", row.get("search_keywords") or "", " ".join(option_labels), " ".join(option_display_names)])
        attrs = extract_attribute_terms_fn(token_text)
        profiles[row["custom_url"]] = {
            "url": row["custom_url"],
            "name": row.get("name") or "",
            "brand_name": row.get("brand_name") or "",
            "search_keywords": row.get("search_keywords") or "",
            "option_labels": option_labels,
            "option_display_names": option_display_names,
            "tokens": tokenize_intent_text_fn(token_text),
            "attributes": attrs,
            "cluster_profile": build_cluster_profile_fn(product_name=row.get("name") or "", product_url=row["custom_url"], brand_name=row.get("brand_name") or "", search_keywords=row.get("search_keywords") or "", attribute_profile=attrs),
        }
    for url in normalized_urls:
        if url in profiles:
            continue
        empty_attrs = extract_attribute_terms_fn(url)
        profiles[url] = {
            "url": url,
            "name": "",
            "brand_name": "",
            "search_keywords": "",
            "option_labels": [],
            "option_display_names": [],
            "tokens": tokenize_intent_text_fn(url),
            "attributes": empty_attrs,
            "cluster_profile": build_cluster_profile_fn(product_name="", product_url=url, brand_name="", search_keywords="", attribute_profile=empty_attrs),
        }
    return profiles


def humanize_url_path_title(url_path: str | None, *, normalize_storefront_path_fn: Callable[[str | None], str]) -> str:
    normalized = normalize_storefront_path_fn(url_path)
    if not normalized or normalized == "/":
        return ""
    segment = normalized.strip("/").split("/")[-1]
    if segment.endswith(".html"):
        segment = segment[:-5]
    words: list[str] = []
    for part in re.split(r"[-_]+", segment):
        cleaned = (part or "").strip()
        if not cleaned:
            continue
        lower = cleaned.lower()
        if lower == "vs":
            words.append("vs")
        elif lower in {"faq", "opl"}:
            words.append(lower.upper())
        elif lower.isdigit():
            words.append(lower)
        else:
            words.append(lower.capitalize())
    title = " ".join(words).strip()
    return title or normalized.strip("/")


def looks_like_content_path(url_path: str | None, *, normalize_storefront_path_fn: Callable[[str | None], str]) -> bool:
    normalized = normalize_storefront_path_fn(url_path)
    if not normalized:
        return False
    if normalized in {"/blog/", "/hospitality-bedding-blog/"}:
        return False
    if normalized.startswith("/blog/") or normalized.startswith("/hospitality-bedding-blog/"):
        return True
    return any(fragment in normalized for fragment in ("/guide/", "/guides/", "/faq/", "/how-to/", "/comparison/", "/compare/", "/vs-", "-guide/", "-faq/"))


def synthetic_content_entity_id(url_path: str | None, *, normalize_storefront_path_fn: Callable[[str | None], str]) -> int:
    normalized = normalize_storefront_path_fn(url_path)
    if not normalized:
        return 0
    return max(1, int(hashlib.md5(normalized.encode("utf-8")).hexdigest()[:8], 16) % 2_000_000_000)


def load_ga4_page_metrics(
    store_hash: str,
    urls: list[str],
    days: int = 90,
    *,
    get_pg_conn_fn: Callable[[], Any],
    normalize_store_hash_fn: Callable[[str | None], str],
    normalize_storefront_path_fn: Callable[[str | None], str],
) -> dict[str, dict[str, Any]]:
    normalized_store_hash = normalize_store_hash_fn(store_hash)
    normalized_urls = sorted({normalize_storefront_path_fn(url) for url in urls if url})
    if not normalized_urls:
        return {}
    sql = """
        WITH raw_pages AS (
            SELECT lower(CASE WHEN page_path = '/' THEN '/' WHEN right(page_path, 1) = '/' THEN page_path ELSE page_path || '/' END) AS page_path,
                   channel_group, sessions, total_users, engaged_sessions, add_to_carts, ecommerce_purchases, purchase_revenue
            FROM app_runtime.store_ga4_pages_daily
            WHERE store_hash = %s
              AND date >= CURRENT_DATE - (%s * INTERVAL '1 day')
              AND page_path IS NOT NULL
        )
        SELECT page_path, SUM(sessions) AS sessions_90d, SUM(total_users) AS total_users_90d, SUM(engaged_sessions) AS engaged_sessions_90d,
               CASE WHEN SUM(sessions) > 0 THEN SUM(engaged_sessions)::double precision / SUM(sessions) ELSE 0 END AS engagement_rate_90d,
               SUM(add_to_carts) AS add_to_carts_90d, SUM(ecommerce_purchases) AS purchases_90d, SUM(purchase_revenue) AS revenue_90d,
               SUM(CASE WHEN channel_group = 'Organic Search' THEN sessions ELSE 0 END) AS organic_sessions_90d,
               SUM(CASE WHEN channel_group = 'Organic Search' THEN total_users ELSE 0 END) AS organic_total_users_90d,
               SUM(CASE WHEN channel_group = 'Organic Search' THEN engaged_sessions ELSE 0 END) AS organic_engaged_sessions_90d,
               CASE WHEN SUM(CASE WHEN channel_group = 'Organic Search' THEN sessions ELSE 0 END) > 0
                    THEN SUM(CASE WHEN channel_group = 'Organic Search' THEN engaged_sessions ELSE 0 END)::double precision
                         / SUM(CASE WHEN channel_group = 'Organic Search' THEN sessions ELSE 0 END)
                    ELSE 0 END AS organic_engagement_rate_90d,
               SUM(CASE WHEN channel_group = 'Organic Search' THEN add_to_carts ELSE 0 END) AS organic_add_to_carts_90d,
               SUM(CASE WHEN channel_group = 'Organic Search' THEN ecommerce_purchases ELSE 0 END) AS organic_purchases_90d,
               SUM(CASE WHEN channel_group = 'Organic Search' THEN purchase_revenue ELSE 0 END) AS organic_revenue_90d
        FROM raw_pages
        WHERE page_path = ANY(%s)
        GROUP BY page_path;
    """
    with get_pg_conn_fn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (normalized_store_hash, days, normalized_urls))
            rows = [dict(row) for row in cur.fetchall()]
    metrics_by_url: dict[str, dict[str, Any]] = {}
    for row in rows:
        page_path = normalize_storefront_path_fn(row.get("page_path"))
        if not page_path:
            continue
        metrics_by_url[page_path] = {
            "sessions_90d": int(row.get("sessions_90d") or 0),
            "total_users_90d": int(row.get("total_users_90d") or 0),
            "engaged_sessions_90d": int(row.get("engaged_sessions_90d") or 0),
            "engagement_rate_90d": float(row.get("engagement_rate_90d") or 0.0),
            "add_to_carts_90d": int(row.get("add_to_carts_90d") or 0),
            "purchases_90d": int(row.get("purchases_90d") or 0),
            "revenue_90d": float(row.get("revenue_90d") or 0.0),
            "organic_sessions_90d": int(row.get("organic_sessions_90d") or 0),
            "organic_total_users_90d": int(row.get("organic_total_users_90d") or 0),
            "organic_engaged_sessions_90d": int(row.get("organic_engaged_sessions_90d") or 0),
            "organic_engagement_rate_90d": float(row.get("organic_engagement_rate_90d") or 0.0),
            "organic_add_to_carts_90d": int(row.get("organic_add_to_carts_90d") or 0),
            "organic_purchases_90d": int(row.get("organic_purchases_90d") or 0),
            "organic_revenue_90d": float(row.get("organic_revenue_90d") or 0.0),
        }
    return metrics_by_url


def dedupe_entity_profiles(
    profiles: list[dict[str, Any]],
    *,
    normalize_storefront_path_fn: Callable[[str | None], str],
    duplicate_suffix_base_url_fn: Callable[[str | None, set[str] | None], str],
    prefer_canonical: bool = True,
) -> list[dict[str, Any]]:
    if not profiles:
        return []
    known_urls = {normalize_storefront_path_fn(profile.get("url")) for profile in profiles if normalize_storefront_path_fn(profile.get("url"))}
    grouped: dict[str, list[dict[str, Any]]] = {}
    for profile in profiles:
        normalized_url = normalize_storefront_path_fn(profile.get("url"))
        if normalized_url:
            grouped.setdefault(duplicate_suffix_base_url_fn(normalized_url, known_urls), []).append(profile)
    deduped: list[dict[str, Any]] = []
    for group_rows in grouped.values():
        def _quality(profile: dict[str, Any]) -> tuple[int, int, int, int]:
            normalized_url = normalize_storefront_path_fn(profile.get("url"))
            return (
                1 if (prefer_canonical and profile.get("is_canonical_target", True)) else 0,
                1 if profile.get("eligible_for_routing", True) else 0,
                -1 if duplicate_suffix_base_url_fn(normalized_url, known_urls) != normalized_url else 0,
                -len(normalized_url),
            )
        deduped.append(max(group_rows, key=_quality))
    return deduped


def load_store_product_profiles(
    store_hash: str,
    product_urls: list[str],
    *,
    get_pg_conn_fn: Callable[[], Any],
    normalize_store_hash_fn: Callable[[str | None], str],
    tokenize_intent_text_fn: Callable[[str | None], set[str]],
    extract_attribute_terms_fn: Callable[[str | None], dict[str, set[str]]],
) -> dict[str, dict[str, Any]]:
    normalized_urls = sorted({url for url in product_urls if url})
    if not normalized_urls:
        return {}
    sql = """
        SELECT bc_product_id, product_url, product_name, brand_name, search_keywords, source_data, attribute_profile,
               cluster_profile, canonical_group_key, is_canonical_target, is_visible, availability, is_price_hidden, eligible_for_routing
        FROM app_runtime.store_product_profiles
        WHERE store_hash = %s AND product_url = ANY(%s);
    """
    with get_pg_conn_fn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (normalize_store_hash_fn(store_hash), normalized_urls))
            rows = [dict(row) for row in cur.fetchall()]
    profiles: dict[str, dict[str, Any]] = {}
    for row in rows:
        source_data = row.get("source_data") or {}
        option_pairs = source_data.get("option_pairs") or []
        option_labels = [pair.get("name") for pair in option_pairs if pair.get("name")]
        option_values = [pair.get("value") for pair in option_pairs if pair.get("value")]
        combined_text = " ".join(filter(None, [row.get("product_name") or "", row.get("brand_name") or "", row.get("search_keywords") or "", row.get("product_url") or "", " ".join(option_labels), " ".join(option_values)]))
        raw_attrs = row.get("attribute_profile") or {}
        attrs = {bucket: set(values or []) for bucket, values in raw_attrs.items()}
        profiles[row["product_url"]] = {
            "bc_product_id": row.get("bc_product_id"),
            "url": row["product_url"],
            "name": row.get("product_name") or "",
            "brand_name": row.get("brand_name") or "",
            "search_keywords": row.get("search_keywords") or "",
            "option_labels": option_labels,
            "option_display_names": option_values,
            "tokens": tokenize_intent_text_fn(combined_text),
            "attributes": attrs or extract_attribute_terms_fn(combined_text),
            "cluster_profile": row.get("cluster_profile") or {},
            "canonical_group_key": row.get("canonical_group_key") or "",
            "is_canonical_target": bool(row.get("is_canonical_target")),
            "is_visible": bool(row.get("is_visible", True)),
            "availability": row.get("availability") or "",
            "is_price_hidden": bool(row.get("is_price_hidden", False)),
            "eligible_for_routing": bool(row.get("eligible_for_routing", True)),
        }
    return profiles


def load_store_category_profiles(
    store_hash: str,
    category_urls: list[str] | None = None,
    canonical_only: bool = False,
    *,
    get_pg_conn_fn: Callable[[], Any],
    normalize_store_hash_fn: Callable[[str | None], str],
    tokenize_intent_text_fn: Callable[[str | None], set[str]],
    extract_attribute_terms_fn: Callable[[str | None], dict[str, set[str]]],
) -> dict[str, dict[str, Any]]:
    params: list[Any] = [normalize_store_hash_fn(store_hash)]
    filters = ["store_hash = %s"]
    if category_urls:
        filters.append("category_url = ANY(%s)")
        params.append(sorted({url for url in category_urls if url}))
    if canonical_only:
        filters.append("is_canonical_target = TRUE")
    sql = f"""
        SELECT bc_category_id, parent_category_id, category_name, category_url, page_title, description, meta_keywords,
               source_data, attribute_profile, cluster_profile, canonical_group_key, is_canonical_target, is_visible, eligible_for_routing
        FROM app_runtime.store_category_profiles
        WHERE {' AND '.join(filters)};
    """
    with get_pg_conn_fn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = [dict(row) for row in cur.fetchall()]
    profiles: dict[str, dict[str, Any]] = {}
    for row in rows:
        meta_keywords = row.get("meta_keywords") or []
        combined_text = " ".join(filter(None, [row.get("category_name") or "", row.get("page_title") or "", row.get("category_url") or "", " ".join(meta_keywords), re.sub(r"<[^>]+>", " ", row.get("description") or "")]))
        raw_attrs = row.get("attribute_profile") or {}
        attrs = {bucket: set(values or []) for bucket, values in raw_attrs.items()}
        profiles[row["category_url"]] = {
            "url": row["category_url"],
            "name": row.get("category_name") or "",
            "brand_name": "",
            "search_keywords": " ".join(meta_keywords),
            "option_labels": [],
            "option_display_names": [],
            "tokens": tokenize_intent_text_fn(combined_text),
            "attributes": attrs or extract_attribute_terms_fn(combined_text),
            "cluster_profile": row.get("cluster_profile") or {},
            "bc_category_id": row.get("bc_category_id"),
            "parent_category_id": row.get("parent_category_id"),
            "is_canonical_target": bool(row.get("is_canonical_target")),
            "canonical_group_key": row.get("canonical_group_key") or "",
            "is_visible": bool(row.get("is_visible", True)),
            "eligible_for_routing": bool(row.get("eligible_for_routing", True)),
        }
    return profiles


def load_store_brand_profiles(
    store_hash: str,
    *,
    get_pg_conn_fn: Callable[[], Any],
    normalize_storefront_path_fn: Callable[[str | None], str],
    extract_attribute_terms_fn: Callable[[str | None], dict[str, set[str]]],
    tokenize_intent_text_fn: Callable[[str | None], set[str]],
    build_cluster_profile_fn: Callable[..., dict[str, Any]],
    dedupe_entity_profiles_fn: Callable[[list[dict[str, Any]]], list[dict[str, Any]]],
) -> dict[str, dict[str, Any]]:
    del store_hash
    sql = """
        SELECT id, name, page_title, meta_keywords, meta_description, search_keywords, custom_url
        FROM h4h_import2.brands
        WHERE custom_url IS NOT NULL AND custom_url <> '';
    """
    with get_pg_conn_fn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            try:
                cur.execute(sql)
                rows = [dict(row) for row in cur.fetchall()]
            except UndefinedTable:
                conn.rollback()
                rows = []
    profiles: list[dict[str, Any]] = []
    for row in rows:
        brand_url = normalize_storefront_path_fn(row.get("custom_url"))
        if not brand_url:
            continue
        combined_text = " ".join(filter(None, [row.get("name") or "", row.get("page_title") or "", row.get("search_keywords") or "", row.get("meta_keywords") or "", row.get("meta_description") or "", brand_url]))
        attrs = extract_attribute_terms_fn(combined_text)
        search_keywords = " ".join(filter(None, [row.get("search_keywords") or "", row.get("meta_keywords") or ""]))
        profiles.append({"entity_type": "brand", "bc_entity_id": int(row.get("id") or 0), "url": brand_url, "name": row.get("name") or "", "brand_name": row.get("name") or "", "search_keywords": search_keywords, "option_labels": [], "option_display_names": [], "tokens": tokenize_intent_text_fn(combined_text), "attributes": attrs, "cluster_profile": build_cluster_profile_fn(product_name=row.get("name") or "", product_url=brand_url, brand_name=row.get("name") or "", search_keywords=search_keywords, attribute_profile=attrs), "is_canonical_target": True, "eligible_for_routing": True})
    return {profile["url"]: profile for profile in dedupe_entity_profiles_fn(profiles)}


def load_reserved_storefront_urls(
    store_hash: str,
    *,
    get_pg_conn_fn: Callable[[], Any],
    normalize_store_hash_fn: Callable[[str | None], str],
    normalize_storefront_path_fn: Callable[[str | None], str],
) -> set[str]:
    normalized_hash = normalize_store_hash_fn(store_hash)
    sql = """
        SELECT url
        FROM (
            SELECT product_url AS url FROM app_runtime.store_product_profiles WHERE store_hash = %s AND product_url IS NOT NULL AND product_url <> ''
            UNION
            SELECT category_url AS url FROM app_runtime.store_category_profiles WHERE store_hash = %s AND category_url IS NOT NULL AND category_url <> ''
        ) urls;
    """
    with get_pg_conn_fn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (normalized_hash, normalized_hash))
            rows = cur.fetchall()
    return {normalized for row in rows for normalized in [normalize_storefront_path_fn(row[0] if isinstance(row, tuple) else row.get("url"))] if normalized}


def load_store_content_profiles(
    store_hash: str,
    include_backlog: bool = False,
    *,
    get_pg_conn_fn: Callable[[], Any],
    normalize_storefront_path_fn: Callable[[str | None], str],
    looks_like_content_path_fn: Callable[[str | None], bool],
    load_reserved_storefront_urls_fn: Callable[[str], set[str]],
    extract_attribute_terms_fn: Callable[[str | None], dict[str, set[str]]],
    tokenize_intent_text_fn: Callable[[str | None], set[str]],
    build_cluster_profile_fn: Callable[..., dict[str, Any]],
    synthetic_content_entity_id_fn: Callable[[str | None], int],
    humanize_url_path_title_fn: Callable[[str | None], str],
    dedupe_entity_profiles_fn: Callable[[list[dict[str, Any]]], list[dict[str, Any]]],
) -> dict[str, dict[str, Any]]:
    sql = """
        SELECT id, title, url, body, metadescription, metakeywords, author
        FROM h4h_imports.blogposts
        WHERE COALESCE(ispublished, TRUE) = TRUE AND url IS NOT NULL AND url <> '';
    """
    with get_pg_conn_fn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            try:
                cur.execute(sql)
                rows = [dict(row) for row in cur.fetchall()]
            except UndefinedTable:
                conn.rollback()
                rows = []
    profiles: list[dict[str, Any]] = []
    reserved_urls = load_reserved_storefront_urls_fn(store_hash)
    for row in rows:
        content_url = normalize_storefront_path_fn(row.get("url"))
        if not content_url or not looks_like_content_path_fn(content_url) or content_url in reserved_urls:
            continue
        body_text = re.sub(r"<[^>]+>", " ", row.get("body") or "")
        search_keywords = " ".join(filter(None, [row.get("metakeywords") or "", row.get("metadescription") or ""]))
        combined_text = " ".join(filter(None, [row.get("title") or "", content_url, row.get("metadescription") or "", row.get("metakeywords") or "", row.get("author") or "", body_text[:1200]]))
        attrs = extract_attribute_terms_fn(combined_text)
        profiles.append({"entity_type": "content", "bc_entity_id": int(row.get("id") or 0), "url": content_url, "name": row.get("title") or "", "brand_name": "", "search_keywords": search_keywords, "option_labels": [], "option_display_names": [], "tokens": tokenize_intent_text_fn(combined_text), "attributes": attrs, "cluster_profile": build_cluster_profile_fn(product_name=row.get("title") or "", product_url=content_url, brand_name="", search_keywords=search_keywords, attribute_profile=attrs), "is_canonical_target": True, "eligible_for_routing": True})

    if include_backlog:
        backlog_sql = """
        SELECT url_path, priority_score, gsc_impressions_90d, gsc_clicks_90d, ga4_sessions_90d, ga4_engagement_rate_90d
        FROM analytics_data.content_backlog
        WHERE url_path IS NOT NULL AND url_path <> '';
        """
        with get_pg_conn_fn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                try:
                    cur.execute(backlog_sql)
                    backlog_rows = [dict(row) for row in cur.fetchall()]
                except UndefinedTable:
                    conn.rollback()
                    backlog_rows = []
        known_urls = {normalized for normalized in {normalize_storefront_path_fn(profile.get("url")) for profile in profiles} if normalized} | reserved_urls
        for row in backlog_rows:
            content_url = normalize_storefront_path_fn(row.get("url_path"))
            if not content_url or content_url in known_urls or not looks_like_content_path_fn(content_url):
                continue
            title = humanize_url_path_title_fn(content_url)
            combined_text = " ".join(filter(None, [title, content_url, f"priority {row.get('priority_score') or 0}", f"gsc impressions {int(row.get('gsc_impressions_90d') or 0)}", f"gsc clicks {int(row.get('gsc_clicks_90d') or 0)}"]))
            attrs = extract_attribute_terms_fn(combined_text)
            profiles.append({"entity_type": "content", "bc_entity_id": synthetic_content_entity_id_fn(content_url), "url": content_url, "name": title, "brand_name": "", "search_keywords": title, "option_labels": [], "option_display_names": [], "tokens": tokenize_intent_text_fn(combined_text), "attributes": attrs, "cluster_profile": build_cluster_profile_fn(product_name=title, product_url=content_url, brand_name="", search_keywords=title, attribute_profile=attrs), "is_canonical_target": True, "eligible_for_routing": True, "content_origin": "content_backlog"})
    return {profile["url"]: profile for profile in dedupe_entity_profiles_fn(profiles)}


__all__ = ["dedupe_entity_profiles", "humanize_url_path_title", "load_ga4_page_metrics", "load_product_profiles", "load_reserved_storefront_urls", "load_store_brand_profiles", "load_store_category_profiles", "load_store_content_profiles", "load_store_product_profiles", "looks_like_content_path", "synthetic_content_entity_id"]
