"""Fulcrum platform and BigCommerce integration helpers."""

from __future__ import annotations

import base64
import glob
import hashlib
import hmac
import json
import math
import re
from pathlib import Path
from typing import Any, Callable

import psycopg2
import requests
from psycopg2.extras import RealDictCursor, execute_batch

from app.fulcrum.config import Config


ROOT_DIR = Path(Config.FULCRUM_ENV_PATH).resolve().parent


def get_pg_conn():
    if Config.DATABASE_URL:
        return psycopg2.connect(Config.DATABASE_URL)

    return psycopg2.connect(
        dbname=Config.DB_NAME,
        user=Config.DB_USER,
        password=Config.DB_PASSWORD,
        host=Config.DB_HOST,
        port=Config.DB_PORT,
    )


def base64url_decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + padding)


def decode_signed_payload(token: str, secret: str) -> dict[str, Any]:
    parts = token.split(".")
    if len(parts) != 3:
        raise ValueError("Invalid signed_payload_jwt format.")

    header_b64, payload_b64, signature_b64 = parts
    signing_input = f"{header_b64}.{payload_b64}".encode("utf-8")
    expected_signature = hmac.new(
        secret.encode("utf-8"),
        signing_input,
        hashlib.sha256,
    ).digest()
    actual_signature = base64url_decode(signature_b64)

    if not hmac.compare_digest(expected_signature, actual_signature):
        raise ValueError("Invalid signed_payload_jwt signature.")

    return json.loads(base64url_decode(payload_b64).decode("utf-8"))


def normalize_store_hash(value: str | None) -> str:
    if not value:
        return ""
    normalized = str(value).strip()
    normalized = normalized.split("?", 1)[0].split("#", 1)[0]
    if normalized.lower().startswith("stores/"):
        normalized = normalized.split("/", 1)[1]
    normalized = normalized.split("/", 1)[0]
    normalized = normalized.split(":", 1)[0]
    normalized = normalized.strip().lower()
    match = re.match(r"^[a-z0-9]+", normalized)
    return match.group(0) if match else normalized


def exchange_auth_code(code: str, scope: str, context: str) -> dict[str, Any]:
    response = requests.post(
        "https://login.bigcommerce.com/oauth2/token",
        headers={"Accept": "application/json", "Content-Type": "application/json"},
        json={
            "client_id": Config.FULCRUM_BC_CLIENT_ID,
            "client_secret": Config.FULCRUM_BC_CLIENT_SECRET,
            "redirect_uri": Config.FULCRUM_AUTH_CALLBACK_URL,
            "grant_type": "authorization_code",
            "code": code,
            "scope": scope,
            "context": context,
        },
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def upsert_store_installation(
    store_hash: str,
    context: str,
    access_token: str | None,
    scope: str | None,
    user_id: str | None = None,
    owner_email: str | None = None,
    install_source: str = "oauth",
    metadata: dict[str, Any] | None = None,
) -> None:
    metadata = metadata or {}
    sql = """
        INSERT INTO app_runtime.store_installations (
            store_hash,
            context,
            account_uuid,
            access_token,
            scope,
            user_id,
            owner_email,
            status,
            install_source,
            metadata,
            installed_at,
            updated_at,
            uninstalled_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, 'active', %s, %s::jsonb, NOW(), NOW(), NULL)
        ON CONFLICT (store_hash) DO UPDATE SET
            context = EXCLUDED.context,
            account_uuid = EXCLUDED.account_uuid,
            access_token = COALESCE(EXCLUDED.access_token, app_runtime.store_installations.access_token),
            scope = COALESCE(EXCLUDED.scope, app_runtime.store_installations.scope),
            user_id = COALESCE(EXCLUDED.user_id, app_runtime.store_installations.user_id),
            owner_email = COALESCE(EXCLUDED.owner_email, app_runtime.store_installations.owner_email),
            status = 'active',
            install_source = EXCLUDED.install_source,
            metadata = app_runtime.store_installations.metadata || EXCLUDED.metadata,
            updated_at = NOW(),
            uninstalled_at = NULL;
    """

    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    store_hash,
                    context,
                    Config.FULCRUM_BC_ACCOUNT_UUID,
                    access_token,
                    scope,
                    user_id,
                    owner_email,
                    install_source,
                    json.dumps(metadata),
                ),
            )
        conn.commit()


def merge_store_installation_metadata(
    store_hash: str,
    metadata: dict[str, Any] | None = None,
    *,
    context: str | None = None,
    scope: str | None = None,
    user_id: str | None = None,
    owner_email: str | None = None,
    install_source: str | None = None,
) -> None:
    normalized_hash = normalize_store_hash(store_hash)
    if not normalized_hash:
        return

    metadata = metadata or {}
    normalized_context = str((context or f"stores/{normalized_hash}")).strip() or f"stores/{normalized_hash}"
    normalized_install_source = str(install_source or "").strip()
    sql = """
        INSERT INTO app_runtime.store_installations (
            store_hash,
            context,
            account_uuid,
            access_token,
            scope,
            user_id,
            owner_email,
            status,
            install_source,
            metadata,
            installed_at,
            updated_at,
            uninstalled_at
        )
        VALUES (%s, %s, %s, NULL, %s, %s, %s, 'active', %s, %s::jsonb, NOW(), NOW(), NULL)
        ON CONFLICT (store_hash) DO UPDATE SET
            context = COALESCE(NULLIF(EXCLUDED.context, ''), app_runtime.store_installations.context),
            account_uuid = COALESCE(EXCLUDED.account_uuid, app_runtime.store_installations.account_uuid),
            scope = COALESCE(EXCLUDED.scope, app_runtime.store_installations.scope),
            user_id = COALESCE(EXCLUDED.user_id, app_runtime.store_installations.user_id),
            owner_email = COALESCE(EXCLUDED.owner_email, app_runtime.store_installations.owner_email),
            status = 'active',
            install_source = COALESCE(NULLIF(EXCLUDED.install_source, ''), app_runtime.store_installations.install_source),
            metadata = app_runtime.store_installations.metadata || EXCLUDED.metadata,
            updated_at = NOW(),
            uninstalled_at = NULL;
    """

    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    normalized_hash,
                    normalized_context,
                    Config.FULCRUM_BC_ACCOUNT_UUID,
                    scope,
                    user_id,
                    owner_email,
                    normalized_install_source or 'callback_probe',
                    json.dumps(metadata),
                ),
            )
        conn.commit()


def mark_store_uninstalled(store_hash: str, metadata: dict[str, Any] | None = None) -> None:
    metadata = metadata or {}
    sql = """
        UPDATE app_runtime.store_installations
        SET status = 'uninstalled',
            metadata = metadata || %s::jsonb,
            updated_at = NOW(),
            uninstalled_at = NOW()
        WHERE store_hash = %s;
    """
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (json.dumps(metadata), store_hash))
        conn.commit()


def list_installations() -> list[dict[str, Any]]:
    sql = """
        SELECT store_hash, context, status, install_source, installed_at, updated_at, uninstalled_at
        FROM app_runtime.store_installations
        ORDER BY updated_at DESC, store_hash;
    """
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql)
            return [dict(row) for row in cur.fetchall()]


def get_store_owner_email(store_hash: str) -> str:
    sql = """
        SELECT owner_email
        FROM app_runtime.store_installations
        WHERE store_hash = %s
          AND status = 'active'
          AND owner_email IS NOT NULL
          AND owner_email <> ''
        ORDER BY updated_at DESC
        LIMIT 1;
    """
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (normalize_store_hash(store_hash),))
            row = cur.fetchone()
    return str((row or [""])[0] or "").strip()


def _resolve_store_token(store_hash: str) -> str:
    normalized_hash = normalize_store_hash(store_hash)

    sql = """
        SELECT access_token
        FROM app_runtime.store_installations
        WHERE store_hash = %s
          AND status = 'active'
          AND access_token IS NOT NULL
        ORDER BY updated_at DESC
        LIMIT 1;
    """
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (normalized_hash,))
            row = cur.fetchone()
            if row and row[0]:
                return row[0]

    if normalized_hash == normalize_store_hash(Config.BIG_COMMERCE_STORE_HASH):
        return Config.BIG_COMMERCE_ACCESS_TOKEN or ""

    credential_files = list(glob.glob(str(ROOT_DIR / "BigCommerceAPI-credentials-*.txt")))
    sandbox_file = ROOT_DIR / "Sandbox_credentials.txt"
    if sandbox_file.exists():
        credential_files.append(str(sandbox_file))

    for path in credential_files:
        values = {}
        for line in Path(path).read_text(encoding="utf-8").splitlines():
            if ":" not in line:
                continue
            key, value = line.split(":", 1)
            values[key.strip().upper()] = value.strip()
        api_path = values.get("API PATH", "")
        if f"/stores/{normalized_hash}/" in api_path:
            return values.get("ACCESS TOKEN", "")

    return ""


def get_bc_headers(store_hash: str) -> dict[str, str]:
    token = _resolve_store_token(store_hash)
    if not token:
        raise RuntimeError(f"No BigCommerce token available for store {store_hash}.")
    return {
        "X-Auth-Token": token,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def _list_bc_paginated(store_hash: str, path: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    normalized_hash = normalize_store_hash(store_hash)
    headers = get_bc_headers(normalized_hash)
    api_base = f"https://api.bigcommerce.com/stores/{normalized_hash}/v3"
    items: list[dict[str, Any]] = []
    page = 1

    while True:
        query = {"limit": 250, "page": page, **(params or {})}
        response = requests.get(
            f"{api_base}/{path.lstrip('/')}",
            headers=headers,
            params=query,
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        data = payload.get("data", [])
        items.extend(data)
        pagination = payload.get("meta", {}).get("pagination", {})
        total_pages = pagination.get("total_pages")
        if total_pages is None:
            total_items = pagination.get("total_items") or pagination.get("total") or len(items)
            per_page = pagination.get("limit") or pagination.get("per_page") or query["limit"]
            try:
                total_pages = max(1, math.ceil(int(total_items or 0) / max(int(per_page or 1), 1)))
            except (TypeError, ValueError):
                total_pages = 1
        if page >= int(total_pages or 1):
            break
        page += 1

    return items


def list_store_products(store_hash: str) -> list[dict[str, Any]]:
    normalized_hash = normalize_store_hash(store_hash)
    headers = get_bc_headers(normalized_hash)
    api_base = f"https://api.bigcommerce.com/stores/{normalized_hash}/v3"
    products: list[dict[str, Any]] = []
    page = 1

    while True:
        response = requests.get(
            f"{api_base}/catalog/products",
            headers=headers,
            params={"limit": 250, "page": page, "include_fields": "id,name,custom_url"},
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        data = payload.get("data", [])
        products.extend(data)

        pagination = payload.get("meta", {}).get("pagination", {})
        if page >= pagination.get("total_pages", 1):
            break
        page += 1

    return products


def list_store_categories(store_hash: str) -> list[dict[str, Any]]:
    return _list_bc_paginated(
        store_hash,
        "/catalog/categories",
        {"include_fields": "id,parent_id,name,custom_url,description,page_title,meta_keywords,is_visible"},
    )


def list_store_channels(store_hash: str) -> list[dict[str, Any]]:
    try:
        return _list_bc_paginated(store_hash, "/channels")
    except requests.HTTPError:
        return []


def list_store_sites(store_hash: str) -> list[dict[str, Any]]:
    try:
        return _list_bc_paginated(store_hash, "/sites")
    except requests.HTTPError:
        return []


def resolve_store_product_id_by_url(store_hash: str, source_url: str) -> tuple[int | None, str | None]:
    normalized_source_url = source_url.rstrip("/") + "/"
    for product in list_store_products(store_hash):
        custom_url = (product.get("custom_url") or {}).get("url", "")
        if not custom_url:
            continue
        if custom_url.rstrip("/") + "/" == normalized_source_url:
            return product.get("id"), product.get("name")
    return None, None


def resolve_store_category_id_by_url(store_hash: str, category_url: str) -> tuple[int | None, str | None]:
    normalized_category_url = category_url.rstrip("/") + "/"
    for category in list_store_categories(store_hash):
        custom_url = (category.get("custom_url") or {}).get("url", "")
        if not custom_url:
            continue
        if custom_url.rstrip("/") + "/" == normalized_category_url:
            return category.get("id"), category.get("name")
    return None, None


def _canonical_storefront_site_url(site: dict[str, Any]) -> dict[str, str]:
    urls = site.get("urls") or []
    typed_urls: dict[str, str] = {}
    for item in urls:
        url_value = (item.get("url") or "").strip().rstrip("/")
        url_type = (item.get("type") or "").strip().lower()
        if url_value and url_type and url_type not in typed_urls:
            typed_urls[url_type] = url_value
    fallback_url = (site.get("url") or "").strip().rstrip("/")
    return {
        "site_url": fallback_url,
        "primary_url": typed_urls.get("primary") or fallback_url or typed_urls.get("canonical", ""),
        "canonical_url": typed_urls.get("canonical") or fallback_url,
        "checkout_url": typed_urls.get("checkout") or typed_urls.get("primary") or fallback_url,
    }


def sync_store_storefront_sites(
    store_hash: str,
    initiated_by: str | None = None,
    *,
    clear_storefront_site_caches: Callable[[], None] | None = None,
    resolve_default_base_url: Callable[[str], str] | None = None,
) -> dict[str, Any]:
    normalized_hash = normalize_store_hash(store_hash)
    channels = list_store_channels(normalized_hash)
    sites = list_store_sites(normalized_hash)
    channel_map = {int(channel["id"]): dict(channel) for channel in channels if channel.get("id") is not None}

    records: list[tuple[Any, ...]] = []
    storefront_count = 0
    for site in sites:
        channel_id = site.get("channel_id")
        site_id = site.get("id")
        if channel_id is None or site_id is None:
            continue
        channel = channel_map.get(int(channel_id), {})
        urls = _canonical_storefront_site_url(site)
        channel_type = (channel.get("type") or "").strip().lower()
        if channel_type == "storefront":
            storefront_count += 1
        records.append(
            (
                normalized_hash,
                int(channel_id),
                int(site_id),
                channel.get("name") or "",
                channel.get("platform") or "",
                channel_type,
                channel.get("status") or "",
                bool(channel.get("is_enabled", False)),
                urls["site_url"],
                urls["primary_url"],
                urls["canonical_url"],
                urls["checkout_url"],
                json.dumps(
                    {
                        "channel": channel,
                        "site": site,
                        "initiated_by": initiated_by or "fulcrum",
                    }
                ),
            )
        )

    upsert_sql = """
        INSERT INTO app_runtime.store_storefront_sites (
            store_hash,
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
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW()
        )
        ON CONFLICT (store_hash, channel_id, site_id) DO UPDATE SET
            channel_name = EXCLUDED.channel_name,
            channel_platform = EXCLUDED.channel_platform,
            channel_type = EXCLUDED.channel_type,
            channel_status = EXCLUDED.channel_status,
            is_channel_enabled = EXCLUDED.is_channel_enabled,
            site_url = EXCLUDED.site_url,
            primary_url = EXCLUDED.primary_url,
            canonical_url = EXCLUDED.canonical_url,
            checkout_url = EXCLUDED.checkout_url,
            metadata = EXCLUDED.metadata,
            last_synced_at = NOW();
    """
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            if records:
                execute_batch(cur, upsert_sql, records, page_size=100)
                current_pairs = {(record[1], record[2]) for record in records}
                current_pairs_sql = ",".join(f"({int(channel_id)}, {int(site_id)})" for channel_id, site_id in sorted(current_pairs))
                cur.execute(
                    f"""
                    DELETE FROM app_runtime.store_storefront_sites
                    WHERE store_hash = %s
                      AND (channel_id, site_id) NOT IN ({current_pairs_sql});
                    """,
                    (normalized_hash,),
                )
            else:
                cur.execute(
                    "DELETE FROM app_runtime.store_storefront_sites WHERE store_hash = %s;",
                    (normalized_hash,),
                )
        conn.commit()

    if clear_storefront_site_caches is not None:
        clear_storefront_site_caches()
    default_base_url = resolve_default_base_url(normalized_hash) if resolve_default_base_url else ""
    return {
        "store_hash": normalized_hash,
        "synced_site_count": len(records),
        "storefront_site_count": storefront_count,
        "default_base_url": default_base_url,
    }


def fetch_store_brand_map(store_hash: str) -> dict[int, str]:
    try:
        brands = _list_bc_paginated(store_hash, "/catalog/brands", {"include_fields": "id,name"})
    except requests.HTTPError:
        return {}
    return {int(brand["id"]): brand.get("name") or "" for brand in brands if brand.get("id") is not None}


def fetch_store_product_options(store_hash: str, product_id: int) -> list[dict[str, Any]]:
    try:
        return _list_bc_paginated(store_hash, f"/catalog/products/{product_id}/options")
    except requests.HTTPError:
        return []


def _flatten_option_pairs(options: list[dict[str, Any]]) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    for option in options:
        raw_name = option.get("display_name") or option.get("name") or ""
        option_values = option.get("option_values") or []
        if not option_values and raw_name:
            pairs.append((raw_name, raw_name))
            continue
        for option_value in option_values:
            raw_value = option_value.get("label") or option_value.get("value_data") or option_value.get("value") or ""
            if raw_name and raw_value:
                pairs.append((raw_name, str(raw_value)))
    return pairs


__all__ = [
    "_canonical_storefront_site_url",
    "_flatten_option_pairs",
    "_list_bc_paginated",
    "_resolve_store_token",
    "base64url_decode",
    "decode_signed_payload",
    "exchange_auth_code",
    "fetch_store_brand_map",
    "fetch_store_product_options",
    "get_bc_headers",
    "get_pg_conn",
    "get_store_owner_email",
    "list_installations",
    "list_store_categories",
    "list_store_channels",
    "list_store_products",
    "list_store_sites",
    "mark_store_uninstalled",
    "normalize_store_hash",
    "resolve_store_category_id_by_url",
    "resolve_store_product_id_by_url",
    "sync_store_storefront_sites",
    "upsert_store_installation",
]
