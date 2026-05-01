from __future__ import annotations

from collections import Counter
from typing import Any

import requests

from app.fulcrum.services import (
    get_bc_headers,
    get_pg_conn,
    list_publications,
    list_store_categories,
    list_store_products,
    normalize_store_hash,
    publish_approved_entities,
    unpublish_entities,
)


LINK_METAFIELD_KEYS = {
    "internal_links_html",
    "internal_category_links_html",
    "internal_product_links_html",
}


def _normalize_url_path(url: str | None) -> str:
    value = (url or "").strip()
    if not value:
        return ""
    if not value.startswith("/"):
        value = f"/{value}"
    return value.rstrip("/") + "/"


def _api_base(store_hash: str) -> str:
    return f"https://api.bigcommerce.com/stores/{normalize_store_hash(store_hash)}/v3"


def _entity_path(entity_type: str) -> str:
    return "products" if (entity_type or "").strip().lower() == "product" else "categories"


def list_remote_link_metafields(store_hash: str) -> list[dict[str, Any]]:
    headers = get_bc_headers(store_hash)
    api_base = _api_base(store_hash)
    results: list[dict[str, Any]] = []

    for product in list_store_products(store_hash):
        entity_id = int(product.get("id") or 0)
        if not entity_id:
            continue
        response = requests.get(
            f"{api_base}/catalog/products/{entity_id}/metafields",
            headers=headers,
            params={"namespace": "h4h", "limit": 250},
            timeout=30,
        )
        response.raise_for_status()
        for metafield in response.json().get("data", []):
            key = (metafield.get("key") or "").strip()
            if key not in LINK_METAFIELD_KEYS:
                continue
            results.append(
                {
                    "entity_type": "product",
                    "entity_id": entity_id,
                    "entity_name": product.get("name") or "",
                    "entity_url": (product.get("custom_url") or {}).get("url") or "",
                    "metafield_id": int(metafield.get("id") or 0),
                    "key": key,
                }
            )

    for category in list_store_categories(store_hash):
        entity_id = int(category.get("id") or 0)
        if not entity_id:
            continue
        response = requests.get(
            f"{api_base}/catalog/categories/{entity_id}/metafields",
            headers=headers,
            params={"namespace": "h4h", "limit": 250},
            timeout=30,
        )
        response.raise_for_status()
        for metafield in response.json().get("data", []):
            key = (metafield.get("key") or "").strip()
            if key not in LINK_METAFIELD_KEYS:
                continue
            results.append(
                {
                    "entity_type": "category",
                    "entity_id": entity_id,
                    "entity_name": category.get("name") or "",
                    "entity_url": (category.get("custom_url") or {}).get("url") or "",
                    "metafield_id": int(metafield.get("id") or 0),
                    "key": key,
                }
            )

    return results


def _delete_remote_link_metafield(store_hash: str, row: dict[str, Any]) -> None:
    headers = get_bc_headers(store_hash)
    api_base = _api_base(store_hash)
    entity_type = (row.get("entity_type") or "").strip().lower()
    entity_id = int(row.get("entity_id") or 0)
    metafield_id = int(row.get("metafield_id") or 0)
    if entity_type not in {"product", "category"} or not entity_id or not metafield_id:
        return
    response = requests.delete(
        f"{api_base}/catalog/{_entity_path(entity_type)}/{entity_id}/metafields/{metafield_id}",
        headers=headers,
        timeout=30,
    )
    if response.status_code not in (200, 204, 404):
        response.raise_for_status()


def _active_publication_keyset(store_hash: str) -> set[tuple[str, str, str]]:
    rows = list_publications(store_hash, active_only=True, limit=5000)
    keyset: set[tuple[str, str, str]] = set()
    for row in rows:
        keyset.add(
            (
                (row.get("source_entity_type") or "product").strip().lower(),
                _normalize_url_path(row.get("source_url")),
                (row.get("metafield_key") or "internal_links_html").strip(),
            )
        )
    return keyset


def _approved_source_counts(store_hash: str) -> dict[str, int]:
    sql = """
        SELECT source_entity_type, COUNT(DISTINCT source_product_id) AS source_count
        FROM app_runtime.link_candidates
        WHERE store_hash = %s
          AND review_status = 'approved'
        GROUP BY source_entity_type;
    """
    counts = {"product": 0, "category": 0}
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (normalize_store_hash(store_hash),))
            for source_entity_type, source_count in cur.fetchall():
                counts[str(source_entity_type or "product")] = int(source_count or 0)
    return counts


def reset_and_republish_bigcommerce_links(store_hash: str, execute: bool = False) -> dict[str, Any]:
    normalized_hash = normalize_store_hash(store_hash)
    remote_before = list_remote_link_metafields(normalized_hash)
    active_publications = list_publications(normalized_hash, active_only=True, limit=5000)
    active_keyset = _active_publication_keyset(normalized_hash)
    orphan_remote = [
        row
        for row in remote_before
        if (
            (row.get("entity_type") or "").strip().lower(),
            _normalize_url_path(row.get("entity_url")),
            (row.get("key") or "").strip(),
        )
        not in active_keyset
    ]

    summary: dict[str, Any] = {
        "store_hash": normalized_hash,
        "execute": bool(execute),
        "remote_before_count": len(remote_before),
        "remote_before_by_key": dict(Counter(row.get("key") or "" for row in remote_before)),
        "active_publications_before_count": len(active_publications),
        "active_publications_before_by_key": dict(
            Counter((row.get("metafield_key") or "internal_links_html") for row in active_publications)
        ),
        "approved_source_counts": _approved_source_counts(normalized_hash),
        "orphan_remote_count": len(orphan_remote),
        "orphan_remote_sample": orphan_remote[:25],
        "unpublished_count": 0,
        "deleted_orphan_count": 0,
        "republished_count": 0,
        "republished_by_key": {},
        "remote_after_count": len(remote_before),
        "remote_after_by_key": dict(Counter(row.get("key") or "" for row in remote_before)),
    }

    if not execute:
        return summary

    active_source_ids = sorted(
        {
            int(row.get("source_product_id") or 0)
            for row in active_publications
            if int(row.get("source_product_id") or 0)
        }
    )
    unpublished = unpublish_entities(normalized_hash, active_source_ids) if active_source_ids else []

    remaining_remote = list_remote_link_metafields(normalized_hash)
    deleted_orphan_count = 0
    for row in remaining_remote:
        _delete_remote_link_metafield(normalized_hash, row)
        deleted_orphan_count += 1

    republished = publish_approved_entities(normalized_hash)
    remote_after = list_remote_link_metafields(normalized_hash)

    summary.update(
        {
            "unpublished_count": len(unpublished),
            "deleted_orphan_count": deleted_orphan_count,
            "republished_count": len(republished),
            "republished_by_key": dict(Counter((row.get("metafield_key") or "") for row in republished)),
            "remote_after_count": len(remote_after),
            "remote_after_by_key": dict(Counter(row.get("key") or "" for row in remote_after)),
        }
    )
    return summary
