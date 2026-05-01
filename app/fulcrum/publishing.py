"""Publication and metafield orchestration for Fulcrum."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Callable

import requests
from psycopg2.extras import RealDictCursor


PgConnFactory = Callable[[], Any]
ApprovedRowsGetter = Callable[[str, int, str], list[dict[str, Any]]]
EntityResolver = Callable[[str, str], tuple[int | None, str | None]]
LinkHtmlBuilder = Callable[[list[dict[str, Any]], str], str | None]
CacheInvalidator = Callable[[str, list[str] | None], None]
HeadersGetter = Callable[[str], dict[str, str]]
StoreHashNormalizer = Callable[[str], str]
MetafieldUpserter = Callable[[str, str, int, str, str], dict[str, Any]]


def upsert_entity_metafield(
    *,
    store_hash: str,
    entity_type: str,
    entity_id: int,
    key: str,
    html: str,
    get_bc_headers: HeadersGetter,
    normalize_store_hash: StoreHashNormalizer,
    requests_module=requests,
) -> dict[str, Any]:
    headers = get_bc_headers(store_hash)
    api_base = f"https://api.bigcommerce.com/stores/{normalize_store_hash(store_hash)}/v3"
    entity_path = "products" if entity_type == "product" else "categories"
    list_url = f"{api_base}/catalog/{entity_path}/{entity_id}/metafields"
    params = {"namespace": "h4h", "key": key}

    get_response = requests_module.get(list_url, headers=headers, params=params, timeout=30)
    get_response.raise_for_status()
    existing = get_response.json().get("data", [])

    payload = {
        "permission_set": "write_and_sf_access",
        "namespace": "h4h",
        "key": key,
        "value": html,
        "description": "Fulcrum auto-generated internal links HTML snippet",
    }

    if existing:
        metafield_id = existing[0]["id"]
        response = requests_module.put(
            f"{list_url}/{metafield_id}",
            headers=headers,
            json=payload,
            timeout=30,
        )
        response.raise_for_status()
        return {"action": "updated", "metafield_id": metafield_id}

    response = requests_module.post(list_url, headers=headers, json=payload, timeout=30)
    response.raise_for_status()
    return {"action": "created", "metafield_id": response.json()["data"]["id"]}


def _build_html_specs(
    *,
    source_entity_type: str,
    rows: list[dict[str, Any]],
    build_links_html: LinkHtmlBuilder,
) -> tuple[list[tuple[str, str | None]], str]:
    if source_entity_type == "category":
        return (
            [
                (
                    "internal_category_links_html",
                    build_links_html(
                        [row for row in rows if (row.get("target_entity_type") or "product") == "category"],
                        "Related Categories",
                    ),
                ),
                (
                    "internal_product_links_html",
                    build_links_html(
                        [row for row in rows if (row.get("target_entity_type") or "product") == "product"],
                        "Shop Matching Products",
                    ),
                ),
            ],
            "skipped_missing_store_category",
        )

    return [("internal_links_html", build_links_html(rows, "Related options"))], "skipped_missing_store_product"


def _publication_source_match_ids(
    *,
    source_entity_type: str,
    source_entity_id: int | None,
    live_entity_id: int | None,
) -> list[int]:
    source_ids: list[int] = []
    try:
        normalized_source_id = int(source_entity_id or 0)
    except (TypeError, ValueError):
        normalized_source_id = 0
    if normalized_source_id != 0:
        source_ids.append(normalized_source_id)

    if source_entity_type == "category":
        try:
            legacy_category_id = -abs(int(live_entity_id or 0))
        except (TypeError, ValueError):
            legacy_category_id = 0
        if legacy_category_id != 0 and legacy_category_id not in source_ids:
            source_ids.append(legacy_category_id)

    return source_ids


def publish_approved_entities(
    *,
    store_hash: str,
    source_entity_ids: list[int] | None = None,
    run_id: int | None = None,
    get_pg_conn: PgConnFactory,
    get_approved_rows_for_source: ApprovedRowsGetter,
    resolve_store_category_id_by_url: EntityResolver,
    resolve_store_product_id_by_url: EntityResolver,
    build_links_html: LinkHtmlBuilder,
    upsert_entity_metafield: MetafieldUpserter,
    invalidate_admin_metric_cache: CacheInvalidator,
) -> list[dict[str, Any]]:
    approved_sql = """
        SELECT DISTINCT ON (source_entity_type, source_product_id)
            source_entity_type,
            source_product_id,
            source_name,
            source_url
        FROM app_runtime.link_candidates
        WHERE store_hash = %s
          AND review_status = 'approved'
          AND (%s::int[] IS NULL OR source_product_id = ANY(%s::int[]))
        ORDER BY source_entity_type, source_product_id, candidate_id DESC;
    """

    publications = []
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(approved_sql, (store_hash, source_entity_ids, source_entity_ids))
            sources = [dict(row) for row in cur.fetchall()]

        for source in sources:
            source_entity_type = source.get("source_entity_type") or "product"
            rows = get_approved_rows_for_source(
                store_hash,
                source["source_product_id"],
                source_entity_type,
            )
            html_specs, missing_status = _build_html_specs(
                source_entity_type=source_entity_type,
                rows=rows,
                build_links_html=build_links_html,
            )

            if source_entity_type == "category":
                live_entity_id, live_entity_name = resolve_store_category_id_by_url(store_hash, source["source_url"])
            else:
                live_entity_id, live_entity_name = resolve_store_product_id_by_url(store_hash, source["source_url"])

            if not live_entity_id:
                publications.append(
                    {
                        "source_product_id": source["source_product_id"],
                        "source_entity_type": source_entity_type,
                        "source_url": source["source_url"],
                        "status": missing_status,
                    }
                )
                continue

            publication_source_ids = _publication_source_match_ids(
                source_entity_type=source_entity_type,
                source_entity_id=source.get("source_product_id"),
                live_entity_id=live_entity_id,
            )
            metadata_bc_key = f"bc_{source_entity_type}_id"
            metadata_bc_value = str(int(live_entity_id))

            for metafield_key, html in html_specs:
                if not html:
                    continue

                metafield_result = upsert_entity_metafield(
                    store_hash,
                    source_entity_type,
                    live_entity_id,
                    metafield_key,
                    html,
                )
                html_hash = hashlib.sha256(html.encode("utf-8")).hexdigest()

                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE app_runtime.link_publications
                        SET publication_status = 'superseded',
                            unpublished_at = NOW()
                        WHERE store_hash = %s
                          AND source_entity_type = %s
                          AND COALESCE(metafield_key, 'internal_links_html') = %s
                          AND unpublished_at IS NULL
                          AND (
                              source_entity_id = ANY(%s::bigint[])
                              OR source_product_id = ANY(%s::bigint[])
                              OR source_url = %s
                              OR COALESCE(metadata ->> %s, '') = %s
                          );
                        """,
                        (
                            store_hash,
                            source_entity_type,
                            metafield_key,
                            publication_source_ids,
                            publication_source_ids,
                            source["source_url"],
                            metadata_bc_key,
                            metadata_bc_value,
                        ),
                    )
                conn.commit()
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO app_runtime.link_publications (
                            store_hash,
                            source_product_id,
                            source_name,
                            source_url,
                            metafield_id,
                            metafield_key,
                            html_hash,
                            html_snapshot,
                            publication_status,
                            run_id,
                            source_entity_type,
                            source_entity_id,
                            metadata,
                            published_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'published', %s, %s, %s, %s::jsonb, NOW());
                        """,
                        (
                            store_hash,
                            source["source_product_id"],
                            live_entity_name or source["source_name"],
                            source["source_url"],
                            metafield_result["metafield_id"],
                            metafield_key,
                            html_hash,
                            html,
                            run_id,
                            source_entity_type,
                            source["source_product_id"],
                            json.dumps(
                                {
                                    metadata_bc_key: live_entity_id,
                                    "bc_action": metafield_result["action"],
                                    "metafield_key": metafield_key,
                                }
                            ),
                        ),
                    )
                publications.append(
                    {
                        "source_product_id": source["source_product_id"],
                        "source_entity_type": source_entity_type,
                        "source_url": source["source_url"],
                        f"bc_{source_entity_type}_id": live_entity_id,
                        "metafield_key": metafield_key,
                        "status": metafield_result["action"],
                    }
                )

        conn.commit()

    if publications:
        invalidate_admin_metric_cache(store_hash, metric_keys=["live_gsc_performance", "live_gsc_performance_store_scoped_v2"])
    return publications


def unpublish_entities(
    *,
    store_hash: str,
    source_entity_ids: list[int],
    get_pg_conn: PgConnFactory,
    get_bc_headers: HeadersGetter,
    normalize_store_hash: StoreHashNormalizer,
    resolve_store_category_id_by_url: EntityResolver,
    resolve_store_product_id_by_url: EntityResolver,
    invalidate_admin_metric_cache: CacheInvalidator,
    requests_module=requests,
) -> list[dict[str, Any]]:
    headers = get_bc_headers(store_hash)
    api_base = f"https://api.bigcommerce.com/stores/{normalize_store_hash(store_hash)}/v3"
    results = []

    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT publication_id, source_entity_type, source_product_id, source_url, metafield_id, metafield_key
                FROM app_runtime.link_publications
                WHERE store_hash = %s
                  AND source_product_id = ANY(%s::int[])
                  AND unpublished_at IS NULL;
                """,
                (store_hash, source_entity_ids),
            )
            rows = [dict(row) for row in cur.fetchall()]

        for row in rows:
            entity_type = row.get("source_entity_type") or "product"
            if entity_type == "category":
                bc_entity_id, _ = resolve_store_category_id_by_url(store_hash, row["source_url"])
                entity_path = "categories"
            else:
                bc_entity_id, _ = resolve_store_product_id_by_url(store_hash, row["source_url"])
                entity_path = "products"
            if bc_entity_id and row["metafield_id"]:
                response = requests_module.delete(
                    f"{api_base}/catalog/{entity_path}/{bc_entity_id}/metafields/{row['metafield_id']}",
                    headers=headers,
                    timeout=30,
                )
                if response.status_code not in (200, 204, 404):
                    response.raise_for_status()

            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE app_runtime.link_publications
                    SET publication_status = 'unpublished',
                        unpublished_at = NOW()
                    WHERE publication_id = %s;
                    """,
                    (row["publication_id"],),
                )

            results.append(
                {
                    "source_product_id": row["source_product_id"],
                    "source_entity_type": entity_type,
                    "source_url": row["source_url"],
                    "metafield_key": row.get("metafield_key"),
                    "status": "unpublished",
                }
            )
        conn.commit()

    if results:
        invalidate_admin_metric_cache(store_hash, metric_keys=["live_gsc_performance", "live_gsc_performance_store_scoped_v2"])
    return results


def list_publications(
    *,
    store_hash: str,
    active_only: bool = True,
    limit: int = 100,
    get_pg_conn: PgConnFactory,
) -> list[dict[str, Any]]:
    sql = """
        SELECT
            publication_id,
            source_entity_type,
            source_entity_id,
            source_product_id,
            source_name,
            source_url,
            metafield_id,
            metafield_key,
            metadata,
            publication_status,
            published_at,
            unpublished_at
        FROM app_runtime.link_publications
        WHERE store_hash = %s
          AND (%s = FALSE OR unpublished_at IS NULL)
        ORDER BY published_at DESC
        LIMIT %s;
    """
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (store_hash, active_only, limit))
            return [dict(row) for row in cur.fetchall()]


def count_publications(
    *,
    store_hash: str,
    active_only: bool = True,
    get_pg_conn: PgConnFactory,
) -> int:
    sql = """
        SELECT COUNT(*)
        FROM app_runtime.link_publications
        WHERE store_hash = %s
          AND (%s = FALSE OR unpublished_at IS NULL);
    """
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (store_hash, active_only))
            row = cur.fetchone()
    return int((row or [0])[0] or 0)


def summarize_live_publications(
    *,
    store_hash: str,
    get_pg_conn: PgConnFactory,
    normalize_store_hash: StoreHashNormalizer,
) -> dict[str, int]:
    sql = """
        SELECT
            COUNT(*) FILTER (
                WHERE source_entity_type = 'product'
                  AND COALESCE(metafield_key, 'internal_links_html') = 'internal_links_html'
            ) AS product_page_blocks,
            COUNT(*) FILTER (
                WHERE source_entity_type = 'category'
                  AND COALESCE(metafield_key, '') = 'internal_product_links_html'
            ) AS category_product_blocks,
            COUNT(*) FILTER (
                WHERE source_entity_type = 'category'
                  AND COALESCE(metafield_key, '') = 'internal_category_links_html'
            ) AS category_category_blocks,
            COUNT(*) AS total_live_blocks
        FROM app_runtime.link_publications
        WHERE store_hash = %s
          AND unpublished_at IS NULL;
    """
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (normalize_store_hash(store_hash),))
            row = dict(cur.fetchone() or {})
    return {
        "product_page_blocks": int(row.get("product_page_blocks") or 0),
        "category_product_blocks": int(row.get("category_product_blocks") or 0),
        "category_category_blocks": int(row.get("category_category_blocks") or 0),
        "total_live_blocks": int(row.get("total_live_blocks") or 0),
    }
