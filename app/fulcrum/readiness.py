"""Mapping review and readiness operations for Fulcrum."""

from __future__ import annotations

import json
from pathlib import Path
import re
from typing import Any, Callable

from psycopg2.extras import RealDictCursor

from app.fulcrum.config import Config
from app.fulcrum.constants import (
    ALPHA_DEFAULT_BUCKETS,
    MAPPING_AUTO_APPROVE_MIN_CONFIDENCE,
    MAPPING_PENDING_STATUS,
)
from app.fulcrum.platform import get_pg_conn, normalize_store_hash
from app.fulcrum.rendering import (
    category_theme_hook_present as render_category_theme_hook_present,
    theme_hook_present as render_theme_hook_present,
)


def _mapping_review_status(confidence: float | int | None) -> str:
    try:
        numeric = float(confidence or 0)
    except (TypeError, ValueError):
        numeric = 0.0
    return "auto_approved" if numeric >= MAPPING_AUTO_APPROVE_MIN_CONFIDENCE else MAPPING_PENDING_STATUS


def _normalize_mapping_review_statuses(store_hash: str) -> dict[str, int]:
    normalized_hash = normalize_store_hash(store_hash)
    status_case = f"""
        CASE
            WHEN confidence >= {MAPPING_AUTO_APPROVE_MIN_CONFIDENCE} THEN 'auto_approved'
            ELSE '{MAPPING_PENDING_STATUS}'
        END
    """
    sql = f"""
        WITH updated_name AS (
            UPDATE app_runtime.store_option_name_mappings
            SET review_status = {status_case},
                updated_at = NOW()
            WHERE store_hash = %s
              AND review_status IN ('auto_approved', '{MAPPING_PENDING_STATUS}')
            RETURNING review_status
        ),
        updated_value AS (
            UPDATE app_runtime.store_option_value_mappings
            SET review_status = {status_case},
                updated_at = NOW()
            WHERE store_hash = %s
              AND review_status IN ('auto_approved', '{MAPPING_PENDING_STATUS}')
            RETURNING review_status
        )
        SELECT
            (SELECT COUNT(*) FROM updated_name WHERE review_status = '{MAPPING_PENDING_STATUS}') AS pending_name_count,
            (SELECT COUNT(*) FROM updated_value WHERE review_status = '{MAPPING_PENDING_STATUS}') AS pending_value_count;
    """
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (normalized_hash, normalized_hash))
            result = dict(cur.fetchone() or {})
        conn.commit()
    return {
        "pending_name_count": int(result.get("pending_name_count") or 0),
        "pending_value_count": int(result.get("pending_value_count") or 0),
    }


def _normalize_mapping_label(value: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", "", (value or "").strip().lower())


def _should_ignore_option_name_mapping(row: dict[str, Any]) -> bool:
    normalized_name = _normalize_mapping_label(row.get("raw_option_name"))
    bucket_key = (row.get("bucket_key") or "").strip().lower()
    if not normalized_name:
        return True
    if normalized_name == "caseof" and bucket_key == "pack_size":
        return False
    generic_names = {
        "options",
        "option",
        "chooseyouroption",
        "chooseyouroptions",
        "chooseanoption",
        "chooseyouraccessory",
        "chooseyouramenity",
        "chooseyourbedscarf",
        "chooseyourbedspreads",
        "chooseyourcasequanity",
        "chooseyourrack",
        "chooseyourspringwrap",
        "chooseyourtires",
        "chooseyourtowels",
        "chooseyourwheelsandoptions",
        "choosebedskirtorboxspringwrap",
        "chooseboneorwhite",
        "cartisavailablein",
        "category",
        "condocart",
        "cupslids",
        "ironingboardsupplies",
        "ktx",
        "pattern",
        "regularorpillowtop",
        "rollaway",
        "tyoe",
    }
    return normalized_name in generic_names


def _value_mapping_is_obvious(row: dict[str, Any]) -> bool:
    bucket_key = (row.get("bucket_key") or "").strip().lower()
    raw_value = (row.get("raw_option_value") or "").strip().lower()
    proposed_value = (row.get("proposed_value") or "").strip().lower()
    if not raw_value or not proposed_value:
        return False

    normalized_raw = re.sub(r"\s+", " ", raw_value)
    normalized_compact = _normalize_mapping_label(raw_value)

    if bucket_key == "pack_size":
        if proposed_value.isdigit():
            return bool(re.search(rf"(?<!\d){re.escape(proposed_value)}(?!\d)", raw_value))
        return proposed_value in normalized_compact

    if bucket_key == "color":
        color_aliases = {
            "grey": {"grey", "gray"},
            "gray": {"grey", "gray"},
            "gold": {"gold", "titanium gold"},
            "white": {"white"},
            "black": {"black"},
            "beige": {"beige", "bone"},
            "green": {"green"},
        }
        aliases = color_aliases.get(proposed_value, {proposed_value})
        return any(alias in normalized_raw for alias in aliases)

    if bucket_key == "material":
        material_aliases = {
            "steel": {"steel", "stainless steel", "stainless"},
            "rubber": {"rubber"},
            "poly": {"poly", "polyurethane", "poly deck", "poly decking"},
            "microfiber": {"microfiber"},
            "pillow-top": {"pillow top", "pillowtop"},
            "spring": {"spring", "box spring", "coil spring", "innerspring"},
            "foam": {"foam"},
        }
        aliases = material_aliases.get(proposed_value, {proposed_value})
        return any(alias in normalized_raw for alias in aliases)

    return False


def list_pending_mapping_reviews(store_hash: str, limit: int = 100) -> list[dict[str, Any]]:
    sql = """
        SELECT *
        FROM (
            SELECT
                'option_name'::text AS mapping_kind,
                mapping_id::text AS mapping_id,
                raw_option_name,
                NULL::text AS raw_option_value,
                bucket_key,
                normalized_name AS proposed_value,
                confidence,
                review_status,
                metadata,
                updated_at
            FROM app_runtime.store_option_name_mappings
            WHERE store_hash = %s
              AND review_status = %s
            UNION ALL
            SELECT
                'option_value'::text AS mapping_kind,
                value_mapping_id::text AS mapping_id,
                raw_option_name,
                raw_option_value,
                bucket_key,
                canonical_value AS proposed_value,
                confidence,
                review_status,
                metadata,
                updated_at
            FROM app_runtime.store_option_value_mappings
            WHERE store_hash = %s
              AND review_status = %s
        ) mapping_rows
        ORDER BY confidence ASC, updated_at DESC
        LIMIT %s;
    """
    normalized_hash = normalize_store_hash(store_hash)
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                sql,
                (
                    normalized_hash,
                    MAPPING_PENDING_STATUS,
                    normalized_hash,
                    MAPPING_PENDING_STATUS,
                    limit,
                ),
            )
            rows = [dict(row) for row in cur.fetchall()]
    for row in rows:
        row["mapping_ref"] = f"{row['mapping_kind']}:{row['mapping_id']}"
    return rows


def auto_resolve_pending_mappings(
    store_hash: str,
    reviewed_by: str | None = None,
    *,
    get_store_profile_summary: Callable[[str], dict[str, Any]],
) -> dict[str, Any]:
    pending_rows = list_pending_mapping_reviews(store_hash, limit=5000)
    approve_refs: list[str] = []
    ignore_refs: list[str] = []

    for row in pending_rows:
        mapping_ref = row.get("mapping_ref")
        if not mapping_ref:
            continue
        if row.get("mapping_kind") == "option_name":
            if _should_ignore_option_name_mapping(row):
                ignore_refs.append(mapping_ref)
        elif row.get("mapping_kind") == "option_value":
            if _value_mapping_is_obvious(row):
                approve_refs.append(mapping_ref)

    approve_result = review_mapping_rows(
        store_hash,
        approve_refs,
        "approved",
        reviewed_by=reviewed_by or "fulcrum-auto-resolve",
        note="Auto-approved obvious mapping based on direct value match.",
        get_store_profile_summary=get_store_profile_summary,
    )
    ignore_result = review_mapping_rows(
        store_hash,
        ignore_refs,
        "ignored",
        reviewed_by=reviewed_by or "fulcrum-auto-resolve",
        note="Ignored noisy option label mapping that should not block onboarding.",
        get_store_profile_summary=get_store_profile_summary,
    )
    remaining = get_store_readiness(store_hash)
    return {
        "approved_refs": len(approve_refs),
        "ignored_refs": len(ignore_refs),
        "approved_result": approve_result,
        "ignored_result": ignore_result,
        "remaining_unresolved_option_names": remaining.get("unresolved_option_name_count"),
        "remaining_unresolved_option_values": remaining.get("unresolved_option_value_count"),
    }


def review_mapping_rows(
    store_hash: str,
    mapping_refs: list[str],
    review_status: str,
    reviewed_by: str | None = None,
    note: str | None = None,
    *,
    get_store_profile_summary: Callable[[str], dict[str, Any]],
) -> dict[str, int]:
    normalized_hash = normalize_store_hash(store_hash)
    if not mapping_refs:
        return {"updated_option_names": 0, "updated_option_values": 0}

    option_name_ids: list[int] = []
    option_value_ids: list[int] = []
    for mapping_ref in mapping_refs:
        prefix, _, raw_id = (mapping_ref or "").partition(":")
        if not raw_id:
            continue
        try:
            numeric_id = int(raw_id)
        except ValueError:
            continue
        if prefix == "option_name":
            option_name_ids.append(numeric_id)
        elif prefix == "option_value":
            option_value_ids.append(numeric_id)

    metadata = json.dumps(
        {
            "reviewed_by": reviewed_by or "fulcrum",
            "review_note": note or "",
            "review_status": review_status,
        }
    )
    updated_option_names = 0
    updated_option_values = 0

    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            if option_name_ids:
                cur.execute(
                    """
                    UPDATE app_runtime.store_option_name_mappings
                    SET review_status = %s,
                        metadata = metadata || %s::jsonb,
                        updated_at = NOW()
                    WHERE store_hash = %s
                      AND mapping_id = ANY(%s::bigint[]);
                    """,
                    (review_status, metadata, normalized_hash, option_name_ids),
                )
                updated_option_names = cur.rowcount
            if option_value_ids:
                cur.execute(
                    """
                    UPDATE app_runtime.store_option_value_mappings
                    SET review_status = %s,
                        metadata = metadata || %s::jsonb,
                        updated_at = NOW()
                    WHERE store_hash = %s
                      AND value_mapping_id = ANY(%s::bigint[]);
                    """,
                    (review_status, metadata, normalized_hash, option_value_ids),
                )
                updated_option_values = cur.rowcount
        conn.commit()

    refresh_store_readiness(
        normalized_hash,
        get_store_profile_summary=get_store_profile_summary,
    )
    return {
        "updated_option_names": updated_option_names,
        "updated_option_values": updated_option_values,
    }


def get_store_readiness(store_hash: str) -> dict[str, Any]:
    sql = """
        SELECT
            store_hash,
            catalog_synced,
            attribute_mappings_ready,
            theme_hook_ready,
            auto_publish_ready,
            category_beta_ready,
            unresolved_option_name_count,
            unresolved_option_value_count,
            metadata,
            updated_at
        FROM app_runtime.store_readiness
        WHERE store_hash = %s;
    """
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (normalize_store_hash(store_hash),))
            row = cur.fetchone()
    return dict(row) if row else {}


def category_publishing_enabled_for_store(store_hash: str) -> bool:
    readiness = get_store_readiness(store_hash)
    metadata = dict(readiness.get("metadata") or {})
    return bool(
        Config.FULCRUM_ENABLE_CATEGORY_PUBLISHING
        or metadata.get("category_publishing_enabled_override")
    )


def refresh_store_readiness(
    store_hash: str,
    *,
    get_store_profile_summary: Callable[[str], dict[str, Any]],
) -> dict[str, Any]:
    normalized_hash = normalize_store_hash(store_hash)
    _normalize_mapping_review_statuses(normalized_hash)
    current = get_store_readiness(normalized_hash)
    current_metadata = dict(current.get("metadata") or {})
    profile_summary = get_store_profile_summary(normalized_hash)

    unresolved_sql = """
        SELECT
            (SELECT COUNT(*) FROM app_runtime.store_option_name_mappings WHERE store_hash = %s AND review_status = %s) AS unresolved_option_name_count,
            (SELECT COUNT(*) FROM app_runtime.store_option_value_mappings WHERE store_hash = %s AND review_status = %s) AS unresolved_option_value_count;
    """
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                unresolved_sql,
                (
                    normalized_hash,
                    MAPPING_PENDING_STATUS,
                    normalized_hash,
                    MAPPING_PENDING_STATUS,
                ),
            )
            unresolved = dict(cur.fetchone() or {})

    unresolved_name_count = int(unresolved.get("unresolved_option_name_count") or 0)
    unresolved_value_count = int(unresolved.get("unresolved_option_value_count") or 0)
    catalog_synced = bool(
        (profile_summary.get("profile_count") or 0) > 0
        and (profile_summary.get("category_profile_count") or 0) > 0
    )
    attribute_mappings_ready = bool(
        catalog_synced
        and (profile_summary.get("option_name_mapping_count") or 0) > 0
        and (profile_summary.get("option_value_mapping_count") or 0) > 0
        and unresolved_name_count == 0
        and unresolved_value_count == 0
    )
    product_template_path = Path(Config.FULCRUM_THEME_PRODUCT_TEMPLATE)
    category_template_path = Path(Config.FULCRUM_THEME_CATEGORY_TEMPLATE)
    product_hook_ready = bool(
        (
            product_template_path.exists()
            and render_theme_hook_present(Config.FULCRUM_THEME_PRODUCT_TEMPLATE)
        )
        or current.get("theme_hook_ready")
    )
    category_hook_ready = bool(
        (
            category_template_path.exists()
            and render_category_theme_hook_present(Config.FULCRUM_THEME_CATEGORY_TEMPLATE)
        )
        or current_metadata.get("category_theme_hook_present")
    )
    category_flags = {
        "category_metafields_readable": bool(current_metadata.get("category_metafields_readable")),
        "category_render_verified": bool(current_metadata.get("category_render_verified")),
        "category_rollback_verified": bool(current_metadata.get("category_rollback_verified")),
    }
    category_publishing_enabled = bool(
        Config.FULCRUM_ENABLE_CATEGORY_PUBLISHING
        or current_metadata.get("category_publishing_enabled_override")
    )
    auto_publish_ready = bool(
        Config.FULCRUM_AUTO_PUBLISH_ENABLED
        and catalog_synced
        and product_hook_ready
    )
    category_beta_ready = bool(
        category_publishing_enabled
        and product_hook_ready
        and category_hook_ready
        and all(category_flags.values())
    )
    metadata = {
        **current_metadata,
        "alpha_default_buckets": list(ALPHA_DEFAULT_BUCKETS),
        "category_theme_hook_present": category_hook_ready,
        "feature_flags": {
            "category_publishing_enabled": category_publishing_enabled,
            "category_publishing_global_default": Config.FULCRUM_ENABLE_CATEGORY_PUBLISHING,
            "auto_publish_enabled": Config.FULCRUM_AUTO_PUBLISH_ENABLED,
            "auto_publish_min_score": Config.FULCRUM_AUTO_PUBLISH_MIN_SCORE,
            "auto_publish_max_links_per_source": Config.FULCRUM_AUTO_PUBLISH_MAX_LINKS_PER_SOURCE,
            "require_review_for_categories": Config.FULCRUM_REQUIRE_REVIEW_FOR_CATEGORIES,
        },
    }

    sql = """
        INSERT INTO app_runtime.store_readiness (
            store_hash,
            catalog_synced,
            attribute_mappings_ready,
            theme_hook_ready,
            auto_publish_ready,
            category_beta_ready,
            unresolved_option_name_count,
            unresolved_option_value_count,
            metadata,
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW())
        ON CONFLICT (store_hash) DO UPDATE SET
            catalog_synced = EXCLUDED.catalog_synced,
            attribute_mappings_ready = EXCLUDED.attribute_mappings_ready,
            theme_hook_ready = EXCLUDED.theme_hook_ready,
            auto_publish_ready = EXCLUDED.auto_publish_ready,
            category_beta_ready = EXCLUDED.category_beta_ready,
            unresolved_option_name_count = EXCLUDED.unresolved_option_name_count,
            unresolved_option_value_count = EXCLUDED.unresolved_option_value_count,
            metadata = app_runtime.store_readiness.metadata || EXCLUDED.metadata,
            updated_at = NOW()
        RETURNING
            store_hash,
            catalog_synced,
            attribute_mappings_ready,
            theme_hook_ready,
            auto_publish_ready,
            category_beta_ready,
            unresolved_option_name_count,
            unresolved_option_value_count,
            metadata,
            updated_at;
    """
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                sql,
                (
                    normalized_hash,
                    catalog_synced,
                    attribute_mappings_ready,
                    product_hook_ready,
                    auto_publish_ready,
                    category_beta_ready,
                    unresolved_name_count,
                    unresolved_value_count,
                    json.dumps(metadata),
                ),
            )
            row = dict(cur.fetchone() or {})
        conn.commit()
    return row


__all__ = [
    "_normalize_mapping_review_statuses",
    "auto_resolve_pending_mappings",
    "category_publishing_enabled_for_store",
    "get_store_readiness",
    "list_pending_mapping_reviews",
    "refresh_store_readiness",
    "review_mapping_rows",
]
