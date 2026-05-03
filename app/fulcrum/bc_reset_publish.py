from __future__ import annotations

import csv
import json
from collections import Counter
from pathlib import Path
from typing import Any

import requests

from app.fulcrum.candidate_runs import candidate_publish_block_reason
from app.fulcrum.config import Config
from app.fulcrum.platform import get_bc_headers, list_store_categories, list_store_products, normalize_store_hash
from app.fulcrum.services import (
    category_publishing_enabled_for_store,
    get_pg_conn,
    list_publications,
    publish_approved_entities,
    unpublish_entities,
)
from app.fulcrum.storefront import get_storefront_base_url


LINK_METAFIELD_KEYS = {
    "internal_links_html",
    "internal_category_links_html",
    "internal_product_links_html",
}

CLEANUP_REPORT_FIELDS = [
    "batch_number",
    "cleanup_reason",
    "review_target_spec",
    "entity_type",
    "entity_id",
    "entity_name",
    "entity_url",
    "metafield_id",
    "metafield_key",
    "active_publication_match_status",
    "policy_block_reason",
    "storefront_url",
    "storefront_check_command",
]


def parse_reviewed_metafield_spec(value: str) -> dict[str, int | str]:
    parts = [part.strip() for part in (value or "").split(":")]
    if len(parts) != 3:
        raise ValueError("Reviewed metafield targets must use `<product|category>:<entity_id>:<metafield_id>`.")
    entity_type = parts[0].lower()
    if entity_type == "categories":
        entity_type = "category"
    if entity_type.endswith("s"):
        entity_type = entity_type[:-1]
    if entity_type not in {"product", "category"}:
        raise ValueError("Reviewed metafield entity type must be `product` or `category`.")
    try:
        entity_id = int(parts[1])
        metafield_id = int(parts[2])
    except ValueError as exc:
        raise ValueError("Reviewed metafield entity and metafield ids must be integers.") from exc
    if entity_id <= 0 or metafield_id <= 0:
        raise ValueError("Reviewed metafield entity and metafield ids must be positive.")
    return {"entity_type": entity_type, "entity_id": entity_id, "metafield_id": metafield_id}


def _normalize_reviewed_metafield_targets(values: list[dict[str, Any] | str] | None) -> list[dict[str, int | str]]:
    targets: list[dict[str, int | str]] = []
    seen: set[tuple[str, int, int]] = set()
    for value in values or []:
        if isinstance(value, str):
            target = parse_reviewed_metafield_spec(value)
        else:
            target = parse_reviewed_metafield_spec(
                f"{value.get('entity_type')}:{value.get('entity_id')}:{value.get('metafield_id')}"
            )
        key = (
            str(target["entity_type"]),
            int(target["entity_id"]),
            int(target["metafield_id"]),
        )
        if key not in seen:
            seen.add(key)
            targets.append(target)
    return targets


def _normalize_url_path(url: str | None) -> str:
    value = (url or "").strip()
    if not value:
        return ""
    if not value.startswith("/"):
        value = f"/{value}"
    return value.rstrip("/") + "/"


def _route_block_heading_for_key(key: str | None) -> str:
    normalized_key = (key or "").strip()
    if normalized_key == "internal_category_links_html":
        return "Related Categories"
    if normalized_key == "internal_product_links_html":
        return "Shop Matching Products"
    return "Related options"


def _absolute_storefront_url(store_hash: str, entity_url: str | None) -> str:
    value = (entity_url or "").strip()
    if not value:
        return ""
    if value.startswith(("http://", "https://")):
        return value
    if not value.startswith("/"):
        value = f"/{value}"
    return f"{get_storefront_base_url(store_hash).rstrip('/')}{value}"


def _storefront_check_command(storefront_url: str, metafield_key: str | None) -> str:
    if not storefront_url:
        return ""
    heading = _route_block_heading_for_key(metafield_key)
    return (
        f"$html=(Invoke-WebRequest -Uri '{storefront_url}' -UseBasicParsing -TimeoutSec 60).Content; "
        f"[PSCustomObject]@{{ContainsRouteAuthorityBlock=($html -match 'h4h-internal-links'); "
        f"ContainsExpectedHeading=($html -match '{heading}')}}"
    )


def _api_base(store_hash: str) -> str:
    return f"https://api.bigcommerce.com/stores/{normalize_store_hash(store_hash)}/v3"


def _entity_path(entity_type: str) -> str:
    return "products" if (entity_type or "").strip().lower() == "product" else "categories"


def _custom_url_value(entity: dict[str, Any]) -> str:
    custom_url = entity.get("custom_url") or ""
    if isinstance(custom_url, dict):
        return custom_url.get("url") or ""
    return str(custom_url or "")


def _require_allowed_store(store_hash: str) -> None:
    normalized_hash = normalize_store_hash(store_hash)
    allowed = {normalize_store_hash(item) for item in (Config.FULCRUM_ALLOWED_STORES or []) if item}
    if allowed and normalized_hash not in allowed:
        raise ValueError(f"Store `{normalized_hash}` is not in FULCRUM_ALLOWED_STORES.")


def _gate_disposition_counts(store_hash: str) -> dict[str, int]:
    sql = """
        WITH latest_run AS (
            SELECT MAX(run_id) AS run_id
            FROM app_runtime.query_gate_records
            WHERE store_hash = %s
        )
        SELECT COALESCE(disposition, 'hold') AS disposition, COUNT(*) AS row_count
        FROM app_runtime.query_gate_records
        WHERE store_hash = %s
          AND run_id = (SELECT run_id FROM latest_run)
        GROUP BY COALESCE(disposition, 'hold');
    """
    counts = {"pass": 0, "hold": 0, "reject": 0}
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (normalize_store_hash(store_hash), normalize_store_hash(store_hash)))
            for disposition, row_count in cur.fetchall():
                counts[str(disposition or "hold")] = int(row_count or 0)
    return counts


def _fetch_entity_summary(
    store_hash: str,
    entity_type: str,
    entity_id: int,
    *,
    headers: dict[str, str],
    api_base: str,
) -> dict[str, Any]:
    response = requests.get(
        f"{api_base}/catalog/{_entity_path(entity_type)}/{int(entity_id)}",
        headers=headers,
        params={"include_fields": "id,name,custom_url"},
        timeout=30,
    )
    response.raise_for_status()
    return dict(response.json().get("data") or {})


def _scan_entity_metafields(
    store_hash: str,
    entity_type: str,
    entity: dict[str, Any],
    *,
    headers: dict[str, str],
    api_base: str,
) -> list[dict[str, Any]]:
    entity_id = int(entity.get("id") or 0)
    if not entity_id:
        return []
    response = requests.get(
        f"{api_base}/catalog/{_entity_path(entity_type)}/{entity_id}/metafields",
        headers=headers,
        params={"namespace": "h4h", "limit": 250},
        timeout=30,
    )
    response.raise_for_status()
    rows: list[dict[str, Any]] = []
    for metafield in response.json().get("data", []):
        key = (metafield.get("key") or "").strip()
        if key not in LINK_METAFIELD_KEYS:
            continue
        rows.append(
            {
                "entity_type": entity_type,
                "entity_id": entity_id,
                "entity_name": entity.get("name") or "",
                "entity_url": _custom_url_value(entity),
                "metafield_id": int(metafield.get("id") or 0),
                "key": key,
            }
        )
    return rows


def list_remote_link_metafields(
    store_hash: str,
    *,
    product_ids: list[int] | None = None,
    category_ids: list[int] | None = None,
    max_entities: int | None = None,
) -> list[dict[str, Any]]:
    _require_allowed_store(store_hash)
    headers = get_bc_headers(store_hash)
    api_base = _api_base(store_hash)
    results: list[dict[str, Any]] = []

    scanned_count = 0
    normalized_product_ids = [int(entity_id) for entity_id in product_ids or [] if int(entity_id or 0)]
    normalized_category_ids = [int(entity_id) for entity_id in category_ids or [] if int(entity_id or 0)]
    has_product_filter = bool(normalized_product_ids)
    has_category_filter = bool(normalized_category_ids)
    product_entities = (
        [
            _fetch_entity_summary(store_hash, "product", entity_id, headers=headers, api_base=api_base)
            for entity_id in normalized_product_ids
        ]
        if has_product_filter
        else ([] if has_category_filter else list_store_products(store_hash))
    )
    category_entities = (
        [
            _fetch_entity_summary(store_hash, "category", entity_id, headers=headers, api_base=api_base)
            for entity_id in normalized_category_ids
        ]
        if has_category_filter
        else ([] if has_product_filter else list_store_categories(store_hash))
    )

    for entity_type, entities in (("product", product_entities), ("category", category_entities)):
        for entity in entities:
            if max_entities is not None and scanned_count >= max(0, int(max_entities)):
                return results
            scanned_count += 1
            results.extend(
                _scan_entity_metafields(
                    store_hash,
                    entity_type,
                    entity,
                    headers=headers,
                    api_base=api_base,
                )
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


def _remote_metafield_target_key(row: dict[str, Any]) -> tuple[str, int, int]:
    return (
        (row.get("entity_type") or "").strip().lower(),
        int(row.get("entity_id") or 0),
        int(row.get("metafield_id") or 0),
    )


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


def _policy_block_reason_map(
    approved_candidate_rows: list[dict[str, Any]],
    category_enabled: bool,
) -> tuple[set[tuple[str, str]], dict[tuple[str, str], list[str]]]:
    publishable_source_keys: set[tuple[str, str]] = set()
    blocked_reasons: dict[tuple[str, str], list[str]] = {}
    for row in approved_candidate_rows:
        source_key = _source_key_for_row(row)
        reason = candidate_publish_block_reason(row, category_enabled)
        if reason:
            blocked_reasons.setdefault(source_key, [])
            if reason not in blocked_reasons[source_key]:
                blocked_reasons[source_key].append(reason)
        else:
            publishable_source_keys.add(source_key)
    return publishable_source_keys, blocked_reasons


def _latest_approved_candidate_rows(store_hash: str) -> list[dict[str, Any]]:
    sql = """
        WITH latest_pairs AS (
            SELECT DISTINCT ON (source_entity_type, source_product_id, target_entity_type, target_product_id)
                candidate_id,
                source_entity_type,
                target_entity_type,
                source_entity_id,
                target_entity_id,
                source_product_id,
                source_name,
                source_url,
                target_product_id,
                target_name,
                target_url,
                review_status,
                metadata,
                created_at
            FROM app_runtime.link_candidates
            WHERE store_hash = %s
            ORDER BY
                source_entity_type,
                source_product_id,
                target_entity_type,
                target_product_id,
                candidate_id DESC
        )
        SELECT *
        FROM latest_pairs
        WHERE review_status = 'approved';
    """
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (normalize_store_hash(store_hash),))
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]


def _source_key_for_row(row: dict[str, Any]) -> tuple[str, str]:
    return (
        (row.get("source_entity_type") or row.get("entity_type") or "product").strip().lower(),
        _normalize_url_path(row.get("source_url") or row.get("entity_url")),
    )


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


def build_cleanup_candidate_report(
    store_hash: str,
    *,
    product_ids: list[int] | None = None,
    category_ids: list[int] | None = None,
    max_entities: int | None = None,
    batch_size: int = 50,
    storefront_check_hints: bool = False,
) -> dict[str, Any]:
    normalized_hash = normalize_store_hash(store_hash)
    _require_allowed_store(normalized_hash)
    remote_before = list_remote_link_metafields(
        normalized_hash,
        product_ids=product_ids,
        category_ids=category_ids,
        max_entities=max_entities,
    )
    active_publications = list_publications(normalized_hash, active_only=True, limit=5000)
    active_keyset = _active_publication_keyset(normalized_hash)
    category_enabled = category_publishing_enabled_for_store(normalized_hash)
    approved_candidate_rows = _latest_approved_candidate_rows(normalized_hash)
    publishable_source_keys, blocked_reasons = _policy_block_reason_map(approved_candidate_rows, category_enabled)
    policy_blocked_source_keys = set(blocked_reasons) - publishable_source_keys

    normalized_batch_size = max(int(batch_size or 50), 1)
    raw_candidates: list[dict[str, Any]] = []
    seen_targets: set[tuple[str, int, int]] = set()
    for row in remote_before:
        entity_type = (row.get("entity_type") or "").strip().lower()
        entity_url = row.get("entity_url")
        metafield_key = (row.get("key") or "").strip()
        active_publication_key = (entity_type, _normalize_url_path(entity_url), metafield_key)
        source_key = _source_key_for_row(row)
        is_orphan = active_publication_key not in active_keyset
        is_policy_blocked = source_key in policy_blocked_source_keys
        if not is_orphan and not is_policy_blocked:
            continue

        target_key = _remote_metafield_target_key(row)
        if target_key in seen_targets:
            continue
        seen_targets.add(target_key)

        cleanup_reasons: list[str] = []
        if is_orphan:
            cleanup_reasons.append("orphan_remote")
        if is_policy_blocked:
            cleanup_reasons.append("policy_blocked_active_remote")

        storefront_url = _absolute_storefront_url(normalized_hash, entity_url)
        raw_candidates.append(
            {
                "cleanup_reason": ";".join(cleanup_reasons),
                "review_target_spec": f"{entity_type}:{int(row.get('entity_id') or 0)}:{int(row.get('metafield_id') or 0)}",
                "entity_type": entity_type,
                "entity_id": int(row.get("entity_id") or 0),
                "entity_name": row.get("entity_name") or "",
                "entity_url": entity_url or "",
                "metafield_id": int(row.get("metafield_id") or 0),
                "metafield_key": metafield_key,
                "active_publication_match_status": "matched" if not is_orphan else "missing",
                "policy_block_reason": "; ".join(blocked_reasons.get(source_key, [])),
                "storefront_url": storefront_url,
                "storefront_check_command": _storefront_check_command(storefront_url, metafield_key)
                if storefront_check_hints
                else "",
            }
        )

    raw_candidates.sort(
        key=lambda item: (
            item["cleanup_reason"],
            item["entity_type"],
            int(item["entity_id"]),
            item["metafield_key"],
            int(item["metafield_id"]),
        )
    )
    for index, row in enumerate(raw_candidates):
        row["batch_number"] = (index // normalized_batch_size) + 1

    reason_counts = Counter()
    for row in raw_candidates:
        for reason in str(row.get("cleanup_reason") or "").split(";"):
            if reason:
                reason_counts[reason] += 1

    return {
        "store_hash": normalized_hash,
        "execute": False,
        "report_only": True,
        "scan_filters": {
            "product_ids": [int(entity_id) for entity_id in product_ids or []],
            "category_ids": [int(entity_id) for entity_id in category_ids or []],
            "max_entities": max_entities,
            "filtered_scan": bool(product_ids or category_ids or max_entities is not None),
        },
        "batch_size": normalized_batch_size,
        "batch_count": (len(raw_candidates) + normalized_batch_size - 1) // normalized_batch_size,
        "remote_before_count": len(remote_before),
        "active_publications_before_count": len(active_publications),
        "candidate_count": len(raw_candidates),
        "candidate_counts_by_reason": dict(reason_counts),
        "remote_before_by_key": dict(Counter(row.get("key") or "" for row in remote_before)),
        "candidate_counts_by_key": dict(Counter(row.get("metafield_key") or "" for row in raw_candidates)),
        "candidates": raw_candidates,
    }


def write_cleanup_candidate_report(report: dict[str, Any], output_base_path: str | Path) -> dict[str, str]:
    base_path = Path(output_base_path)
    if base_path.suffix.lower() in {".json", ".csv"}:
        base_path = base_path.with_suffix("")
    base_path.parent.mkdir(parents=True, exist_ok=True)
    json_path = base_path.with_suffix(".json")
    csv_path = base_path.with_suffix(".csv")

    json_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")

    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CLEANUP_REPORT_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for row in report.get("candidates", []):
            writer.writerow(row)

    return {"json_path": str(json_path), "csv_path": str(csv_path)}


def reset_and_republish_bigcommerce_links(
    store_hash: str,
    execute: bool = False,
    *,
    product_ids: list[int] | None = None,
    category_ids: list[int] | None = None,
    max_entities: int | None = None,
    reviewed_metafields: list[dict[str, Any] | str] | None = None,
) -> dict[str, Any]:
    normalized_hash = normalize_store_hash(store_hash)
    _require_allowed_store(normalized_hash)
    reviewed_targets = _normalize_reviewed_metafield_targets(reviewed_metafields)
    reviewed_product_ids = [int(target["entity_id"]) for target in reviewed_targets if target["entity_type"] == "product"]
    reviewed_category_ids = [int(target["entity_id"]) for target in reviewed_targets if target["entity_type"] == "category"]
    scan_product_ids = product_ids or (reviewed_product_ids if execute and reviewed_targets else None)
    scan_category_ids = category_ids or (reviewed_category_ids if execute and reviewed_targets else None)
    filtered_scan = bool(scan_product_ids or scan_category_ids or max_entities is not None)
    if execute and filtered_scan and not reviewed_targets:
        raise ValueError("Filtered BigCommerce reset scans are dry-run only.")
    remote_before = list_remote_link_metafields(
        normalized_hash,
        product_ids=scan_product_ids,
        category_ids=scan_category_ids,
        max_entities=max_entities,
    )
    active_publications = list_publications(normalized_hash, active_only=True, limit=5000)
    active_keyset = _active_publication_keyset(normalized_hash)
    category_enabled = category_publishing_enabled_for_store(normalized_hash)
    approved_candidate_rows = _latest_approved_candidate_rows(normalized_hash)
    publishable_source_keys, blocked_reasons = _policy_block_reason_map(approved_candidate_rows, category_enabled)
    policy_blocked_source_keys = set(blocked_reasons) - publishable_source_keys
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
    policy_blocked_remote = [
        row
        for row in remote_before
        if _source_key_for_row(row) in policy_blocked_source_keys
    ]

    summary: dict[str, Any] = {
        "store_hash": normalized_hash,
        "execute": bool(execute),
        "scan_filters": {
            "product_ids": [int(entity_id) for entity_id in product_ids or []],
            "category_ids": [int(entity_id) for entity_id in category_ids or []],
            "max_entities": max_entities,
            "filtered_scan": filtered_scan,
        },
        "reviewed_metafield_targets": reviewed_targets,
        "remote_before_count": len(remote_before),
        "remote_before_by_key": dict(Counter(row.get("key") or "" for row in remote_before)),
        "active_publications_before_count": len(active_publications),
        "active_publications_before_by_key": dict(
            Counter((row.get("metafield_key") or "internal_links_html") for row in active_publications)
        ),
        "approved_source_counts": _approved_source_counts(normalized_hash),
        "latest_gate_disposition_counts": _gate_disposition_counts(normalized_hash),
        "orphan_remote_count": len(orphan_remote),
        "orphan_remote_sample": orphan_remote[:25],
        "policy_blocked_approved_candidate_count": len(
            [row for row in approved_candidate_rows if candidate_publish_block_reason(row, category_enabled)]
        ),
        "policy_blocked_active_remote_count": len(policy_blocked_remote),
        "policy_blocked_active_remote_sample": policy_blocked_remote[:25],
        "reviewed_delete_eligible_count": 0,
        "deleted_reviewed_metafield_count": 0,
        "skipped_reviewed_metafield_targets": [],
        "unpublished_count": 0,
        "deleted_orphan_count": 0,
        "republished_count": 0,
        "republished_by_key": {},
        "remote_after_count": len(remote_before),
        "remote_after_by_key": dict(Counter(row.get("key") or "" for row in remote_before)),
    }

    if not execute:
        return summary

    if reviewed_targets:
        eligible_remote = {
            _remote_metafield_target_key(row): row
            for row in orphan_remote + policy_blocked_remote
        }
        remote_by_key = {
            _remote_metafield_target_key(row): row
            for row in remote_before
        }
        deleted_count = 0
        skipped_targets: list[dict[str, Any]] = []
        for target in reviewed_targets:
            key = (str(target["entity_type"]), int(target["entity_id"]), int(target["metafield_id"]))
            row = eligible_remote.get(key)
            if not row:
                skipped = dict(target)
                skipped["reason"] = "not_found_or_not_currently_orphan_or_policy_blocked"
                skipped["remote_found"] = key in remote_by_key
                skipped_targets.append(skipped)
                continue
            _delete_remote_link_metafield(normalized_hash, row)
            deleted_count += 1

        remote_after = list_remote_link_metafields(
            normalized_hash,
            product_ids=reviewed_product_ids,
            category_ids=reviewed_category_ids,
        )
        summary.update(
            {
                "reviewed_delete_eligible_count": len(eligible_remote),
                "deleted_reviewed_metafield_count": deleted_count,
                "skipped_reviewed_metafield_targets": skipped_targets,
                "remote_after_count": len(remote_after),
                "remote_after_by_key": dict(Counter(row.get("key") or "" for row in remote_after)),
            }
        )
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
