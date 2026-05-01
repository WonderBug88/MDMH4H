"""Quality and logic-reporting helpers for Fulcrum."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from psycopg2.extras import RealDictCursor

from app.fulcrum.platform import get_pg_conn, normalize_store_hash


def get_entity_coverage_summary(
    store_hash: str,
    *,
    load_store_brand_profiles_fn: Callable[[str], list[dict[str, Any]]],
    load_store_content_profiles_fn: Callable[..., list[dict[str, Any]]],
) -> dict[str, int]:
    normalized_hash = normalize_store_hash(store_hash)
    sql = """
        SELECT
            (SELECT COUNT(*) FROM app_runtime.store_product_profiles WHERE store_hash = %s AND eligible_for_routing = TRUE) AS product_count,
            (SELECT COUNT(*) FROM app_runtime.store_product_profiles WHERE store_hash = %s AND eligible_for_routing = TRUE AND is_canonical_target = TRUE) AS canonical_product_count,
            (SELECT COUNT(*) FROM app_runtime.store_category_profiles WHERE store_hash = %s AND eligible_for_routing = TRUE) AS category_count,
            (SELECT COUNT(*) FROM app_runtime.store_category_profiles WHERE store_hash = %s AND eligible_for_routing = TRUE AND is_canonical_target = TRUE) AS canonical_category_count;
    """
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (normalized_hash, normalized_hash, normalized_hash, normalized_hash))
            row = dict(cur.fetchone() or {})

    brand_count = len(load_store_brand_profiles_fn(normalized_hash))
    content_count = len(load_store_content_profiles_fn(normalized_hash, include_backlog=True))
    return {
        "product_count": int(row.get("product_count") or 0),
        "canonical_product_count": int(row.get("canonical_product_count") or 0),
        "category_count": int(row.get("category_count") or 0),
        "canonical_category_count": int(row.get("canonical_category_count") or 0),
        "brand_count": int(brand_count),
        "content_count": int(content_count),
    }


def format_logic_change_timestamp(value: str | None) -> str:
    text = (value or "").strip()
    if not text:
        return ""
    try:
        dt = datetime.fromisoformat(text)
        return dt.strftime("%Y-%m-%d %I:%M %p")
    except ValueError:
        return text


def format_logic_validation_status(status: str | None) -> str:
    normalized = (status or "").strip().lower()
    if normalized == "verified_pass":
        return "Verified Pass"
    if normalized == "verified_fail":
        return "Verified Fail"
    if normalized == "untested":
        return "Untested"
    return normalized.replace("_", " ").title()


def logic_validation_bucket(status: str | None) -> str:
    normalized = (status or "").strip().lower()
    if normalized in {"pass", "verified_pass"}:
        return "pass"
    if normalized in {"fail", "verified_fail"}:
        return "fail"
    return "untested"


def load_logic_change_log(
    *,
    changelog_path: Path,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    if not changelog_path.exists():
        return []
    try:
        payload = json.loads(changelog_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    if not isinstance(payload, list):
        return []
    rows: list[dict[str, Any]] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        row = dict(item)
        row["timestamp"] = (row.get("timestamp") or "").strip()
        row["timestamp_display"] = format_logic_change_timestamp(row.get("timestamp"))
        validation = row.get("validation") if isinstance(row.get("validation"), dict) else {}
        row["validation"] = validation
        row["validation_status_label"] = format_logic_validation_status(validation.get("status"))
        row["validation_verified_display"] = format_logic_change_timestamp(validation.get("verified_at"))
        rows.append(row)
    rows.sort(key=lambda item: item.get("timestamp") or "", reverse=True)
    if limit is not None:
        return rows[: max(int(limit or 0), 0)]
    return rows


def get_logic_change_summary(
    *,
    changelog_path: Path,
    limit: int = 5,
) -> dict[str, Any]:
    entries = load_logic_change_log(changelog_path=changelog_path, limit=None)
    latest = entries[0] if entries else {}
    verified_pass_count = 0
    verified_fail_count = 0
    untested_count = 0
    for entry in entries:
        bucket = logic_validation_bucket((entry.get("validation") or {}).get("status"))
        if bucket == "pass":
            verified_pass_count += 1
        elif bucket == "fail":
            verified_fail_count += 1
        else:
            untested_count += 1
    return {
        "revision_count": len(entries),
        "verified_pass_count": verified_pass_count,
        "verified_fail_count": verified_fail_count,
        "untested_count": untested_count,
        "needs_review_count": verified_fail_count + untested_count,
        "latest": latest,
        "recent_changes": entries[: max(int(limit or 0), 0)],
    }


__all__ = [
    "format_logic_change_timestamp",
    "format_logic_validation_status",
    "get_entity_coverage_summary",
    "get_logic_change_summary",
    "logic_validation_bucket",
    "load_logic_change_log",
]
