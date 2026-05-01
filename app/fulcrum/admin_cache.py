"""Admin metric cache helpers for Fulcrum."""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
import json
from typing import Any, Callable

from psycopg2.extras import RealDictCursor

from app.fulcrum.platform import get_pg_conn, normalize_store_hash


def json_cache_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): json_cache_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [json_cache_safe(item) for item in value]
    if isinstance(value, tuple):
        return [json_cache_safe(item) for item in value]
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def invalidate_admin_metric_cache(
    store_hash: str,
    metric_keys: list[str] | None = None,
    *,
    apply_runtime_schema_fn: Callable[[], None],
) -> None:
    apply_runtime_schema_fn()
    normalized_hash = normalize_store_hash(store_hash)
    if not normalized_hash:
        return
    if metric_keys:
        sql = """
            DELETE FROM app_runtime.admin_metric_cache
            WHERE store_hash = %s
              AND metric_key = ANY(%s::text[]);
        """
        params = (normalized_hash, metric_keys)
    else:
        sql = """
            DELETE FROM app_runtime.admin_metric_cache
            WHERE store_hash = %s;
        """
        params = (normalized_hash,)
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()


def load_admin_metric_cache(
    store_hash: str,
    metric_key: str,
    *,
    max_age: timedelta,
    apply_runtime_schema_fn: Callable[[], None],
    format_timestamp_display_fn: Callable[[Any], str | None],
    format_relative_time_fn: Callable[[Any], str | None],
) -> dict[str, Any] | None:
    apply_runtime_schema_fn()
    sql = """
        SELECT payload, updated_at
        FROM app_runtime.admin_metric_cache
        WHERE store_hash = %s
          AND metric_key = %s
        LIMIT 1;
    """
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (normalize_store_hash(store_hash), (metric_key or "").strip().lower()))
            row = dict(cur.fetchone() or {})
    if not row:
        return None
    updated_at = row.get("updated_at")
    if not isinstance(updated_at, datetime):
        return None
    age = datetime.now().astimezone() - updated_at.astimezone()
    if age > max_age:
        return None
    payload = dict(row.get("payload") or {})
    payload["cached_at_display"] = format_timestamp_display_fn(updated_at)
    payload["cached_at_relative"] = format_relative_time_fn(updated_at)
    return payload


def store_admin_metric_cache(
    store_hash: str,
    metric_key: str,
    payload: dict[str, Any],
    *,
    apply_runtime_schema_fn: Callable[[], None],
    format_timestamp_display_fn: Callable[[Any], str | None],
    format_relative_time_fn: Callable[[Any], str | None],
) -> dict[str, Any]:
    apply_runtime_schema_fn()
    normalized_hash = normalize_store_hash(store_hash)
    normalized_key = (metric_key or "").strip().lower()
    safe_payload = json_cache_safe(payload or {})
    sql = """
        INSERT INTO app_runtime.admin_metric_cache (
            store_hash,
            metric_key,
            payload,
            updated_at
        ) VALUES (%s, %s, %s::jsonb, NOW())
        ON CONFLICT (store_hash, metric_key) DO UPDATE
        SET payload = EXCLUDED.payload,
            updated_at = NOW()
        RETURNING payload, updated_at;
    """
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (normalized_hash, normalized_key, json.dumps(safe_payload)))
            row = dict(cur.fetchone() or {})
        conn.commit()
    cached_payload = dict(row.get("payload") or safe_payload or {})
    updated_at = row.get("updated_at")
    cached_payload["cached_at_display"] = format_timestamp_display_fn(updated_at)
    cached_payload["cached_at_relative"] = format_relative_time_fn(updated_at)
    return cached_payload


__all__ = [
    "invalidate_admin_metric_cache",
    "json_cache_safe",
    "load_admin_metric_cache",
    "store_admin_metric_cache",
]
