"""Merchant-facing onboarding helpers for Route Authority."""

from __future__ import annotations

from base64 import urlsafe_b64encode
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import secrets
from typing import Any
from urllib.parse import urlparse

from cryptography.fernet import Fernet
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from itsdangerous import BadSignature, URLSafeSerializer
from psycopg2.extras import RealDictCursor, execute_batch

from app.fulcrum.config import Config
from app.fulcrum.platform import get_pg_conn, normalize_store_hash
from app.fulcrum.readiness import get_store_readiness
from app.fulcrum.rendering import category_theme_hook_present, theme_hook_present
from app.fulcrum.storefront import get_store_profile_summary, list_storefront_base_urls


REQUIRED_CHECKLIST_KEYS = (
    "bigcommerce_install",
    "search_console",
    "ga4",
    "catalog_sync",
    "theme_verification",
    "readiness",
)
GOOGLE_INTEGRATION_KEYS = {"gsc", "ga4"}
INTEGRATION_DISPLAY = {
    "bigcommerce": "BigCommerce",
    "gsc": "Search Console",
    "ga4": "Google Analytics 4",
}
GOOGLE_SCOPES = {
    "gsc": ["https://www.googleapis.com/auth/webmasters.readonly"],
    "ga4": ["https://www.googleapis.com/auth/analytics.readonly"],
}
LIVE_GSC_CACHE_KEYS = ["live_gsc_performance", "live_gsc_performance_store_scoped_v2"]
COMPARISON_WINDOW_END_DAYS_AGO = 2
COMPARISON_WINDOW_LOOKBACK_DAYS = 454
GOOGLE_API_TIMEOUT_SECONDS = 60
SYNC_RUN_ACTIVE_STATUSES = {"queued", "running"}
SYNC_RUN_FINAL_STATUSES = {"succeeded", "warning", "failed"}


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _iso_now() -> str:
    return _utcnow().isoformat()


def _normalized_host(value: str | None) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    if raw.startswith("sc-domain:"):
        raw = raw.split(":", 1)[1]
    elif "://" not in raw:
        raw = "https://" + raw
    parsed = urlparse(raw)
    host = (parsed.netloc or parsed.path or "").strip().lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def _comparison_window_dates(reference_today: date | None = None) -> tuple[date, date]:
    today = reference_today or date.today()
    end_date = today - timedelta(days=COMPARISON_WINDOW_END_DAYS_AGO)
    start_date = end_date - timedelta(days=COMPARISON_WINDOW_LOOKBACK_DAYS)
    return start_date, end_date


def _comparison_window_relative_dates() -> tuple[str, str]:
    start_days_ago = COMPARISON_WINDOW_END_DAYS_AGO + COMPARISON_WINDOW_LOOKBACK_DAYS
    return f"{start_days_ago}daysAgo", f"{COMPARISON_WINDOW_END_DAYS_AGO}daysAgo"


def _gsc_sync_window_dates(reference_today: date | None = None) -> tuple[date, date]:
    today = reference_today or date.today()
    lookback_days = max(1, int(getattr(Config, "FULCRUM_GSC_SYNC_LOOKBACK_DAYS", 180) or 180))
    end_date = today - timedelta(days=COMPARISON_WINDOW_END_DAYS_AGO)
    start_date = end_date - timedelta(days=lookback_days)
    return start_date, end_date


def _gsc_api_row_limit() -> int:
    configured = int(getattr(Config, "FULCRUM_GSC_API_ROW_LIMIT", 25000) or 25000)
    return min(max(configured, 1), 25000)


def _gsc_sync_max_rows() -> int:
    configured = int(getattr(Config, "FULCRUM_GSC_SYNC_MAX_ROWS", 100000) or 100000)
    return max(configured, 1)


def _gsc_sync_min_impressions() -> int:
    configured = int(getattr(Config, "FULCRUM_GSC_SYNC_MIN_IMPRESSIONS", 3) or 3)
    return max(configured, 0)


def _invalidate_store_metric_cache(store_hash: str, metric_keys: list[str]) -> None:
    normalized_hash = normalize_store_hash(store_hash)
    if not normalized_hash or not metric_keys:
        return
    sql = """
        DELETE FROM app_runtime.admin_metric_cache
        WHERE store_hash = %s
          AND metric_key = ANY(%s::text[]);
    """
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (normalized_hash, metric_keys))
        conn.commit()


def _google_client_config() -> dict[str, dict[str, str]]:
    if not Config.GOOGLE_OAUTH_CLIENT_ID or not Config.GOOGLE_OAUTH_CLIENT_SECRET:
        raise RuntimeError("Google OAuth client id/secret are not configured.")
    return {
        "web": {
            "client_id": Config.GOOGLE_OAUTH_CLIENT_ID,
            "client_secret": Config.GOOGLE_OAUTH_CLIENT_SECRET,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
        }
    }


def _integration_redirect_uri(integration_key: str) -> str:
    if integration_key == "gsc":
        return Config.FULCRUM_GSC_OAUTH_CALLBACK_URL
    if integration_key == "ga4":
        return Config.FULCRUM_GA4_OAUTH_CALLBACK_URL
    raise ValueError(f"Unsupported integration key: {integration_key}")


def _oauth_state_prefix(integration_key: str) -> str:
    return f"route-authority-{integration_key}"


def _oauth_state_serializer() -> URLSafeSerializer:
    secret = Config.FULCRUM_INTEGRATION_SECRET or Config.SECRET_KEY
    return URLSafeSerializer(secret, salt="route-authority-google-oauth")


def _encode_oauth_state(integration_key: str, store_hash: str) -> str:
    return _oauth_state_serializer().dumps(
        {
            "integration_key": integration_key,
            "store_hash": normalize_store_hash(store_hash),
            "nonce": secrets.token_urlsafe(12),
        }
    )


def decode_google_oauth_state(state: str | None) -> dict[str, str]:
    if not state:
        return {}
    try:
        payload = _oauth_state_serializer().loads(state)
    except BadSignature:
        return {}
    if not isinstance(payload, dict):
        return {}
    integration_key = (payload.get("integration_key") or "").strip().lower()
    store_hash = normalize_store_hash(payload.get("store_hash") or "")
    if integration_key not in GOOGLE_INTEGRATION_KEYS or not store_hash:
        return {}
    return {"integration_key": integration_key, "store_hash": store_hash}


def _build_google_flow(integration_key: str, *, state: str | None = None) -> Flow:
    flow = Flow.from_client_config(
        _google_client_config(),
        scopes=GOOGLE_SCOPES[integration_key],
        state=state,
    )
    flow.redirect_uri = _integration_redirect_uri(integration_key)
    return flow


def _build_google_service(service_name: str, version: str, credentials: Credentials):
    try:
        import httplib2
        from google_auth_httplib2 import AuthorizedHttp

        http = AuthorizedHttp(credentials, http=httplib2.Http(timeout=GOOGLE_API_TIMEOUT_SECONDS))
        return build(service_name, version, http=http, cache_discovery=False)
    except Exception:
        return build(service_name, version, credentials=credentials, cache_discovery=False)


def _integration_fernet() -> Fernet:
    secret = (Config.FULCRUM_INTEGRATION_SECRET or Config.SECRET_KEY or "").encode("utf-8")
    if not secret:
        raise RuntimeError("Route Authority integration secret is not configured.")
    derived_key = urlsafe_b64encode(hashlib.sha256(secret).digest())
    return Fernet(derived_key)


def _encrypt_auth_payload(auth_payload: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(auth_payload or {})
    if not payload:
        return {}
    token = _integration_fernet().encrypt(
        json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    ).decode("utf-8")
    return {
        "encrypted": True,
        "key_version": "v1",
        "ciphertext": token,
    }


def _decrypt_auth_payload(auth_payload: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(auth_payload or {})
    if not payload:
        return {}
    if payload.get("encrypted") and payload.get("ciphertext"):
        plaintext = _integration_fernet().decrypt(str(payload.get("ciphertext") or "").encode("utf-8"))
        return dict(json.loads(plaintext.decode("utf-8")))
    return payload


def _maybe_reencrypt_legacy_auth_payload(store_hash: str, integration_key: str, auth_payload: dict[str, Any] | None) -> None:
    payload = dict(auth_payload or {})
    if not payload or payload.get("encrypted"):
        return
    token_keys = {"access_token", "refresh_token", "token", "client_secret"}
    if not any(key in payload for key in token_keys):
        return
    sql = """
        UPDATE app_runtime.store_integrations
        SET auth_payload = %s::jsonb,
            updated_at = NOW()
        WHERE store_hash = %s
          AND integration_key = %s;
    """
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    json.dumps(_encrypt_auth_payload(payload)),
                    normalize_store_hash(store_hash),
                    integration_key,
                ),
            )
        conn.commit()


def _credential_payload_to_credentials(auth_payload: dict[str, Any] | None) -> Credentials:
    payload = _decrypt_auth_payload(auth_payload)
    return Credentials.from_authorized_user_info(payload, scopes=list(payload.get("scopes") or []))


def _upsert_store_integration(
    store_hash: str,
    integration_key: str,
    *,
    connection_status: str | None = None,
    configuration_status: str | None = None,
    selected_resource_id: str | None = None,
    selected_resource_label: str | None = None,
    auth_payload: dict[str, Any] | None = None,
    metadata_updates: dict[str, Any] | None = None,
    error_message: str | None = None,
    mark_success: bool = False,
    mark_error: bool = False,
) -> dict[str, Any]:
    normalized_hash = normalize_store_hash(store_hash)
    metadata_updates = dict(metadata_updates or {})
    encrypted_auth_payload = _encrypt_auth_payload(auth_payload)
    sql = """
        INSERT INTO app_runtime.store_integrations (
            store_hash,
            integration_key,
            connection_status,
            configuration_status,
            selected_resource_id,
            selected_resource_label,
            auth_payload,
            metadata,
            last_success_at,
            last_error_at,
            last_error_message,
            created_at,
            updated_at
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb,
            CASE WHEN %s THEN NOW() ELSE NULL END,
            CASE WHEN %s THEN NOW() ELSE NULL END,
            %s,
            NOW(),
            NOW()
        )
        ON CONFLICT (store_hash, integration_key) DO UPDATE SET
            connection_status = COALESCE(EXCLUDED.connection_status, app_runtime.store_integrations.connection_status),
            configuration_status = COALESCE(EXCLUDED.configuration_status, app_runtime.store_integrations.configuration_status),
            selected_resource_id = COALESCE(EXCLUDED.selected_resource_id, app_runtime.store_integrations.selected_resource_id),
            selected_resource_label = COALESCE(EXCLUDED.selected_resource_label, app_runtime.store_integrations.selected_resource_label),
            auth_payload = CASE
                WHEN EXCLUDED.auth_payload = '{}'::jsonb THEN app_runtime.store_integrations.auth_payload
                ELSE EXCLUDED.auth_payload
            END,
            metadata = app_runtime.store_integrations.metadata || EXCLUDED.metadata,
            last_success_at = CASE
                WHEN %s THEN NOW()
                ELSE app_runtime.store_integrations.last_success_at
            END,
            last_error_at = CASE
                WHEN %s THEN NOW()
                ELSE app_runtime.store_integrations.last_error_at
            END,
            last_error_message = CASE
                WHEN %s IS NOT NULL THEN %s
                WHEN %s THEN NULL
                ELSE app_runtime.store_integrations.last_error_message
            END,
            updated_at = NOW()
        RETURNING
            store_hash,
            integration_key,
            connection_status,
            configuration_status,
            selected_resource_id,
            selected_resource_label,
            auth_payload,
            metadata,
            last_success_at,
            last_error_at,
            last_error_message,
            updated_at;
    """
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                sql,
                (
                    normalized_hash,
                    integration_key,
                    connection_status or "not_connected",
                    configuration_status or "not_configured",
                    selected_resource_id,
                    selected_resource_label,
                    json.dumps(encrypted_auth_payload),
                    json.dumps(metadata_updates),
                    bool(mark_success),
                    bool(mark_error),
                    error_message,
                    bool(mark_success),
                    bool(mark_error),
                    error_message,
                    error_message,
                    bool(mark_success),
                ),
            )
            row = dict(cur.fetchone() or {})
        conn.commit()
    return row


def get_store_integration(store_hash: str, integration_key: str) -> dict[str, Any]:
    sql = """
        SELECT
            store_hash,
            integration_key,
            connection_status,
            configuration_status,
            selected_resource_id,
            selected_resource_label,
            auth_payload,
            metadata,
            last_success_at,
            last_error_at,
            last_error_message,
            updated_at
        FROM app_runtime.store_integrations
        WHERE store_hash = %s
          AND integration_key = %s;
    """
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (normalize_store_hash(store_hash), integration_key))
            row = dict(cur.fetchone() or {})
    if row:
        _maybe_reencrypt_legacy_auth_payload(store_hash, integration_key, row.get("auth_payload"))
        row["auth_payload"] = _decrypt_auth_payload(row.get("auth_payload"))
    return row


def list_store_integrations(store_hash: str) -> list[dict[str, Any]]:
    sql = """
        SELECT
            store_hash,
            integration_key,
            connection_status,
            configuration_status,
            selected_resource_id,
            selected_resource_label,
            auth_payload,
            metadata,
            last_success_at,
            last_error_at,
            last_error_message,
            updated_at
        FROM app_runtime.store_integrations
        WHERE store_hash = %s
        ORDER BY integration_key;
    """
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (normalize_store_hash(store_hash),))
            rows = [dict(row) for row in cur.fetchall()]
    for row in rows:
        _maybe_reencrypt_legacy_auth_payload(store_hash, row.get("integration_key") or "", row.get("auth_payload"))
        row["auth_payload"] = _decrypt_auth_payload(row.get("auth_payload"))
    return rows


def get_store_installation(store_hash: str) -> dict[str, Any]:
    sql = """
        SELECT
            store_hash,
            context,
            access_token,
            owner_email,
            status,
            install_source,
            metadata,
            installed_at,
            updated_at,
            uninstalled_at
        FROM app_runtime.store_installations
        WHERE store_hash = %s
        ORDER BY updated_at DESC
        LIMIT 1;
    """
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (normalize_store_hash(store_hash),))
            row = dict(cur.fetchone() or {})
    return row


def sync_bigcommerce_integration(store_hash: str) -> dict[str, Any]:
    installation = get_store_installation(store_hash)
    if installation and (installation.get("status") or "").strip().lower() == "active":
        return _upsert_store_integration(
            store_hash,
            "bigcommerce",
            connection_status="connected",
            configuration_status="ready",
            selected_resource_id=normalize_store_hash(store_hash),
            selected_resource_label=normalize_store_hash(store_hash),
            metadata_updates={
                "install_source": installation.get("install_source") or "oauth",
                "owner_email": installation.get("owner_email") or "",
            },
            mark_success=True,
        )
    return _upsert_store_integration(
        store_hash,
        "bigcommerce",
        connection_status="not_connected",
        configuration_status="not_configured",
        selected_resource_id=None,
        selected_resource_label=None,
        metadata_updates={},
    )


def get_store_publish_settings(store_hash: str) -> dict[str, Any]:
    sql = """
        INSERT INTO app_runtime.store_publish_settings (
            store_hash,
            publishing_enabled,
            category_publishing_enabled,
            metadata,
            updated_at
        )
        VALUES (%s, FALSE, FALSE, %s::jsonb, NOW())
        ON CONFLICT (store_hash) DO UPDATE SET
            updated_at = app_runtime.store_publish_settings.updated_at
        RETURNING
            store_hash,
            publishing_enabled,
            category_publishing_enabled,
            metadata,
            updated_at;
    """
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                sql,
                (
                    normalize_store_hash(store_hash),
                    json.dumps({"merchant_publish_preference_set": False}),
                ),
            )
            row = dict(cur.fetchone() or {})
            metadata = dict(row.get("metadata") or {})
            if "merchant_publish_preference_set" not in metadata:
                metadata["merchant_publish_preference_set"] = False
                cur.execute(
                    """
                    UPDATE app_runtime.store_publish_settings
                    SET publishing_enabled = FALSE,
                        metadata = %s::jsonb,
                        updated_at = NOW()
                    WHERE store_hash = %s
                    RETURNING
                        store_hash,
                        publishing_enabled,
                        category_publishing_enabled,
                        metadata,
                        updated_at;
                    """,
                    (
                        json.dumps(metadata),
                        normalize_store_hash(store_hash),
                    ),
                )
                row = dict(cur.fetchone() or row)
        conn.commit()
    return row


def purge_store_data_on_uninstall(store_hash: str) -> dict[str, int]:
    normalized_hash = normalize_store_hash(store_hash)
    statements = [
        ("DELETE FROM app_runtime.store_gsc_daily WHERE store_hash = %s;", (normalized_hash,)),
        ("DELETE FROM app_runtime.store_ga4_pages_daily WHERE store_hash = %s;", (normalized_hash,)),
        ("DELETE FROM app_runtime.integration_sync_runs WHERE store_hash = %s;", (normalized_hash,)),
        ("DELETE FROM app_runtime.admin_metric_cache WHERE store_hash = %s;", (normalized_hash,)),
        ("DELETE FROM app_runtime.store_theme_verifications WHERE store_hash = %s;", (normalized_hash,)),
        (
            """
            UPDATE app_runtime.store_publish_settings
            SET publishing_enabled = FALSE,
                category_publishing_enabled = FALSE,
                metadata = metadata || %s::jsonb,
                updated_at = NOW()
            WHERE store_hash = %s;
            """,
            (json.dumps({"uninstalled_at": _iso_now()}), normalized_hash),
        ),
        (
            """
            UPDATE app_runtime.store_integrations
            SET connection_status = 'not_connected',
                configuration_status = 'not_configured',
                selected_resource_id = NULL,
                selected_resource_label = NULL,
                auth_payload = '{}'::jsonb,
                metadata = metadata || %s::jsonb,
                last_error_message = NULL,
                updated_at = NOW()
            WHERE store_hash = %s
              AND integration_key IN ('gsc', 'ga4');
            """,
            (json.dumps({"uninstalled_at": _iso_now(), "retention_action": "credentials_cleared"}), normalized_hash),
        ),
    ]
    summary = {
        "store_gsc_daily_deleted": 0,
        "store_ga4_pages_daily_deleted": 0,
        "integration_sync_runs_deleted": 0,
        "admin_metric_cache_deleted": 0,
        "store_theme_verifications_deleted": 0,
        "store_publish_settings_updated": 0,
        "store_integrations_updated": 0,
    }
    keys = list(summary.keys())
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            for index, (sql, params) in enumerate(statements):
                cur.execute(sql, params)
                summary[keys[index]] = max(int(cur.rowcount or 0), 0)
        conn.commit()
    return summary


def _sync_category_publish_override(store_hash: str, enabled: bool) -> None:
    sql = """
        INSERT INTO app_runtime.store_readiness (
            store_hash,
            metadata,
            updated_at
        )
        VALUES (%s, %s::jsonb, NOW())
        ON CONFLICT (store_hash) DO UPDATE SET
            metadata = app_runtime.store_readiness.metadata || EXCLUDED.metadata,
            updated_at = NOW();
    """
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    normalize_store_hash(store_hash),
                    json.dumps({"category_publishing_enabled_override": bool(enabled)}),
                ),
            )
        conn.commit()


def upsert_store_publish_settings(
    store_hash: str,
    *,
    publishing_enabled: bool | None = None,
    category_publishing_enabled: bool | None = None,
    metadata_updates: dict[str, Any] | None = None,
) -> dict[str, Any]:
    current = get_store_publish_settings(store_hash)
    next_publishing_enabled = current.get("publishing_enabled") if publishing_enabled is None else bool(publishing_enabled)
    next_category_enabled = current.get("category_publishing_enabled") if category_publishing_enabled is None else bool(category_publishing_enabled)
    metadata = dict(current.get("metadata") or {})
    metadata.update(metadata_updates or {})
    metadata["merchant_publish_preference_set"] = True
    sql = """
        INSERT INTO app_runtime.store_publish_settings (
            store_hash,
            publishing_enabled,
            category_publishing_enabled,
            metadata,
            updated_at
        )
        VALUES (%s, %s, %s, %s::jsonb, NOW())
        ON CONFLICT (store_hash) DO UPDATE SET
            publishing_enabled = EXCLUDED.publishing_enabled,
            category_publishing_enabled = EXCLUDED.category_publishing_enabled,
            metadata = app_runtime.store_publish_settings.metadata || EXCLUDED.metadata,
            updated_at = NOW()
        RETURNING
            store_hash,
            publishing_enabled,
            category_publishing_enabled,
            metadata,
            updated_at;
    """
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                sql,
                (
                    normalize_store_hash(store_hash),
                    bool(next_publishing_enabled),
                    bool(next_category_enabled),
                    json.dumps(metadata),
                ),
            )
            row = dict(cur.fetchone() or {})
        conn.commit()
    _sync_category_publish_override(store_hash, bool(next_category_enabled))
    return row


def _storefront_hosts(store_hash: str) -> set[str]:
    hosts = {_normalized_host(url) for url in list_storefront_base_urls(store_hash)}
    return {host for host in hosts if host}


def _integration_status_label(row: dict[str, Any]) -> str:
    connection_status = (row.get("connection_status") or "not_connected").strip().lower()
    configuration_status = (row.get("configuration_status") or "not_configured").strip().lower()
    if connection_status in {"error", "needs_reauth"} or configuration_status == "error":
        return "Error"
    if configuration_status == "sync_error":
        return "Sync error"
    if configuration_status == "sync_warning":
        return "Needs attention"
    if configuration_status in {"syncing", "sync_queued"}:
        return "Syncing"
    if configuration_status == "ready":
        if row.get("data_ready") is False:
            return "Needs data"
        return "Ready"
    if connection_status == "connected" and configuration_status in {"not_configured", "needs_configuration"}:
        return "Needs configuration"
    if connection_status == "connected":
        return "Connected"
    return "Not connected"


def _ga4_property_id(selected_resource_id: str | None) -> str:
    raw = (selected_resource_id or "").strip()
    if raw.startswith("properties/"):
        return raw.split("/", 1)[-1].strip()
    return raw


def _ga4_property_name(selected_resource_id: str | None) -> str:
    property_id = _ga4_property_id(selected_resource_id)
    return f"properties/{property_id}" if property_id else ""


def _public_store_integration(store_hash: str, integration_key: str) -> dict[str, Any]:
    sql = """
        SELECT
            store_hash,
            integration_key,
            connection_status,
            configuration_status,
            selected_resource_id,
            selected_resource_label,
            metadata,
            last_success_at,
            last_error_at,
            last_error_message,
            updated_at
        FROM app_runtime.store_integrations
        WHERE store_hash = %s
          AND integration_key = %s;
    """
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (normalize_store_hash(store_hash), integration_key))
            row = dict(cur.fetchone() or {})
    return row


def get_latest_integration_sync_run(store_hash: str, integration_key: str) -> dict[str, Any]:
    sql = """
        SELECT
            sync_run_id,
            store_hash,
            integration_key,
            selected_resource_id,
            status,
            triggered_by,
            row_count,
            start_date,
            end_date,
            error_message,
            metadata,
            queued_at,
            started_at,
            finished_at,
            updated_at
        FROM app_runtime.integration_sync_runs
        WHERE store_hash = %s
          AND integration_key = %s
        ORDER BY queued_at DESC, sync_run_id DESC
        LIMIT 1;
    """
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (normalize_store_hash(store_hash), integration_key))
            row = dict(cur.fetchone() or {})
    return row


def get_store_integration_data_summary(
    store_hash: str,
    integration_key: str,
    selected_resource_id: str | None = None,
) -> dict[str, Any]:
    normalized_hash = normalize_store_hash(store_hash)
    if integration_key == "gsc":
        selected = (selected_resource_id or "").strip()
        if selected:
            sql = """
                SELECT
                    COUNT(*)::bigint AS row_count,
                    MIN(date)::text AS start_date,
                    MAX(date)::text AS end_date,
                    MAX(updated_at) AS updated_at
                FROM app_runtime.store_gsc_daily
                WHERE store_hash = %s
                  AND property_site_url = %s;
            """
            params = (normalized_hash, selected)
        else:
            sql = """
                SELECT
                    COUNT(*)::bigint AS row_count,
                    MIN(date)::text AS start_date,
                    MAX(date)::text AS end_date,
                    MAX(updated_at) AS updated_at
                FROM app_runtime.store_gsc_daily
                WHERE store_hash = %s;
            """
            params = (normalized_hash,)
    elif integration_key == "ga4":
        selected = _ga4_property_id(selected_resource_id)
        if selected:
            sql = """
                SELECT
                    COUNT(*)::bigint AS row_count,
                    MIN(date)::text AS start_date,
                    MAX(date)::text AS end_date,
                    MAX(updated_at) AS updated_at
                FROM app_runtime.store_ga4_pages_daily
                WHERE store_hash = %s
                  AND property_id = %s;
            """
            params = (normalized_hash, selected)
        else:
            sql = """
                SELECT
                    COUNT(*)::bigint AS row_count,
                    MIN(date)::text AS start_date,
                    MAX(date)::text AS end_date,
                    MAX(updated_at) AS updated_at
                FROM app_runtime.store_ga4_pages_daily
                WHERE store_hash = %s;
            """
            params = (normalized_hash,)
    else:
        raise ValueError(f"Unsupported integration key: {integration_key}")

    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            row = dict(cur.fetchone() or {})
    row_count = int(row.get("row_count") or 0)
    latest_run = get_latest_integration_sync_run(normalized_hash, integration_key)
    return {
        "row_count": row_count,
        "has_data": row_count > 0,
        "start_date": row.get("start_date") or "",
        "end_date": row.get("end_date") or "",
        "updated_at": row.get("updated_at"),
        "latest_sync_run": latest_run,
        "latest_sync_status": (latest_run.get("status") or "").strip().lower(),
        "latest_sync_error": latest_run.get("error_message") or "",
    }


def _integration_data_ready(row: dict[str, Any], data_summary: dict[str, Any]) -> bool:
    return bool(
        (row.get("connection_status") or "").strip().lower() == "connected"
        and (row.get("configuration_status") or "").strip().lower() == "ready"
        and data_summary.get("has_data")
    )


def _reconcile_integration_data_readiness(
    store_hash: str,
    integration_key: str,
    row: dict[str, Any],
    data_summary: dict[str, Any],
) -> dict[str, Any]:
    connection_status = (row.get("connection_status") or "").strip().lower()
    configuration_status = (row.get("configuration_status") or "").strip().lower()
    selected_resource_id = (row.get("selected_resource_id") or "").strip()
    if (
        connection_status == "connected"
        and configuration_status == "ready"
        and selected_resource_id
        and not data_summary.get("has_data")
    ):
        message = "Connected and selected, but no imported data is available yet."
        updated = _upsert_store_integration(
            store_hash,
            integration_key,
            connection_status="connected",
            configuration_status="sync_warning",
            selected_resource_id=selected_resource_id,
            selected_resource_label=row.get("selected_resource_label") or selected_resource_id,
            metadata_updates={
                "sync_status": "warning",
                "last_sync_status": "warning",
                "last_sync_row_count": int(data_summary.get("row_count") or 0),
                "last_sync_error": message,
                "data_readiness_reconciled_at": _iso_now(),
            },
            mark_error=True,
            error_message=message,
        )
        return {**row, **updated}
    return row


def _integration_detail(row: dict[str, Any], data_summary: dict[str, Any], default_detail: str) -> str:
    selected_label = (row.get("selected_resource_label") or row.get("selected_resource_id") or "").strip()
    row_count = int(data_summary.get("row_count") or 0)
    start_date = data_summary.get("start_date") or ""
    end_date = data_summary.get("end_date") or ""
    latest_error = data_summary.get("latest_sync_error") or row.get("last_error_message") or ""
    configuration_status = (row.get("configuration_status") or "").strip().lower()
    if row_count > 0:
        date_range = f" ({start_date} to {end_date})" if start_date and end_date else ""
        return f"{selected_label or default_detail} - {row_count:,} rows imported{date_range}."
    if selected_label and configuration_status in {"syncing", "sync_queued"}:
        return f"{selected_label} is selected. Data import is queued or running."
    if selected_label and latest_error:
        return f"{selected_label} is selected, but data import needs attention: {latest_error}"
    if selected_label:
        return f"{selected_label} is selected, but no imported data is available yet."
    return default_detail


def _empty_integration_data_summary(error_message: str = "") -> dict[str, Any]:
    return {
        "row_count": 0,
        "has_data": False,
        "start_date": "",
        "end_date": "",
        "updated_at": None,
        "latest_sync_run": {},
        "latest_sync_status": "error" if error_message else "",
        "latest_sync_error": error_message,
    }


def _safe_store_integration_data_summary(
    store_hash: str,
    integration_key: str,
    selected_resource_id: str | None = None,
) -> dict[str, Any]:
    try:
        return get_store_integration_data_summary(store_hash, integration_key, selected_resource_id)
    except Exception as exc:  # noqa: BLE001
        return _empty_integration_data_summary(str(exc) or type(exc).__name__)


def enqueue_integration_sync(
    store_hash: str,
    integration_key: str,
    *,
    triggered_by: str = "merchant",
    selected_resource_id: str | None = None,
    metadata_updates: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if integration_key not in GOOGLE_INTEGRATION_KEYS:
        raise ValueError(f"Unsupported integration key: {integration_key}")
    normalized_hash = normalize_store_hash(store_hash)
    integration = get_store_integration(normalized_hash, integration_key)
    selected = (selected_resource_id or integration.get("selected_resource_id") or "").strip()
    if not selected:
        return {
            "status": "error",
            "reason": f"{INTEGRATION_DISPLAY[integration_key]} property selection is missing.",
            "row_count": 0,
        }

    metadata = {
        "queued_by": triggered_by,
        "queued_at": _iso_now(),
        **dict(metadata_updates or {}),
    }
    insert_sql = """
        INSERT INTO app_runtime.integration_sync_runs (
            store_hash,
            integration_key,
            selected_resource_id,
            status,
            triggered_by,
            metadata,
            queued_at,
            updated_at
        )
        VALUES (%s, %s, %s, 'queued', %s, %s::jsonb, NOW(), NOW())
        RETURNING
            sync_run_id,
            store_hash,
            integration_key,
            selected_resource_id,
            status,
            triggered_by,
            row_count,
            start_date,
            end_date,
            error_message,
            metadata,
            queued_at,
            started_at,
            finished_at,
            updated_at;
    """
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                insert_sql,
                (
                    normalized_hash,
                    integration_key,
                    selected,
                    triggered_by,
                    json.dumps(metadata),
                ),
            )
            run = dict(cur.fetchone() or {})
        conn.commit()

    _upsert_store_integration(
        normalized_hash,
        integration_key,
        connection_status="connected",
        configuration_status="syncing",
        selected_resource_id=selected,
        selected_resource_label=integration.get("selected_resource_label") or selected,
        metadata_updates={
            "sync_status": "queued",
            "last_sync_status": "queued",
            "last_sync_run_id": run.get("sync_run_id"),
            "last_sync_queued_at": _iso_now(),
        },
    )
    return {"status": "queued", **run}


def claim_next_integration_sync_run(
    *,
    store_hash: str | None = None,
    integration_key: str | None = None,
) -> dict[str, Any]:
    conditions = ["status = 'queued'"]
    params: list[Any] = []
    if store_hash:
        conditions.append("store_hash = %s")
        params.append(normalize_store_hash(store_hash))
    if integration_key:
        if integration_key not in GOOGLE_INTEGRATION_KEYS:
            raise ValueError(f"Unsupported integration key: {integration_key}")
        conditions.append("integration_key = %s")
        params.append(integration_key)
    where_clause = " AND ".join(conditions)
    sql = f"""
        WITH candidate AS (
            SELECT sync_run_id
            FROM app_runtime.integration_sync_runs
            WHERE {where_clause}
            ORDER BY queued_at ASC, sync_run_id ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        ),
        locked AS (
            SELECT run.sync_run_id
            FROM app_runtime.integration_sync_runs run
            JOIN candidate ON candidate.sync_run_id = run.sync_run_id
            WHERE pg_try_advisory_xact_lock(
                hashtext('fulcrum-integration-sync:' || run.store_hash || ':' || run.integration_key)
            )
        )
        UPDATE app_runtime.integration_sync_runs run
        SET status = 'running',
            started_at = COALESCE(run.started_at, NOW()),
            updated_at = NOW()
        FROM locked
        WHERE run.sync_run_id = locked.sync_run_id
        RETURNING
            run.sync_run_id,
            run.store_hash,
            run.integration_key,
            run.selected_resource_id,
            run.status,
            run.triggered_by,
            run.row_count,
            run.start_date,
            run.end_date,
            run.error_message,
            run.metadata,
            run.queued_at,
            run.started_at,
            run.finished_at,
            run.updated_at;
    """
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            row = dict(cur.fetchone() or {})
        conn.commit()
    if row:
        _upsert_store_integration(
            row["store_hash"],
            row["integration_key"],
            connection_status="connected",
            configuration_status="syncing",
            metadata_updates={
                "sync_status": "running",
                "last_sync_status": "running",
                "last_sync_run_id": row.get("sync_run_id"),
                "last_sync_started_at": _iso_now(),
            },
        )
    return row


def expire_stale_integration_sync_runs(*, max_age_minutes: int = 30) -> list[dict[str, Any]]:
    sql = """
        UPDATE app_runtime.integration_sync_runs
        SET status = 'failed',
            error_message = COALESCE(error_message, 'Sync worker stopped before finishing. Queue a retry.'),
            metadata = metadata || %s::jsonb,
            finished_at = NOW(),
            updated_at = NOW()
        WHERE status = 'running'
          AND started_at < NOW() - (%s::text || ' minutes')::interval
        RETURNING
            sync_run_id,
            store_hash,
            integration_key,
            selected_resource_id,
            status,
            triggered_by,
            row_count,
            start_date,
            end_date,
            error_message,
            metadata,
            queued_at,
            started_at,
            finished_at,
            updated_at;
    """
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                sql,
                (
                    json.dumps({"expired_at": _iso_now(), "expired_by": "integration_sync_worker"}),
                    str(max(int(max_age_minutes or 0), 0)),
                ),
            )
            rows = [dict(row) for row in cur.fetchall()]
        conn.commit()
    for row in rows:
        _upsert_store_integration(
            row["store_hash"],
            row["integration_key"],
            connection_status="connected",
            configuration_status="sync_error",
            metadata_updates={
                "sync_status": "failed",
                "last_sync_status": "failed",
                "last_sync_run_id": row.get("sync_run_id"),
                "last_sync_error": row.get("error_message") or "Sync worker stopped before finishing.",
                "last_sync_finished_at": _iso_now(),
            },
            mark_error=True,
            error_message=row.get("error_message") or "Sync worker stopped before finishing.",
        )
    return rows


def _finish_integration_sync_run(
    sync_run_id: int,
    *,
    status: str,
    row_count: int = 0,
    start_date: str | None = None,
    end_date: str | None = None,
    error_message: str | None = None,
    metadata_updates: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if status not in SYNC_RUN_FINAL_STATUSES:
        raise ValueError(f"Unsupported sync run status: {status}")
    sql = """
        UPDATE app_runtime.integration_sync_runs
        SET status = %s,
            row_count = %s,
            start_date = %s,
            end_date = %s,
            error_message = %s,
            metadata = metadata || %s::jsonb,
            finished_at = NOW(),
            updated_at = NOW()
        WHERE sync_run_id = %s
        RETURNING
            sync_run_id,
            store_hash,
            integration_key,
            selected_resource_id,
            status,
            triggered_by,
            row_count,
            start_date,
            end_date,
            error_message,
            metadata,
            queued_at,
            started_at,
            finished_at,
            updated_at;
    """
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                sql,
                (
                    status,
                    int(row_count or 0),
                    start_date,
                    end_date,
                    error_message,
                    json.dumps(dict(metadata_updates or {})),
                    int(sync_run_id),
                ),
            )
            row = dict(cur.fetchone() or {})
        conn.commit()
    return row


def run_integration_sync_run(sync_run_id: int) -> dict[str, Any]:
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                UPDATE app_runtime.integration_sync_runs
                SET status = 'running',
                    started_at = COALESCE(started_at, NOW()),
                    updated_at = NOW()
                WHERE sync_run_id = %s
                  AND status IN ('queued', 'running')
                RETURNING
                    sync_run_id,
                    store_hash,
                    integration_key,
                    selected_resource_id,
                    status;
                """,
                (int(sync_run_id),),
            )
            run = dict(cur.fetchone() or {})
        conn.commit()
    if not run:
        return {"status": "skipped", "reason": "Sync run is not queued or running.", "sync_run_id": int(sync_run_id)}

    store_hash = run["store_hash"]
    integration_key = run["integration_key"]
    _upsert_store_integration(
        store_hash,
        integration_key,
        connection_status="connected",
        configuration_status="syncing",
        selected_resource_id=run.get("selected_resource_id"),
        metadata_updates={
            "sync_status": "running",
            "last_sync_status": "running",
            "last_sync_run_id": int(sync_run_id),
            "last_sync_started_at": _iso_now(),
        },
    )
    try:
        if integration_key == "gsc":
            result = refresh_store_gsc_data(store_hash)
        elif integration_key == "ga4":
            result = refresh_store_ga4_data(store_hash)
        else:
            raise ValueError(f"Unsupported integration key: {integration_key}")
    except Exception as exc:  # noqa: BLE001
        error_message = str(exc) or type(exc).__name__
        row = _finish_integration_sync_run(
            int(sync_run_id),
            status="failed",
            row_count=0,
            error_message=error_message,
            metadata_updates={"exception_type": type(exc).__name__},
        )
        _upsert_store_integration(
            store_hash,
            integration_key,
            connection_status="connected",
            configuration_status="sync_error",
            metadata_updates={
                "sync_status": "failed",
                "last_sync_status": "failed",
                "last_sync_run_id": int(sync_run_id),
                "last_sync_error": error_message,
                "last_sync_finished_at": _iso_now(),
            },
            mark_error=True,
            error_message=error_message,
        )
        return {"status": "failed", "sync_run": row, "reason": error_message}

    result_status = (result.get("status") or "").strip().lower()
    row_count = int(result.get("row_count") or 0)
    final_status = "succeeded" if result_status == "ok" and row_count > 0 else "warning"
    error_message = result.get("reason") if final_status == "warning" else None
    row = _finish_integration_sync_run(
        int(sync_run_id),
        status=final_status,
        row_count=row_count,
        start_date=result.get("start_date"),
        end_date=result.get("end_date"),
        error_message=error_message,
        metadata_updates={"sync_result": result},
    )
    if final_status == "warning":
        warning_message = error_message or "Data sync finished without imported rows."
        _upsert_store_integration(
            store_hash,
            integration_key,
            connection_status="connected",
            configuration_status="sync_warning",
            metadata_updates={
                "sync_status": "warning",
                "last_sync_status": "warning",
                "last_sync_run_id": int(sync_run_id),
                "last_sync_error": warning_message,
                "last_sync_finished_at": _iso_now(),
            },
            mark_error=True,
            error_message=warning_message,
        )
    return {"status": final_status, "sync_run": row, "sync_result": result}


def process_queued_integration_syncs(
    *,
    limit: int = 10,
    store_hash: str | None = None,
    integration_key: str | None = None,
    expire_running_after_minutes: int = 30,
) -> dict[str, Any]:
    expired = expire_stale_integration_sync_runs(max_age_minutes=expire_running_after_minutes)
    processed: list[dict[str, Any]] = []
    for _ in range(max(int(limit or 0), 0)):
        run = claim_next_integration_sync_run(store_hash=store_hash, integration_key=integration_key)
        if not run:
            break
        processed.append(run_integration_sync_run(int(run["sync_run_id"])))
    return {"status": "ok", "expired_count": len(expired), "expired": expired, "processed_count": len(processed), "processed": processed}


def _record_theme_verification(
    store_hash: str,
    *,
    verification_status: str,
    failure_classification: str | None,
    summary: str,
    details: dict[str, Any],
) -> dict[str, Any]:
    sql = """
        INSERT INTO app_runtime.store_theme_verifications (
            store_hash,
            verification_status,
            failure_classification,
            summary,
            details,
            created_at
        )
        VALUES (%s, %s, %s, %s, %s::jsonb, NOW())
        RETURNING
            verification_id,
            store_hash,
            verification_status,
            failure_classification,
            summary,
            details,
            created_at;
    """
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                sql,
                (
                    normalize_store_hash(store_hash),
                    verification_status,
                    failure_classification,
                    summary,
                    json.dumps(details),
                ),
            )
            row = dict(cur.fetchone() or {})
        conn.commit()
    return row


def latest_theme_verification(store_hash: str) -> dict[str, Any]:
    sql = """
        SELECT
            verification_id,
            store_hash,
            verification_status,
            failure_classification,
            summary,
            details,
            created_at
        FROM app_runtime.store_theme_verifications
        WHERE store_hash = %s
        ORDER BY created_at DESC
        LIMIT 1;
    """
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (normalize_store_hash(store_hash),))
            row = dict(cur.fetchone() or {})
    return row


def evaluate_theme_verification(
    store_hash: str,
    *,
    persist: bool = False,
) -> dict[str, Any]:
    publish_settings = get_store_publish_settings(store_hash)
    product_template = Path(Config.FULCRUM_THEME_PRODUCT_TEMPLATE)
    category_template = Path(Config.FULCRUM_THEME_CATEGORY_TEMPLATE)
    product_exists = product_template.exists()
    category_exists = category_template.exists()
    product_ready = product_exists and theme_hook_present(Config.FULCRUM_THEME_PRODUCT_TEMPLATE)
    category_required = bool(publish_settings.get("category_publishing_enabled"))
    category_ready = category_exists and category_theme_hook_present(Config.FULCRUM_THEME_CATEGORY_TEMPLATE)
    details = {
        "product_template_path": str(product_template),
        "category_template_path": str(category_template),
        "product_template_exists": product_exists,
        "category_template_exists": category_exists,
        "product_hook_ready": product_ready,
        "category_hook_ready": category_ready,
        "category_required": category_required,
        "next_action": "",
    }

    if not product_exists:
        result = {
            "verification_status": "failed",
            "failure_classification": "support_required",
            "summary": "Route Authority could not locate the configured product template file.",
        }
        details["missing"] = "Configured product template path is not available on disk."
        details["next_action"] = "Confirm the production theme template path or contact support."
    elif not product_ready:
        result = {
            "verification_status": "failed",
            "failure_classification": "manual",
            "summary": "The product template does not render the Route Authority product metafield hook.",
        }
        details["missing"] = "internal_links_html render hook is missing from the product template."
        details["next_action"] = "Add the h4h product metafield render block to the product template, then run the check again."
    elif category_required and not category_exists:
        result = {
            "verification_status": "failed",
            "failure_classification": "support_required",
            "summary": "Category publishing is enabled, but Route Authority could not locate the configured category template file.",
        }
        details["missing"] = "Configured category template path is not available on disk."
        details["next_action"] = "Confirm the production category template path or contact support."
    elif category_required and not category_ready:
        result = {
            "verification_status": "failed",
            "failure_classification": "automatic",
            "summary": "Category publishing is enabled, but the category template hook is missing.",
        }
        details["missing"] = "internal_category_links_html and internal_product_links_html render hooks are missing from the category template."
        details["next_action"] = "Use the automatic fix to switch publishing to product-only mode, or update the category template manually."
    else:
        result = {
            "verification_status": "ready",
            "failure_classification": None,
            "summary": "Theme verification passed.",
        }
        details["next_action"] = "Theme support is ready."

    payload = {**result, "details": details}
    if persist:
        stored = _record_theme_verification(
            store_hash,
            verification_status=result["verification_status"],
            failure_classification=result["failure_classification"],
            summary=result["summary"],
            details=details,
        )
        payload["stored_verification"] = stored
    return payload


def apply_theme_automatic_fix(store_hash: str) -> dict[str, Any]:
    evaluation = evaluate_theme_verification(store_hash, persist=False)
    if evaluation.get("failure_classification") != "automatic":
        return {
            "status": "skipped",
            "reason": evaluation.get("summary") or "No automatic theme fix is available.",
            "verification": evaluation,
        }
    upsert_store_publish_settings(
        store_hash,
        category_publishing_enabled=False,
        metadata_updates={"automatic_theme_fix_applied_at": _iso_now()},
    )
    updated = evaluate_theme_verification(store_hash, persist=True)
    return {
        "status": "ok",
        "reason": "Route Authority switched publishing to product-only mode and reran verification.",
        "verification": updated,
    }


def build_google_authorization_url(integration_key: str, store_hash: str | None = None) -> tuple[str, str]:
    state = _encode_oauth_state(integration_key, store_hash or "")
    flow = _build_google_flow(integration_key, state=state)
    authorization_url, state = flow.authorization_url(
        access_type="offline",
        prompt="select_account consent",
    )
    return authorization_url, state


def complete_google_oauth(
    integration_key: str,
    *,
    store_hash: str,
    state: str,
    authorization_response: str,
) -> dict[str, Any]:
    flow = _build_google_flow(integration_key, state=state)
    flow.fetch_token(authorization_response=authorization_response)
    credentials = flow.credentials
    auth_payload = json.loads(credentials.to_json())
    integration = _upsert_store_integration(
        store_hash,
        integration_key,
        connection_status="connected",
        configuration_status="needs_configuration",
        auth_payload=auth_payload,
        metadata_updates={
            "available_resources": [],
            "connected_at": _iso_now(),
        },
        mark_success=True,
    )
    try:
        if integration_key == "gsc":
            options = list_search_console_properties(auth_payload)
        elif integration_key == "ga4":
            options = list_ga4_properties(auth_payload)
        else:
            raise ValueError(f"Unsupported integration key: {integration_key}")
    except Exception as exc:
        return {
            "integration": integration,
            "available_resources": [],
            "suggested_resource": None,
            "auto_selected": None,
            "selection_result": {
                "status": "warning",
                "reason": (
                    f"{INTEGRATION_DISPLAY[integration_key]} connected, but Google resources "
                    f"could not be listed yet: {exc}"
                ),
            },
        }
    suggested = suggest_storefront_resource(store_hash, options, integration_key=integration_key)
    integration = _upsert_store_integration(
        store_hash,
        integration_key,
        connection_status="connected",
        configuration_status="needs_configuration",
        auth_payload=auth_payload,
        metadata_updates={
            "available_resources": options,
            "suggested_resource_id": suggested.get("id") if suggested else None,
            "connected_at": _iso_now(),
        },
        mark_success=True,
    )
    verified_options = [
        option
        for option in options
        if _verify_selected_option(store_hash, option, integration_key=integration_key)[0]
    ]
    auto_selected = verified_options[0] if len(verified_options) == 1 else None
    auto_selected_result = None
    if auto_selected:
        try:
            auto_selected_result = select_google_resource(
                store_hash,
                integration_key=integration_key,
                selected_resource_id=str(auto_selected.get("id") or ""),
            )
        except Exception as exc:
            auto_selected_result = {
                "status": "warning",
                "reason": (
                    f"{INTEGRATION_DISPLAY[integration_key]} connected, but the first data sync "
                    f"did not finish: {exc}"
                ),
            }
    return {
        "integration": integration,
        "available_resources": options,
        "suggested_resource": suggested,
        "auto_selected": auto_selected,
        "selection_result": auto_selected_result,
    }


def list_search_console_properties(auth_payload: dict[str, Any]) -> list[dict[str, Any]]:
    credentials = _credential_payload_to_credentials(auth_payload)
    service = _build_google_service("searchconsole", "v1", credentials)
    response = service.sites().list().execute() or {}
    options = []
    for row in response.get("siteEntry", []) or []:
        site_url = (row.get("siteUrl") or "").strip()
        if not site_url:
            continue
        options.append(
            {
                "id": site_url,
                "label": site_url,
                "site_url": site_url,
                "permission_level": (row.get("permissionLevel") or "").strip(),
                "default_uri": site_url if site_url.startswith("http") else "",
            }
        )
    options.sort(key=lambda item: item.get("label") or item.get("id") or "")
    return options


def list_ga4_properties(auth_payload: dict[str, Any]) -> list[dict[str, Any]]:
    credentials = _credential_payload_to_credentials(auth_payload)
    service = _build_google_service("analyticsadmin", "v1beta", credentials)
    options: list[dict[str, Any]] = []
    request = service.accountSummaries().list(pageSize=200)
    while request is not None:
        response = request.execute() or {}
        for account in response.get("accountSummaries", []) or []:
            account_name = (account.get("displayName") or account.get("name") or "").strip()
            for property_summary in account.get("propertySummaries", []) or []:
                property_name = (property_summary.get("property") or "").strip()
                property_id = property_name.split("/", 1)[-1] if property_name else ""
                default_uri = ""
                try:
                    streams = service.properties().dataStreams().list(parent=property_name).execute() or {}
                    for stream in streams.get("dataStreams", []) or []:
                        default_uri = ((stream.get("webStreamData") or {}).get("defaultUri") or "").strip()
                        if default_uri:
                            break
                except Exception:
                    default_uri = ""
                options.append(
                    {
                        "id": property_name or property_id,
                        "label": property_summary.get("displayName") or property_name or property_id,
                        "property_id": property_id,
                        "property_name": property_name,
                        "account_name": account_name,
                        "default_uri": default_uri,
                    }
                )
        request = service.accountSummaries().list_next(previous_request=request, previous_response=response)
    options.sort(key=lambda item: (item.get("account_name") or "", item.get("label") or item.get("id") or ""))
    return options


def suggest_storefront_resource(
    store_hash: str,
    options: list[dict[str, Any]],
    *,
    integration_key: str,
) -> dict[str, Any] | None:
    storefront_hosts = _storefront_hosts(store_hash)
    best: tuple[int, dict[str, Any] | None] = (0, None)
    for option in options:
        if integration_key == "gsc":
            host = _normalized_host(option.get("site_url"))
            exact = option.get("site_url", "").startswith("sc-domain:")
            score = 120 if exact and host in storefront_hosts else 100 if host in storefront_hosts else 0
        else:
            host = _normalized_host(option.get("default_uri"))
            score = 100 if host in storefront_hosts else 0
        if score > best[0]:
            best = (score, option)
    return best[1]


def _verify_selected_option(
    store_hash: str,
    option: dict[str, Any] | None,
    *,
    integration_key: str,
) -> tuple[bool, str]:
    if not option:
        return False, "The selected resource was not found in the Google response."
    storefront_hosts = _storefront_hosts(store_hash)
    if integration_key == "gsc":
        host = _normalized_host(option.get("site_url"))
        if host and host in storefront_hosts:
            return True, "Verified against the store storefront domain."
        return False, "The selected Search Console property does not match the store storefront domain."
    host = _normalized_host(option.get("default_uri"))
    if host and host in storefront_hosts:
        return True, "Verified against the store storefront domain."
    return False, "The selected GA4 property does not expose a matching web stream for the store storefront domain."


def refresh_store_gsc_data(store_hash: str) -> dict[str, Any]:
    integration = get_store_integration(store_hash, "gsc")
    property_site_url = (integration.get("selected_resource_id") or "").strip()
    if not property_site_url:
        return {"status": "error", "reason": "Search Console property selection is missing.", "row_count": 0}
    credentials = _credential_payload_to_credentials(integration.get("auth_payload"))
    service = _build_google_service("searchconsole", "v1", credentials)
    start_date, end_date = _gsc_sync_window_dates()
    api_row_limit = _gsc_api_row_limit()
    max_rows = _gsc_sync_max_rows()
    min_impressions = _gsc_sync_min_impressions()
    rows: list[tuple[Any, ...]] = []
    source_row_count = 0
    filtered_low_signal_row_count = 0
    truncated = False
    start_row = 0
    while True:
        response = service.searchanalytics().query(
            siteUrl=property_site_url,
            body={
                "startDate": start_date.isoformat(),
                "endDate": end_date.isoformat(),
                "dimensions": ["date", "page", "query"],
                "rowLimit": api_row_limit,
                "startRow": start_row,
            },
        ).execute() or {}
        batch = response.get("rows", []) or []
        if not batch:
            break
        for item in batch:
            keys = list(item.get("keys") or [])
            if len(keys) < 3:
                continue
            clicks = int(item.get("clicks") or 0)
            impressions = int(item.get("impressions") or 0)
            source_row_count += 1
            if clicks <= 0 and impressions < min_impressions:
                filtered_low_signal_row_count += 1
                continue
            rows.append(
                (
                    normalize_store_hash(store_hash),
                    property_site_url,
                    keys[1],
                    keys[2],
                    keys[0],
                    clicks,
                    impressions,
                    float(item.get("ctr") or 0.0),
                    float(item.get("position") or 0.0),
                    json.dumps({}),
                )
            )
            if len(rows) >= max_rows:
                truncated = True
                break
        if truncated:
            break
        start_row += len(batch)
        if len(batch) < api_row_limit:
            break

    if not rows:
        if source_row_count:
            reason = (
                "Search Console returned rows, but none met the storage filter "
                f"(clicks > 0 or impressions >= {min_impressions}). Existing data was preserved."
            )
        else:
            reason = "Search Console returned zero rows for the selected property and comparison window. Existing data was preserved."
        _upsert_store_integration(
            store_hash,
            "gsc",
            connection_status="connected",
            configuration_status="sync_warning",
            selected_resource_id=property_site_url,
            selected_resource_label=integration.get("selected_resource_label") or property_site_url,
            metadata_updates={
                "sync_status": "warning",
                "last_sync_status": "warning",
                "last_sync_row_count": 0,
                "last_sync_source_row_count": source_row_count,
                "last_sync_filtered_low_signal_row_count": filtered_low_signal_row_count,
                "last_sync_error": reason,
                "last_sync_started_at": _iso_now(),
                "last_sync_finished_at": _iso_now(),
                "last_sync_start_date": start_date.isoformat(),
                "last_sync_end_date": end_date.isoformat(),
                "last_sync_min_impressions": min_impressions,
            },
            mark_error=True,
            error_message=reason,
        )
        return {
            "status": "warning",
            "reason": reason,
            "row_count": 0,
            "source_row_count": source_row_count,
            "filtered_low_signal_row_count": filtered_low_signal_row_count,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        }

    insert_sql = """
        INSERT INTO app_runtime.store_gsc_daily (
            store_hash,
            property_site_url,
            page,
            query,
            date,
            clicks,
            impressions,
            ctr,
            position,
            metadata,
            updated_at
        ) VALUES (%s, %s, %s, %s, %s::date, %s, %s, %s, %s, %s::jsonb, NOW())
        ON CONFLICT (store_hash, property_site_url, page, query, date) DO UPDATE SET
            clicks = EXCLUDED.clicks,
            impressions = EXCLUDED.impressions,
            ctr = EXCLUDED.ctr,
            position = EXCLUDED.position,
            metadata = EXCLUDED.metadata,
            updated_at = NOW()
    """
    delete_sql = """
        DELETE FROM app_runtime.store_gsc_daily
        WHERE store_hash = %s
          AND property_site_url = %s;
    """
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(delete_sql, (normalize_store_hash(store_hash), property_site_url))
            execute_batch(cur, insert_sql, rows, page_size=500)
        conn.commit()
    _invalidate_store_metric_cache(store_hash, LIVE_GSC_CACHE_KEYS)
    _upsert_store_integration(
        store_hash,
        "gsc",
        connection_status="connected",
        configuration_status="ready",
        selected_resource_id=property_site_url,
        selected_resource_label=integration.get("selected_resource_label") or property_site_url,
        metadata_updates={
            "sync_status": "ready",
            "last_sync_status": "ready",
            "last_sync_row_count": len(rows),
            "last_sync_source_row_count": source_row_count,
            "last_sync_filtered_low_signal_row_count": filtered_low_signal_row_count,
            "last_sync_truncated": truncated,
            "last_sync_min_impressions": min_impressions,
            "last_synced_at": _iso_now(),
            "last_sync_finished_at": _iso_now(),
            "last_sync_start_date": start_date.isoformat(),
            "last_sync_end_date": end_date.isoformat(),
        },
        mark_success=True,
    )
    return {
        "status": "ok",
        "row_count": len(rows),
        "source_row_count": source_row_count,
        "filtered_low_signal_row_count": filtered_low_signal_row_count,
        "truncated": truncated,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
    }


def refresh_store_ga4_data(store_hash: str) -> dict[str, Any]:
    integration = get_store_integration(store_hash, "ga4")
    selected_resource_id = (integration.get("selected_resource_id") or "").strip()
    property_name = selected_resource_id if selected_resource_id.startswith("properties/") else f"properties/{selected_resource_id}"
    property_id = property_name.split("/", 1)[-1]
    if not property_id:
        return {"status": "error", "reason": "GA4 property selection is missing.", "row_count": 0}
    from google.analytics.data_v1beta import BetaAnalyticsDataClient
    from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest

    credentials = _credential_payload_to_credentials(integration.get("auth_payload"))
    client = BetaAnalyticsDataClient(credentials=credentials)
    offset = 0
    rows: list[tuple[Any, ...]] = []
    start_date_relative, end_date_relative = _comparison_window_relative_dates()
    while True:
        request = RunReportRequest(
            property=property_name,
            dimensions=[
                Dimension(name="date"),
                Dimension(name="pagePath"),
                Dimension(name="sessionDefaultChannelGroup"),
            ],
            metrics=[
                Metric(name="sessions"),
                Metric(name="totalUsers"),
                Metric(name="engagedSessions"),
                Metric(name="addToCarts"),
                Metric(name="ecommercePurchases"),
                Metric(name="purchaseRevenue"),
            ],
            date_ranges=[DateRange(start_date=start_date_relative, end_date=end_date_relative)],
            limit=100000,
            offset=offset,
        )
        response = client.run_report(request, timeout=GOOGLE_API_TIMEOUT_SECONDS)
        if not response.rows:
            break
        for row in response.rows:
            rows.append(
                (
                    normalize_store_hash(store_hash),
                    property_id,
                    row.dimension_values[0].value,
                    row.dimension_values[1].value or "/",
                    row.dimension_values[2].value or "",
                    int(row.metric_values[0].value or 0),
                    int(row.metric_values[1].value or 0),
                    int(row.metric_values[2].value or 0),
                    int(row.metric_values[3].value or 0),
                    int(row.metric_values[4].value or 0),
                    float(row.metric_values[5].value or 0.0),
                    json.dumps({}),
                )
            )
        offset += len(response.rows)
        if len(response.rows) < 100000:
            break

    start_date = min((str(row[2]) for row in rows), default=start_date_relative)
    end_date = max((str(row[2]) for row in rows), default=end_date_relative)
    if not rows:
        reason = "GA4 returned zero rows for the selected property and comparison window. Existing data was preserved."
        _upsert_store_integration(
            store_hash,
            "ga4",
            connection_status="connected",
            configuration_status="sync_warning",
            selected_resource_id=property_name,
            selected_resource_label=integration.get("selected_resource_label") or property_name,
            metadata_updates={
                "sync_status": "warning",
                "last_sync_status": "warning",
                "last_sync_row_count": 0,
                "last_sync_error": reason,
                "last_sync_started_at": _iso_now(),
                "last_sync_finished_at": _iso_now(),
                "last_sync_start_date": start_date,
                "last_sync_end_date": end_date,
            },
            mark_error=True,
            error_message=reason,
        )
        return {
            "status": "warning",
            "reason": reason,
            "row_count": 0,
            "start_date": start_date,
            "end_date": end_date,
        }

    temp_insert_sql = """
        INSERT INTO temp_store_ga4_pages_daily (
            store_hash,
            property_id,
            date,
            page_path,
            channel_group,
            sessions,
            total_users,
            engaged_sessions,
            add_to_carts,
            ecommerce_purchases,
            purchase_revenue,
            metadata,
            updated_at
        ) VALUES (%s, %s, %s::date, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW())
    """
    replace_sql = """
        DELETE FROM app_runtime.store_ga4_pages_daily
        WHERE store_hash = %s
          AND property_id = %s;
        INSERT INTO app_runtime.store_ga4_pages_daily (
            store_hash,
            property_id,
            date,
            page_path,
            channel_group,
            sessions,
            total_users,
            engaged_sessions,
            add_to_carts,
            ecommerce_purchases,
            purchase_revenue,
            metadata,
            updated_at
        )
        SELECT
            store_hash,
            property_id,
            date,
            page_path,
            channel_group,
            sessions,
            total_users,
            engaged_sessions,
            add_to_carts,
            ecommerce_purchases,
            purchase_revenue,
            metadata,
            updated_at
        FROM temp_store_ga4_pages_daily
        WHERE TRUE
        ON CONFLICT (store_hash, property_id, date, page_path, channel_group) DO UPDATE SET
            sessions = EXCLUDED.sessions,
            total_users = EXCLUDED.total_users,
            engaged_sessions = EXCLUDED.engaged_sessions,
            add_to_carts = EXCLUDED.add_to_carts,
            ecommerce_purchases = EXCLUDED.ecommerce_purchases,
            purchase_revenue = EXCLUDED.purchase_revenue,
            metadata = EXCLUDED.metadata,
            updated_at = NOW();
    """
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TEMP TABLE temp_store_ga4_pages_daily
                (LIKE app_runtime.store_ga4_pages_daily INCLUDING DEFAULTS)
                ON COMMIT DROP;
                """
            )
            execute_batch(cur, temp_insert_sql, rows, page_size=500)
            cur.execute(replace_sql, (normalize_store_hash(store_hash), property_id))
        conn.commit()
    _upsert_store_integration(
        store_hash,
        "ga4",
        connection_status="connected",
        configuration_status="ready",
        selected_resource_id=property_name,
        selected_resource_label=integration.get("selected_resource_label") or property_name,
        metadata_updates={
            "sync_status": "ready",
            "last_sync_status": "ready",
            "last_sync_row_count": len(rows),
            "last_synced_at": _iso_now(),
            "last_sync_finished_at": _iso_now(),
            "last_sync_start_date": start_date,
            "last_sync_end_date": end_date,
        },
        mark_success=True,
    )
    return {"status": "ok", "row_count": len(rows), "start_date": start_date, "end_date": end_date}


def select_google_resource(
    store_hash: str,
    *,
    integration_key: str,
    selected_resource_id: str,
) -> dict[str, Any]:
    integration = get_store_integration(store_hash, integration_key)
    auth_payload = dict(integration.get("auth_payload") or {})
    if not auth_payload:
        return {"status": "error", "reason": f"{INTEGRATION_DISPLAY[integration_key]} is not connected yet."}
    if integration_key == "gsc":
        options = list_search_console_properties(auth_payload)
    elif integration_key == "ga4":
        options = list_ga4_properties(auth_payload)
    else:
        return {"status": "error", "reason": "Unsupported integration."}
    option_map = {str(item.get("id") or ""): item for item in options}
    selected = option_map.get(str(selected_resource_id or "").strip())
    verified, verification_message = _verify_selected_option(store_hash, selected, integration_key=integration_key)
    if not verified:
        _upsert_store_integration(
            store_hash,
            integration_key,
            connection_status="connected",
            configuration_status="error",
            metadata_updates={"available_resources": options, "verification_message": verification_message},
            mark_error=True,
            error_message=verification_message,
        )
        return {"status": "error", "reason": verification_message}

    _upsert_store_integration(
        store_hash,
        integration_key,
        connection_status="connected",
        configuration_status="syncing",
        selected_resource_id=selected.get("id"),
        selected_resource_label=selected.get("label") or selected.get("id"),
        auth_payload=auth_payload,
        metadata_updates={
            "available_resources": options,
            "verification_message": verification_message,
            "verified_at": _iso_now(),
            "default_uri": selected.get("default_uri") or "",
            "sync_status": "queued",
        },
        mark_success=True,
    )
    sync_result = enqueue_integration_sync(
        store_hash,
        integration_key,
        triggered_by="property_selection",
        selected_resource_id=str(selected.get("id") or ""),
        metadata_updates={"verification_message": verification_message},
    )
    return {"status": "ok", "selected": selected, "sync_result": sync_result}


def build_setup_context(store_hash: str) -> dict[str, Any]:
    normalized_hash = normalize_store_hash(store_hash)
    installation = get_store_installation(normalized_hash)
    bigcommerce = sync_bigcommerce_integration(normalized_hash)
    gsc = get_store_integration(normalized_hash, "gsc")
    ga4 = get_store_integration(normalized_hash, "ga4")
    gsc_data_summary = _safe_store_integration_data_summary(normalized_hash, "gsc", gsc.get("selected_resource_id"))
    ga4_data_summary = _safe_store_integration_data_summary(normalized_hash, "ga4", ga4.get("selected_resource_id"))
    gsc = _reconcile_integration_data_readiness(normalized_hash, "gsc", gsc, gsc_data_summary)
    ga4 = _reconcile_integration_data_readiness(normalized_hash, "ga4", ga4, ga4_data_summary)
    gsc = {**gsc, "data_summary": gsc_data_summary, "data_ready": bool(gsc_data_summary.get("has_data"))}
    ga4 = {**ga4, "data_summary": ga4_data_summary, "data_ready": bool(ga4_data_summary.get("has_data"))}
    publish_settings = get_store_publish_settings(normalized_hash)
    readiness = get_store_readiness(normalized_hash)
    profile_summary = get_store_profile_summary(normalized_hash)
    theme_status = evaluate_theme_verification(normalized_hash, persist=False)

    bigcommerce_ready = bool(installation and (installation.get("status") or "").strip().lower() == "active")
    gsc_ready = _integration_data_ready(gsc, gsc_data_summary)
    ga4_ready = _integration_data_ready(ga4, ga4_data_summary)
    catalog_synced = bool(readiness.get("catalog_synced"))
    theme_ready = (theme_status.get("verification_status") or "").strip().lower() == "ready"
    publishing_enabled = bool(publish_settings.get("publishing_enabled"))

    readiness_state = "needs_setup"
    readiness_label = "Needs setup"
    readiness_detail = "One or more required setup items are incomplete."
    if bigcommerce_ready and gsc_ready and ga4_ready and catalog_synced:
        readiness_state = "ready_to_generate"
        readiness_label = "Ready to generate"
        readiness_detail = "Route Authority can generate recommendations for this store."
    if readiness_state == "ready_to_generate" and theme_ready and publishing_enabled:
        readiness_state = "ready_for_publishing"
        readiness_label = "Ready Set Published"
        readiness_detail = "Setup is complete and publishing can run for this store."

    checklist = [
        {
            "key": "bigcommerce_install",
            "label": "BigCommerce install",
            "complete": bigcommerce_ready,
            "status_label": _integration_status_label(bigcommerce),
            "detail": "Store identity and install context are active." if bigcommerce_ready else "The app has not completed install for this store.",
        },
        {
            "key": "search_console",
            "label": "Search Console",
            "complete": gsc_ready,
            "status_label": _integration_status_label(gsc),
            "detail": _integration_detail(gsc, gsc_data_summary, "Connect and select the correct Search Console property."),
        },
        {
            "key": "ga4",
            "label": "GA4",
            "complete": ga4_ready,
            "status_label": _integration_status_label(ga4),
            "detail": _integration_detail(ga4, ga4_data_summary, "Connect and select the correct GA4 property."),
        },
        {
            "key": "catalog_sync",
            "label": "Catalog sync",
            "complete": catalog_synced,
            "status_label": "Ready" if catalog_synced else "Needs sync",
            "detail": f"{int(profile_summary.get('profile_count') or 0)} products and {int(profile_summary.get('category_profile_count') or 0)} categories synced.",
        },
        {
            "key": "theme_verification",
            "label": "Theme verification",
            "complete": theme_ready,
            "status_label": "Ready" if theme_ready else "Needs check",
            "detail": theme_status.get("summary") or "Run theme verification.",
        },
        {
            "key": "readiness",
            "label": "Readiness",
            "complete": readiness_state in {"ready_to_generate", "ready_for_publishing"},
            "status_label": readiness_label,
            "detail": readiness_detail,
        },
    ]
    return {
        "store_hash": normalized_hash,
        "installation": installation,
        "integrations": {
            "bigcommerce": {**bigcommerce, "status_label": _integration_status_label(bigcommerce), "display_name": INTEGRATION_DISPLAY["bigcommerce"]},
            "gsc": {**gsc, "status_label": _integration_status_label(gsc or {}), "display_name": INTEGRATION_DISPLAY["gsc"]},
            "ga4": {**ga4, "status_label": _integration_status_label(ga4 or {}), "display_name": INTEGRATION_DISPLAY["ga4"]},
        },
        "publish_settings": publish_settings,
        "theme_status": theme_status,
        "readiness": readiness,
        "profile_summary": profile_summary,
        "checklist": checklist,
        "readiness_state": readiness_state,
        "readiness_label": readiness_label,
        "readiness_detail": readiness_detail,
        "setup_complete": readiness_state in {"ready_to_generate", "ready_for_publishing"},
    }


def build_store_readiness_snapshot(store_hash: str) -> dict[str, Any]:
    normalized_hash = normalize_store_hash(store_hash)
    installation = get_store_installation(normalized_hash)
    gsc = _public_store_integration(normalized_hash, "gsc")
    ga4 = _public_store_integration(normalized_hash, "ga4")
    gsc_summary = _safe_store_integration_data_summary(normalized_hash, "gsc", gsc.get("selected_resource_id"))
    ga4_summary = _safe_store_integration_data_summary(normalized_hash, "ga4", ga4.get("selected_resource_id"))
    gsc_public = {**gsc, "data_summary": gsc_summary, "data_ready": bool(gsc_summary.get("has_data"))}
    ga4_public = {**ga4, "data_summary": ga4_summary, "data_ready": bool(ga4_summary.get("has_data"))}
    readiness = get_store_readiness(normalized_hash)
    profile_summary = get_store_profile_summary(normalized_hash)
    readiness_metadata = readiness.get("metadata") if isinstance(readiness.get("metadata"), dict) else {}
    product_theme_ready = bool(
        theme_hook_present(Config.FULCRUM_THEME_PRODUCT_TEMPLATE)
        or readiness.get("theme_hook_ready")
    )
    category_theme_ready = bool(
        category_theme_hook_present(Config.FULCRUM_THEME_CATEGORY_TEMPLATE)
        or readiness_metadata.get("category_theme_hook_present")
    )
    checks = {
        "bigcommerce": {
            "ready": bool(installation and (installation.get("status") or "").strip().lower() == "active"),
            "status": (installation.get("status") or "missing") if installation else "missing",
            "detail": "Active BigCommerce install record found." if installation else "No active BigCommerce install record found.",
            "updated_at": installation.get("updated_at") if installation else None,
        },
        "gsc": {
            "ready": _integration_data_ready(gsc_public, gsc_summary),
            "status": _integration_status_label(gsc_public),
            "connection_status": gsc.get("connection_status") or "not_connected",
            "configuration_status": gsc.get("configuration_status") or "not_configured",
            "selected_resource_id": gsc.get("selected_resource_id") or "",
            "selected_resource_label": gsc.get("selected_resource_label") or "",
            "row_count": int(gsc_summary.get("row_count") or 0),
            "date_range": [gsc_summary.get("start_date") or "", gsc_summary.get("end_date") or ""],
            "last_error": gsc_summary.get("latest_sync_error") or gsc.get("last_error_message") or "",
            "latest_sync_run": gsc_summary.get("latest_sync_run") or {},
        },
        "ga4": {
            "ready": _integration_data_ready(ga4_public, ga4_summary),
            "status": _integration_status_label(ga4_public),
            "connection_status": ga4.get("connection_status") or "not_connected",
            "configuration_status": ga4.get("configuration_status") or "not_configured",
            "selected_resource_id": ga4.get("selected_resource_id") or "",
            "selected_resource_label": ga4.get("selected_resource_label") or "",
            "row_count": int(ga4_summary.get("row_count") or 0),
            "date_range": [ga4_summary.get("start_date") or "", ga4_summary.get("end_date") or ""],
            "last_error": ga4_summary.get("latest_sync_error") or ga4.get("last_error_message") or "",
            "latest_sync_run": ga4_summary.get("latest_sync_run") or {},
        },
        "catalog": {
            "ready": bool(readiness.get("catalog_synced")),
            "status": "ready" if readiness.get("catalog_synced") else "needs_sync",
            "product_count": int(profile_summary.get("profile_count") or 0),
            "category_count": int(profile_summary.get("category_profile_count") or 0),
        },
        "theme": {
            "ready": bool(product_theme_ready),
            "status": "ready" if product_theme_ready else "needs_hook",
            "product_theme_hook_ready": bool(product_theme_ready),
            "category_theme_hook_ready": bool(category_theme_ready),
        },
    }
    overall_ready = all(checks[key]["ready"] for key in ("bigcommerce", "gsc", "ga4", "catalog"))
    return {
        "status": "ok" if overall_ready else "needs_attention",
        "store_hash": normalized_hash,
        "checked_at": _iso_now(),
        "checks": checks,
        "readiness": readiness,
        "profile_summary": profile_summary,
    }


def merchant_landing_path(store_hash: str) -> str:
    context = build_setup_context(store_hash)
    readiness_state = (context.get("readiness_state") or "").strip().lower()
    if readiness_state in {"ready_to_generate", "ready_for_publishing"} or context.get("setup_complete") is True:
        return "results"
    return "setup"
