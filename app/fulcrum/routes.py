import hashlib
import hmac
import re
import os
import threading
import time
from datetime import UTC, datetime
from urllib.parse import urlparse

import requests
from flask import (
    Blueprint,
    abort,
    current_app,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)

from app.fulcrum.config import Config
from app.fulcrum.logic_regression import (
    record_regression_against_logic_changelog,
    resolve_logic_change_case_ids,
    run_logic_regression,
)
from .services import (
    _gate_review_map_for_ids,
    _query_gate_record_map_for_ids,
    _resolve_store_token,
    apply_theme_automatic_fix,
    apply_runtime_schema,
    build_google_authorization_url,
    build_store_readiness_snapshot,
    build_setup_context,
    category_theme_hook_present,
    category_publishing_enabled_for_store,
    complete_google_oauth,
    count_query_gate_review_requests,
    decode_google_oauth_state,
    decode_signed_payload,
    evaluate_theme_verification,
    exchange_auth_code,
    generate_candidate_run,
    enqueue_integration_sync,
    get_query_gate_review_request_by_id,
    get_dashboard_context,
    invalidate_admin_metric_cache,
    list_query_gate_agent_reviews,
    list_publications,
    preview_product_html,
    mark_store_uninstalled,
    normalize_store_hash,
    publish_approved_entities,
    refresh_store_readiness,
    review_candidates,
    review_mapping_rows,
    request_query_gate_review,
    pause_source_for_review,
    publish_all_current_results,
    purge_store_data_on_uninstall,
    queue_candidate_run,
    resolve_publish_source_entity_ids,
    review_all_edge_cases,
    resolve_query_gate_review_request,
    restore_source_after_review,
    run_changed_route_agent_review,
    run_query_gate_agent_review,
    select_google_resource,
    sync_bigcommerce_integration,
    set_query_target_override,
    submit_query_gate_review_session,
    sync_store_catalog_profiles,
    theme_hook_present,
    unpublish_entities,
    upsert_store_publish_settings,
    update_query_gate_review_request_metadata,
    upsert_store_installation,
    merchant_landing_path,
    merge_store_installation_metadata,
)


fulcrum_bp = Blueprint("fulcrum", __name__, url_prefix="/fulcrum", template_folder="templates")


@fulcrum_bp.after_request
def _disable_response_caching(response):
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


def _status_label(status: str) -> str:
    normalized = (status or "").strip().lower()
    if normalized == "pass":
        return "Pass"
    if normalized == "warning":
        return "Review"
    return "Fail"


def _format_timestamp_label(value) -> str:
    if not value:
        return "Not recorded"
    if hasattr(value, "strftime"):
        label = value.strftime("%Y-%m-%d %H:%M:%S %Z").strip()
        return label or value.isoformat(sep=" ", timespec="seconds")
    return str(value)


def _callback_probe_timestamp() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _is_https_url(value: str | None) -> bool:
    parsed = urlparse((value or "").strip())
    return parsed.scheme.lower() == "https" and bool(parsed.netloc)


def _check_public_url(url: str | None) -> dict[str, object]:
    target = (url or "").strip()
    if not target:
        return {"ok": False, "status_code": None, "detail": "URL is not configured."}
    try:
        response = requests.get(target, timeout=2, allow_redirects=True)
    except requests.RequestException as exc:
        return {"ok": False, "status_code": None, "detail": f"Reachability check failed: {exc}"}
    if response.ok:
        return {"ok": True, "status_code": response.status_code, "detail": f"Reachable with HTTP {response.status_code}."}
    return {"ok": False, "status_code": response.status_code, "detail": f"Returned HTTP {response.status_code}."}


def _readiness_check_item(*, label: str, ok: bool, detail: str, url: str | None = None, status: str | None = None) -> dict[str, object]:
    resolved_status = status or ("pass" if ok else "fail")
    return {
        "label": label,
        "ok": ok,
        "status": resolved_status,
        "status_label": _status_label(resolved_status),
        "detail": detail,
        "url": url or "",
    }


def _build_marketplace_review_context(context: dict[str, object], *, verify_public_urls: bool = True) -> dict[str, object]:
    installation = context.get("installation") if isinstance(context.get("installation"), dict) else {}
    metadata = installation.get("metadata") if isinstance(installation.get("metadata"), dict) else {}
    callback_urls = _callback_urls()
    google_oauth_configured = bool(Config.GOOGLE_OAUTH_CLIENT_ID and Config.GOOGLE_OAUTH_CLIENT_SECRET)
    terms_url = Config.FULCRUM_TERMS_OF_SERVICE_URL
    developer_callbacks = {
        "gsc_callback": Config.FULCRUM_GSC_OAUTH_CALLBACK_URL,
        "ga4_callback": Config.FULCRUM_GA4_OAUTH_CALLBACK_URL,
        "privacy_policy": Config.FULCRUM_PRIVACY_POLICY_URL,
        "support_url": Config.FULCRUM_SUPPORT_URL,
        "terms_url": terms_url,
    }

    if verify_public_urls:
        legal_checks = {
            "privacy": _check_public_url(Config.FULCRUM_PRIVACY_POLICY_URL),
            "support": _check_public_url(Config.FULCRUM_SUPPORT_URL),
            "terms": _check_public_url(terms_url),
        }
    else:
        unchecked = {"ok": False, "status_code": None, "detail": "Public reachability is not checked during page render."}
        legal_checks = {"privacy": unchecked, "support": unchecked, "terms": unchecked}
    readiness_state = (context.get("readiness_state") or "").strip().lower()
    readiness_label = (context.get("readiness_label") or "").strip() or "Needs setup"
    readiness_detail = (context.get("readiness_detail") or "").strip() or "Store readiness is incomplete."
    install_status = (installation.get("status") or "").strip().lower() or "missing"
    install_source = (installation.get("install_source") or "").strip() or "not recorded"
    auth_fallback = bool(metadata.get("auth_fallback"))
    auth_error_type = (metadata.get("auth_error_type") or "").strip()
    auth_error_status = metadata.get("auth_error_status")
    last_auth_callback_seen_at = _format_timestamp_label(metadata.get("last_auth_callback_seen_at"))
    last_auth_callback_outcome = (metadata.get("last_auth_callback_outcome") or "").strip() or "Not recorded"
    last_load_callback_seen_at = _format_timestamp_label(metadata.get("last_load_callback_seen_at"))
    last_load_callback_outcome = (metadata.get("last_load_callback_outcome") or "").strip() or "Not recorded"
    auth_error_label = auth_error_type or "unknown error"
    if auth_error_status:
        auth_error_label = f"{auth_error_label} / HTTP {auth_error_status}"

    marketplace_readiness = [
        _readiness_check_item(
            label="BigCommerce auth callback",
            ok=bool(callback_urls["auth"]) and _is_https_url(callback_urls["auth"]),
            detail="Configured on HTTPS for Developer Portal install flow." if _is_https_url(callback_urls["auth"]) else "Use a public HTTPS auth callback before submission.",
            url=callback_urls["auth"],
        ),
        _readiness_check_item(
            label="BigCommerce load callback",
            ok=bool(callback_urls["load"]) and _is_https_url(callback_urls["load"]),
            detail="Configured on HTTPS for iframe load flow." if _is_https_url(callback_urls["load"]) else "Use a public HTTPS load callback before submission.",
            url=callback_urls["load"],
        ),
        _readiness_check_item(
            label="BigCommerce uninstall callback",
            ok=bool(callback_urls["uninstall"]) and _is_https_url(callback_urls["uninstall"]),
            detail="Configured on HTTPS for uninstall cleanup." if _is_https_url(callback_urls["uninstall"]) else "Use a public HTTPS uninstall callback before submission.",
            url=callback_urls["uninstall"],
        ),
        _readiness_check_item(
            label="BigCommerce remove-user callback",
            ok=bool(callback_urls["remove_user"]) and _is_https_url(callback_urls["remove_user"]),
            detail="Configured on HTTPS for multi-user revoke events." if _is_https_url(callback_urls["remove_user"]) else "Use a public HTTPS remove-user callback before submission.",
            url=callback_urls["remove_user"],
        ),
        _readiness_check_item(
            label="Google OAuth credentials",
            ok=google_oauth_configured,
            detail="Google OAuth client ID and secret are configured." if google_oauth_configured else "Configure Google OAuth before Marketplace review.",
        ),
        _readiness_check_item(
            label="Active installation record",
            ok=install_status == "active",
            detail="Current install status is pending summary update.",
        ),
        _readiness_check_item(
            label="Multi-user support coverage",
            ok=bool(callback_urls["load"]) and bool(callback_urls["remove_user"]),
            detail="Load and remove-user callback paths exist for owner and non-owner access." if callback_urls["load"] and callback_urls["remove_user"] else "Add both load and remove-user support before submission.",
        ),
        _readiness_check_item(
            label="Privacy policy page",
            ok=bool(legal_checks["privacy"]["ok"]),
            detail=str(legal_checks["privacy"]["detail"]),
            url=Config.FULCRUM_PRIVACY_POLICY_URL,
            status="warning" if not verify_public_urls else None,
        ),
        _readiness_check_item(
            label="Support page",
            ok=bool(legal_checks["support"]["ok"]),
            detail=str(legal_checks["support"]["detail"]),
            url=Config.FULCRUM_SUPPORT_URL,
            status="warning" if not verify_public_urls else None,
        ),
        _readiness_check_item(
            label="Terms of service page",
            ok=bool(legal_checks["terms"]["ok"]),
            detail=str(legal_checks["terms"]["detail"]),
            url=terms_url,
            status="warning" if not verify_public_urls else None,
        ),
        _readiness_check_item(
            label="Current store readiness",
            ok=readiness_state == "ready_for_publishing",
            detail=f"{readiness_label}: {readiness_detail}",
        ),
        _readiness_check_item(
            label="Install auth path",
            ok=not auth_fallback,
            status="warning" if auth_fallback else "pass",
            detail=(
                f"Fallback token path was used previously ({auth_error_label}). Re-test a clean install before submission."
                if auth_fallback
                else "The current install record does not show auth fallback metadata."
            ),
        ),
    ]

    marketplace_readiness[5]["detail"] = f"Current install status is {install_status or 'missing'} via {install_source}."
    store_hash = (context.get("store_hash") or "").strip()
    try:
        readiness_snapshot = build_store_readiness_snapshot(store_hash) if store_hash else {}
    except Exception as exc:  # noqa: BLE001
        current_app.logger.exception("Could not build Fulcrum readiness snapshot.")
        readiness_snapshot = {
            "status": "error",
            "store_hash": store_hash,
            "error": str(exc) or type(exc).__name__,
            "checks": {},
        }
    database_label = "DATABASE_URL" if Config.DATABASE_URL else f"{Config.DB_HOST or 'unset'}:{Config.DB_PORT}/{Config.DB_NAME or 'unset'}"
    runtime_diagnostics = {
        "app_base_url": Config.FULCRUM_APP_BASE_URL,
        "worker_host": os.environ.get("FULCRUM_HOST") or "not set",
        "worker_port": os.environ.get("FULCRUM_PORT") or "not set",
        "flask_env": Config.FLASK_ENV,
        "database_label": database_label,
        "scheduler_enabled": bool(Config.ENABLE_SCHEDULER),
        "embedded_scheduler_enabled": bool(Config.FULCRUM_RUN_EMBEDDED_SCHEDULER),
        "allowed_stores": list(Config.FULCRUM_ALLOWED_STORES or []),
    }

    return {
        "callback_urls": callback_urls,
        "google_oauth_configured": google_oauth_configured,
        "developer_callbacks": developer_callbacks,
        "terms_url": terms_url,
        "installation_status": install_status or "missing",
        "install_source": install_source,
        "installed_at": _format_timestamp_label(installation.get("installed_at")),
        "updated_at": _format_timestamp_label(installation.get("updated_at")),
        "owner_email": (installation.get("owner_email") or "").strip() or "Not recorded",
        "auth_fallback": auth_fallback,
        "auth_error_type": auth_error_type,
        "auth_error_status": auth_error_status,
        "last_auth_callback_seen_at": last_auth_callback_seen_at,
        "last_auth_callback_outcome": last_auth_callback_outcome,
        "last_load_callback_seen_at": last_load_callback_seen_at,
        "last_load_callback_outcome": last_load_callback_outcome,
        "marketplace_readiness": marketplace_readiness,
        "readiness_snapshot": readiness_snapshot,
        "runtime_diagnostics": runtime_diagnostics,
    }


def _normalize_storefront_path(url: str | None) -> str:
    raw = (url or "").strip()
    if not raw:
        return ""
    if raw.startswith("/"):
        path = raw
    else:
        parsed = urlparse(raw)
        path = parsed.path or raw
    path = re.sub(r"/{2,}", "/", path).strip()
    if not path.startswith("/"):
        path = "/" + path
    if path != "/" and not path.endswith("/"):
        path += "/"
    return path.lower()


def _review_target_matches_source(
    *,
    source_entity_type: str | None,
    source_entity_id: int | None,
    source_url: str | None,
    target_entity_type: str | None,
    target_entity_id: int | None,
    target_url: str | None,
) -> bool:
    normalized_source_type = (source_entity_type or "").strip().lower()
    normalized_target_type = (target_entity_type or "").strip().lower()
    source_id = int(source_entity_id or 0)
    target_id = int(target_entity_id or 0)
    normalized_source_url = _normalize_storefront_path(source_url)
    normalized_target_url = _normalize_storefront_path(target_url)
    same_entity = bool(
        normalized_source_type
        and normalized_target_type
        and normalized_source_type == normalized_target_type
        and source_id
        and target_id
        and source_id == target_id
    )
    same_url = bool(normalized_source_url and normalized_target_url and normalized_source_url == normalized_target_url)
    return same_entity or same_url


def _resolve_review_restore_target(
    request_row: dict[str, object] | None,
    *,
    gate_review: dict[str, object] | None = None,
    gate_row: dict[str, object] | None = None,
) -> tuple[str | None, int | None, str | None, str]:
    request_row = request_row if isinstance(request_row, dict) else {}
    gate_review = gate_review if isinstance(gate_review, dict) else {}
    gate_row = gate_row if isinstance(gate_row, dict) else {}

    recommended_action = ((gate_review or {}).get("recommended_action") or "").strip().lower()
    if recommended_action == "use_original":
        return None, None, None, recommended_action

    source_entity_type = (request_row.get("source_entity_type") or "product").strip().lower()
    try:
        source_entity_id = int(request_row.get("source_entity_id") or 0)
    except (TypeError, ValueError):
        source_entity_id = 0
    source_url = request_row.get("source_url")

    gate_target = dict(gate_row.get("suggested_target") or {}) if isinstance(gate_row.get("suggested_target"), dict) else {}
    review_metadata = gate_review.get("metadata") if isinstance(gate_review.get("metadata"), dict) else {}
    review_winner = dict(review_metadata.get("winner") or {}) if isinstance(review_metadata, dict) else {}

    default_target_entity_type = (request_row.get("target_entity_type") or "").strip().lower() or None
    try:
        default_target_entity_id = int(request_row.get("target_entity_id") or 0)
    except (TypeError, ValueError):
        default_target_entity_id = 0
    default_target_url = (request_row.get("target_url") or "").strip() or None

    if recommended_action == "keep_winner":
        target_entity_type = (
            (gate_target.get("entity_type") or "").strip().lower()
            or (review_winner.get("entity_type") or "").strip().lower()
            or default_target_entity_type
        )
        try:
            target_entity_id = int(
                gate_target.get("entity_id")
                or review_winner.get("entity_id")
                or default_target_entity_id
                or 0
            )
        except (TypeError, ValueError):
            target_entity_id = 0
        target_url = (
            (gate_target.get("url") or "").strip()
            or (review_winner.get("url") or "").strip()
            or default_target_url
        )
    else:
        target_entity_type = default_target_entity_type
        target_entity_id = default_target_entity_id
        target_url = default_target_url

    if not target_entity_type or not (target_entity_id or target_url):
        return None, None, None, recommended_action

    if _review_target_matches_source(
        source_entity_type=source_entity_type,
        source_entity_id=source_entity_id,
        source_url=source_url,
        target_entity_type=target_entity_type,
        target_entity_id=target_entity_id,
        target_url=target_url,
    ):
        return None, None, None, recommended_action

    return target_entity_type, (target_entity_id or None), target_url, recommended_action


def _current_store_hash() -> str:
    return (
        request.args.get("store_hash")
        or request.form.get("store_hash")
        or session.get("fulcrum_store_hash")
        or (Config.FULCRUM_ALLOWED_STORES[0] if Config.FULCRUM_ALLOWED_STORES else Config.BIG_COMMERCE_STORE_HASH)
    )


def _has_explicit_store_context() -> bool:
    return bool(
        (request.args.get("store_hash") or "").strip()
        or (request.form.get("store_hash") or "").strip()
        or session.get("fulcrum_store_hash")
    )


def _require_store_allowed(store_hash: str) -> None:
    normalized = normalize_store_hash(store_hash)
    allowed = set(Config.FULCRUM_ALLOWED_STORES or [])
    if allowed and normalized not in allowed:
        abort(403)


def _require_internal_api_auth() -> None:
    shared_secret = (Config.FULCRUM_SHARED_SECRET or "").strip()
    if not shared_secret:
        abort(503)

    direct_secret = (request.headers.get("X-Fulcrum-Shared-Secret") or "").strip()
    if direct_secret and hmac.compare_digest(direct_secret, shared_secret):
        return

    signature = (request.headers.get("X-Fulcrum-Signature") or "").strip()
    timestamp = (request.headers.get("X-Fulcrum-Timestamp") or "").strip()
    if signature and timestamp:
        try:
            if abs(time.time() - int(timestamp)) > 300:
                abort(401)
        except ValueError:
            abort(401)
        payload = request.get_data(cache=True) or b""
        message = timestamp.encode("utf-8") + b"\n" + payload
        expected = hmac.new(shared_secret.encode("utf-8"), message, hashlib.sha256).hexdigest()
        provided = signature.split("=", 1)[-1]
        if hmac.compare_digest(provided, expected):
            return

    abort(401)


def _redirect_to_requested_view(store_hash: str):
    redirect_to = (request.form.get("redirect_to") or request.args.get("redirect_to") or "").strip().lower()
    if redirect_to == "setup":
        return redirect(url_for("fulcrum.setup_page", store_hash=store_hash))
    if redirect_to == "connections":
        return _redirect_to_setup_section(store_hash, "connections")
    if redirect_to in {"store_setup", "store_checks"}:
        return _redirect_to_setup_section(store_hash, "store-checks")
    if redirect_to == "review":
        return redirect(url_for("fulcrum.review_page", store_hash=store_hash))
    if redirect_to == "settings":
        return _redirect_to_setup_section(store_hash, "publishing-settings")
    if redirect_to == "admin":
        return redirect(url_for("fulcrum.admin_dashboard", store_hash=store_hash))
    if redirect_to == "admin_developer":
        return redirect(url_for("fulcrum.admin_developer_dashboard", store_hash=store_hash))
    if redirect_to == "admin_quality":
        return redirect(url_for("fulcrum.admin_quality_dashboard", store_hash=store_hash))
    return redirect(url_for("fulcrum.dashboard", store_hash=store_hash))


def _callback_urls() -> dict[str, str]:
    return {
        "auth": Config.FULCRUM_AUTH_CALLBACK_URL,
        "load": Config.FULCRUM_LOAD_CALLBACK_URL,
        "uninstall": Config.FULCRUM_UNINSTALL_CALLBACK_URL,
        "remove_user": Config.FULCRUM_REMOVE_USER_CALLBACK_URL,
    }


def _merchant_base_context(store_hash: str) -> dict[str, object]:
    context = build_setup_context(store_hash)
    dashboard_context = get_dashboard_context(store_hash, include_admin=False)
    context["callback_urls"] = _callback_urls()
    context["gadgets_configured"] = bool(Config.FULCRUM_GADGETS_API_KEY)
    context["privacy_policy_url"] = Config.FULCRUM_PRIVACY_POLICY_URL
    context["support_url"] = Config.FULCRUM_SUPPORT_URL
    context["terms_url"] = Config.FULCRUM_TERMS_OF_SERVICE_URL
    context["review_bucket_requests"] = dashboard_context.get("review_bucket_requests") or []
    context["review_bucket_count"] = dashboard_context.get("review_bucket_count") or 0
    context["latest_gate_run_id"] = dashboard_context.get("latest_gate_run_id")
    return context


def _merchant_landing_redirect(store_hash: str):
    destination = merchant_landing_path(store_hash)
    if destination == "results":
        return redirect(url_for("fulcrum.dashboard", store_hash=store_hash))
    return redirect(url_for("fulcrum.setup_page", store_hash=store_hash))


def _post_install_redirect(store_hash: str):
    return redirect(url_for("fulcrum.install_complete_page", store_hash=store_hash))


def _redirect_to_setup_section(store_hash: str, section_id: str | None = None):
    target = url_for("fulcrum.setup_page", store_hash=store_hash)
    if section_id:
        target = f"{target}#{section_id}"
    return redirect(target)


def _oauth_session_key(integration_key: str) -> str:
    return f"route_authority_google_oauth_{integration_key}"


def _checkbox_enabled(field_name: str) -> bool:
    return (request.form.get(field_name) or "").strip().lower() in {"1", "true", "yes", "on"}


def _oauth_start_redirect(integration_key: str):
    apply_runtime_schema()
    store_hash = normalize_store_hash(request.args.get("store_hash") or _current_store_hash())
    _require_store_allowed(store_hash)
    sync_bigcommerce_integration(store_hash)
    try:
        authorization_url, state = build_google_authorization_url(integration_key, store_hash=store_hash)
    except Exception as exc:  # noqa: BLE001
        current_app.logger.exception("Could not start %s OAuth.", integration_key)
        flash(str(exc) or f"Could not start {integration_key.upper()} OAuth.", "danger")
        return _redirect_to_setup_section(store_hash, "connections")
    session[_oauth_session_key(integration_key)] = {"state": state, "store_hash": store_hash}
    return redirect(authorization_url)


def _oauth_callback_authorization_response(integration_key: str) -> str:
    if integration_key == "gsc":
        callback_url = Config.FULCRUM_GSC_OAUTH_CALLBACK_URL
    elif integration_key == "ga4":
        callback_url = Config.FULCRUM_GA4_OAUTH_CALLBACK_URL
    else:
        raise ValueError(f"Unsupported integration key: {integration_key}")
    query_string = (request.query_string or b"").decode("utf-8")
    return f"{callback_url}?{query_string}" if query_string else callback_url


def _oauth_callback_redirect(integration_key: str):
    apply_runtime_schema()
    oauth_error = (request.args.get("error") or "").strip()
    session_state = dict(session.get(_oauth_session_key(integration_key)) or {})
    received_state = (request.args.get("state") or "").strip()
    signed_state = decode_google_oauth_state(received_state)
    if signed_state and signed_state.get("integration_key") != integration_key:
        signed_state = {}
    store_hash = normalize_store_hash(session_state.get("store_hash") or signed_state.get("store_hash") or _current_store_hash())
    if store_hash:
        _require_store_allowed(store_hash)
    if oauth_error:
        flash(f"{integration_key.upper()} connection failed: {oauth_error}.", "danger")
        return _redirect_to_setup_section(store_hash, "connections")
    expected_state = (session_state.get("state") or "").strip()
    if not ((expected_state and received_state and expected_state == received_state) or signed_state):
        flash("Google OAuth state did not match the current session. Start the connection again.", "danger")
        return _redirect_to_setup_section(store_hash, "connections")
    try:
        result = complete_google_oauth(
            integration_key,
            store_hash=store_hash,
            state=received_state,
            authorization_response=_oauth_callback_authorization_response(integration_key),
        )
        auto_selected = result.get("auto_selected") or {}
        selection_result = result.get("selection_result") or {}
        if (selection_result.get("status") or "").strip().lower() in {"ok", "queued"} and auto_selected:
            flash(
                f"{auto_selected.get('label') or auto_selected.get('id') or ('Search Console' if integration_key == 'gsc' else 'GA4')} is connected. "
                "Data sync is queued and will show progress on this page.",
                "success",
            )
        else:
            suggested = (result.get("suggested_resource") or {}).get("label") or "Select the correct property to finish setup."
            flash(
                f"{'Search Console' if integration_key == 'gsc' else 'GA4'} connected. {suggested}",
                "success",
            )
    except Exception as exc:  # noqa: BLE001
        current_app.logger.exception("Could not complete %s OAuth.", integration_key)
        error_text = str(exc) or f"Could not complete {integration_key.upper()} OAuth."
        if "invalid_grant" in error_text.lower():
            error_text = (
                "Google rejected that OAuth callback as stale or already used. "
                "Start the Google connection again instead of refreshing the callback page."
            )
        flash(error_text, "danger")
    finally:
        session.pop(_oauth_session_key(integration_key), None)
    return _redirect_to_setup_section(store_hash, "connections")


def _select_google_resource_redirect(integration_key: str):
    apply_runtime_schema()
    store_hash = normalize_store_hash(request.form.get("store_hash") or _current_store_hash())
    _require_store_allowed(store_hash)
    selected_resource_id = (request.form.get("selected_resource_id") or "").strip()
    if not selected_resource_id:
        flash("Select a property before saving the Google connection.", "danger")
        return _redirect_to_setup_section(store_hash, "connections")
    try:
        result = select_google_resource(
            store_hash,
            integration_key=integration_key,
            selected_resource_id=selected_resource_id,
        )
    except Exception as exc:  # noqa: BLE001
        current_app.logger.exception("Could not select %s resource.", integration_key)
        flash(str(exc) or f"Could not verify the selected {integration_key.upper()} resource.", "danger")
        return _redirect_to_setup_section(store_hash, "connections")
    if (result.get("status") or "").strip().lower() == "ok":
        selected = result.get("selected") or {}
        sync_result = result.get("sync_result") or {}
        sync_status = (sync_result.get("status") or "").strip().lower()
        if sync_status in {"queued", "running"}:
            message = (
                f"{selected.get('label') or selected.get('id') or integration_key.upper()} was selected. "
                "Data sync is queued and will update this page when the worker finishes."
            )
        elif sync_status == "warning":
            message = (
                f"{selected.get('label') or selected.get('id') or integration_key.upper()} was selected. "
                f"{sync_result.get('reason') or 'The first data sync can be retried later.'}"
            )
        else:
            message = (
                f"{selected.get('label') or selected.get('id') or integration_key.upper()} is ready. "
                f"Synced {int(sync_result.get('row_count') or 0)} rows."
            )
        if _wants_json_response():
            return jsonify(
                {
                    "status": "ok",
                    "message": message,
                    "store_hash": store_hash,
                    "integration_key": integration_key,
                    "selected_resource_id": selected.get("id"),
                    "selected_resource_label": selected.get("label"),
                    "sync_status": sync_status or "unknown",
                    "sync_run_id": sync_result.get("sync_run_id"),
                    "row_count": int(sync_result.get("row_count") or 0),
                }
            )
        flash(message, "success")
    else:
        message = result.get("reason") or f"The selected {integration_key.upper()} resource could not be verified."
        if _wants_json_response():
            return jsonify(
                {
                    "status": "error",
                    "message": message,
                    "store_hash": store_hash,
                    "integration_key": integration_key,
                }
            ), 400
        flash(message, "danger")
    return _redirect_to_setup_section(store_hash, "connections")


def _wants_json_response() -> bool:
    requested_with = (request.headers.get("X-Requested-With") or "").strip().lower()
    accepted = request.accept_mimetypes
    return requested_with == "xmlhttprequest" or accepted.best == "application/json"


def _queue_gate_review_audit_async(
    *,
    store_hash: str,
    request_id: int,
    gate_record_id: int,
    run_id: int | None,
    initiated_by: str,
) -> None:
    if request_id <= 0 or gate_record_id <= 0:
        return

    app = current_app._get_current_object()

    def worker() -> None:
        with app.app_context():
            try:
                audit_result = run_query_gate_agent_review(
                    store_hash=store_hash,
                    run_id=run_id,
                    gate_record_ids=[gate_record_id],
                    limit=1,
                    initiated_by=initiated_by,
                )
                audit_status = (audit_result.get("status") or "").strip().lower()
                stored_reviews = audit_result.get("reviews") or []
                audit_review = stored_reviews[0] if stored_reviews else None
                update_query_gate_review_request_metadata(
                    store_hash=store_hash,
                    request_id=request_id,
                    metadata_updates={
                        "audit_completed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                        "audit_status": audit_status or "pending",
                        "audit_reason": audit_result.get("reason") or "",
                        "audit_verdict": (audit_review or {}).get("verdict"),
                    },
                )
                invalidate_admin_metric_cache(store_hash)
            except Exception:
                app.logger.exception(
                    "Background query gate audit failed for store %s request %s gate %s.",
                    store_hash,
                    request_id,
                    gate_record_id,
                )
                try:
                    update_query_gate_review_request_metadata(
                        store_hash=store_hash,
                        request_id=request_id,
                        metadata_updates={
                            "audit_completed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                            "audit_status": "error",
                            "audit_reason": "Background audit failed.",
                        },
                    )
                except Exception:
                    app.logger.exception(
                        "Could not store the failed audit state for store %s request %s.",
                        store_hash,
                        request_id,
                    )

    threading.Thread(
        target=worker,
        name=f"fulcrum-gate-audit-{request_id}",
        daemon=True,
    ).start()


def _callback_error(message: str, status_code: int = 500, *, details=None):
    payload = {
        "status": "error",
        "message": message,
    }
    if details is not None:
        payload["details"] = details
    return jsonify(payload), status_code


@fulcrum_bp.route("/")
def merchant_home():
    apply_runtime_schema()
    store_hash = normalize_store_hash(_current_store_hash())
    _require_store_allowed(store_hash)
    return _merchant_landing_redirect(store_hash)


@fulcrum_bp.route("/installed")
def install_complete_page():
    apply_runtime_schema()
    store_hash = normalize_store_hash(_current_store_hash())
    _require_store_allowed(store_hash)
    return render_template(
        "fulcrum/install_complete.html",
        store_hash=store_hash,
        setup_url=url_for("fulcrum.setup_page", store_hash=store_hash),
        dashboard_url=url_for("fulcrum.dashboard", store_hash=store_hash),
        privacy_policy_url=Config.FULCRUM_PRIVACY_POLICY_URL,
        support_url=Config.FULCRUM_SUPPORT_URL,
        terms_url=Config.FULCRUM_TERMS_OF_SERVICE_URL,
        name=session.get("name"),
    )


@fulcrum_bp.route("/setup")
def setup_page():
    if not _has_explicit_store_context():
        return render_template(
            "fulcrum/public_home.html",
            store_hash="",
            active_page="setup",
            privacy_policy_url=Config.FULCRUM_PRIVACY_POLICY_URL,
            support_url=Config.FULCRUM_SUPPORT_URL,
            terms_url=Config.FULCRUM_TERMS_OF_SERVICE_URL,
            name=session.get("name"),
        )
    apply_runtime_schema()
    store_hash = normalize_store_hash(_current_store_hash())
    _require_store_allowed(store_hash)
    context = _merchant_base_context(store_hash)
    context["active_page"] = "setup"
    return render_template("fulcrum/setup.html", **context, name=session.get("name"))


@fulcrum_bp.route("/connections")
def connections_page():
    apply_runtime_schema()
    store_hash = normalize_store_hash(_current_store_hash())
    _require_store_allowed(store_hash)
    return _redirect_to_setup_section(store_hash, "connections")


@fulcrum_bp.route("/store-setup")
def store_setup_page():
    apply_runtime_schema()
    store_hash = normalize_store_hash(_current_store_hash())
    _require_store_allowed(store_hash)
    return _redirect_to_setup_section(store_hash, "store-checks")


@fulcrum_bp.route("/results")
def dashboard():
    apply_runtime_schema()
    store_hash = normalize_store_hash(_current_store_hash())
    _require_store_allowed(store_hash)
    context = get_dashboard_context(store_hash, include_admin=False)
    context["callback_urls"] = _callback_urls()
    context["gadgets_configured"] = bool(Config.FULCRUM_GADGETS_API_KEY)
    context["privacy_policy_url"] = Config.FULCRUM_PRIVACY_POLICY_URL
    context["support_url"] = Config.FULCRUM_SUPPORT_URL
    context["terms_url"] = Config.FULCRUM_TERMS_OF_SERVICE_URL
    context["full_results_url"] = url_for("fulcrum.dashboard_full", store_hash=store_hash)
    context["active_page"] = "results"
    return render_template("fulcrum/dashboard.html", **context, name=session.get("name"))


@fulcrum_bp.route("/results/full")
def dashboard_full():
    apply_runtime_schema()
    store_hash = normalize_store_hash(_current_store_hash())
    _require_store_allowed(store_hash)
    context = get_dashboard_context(store_hash, include_admin=False)
    context["callback_urls"] = _callback_urls()
    context["gadgets_configured"] = bool(Config.FULCRUM_GADGETS_API_KEY)
    context["privacy_policy_url"] = Config.FULCRUM_PRIVACY_POLICY_URL
    context["support_url"] = Config.FULCRUM_SUPPORT_URL
    context["terms_url"] = Config.FULCRUM_TERMS_OF_SERVICE_URL
    context["active_page"] = "results"
    return render_template("fulcrum/dashboard_full.html", **context, name=session.get("name"))


@fulcrum_bp.route("/review")
def review_page():
    apply_runtime_schema()
    store_hash = normalize_store_hash(_current_store_hash())
    _require_store_allowed(store_hash)
    context = get_dashboard_context(store_hash, include_admin=False)
    context["callback_urls"] = _callback_urls()
    context["gadgets_configured"] = bool(Config.FULCRUM_GADGETS_API_KEY)
    context["privacy_policy_url"] = Config.FULCRUM_PRIVACY_POLICY_URL
    context["support_url"] = Config.FULCRUM_SUPPORT_URL
    context["terms_url"] = Config.FULCRUM_TERMS_OF_SERVICE_URL
    context["active_page"] = "review"
    return render_template("fulcrum/review.html", **context, name=session.get("name"))


@fulcrum_bp.route("/settings")
def settings_page():
    apply_runtime_schema()
    store_hash = normalize_store_hash(_current_store_hash())
    _require_store_allowed(store_hash)
    return _redirect_to_setup_section(store_hash, "publishing-settings")


@fulcrum_bp.route("/admin")
def admin_dashboard():
    apply_runtime_schema()
    store_hash = normalize_store_hash(_current_store_hash())
    _require_store_allowed(store_hash)
    changed_route_search = (request.args.get("changed_route_search") or "").strip()
    changed_route_sort = (request.args.get("changed_route_sort") or "score_desc").strip() or "score_desc"
    try:
        changed_route_page = max(int(request.args.get("changed_route_page") or 1), 1)
    except ValueError:
        changed_route_page = 1
    try:
        changed_route_page_size = max(int(request.args.get("changed_route_page_size") or 25), 1)
    except ValueError:
        changed_route_page_size = 25
    context = get_dashboard_context(
        store_hash,
        include_admin=True,
        include_quality=False,
        changed_route_search=changed_route_search,
        changed_route_sort=changed_route_sort,
        changed_route_page=changed_route_page,
        changed_route_page_size=changed_route_page_size,
    )
    context["callback_urls"] = _callback_urls()
    context["gadgets_configured"] = bool(Config.FULCRUM_GADGETS_API_KEY)
    return render_template("fulcrum/admin.html", **context, name=session.get("name"))


@fulcrum_bp.route("/admin/quality")
def admin_quality_dashboard():
    apply_runtime_schema()
    store_hash = normalize_store_hash(_current_store_hash())
    _require_store_allowed(store_hash)
    context = get_dashboard_context(store_hash, include_admin=True, include_quality=True)
    context["callback_urls"] = _callback_urls()
    context["gadgets_configured"] = bool(Config.FULCRUM_GADGETS_API_KEY)
    return render_template("fulcrum/admin_quality.html", **context, name=session.get("name"))


@fulcrum_bp.route("/admin/developer")
def admin_developer_dashboard():
    apply_runtime_schema()
    store_hash = normalize_store_hash(_current_store_hash())
    _require_store_allowed(store_hash)
    context = build_setup_context(store_hash)
    context["callback_urls"] = _callback_urls()
    context["gadgets_configured"] = bool(Config.FULCRUM_GADGETS_API_KEY)
    context["privacy_policy_url"] = Config.FULCRUM_PRIVACY_POLICY_URL
    context["support_url"] = Config.FULCRUM_SUPPORT_URL
    context["terms_url"] = Config.FULCRUM_TERMS_OF_SERVICE_URL
    context["review_bucket_requests"] = []
    context["review_bucket_count"] = 0
    context["latest_gate_run_id"] = None
    context.update(_build_marketplace_review_context(context, verify_public_urls=False))
    context["active_page"] = "admin_developer"
    return render_template("fulcrum/admin_developer.html", **context, name=session.get("name"))


@fulcrum_bp.route("/health")
def health():
    store_hash = normalize_store_hash(request.args.get("store_hash") or "")
    health_store_hash = store_hash or normalize_store_hash((Config.FULCRUM_ALLOWED_STORES or [""])[0])
    product_theme_ready = theme_hook_present()
    category_theme_ready = category_theme_hook_present()
    if health_store_hash:
        try:
            snapshot = build_store_readiness_snapshot(health_store_hash)
            theme_checks = snapshot.get("checks", {}).get("theme", {})
            product_theme_ready = bool(theme_checks.get("product_theme_hook_ready") or product_theme_ready)
            category_theme_ready = bool(theme_checks.get("category_theme_hook_ready") or category_theme_ready)
        except Exception:
            current_app.logger.exception("Fulcrum health readiness snapshot failed.")
    payload = {
        "status": "ok",
        "app": "fulcrum",
        "gadgets_configured": bool(Config.FULCRUM_GADGETS_API_KEY),
        "scheduler_enabled": bool(Config.ENABLE_SCHEDULER),
        "embedded_scheduler_enabled": bool(Config.FULCRUM_RUN_EMBEDDED_SCHEDULER),
        "allowed_store_count": len(Config.FULCRUM_ALLOWED_STORES or []),
        "product_theme_hook_ready": product_theme_ready,
        "category_theme_hook_ready": category_theme_ready,
    }
    if store_hash:
        _require_store_allowed(store_hash)
        payload["store_hash"] = store_hash
    return jsonify(payload)


@fulcrum_bp.route("/readiness")
def readiness_status():
    apply_runtime_schema()
    store_hash = normalize_store_hash(request.args.get("store_hash") or _current_store_hash())
    _require_store_allowed(store_hash)
    return jsonify(build_store_readiness_snapshot(store_hash))


@fulcrum_bp.route("/auth")
def auth_callback():
    apply_runtime_schema()
    error = request.args.get("error")
    code = request.args.get("code")
    scope = request.args.get("scope", "")
    context = request.args.get("context", "")
    callback_seen_at = _callback_probe_timestamp()
    callback_store_hash = normalize_store_hash(context)
    if callback_store_hash:
        merge_store_installation_metadata(
            callback_store_hash,
            context=context,
            scope=scope,
            metadata={
                "last_auth_callback_seen_at": callback_seen_at,
                "last_auth_callback_scope": scope or None,
                "last_auth_callback_has_code": bool(code),
                "last_auth_callback_context": context,
                "last_auth_callback_error_param": error or None,
                "last_auth_callback_outcome": "error_param" if error else "started",
            },
        )
    if error:
        return jsonify({"status": "error", "error": error, "details": request.args.to_dict()}), 400

    if not code or not context:
        return jsonify({"status": "error", "message": "Missing code or context."}), 400

    try:
        token_payload = exchange_auth_code(code=code, scope=scope, context=context)
        store_hash = normalize_store_hash(token_payload.get("context") or context)
        _require_store_allowed(store_hash)

        upsert_store_installation(
            store_hash=store_hash,
            context=token_payload.get("context") or context,
            access_token=token_payload.get("access_token"),
            scope=token_payload.get("scope") or scope,
            user_id=str(token_payload.get("user", {}).get("id") or ""),
            owner_email=token_payload.get("user", {}).get("email") or "",
            metadata={
                "user": token_payload.get("user", {}),
                "auth_fallback": False,
                "auth_error_type": None,
                "auth_error_status": None,
                "auth_error_message": None,
                "auth_error_body": None,
                "last_auth_callback_seen_at": callback_seen_at,
                "last_auth_callback_scope": token_payload.get("scope") or scope,
                "last_auth_callback_has_code": True,
                "last_auth_callback_context": token_payload.get("context") or context,
                "last_auth_callback_error_param": None,
                "last_auth_callback_outcome": "success",
                "last_auth_success_at": callback_seen_at,
            },
        )
        sync_bigcommerce_integration(store_hash)

        session["fulcrum_store_hash"] = store_hash
        flash(f"Route Authority installed for store {store_hash}.", "success")
        return _post_install_redirect(store_hash)
    except Exception as exc:  # noqa: BLE001
        current_app.logger.exception("Fulcrum auth callback failed.")
        fallback_store_hash = normalize_store_hash(context)
        fallback_token = _resolve_store_token(fallback_store_hash) if fallback_store_hash else ""
        upstream_status = None
        upstream_body = ""
        response = getattr(exc, "response", None)
        if response is not None:
            upstream_status = getattr(response, "status_code", None)
            try:
                upstream_body = (response.text or "").strip()
            except Exception:  # noqa: BLE001
                upstream_body = ""
        if fallback_store_hash:
            merge_store_installation_metadata(
                fallback_store_hash,
                context=context,
                scope=scope,
                metadata={
                    "last_auth_callback_seen_at": callback_seen_at,
                    "last_auth_callback_scope": scope or None,
                    "last_auth_callback_has_code": True,
                    "last_auth_callback_context": context,
                    "last_auth_callback_error_param": None,
                    "last_auth_callback_outcome": "error",
                    "auth_error_type": type(exc).__name__,
                    "auth_error_status": upstream_status,
                    "auth_error_message": str(exc),
                    "auth_error_body": upstream_body[:500] if upstream_body else None,
                },
            )
        if fallback_store_hash and fallback_token:
            _require_store_allowed(fallback_store_hash)
            upsert_store_installation(
                store_hash=fallback_store_hash,
                context=context,
                access_token=fallback_token,
                scope=scope,
                install_source="oauth_fallback_existing_token",
                metadata={
                    "auth_fallback": True,
                    "auth_error_type": type(exc).__name__,
                    "auth_error_status": upstream_status,
                    "auth_error_message": str(exc),
                    "auth_error_body": upstream_body[:500] if upstream_body else None,
                    "last_auth_callback_seen_at": callback_seen_at,
                    "last_auth_callback_scope": scope or None,
                    "last_auth_callback_has_code": True,
                    "last_auth_callback_context": context,
                    "last_auth_callback_error_param": None,
                    "last_auth_callback_outcome": "fallback_existing_token",
                },
            )
            sync_bigcommerce_integration(fallback_store_hash)
            session["fulcrum_store_hash"] = fallback_store_hash
            flash(
                f"Route Authority opened for store {fallback_store_hash} using the existing stored token because the BigCommerce auth exchange failed.",
                "warning",
            )
            return _post_install_redirect(fallback_store_hash)
        details = {
            "type": type(exc).__name__,
            "auth_callback_url": Config.FULCRUM_AUTH_CALLBACK_URL,
        }
        if hasattr(exc, "response") and getattr(exc, "response", None) is not None:
            response = exc.response
            details["upstream_status"] = getattr(response, "status_code", None)
            try:
                details["upstream_body"] = response.text
            except Exception:  # noqa: BLE001
                pass
        return _callback_error(
            "Route Authority auth callback failed. This is usually a BigCommerce client-secret or callback-URL mismatch.",
            502,
            details=details,
        )


@fulcrum_bp.route("/load")
def load_callback():
    apply_runtime_schema()
    signed_payload = request.args.get("signed_payload_jwt")
    if not signed_payload:
        return redirect(url_for("fulcrum.merchant_home"))

    try:
        payload = decode_signed_payload(signed_payload, Config.FULCRUM_BC_CLIENT_SECRET)
        context = payload.get("context") or payload.get("sub") or payload.get("store_hash") or ""
        store_hash = normalize_store_hash(context)
        _require_store_allowed(store_hash)
        callback_seen_at = _callback_probe_timestamp()

        upsert_store_installation(
            store_hash=store_hash,
            context=context,
            access_token=None,
            scope=payload.get("scope", ""),
            user_id=str(payload.get("user", {}).get("id") or ""),
            owner_email=payload.get("user", {}).get("email") or "",
            install_source="load_callback",
            metadata={
                "payload": payload,
                "last_load_callback_seen_at": callback_seen_at,
                "last_load_callback_scope": payload.get("scope", "") or None,
                "last_load_callback_context": context,
                "last_load_callback_outcome": "success",
            },
        )
        sync_bigcommerce_integration(store_hash)

        session["fulcrum_store_hash"] = store_hash
        return _merchant_landing_redirect(store_hash)
    except ValueError as exc:
        current_app.logger.exception("Fulcrum load callback failed to decode signed payload.")
        return _callback_error(
            "Route Authority load callback could not verify BigCommerce signed_payload_jwt. Check the app client secret in fulcrum.alpha.env against the Developer Portal.",
            401,
            details={
                "type": type(exc).__name__,
                "reason": str(exc),
                "load_callback_url": Config.FULCRUM_LOAD_CALLBACK_URL,
            },
        )
    except Exception as exc:  # noqa: BLE001
        current_app.logger.exception("Fulcrum load callback failed.")
        return _callback_error(
            "Route Authority load callback failed unexpectedly.",
            500,
            details={
                "type": type(exc).__name__,
                "reason": str(exc),
                "load_callback_url": Config.FULCRUM_LOAD_CALLBACK_URL,
            },
        )


@fulcrum_bp.route("/uninstall", methods=["GET", "POST"])
def uninstall_callback():
    apply_runtime_schema()
    store_hash = normalize_store_hash(
        request.values.get("store_hash")
        or request.values.get("context")
        or request.args.get("context")
    )
    cleanup = {}
    if store_hash:
        mark_store_uninstalled(store_hash, metadata={"reason": "uninstall_callback"})
        cleanup = purge_store_data_on_uninstall(store_hash)
        sync_bigcommerce_integration(store_hash)
    return jsonify({"status": "ok", "store_hash": store_hash, "cleanup": cleanup})


@fulcrum_bp.route("/remove-user", methods=["GET", "POST"])
def remove_user_callback():
    store_hash = normalize_store_hash(
        request.values.get("store_hash")
        or request.values.get("context")
        or request.args.get("context")
        or request.args.get("store_hash")
    )
    payload = {"status": "ok"}
    if store_hash:
        _require_store_allowed(store_hash)
        payload["store_hash"] = store_hash
    return jsonify(payload)


@fulcrum_bp.route("/integrations/gsc/start")
def gsc_oauth_start():
    return _oauth_start_redirect("gsc")


@fulcrum_bp.route("/integrations/gsc/callback")
def gsc_oauth_callback():
    return _oauth_callback_redirect("gsc")


@fulcrum_bp.route("/integrations/gsc/select", methods=["POST"])
def gsc_select_resource():
    return _select_google_resource_redirect("gsc")


@fulcrum_bp.route("/integrations/ga4/start")
def ga4_oauth_start():
    return _oauth_start_redirect("ga4")


@fulcrum_bp.route("/integrations/ga4/callback")
def ga4_oauth_callback():
    return _oauth_callback_redirect("ga4")


@fulcrum_bp.route("/integrations/ga4/select", methods=["POST"])
def ga4_select_resource():
    return _select_google_resource_redirect("ga4")


@fulcrum_bp.route("/integrations/<provider>/sync", methods=["POST"])
def integration_sync_retry(provider: str):
    apply_runtime_schema()
    integration_key = (provider or "").strip().lower()
    if integration_key not in {"gsc", "ga4"}:
        abort(404)
    store_hash = normalize_store_hash(request.form.get("store_hash") or request.args.get("store_hash") or _current_store_hash())
    _require_store_allowed(store_hash)
    try:
        result = enqueue_integration_sync(
            store_hash,
            integration_key,
            triggered_by=session.get("email") or "merchant_retry",
        )
    except Exception as exc:  # noqa: BLE001
        current_app.logger.exception("Could not enqueue %s sync.", integration_key)
        message = str(exc) or f"Could not queue {integration_key.upper()} sync."
        if _wants_json_response():
            return jsonify({"status": "error", "message": message, "store_hash": store_hash, "integration_key": integration_key}), 400
        flash(message, "danger")
        return _redirect_to_setup_section(store_hash, "connections")

    if (result.get("status") or "").strip().lower() == "error":
        message = result.get("reason") or f"Could not queue {integration_key.upper()} sync."
        if _wants_json_response():
            return jsonify({"status": "error", "message": message, "store_hash": store_hash, "integration_key": integration_key}), 400
        flash(message, "danger")
        return _redirect_to_setup_section(store_hash, "connections")

    message = f"{'Search Console' if integration_key == 'gsc' else 'GA4'} sync queued."
    if _wants_json_response():
        return jsonify(
            {
                "status": "queued",
                "message": message,
                "store_hash": store_hash,
                "integration_key": integration_key,
                "sync_run_id": result.get("sync_run_id"),
            }
        )
    flash(message, "success")
    return _redirect_to_setup_section(store_hash, "connections")


@fulcrum_bp.route("/setup/theme-verify", methods=["GET", "POST"])
def theme_verify():
    if request.method == "GET":
        store_hash = normalize_store_hash(request.args.get("store_hash") or _current_store_hash())
        return _redirect_to_setup_section(store_hash, "store-checks")
    apply_runtime_schema()
    store_hash = normalize_store_hash(request.form.get("store_hash") or _current_store_hash())
    _require_store_allowed(store_hash)
    result = evaluate_theme_verification(store_hash, persist=True)
    if (result.get("verification_status") or "").strip().lower() == "ready":
        flash("Theme verification passed.", "success")
    else:
        flash(result.get("summary") or "Theme verification found a blocking issue.", "danger")
    refresh_store_readiness(store_hash)
    return _redirect_to_setup_section(store_hash, "store-checks")


@fulcrum_bp.route("/setup/theme-fix-auto", methods=["POST"])
def theme_fix_auto():
    apply_runtime_schema()
    store_hash = normalize_store_hash(request.form.get("store_hash") or _current_store_hash())
    _require_store_allowed(store_hash)
    result = apply_theme_automatic_fix(store_hash)
    if (result.get("status") or "").strip().lower() == "ok":
        flash(result.get("reason") or "Automatic theme fix applied.", "success")
    else:
        flash(result.get("reason") or "No automatic theme fix is available for this store.", "danger")
    refresh_store_readiness(store_hash)
    return _redirect_to_setup_section(store_hash, "store-checks")


@fulcrum_bp.route("/setup/publish-settings", methods=["GET", "POST"])
def save_publish_settings():
    if request.method == "GET":
        store_hash = normalize_store_hash(request.args.get("store_hash") or _current_store_hash())
        return _redirect_to_setup_section(store_hash, "publishing-settings")
    apply_runtime_schema()
    store_hash = normalize_store_hash(request.form.get("store_hash") or _current_store_hash())
    _require_store_allowed(store_hash)
    updated = upsert_store_publish_settings(
        store_hash,
        publishing_enabled=_checkbox_enabled("publishing_enabled"),
        category_publishing_enabled=_checkbox_enabled("category_publishing_enabled"),
        metadata_updates={"updated_by": session.get("email") or "merchant"},
    )
    refresh_store_readiness(store_hash)
    message = (
        "Publishing settings saved. "
        f"Publishing is {'enabled' if updated.get('publishing_enabled') else 'disabled'} and "
        f"category publishing is {'enabled' if updated.get('category_publishing_enabled') else 'disabled'}."
    )
    if _wants_json_response():
        return jsonify(
            {
                "status": "ok",
                "message": message,
                "store_hash": store_hash,
                "publishing_enabled": bool(updated.get("publishing_enabled")),
                "category_publishing_enabled": bool(updated.get("category_publishing_enabled")),
            }
        )
    flash(message, "success")
    return _redirect_to_setup_section(store_hash, "publishing-settings")


@fulcrum_bp.route("/privacy")
def privacy_policy():
    store_hash = normalize_store_hash(request.args.get("store_hash") or _current_store_hash())
    context = {
        "store_hash": store_hash,
        "active_page": "privacy",
        "privacy_policy_url": Config.FULCRUM_PRIVACY_POLICY_URL,
        "support_url": Config.FULCRUM_SUPPORT_URL,
        "terms_url": Config.FULCRUM_TERMS_OF_SERVICE_URL,
    }
    return render_template("fulcrum/privacy.html", **context, name=session.get("name"))


@fulcrum_bp.route("/support")
def support_page():
    store_hash = normalize_store_hash(request.args.get("store_hash") or _current_store_hash())
    context = {
        "store_hash": store_hash,
        "active_page": "support",
        "privacy_policy_url": Config.FULCRUM_PRIVACY_POLICY_URL,
        "support_url": Config.FULCRUM_SUPPORT_URL,
        "terms_url": Config.FULCRUM_TERMS_OF_SERVICE_URL,
    }
    return render_template("fulcrum/support.html", **context, name=session.get("name"))


@fulcrum_bp.route("/guide")
def guide_page():
    store_hash = normalize_store_hash(request.args.get("store_hash") or _current_store_hash())
    context = {
        "store_hash": store_hash,
        "active_page": "guide",
        "privacy_policy_url": Config.FULCRUM_PRIVACY_POLICY_URL,
        "support_url": Config.FULCRUM_SUPPORT_URL,
        "terms_url": Config.FULCRUM_TERMS_OF_SERVICE_URL,
    }
    return render_template("fulcrum/guide.html", **context, name=session.get("name"))


@fulcrum_bp.route("/terms")
def terms_page():
    store_hash = normalize_store_hash(request.args.get("store_hash") or _current_store_hash())
    context = {
        "store_hash": store_hash,
        "active_page": "terms",
        "privacy_policy_url": Config.FULCRUM_PRIVACY_POLICY_URL,
        "support_url": Config.FULCRUM_SUPPORT_URL,
        "terms_url": Config.FULCRUM_TERMS_OF_SERVICE_URL,
    }
    return render_template("fulcrum/terms.html", **context, name=session.get("name"))


@fulcrum_bp.route("/runs/generate", methods=["POST"])
def generate_run():
    apply_runtime_schema()
    store_hash = normalize_store_hash(request.form.get("store_hash") or _current_store_hash())
    _require_store_allowed(store_hash)
    cluster = (request.form.get("cluster") or "").strip() or None
    result = queue_candidate_run(
        store_hash=store_hash,
        initiated_by=session.get("email") or "fulcrum",
        cluster=cluster,
        max_links_per_product=Config.FULCRUM_AUTO_PUBLISH_MAX_LINKS_PER_SOURCE,
        min_hit_count=3,
        limit_total=300,
        run_source="manual",
    )
    if result.get("duplicate"):
        flash(
            f"Run {result['run_id']} is already {result.get('status')}. Route Authority is still working on the latest refresh.",
            "success",
        )
    elif result.get("queued"):
        flash(
            f"Queued run {result['run_id']} for background generation. The results page will update when it finishes.",
            "success",
        )
    else:
        flash(result.get("reason") or "Route Authority could not queue the generation worker.", "danger")
    return _redirect_to_requested_view(store_hash)


@fulcrum_bp.route("/catalog/sync", methods=["POST"])
def sync_catalog():
    apply_runtime_schema()
    store_hash = normalize_store_hash(request.form.get("store_hash") or _current_store_hash())
    _require_store_allowed(store_hash)
    result = sync_store_catalog_profiles(
        store_hash=store_hash,
        initiated_by=session.get("email") or "fulcrum",
    )
    flash(
        "Catalog sync complete: "
        f"{result['synced_products']} products, "
        f"{result['synced_categories']} categories, "
        f"{result['mapped_option_names']} option-name mappings, "
        f"{result['mapped_option_values']} option-value mappings, "
        f"{result['pending_option_name_mappings']} option names pending review, "
        f"{result['pending_option_value_mappings']} option values pending review.",
        "success",
    )
    return _redirect_to_requested_view(store_hash)


@fulcrum_bp.route("/admin/review-edge-cases", methods=["POST"])
def review_edge_cases_bulk():
    apply_runtime_schema()
    store_hash = normalize_store_hash(request.form.get("store_hash") or _current_store_hash())
    _require_store_allowed(store_hash)
    result = review_all_edge_cases(
        store_hash=store_hash,
        initiated_by=session.get("email") or "fulcrum",
    )
    status = (result.get("status") or "").strip().lower()
    if status == "ok":
        summary = result.get("summary") or {}
        flash(
            (
                f"Reviewed {result.get('request_count') or 0} edge case(s). "
                f"Stored {result.get('stored_count') or 0} verdict(s). "
                f"Incorrect: {summary.get('incorrect') or 0}, unclear: {summary.get('unclear') or 0}."
            ),
            "success",
        )
    elif status == "skipped":
        flash(result.get("reason") or "No edge cases were ready for review.", "success")
    else:
        flash(result.get("reason") or "Edge-case review did not complete cleanly.", "danger")
    return _redirect_to_requested_view(store_hash)


@fulcrum_bp.route("/publish/all", methods=["POST"])
def publish_all_results():
    apply_runtime_schema()
    store_hash = normalize_store_hash(request.form.get("store_hash") or _current_store_hash())
    _require_store_allowed(store_hash)

    if not theme_hook_present():
        flash("Theme hook check failed. The configured product template does not expose internal_links_html.", "danger")
        return _redirect_to_requested_view(store_hash)

    result = publish_all_current_results(
        store_hash=store_hash,
        initiated_by=session.get("email") or "fulcrum",
    )
    unresolved_approved_count = int(result.get("unresolved_approved_source_count") or 0)
    flash(
        (
            f"Publish All Results approved {result.get('approved_count') or 0} pending result(s), "
            f"published {result.get('published_count') or 0} approved source block(s), "
            f"and skipped {result.get('blocked_source_count') or 0} source(s) that are in the review queue."
            + (
                f" {unresolved_approved_count} approved source(s) still are not live and need attention."
                if unresolved_approved_count
                else ""
            )
        ),
        "danger" if unresolved_approved_count else "success",
    )
    return _redirect_to_requested_view(store_hash)


@fulcrum_bp.route("/reviews/bulk", methods=["POST"])
def review_bulk():
    apply_runtime_schema()
    store_hash = normalize_store_hash(request.form.get("store_hash") or _current_store_hash())
    _require_store_allowed(store_hash)
    action = request.form.get("action", "approve").strip().lower()
    candidate_ids = [int(value) for value in request.form.getlist("candidate_ids") if value.strip()]
    review_status = "approved" if action == "approve" else "rejected"
    review_note = request.form.get("review_note") or None
    if action != "approve" and not review_note:
        review_note = "Agent review requested from dashboard."
    updated = review_candidates(
        candidate_ids=candidate_ids,
        review_status=review_status,
        reviewed_by=session.get("email") or "fulcrum",
        note=review_note,
    )
    if action == "approve":
        flash(f"Approved {updated} result(s).", "success")
    else:
        flash(f"Requested admin and agent review for {updated} result(s).", "success")
    return _redirect_to_requested_view(store_hash)


@fulcrum_bp.route("/mappings/review", methods=["POST"])
def review_mappings():
    apply_runtime_schema()
    store_hash = normalize_store_hash(request.form.get("store_hash") or _current_store_hash())
    _require_store_allowed(store_hash)
    action = (request.form.get("action") or "approve").strip().lower()
    review_status = "approved" if action == "approve" else "ignored"
    result = review_mapping_rows(
        store_hash=store_hash,
        mapping_refs=request.form.getlist("mapping_refs"),
        review_status=review_status,
        reviewed_by=session.get("email") or "fulcrum",
        note=request.form.get("review_note") or None,
    )
    flash(
        f"{review_status.title()} {result['updated_option_names'] + result['updated_option_values']} mapping(s).",
        "success",
    )
    return _redirect_to_requested_view(store_hash)


@fulcrum_bp.route("/gate/override", methods=["POST"])
def set_gate_override():
    apply_runtime_schema()
    store_hash = normalize_store_hash(request.form.get("store_hash") or _current_store_hash())
    _require_store_allowed(store_hash)
    normalized_query_key = request.form.get("normalized_query_key") or ""
    source_url = request.form.get("source_url") or ""
    source_entity_type = request.form.get("source_entity_type") or "product"
    source_entity_id = request.form.get("source_entity_id") or None
    target_entity_type = request.form.get("target_entity_type") or "product"
    target_entity_id = request.form.get("target_entity_id") or "0"
    target_name = request.form.get("target_name") or ""
    target_url = request.form.get("target_url") or ""
    try:
        parsed_target_entity_id = int(target_entity_id or 0)
    except ValueError:
        parsed_target_entity_id = 0

    if not normalized_query_key or not source_url or not parsed_target_entity_id:
        flash("Could not save the Route Authority target override because required routing details were missing.", "danger")
        return redirect(url_for("fulcrum.dashboard", store_hash=store_hash))

    set_query_target_override(
        store_hash=store_hash,
        normalized_query_key=normalized_query_key,
        source_url=source_url,
        source_entity_type=source_entity_type,
        source_entity_id=int(source_entity_id) if source_entity_id not in {None, ""} else None,
        target_entity_type=target_entity_type,
        target_entity_id=parsed_target_entity_id,
        created_by=session.get("email") or "fulcrum",
        metadata={
            "target_name": target_name,
            "target_url": target_url,
        },
    )
    flash(
        f"Route Authority will now prefer {target_name or target_url or 'the selected target'} for this query family. Run Generate Candidates again to refresh candidate rows.",
        "success",
    )
    return _redirect_to_requested_view(store_hash)


@fulcrum_bp.route("/gate/request-review", methods=["POST"])
def request_gate_review():
    apply_runtime_schema()
    store_hash = normalize_store_hash(request.form.get("store_hash") or _current_store_hash())
    _require_store_allowed(store_hash)
    wants_json = _wants_json_response()
    try:
        gate_record_id = int(request.form.get("gate_record_id") or 0)
    except ValueError:
        gate_record_id = 0

    if gate_record_id <= 0:
        message = "Could not send this result to the review bucket because the result id was missing."
        if wants_json:
            return jsonify({"status": "error", "message": message}), 400
        flash(message, "danger")
        return redirect(url_for("fulcrum.dashboard", store_hash=store_hash))

    target_entity_id_raw = request.form.get("target_entity_id") or None
    try:
        target_entity_id = int(target_entity_id_raw) if target_entity_id_raw not in {None, ""} else None
    except ValueError:
        target_entity_id = None

    initiated_by = session.get("email") or "fulcrum"

    request_row = request_query_gate_review(
        store_hash=store_hash,
        gate_record_id=gate_record_id,
        target_entity_type=request.form.get("target_entity_type") or None,
        target_entity_id=target_entity_id,
        target_name=request.form.get("target_name") or None,
        target_url=request.form.get("target_url") or None,
        reason_summary=request.form.get("reason_summary") or None,
        requested_by=initiated_by,
        note=request.form.get("review_note") or None,
    )
    if not request_row:
        message = "Could not send this result to the review bucket."
        if wants_json:
            return jsonify({"status": "error", "message": message}), 500
        flash(message, "danger")
        return redirect(url_for("fulcrum.dashboard", store_hash=store_hash))

    pause_result = {
        "live_block_paused": False,
        "review_reset_count": 0,
        "publication_count": 0,
    }
    pause_error = None
    try:
        pause_result = pause_source_for_review(
            store_hash=store_hash,
            source_entity_id=request_row.get("source_entity_id"),
            source_entity_type=request_row.get("source_entity_type") or "product",
            reviewed_by=initiated_by,
            note="Paused for review from the results table.",
        )
    except Exception as exc:
        pause_error = str(exc) or exc.__class__.__name__
        current_app.logger.exception(
            "Could not pause the current live block for store %s gate %s during review queueing.",
            store_hash,
            gate_record_id,
        )
    audit_requested_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    request_row = update_query_gate_review_request_metadata(
        store_hash=store_hash,
        request_id=int(request_row.get("request_id") or 0),
        metadata_updates={
            "live_block_paused": bool(pause_result.get("live_block_paused")),
            "review_reset_count": int(pause_result.get("review_reset_count") or 0),
            "publication_count": int(pause_result.get("publication_count") or 0),
            "pause_requested_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "pause_error": pause_error,
            "audit_requested_at": audit_requested_at,
            "audit_status": "queued",
        },
    ) or request_row

    _queue_gate_review_audit_async(
        store_hash=store_hash,
        request_id=int(request_row.get("request_id") or 0),
        gate_record_id=gate_record_id,
        run_id=int(request_row.get("run_id") or 0) or None,
        initiated_by=initiated_by,
    )

    if pause_result.get("live_block_paused"):
        message = (
            f"Queued {request_row.get('representative_query') or 'this result'} for re-analysis "
            f"and paused {pause_result.get('publication_count') or 0} live block(s)."
        )
    elif pause_error:
        message = (
            f"Queued {request_row.get('representative_query') or 'this result'} for re-analysis. "
            "Review is active, but the current live block could not be paused automatically."
        )
    else:
        message = (
            f"Queued {request_row.get('representative_query') or 'this result'} for re-analysis. "
            "It was not live on the site."
        )
    message = f"{message} Audit is running in the background."
    invalidate_admin_metric_cache(store_hash)
    if wants_json:
        review_metadata = dict(request_row.get("metadata") or {})
        return jsonify(
            {
                "status": "ok",
                "message": message,
                "request_id": int(request_row.get("request_id") or 0),
                "gate_record_id": gate_record_id,
                "review_bucket_count": count_query_gate_review_requests(store_hash, request_status="requested"),
                "review_bucket_entry": {
                    "request_id": int(request_row.get("request_id") or 0),
                    "representative_query": request_row.get("representative_query") or request_row.get("source_name") or request_row.get("source_url") or "",
                    "source_name": request_row.get("source_name") or "",
                    "source_url": request_row.get("source_url") or "",
                    "target_name": request_row.get("target_name") or "",
                    "target_url": request_row.get("target_url") or "",
                    "review_status_label": (
                        "Agent diagnosis ready for support"
                        if review_metadata.get("audit_status") == "ok"
                        else "Agent diagnosis pending"
                    ),
                    "applied_status_label": (
                        "Live block paused for review"
                        if review_metadata.get("live_block_paused")
                        else "Not live yet"
                    ),
                    "requested_at_relative": "just now",
                },
            }
        )
    flash(message, "success")
    return _redirect_to_requested_view(store_hash)


@fulcrum_bp.route("/gate/review-session/submit", methods=["POST"])
def submit_gate_review_session():
    apply_runtime_schema()
    payload = request.get_json(silent=True) or {}
    store_hash = normalize_store_hash(
        payload.get("store_hash") or request.form.get("store_hash") or _current_store_hash()
    )
    _require_store_allowed(store_hash)

    def _payload_id_list(key: str) -> list[int]:
        raw_value = payload.get(key)
        if isinstance(raw_value, list):
            return raw_value
        return []

    run_id_raw = payload.get("run_id")
    try:
        run_id = int(run_id_raw) if run_id_raw not in {None, ""} else None
    except (TypeError, ValueError):
        run_id = None

    submission = submit_query_gate_review_session(
        store_hash,
        run_id=run_id,
        submitted_by=session.get("email") or session.get("name") or "fulcrum",
        all_gate_record_ids=_payload_id_list("all_gate_record_ids"),
        cleared_gate_record_ids=_payload_id_list("cleared_gate_record_ids"),
        review_bucket_gate_record_ids=_payload_id_list("review_bucket_gate_record_ids"),
        metadata={
            "client_submitted_at": payload.get("client_submitted_at"),
            "page_length": payload.get("page_length"),
            "results_left_in_view": payload.get("results_left_in_view"),
        },
    )
    return jsonify(
        {
            "status": "ok",
            "submission_id": int(submission.get("submission_id") or 0),
            "cleared_count": int(submission.get("cleared_count") or 0),
            "review_bucket_count": int(submission.get("review_bucket_count") or 0),
            "remaining_count": int(submission.get("remaining_count") or 0),
            "total_result_count": int(submission.get("total_result_count") or 0),
            "message": (
                f"Saved review batch #{int(submission.get('submission_id') or 0)}. "
                f"Cleared {int(submission.get('cleared_count') or 0)}, "
                f"sent {int(submission.get('review_bucket_count') or 0)} to review, "
                f"left {int(submission.get('remaining_count') or 0)} untouched."
            ),
        }
    )


@fulcrum_bp.route("/gate/review-request/resolve", methods=["POST"])
def resolve_gate_review_request():
    apply_runtime_schema()
    store_hash = normalize_store_hash(request.form.get("store_hash") or _current_store_hash())
    _require_store_allowed(store_hash)
    try:
        request_id = int(request.form.get("request_id") or 0)
    except ValueError:
        request_id = 0
    restore_live = (request.form.get("restore_live") or "").strip().lower() in {"1", "true", "yes", "on"}
    if request_id <= 0:
        flash("Could not resolve this review request because the request id was missing.", "danger")
        return _redirect_to_requested_view(store_hash)

    request_row = get_query_gate_review_request_by_id(store_hash, request_id)
    if not request_row:
        flash("That review request could not be found anymore.", "danger")
        return _redirect_to_requested_view(store_hash)

    metadata_updates = {"resolved_from": "admin_dashboard"}
    restored = None
    if restore_live:
        live_approval_attempted_at = datetime.now().astimezone().isoformat()
        run_id = int(request_row.get("run_id") or 0) or None
        gate_record_id = int(request_row.get("gate_record_id") or 0)
        review_run_ids = {run_id} if run_id else None
        gate_review = (
            _gate_review_map_for_ids(
                store_hash,
                {gate_record_id},
                run_ids=review_run_ids,
            ).get(gate_record_id)
            if gate_record_id > 0
            else None
        )
        gate_row = (
            _query_gate_record_map_for_ids(
                store_hash,
                {gate_record_id},
                run_ids=review_run_ids,
                fresh_suggestions=True,
            ).get(gate_record_id)
            if gate_record_id > 0
            else None
        ) or {}
        (
            restored_target_entity_type,
            restored_target_entity_id,
            restored_target_url,
            recommended_action,
        ) = _resolve_review_restore_target(
            request_row,
            gate_review=gate_review,
            gate_row=gate_row,
        )
        if recommended_action:
            metadata_updates["restored_following_audit"] = recommended_action

        if restored_target_entity_type and (restored_target_entity_id or restored_target_url):
            try:
                restored = restore_source_after_review(
                    store_hash=store_hash,
                    source_entity_id=request_row.get("source_entity_id"),
                    source_entity_type=request_row.get("source_entity_type") or "product",
                    target_entity_type=restored_target_entity_type,
                    target_entity_id=restored_target_entity_id,
                    target_url=restored_target_url,
                    reviewed_by=session.get("email") or "fulcrum",
                    note="Restored from the admin support queue.",
                )
            except Exception as exc:
                error_response = getattr(exc, "response", None)
                error_request = getattr(exc, "request", None)
                error_status = getattr(error_response, "status_code", None)
                error_url = getattr(error_request, "url", None) or getattr(error_response, "url", None)
                current_app.logger.exception(
                    "Could not apply live fix for review request %s in store %s.",
                    request_id,
                    store_hash,
                )
                update_query_gate_review_request_metadata(
                    store_hash=store_hash,
                    request_id=request_id,
                    metadata_updates={
                        "restored_following_audit": recommended_action,
                        "live_approval_attempted_at": live_approval_attempted_at,
                        "live_approval_error": str(exc) or exc.__class__.__name__,
                        "live_approval_error_status": int(error_status) if error_status else None,
                        "live_approval_error_url": error_url,
                    },
                )
                invalidate_admin_metric_cache(store_hash)
                if error_status:
                    flash(
                        f"Could not apply the live fix for review request #{request_id}. BigCommerce returned HTTP {error_status}. The request is still waiting in support.",
                        "danger",
                    )
                else:
                    flash(
                        f"Could not apply the live fix for review request #{request_id}. The request is still waiting in support.",
                        "danger",
                    )
                return _redirect_to_requested_view(store_hash)
            metadata_updates.update(
                {
                    "live_block_restored": bool((restored or {}).get("live_block_restored")),
                    "restore_publication_count": int((restored or {}).get("publication_count") or 0),
                    "restore_approved_candidate_count": int((restored or {}).get("approved_candidate_count") or 0),
                    "live_approval_error": None,
                    "live_approval_error_status": None,
                    "live_approval_error_url": None,
                }
            )
        else:
            metadata_updates.setdefault("live_block_restored", False)
            metadata_updates.setdefault("restore_publication_count", 0)
            metadata_updates.setdefault("restore_approved_candidate_count", 0)
            metadata_updates.setdefault("live_approval_error", None)
            metadata_updates.setdefault("live_approval_error_status", None)
            metadata_updates.setdefault("live_approval_error_url", None)

        metadata_updates["live_approval_completed"] = True
        metadata_updates["live_approval_completed_at"] = live_approval_attempted_at
    restore_succeeded = bool((restored or {}).get("live_block_restored"))
    resolution_note = (request.form.get("resolution_note") or "").strip()
    if not resolution_note:
        if restore_live:
            if restore_succeeded:
                resolution_note = "Support investigation applied the fix and restored the live block."
            else:
                resolution_note = "Investigation completed; no live block was restored."
        else:
            resolution_note = "Investigation completed."

    resolve_query_gate_review_request(
        store_hash=store_hash,
        request_id=request_id,
        resolved_by=session.get("email") or "fulcrum",
        resolution_note=resolution_note,
        metadata_updates=metadata_updates,
    )
    invalidate_admin_metric_cache(store_hash)
    if restore_live:
        restored_count = int((restored or {}).get("publication_count") or 0)
        if restored_count > 0:
            flash(
                f"Applied support fix for review request #{request_id} and restored {restored_count} live block(s).",
                "success",
            )
        else:
            flash(
                f"Investigation recorded for review request #{request_id}. The agent kept the current page, so no live block was restored.",
                "success",
            )
    else:
        flash(f"Investigation recorded for review request #{request_id}.", "success")
    return _redirect_to_requested_view(store_hash)


@fulcrum_bp.route("/logic/regression", methods=["POST"])
def run_logic_regression_route():
    apply_runtime_schema()
    store_hash = normalize_store_hash(request.form.get("store_hash") or _current_store_hash())
    _require_store_allowed(store_hash)
    result = run_logic_regression(store_hash=store_hash)
    changelog_result = record_regression_against_logic_changelog(result)
    if result.get("status") == "ok":
        flash(
            (
                f"Full regression passed: {result.get('passed_count') or 0}/{result.get('case_count') or 0} cases. "
                f"Recorded against {changelog_result.get('updated_count') or 0} changelog note(s)."
            ),
            "success",
        )
    else:
        flash(
            (
                f"Full regression found {result.get('failed_count') or 0} failing case(s). "
                f"Recorded against {changelog_result.get('updated_count') or 0} changelog note(s)."
            ),
            "danger",
        )
    return _redirect_to_requested_view(store_hash)


@fulcrum_bp.route("/logic/regression/change", methods=["POST"])
def run_logic_change_regression_route():
    apply_runtime_schema()
    store_hash = normalize_store_hash(request.form.get("store_hash") or _current_store_hash())
    _require_store_allowed(store_hash)
    change_id = (request.form.get("change_id") or "").strip()
    if not change_id:
        flash("Could not verify this change because the logic change id was missing.", "danger")
        return redirect(url_for("fulcrum.dashboard", store_hash=store_hash))

    case_ids = resolve_logic_change_case_ids(change_id)
    if not case_ids:
        flash(
            f"Could not verify {change_id} because no regression cases are mapped to its affected queries yet.",
            "danger",
        )
        return redirect(url_for("fulcrum.dashboard", store_hash=store_hash))

    result = run_logic_regression(store_hash=store_hash, case_ids=case_ids)
    changelog_result = record_regression_against_logic_changelog(result, change_ids=[change_id])
    latest_status = "passed" if result.get("status") == "ok" else "failed"
    if result.get("status") == "ok":
        flash(
            (
                f"Verified {change_id}: {latest_status} "
                f"({result.get('passed_count') or 0}/{result.get('case_count') or 0} cases)."
            ),
            "success",
        )
    else:
        flash(
            (
                f"Verified {change_id}: {latest_status} "
                f"({result.get('failed_count') or 0} failing case(s))."
            ),
            "danger",
        )
    if changelog_result.get("status") != "ok":
        flash(
            f"The regression ran, but the changelog could not be updated cleanly for {change_id}.",
            "danger",
        )
    return _redirect_to_requested_view(store_hash)


@fulcrum_bp.route("/gate/agent-review", methods=["POST"])
def run_gate_agent_review():
    apply_runtime_schema()
    store_hash = normalize_store_hash(request.form.get("store_hash") or _current_store_hash())
    _require_store_allowed(store_hash)
    disposition = (request.form.get("disposition") or "").strip().lower() or None
    if disposition == "all":
        disposition = None
    limit = request.form.get("limit") or "40"
    try:
        parsed_limit = max(int(limit), 1)
    except ValueError:
        parsed_limit = 40
    changed_only = (request.form.get("changed_only") or "").strip().lower() in {"1", "true", "yes", "on"}

    if changed_only:
        result = run_changed_route_agent_review(
            store_hash=store_hash,
            limit=parsed_limit,
            initiated_by=session.get("email") or "fulcrum",
        )
    else:
        result = run_query_gate_agent_review(
            store_hash=store_hash,
            disposition=disposition,
            limit=parsed_limit,
            initiated_by=session.get("email") or "fulcrum",
        )
    status = (result.get("status") or "").strip().lower()
    if status == "ok":
        summary = result.get("changed_route_review_summary") if changed_only else result.get("summary")
        summary = summary or {}
        if changed_only:
            flash(
                (
                    f"AI audited {result.get('reviewed_count') or 0} changed route(s) and stored "
                    f"{result.get('stored_count') or 0} verdict(s). Incorrect: {summary.get('incorrect') or 0}, "
                    f"Unclear: {summary.get('unclear') or 0}."
                ),
                "success",
            )
        else:
            flash(
                (
                    f"AI reviewed {result.get('reviewed_count') or 0} query families and stored "
                    f"{result.get('stored_count') or 0} verdicts. Incorrect: {summary.get('incorrect') or 0}, "
                    f"Unclear: {summary.get('unclear') or 0}."
                ),
                "success",
            )
    elif status == "skipped":
        flash(result.get("reason") or "AI review was skipped.", "danger")
    else:
        flash(result.get("reason") or "AI review failed.", "danger")
    invalidate_admin_metric_cache(store_hash)
    return _redirect_to_requested_view(store_hash)


@fulcrum_bp.route("/gate/agent-review/one", methods=["POST"])
def run_single_gate_agent_review():
    apply_runtime_schema()
    store_hash = normalize_store_hash(request.form.get("store_hash") or _current_store_hash())
    _require_store_allowed(store_hash)
    try:
        gate_record_id = int(request.form.get("gate_record_id") or 0)
    except ValueError:
        gate_record_id = 0
    try:
        run_id = int(request.form.get("run_id") or 0) or None
    except ValueError:
        run_id = None
    if gate_record_id <= 0:
        flash("Could not audit this route because the gate id was missing.", "danger")
        return _redirect_to_requested_view(store_hash)

    result = run_query_gate_agent_review(
        store_hash=store_hash,
        run_id=run_id,
        gate_record_ids=[gate_record_id],
        limit=1,
        initiated_by=session.get("email") or "fulcrum",
    )
    invalidate_admin_metric_cache(store_hash)
    if (result.get("status") or "").strip().lower() == "ok":
        stored_reviews = result.get("reviews") or []
        review = stored_reviews[0] if stored_reviews else None
        if review:
            flash(
                (
                    f"Audited Gate #{gate_record_id}: {review.get('verdict') or 'unclear'} - "
                    f"{review.get('rationale') or 'No extra rationale returned.'}"
                ),
                "success",
            )
        else:
            flash(f"Audited Gate #{gate_record_id}. No structured review was stored.", "success")
    else:
        flash(result.get("reason") or f"AI audit failed for Gate #{gate_record_id}.", "danger")
    return _redirect_to_requested_view(store_hash)


@fulcrum_bp.route("/publish", methods=["POST"])
def publish():
    apply_runtime_schema()
    store_hash = normalize_store_hash(request.form.get("store_hash") or _current_store_hash())
    _require_store_allowed(store_hash)

    source_entity_ids = [
        int(value)
        for value in (request.form.getlist("source_entity_ids") or request.form.getlist("source_product_ids"))
        if value.strip()
    ]
    if not source_entity_ids:
        raw_source_entity_id = (request.form.get("source_entity_id") or "").strip()
        try:
            source_entity_id = int(raw_source_entity_id) if raw_source_entity_id else None
        except ValueError:
            source_entity_id = None
        source_entity_ids = resolve_publish_source_entity_ids(
            store_hash,
            source_entity_type=(request.form.get("source_entity_type") or "").strip().lower() or None,
            source_entity_id=source_entity_id,
            source_url=request.form.get("source_url"),
        )
    if not source_entity_ids:
        flash("Could not resolve a publishable source from this row.", "danger")
        return _redirect_to_requested_view(store_hash)
    wants_category_publish = any(value < 0 for value in source_entity_ids)

    if not theme_hook_present():
        flash("Theme hook check failed. The configured product template does not expose internal_links_html.", "danger")
        return _redirect_to_requested_view(store_hash)
    if wants_category_publish and not category_publishing_enabled_for_store(store_hash):
        flash("Category publishing is still feature-flagged for beta verification only.", "danger")
        return _redirect_to_requested_view(store_hash)
    if wants_category_publish and not category_theme_hook_present():
        flash("Category theme hook check failed. The category template does not expose Route Authority metafields yet.", "danger")
        return _redirect_to_requested_view(store_hash)

    publications = publish_approved_entities(
        store_hash=store_hash,
        source_entity_ids=source_entity_ids or None,
    )
    flash(f"Published {len(publications)} Route Authority block(s).", "success")
    return _redirect_to_requested_view(store_hash)


@fulcrum_bp.route("/unpublish", methods=["POST"])
def unpublish():
    apply_runtime_schema()
    store_hash = normalize_store_hash(request.form.get("store_hash") or _current_store_hash())
    _require_store_allowed(store_hash)
    source_entity_ids = [
        int(value)
        for value in (request.form.getlist("source_entity_ids") or request.form.getlist("source_product_ids"))
        if value.strip()
    ]
    results = unpublish_entities(store_hash=store_hash, source_entity_ids=source_entity_ids)
    flash(f"Unpublished {len(results)} Route Authority block(s).", "success")
    return redirect(url_for("fulcrum.dashboard", store_hash=store_hash))


@fulcrum_bp.route("/products/<int:source_product_id>/preview")
def preview(source_product_id: int):
    apply_runtime_schema()
    store_hash = normalize_store_hash(request.args.get("store_hash") or _current_store_hash())
    _require_store_allowed(store_hash)
    source_entity_type = (request.args.get("source_entity_type") or "product").strip().lower()
    payload = preview_product_html(
        store_hash=store_hash,
        source_product_id=source_product_id,
        source_entity_type=source_entity_type,
    )
    return render_template("fulcrum/preview.html", **payload, store_hash=store_hash, name=session.get("name"))


@fulcrum_bp.route("/preview/<source_entity_type>/<int:source_entity_id>")
def preview_entity(source_entity_type: str, source_entity_id: int):
    apply_runtime_schema()
    store_hash = normalize_store_hash(request.args.get("store_hash") or _current_store_hash())
    _require_store_allowed(store_hash)
    payload = preview_product_html(
        store_hash=store_hash,
        source_product_id=source_entity_id,
        source_entity_type=(source_entity_type or "product").strip().lower(),
    )
    return render_template("fulcrum/preview.html", **payload, store_hash=store_hash, name=session.get("name"))


@fulcrum_bp.route("/api/internal-links/runs/generate", methods=["POST"])
def api_generate_run():
    apply_runtime_schema()
    _require_internal_api_auth()
    data = request.get_json(silent=True) or {}
    store_hash = normalize_store_hash(data.get("store_hash") or _current_store_hash())
    _require_store_allowed(store_hash)
    async_requested = str(data.get("async", "false")).strip().lower() in {"1", "true", "yes", "on"}
    if async_requested:
        result = queue_candidate_run(
            store_hash=store_hash,
            initiated_by=data.get("initiated_by") or session.get("email") or "fulcrum-api",
            cluster=data.get("cluster"),
            max_links_per_product=int(data.get("max_links_per_product", Config.FULCRUM_AUTO_PUBLISH_MAX_LINKS_PER_SOURCE)),
            min_hit_count=int(data.get("min_hit_count", 3)),
            limit_total=int(data.get("limit_total", 300)),
            run_source="api",
        )
        http_status = 202 if result.get("queued") or result.get("duplicate") else 500
        return jsonify(result), http_status

    result = generate_candidate_run(
        store_hash=store_hash,
        initiated_by=data.get("initiated_by") or session.get("email") or "fulcrum-api",
        cluster=data.get("cluster"),
        max_links_per_product=int(data.get("max_links_per_product", Config.FULCRUM_AUTO_PUBLISH_MAX_LINKS_PER_SOURCE)),
        min_hit_count=int(data.get("min_hit_count", 3)),
        limit_total=int(data.get("limit_total", 300)),
    )
    return jsonify(result)


@fulcrum_bp.route("/api/internal-links/products/<int:source_product_id>/preview")
def api_preview(source_product_id: int):
    apply_runtime_schema()
    _require_internal_api_auth()
    store_hash = normalize_store_hash(request.args.get("store_hash") or _current_store_hash())
    _require_store_allowed(store_hash)
    source_entity_type = (request.args.get("source_entity_type") or "product").strip().lower()
    return jsonify(preview_product_html(store_hash, source_product_id, source_entity_type=source_entity_type))


@fulcrum_bp.route("/api/internal-links/preview/<source_entity_type>/<int:source_entity_id>")
def api_preview_entity(source_entity_type: str, source_entity_id: int):
    apply_runtime_schema()
    _require_internal_api_auth()
    store_hash = normalize_store_hash(request.args.get("store_hash") or _current_store_hash())
    _require_store_allowed(store_hash)
    return jsonify(
        preview_product_html(
            store_hash,
            source_entity_id,
            source_entity_type=(source_entity_type or "product").strip().lower(),
        )
    )


@fulcrum_bp.route("/api/internal-links/catalog/sync", methods=["POST"])
def api_sync_catalog():
    apply_runtime_schema()
    _require_internal_api_auth()
    data = request.get_json(silent=True) or {}
    store_hash = normalize_store_hash(data.get("store_hash") or _current_store_hash())
    _require_store_allowed(store_hash)
    result = sync_store_catalog_profiles(
        store_hash=store_hash,
        initiated_by=data.get("initiated_by") or session.get("email") or "fulcrum-api",
    )
    return jsonify(result)


@fulcrum_bp.route("/api/internal-links/reviews/bulk", methods=["POST"])
def api_review_bulk():
    apply_runtime_schema()
    _require_internal_api_auth()
    data = request.get_json(silent=True) or {}
    store_hash = normalize_store_hash(data.get("store_hash") or _current_store_hash())
    _require_store_allowed(store_hash)
    candidate_ids = [int(value) for value in data.get("candidate_ids", [])]
    review_status = data.get("review_status", "approved")
    updated = review_candidates(
        candidate_ids=candidate_ids,
        review_status=review_status,
        reviewed_by=data.get("reviewed_by") or session.get("email") or "fulcrum-api",
        note=data.get("review_note"),
    )
    return jsonify({"updated": updated, "review_status": review_status})


@fulcrum_bp.route("/api/internal-links/dashboard-context")
def api_dashboard_context():
    apply_runtime_schema()
    _require_internal_api_auth()
    store_hash = normalize_store_hash(request.args.get("store_hash") or _current_store_hash())
    _require_store_allowed(store_hash)
    return jsonify(get_dashboard_context(store_hash, include_admin=True))


@fulcrum_bp.route("/api/internal-links/publish", methods=["POST"])
def api_publish():
    apply_runtime_schema()
    _require_internal_api_auth()
    data = request.get_json(silent=True) or {}
    store_hash = normalize_store_hash(data.get("store_hash") or _current_store_hash())
    _require_store_allowed(store_hash)
    source_entity_ids = [int(value) for value in data.get("source_entity_ids", data.get("source_product_ids", [])) or []]
    wants_category_publish = any(value < 0 for value in source_entity_ids)
    if wants_category_publish and not category_publishing_enabled_for_store(store_hash):
        return jsonify({"error": "Category publishing is feature-flagged off."}), 409
    publications = publish_approved_entities(
        store_hash=store_hash,
        source_entity_ids=source_entity_ids or None,
        run_id=data.get("run_id"),
    )
    return jsonify({"published": publications})


@fulcrum_bp.route("/api/internal-links/unpublish", methods=["POST"])
def api_unpublish():
    apply_runtime_schema()
    _require_internal_api_auth()
    data = request.get_json(silent=True) or {}
    store_hash = normalize_store_hash(data.get("store_hash") or _current_store_hash())
    _require_store_allowed(store_hash)
    results = unpublish_entities(
        store_hash=store_hash,
        source_entity_ids=[int(value) for value in data.get("source_entity_ids", data.get("source_product_ids", [])) or []],
    )
    return jsonify({"unpublished": results})


@fulcrum_bp.route("/publications")
def publications():
    apply_runtime_schema()
    store_hash = normalize_store_hash(request.args.get("store_hash") or _current_store_hash())
    _require_store_allowed(store_hash)
    return jsonify(list_publications(store_hash, active_only=False, limit=200))






