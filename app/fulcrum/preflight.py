from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlparse
import os

from app.fulcrum.app import create_fulcrum_app
from app.fulcrum.config import Config, load_config
from app.fulcrum.services import (
    category_theme_hook_present,
    get_pg_conn,
    normalize_store_hash,
    theme_hook_present,
)


PLACEHOLDER_PREFIXES = (
    "replace_me",
    "your-",
    "your_",
    "http://127.0.0.1",
    "https://your-",
)


@dataclass
class PreflightResult:
    checks: list[dict[str, str]] = field(default_factory=list)

    def add(self, status: str, name: str, detail: str) -> None:
        self.checks.append({"status": status, "name": name, "detail": detail})

    @property
    def has_errors(self) -> bool:
        return any(check["status"] == "error" for check in self.checks)

    def emit(self) -> None:
        for check in self.checks:
            marker = {
                "ok": "[ok]",
                "warn": "[warn]",
                "error": "[error]",
            }.get(check["status"], "[info]")
            print(f"{marker} {check['name']}: {check['detail']}")


def _is_placeholder(value: str | None) -> bool:
    raw = (value or "").strip()
    if not raw:
        return True
    lowered = raw.lower()
    return any(lowered.startswith(prefix) for prefix in PLACEHOLDER_PREFIXES)


def _is_local_url(value: str | None) -> bool:
    try:
        parsed = urlparse((value or "").strip())
    except Exception:
        return False
    host = (parsed.hostname or "").lower()
    return host in {"127.0.0.1", "localhost"}


def _validate_url(value: str | None) -> bool:
    try:
        parsed = urlparse((value or "").strip())
    except Exception:
        return False
    return bool(parsed.scheme and parsed.netloc)


def run_preflight() -> PreflightResult:
    result = PreflightResult()

    runtime_env = os.environ.get("FLASK_ENV", "development")
    config_name = runtime_env if runtime_env in load_config else "development"
    app = create_fulcrum_app(load_config[config_name])

    result.add("ok", "Config", f"Loaded {config_name} config from {Config.FULCRUM_ENV_PATH}")

    required_values = {
        "FULCRUM_GADGETS_API_KEY": Config.FULCRUM_GADGETS_API_KEY,
        "FULCRUM_BC_CLIENT_ID": Config.FULCRUM_BC_CLIENT_ID,
        "FULCRUM_BC_CLIENT_SECRET": Config.FULCRUM_BC_CLIENT_SECRET,
        "FULCRUM_BC_ACCOUNT_UUID": Config.FULCRUM_BC_ACCOUNT_UUID,
        "FULCRUM_SHARED_SECRET": Config.FULCRUM_SHARED_SECRET,
        "FULCRUM_APP_BASE_URL": Config.FULCRUM_APP_BASE_URL,
        "FULCRUM_AUTH_CALLBACK_URL": Config.FULCRUM_AUTH_CALLBACK_URL,
        "FULCRUM_LOAD_CALLBACK_URL": Config.FULCRUM_LOAD_CALLBACK_URL,
        "FULCRUM_UNINSTALL_CALLBACK_URL": Config.FULCRUM_UNINSTALL_CALLBACK_URL,
        "FULCRUM_REMOVE_USER_CALLBACK_URL": Config.FULCRUM_REMOVE_USER_CALLBACK_URL,
    }
    for key, value in required_values.items():
        if _is_placeholder(value):
            result.add("error", key, "Missing or still using a placeholder value")
        else:
            result.add("ok", key, "Configured")

    if not Config.FULCRUM_ALLOWED_STORES:
        result.add("error", "FULCRUM_ALLOWED_STORES", "No allowlisted stores configured")
    else:
        stores = ", ".join(normalize_store_hash(store) for store in Config.FULCRUM_ALLOWED_STORES)
        result.add("ok", "FULCRUM_ALLOWED_STORES", stores)

    base_url = Config.FULCRUM_APP_BASE_URL
    if not _validate_url(base_url):
        result.add("error", "FULCRUM_APP_BASE_URL", "Not a valid absolute URL")
    elif not _is_local_url(base_url) and urlparse(base_url).scheme != "https":
        result.add("warn", "FULCRUM_APP_BASE_URL", "External app host should use HTTPS")

    callback_urls = {
        "auth": Config.FULCRUM_AUTH_CALLBACK_URL,
        "load": Config.FULCRUM_LOAD_CALLBACK_URL,
        "uninstall": Config.FULCRUM_UNINSTALL_CALLBACK_URL,
        "remove-user": Config.FULCRUM_REMOVE_USER_CALLBACK_URL,
    }
    for label, callback_url in callback_urls.items():
        if not _validate_url(callback_url):
            result.add("error", f"{label} callback", "Not a valid absolute URL")
            continue
        if base_url and callback_url.rstrip("/").startswith(base_url.rstrip("/")):
            result.add("ok", f"{label} callback", callback_url)
        else:
            result.add("warn", f"{label} callback", f"Does not start with app base URL: {callback_url}")

    product_template = Path(Config.FULCRUM_THEME_PRODUCT_TEMPLATE)
    category_template = Path(Config.FULCRUM_THEME_CATEGORY_TEMPLATE)
    if product_template.exists():
        result.add("ok", "Product template", str(product_template))
    else:
        result.add("error", "Product template", f"Missing file: {product_template}")
    if category_template.exists():
        result.add("ok", "Category template", str(category_template))
    else:
        result.add("error", "Category template", f"Missing file: {category_template}")

    try:
        product_hook = theme_hook_present()
        result.add("ok" if product_hook else "error", "Product theme hook", "Present" if product_hook else "Missing render hook")
    except Exception as exc:
        result.add("error", "Product theme hook", f"Check failed: {exc}")

    try:
        category_hook = category_theme_hook_present()
        result.add("ok" if category_hook else "warn", "Category theme hook", "Present" if category_hook else "Missing render hook")
    except Exception as exc:
        result.add("error", "Category theme hook", f"Check failed: {exc}")

    try:
        with get_pg_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                cur.fetchone()
        result.add("ok", "Database", "Connection succeeded")
    except Exception as exc:
        result.add("error", "Database", f"Connection failed: {exc}")

    try:
        client = app.test_client()
        basic_health = client.get("/fulcrum/health")
        if basic_health.status_code == 200:
            result.add("ok", "Health endpoint", "/fulcrum/health returned 200")
        else:
            result.add("error", "Health endpoint", f"/fulcrum/health returned {basic_health.status_code}")
    except Exception as exc:
        result.add("error", "Health endpoint", f"Check failed: {exc}")

    if Config.FULCRUM_ALLOWED_STORES:
        store_hash = normalize_store_hash(Config.FULCRUM_ALLOWED_STORES[0])
        try:
            client = app.test_client()
            store_health = client.get(f"/fulcrum/health?store_hash={store_hash}")
            if store_health.status_code == 200:
                result.add("ok", "Store health", f"{store_hash} returned 200")
            else:
                result.add("error", "Store health", f"{store_hash} returned {store_health.status_code}")
        except Exception as exc:
            result.add("error", "Store health", f"{store_hash} check failed: {exc}")

    return result


def main() -> int:
    result = run_preflight()
    result.emit()
    return 1 if result.has_errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
