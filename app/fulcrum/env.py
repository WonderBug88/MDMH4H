import os
from pathlib import Path


FULCRUM_LABEL_MAP = {
    "CLIENT ID": "FULCRUM_BC_CLIENT_ID",
    "CLIENT SECRET": "FULCRUM_BC_CLIENT_SECRET",
    "ACCOUNT UUID": "FULCRUM_BC_ACCOUNT_UUID",
    "AUTH CALLBACK URL": "FULCRUM_AUTH_CALLBACK_URL",
    "LOAD CALLBACK URL": "FULCRUM_LOAD_CALLBACK_URL",
    "UNINSTALL CALLBACK URL": "FULCRUM_UNINSTALL_CALLBACK_URL",
    "REMOVE USER CALLBACK URL": "FULCRUM_REMOVE_USER_CALLBACK_URL",
    "APP BASE URL": "FULCRUM_APP_BASE_URL",
}

FULCRUM_PASSTHROUGH_ENV_KEYS = {
    "OPENAI_API_KEY",
    "FULCRUM_GATE_REVIEW_AGENT_MODEL",
    "FULCRUM_CHANGED_ROUTE_REASONING_MODEL",
}


def _env_truthy(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return ""


def parse_fulcrum_env_file(path: str | os.PathLike | None) -> dict[str, str]:
    """
    Supports both:
      1) Standard KEY=value env files
      2) The current human-readable Fulcrum note format, e.g.
            Developer Portal
            Client ID
            <value>
    """
    if not path:
        return {}

    raw = _read_text(Path(path))
    if not raw.strip():
        return {}

    parsed: dict[str, str] = {}
    lines = raw.splitlines()
    idx = 0

    while idx < len(lines):
        line = lines[idx].strip()

        if not line or line.startswith("#"):
            idx += 1
            continue

        if "=" in line:
            key, value = line.split("=", 1)
            parsed[key.strip()] = value.strip()
            idx += 1
            continue

        normalized = line.rstrip(":").strip().upper()

        if normalized == "FULCRUM":
            next_idx = idx + 1
            while next_idx < len(lines) and not lines[next_idx].strip():
                next_idx += 1
            if next_idx < len(lines):
                parsed["FULCRUM_GADGETS_API_KEY"] = lines[next_idx].strip()
                idx = next_idx + 1
                continue

        mapped_key = FULCRUM_LABEL_MAP.get(normalized)
        if mapped_key:
            next_idx = idx + 1
            while next_idx < len(lines) and not lines[next_idx].strip():
                next_idx += 1
            if next_idx < len(lines):
                parsed[mapped_key] = lines[next_idx].strip()
                idx = next_idx + 1
                continue

        idx += 1

    return parsed


def load_fulcrum_settings(path: str | os.PathLike | None) -> dict[str, str]:
    """
    Returns Fulcrum-specific settings with environment-variable overrides.
    """
    file_values = parse_fulcrum_env_file(path)
    merged = dict(file_values)

    for key in list(file_values.keys()) + [
        "FULCRUM_GADGETS_API_KEY",
        "FULCRUM_BC_CLIENT_ID",
        "FULCRUM_BC_CLIENT_SECRET",
        "FULCRUM_BC_ACCOUNT_UUID",
        "FULCRUM_AUTH_CALLBACK_URL",
        "FULCRUM_LOAD_CALLBACK_URL",
        "FULCRUM_UNINSTALL_CALLBACK_URL",
        "FULCRUM_REMOVE_USER_CALLBACK_URL",
        "FULCRUM_APP_BASE_URL",
        "FULCRUM_SHARED_SECRET",
        "FULCRUM_ALLOWED_STORES",
        "FULCRUM_THEME_PRODUCT_TEMPLATE",
        "FULCRUM_THEME_CATEGORY_TEMPLATE",
        "FULCRUM_ENABLE_CATEGORY_PUBLISHING",
        "FULCRUM_AUTO_PUBLISH_ENABLED",
        "FULCRUM_AUTO_PUBLISH_MIN_SCORE",
        "FULCRUM_AUTO_PUBLISH_MAX_LINKS_PER_SOURCE",
        "FULCRUM_REQUIRE_REVIEW_FOR_CATEGORIES",
        "DATABASE_URL",
        "DB_NAME",
        "DB_USER",
        "DB_PASSWORD",
        "DB_HOST",
        "DB_PORT",
        *sorted(FULCRUM_PASSTHROUGH_ENV_KEYS),
    ]:
        if os.getenv(key):
            merged[key] = os.getenv(key, "").strip()

    for key in FULCRUM_PASSTHROUGH_ENV_KEYS:
        value = (merged.get(key) or "").strip()
        if value and not os.getenv(key):
            os.environ[key] = value

    return merged
