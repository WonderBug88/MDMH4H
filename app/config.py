import os
from os import environ
from pathlib import Path

from dotenv import load_dotenv


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

# Get the absolute path of the parent directory of the directory containing the current file
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, os.pardir))
load_dotenv(dotenv_path=os.path.join(BASE_DIR, ".env"))
FULCRUM_ENV_PATH = environ.get("FULCRUM_ENV_PATH", os.path.join(ROOT_DIR, "fulcrum.env"))


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return ""


def parse_fulcrum_env_file(path: str | os.PathLike | None) -> dict[str, str]:
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
        *sorted(FULCRUM_PASSTHROUGH_ENV_KEYS),
    ]:
        if os.getenv(key):
            merged[key] = os.getenv(key, "").strip()

    for key in FULCRUM_PASSTHROUGH_ENV_KEYS:
        value = (merged.get(key) or "").strip()
        if value and not os.getenv(key):
            os.environ[key] = value

    return merged


FULCRUM_SETTINGS = load_fulcrum_settings(FULCRUM_ENV_PATH)


def _setting_bool(value, default=False):
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _setting_int(value, default):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


class Config:
    """Base configuration."""

    BASE_DIR = BASE_DIR
    ROOT_DIR = ROOT_DIR
    FLASK_ENV = environ.get("FLASK_ENV")
    FLASK_APP = environ.get("FLASK_APP")
    SECRET_KEY = environ.get("SECRET_KEY")
    TESTING = environ.get("TESTING")
    FLASK_DEBUG = environ.get("FLASK_DEBUG")
    STATIC_FOLDER = "static"
    TEMPLATES_FOLDER = "templates"
    SITEMAP_INCLUDE_RULES_WITHOUT_PARAMS = True
    SESSION_PERMANENT = True
    ALLOWED_EXTENSIONS = {"pdf", "doc", "docx", "xls", "xlsx"}
    UPLOAD_FOLDER = os.path.join(BASE_DIR, "uploads")
    ETL_SCRIPTS_PATH = os.path.join(BASE_DIR, "etl_scripts")
    GMAIL_CREDENTIALS_FILE = environ.get("GMAIL_CREDENTIALS_FILE")
    GOOGLE_SERVICE_ACCOUNT_FILE = environ.get("GOOGLE_SERVICE_ACCOUNT_FILE")
    BIG_COMMERCE_ACCESS_TOKEN = environ.get("BIG_COMMERCE_ACCESS_TOKEN")
    BIG_COMMERCE_STORE_HASH = environ.get("BIG_COMMERCE_STORE_HASH")
    FULCRUM_ENV_PATH = FULCRUM_ENV_PATH
    FULCRUM_GADGETS_API_KEY = FULCRUM_SETTINGS.get("FULCRUM_GADGETS_API_KEY", "")
    FULCRUM_BC_CLIENT_ID = FULCRUM_SETTINGS.get("FULCRUM_BC_CLIENT_ID", "")
    FULCRUM_BC_CLIENT_SECRET = FULCRUM_SETTINGS.get("FULCRUM_BC_CLIENT_SECRET", "")
    FULCRUM_BC_ACCOUNT_UUID = FULCRUM_SETTINGS.get("FULCRUM_BC_ACCOUNT_UUID", "")
    FULCRUM_APP_BASE_URL = FULCRUM_SETTINGS.get("FULCRUM_APP_BASE_URL", "http://127.0.0.1:5000")
    FULCRUM_AUTH_CALLBACK_URL = FULCRUM_SETTINGS.get(
        "FULCRUM_AUTH_CALLBACK_URL",
        f"{FULCRUM_APP_BASE_URL.rstrip('/')}/fulcrum/auth",
    )
    FULCRUM_LOAD_CALLBACK_URL = FULCRUM_SETTINGS.get(
        "FULCRUM_LOAD_CALLBACK_URL",
        f"{FULCRUM_APP_BASE_URL.rstrip('/')}/fulcrum/load",
    )
    FULCRUM_UNINSTALL_CALLBACK_URL = FULCRUM_SETTINGS.get(
        "FULCRUM_UNINSTALL_CALLBACK_URL",
        f"{FULCRUM_APP_BASE_URL.rstrip('/')}/fulcrum/uninstall",
    )
    FULCRUM_REMOVE_USER_CALLBACK_URL = FULCRUM_SETTINGS.get(
        "FULCRUM_REMOVE_USER_CALLBACK_URL",
        f"{FULCRUM_APP_BASE_URL.rstrip('/')}/fulcrum/remove-user",
    )
    FULCRUM_SHARED_SECRET = FULCRUM_SETTINGS.get("FULCRUM_SHARED_SECRET", SECRET_KEY or "")
    FULCRUM_ALLOWED_STORES = [
        item.strip()
        for item in FULCRUM_SETTINGS.get(
            "FULCRUM_ALLOWED_STORES",
            "",
        ).split(",")
        if item.strip()
    ]
    FULCRUM_THEME_PRODUCT_TEMPLATE = FULCRUM_SETTINGS.get(
        "FULCRUM_THEME_PRODUCT_TEMPLATE",
        os.path.join(
            ROOT_DIR,
            "theme_work",
            "PartsWarehous-6-8-2025a-live-baseline-2026-04-07",
            "templates",
            "pages",
            "product.html",
        ),
    )
    FULCRUM_THEME_CATEGORY_TEMPLATE = FULCRUM_SETTINGS.get(
        "FULCRUM_THEME_CATEGORY_TEMPLATE",
        os.path.join(
            ROOT_DIR,
            "theme_work",
            "PartsWarehous-6-8-2025a-live-baseline-2026-04-07",
            "templates",
            "pages",
            "category.html",
        ),
    )
    FULCRUM_ENABLE_CATEGORY_PUBLISHING = _setting_bool(
        FULCRUM_SETTINGS.get("FULCRUM_ENABLE_CATEGORY_PUBLISHING"),
        default=False,
    )
    FULCRUM_AUTO_PUBLISH_ENABLED = _setting_bool(
        FULCRUM_SETTINGS.get("FULCRUM_AUTO_PUBLISH_ENABLED"),
        default=True,
    )
    FULCRUM_AUTO_PUBLISH_MIN_SCORE = _setting_int(
        FULCRUM_SETTINGS.get("FULCRUM_AUTO_PUBLISH_MIN_SCORE"),
        85,
    )
    FULCRUM_AUTO_PUBLISH_MAX_LINKS_PER_SOURCE = _setting_int(
        FULCRUM_SETTINGS.get("FULCRUM_AUTO_PUBLISH_MAX_LINKS_PER_SOURCE"),
        4,
    )
    FULCRUM_REQUIRE_REVIEW_FOR_CATEGORIES = _setting_bool(
        FULCRUM_SETTINGS.get("FULCRUM_REQUIRE_REVIEW_FOR_CATEGORIES"),
        default=True,
    )
    FULCRUM_DAILY_SYNC_HOUR = _setting_int(
        FULCRUM_SETTINGS.get("FULCRUM_DAILY_SYNC_HOUR"),
        1,
    )
    FULCRUM_DAILY_SYNC_MINUTE = _setting_int(
        FULCRUM_SETTINGS.get("FULCRUM_DAILY_SYNC_MINUTE"),
        15,
    )
    FULCRUM_WEEKLY_GENERATION_DAY = FULCRUM_SETTINGS.get("FULCRUM_WEEKLY_GENERATION_DAY", "sun")
    FULCRUM_WEEKLY_GENERATION_HOUR = _setting_int(
        FULCRUM_SETTINGS.get("FULCRUM_WEEKLY_GENERATION_HOUR"),
        2,
    )
    FULCRUM_WEEKLY_GENERATION_MINUTE = _setting_int(
        FULCRUM_SETTINGS.get("FULCRUM_WEEKLY_GENERATION_MINUTE"),
        15,
    )
    ENABLE_SCHEDULER = _setting_bool(environ.get("ENABLE_SCHEDULER"), default=True)
    FULCRUM_RUN_EMBEDDED_SCHEDULER = _setting_bool(
        environ.get("FULCRUM_RUN_EMBEDDED_SCHEDULER"),
        default=False,
    )
    # DB
    DB_NAME = environ.get('DB_NAME')
    DB_USER = environ.get('DB_USER')
    DB_PASSWORD = environ.get('DB_PASSWORD')
    DB_HOST = environ.get('DB_HOST')
    DB_PORT = 5432


class DevelopmentConfig(Config):
    """Development configuration."""

    # SQLALCHEMY_DATABASE_URI = environ.get("DEV_DATABASE_URL")
    SQLALCHEMY_DATABASE_URI  = (
        f"postgresql://{Config.DB_USER}:{Config.DB_PASSWORD}@{Config.DB_HOST}/{Config.DB_NAME}"
    )
    SQLALCHEMY_TRACK_MODIFICATIONS = False


class StagingConfig(Config):
    """Staging configuration."""

    SQLALCHEMY_DATABASE_URI = environ.get("STAGING_DATABASE_URL")
    SQLALCHEMY_TRACK_MODIFICATIONS = False


class ProductionConfig(Config):
    """Production configuration."""

    SQLALCHEMY_DATABASE_URI = environ.get("PRODUCTION_DATABASE_URL")
    SQLALCHEMY_TRACK_MODIFICATIONS = False


class TestConfig(Config):
    """Test configuration."""

    TESTING = True
    # Set test DB diffrent from main db
    SQLALCHEMY_DATABASE_URI = "sqlite:///" + os.path.join(
        BASE_DIR, "../tests", "test_db.sqlite"
    )
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    IMAGE_SAVE_DIRECTORY = os.path.join(
        BASE_DIR, "../tests" "/images"
    )  # Test Images folder


# Load Config from environment variables
load_config = {
    "development": DevelopmentConfig,
    "staging": StagingConfig,
    "production": ProductionConfig,
    "testing": TestConfig,
    "default": DevelopmentConfig,
}
