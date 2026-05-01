"""Fulcrum-local configuration.

This module lets Fulcrum run as a standalone Flask app without depending on
the broader PAM app bootstrap.
"""

from __future__ import annotations

import os
from os import environ

from dotenv import load_dotenv

from app.fulcrum.env import load_fulcrum_settings


BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, os.pardir))
load_dotenv(dotenv_path=os.path.join(BASE_DIR, ".env"))
FULCRUM_ENV_PATH = environ.get("FULCRUM_ENV_PATH", os.path.join(ROOT_DIR, "fulcrum.env"))
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


def resolve_config_name(name: str | None = None) -> str:
    candidate = (name or environ.get("FLASK_ENV") or "development").strip().lower()
    return candidate if candidate in load_config else "development"


class Config:
    """Base configuration for the standalone Fulcrum app."""

    BASE_DIR = BASE_DIR
    ROOT_DIR = ROOT_DIR
    FLASK_ENV = environ.get("FLASK_ENV", "development")
    FLASK_APP = environ.get("FLASK_APP")
    SECRET_KEY = environ.get("SECRET_KEY") or "fulcrum-dev-secret"
    TESTING = _setting_bool(environ.get("TESTING"), default=False)
    FLASK_DEBUG = environ.get("FLASK_DEBUG")
    STATIC_FOLDER = "static"
    TEMPLATES_FOLDER = "templates"
    SESSION_PERMANENT = True
    GSC_CREDENTIALS_FILE_PATH = environ.get("GSC_CREDENTIALS_FILE_PATH")
    BIG_COMMERCE_ACCESS_TOKEN = environ.get("BIG_COMMERCE_ACCESS_TOKEN")
    BIG_COMMERCE_STORE_HASH = environ.get("BIG_COMMERCE_STORE_HASH")
    FULCRUM_ENV_PATH = FULCRUM_ENV_PATH
    FULCRUM_GADGETS_API_KEY = FULCRUM_SETTINGS.get("FULCRUM_GADGETS_API_KEY", "")
    FULCRUM_BC_CLIENT_ID = FULCRUM_SETTINGS.get("FULCRUM_BC_CLIENT_ID", "")
    FULCRUM_BC_CLIENT_SECRET = FULCRUM_SETTINGS.get("FULCRUM_BC_CLIENT_SECRET", "")
    FULCRUM_BC_ACCOUNT_UUID = FULCRUM_SETTINGS.get("FULCRUM_BC_ACCOUNT_UUID", "")
    FULCRUM_APP_BASE_URL = FULCRUM_SETTINGS.get("FULCRUM_APP_BASE_URL", "http://127.0.0.1:5057")
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
    GOOGLE_OAUTH_CLIENT_ID = (
        FULCRUM_SETTINGS.get("ROUTE_AUTHORITY_GOOGLE_CLIENT_ID")
        or FULCRUM_SETTINGS.get("GOOGLE_OAUTH_CLIENT_ID")
        or environ.get("GOOGLE_OAUTH_CLIENT_ID", "")
    )
    GOOGLE_OAUTH_CLIENT_SECRET = (
        FULCRUM_SETTINGS.get("ROUTE_AUTHORITY_GOOGLE_CLIENT_SECRET")
        or FULCRUM_SETTINGS.get("GOOGLE_OAUTH_CLIENT_SECRET")
        or environ.get("GOOGLE_OAUTH_CLIENT_SECRET", "")
    )
    FULCRUM_GSC_OAUTH_CALLBACK_URL = FULCRUM_SETTINGS.get(
        "FULCRUM_GSC_OAUTH_CALLBACK_URL",
        f"{FULCRUM_APP_BASE_URL.rstrip('/')}/fulcrum/integrations/gsc/callback",
    )
    FULCRUM_GA4_OAUTH_CALLBACK_URL = FULCRUM_SETTINGS.get(
        "FULCRUM_GA4_OAUTH_CALLBACK_URL",
        f"{FULCRUM_APP_BASE_URL.rstrip('/')}/fulcrum/integrations/ga4/callback",
    )
    FULCRUM_PRIVACY_POLICY_URL = FULCRUM_SETTINGS.get(
        "FULCRUM_PRIVACY_POLICY_URL",
        f"{FULCRUM_APP_BASE_URL.rstrip('/')}/fulcrum/privacy",
    )
    FULCRUM_SUPPORT_URL = FULCRUM_SETTINGS.get(
        "FULCRUM_SUPPORT_URL",
        f"{FULCRUM_APP_BASE_URL.rstrip('/')}/fulcrum/support",
    )
    FULCRUM_TERMS_OF_SERVICE_URL = FULCRUM_SETTINGS.get(
        "FULCRUM_TERMS_OF_SERVICE_URL",
        f"{FULCRUM_APP_BASE_URL.rstrip('/')}/fulcrum/terms",
    )
    FULCRUM_INTEGRATION_SECRET = (
        FULCRUM_SETTINGS.get("FULCRUM_INTEGRATION_SECRET")
        or environ.get("FULCRUM_INTEGRATION_SECRET")
        or SECRET_KEY
    )
    FULCRUM_SHARED_SECRET = FULCRUM_SETTINGS.get("FULCRUM_SHARED_SECRET", SECRET_KEY)
    FULCRUM_ALLOWED_STORES = [
        item.strip()
        for item in FULCRUM_SETTINGS.get("FULCRUM_ALLOWED_STORES", "").split(",")
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
    FULCRUM_GSC_SYNC_LOOKBACK_DAYS = _setting_int(
        FULCRUM_SETTINGS.get("FULCRUM_GSC_SYNC_LOOKBACK_DAYS")
        or environ.get("FULCRUM_GSC_SYNC_LOOKBACK_DAYS"),
        180,
    )
    FULCRUM_GSC_SYNC_MIN_IMPRESSIONS = _setting_int(
        FULCRUM_SETTINGS.get("FULCRUM_GSC_SYNC_MIN_IMPRESSIONS")
        or environ.get("FULCRUM_GSC_SYNC_MIN_IMPRESSIONS"),
        3,
    )
    FULCRUM_GSC_SYNC_MAX_ROWS = _setting_int(
        FULCRUM_SETTINGS.get("FULCRUM_GSC_SYNC_MAX_ROWS")
        or environ.get("FULCRUM_GSC_SYNC_MAX_ROWS"),
        100000,
    )
    FULCRUM_GSC_API_ROW_LIMIT = _setting_int(
        FULCRUM_SETTINGS.get("FULCRUM_GSC_API_ROW_LIMIT")
        or environ.get("FULCRUM_GSC_API_ROW_LIMIT"),
        25000,
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
    DATABASE_URL = FULCRUM_SETTINGS.get("DATABASE_URL") or environ.get("DATABASE_URL")
    DB_NAME = FULCRUM_SETTINGS.get("DB_NAME") or environ.get("DB_NAME")
    DB_USER = FULCRUM_SETTINGS.get("DB_USER") or environ.get("DB_USER")
    DB_PASSWORD = FULCRUM_SETTINGS.get("DB_PASSWORD") or environ.get("DB_PASSWORD")
    DB_HOST = FULCRUM_SETTINGS.get("DB_HOST") or environ.get("DB_HOST")
    DB_PORT = _setting_int(FULCRUM_SETTINGS.get("DB_PORT") or environ.get("DB_PORT"), 5432)


class DevelopmentConfig(Config):
    SQLALCHEMY_DATABASE_URI = Config.DATABASE_URL or (
        f"postgresql://{Config.DB_USER}:{Config.DB_PASSWORD}@{Config.DB_HOST}/{Config.DB_NAME}"
    )
    SQLALCHEMY_TRACK_MODIFICATIONS = False


class StagingConfig(Config):
    SQLALCHEMY_DATABASE_URI = environ.get("STAGING_DATABASE_URL")
    SQLALCHEMY_TRACK_MODIFICATIONS = False


class ProductionConfig(Config):
    SQLALCHEMY_DATABASE_URI = environ.get("PRODUCTION_DATABASE_URL")
    SQLALCHEMY_TRACK_MODIFICATIONS = False


class TestConfig(Config):
    TESTING = True
    SQLALCHEMY_DATABASE_URI = "sqlite:///" + os.path.join(BASE_DIR, "..", "tests", "test_db.sqlite")
    SQLALCHEMY_TRACK_MODIFICATIONS = False


load_config = {
    "development": DevelopmentConfig,
    "staging": StagingConfig,
    "production": ProductionConfig,
    "testing": TestConfig,
    "default": DevelopmentConfig,
}


def get_config_class(name: str | None = None):
    return load_config[resolve_config_name(name)]


__all__ = [
    "Config",
    "DevelopmentConfig",
    "ProductionConfig",
    "StagingConfig",
    "TestConfig",
    "get_config_class",
    "load_config",
    "resolve_config_name",
]

