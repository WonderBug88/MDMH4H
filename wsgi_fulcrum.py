"""Production WSGI entrypoint for Route Authority."""

from __future__ import annotations

import os

os.environ.setdefault("FLASK_ENV", "production")
os.environ.setdefault("ENABLE_SCHEDULER", "0")
os.environ.setdefault("FULCRUM_RUN_EMBEDDED_SCHEDULER", "0")

from app.fulcrum.app import create_fulcrum_app  # noqa: E402
from app.fulcrum.config import get_config_class  # noqa: E402


application = create_fulcrum_app(get_config_class(os.environ.get("FLASK_ENV", "production")))
app = application

