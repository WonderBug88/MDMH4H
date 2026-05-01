"""Fulcrum Flask app factory."""

from __future__ import annotations

import logging

from flask import Flask, redirect, request, url_for
from werkzeug.middleware.proxy_fix import ProxyFix

from app.fulcrum.config import Config, get_config_class
from app.fulcrum.routes import fulcrum_bp
from app.fulcrum.scheduler import start_embedded_scheduler


def create_fulcrum_app(config_class=None) -> Flask:
    config_class = config_class or get_config_class()

    app = Flask(__name__, template_folder="templates")
    app.secret_key = getattr(config_class, "SECRET_KEY", None) or Config.SECRET_KEY
    app.config.from_object(config_class)
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1)
    app.register_blueprint(fulcrum_bp)

    @app.get("/")
    def root():
        store_hash = (
            (request.args.get("store_hash") or "").strip()
            or (app.config.get("FULCRUM_ALLOWED_STORES") or [None])[0]
            or app.config.get("BIG_COMMERCE_STORE_HASH")
        )
        if store_hash:
            return redirect(url_for("fulcrum.merchant_home", store_hash=store_hash))
        return redirect(url_for("fulcrum.merchant_home"))

    if app.config.get("ENABLE_SCHEDULER") and app.config.get("FULCRUM_RUN_EMBEDDED_SCHEDULER"):
        scheduler = start_embedded_scheduler(job_logger=logging.getLogger(__name__))
        app.extensions["fulcrum_scheduler"] = scheduler

    return app


__all__ = ["create_fulcrum_app"]
