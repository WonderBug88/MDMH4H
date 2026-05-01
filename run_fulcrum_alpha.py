import os

from waitress import serve

from app.fulcrum.app import create_fulcrum_app
from app.fulcrum.config import get_config_class


def create_app_for_waitress():
    return create_fulcrum_app(get_config_class(os.environ.get("FLASK_ENV", "development")))


if __name__ == "__main__":
    host = os.environ.get("FULCRUM_HOST", "0.0.0.0")
    port = int(os.environ.get("FULCRUM_PORT", os.environ.get("PORT", "5057")))
    serve(create_app_for_waitress(), host=host, port=port)
