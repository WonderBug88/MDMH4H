import os

from app.fulcrum.app import create_fulcrum_app
from app.fulcrum.config import get_config_class


app = create_fulcrum_app(get_config_class(os.environ.get("FLASK_ENV", "development")))
