import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.fulcrum.app import create_fulcrum_app
from app.fulcrum.config import get_config_class


if __name__ == "__main__":
    os.environ.setdefault("FULCRUM_ENV_PATH", r"C:\Users\juddu\Downloads\PAM\fulcrum.alpha.env")
    os.environ.setdefault("ENABLE_SCHEDULER", "0")
    os.environ.setdefault("FULCRUM_HOST", "127.0.0.1")
    os.environ.setdefault("FULCRUM_PORT", "5093")
    os.environ.setdefault("FLASK_ENV", "development")

    app = create_fulcrum_app(get_config_class(os.environ.get("FLASK_ENV", "development")))
    app.run(
        host=os.environ.get("FULCRUM_HOST", "127.0.0.1"),
        port=int(os.environ.get("FULCRUM_PORT", "5093")),
        debug=False,
        use_reloader=False,
        threaded=True,
    )
