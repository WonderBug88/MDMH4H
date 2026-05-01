from pathlib import Path
import os
import sys
import time

from werkzeug.serving import make_server

ROOT = Path(__file__).resolve().parent
os.chdir(ROOT)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.fulcrum.app import create_fulcrum_app
from app.fulcrum.config import DevelopmentConfig

REQUEST_LOG_PATH = ROOT / "tmp_run_fulcrum_flask_5058.request.log"


def _log(message: str) -> None:
    with REQUEST_LOG_PATH.open("a", encoding="utf-8") as handle:
        handle.write(f"{message}\n")


app = create_fulcrum_app(DevelopmentConfig)
dashboard_view = app.view_functions.get("fulcrum.dashboard")
if dashboard_view is not None:
    def _wrapped_dashboard(*args, **kwargs):
        _log(f"VIEW_START {time.time():.3f}")
        result = dashboard_view(*args, **kwargs)
        _log(f"VIEW_END {time.time():.3f} {type(result).__name__}")
        return result

    app.view_functions["fulcrum.dashboard"] = _wrapped_dashboard

_log(f"SERVER_START {time.time():.3f}")
server = make_server("127.0.0.1", 5058, app, threaded=True)
server.serve_forever()
