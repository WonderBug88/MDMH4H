from pathlib import Path
import os
import sys

from werkzeug.serving import make_server

ROOT = Path(__file__).resolve().parent
os.chdir(ROOT)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.fulcrum.app import create_fulcrum_app
from app.fulcrum.config import DevelopmentConfig

app = create_fulcrum_app(DevelopmentConfig)
server = make_server("127.0.0.1", 5063, app, threaded=True)
server.serve_forever()
