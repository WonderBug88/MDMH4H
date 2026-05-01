from pathlib import Path
import os
import sys

ROOT = Path(__file__).resolve().parent
os.chdir(ROOT)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.fulcrum.app import create_fulcrum_app
from app.fulcrum.config import DevelopmentConfig

app = create_fulcrum_app(DevelopmentConfig)
app.run(host="127.0.0.1", port=5060, debug=False, use_reloader=False)
