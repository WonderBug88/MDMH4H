from pathlib import Path
import os
import sys

ROOT = Path(__file__).resolve().parent
os.chdir(ROOT)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

os.environ.setdefault('FLASK_ENV', 'development')
os.environ.setdefault('FULCRUM_PORT', '5087')
os.environ.setdefault('FULCRUM_APP_BASE_URL', 'http://127.0.0.1:5087')
os.environ.setdefault('FULCRUM_ALLOWED_STORES', 'pdwzti0dpv,99oa2tso')

from app.fulcrum.app import create_fulcrum_app
from app.fulcrum.config import DevelopmentConfig

app = create_fulcrum_app(DevelopmentConfig)
app.run(host='127.0.0.1', port=5087, debug=False, use_reloader=False)
