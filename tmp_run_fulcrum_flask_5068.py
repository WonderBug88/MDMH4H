from app.fulcrum.app import create_fulcrum_app
from app.fulcrum.config import load_config

app = create_fulcrum_app(load_config['development'])
app.run(host='127.0.0.1', port=5068, debug=False, use_reloader=False)
