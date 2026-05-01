$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$env:FULCRUM_ENV_PATH = "C:\Users\juddu\Downloads\PAM\fulcrum.alpha.env"
$env:ENABLE_SCHEDULER = "0"
$env:FULCRUM_PORT = "5077"
$env:FLASK_ENV = "development"
$env:FULCRUM_APP_BASE_URL = "http://127.0.0.1:5077"

Set-Location $repoRoot
python -c "from app.fulcrum.app import create_fulcrum_app; from app.fulcrum.config import load_config; app=create_fulcrum_app(load_config['development']); app.run(host='127.0.0.1', port=5077, debug=False, use_reloader=False)"
