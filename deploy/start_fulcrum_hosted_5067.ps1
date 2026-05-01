$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$env:FULCRUM_ENV_PATH = "C:\Users\juddu\Downloads\PAM\fulcrum.alpha.env"
$env:ENABLE_SCHEDULER = "0"
$env:FULCRUM_HOST = "127.0.0.1"
$env:FULCRUM_PORT = "5077"
$env:FLASK_ENV = "development"

Set-Location $repoRoot
python .\run_fulcrum_alpha.py
