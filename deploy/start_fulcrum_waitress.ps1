$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$env:FLASK_ENV = if ($env:FLASK_ENV) { $env:FLASK_ENV } else { "development" }
$env:ENABLE_SCHEDULER = if ($env:ENABLE_SCHEDULER) { $env:ENABLE_SCHEDULER } else { "0" }
$env:FULCRUM_PORT = if ($env:FULCRUM_PORT) { $env:FULCRUM_PORT } else { "5057" }

Set-Location $repoRoot
python .\run_fulcrum_alpha.py
