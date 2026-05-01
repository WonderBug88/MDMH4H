$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$env:FLASK_ENV = if ($env:FLASK_ENV) { $env:FLASK_ENV } else { "development" }
$env:ENABLE_SCHEDULER = if ($env:ENABLE_SCHEDULER) { $env:ENABLE_SCHEDULER } else { "1" }
$env:FULCRUM_RUN_EMBEDDED_SCHEDULER = "0"

Set-Location $repoRoot
python .\run_fulcrum_scheduler.py
