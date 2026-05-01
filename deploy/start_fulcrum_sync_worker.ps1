$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$pythonFallback = "C:\Users\juddu\AppData\Local\Programs\Python\Python312\python.exe"
$pythonExe = $env:FULCRUM_PYTHON_EXE
if (-not $pythonExe) {
    $pythonCmd = Get-Command python -ErrorAction SilentlyContinue
    if ($pythonCmd) {
        $pythonExe = $pythonCmd.Source
    }
}
if (-not $pythonExe -and (Test-Path $pythonFallback)) {
    $pythonExe = $pythonFallback
}
if (-not $pythonExe) {
    throw "Python is not installed or not discoverable. Install Python first, then rerun this script."
}

$env:FULCRUM_ENV_PATH = "C:\Users\juddu\Downloads\PAM\fulcrum.alpha.env"
$env:ENABLE_SCHEDULER = "0"
$env:FLASK_ENV = "development"

Set-Location $repoRoot
& $pythonExe .\run_fulcrum_integration_sync_worker.py
