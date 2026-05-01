$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$localPort = 5077

$listeners = Get-NetTCPConnection -LocalPort $localPort -State Listen -ErrorAction SilentlyContinue
if ($listeners) {
    $processIds = $listeners | Select-Object -ExpandProperty OwningProcess -Unique
    foreach ($processId in $processIds) {
        Stop-Process -Id $processId -Force -ErrorAction SilentlyContinue
    }
    Start-Sleep -Seconds 2
}

Start-Process powershell -WindowStyle Hidden -ArgumentList @(
    '-ExecutionPolicy', 'Bypass',
    '-File', (Join-Path $PSScriptRoot 'start_fulcrum_local_5077.ps1')
)

Start-Sleep -Seconds 6

Write-Host "Fulcrum local restarted."
Write-Host "Dashboard: http://127.0.0.1:5077/fulcrum/?store_hash=99oa2tso"
Write-Host "Health:    http://127.0.0.1:5077/fulcrum/health?store_hash=99oa2tso"
