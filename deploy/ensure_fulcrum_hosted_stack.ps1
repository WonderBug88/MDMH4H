$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$portsToStop = @(5087, 5092, 5093)
$healthUrl = "http://127.0.0.1:5093/fulcrum/health?store_hash=99oa2tso"
$startupTimeoutSeconds = 30
$fulcrumScript = Join-Path $PSScriptRoot "start_fulcrum_hosted_5093.ps1"
$syncWorkerScript = Join-Path $PSScriptRoot "start_fulcrum_sync_worker.ps1"
$caddyScript = Join-Path $PSScriptRoot "start_caddy_fulcrum.ps1"
$caddyfile = Join-Path $repoRoot "deploy\Caddyfile.fulcrum"
$powershellExe = Join-Path $env:SystemRoot "System32\WindowsPowerShell\v1.0\powershell.exe"
$logDir = Join-Path $repoRoot "deploy\logs"
$fulcrumLog = Join-Path $logDir "fulcrum-hosted-5093.out.log"
$fulcrumErrorLog = Join-Path $logDir "fulcrum-hosted-5093.err.log"
$syncWorkerLog = Join-Path $logDir "fulcrum-sync-worker.out.log"
$syncWorkerErrorLog = Join-Path $logDir "fulcrum-sync-worker.err.log"
$staleCleanupScript = Join-Path $PSScriptRoot "stop_stale_fulcrum_5092_elevated.ps1"

if (-not (Test-Path $logDir)) {
    New-Item -ItemType Directory -Path $logDir | Out-Null
}

function Resolve-CaddyExe {
    $fallbackCaddyExe = "C:\Users\juddu\AppData\Local\Microsoft\WinGet\Packages\CaddyServer.Caddy_Microsoft.Winget.Source_8wekyb3d8bbwe\caddy.exe"
    if ($env:FULCRUM_CADDY_EXE) {
        return $env:FULCRUM_CADDY_EXE
    }

    $caddyCmd = Get-Command caddy -ErrorAction SilentlyContinue
    if ($caddyCmd) {
        return $caddyCmd.Source
    }

    if ($env:LOCALAPPDATA) {
        $wingetCaddy = Get-ChildItem -Path "$env:LOCALAPPDATA\Microsoft\WinGet\Packages" -Recurse -Filter caddy.exe -ErrorAction SilentlyContinue |
            Select-Object -First 1 -ExpandProperty FullName
        if ($wingetCaddy) {
            return $wingetCaddy
        }
    }

    if (Test-Path $fallbackCaddyExe) {
        return $fallbackCaddyExe
    }

    return $null
}

function Stop-FulcrumListeners {
    param([int[]]$Ports)

    $listeners = Get-NetTCPConnection -State Listen -ErrorAction SilentlyContinue |
        Where-Object { $_.LocalPort -in $Ports }
    if (-not $listeners) {
        return
    }

    $processIds = $listeners | Select-Object -ExpandProperty OwningProcess -Unique
    foreach ($processId in $processIds) {
        try {
            Stop-Process -Id $processId -Force -ErrorAction Stop
        } catch {
            & taskkill.exe /PID $processId /F | Out-Null
        }
    }
}

function Stop-FulcrumLaunchers {
    $launcherProcesses = Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
        Where-Object {
            $_.Name -in @('python.exe', 'pythonw.exe', 'powershell.exe') -and
            $_.CommandLine -and
            (
                $_.CommandLine -like '*run_fulcrum_alpha.py*' -or
                $_.CommandLine -like '*run_fulcrum_flask_5093.py*' -or
                $_.CommandLine -like '*start_fulcrum_hosted_5087.ps1*' -or
                $_.CommandLine -like '*start_fulcrum_hosted_5093.ps1*' -or
                $_.CommandLine -like '*run_fulcrum_integration_sync_worker.py*' -or
                $_.CommandLine -like '*start_fulcrum_sync_worker.ps1*'
            )
        }

    foreach ($process in $launcherProcesses) {
        Stop-Process -Id $process.ProcessId -Force -ErrorAction SilentlyContinue
    }
}

function Get-FulcrumListenerPids {
    param([int]$Port)

    $listeners = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
    if (-not $listeners) {
        return @()
    }
    return @($listeners | Select-Object -ExpandProperty OwningProcess -Unique)
}

function Assert-SingleFulcrumListener {
    param([int]$Port)

    $listenerPids = Get-FulcrumListenerPids -Port $Port
    if ($listenerPids.Count -ne 1) {
        throw "Expected exactly one Fulcrum listener on 127.0.0.1:$Port, found $($listenerPids.Count): $($listenerPids -join ', ')."
    }
    Write-Host "Verified one Fulcrum listener on 127.0.0.1:$Port (PID $($listenerPids[0]))."
}

function Get-ProcessSummary {
    param([int]$ProcessId)

    $process = Get-CimInstance Win32_Process -Filter "ProcessId = $ProcessId" -ErrorAction SilentlyContinue
    if (-not $process) {
        return "PID $ProcessId"
    }
    $commandLine = ($process.CommandLine -replace '\s+', ' ').Trim()
    if (-not $commandLine) {
        $commandLine = "<command line unavailable>"
    }
    return "PID $ProcessId $($process.Name): $commandLine"
}

function Assert-NoUnexpectedFulcrumListeners {
    param([int[]]$Ports)

    $errors = @()
    foreach ($port in $Ports) {
        $listenerPids = Get-FulcrumListenerPids -Port $port
        if ($listenerPids.Count -gt 0) {
            $summaries = @($listenerPids | ForEach-Object { Get-ProcessSummary -ProcessId $_ })
            $errors += "Unexpected stale listener remains on 127.0.0.1:$port -> $($summaries -join '; ')"
        }
    }

    if ($errors.Count -gt 0) {
        $message = $errors -join "`n"
        throw "$message`nRun from an elevated shell: powershell -ExecutionPolicy Bypass -File `"$staleCleanupScript`""
    }
}

function Reload-Or-Start-Caddy {
    $caddyExe = Resolve-CaddyExe
    if (-not $caddyExe) {
        throw "Caddy is not installed or not discoverable. Install it first, then rerun this script."
    }

    $runningCaddy = Get-Process caddy -ErrorAction SilentlyContinue
    if ($runningCaddy) {
        try {
            & $caddyExe reload --config $caddyfile | Out-Null
            Write-Host "Reloaded existing Caddy config."
            return
        } catch {
            Write-Warning "Failed to reload existing Caddy config: $($_.Exception.Message)"
        }
    }

    Start-Process $powershellExe -WindowStyle Hidden -ArgumentList @(
        '-NoProfile',
        '-ExecutionPolicy', 'Bypass',
        '-File', $caddyScript
    )
    Write-Host "Started Caddy."
}

Stop-FulcrumListeners -Ports $portsToStop
Stop-FulcrumLaunchers
Start-Sleep -Seconds 2

Start-Process $powershellExe -WindowStyle Hidden -ArgumentList @(
    '-NoProfile',
    '-ExecutionPolicy', 'Bypass',
    '-File', $fulcrumScript
) -RedirectStandardOutput $fulcrumLog -RedirectStandardError $fulcrumErrorLog

$deadline = (Get-Date).AddSeconds($startupTimeoutSeconds)
$healthy = $false
while ((Get-Date) -lt $deadline) {
    try {
        $response = Invoke-WebRequest -Uri $healthUrl -UseBasicParsing -TimeoutSec 5
        if ($response.StatusCode -eq 200) {
            $healthy = $true
            break
        }
    } catch {
    }
    Start-Sleep -Seconds 1
}

if (-not $healthy) {
    throw "Fulcrum hosted app did not become healthy on 127.0.0.1:5093 within $startupTimeoutSeconds seconds. See $fulcrumLog."
}

Assert-SingleFulcrumListener -Port 5093
Assert-NoUnexpectedFulcrumListeners -Ports @(5087, 5092)

Start-Process $powershellExe -WindowStyle Hidden -ArgumentList @(
    '-NoProfile',
    '-ExecutionPolicy', 'Bypass',
    '-File', $syncWorkerScript
) -RedirectStandardOutput $syncWorkerLog -RedirectStandardError $syncWorkerErrorLog

Reload-Or-Start-Caddy

Write-Host "Fulcrum hosted stack started."
Write-Host "Local health:  $healthUrl"
Write-Host "Hosted health: https://fulcrum.fulcrumagentics.com/fulcrum/health?store_hash=99oa2tso"
Write-Host "App log:       $fulcrumLog"
Write-Host "App errors:    $fulcrumErrorLog"
Write-Host "Sync log:      $syncWorkerLog"
Write-Host "Sync errors:   $syncWorkerErrorLog"

