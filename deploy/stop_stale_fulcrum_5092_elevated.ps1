$ErrorActionPreference = "Stop"

$oldPorts = @(5087, 5092)
$oldPatterns = @(
    "start_fulcrum_hosted_5087.ps1",
    "run_fulcrum_alpha.py",
    "FULCRUM_PORT = `"5092`"",
    "FULCRUM_PORT=5092"
)

$identity = [Security.Principal.WindowsIdentity]::GetCurrent()
$principal = New-Object Security.Principal.WindowsPrincipal($identity)
$isAdmin = $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    $powershellExe = Join-Path $env:SystemRoot "System32\WindowsPowerShell\v1.0\powershell.exe"
    $argumentList = "-NoExit -ExecutionPolicy Bypass -File `"$PSCommandPath`""
    Start-Process -FilePath $powershellExe -Verb RunAs -ArgumentList $argumentList
    Write-Host "Opened an elevated PowerShell window. Accept the UAC prompt, then watch that window for cleanup results."
    exit 0
}

function Test-TextMatchesOldFulcrum {
    param([string]$Text)
    if (-not $Text) {
        return $false
    }
    foreach ($pattern in $oldPatterns) {
        if ($Text -like "*$pattern*") {
            return $true
        }
    }
    return $false
}

function Stop-PortListeners {
    param([int[]]$Ports)

    $listeners = Get-NetTCPConnection -State Listen -ErrorAction SilentlyContinue |
        Where-Object { $_.LocalPort -in $Ports }
    if (-not $listeners) {
        Write-Host "No stale Fulcrum listeners found on ports $($Ports -join ', ')."
        return
    }

    foreach ($processId in ($listeners | Select-Object -ExpandProperty OwningProcess -Unique)) {
        $process = Get-CimInstance Win32_Process -Filter "ProcessId = $processId" -ErrorAction SilentlyContinue
        if ($process) {
            Write-Host "Stopping stale listener PID $processId ($($process.Name))."
        } else {
            Write-Host "Stopping stale listener PID $processId."
        }
        Stop-Process -Id $processId -Force -ErrorAction Stop
    }
}

function Stop-OldLaunchers {
    $processes = Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
        Where-Object {
            $_.Name -in @("python.exe", "pythonw.exe", "powershell.exe", "pwsh.exe") -and
            (Test-TextMatchesOldFulcrum -Text $_.CommandLine)
        }

    foreach ($process in $processes) {
        Write-Host "Stopping old Fulcrum launcher PID $($process.ProcessId): $($process.CommandLine)"
        Stop-Process -Id $process.ProcessId -Force -ErrorAction Stop
    }
}

function Disable-OldScheduledTasks {
    $tasks = Get-ScheduledTask -ErrorAction SilentlyContinue |
        Where-Object {
            $actionText = ($_.Actions | Out-String)
            Test-TextMatchesOldFulcrum -Text $actionText
        }

    foreach ($task in $tasks) {
        Write-Host "Disabling old Fulcrum scheduled task: $($task.TaskPath)$($task.TaskName)"
        Disable-ScheduledTask -TaskName $task.TaskName -TaskPath $task.TaskPath | Out-Null
    }
}

function Remove-OldRunKeys {
    $runKeyPath = "HKCU:\Software\Microsoft\Windows\CurrentVersion\Run"
    if (-not (Test-Path $runKeyPath)) {
        return
    }

    $item = Get-ItemProperty -Path $runKeyPath
    foreach ($property in $item.PSObject.Properties) {
        if ($property.Name -like "PS*") {
            continue
        }
        if (Test-TextMatchesOldFulcrum -Text ([string]$property.Value)) {
            Write-Host "Removing old Fulcrum HKCU Run entry: $($property.Name)"
            Remove-ItemProperty -Path $runKeyPath -Name $property.Name -ErrorAction Stop
        }
    }
}

function Assert-NoOldListeners {
    $remaining = Get-NetTCPConnection -State Listen -ErrorAction SilentlyContinue |
        Where-Object { $_.LocalPort -in $oldPorts }
    if ($remaining) {
        $details = $remaining | ForEach-Object { "127.0.0.1:$($_.LocalPort) PID $($_.OwningProcess)" }
        throw "Stale Fulcrum listener still present: $($details -join '; ')"
    }
}

Disable-OldScheduledTasks
Remove-OldRunKeys
Stop-OldLaunchers
Stop-PortListeners -Ports $oldPorts
Assert-NoOldListeners

Write-Host "Stale Fulcrum 5087/5092 workers are stopped and old startup sources are disabled."
