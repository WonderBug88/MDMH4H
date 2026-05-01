$ErrorActionPreference = "Stop"

$powershellExe = Join-Path $env:SystemRoot "System32\WindowsPowerShell\v1.0\powershell.exe"
$bootstrapScript = "C:\Users\juddu\Downloads\PAM\MDMH4H\deploy\ensure_fulcrum_hosted_stack.ps1"
$taskArguments = "-NoProfile -ExecutionPolicy Bypass -File `"$bootstrapScript`""
$taskCommand = "$powershellExe $taskArguments"
$startupTaskName = "Fulcrum Hosted Stack Startup"
$logonTaskName = "Fulcrum Hosted Stack Logon"
$runKeyPath = "HKCU:\Software\Microsoft\Windows\CurrentVersion\Run"
$runValueName = "FulcrumHostedStack"

$identity = [Security.Principal.WindowsIdentity]::GetCurrent()
$principal = New-Object Security.Principal.WindowsPrincipal($identity)
$isAdmin = $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)

function Install-StartupTask {
    param(
        [string]$TaskName,
        [string]$Execute,
        [string]$Argument
    )

    try {
        $action = New-ScheduledTaskAction -Execute $Execute -Argument $Argument
        $trigger = New-ScheduledTaskTrigger -AtStartup
        $principal = New-ScheduledTaskPrincipal -UserId "SYSTEM" -LogonType ServiceAccount -RunLevel Highest
        $settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable
        Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Principal $principal -Settings $settings -Force | Out-Null
        return @{ Installed = $true; Method = "ScheduledTasks" }
    } catch {
        try {
            $schtasksArgs = @(
                '/Create',
                '/TN', $TaskName,
                '/SC', 'ONSTART',
                '/RU', 'SYSTEM',
                '/RL', 'HIGHEST',
                '/TR', "$Execute $Argument",
                '/F'
            )
            $proc = Start-Process -FilePath schtasks.exe -ArgumentList $schtasksArgs -WindowStyle Hidden -PassThru -Wait
            if ($proc.ExitCode -eq 0) {
                return @{ Installed = $true; Method = "schtasks" }
            }
            return @{ Installed = $false; Method = "schtasks"; Error = "schtasks exit code $($proc.ExitCode)" }
        } catch {
            return @{ Installed = $false; Method = "fallback"; Error = $_.Exception.Message }
        }
    }
}

function Install-LogonTask {
    param(
        [string]$TaskName,
        [string]$Execute,
        [string]$Argument
    )

    try {
        $action = New-ScheduledTaskAction -Execute $Execute -Argument $Argument
        $trigger = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME
        $principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Highest
        $settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable
        Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Principal $principal -Settings $settings -Force | Out-Null
        return @{ Installed = $true; Method = "ScheduledTasks" }
    } catch {
        try {
            $schtasksArgs = @(
                '/Create',
                '/TN', $TaskName,
                '/SC', 'ONLOGON',
                '/RL', 'HIGHEST',
                '/TR', "$Execute $Argument",
                '/F'
            )
            $proc = Start-Process -FilePath schtasks.exe -ArgumentList $schtasksArgs -WindowStyle Hidden -PassThru -Wait
            if ($proc.ExitCode -eq 0) {
                return @{ Installed = $true; Method = "schtasks" }
            }
            return @{ Installed = $false; Method = "schtasks"; Error = "schtasks exit code $($proc.ExitCode)" }
        } catch {
            return @{ Installed = $false; Method = "fallback"; Error = $_.Exception.Message }
        }
    }
}

$startupTaskInstalled = $false
if ($isAdmin) {
    $startupResult = Install-StartupTask -TaskName $startupTaskName -Execute $powershellExe -Argument $taskArguments
    $startupTaskInstalled = [bool]$startupResult.Installed
    if ($startupTaskInstalled) {
        Write-Host "Installed startup task:"
        Write-Host " - $startupTaskName"
        Write-Host " - method: $($startupResult.Method)"
    } else {
        Write-Warning "Could not install the system startup task even though this shell is elevated. Falling back to current-user logon autostart."
        if ($startupResult.Error) {
            Write-Warning "Startup task error: $($startupResult.Error)"
        }
    }
} else {
    Write-Warning "This shell is not elevated. Falling back to current-user logon autostart."
}

if ($env:USERNAME) {
    $logonResult = Install-LogonTask -TaskName $logonTaskName -Execute $powershellExe -Argument $taskArguments
    if ($logonResult.Installed) {
        Write-Host "Installed logon task:"
        Write-Host " - $logonTaskName"
        Write-Host " - method: $($logonResult.Method)"
    } else {
        Write-Warning "Could not install the current-user logon task. Falling back to HKCU Run."
        if ($logonResult.Error) {
            Write-Warning "Logon task error: $($logonResult.Error)"
        }
    }
}

New-Item -Path $runKeyPath -Force | Out-Null
New-ItemProperty -Path $runKeyPath -Name $runValueName -Value $taskCommand -PropertyType String -Force | Out-Null

Write-Host "Installed current-user logon autostart:"
Write-Host " - HKCU Run -> $runValueName"
Write-Host ""
Write-Host "Boot script:"
Write-Host " $bootstrapScript"
