$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$caddyfile = Join-Path $repoRoot "deploy\Caddyfile.fulcrum"

$fallbackCaddyExe = "C:\Users\juddu\AppData\Local\Microsoft\WinGet\Packages\CaddyServer.Caddy_Microsoft.Winget.Source_8wekyb3d8bbwe\caddy.exe"
$caddyExe = $env:FULCRUM_CADDY_EXE
if (-not $caddyExe) {
    $caddyCmd = Get-Command caddy -ErrorAction SilentlyContinue
    if ($caddyCmd) {
        $caddyExe = $caddyCmd.Source
    }
}
if (-not $caddyExe -and $env:LOCALAPPDATA) {
    $caddyExe = Get-ChildItem -Path "$env:LOCALAPPDATA\Microsoft\WinGet\Packages" -Recurse -Filter caddy.exe -ErrorAction SilentlyContinue |
        Select-Object -First 1 -ExpandProperty FullName
}
if (-not $caddyExe -and (Test-Path $fallbackCaddyExe)) {
    $caddyExe = $fallbackCaddyExe
}

if (-not $caddyExe) {
    throw "Caddy is not installed or not discoverable. Install it first, then rerun this script."
}

Set-Location $repoRoot
& $caddyExe run --config $caddyfile
