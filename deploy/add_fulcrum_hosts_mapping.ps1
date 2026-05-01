$hostsPath = "C:\Windows\System32\drivers\etc\hosts"
$entry = "127.0.0.1 fulcrum.fulcrumagentics.com"

$isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole(
    [Security.Principal.WindowsBuiltInRole]::Administrator
)

if (-not $isAdmin) {
    throw "Run this script from an Administrator PowerShell so it can update the hosts file."
}

$content = Get-Content $hostsPath -ErrorAction Stop
if ($content -contains $entry) {
    Write-Output "Hosts mapping already present:"
    Write-Output " - $entry"
    exit 0
}

Add-Content -Path $hostsPath -Value $entry -ErrorAction Stop
Write-Output "Added hosts mapping:"
Write-Output " - $entry"


