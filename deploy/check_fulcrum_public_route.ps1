param(
    [string]$Hostname = "fulcrum.fulcrumagentics.com",
    [string]$StoreHash = "99oa2tso",
    [string]$ExpectedPublicIp = "38.13.122.136",
    [int]$WorkerPort = 5093,
    [switch]$InspectFirewall
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$caddyfile = Join-Path $repoRoot "deploy\Caddyfile.fulcrum"
$healthPath = "/fulcrum/health?store_hash=$StoreHash"
$publicUrl = "https://$Hostname$healthPath"
$localWorkerUrl = "http://127.0.0.1:$WorkerPort$healthPath"
$failures = New-Object System.Collections.Generic.List[string]
$vpnLikeInterfacePattern = "nord|vpn|wireguard|openvpn|tailscale|zerotier"

function Add-Failure {
    param([string]$Message)
    $failures.Add($Message) | Out-Null
    Write-Warning $Message
}

function Invoke-CurlCheck {
    param([string[]]$Arguments)

    $previousErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $output = & curl.exe @Arguments 2>&1
        $exitCode = $LASTEXITCODE
    } catch {
        $output = $_.Exception.Message
        $exitCode = 1
    } finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }
    $outputText = $output | ForEach-Object {
        if ($_ -is [System.Management.Automation.ErrorRecord]) {
            $_.Exception.Message
        } else {
            [string]$_
        }
    }
    return [pscustomobject]@{
        ExitCode = $exitCode
        Output = ($outputText | Out-String).Trim()
    }
}

Write-Host "Checking Fulcrum public route for $Hostname."

try {
    $dnsRecords = Resolve-DnsName -Name $Hostname -Type A -ErrorAction Stop |
        Where-Object { $_.IPAddress } |
        Select-Object -ExpandProperty IPAddress
    Write-Host "DNS A records: $($dnsRecords -join ', ')"
    if ($ExpectedPublicIp -and ($dnsRecords -notcontains $ExpectedPublicIp)) {
        Add-Failure "DNS does not include expected IP $ExpectedPublicIp."
    }
} catch {
    Add-Failure "DNS lookup failed: $($_.Exception.Message)"
}

$vpnDefaultRoutes = @(
    Get-NetRoute -DestinationPrefix "0.0.0.0/0" -ErrorAction SilentlyContinue |
        Where-Object { $_.InterfaceAlias -match $vpnLikeInterfacePattern } |
        Sort-Object RouteMetric, InterfaceMetric
)
if ($vpnDefaultRoutes.Count -gt 0) {
    $routeDescriptions = $vpnDefaultRoutes | ForEach-Object {
        "$($_.InterfaceAlias) via $($_.NextHop)"
    }
    Write-Warning "VPN-like default route is active: $($routeDescriptions -join '; '). Public inbound HTTPS usually fails while the host routes replies through VPN."
}

try {
    $observedPublicIp = (Invoke-RestMethod -Uri "https://api.ipify.org" -TimeoutSec 8).Trim()
    Write-Host "Observed outbound public IP: $observedPublicIp"
    if ($dnsRecords -and $observedPublicIp -and ($dnsRecords -notcontains $observedPublicIp) -and $vpnDefaultRoutes.Count -gt 0) {
        Write-Warning "Outbound public IP differs from DNS while VPN is active. This is expected with VPN, but it prevents this local test from proving the real WAN route."
    }
} catch {
    Write-Warning "Could not check observed outbound public IP: $($_.Exception.Message)"
}

try {
    $workerResponse = Invoke-WebRequest -Uri $localWorkerUrl -UseBasicParsing -TimeoutSec 5
    Write-Host "Local worker health: HTTP $($workerResponse.StatusCode)"
} catch {
    Add-Failure "Local worker health failed at ${localWorkerUrl}: $($_.Exception.Message)"
}

$localCaddy = Invoke-CurlCheck -Arguments @(
    "--silent",
    "--show-error",
    "--fail",
    "--max-time",
    "20",
    "--resolve",
    "${Hostname}:443:127.0.0.1",
    $publicUrl
)
if ($localCaddy.ExitCode -eq 0) {
    Write-Host "Local Caddy HTTPS health with --resolve: ok"
} else {
    Add-Failure "Local Caddy HTTPS health with --resolve failed: $($localCaddy.Output)"
}

if ($InspectFirewall) {
    try {
        $allowedWebPorts = Get-NetFirewallRule -Enabled True -Direction Inbound -Action Allow -ErrorAction Stop |
            Get-NetFirewallPortFilter -ErrorAction Stop |
            Where-Object {
                ($_.Protocol -eq "TCP" -or $_.Protocol -eq "Any") -and
                ($_.LocalPort -eq "Any" -or $_.LocalPort -match "(^|,)80(,|$)" -or $_.LocalPort -match "(^|,)443(,|$)")
            }
        if ($allowedWebPorts) {
            Write-Host "Windows Firewall has enabled inbound allow coverage for web ports."
        } else {
            Write-Warning "No explicit enabled inbound Windows Firewall allow rule was found for TCP 80/443. A program-scoped Caddy rule may still allow traffic."
        }
    } catch {
        Write-Warning "Could not inspect Windows Firewall rules: $($_.Exception.Message)"
    }
} else {
    Write-Host "Windows Firewall inspection skipped. Re-run with -InspectFirewall if router/firewall state is the suspected failure."
}

$public = Invoke-CurlCheck -Arguments @(
    "--silent",
    "--show-error",
    "--fail",
    "--max-time",
    "20",
    $publicUrl
)
if ($public.ExitCode -eq 0) {
    Write-Host "Public DNS HTTPS health: ok"
} else {
    Add-Failure "Public DNS HTTPS health failed: $($public.Output)"
}

if ($localCaddy.ExitCode -eq 0 -and $public.ExitCode -ne 0) {
    if ($vpnDefaultRoutes.Count -gt 0) {
        Add-Failure "Local Caddy works but public DNS path fails while a VPN default route is active. Disconnect NordVPN on this host, or move Fulcrum to cloud hosting, then retest."
    } else {
        Add-Failure "Local Caddy works but public DNS path fails. Check WAN 80/443 port forwarding to this Windows host and Windows Firewall inbound rules."
    }
}

if (Test-Path $caddyfile) {
    $caddyText = Get-Content -Path $caddyfile -Raw
    if ($caddyText -match "reverse_proxy\s+127\.0\.0\.1:$WorkerPort") {
        Write-Host "Caddy upstream is locked to 127.0.0.1:$WorkerPort."
    } else {
        Add-Failure "Caddyfile does not point reverse_proxy to 127.0.0.1:$WorkerPort."
    }
} else {
    Add-Failure "Caddyfile not found at $caddyfile."
}

$canonicalListeners = Get-NetTCPConnection -LocalPort $WorkerPort -State Listen -ErrorAction SilentlyContinue
$canonicalPids = @($canonicalListeners | Select-Object -ExpandProperty OwningProcess -Unique)
if ($canonicalPids.Count -eq 1) {
    $listenerPid = $canonicalPids[0]
    Write-Host "Canonical worker listener: 127.0.0.1:$WorkerPort PID $listenerPid"
} else {
    Add-Failure "Expected exactly one listener on 127.0.0.1:$WorkerPort."
}

$staleListeners = Get-NetTCPConnection -State Listen -ErrorAction SilentlyContinue |
    Where-Object { $_.LocalPort -in @(5087, 5092) }
if ($staleListeners) {
    $details = $staleListeners | ForEach-Object { "127.0.0.1:$($_.LocalPort) PID $($_.OwningProcess)" }
    Add-Failure "Stale Fulcrum listener detected: $($details -join '; ')"
} else {
    Write-Host "No stale Fulcrum listeners on 5087/5092."
}

if ($failures.Count -gt 0) {
    Write-Host ""
    Write-Host "Fulcrum public route check failed:"
    $failures | ForEach-Object { Write-Host " - $_" }
    exit 1
}

Write-Host "Fulcrum public route check passed."
