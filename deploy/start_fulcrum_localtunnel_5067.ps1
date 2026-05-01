$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$env:FULCRUM_TUNNEL_PORT = "5067"
$env:FULCRUM_TUNNEL_URL_FILE = "C:\Users\juddu\Downloads\PAM\fulcrum_tunnel_url_5067.txt"

Set-Location $repoRoot
node .\deploy\start_fulcrum_localtunnel.js
