# Fulcrum Route Authority Operations Runbook

## Current Runtime

- Public app host: `https://fulcrum.fulcrumagentics.com`
- Caddy upstream: `127.0.0.1:5093`
- Runtime database: Neon Postgres from `C:\Users\juddu\Downloads\PAM\fulcrum.alpha.env`
- Merchant storefront data source: `https://www.hotels4humanity.com/`

This Windows host is now staging/fallback. Durable production should use the Ubuntu VPS path in `docs/FULCRUM_VPS_PRODUCTION_MIGRATION.md`.

## Recover Local Stack

```powershell
cd C:\Users\juddu\Downloads\PAM\MDMH4H
.\deploy\ensure_fulcrum_hosted_stack.ps1
```

Expected result:

- one Fulcrum web listener on `127.0.0.1:5093`
- no Fulcrum listeners on `5087` or `5092`
- Caddy reverse proxy loaded from `deploy\Caddyfile.fulcrum`
- sync worker started with logs under `deploy\logs`

If the script reports a stale `5092` worker, run this once from an elevated PowerShell window:

```powershell
cd C:\Users\juddu\Downloads\PAM\MDMH4H
powershell -ExecutionPolicy Bypass -File .\deploy\stop_stale_fulcrum_5092_elevated.ps1
```

## Recover Sync Worker

```powershell
cd C:\Users\juddu\Downloads\PAM\MDMH4H
.\deploy\start_fulcrum_sync_worker.ps1
```

Main logs:

- `deploy\logs\fulcrum-sync-worker.out.log`
- `deploy\logs\fulcrum-sync-worker.err.log`

## Health And Readiness

```powershell
curl.exe http://127.0.0.1:5093/fulcrum/health?store_hash=99oa2tso
curl.exe --resolve fulcrum.fulcrumagentics.com:443:127.0.0.1 https://fulcrum.fulcrumagentics.com/fulcrum/health?store_hash=99oa2tso
curl.exe https://fulcrum.fulcrumagentics.com/fulcrum/health?store_hash=99oa2tso
curl.exe https://fulcrum.fulcrumagentics.com/fulcrum/readiness?store_hash=99oa2tso
```

Use the public-route smoke check when DNS/TLS is suspect:

```powershell
cd C:\Users\juddu\Downloads\PAM\MDMH4H
.\deploy\check_fulcrum_public_route.ps1
```

If local `--resolve` passes but public DNS fails, the app and Caddy are working locally; check router/NAT and Windows Firewall for WAN `80/443 -> 192.168.1.66:80/443`.

## Queue A Google Sync

```powershell
curl.exe -X POST https://fulcrum.fulcrumagentics.com/fulcrum/integrations/gsc/sync?store_hash=99oa2tso
curl.exe -X POST https://fulcrum.fulcrumagentics.com/fulcrum/integrations/ga4/sync?store_hash=99oa2tso
```

The worker records attempts in `app_runtime.integration_sync_runs`.

## Neon Storage Guardrails

GSC sync is bounded by these optional settings in `fulcrum.alpha.env`:

```text
FULCRUM_GSC_SYNC_LOOKBACK_DAYS=180
FULCRUM_GSC_SYNC_MIN_IMPRESSIONS=3
FULCRUM_GSC_SYNC_MAX_ROWS=100000
FULCRUM_GSC_API_ROW_LIMIT=25000
```

GSC replacement is transactional and writes directly to `app_runtime.store_gsc_daily`; it no longer creates a duplicate temporary table. A failed insert rolls back and preserves the previous rows.

## Final Smoke Checklist

1. `.\deploy\check_fulcrum_public_route.ps1`
2. Public health returns `200`
3. Public readiness shows `gsc.ready=true`, `ga4.ready=true`, and catalog ready
4. Logo loads: `https://fulcrum.fulcrumagentics.com/static/fulcrum/route-authority-logo.png`
5. BigCommerce launch/load opens the setup page
6. Admin developer page opens
7. `app_runtime.store_gsc_daily` and `app_runtime.store_ga4_pages_daily` row counts/date ranges match readiness
8. Neon database size remains below the current project limit after GSC sync
