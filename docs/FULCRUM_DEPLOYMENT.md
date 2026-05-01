# Fulcrum Deployment

## Purpose

This deploys the current Fulcrum private alpha as a standalone hosted Flask app with Waitress.

For a stable, tunnel-free HTTPS deployment, use the Caddy path documented in:

- `docs/FULCRUM_CADDY_DEPLOYMENT.md`

## Files

- `run_fulcrum_alpha.py`
- `wsgi.py`
- `app/fulcrum/app.py`
- `requirements-fulcrum-alpha.txt`
- `deploy/start_fulcrum_waitress.ps1`
- `../fulcrum.env.example`

## Recommended Runtime Defaults

- `FLASK_ENV=development` for local/private alpha testing only
- `ENABLE_SCHEDULER=0` on web processes
- `FULCRUM_PORT=5057`

Run scheduled jobs separately if you want GSC weekly refresh outside the web process.

## Install

```powershell
python -m pip install -r .\requirements-fulcrum-alpha.txt
```

## Release Gate

Run the local release checks before you deploy or merge a Fulcrum change:

```powershell
python .\deploy\run_fulcrum_release_checks.py
```

This runs:

- `py_compile` across the standalone Fulcrum entrypoints and `app/fulcrum`
- the full Fulcrum unit suite in `app/fulcrum/tests`

GitHub Actions now runs the same command in `.github/workflows/fulcrum-ci.yml`.

## Preflight

Run the deployment preflight before you host Fulcrum or point the BigCommerce app callbacks at it.

```powershell
python .\deploy\run_fulcrum_preflight.py
```

The preflight checks:

- required Fulcrum env values are present and not left as placeholders
- callback URLs are valid absolute URLs
- allowlisted stores are configured
- product/category template paths exist
- product/category render hooks are present
- database connectivity works
- `/fulcrum/health` and one store-aware health check return `200`

## Start

```powershell
.\deploy\start_fulcrum_waitress.ps1
```

Or:

```powershell
$env:ENABLE_SCHEDULER='0'
$env:FULCRUM_PORT='5057'
python .\run_fulcrum_alpha.py
```

## Health Check

Basic:

```text
GET /fulcrum/health
```

Store-aware:

```text
GET /fulcrum/health?store_hash=<store_hash>
```

## Internal API

Use shared-secret or HMAC headers as documented in `FULCRUM_README.md`.

## Required Manual Steps

- rotate exposed Gadgets and BigCommerce secrets
- update `fulcrum.env` with the rotated values
- run the Fulcrum preflight and clear all `error` results
- configure the BigCommerce draft app callback URLs to the real host
- deploy the Flask app host
- point Gadgets or your embedded shell at the host

## Stable Host Recommendation

For the easiest free/open-source production-style setup on Windows:

- run Fulcrum on `127.0.0.1:5093`
- run Caddy in front of it
- use `fulcrum.fulcrumagentics.com`

See:

- `docs/FULCRUM_CADDY_DEPLOYMENT.md`
- `docs/FULCRUM_OPERATIONS_RUNBOOK.md`

## Merchant Center

Merchant Center is intentionally out of scope for the current private alpha.

For `v2`, the next use of Merchant API should be:

- PDP health checks for Shopping / free-listing eligibility
- variant landing-page validation for attributes like `white`, `twin`, and `queen`
- product-status diagnostics and issue-based target suppression
- product-performance hints as a small tie-break signal only

