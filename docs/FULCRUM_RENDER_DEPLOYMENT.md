# Fulcrum Render Deployment

## Target

- Platform: Render Blueprint
- Web service: `fulcrum-web`
- Worker service: `fulcrum-sync-worker`
- Database: existing Neon `DATABASE_URL`
- Public host: `https://fulcrum.fulcrumagentics.com`
- Health check: `/fulcrum/health`

## Render Setup

1. Push this repo to GitHub.
2. In Render, choose **New > Blueprint**.
3. Connect the GitHub repo that contains `render.yaml`.
4. If Render asks for the Blueprint path, use `render.yaml` from the `MDMH4H` repo root.
5. Render will create:
   - `fulcrum-web`
   - `fulcrum-sync-worker`
   - `fulcrum-production-settings`

## Required Secret Values

Render prompts for `sync: false` values during the first Blueprint creation. Use the current production values from `fulcrum.alpha.env` or the active secret source:

- `SECRET_KEY`
- `FULCRUM_INTEGRATION_SECRET`
- `FULCRUM_SHARED_SECRET`
- `DATABASE_URL`
- `BIG_COMMERCE_ACCESS_TOKEN`
- `FULCRUM_BC_CLIENT_ID`
- `FULCRUM_BC_CLIENT_SECRET`
- `FULCRUM_BC_ACCOUNT_UUID`
- `ROUTE_AUTHORITY_GOOGLE_CLIENT_ID`
- `ROUTE_AUTHORITY_GOOGLE_CLIENT_SECRET`
- `OPENAI_API_KEY`

The worker reads those same secret values from `fulcrum-web`, so enter them on the web service when the Blueprint prompts.

## Commands

Render web build:

```bash
pip install -r requirements-fulcrum-alpha.txt
```

Render web pre-deploy:

```bash
python deploy/apply_fulcrum_runtime_schema.py
```

Render web start:

```bash
gunicorn --bind 0.0.0.0:$PORT --workers 2 --threads 4 --timeout 120 wsgi_fulcrum:application
```

Render worker start:

```bash
python run_fulcrum_integration_sync_worker.py --limit 5 --sleep 10 --expire-running-after-minutes 30
```

## Domain Cutover

Add `fulcrum.fulcrumagentics.com` as a custom domain on `fulcrum-web`. Render will show the DNS target to create in GoDaddy.

Do not guess this DNS value. Use the exact target Render shows for the custom domain, then wait until Render marks the domain verified and TLS active.

## Post-Deploy Checks

After Render deploys and the domain is verified:

```bash
curl https://fulcrum.fulcrumagentics.com/fulcrum/health
curl https://fulcrum.fulcrumagentics.com/fulcrum/readiness?store_hash=99oa2tso
```

Then queue one Search Console sync from the public host and watch `fulcrum-sync-worker` logs in Render:

```bash
curl -X POST "https://fulcrum.fulcrumagentics.com/fulcrum/integrations/gsc/sync?store_hash=99oa2tso"
```

Expected result:

- web health returns `status: ok`
- readiness uses `DATABASE_URL`
- GSC/GA4 callbacks show the `fulcrum.fulcrumagentics.com` URLs
- the worker logs show a queued sync being processed or a clear provider-side authorization error

