# Route Authority Production Readiness Report - 2026-05-01

## Verdict

Route Authority is ready for hosted live-store operation and Marketplace-facing review routes on `https://fulcrum.fulcrumagentics.com`.

It is not yet fully automated production until the recurring worker is enabled. With the worker skipped, the current operating label is `manual-ops production alpha`: hosted access, setup, dashboards, readiness, generation, publishing, and rollback work, but future GSC/GA4 queue processing and scheduled generation require manual runs.

## Verified Passes

### Hosting, Domain, And Public Routes

- `fulcrum.fulcrumagentics.com` resolves to `fulcrum-web.onrender.com`, then Render/Cloudflare.
- Live route checks returned `200`:
  - `/fulcrum/health?store_hash=99oa2tso`
  - `/fulcrum/readiness?store_hash=99oa2tso`
  - `/fulcrum/setup?store_hash=99oa2tso`
  - `/fulcrum/results?store_hash=99oa2tso`
  - `/fulcrum/review?store_hash=99oa2tso`
  - `/fulcrum/admin/developer?store_hash=99oa2tso`
  - `/fulcrum/privacy`
  - `/fulcrum/terms`
  - `/fulcrum/support`
- Render command in `render.yaml` remains:
  - build: `pip install -r requirements-fulcrum-alpha.txt`
  - pre-deploy: `python deploy/apply_fulcrum_runtime_schema.py`
  - start: `gunicorn --bind 0.0.0.0:$PORT --workers 2 --threads 4 --timeout 120 wsgi_fulcrum:application`
- Live HTML checks found no `.onrender.com` callback leak on setup, results, readiness, developer, privacy, terms, or support pages.
- Unsupported store health check returned `403`.

### Data And Readiness

- Store: `99oa2tso`.
- Catalog profiles:
  - products: `824`
  - categories: `137`
- Search data:
  - GSC rows: `100000`, date range `2025-10-30` to `2026-04-28`
  - GA4 rows: `88231`, date range `2025-01-29` to `2026-04-28`
- Integrations:
  - BigCommerce: `connected`, `ready`
  - GSC: `connected`, `ready`
  - GA4: `connected`, `ready`
- Readiness:
  - `catalog_synced=true`
  - `theme_hook_ready=true`
  - `auto_publish_ready=true`
  - `category_theme_hook_present=true`
  - mapping backlog remains visible but does not block already gated and routed publishing.

### Generation And Publishing

- Latest run: `run_id=1`, `completed`.
- Run notes: `Gate: 286 pass, 314 hold, 0 reject. Generated 50 intentional candidates from direct GSC entity routing.`
- Candidate/publication state:
  - approved candidates: `50`
  - active publications: `47`
  - published result count shown on live Results page: `47`
- BigCommerce metafield proof:
  - product `112557`, key `h4h.internal_links_html`, remote status `200`, value includes `h4h-internal-links`
  - category `5388`, key `h4h.internal_category_links_html`, remote status `200`, value includes `h4h-internal-links`
  - category `5394`, key `h4h.internal_product_links_html`, remote status `200`, value includes `h4h-internal-links`
- Storefront render proof:
  - product page `/products/oxford-diamond-pillows-by-ganesh-mills.html` returned `200` and rendered `Hotel Bedding Supply`
  - category page `/hotel-towels/` returned `200` and rendered `Martex Bath Towels`

### Rollback

- Dry-run reset/republish audit now executes successfully.
- Dry-run found:
  - active tracked publications: `47`
  - remote link metafields: `206`
  - orphan remote link metafields: `159`
- Targeted live rollback proof completed:
  - unpublished and republished product source `112557`
  - unpublished and republished category source `-1000005388`
  - active publication count remained `47`
  - storefront render checks still passed after republish
- BigCommerce returned `403` when deleting one category metafield. The rollback path now handles this by blanking the metafield value with an inert comment, then republishing the approved block.

### Repo, Tests, And Secret Hygiene

- Repo state before report changes: `main...origin/main`.
- Full Fulcrum test suite passed:
  - `python -m unittest discover app/fulcrum/tests`
  - `238 tests OK`
- Tracked filename secret scan found no committed `.env`, `.pem`, token, credential, secret, cookie, or browser-login files.
- Tracked content scan found no real private keys, `DATABASE_URL=postgres`, OpenAI `sk-` key, Google `AIza` key, or real access-token assignments. Matches were placeholders/examples or non-secret code references.

## Remaining Production Items

### Required For Fully Automated Production

- Enable `fulcrum-sync-worker` or equivalent scheduled worker.
- Worker command:
  - `python run_fulcrum_integration_sync_worker.py --limit 5 --sleep 10 --expire-running-after-minutes 30`
- Until enabled, queued integration syncs and scheduled generation are manual operations.

### Cleanup Recommended Before Broad Marketplace Launch

- Review the `159` orphan remote link metafields from the reset dry run.
- Do not delete them blindly; they may include legacy/internal-link blocks from earlier versions.
- Run `python .\deploy\run_fulcrum_bc_reset_publish.py --store-hash 99oa2tso --execute` only during a deliberate cleanup window after confirming the orphan sample is safe to remove.

### Marketplace Evidence Packet

- Existing public routes and developer readiness page are live.
- Current screenshots should be refreshed for submission if the Marketplace packet requires date-current images:
  - setup
  - results
  - review queue
  - developer readiness
  - privacy
  - terms
  - support

## Commands Used

```powershell
python -m unittest discover app/fulcrum/tests
python .\deploy\run_fulcrum_bc_reset_publish.py --store-hash 99oa2tso
```

Live checks used `Invoke-WebRequest`/`Invoke-RestMethod` against `https://fulcrum.fulcrumagentics.com`.
