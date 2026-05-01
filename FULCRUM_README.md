# Route Authority Internal Link Orchestrator

Route Authority is the private BigCommerce app shell and operator surface for the internal-linking engine.

## What Is Implemented

- `Flask blueprint`: `app.fulcrum.routes`
- `Standalone app factory`: `app.fulcrum.app.create_fulcrum_app`
- `Runtime services`: `app.fulcrum.services`
- `Runtime tables`: `app/fulcrum/sql/fulcrum_runtime.sql`
- `Bootstrap script`: `app.fulcrum.bootstrap`
- `Embedded/admin pages`:
  - `/fulcrum/`
  - `/fulcrum/health`
  - `/fulcrum/auth`
  - `/fulcrum/load`
  - `/fulcrum/uninstall`
  - `/fulcrum/remove-user`
- `JSON endpoints`:
  - `POST /fulcrum/api/internal-links/runs/generate`
  - `POST /fulcrum/api/internal-links/catalog/sync`
  - `GET /fulcrum/api/internal-links/dashboard-context`
  - `GET /fulcrum/api/internal-links/products/<source_product_id>/preview`
  - `GET /fulcrum/api/internal-links/preview/<source_entity_type>/<source_entity_id>`
  - `POST /fulcrum/api/internal-links/reviews/bulk`
  - `POST /fulcrum/api/internal-links/publish`
- `POST /fulcrum/api/internal-links/unpublish`

Deployment helpers:

- `run_fulcrum_alpha.py`
- `wsgi.py`
- `requirements-fulcrum-alpha.txt`
- `docs/FULCRUM_DEPLOYMENT.md`

Architecture note:

- Fulcrum now runs as its own Flask app and is no longer registered inside the shared PAM `app.create_app()` bootstrap.

Generation behavior:

- `POST /fulcrum/runs/generate` now queues a background generation worker instead of holding the browser request open.
- `POST /fulcrum/api/internal-links/runs/generate` stays synchronous by default, and supports async queueing with `"async": true`.
- Fulcrum run statuses now move through `queued -> running -> completed/failed`.

## Env Inputs

Fulcrum reads `fulcrum.env` from the repo root by default.

Supported formats:

1. Standard env style

```env
FULCRUM_BC_CLIENT_ID=...
FULCRUM_BC_CLIENT_SECRET=...
FULCRUM_BC_ACCOUNT_UUID=...
FULCRUM_GADGETS_API_KEY=...
FULCRUM_APP_BASE_URL=https://your-app-host
```

2. Human-readable note style

```text
Gadget API
Fulcrum:
<gadgets key>

Developer Portal
Client ID
<client id>

Client Secret
<client secret>

Account UUID
<account uuid>
```

Optional overrides:

- `FULCRUM_APP_BASE_URL`
- `FULCRUM_AUTH_CALLBACK_URL`
- `FULCRUM_LOAD_CALLBACK_URL`
- `FULCRUM_UNINSTALL_CALLBACK_URL`
- `FULCRUM_REMOVE_USER_CALLBACK_URL`
- `FULCRUM_ALLOWED_STORES`
- `FULCRUM_THEME_PRODUCT_TEMPLATE`
- `FULCRUM_THEME_CATEGORY_TEMPLATE`
- `FULCRUM_SHARED_SECRET`
- `FULCRUM_ENABLE_CATEGORY_PUBLISHING`
- `FULCRUM_AUTO_PUBLISH_ENABLED`
- `FULCRUM_AUTO_PUBLISH_MIN_SCORE`
- `FULCRUM_AUTO_PUBLISH_MAX_LINKS_PER_SOURCE`
- `FULCRUM_REQUIRE_REVIEW_FOR_CATEGORIES`

Default alpha values:

- `FULCRUM_ENABLE_CATEGORY_PUBLISHING=false`
- `FULCRUM_AUTO_PUBLISH_ENABLED=true`
- `FULCRUM_AUTO_PUBLISH_MIN_SCORE=85`
- `FULCRUM_AUTO_PUBLISH_MAX_LINKS_PER_SOURCE=4`
- `FULCRUM_REQUIRE_REVIEW_FOR_CATEGORIES=true`

## Bootstrap

Run this once after env/secrets are in place:

```powershell
python - <<'PY'
import sys
sys.path.insert(0, r'C:\Users\juddu\Downloads\PAM\MDMH4H')
from app.fulcrum.bootstrap import main
main()
PY
```

This will:

- create `app_runtime`
- create Fulcrum runtime tables
- seed installation records from current BigCommerce credential sources when available

## BigCommerce Developer Portal

Use the `Fulcrum: Internal Link Orchestrator` draft app.

Recommended callbacks:

- Auth: `/fulcrum/auth`
- Load: `/fulcrum/load`
- Uninstall: `/fulcrum/uninstall`
- Remove user: `/fulcrum/remove-user`

## Internal API Auth

The JSON API layer is intended for Gadgets-to-Flask traffic only.

Supported auth modes:

1. Direct shared secret header

```http
X-Fulcrum-Shared-Secret: <shared secret>
```

2. HMAC headers

```http
X-Fulcrum-Timestamp: <unix timestamp>
X-Fulcrum-Signature: sha256=<hex digest of "<timestamp>\\n<body>">
```

The shared secret is `FULCRUM_SHARED_SECRET`.

## Current Render Contract

Fulcrum now publishes approved links into BigCommerce metafields for both products and categories:

- product page:
  - namespace: `h4h`
  - key: `internal_links_html`
- category page:
  - namespace: `h4h`
  - key: `internal_category_links_html`
  - key: `internal_product_links_html`

The active theme must render those metafields on the matching entity template.

## Current Alpha Behavior

- Fulcrum is now being completed as a `stable self-serve alpha`
- Fulcrum now gates normalized query families before routing them into entity scoring
- each gated query family gets `pass`, `hold`, or `reject`
- `pass` query families generate candidates
- `hold` query families stay visible in the dashboard for review and threshold tuning
- `reject` query families are stored for diagnostics only
- all live, publishable entities can compete in routing:
  - `products`
  - `categories`
  - `brands`
  - `content`
- only `publishable` entities compete by default; hidden, placeholder, duplicate, and non-merchandisable entities are filtered out before scoring
- canonical URLs are preferred; duplicate/suffixed variants such as `-1` are suppressed when a stronger base URL exists
- high-confidence `product -> product` and `product/category -> category/product` candidates can auto-approve and auto-publish once the store is readiness-complete
- store-scoped `PDP category competition` can surface a canonical category target inside the existing product-page block for broad product-family queries such as `a rollaway bed`, `hotel shower curtain`, or `bell cart`
- production currently allows PDP category competition for `rollaway`, `towels`, `shower-curtains`, and `luggage`; sandbox remains limited to the clusters explicitly allowlisted in code
- brand and content targets stay review-gated by default
- low-confidence candidates stay in manual review
- max `4` links per source entity by default
- each candidate stores a short rationale so review focuses on intentional links, not just raw query overlap
- candidate scoring uses catalog-aware attributes from the synced BigCommerce catalog, including size, form, pack size, brand, and search keywords
- approvals reward similar future connections, rejections penalize them, and manual overrides count as a stronger but still bounded learning signal
- default operating cadence:
  - daily GSC + catalog sync
  - weekly candidate generation

## App Specification: Gate vs Routing vs Publish

Fulcrum now treats `gate`, `routing`, and `publish` as separate layers.

Definitions:

- `Gate`
  - runs first on the normalized query family
  - decides only `pass`, `hold`, or `reject`
  - is driven by family-level evidence such as demand, opportunity, clarity, and noise
- `Routing`
  - runs after the gate
  - proposes or confirms the best target page
  - may produce outcomes such as a reroute target, same-page winner, or no target
- `Publish`
  - is downstream of both
  - only applies when Fulcrum has a publishable live-block action to take

Hard rules:

- gate and routing are separate litmus tests
- a row that failed the gate must not later be reclassified by a routing outcome
- a routing decision can explain a `pass` row, but it must not overwrite a failed gate reason
- a row is only treated as `published` when it both:
  - passed the gate
  - has a real publishable live-block path
- `current_page_preservation_guard` is diagnostic metadata only
  - it can be stored for analysis
  - it must not act as a blocking gate reason by itself

Current user-facing non-publish reason families are:

- `Gating - Top-10`
- `Gating - Low Clarity`
- `Gating - Noise`
- `Routing - Same Page Winner`
- `Routing - No Target`
- `Awaiting publish`
- `Blocked by review`
- `Category publishing off`
- `Source type not publishable`

Interpretation rule:

- if a row is `hold` or `reject`, the visible reason should stay gate-driven
- if a row is `pass` but still not published, the visible reason can be routing-driven or workflow/config-driven

## App Specification: Review Workflow

User action:

- `Review`
  - sends the row into `app_runtime.query_gate_review_requests`
  - queues the admin/audit workflow
  - pauses live blocks for that source page if they exist
  - resets approved candidates for that source back out of live approval

Admin actions:

- `Resolve`
  - closes the review request
  - does not republish anything
  - use this when the row should stay off/live-paused after review
- `Approve`
  - closes the review request
  - attempts to restore and republish the source page using the reviewed target
  - if no valid restore path exists, the request still resolves, but nothing is republished

Operational meaning:

- `Review` is the user safety valve
- `Resolve` means `close and keep off`
- `Approve` means `close and try to republish`

## Threshold Tuning Guardrail

When tuning thresholds, only change the gate rules for family-level decisions such as:

- top-10 suppression
- low-clarity handling
- noise handling
- demand / opportunity cutoffs

Do not use routing outcomes to retroactively explain or override failed gate decisions.

## Attribute-First Onboarding

Fulcrum now supports store-level catalog profiling so a new customer does not need to be set up cluster by cluster.

What the sync does:

- pulls the BigCommerce catalog for the store
- pulls both products and categories
- infers option buckets such as `size`, `color`, `material`, `form`, `finish`, `pack_size`, `collection`, and `brand`
- normalizes option values into canonical values
- stores a reusable product profile per product in `app_runtime.store_product_profiles`
- stores a reusable category profile per category in `app_runtime.store_category_profiles`
- refreshes store-level intent signals for the query-family gate in `app_runtime.store_intent_signal_enrichments`
- assigns cluster hints such as `rollaway`, `luggage`, `towels`, and `bedding`
- records towel subclusters such as `bath-towels`, `hand-towels`, `washcloths`, `pool-towels`, and `bath-mats`

Main runtime tables:

- `app_runtime.store_attribute_buckets`
- `app_runtime.store_option_name_mappings`
- `app_runtime.store_option_value_mappings`
- `app_runtime.store_product_profiles`
- `app_runtime.store_category_profiles`
- `app_runtime.store_intent_signal_enrichments`
- `app_runtime.store_cluster_rules`

Main operator actions:

- `POST /fulcrum/catalog/sync`
- `POST /fulcrum/api/internal-links/catalog/sync`
- `POST /fulcrum/mappings/review`

Current behavior:

- cluster-specific runs now trust synced product profiles instead of only URL patterns
- category runs use canonical category profiles and product membership from the synced catalog
- towel runs stay towel-to-towel when profile data is available
- pack-size and towel-form signals are included in scoring
- category pages can publish two separate blocks: `Related Categories` and `Shop Matching Products`
- low-confidence option names and values are moved into `pending_review` instead of being blindly auto-approved
- broad head-term queries such as `rollaway bed` are now tagged as `broad_product_family`, which lets Fulcrum keep product-family anchors broad while category publishing remains gated
- query-family intent clarity is now `catalog-first`:
  - brand signals come from BigCommerce brands and brand aliases
  - hard and soft attribute signals come from options, option values, and synced product profiles
  - topic signals come from category taxonomy first
  - SKU/model signals come from product and variant SKU data
  - collection signals are stored separately instead of being collapsed into brand
- ambiguous intent labels can be persisted as agent enrichments later, but live routing never waits on request-time agent calls
- when sandbox category competition is enabled for a cluster, those same broad head-term queries can reserve one PDP slot for a canonical category target before sibling PDPs are selected
- high-confidence `product -> category` competition rows can now auto-publish when they are tagged as `pdp_category_competition`, while source-category page publication still stays review-driven
- the routing gate stores per-family demand, opportunity, intent clarity, noise, and freshness context in `app_runtime.query_gate_records`
- storefront URL building is now `channel-aware`:
  - BigCommerce `channels` and `sites` are synced into `app_runtime.store_storefront_sites`
  - live URLs prefer the matching channel/site when a `channel_id` is available
  - otherwise Fulcrum falls back to the store's primary storefront site
  - generic `.mybigcommerce.com` fallback remains only as the last safety rail

## Storefront Sites

Fulcrum now syncs BigCommerce site/channel data to support multi-storefront-safe URL generation.

Main runtime table:

- `app_runtime.store_storefront_sites`

Main behavior:

- sync reads BigCommerce `channels` plus `sites`
- primary storefront domains are stored per `channel_id`
- `build_storefront_url(...)` now resolves against synced BigCommerce site data instead of only hardcoded store domains
- if a row later carries a `channel_id`, Fulcrum can generate the correct storefront-domain link for that specific channel

## Local Hosts Speed Fix

If you want `fulcrum.hotels4humanity.com` to resolve locally to `127.0.0.1` on the current machine, run this from an Administrator PowerShell:

```powershell
cd C:\Users\juddu\Downloads\PAM\MDMH4H
.\deploy\add_fulcrum_hosts_mapping.ps1
```

This is only a local developer-speed optimization. It does not affect external merchants or the hosted production domain.

## Production Architecture

The current production recommendation is documented in:

- `docs/FULCRUM_PRODUCTION_ARCHITECTURE_PLAN.md`
- `docs/FULCRUM_VPS_PRODUCTION_MIGRATION.md`

Short version:

- keep `Flask` as the routing/scoring engine
- move heavy jobs to workers over time
- use `Gadget` later as the embedded React/Node shell
- do not duplicate routing logic in both Flask and Gadget

## Deployment Hardening

Before deploying Fulcrum to a hosted environment:

- run `python .\deploy\run_fulcrum_preflight.py`
- clear all preflight `error` results
- confirm callback URLs point to the real host
- confirm the theme verification paths point at the current working baseline
- confirm the product and category hooks are present in those theme files

The preflight validates env settings, callback URLs, store allowlisting, theme files, render hooks, database connectivity, and the Fulcrum health endpoint.

Runtime note:

- Fulcrum now applies `app/fulcrum/sql/fulcrum_runtime.sql` once per process during first use, instead of re-running the schema DDL on every request.
- Use preflight and bootstrap to verify schema readiness before deployment rather than relying on repeated request-time DDL checks.

## Gate Audit

When a query family is `hold` or `reject`, use the gate audit command to double-check the logic outside the dashboard.

Examples:

- latest rejected families:
  - `python .\deploy\run_fulcrum_gate_audit.py --store-hash 99oa2tso --disposition reject`
- latest held families:
  - `python .\deploy\run_fulcrum_gate_audit.py --store-hash 99oa2tso --disposition hold`
- filter to a specific family/topic:
  - `python .\deploy\run_fulcrum_gate_audit.py --store-hash 99oa2tso --disposition hold --query sheets`
- JSON output:
  - `python .\deploy\run_fulcrum_gate_audit.py --store-hash 99oa2tso --disposition hold --json`

The audit output includes:

- gate scores:
  - `opportunity`
  - `demand`
  - `intent`
  - `noise`
- current source page and current/preferred page type
- reason summary
- resolved signals:
  - `brand`
  - `hard`
  - `soft`
  - `collection`
  - `topic`
  - `sku`
- current routed winner and alternate target
- any active manual override

## AI Gate Review

Fulcrum can run a second-pass AI audit over the latest query-family gate rows to catch obvious misses.

What it does:

- reviews the current Fulcrum winner for each gate row
- labels it as:
  - `correct`
  - `incorrect`
  - `unclear`
- stores an issue type and recommended action
- clusters similar failures together so logic tuning is easier

How to run it:

- from the dashboard:
  - use `Run AI Review`
- from the command line:
  - `python .\deploy\run_fulcrum_gate_agent_review.py --store-hash 99oa2tso`

Useful variants:

- review only held rows:
  - `python .\deploy\run_fulcrum_gate_agent_review.py --store-hash 99oa2tso --disposition hold`
- review only rejected rows:
  - `python .\deploy\run_fulcrum_gate_agent_review.py --store-hash 99oa2tso --disposition reject`
- JSON output:
  - `python .\deploy\run_fulcrum_gate_agent_review.py --store-hash 99oa2tso --json`

Notes:

- this is a background audit layer only; live routing does not wait for it
- it requires `OPENAI_API_KEY`
- products/categories/brands/content are audited using the same gate row context shown in the dashboard

## Logic Regression

Fulcrum also ships with a deterministic regression runner for the known logic edge cases we have already fixed.

What it does:

- replays a curated set of query-family cases
- checks expected:
  - intent scope
  - preferred page type
  - winner page type
  - winner target name
- catches logic drift before a scoring tweak quietly reintroduces an old bug

How to run it:

- text report:
  - `python .\deploy\run_fulcrum_logic_regression.py --store-hash 99oa2tso`
- JSON output:
  - `python .\deploy\run_fulcrum_logic_regression.py --store-hash 99oa2tso --json`
- fail the shell on any regression:
  - `python .\deploy\run_fulcrum_logic_regression.py --store-hash 99oa2tso --strict`
- run one specific case:
  - `python .\deploy\run_fulcrum_logic_regression.py --store-hash 99oa2tso --case-id twin-rollaway-stays-rollaway`

Notes:

- when a query is not present in the latest gate run, the regression runner evaluates it with a synthetic gate row using the live entity index
- this is meant to convert repeated AI-review misses into deterministic code and repeatable checks

## Logic Change Notes

Fulcrum now keeps a lightweight user-facing logic ledger in:

- `docs/FULCRUM_LOGIC_CHANGELOG.json`

Each entry should capture:

- `change_id`
- `timestamp`
- `title`
- `reason`
- `monitoring_note`
- `affected_queries`

The dashboard reads this ledger and shows:

- total revision count
- latest logic update
- recent change notes

Use it whenever a routing or scoring fix lands so we can track when a fix for one query family later causes drift somewhere else.

To stamp the current regression outcome back onto every changelog entry:

- `python .\deploy\run_fulcrum_logic_regression.py --store-hash 99oa2tso --record-changelog`

That writes a `validation` block onto each logic note so the dashboard can show:

- whether the change currently looks correct
- when it was last checked
- how many affected cases passed
- which affected queries still fail

Dashboard workflow:

- `Run Full Regression + Record`
  - runs the full regression suite and records the result onto all matching changelog entries
- `Verify This Change`
  - runs only the regression cases tied to that change note's `affected_queries`
  - records the result back onto that single changelog entry

## V2 Backlog

Merchant Center is intentionally deferred from the current private alpha.

When we circle back for `v2`, add Merchant API support for:

- PDP health validation for Shopping / free listings
- variant landing-page checks so attribute-led queries like `white` or `twin` land on the correct selected PDP state
- product issue / disapproval suppression in routing
- Merchant performance signals as a small tie-break layer that never overrides GSC intent

## Store Readiness

Fulcrum stores readiness in `app_runtime.store_readiness` and exposes it in the dashboard/API.

Tracked flags:

- `catalog_synced`
- `attribute_mappings_ready`
- `theme_hook_ready`
- `auto_publish_ready`
- `category_beta_ready`

Current alpha expectation:

- a store is auto-publish ready only when the catalog is synced, no mapping reviews are pending, the product theme hook is present, and auto-publish is enabled
- category beta remains off until category metafields are storefront-readable, visually rendered in sandbox, and rollback is proven

## Operational Watchdog

Use this when you want a quick production-style health read for one store.

Command:

- `python .\deploy\run_fulcrum_watchdog.py --store-hash 99oa2tso`

Optional flags:

- `--json`
  - emit machine-readable JSON
- `--strict`
  - exit non-zero on both `watch` and `urgent` states
  - without `--strict`, the script exits non-zero only for `urgent`

What it checks:

- stale or missing generation runs
- recent failed runs
- missing product/category theme hooks
- unresolved attribute mappings
- no live storefront blocks despite passing query families
- growing edge-case review queue

## Admin GSC Performance Cache

The admin page caches the `GSC Value On Fulcrum Pages` summary for a short window so the page stays responsive.

## Go-Live Checklist

For launch-week operations, use:

- [docs/FULCRUM_GO_LIVE_CHECKLIST.md](C:/Users/juddu/Downloads/PAM/MDMH4H/docs/FULCRUM_GO_LIVE_CHECKLIST.md)

- cache key: `live_gsc_performance`
- default cache window: `30 minutes`
- the cache is automatically invalidated when Fulcrum publishes or unpublishes live storefront blocks

## BigCommerce Reset + Republish

Use this when you want to remove old v1 / legacy Fulcrum link metafields from BigCommerce and republish only the current approved outputs.

Dry run:

- `python .\deploy\run_fulcrum_bc_reset_publish.py --store-hash 99oa2tso`

Execute:

- `python .\deploy\run_fulcrum_bc_reset_publish.py --store-hash 99oa2tso --execute`

What it does:

- audits live `h4h` internal-link metafields on BigCommerce
- unpublishes currently tracked live Fulcrum publications
- deletes remaining orphaned legacy link metafields
- republishes the current approved Fulcrum outputs

The script only touches these keys:

- `internal_links_html`
- `internal_category_links_html`
- `internal_product_links_html`
