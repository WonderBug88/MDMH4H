# Fulcrum Production Architecture Plan

## Summary

Fulcrum should remain a `Flask-first engine` with `Gadget as the embedded shell`, not a duplicated dual-stack app.

BigCommerce is right that Gadget gives us:

- `React + Vite` on the frontend
- `Node.js + npm` on the backend

That makes Gadget a strong fit for:

- embedded app UX
- auth/session polish
- customer-facing packaging
- BigCommerce admin shell integration

It does **not** mean we should move Fulcrum's routing engine there right now.

## Recommended production split

### Keep in Flask

Flask remains the source of truth for:

- query-family gate
- intent clarity
- entity routing and scoring
- review memory
- publish / unpublish logic
- BigCommerce catalog sync
- internal read models for dashboard results

Why:

- the router logic already lives here
- the Postgres runtime model already lives here
- duplicating scoring logic in two runtimes would create drift

### Move out of request path into workers

These should run as explicit jobs, not inline user requests:

- full catalog sync
- query-family generation
- AI review batches
- publish / unpublish batches
- regression runs
- changelog validation runs

Good production shape:

- Flask web app
- one or more worker processes
- Postgres as shared source of truth
- Caddy or another reverse proxy in front

### Use Gadget as the shell

Gadget should become:

- the BigCommerce embedded shell
- the operator-facing React app
- a client for Fulcrum internal APIs

Gadget should call Flask for:

- dashboard context
- generate run
- catalog sync
- review actions
- publish / unpublish
- preview HTML

This matches the current handoff document in:

- `docs/fulcrum_alpha_sales_kit/gadgets_deployment_handoff.md`

## Update workflow

### Right now

Update logic here first:

- `app/fulcrum/services.py`
- runtime SQL
- regression suite
- dashboard data contracts

Then let Gadget consume those APIs.

### Do not do this

Do not maintain:

- one router in Flask
- another router in Gadget/Node

That would make debugging much harder and would break confidence in the review loop.

## Multi-store and MSF posture

### Multi-store

Fulcrum is already multi-store at the runtime level through `store_hash`.

### Multi-storefront

To claim MSF readiness honestly, Fulcrum should resolve storefront domains from BigCommerce:

- `Channels`
- `Sites`

The correct URL-building order is:

1. use a known `channel_id` when available
2. resolve the matching BigCommerce site/domain
3. fall back to the primary storefront site for the store
4. only then use the generic `.mybigcommerce.com` fallback

## Recommended rollout phases

### Phase 1: current

- Flask engine
- Caddy hosting
- local + hosted use
- cached dashboard read model
- current private alpha

### Phase 2: production hardening

- explicit worker process for heavy jobs
- structured monitoring and alerts
- stable startup/service supervision
- channel-aware storefront URL support
- admin/user UI separation
- move public hosting from the Windows/Caddy fallback host to the Ubuntu VPS described in `docs/FULCRUM_VPS_PRODUCTION_MIGRATION.md`

### Phase 3: Gadget shell

- React/Vite embedded admin experience in Gadget
- Gadget talks to Flask internal APIs
- keep routing/publishing engine in Flask

### Phase 4: broader distribution

- install/onboarding polish
- stronger tenant isolation and observability
- Marketplace submission and support workflows

## Recommendation

For now:

- `keep Flask`
- `move heavy work into workers`
- `use Gadget later as the shell`

That gives us the fastest path to reliable production without rewriting the engine before the product logic is fully settled.
