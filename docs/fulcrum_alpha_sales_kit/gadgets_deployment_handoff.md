# Gadgets Deployment Handoff

## Purpose

Gadgets is the hosted shell for the private BigCommerce app. Flask remains the scoring and publishing engine.

## BigCommerce Callback Targets

- Auth: `/fulcrum/auth`
- Load: `/fulcrum/load`
- Uninstall: `/fulcrum/uninstall`
- Remove user: `/fulcrum/remove-user`

## Gadgets To Flask Internal API

- `POST /fulcrum/api/internal-links/runs/generate`
- `POST /fulcrum/api/internal-links/catalog/sync`
- `GET /fulcrum/api/internal-links/dashboard-context`
- `GET /fulcrum/api/internal-links/preview/<source_entity_type>/<source_entity_id>`
- `POST /fulcrum/api/internal-links/reviews/bulk`
- `POST /fulcrum/api/internal-links/publish`
- `POST /fulcrum/api/internal-links/unpublish`

## Required Headers

Either:

- `X-Fulcrum-Shared-Secret`

Or:

- `X-Fulcrum-Timestamp`
- `X-Fulcrum-Signature`

The HMAC digest is computed over:

`<timestamp>\n<body>`

using SHA-256 and the shared secret.

## Required Env

- `FULCRUM_GADGETS_API_KEY`
- `FULCRUM_BC_CLIENT_ID`
- `FULCRUM_BC_CLIENT_SECRET`
- `FULCRUM_BC_ACCOUNT_UUID`
- `FULCRUM_APP_BASE_URL`
- `FULCRUM_AUTH_CALLBACK_URL`
- `FULCRUM_LOAD_CALLBACK_URL`
- `FULCRUM_UNINSTALL_CALLBACK_URL`
- `FULCRUM_REMOVE_USER_CALLBACK_URL`
- `FULCRUM_ALLOWED_STORES`
- `FULCRUM_SHARED_SECRET`
- `FULCRUM_THEME_PRODUCT_TEMPLATE`
- `FULCRUM_THEME_CATEGORY_TEMPLATE`
- `FULCRUM_ENABLE_CATEGORY_PUBLISHING`
- `FULCRUM_AUTO_PUBLISH_ENABLED`
- `FULCRUM_AUTO_PUBLISH_MIN_SCORE`
- `FULCRUM_AUTO_PUBLISH_MAX_LINKS_PER_SOURCE`
- `FULCRUM_REQUIRE_REVIEW_FOR_CATEGORIES`

## Current Launch Posture

- managed private alpha
- product-page publishing on
- category publishing off by default
- auto-publish only when the store is readiness-complete
