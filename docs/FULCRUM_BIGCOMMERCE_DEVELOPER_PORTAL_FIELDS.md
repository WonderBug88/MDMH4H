# Route Authority BigCommerce Developer Portal Fill Sheet

Last refreshed: 2026-04-22

This is the exact field-by-field prep sheet for the BigCommerce Developer Portal based on the current Route Authority app and the current BigCommerce publishing docs.

Official references:
- Publishing guide: https://developer.bigcommerce.com/docs/integrations/apps/guide/publishing
- Callback guide: https://developer.bigcommerce.com/docs/integrations/apps/guide/callbacks
- Multiple users guide: https://developer.bigcommerce.com/docs/integrations/apps/guide/users
- Developer Portal guide: https://developer.bigcommerce.com/docs/integrations/apps/guide/developer-portal
- Unified Billing overview: https://developer.bigcommerce.com/docs/integrations/apps/unified-billing

## Route Authority: use these exact values

### App Information tab

- App name:
  - `Route Authority`
- Auth callback URL:
  - `https://fulcrum.fulcrumagentics.com/fulcrum/auth`
- Load callback URL:
  - `https://fulcrum.fulcrumagentics.com/fulcrum/load`
- Uninstall callback URL:
  - `https://fulcrum.fulcrumagentics.com/fulcrum/uninstall`
- Enable multiple users:
  - `Yes`
- Remove user callback URL:
  - `https://fulcrum.fulcrumagentics.com/fulcrum/remove-user`

Why this setting:
- Route Authority already implements `auth`, `load`, `uninstall`, and `remove-user`.
- The app stores access at the store level and already handles non-owner load plus remove-user callbacks.

### OAuth Scopes tab

Use:
- `Set manually`

Do not use:
- `Modify all`

Current instruction for this submission:
- Trim to the minimum scopes Route Authority actually needs before review.
- Do not submit with broad test scopes.

Working scope audit checklist:
- Keep only scopes required for install, catalog reads, storefront/theme verification work, content/metafield publishing, and any settings Route Authority actually modifies.
- Remove any scope that was added for experimentation but is not required by the current live app path.
- Re-test install, load, setup, publish, uninstall, and remove-user after trimming.

### Listing Information tab

#### Marketplace profile

- Contact Name:
  - Use the email attached to the Partner account that owns the listing.
- Partner Name:
  - `Hotels for Humanity`
- Partner Website:
  - `https://www.hotels4humanity.com`
- Support Email:
  - `support@hotels4humanity.com`
  - This is the current best known mailbox from the live install record. Replace only if you want a different support mailbox on the listing.
- Support Website:
  - `https://fulcrum.fulcrumagentics.com/fulcrum/support`
- Partner ID:
  - Use the Partner Portal assigned value.
- Marketplace category:
  - Recommended: choose the closest category for SEO / merchandising workflow tooling in the current portal.
  - Current product fit recommendation: `Marketing` if available; otherwise use the closest search/SEO or merchandising category shown in the portal.

#### Marketplace copy

- App summary:
  - `Route Authority turns store, Search Console, and GA4 data into publish-ready internal linking recommendations for BigCommerce merchants.`
- Full description:
  - `Route Authority helps BigCommerce teams connect Search Console, GA4, and store catalog data so they can generate, review, and publish internal-linking recommendations inside a single embedded app. Merchants can complete setup, verify publishing readiness, review queue activity, and manage app-generated publishing from the BigCommerce control panel.`

#### Features

Use these 5 features max:
- `Merchant setup workflow` — Connect BigCommerce, Search Console, and GA4 inside one embedded setup flow.
- `Publishing readiness checks` — Verify catalog sync, storefront hook status, and publish settings before going live.
- `Internal-link recommendations` — Turn store and search data into reviewable internal-link routing opportunities.
- `Review queue and admin control` — Send disputed results to review without leaving the app workflow.
- `Live publishing visibility` — See what is published, what is waiting, and what still needs setup.

#### Resources and legal

- Company privacy policy:
  - `https://fulcrum.fulcrumagentics.com/fulcrum/privacy`
- Company terms of service:
  - `https://fulcrum.fulcrumagentics.com/fulcrum/terms`
- Installation guide:
  - `https://fulcrum.fulcrumagentics.com/fulcrum/guide`
- User guide:
  - `https://fulcrum.fulcrumagentics.com/fulcrum/guide`

#### Media assets

- App icon:
  - `app/fulcrum/static/fulcrum/route-authority-icon.png`
- Primary logo:
  - `app/fulcrum/static/fulcrum/route-authority-logo.png`
- Alternate logo:
  - Create only if the portal requires it for featured placement.
- App screenshots:
  - `docs/bigcommerce_marketplace_assets/2026-04-22/route-authority-setup-2026-04-22.png`
  - `docs/bigcommerce_marketplace_assets/2026-04-22/route-authority-results-2026-04-22.png`
  - `docs/bigcommerce_marketplace_assets/2026-04-22/route-authority-review-2026-04-22.png`
  - `docs/bigcommerce_marketplace_assets/2026-04-22/route-authority-developer-2026-04-22.png`
  - Optional legal screenshot: `docs/bigcommerce_marketplace_assets/2026-04-22/route-authority-terms-2026-04-22.png`
- Videos:
  - Optional for first submission.
- Case studies:
  - Optional for first submission.

#### Pricing / billing

For the first submission use:
- Pricing model: `Free to use`

Do not configure yet:
- Unified Billing
- Free trial terms
- Recurring paid plans
- One-time charges

#### Storefront compatibility

- Multiple users:
  - `Enabled`
- Multi-storefront compatibility:
  - Recommended current listing value: `Single Storefront`
  - Change only after explicit MSF validation in a sandbox and reviewer-ready testing.

## Pre-submit checklist

### Production URL checks

All of these must return `200` on production before submission:
- `https://fulcrum.fulcrumagentics.com/fulcrum/privacy`
- `https://fulcrum.fulcrumagentics.com/fulcrum/support`
- `https://fulcrum.fulcrumagentics.com/fulcrum/terms`

### Embedded app behavior checks

- `auth` callback works from a clean install.
- `load` callback opens a usable iframe page.
- `uninstall` callback clears store-scoped app data correctly.
- `remove-user` callback handles revoked user access correctly.
- Setup page shows privacy, support, and terms links.
- Results page no longer shows alpha wording.
- Developer page shows Marketplace review readiness, install source, callback URLs, legal URLs, and readiness checks.

### Sandbox reviewer pass

Run the reviewer flow in:
- `docs/FULCRUM_BIGCOMMERCE_REVIEWER_RUNBOOK.md`

Specifically verify:
- owner install
- non-owner load
- remove-user behavior
- uninstall behavior
- legal links
- iframe usability
- current support destination

### Portal hygiene checks

- All callback URLs are HTTPS and fully qualified.
- Scopes are manually selected and minimized.
- Support email, support website, partner website, privacy, and terms are filled.
- Screenshots match the current public-facing product copy.
- Listing copy does not mention alpha, pilot, or private install.
- Pricing is set to `Free to use`.

## Known owner-input fields still needed

These still need final owner confirmation before submission:
- Partner account Contact Name
- Partner ID
- Final marketplace category chosen from the current portal dropdown
- Final support mailbox if you want something other than `support@hotels4humanity.com`
- Optional demo video URL



