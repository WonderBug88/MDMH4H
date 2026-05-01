# Fulcrum Install And Onboarding Checklist

## Before Install

- Confirm BigCommerce store hash
- Confirm Fulcrum app is allowlisted for the store
- Confirm active theme includes the product metafield hook for `h4h/internal_links_html`
- Confirm BigCommerce app scopes are active
- Confirm Gadgets and Flask secrets are rotated and stored in host-managed env storage

## Install

- Install the draft/private `Fulcrum: Internal Link Orchestrator` app on the target store
- Verify auth callback succeeds
- Verify load callback opens the embedded app
- Confirm installation row appears in `app_runtime.store_installations`

## Catalog Onboarding

- Run catalog sync
- Verify products and categories were profiled
- Review unresolved option-name mappings
- Review unresolved option-value mappings
- Confirm `catalog_synced` and `attribute_mappings_ready`

## Theme Verification

- Verify product theme hook is present
- Keep category rendering gated unless category beta is specifically being tested
- Record any theme touches in `theme_work/THEME_CHANGE_LOG.md`

## First Candidate Run

- Generate one cluster-specific run first
- Check pending candidate quality
- Approve or reject anything below alpha confidence standards
- Confirm auto-publish only triggers if store readiness is complete

## Go-Live Checklist

- At least one sandbox proof captured
- Rollback path documented
- Weekly report template prepared
- Pilot scope agreed
