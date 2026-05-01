# Fulcrum BigCommerce Marketplace Plan

## Current State

- Fulcrum is live behind Caddy at `https://fulcrum.fulcrumagentics.com`
- the BigCommerce embedded app is already loading from the control panel
- the app can be priced as `Free to use`
- production BigCommerce link metafields have been reset and republished from the current Fulcrum outputs

## Immediate Free Distribution

If the goal is `free right now`, use this order:

1. keep the app `Draft` or `Unlisted` while alpha testing
2. set pricing to `Free to use` in the Developer Portal
3. install only on approved test/customer stores first
4. keep the callback URLs on:
   - `https://fulcrum.fulcrumagentics.com/fulcrum/auth`
   - `https://fulcrum.fulcrumagentics.com/fulcrum/load`
   - `https://fulcrum.fulcrumagentics.com/fulcrum/uninstall`
   - `https://fulcrum.fulcrumagentics.com/fulcrum/remove-user`

This gives merchants a free install path immediately without waiting for Marketplace review.

## Marketplace Submission Checklist

Before submitting Fulcrum for public Marketplace approval:

1. confirm multi-user support is enabled
2. keep all callback URLs on HTTPS
3. trim scopes to the minimum needed
4. fill out all listing fields with real content
5. add support email, support website, partner website, privacy policy, and terms of service
6. add logo, screenshots, and a short demo/video if available
7. add onboarding instructions inside the iframe app
8. make sure install, load, uninstall, and remove-user all work on a sandbox store
9. test against a multi-storefront capable store if you want to claim MSF support
10. keep `Free to use` selected if the merchant-facing price should remain zero

## Notes On Cost

- `Free to use` means free for merchants
- public Marketplace submission is still a separate review/listing process
- if the goal is zero-cost merchant access immediately, `Draft` / `Unlisted` is the fastest path

## Why This Order

- `Draft / Unlisted` gets Fulcrum into stores immediately
- `Marketplace approval` adds discoverability later
- this keeps product work moving while listing, support, and legal assets are finished


