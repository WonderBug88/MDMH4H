# Route Authority BigCommerce Reviewer Runbook

## Goal

Verify that Route Authority installs cleanly, loads in the BigCommerce iframe, exposes setup and legal surfaces, supports non-owner access, and handles user removal plus uninstall cleanup.

## Sandbox Test Flow

1. Install Route Authority from the BigCommerce app entry and authorize the requested scopes.
2. Confirm the app lands inside the embedded iframe and shows the Route Authority setup flow.
3. On setup, confirm the page shows:
   - BigCommerce install state
   - Search Console connection
   - GA4 connection
   - publishing settings
   - privacy, support, and terms links
4. Open the Results page and confirm the app renders merchant-facing Route Authority content inside the iframe without alpha-only wording.
5. Open the Developer page and confirm the Marketplace checklist renders install status, callback configuration, legal URLs, and current readiness.

## Multi-User Flow

1. Log in as a non-owner admin user who has access to the installed app.
2. Launch Route Authority from the BigCommerce control panel.
3. Confirm the app loads in the iframe and does not require a reinstall.
4. Revoke that user from the store and verify the remove-user callback path succeeds without breaking owner access.

## Uninstall Flow

1. Uninstall Route Authority from the sandbox store.
2. Confirm the uninstall callback runs and the app clears store-scoped runtime data as expected.
3. Reinstall and confirm the app returns to the setup or results surface based on readiness.

## Legal and Support Checks

- Privacy page must return `200`
- Support page must return `200`
- Terms page must return `200`
- Support, privacy, and terms links should be reachable from merchant-facing app surfaces
