# Fulcrum Go-Live Checklist

This is the practical launch checklist for running Fulcrum live with a human in the loop.

The goal is not perfect routing on day one. The goal is:

- publish confidently
- catch edge cases through the review bucket
- monitor whether Fulcrum-managed pages improve in GSC
- turn repeated mistakes into better code over time

## Launch Rule

Fulcrum is live when all of these are true:

- the local app is healthy
- the hosted app is healthy
- BigCommerce callbacks are pointing at `https://fulcrum.fulcrumagentics.com`
- theme hooks are present
- the customer review queue is visible
- `Publish All Results` is available

## Before Go-Live

1. Open the user view:
   - `http://127.0.0.1:5077/fulcrum/?store_hash=99oa2tso`
2. Open the admin view:
   - `http://127.0.0.1:5077/fulcrum/admin?store_hash=99oa2tso`
3. Confirm local and hosted health:
   - `http://127.0.0.1:5077/fulcrum/health?store_hash=99oa2tso`
   - `https://fulcrum.fulcrumagentics.com/fulcrum/health?store_hash=99oa2tso`
4. Run `Sync Catalog`.
5. Run `Rerun Routing Pipeline`.
6. Review the main results screen for anything obviously wrong.
7. If results look good, run `Publish All Results`.

## Daily Operating Order

1. `Sync Catalog`
2. `Rerun Routing Pipeline`
3. `Publish All Results`
4. Check `Customer Review Queue`
5. Check `GSC Value On Fulcrum Pages`

## What The User Does

The user only needs to do two things:

- trust the published results when they look right
- hit `Review Bucket` when something looks wrong

That review action should be treated as the user saying:

- "this may be an edge case"
- "pause this live block"
- "admin should look at this later"

## What Admin Watches

Admin should focus on two questions:

1. Are customers hitting the review bucket?
2. Are Fulcrum-managed pages gaining value in GSC?

The main admin signals are:

- `Customer Review Queue`
- `GSC Value On Fulcrum Pages`

## What Counts As Healthy

Healthy launch-week behavior looks like:

- small review queue
- review bucket fills slowly, not all at once
- Fulcrum pages keep publishing successfully
- GSC CTR holds or improves
- GSC average position improves gradually
- no repeated outage or failed-run pattern

## What Counts As Normal

These are normal and do not mean the launch failed:

- some edge cases land in the review bucket
- some broad queries route imperfectly
- not every family publishes immediately
- current 90-day GSC numbers may lag before value is obvious

## What Counts As A Real Problem

These need action quickly:

- hosted health check fails
- generation stops completing
- `Publish All Results` stops creating live blocks
- review queue piles up fast
- the same routing mistake keeps appearing across many families
- published blocks disappear unexpectedly

## First 7 Days

### Day 1

- run `Sync Catalog`
- run `Rerun Routing Pipeline`
- run `Publish All Results`
- verify at least a few live pages manually
- check the review bucket at the end of the day

### Days 2-3

- keep the same daily operating order
- inspect the first group of edge cases
- only change logic if you see a repeated pattern

### Days 4-7

- compare `GSC Value On Fulcrum Pages`
- look for CTR and rank movement on Fulcrum-managed pages
- keep logging repeated edge-case themes
- turn repeated themes into deterministic rule updates

## Human-In-The-Loop Rule

Do not try to eliminate all uncertainty before launch.

Instead:

- let Fulcrum publish
- let humans catch the weird cases
- let GSC tell us whether the system is creating value
- tighten the rules from real production feedback

## If Something Looks Off

Use this triage order:

1. Check health endpoints
2. Check whether the result is already in `Review Bucket`
3. Check whether the page is actually live
4. Check whether the issue is one odd case or a repeated pattern
5. Only then change routing logic

## Command-Line Checks

Watchdog:

```powershell
cd C:\Users\juddu\Downloads\PAM\MDMH4H
python .\deploy\run_fulcrum_watchdog.py --store-hash 99oa2tso
```

If needed, restart local Fulcrum:

```powershell
cd C:\Users\juddu\Downloads\PAM\MDMH4H
.\deploy\restart_fulcrum_local_5077.ps1
```

## Launch Summary

The launch plan is:

- go live now
- publish confidently
- use the review bucket as the safety valve
- use GSC as the value scoreboard
- improve logic from repeated real-world failures, not theoretical ones


