# Fulcrum Rollback And Support SOP

## When To Roll Back

- merchant reports confusing or off-topic links
- unexpected publication volume
- wrong cluster bleed
- product template hook issue
- QA finds mismatched anchors or broken destination URLs

## Rollback Steps

1. identify affected source entities
2. use Fulcrum unpublish flow for those entity ids
3. verify BigCommerce metafields were removed
4. verify storefront block no longer renders
5. mark issue in operator notes

## If Theme Rendering Is Involved

- confirm active theme UUID
- confirm whether the hook changed
- log the theme state in `theme_work/THEME_CHANGE_LOG.md`
- do not enable category beta again until sandbox proves the fix

## Support Triage

- mapping issue: review unresolved attribute mappings
- scoring issue: inspect reason summary and shared cluster evidence
- publish issue: verify theme hook and store readiness
- API issue: verify Gadgets shared secret/HMAC headers

## Recovery Goal

- restore a clean storefront state without manual HTML edits
- preserve audit history in Fulcrum runtime tables
