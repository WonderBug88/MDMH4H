"""Admin and quality metric summaries for Fulcrum."""

from __future__ import annotations

from typing import Any, Callable

from psycopg2.extras import RealDictCursor

from app.fulcrum.platform import get_pg_conn, normalize_store_hash


def percent_share(numerator: int | float, denominator: int | float) -> float:
    if not denominator:
        return 0.0
    return round((float(numerator) / float(denominator)) * 100.0, 1)


def format_change_value(value: float | int | None, *, suffix: str = "", invert_good: bool = False) -> dict[str, Any]:
    numeric = float(value or 0.0)
    direction = "flat"
    if abs(numeric) >= 0.0001:
        direction = "down" if numeric < 0 else "up"
    tone = "gray"
    if direction != "flat":
        positive_is_good = not invert_good
        is_good = (numeric > 0 and positive_is_good) or (numeric < 0 and not positive_is_good)
        tone = "green" if is_good else "red"
    prefix = "+" if numeric > 0 else ""
    if suffix == "%":
        display = f"{prefix}{numeric:.1f}%"
    elif suffix == " pos":
        display = f"{prefix}{numeric:.2f}{suffix}"
    else:
        display = f"{prefix}{numeric:,.0f}{suffix}"
    if direction == "flat":
        display = "0"
    return {
        "value": numeric,
        "display": display,
        "tone": tone,
        "direction": direction,
    }


def summarize_gsc_routing_coverage(
    store_hash: str,
    run_id: int | None = None,
    *,
    latest_gate_run_id_fn: Callable[[str], int | None],
) -> dict[str, Any]:
    resolved_run_id = run_id or latest_gate_run_id_fn(store_hash)
    if not resolved_run_id:
        return {
            "run_id": None,
            "family_count": 0,
            "raw_variant_count": 0,
            "pass_count": 0,
            "hold_count": 0,
            "reject_count": 0,
            "pass_variant_count": 0,
            "hold_variant_count": 0,
            "reject_variant_count": 0,
            "pass_family_pct": 0.0,
            "hold_family_pct": 0.0,
            "reject_family_pct": 0.0,
            "pass_variant_pct": 0.0,
            "hold_variant_pct": 0.0,
            "reject_variant_pct": 0.0,
        }

    sql = """
        SELECT
            disposition,
            COUNT(*) AS family_count,
            SUM(COALESCE((metadata->>'query_variant_count')::int, 0)) AS raw_variant_count
        FROM app_runtime.query_gate_records
        WHERE store_hash = %s
          AND run_id = %s
        GROUP BY disposition;
    """
    counts = {
        "pass": {"families": 0, "variants": 0},
        "hold": {"families": 0, "variants": 0},
        "reject": {"families": 0, "variants": 0},
    }
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (normalize_store_hash(store_hash), int(resolved_run_id)))
            for disposition, family_count, raw_variant_count in cur.fetchall():
                key = (str(disposition or "hold").strip().lower()) or "hold"
                if key not in counts:
                    counts[key] = {"families": 0, "variants": 0}
                counts[key]["families"] = int(family_count or 0)
                counts[key]["variants"] = int(raw_variant_count or 0)

    total_families = sum(bucket["families"] for bucket in counts.values())
    total_variants = sum(bucket["variants"] for bucket in counts.values())
    return {
        "run_id": resolved_run_id,
        "family_count": total_families,
        "raw_variant_count": total_variants,
        "pass_count": counts["pass"]["families"],
        "hold_count": counts["hold"]["families"],
        "reject_count": counts["reject"]["families"],
        "pass_variant_count": counts["pass"]["variants"],
        "hold_variant_count": counts["hold"]["variants"],
        "reject_variant_count": counts["reject"]["variants"],
        "pass_family_pct": percent_share(counts["pass"]["families"], total_families),
        "hold_family_pct": percent_share(counts["hold"]["families"], total_families),
        "reject_family_pct": percent_share(counts["reject"]["families"], total_families),
        "pass_variant_pct": percent_share(counts["pass"]["variants"], total_variants),
        "hold_variant_pct": percent_share(counts["hold"]["variants"], total_variants),
        "reject_variant_pct": percent_share(counts["reject"]["variants"], total_variants),
    }


def summarize_blocked_gate_families(
    store_hash: str,
    run_id: int | None = None,
    limit: int = 2000,
    *,
    latest_gate_run_id_fn: Callable[[str], int | None],
    list_query_gate_records_fn: Callable[..., list[dict[str, Any]]],
) -> dict[str, Any]:
    resolved_run_id = run_id or latest_gate_run_id_fn(store_hash)
    if not resolved_run_id:
        return {
            "run_id": None,
            "hold_count": 0,
            "reject_count": 0,
            "categories": [],
        }

    rows = list_query_gate_records_fn(store_hash, disposition=None, limit=limit, run_id=resolved_run_id)
    blocked_rows = [row for row in rows if (row.get("disposition") or "").strip().lower() in {"hold", "reject"}]

    category_map: dict[str, dict[str, Any]] = {}

    def ensure_bucket(key: str, label: str, description: str) -> dict[str, Any]:
        bucket = category_map.setdefault(
            key,
            {
                "key": key,
                "label": label,
                "description": description,
                "count": 0,
                "hold_count": 0,
                "reject_count": 0,
                "samples": [],
            },
        )
        return bucket

    for row in blocked_rows:
        disposition = (row.get("disposition") or "hold").strip().lower()
        avg_position = float(row.get("avg_position_90d") or 999.0)
        query_scope = (row.get("query_intent_scope") or "").strip().lower()
        demand_score = float(row.get("demand_score") or 0.0)
        noise_penalty = float(row.get("noise_penalty") or 0.0)
        intent_score = float(row.get("intent_clarity_score") or 0.0)
        reason_summary = (row.get("reason_summary") or "").strip()
        reason_headline = reason_summary.split(";", 1)[0].strip() if reason_summary else ""
        if reason_headline and not reason_headline.endswith("."):
            reason_headline = f"{reason_headline}."
        current_page_type = (row.get("current_page_type") or row.get("source_entity_type") or "unknown").strip().lower()
        preferred_entity_type = (row.get("preferred_entity_type") or "unknown").strip().lower()

        if disposition == "hold" and avg_position <= 10.0:
            bucket = ensure_bucket("top_10_hold", "Already Ranking Well", "Fulcrum held these because Google is already ranking them in the top 10.")
        elif disposition == "reject" and noise_penalty >= 30.0:
            bucket = ensure_bucket("too_noisy", "Too Noisy Or Ambiguous", "Fulcrum rejected these because the wording is too noisy or ambiguous to route cleanly.")
        elif disposition == "reject" and demand_score < 12.0:
            bucket = ensure_bucket("low_demand", "Low Demand", "Fulcrum rejected these because the search demand is too weak to justify routing.")
        elif query_scope == "mixed_or_unknown" or intent_score < 55.0:
            bucket = ensure_bucket("low_clarity", "Low Intent Clarity", "Fulcrum held these because the family is real, but the intent is not clear enough yet.")
        else:
            bucket = ensure_bucket("other_blocked", "Other Holds Or Rejects", "Fulcrum kept these out of routing for a mix of demand, clarity, or safety reasons.")

        bucket["count"] += 1
        if disposition == "hold":
            bucket["hold_count"] += 1
        elif disposition == "reject":
            bucket["reject_count"] += 1
        if len(bucket["samples"]) < 5:
            bucket["samples"].append(
                {
                    "gate_record_id": int(row.get("gate_record_id") or 0),
                    "representative_query": row.get("representative_query") or "",
                    "reason_summary": reason_summary,
                    "reason_headline": reason_headline,
                    "source_name": row.get("source_name") or "",
                    "source_url": row.get("source_url") or "",
                    "current_page_type": current_page_type,
                    "preferred_entity_type": preferred_entity_type,
                    "current_page_type_label": current_page_type.replace("_", " ").title(),
                    "preferred_entity_type_label": preferred_entity_type.replace("_", " ").title(),
                    "disposition": disposition,
                    "disposition_label": "On hold" if disposition == "hold" else "Rejected",
                    "impressions_90d": int(row.get("impressions_90d") or 0),
                    "clicks_90d": int(row.get("clicks_90d") or 0),
                    "ctr_90d": float(row.get("ctr_90d") or 0.0),
                    "avg_position_90d": None if row.get("avg_position_90d") is None else float(row.get("avg_position_90d") or 0.0),
                    "query_intent_scope": query_scope,
                }
            )

    categories = list(category_map.values())
    categories.sort(key=lambda item: (-int(item.get("count") or 0), item.get("label") or ""))
    return {
        "run_id": resolved_run_id,
        "hold_count": sum(1 for row in blocked_rows if (row.get("disposition") or "").strip().lower() == "hold"),
        "reject_count": sum(1 for row in blocked_rows if (row.get("disposition") or "").strip().lower() == "reject"),
        "categories": categories,
    }


def candidate_gsc_page_values(
    store_hash: str,
    paths: list[str],
    *,
    normalize_storefront_path_fn: Callable[[Any], str],
    storefront_base_urls_fn: Callable[[str], list[str]],
) -> list[str]:
    values: set[str] = set()
    bases = storefront_base_urls_fn(store_hash)
    for raw_path in paths:
        normalized_path = normalize_storefront_path_fn(raw_path)
        if not normalized_path:
            continue
        path_variants = {normalized_path}
        if normalized_path != "/":
            path_variants.add(normalized_path.rstrip("/"))
        for item in path_variants:
            values.add(item)
            for base in bases:
                values.add(f"{base}{item}")
    return sorted(value for value in values if value)


def summarize_gsc_alignment(
    store_hash: str,
    run_id: int | None = None,
    *,
    latest_gate_run_id_fn: Callable[[str], int | None],
    list_query_gate_records_fn: Callable[..., list[dict[str, Any]]],
    attach_cached_query_gate_suggestions_fn: Callable[[list[dict[str, Any]]], list[dict[str, Any]]],
    row_current_page_matches_winner_fn: Callable[[dict[str, Any]], tuple[bool, bool]],
) -> dict[str, Any]:
    resolved_run_id = run_id or latest_gate_run_id_fn(store_hash)
    if not resolved_run_id:
        return {
            "run_id": None,
            "total_families": 0,
            "aligned_count": 0,
            "wrong_type_count": 0,
            "wrong_page_same_type_count": 0,
            "missing_winner_count": 0,
            "by_target_type": [],
        }

    gate_rows = list_query_gate_records_fn(store_hash, disposition=None, limit=2000, run_id=resolved_run_id)
    gate_rows = attach_cached_query_gate_suggestions_fn(gate_rows)

    overall = {
        "total_families": 0,
        "aligned_count": 0,
        "wrong_type_count": 0,
        "wrong_page_same_type_count": 0,
        "missing_winner_count": 0,
    }
    by_target_type: dict[str, dict[str, Any]] = {}

    def bucket_for(target_type: str) -> dict[str, Any]:
        label_map = {
            "product": "Product targets",
            "category": "Category targets",
            "brand": "Brand targets",
            "content": "Content targets",
        }
        bucket = by_target_type.setdefault(
            target_type,
            {
                "target_type": target_type,
                "label": label_map.get(target_type, f"{target_type.title()} targets"),
                "total_families": 0,
                "aligned_count": 0,
                "wrong_type_count": 0,
                "wrong_page_same_type_count": 0,
            },
        )
        return bucket

    for row in gate_rows:
        winner = dict(row.get("suggested_target") or {})
        if not winner:
            overall["missing_winner_count"] += 1
            continue
        target_type = ((winner.get("entity_type") or "unknown")).strip().lower() or "unknown"
        bucket = bucket_for(target_type)
        overall["total_families"] += 1
        bucket["total_families"] += 1
        is_exact_match, same_type = row_current_page_matches_winner_fn(row)
        if is_exact_match:
            overall["aligned_count"] += 1
            bucket["aligned_count"] += 1
        elif same_type:
            overall["wrong_page_same_type_count"] += 1
            bucket["wrong_page_same_type_count"] += 1
        else:
            overall["wrong_type_count"] += 1
            bucket["wrong_type_count"] += 1

    buckets = list(by_target_type.values())
    buckets.sort(key=lambda item: item.get("label") or "")
    return {
        "run_id": resolved_run_id,
        **overall,
        "by_target_type": buckets,
    }


def summarize_live_gsc_performance(
    store_hash: str,
    *,
    list_publications_fn: Callable[..., list[dict[str, Any]]],
    normalize_storefront_path_fn: Callable[[Any], str],
    candidate_gsc_page_values_fn: Callable[[str, list[str]], list[str]],
    format_timestamp_display_fn: Callable[[Any], str | None],
) -> dict[str, Any]:
    publications = list_publications_fn(store_hash, active_only=True, limit=2000)
    live_paths = sorted(
        {
            normalize_storefront_path_fn(row.get("source_url"))
            for row in publications
            if normalize_storefront_path_fn(row.get("source_url"))
        }
    )
    if not live_paths:
        return {
            "page_count": 0,
            "anchor_end_date": "",
            "periods": {},
            "metric_rows": [],
        }

    page_values = candidate_gsc_page_values_fn(store_hash, live_paths)
    if not page_values:
        return {
            "page_count": len(live_paths),
            "anchor_end_date": "",
            "periods": {},
            "metric_rows": [],
        }

    sql = """
        WITH max_date AS (
            SELECT MAX(date) AS anchor_end
            FROM app_runtime.store_gsc_daily
            WHERE store_hash = %s
        ),
        periodized AS (
            SELECT
                CASE
                    WHEN g.date BETWEEN (m.anchor_end - INTERVAL '89 days')::date AND m.anchor_end THEN 'current_90'
                    WHEN g.date BETWEEN (m.anchor_end - INTERVAL '179 days')::date AND (m.anchor_end - INTERVAL '90 days')::date THEN 'prior_90'
                    WHEN g.date BETWEEN (m.anchor_end - INTERVAL '1 year' - INTERVAL '89 days')::date AND (m.anchor_end - INTERVAL '1 year')::date THEN 'year_prior_90'
                    ELSE NULL
                END AS period_key,
                g.page,
                g.clicks,
                g.impressions,
                g.position
            FROM app_runtime.store_gsc_daily g
            CROSS JOIN max_date m
            WHERE m.anchor_end IS NOT NULL
              AND g.store_hash = %s
              AND g.page = ANY(%s::text[])
              AND g.date BETWEEN (m.anchor_end - INTERVAL '1 year' - INTERVAL '89 days')::date AND m.anchor_end
        ),
        summary AS (
            SELECT
                period_key,
                COUNT(DISTINCT page) AS page_count,
                SUM(clicks) AS clicks,
                SUM(impressions) AS impressions,
                CASE
                    WHEN SUM(impressions) > 0 THEN SUM(clicks)::double precision / SUM(impressions)
                    ELSE 0
                END AS ctr,
                CASE
                    WHEN SUM(impressions) > 0 THEN SUM(position * impressions)::double precision / SUM(impressions)
                    ELSE 0
                END AS avg_position
            FROM periodized
            WHERE period_key IS NOT NULL
            GROUP BY period_key
        )
        SELECT
            (SELECT anchor_end FROM max_date) AS anchor_end,
            COALESCE(jsonb_object_agg(
                summary.period_key,
                jsonb_build_object(
                    'page_count', summary.page_count,
                    'clicks', COALESCE(summary.clicks, 0),
                    'impressions', COALESCE(summary.impressions, 0),
                    'ctr', COALESCE(summary.ctr, 0),
                    'avg_position', COALESCE(summary.avg_position, 0)
                )
            ), '{}'::jsonb) AS periods
        FROM summary;
    """
    normalized_store_hash = normalize_store_hash(store_hash)
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (normalized_store_hash, normalized_store_hash, page_values))
            row = dict(cur.fetchone() or {})

    periods = dict(row.get("periods") or {})
    anchor_end = row.get("anchor_end")
    if not anchor_end or not periods:
        return {
            "page_count": len(live_paths),
            "anchor_end_date": "",
            "periods": {},
            "metric_rows": [],
            "comparison_chart_rows": [],
            "takeaway": {
                "label": "Fulcrum takeaway",
                "title": "Awaiting store-scoped search data",
                "body": "Connect and verify Search Console for this store, then Route Authority will populate the 90-day performance comparison from merchant-owned data only.",
                "badge": "Awaiting data",
            },
        }
    current = dict(periods.get("current_90") or {})
    prior = dict(periods.get("prior_90") or {})
    year_prior = dict(periods.get("year_prior_90") or {})

    def metric_value(metric_key: str, period: dict[str, Any]) -> float:
        return float(period.get(metric_key) or 0.0)

    def pct_change(current_value: float, baseline_value: float) -> float:
        if abs(baseline_value) < 0.0001:
            return 0.0 if abs(current_value) < 0.0001 else 100.0
        return ((current_value - baseline_value) / baseline_value) * 100.0

    def normalize_series(values: list[float], *, invert_good: bool = False) -> list[float]:
        scores: list[float] = []
        for raw_value in values:
            numeric_value = float(raw_value or 0.0)
            if invert_good:
                score = 0.0 if numeric_value <= 0 else (1.0 / numeric_value)
            else:
                score = max(numeric_value, 0.0)
            scores.append(score)
        max_score = max(scores) if any(score > 0 for score in scores) else 0.0
        normalized: list[float] = []
        for score in scores:
            if max_score <= 0.0 or score <= 0.0:
                normalized.append(0.0)
                continue
            pct = round((score / max_score) * 100.0, 1)
            normalized.append(max(pct, 8.0))
        return normalized

    metric_rows = []
    metric_definitions = [
        ("clicks", "Clicks", "", False),
        ("impressions", "Impressions", "", False),
        ("ctr", "CTR", "%", False),
        ("avg_position", "Average Position", " pos", True),
    ]
    for metric_key, label, suffix, invert_good in metric_definitions:
        current_value = metric_value(metric_key, current)
        prior_value = metric_value(metric_key, prior)
        year_prior_value = metric_value(metric_key, year_prior)
        if metric_key == "ctr":
            current_display = f"{current_value * 100:.2f}%"
            prior_display = f"{prior_value * 100:.2f}%"
            year_prior_display = f"{year_prior_value * 100:.2f}%"
            prior_change = format_change_value((current_value - prior_value) * 100.0, suffix="%", invert_good=invert_good)
            year_prior_change = format_change_value((current_value - year_prior_value) * 100.0, suffix="%", invert_good=invert_good)
        elif metric_key == "avg_position":
            current_display = f"{current_value:.2f}"
            prior_display = f"{prior_value:.2f}"
            year_prior_display = f"{year_prior_value:.2f}"
            prior_change = format_change_value(current_value - prior_value, suffix=" pos", invert_good=True)
            year_prior_change = format_change_value(current_value - year_prior_value, suffix=" pos", invert_good=True)
        else:
            current_display = f"{current_value:,.0f}"
            prior_display = f"{prior_value:,.0f}"
            year_prior_display = f"{year_prior_value:,.0f}"
            prior_change = format_change_value(pct_change(current_value, prior_value), suffix="%", invert_good=invert_good)
            year_prior_change = format_change_value(pct_change(current_value, year_prior_value), suffix="%", invert_good=invert_good)
        metric_rows.append(
            {
                "key": metric_key,
                "label": label,
                "current_display": current_display,
                "prior_display": prior_display,
                "year_prior_display": year_prior_display,
                "prior_change": prior_change,
                "year_prior_change": year_prior_change,
            }
        )

    metric_index = {row["key"]: row for row in metric_rows}
    comparison_chart_rows = []
    chart_definitions = (
        ("avg_position", "Rank", True),
        ("impressions", "Impressions", False),
        ("ctr", "CTR", False),
    )
    for metric_key, label, invert_good in chart_definitions:
        current_value = metric_value(metric_key, current)
        prior_value = metric_value(metric_key, prior)
        year_prior_value = metric_value(metric_key, year_prior)
        current_pct, prior_pct, year_prior_pct = normalize_series(
            [current_value, prior_value, year_prior_value],
            invert_good=invert_good,
        )
        metric_row = metric_index.get(metric_key) or {}
        comparison_chart_rows.append(
            {
                "metric": label,
                "current_pct": current_pct,
                "prior_pct": prior_pct,
                "year_prior_pct": year_prior_pct,
                "current_display": metric_row.get("current_display") or "0",
                "prior_display": metric_row.get("prior_display") or "0",
                "year_prior_display": metric_row.get("year_prior_display") or "0",
            }
        )

    clicks_row = metric_index.get("clicks") or {}
    impressions_row = metric_index.get("impressions") or {}
    ctr_row = metric_index.get("ctr") or {}
    avg_position_row = metric_index.get("avg_position") or {}

    clicks_vs_prior = float((clicks_row.get("prior_change") or {}).get("value") or 0.0)
    impressions_vs_prior = float((impressions_row.get("prior_change") or {}).get("value") or 0.0)
    ctr_vs_prior = float((ctr_row.get("prior_change") or {}).get("value") or 0.0)
    position_vs_prior = float((avg_position_row.get("prior_change") or {}).get("value") or 0.0)

    if impressions_vs_prior <= -8.0 and ctr_vs_prior > 0.0:
        takeaway_title = "Click efficiency improved while visibility softened"
    elif clicks_vs_prior > 5.0 and position_vs_prior < 0.0:
        takeaway_title = "Fulcrum pages are gaining momentum"
    elif clicks_vs_prior < -5.0 and ctr_vs_prior < 0.0:
        takeaway_title = "Traffic softened and page efficiency weakened"
    else:
        takeaway_title = "Fulcrum looks mixed, but directionally useful"

    takeaway_body = (
        f"Clicks are {clicks_row.get('prior_change', {}).get('display', '0')} vs the prior 90 days, "
        f"impressions are {impressions_row.get('prior_change', {}).get('display', '0')}, "
        f"CTR is {ctr_row.get('prior_change', {}).get('display', '0')}, "
        f"and average position is {avg_position_row.get('prior_change', {}).get('display', '0')}. "
        f"Against the same 90-day window last year, clicks are {clicks_row.get('year_prior_change', {}).get('display', '0')} "
        f"and average position is {avg_position_row.get('year_prior_change', {}).get('display', '0')}."
    )
    if impressions_vs_prior <= -8.0 and ctr_vs_prior > 0.0:
        takeaway_body += " That usually means the pages are attracting relatively better traffic even if total demand or visibility pulled back."
    elif clicks_vs_prior > 5.0 and position_vs_prior < 0.0:
        takeaway_body += " That combination usually means Fulcrum-managed pages are becoming easier for Google to trust and easier for searchers to choose."
    elif clicks_vs_prior < -5.0 and ctr_vs_prior < 0.0:
        takeaway_body += " That is the kind of pattern worth pairing with the customer review queue so we can catch routing drift early."
    else:
        takeaway_body += " It is a useful signal to keep watching while newly routed pages accumulate more live data."

    return {
        "page_count": len(live_paths),
        "anchor_end_date": format_timestamp_display_fn(anchor_end),
        "periods": periods,
        "metric_rows": metric_rows,
        "comparison_chart_rows": comparison_chart_rows,
        "takeaway": {
            "label": "Fulcrum takeaway",
            "title": takeaway_title,
            "body": takeaway_body,
            "badge": "Better story, faster scan",
        },
    }


def get_cached_live_gsc_performance(
    store_hash: str,
    *,
    force_refresh: bool = False,
    load_admin_metric_cache_fn: Callable[[str, str], dict[str, Any] | None],
    store_admin_metric_cache_fn: Callable[[str, str, dict[str, Any]], dict[str, Any]],
    summarize_live_gsc_performance_fn: Callable[[str], dict[str, Any]],
) -> dict[str, Any]:
    metric_key = "live_gsc_performance_store_scoped_v2"
    if not force_refresh:
        cached = load_admin_metric_cache_fn(store_hash, metric_key)
        if cached is not None and cached.get("takeaway") and cached.get("comparison_chart_rows") is not None:
            return cached
    payload = summarize_live_gsc_performance_fn(store_hash)
    return store_admin_metric_cache_fn(store_hash, metric_key, payload)


__all__ = [
    "candidate_gsc_page_values",
    "format_change_value",
    "get_cached_live_gsc_performance",
    "percent_share",
    "summarize_blocked_gate_families",
    "summarize_gsc_alignment",
    "summarize_gsc_routing_coverage",
    "summarize_live_gsc_performance",
]
