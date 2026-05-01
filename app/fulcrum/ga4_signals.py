"""GA4 scoring helpers for Fulcrum."""

from __future__ import annotations

import math
from typing import Any


def build_ga4_signal(
    target_profile: dict[str, Any] | None,
    target_entity_type: str,
    query_intent_scope: str | None = None,
) -> dict[str, Any]:
    metrics = dict((target_profile or {}).get("ga4_metrics") or {})
    if not metrics:
        return {"active": False, "delta": 0.0, "reason": "", "summary": "", "metrics": {}}

    target_type = (target_entity_type or "").strip().lower() or "product"
    query_scope = (query_intent_scope or "").strip().lower()

    organic_sessions = float(metrics.get("organic_sessions_90d") or 0.0)
    organic_engaged = float(metrics.get("organic_engaged_sessions_90d") or 0.0)
    organic_engagement_rate = float(metrics.get("organic_engagement_rate_90d") or 0.0)
    organic_add_to_carts = float(metrics.get("organic_add_to_carts_90d") or 0.0)
    organic_purchases = float(metrics.get("organic_purchases_90d") or 0.0)
    organic_revenue = float(metrics.get("organic_revenue_90d") or 0.0)

    sessions = float(metrics.get("sessions_90d") or 0.0)
    engaged = float(metrics.get("engaged_sessions_90d") or 0.0)
    engagement_rate = float(metrics.get("engagement_rate_90d") or 0.0)
    add_to_carts = float(metrics.get("add_to_carts_90d") or 0.0)
    purchases = float(metrics.get("purchases_90d") or 0.0)
    revenue = float(metrics.get("revenue_90d") or 0.0)

    primary_sessions = organic_sessions or sessions
    primary_engaged = organic_engaged or engaged
    primary_engagement_rate = organic_engagement_rate or engagement_rate
    primary_add_to_carts = organic_add_to_carts or add_to_carts
    primary_purchases = organic_purchases or purchases
    primary_revenue = organic_revenue or revenue

    delta = 0.0
    reason = ""
    summary = ""

    if target_type == "product":
        delta += min(math.log1p(primary_add_to_carts) * 1.15, 2.5)
        delta += min(math.log1p(primary_purchases) * 2.1, 3.0)
        delta += min(math.log1p(max(primary_revenue, 0.0)) * 0.6, 2.0)
        delta += min(math.log1p(primary_sessions) * 0.45, 1.25)
        if primary_purchases > 0:
            reason = "GA4 shows this PDP already converts"
            summary = "GA4 conversions support this PDP"
        elif primary_add_to_carts > 0:
            reason = "GA4 shows this PDP gets meaningful cart intent"
            summary = "GA4 cart intent supports this PDP"
        elif primary_sessions >= 20 and primary_engagement_rate >= 0.6:
            reason = "GA4 shows this PDP keeps visitors engaged"
            summary = "GA4 engagement supports this PDP"
    elif target_type in {"category", "brand"}:
        delta += min(math.log1p(primary_sessions) * 0.5, 2.0)
        delta += min(primary_engagement_rate * 2.5, 2.5)
        delta += min(math.log1p(primary_add_to_carts) * 0.5, 1.0)
        if primary_sessions >= 30 and primary_engagement_rate >= 0.55:
            reason = "GA4 shows this browse page keeps shoppers engaged"
            summary = "GA4 engagement supports this browse page"
    elif target_type == "content":
        delta += min(math.log1p(primary_sessions) * 0.55, 2.25)
        delta += min(primary_engagement_rate * 3.0, 3.0)
        if query_scope == "informational":
            delta += 1.0
        if primary_sessions >= 20 and primary_engagement_rate >= 0.6:
            reason = "GA4 shows this guide/content page earns engaged visits"
            summary = "GA4 engagement supports this guide"

    if primary_sessions >= 25 and primary_engagement_rate < 0.18:
        delta -= 1.5
        if not reason:
            reason = "GA4 shows weak engagement on this page"
            summary = "GA4 weakens this target"

    delta = max(-2.0, min(6.0, round(delta, 2)))
    if delta > 0 and not reason:
        reason = "GA4 behavior slightly supports this target"
    if delta > 0 and not summary:
        summary = "GA4 supports this target"
    active = abs(delta) >= 0.5 and bool(primary_sessions or primary_add_to_carts or primary_purchases or primary_revenue)
    return {
        "active": active,
        "delta": delta,
        "reason": reason,
        "summary": summary,
        "metrics": {
            "sessions_90d": int(primary_sessions),
            "engaged_sessions_90d": int(primary_engaged),
            "engagement_rate_90d": round(primary_engagement_rate, 4),
            "add_to_carts_90d": int(primary_add_to_carts),
            "purchases_90d": int(primary_purchases),
            "revenue_90d": round(primary_revenue, 2),
            "organic_preferred": bool(organic_sessions),
        },
    }


__all__ = ["build_ga4_signal"]
