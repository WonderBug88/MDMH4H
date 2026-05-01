"""Operational snapshot and status formatting helpers for Fulcrum."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Callable


def format_timestamp_display(value: Any) -> str:
    if isinstance(value, datetime):
        return value.astimezone().strftime("%Y-%m-%d %I:%M %p")
    text = (str(value or "")).strip()
    if not text:
        return ""
    try:
        return datetime.fromisoformat(text).astimezone().strftime("%Y-%m-%d %I:%M %p")
    except ValueError:
        return text


def format_relative_time(value: Any) -> str:
    if not isinstance(value, datetime):
        return ""
    now_utc = datetime.now().astimezone()
    try:
        delta = now_utc - value.astimezone()
    except ValueError:
        delta = now_utc - value.replace(tzinfo=now_utc.tzinfo)
    if delta.total_seconds() < 60:
        return "just now"
    minutes = int(delta.total_seconds() // 60)
    if minutes < 60:
        return f"{minutes}m ago"
    hours = minutes // 60
    if hours < 24:
        return f"{hours}h ago"
    days = hours // 24
    if days < 30:
        return f"{days}d ago"
    return value.astimezone().strftime("%Y-%m-%d")


def alert_severity_rank(severity: str) -> int:
    normalized = (severity or "").strip().lower()
    if normalized == "urgent":
        return 3
    if normalized == "watch":
        return 2
    return 1


def alert_tone_for_severity(severity: str) -> str:
    normalized = (severity or "").strip().lower()
    if normalized == "urgent":
        return "red"
    if normalized == "watch":
        return "amber"
    return "green"


def build_operational_snapshot(
    store_hash: str,
    *,
    runs: list[dict[str, Any]] | None = None,
    active_run: dict[str, Any] | None = None,
    readiness: dict[str, Any] | None = None,
    publication_summary: dict[str, int] | None = None,
    edge_case_requests: list[dict[str, Any]] | None = None,
    gate_summary: dict[str, Any] | None = None,
    normalize_store_hash_fn: Callable[[str], str],
    list_runs_fn: Callable[..., list[dict[str, Any]]],
    refresh_store_readiness_fn: Callable[[str], dict[str, Any]],
    summarize_live_publications_fn: Callable[[str], dict[str, int]],
    summarize_query_gate_dispositions_fn: Callable[[str], dict[str, Any]],
    category_publishing_enabled_for_store_fn: Callable[[str], bool],
    category_theme_hook_present_fn: Callable[[], bool],
    generation_active_statuses: set[str],
    active_run_watch_after: timedelta,
    active_run_urgent_after: timedelta,
    completed_run_watch_after: timedelta,
    completed_run_urgent_after: timedelta,
    edge_case_watch_count: int,
    edge_case_urgent_count: int,
) -> dict[str, Any]:
    normalized_hash = normalize_store_hash_fn(store_hash)
    runs = list(runs or list_runs_fn(normalized_hash, limit=10))
    readiness = dict(readiness or refresh_store_readiness_fn(normalized_hash))
    publication_summary = dict(publication_summary or summarize_live_publications_fn(normalized_hash))
    edge_case_requests = list(edge_case_requests or [])
    gate_summary = dict(gate_summary or summarize_query_gate_dispositions_fn(normalized_hash))
    active_run = active_run or next(
        (run for run in runs if (run.get("status") or "").strip().lower() in generation_active_statuses),
        None,
    )
    latest_completed_run = next(
        (run for run in runs if (run.get("status") or "").strip().lower() == "completed"),
        None,
    )
    failed_runs = [run for run in runs if (run.get("status") or "").strip().lower() == "failed"]
    alerts: list[dict[str, Any]] = []

    def add_alert(severity: str, title: str, detail: str) -> None:
        alerts.append(
            {
                "severity": severity,
                "severity_label": severity.title(),
                "tone": alert_tone_for_severity(severity),
                "title": title,
                "detail": detail,
            }
        )

    if not readiness.get("catalog_synced"):
        add_alert("urgent", "Catalog is not synced", "Run Sync Catalog before generating or publishing new results.")
    if not readiness.get("theme_hook_ready"):
        add_alert("urgent", "Product theme hook is missing", "Product link blocks cannot render until the product theme hook is restored.")
    if category_publishing_enabled_for_store_fn(normalized_hash) and not category_theme_hook_present_fn():
        add_alert("watch", "Category theme hook is missing", "Category link blocks are enabled but category pages cannot render them right now.")
    unresolved_name_count = int(readiness.get("unresolved_option_name_count") or 0)
    unresolved_value_count = int(readiness.get("unresolved_option_value_count") or 0)
    unresolved_total = unresolved_name_count + unresolved_value_count
    if unresolved_total > 0:
        add_alert(
            "watch",
            "Attribute mappings still need review",
            f"{unresolved_total} option mapping item(s) are unresolved, which can weaken routing confidence.",
        )
    if not latest_completed_run:
        add_alert("urgent", "No completed generation run yet", "Rerun Routing Pipeline once after syncing so Fulcrum has a publishable decision set.")
    else:
        completed_at = latest_completed_run.get("completed_at") or latest_completed_run.get("started_at")
        if isinstance(completed_at, datetime):
            age = datetime.now().astimezone() - completed_at.astimezone()
            if age > completed_run_urgent_after:
                add_alert(
                    "urgent",
                    "Generation results are stale",
                    f"The latest completed run was {format_relative_time(completed_at)}. Weekly generation may have been missed.",
                )
            elif age > completed_run_watch_after:
                add_alert(
                    "watch",
                    "Generation results are getting old",
                    f"The latest completed run was {format_relative_time(completed_at)}. Consider refreshing results.",
                )
    if active_run:
        started_at = active_run.get("started_at")
        if isinstance(started_at, datetime):
            age = datetime.now().astimezone() - started_at.astimezone()
            if age > active_run_urgent_after:
                add_alert(
                    "urgent",
                    "A generation run looks stuck",
                    f"Run {active_run.get('run_id')} has been {active_run.get('status')} for {format_relative_time(started_at)}.",
                )
            elif age > active_run_watch_after:
                add_alert(
                    "watch",
                    "A generation run is taking longer than usual",
                    f"Run {active_run.get('run_id')} has been {active_run.get('status')} for {format_relative_time(started_at)}.",
                )
    if failed_runs:
        latest_failed = failed_runs[0]
        failed_started = latest_failed.get("started_at")
        failed_detail = (
            f"Latest failure was run {latest_failed.get('run_id')}"
            + (f" ({format_relative_time(failed_started)})" if isinstance(failed_started, datetime) else "")
            + "."
        )
        if latest_failed.get("notes"):
            failed_detail += f" {latest_failed.get('notes')}"
        add_alert("watch", "Recent generation failures detected", failed_detail)
    if publication_summary.get("total_live_blocks", 0) == 0 and int(gate_summary.get("pass") or 0) > 0:
        add_alert(
            "watch",
            "Nothing is live on the storefront yet",
            f"{int(gate_summary.get('pass') or 0)} query families passed the gate, but there are no live Fulcrum blocks on the site.",
        )
    edge_case_count = len(edge_case_requests)
    if edge_case_count >= edge_case_urgent_count:
        add_alert(
            "urgent",
            "Review queue is piling up",
            f"{edge_case_count} edge cases are waiting for re-analysis. Fulcrum may be drifting on important queries.",
        )
    elif edge_case_count >= edge_case_watch_count:
        add_alert(
            "watch",
            "Review queue needs attention",
            f"{edge_case_count} edge cases are waiting for re-analysis.",
        )
    elif edge_case_count > 0:
        add_alert(
            "healthy",
            "A few edge cases are queued",
            f"{edge_case_count} result(s) are waiting for review, which is normal during tuning.",
        )

    if not alerts:
        add_alert(
            "healthy",
            "Store looks healthy",
            "Catalog, generation, publishing, and review flow all look normal right now.",
        )

    alerts.sort(key=lambda item: alert_severity_rank(item.get("severity") or ""), reverse=True)
    urgent_count = sum(1 for item in alerts if item.get("severity") == "urgent")
    watch_count = sum(1 for item in alerts if item.get("severity") == "watch")
    healthy_count = sum(1 for item in alerts if item.get("severity") == "healthy")
    overall_status = "urgent" if urgent_count else "watch" if watch_count else "healthy"
    overall_label = "Needs Attention" if overall_status == "urgent" else "Watching" if overall_status == "watch" else "Healthy"

    system_cards = [
        {
            "label": "Catalog Sync",
            "value": "Ready" if readiness.get("catalog_synced") else "Needs sync",
            "detail": f"Last checked {format_timestamp_display(readiness.get('updated_at')) or 'not yet'}",
            "tone": "green" if readiness.get("catalog_synced") else "red",
        },
        {
            "label": "Auto Publish",
            "value": "Ready" if readiness.get("auto_publish_ready") else "Blocked",
            "detail": "Products and categories can publish automatically" if readiness.get("auto_publish_ready") else "A readiness dependency is still missing",
            "tone": "green" if readiness.get("auto_publish_ready") else "amber",
        },
        {
            "label": "Latest Completed Run",
            "value": f"Run {latest_completed_run.get('run_id')}" if latest_completed_run else "None yet",
            "detail": (
                f"Completed {format_relative_time(latest_completed_run.get('completed_at') or latest_completed_run.get('started_at'))}"
                if latest_completed_run
                else "Rerun Routing Pipeline after syncing"
            ),
            "tone": "green" if latest_completed_run else "red",
        },
        {
            "label": "Live Storefront Blocks",
            "value": str(int(publication_summary.get("total_live_blocks") or 0)),
            "detail": "Active Fulcrum blocks currently rendered on the site",
            "tone": "green" if int(publication_summary.get("total_live_blocks") or 0) > 0 else "amber",
        },
    ]

    return {
        "overall_status": overall_status,
        "overall_status_label": overall_label,
        "overall_tone": alert_tone_for_severity(overall_status),
        "counts": {
            "urgent": urgent_count,
            "watch": watch_count,
            "healthy": healthy_count,
        },
        "alerts": alerts,
        "system_cards": system_cards,
    }


__all__ = [
    "alert_severity_rank",
    "alert_tone_for_severity",
    "build_operational_snapshot",
    "format_relative_time",
    "format_timestamp_display",
]
