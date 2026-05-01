from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from app.fulcrum import services


@dataclass(frozen=True)
class LogicRegressionCase:
    case_id: str
    query: str
    source_url: str
    expected_intent_scope: str | None = None
    expected_preferred_entity_type: str | None = None
    expected_winner_entity_type: str | None = None
    expected_winner_name_contains: tuple[str, ...] = ()


DEFAULT_LOGIC_REGRESSION_CASES: tuple[LogicRegressionCase, ...] = (
    LogicRegressionCase(
        case_id="hotel-curtains-category",
        query="hotel curtains",
        source_url="/fabric-curtains/",
        expected_intent_scope="broad_product_family",
        expected_preferred_entity_type="category",
        expected_winner_entity_type="category",
        expected_winner_name_contains=("Hotel Shower Curtains",),
    ),
    LogicRegressionCase(
        case_id="hotel-sheets-category",
        query="hotel sheets",
        source_url="/hotel-bedding-supply/",
        expected_intent_scope="broad_product_family",
        expected_preferred_entity_type="category",
        expected_winner_entity_type="category",
        expected_winner_name_contains=("Hotel Bedding Supply", "Hotel Top Sheet", "Hotel Fitted Sheets"),
    ),
    LogicRegressionCase(
        case_id="luxury-hotel-robes-category",
        query="luxury hotel robes",
        source_url="/spa-hotel-bath-robes/",
        expected_intent_scope="broad_product_family",
        expected_preferred_entity_type="category",
        expected_winner_entity_type="category",
        expected_winner_name_contains=("Spa Hotel Bath Robes",),
    ),
    LogicRegressionCase(
        case_id="box-spring-cover-category",
        query="box spring cover",
        source_url="/hotel-box-spring-covers-wraps/",
        expected_intent_scope="broad_product_family",
        expected_preferred_entity_type="category",
        expected_winner_entity_type="category",
        expected_winner_name_contains=("Hotel Box Spring Covers",),
    ),
    LogicRegressionCase(
        case_id="hookless-curtain-category",
        query="hookless shower curtain",
        source_url="/hookless-shower-curtains/",
        expected_intent_scope="commercial_topic",
        expected_preferred_entity_type="category",
        expected_winner_entity_type="category",
        expected_winner_name_contains=("Hookless Shower Curtains",),
    ),
    LogicRegressionCase(
        case_id="wholesale-shower-curtains-suppliers-route-to-category",
        query="wholesale shower curtains suppliers",
        source_url="/ganesh-mills-hookless-shower-curtains-71x74-pack-of-12/",
        expected_intent_scope="commercial_topic",
        expected_preferred_entity_type="category",
        expected_winner_entity_type="category",
        expected_winner_name_contains=("Hookless Shower Curtains",),
    ),
    LogicRegressionCase(
        case_id="courtyard-curtain-product",
        query="courtyard shower curtain",
        source_url="/hotel-shower-curtains/",
        expected_intent_scope="specific_product",
        expected_preferred_entity_type="product",
        expected_winner_entity_type="product",
        expected_winner_name_contains=("Courtyard Waffle Polyester Hotel Shower Curtain",),
    ),
    LogicRegressionCase(
        case_id="ganesh-curtain-product",
        query="ganesh shower curtain",
        source_url="/hotel-shower-curtains/",
        expected_intent_scope="specific_product",
        expected_preferred_entity_type="product",
        expected_winner_entity_type="product",
        expected_winner_name_contains=("Ganesh Mills",),
    ),
    LogicRegressionCase(
        case_id="kartri-curtain-product",
        query="kartri shower curtain",
        source_url="/hotel-shower-curtains/",
        expected_intent_scope="specific_product",
        expected_preferred_entity_type="product",
        expected_winner_entity_type="product",
        expected_winner_name_contains=("KARTRI", "Kartri"),
    ),
    LogicRegressionCase(
        case_id="mills-brand-page",
        query="1888 mills",
        source_url="/hotel-towels/",
        expected_intent_scope="brand_navigation",
        expected_preferred_entity_type="brand",
        expected_winner_entity_type="brand",
        expected_winner_name_contains=("1888 Mills",),
    ),
    LogicRegressionCase(
        case_id="downlite-blankets-brand-page",
        query="downlite blankets",
        source_url="/downlite-wholesale-bedding-enviroloft/",
        expected_intent_scope="brand_navigation",
        expected_preferred_entity_type="brand",
        expected_winner_entity_type="brand",
        expected_winner_name_contains=("DownLite Bedding",),
    ),
    LogicRegressionCase(
        case_id="downlite-hospitality-pillows-category",
        query="downlite hospitality pillows",
        source_url="/downlite-hospitality-pillow/",
        expected_intent_scope="commercial_topic",
        expected_preferred_entity_type="category",
        expected_winner_entity_type="category",
        expected_winner_name_contains=("Downlite Hospitality Pillow",),
    ),
    LogicRegressionCase(
        case_id="bedbug-cover-stays-category",
        query="bedbug cover",
        source_url="/bed-bug-mattress-covers/",
        expected_intent_scope="commercial_topic",
        expected_preferred_entity_type="category",
        expected_winner_entity_type="category",
        expected_winner_name_contains=("Bed Bug Mattress Covers",),
    ),
    LogicRegressionCase(
        case_id="bedbug-mattress-stays-category",
        query="bedbug mattress",
        source_url="/bed-bug-mattress-covers/",
        expected_intent_scope="commercial_topic",
        expected_preferred_entity_type="category",
        expected_winner_entity_type="category",
        expected_winner_name_contains=("Bed Bug Mattress Covers",),
    ),
    LogicRegressionCase(
        case_id="hotel-style-mattress-stays-category",
        query="buy hotel-style luxury mattress",
        source_url="/hotel-mattress-sets/",
        expected_intent_scope="broad_product_family",
        expected_preferred_entity_type="category",
        expected_winner_entity_type="category",
        expected_winner_name_contains=("Hotel Mattress Sets",),
    ),
    LogicRegressionCase(
        case_id="cuisinart-stockpot-stays-product",
        query="cuisinart 6 qt stock pot",
        source_url="/cuisinart-6-quart-stockpot-anodized-w-glass-cover-pack-of-4/",
        expected_intent_scope="specific_product",
        expected_preferred_entity_type="product",
        expected_winner_entity_type="product",
        expected_winner_name_contains=("6 Quart Stockpot",),
    ),
    LogicRegressionCase(
        case_id="damask-sheets-preserve-current-product",
        query="damask sheets",
        source_url="/soft-dimensions-white-damask-stripe-200-gsm-top-sheet/",
        expected_winner_entity_type="product",
        expected_winner_name_contains=("Damask Stripe", "SOFT DIMENSIONS", "Soft Dimensions"),
    ),
    LogicRegressionCase(
        case_id="down-alternative-pillow-preserves-alt-down-category",
        query="down alternative hotel pillow",
        source_url="/synthetic-alternative-down/",
        expected_preferred_entity_type="category",
        expected_winner_entity_type="category",
        expected_winner_name_contains=("Synthetic / Alternative Down",),
    ),
    LogicRegressionCase(
        case_id="hospital-bed-bedding-stays-category",
        query="hospital bed bedding",
        source_url="/bulk-healthcare-bed-linens-hospital-bed-sheets/",
        expected_preferred_entity_type="category",
        expected_winner_entity_type="category",
        expected_winner_name_contains=("Bulk Healthcare Bed Linens", "Hospital Bed Sheets"),
    ),
    LogicRegressionCase(
        case_id="twin-rollaway-stays-rollaway",
        query="twin rollaway bed",
        source_url="/rollaway-portable-foldable-beds-for-hotels/",
        expected_winner_entity_type="category",
        expected_winner_name_contains=("Rollaway / Portable / Foldable Beds for hotels",),
    ),
    LogicRegressionCase(
        case_id="five-star-pillows-do-not-route-to-cases",
        query="5 star hotel pillows",
        source_url="/five-star-hotel-microdenier-gel-fiber-pillow-all-sizes/",
        expected_preferred_entity_type="category",
        expected_winner_entity_type="category",
        expected_winner_name_contains=("Hotel Pillow", "Five Star Hotel"),
    ),
    LogicRegressionCase(
        case_id="bellhop-cart-stays-bellman-family",
        query="bellhop cart",
        source_url="/bellman-cart-contemporary-series-all-styles/",
        expected_preferred_entity_type="category",
        expected_winner_entity_type="category",
        expected_winner_name_contains=("Bellman", "Birdcage"),
    ),
    LogicRegressionCase(
        case_id="bellman-carts-route-to-bellman-category",
        query="bellman carts",
        source_url="/bellman-cart-contemporary-series-all-styles/",
        expected_intent_scope="broad_product_family",
        expected_preferred_entity_type="category",
        expected_winner_entity_type="category",
        expected_winner_name_contains=("Bellman", "Birdcage"),
    ),
    LogicRegressionCase(
        case_id="hotel-cleaning-supplies-route-to-cleaning-supplies",
        query="hotel cleaning supplies",
        source_url="/housekeeping-janitorial/",
        expected_intent_scope="broad_product_family",
        expected_preferred_entity_type="category",
        expected_winner_entity_type="category",
        expected_winner_name_contains=("Cleaning Supplies",),
    ),
    LogicRegressionCase(
        case_id="gym-towels-route-to-gym-towels-category",
        query="gym towels",
        source_url="/1888-mills-best-gym-towels-for-sports-fitness/",
        expected_intent_scope="broad_product_family",
        expected_preferred_entity_type="category",
        expected_winner_entity_type="category",
        expected_winner_name_contains=("Gym Towels",),
    ),
    LogicRegressionCase(
        case_id="hotel-housekeeping-supplies-stays-current-category",
        query="hotel housekeeping supplies",
        source_url="/housekeeping-janitorial/",
        expected_intent_scope="broad_product_family",
        expected_preferred_entity_type="category",
        expected_winner_entity_type="category",
        expected_winner_name_contains=("Housekeeping & Janitorial",),
    ),
    LogicRegressionCase(
        case_id="hotel-bedding-stays-current-category",
        query="hotel bedding",
        source_url="/hotel-bedding-supply/",
        expected_intent_scope="broad_product_family",
        expected_preferred_entity_type="category",
        expected_winner_entity_type="category",
        expected_winner_name_contains=("Hotel Bedding Supply",),
    ),
    LogicRegressionCase(
        case_id="king-size-beds-stay-broad",
        query="hotels with king size beds",
        source_url="/hotel-bed-frame/",
        expected_preferred_entity_type="category",
        expected_winner_entity_type="category",
        expected_winner_name_contains=("Hotel Bed", "Bed Frame"),
    ),
    LogicRegressionCase(
        case_id="hotel-suite-pillows-do-not-route-to-brand",
        query="hotel suite pillows",
        source_url="/hotel-pillow/",
        expected_preferred_entity_type="category",
        expected_winner_entity_type="category",
        expected_winner_name_contains=("Hotel Pillow",),
    ),
)

LOGIC_CHANGELOG_PATH = Path(services.Config.BASE_DIR) / "docs" / "FULCRUM_LOGIC_CHANGELOG.json"


def _normalize_case_name(name: str | None) -> str:
    return (name or "").strip().lower()


def _load_logic_changelog_entries(changelog_path: Path | None = None) -> list[dict[str, Any]]:
    path = changelog_path or LOGIC_CHANGELOG_PATH
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    if not isinstance(payload, list):
        return []
    return [dict(item) for item in payload if isinstance(item, dict)]


def _case_ids_for_queries(queries: list[str] | tuple[str, ...]) -> list[str]:
    normalized_queries = {(query or "").strip().lower() for query in queries if (query or "").strip()}
    case_ids: list[str] = []
    for case in DEFAULT_LOGIC_REGRESSION_CASES:
        if (case.query or "").strip().lower() in normalized_queries:
            case_ids.append(case.case_id)
    return case_ids


def resolve_logic_change_case_ids(change_id: str, changelog_path: Path | None = None) -> list[str]:
    normalized_change_id = _normalize_case_name(change_id)
    for entry in _load_logic_changelog_entries(changelog_path=changelog_path):
        if _normalize_case_name(entry.get("change_id")) != normalized_change_id:
            continue
        return _case_ids_for_queries(entry.get("affected_queries") or [])
    return []


def _find_existing_gate_row(
    run_rows: list[dict[str, Any]],
    case: LogicRegressionCase,
) -> dict[str, Any] | None:
    query = case.query.strip().lower()
    source_url = (case.source_url or "").strip().lower()
    for row in run_rows:
        if (row.get("representative_query") or "").strip().lower() != query:
            continue
        if source_url and (row.get("source_url") or "").strip().lower() != source_url:
            continue
        return row
    return None


def _build_synthetic_gate_row(
    store_hash: str,
    case: LogicRegressionCase,
    entity_index: dict[str, Any],
    signal_library: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    source_profile = (entity_index.get("sources") or {}).get(case.source_url)
    if not source_profile:
        raise ValueError(f"Source page not found in entity index: {case.source_url}")

    resolved_signals = services._resolve_query_signal_context(
        store_hash=store_hash,
        example_query=case.query,
        signal_library=signal_library,
        source_profile=source_profile,
    )
    query_tokens = set(resolved_signals.get("query_tokens") or [])
    query_attrs = {key: set(values or []) for key, values in (resolved_signals.get("query_attrs") or {}).items()}
    query_brand_tokens = {
        token
        for signal in (resolved_signals.get("brand_signals") or [])
        for token in (signal.get("matched_tokens") or [])
    }
    query_intent_scope, preferred_entity_type = services._classify_query_intent_scope(
        example_query=case.query,
        query_tokens=query_tokens,
        query_attrs=query_attrs,
        query_brand_tokens=query_brand_tokens,
        resolved_signals=resolved_signals,
    )
    return {
        "gate_record_id": 0,
        "store_hash": store_hash,
        "representative_query": case.query,
        "normalized_query_key": services._normalize_query_family_key(case.query),
        "source_url": source_profile.get("url"),
        "source_name": source_profile.get("name"),
        "source_entity_type": source_profile.get("entity_type"),
        "source_entity_id": source_profile.get("bc_entity_id"),
        "current_page_type": source_profile.get("entity_type"),
        "preferred_entity_type": preferred_entity_type,
        "query_intent_scope": query_intent_scope,
        "clicks_90d": 0,
        "impressions_90d": 100,
        "metadata": {
            "resolved_signals": resolved_signals,
            "semantics_analysis": services._build_query_semantics_analysis(
                store_hash=store_hash,
                example_query=case.query,
                resolved_signals=resolved_signals,
                signal_library=signal_library,
            ),
            "synthetic_case": True,
        },
    }


def _evaluate_case(
    case: LogicRegressionCase,
    gate_row: dict[str, Any],
    ranked_targets: list[dict[str, Any]],
) -> dict[str, Any]:
    winner = ranked_targets[0] if ranked_targets else {}
    failures: list[str] = []

    actual_intent_scope = (gate_row.get("query_intent_scope") or "").strip().lower()
    actual_preferred = (gate_row.get("preferred_entity_type") or "").strip().lower()
    actual_winner_type = (winner.get("entity_type") or "").strip().lower()
    actual_winner_name = winner.get("name") or ""

    if case.expected_intent_scope and actual_intent_scope != case.expected_intent_scope:
        failures.append(f"intent_scope expected `{case.expected_intent_scope}` but got `{actual_intent_scope or 'missing'}`")
    if case.expected_preferred_entity_type and actual_preferred != case.expected_preferred_entity_type:
        failures.append(
            f"preferred_entity_type expected `{case.expected_preferred_entity_type}` but got `{actual_preferred or 'missing'}`"
        )
    if case.expected_winner_entity_type and actual_winner_type != case.expected_winner_entity_type:
        failures.append(
            f"winner.entity_type expected `{case.expected_winner_entity_type}` but got `{actual_winner_type or 'missing'}`"
        )
    if case.expected_winner_name_contains:
        expected_matches = tuple(value for value in case.expected_winner_name_contains if value)
        if expected_matches and not any(value.lower() in actual_winner_name.lower() for value in expected_matches):
            failures.append(
                "winner.name did not contain any expected match: "
                + ", ".join(f"`{value}`" for value in expected_matches)
            )

    return {
        "case_id": case.case_id,
        "query": case.query,
        "source_url": gate_row.get("source_url") or case.source_url,
        "expected": {
            "intent_scope": case.expected_intent_scope,
            "preferred_entity_type": case.expected_preferred_entity_type,
            "winner_entity_type": case.expected_winner_entity_type,
            "winner_name_contains": list(case.expected_winner_name_contains),
        },
        "actual": {
            "intent_scope": actual_intent_scope,
            "preferred_entity_type": actual_preferred,
            "winner": winner,
            "second_option": ranked_targets[1] if len(ranked_targets) > 1 else None,
        },
        "status": "passed" if not failures else "failed",
        "failures": failures,
        "synthetic_case": bool(((gate_row.get("metadata") or {}).get("synthetic_case"))),
    }


def run_logic_regression(
    store_hash: str,
    run_id: int | None = None,
    case_ids: list[str] | None = None,
) -> dict[str, Any]:
    resolved_store_hash = services.normalize_store_hash(store_hash)
    resolved_run_id = run_id or services._latest_gate_run_id(resolved_store_hash)
    entity_index = services._build_unified_entity_index(resolved_store_hash)
    signal_library = services._build_store_signal_library(resolved_store_hash)
    overrides = services._load_query_target_overrides(resolved_store_hash)
    feedback_maps = services._load_review_feedback_maps(resolved_store_hash)
    run_rows = (
        services.list_query_gate_records(resolved_store_hash, run_id=resolved_run_id, limit=5000)
        if resolved_run_id
        else []
    )

    selected_cases = [
        case
        for case in DEFAULT_LOGIC_REGRESSION_CASES
        if not case_ids or case.case_id in {_normalize_case_name(case_id) for case_id in case_ids}
    ]

    results: list[dict[str, Any]] = []
    for case in selected_cases:
        gate_row = _find_existing_gate_row(run_rows, case)
        if gate_row is None:
            gate_row = _build_synthetic_gate_row(resolved_store_hash, case, entity_index, signal_library)
        else:
            gate_row = services._refresh_query_gate_row_live_state(
                resolved_store_hash,
                gate_row,
                entity_index.get("sources") or {},
                entity_index.get("targets") or [],
                signal_library,
            )
        ranked_targets = services._rank_target_options_for_gate_row(
            gate_row=gate_row,
            source_profiles=entity_index.get("sources") or {},
            target_entities=entity_index.get("targets") or [],
            overrides=overrides,
            review_feedback_maps=feedback_maps,
            limit=2,
        )
        results.append(_evaluate_case(case, gate_row, ranked_targets))

    failed = [result for result in results if result["status"] == "failed"]
    return {
        "status": "ok" if not failed else "failed",
        "store_hash": resolved_store_hash,
        "run_id": resolved_run_id,
        "generated_at": datetime.now().astimezone().isoformat(),
        "case_count": len(results),
        "passed_count": len(results) - len(failed),
        "failed_count": len(failed),
        "results": results,
    }


def record_regression_against_logic_changelog(
    payload: dict[str, Any],
    changelog_path: Path | None = None,
    change_ids: list[str] | None = None,
) -> dict[str, Any]:
    path = changelog_path or LOGIC_CHANGELOG_PATH
    if not path.exists():
        return {"status": "missing", "updated_count": 0, "path": str(path)}
    try:
        raw_entries = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"status": "error", "updated_count": 0, "path": str(path)}
    if not isinstance(raw_entries, list):
        return {"status": "error", "updated_count": 0, "path": str(path)}

    results = list(payload.get("results") or [])
    results_by_query = {
        (result.get("query") or "").strip().lower(): result
        for result in results
        if (result.get("query") or "").strip()
    }
    updated_entries: list[dict[str, Any]] = []
    updated_count = 0
    normalized_change_ids = {_normalize_case_name(change_id) for change_id in (change_ids or []) if change_id}
    for entry in raw_entries:
        if not isinstance(entry, dict):
            updated_entries.append(entry)
            continue
        updated = dict(entry)
        entry_change_id = _normalize_case_name(updated.get("change_id"))
        if normalized_change_ids and entry_change_id not in normalized_change_ids:
            updated_entries.append(updated)
            continue
        affected_queries = [
            (query or "").strip().lower()
            for query in (updated.get("affected_queries") or [])
            if (query or "").strip()
        ]
        relevant_results = [results_by_query[query] for query in affected_queries if query in results_by_query]
        if relevant_results:
            failed_results = [result for result in relevant_results if result.get("status") != "passed"]
            validation_status = "verified_pass" if not failed_results else "verified_fail"
            updated["validation"] = {
                "status": validation_status,
                "store_hash": payload.get("store_hash"),
                "run_id": payload.get("run_id"),
                "verified_at": payload.get("generated_at"),
                "case_count": len(relevant_results),
                "passed_count": len(relevant_results) - len(failed_results),
                "failed_count": len(failed_results),
                "failed_queries": [result.get("query") for result in failed_results],
                "failed_case_ids": [result.get("case_id") for result in failed_results],
            }
            updated_count += 1
        else:
            updated.setdefault(
                "validation",
                {
                    "status": "untested",
                    "store_hash": payload.get("store_hash"),
                    "run_id": payload.get("run_id"),
                    "verified_at": payload.get("generated_at"),
                    "case_count": 0,
                    "passed_count": 0,
                    "failed_count": 0,
                    "failed_queries": [],
                    "failed_case_ids": [],
                },
            )
        updated_entries.append(updated)

    path.write_text(json.dumps(updated_entries, indent=2), encoding="utf-8")
    return {"status": "ok", "updated_count": updated_count, "path": str(path)}


def format_logic_regression_report(payload: dict[str, Any]) -> str:
    lines = [
        f"Fulcrum logic regression for {payload.get('store_hash')}",
        f"Run ID: {payload.get('run_id') or 'synthetic-only'}",
        f"Cases: {payload.get('case_count', 0)}",
        f"Passed: {payload.get('passed_count', 0)}",
        f"Failed: {payload.get('failed_count', 0)}",
        "",
    ]
    for result in payload.get("results") or []:
        winner = ((result.get("actual") or {}).get("winner") or {})
        lines.append(f"[{result.get('status').upper()}] {result.get('case_id')}: {result.get('query')}")
        lines.append(
            "  actual: "
            f"{(result.get('actual') or {}).get('intent_scope') or 'missing'} / "
            f"{(result.get('actual') or {}).get('preferred_entity_type') or 'missing'} / "
            f"{winner.get('entity_type') or 'missing'} -> {winner.get('name') or 'missing'}"
        )
        if result.get("failures"):
            for failure in result["failures"]:
                lines.append(f"  - {failure}")
        if result.get("synthetic_case"):
            lines.append("  - evaluated with a synthetic gate row")
        lines.append("")
    return "\n".join(lines).rstrip()


def format_logic_regression_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, indent=2, default=str)
