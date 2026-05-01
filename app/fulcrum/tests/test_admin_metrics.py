import sys
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

APP_ROOT = ROOT / "MDMH4H"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from app.fulcrum import admin_metrics


class _FakeCursor:
    def __init__(self, *, fetchone_results=None, fetchall_results=None):
        self.executions = []
        self._fetchone_results = list(fetchone_results or [])
        self._fetchall_results = list(fetchall_results or [])

    def execute(self, sql, params=None):
        self.executions.append((sql, params))

    def fetchone(self):
        return self._fetchone_results.pop(0) if self._fetchone_results else None

    def fetchall(self):
        return self._fetchall_results.pop(0) if self._fetchall_results else []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeConnection:
    def __init__(self, cursors):
        self._cursors = list(cursors)
        self._last_cursor = self._cursors[-1] if self._cursors else _FakeCursor()

    def cursor(self, *args, **kwargs):
        if self._cursors:
            self._last_cursor = self._cursors.pop(0)
        return self._last_cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FulcrumAdminMetricsTests(unittest.TestCase):
    def test_candidate_gsc_page_values_adds_path_and_storefront_variants(self):
        values = admin_metrics.candidate_gsc_page_values(
            "99oa2tso",
            ["/hotel-shower-curtains/", "/"],
            normalize_storefront_path_fn=lambda value: str(value or "").strip(),
            storefront_base_urls_fn=lambda store_hash: ["https://example.com"],
        )

        self.assertIn("/hotel-shower-curtains/", values)
        self.assertIn("/hotel-shower-curtains", values)
        self.assertIn("https://example.com/hotel-shower-curtains/", values)
        self.assertIn("https://example.com/", values)

    def test_summarize_gsc_routing_coverage_uses_run_counts(self):
        cursor = _FakeCursor(fetchall_results=[[("pass", 2, 20), ("hold", 1, 5), ("reject", 1, 1)]])
        conn = _FakeConnection([cursor])

        with patch("app.fulcrum.admin_metrics.get_pg_conn", return_value=conn):
            summary = admin_metrics.summarize_gsc_routing_coverage(
                "Stores/99OA2TSO",
                latest_gate_run_id_fn=lambda store_hash: 44,
            )

        self.assertEqual(summary["run_id"], 44)
        self.assertEqual(summary["family_count"], 4)
        self.assertEqual(summary["pass_variant_count"], 20)
        self.assertEqual(summary["hold_family_pct"], 25.0)
        self.assertEqual(cursor.executions[0][1], ("99oa2tso", 44))

    def test_summarize_blocked_gate_families_groups_block_reasons(self):
        summary = admin_metrics.summarize_blocked_gate_families(
            "99oa2tso",
            latest_gate_run_id_fn=lambda store_hash: 44,
            list_query_gate_records_fn=lambda *args, **kwargs: [
                {"gate_record_id": 8, "disposition": "hold", "avg_position_90d": 4.0, "representative_query": "hookless shower curtains"},
                {"gate_record_id": 9, "disposition": "reject", "noise_penalty": 35.0, "representative_query": "best thing for hotel"},
            ],
        )

        self.assertEqual(summary["hold_count"], 1)
        self.assertEqual(summary["reject_count"], 1)
        self.assertEqual(summary["categories"][0]["key"], "top_10_hold")
        self.assertEqual(summary["categories"][1]["key"], "too_noisy")

    def test_summarize_gsc_alignment_counts_by_target_type(self):
        summary = admin_metrics.summarize_gsc_alignment(
            "99oa2tso",
            latest_gate_run_id_fn=lambda store_hash: 44,
            list_query_gate_records_fn=lambda *args, **kwargs: [
                {"gate_record_id": 1, "suggested_target": {"entity_type": "product"}},
                {"gate_record_id": 2, "suggested_target": {"entity_type": "category"}},
                {"gate_record_id": 3, "suggested_target": {}},
            ],
            attach_cached_query_gate_suggestions_fn=lambda rows: rows,
            row_current_page_matches_winner_fn=lambda row: {1: (True, True), 2: (False, False), 3: (False, False)}[row["gate_record_id"]],
        )

        self.assertEqual(summary["aligned_count"], 1)
        self.assertEqual(summary["wrong_type_count"], 1)
        self.assertEqual(summary["missing_winner_count"], 1)
        self.assertEqual(len(summary["by_target_type"]), 2)

    def test_summarize_live_gsc_performance_builds_metric_rows_and_takeaway(self):
        cursor = _FakeCursor(
            fetchone_results=[
                {
                    "anchor_end": datetime(2026, 4, 15, 12, 0, 0),
                    "periods": {
                        "current_90": {"clicks": 220, "impressions": 10000, "ctr": 0.022, "avg_position": 3.5},
                        "prior_90": {"clicks": 180, "impressions": 9000, "ctr": 0.020, "avg_position": 4.8},
                        "year_prior_90": {"clicks": 160, "impressions": 8500, "ctr": 0.019, "avg_position": 5.4},
                    },
                }
            ]
        )
        conn = _FakeConnection([cursor])

        with patch("app.fulcrum.admin_metrics.get_pg_conn", return_value=conn):
            summary = admin_metrics.summarize_live_gsc_performance(
                "99oa2tso",
                list_publications_fn=lambda *args, **kwargs: [{"source_url": "/hotel-shower-curtains/"}],
                normalize_storefront_path_fn=lambda value: str(value or "").strip(),
                candidate_gsc_page_values_fn=lambda store_hash, paths: ["/hotel-shower-curtains/"],
                format_timestamp_display_fn=lambda value: "display",
            )

        self.assertEqual(summary["page_count"], 1)
        self.assertEqual(summary["anchor_end_date"], "display")
        self.assertEqual(len(summary["metric_rows"]), 4)
        self.assertEqual(summary["takeaway"]["title"], "Fulcrum pages are gaining momentum")
        self.assertEqual(cursor.executions[0][1], ("99oa2tso", "99oa2tso", ["/hotel-shower-curtains/"]))

    def test_summarize_live_gsc_performance_returns_empty_state_without_store_scoped_data(self):
        cursor = _FakeCursor(
            fetchone_results=[
                {
                    "anchor_end": None,
                    "periods": {},
                }
            ]
        )
        conn = _FakeConnection([cursor])

        with patch("app.fulcrum.admin_metrics.get_pg_conn", return_value=conn):
            summary = admin_metrics.summarize_live_gsc_performance(
                "99oa2tso",
                list_publications_fn=lambda *args, **kwargs: [{"source_url": "/hotel-shower-curtains/"}],
                normalize_storefront_path_fn=lambda value: str(value or "").strip(),
                candidate_gsc_page_values_fn=lambda store_hash, paths: ["/hotel-shower-curtains/"],
                format_timestamp_display_fn=lambda value: "display",
            )

        self.assertEqual(summary["page_count"], 1)
        self.assertEqual(summary["metric_rows"], [])
        self.assertEqual(summary["takeaway"]["badge"], "Awaiting data")

    def test_get_cached_live_gsc_performance_uses_cache_then_refreshes(self):
        cached = admin_metrics.get_cached_live_gsc_performance(
            "99oa2tso",
            load_admin_metric_cache_fn=lambda store_hash, metric_key: {"takeaway": {"title": "Cached"}, "comparison_chart_rows": []},
            store_admin_metric_cache_fn=lambda store_hash, metric_key, payload: {"stored": True},
            summarize_live_gsc_performance_fn=lambda store_hash: {"fresh": True},
        )
        self.assertEqual(cached["takeaway"]["title"], "Cached")

        refreshed = admin_metrics.get_cached_live_gsc_performance(
            "99oa2tso",
            load_admin_metric_cache_fn=lambda store_hash, metric_key: {"takeaway": {}, "comparison_chart_rows": None},
            store_admin_metric_cache_fn=lambda store_hash, metric_key, payload: dict(payload, stored=True),
            summarize_live_gsc_performance_fn=lambda store_hash: {"fresh": True},
        )
        self.assertTrue(refreshed["stored"])
        self.assertTrue(refreshed["fresh"])


if __name__ == "__main__":
    unittest.main()
