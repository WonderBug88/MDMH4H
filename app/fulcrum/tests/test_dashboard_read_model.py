import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch


ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

APP_ROOT = ROOT / "MDMH4H"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from app.fulcrum import dashboard_read_model


class _FakeCursor:
    def __init__(self, *, fetchall_results=None):
        self.executions = []
        self._fetchall_results = list(fetchall_results or [])

    def execute(self, sql, params=None):
        self.executions.append((sql, params))

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


class FulcrumDashboardReadModelTests(unittest.TestCase):
    def test_summarize_changed_route_rows_counts_change_types(self):
        rows = [
            {"gate_record_id": 8},
            {"gate_record_id": 9},
            {"gate_record_id": 10},
        ]
        result = dashboard_read_model.summarize_changed_route_rows(
            rows,
            row_current_page_matches_winner_fn=lambda row: (
                (True, True) if row["gate_record_id"] == 8 else ((False, True) if row["gate_record_id"] == 9 else (False, False))
            ),
        )

        self.assertEqual(result["changed_count"], 2)
        self.assertEqual(result["same_type_changes"], 1)
        self.assertEqual(result["wrong_type_changes"], 1)

    def test_matches_search_and_sort_changed_route_rows(self):
        rows = [
            {
                "gate_record_id": 8,
                "representative_query": "hookless shower curtains",
                "impressions_90d": 40,
                "opportunity_score": 20,
                "suggested_target": {"name": "Hookless Shower Curtains", "score": 0.9},
                "agent_review": {"verdict": "correct"},
            },
            {
                "gate_record_id": 9,
                "representative_query": "luxury bath towels",
                "impressions_90d": 25,
                "opportunity_score": 15,
                "suggested_target": {"name": "Luxury Towels", "score": 0.6},
                "agent_review": {"verdict": "incorrect"},
            },
        ]

        self.assertTrue(dashboard_read_model.matches_changed_route_search(rows[0], "hookless"))
        self.assertFalse(dashboard_read_model.matches_changed_route_search(rows[0], "luxury"))

        ordered = dashboard_read_model.sorted_changed_route_rows(rows, "incorrect_first")
        self.assertEqual([row["gate_record_id"] for row in ordered], [9, 8])

    def test_gate_review_map_for_ids_uses_run_scoped_query_when_run_ids_present(self):
        fake_cursor = _FakeCursor(
            fetchall_results=[
                [
                    {"gate_record_id": 8, "verdict": "incorrect"},
                    {"gate_record_id": 8, "verdict": "correct"},
                    {"gate_record_id": 9, "verdict": "unclear"},
                ]
            ]
        )

        with patch("app.fulcrum.dashboard_read_model.get_pg_conn", return_value=_FakeConnection([fake_cursor])):
            result = dashboard_read_model.gate_review_map_for_ids(
                "99oa2tso",
                {8, 9},
                run_ids={44},
                query_gate_record_map_for_ids_fn=lambda store_hash, gate_record_ids, run_ids=None: {
                    8: {"gate_record_id": 8},
                    9: {"gate_record_id": 9},
                },
                list_query_gate_agent_reviews_fn=Mock(),
                postprocess_gate_agent_reviews_fn=lambda rows, gate_row_map: rows,
            )

        self.assertEqual(sorted(result.keys()), [8, 9])
        self.assertEqual(result[8]["verdict"], "incorrect")
        self.assertEqual(fake_cursor.executions[0][1], ("99oa2tso", [8, 9], [44]))

    def test_list_changed_route_results_builds_live_urls_and_filters_exact_matches(self):
        result = dashboard_read_model.list_changed_route_results(
            "99oa2tso",
            latest_gate_run_id_fn=Mock(return_value=44),
            list_query_gate_records_fn=lambda store_hash, disposition=None, limit=None, run_id=None: [
                {
                    "gate_record_id": 8,
                    "source_url": "/hotel-shower-curtains/",
                    "representative_query": "hookless shower curtains",
                    "opportunity_score": 30,
                    "suggested_target": {"url": "/hookless-shower-curtains/", "score": 0.9},
                },
                {
                    "gate_record_id": 9,
                    "source_url": "/same-page/",
                    "representative_query": "same page",
                    "opportunity_score": 10,
                    "suggested_target": {"url": "/same-page/", "score": 0.3},
                },
            ],
            attach_cached_query_gate_suggestions_fn=lambda rows: rows,
            row_current_page_matches_winner_fn=lambda row: (row["gate_record_id"] == 9, row["gate_record_id"] == 9),
            extract_storefront_channel_id_fn=lambda *args, **kwargs: 7,
            build_storefront_url_fn=lambda store_hash, path, channel_id=None: f"https://example.com{path}",
        )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["route_change_type"], "new_page_type")
        self.assertEqual(result[0]["source_live_url"], "https://example.com/hotel-shower-curtains/")
        self.assertEqual(result[0]["suggested_target"]["live_url"], "https://example.com/hookless-shower-curtains/")

    def test_get_cached_changed_route_results_prefers_cache_then_refreshes(self):
        load_cache_mock = Mock(return_value={"rows": [{"gate_record_id": 8}]})
        store_cache_mock = Mock(return_value={"rows": [{"gate_record_id": 9}]})
        latest_gate_run_id_mock = Mock(return_value=44)

        cached = dashboard_read_model.get_cached_changed_route_results(
            "99oa2tso",
            latest_gate_run_id_fn=latest_gate_run_id_mock,
            load_admin_metric_cache_fn=load_cache_mock,
            store_admin_metric_cache_fn=store_cache_mock,
            list_changed_route_results_fn=Mock(),
        )
        self.assertEqual(cached, [{"gate_record_id": 8}])

        refreshed = dashboard_read_model.get_cached_changed_route_results(
            "99oa2tso",
            force_refresh=True,
            latest_gate_run_id_fn=latest_gate_run_id_mock,
            load_admin_metric_cache_fn=load_cache_mock,
            store_admin_metric_cache_fn=store_cache_mock,
            list_changed_route_results_fn=lambda store_hash, run_id=None, limit=None: [{"gate_record_id": 9}],
        )
        self.assertEqual(refreshed, [{"gate_record_id": 9}])
        store_cache_mock.assert_called_once()


if __name__ == "__main__":
    unittest.main()
