import sys
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

APP_ROOT = ROOT / "MDMH4H"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from app.fulcrum import admin_cache


class _FakeCursor:
    def __init__(self, *, fetchone_results=None):
        self.executions = []
        self._fetchone_results = list(fetchone_results or [])

    def execute(self, sql, params=None):
        self.executions.append((sql, params))

    def fetchone(self):
        return self._fetchone_results.pop(0) if self._fetchone_results else None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeConnection:
    def __init__(self, cursors):
        self._cursors = list(cursors)
        self._last_cursor = self._cursors[-1] if self._cursors else _FakeCursor()
        self.committed = False

    def cursor(self, *args, **kwargs):
        if self._cursors:
            self._last_cursor = self._cursors.pop(0)
        return self._last_cursor

    def commit(self):
        self.committed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FulcrumAdminCacheTests(unittest.TestCase):
    def test_json_cache_safe_converts_nested_decimal_datetime_and_tuple(self):
        payload = admin_cache.json_cache_safe(
            {
                "count": Decimal("2.5"),
                "when": datetime(2026, 4, 15, 12, 0, 0, tzinfo=timezone.utc),
                "items": (Decimal("3"), {"nested": Decimal("4.2")}),
            }
        )

        self.assertEqual(payload["count"], 2.5)
        self.assertEqual(payload["items"][0], 3.0)
        self.assertEqual(payload["items"][1]["nested"], 4.2)
        self.assertIn("2026-04-15T12:00:00+00:00", payload["when"])

    def test_invalidate_admin_metric_cache_executes_expected_sql(self):
        cursor = _FakeCursor()
        conn = _FakeConnection([cursor])

        from unittest.mock import patch

        with patch("app.fulcrum.admin_cache.get_pg_conn", return_value=conn):
            admin_cache.invalidate_admin_metric_cache(
                "Stores/99OA2TSO",
                metric_keys=["live_gsc_performance"],
                apply_runtime_schema_fn=lambda: None,
            )

        self.assertTrue(conn.committed)
        self.assertEqual(cursor.executions[0][1], ("99oa2tso", ["live_gsc_performance"]))

    def test_load_admin_metric_cache_respects_age_and_adds_display_fields(self):
        updated_at = datetime.now(timezone.utc)
        cursor = _FakeCursor(fetchone_results=[{"payload": {"value": 7}, "updated_at": updated_at}])
        conn = _FakeConnection([cursor])

        from unittest.mock import patch

        with patch("app.fulcrum.admin_cache.get_pg_conn", return_value=conn):
            payload = admin_cache.load_admin_metric_cache(
                "Stores/99OA2TSO",
                "Live_GSC_Performance",
                max_age=timedelta(minutes=30),
                apply_runtime_schema_fn=lambda: None,
                format_timestamp_display_fn=lambda value: "display",
                format_relative_time_fn=lambda value: "relative",
            )

        self.assertEqual(payload["value"], 7)
        self.assertEqual(payload["cached_at_display"], "display")
        self.assertEqual(payload["cached_at_relative"], "relative")
        self.assertEqual(cursor.executions[0][1], ("99oa2tso", "live_gsc_performance"))

    def test_load_admin_metric_cache_skips_stale_rows(self):
        updated_at = datetime.now(timezone.utc) - timedelta(days=2)
        cursor = _FakeCursor(fetchone_results=[{"payload": {"value": 7}, "updated_at": updated_at}])
        conn = _FakeConnection([cursor])

        from unittest.mock import patch

        with patch("app.fulcrum.admin_cache.get_pg_conn", return_value=conn):
            payload = admin_cache.load_admin_metric_cache(
                "99oa2tso",
                "metric",
                max_age=timedelta(minutes=30),
                apply_runtime_schema_fn=lambda: None,
                format_timestamp_display_fn=lambda value: "display",
                format_relative_time_fn=lambda value: "relative",
            )

        self.assertIsNone(payload)

    def test_store_admin_metric_cache_normalizes_payload_and_returns_cache_metadata(self):
        updated_at = datetime.now(timezone.utc)
        cursor = _FakeCursor(fetchone_results=[{"payload": {"rows": [1]}, "updated_at": updated_at}])
        conn = _FakeConnection([cursor])

        from unittest.mock import patch

        with patch("app.fulcrum.admin_cache.get_pg_conn", return_value=conn):
            payload = admin_cache.store_admin_metric_cache(
                "Stores/99OA2TSO",
                "Changed_Route_Results",
                {"rows": (Decimal("2"),), "when": datetime(2026, 4, 15, 12, 0, 0, tzinfo=timezone.utc)},
                apply_runtime_schema_fn=lambda: None,
                format_timestamp_display_fn=lambda value: "display",
                format_relative_time_fn=lambda value: "relative",
            )

        self.assertTrue(conn.committed)
        self.assertEqual(payload["rows"], [1])
        self.assertEqual(payload["cached_at_display"], "display")
        self.assertEqual(payload["cached_at_relative"], "relative")
        executed_params = cursor.executions[0][1]
        self.assertEqual(executed_params[0], "99oa2tso")
        self.assertEqual(executed_params[1], "changed_route_results")
        self.assertIn('"rows": [2.0]', executed_params[2])


if __name__ == "__main__":
    unittest.main()
