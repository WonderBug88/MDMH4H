import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

APP_ROOT = ROOT / "MDMH4H"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from app.fulcrum import quality_reporting


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

    def cursor(self, *args, **kwargs):
        if self._cursors:
            self._last_cursor = self._cursors.pop(0)
        return self._last_cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FulcrumQualityReportingTests(unittest.TestCase):
    def test_get_entity_coverage_summary_combines_db_and_profile_counts(self):
        cursor = _FakeCursor(
            fetchone_results=[
                {
                    "product_count": 12,
                    "canonical_product_count": 9,
                    "category_count": 4,
                    "canonical_category_count": 3,
                }
            ]
        )
        conn = _FakeConnection([cursor])
        content_calls = []

        with patch("app.fulcrum.quality_reporting.get_pg_conn", return_value=conn):
            summary = quality_reporting.get_entity_coverage_summary(
                "Stores/99OA2TSO",
                load_store_brand_profiles_fn=lambda store_hash: [{"id": 1}, {"id": 2}],
                load_store_content_profiles_fn=lambda store_hash, include_backlog=False: content_calls.append((store_hash, include_backlog)) or [{"id": 3}],
            )

        self.assertEqual(summary["product_count"], 12)
        self.assertEqual(summary["brand_count"], 2)
        self.assertEqual(summary["content_count"], 1)
        self.assertEqual(content_calls, [("99oa2tso", True)])

    def test_load_logic_change_log_formats_and_sorts_rows(self):
        with tempfile.TemporaryDirectory() as tempdir:
            changelog_path = Path(tempdir) / "changes.json"
            changelog_path.write_text(
                json.dumps(
                    [
                        {"timestamp": "2026-04-01T10:00:00", "summary": "Older", "validation": {"status": "untested"}},
                        {"timestamp": "2026-04-15T14:30:00", "summary": "Newer", "validation": {"status": "verified_pass", "verified_at": "2026-04-15T15:00:00"}},
                    ]
                ),
                encoding="utf-8",
            )

            rows = quality_reporting.load_logic_change_log(changelog_path=changelog_path)

        self.assertEqual(rows[0]["summary"], "Newer")
        self.assertEqual(rows[0]["validation_status_label"], "Verified Pass")
        self.assertTrue(rows[0]["timestamp_display"].startswith("2026-04-15"))
        self.assertTrue(rows[1]["timestamp_display"].startswith("2026-04-01"))

    def test_get_logic_change_summary_returns_counts_and_recent_rows(self):
        with tempfile.TemporaryDirectory() as tempdir:
            changelog_path = Path(tempdir) / "changes.json"
            changelog_path.write_text(
                json.dumps(
                    [
                        {"timestamp": "2026-04-01T10:00:00", "summary": "Older", "validation": {"status": "untested"}},
                        {"timestamp": "2026-04-12T11:00:00", "summary": "Middle", "validation": {"status": "verified_pass"}},
                        {"timestamp": "2026-04-15T14:30:00", "summary": "Newest", "validation": {"status": "verified_fail"}},
                    ]
                ),
                encoding="utf-8",
            )

            summary = quality_reporting.get_logic_change_summary(changelog_path=changelog_path, limit=1)

        self.assertEqual(summary["revision_count"], 3)
        self.assertEqual(summary["verified_pass_count"], 1)
        self.assertEqual(summary["verified_fail_count"], 1)
        self.assertEqual(summary["untested_count"], 1)
        self.assertEqual(summary["needs_review_count"], 2)
        self.assertEqual(summary["latest"]["summary"], "Newest")
        self.assertEqual(len(summary["recent_changes"]), 1)


if __name__ == "__main__":
    unittest.main()
