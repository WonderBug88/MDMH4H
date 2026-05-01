import sys
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

APP_ROOT = ROOT / "MDMH4H"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from app.fulcrum import candidates


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


class FulcrumCandidatesTests(unittest.TestCase):
    def test_latest_candidate_rows_for_store_queries_expected_shape(self):
        cursor = _FakeCursor(fetchall_results=[[{"candidate_id": 8, "source_name": "Hotel Shower Curtains"}]])
        conn = _FakeConnection([cursor])

        with patch("app.fulcrum.candidates.get_pg_conn", return_value=conn):
            rows = candidates.latest_candidate_rows_for_store("99oa2tso", review_status="pending", limit=25)

        self.assertEqual(rows[0]["candidate_id"], 8)
        self.assertEqual(cursor.executions[0][1], ("99oa2tso", "pending", "pending", 25))

    def test_count_pending_candidates_uses_category_flag(self):
        cursor = _FakeCursor(fetchone_results=[[7]])
        conn = _FakeConnection([cursor])

        with patch("app.fulcrum.candidates.get_pg_conn", return_value=conn):
            count = candidates.count_pending_candidates(
                "99oa2tso",
                category_publishing_enabled_for_store_fn=lambda store_hash: False,
            )

        self.assertEqual(count, 7)
        self.assertEqual(cursor.executions[0][1], ("99oa2tso", False))

    def test_review_candidates_updates_rows_and_commits(self):
        cursor = _FakeCursor()
        conn = _FakeConnection([cursor])

        with (
            patch("app.fulcrum.candidates.get_pg_conn", return_value=conn),
            patch("app.fulcrum.candidates.execute_batch") as execute_batch_mock,
        ):
            updated = candidates.review_candidates([8, 9], "approved", "alice@example.com", note="Looks good")

        self.assertEqual(updated, 2)
        self.assertTrue(conn.committed)
        self.assertEqual(cursor.executions[0][1], ("approved", [8, 9]))
        execute_batch_mock.assert_called_once()

    def test_list_candidates_uses_filters_and_ranker_for_approved_rows(self):
        rows = [
            {"candidate_id": 1, "source_entity_type": "product", "source_product_id": 10, "score": 5, "created_at": 1},
            {"candidate_id": 2, "source_entity_type": "product", "source_product_id": 10, "score": 9, "created_at": 2},
            {"candidate_id": 3, "source_entity_type": "product", "source_product_id": 11, "score": 7, "created_at": 3},
        ]

        result = candidates.list_candidates(
            "99oa2tso",
            review_status="approved",
            limit=5,
            latest_candidate_rows_for_store_fn=lambda *args, **kwargs: list(rows),
            category_publishing_enabled_for_store_fn=lambda store_hash: True,
            include_dashboard_candidate_fn=lambda row, review_status, category_enabled: True,
            rank_source_rows_fn=lambda source_rows, source_entity_type="product": sorted(source_rows, key=lambda row: row["candidate_id"], reverse=True)[:1],
        )

        self.assertEqual([row["candidate_id"] for row in result], [2, 3])

    def test_list_approved_sources_dedupes_sources(self):
        result = candidates.list_approved_sources(
            "99oa2tso",
            limit=10,
            list_candidates_fn=lambda store_hash, review_status, limit=0: [
                {"source_entity_type": "product", "source_product_id": 10, "source_name": "A", "source_url": "/a"},
                {"source_entity_type": "product", "source_product_id": 10, "source_name": "A second", "source_url": "/a2"},
                {"source_entity_type": "category", "source_product_id": 11, "source_name": "B", "source_url": "/b"},
            ],
        )

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["source_name"], "A")


if __name__ == "__main__":
    unittest.main()
