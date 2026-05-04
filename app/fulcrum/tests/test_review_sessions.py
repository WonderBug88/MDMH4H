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

from app.fulcrum import review_sessions


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


class FulcrumReviewSessionTests(unittest.TestCase):
    def test_create_query_gate_review_submission_stores_split_batches(self):
        fake_cursor = _FakeCursor(
            fetchone_results=[
                {
                    "submission_id": 12,
                    "store_hash": "99oa2tso",
                    "run_id": 44,
                    "cleared_count": 2,
                    "review_bucket_count": 1,
                    "remaining_count": 1,
                    "total_result_count": 4,
                    "metadata": {
                        "all_gate_record_ids": [7, 8, 9, 10],
                        "cleared_gate_record_ids": [8, 10],
                        "review_bucket_gate_record_ids": [7],
                        "remaining_gate_record_ids": [9],
                    },
                }
            ]
        )
        fake_conn = _FakeConnection([fake_cursor])

        with patch("app.fulcrum.review_sessions.get_pg_conn", return_value=fake_conn):
            row = review_sessions.create_query_gate_review_submission(
                "Stores/99OA2TSO",
                run_id=44,
                submitted_by="qa@example.com",
                all_gate_record_ids=[7, 8, 9, 10],
                cleared_gate_record_ids=[8, 10, 10],
                review_bucket_gate_record_ids=[7],
                metadata={"client_submitted_at": "2026-04-19T12:00:00Z"},
                record_decision_feedback_batch_fn=lambda *args, **kwargs: 0,
            )

        self.assertEqual(row["submission_id"], 12)
        self.assertTrue(fake_conn.committed)
        params = fake_cursor.executions[0][1]
        self.assertEqual(params[0], "99oa2tso")
        self.assertEqual(params[1], 44)
        self.assertEqual(params[3], 4)
        self.assertEqual(params[4], 2)
        self.assertEqual(params[5], 1)
        self.assertEqual(params[6], 1)

    def test_create_query_gate_review_submission_keeps_review_bucket_priority_over_clear(self):
        fake_cursor = _FakeCursor(
            fetchone_results=[
                {
                    "submission_id": 18,
                    "metadata": {
                        "all_gate_record_ids": [7, 8],
                        "cleared_gate_record_ids": [],
                        "review_bucket_gate_record_ids": [7],
                        "remaining_gate_record_ids": [8],
                    },
                }
            ]
        )
        fake_conn = _FakeConnection([fake_cursor])

        with patch("app.fulcrum.review_sessions.get_pg_conn", return_value=fake_conn):
            review_sessions.create_query_gate_review_submission(
                "99oa2tso",
                run_id=44,
                submitted_by="qa@example.com",
                all_gate_record_ids=[7, 8],
                cleared_gate_record_ids=[7],
                review_bucket_gate_record_ids=[7],
                metadata=None,
                record_decision_feedback_batch_fn=lambda *args, **kwargs: 0,
            )

        params = fake_cursor.executions[0][1]
        self.assertEqual(params[4], 0)
        self.assertEqual(params[5], 1)
        self.assertEqual(params[6], 1)

    def test_create_query_gate_review_submission_records_clear_and_review_feedback(self):
        fake_cursor = _FakeCursor(
            fetchone_results=[
                {
                    "submission_id": 20,
                    "metadata": {
                        "all_gate_record_ids": [7, 8, 9],
                        "cleared_gate_record_ids": [8],
                        "review_bucket_gate_record_ids": [7],
                        "remaining_gate_record_ids": [9],
                    },
                }
            ]
        )
        calls = []

        with patch("app.fulcrum.review_sessions.get_pg_conn", return_value=_FakeConnection([fake_cursor])):
            review_sessions.create_query_gate_review_submission(
                "99oa2tso",
                run_id=44,
                submitted_by="qa@example.com",
                all_gate_record_ids=[7, 8, 9],
                cleared_gate_record_ids=[8],
                review_bucket_gate_record_ids=[7],
                metadata={"client_submitted_at": "2026-04-19T12:00:00Z"},
                record_decision_feedback_batch_fn=lambda *args, **kwargs: calls.append((args, kwargs)) or 1,
            )

        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0][0][1], "clear")
        self.assertEqual(calls[0][0][2], [8])
        self.assertEqual(calls[1][0][1], "review")
        self.assertEqual(calls[1][0][2], [7])
        self.assertEqual(calls[1][1]["feedback_status"], "diagnosis_pending")


if __name__ == "__main__":
    unittest.main()
