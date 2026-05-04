import json
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

from app.fulcrum import decision_feedback


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


class FulcrumDecisionFeedbackTests(unittest.TestCase):
    def test_record_review_feedback_snapshots_gate_and_target(self):
        select_cursor = _FakeCursor(
            fetchone_results=[
                {
                    "gate_record_id": 7,
                    "run_id": 44,
                    "store_hash": "99oa2tso",
                    "representative_query": "downlite blankets",
                    "normalized_query_key": "downlite blanket",
                    "source_entity_type": "product",
                    "source_entity_id": 112556,
                    "source_name": "Cloud Top",
                    "source_url": "/products/cloud-top.html",
                    "disposition": "pass",
                    "query_intent_scope": "brand_navigation",
                    "preferred_entity_type": "brand",
                    "demand_score": 12,
                    "opportunity_score": 80,
                    "intent_clarity_score": 91,
                    "noise_penalty": 0,
                    "reason_summary": "Brand intent",
                    "metadata": {"semantics_analysis": {"query_shape": "brand_navigational"}},
                },
                {
                    "feedback_id": 3,
                    "store_hash": "99oa2tso",
                    "gate_record_id": 7,
                    "run_id": 44,
                    "action": "review",
                    "feedback_status": "diagnosis_pending",
                },
            ]
        )
        fake_conn = _FakeConnection([select_cursor])

        with patch("app.fulcrum.decision_feedback.get_pg_conn", return_value=fake_conn):
            row = decision_feedback.record_query_gate_decision_feedback(
                "Stores/99OA2TSO",
                7,
                "review",
                submitted_by="qa@example.com",
                target_entity_type="category",
                target_entity_id=12,
                target_name="Hotel Bedding Supply",
                target_url="/hotel-bedding-supply/",
            )

        self.assertEqual(row["feedback_id"], 3)
        self.assertTrue(fake_conn.committed)
        insert_params = select_cursor.executions[1][1]
        snapshot = json.loads(insert_params[9])
        self.assertEqual(insert_params[0], "99oa2tso")
        self.assertEqual(insert_params[3], "review")
        self.assertEqual(snapshot["representative_query"], "downlite blankets")
        self.assertEqual(snapshot["target"]["name"], "Hotel Bedding Supply")


if __name__ == "__main__":
    unittest.main()
