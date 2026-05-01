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

from app.fulcrum import review_feedback


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


class FulcrumReviewFeedbackTests(unittest.TestCase):
    def test_increment_review_feedback_bucket_counts_only_supported_statuses(self):
        buckets = {}
        review_feedback.increment_review_feedback_bucket(buckets, ("product", 1), "approved")
        review_feedback.increment_review_feedback_bucket(buckets, ("product", 1), "reject")
        review_feedback.increment_review_feedback_bucket(buckets, ("product", 1), "pending")

        self.assertEqual(buckets[("product", 1)]["approved_count"], 1)
        self.assertEqual(buckets[("product", 1)]["rejected_count"], 1)

    def test_load_review_feedback_maps_combines_reviews_and_overrides(self):
        review_cursor = _FakeCursor(
            fetchall_results=[
                [
                    {
                        "source_entity_type": "category",
                        "source_entity_id": 10,
                        "target_entity_type": "product",
                        "target_entity_id": 20,
                        "example_query": "hookless shower curtains",
                        "review_status": "approved",
                    },
                    {
                        "source_entity_type": "category",
                        "source_entity_id": 10,
                        "target_entity_type": "product",
                        "target_entity_id": 21,
                        "example_query": "hookless shower curtains",
                        "review_status": "reject",
                    },
                ]
            ]
        )
        override_cursor = _FakeCursor(
            fetchall_results=[
                [
                    {
                        "normalized_query_key": "hookless curtain",
                        "source_entity_type": "category",
                        "source_entity_id": 10,
                        "target_entity_type": "product",
                        "target_entity_id": 20,
                    }
                ]
            ]
        )

        with patch(
            "app.fulcrum.review_feedback.get_pg_conn",
            side_effect=[_FakeConnection([review_cursor]), _FakeConnection([override_cursor])],
        ):
            feedback_maps = review_feedback.load_review_feedback_maps(
                "Stores/99OA2TSO",
                normalize_query_family_key_fn=lambda value: "hookless curtain" if value else "",
            )

        pair_bucket = feedback_maps["pair"][("category", 10, "product", 20)]
        self.assertEqual(pair_bucket["approved_count"], 3)
        self.assertEqual(pair_bucket["rejected_count"], 0)
        family_bucket = feedback_maps["family_target"][("hookless curtain", "product", 20)]
        self.assertEqual(family_bucket["approved_count"], 3)
        rejected_bucket = feedback_maps["pair"][("category", 10, "product", 21)]
        self.assertEqual(rejected_bucket["rejected_count"], 1)


if __name__ == "__main__":
    unittest.main()
