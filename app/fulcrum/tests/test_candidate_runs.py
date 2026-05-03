import sys
import unittest
from pathlib import Path
from unittest.mock import Mock


ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

APP_ROOT = ROOT / "MDMH4H"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from app.fulcrum import candidate_runs


class _RowsCursor:
    def __init__(self, rows):
        self.rows = rows

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        return None

    def fetchall(self):
        return self.rows


class _RowsConn:
    def __init__(self, rows):
        self.rows = rows

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self, cursor_factory=None):
        return _RowsCursor(self.rows)


class FulcrumCandidateRunTests(unittest.TestCase):
    def test_queue_candidate_run_returns_duplicate_when_active_run_exists(self):
        result = candidate_runs.queue_candidate_run(
            "stores/abc123",
            normalize_store_hash_fn=lambda value: "abc123",
            find_active_run_fn=lambda store_hash: {"run_id": 7, "status": "running"},
            create_run_fn=Mock(),
            start_generation_worker_fn=Mock(),
            complete_run_fn=Mock(),
        )

        self.assertFalse(result["queued"])
        self.assertTrue(result["duplicate"])
        self.assertEqual(result["run_id"], 7)

    def test_publish_all_current_results_respects_blocked_sources(self):
        review_candidates = Mock(return_value=1)
        publish_entities = Mock(return_value=[{"source_entity_id": 22}])

        result = candidate_runs.publish_all_current_results(
            "stores/abc123",
            initiated_by="tester",
            normalize_store_hash_fn=lambda value: "abc123",
            category_publishing_enabled_for_store_fn=lambda store_hash: True,
            list_query_gate_review_requests_fn=lambda store_hash, request_status=None, limit=1000: [
                {"source_entity_type": "product", "source_entity_id": 11}
            ],
            latest_candidate_rows_for_store_fn=lambda store_hash, review_status=None, limit=None: [
                {"candidate_id": 1, "source_entity_type": "product", "source_entity_id": 11, "source_product_id": 11},
                {"candidate_id": 2, "source_entity_type": "product", "source_entity_id": 22, "source_product_id": 22},
            ] if review_status == "pending" else [
                {"candidate_id": 3, "source_entity_type": "product", "source_entity_id": 22, "source_product_id": 22}
            ],
            include_dashboard_candidate_fn=lambda row, tab, category_enabled: True,
            review_candidates_fn=review_candidates,
            publish_approved_entities_fn=publish_entities,
        )

        self.assertEqual(result["blocked_source_count"], 1)
        self.assertEqual(result["publishable_pending_count"], 1)
        self.assertEqual(result["approved_source_count"], 1)
        self.assertEqual(result["published_source_count"], 1)
        self.assertEqual(result["unresolved_approved_source_count"], 0)
        review_candidates.assert_called_once()
        publish_entities.assert_called_once()

    def test_publish_all_current_results_blocks_brand_query_to_category_target(self):
        review_candidates = Mock(return_value=0)
        publish_entities = Mock(return_value=[])

        stale_brand_category = {
            "candidate_id": 7,
            "source_entity_type": "product",
            "source_entity_id": 112556,
            "source_product_id": 112556,
            "target_entity_type": "category",
            "target_entity_id": -1000005394,
            "target_product_id": -1000005394,
            "metadata": {
                "query_intent_scope": "brand_navigation",
                "preferred_entity_type": "brand",
                "query_target_tokens": ["downlite"],
            },
        }

        result = candidate_runs.publish_all_current_results(
            "stores/abc123",
            initiated_by="tester",
            normalize_store_hash_fn=lambda value: "abc123",
            category_publishing_enabled_for_store_fn=lambda store_hash: True,
            list_query_gate_review_requests_fn=lambda store_hash, request_status=None, limit=1000: [],
            latest_candidate_rows_for_store_fn=lambda store_hash, review_status=None, limit=None: [stale_brand_category]
            if review_status == "approved"
            else [],
            include_dashboard_candidate_fn=lambda row, tab, category_enabled: True,
            review_candidates_fn=review_candidates,
            publish_approved_entities_fn=publish_entities,
        )

        self.assertEqual(result["policy_blocked_candidate_count"], 1)
        self.assertEqual(result["approved_source_count"], 0)
        publish_entities.assert_not_called()

    def test_auto_approve_and_publish_run_reuses_publish_all_rules(self):
        publish_all_current_results = Mock(
            return_value={
                "approved_count": 5,
                "approved_source_count": 4,
                "published_count": 3,
                "published_source_count": 3,
                "unresolved_approved_source_count": 1,
                "publications": [{"source_entity_id": 101}],
                "blocked_source_count": 1,
                "publishable_pending_count": 5,
                "pending_row_count": 6,
            }
        )

        result = candidate_runs.auto_approve_and_publish_run(
            "stores/abc123",
            82,
            auto_publish_enabled=True,
            refresh_store_readiness_fn=lambda store_hash: {"auto_publish_ready": True},
            publish_all_current_results_fn=publish_all_current_results,
        )

        publish_all_current_results.assert_called_once_with("stores/abc123", "fulcrum-auto")
        self.assertTrue(result["auto_publish_enabled"])
        self.assertTrue(result["auto_publish_ready"])
        self.assertEqual(result["auto_approved_count"], 5)
        self.assertEqual(result["auto_published_count"], 3)
        self.assertEqual(result["approved_source_count"], 4)
        self.assertEqual(result["unresolved_approved_source_count"], 1)
        self.assertEqual(result["blocked_source_count"], 1)
        self.assertEqual(result["publishable_pending_count"], 5)
        self.assertEqual(result["pending_row_count"], 6)
        self.assertEqual(result["published_entities"], [{"source_entity_id": 101}])

    def test_eligible_auto_publish_allows_brand_target_for_brand_navigation(self):
        rows = [
            {
                "candidate_id": 1,
                "source_entity_type": "product",
                "target_entity_type": "brand",
                "source_entity_id": 112556,
                "target_entity_id": -2000003028,
                "source_product_id": 112556,
                "target_product_id": -2000003028,
                "source_name": "Cloud Top PrimaLoft Plush Blankets & Throws by DOWNLITE",
                "target_name": "DownLite Bedding",
                "anchor_label": "Downlite",
                "score": 100,
                "metadata": {
                    "query_intent_scope": "brand_navigation",
                    "preferred_entity_type": "brand",
                    "query_target_tokens": ["downlite"],
                    "shared_tokens": ["downlite"],
                },
            }
        ]

        selected = candidate_runs.eligible_auto_publish_candidates(
            "99oa2tso",
            88,
            refresh_store_readiness_fn=lambda store_hash: {"auto_publish_ready": True},
            get_pg_conn_fn=lambda: _RowsConn(rows),
            normalize_store_hash_fn=lambda value: str(value or "").lower(),
            category_publishing_enabled_for_store_fn=lambda store_hash: True,
            auto_publish_min_score=85,
            auto_publish_max_links_per_source=4,
        )

        self.assertEqual([row["candidate_id"] for row in selected], [1])

    def test_eligible_auto_publish_rejects_non_brand_query_to_brand_target(self):
        rows = [
            {
                "candidate_id": 1,
                "source_entity_type": "product",
                "target_entity_type": "brand",
                "source_entity_id": 112556,
                "target_entity_id": -2000003028,
                "source_product_id": 112556,
                "target_product_id": -2000003028,
                "source_name": "Cloud Top PrimaLoft Plush Blankets & Throws by DOWNLITE",
                "target_name": "DownLite Bedding",
                "anchor_label": "Downlite",
                "score": 100,
                "metadata": {
                    "query_intent_scope": "commercial_topic",
                    "preferred_entity_type": "category",
                    "query_target_tokens": ["downlite"],
                    "shared_tokens": ["downlite"],
                },
            }
        ]

        selected = candidate_runs.eligible_auto_publish_candidates(
            "99oa2tso",
            88,
            refresh_store_readiness_fn=lambda store_hash: {"auto_publish_ready": True},
            get_pg_conn_fn=lambda: _RowsConn(rows),
            normalize_store_hash_fn=lambda value: str(value or "").lower(),
            category_publishing_enabled_for_store_fn=lambda store_hash: True,
            auto_publish_min_score=85,
            auto_publish_max_links_per_source=4,
        )

        self.assertEqual(selected, [])

    def test_publish_all_current_results_reports_unresolved_approved_sources(self):
        result = candidate_runs.publish_all_current_results(
            "stores/abc123",
            initiated_by="tester",
            normalize_store_hash_fn=lambda value: "abc123",
            category_publishing_enabled_for_store_fn=lambda store_hash: True,
            list_query_gate_review_requests_fn=lambda store_hash, request_status=None, limit=1000: [],
            latest_candidate_rows_for_store_fn=lambda store_hash, review_status=None, limit=None: [
                {"candidate_id": 7, "source_entity_type": "product", "source_entity_id": 22, "source_product_id": 22}
            ] if review_status == "approved" else [],
            include_dashboard_candidate_fn=lambda row, tab, category_enabled: True,
            review_candidates_fn=Mock(return_value=0),
            publish_approved_entities_fn=lambda store_hash, source_entity_ids=None: [
                {"source_entity_type": "product", "source_product_id": 22, "status": "skipped_missing_store_product"}
            ],
        )

        self.assertEqual(result["approved_source_count"], 1)
        self.assertEqual(result["published_source_count"], 0)
        self.assertEqual(result["published_count"], 0)
        self.assertEqual(result["unresolved_approved_source_count"], 1)


if __name__ == "__main__":
    unittest.main()
