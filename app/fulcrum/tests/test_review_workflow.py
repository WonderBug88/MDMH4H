import json
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

from app.fulcrum import review_workflow


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


class FulcrumReviewWorkflowTests(unittest.TestCase):
    def test_request_query_gate_review_inserts_normalized_request(self):
        fake_cursor = _FakeCursor(
            fetchone_results=[
                {
                    "request_id": 14,
                    "gate_record_id": 7,
                    "store_hash": "99oa2tso",
                    "request_status": "requested",
                }
            ]
        )
        fake_conn = _FakeConnection([fake_cursor])

        with patch("app.fulcrum.review_workflow.get_pg_conn", return_value=fake_conn):
            row = review_workflow.request_query_gate_review(
                "Stores/99OA2TSO",
                7,
                "category",
                5451,
                "Hookless Shower Curtains",
                "/hookless-shower-curtains/",
                "Reason",
                "qa@example.com",
                note="Please review",
                get_query_gate_record_by_id_fn=lambda store_hash, gate_record_id: {
                    "gate_record_id": 7,
                    "run_id": 22,
                    "normalized_query_key": "hookless shower curtains",
                    "representative_query": "hookless shower curtains",
                    "source_url": "/hotel-shower-curtains/",
                    "source_name": "Hotel Shower Curtains",
                    "source_entity_type": "category",
                    "source_entity_id": -1000005446,
                    "current_page_type": "category",
                    "disposition": "hold",
                    "query_intent_scope": "broad_product_family",
                    "preferred_entity_type": "category",
                    "reason_summary": "Category intent",
                },
            )

        self.assertEqual(row["request_id"], 14)
        self.assertTrue(fake_conn.committed)
        params = fake_cursor.executions[0][1]
        self.assertEqual(params[2], "99oa2tso")
        self.assertEqual(params[10], "category")
        self.assertEqual(params[11], 5451)
        metadata = json.loads(params[16])
        self.assertEqual(metadata["preferred_entity_type"], "category")

    def test_pause_source_for_review_resets_candidates_and_unpublishes(self):
        review_candidates_mock = Mock(return_value=2)
        unpublish_mock = Mock(return_value=[{"publication_id": 9}])

        result = review_workflow.pause_source_for_review(
            "Stores/99OA2TSO",
            123,
            source_entity_type="category",
            reviewed_by="qa@example.com",
            note="Pause it",
            latest_candidate_rows_for_store=lambda store_hash, review_status=None, limit=None: [
                {
                    "candidate_id": 4,
                    "source_entity_type": "category",
                    "source_product_id": 123,
                },
                {
                    "candidate_id": 5,
                    "source_entity_type": "category",
                    "source_product_id": 123,
                },
            ],
            normalize_storefront_path_fn=lambda value: str(value or "").strip().lower().rstrip("/") + "/" if str(value or "").strip() else "",
            entity_bc_id_fn=lambda entity_type, storage_id: abs(int(storage_id or 0)) - 1000000000 if entity_type == "category" else int(storage_id or 0),
            review_candidates_fn=review_candidates_mock,
            unpublish_entities_fn=unpublish_mock,
        )

        self.assertEqual(result["approved_candidate_count"], 2)
        self.assertEqual(result["review_reset_count"], 2)
        self.assertEqual(result["publication_count"], 1)
        review_candidates_mock.assert_called_once_with([4, 5], "pending", "qa@example.com", "Pause it")
        unpublish_mock.assert_called_once_with("99oa2tso", [123])


    def test_pause_source_for_review_matches_bc_category_id_and_unpublishes_internal_source(self):
        review_candidates_mock = Mock(return_value=1)
        unpublish_mock = Mock(return_value=[{"publication_id": 11}])

        result = review_workflow.pause_source_for_review(
            "Stores/99OA2TSO",
            5388,
            source_entity_type="category",
            reviewed_by="qa@example.com",
            note="Pause it",
            latest_candidate_rows_for_store=lambda store_hash, review_status=None, limit=None: [
                {
                    "candidate_id": 4,
                    "source_entity_type": "category",
                    "source_entity_id": -1000005388,
                    "source_product_id": -1000005388,
                    "source_url": "/hotel-towels/",
                    "metadata": {"source_bc_entity_id": 5388},
                }
            ],
            normalize_storefront_path_fn=lambda value: str(value or "").strip().lower().rstrip("/") + "/" if str(value or "").strip() else "",
            entity_bc_id_fn=lambda entity_type, storage_id: abs(int(storage_id or 0)) - 1000000000 if entity_type == "category" else int(storage_id or 0),
            review_candidates_fn=review_candidates_mock,
            unpublish_entities_fn=unpublish_mock,
        )

        self.assertEqual(result["approved_candidate_count"], 1)
        self.assertEqual(result["review_reset_count"], 1)
        self.assertEqual(result["publication_count"], 1)
        review_candidates_mock.assert_called_once_with([4], "pending", "qa@example.com", "Pause it")
        unpublish_mock.assert_called_once_with("99oa2tso", [-1000005388])

    def test_resolve_query_gate_review_request_merges_metadata(self):
        fake_cursor = _FakeCursor(
            fetchone_results=[
                {
                    "request_id": 18,
                    "request_status": "resolved",
                    "metadata": {"existing": True},
                }
            ]
        )
        fake_conn = _FakeConnection([fake_cursor])

        with patch("app.fulcrum.review_workflow.get_pg_conn", return_value=fake_conn):
            row = review_workflow.resolve_query_gate_review_request(
                "Stores/99OA2TSO",
                18,
                resolved_by="qa@example.com",
                resolution_note="Resolved now",
                metadata_updates={"live_block_restored": True},
                get_query_gate_review_request_by_id_fn=lambda store_hash, request_id: {
                    "request_id": 18,
                    "request_note": "Old note",
                    "metadata": {"existing": True},
                },
            )

        self.assertEqual(row["request_status"], "resolved")
        self.assertTrue(fake_conn.committed)
        params = fake_cursor.executions[0][1]
        self.assertEqual(params[0], "Resolved now")
        merged = json.loads(params[1])
        self.assertTrue(merged["existing"])
        self.assertTrue(merged["live_block_restored"])
        self.assertEqual(merged["resolved_by"], "qa@example.com")

    def test_review_all_edge_cases_groups_requests_by_run(self):
        run_review_mock = Mock(
            side_effect=[
                {
                    "status": "ok",
                    "reason": "",
                    "reviewed_count": 2,
                    "stored_count": 2,
                    "summary": {"correct": 1, "incorrect": 1, "unclear": 0},
                },
                {
                    "status": "skipped",
                    "reason": "Nothing useful",
                    "reviewed_count": 1,
                    "stored_count": 0,
                    "summary": {"correct": 0, "incorrect": 0, "unclear": 1},
                },
            ]
        )

        result = review_workflow.review_all_edge_cases(
            "99oa2tso",
            initiated_by="qa@example.com",
            list_query_gate_review_requests_fn=lambda store_hash, request_status=None, limit=1000: [
                {"gate_record_id": 10, "run_id": 44},
                {"gate_record_id": 11, "run_id": 44},
                {"gate_record_id": 12, "run_id": 55},
            ],
            run_query_gate_agent_review_fn=run_review_mock,
        )

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["reviewed_count"], 3)
        self.assertEqual(result["stored_count"], 2)
        self.assertEqual(result["summary"]["incorrect"], 1)
        self.assertEqual(result["summary"]["unclear"], 1)
        self.assertEqual(run_review_mock.call_args_list[0].kwargs["gate_record_ids"], [10, 11])
        self.assertEqual(run_review_mock.call_args_list[1].kwargs["gate_record_ids"], [12])

    def test_restore_source_after_review_filters_and_publishes(self):
        review_candidates_mock = Mock(return_value=1)
        publish_mock = Mock(return_value=[{"publication_id": 77}])

        result = review_workflow.restore_source_after_review(
            "Stores/99OA2TSO",
            -1000005446,
            "category",
            target_entity_type="category",
            target_entity_id=5451,
            target_url="/hookless-shower-curtains/",
            reviewed_by="qa@example.com",
            note="Restore it",
            latest_candidate_rows_for_store=lambda store_hash, review_status=None, limit=None: [
                {
                    "candidate_id": 8,
                    "source_entity_type": "category",
                    "source_entity_id": -1000005446,
                    "review_status": "pending",
                    "target_entity_type": "category",
                    "target_entity_id": -1000005451,
                    "target_url": "https://www.hotels4humanity.com/hookless-shower-curtains",
                    "metadata": {"target_bc_entity_id": 5451},
                },
                {
                    "candidate_id": 9,
                    "source_entity_type": "category",
                    "source_entity_id": -1000005446,
                    "review_status": "pending",
                    "target_entity_type": "category",
                    "target_entity_id": -1000005448,
                    "target_url": "/different-category/",
                    "metadata": {"target_bc_entity_id": 5448},
                },
            ],
            normalize_storefront_path_fn=lambda value: (
                ""
                if not value
                else (str(value).split("://", 1)[1].split("/", 1)[1] if "://" in str(value) else str(value)).strip().lower().rstrip("/") + "/"
                if str(value).strip() not in {"", "/"}
                else "/"
            ) if value else "",
            entity_bc_id_fn=lambda entity_type, storage_id: abs(int(storage_id or 0)) - 1000000000 if entity_type == "category" else int(storage_id or 0),
            review_candidates_fn=review_candidates_mock,
            publish_approved_entities_fn=publish_mock,
        )

        self.assertEqual(result["approved_candidate_count"], 1)
        self.assertEqual(result["publication_count"], 1)
        self.assertTrue(result["live_block_restored"])
        review_candidates_mock.assert_called_once_with([8], "approved", "qa@example.com", "Restore it")
        publish_mock.assert_called_once_with("99oa2tso", [-1000005446])



    def test_restore_source_after_review_matches_bc_category_source_and_publishes_internal_source(self):
        review_candidates_mock = Mock(return_value=1)
        publish_mock = Mock(return_value=[{"publication_id": 88}])

        result = review_workflow.restore_source_after_review(
            "Stores/99OA2TSO",
            5388,
            "category",
            target_entity_type="category",
            target_entity_id=5426,
            target_url="/bath-spa-towels/",
            reviewed_by="qa@example.com",
            note="Restore it",
            latest_candidate_rows_for_store=lambda store_hash, review_status=None, limit=None: [
                {
                    "candidate_id": 8,
                    "source_entity_type": "category",
                    "source_entity_id": -1000005388,
                    "source_product_id": -1000005388,
                    "source_url": "/hotel-towels/",
                    "review_status": "pending",
                    "target_entity_type": "category",
                    "target_entity_id": -1000005426,
                    "target_url": "https://www.hotels4humanity.com/bath-spa-towels",
                    "metadata": {"source_bc_entity_id": 5388, "target_bc_entity_id": 5426},
                }
            ],
            normalize_storefront_path_fn=lambda value: (
                ""
                if not value
                else (str(value).split("://", 1)[1].split("/", 1)[1] if "://" in str(value) else str(value)).strip().lower().rstrip("/") + "/"
                if str(value).strip() not in {"", "/"}
                else "/"
            ) if value else "",
            entity_bc_id_fn=lambda entity_type, storage_id: abs(int(storage_id or 0)) - 1000000000 if entity_type == "category" else int(storage_id or 0),
            review_candidates_fn=review_candidates_mock,
            publish_approved_entities_fn=publish_mock,
        )

        self.assertEqual(result["approved_candidate_count"], 1)
        self.assertEqual(result["publication_count"], 1)
        self.assertTrue(result["live_block_restored"])
        review_candidates_mock.assert_called_once_with([8], "approved", "qa@example.com", "Restore it")
        publish_mock.assert_called_once_with("99oa2tso", [-1000005388])


if __name__ == "__main__":
    unittest.main()
