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

from app.fulcrum import agent_review


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


class FulcrumAgentReviewTests(unittest.TestCase):
    def test_serialize_query_gate_row_for_agent_review_keeps_signals_and_targets(self):
        payload = agent_review.serialize_query_gate_row_for_agent_review(
            {
                "gate_record_id": 18,
                "representative_query": "hookless shower curtains",
                "normalized_query_key": "hookless shower curtain",
                "source_entity_type": "category",
                "source_name": "Hotel Shower Curtains",
                "source_url": "/hotel-shower-curtains/",
                "current_page_type": "category",
                "preferred_entity_type": "category",
                "query_intent_scope": "broad_product_family",
                "opportunity_score": 55,
                "demand_score": 22,
                "intent_clarity_score": 75,
                "noise_penalty": 4,
                "reason_summary": "Category intent dominates.",
                "metadata": {
                    "resolved_signals": {
                        "brand_signals": [{"label": "Hookless"}],
                        "hard_attribute_signals": [{"normalized_label": "hotel"}],
                    },
                    "semantics_analysis": {
                        "query_shape": "family_plus_modifier",
                        "head_term": "curtains",
                        "head_family": "shower curtains",
                        "eligible_page_types": ["category"],
                        "brand_family_matching_product_count": 3,
                    },
                    "query_variants": [{"query": "hookless shower curtains", "impressions_90d": 14, "avg_position_90d": 3.2}],
                },
                "suggested_target": {"entity_type": "category", "entity_id": 5451, "name": "Hookless Shower Curtains", "url": "/hookless-shower-curtains/", "score": 0.93},
                "second_option": {"entity_type": "product", "entity_id": 991, "name": "Hookless Curtain", "url": "/hookless-curtain/", "score": 0.52},
            }
        )

        self.assertEqual(payload["gate_record_id"], 18)
        self.assertEqual(payload["signals"]["brand_signals"], ["Hookless"])
        self.assertEqual(payload["signals"]["hard_attribute_signals"], ["hotel"])
        self.assertEqual(payload["winner"]["entity_type"], "category")
        self.assertEqual(payload["alternate"]["entity_type"], "product")
        self.assertEqual(payload["query_variants"][0]["impressions_90d"], 14)

    def test_postprocess_gate_agent_reviews_downgrades_false_wrong_page_type(self):
        reviews = [
            {
                "gate_record_id": 8,
                "verdict": "incorrect",
                "issue_type": "wrong_page_type",
                "recommended_action": "use_alternate",
                "rationale": "Picked the wrong page type.",
            }
        ]
        gate_row_map = {
            8: {
                "store_hash": "99oa2tso",
                "preferred_entity_type": "category",
                "query_intent_scope": "broad_product_family",
                "suggested_target": {"entity_type": "category"},
                "metadata": {},
            }
        }

        processed = agent_review.postprocess_gate_agent_reviews(
            reviews,
            gate_row_map,
            gate_row_semantics_analysis_fn=lambda row, store_hash: {},
        )

        self.assertEqual(processed[0]["verdict"], "correct")
        self.assertEqual(processed[0]["issue_type"], "looks_correct")
        self.assertEqual(processed[0]["recommended_action"], "keep_winner")
        self.assertEqual(processed[0]["cluster_key"], "correct:looks_correct:keep_winner")

    def test_store_query_gate_agent_reviews_persists_rows_and_commits(self):
        fake_cursor = _FakeCursor()
        fake_conn = _FakeConnection([fake_cursor])
        execute_batch_mock = Mock()

        with (
            patch("app.fulcrum.agent_review.get_pg_conn", return_value=fake_conn),
            patch("app.fulcrum.agent_review.execute_batch", execute_batch_mock),
        ):
            stored = agent_review.store_query_gate_agent_reviews(
                "99oa2tso",
                44,
                [
                    {
                        "gate_record_id": 8,
                        "verdict": "correct",
                        "issue_type": "looks_correct",
                        "recommended_action": "keep_winner",
                        "confidence": 0.91,
                        "cluster_key": "correct:looks_correct:keep_winner",
                        "cluster_label": "Correct / Looks Correct / Keep Winner",
                        "rationale": "Looks right.",
                        "metadata": {"initiated_by": "qa@example.com"},
                    }
                ],
                {
                    8: {
                        "normalized_query_key": "hookless shower curtain",
                        "representative_query": "hookless shower curtains",
                        "source_url": "/hotel-shower-curtains/",
                        "source_entity_type": "category",
                        "source_entity_id": -1000005446,
                        "suggested_target": {"entity_type": "category", "entity_id": 5451, "name": "Hookless Shower Curtains", "url": "/hookless-shower-curtains/", "score": 0.93},
                        "second_option": {"entity_type": "product", "entity_id": 991, "name": "Hookless Curtain", "url": "/hookless-curtain/", "score": 0.52},
                    }
                },
                model_name="gpt-test",
                created_by="qa@example.com",
            )

        self.assertEqual(stored, 1)
        self.assertTrue(fake_conn.committed)
        self.assertEqual(execute_batch_mock.call_count, 2)
        params = execute_batch_mock.call_args_list[0].args[2][0]
        self.assertEqual(params[0], 8)
        self.assertEqual(params[1], 44)
        self.assertEqual(params[9], 5451)
        metadata = json.loads(params[18])
        self.assertEqual(metadata["winner"]["entity_type"], "category")
        self.assertEqual(metadata["alternate"]["entity_type"], "product")
        feedback_params = execute_batch_mock.call_args_list[1].args[2][0]
        self.assertEqual(feedback_params[0], "looks_correct")
        self.assertEqual(feedback_params[1], "keep_winner")
        self.assertEqual(feedback_params[3], "99oa2tso")
        self.assertEqual(feedback_params[4], 8)

    def test_run_query_gate_agent_review_uses_injected_callbacks(self):
        apply_runtime_schema_mock = Mock()
        latest_gate_run_id_mock = Mock(return_value=44)
        list_query_gate_records_mock = Mock(
            return_value=[
                {
                    "gate_record_id": 8,
                    "run_id": 44,
                    "normalized_query_key": "hookless shower curtain",
                    "representative_query": "hookless shower curtains",
                    "preferred_entity_type": "category",
                    "query_intent_scope": "broad_product_family",
                    "source_url": "/hotel-shower-curtains/",
                    "source_name": "Hotel Shower Curtains",
                    "source_entity_type": "category",
                }
            ]
        )
        annotate_mock = Mock(
            return_value=[
                {
                    "gate_record_id": 8,
                    "run_id": 44,
                    "normalized_query_key": "hookless shower curtain",
                    "representative_query": "hookless shower curtains",
                    "preferred_entity_type": "category",
                    "query_intent_scope": "broad_product_family",
                    "source_url": "/hotel-shower-curtains/",
                    "source_name": "Hotel Shower Curtains",
                    "source_entity_type": "category",
                    "suggested_target": {"entity_type": "category", "entity_id": 5451, "name": "Hookless Shower Curtains"},
                    "second_option": {"entity_type": "product", "entity_id": 991, "name": "Hookless Curtain"},
                }
            ]
        )
        review_with_agent_mock = Mock(
            return_value={
                "status": "ok",
                "reason": "",
                "model_name": "gpt-test",
                "reviews": [
                    {
                        "gate_record_id": 8,
                        "verdict": "incorrect",
                        "issue_type": "wrong_page_type",
                        "recommended_action": "use_alternate",
                        "confidence": 0.74,
                        "rationale": "This looks like the wrong page type.",
                        "metadata": {"initiated_by": "qa@example.com"},
                    }
                ],
            }
        )
        store_reviews_mock = Mock(return_value=1)
        summarize_mock = Mock(return_value={"run_id": 44, "correct": 1, "incorrect": 0, "unclear": 0})
        clusters_mock = Mock(return_value=[{"cluster_key": "correct:looks_correct:keep_winner"}])

        result = agent_review.run_query_gate_agent_review(
            "99oa2tso",
            initiated_by="qa@example.com",
            apply_runtime_schema_fn=apply_runtime_schema_mock,
            get_query_gate_record_by_id_fn=lambda store_hash, gate_record_id: None,
            latest_gate_run_id_fn=latest_gate_run_id_mock,
            list_query_gate_records_fn=list_query_gate_records_mock,
            annotate_query_gate_rows_with_suggestions_fn=annotate_mock,
            summarize_query_gate_agent_reviews_fn=summarize_mock,
            list_query_gate_agent_review_clusters_fn=clusters_mock,
            gate_row_semantics_analysis_fn=lambda row, store_hash: {},
            review_query_gate_rows_with_agent_fn=review_with_agent_mock,
            store_query_gate_agent_reviews_fn=store_reviews_mock,
        )

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["stored_count"], 1)
        self.assertEqual(result["reviews"][0]["verdict"], "correct")
        apply_runtime_schema_mock.assert_called_once()
        latest_gate_run_id_mock.assert_called_once_with("99oa2tso")
        annotate_mock.assert_called_once()
        store_reviews_mock.assert_called_once()
        stored_reviews = store_reviews_mock.call_args.kwargs["reviews"]
        self.assertEqual(stored_reviews[0]["recommended_action"], "keep_winner")


if __name__ == "__main__":
    unittest.main()
