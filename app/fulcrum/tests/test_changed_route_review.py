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

from app.fulcrum import changed_route_review


class FulcrumChangedRouteReviewTests(unittest.TestCase):
    def test_attach_changed_route_agent_reviews_copies_reviews(self):
        rows = [{"gate_record_id": 8, "representative_query": "hookless shower curtains"}]
        review_map = {8: {"verdict": "incorrect", "recommended_action": "use_alternate"}}

        annotated = changed_route_review.attach_changed_route_agent_reviews(rows, review_map)

        self.assertEqual(annotated[0]["agent_review"]["verdict"], "incorrect")
        self.assertIsNot(annotated[0]["agent_review"], review_map[8])

    def test_fallback_changed_route_review_reasoning_groups_problem_patterns(self):
        rows = [
            {"gate_record_id": 8, "representative_query": "hookless shower curtains"},
            {"gate_record_id": 9, "representative_query": "luxury bath towels"},
            {"gate_record_id": 10, "representative_query": "hotel bedding"},
        ]
        review_map = {
            8: {
                "verdict": "incorrect",
                "issue_type": "wrong_page_type",
                "recommended_action": "tune_logic",
                "cluster_label": "Incorrect / Wrong Page Type / Tune Logic",
                "rationale": "Category routing is overfiring.",
                "confidence": 0.8,
            },
            9: {
                "verdict": "unclear",
                "issue_type": "needs_human_review",
                "recommended_action": "manual_review",
                "cluster_label": "Unclear / Needs Human Review / Manual Review",
                "rationale": "Ambiguous intent.",
                "confidence": 0.6,
            },
            10: {"verdict": "correct", "confidence": 0.9},
        }

        payload = changed_route_review.fallback_changed_route_review_reasoning(rows, review_map)

        self.assertEqual(payload["status"], "fallback")
        self.assertIn("1 changed route(s) look incorrect", payload["summary_text"])
        self.assertEqual(len(payload["patterns"]), 2)
        self.assertEqual(sorted(pattern["gate_record_ids"][0] for pattern in payload["patterns"]), [8, 9])
        self.assertTrue(payload["generated_at"])

    def test_get_cached_changed_route_review_reasoning_prefers_cached_payload(self):
        load_cache_mock = Mock(return_value={"status": "cached", "summary_text": "cached"})
        latest_gate_run_id_mock = Mock(return_value=44)

        payload = changed_route_review.get_cached_changed_route_review_reasoning(
            "99oa2tso",
            rows=[{"gate_record_id": 8}],
            latest_gate_run_id_fn=latest_gate_run_id_mock,
            load_admin_metric_cache_fn=load_cache_mock,
            store_admin_metric_cache_fn=Mock(),
            gate_review_map_for_ids_fn=Mock(),
            reason_about_changed_route_reviews_with_agent_fn=Mock(),
        )

        self.assertEqual(payload["status"], "cached")
        latest_gate_run_id_mock.assert_called_once_with("99oa2tso")
        load_cache_mock.assert_called_once()

    def test_run_changed_route_agent_review_orchestrates_reasoning_refresh(self):
        latest_gate_run_id_mock = Mock(return_value=44)
        list_rows_mock = Mock(
            return_value=[
                {"gate_record_id": 8, "representative_query": "hookless shower curtains"},
                {"gate_record_id": 9, "representative_query": "luxury bath towels"},
            ]
        )
        run_gate_review_mock = Mock(return_value={"status": "ok", "reviewed_count": 2, "stored_count": 2})
        review_map_mock = Mock(
            return_value={
                8: {"verdict": "incorrect"},
                9: {"verdict": "unclear"},
            }
        )
        reasoning_mock = Mock(return_value={"status": "ok", "summary_text": "Pattern summary"})

        result = changed_route_review.run_changed_route_agent_review(
            "99oa2tso",
            initiated_by="qa@example.com",
            latest_gate_run_id_fn=latest_gate_run_id_mock,
            list_changed_route_results_fn=list_rows_mock,
            run_query_gate_agent_review_fn=run_gate_review_mock,
            gate_review_map_for_ids_fn=review_map_mock,
            get_cached_changed_route_review_reasoning_fn=reasoning_mock,
        )

        self.assertEqual(result["reasoning"]["status"], "ok")
        self.assertEqual(result["changed_route_review_summary"]["incorrect"], 1)
        self.assertEqual(result["changed_route_review_summary"]["unclear"], 1)
        run_gate_review_mock.assert_called_once_with(
            store_hash="99oa2tso",
            run_id=44,
            gate_record_ids=[8, 9],
            initiated_by="qa@example.com",
        )
        reasoning_mock.assert_called_once()
        self.assertTrue(reasoning_mock.call_args.kwargs["force_refresh"])


if __name__ == "__main__":
    unittest.main()
