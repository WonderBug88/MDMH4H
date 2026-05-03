import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

APP_ROOT = ROOT / "MDMH4H"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from app.fulcrum import routing_ranker


class FulcrumRoutingRankerTests(unittest.TestCase):
    def test_build_review_feedback_signal_aggregates_pair_family_and_target(self):
        signal = routing_ranker.build_review_feedback_signal(
            "Hookless Curtain",
            "category",
            10,
            "product",
            22,
            feedback_maps={
                "pair": {("category", 10, "product", 22): {"approved_count": 2, "rejected_count": 0}},
                "family_target": {("hookless curtain", "product", 22): {"approved_count": 1, "rejected_count": 0}},
                "target": {("product", 22): {"approved_count": 3, "rejected_count": 0}},
            },
            normalize_query_family_key_fn=lambda value: str(value or "").strip().lower(),
        )

        self.assertTrue(signal["active"])
        self.assertGreater(signal["delta"], 0)
        self.assertEqual(signal["summary"], "Past approvals support this connection")

    def test_append_reason_summary_avoids_duplicate_text(self):
        self.assertEqual(
            routing_ranker.append_reason_summary("Strong route", "Past approvals support this target"),
            "Strong route; Past approvals support this target",
        )
        self.assertEqual(
            routing_ranker.append_reason_summary("Past approvals support this target", "support this target"),
            "Past approvals support this target",
        )

    def test_rank_target_options_for_gate_row_prefers_manual_override(self):
        gate_row = {
            "source_url": "/hotel-shower-curtains/",
            "representative_query": "hookless shower curtains",
            "normalized_query_key": "hookless curtain",
            "preferred_entity_type": "category",
            "query_intent_scope": "commercial_topic",
            "clicks_90d": 50,
            "metadata": {},
        }
        source_profiles = {
            "/hotel-shower-curtains/": {
                "entity_type": "category",
                "bc_entity_id": 10,
                "store_hash": "99oa2tso",
                "url": "/hotel-shower-curtains/",
                "name": "Hotel Shower Curtains",
            }
        }
        target_entities = [
            {"entity_type": "category", "bc_entity_id": 22, "url": "/hookless-shower-curtains/", "name": "Hookless Shower Curtains"},
            {"entity_type": "category", "bc_entity_id": 23, "url": "/plain-shower-curtains/", "name": "Plain Shower Curtains"},
        ]

        ranked = routing_ranker.rank_target_options_for_gate_row(
            gate_row,
            source_profiles,
            target_entities,
            overrides={("hookless curtain", "/hotel-shower-curtains/"): {"target_entity_type": "category", "target_entity_id": 22}},
            review_feedback_maps={"pair": {}, "family_target": {}, "target": {}},
            normalize_storefront_path_fn=lambda value: str(value or "").strip(),
            gate_row_query_signal_context_fn=lambda gate_row: {},
            tokenize_intent_text_fn=lambda value: set(str(value or "").lower().replace("/", " ").split()),
            gate_row_semantics_analysis_fn=lambda gate_row, store_hash, signal_library=None: {"ambiguity_level": "low"},
            query_target_override_key_fn=lambda normalized_query_key, source_url: (normalized_query_key or "", source_url or ""),
            semantics_target_block_reason_fn=lambda semantics_analysis, target_profile=None: None,
            target_prefilter_fn=lambda query, target_profile: True,
            build_intent_profile_fn=lambda **kwargs: {
                "passes": True,
                "raw_score": 70 if "Hookless" in kwargs["target_name"] else 80,
                "score": 70 if "Hookless" in kwargs["target_name"] else 80,
                "reason_summary": kwargs["target_name"],
                "anchor_label": "anchor",
                "preferred_entity_type": "category",
                "fuzzy_signal": {"score": 0},
                "source_query_topic_match_count": 2,
                "source_query_topic_missing_count": 0,
                "source_query_modifier_match_count": 1,
                "source_query_modifier_missing_count": 0,
            },
            build_review_feedback_signal_fn=lambda **kwargs: {"delta": 0.0, "summary": "", "active": False},
            entity_type_fit_adjustment_fn=lambda **kwargs: (0.0, "fit"),
            append_reason_summary_fn=routing_ranker.append_reason_summary,
            apply_semantics_control_to_ranked_targets_fn=lambda gate_row, ranked_targets, **kwargs: (ranked_targets, {"judge_verdict": "allow"}),
        )

        self.assertEqual(ranked[0]["entity_id"], 22)
        self.assertTrue(ranked[0]["manual_override"])
        self.assertEqual(ranked[0]["score"], 100.0)

    def test_rank_target_options_for_gate_row_prefers_current_page_when_gsc_and_qualifiers_align(self):
        gate_row = {
            "source_url": "/1888-mills-towels-lotus-100-egyptian-cotton-wholesale-in-bulk/",
            "representative_query": "egyptian cotton towels moscow buy",
            "normalized_query_key": "egyptian cotton towel",
            "preferred_entity_type": "category",
            "query_intent_scope": "commercial_topic",
            "clicks_90d": 44,
            "impressions_90d": 1314,
            "avg_position_90d": 9.77,
            "metadata": {},
        }
        source_profiles = {
            "/1888-mills-towels-lotus-100-egyptian-cotton-wholesale-in-bulk/": {
                "entity_type": "product",
                "bc_entity_id": 9917,
                "store_hash": "99oa2tso",
                "url": "/1888-mills-towels-lotus-100-egyptian-cotton-wholesale-in-bulk/",
                "name": "1888 Mills Towels | Lotus 100% Egyptian Cotton | Wholesale in Bulk",
            }
        }
        target_entities = [
            {
                "entity_type": "product",
                "bc_entity_id": 9917,
                "url": "/1888-mills-towels-lotus-100-egyptian-cotton-wholesale-in-bulk/",
                "name": "1888 Mills Towels | Lotus 100% Egyptian Cotton | Wholesale in Bulk",
            },
            {
                "entity_type": "category",
                "bc_entity_id": 22,
                "url": "/organic-towels/",
                "name": "Organic Towels",
            },
        ]

        def _profile(**kwargs):
            if (kwargs.get("target_profile") or {}).get("entity_type") == "product":
                return {
                    "passes": True,
                    "raw_score": 72,
                    "score": 72,
                    "reason_summary": "current product preserves egyptian cotton towels",
                    "anchor_label": "Egyptian Cotton Towels",
                    "preferred_entity_type": "product",
                    "fuzzy_signal": {"score": 70},
                    "source_query_topic_match_count": 1,
                    "source_query_topic_missing_count": 0,
                    "source_query_modifier_match_count": 2,
                    "source_query_modifier_missing_count": 0,
                }
            return {
                "passes": True,
                "raw_score": 90,
                "score": 90,
                "reason_summary": "broad towel category",
                "anchor_label": "Organic Towels",
                "preferred_entity_type": "category",
                "fuzzy_signal": {"score": 82},
                "source_query_topic_match_count": 1,
                "source_query_topic_missing_count": 0,
                "source_query_modifier_match_count": 0,
                "source_query_modifier_missing_count": 2,
            }

        ranked = routing_ranker.rank_target_options_for_gate_row(
            gate_row,
            source_profiles,
            target_entities,
            overrides={},
            review_feedback_maps={"pair": {}, "family_target": {}, "target": {}},
            normalize_storefront_path_fn=lambda value: str(value or "").strip(),
            gate_row_query_signal_context_fn=lambda gate_row: {"soft_attribute_signals": [{"matched_tokens": ["cotton"]}]},
            tokenize_intent_text_fn=lambda value: set(str(value or "").lower().replace("/", " ").replace("-", " ").split()),
            gate_row_semantics_analysis_fn=lambda gate_row, store_hash, signal_library=None: {"ambiguity_level": "low"},
            query_target_override_key_fn=lambda normalized_query_key, source_url: (normalized_query_key or "", source_url or ""),
            semantics_target_block_reason_fn=lambda semantics_analysis, target_profile=None: None,
            target_prefilter_fn=lambda query, target_profile: True,
            build_intent_profile_fn=_profile,
            build_review_feedback_signal_fn=lambda **kwargs: {"delta": 0.0, "summary": "", "active": False},
            entity_type_fit_adjustment_fn=lambda **kwargs: (0.0, "fit"),
            append_reason_summary_fn=routing_ranker.append_reason_summary,
            apply_semantics_control_to_ranked_targets_fn=lambda gate_row, ranked_targets, **kwargs: (ranked_targets, {"judge_verdict": "allow"}),
        )

        self.assertEqual(ranked[0]["entity_type"], "product")
        self.assertEqual(ranked[0]["entity_id"], 9917)

    def test_rank_target_options_for_gate_row_prefers_exact_brand_name_over_subbrand(self):
        gate_row = {
            "source_url": "/hotel-towels/",
            "representative_query": "1888 mills",
            "normalized_query_key": "1888 mills",
            "preferred_entity_type": "brand",
            "query_intent_scope": "brand_navigation",
            "clicks_90d": 10,
            "metadata": {},
        }
        source_profiles = {
            "/hotel-towels/": {
                "entity_type": "category",
                "bc_entity_id": 10,
                "store_hash": "99oa2tso",
                "url": "/hotel-towels/",
                "name": "Hotel Towels",
            }
        }
        target_entities = [
            {"entity_type": "brand", "bc_entity_id": 3030, "url": "/suite-touch-by-1888-mills/", "name": "Suite Touch by 1888 Mills"},
            {"entity_type": "brand", "bc_entity_id": 2966, "url": "/1888-mills/", "name": "1888 Mills"},
        ]

        def _profile(**kwargs):
            target_name = kwargs["target_name"]
            return {
                "passes": True,
                "raw_score": 120 if target_name.startswith("Suite") else 80,
                "score": 100 if target_name.startswith("Suite") else 80,
                "reason_summary": target_name,
                "anchor_label": target_name,
                "preferred_entity_type": "brand",
                "fuzzy_signal": {"score": 90},
                "source_query_topic_match_count": 0,
                "source_query_topic_missing_count": 0,
                "source_query_modifier_match_count": 0,
                "source_query_modifier_missing_count": 0,
            }

        ranked = routing_ranker.rank_target_options_for_gate_row(
            gate_row,
            source_profiles,
            target_entities,
            overrides={},
            review_feedback_maps={"pair": {}, "family_target": {}, "target": {}},
            normalize_storefront_path_fn=lambda value: str(value or "").strip(),
            gate_row_query_signal_context_fn=lambda gate_row: {"brand_signals": [{"matched_tokens": ["1888", "mills"]}]},
            tokenize_intent_text_fn=lambda value: set(str(value or "").lower().replace("/", " ").replace("-", " ").split()),
            gate_row_semantics_analysis_fn=lambda gate_row, store_hash, signal_library=None: {"ambiguity_level": "low"},
            query_target_override_key_fn=lambda normalized_query_key, source_url: (normalized_query_key or "", source_url or ""),
            semantics_target_block_reason_fn=lambda semantics_analysis, target_profile=None: None,
            target_prefilter_fn=lambda query, target_profile: True,
            build_intent_profile_fn=_profile,
            build_review_feedback_signal_fn=lambda **kwargs: {"delta": 0.0, "summary": "", "active": False},
            entity_type_fit_adjustment_fn=lambda **kwargs: (0.0, "fit"),
            append_reason_summary_fn=routing_ranker.append_reason_summary,
            apply_semantics_control_to_ranked_targets_fn=lambda gate_row, ranked_targets, **kwargs: (ranked_targets, {"judge_verdict": "allow"}),
        )

        self.assertEqual(ranked[0]["entity_id"], 2966)
        self.assertTrue(ranked[0]["exact_brand_name_match"])

    def test_refresh_query_gate_row_live_state_rebuilds_metrics(self):
        refreshed = routing_ranker.refresh_query_gate_row_live_state(
            "99oa2tso",
            {
                "normalized_query_key": "hookless curtain",
                "representative_query": "hookless shower curtains",
                "source_url": "/hotel-shower-curtains/",
                "metadata": {"query_variants": [{"query": "hookless shower curtains"}]},
            },
            source_profiles={},
            target_entities=[],
            normalize_storefront_path_fn=lambda value: str(value or "").strip(),
            build_query_gate_record_fn=lambda **kwargs: {
                "current_page_type": "category",
                "query_intent_scope": "commercial_topic",
                "preferred_entity_type": "category",
                "clicks_90d": 50,
                "impressions_90d": 500,
                "metadata": {"resolved_signals": {"brand_signals": []}},
            },
            build_store_signal_library_fn=lambda store_hash: {"signals": True},
        )

        self.assertEqual(refreshed["query_intent_scope"], "commercial_topic")
        self.assertEqual(refreshed["clicks_90d"], 50)
        self.assertIn("resolved_signals", refreshed["metadata"])


if __name__ == "__main__":
    unittest.main()
