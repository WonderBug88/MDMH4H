import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

APP_ROOT = ROOT / "MDMH4H"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from app.fulcrum import direct_routing


class FulcrumDirectRoutingTests(unittest.TestCase):
    def test_looks_informational_query_detects_help_language(self):
        self.assertTrue(
            direct_routing.looks_informational_query(
                "How to wash shower curtains",
                tokenize_intent_text_fn=lambda value: set(str(value or "").lower().split()),
            )
        )
        self.assertFalse(
            direct_routing.looks_informational_query(
                "hookless shower curtains",
                tokenize_intent_text_fn=lambda value: {"hookless", "shower", "curtain"},
            )
        )

    def test_entity_type_fit_adjustment_rewards_and_penalizes_expected_targets(self):
        self.assertEqual(
            direct_routing.entity_type_fit_adjustment(
                "how to wash shower curtains",
                "content",
                "content",
                looks_informational_query_fn=lambda query: True,
            ),
            (18.0, "informational query fits content"),
        )
        self.assertEqual(
            direct_routing.entity_type_fit_adjustment(
                "hookless shower curtains",
                "product",
                "category",
                fuzzy_signal={"score": 60.0, "matched_kind": "title"},
                current_page=True,
                source_query_topic_match_count=2,
                looks_informational_query_fn=lambda query: False,
            ),
            (4.0, "current category strongly preserves the query topic"),
        )

    def test_target_prefilter_accepts_token_or_fuzzy_matches(self):
        matched = direct_routing.target_prefilter(
            "hookless curtain",
            {"tokens": {"hookless", "curtain"}, "attributes": {}, "brand_name": "", "name": "Hookless Curtain", "url": "/hookless/"},
            tokenize_intent_text_fn=lambda value: set(str(value or "").lower().split()),
            extract_attribute_terms_fn=lambda value: {},
            fuzzy_match_score_fn=lambda left, right: 0.0,
            generic_routing_tokens={"hotel"},
        )
        fuzzy_only = direct_routing.target_prefilter(
            "luxury hospitality panel",
            {"tokens": set(), "attributes": {}, "brand_name": "", "name": "Hospitality Panel", "url": "/hospitality-panel/"},
            tokenize_intent_text_fn=lambda value: set(str(value or "").lower().split()),
            extract_attribute_terms_fn=lambda value: {},
            fuzzy_match_score_fn=lambda left, right: 72.0 if "hospitality" in str(right or "").lower() else 40.0,
            generic_routing_tokens={"hotel"},
        )

        self.assertTrue(matched)
        self.assertTrue(fuzzy_only)

    def test_direct_route_candidates_from_gsc_builds_best_route_row(self):
        entity_index = {
            "sources": {
                "/hotel-shower-curtains/": {
                    "entity_type": "category",
                    "bc_entity_id": 10,
                    "url": "/hotel-shower-curtains/",
                    "name": "Hotel Shower Curtains",
                }
            },
            "targets": [
                {
                    "entity_type": "category",
                    "bc_entity_id": 22,
                    "url": "/hookless-shower-curtains/",
                    "name": "Hookless Shower Curtains",
                }
            ],
        }
        gate_rows = [
            {
                "source_url": "/hotel-shower-curtains/",
                "representative_query": "hookless shower curtains",
                "normalized_query_key": "hookless shower curtain",
                "disposition": "pass",
                "clicks_90d": 12,
                "metadata": {"semantics_analysis": {"judge_verdict": "allow"}},
                "suggested_target": {"entity_type": "category", "entity_id": 22},
            }
        ]

        rows = direct_routing.direct_route_candidates_from_gsc(
            "99oa2tso",
            entity_index=entity_index,
            gate_rows=gate_rows,
            build_unified_entity_index_fn=lambda store_hash, cluster=None: entity_index,
            load_query_target_overrides_fn=lambda store_hash: {},
            load_review_feedback_maps_fn=lambda store_hash: {"pair": {}, "family_target": {}, "target": {}},
            build_query_gate_records_fn=lambda **kwargs: gate_rows,
            normalize_storefront_path_fn=lambda value: str(value or "").strip().lower(),
            entity_storage_id_fn=lambda entity_type, entity_id: int(entity_id or 0) + (1000 if entity_type == "category" else 0),
            gate_row_query_signal_context_fn=lambda gate_row: {"brand_signals": []},
            query_target_override_key_fn=lambda normalized_query_key, source_url: (normalized_query_key or "", source_url or ""),
            target_prefilter_fn=lambda query, target_profile: True,
            build_intent_profile_fn=lambda **kwargs: {
                "passes": True,
                "score": 76.0,
                "anchor_label": "Hookless Shower Curtains",
                "topic_key": "hookless shower curtain",
                "anchor_label_source": "target_name",
                "anchor_quality": 88.0,
                "reason_summary": "Strong topical match",
                "reasons": ["topic overlap"],
                "shared_tokens": ["hookless", "curtain"],
                "query_target_tokens": ["hookless", "curtain"],
                "query_source_tokens": ["curtain"],
                "attributes": {},
                "fuzzy_signal": {"score": 66.0, "matched_kind": "title"},
                "ga4_signal": {},
                "source_primary_cluster": "curtains",
                "target_primary_cluster": "curtains",
                "query_intent_scope": "commercial_topic",
                "preferred_entity_type": "category",
                "source_query_topic_match_count": 2,
                "query_signals": {"brand_signals": [], "collection_signals": [], "sku_signals": []},
            },
            build_review_feedback_signal_fn=lambda **kwargs: {
                "active": True,
                "delta": 4.0,
                "summary": "Past approvals support this target",
                "reason": "past approvals rewarded this target for similar queries",
            },
            entity_type_fit_adjustment_fn=lambda **kwargs: (16.0, "page type matches query intent"),
            append_reason_summary_fn=lambda base, extra: f"{base}; {extra}" if extra else (base or ""),
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["score"], 96.0)
        self.assertEqual(rows[0]["source_product_id"], 1010)
        self.assertEqual(rows[0]["target_product_id"], 1022)
        self.assertEqual(rows[0]["metadata"]["reason_summary"], "Strong topical match; Past approvals support this target")
        self.assertEqual(rows[0]["metadata"]["semantics_analysis"]["judge_verdict"], "allow")


if __name__ == "__main__":
    unittest.main()
