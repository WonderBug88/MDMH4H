import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

APP_ROOT = ROOT / "MDMH4H"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from app.fulcrum import query_gate_builder


def _tokenize(value):
    return {token for token in str(value or "").lower().replace("-", " ").split() if token}


class FulcrumQueryGateBuilderTests(unittest.TestCase):
    def test_expected_ctr_for_position_uses_bucket_thresholds(self):
        self.assertEqual(query_gate_builder.expected_ctr_for_position(2), 0.16)
        self.assertEqual(query_gate_builder.expected_ctr_for_position(8), 0.045)
        self.assertEqual(query_gate_builder.expected_ctr_for_position(25), 0.01)

    def test_build_freshness_context_computes_rising_trend(self):
        result = query_gate_builder.build_freshness_context(50, 500, 90, 900)

        self.assertEqual(result["trend_label"], "rising")
        self.assertGreater(result["click_delta_pct"], 20)

    def test_build_query_gate_record_assembles_metrics_and_metadata(self):
        resolved_signals = {
            "query_attrs": {"color": ["white"]},
            "brand_signals": [{"matched_tokens": ["hookless"], "normalized_label": "hookless"}],
            "hard_attribute_signals": [],
            "soft_attribute_signals": [{"matched_tokens": ["white"], "normalized_label": "white"}],
            "collection_signals": [],
            "topic_signals": [{"matched_tokens": ["curtain"], "normalized_label": "curtain"}],
            "sku_signals": [],
        }
        semantics_analysis = {
            "query_shape": "exact_product_like",
            "brand_confidence": 0.81,
            "pdp_confidence": 0.88,
        }
        evidence_rows = [
            {
                "query": "hookless white curtain",
                "source_url": "/hotel-shower-curtains/",
                "source_name": "Hotel Shower Curtains",
                "clicks_28d": 10,
                "impressions_28d": 100,
                "clicks_90d": 25,
                "impressions_90d": 300,
                "avg_position_28d": 14.0,
                "avg_position_90d": 16.0,
            },
            {
                "query": "hookless curtain white",
                "source_url": "/hotel-shower-curtains/",
                "source_name": "Hotel Shower Curtains",
                "clicks_28d": 6,
                "impressions_28d": 80,
                "clicks_90d": 20,
                "impressions_90d": 240,
                "avg_position_28d": 12.0,
                "avg_position_90d": 13.0,
            },
        ]

        record = query_gate_builder.build_query_gate_record(
            "99oa2tso",
            "hookless curtain",
            "hookless white curtain",
            evidence_rows,
            {
                "/hotel-shower-curtains/": {
                    "entity_type": "category",
                    "bc_entity_id": 10,
                    "name": "Hotel Shower Curtains",
                    "url": "/hotel-shower-curtains/",
                }
            },
            [{"entity_type": "product", "entity_id": 22}],
            None,
            normalize_storefront_path_fn=lambda value: str(value or "").strip().lower(),
            tokenize_intent_text_fn=_tokenize,
            resolve_query_signal_context_fn=lambda **kwargs: resolved_signals,
            classify_query_intent_scope_fn=lambda **kwargs: ("specific_product", "product"),
            build_query_semantics_analysis_fn=lambda **kwargs: semantics_analysis,
            fuzzy_match_score_fn=lambda query, candidate: 84.0 if "hotel" in str(candidate).lower() else 72.0,
            expected_ctr_for_position_fn=lambda avg_position: 0.09,
            build_freshness_context_fn=lambda clicks_28d, impressions_28d, clicks_90d, impressions_90d: {"trend_label": "stable"},
            query_noise_words={"the", "and"},
        )

        self.assertIsNotNone(record)
        self.assertEqual(record["source_url"], "/hotel-shower-curtains/")
        self.assertEqual(record["preferred_entity_type"], "product")
        self.assertEqual(record["current_page_type"], "category")
        self.assertEqual(record["freshness_context"]["trend_label"], "stable")
        self.assertEqual(record["metadata"]["query_variant_count"], 2)
        self.assertEqual(record["metadata"]["resolved_signals"], resolved_signals)
        self.assertEqual(record["metadata"]["semantics_analysis"], semantics_analysis)
        self.assertEqual(record["metadata"]["query_variants"][0]["query"], "hookless white curtain")
        self.assertIn(record["disposition"], {"pass", "hold"})

    def test_build_query_gate_record_does_not_hold_just_for_current_page_preservation(self):
        resolved_signals = {
            "query_attrs": {"material": ["cotton"]},
            "brand_signals": [],
            "hard_attribute_signals": [],
            "soft_attribute_signals": [{"matched_tokens": ["cotton"], "normalized_label": "cotton"}],
            "collection_signals": [],
            "topic_signals": [{"matched_tokens": ["towels"], "normalized_label": "towel"}],
            "sku_signals": [],
        }
        semantics_analysis = {
            "query_shape": "broad_descriptive",
            "head_term": "towel",
            "brand_confidence": 0.0,
            "pdp_confidence": 0.22,
        }
        evidence_rows = [
            {
                "query": "egyptian cotton towels moscow buy",
                "source_url": "/1888-mills-towels-lotus-100-egyptian-cotton-wholesale-in-bulk/",
                "source_name": "1888 Mills Towels | Lotus 100% Egyptian Cotton | Wholesale in Bulk",
                "clicks_28d": 12,
                "impressions_28d": 275,
                "clicks_90d": 44,
                "impressions_90d": 1314,
                "avg_position_28d": 10.75,
                "avg_position_90d": 9.77,
            }
        ]

        record = query_gate_builder.build_query_gate_record(
            "99oa2tso",
            "egyptian cotton towels moscow buy",
            "egyptian cotton towels moscow buy",
            evidence_rows,
            {
                "/1888-mills-towels-lotus-100-egyptian-cotton-wholesale-in-bulk/": {
                    "entity_type": "product",
                    "bc_entity_id": 9917,
                    "name": "1888 Mills Towels | Lotus 100% Egyptian Cotton | Wholesale in Bulk",
                    "url": "/1888-mills-towels-lotus-100-egyptian-cotton-wholesale-in-bulk/",
                }
            },
            [{"entity_type": "category", "entity_id": 22}],
            None,
            normalize_storefront_path_fn=lambda value: str(value or "").strip().lower(),
            tokenize_intent_text_fn=_tokenize,
            resolve_query_signal_context_fn=lambda **kwargs: resolved_signals,
            classify_query_intent_scope_fn=lambda **kwargs: ("commercial_topic", "category"),
            build_query_semantics_analysis_fn=lambda **kwargs: semantics_analysis,
            fuzzy_match_score_fn=lambda query, candidate: 53.36,
            expected_ctr_for_position_fn=lambda avg_position: 0.045,
            build_freshness_context_fn=lambda clicks_28d, impressions_28d, clicks_90d, impressions_90d: {"trend_label": "stable"},
            query_noise_words={"the", "and"},
        )

        self.assertIsNotNone(record)
        self.assertEqual(record["preferred_entity_type"], "category")
        self.assertEqual(record["current_page_type"], "product")
        self.assertNotEqual(record["disposition"], "hold")
        self.assertNotIn("Google already aligns this query", record["reason_summary"])
        self.assertTrue(record["metadata"]["current_page_preservation_guard"]["active"])
        self.assertEqual(
            record["metadata"]["current_page_preservation_guard"]["leading_qualifiers"],
            ["egyptian", "cotton"],
        )


if __name__ == "__main__":
    unittest.main()
