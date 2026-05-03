import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

APP_ROOT = ROOT / "MDMH4H"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from app.fulcrum import routing_semantics


class FulcrumRoutingSemanticsTests(unittest.TestCase):
    def test_gate_row_query_signal_context_reads_metadata_snapshot(self):
        result = routing_semantics.gate_row_query_signal_context(
            {"metadata": {"resolved_signals": {"brand_signals": ["hookless"]}}}
        )

        self.assertEqual(result, {"brand_signals": ["hookless"]})

    def test_gate_row_semantics_analysis_uses_cached_or_builds_new(self):
        cached = routing_semantics.gate_row_semantics_analysis(
            {"metadata": {"semantics_analysis": {"normalized_query": "hookless curtain"}}},
            "99oa2tso",
            gate_row_query_signal_context_fn=lambda gate_row: None,
            resolve_query_signal_context_fn=lambda **kwargs: {"signals": True},
            build_query_semantics_analysis_fn=lambda **kwargs: {"normalized_query": "fresh"},
        )
        self.assertEqual(cached["normalized_query"], "hookless curtain")

        built = routing_semantics.gate_row_semantics_analysis(
            {"representative_query": "Hookless Shower Curtains", "metadata": {}},
            "99oa2tso",
            gate_row_query_signal_context_fn=lambda gate_row: {"resolved": True},
            resolve_query_signal_context_fn=lambda **kwargs: {"signals": False},
            build_query_semantics_analysis_fn=lambda **kwargs: {"normalized_query": "hookless curtain", "used_resolved": kwargs["resolved_signals"]["resolved"]},
        )
        self.assertTrue(built["used_resolved"])

    def test_semantics_target_block_reason_checks_rule_types(self):
        analysis = {
            "normalized_query": "hookless shower curtain",
            "eligible_page_types": ["category", "product"],
            "constraint_rules": [
                {"kind": "require_head_term_presence", "head_term": "curtain", "message": "missing head term"},
            ],
        }
        reason = routing_semantics.semantics_target_block_reason(
            analysis,
            {"entity_type": "category", "name": "Hookless Panel", "url": "/hookless-panel/"},
            tokenize_intent_text_fn=lambda value: set(str(value or "").lower().replace("/", " ").split()),
            normalize_signal_label_fn=lambda value: str(value or "").strip().lower(),
            semantic_pluralize_fn=lambda value: f"{value}s",
        )

        self.assertEqual(reason, "missing head term")

    def test_brand_family_query_blocks_broad_category_fallback(self):
        analysis = {
            "normalized_query": "downlite blankets",
            "eligible_page_types": ["brand", "category"],
            "constraint_rules": [
                {
                    "kind": "prefer_brand_when_family_has_multiple_products",
                    "brand_label": "downlite",
                    "message": "brand-family query cannot fall back to a broad category",
                }
            ],
        }

        broad_reason = routing_semantics.semantics_target_block_reason(
            analysis,
            {"entity_type": "category", "name": "Hotel Bedding Supply", "url": "/hotel-bedding-supply/"},
            tokenize_intent_text_fn=lambda value: set(str(value or "").lower().replace("/", " ").replace("-", " ").split()),
            normalize_signal_label_fn=lambda value: str(value or "").strip().lower(),
            semantic_pluralize_fn=lambda value: f"{value}s",
        )
        brand_category_reason = routing_semantics.semantics_target_block_reason(
            analysis,
            {"entity_type": "category", "name": "Downlite Blankets", "url": "/downlite-blankets/"},
            tokenize_intent_text_fn=lambda value: set(str(value or "").lower().replace("/", " ").replace("-", " ").split()),
            normalize_signal_label_fn=lambda value: str(value or "").strip().lower(),
            semantic_pluralize_fn=lambda value: f"{value}s",
        )
        brand_page_reason = routing_semantics.semantics_target_block_reason(
            analysis,
            {"entity_type": "brand", "name": "DownLite Bedding", "url": "/downlite/"},
            tokenize_intent_text_fn=lambda value: set(str(value or "").lower().replace("/", " ").replace("-", " ").split()),
            normalize_signal_label_fn=lambda value: str(value or "").strip().lower(),
            semantic_pluralize_fn=lambda value: f"{value}s",
        )

        self.assertEqual(broad_reason, "brand-family query cannot fall back to a broad category")
        self.assertIsNone(brand_category_reason)
        self.assertIsNone(brand_page_reason)

    def test_apply_semantics_control_falls_back_to_current_page_when_top_target_blocked(self):
        result, semantics = routing_semantics.apply_semantics_control_to_ranked_targets(
            {"source_entity_id": 10, "source_url": "/hotel-shower-curtains/"},
            [{"entity_type": "product", "entity_id": 22}],
            store_hash="99oa2tso",
            source_profile={"entity_type": "category", "bc_entity_id": 10, "url": "/hotel-shower-curtains/", "name": "Hotel Shower Curtains"},
            target_entities_by_key={("product", 22): {"entity_type": "product"}},
            source_profiles={},
            target_entities=[],
            gate_row_semantics_analysis_fn=lambda gate_row, store_hash, signal_library=None: {"ambiguity_level": "high", "negative_constraints": []},
            gate_row_current_page_snapshot_fn=lambda gate_row, source_profile=None: {"entity_type": "category", "entity_id": 10, "url": "/hotel-shower-curtains/"},
            semantics_target_block_reason_fn=lambda semantics_analysis, target_profile=None: None if (target_profile or {}).get("entity_type") == "category" else "blocked",
            rank_target_options_for_gate_row_fn=lambda **kwargs: [],
        )

        self.assertEqual(result[0]["entity_type"], "category")
        self.assertEqual(semantics["judge_verdict"], "reject")
        self.assertTrue(semantics["resolver_invoked"])


if __name__ == "__main__":
    unittest.main()
