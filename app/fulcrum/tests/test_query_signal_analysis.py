import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

APP_ROOT = ROOT / "MDMH4H"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from app.fulcrum import query_signal_analysis


def _tokenize(value):
    return {token for token in str(value or "").lower().replace("-", " ").split() if token}


class FulcrumQuerySignalAnalysisTests(unittest.TestCase):
    def test_build_fallback_query_signal_context_derives_brand_topic_and_sku_signals(self):
        result = query_signal_analysis.build_fallback_query_signal_context(
            "Hookless hotel curtain H123",
            {"hookless", "hotel", "curtain", "h123"},
            source_profile={"brand_name": "Hookless"},
            extract_attribute_terms_fn=lambda value: {"material": {"vinyl"}, "color": {"white"}},
            tokenize_intent_text_fn=_tokenize,
            fallback_signal_match_fn=lambda signal_kind, label, **kwargs: {
                "signal_kind": signal_kind,
                "label": label,
                "normalized_label": kwargs.get("normalized_label", label),
                "matched_tokens": sorted(kwargs.get("matched_tokens") or []),
                "bucket_key": kwargs.get("bucket_key"),
                "confidence": kwargs.get("confidence"),
            },
            has_model_or_sku_signal_fn=lambda value: "h123" in str(value).lower(),
            serialize_query_signal_matches_fn=lambda items: list(items),
            signal_kind_from_bucket_fn=lambda bucket: {
                "material": "soft_attribute",
                "color": "soft_attribute",
            }.get(bucket),
            expand_signal_tokens_fn=lambda tokens: set(tokens),
            non_generic_signal_tokens_fn=lambda tokens: {token for token in tokens if token not in {"hotel"}},
            topic_priority={"hotel"},
            topic_display_map={"curtain": "Curtain"},
            material_tokens={"vinyl"},
            form_tokens=set(),
        )

        self.assertEqual(result["brand_signals"][0]["label"], "hookless")
        self.assertEqual([signal["label"] for signal in result["topic_signals"]], ["curtain"])
        self.assertEqual(result["sku_signals"][0]["signal_kind"], "sku_pattern")
        self.assertEqual(
            sorted(signal["label"] for signal in result["soft_attribute_signals"]),
            ["vinyl", "white"],
        )

    def test_resolve_query_signal_context_filters_brand_overlap_with_soft_attribute_tokens(self):
        signal_library = {
            "brand_alias": [
                {
                    "raw_label": "Champagne",
                    "normalized_label": "champagne",
                    "matched_tokens": ["champagne"],
                    "tokens": ["champagne"],
                }
            ],
            "soft_attribute": [
                {
                    "raw_label": "Champagne",
                    "normalized_label": "champagne",
                    "matched_tokens": ["champagne"],
                    "tokens": ["champagne"],
                    "metadata": {"bucket_key": "color"},
                }
            ],
            "hard_attribute": [],
            "collection": [],
            "topic_token": [],
            "sku_pattern": [],
        }

        result = query_signal_analysis.resolve_query_signal_context(
            store_hash=None,
            example_query="champagne curtain",
            signal_library=signal_library,
            tokenize_intent_text_fn=_tokenize,
            build_store_signal_library_fn=lambda store_hash: {},
            match_store_signal_entries_fn=lambda query, query_tokens, entries, signal_kind: [
                {
                    "label": entry.get("raw_label"),
                    "normalized_label": entry.get("normalized_label"),
                    "matched_tokens": list(entry.get("matched_tokens") or []),
                    "bucket_key": (entry.get("metadata") or {}).get("bucket_key"),
                }
                for entry in entries
            ],
            extract_attribute_terms_fn=lambda value: {},
            query_has_explicit_attribute_intent_fn=lambda attrs, bucket, query_tokens: False,
            build_fallback_query_signal_context_fn=lambda **kwargs: {
                "brand_signals": [],
                "hard_attribute_signals": [],
                "soft_attribute_signals": [],
                "collection_signals": [],
                "topic_signals": [],
                "sku_signals": [],
            },
            match_has_specific_attribute_tokens_fn=lambda query_tokens, match, signal_kind: True,
            expand_signal_tokens_fn=lambda tokens: set(tokens),
            serialize_query_signal_matches_fn=lambda items: list(items),
        )

        self.assertEqual(result["brand_signals"], [])
        self.assertEqual(result["soft_attribute_signals"][0]["normalized_label"], "champagne")
        self.assertEqual(result["query_attrs"], {"color": ["champagne"]})

    def test_build_query_semantics_analysis_returns_category_shape_for_family_query(self):
        analysis = query_signal_analysis.build_query_semantics_analysis(
            "99oa2tso",
            "hotel curtain",
            {
                "query_tokens": ["hotel", "curtain"],
                "brand_signals": [],
                "hard_attribute_signals": [],
                "soft_attribute_signals": [],
                "collection_signals": [],
                "topic_signals": [
                    {"matched_tokens": ["hotel", "curtain"], "entity_type": "category", "normalized_label": "hotel curtain"}
                ],
                "sku_signals": [],
            },
            signal_library={"protected_phrase": [], "taxonomy_alias": [], "ambiguous_modifier": []},
            build_store_signal_library_fn=lambda store_hash: {},
            ordered_intent_tokens_fn=lambda value: list(_tokenize(value)),
            expand_signal_tokens_fn=lambda tokens: set(tokens),
            tokenize_intent_text_fn=_tokenize,
            non_generic_signal_tokens_fn=lambda tokens: set(tokens),
            match_semantic_signal_entries_fn=lambda query, query_tokens, entries: [],
            semantic_head_term_fn=lambda query, query_tokens, bound_phrase_matches, resolved_signals: "curtain",
            semantic_head_family_fn=lambda head_term, query_tokens, bound_phrase_matches, taxonomy_alias_matches: "curtains",
            query_has_exact_brand_phrase_fn=lambda query, brand_signals: 0.0,
            query_is_broad_descriptive_fn=lambda query, query_tokens, resolved_signals: False,
            semantic_family_candidate_tokens_fn=lambda head_term, head_family, taxonomy_alias_matches: {"curtain"},
            normalize_signal_label_fn=lambda value: str(value or "").strip().lower(),
            brand_family_catalog_evidence_fn=lambda store_hash, brand_label, family_tokens: {"matching_product_count": 0, "matching_product_urls": []},
            semantic_head_term_from_phrases_fn=lambda matches: "",
            semantic_token_roles_fn=lambda query, head_term, resolved_signals, taxonomy_alias_matches, ambiguous_modifier_matches: [{"text": "curtain", "role": "head_product"}],
            generic_brand_alias_tokens=set(),
            semantic_allowed_page_types={"brand", "category", "content", "product"},
            semantic_accessory_block_rules={},
            semantic_subtype_constraints={},
            context_keep_tokens=set(),
            query_noise_words=set(),
            generic_routing_tokens=set(),
        )

        self.assertEqual(analysis["query_shape"], "category_like")
        self.assertEqual(analysis["eligible_page_types"], ["category"])
        self.assertTrue(
            any(rule["kind"] == "require_head_term_presence" for rule in analysis["constraint_rules"])
        )

    def test_classify_query_intent_scope_uses_resolved_signals_when_present(self):
        result = query_signal_analysis.classify_query_intent_scope(
            "hookless curtain",
            {"hookless", "curtain"},
            {},
            resolved_signals={"brand_signals": ["x"]},
            classify_query_intent_from_signals_fn=lambda example_query, resolved_signals: ("brand_navigation", "brand"),
            looks_informational_query_fn=lambda value: False,
            has_model_or_sku_signal_fn=lambda value: False,
            topic_priority={"curtain"},
            topic_display_map={},
        )

        self.assertEqual(result, ("brand_navigation", "brand"))

    def test_semantic_head_term_prefers_mattress_for_rollaway_component_query(self):
        head_term = query_signal_analysis.semantic_head_term(
            "rollaway bed mattress only",
            {"rollaway", "bed", "mattress", "only"},
            [
                {
                    "normalized_label": "rollaway bed",
                    "metadata": {"head_term": "bed", "head_family": "rollaway beds"},
                }
            ],
            {"topic_signals": [{"matched_tokens": ["rollaway"]}]},
            semantic_head_term_from_phrases_fn=lambda matches: query_signal_analysis.semantic_head_term_from_phrases(
                matches,
                normalize_signal_label_fn=lambda value: str(value or "").strip().lower(),
            ),
            ordered_intent_tokens_fn=lambda value: ["rollaway", "bed", "mattress", "only"],
            canonical_word_token_fn=lambda value: str(value or "").strip().lower(),
            topic_priority={"rollaway"},
            query_noise_words=set(),
            generic_routing_tokens=set(),
            context_keep_tokens=set(),
        )

        self.assertEqual(head_term, "mattress")

    def test_classify_query_intent_from_signals_prefers_product_for_rollaway_component_query(self):
        result = query_signal_analysis.classify_query_intent_from_signals(
            "rollaway bed mattress only",
            {
                "query_tokens": ["rollaway", "bed", "mattress", "only"],
                "brand_signals": [],
                "hard_attribute_signals": [],
                "soft_attribute_signals": [],
                "collection_signals": [],
                "topic_signals": [
                    {
                        "matched_tokens": ["rollaway"],
                        "entity_type": "category",
                        "normalized_label": "rollaway portable foldable beds for hotels",
                    }
                ],
                "sku_signals": [],
            },
            ordered_intent_tokens_fn=lambda value: ["rollaway", "bed", "mattress", "only"],
            expand_signal_tokens_fn=lambda tokens: set(tokens),
            tokenize_intent_text_fn=_tokenize,
            non_generic_signal_tokens_fn=lambda tokens: set(tokens),
            looks_informational_query_fn=lambda value: False,
            generic_brand_alias_tokens=set(),
        )

        self.assertEqual(result, ("specific_product", "product"))

    def test_build_query_semantics_analysis_keeps_rollaway_component_query_product_led(self):
        analysis = query_signal_analysis.build_query_semantics_analysis(
            "pdwzti0dpv",
            "rollaway bed mattress only",
            {
                "query_tokens": ["rollaway", "bed", "mattress", "only"],
                "brand_signals": [],
                "hard_attribute_signals": [],
                "soft_attribute_signals": [],
                "collection_signals": [],
                "topic_signals": [
                    {
                        "matched_tokens": ["rollaway"],
                        "entity_type": "category",
                        "normalized_label": "rollaway portable foldable beds for hotels",
                    }
                ],
                "sku_signals": [],
            },
            signal_library={
                "protected_phrase": [
                    {
                        "normalized_label": "rollaway bed",
                        "metadata": {"head_term": "bed", "head_family": "rollaway beds"},
                    }
                ],
                "taxonomy_alias": [],
                "ambiguous_modifier": [],
            },
            build_store_signal_library_fn=lambda store_hash: {},
            ordered_intent_tokens_fn=lambda value: ["rollaway", "bed", "mattress", "only"],
            expand_signal_tokens_fn=lambda tokens: set(tokens),
            tokenize_intent_text_fn=_tokenize,
            non_generic_signal_tokens_fn=lambda tokens: set(tokens),
            match_semantic_signal_entries_fn=lambda query, query_tokens, entries: list(entries),
            semantic_head_term_fn=lambda query, query_tokens, bound_phrase_matches, resolved_signals: query_signal_analysis.semantic_head_term(
                query,
                query_tokens,
                bound_phrase_matches,
                resolved_signals,
                semantic_head_term_from_phrases_fn=lambda matches: query_signal_analysis.semantic_head_term_from_phrases(
                    matches,
                    normalize_signal_label_fn=lambda value: str(value or "").strip().lower(),
                ),
                ordered_intent_tokens_fn=lambda value: ["rollaway", "bed", "mattress", "only"],
                canonical_word_token_fn=lambda value: str(value or "").strip().lower(),
                topic_priority={"rollaway"},
                query_noise_words=set(),
                generic_routing_tokens=set(),
                context_keep_tokens=set(),
            ),
            semantic_head_family_fn=lambda head_term, query_tokens, bound_phrase_matches, taxonomy_alias_matches: query_signal_analysis.semantic_head_family(
                head_term,
                query_tokens,
                bound_phrase_matches,
                taxonomy_alias_matches,
                normalize_signal_label_fn=lambda value: str(value or "").strip().lower(),
                semantic_pluralize_fn=lambda value: str(value or "").strip().lower() + "s",
            ),
            query_has_exact_brand_phrase_fn=lambda query, brand_signals: 0.0,
            query_is_broad_descriptive_fn=lambda query, query_tokens, resolved_signals: False,
            semantic_family_candidate_tokens_fn=lambda head_term, head_family, taxonomy_alias_matches: {"rollaway", "bed"},
            normalize_signal_label_fn=lambda value: str(value or "").strip().lower(),
            brand_family_catalog_evidence_fn=lambda store_hash, brand_label, family_tokens: {"matching_product_count": 0, "matching_product_urls": []},
            semantic_head_term_from_phrases_fn=lambda matches: query_signal_analysis.semantic_head_term_from_phrases(
                matches,
                normalize_signal_label_fn=lambda value: str(value or "").strip().lower(),
            ),
            semantic_token_roles_fn=lambda query, head_term, resolved_signals, taxonomy_alias_matches, ambiguous_modifier_matches: [
                {"text": head_term, "role": "head_product"}
            ],
            generic_brand_alias_tokens=set(),
            semantic_allowed_page_types={"brand", "category", "content", "product"},
            semantic_accessory_block_rules={},
            semantic_subtype_constraints={},
            context_keep_tokens=set(),
            query_noise_words=set(),
            generic_routing_tokens=set(),
        )

        self.assertEqual(analysis["head_term"], "mattress")
        self.assertEqual(analysis["query_shape"], "exact_product_like")
        self.assertEqual(analysis["eligible_page_types"], ["product"])

    def test_product_like_brand_collection_query_does_not_add_brand_page_guard(self):
        analysis = query_signal_analysis.build_query_semantics_analysis(
            "99oa2tso",
            "kartri courtyard curtain",
            {
                "query_tokens": ["kartri", "courtyard", "curtain"],
                "brand_signals": [{"matched_tokens": ["kartri"], "normalized_label": "kartri"}],
                "hard_attribute_signals": [],
                "soft_attribute_signals": [],
                "collection_signals": [{"matched_tokens": ["courtyard"], "normalized_label": "courtyard"}],
                "topic_signals": [{"matched_tokens": ["curtain"], "normalized_label": "curtain"}],
                "sku_signals": [],
            },
            signal_library={"protected_phrase": [], "taxonomy_alias": [], "ambiguous_modifier": []},
            build_store_signal_library_fn=lambda store_hash: {},
            ordered_intent_tokens_fn=lambda value: ["kartri", "courtyard", "curtain"],
            expand_signal_tokens_fn=lambda tokens: set(tokens),
            tokenize_intent_text_fn=_tokenize,
            non_generic_signal_tokens_fn=lambda tokens: set(tokens),
            match_semantic_signal_entries_fn=lambda query, query_tokens, entries: [],
            semantic_head_term_fn=lambda query, query_tokens, bound_phrase_matches, resolved_signals: "curtain",
            semantic_head_family_fn=lambda head_term, query_tokens, bound_phrase_matches, taxonomy_alias_matches: "curtains",
            query_has_exact_brand_phrase_fn=lambda query, brand_signals: 0.0,
            query_is_broad_descriptive_fn=lambda query, query_tokens, resolved_signals: False,
            semantic_family_candidate_tokens_fn=lambda head_term, head_family, taxonomy_alias_matches: {"curtain"},
            normalize_signal_label_fn=lambda value: str(value or "").strip().lower(),
            brand_family_catalog_evidence_fn=lambda store_hash, brand_label, family_tokens: {
                "matching_product_count": 4,
                "matching_product_urls": ["/kartri-courtyard-curtain/"],
            },
            semantic_head_term_from_phrases_fn=lambda matches: "",
            semantic_token_roles_fn=lambda query, head_term, resolved_signals, taxonomy_alias_matches, ambiguous_modifier_matches: [
                {"text": "kartri", "role": "brand_candidate"},
                {"text": "courtyard", "role": "collection"},
                {"text": "curtain", "role": "head_product"},
            ],
            generic_brand_alias_tokens=set(),
            semantic_allowed_page_types={"brand", "category", "content", "product"},
            semantic_accessory_block_rules={},
            semantic_subtype_constraints={},
            context_keep_tokens=set(),
            query_noise_words=set(),
            generic_routing_tokens=set(),
        )

        self.assertEqual(analysis["query_shape"], "exact_product_like")
        self.assertNotIn(
            "prefer_brand_when_family_has_multiple_products",
            {rule["kind"] for rule in analysis["constraint_rules"]},
        )


if __name__ == "__main__":
    unittest.main()
