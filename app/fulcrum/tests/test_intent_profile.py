import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

APP_ROOT = ROOT / "MDMH4H"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from app.fulcrum import intent_profile


def _tokenize(value):
    return {token for token in str(value or "").lower().replace("-", " ").split() if token}


class FulcrumIntentProfileTests(unittest.TestCase):
    def test_build_intent_profile_scores_strong_target_match(self):
        result = intent_profile.build_intent_profile(
            "Hookless Shower Curtains",
            "/hookless-curtains/",
            "Hookless White Curtain",
            "/hookless-white-curtain/",
            "hookless white curtain",
            "product",
            3,
            source_profile={
                "brand_name": "Hookless",
                "tokens": {"hookless", "shower", "curtains"},
                "attributes": {"form": {"hookless"}},
                "cluster_profile": {"primary": "shower-curtains"},
                "entity_type": "category",
                "bc_entity_id": 10,
            },
            target_profile={
                "brand_name": "Hookless",
                "tokens": {"hookless", "white", "curtain"},
                "attributes": {"form": {"hookless"}, "color": {"white"}},
                "cluster_profile": {"primary": "shower-curtains"},
                "entity_type": "product",
                "bc_entity_id": 22,
            },
            query_signal_context={
                "query_attrs": {"form": ["hookless"], "color": ["white"]},
                "brand_signals": [{"matched_tokens": ["hookless"], "normalized_label": "hookless"}],
                "hard_attribute_signals": [],
                "soft_attribute_signals": [{"matched_tokens": ["white"], "normalized_label": "white"}],
                "collection_signals": [],
                "topic_signals": [{"matched_tokens": ["curtain"], "normalized_label": "curtain"}],
                "sku_signals": [],
            },
            tokenize_intent_text_fn=_tokenize,
            extract_attribute_terms_fn=lambda value: {},
            resolve_query_signal_context_fn=lambda **kwargs: {},
            build_fuzzy_signal_fn=lambda **kwargs: {"active": True, "score": 86.0, "matched_kind": "title"},
            classify_query_intent_scope_fn=lambda **kwargs: ("specific_product", "product"),
            select_anchor_label_fn=lambda **kwargs: {
                "label": "Hookless White Curtain",
                "label_source": "target_name",
                "quality": 84.0,
                "generic": False,
            },
            build_ga4_signal_fn=lambda **kwargs: {
                "active": True,
                "delta": 2.0,
                "reason": "GA4 shows this PDP already converts",
                "summary": "GA4 conversions support this PDP",
            },
            is_replacement_or_accessory_target_fn=lambda query_tokens, target_tokens, target_name: False,
            attribute_sets_to_list_fn=lambda attrs: {key: sorted(values) for key, values in attrs.items() if values},
            topic_priority={"curtain"},
            topic_display_map={},
            form_family_tokens=set(),
            generic_routing_tokens=set(),
            query_noise_words={"the", "and"},
            intent_stopwords={"for"},
            context_keep_tokens=set(),
            narrow_accessory_target_tokens={"replacement"},
            replacement_intent_tokens={"replacement"},
        )

        self.assertTrue(result["passes"])
        self.assertGreater(result["score"], 58)
        self.assertEqual(result["anchor_label"], "Hookless White Curtain")
        self.assertEqual(result["query_intent_scope"], "specific_product")
        self.assertEqual(result["preferred_entity_type"], "product")
        self.assertEqual(result["attributes"]["query"]["color"], ["white"])
        self.assertIn("shared query hits", result["reason_summary"])

    def test_build_intent_profile_promotes_product_when_fuzzy_match_is_strong(self):
        result = intent_profile.build_intent_profile(
            "Hotel Curtains",
            "/hotel-curtains/",
            "Hookless Curtain",
            "/hookless-curtain/",
            "hookless curtain",
            "product",
            1,
            target_profile={"entity_type": "product", "bc_entity_id": 22, "tokens": {"hookless", "curtain"}},
            query_signal_context={
                "query_attrs": {},
                "brand_signals": [],
                "hard_attribute_signals": [],
                "soft_attribute_signals": [],
                "collection_signals": [],
                "topic_signals": [],
                "sku_signals": [],
            },
            tokenize_intent_text_fn=_tokenize,
            extract_attribute_terms_fn=lambda value: {},
            resolve_query_signal_context_fn=lambda **kwargs: {},
            build_fuzzy_signal_fn=lambda **kwargs: {"active": True, "score": 82.0, "matched_kind": "title"},
            classify_query_intent_scope_fn=lambda **kwargs: ("commercial_topic", "category"),
            select_anchor_label_fn=lambda **kwargs: {
                "label": "Hookless Curtain",
                "label_source": "target_name",
                "quality": 70.0,
                "generic": False,
            },
            build_ga4_signal_fn=lambda **kwargs: {"active": False, "delta": 0.0, "reason": "", "summary": ""},
            is_replacement_or_accessory_target_fn=lambda query_tokens, target_tokens, target_name: False,
            attribute_sets_to_list_fn=lambda attrs: {key: sorted(values) for key, values in attrs.items() if values},
            topic_priority={"curtain"},
            topic_display_map={},
            form_family_tokens=set(),
            generic_routing_tokens=set(),
            query_noise_words=set(),
            intent_stopwords=set(),
            context_keep_tokens=set(),
            narrow_accessory_target_tokens={"replacement"},
            replacement_intent_tokens={"replacement"},
        )

        self.assertEqual(result["query_intent_scope"], "specific_product")
        self.assertEqual(result["preferred_entity_type"], "product")


if __name__ == "__main__":
    unittest.main()
