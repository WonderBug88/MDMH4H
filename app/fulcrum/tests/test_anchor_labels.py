import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

APP_ROOT = ROOT / "MDMH4H"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from app.fulcrum import anchor_labels


def _tokenize(value):
    return {token for token in str(value or "").lower().replace("-", " ").split() if token}


class FulcrumAnchorLabelTests(unittest.TestCase):
    def test_normalize_anchor_text_reorders_size_suffix(self):
        result = anchor_labels.normalize_anchor_text(
            "Rollaway Bed Queen-Size",
            title_case_anchor_fn=lambda text: anchor_labels.title_case_anchor(text, anchor_small_words={"and", "of"}),
            anchor_phrase_replacements=[],
        )
        self.assertEqual(result, "Queen-Size Rollaway Bed")

    def test_select_anchor_label_prefers_specific_target_fragment(self):
        result = anchor_labels.select_anchor_label(
            "product",
            "white towels",
            "/white-towels/",
            target_name="Luxury White Towels",
            target_profile={"tokens": {"luxury", "white", "towels"}, "attributes": {"color": {"white"}}},
            legacy_fallback_anchor_label_fn=lambda relation_type, example_query, target_url: "Fallback",
            label_from_target_url_fn=lambda target_url: "White Towels",
            looks_generic_phrase_fn=lambda text: False,
            tokenize_intent_text_fn=_tokenize,
            extract_attribute_terms_fn=lambda text: {"color": {"white"}} if "white" in str(text or "").lower() else {},
            profile_topic_label_fn=lambda profile: "Towels",
            profile_brand_label_fn=lambda profile: "",
            extract_label_candidates_fn=lambda target_name, target_url, example_query, target_profile=None: [
                ("White Towels", "target_fragment"),
                ("Towels", "query"),
            ],
            query_noise_words={"the"},
            size_tokens={"queen-size"},
            topic_priority={"towels"},
            form_family_tokens=set(),
            generic_routing_tokens=set(),
            topic_display_map={},
        )

        self.assertEqual(result["label"], "White Towels")
        self.assertEqual(result["label_source"], "target_fragment")


if __name__ == "__main__":
    unittest.main()
