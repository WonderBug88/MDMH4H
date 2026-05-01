import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

APP_ROOT = ROOT / "MDMH4H"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from app.fulcrum import routing_text


class FulcrumRoutingTextTests(unittest.TestCase):
    def test_tokenize_intent_text_applies_replacements_aliases_and_stopwords(self):
        tokens = routing_text.tokenize_intent_text(
            "Roll Away Mattresses & All Towels",
            anchor_phrase_replacements=[("roll away", "rollaway")],
            context_token_aliases={"mattresse": "mattress"},
            intent_stopwords={"all", "and"},
            context_keep_tokens={"all"},
        )

        self.assertEqual(tokens, {"rollaway", "mattress", "all", "towel"})

    def test_ordered_intent_tokens_dedupes_and_keeps_order(self):
        tokens = routing_text.ordered_intent_tokens(
            "Hookless Curtains / Hookless All Towels",
            anchor_phrase_replacements=[],
            context_token_aliases={},
            intent_stopwords={"all"},
            context_keep_tokens=set(),
        )

        self.assertEqual(tokens, ["hookless", "curtain", "towel"])

    def test_normalize_signal_label_joins_ordered_tokens(self):
        normalized = routing_text.normalize_signal_label(
            "ignored",
            ordered_intent_tokens_fn=lambda raw_label: ["hookless", "curtain"],
        )

        self.assertEqual(normalized, "hookless curtain")

    def test_semantic_pluralize_handles_y_words_existing_plural_and_blank(self):
        self.assertEqual(routing_text.semantic_pluralize("battery"), "batteries")
        self.assertEqual(routing_text.semantic_pluralize("towels"), "towels")
        self.assertEqual(routing_text.semantic_pluralize("blanket"), "blankets")
        self.assertEqual(routing_text.semantic_pluralize(None), "")

    def test_profile_topic_label_prefers_form_mapping_and_towel_family(self):
        result = routing_text.profile_topic_label(
            {"attributes": {"form": {"bath-towel"}}, "name": "Luxury Bath Towel"},
            form_display_map={"sheet": "Sheets"},
            topic_display_map={"towel": "Towels"},
            normalize_anchor_text_fn=lambda value: str(value or "").title(),
            tokenize_intent_text_fn=lambda value: set(str(value or "").lower().split()),
        )

        self.assertEqual(result, "Towels")

    def test_profile_topic_label_falls_back_to_name_tokens(self):
        result = routing_text.profile_topic_label(
            {
                "name": "Premium Rollaway Bed",
                "option_labels": ["Hospitality"],
                "option_display_names": [],
                "attributes": {},
            },
            form_display_map={},
            topic_display_map={"bed": "Beds"},
            normalize_anchor_text_fn=lambda value: str(value or "").title(),
            tokenize_intent_text_fn=lambda value: set(str(value or "").lower().replace("-", " ").split()),
        )

        self.assertEqual(result, "Beds")

    def test_profile_brand_label_strips_hospitality_suffix(self):
        result = routing_text.profile_brand_label(
            {"brand_name": "Hookless Hospitality"},
            normalize_anchor_text_fn=lambda value: str(value or "").strip().title(),
        )

        self.assertEqual(result, "Hookless")

    def test_normalize_phrase_for_match_compacts_whitespace_and_lowercases(self):
        normalized = routing_text.normalize_phrase_for_match(
            "  Hookless   Shower   Curtains ",
            normalize_anchor_text_fn=lambda value: str(value or ""),
        )

        self.assertEqual(normalized, "hookless shower curtains")

    def test_fuzzy_match_score_uses_ratio_overlap_and_bonus(self):
        score = routing_text.fuzzy_match_score(
            "hookless curtain",
            "hookless shower curtain",
            normalize_phrase_for_match_fn=lambda value: str(value or "").strip().lower(),
            tokenize_intent_text_fn=lambda value: set(str(value or "").strip().lower().split()),
        )

        self.assertGreaterEqual(score, 80.0)

    def test_fuzzy_candidate_kind_maps_known_sources(self):
        self.assertEqual(routing_text.fuzzy_candidate_kind("target_name"), "title")
        self.assertEqual(routing_text.fuzzy_candidate_kind("profile_brand_collection"), "collection")
        self.assertEqual(routing_text.fuzzy_candidate_kind("target_url"), "category phrase")
        self.assertEqual(routing_text.fuzzy_candidate_kind("brand"), "brand")
        self.assertEqual(routing_text.fuzzy_candidate_kind("other"), "entity")

    def test_build_fuzzy_signal_selects_best_non_query_candidate(self):
        result = routing_text.build_fuzzy_signal(
            "hookless shower curtain",
            "Hotel Shower Curtain",
            "/hookless-shower-curtains/",
            target_profile={"brand_name": "Hookless Hospitality"},
            normalize_phrase_for_match_fn=lambda value: str(value or "").strip().lower(),
            profile_brand_label_fn=lambda profile: "Hookless",
            extract_label_candidates_fn=lambda target_name, target_url, example_query, target_profile: [
                ("Hookless Shower Curtains", "target_fragment"),
                ("Hotel Collection", "profile_collection"),
                ("hookless shower curtain", "query"),
            ],
            fuzzy_match_score_fn=lambda left, right: 94.0 if "hookless shower curtains" in str(right or "") else 60.0,
            normalize_anchor_text_fn=lambda value: str(value or "").title(),
            fuzzy_candidate_kind_fn=routing_text.fuzzy_candidate_kind,
        )

        self.assertTrue(result["active"])
        self.assertEqual(result["matched_text"], "Hookless Shower Curtains")
        self.assertEqual(result["matched_kind"], "title")
        self.assertEqual(result["matched_source"], "target_fragment")

    def test_normalize_query_family_key_prefers_sorted_tokens_without_noise(self):
        key = routing_text.normalize_query_family_key(
            "Best Hookless Curtain For Hotels",
            normalize_phrase_for_match_fn=lambda value: str(value or "").strip().lower(),
            tokenize_intent_text_fn=lambda value: {"best", "hookless", "curtain", "for", "hotel"},
            query_noise_words={"best", "for"},
        )

        self.assertEqual(key, "curtain hookless hotel")

    def test_normalize_query_family_key_falls_back_to_normalized_query(self):
        key = routing_text.normalize_query_family_key(
            "How To Choose",
            normalize_phrase_for_match_fn=lambda value: str(value or "").strip().lower(),
            tokenize_intent_text_fn=lambda value: {"how", "to"},
            query_noise_words={"how", "to"},
        )

        self.assertEqual(key, "how to choose")


if __name__ == "__main__":
    unittest.main()
