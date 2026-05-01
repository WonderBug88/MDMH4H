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

from app.fulcrum import intent_signals


def _tokenize(value):
    return {token for token in str(value or "").lower().replace("-", " ").split() if token}


class FulcrumIntentSignalTests(unittest.TestCase):
    def test_valid_brand_alias_token_rejects_generic_terms(self):
        self.assertTrue(
            intent_signals.valid_brand_alias_token(
                "martex",
                generic_brand_alias_tokens={"hotel"},
                topic_priority={"towel"},
                material_tokens={"cotton"},
                form_tokens={"sheet"},
                generic_routing_tokens={"buy"},
                query_noise_words={"the"},
            )
        )
        self.assertFalse(
            intent_signals.valid_brand_alias_token(
                "hotel",
                generic_brand_alias_tokens={"hotel"},
                topic_priority={"towel"},
                material_tokens={"cotton"},
                form_tokens={"sheet"},
                generic_routing_tokens={"buy"},
                query_noise_words={"the"},
            )
        )

    def test_refresh_store_intent_signal_enrichments_builds_summary(self):
        replace_mock = Mock(return_value=4)

        result = intent_signals.refresh_store_intent_signal_enrichments(
            "stores/abc123",
            initiated_by="test-suite",
            normalize_store_hash_fn=lambda value: "abc123",
            load_all_store_product_profiles_fn=lambda store_hash: [
                {
                    "bc_product_id": 10,
                    "name": "Martex Towels",
                    "brand_name": "Martex",
                    "source_data": {"option_pairs": [{"name": "Color", "value": "White"}]},
                    "attributes": {"form": {"towel"}},
                }
            ],
            load_store_category_profiles_fn=lambda store_hash: {
                "/towels/": {"name": "Hotel Towels", "bc_category_id": 3}
            },
            load_store_brand_profiles_fn=lambda store_hash: {
                "/brands/martex/": {"name": "Martex", "bc_entity_id": 8}
            },
            normalize_signal_label_fn=lambda value: str(value or "").strip().lower(),
            intent_signal_row_fn=lambda **kwargs: kwargs,
            tokenize_intent_text_fn=_tokenize,
            non_generic_signal_tokens_fn=lambda tokens: {token for token in tokens if token != "hotel"},
            valid_brand_alias_token_fn=lambda token: token == "martex",
            infer_bucket_from_option_name_fn=lambda raw_name, raw_values=None: ("color", 0.9),
            signal_kind_from_bucket_fn=lambda bucket: "soft_attribute" if bucket == "color" else None,
            canonicalize_attribute_value_fn=lambda bucket, value: str(value or "").strip().lower(),
            derive_collection_seed_from_product_fn=lambda profile, category_topic_tokens: "",
            load_store_variant_sku_rows_fn=lambda product_ids: [],
            semantic_builtin_enrichment_rows_fn=lambda store_hash: [],
            label_ambiguous_intent_signals_with_agent_fn=lambda store_hash, items: [],
            replace_store_intent_signal_enrichments_fn=replace_mock,
            dedupe_intent_signal_rows_fn=lambda rows: rows,
            intent_signal_collection_min_repeat=2,
        )

        self.assertEqual(result["store_hash"], "abc123")
        self.assertEqual(result["stored_signals"], 4)
        self.assertEqual(result["agent_signal_count"], 0)
        self.assertEqual(result["ambiguous_signal_count"], 0)
        replace_mock.assert_called_once()


if __name__ == "__main__":
    unittest.main()
