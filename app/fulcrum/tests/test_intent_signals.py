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


class _FakeSkuCursor:
    def __init__(self, conn):
        self.conn = conn
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.conn.executed.append((sql, params))

    def fetchone(self):
        return self.conn.fetchone

    def fetchall(self):
        return self.conn.fetchall


class _FakeSkuConn:
    def __init__(self, *, fetchone, fetchall=None):
        self.fetchone = fetchone
        self.fetchall = list(fetchall or [])
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self, cursor_factory=None):
        return _FakeSkuCursor(self)


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
        stored_rows = replace_mock.call_args.args[1]
        self.assertTrue(
            any(
                row.get("signal_kind") == "brand_alias"
                and row.get("raw_label") == "Martex"
                and row.get("scope_kind") == "product_brand_name"
                for row in stored_rows
            )
        )

    def test_refresh_store_intent_signal_enrichments_blocks_ambiguous_brand_tokens(self):
        replace_mock = Mock(return_value=1)

        intent_signals.refresh_store_intent_signal_enrichments(
            "stores/abc123",
            initiated_by="test-suite",
            normalize_store_hash_fn=lambda value: "abc123",
            load_all_store_product_profiles_fn=lambda store_hash: [
                {
                    "bc_product_id": 10,
                    "name": "Suite Touch Pillow",
                    "brand_name": "Suite Touch by 1888 Mills",
                    "source_data": {"option_pairs": []},
                    "attributes": {},
                }
            ],
            load_store_category_profiles_fn=lambda store_hash: {},
            load_store_brand_profiles_fn=lambda store_hash: {
                "/suite-touch/": {"name": "Suite Touch by 1888 Mills", "bc_entity_id": 8}
            },
            normalize_signal_label_fn=lambda value: str(value or "").strip().lower(),
            intent_signal_row_fn=lambda **kwargs: kwargs,
            tokenize_intent_text_fn=_tokenize,
            non_generic_signal_tokens_fn=lambda tokens: set(tokens),
            valid_brand_alias_token_fn=lambda token: True,
            infer_bucket_from_option_name_fn=lambda raw_name, raw_values=None: (None, 0.0),
            signal_kind_from_bucket_fn=lambda bucket: None,
            canonicalize_attribute_value_fn=lambda bucket, value: str(value or "").strip().lower(),
            derive_collection_seed_from_product_fn=lambda profile, category_topic_tokens: "",
            load_store_variant_sku_rows_fn=lambda product_ids: [],
            semantic_builtin_enrichment_rows_fn=lambda store_hash: [
                {
                    "signal_kind": "ambiguous_modifier",
                    "raw_label": "suite",
                    "normalized_label": "suite",
                    "scope_kind": "token",
                }
            ],
            label_ambiguous_intent_signals_with_agent_fn=lambda store_hash, items: [],
            replace_store_intent_signal_enrichments_fn=replace_mock,
            dedupe_intent_signal_rows_fn=lambda rows: rows,
            intent_signal_collection_min_repeat=2,
        )

        stored_rows = replace_mock.call_args.args[1]
        suite_token_rows = [
            row
            for row in stored_rows
            if row.get("signal_kind") == "brand_alias"
            and row.get("normalized_label") == "suite"
            and row.get("scope_kind") in {"brand_name", "product_brand_name"}
        ]
        self.assertEqual(suite_token_rows, [])
        self.assertTrue(
            any(
                row.get("signal_kind") == "brand_alias"
                and row.get("normalized_label") == "suite touch by 1888 mills"
                for row in stored_rows
            )
        )

    def test_load_store_variant_sku_rows_tolerates_missing_legacy_tables(self):
        conn = _FakeSkuConn(
            fetchone={"has_product_sku_mapping": False, "has_variants": False},
        )

        rows = intent_signals.load_store_variant_sku_rows(
            [10, 11],
            get_pg_conn_fn=lambda: conn,
        )

        self.assertEqual(rows, [])
        self.assertEqual(len(conn.executed), 1)

    def test_load_store_variant_sku_rows_queries_existing_legacy_tables(self):
        conn = _FakeSkuConn(
            fetchone={"has_product_sku_mapping": True, "has_variants": False},
            fetchall=[{"product_id": 10, "sku": "ABC", "scope_kind": "product"}],
        )

        rows = intent_signals.load_store_variant_sku_rows(
            [10],
            get_pg_conn_fn=lambda: conn,
        )

        self.assertEqual(rows, [{"product_id": 10, "sku": "ABC", "scope_kind": "product"}])
        self.assertEqual(len(conn.executed), 2)
        self.assertIn("product_sku_mapping", conn.executed[1][0])
        self.assertNotIn("h4h_import2.variants", conn.executed[1][0])


if __name__ == "__main__":
    unittest.main()
