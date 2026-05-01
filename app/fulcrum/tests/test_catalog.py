import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch


ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

APP_ROOT = ROOT / "MDMH4H"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from app.fulcrum.catalog import sync_store_catalog_profiles


class _FakeCursor:
    def __init__(self):
        self.executions = []

    def execute(self, sql, params=None):
        self.executions.append((sql, params))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeConnection:
    def __init__(self):
        self.cursor_instance = _FakeCursor()
        self.committed = False

    def cursor(self, *args, **kwargs):
        return self.cursor_instance

    def commit(self):
        self.committed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FulcrumCatalogTests(unittest.TestCase):
    def test_sync_store_catalog_profiles_returns_expected_summary(self):
        fake_conn = _FakeConnection()
        execute_batch_mock = Mock()
        seed_buckets = Mock()
        seed_rules = Mock()
        storefront_sync = Mock(return_value={"synced_site_count": 1})
        refresh_signals = Mock(return_value={"status": "ok"})
        normalize_mapping_statuses = Mock(return_value={"pending_name_count": 2, "pending_value_count": 1})
        refresh_readiness = Mock(return_value={"catalog_synced": True})

        with (
            patch(
                "app.fulcrum.catalog._list_bc_paginated",
                return_value=[
                    {
                        "id": 1,
                        "name": "Bath Towel",
                        "custom_url": {"url": "/bath-towel/"},
                        "search_keywords": "hotel bath towel",
                        "brand_id": 7,
                        "categories": [2],
                        "is_visible": True,
                        "availability": "available",
                        "is_price_hidden": False,
                    }
                ],
            ),
            patch(
                "app.fulcrum.catalog.list_store_categories",
                return_value=[
                    {
                        "id": 2,
                        "name": "Towels",
                        "custom_url": {"url": "/towels/"},
                        "page_title": "Hotel Towels",
                        "description": "<p>Soft towels</p>",
                        "meta_keywords": ["towels"],
                        "parent_id": 0,
                        "is_visible": True,
                    }
                ],
            ),
            patch("app.fulcrum.catalog.fetch_store_brand_map", return_value={7: "Acme"}),
            patch(
                "app.fulcrum.catalog.fetch_store_product_options",
                return_value=[{"display_name": "Color", "option_values": [{"label": "White"}]}],
            ),
            patch("app.fulcrum.catalog.get_pg_conn", return_value=fake_conn),
            patch("app.fulcrum.catalog.execute_batch", execute_batch_mock),
        ):
            result = sync_store_catalog_profiles(
                "stores/99OA2TSO",
                initiated_by="test-suite",
                seed_store_attribute_buckets=seed_buckets,
                seed_store_cluster_rules=seed_rules,
                sync_store_storefront_sites=storefront_sync,
                normalize_storefront_path=lambda value: str(value or "").strip().lower(),
                pick_canonical_product_ids=lambda products: {1},
                pick_canonical_category_ids=lambda categories: {2},
                infer_bucket_from_option_name=lambda raw_name, raw_values=None: ("color", 0.93),
                canonicalize_attribute_value=lambda bucket_key, raw_value: str(raw_value or "").strip().lower(),
                extract_attribute_terms=lambda raw_text: {},
                slugify_value=lambda raw_value: str(raw_value or "").strip().lower().replace(" ", "-"),
                build_cluster_profile=lambda **kwargs: {"clusters": ["towels"]},
                canonical_product_group_key=lambda product, known_urls: "bath-towel",
                product_eligible_for_routing=lambda product: True,
                canonical_category_group_key=lambda category: "towels",
                category_eligible_for_routing=lambda category: True,
                serialize_attribute_profile=lambda profile: {key: sorted(values) for key, values in profile.items()},
                refresh_store_intent_signal_enrichments=refresh_signals,
                normalize_mapping_review_statuses=normalize_mapping_statuses,
                refresh_store_readiness=refresh_readiness,
            )

        self.assertEqual(result["store_hash"], "99oa2tso")
        self.assertEqual(result["synced_products"], 1)
        self.assertEqual(result["synced_categories"], 1)
        self.assertEqual(result["mapped_option_names"], 1)
        self.assertEqual(result["mapped_option_values"], 1)
        self.assertEqual(result["towel_profiles"], 1)
        self.assertEqual(result["pending_option_name_mappings"], 2)
        self.assertEqual(result["pending_option_value_mappings"], 1)
        self.assertEqual(result["intent_signal_refresh"], {"status": "ok"})
        self.assertEqual(result["readiness"], {"catalog_synced": True})
        self.assertTrue(fake_conn.committed)
        self.assertEqual(execute_batch_mock.call_count, 4)
        seed_buckets.assert_called_once_with("99oa2tso")
        seed_rules.assert_called_once_with("99oa2tso")
        storefront_sync.assert_called_once_with("99oa2tso", initiated_by="test-suite")
        refresh_signals.assert_called_once_with("99oa2tso", initiated_by="test-suite")
        normalize_mapping_statuses.assert_called_once_with("99oa2tso")
        refresh_readiness.assert_called_once_with("99oa2tso")


if __name__ == "__main__":
    unittest.main()
