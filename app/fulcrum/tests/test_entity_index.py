import sys
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

APP_ROOT = ROOT / "MDMH4H"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from app.fulcrum import entity_index


class _FakeCursor:
    def __init__(self, rows):
        self.rows = list(rows)
        self.executions = []

    def execute(self, sql, params=None):
        self.executions.append((sql, params))

    def fetchall(self):
        return list(self.rows)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeConnection:
    def __init__(self, rows):
        self.cursor_instance = _FakeCursor(rows)

    def cursor(self, *args, **kwargs):
        return self.cursor_instance

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FulcrumEntityIndexTests(unittest.TestCase):
    def test_profile_matches_cluster_checks_primary_and_cluster_list(self):
        self.assertTrue(
            entity_index.profile_matches_cluster(
                {"cluster_profile": {"primary": "towels", "clusters": ["bath", "towels"]}},
                "towels",
            )
        )
        self.assertTrue(
            entity_index.profile_matches_cluster(
                {"cluster_profile": {"primary": "linens", "clusters": ["bath", "towels"]}},
                "towels",
            )
        )
        self.assertFalse(entity_index.profile_matches_cluster({"cluster_profile": {"primary": "linens"}}, "towels"))

    def test_load_all_store_product_profiles_builds_tokens_and_filters_cluster(self):
        fake_conn = _FakeConnection(
            [
                {
                    "bc_product_id": 11,
                    "product_name": "Bath Towel",
                    "product_url": "/bath-towel/",
                    "brand_name": "Acme",
                    "search_keywords": "hotel towel",
                    "source_data": {"option_pairs": [{"name": "Color", "value": "White"}]},
                    "attribute_profile": {"color": ["white"]},
                    "cluster_profile": {"primary": "towels", "clusters": ["bath"]},
                    "canonical_group_key": "bath-towel",
                    "is_canonical_target": True,
                    "is_visible": True,
                    "availability": "available",
                    "is_price_hidden": False,
                    "eligible_for_routing": True,
                }
            ]
        )

        with patch("app.fulcrum.entity_index.get_pg_conn", return_value=fake_conn):
            rows = entity_index.load_all_store_product_profiles(
                "stores/99OA2TSO",
                cluster="towels",
                profile_matches_cluster_fn=entity_index.profile_matches_cluster,
                tokenize_intent_text_fn=lambda value: set(str(value or "").lower().replace("/", " ").split()),
                extract_attribute_terms_fn=lambda value: {"fallback": {"x"}},
            )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["bc_product_id"], 11)
        self.assertIn("bath", rows[0]["tokens"])
        self.assertEqual(rows[0]["attributes"]["color"], {"white"})
        self.assertEqual(fake_conn.cursor_instance.executions[0][1], ("99oa2tso",))

    def test_build_unified_entity_index_assembles_sources_targets_and_ga4(self):
        result = entity_index.build_unified_entity_index(
            "99oa2tso",
            cluster="towels",
            load_all_store_product_profiles_fn=lambda store_hash, cluster=None: [
                {
                    "bc_product_id": 11,
                    "url": "/bath-towel/",
                    "name": "Bath Towel",
                    "eligible_for_routing": True,
                    "is_canonical_target": True,
                }
            ],
            load_store_category_profiles_fn=lambda store_hash, canonical_only=False: {
                "/towels/": {
                    "bc_category_id": 22,
                    "url": "/towels/",
                    "name": "Towels",
                    "eligible_for_routing": True,
                    "cluster_profile": {"primary": "towels"},
                }
            },
            load_store_brand_profiles_fn=lambda store_hash: {
                "/acme/": {
                    "bc_entity_id": 33,
                    "entity_type": "brand",
                    "url": "/acme/",
                    "name": "Acme",
                    "eligible_for_routing": True,
                    "cluster_profile": {"clusters": ["towels"]},
                }
            },
            load_store_content_profiles_fn=lambda store_hash, include_backlog=False: {
                "/guides/towels/": {
                    "bc_entity_id": 44,
                    "entity_type": "content",
                    "url": "/guides/towels/",
                    "name": "Towel Guide",
                    "eligible_for_routing": True,
                    "cluster_profile": {"clusters": ["towels"]},
                }
            },
            normalize_storefront_path_fn=lambda value: str(value or "").strip().lower(),
            profile_matches_cluster_fn=entity_index.profile_matches_cluster,
            load_ga4_page_metrics_fn=lambda urls, days: {
                "/bath-towel/": {"views": 10},
                "/towels/": {"views": 5},
                "/acme/": {"views": 3},
            },
        )

        self.assertIn("/bath-towel/", result["sources"])
        self.assertIn("/towels/", result["sources"])
        self.assertIn("/acme/", result["sources"])
        self.assertEqual(result["sources"]["/bath-towel/"]["ga4_metrics"], {"views": 10})
        self.assertEqual(result["sources"]["/towels/"]["entity_type"], "category")
        self.assertEqual(result["targets"][0]["entity_type"], "product")
        self.assertTrue(any(row.get("entity_type") == "brand" for row in result["targets"]))


if __name__ == "__main__":
    unittest.main()
