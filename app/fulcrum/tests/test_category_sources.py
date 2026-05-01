import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

APP_ROOT = ROOT / "MDMH4H"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from app.fulcrum import category_sources


class FulcrumCategorySourcesTests(unittest.TestCase):
    def test_load_canonical_cluster_categories_prefers_hinted_urls(self):
        rows = category_sources.load_canonical_cluster_categories(
            "99oa2tso",
            "towels",
            load_store_category_profiles_fn=lambda store_hash, canonical_only=True: {
                "/bath/": {"url": "/collections/bath-towels/", "name": "Bath Towels", "is_canonical_target": True, "cluster_profile": {"primary": "towels"}},
                "/plain/": {"url": "/collections/towels/", "name": "Towels", "is_canonical_target": True, "cluster_profile": {"primary": "towels"}},
            },
            profile_matches_cluster_fn=lambda profile, cluster: True,
            category_competition_url_hints={"towels": ("bath-towels",)},
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["name"], "Bath Towels")

    def test_category_anchor_label_and_specificity_bonus_follow_cluster_hints(self):
        label = category_sources.category_anchor_label_for_cluster(
            "beds",
            {"name": "Rollaway Beds", "url": "/collections/rollaway-beds/"},
            category_competition_specific_hints={
                "beds": (
                    {"fragment": "rollaway-beds", "tokens": {"rollaway"}},
                    {"fragment": "beds"},
                )
            },
            category_cluster_labels={"beds": "Shop Beds"},
            normalize_anchor_text_fn=lambda value: str(value or "").strip(),
            label_from_target_url_fn=lambda value: "From URL",
        )
        bonus, fragment = category_sources.category_competition_specificity_bonus(
            "beds",
            {"example_query": "rollaway bed"},
            {"tokens": {"rollaway"}, "cluster_profile": {"subclusters": ["rollaway"]}},
            {"url": "/collections/rollaway-beds/"},
            category_competition_specific_hints={
                "beds": (
                    {"fragment": "rollaway-beds", "tokens": {"rollaway"}, "subclusters": {"rollaway"}},
                    {"fragment": "beds"},
                )
            },
            tokenize_intent_text_fn=lambda value: set(str(value or "").lower().split()),
        )

        self.assertEqual(label, "Rollaway Beds")
        self.assertEqual((bonus, fragment), (18.0, "rollaway-beds"))

    def test_build_pdp_category_competition_candidate_builds_metadata(self):
        row = category_sources.build_pdp_category_competition_candidate(
            "99oa2tso",
            "towels",
            {
                "source_product_id": 11,
                "source_name": "Bath Towel",
                "source_url": "/bath-towel/",
                "example_query": "hotel bath towels",
                "hit_count": 8,
            },
            {"tokens": {"bath", "towel"}},
            {
                "score": 70.0,
                "topic_key": "bath towel",
                "source_primary_cluster": "towels",
                "query_intent_scope": "broad_product_family",
                "preferred_entity_type": "category",
            },
            {
                "bc_category_id": 22,
                "name": "Bath Towels",
                "url": "/collections/bath-towels/",
                "cluster_profile": {"primary": "towels"},
            },
            build_intent_profile_fn=lambda **kwargs: {
                "passes": True,
                "score": 74.0,
                "reason_summary": "Category matches the family",
                "anchor_quality": 88.0,
                "reasons": ["topic overlap"],
                "shared_tokens": ["bath", "towel"],
                "query_target_tokens": ["bath", "towel"],
                "query_source_tokens": ["bath", "towel"],
                "attributes": {},
            },
            entity_storage_id_fn=lambda entity_type, entity_id: int(entity_id or 0) + (1000 if entity_type == "category" else 0),
            category_competition_specificity_bonus_fn=lambda **kwargs: (10.0, "bath-towels"),
            category_anchor_label_for_cluster_fn=lambda cluster, category_profile: "Bath Towels",
        )

        self.assertEqual(row["score"], 86.0)
        self.assertEqual(row["target_product_id"], 1022)
        self.assertIn("canonical subcategory aligns with the query", row["metadata"]["reason_summary"])

    def test_build_category_descendants_and_shared_subclusters(self):
        descendants = category_sources.build_category_descendants(
            [
                {"bc_category_id": 1, "parent_category_id": 0},
                {"bc_category_id": 2, "parent_category_id": 1},
                {"bc_category_id": 3, "parent_category_id": 2},
            ]
        )
        shared = category_sources.shared_subclusters(
            {"cluster_profile": {"subclusters": ["bath", "luxury"]}},
            {"cluster_profile": {"subclusters": ["luxury", "spa"]}},
        )

        self.assertEqual(descendants[1], {1, 2, 3})
        self.assertEqual(shared, {"luxury"})

    def test_generate_category_source_candidates_emits_related_category_and_product(self):
        rows = category_sources.generate_category_source_candidates(
            "99oa2tso",
            cluster="towels",
            load_store_category_profiles_fn=lambda store_hash, canonical_only=True: {
                "/towels/": {
                    "bc_category_id": 1,
                    "parent_category_id": 0,
                    "url": "/towels/",
                    "name": "Towels",
                    "cluster_profile": {"primary": "towels", "subclusters": ["bath"]},
                },
                "/bath-towels/": {
                    "bc_category_id": 2,
                    "parent_category_id": 1,
                    "url": "/bath-towels/",
                    "name": "Bath Towels",
                    "cluster_profile": {"primary": "towels", "subclusters": ["bath"]},
                },
            },
            load_all_store_product_profiles_fn=lambda store_hash, cluster=None: [
                {
                    "bc_product_id": 11,
                    "url": "/bath-towel-product/",
                    "name": "Hotel Bath Towel",
                    "source_data": {"product": {"categories": [2]}},
                    "cluster_profile": {"primary": "towels", "subclusters": ["bath"]},
                }
            ],
            profile_matches_cluster_fn=lambda profile, cluster: True,
            build_category_descendants_fn=category_sources.build_category_descendants,
            entity_storage_id_fn=lambda entity_type, entity_id: int(entity_id or 0) + (1000 if entity_type == "category" else 0),
            shared_subclusters_fn=category_sources.shared_subclusters,
            build_intent_profile_fn=lambda **kwargs: {
                "score": 50.0 if kwargs["relation_type"] == "category" else 42.0,
                "anchor_label": kwargs["target_name"],
                "topic_key": "bath towel",
                "anchor_label_source": "target_name",
                "anchor_quality": 80.0,
                "reason_summary": "Strong relevance",
                "reasons": ["topic overlap"],
                "shared_tokens": ["bath"],
                "query_target_tokens": ["bath"],
                "query_source_tokens": ["bath"],
                "attributes": {},
                "source_primary_cluster": "towels",
                "target_primary_cluster": "towels",
                "query_intent_scope": "commercial_topic",
                "preferred_entity_type": "category",
            },
            select_category_product_anchor_label_fn=lambda **kwargs: {"label": "Shop Hotel Bath Towel", "label_source": "target_name", "quality": 90.0},
        )

        self.assertEqual(len(rows), 4)
        self.assertTrue(any(row["target_entity_type"] == "category" for row in rows))
        self.assertTrue(any(row["target_entity_type"] == "product" for row in rows))


if __name__ == "__main__":
    unittest.main()
