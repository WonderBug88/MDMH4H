import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

APP_ROOT = ROOT / "MDMH4H"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from app.fulcrum import services


class FulcrumServicesRankingTests(unittest.TestCase):
    def test_rank_source_rows_keeps_best_category_target_for_category_sources(self):
        rows = [
            {
                "candidate_id": 10,
                "target_entity_type": "product",
                "target_product_id": 1001,
                "target_url": "/product-a/",
                "example_query": "luxury bath towels wholesale",
                "target_name": "Product A",
                "score": 100,
                "metadata": {"query_intent_scope": "specific_product", "preferred_entity_type": "product"},
            },
            {
                "candidate_id": 11,
                "target_entity_type": "product",
                "target_product_id": 1002,
                "target_url": "/product-b/",
                "example_query": "luxury bath towels wholesale",
                "target_name": "Product B",
                "score": 99,
                "metadata": {"query_intent_scope": "specific_product", "preferred_entity_type": "product"},
            },
            {
                "candidate_id": 12,
                "target_entity_type": "product",
                "target_product_id": 1003,
                "target_url": "/product-c/",
                "example_query": "luxury bath towels wholesale",
                "target_name": "Product C",
                "score": 98,
                "metadata": {"query_intent_scope": "specific_product", "preferred_entity_type": "product"},
            },
            {
                "candidate_id": 13,
                "target_entity_type": "product",
                "target_product_id": 1004,
                "target_url": "/product-d/",
                "example_query": "luxury bath towels wholesale",
                "target_name": "Product D",
                "score": 97,
                "metadata": {"query_intent_scope": "specific_product", "preferred_entity_type": "product"},
            },
            {
                "candidate_id": 20,
                "target_entity_type": "category",
                "target_product_id": -1000005426,
                "target_url": "/bath-spa-towels/",
                "example_query": "bath towels in bulk",
                "target_name": "Bath & Spa Towels",
                "score": 100,
                "metadata": {"query_intent_scope": "commercial_topic", "preferred_entity_type": "category"},
            },
        ]

        ranked = services._rank_source_rows(rows, source_entity_type="category")

        self.assertEqual(len(ranked), 4)
        self.assertEqual(ranked[0]["candidate_id"], 20)
        self.assertIn(20, [row["candidate_id"] for row in ranked])


if __name__ == "__main__":
    unittest.main()
