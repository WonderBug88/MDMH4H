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

from app.fulcrum import services


class PublishSourceResolutionTests(unittest.TestCase):
    def test_resolve_publish_source_entity_ids_matches_category_by_bc_id(self):
        with patch(
            "app.fulcrum.services._latest_candidate_rows_for_store",
            return_value=[
                {
                    "source_entity_type": "category",
                    "source_entity_id": -1000005388,
                    "source_product_id": -1000005388,
                    "source_url": "/hotel-towels/",
                    "metadata": {"source_bc_entity_id": 5388},
                }
            ],
        ):
            result = services.resolve_publish_source_entity_ids(
                "Stores/99OA2TSO",
                source_entity_type="category",
                source_entity_id=5388,
                source_url="/hotel-towels/",
            )

        self.assertEqual(result, [-1000005388])

    def test_resolve_publish_source_entity_ids_matches_category_by_url_fallback(self):
        with patch(
            "app.fulcrum.services._latest_candidate_rows_for_store",
            return_value=[
                {
                    "source_entity_type": "category",
                    "source_entity_id": -5450,
                    "source_product_id": -5450,
                    "source_url": "https://www.hotels4humanity.com/fabric-curtains",
                    "metadata": {},
                }
            ],
        ):
            result = services.resolve_publish_source_entity_ids(
                "99oa2tso",
                source_entity_type="category",
                source_url="/fabric-curtains/",
            )

        self.assertEqual(result, [-5450])


if __name__ == "__main__":
    unittest.main()
