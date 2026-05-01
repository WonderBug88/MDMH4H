import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

APP_ROOT = ROOT / "MDMH4H"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from app.fulcrum.services import _candidate_matches_review_target


class RestoreTargetMatchingTests(unittest.TestCase):
    def test_matches_category_candidate_using_bc_id_metadata(self):
        row = {
            "target_entity_type": "category",
            "target_entity_id": -1000005451,
            "target_url": "/hookless-shower-curtains/",
            "metadata": {"target_bc_entity_id": 5451},
        }

        self.assertTrue(
            _candidate_matches_review_target(
                row,
                target_entity_type="category",
                target_entity_id=5451,
                target_url="/hookless-shower-curtains/",
            )
        )

    def test_matches_legacy_category_candidate_using_url_fallback(self):
        row = {
            "target_entity_type": "category",
            "target_entity_id": -5451,
            "target_url": "https://www.hotels4humanity.com/hookless-shower-curtains",
            "metadata": {},
        }

        self.assertTrue(
            _candidate_matches_review_target(
                row,
                target_entity_type="category",
                target_entity_id=5451,
                target_url="/hookless-shower-curtains/",
            )
        )

    def test_rejects_different_target(self):
        row = {
            "target_entity_type": "category",
            "target_entity_id": -1000005446,
            "target_url": "/hotel-shower-curtains/",
            "metadata": {"target_bc_entity_id": 5446},
        }

        self.assertFalse(
            _candidate_matches_review_target(
                row,
                target_entity_type="category",
                target_entity_id=5451,
                target_url="/hookless-shower-curtains/",
            )
        )


if __name__ == "__main__":
    unittest.main()
