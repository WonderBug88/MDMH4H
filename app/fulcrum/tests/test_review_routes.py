import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

APP_ROOT = ROOT / "MDMH4H"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from app.fulcrum import routes


class FulcrumReviewRouteHelperTests(unittest.TestCase):
    def test_review_target_matches_source_handles_internal_category_ids(self):
        self.assertTrue(
            routes._review_target_matches_source(
                source_entity_type="category",
                source_entity_id=-100000111,
                source_url="/hotel-towels/",
                target_entity_type="category",
                target_entity_id=-100000111,
                target_url="https://example.com/hotel-towels",
            )
        )

    def test_resolve_review_restore_target_keeps_category_winner_with_negative_id(self):
        request_row = {
            "source_entity_type": "category",
            "source_entity_id": -100000111,
            "source_url": "/hotel-towels/",
            "target_entity_type": "category",
            "target_entity_id": -100000222,
            "target_url": "/bath-spa-towels/",
        }
        gate_review = {
            "recommended_action": "keep_winner",
            "metadata": {
                "winner": {
                    "entity_type": "category",
                    "entity_id": -100000222,
                    "url": "/bath-spa-towels/",
                }
            },
        }
        gate_row = {
            "suggested_target": {
                "entity_type": "category",
                "entity_id": -100000222,
                "url": "/bath-spa-towels/",
            }
        }

        resolved = routes._resolve_review_restore_target(
            request_row,
            gate_review=gate_review,
            gate_row=gate_row,
        )

        self.assertEqual(resolved, ("category", -100000222, "/bath-spa-towels/", "keep_winner"))

    def test_resolve_review_restore_target_skips_use_original(self):
        request_row = {
            "source_entity_type": "category",
            "source_entity_id": -100000111,
            "source_url": "/hotel-towels/",
            "target_entity_type": "category",
            "target_entity_id": -100000222,
            "target_url": "/bath-spa-towels/",
        }

        resolved = routes._resolve_review_restore_target(
            request_row,
            gate_review={"recommended_action": "use_original"},
            gate_row={},
        )

        self.assertEqual(resolved, (None, None, None, "use_original"))


if __name__ == "__main__":
    unittest.main()