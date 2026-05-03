import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

APP_ROOT = ROOT / "MDMH4H"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from app.fulcrum import bc_reset_publish


class FulcrumBigCommerceResetPublishTests(unittest.TestCase):
    def test_require_allowed_store_rejects_cross_store_cleanup(self):
        original = list(bc_reset_publish.Config.FULCRUM_ALLOWED_STORES)
        try:
            bc_reset_publish.Config.FULCRUM_ALLOWED_STORES = ["99oa2tso"]

            with self.assertRaises(ValueError):
                bc_reset_publish._require_allowed_store("otherstore")

            bc_reset_publish._require_allowed_store("stores/99oa2tso")
        finally:
            bc_reset_publish.Config.FULCRUM_ALLOWED_STORES = original


if __name__ == "__main__":
    unittest.main()
