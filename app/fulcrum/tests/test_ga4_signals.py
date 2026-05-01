import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

APP_ROOT = ROOT / "MDMH4H"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from app.fulcrum.ga4_signals import build_ga4_signal


class FulcrumGa4SignalTests(unittest.TestCase):
    def test_product_signal_prefers_organic_engagement_metrics(self):
        result = build_ga4_signal(
            {
                "ga4_metrics": {
                    "sessions_90d": 20,
                    "engaged_sessions_90d": 4,
                    "organic_sessions_90d": 50,
                    "organic_engaged_sessions_90d": 30,
                    "organic_engagement_rate_90d": 0.6,
                    "organic_add_to_carts_90d": 3,
                    "organic_purchases_90d": 1,
                }
            },
            "product",
        )

        self.assertTrue(result["active"])
        self.assertGreater(result["delta"], 0)
        self.assertEqual(result["metrics"]["sessions_90d"], 50)
        self.assertEqual(result["metrics"]["engaged_sessions_90d"], 30)
        self.assertIn("PDP", result["reason"])

    def test_browse_signal_penalizes_weak_engagement(self):
        result = build_ga4_signal(
            {
                "ga4_metrics": {
                    "sessions_90d": 60,
                    "engagement_rate_90d": 0.1,
                }
            },
            "category",
        )

        self.assertLess(result["delta"], 1.0)
        self.assertIn("weak", result["summary"].lower())


if __name__ == "__main__":
    unittest.main()
