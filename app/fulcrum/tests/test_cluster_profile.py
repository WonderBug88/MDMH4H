import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

APP_ROOT = ROOT / "MDMH4H"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from app.fulcrum import cluster_profile


def _tokenize(value):
    return {token for token in str(value or "").lower().replace("-", " ").split() if token}


class FulcrumClusterProfileTests(unittest.TestCase):
    def test_build_cluster_profile_detects_shower_curtain_subclusters(self):
        result = cluster_profile.build_cluster_profile(
            "Hookless Vinyl Shower Curtain",
            "/hookless-vinyl-shower-curtain/",
            "Hookless",
            "",
            {"form": {"hookless"}, "material": {"vinyl"}},
            tokenize_intent_text_fn=_tokenize,
            shower_curtain_subcluster_map={"hookless": "hookless-shower-curtains"},
            towel_subcluster_map={"bath-towel": "bath-towels"},
            topic_priority={"curtain", "bedding"},
        )

        self.assertEqual(result["primary"], "shower-curtains")
        self.assertIn("shower-curtains", result["clusters"])
        self.assertIn("hookless-shower-curtains", result["subclusters"])
        self.assertIn("vinyl-shower-curtains", result["subclusters"])

    def test_build_cluster_profile_falls_back_to_topic_priority(self):
        result = cluster_profile.build_cluster_profile(
            "Hospitality Bedding Guide",
            "/hospitality-bedding-guide/",
            "",
            "",
            {},
            tokenize_intent_text_fn=_tokenize,
            shower_curtain_subcluster_map={},
            towel_subcluster_map={},
            topic_priority={"bedding", "towels"},
        )

        self.assertEqual(result["primary"], "bedding")
        self.assertEqual(result["clusters"], ["bedding"])


if __name__ == "__main__":
    unittest.main()
