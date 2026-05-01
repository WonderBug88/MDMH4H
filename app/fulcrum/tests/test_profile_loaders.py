import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

APP_ROOT = ROOT / "MDMH4H"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from app.fulcrum import profile_loaders


def _normalize_path(value):
    raw = str(value or "").strip().lower()
    if not raw:
        return ""
    if not raw.startswith("/"):
        raw = "/" + raw
    if raw != "/" and not raw.endswith("/"):
        raw += "/"
    return raw


class FulcrumProfileLoaderTests(unittest.TestCase):
    def test_humanize_and_content_path_helpers(self):
        self.assertEqual(
            profile_loaders.humanize_url_path_title(
                "/blog/hotel-linen-faq/",
                normalize_storefront_path_fn=_normalize_path,
            ),
            "Hotel Linen FAQ",
        )
        self.assertTrue(
            profile_loaders.looks_like_content_path(
                "/guides/rollaway-bed-buying-guide/",
                normalize_storefront_path_fn=_normalize_path,
            )
        )
        self.assertFalse(
            profile_loaders.looks_like_content_path(
                "/towels/",
                normalize_storefront_path_fn=_normalize_path,
            )
        )

    def test_dedupe_entity_profiles_prefers_canonical_base_url(self):
        profiles = [
            {"url": "/towels/", "is_canonical_target": True, "eligible_for_routing": True},
            {"url": "/towels-2/", "is_canonical_target": False, "eligible_for_routing": True},
        ]

        deduped = profile_loaders.dedupe_entity_profiles(
            profiles,
            normalize_storefront_path_fn=_normalize_path,
            duplicate_suffix_base_url_fn=lambda url, known_urls=None: "/towels/" if url == "/towels-2/" else url,
        )

        self.assertEqual(len(deduped), 1)
        self.assertEqual(deduped[0]["url"], "/towels/")


if __name__ == "__main__":
    unittest.main()
