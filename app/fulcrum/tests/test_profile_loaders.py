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
from psycopg2.errors import UndefinedTable


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

    def test_missing_optional_brand_table_returns_empty_profiles(self):
        conn = _MissingTableConnection()

        result = profile_loaders.load_store_brand_profiles(
            "99oa2tso",
            get_pg_conn_fn=lambda: conn,
            normalize_storefront_path_fn=_normalize_path,
            extract_attribute_terms_fn=lambda value: {},
            tokenize_intent_text_fn=lambda value: set(),
            build_cluster_profile_fn=lambda **kwargs: {},
            dedupe_entity_profiles_fn=lambda profiles: profiles,
        )

        self.assertEqual(result, {})
        self.assertTrue(conn.rolled_back)

    def test_brand_profiles_fall_back_to_bigcommerce_api_rows(self):
        conn = _RowsConnection([])

        result = profile_loaders.load_store_brand_profiles(
            "99oa2tso",
            get_pg_conn_fn=lambda: conn,
            normalize_storefront_path_fn=_normalize_path,
            extract_attribute_terms_fn=lambda value: {"brand": {"downlite"}},
            tokenize_intent_text_fn=lambda value: {
                token
                for token in str(value or "").lower().replace("/", " ").replace("-", " ").split()
                if token
            },
            build_cluster_profile_fn=lambda **kwargs: {"primary": "bedding"},
            dedupe_entity_profiles_fn=lambda profiles: profiles,
            list_store_brands_fn=lambda store_hash: [
                {
                    "id": 3028,
                    "name": "DownLite Bedding",
                    "page_title": "DownLite Bedding",
                    "custom_url": {"url": "/downlite/", "is_customized": True},
                }
            ],
        )

        self.assertIn("/downlite/", result)
        self.assertEqual(result["/downlite/"]["entity_type"], "brand")
        self.assertEqual(result["/downlite/"]["bc_entity_id"], 3028)
        self.assertEqual(result["/downlite/"]["name"], "DownLite Bedding")

    def test_missing_optional_content_tables_return_empty_profiles(self):
        conn = _MissingTableConnection()

        result = profile_loaders.load_store_content_profiles(
            "99oa2tso",
            include_backlog=True,
            get_pg_conn_fn=lambda: conn,
            normalize_storefront_path_fn=_normalize_path,
            looks_like_content_path_fn=lambda value: True,
            load_reserved_storefront_urls_fn=lambda store_hash: set(),
            extract_attribute_terms_fn=lambda value: {},
            tokenize_intent_text_fn=lambda value: set(),
            build_cluster_profile_fn=lambda **kwargs: {},
            synthetic_content_entity_id_fn=lambda value: 1,
            humanize_url_path_title_fn=lambda value: "Content",
            dedupe_entity_profiles_fn=lambda profiles: profiles,
        )

        self.assertEqual(result, {})
        self.assertTrue(conn.rolled_back)


class _MissingTableCursor:
    def execute(self, sql, params=None):
        raise UndefinedTable()

    def fetchall(self):
        return []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _MissingTableConnection:
    def __init__(self):
        self.rolled_back = False

    def cursor(self, *args, **kwargs):
        return _MissingTableCursor()

    def rollback(self):
        self.rolled_back = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _RowsCursor:
    def __init__(self, rows):
        self.rows = rows

    def execute(self, sql, params=None):
        return None

    def fetchall(self):
        return list(self.rows)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _RowsConnection:
    def __init__(self, rows):
        self.rows = rows

    def cursor(self, *args, **kwargs):
        return _RowsCursor(self.rows)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


if __name__ == "__main__":
    unittest.main()
