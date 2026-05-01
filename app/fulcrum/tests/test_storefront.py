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

from app.fulcrum import storefront


class _FakeCursor:
    def __init__(self, *, fetchone_results=None, fetchall_results=None):
        self.executions = []
        self._fetchone_results = list(fetchone_results or [])
        self._fetchall_results = list(fetchall_results or [])

    def execute(self, sql, params=None):
        self.executions.append((sql, params))

    def fetchone(self):
        return self._fetchone_results.pop(0) if self._fetchone_results else None

    def fetchall(self):
        return self._fetchall_results.pop(0) if self._fetchall_results else []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeConnection:
    def __init__(self, cursors):
        self._cursors = list(cursors)
        self._last_cursor = self._cursors[-1] if self._cursors else _FakeCursor()

    def cursor(self, *args, **kwargs):
        if self._cursors:
            self._last_cursor = self._cursors.pop(0)
        return self._last_cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FulcrumStorefrontTests(unittest.TestCase):
    def setUp(self):
        storefront.clear_storefront_site_caches()

    def tearDown(self):
        storefront.clear_storefront_site_caches()

    def test_select_storefront_site_row_prefers_requested_channel_then_storefront(self):
        rows = [
            {"channel_id": 8, "channel_type": "marketplace"},
            {"channel_id": 3, "channel_type": "storefront"},
            {"channel_id": 1, "channel_type": "storefront"},
        ]

        self.assertEqual(
            storefront.select_storefront_site_row(rows, channel_id=8),
            {"channel_id": 8, "channel_type": "marketplace"},
        )
        self.assertEqual(
            storefront.select_storefront_site_row(rows),
            {"channel_id": 3, "channel_type": "storefront"},
        )

    def test_get_storefront_base_url_from_db_uses_cache_until_cleared(self):
        first_cursor = _FakeCursor(
            fetchall_results=[
                [
                    {
                        "channel_id": 1,
                        "channel_type": "storefront",
                        "primary_url": "https://first.example.com",
                        "site_url": "",
                        "canonical_url": "",
                        "checkout_url": "",
                    }
                ]
            ]
        )
        second_cursor = _FakeCursor(
            fetchall_results=[
                [
                    {
                        "channel_id": 1,
                        "channel_type": "storefront",
                        "primary_url": "https://second.example.com",
                        "site_url": "",
                        "canonical_url": "",
                        "checkout_url": "",
                    }
                ]
            ]
        )

        with patch(
            "app.fulcrum.storefront.get_pg_conn",
            side_effect=[_FakeConnection([first_cursor]), _FakeConnection([second_cursor])],
        ):
            self.assertEqual(
                storefront.get_storefront_base_url_from_db("stores/99OA2TSO"),
                "https://first.example.com",
            )
            self.assertEqual(
                storefront.get_storefront_base_url_from_db("stores/99OA2TSO"),
                "https://first.example.com",
            )
            storefront.clear_storefront_site_caches()
            self.assertEqual(
                storefront.get_storefront_base_url_from_db("stores/99OA2TSO"),
                "https://second.example.com",
            )

        self.assertEqual(len(first_cursor.executions), 1)
        self.assertEqual(len(second_cursor.executions), 1)

    def test_get_storefront_base_url_falls_back_for_known_and_generic_store(self):
        with patch("app.fulcrum.storefront.get_storefront_base_url_from_db", return_value=""):
            self.assertEqual(
                storefront.get_storefront_base_url("stores/99OA2TSO"),
                "https://www.hotels4humanity.com",
            )
            self.assertEqual(
                storefront.get_storefront_base_url("stores/pdwzti0dpv"),
                "https://www.hotels4humanity.com",
            )
            self.assertEqual(
                storefront.get_storefront_base_url("stores/abc123"),
                "https://store-abc123.mybigcommerce.com",
            )

    def test_list_storefront_base_urls_keeps_known_storefront_override_alongside_db_rows(self):
        rows = [
            {
                "primary_url": "https://rankops-authority-routing.mybigcommerce.com",
                "site_url": "",
                "canonical_url": "https://store-pdwzti0dpv-1.mybigcommerce.com",
                "checkout_url": "",
            }
        ]

        with patch("app.fulcrum.storefront.load_storefront_site_rows", return_value=rows):
            urls = storefront.list_storefront_base_urls("stores/pdwzti0dpv")

        self.assertEqual(
            urls,
            [
                "https://rankops-authority-routing.mybigcommerce.com",
                "https://store-pdwzti0dpv-1.mybigcommerce.com",
                "https://www.hotels4humanity.com",
            ],
        )

    def test_extract_storefront_channel_id_checks_metadata_candidates(self):
        channel_id = storefront.extract_storefront_channel_id(
            {"metadata": {"target_channel_id": "7"}},
            {"source_channel_id": "4"},
        )
        self.assertEqual(channel_id, 7)

    def test_get_store_profile_summary_returns_counts_clusters_and_default_url(self):
        summary_cursor = _FakeCursor(
            fetchone_results=[
                {
                    "profile_count": 12,
                    "category_profile_count": 5,
                    "option_name_mapping_count": 3,
                    "option_value_mapping_count": 9,
                    "storefront_site_count": 2,
                    "storefront_channel_count": 1,
                    "last_synced_at": "2026-04-15T12:00:00",
                }
            ],
            fetchall_results=[
                [
                    {"primary_cluster": "towels", "profile_count": 8},
                    {"primary_cluster": "bedding", "profile_count": 4},
                ]
            ],
        )
        fake_conn = _FakeConnection([summary_cursor])

        with (
            patch("app.fulcrum.storefront.get_pg_conn", return_value=fake_conn),
            patch(
                "app.fulcrum.storefront.get_storefront_base_url",
                return_value="https://store.example.com",
            ),
        ):
            summary = storefront.get_store_profile_summary("Stores/99OA2TSO")

        self.assertEqual(summary["profile_count"], 12)
        self.assertEqual(summary["cluster_counts"][0]["primary_cluster"], "towels")
        self.assertEqual(summary["default_storefront_base_url"], "https://store.example.com")
        self.assertEqual(summary_cursor.executions[0][1][0], "99oa2tso")
        self.assertEqual(summary_cursor.executions[1][1], ("99oa2tso",))


if __name__ == "__main__":
    unittest.main()
