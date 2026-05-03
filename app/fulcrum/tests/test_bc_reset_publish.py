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

    def test_filtered_execute_is_blocked(self):
        original = list(bc_reset_publish.Config.FULCRUM_ALLOWED_STORES)
        try:
            bc_reset_publish.Config.FULCRUM_ALLOWED_STORES = ["99oa2tso"]

            with self.assertRaises(ValueError):
                bc_reset_publish.reset_and_republish_bigcommerce_links(
                    "99oa2tso",
                    execute=True,
                    product_ids=[112556],
                )
        finally:
            bc_reset_publish.Config.FULCRUM_ALLOWED_STORES = original

    def test_list_remote_link_metafields_can_scan_specific_product_id(self):
        original = list(bc_reset_publish.Config.FULCRUM_ALLOWED_STORES)
        try:
            bc_reset_publish.Config.FULCRUM_ALLOWED_STORES = ["99oa2tso"]

            def _fake_get(url, **kwargs):
                if url.endswith("/catalog/products/112556"):
                    return _FakeResponse(
                        {
                            "data": {
                                "id": 112556,
                                "name": "Cloud Top",
                                "custom_url": {"url": "/cloud-top/"},
                            }
                        }
                    )
                if url.endswith("/catalog/products/112556/metafields"):
                    return _FakeResponse(
                        {
                            "data": [
                                {"id": 44, "key": "internal_links_html"},
                                {"id": 45, "key": "not_route_authority"},
                            ]
                        }
                    )
                raise AssertionError(f"Unexpected URL: {url}")

            with patch("app.fulcrum.bc_reset_publish.get_bc_headers", return_value={"X-Auth-Token": "token"}), patch(
                "app.fulcrum.bc_reset_publish.requests.get",
                side_effect=_fake_get,
            ), patch("app.fulcrum.bc_reset_publish.list_store_categories", return_value=[]):
                rows = bc_reset_publish.list_remote_link_metafields("99oa2tso", product_ids=[112556])

            self.assertEqual(
                rows,
                [
                    {
                        "entity_type": "product",
                        "entity_id": 112556,
                        "entity_name": "Cloud Top",
                        "entity_url": "/cloud-top/",
                        "metafield_id": 44,
                        "key": "internal_links_html",
                    }
                ],
            )
        finally:
            bc_reset_publish.Config.FULCRUM_ALLOWED_STORES = original


class _FakeResponse:
    def __init__(self, payload, status_code=200):
        self.payload = payload
        self.status_code = status_code

    def json(self):
        return self.payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


if __name__ == "__main__":
    unittest.main()
