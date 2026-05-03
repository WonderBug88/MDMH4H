import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch


ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

APP_ROOT = ROOT / "MDMH4H"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from app.fulcrum import bc_reset_publish


class FulcrumBigCommerceResetPublishTests(unittest.TestCase):
    def test_parse_reviewed_metafield_spec_requires_entity_and_metafield_ids(self):
        self.assertEqual(
            bc_reset_publish.parse_reviewed_metafield_spec("product:112556:11377"),
            {"entity_type": "product", "entity_id": 112556, "metafield_id": 11377},
        )
        self.assertEqual(
            bc_reset_publish.parse_reviewed_metafield_spec("categories:42:8"),
            {"entity_type": "category", "entity_id": 42, "metafield_id": 8},
        )
        with self.assertRaises(ValueError):
            bc_reset_publish.parse_reviewed_metafield_spec("product:112556")

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

    def test_targeted_execute_deletes_only_reviewed_policy_blocked_metafield(self):
        original = list(bc_reset_publish.Config.FULCRUM_ALLOWED_STORES)
        remote_before = [
            {
                "entity_type": "product",
                "entity_id": 112556,
                "entity_name": "Cloud Top",
                "entity_url": "/products/cloud-top-primaloft-plush-blankets-throws-by-downlite.html",
                "metafield_id": 11377,
                "key": "internal_links_html",
            }
        ]
        active_publications = [
            {
                "source_entity_type": "product",
                "source_product_id": 112556,
                "source_url": "/products/cloud-top-primaloft-plush-blankets-throws-by-downlite.html",
                "metafield_key": "internal_links_html",
            }
        ]
        approved_rows = [
            {
                "source_entity_type": "product",
                "source_product_id": 112556,
                "source_url": "/products/cloud-top-primaloft-plush-blankets-throws-by-downlite.html",
            }
        ]
        delete_mock = Mock()
        try:
            bc_reset_publish.Config.FULCRUM_ALLOWED_STORES = ["99oa2tso"]
            with patch(
                "app.fulcrum.bc_reset_publish.list_remote_link_metafields",
                side_effect=[remote_before, []],
            ) as list_remote_mock, patch(
                "app.fulcrum.bc_reset_publish.list_publications",
                return_value=active_publications,
            ), patch(
                "app.fulcrum.bc_reset_publish.category_publishing_enabled_for_store",
                return_value=False,
            ), patch(
                "app.fulcrum.bc_reset_publish._latest_approved_candidate_rows",
                return_value=approved_rows,
            ), patch(
                "app.fulcrum.bc_reset_publish.candidate_publish_block_reason",
                return_value="brand-navigation query cannot publish to category",
            ), patch(
                "app.fulcrum.bc_reset_publish._approved_source_counts",
                return_value={"product": 1, "category": 0},
            ), patch(
                "app.fulcrum.bc_reset_publish._gate_disposition_counts",
                return_value={"pass": 1, "hold": 0, "reject": 0},
            ), patch(
                "app.fulcrum.bc_reset_publish._delete_remote_link_metafield",
                delete_mock,
            ), patch(
                "app.fulcrum.bc_reset_publish.unpublish_entities",
            ) as unpublish_mock, patch(
                "app.fulcrum.bc_reset_publish.publish_approved_entities",
            ) as publish_mock:
                result = bc_reset_publish.reset_and_republish_bigcommerce_links(
                    "99oa2tso",
                    execute=True,
                    reviewed_metafields=["product:112556:11377"],
                )

            self.assertEqual(result["deleted_reviewed_metafield_count"], 1)
            self.assertEqual(result["reviewed_delete_eligible_count"], 1)
            self.assertEqual(result["skipped_reviewed_metafield_targets"], [])
            delete_mock.assert_called_once_with("99oa2tso", remote_before[0])
            unpublish_mock.assert_not_called()
            publish_mock.assert_not_called()
            self.assertEqual(list_remote_mock.call_args_list[0].kwargs["product_ids"], [112556])
        finally:
            bc_reset_publish.Config.FULCRUM_ALLOWED_STORES = original

    def test_targeted_execute_skips_reviewed_metafield_when_current_policy_allows_it(self):
        original = list(bc_reset_publish.Config.FULCRUM_ALLOWED_STORES)
        remote_before = [
            {
                "entity_type": "product",
                "entity_id": 112556,
                "entity_name": "Cloud Top",
                "entity_url": "/products/cloud-top-primaloft-plush-blankets-throws-by-downlite.html",
                "metafield_id": 11377,
                "key": "internal_links_html",
            }
        ]
        active_publications = [
            {
                "source_entity_type": "product",
                "source_product_id": 112556,
                "source_url": "/products/cloud-top-primaloft-plush-blankets-throws-by-downlite.html",
                "metafield_key": "internal_links_html",
            }
        ]
        try:
            bc_reset_publish.Config.FULCRUM_ALLOWED_STORES = ["99oa2tso"]
            with patch(
                "app.fulcrum.bc_reset_publish.list_remote_link_metafields",
                side_effect=[remote_before, remote_before],
            ), patch(
                "app.fulcrum.bc_reset_publish.list_publications",
                return_value=active_publications,
            ), patch(
                "app.fulcrum.bc_reset_publish.category_publishing_enabled_for_store",
                return_value=False,
            ), patch(
                "app.fulcrum.bc_reset_publish._latest_approved_candidate_rows",
                return_value=[],
            ), patch(
                "app.fulcrum.bc_reset_publish._approved_source_counts",
                return_value={"product": 1, "category": 0},
            ), patch(
                "app.fulcrum.bc_reset_publish._gate_disposition_counts",
                return_value={"pass": 1, "hold": 0, "reject": 0},
            ), patch(
                "app.fulcrum.bc_reset_publish._delete_remote_link_metafield",
            ) as delete_mock:
                result = bc_reset_publish.reset_and_republish_bigcommerce_links(
                    "99oa2tso",
                    execute=True,
                    reviewed_metafields=["product:112556:11377"],
                )

            self.assertEqual(result["deleted_reviewed_metafield_count"], 0)
            self.assertEqual(result["skipped_reviewed_metafield_targets"][0]["reason"], "not_found_or_not_currently_orphan_or_policy_blocked")
            self.assertTrue(result["skipped_reviewed_metafield_targets"][0]["remote_found"])
            delete_mock.assert_not_called()
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
