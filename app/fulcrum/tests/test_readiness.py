import json
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

from app.fulcrum import readiness


class _FakeCursor:
    def __init__(self, *, fetchone_results=None, fetchall_result=None, rowcounts=None):
        self.executions = []
        self._fetchone_results = list(fetchone_results or [])
        self._fetchall_result = list(fetchall_result or [])
        self._rowcounts = list(rowcounts or [])
        self.rowcount = 0

    def execute(self, sql, params=None):
        self.executions.append((sql, params))
        if self._rowcounts:
            self.rowcount = self._rowcounts.pop(0)

    def fetchone(self):
        return self._fetchone_results.pop(0) if self._fetchone_results else None

    def fetchall(self):
        return list(self._fetchall_result)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeConnection:
    def __init__(self, cursors):
        self._cursors = list(cursors)
        self._last_cursor = self._cursors[-1] if self._cursors else _FakeCursor()
        self.committed = False

    def cursor(self, *args, **kwargs):
        if self._cursors:
            self._last_cursor = self._cursors.pop(0)
        return self._last_cursor

    def commit(self):
        self.committed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FulcrumReadinessTests(unittest.TestCase):
    def test_list_pending_mapping_reviews_adds_mapping_refs(self):
        fake_cursor = _FakeCursor(
            fetchall_result=[
                {"mapping_kind": "option_name", "mapping_id": "12", "raw_option_name": "Color"},
                {"mapping_kind": "option_value", "mapping_id": "21", "raw_option_name": "Color"},
            ]
        )
        fake_conn = _FakeConnection([fake_cursor])

        with patch("app.fulcrum.readiness.get_pg_conn", return_value=fake_conn):
            rows = readiness.list_pending_mapping_reviews("Stores/99OA2TSO", limit=25)

        self.assertEqual([row["mapping_ref"] for row in rows], ["option_name:12", "option_value:21"])
        self.assertEqual(fake_cursor.executions[0][1][0], "99oa2tso")
        self.assertEqual(fake_cursor.executions[0][1][-1], 25)

    def test_auto_resolve_pending_mappings_sorts_rows_into_approve_and_ignore(self):
        profile_summary = Mock(return_value={})
        review_mapping_rows = Mock(
            side_effect=[
                {"updated_option_names": 0, "updated_option_values": 1},
                {"updated_option_names": 1, "updated_option_values": 0},
            ]
        )

        with (
            patch(
                "app.fulcrum.readiness.list_pending_mapping_reviews",
                return_value=[
                    {
                        "mapping_kind": "option_name",
                        "mapping_ref": "option_name:4",
                        "raw_option_name": "Choose Your Towels",
                        "bucket_key": "color",
                    },
                    {
                        "mapping_kind": "option_value",
                        "mapping_ref": "option_value:8",
                        "raw_option_value": "Titanium Gold",
                        "proposed_value": "gold",
                        "bucket_key": "color",
                    },
                    {
                        "mapping_kind": "option_value",
                        "mapping_ref": "option_value:9",
                        "raw_option_value": "Custom",
                        "proposed_value": "green",
                        "bucket_key": "color",
                    },
                ],
            ),
            patch("app.fulcrum.readiness.review_mapping_rows", review_mapping_rows),
            patch(
                "app.fulcrum.readiness.get_store_readiness",
                return_value={
                    "unresolved_option_name_count": 3,
                    "unresolved_option_value_count": 2,
                },
            ),
        ):
            result = readiness.auto_resolve_pending_mappings(
                "99oa2tso",
                reviewed_by="qa@example.com",
                get_store_profile_summary=profile_summary,
            )

        self.assertEqual(result["approved_refs"], 1)
        self.assertEqual(result["ignored_refs"], 1)
        self.assertEqual(result["remaining_unresolved_option_names"], 3)
        self.assertEqual(result["remaining_unresolved_option_values"], 2)
        self.assertEqual(review_mapping_rows.call_count, 2)
        self.assertEqual(review_mapping_rows.call_args_list[0].args[1], ["option_value:8"])
        self.assertEqual(review_mapping_rows.call_args_list[0].args[2], "approved")
        self.assertEqual(review_mapping_rows.call_args_list[1].args[1], ["option_name:4"])
        self.assertEqual(review_mapping_rows.call_args_list[1].args[2], "ignored")

    def test_review_mapping_rows_updates_tables_and_refreshes_readiness(self):
        fake_cursor = _FakeCursor(rowcounts=[1, 1])
        fake_conn = _FakeConnection([fake_cursor])
        profile_summary = Mock(return_value={})

        with (
            patch("app.fulcrum.readiness.get_pg_conn", return_value=fake_conn),
            patch("app.fulcrum.readiness.refresh_store_readiness", return_value={"status": "ok"}) as refresh_mock,
        ):
            result = readiness.review_mapping_rows(
                "Stores/99OA2TSO",
                ["option_name:5", "option_value:9", "option_name:not-a-number"],
                "approved",
                reviewed_by="qa@example.com",
                note="Reviewed in test",
                get_store_profile_summary=profile_summary,
            )

        self.assertEqual(result, {"updated_option_names": 1, "updated_option_values": 1})
        self.assertTrue(fake_conn.committed)
        self.assertEqual(fake_cursor.executions[0][1][2], "99oa2tso")
        self.assertEqual(fake_cursor.executions[0][1][3], [5])
        self.assertEqual(fake_cursor.executions[1][1][2], "99oa2tso")
        self.assertEqual(fake_cursor.executions[1][1][3], [9])
        refresh_mock.assert_called_once_with(
            "99oa2tso",
            get_store_profile_summary=profile_summary,
        )

    def test_refresh_store_readiness_builds_expected_flags(self):
        unresolved_cursor = _FakeCursor(
            fetchone_results=[
                {
                    "unresolved_option_name_count": 0,
                    "unresolved_option_value_count": 0,
                }
            ]
        )
        upsert_cursor = _FakeCursor(
            fetchone_results=[
                {
                    "store_hash": "99oa2tso",
                    "catalog_synced": True,
                    "attribute_mappings_ready": True,
                    "theme_hook_ready": True,
                    "auto_publish_ready": True,
                    "category_beta_ready": True,
                    "unresolved_option_name_count": 0,
                    "unresolved_option_value_count": 0,
                    "metadata": {"persisted": True},
                    "updated_at": "2026-04-15T10:00:00",
                }
            ]
        )
        unresolved_conn = _FakeConnection([unresolved_cursor])
        upsert_conn = _FakeConnection([upsert_cursor])

        with (
            patch("app.fulcrum.readiness._normalize_mapping_review_statuses", return_value={"pending_name_count": 0, "pending_value_count": 0}),
            patch(
                "app.fulcrum.readiness.get_store_readiness",
                return_value={
                    "metadata": {
                        "category_publishing_enabled_override": True,
                        "category_metafields_readable": True,
                        "category_render_verified": True,
                        "category_rollback_verified": True,
                    }
                },
            ),
            patch("app.fulcrum.readiness.get_pg_conn", side_effect=[unresolved_conn, upsert_conn]),
            patch("app.fulcrum.readiness.render_theme_hook_present", return_value=True),
            patch("app.fulcrum.readiness.render_category_theme_hook_present", return_value=True),
            patch.object(readiness.Config, "FULCRUM_ENABLE_CATEGORY_PUBLISHING", False),
            patch.object(readiness.Config, "FULCRUM_AUTO_PUBLISH_ENABLED", True),
            patch.object(readiness.Config, "FULCRUM_AUTO_PUBLISH_MIN_SCORE", 78),
            patch.object(readiness.Config, "FULCRUM_AUTO_PUBLISH_MAX_LINKS_PER_SOURCE", 4),
            patch.object(readiness.Config, "FULCRUM_REQUIRE_REVIEW_FOR_CATEGORIES", True),
        ):
            result = readiness.refresh_store_readiness(
                "Stores/99OA2TSO",
                get_store_profile_summary=lambda store_hash: {
                    "profile_count": 12,
                    "category_profile_count": 4,
                    "option_name_mapping_count": 3,
                    "option_value_mapping_count": 8,
                },
            )

        self.assertEqual(result["store_hash"], "99oa2tso")
        self.assertTrue(upsert_conn.committed)

        params = upsert_cursor.executions[0][1]
        self.assertEqual(params[0], "99oa2tso")
        self.assertTrue(params[1])
        self.assertTrue(params[2])
        self.assertTrue(params[3])
        self.assertTrue(params[4])
        self.assertTrue(params[5])

        metadata = json.loads(params[8])
        self.assertEqual(metadata["alpha_default_buckets"], list(readiness.ALPHA_DEFAULT_BUCKETS))
        self.assertTrue(metadata["category_theme_hook_present"])
        self.assertTrue(metadata["feature_flags"]["category_publishing_enabled"])
        self.assertTrue(metadata["feature_flags"]["auto_publish_enabled"])

    def test_refresh_store_readiness_preserves_hosted_theme_state_without_local_templates(self):
        unresolved_cursor = _FakeCursor(
            fetchone_results=[
                {
                    "unresolved_option_name_count": 0,
                    "unresolved_option_value_count": 0,
                }
            ]
        )
        upsert_cursor = _FakeCursor(
            fetchone_results=[
                {
                    "store_hash": "99oa2tso",
                    "catalog_synced": True,
                    "attribute_mappings_ready": True,
                    "theme_hook_ready": True,
                    "auto_publish_ready": True,
                    "category_beta_ready": True,
                    "unresolved_option_name_count": 0,
                    "unresolved_option_value_count": 0,
                    "metadata": {"category_theme_hook_present": True},
                    "updated_at": "2026-05-01T10:00:00",
                }
            ]
        )

        with (
            patch("app.fulcrum.readiness._normalize_mapping_review_statuses", return_value={}),
            patch(
                "app.fulcrum.readiness.get_store_readiness",
                return_value={
                    "theme_hook_ready": True,
                    "metadata": {
                        "category_theme_hook_present": True,
                        "category_publishing_enabled_override": True,
                        "category_metafields_readable": True,
                        "category_render_verified": True,
                        "category_rollback_verified": True,
                    },
                },
            ),
            patch("app.fulcrum.readiness.get_pg_conn", side_effect=[_FakeConnection([unresolved_cursor]), _FakeConnection([upsert_cursor])]),
            patch(
                "app.fulcrum.readiness.render_theme_hook_present",
                side_effect=AssertionError("local product template check should not run"),
            ),
            patch(
                "app.fulcrum.readiness.render_category_theme_hook_present",
                side_effect=AssertionError("local category template check should not run"),
            ),
            patch.object(readiness.Config, "FULCRUM_THEME_PRODUCT_TEMPLATE", "Z:/missing/product.html"),
            patch.object(readiness.Config, "FULCRUM_THEME_CATEGORY_TEMPLATE", "Z:/missing/category.html"),
            patch.object(readiness.Config, "FULCRUM_ENABLE_CATEGORY_PUBLISHING", False),
            patch.object(readiness.Config, "FULCRUM_AUTO_PUBLISH_ENABLED", True),
        ):
            readiness.refresh_store_readiness(
                "99oa2tso",
                get_store_profile_summary=lambda store_hash: {
                    "profile_count": 12,
                    "category_profile_count": 4,
                    "option_name_mapping_count": 3,
                    "option_value_mapping_count": 8,
                },
            )

        params = upsert_cursor.executions[0][1]
        self.assertTrue(params[3])
        self.assertTrue(params[4])
        self.assertTrue(params[5])
        metadata = json.loads(params[8])
        self.assertTrue(metadata["category_theme_hook_present"])

    def test_refresh_store_readiness_does_not_block_auto_publish_on_pending_mapping_reviews(self):
        unresolved_cursor = _FakeCursor(
            fetchone_results=[
                {
                    "unresolved_option_name_count": 29,
                    "unresolved_option_value_count": 185,
                }
            ]
        )
        upsert_cursor = _FakeCursor(
            fetchone_results=[
                {
                    "store_hash": "99oa2tso",
                    "catalog_synced": True,
                    "attribute_mappings_ready": False,
                    "theme_hook_ready": True,
                    "auto_publish_ready": True,
                    "category_beta_ready": False,
                    "unresolved_option_name_count": 29,
                    "unresolved_option_value_count": 185,
                    "metadata": {},
                    "updated_at": "2026-05-01T10:00:00",
                }
            ]
        )

        with (
            patch("app.fulcrum.readiness._normalize_mapping_review_statuses", return_value={}),
            patch("app.fulcrum.readiness.get_store_readiness", return_value={"metadata": {}}),
            patch("app.fulcrum.readiness.get_pg_conn", side_effect=[_FakeConnection([unresolved_cursor]), _FakeConnection([upsert_cursor])]),
            patch("app.fulcrum.readiness.render_theme_hook_present", return_value=True),
            patch("app.fulcrum.readiness.render_category_theme_hook_present", return_value=False),
            patch.object(readiness.Config, "FULCRUM_AUTO_PUBLISH_ENABLED", True),
        ):
            readiness.refresh_store_readiness(
                "99oa2tso",
                get_store_profile_summary=lambda store_hash: {
                    "profile_count": 12,
                    "category_profile_count": 4,
                    "option_name_mapping_count": 3,
                    "option_value_mapping_count": 8,
                },
            )

        params = upsert_cursor.executions[0][1]
        self.assertFalse(params[2])
        self.assertTrue(params[3])
        self.assertTrue(params[4])


if __name__ == "__main__":
    unittest.main()
