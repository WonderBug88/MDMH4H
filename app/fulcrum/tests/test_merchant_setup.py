import sys
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch
import json


ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

APP_ROOT = ROOT / "MDMH4H"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from app.fulcrum import merchant_setup


class _FakeCursor:
    def __init__(self, row=None):
        self.row = row
        self.executions = []

    def execute(self, sql, params=None):
        self.executions.append((sql, params))

    def fetchone(self):
        return self.row

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor
        self.committed = False

    def cursor(self, *args, **kwargs):
        return self._cursor

    def commit(self):
        self.committed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeSearchAnalyticsQuery:
    def __init__(self, responses):
        self.responses = responses

    def query(self, **kwargs):
        return self

    def execute(self):
        return self.responses.pop(0) if self.responses else {"rows": []}


class _FakeSearchConsoleService:
    def __init__(self, responses):
        self._query = _FakeSearchAnalyticsQuery(responses)

    def searchanalytics(self):
        return self._query


class MerchantSetupTests(unittest.TestCase):
    def test_comparison_window_dates_cover_current_prior_and_year_prior_windows(self):
        start_date, end_date = merchant_setup._comparison_window_dates(date(2026, 4, 20))

        self.assertEqual(start_date.isoformat(), "2025-01-19")
        self.assertEqual(end_date.isoformat(), "2026-04-18")
        self.assertEqual(merchant_setup._comparison_window_relative_dates(), ("456daysAgo", "2daysAgo"))

    def test_gsc_sync_window_uses_bounded_storage_default(self):
        with patch.object(merchant_setup.Config, "FULCRUM_GSC_SYNC_LOOKBACK_DAYS", 180):
            start_date, end_date = merchant_setup._gsc_sync_window_dates(date(2026, 4, 20))

        self.assertEqual(start_date.isoformat(), "2025-10-20")
        self.assertEqual(end_date.isoformat(), "2026-04-18")

    def test_build_google_authorization_url_does_not_request_incremental_scopes(self):
        class _FakeFlow:
            def __init__(self):
                self.kwargs = None

            def authorization_url(self, **kwargs):
                self.kwargs = kwargs
                return "https://accounts.google.com/o/oauth2/auth?stub=1", "state-123"

        fake_flow = _FakeFlow()

        with patch("app.fulcrum.merchant_setup._build_google_flow", return_value=fake_flow):
            authorization_url, state = merchant_setup.build_google_authorization_url("gsc")

        self.assertEqual(authorization_url, "https://accounts.google.com/o/oauth2/auth?stub=1")
        self.assertEqual(state, "state-123")
        self.assertEqual(fake_flow.kwargs["access_type"], "offline")
        self.assertEqual(fake_flow.kwargs["prompt"], "select_account consent")
        self.assertNotIn("include_granted_scopes", fake_flow.kwargs)

    def test_auth_payload_is_encrypted_before_storage_and_roundtrips(self):
        raw = {
            "token": "secret-token",
            "refresh_token": "refresh-secret",
            "client_id": "client",
            "client_secret": "secret",
            "scopes": ["scope"],
        }

        encrypted = merchant_setup._encrypt_auth_payload(raw)

        self.assertTrue(encrypted["encrypted"])
        self.assertNotIn("secret-token", json.dumps(encrypted))
        self.assertEqual(merchant_setup._decrypt_auth_payload(encrypted), raw)

    def test_refresh_store_gsc_data_zero_rows_marks_warning_without_db_delete(self):
        with (
            patch(
                "app.fulcrum.merchant_setup.get_store_integration",
                return_value={
                    "selected_resource_id": "https://www.hotels4humanity.com/",
                    "selected_resource_label": "https://www.hotels4humanity.com/",
                    "auth_payload": {"token": "token"},
                },
            ),
            patch("app.fulcrum.merchant_setup._credential_payload_to_credentials", return_value=object()),
            patch("app.fulcrum.merchant_setup.build", return_value=_FakeSearchConsoleService([{"rows": []}])),
            patch("app.fulcrum.merchant_setup.get_pg_conn", side_effect=AssertionError("old rows should not be deleted")),
            patch("app.fulcrum.merchant_setup._upsert_store_integration", return_value={}) as upsert_mock,
        ):
            result = merchant_setup.refresh_store_gsc_data("99oa2tso")

        self.assertEqual(result["status"], "warning")
        self.assertEqual(result["row_count"], 0)
        kwargs = upsert_mock.call_args.kwargs
        self.assertEqual(kwargs["configuration_status"], "sync_warning")
        self.assertIn("zero rows", kwargs["error_message"])

    def test_refresh_store_gsc_data_low_signal_rows_mark_warning_without_db_delete(self):
        with (
            patch(
                "app.fulcrum.merchant_setup.get_store_integration",
                return_value={
                    "selected_resource_id": "https://www.hotels4humanity.com/",
                    "selected_resource_label": "https://www.hotels4humanity.com/",
                    "auth_payload": {"token": "token"},
                },
            ),
            patch("app.fulcrum.merchant_setup._credential_payload_to_credentials", return_value=object()),
            patch(
                "app.fulcrum.merchant_setup.build",
                return_value=_FakeSearchConsoleService(
                    [
                        {
                            "rows": [
                                {
                                    "keys": ["2026-04-01", "https://www.hotels4humanity.com/towels/", "hotel towels"],
                                    "clicks": 0,
                                    "impressions": 1,
                                    "ctr": 0.0,
                                    "position": 80.0,
                                }
                            ]
                        },
                        {"rows": []},
                    ]
                ),
            ),
            patch("app.fulcrum.merchant_setup.get_pg_conn", side_effect=AssertionError("old rows should not be deleted")),
            patch("app.fulcrum.merchant_setup._upsert_store_integration", return_value={}) as upsert_mock,
        ):
            result = merchant_setup.refresh_store_gsc_data("99oa2tso")

        self.assertEqual(result["status"], "warning")
        self.assertEqual(result["row_count"], 0)
        self.assertEqual(result["source_row_count"], 1)
        self.assertEqual(result["filtered_low_signal_row_count"], 1)
        kwargs = upsert_mock.call_args.kwargs
        self.assertEqual(kwargs["configuration_status"], "sync_warning")
        self.assertIn("storage filter", kwargs["error_message"])

    def test_refresh_store_gsc_data_replaces_existing_data_in_one_transaction(self):
        cursor = _FakeCursor()
        conn = _FakeConnection(cursor)
        responses = [
            {
                "rows": [
                    {
                        "keys": ["2026-04-01", "https://www.hotels4humanity.com/towels/", "hotel towels"],
                        "clicks": 3,
                        "impressions": 20,
                        "ctr": 0.15,
                        "position": 4.2,
                    }
                ]
            },
            {"rows": []},
        ]
        with (
            patch(
                "app.fulcrum.merchant_setup.get_store_integration",
                return_value={
                    "selected_resource_id": "https://www.hotels4humanity.com/",
                    "selected_resource_label": "https://www.hotels4humanity.com/",
                    "auth_payload": {"token": "token"},
                },
            ),
            patch("app.fulcrum.merchant_setup._credential_payload_to_credentials", return_value=object()),
            patch("app.fulcrum.merchant_setup.build", return_value=_FakeSearchConsoleService(responses)),
            patch("app.fulcrum.merchant_setup.get_pg_conn", return_value=conn),
            patch("app.fulcrum.merchant_setup.execute_batch") as batch_mock,
            patch("app.fulcrum.merchant_setup._invalidate_store_metric_cache"),
            patch("app.fulcrum.merchant_setup._upsert_store_integration", return_value={}),
        ):
            result = merchant_setup.refresh_store_gsc_data("99oa2tso")

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["row_count"], 1)
        self.assertTrue(conn.committed)
        executed_sql = "\n".join(sql for sql, _ in cursor.executions)
        self.assertNotIn("CREATE TEMP TABLE temp_store_gsc_daily", executed_sql)
        self.assertIn("DELETE FROM app_runtime.store_gsc_daily", executed_sql)
        self.assertIn("INSERT INTO app_runtime.store_gsc_daily", batch_mock.call_args.args[1])
        batch_mock.assert_called_once()

    def test_get_store_publish_settings_bootstraps_with_publishing_disabled(self):
        cursor = _FakeCursor(
            {
                "store_hash": "99oa2tso",
                "publishing_enabled": False,
                "category_publishing_enabled": False,
                "metadata": {"merchant_publish_preference_set": False},
                "updated_at": None,
            }
        )
        conn = _FakeConnection(cursor)

        with patch("app.fulcrum.merchant_setup.get_pg_conn", return_value=conn):
            row = merchant_setup.get_store_publish_settings("99oa2tso")

        self.assertFalse(row["publishing_enabled"])
        self.assertFalse(row["category_publishing_enabled"])
        self.assertIn("VALUES (%s, FALSE, FALSE", cursor.executions[0][0])
        self.assertTrue(conn.committed)

    def test_get_store_publish_settings_migrates_legacy_rows_to_disabled_until_merchant_chooses(self):
        cursor = _FakeCursor(
            {
                "store_hash": "99oa2tso",
                "publishing_enabled": True,
                "category_publishing_enabled": False,
                "metadata": {},
                "updated_at": None,
            }
        )
        legacy_update_row = {
            "store_hash": "99oa2tso",
            "publishing_enabled": False,
            "category_publishing_enabled": False,
            "metadata": {"merchant_publish_preference_set": False},
            "updated_at": None,
        }
        cursor.fetchone = lambda: cursor.row if len(cursor.executions) == 1 else legacy_update_row
        conn = _FakeConnection(cursor)

        with patch("app.fulcrum.merchant_setup.get_pg_conn", return_value=conn):
            row = merchant_setup.get_store_publish_settings("99oa2tso")

        self.assertFalse(row["publishing_enabled"])
        self.assertEqual(len(cursor.executions), 2)
        self.assertIn("UPDATE app_runtime.store_publish_settings", cursor.executions[1][0])

    def test_build_setup_context_marks_store_ready_for_publishing(self):
        with (
            patch("app.fulcrum.merchant_setup.get_store_installation", return_value={"status": "active"}),
            patch(
                "app.fulcrum.merchant_setup.sync_bigcommerce_integration",
                return_value={"connection_status": "connected", "configuration_status": "ready"},
            ),
            patch("app.fulcrum.merchant_setup.get_store_integration", side_effect=[
                {"connection_status": "connected", "configuration_status": "ready", "selected_resource_label": "sc-domain:example.com"},
                {"connection_status": "connected", "configuration_status": "ready", "selected_resource_label": "Example GA4"},
            ]),
            patch(
                "app.fulcrum.merchant_setup.get_store_integration_data_summary",
                side_effect=[
                    {"row_count": 12, "has_data": True, "start_date": "2026-01-01", "end_date": "2026-01-31", "latest_sync_run": {}},
                    {"row_count": 34, "has_data": True, "start_date": "2026-01-01", "end_date": "2026-01-31", "latest_sync_run": {}},
                ],
            ),
            patch("app.fulcrum.merchant_setup.get_store_publish_settings", return_value={"publishing_enabled": True, "category_publishing_enabled": False}),
            patch("app.fulcrum.merchant_setup.get_store_readiness", return_value={"catalog_synced": True}),
            patch("app.fulcrum.merchant_setup.get_store_profile_summary", return_value={"profile_count": 12, "category_profile_count": 5}),
            patch("app.fulcrum.merchant_setup.evaluate_theme_verification", return_value={"verification_status": "ready", "summary": "Theme verification passed."}),
        ):
            context = merchant_setup.build_setup_context("99oa2tso")

        self.assertEqual(context["readiness_state"], "ready_for_publishing")
        self.assertEqual(context["readiness_label"], "Ready Set Published")
        self.assertTrue(context["setup_complete"])

    def test_build_setup_context_marks_store_needs_setup_when_required_items_are_missing(self):
        with (
            patch("app.fulcrum.merchant_setup.get_store_installation", return_value={}),
            patch(
                "app.fulcrum.merchant_setup.sync_bigcommerce_integration",
                return_value={"connection_status": "not_connected", "configuration_status": "not_configured"},
            ),
            patch("app.fulcrum.merchant_setup.get_store_integration", side_effect=[
                {"connection_status": "not_connected", "configuration_status": "not_configured"},
                {"connection_status": "connected", "configuration_status": "needs_configuration"},
            ]),
            patch(
                "app.fulcrum.merchant_setup.get_store_integration_data_summary",
                side_effect=[
                    {"row_count": 0, "has_data": False, "latest_sync_run": {}},
                    {"row_count": 0, "has_data": False, "latest_sync_run": {}},
                ],
            ),
            patch("app.fulcrum.merchant_setup.get_store_publish_settings", return_value={"publishing_enabled": False, "category_publishing_enabled": False}),
            patch("app.fulcrum.merchant_setup.get_store_readiness", return_value={"catalog_synced": False}),
            patch("app.fulcrum.merchant_setup.get_store_profile_summary", return_value={"profile_count": 0, "category_profile_count": 0}),
            patch(
                "app.fulcrum.merchant_setup.evaluate_theme_verification",
                return_value={"verification_status": "failed", "summary": "Theme verification is blocked."},
            ),
        ):
            context = merchant_setup.build_setup_context("99oa2tso")

        self.assertEqual(context["readiness_state"], "needs_setup")
        self.assertFalse(context["setup_complete"])
        self.assertEqual(context["checklist"][0]["label"], "BigCommerce install")

    def test_merchant_landing_path_routes_ready_stores_to_results(self):
        with patch("app.fulcrum.merchant_setup.build_setup_context", return_value={"setup_complete": True}):
            landing = merchant_setup.merchant_landing_path("99oa2tso")

        self.assertEqual(landing, "results")

    def test_complete_google_oauth_auto_selects_single_verified_resource(self):
        class _FakeCredentials:
            def to_json(self):
                return json.dumps({"token": "token"})

        class _FakeFlow:
            def __init__(self):
                self.credentials = _FakeCredentials()

            def fetch_token(self, authorization_response):
                self.authorization_response = authorization_response

        with (
            patch("app.fulcrum.merchant_setup._build_google_flow", return_value=_FakeFlow()),
            patch(
                "app.fulcrum.merchant_setup.list_search_console_properties",
                return_value=[
                    {
                        "id": "https://www.hotels4humanity.com/",
                        "label": "https://www.hotels4humanity.com/",
                        "site_url": "https://www.hotels4humanity.com/",
                        "default_uri": "https://www.hotels4humanity.com/",
                    }
                ],
            ),
            patch("app.fulcrum.merchant_setup._upsert_store_integration", return_value={"configuration_status": "needs_configuration"}),
            patch(
                "app.fulcrum.merchant_setup.select_google_resource",
                return_value={"status": "ok", "selected": {"id": "https://www.hotels4humanity.com/"}, "sync_result": {"row_count": 7}},
            ) as select_resource,
            patch("app.fulcrum.merchant_setup._storefront_hosts", return_value={"hotels4humanity.com"}),
        ):
            result = merchant_setup.complete_google_oauth(
                "gsc",
                store_hash="99oa2tso",
                state="state-123",
                authorization_response="https://fulcrum.hotels4humanity.com/fulcrum/integrations/gsc/callback?state=state-123&code=test",
            )

        self.assertEqual((result.get("auto_selected") or {}).get("id"), "https://www.hotels4humanity.com/")
        self.assertEqual((result.get("selection_result") or {}).get("status"), "ok")
        select_resource.assert_called_once()

    def test_complete_google_oauth_keeps_connection_when_auto_sync_fails(self):
        class _FakeCredentials:
            def to_json(self):
                return json.dumps({"token": "token"})

        class _FakeFlow:
            def __init__(self):
                self.credentials = _FakeCredentials()

            def fetch_token(self, authorization_response):
                self.authorization_response = authorization_response

        with (
            patch("app.fulcrum.merchant_setup._build_google_flow", return_value=_FakeFlow()),
            patch(
                "app.fulcrum.merchant_setup.list_search_console_properties",
                return_value=[
                    {
                        "id": "https://www.hotels4humanity.com/",
                        "label": "https://www.hotels4humanity.com/",
                        "site_url": "https://www.hotels4humanity.com/",
                        "default_uri": "https://www.hotels4humanity.com/",
                    }
                ],
            ),
            patch("app.fulcrum.merchant_setup._upsert_store_integration", return_value={"configuration_status": "needs_configuration"}),
            patch(
                "app.fulcrum.merchant_setup.select_google_resource",
                side_effect=TimeoutError("sync timed out"),
            ),
            patch("app.fulcrum.merchant_setup._storefront_hosts", return_value={"hotels4humanity.com"}),
        ):
            result = merchant_setup.complete_google_oauth(
                "gsc",
                store_hash="99oa2tso",
                state="state-123",
                authorization_response="https://fulcrum.hotels4humanity.com/fulcrum/integrations/gsc/callback?state=state-123&code=test",
            )

        self.assertEqual((result.get("auto_selected") or {}).get("id"), "https://www.hotels4humanity.com/")
        self.assertEqual((result.get("selection_result") or {}).get("status"), "warning")
        self.assertIn("connected", (result.get("selection_result") or {}).get("reason", ""))

    def test_complete_google_oauth_keeps_connection_when_resource_listing_fails(self):
        class _FakeCredentials:
            def to_json(self):
                return json.dumps({"token": "token"})

        class _FakeFlow:
            def __init__(self):
                self.credentials = _FakeCredentials()

            def fetch_token(self, authorization_response):
                self.authorization_response = authorization_response

        with (
            patch("app.fulcrum.merchant_setup._build_google_flow", return_value=_FakeFlow()),
            patch("app.fulcrum.merchant_setup.list_search_console_properties", side_effect=TimeoutError("list timed out")),
            patch("app.fulcrum.merchant_setup._upsert_store_integration", return_value={"configuration_status": "needs_configuration"}) as upsert,
        ):
            result = merchant_setup.complete_google_oauth(
                "gsc",
                store_hash="99oa2tso",
                state="state-123",
                authorization_response="https://fulcrum.hotels4humanity.com/fulcrum/integrations/gsc/callback?state=state-123&code=test",
            )

        self.assertEqual((result.get("selection_result") or {}).get("status"), "warning")
        self.assertIn("could not be listed", (result.get("selection_result") or {}).get("reason", ""))
        self.assertEqual(upsert.call_count, 1)

    def test_select_google_resource_saves_selection_and_queues_background_sync(self):
        integration = {"auth_payload": {"token": "token"}}
        options = [
            {
                "id": "https://www.hotels4humanity.com/",
                "label": "https://www.hotels4humanity.com/",
                "site_url": "https://www.hotels4humanity.com/",
                "default_uri": "https://www.hotels4humanity.com/",
            }
        ]
        with (
            patch("app.fulcrum.merchant_setup.get_store_integration", return_value=integration),
            patch("app.fulcrum.merchant_setup.list_search_console_properties", return_value=options),
            patch("app.fulcrum.merchant_setup._storefront_hosts", return_value={"hotels4humanity.com"}),
            patch("app.fulcrum.merchant_setup._upsert_store_integration", return_value={"configuration_status": "syncing"}),
            patch(
                "app.fulcrum.merchant_setup.enqueue_integration_sync",
                return_value={"status": "queued", "sync_run_id": 123, "row_count": 0},
            ) as enqueue_sync,
        ):
            result = merchant_setup.select_google_resource(
                "99oa2tso",
                integration_key="gsc",
                selected_resource_id="https://www.hotels4humanity.com/",
            )

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["sync_result"]["status"], "queued")
        self.assertEqual(result["sync_result"]["sync_run_id"], 123)
        enqueue_sync.assert_called_once()

    def test_theme_verification_uses_persisted_readiness_when_templates_are_not_local(self):
        with (
            patch("app.fulcrum.merchant_setup.get_store_publish_settings", return_value={"category_publishing_enabled": True}),
            patch(
                "app.fulcrum.merchant_setup.get_store_readiness",
                return_value={
                    "theme_hook_ready": True,
                    "metadata": {"category_theme_hook_present": True},
                },
            ),
            patch("app.fulcrum.merchant_setup._record_theme_verification", return_value={"verification_status": "ready"}) as record,
            patch.object(merchant_setup.Config, "FULCRUM_THEME_PRODUCT_TEMPLATE", "Z:/missing/product.html"),
            patch.object(merchant_setup.Config, "FULCRUM_THEME_CATEGORY_TEMPLATE", "Z:/missing/category.html"),
        ):
            result = merchant_setup.evaluate_theme_verification("99oa2tso", persist=True)

        self.assertEqual(result["verification_status"], "ready")
        self.assertEqual(result["summary"], "Theme verification passed.")
        self.assertTrue(result["details"]["product_hook_ready"])
        self.assertTrue(result["details"]["category_hook_ready"])
        record.assert_called_once()


if __name__ == "__main__":
    unittest.main()
