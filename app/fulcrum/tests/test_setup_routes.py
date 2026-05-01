import sys
import unittest

import requests
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
APP_ROOT = ROOT / "MDMH4H"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from app.fulcrum import create_fulcrum_app
from app.fulcrum.config import load_config


class RouteAuthoritySetupRouteTests(unittest.TestCase):
    def setUp(self):
        self.app = create_fulcrum_app(load_config["testing"])
        self.client = self.app.test_client()

    def test_merchant_home_redirects_to_setup_when_incomplete(self):
        with patch("app.fulcrum.routes.apply_runtime_schema"), patch("app.fulcrum.routes._require_store_allowed"), patch("app.fulcrum.routes.merchant_landing_path", return_value="setup"):
            response = self.client.get("/fulcrum/?store_hash=99oa2tso")
        self.assertEqual(response.status_code, 302)
        self.assertIn("/fulcrum/setup?store_hash=99oa2tso", response.headers["Location"])

    def test_merchant_home_redirects_to_results_when_setup_is_complete(self):
        with patch("app.fulcrum.routes.apply_runtime_schema"), patch("app.fulcrum.routes._require_store_allowed"), patch("app.fulcrum.routes.merchant_landing_path", return_value="results"):
            response = self.client.get("/fulcrum/?store_hash=99oa2tso")
        self.assertEqual(response.status_code, 302)
        self.assertIn("/fulcrum/results?store_hash=99oa2tso", response.headers["Location"])

    def test_auth_callback_redirects_into_install_complete_flow(self):
        with patch("app.fulcrum.routes.apply_runtime_schema"), patch("app.fulcrum.routes.exchange_auth_code", return_value={"context": "stores/99oa2tso", "access_token": "token", "scope": "scope", "user": {"id": 7, "email": "merchant@example.com"}}), patch("app.fulcrum.routes._require_store_allowed"), patch("app.fulcrum.routes.merge_store_installation_metadata") as merge_mock, patch("app.fulcrum.routes.upsert_store_installation") as upsert_mock, patch("app.fulcrum.routes.sync_bigcommerce_integration"):
            response = self.client.get("/fulcrum/auth?code=test-code&context=stores/99oa2tso")
        self.assertEqual(response.status_code, 302)
        self.assertIn("/fulcrum/installed?store_hash=99oa2tso", response.headers["Location"])
        self.assertTrue(merge_mock.called)
        metadata = upsert_mock.call_args.kwargs["metadata"]
        self.assertFalse(metadata["auth_fallback"])
        self.assertIsNone(metadata["auth_error_status"])
        self.assertEqual(metadata["last_auth_callback_outcome"], "success")

    def test_auth_callback_fallback_records_upstream_error_details(self):
        error = requests.HTTPError("422 Client Error: Unprocessable Content for url: https://login.bigcommerce.com/oauth2/token")
        error.response = type("Resp", (), {"status_code": 422, "text": '{"status":422,"title":"Invalid scope"}'})()
        with patch("app.fulcrum.routes.apply_runtime_schema"), patch("app.fulcrum.routes.exchange_auth_code", side_effect=error), patch("app.fulcrum.routes._resolve_store_token", return_value="stored-token"), patch("app.fulcrum.routes._require_store_allowed"), patch("app.fulcrum.routes.merge_store_installation_metadata") as merge_mock, patch("app.fulcrum.routes.upsert_store_installation") as upsert_mock, patch("app.fulcrum.routes.sync_bigcommerce_integration"):
            response = self.client.get("/fulcrum/auth?code=test-code&scope=scope&context=stores/99oa2tso")
        self.assertEqual(response.status_code, 302)
        self.assertIn("/fulcrum/installed?store_hash=99oa2tso", response.headers["Location"])
        self.assertTrue(merge_mock.called)
        metadata = upsert_mock.call_args.kwargs["metadata"]
        self.assertTrue(metadata["auth_fallback"])
        self.assertEqual(metadata["auth_error_type"], "HTTPError")
        self.assertEqual(metadata["auth_error_status"], 422)
        self.assertIn("Unprocessable Content", metadata["auth_error_message"])
        self.assertIn("Invalid scope", metadata["auth_error_body"])
        self.assertEqual(metadata["last_auth_callback_outcome"], "fallback_existing_token")

    def test_load_callback_redirects_to_merchant_landing_after_install(self):
        with patch("app.fulcrum.routes.apply_runtime_schema"), patch("app.fulcrum.routes.decode_signed_payload", return_value={"context": "stores/99oa2tso", "scope": "scope", "user": {"id": 7, "email": "merchant@example.com"}}), patch("app.fulcrum.routes._require_store_allowed"), patch("app.fulcrum.routes.upsert_store_installation") as upsert_mock, patch("app.fulcrum.routes.sync_bigcommerce_integration"), patch("app.fulcrum.routes.merchant_landing_path", return_value="setup"):
            response = self.client.get("/fulcrum/load?signed_payload_jwt=test")
        self.assertEqual(response.status_code, 302)
        self.assertIn("/fulcrum/setup?store_hash=99oa2tso", response.headers["Location"])
        metadata = upsert_mock.call_args.kwargs["metadata"]
        self.assertEqual(metadata["last_load_callback_outcome"], "success")

    def test_install_complete_page_renders(self):
        with patch("app.fulcrum.routes.apply_runtime_schema"), patch("app.fulcrum.routes._require_store_allowed"):
            response = self.client.get("/fulcrum/installed?store_hash=99oa2tso")
        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Route Authority is connected.", body)
        self.assertIn("Open setup", body)
        self.assertIn("Terms of Service", body)

    def test_uninstall_callback_clears_store_data_and_returns_cleanup_summary(self):
        with patch("app.fulcrum.routes.apply_runtime_schema"), patch("app.fulcrum.routes.mark_store_uninstalled"), patch("app.fulcrum.routes.purge_store_data_on_uninstall", return_value={"store_gsc_daily_deleted": 12, "store_ga4_pages_daily_deleted": 7}), patch("app.fulcrum.routes.sync_bigcommerce_integration"):
            response = self.client.post("/fulcrum/uninstall", data={"context": "stores/99oa2tso"})
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["store_hash"], "99oa2tso")
        self.assertEqual(payload["cleanup"]["store_gsc_daily_deleted"], 12)
        self.assertEqual(payload["cleanup"]["store_ga4_pages_daily_deleted"], 7)

    def test_oauth_callback_authorization_response_uses_configured_https_url(self):
        from app.fulcrum.routes import _oauth_callback_authorization_response
        with patch("app.fulcrum.routes.Config.FULCRUM_GSC_OAUTH_CALLBACK_URL", "https://fulcrum.hotels4humanity.com/fulcrum/integrations/gsc/callback"):
            with self.app.test_request_context("/fulcrum/integrations/gsc/callback?state=state-123&code=test-code", base_url="http://fulcrum.hotels4humanity.com"):
                value = _oauth_callback_authorization_response("gsc")
        self.assertEqual(value, "https://fulcrum.hotels4humanity.com/fulcrum/integrations/gsc/callback?state=state-123&code=test-code")

    def test_merchant_redirect_only_pages_go_to_setup_sections(self):
        cases = {
            "/fulcrum/connections?store_hash=99oa2tso": "#connections",
            "/fulcrum/store-setup?store_hash=99oa2tso": "#store-checks",
            "/fulcrum/settings?store_hash=99oa2tso": "#publishing-settings",
        }
        with patch("app.fulcrum.routes.apply_runtime_schema"), patch("app.fulcrum.routes._require_store_allowed"):
            for path, anchor in cases.items():
                with self.subTest(path=path):
                    response = self.client.get(path)
                    self.assertEqual(response.status_code, 302)
                    self.assertIn(f"/fulcrum/setup?store_hash=99oa2tso{anchor}", response.headers["Location"])

    def test_settings_redirect_keeps_no_store_cache_headers(self):
        with patch("app.fulcrum.routes.apply_runtime_schema"), patch("app.fulcrum.routes._require_store_allowed"):
            response = self.client.get("/fulcrum/settings?store_hash=99oa2tso")
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers.get("Cache-Control"), "no-store, no-cache, must-revalidate, max-age=0")
        self.assertEqual(response.headers.get("Pragma"), "no-cache")
        self.assertEqual(response.headers.get("Expires"), "0")
        self.assertIn("#publishing-settings", response.headers["Location"])

    def test_setup_page_renders_merged_sections_and_trimmed_nav(self):
        setup_context = {
            "store_hash": "99oa2tso",
            "installation": {"status": "active"},
            "integrations": {
                "bigcommerce": {"status_label": "Ready", "display_name": "BigCommerce", "metadata": {}},
                "gsc": {"status_label": "Needs configuration", "display_name": "Search Console", "selected_resource_id": "", "metadata": {"available_resources": [{"id": "https://www.hotels4humanity.com/", "label": "https://www.hotels4humanity.com/"}]},},
                "ga4": {"status_label": "Needs configuration", "display_name": "Google Analytics 4", "selected_resource_id": "", "metadata": {"available_resources": [{"id": "properties/1234", "label": "Hotels for Humanity - GA4", "account_name": "Hotels for Humanity"}]},},
            },
            "publish_settings": {"publishing_enabled": True, "category_publishing_enabled": False},
            "theme_status": {"verification_status": "blocked", "summary": "Theme verification needs attention.", "details": {"missing": "internal_links_html", "next_action": "Run theme check."}, "failure_classification": "automatic"},
            "readiness": {},
            "profile_summary": {"profile_count": 28, "category_profile_count": 7, "storefront_site_count": 1},
            "checklist": [
                {"key": "bigcommerce_install", "label": "BigCommerce install", "complete": True, "status_label": "Ready", "detail": "Store identity and install context are active."},
                {"key": "search_console", "label": "Search Console", "complete": False, "status_label": "Needs configuration", "detail": "Connect and select the correct Search Console property."},
                {"key": "ga4", "label": "GA4", "complete": False, "status_label": "Not connected", "detail": "Connect and select the correct GA4 property."},
                {"key": "catalog_sync", "label": "Catalog sync", "complete": True, "status_label": "Ready", "detail": "28 products and 7 categories synced."},
                {"key": "theme_verification", "label": "Theme verification", "complete": False, "status_label": "Needs check", "detail": "Theme verification needs attention."},
                {"key": "readiness", "label": "Readiness", "complete": False, "status_label": "Needs setup", "detail": "One or more required setup items are incomplete."},
            ],
            "readiness_state": "needs_setup",
            "readiness_label": "Needs setup",
            "readiness_detail": "One or more required setup items are incomplete.",
            "setup_complete": False,
        }
        dashboard_context = {"review_bucket_requests": [{"representative_query": "hotel towels", "source_url": "https://www.hotels4humanity.com/towels/", "target_url": "https://www.hotels4humanity.com/bath-towels/", "review_status_label": "Waiting for support investigation", "applied_status_label": "No live block changes recorded"}], "review_bucket_count": 1, "latest_gate_run_id": 42}
        with patch("app.fulcrum.routes.apply_runtime_schema"), patch("app.fulcrum.routes._require_store_allowed"), patch("app.fulcrum.routes.build_setup_context", return_value=setup_context), patch("app.fulcrum.routes.get_dashboard_context", return_value=dashboard_context):
            response = self.client.get("/fulcrum/setup?store_hash=99oa2tso")
        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        for expected in [
            "Connect required data sources",
            "Catalog sync, theme verification, and readiness",
            "Enable publishing",
            "Connect Search Console",
            "Connect GA4",
        ]:
            self.assertIn(expected, body)
        for old_link in ['href="/fulcrum/connections?store_hash=99oa2tso"', 'href="/fulcrum/store-setup?store_hash=99oa2tso"', 'href="/fulcrum/settings?store_hash=99oa2tso"']:
            self.assertNotIn(old_link, body)
        self.assertIn('href="/fulcrum/review?store_hash=99oa2tso"', body)
        self.assertIn('Terms of service', body)
        self.assertIn('Open guide', body)
        self.assertIn('href="/fulcrum/guide?store_hash=99oa2tso"', body)
        self.assertIn('Open results', body)
        self.assertNotIn('Review results', body)
        self.assertEqual(body.count('Support contact'), 0)

    def test_review_page_renders_standalone_queue(self):
        dashboard_context = {
            "store_hash": "99oa2tso",
            "review_bucket_requests": [{
                "request_id": 7,
                "representative_query": "hotel towels",
                "source_name": "Current towels page",
                "source_url": "https://www.hotels4humanity.com/towels/",
                "target_name": "Bath towels",
                "target_url": "https://www.hotels4humanity.com/bath-towels/",
                "review_status_label": "Waiting for support investigation",
                "applied_status_label": "No live block changes recorded",
                "audit_status": "queued",
                "live_block_paused": False,
            }],
            "review_bucket_count": 1,
            "latest_gate_run_id": 42,
            "review_bucket_summary": {
                "open_count": 1,
                "audited_count": 0,
                "paused_live_blocks": 0,
            },
        }
        with patch("app.fulcrum.routes.apply_runtime_schema"), patch("app.fulcrum.routes._require_store_allowed"), patch("app.fulcrum.routes.get_dashboard_context", return_value=dashboard_context):
            response = self.client.get("/fulcrum/review?store_hash=99oa2tso")
        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Merchant review queue", body)
        self.assertIn("Results you send to review are removed from the Results workflow", body)
        self.assertIn("hotel towels", body)
        self.assertIn("Waiting for support investigation", body)
        self.assertIn("Terms of service", body)

    def test_results_page_renders_public_positioning_and_terms_link(self):
        dashboard_context = {
            "store_hash": "99oa2tso",
            "publication_count": 12,
            "query_family_review": [],
            "review_bucket_count": 0,
            "latest_gate_run_id": 42,
            "results_report": {
                "raw_query_count": 12,
                "family_count": 4,
                "published_query_count": 9,
                "not_published_query_count": 3,
                "pass_published_query_count": 6,
                "pass_not_published_query_count": 3,
                "review_bucket_query_count": 0,
            },
            "gsc_performance_summary": {"takeaway": {}, "comparison_chart_rows": [], "metric_rows": []},
            "active_run": None,
            "review_bucket_requests": [],
        }
        with patch("app.fulcrum.routes.apply_runtime_schema"), patch("app.fulcrum.routes._require_store_allowed"), patch("app.fulcrum.routes.get_dashboard_context", return_value=dashboard_context):
            response = self.client.get("/fulcrum/results?store_hash=99oa2tso")
        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Route Authority for BigCommerce", body)
        self.assertNotIn("Route Authority self-serve alpha", body)
        self.assertIn("Terms of service", body)
        self.assertIn("Open guide", body)
        self.assertIn('href="/fulcrum/guide?store_hash=99oa2tso"', body)
        self.assertIn("Review queue <strong id=\"fulcrum-review-bucket-count\"", body)
        self.assertNotIn('href="/fulcrum/review?store_hash=99oa2tso">Review queue', body)

    def test_admin_developer_page_renders_marketplace_readiness(self):
        setup_context = {
            "store_hash": "99oa2tso",
            "installation": {"status": "active", "install_source": "load_callback", "owner_email": "support@hotels4humanity.com"},
            "integrations": {
                "bigcommerce": {"status_label": "Ready", "display_name": "BigCommerce"},
                "gsc": {"status_label": "Ready", "display_name": "Search Console", "selected_resource_label": "https://www.hotels4humanity.com/"},
                "ga4": {"status_label": "Ready", "display_name": "GA4", "selected_resource_label": "Hotels4Humanity GA4"},
            },
            "publish_settings": {"publishing_enabled": True, "category_publishing_enabled": False},
            "theme_status": {"verification_status": "ready", "summary": "Theme hook present."},
            "readiness": {},
            "profile_summary": {},
            "checklist": [],
            "readiness_state": "ready_for_publishing",
            "readiness_label": "Ready Set Published",
            "readiness_detail": "Setup is complete and publishing can run for this store.",
            "setup_complete": True,
        }
        dashboard_context = {"review_bucket_requests": [], "review_bucket_count": 0, "latest_gate_run_id": 42}
        marketplace_context = {
            "callback_urls": {
                "auth": "https://fulcrum.hotels4humanity.com/fulcrum/auth",
                "load": "https://fulcrum.hotels4humanity.com/fulcrum/load",
                "uninstall": "https://fulcrum.hotels4humanity.com/fulcrum/uninstall",
                "remove_user": "https://fulcrum.hotels4humanity.com/fulcrum/remove-user",
            },
            "google_oauth_configured": True,
            "developer_callbacks": {
                "gsc_callback": "https://fulcrum.hotels4humanity.com/fulcrum/integrations/gsc/callback",
                "ga4_callback": "https://fulcrum.hotels4humanity.com/fulcrum/integrations/ga4/callback",
                "privacy_policy": "https://fulcrum.hotels4humanity.com/fulcrum/privacy",
                "support_url": "https://fulcrum.hotels4humanity.com/fulcrum/support",
                "terms_url": "https://fulcrum.hotels4humanity.com/fulcrum/terms",
            },
            "terms_url": "https://fulcrum.hotels4humanity.com/fulcrum/terms",
            "installation_status": "active",
            "install_source": "load_callback",
            "installed_at": "2026-04-07 17:58:05 MST",
            "updated_at": "2026-04-22 10:40:31 MST",
            "owner_email": "support@hotels4humanity.com",
            "auth_fallback": True,
            "auth_error_type": "HTTPError",
            "marketplace_readiness": [
                {"label": "BigCommerce auth callback", "status": "pass", "status_label": "Pass", "detail": "Configured on HTTPS for Developer Portal install flow.", "url": "https://fulcrum.hotels4humanity.com/fulcrum/auth"},
                {"label": "Terms of service page", "status": "fail", "status_label": "Fail", "detail": "Returned HTTP 404.", "url": "https://fulcrum.hotels4humanity.com/fulcrum/terms"},
                {"label": "Install auth path", "status": "warning", "status_label": "Review", "detail": "Fallback token path was used previously (HTTPError). Re-test a clean install before submission.", "url": ""},
            ],
            "runtime_diagnostics": {
                "app_base_url": "https://fulcrum.hotels4humanity.com",
                "worker_host": "127.0.0.1",
                "worker_port": "5093",
                "flask_env": "development",
                "database_label": "DATABASE_URL",
                "scheduler_enabled": False,
                "embedded_scheduler_enabled": False,
            },
            "readiness_snapshot": {
                "checks": {
                    "gsc": {
                        "ready": True,
                        "status": "Ready",
                        "connection_status": "connected",
                        "configuration_status": "ready",
                        "selected_resource_label": "https://www.hotels4humanity.com/",
                        "selected_resource_id": "https://www.hotels4humanity.com/",
                        "row_count": 12,
                        "date_range": ["2026-01-01", "2026-01-31"],
                        "latest_sync_run": {"sync_run_id": 7, "status": "succeeded"},
                        "last_error": "",
                    },
                    "ga4": {
                        "ready": True,
                        "status": "Ready",
                        "connection_status": "connected",
                        "configuration_status": "ready",
                        "selected_resource_label": "Hotels4Humanity GA4",
                        "selected_resource_id": "properties/1234",
                        "row_count": 34,
                        "date_range": ["2026-01-01", "2026-01-31"],
                        "latest_sync_run": {"sync_run_id": 8, "status": "succeeded"},
                        "last_error": "",
                    },
                },
            },
        }
        with patch("app.fulcrum.routes.apply_runtime_schema"), patch("app.fulcrum.routes._require_store_allowed"), patch("app.fulcrum.routes.build_setup_context", return_value=setup_context), patch("app.fulcrum.routes.get_dashboard_context", return_value=dashboard_context), patch("app.fulcrum.routes._build_marketplace_review_context", return_value=marketplace_context):
            response = self.client.get("/fulcrum/admin/developer?store_hash=99oa2tso")
        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Marketplace review readiness", body)
        self.assertIn("Installation record", body)
        self.assertIn("load_callback", body)
        self.assertIn("support@hotels4humanity.com", body)
        self.assertIn("Terms of service page", body)
        self.assertIn("Review", body)
        self.assertIn("Runtime", body)
        self.assertIn("Search Console data", body)

    def test_terms_page_renders(self):
        response = self.client.get("/fulcrum/terms?store_hash=99oa2tso")
        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Terms of Service", body)
        self.assertIn("Privacy Policy", body)

    def test_guide_page_renders(self):
        response = self.client.get("/fulcrum/guide?store_hash=99oa2tso")
        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Installation Guide", body)
        self.assertIn("Merchant install flow", body)
        self.assertIn("How to read Results", body)
        self.assertIn("Gate decision", body)
        self.assertIn("Routing decision", body)
        self.assertIn("Published live", body)
        self.assertIn("Terms of Service", body)

    def test_request_gate_review_redirects_to_review_page_when_requested(self):
        request_row = {
            "request_id": 11,
            "gate_record_id": 22,
            "representative_query": "hotel towels",
            "source_entity_id": 333,
            "source_entity_type": "product",
            "run_id": 42,
        }
        pause_result = {"live_block_paused": False, "review_reset_count": 0, "publication_count": 0}
        with patch("app.fulcrum.routes.apply_runtime_schema"), patch("app.fulcrum.routes._require_store_allowed"), patch("app.fulcrum.routes.request_query_gate_review", return_value=request_row), patch("app.fulcrum.routes.pause_source_for_review", return_value=pause_result), patch("app.fulcrum.routes.update_query_gate_review_request_metadata", return_value=request_row), patch("app.fulcrum.routes._queue_gate_review_audit_async"), patch("app.fulcrum.routes.invalidate_admin_metric_cache"), patch("app.fulcrum.routes.count_query_gate_review_requests", return_value=1):
            response = self.client.post("/fulcrum/gate/request-review", data={"store_hash": "99oa2tso", "gate_record_id": "22", "redirect_to": "review"})
        self.assertEqual(response.status_code, 302)
        self.assertIn("/fulcrum/review?store_hash=99oa2tso", response.headers["Location"])

    def test_request_gate_review_returns_bucket_entry_for_ajax_requests(self):
        request_row = {
            "request_id": 11,
            "gate_record_id": 22,
            "representative_query": "hotel towels",
            "source_name": "Current towels page",
            "source_url": "/towels/",
            "target_name": "Bath towels",
            "target_url": "/bath-towels/",
            "source_entity_id": 333,
            "source_entity_type": "product",
            "run_id": 42,
            "metadata": {"audit_status": "queued", "live_block_paused": True},
        }
        pause_result = {"live_block_paused": True, "review_reset_count": 1, "publication_count": 1}
        with patch("app.fulcrum.routes.apply_runtime_schema"), patch("app.fulcrum.routes._require_store_allowed"), patch("app.fulcrum.routes.request_query_gate_review", return_value=request_row), patch("app.fulcrum.routes.pause_source_for_review", return_value=pause_result), patch("app.fulcrum.routes.update_query_gate_review_request_metadata", return_value=request_row), patch("app.fulcrum.routes._queue_gate_review_audit_async"), patch("app.fulcrum.routes.invalidate_admin_metric_cache"), patch("app.fulcrum.routes.count_query_gate_review_requests", return_value=3):
            response = self.client.post("/fulcrum/gate/request-review", data={"store_hash": "99oa2tso", "gate_record_id": "22"}, headers={"X-Requested-With": "XMLHttpRequest", "Accept": "application/json"})
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["review_bucket_count"], 3)
        self.assertEqual(payload["review_bucket_entry"]["request_id"], 11)
        self.assertEqual(payload["review_bucket_entry"]["representative_query"], "hotel towels")
        self.assertEqual(payload["review_bucket_entry"]["review_status_label"], "Agent diagnosis pending")
        self.assertEqual(payload["review_bucket_entry"]["applied_status_label"], "Live block paused for review")

    def test_resolve_gate_review_request_live_fix_redirects_without_error(self):
        request_row = {
            "request_id": 54,
            "gate_record_id": 1261,
            "run_id": 42,
            "source_entity_id": 333,
            "source_entity_type": "product",
            "target_entity_type": "category",
            "target_entity_id": -100000222,
            "target_url": "/metal-luggage-racks/",
        }
        gate_review_map = {
            1261: {
                "recommended_action": "keep_winner",
                "metadata": {
                    "winner": {
                        "entity_type": "category",
                        "entity_id": -100000222,
                        "url": "/metal-luggage-racks/",
                    }
                },
            }
        }
        gate_row_map = {
            1261: {
                "suggested_target": {
                    "entity_type": "category",
                    "entity_id": -100000222,
                    "url": "/metal-luggage-racks/",
                }
            }
        }
        restore_result = {"live_block_restored": True, "publication_count": 1, "approved_candidate_count": 1}
        with patch("app.fulcrum.routes.apply_runtime_schema"), patch("app.fulcrum.routes._require_store_allowed"), patch("app.fulcrum.routes.get_query_gate_review_request_by_id", return_value=request_row), patch("app.fulcrum.routes._gate_review_map_for_ids", return_value=gate_review_map), patch("app.fulcrum.routes._query_gate_record_map_for_ids", return_value=gate_row_map), patch("app.fulcrum.routes.restore_source_after_review", return_value=restore_result) as restore_mock, patch("app.fulcrum.routes.resolve_query_gate_review_request") as resolve_mock, patch("app.fulcrum.routes.invalidate_admin_metric_cache"):
            with self.client.session_transaction() as sess:
                sess["email"] = "support@example.com"
            response = self.client.post("/fulcrum/gate/review-request/resolve", data={"store_hash": "99oa2tso", "request_id": "54", "restore_live": "1", "redirect_to": "admin"})
        self.assertEqual(response.status_code, 302)
        self.assertIn("/fulcrum/admin?store_hash=99oa2tso", response.headers["Location"])
        restore_mock.assert_called_once()
        metadata_updates = resolve_mock.call_args.kwargs["metadata_updates"]
        self.assertTrue(metadata_updates["live_approval_completed"])
        self.assertTrue(metadata_updates["live_block_restored"])
        self.assertEqual(metadata_updates["restore_publication_count"], 1)

    def test_resolve_gate_review_request_live_fix_keeps_request_pending_when_publish_fails(self):
        request_row = {
            "request_id": 54,
            "gate_record_id": 1261,
            "run_id": 42,
            "source_entity_id": 333,
            "source_entity_type": "product",
            "target_entity_type": "category",
            "target_entity_id": -100000222,
            "target_url": "/metal-luggage-racks/",
        }
        gate_review_map = {
            1261: {
                "recommended_action": "keep_winner",
                "metadata": {
                    "winner": {
                        "entity_type": "category",
                        "entity_id": -100000222,
                        "url": "/metal-luggage-racks/",
                    }
                },
            }
        }
        gate_row_map = {
            1261: {
                "suggested_target": {
                    "entity_type": "category",
                    "entity_id": -100000222,
                    "url": "/metal-luggage-racks/",
                }
            }
        }
        error = requests.HTTPError("403 Client Error: Forbidden for url: https://api.bigcommerce.com/test")
        error.response = type("Resp", (), {"status_code": 403, "url": "https://api.bigcommerce.com/test"})()
        error.request = type("Req", (), {"url": "https://api.bigcommerce.com/test"})()
        with patch("app.fulcrum.routes.apply_runtime_schema"), patch("app.fulcrum.routes._require_store_allowed"), patch("app.fulcrum.routes.get_query_gate_review_request_by_id", return_value=request_row), patch("app.fulcrum.routes._gate_review_map_for_ids", return_value=gate_review_map), patch("app.fulcrum.routes._query_gate_record_map_for_ids", return_value=gate_row_map), patch("app.fulcrum.routes.restore_source_after_review", side_effect=error), patch("app.fulcrum.routes.update_query_gate_review_request_metadata") as metadata_mock, patch("app.fulcrum.routes.resolve_query_gate_review_request") as resolve_mock, patch("app.fulcrum.routes.invalidate_admin_metric_cache"):
            with self.client.session_transaction() as sess:
                sess["email"] = "support@example.com"
            response = self.client.post("/fulcrum/gate/review-request/resolve", data={"store_hash": "99oa2tso", "request_id": "54", "restore_live": "1", "redirect_to": "admin"})
        self.assertEqual(response.status_code, 302)
        self.assertIn("/fulcrum/admin?store_hash=99oa2tso", response.headers["Location"])
        self.assertFalse(resolve_mock.called)
        metadata_updates = metadata_mock.call_args.kwargs["metadata_updates"]
        self.assertEqual(metadata_updates["restored_following_audit"], "keep_winner")
        self.assertEqual(metadata_updates["live_approval_error_status"], 403)
        self.assertIn("Forbidden", metadata_updates["live_approval_error"])

    def test_save_publish_settings_returns_json_for_ajax_requests(self):
        with patch("app.fulcrum.routes.apply_runtime_schema"), patch("app.fulcrum.routes._require_store_allowed"), patch("app.fulcrum.routes.upsert_store_publish_settings", return_value={"publishing_enabled": True, "category_publishing_enabled": False}), patch("app.fulcrum.routes.refresh_store_readiness"):
            response = self.client.post("/fulcrum/setup/publish-settings", data={"store_hash": "99oa2tso", "publishing_enabled": "on"}, headers={"X-Requested-With": "XMLHttpRequest", "Accept": "application/json"})
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["store_hash"], "99oa2tso")
        self.assertTrue(payload["publishing_enabled"])
        self.assertFalse(payload["category_publishing_enabled"])

    def test_save_publish_settings_redirects_to_setup_section_for_non_ajax_requests(self):
        with patch("app.fulcrum.routes.apply_runtime_schema"), patch("app.fulcrum.routes._require_store_allowed"), patch("app.fulcrum.routes.upsert_store_publish_settings", return_value={"publishing_enabled": True, "category_publishing_enabled": False}), patch("app.fulcrum.routes.refresh_store_readiness"):
            response = self.client.post("/fulcrum/setup/publish-settings", data={"store_hash": "99oa2tso", "publishing_enabled": "on"})
        self.assertEqual(response.status_code, 302)
        self.assertIn("/fulcrum/setup?store_hash=99oa2tso#publishing-settings", response.headers["Location"])

    def test_select_google_resource_returns_json_for_ajax_requests(self):
        result = {"status": "ok", "selected": {"id": "https://www.hotels4humanity.com/", "label": "https://www.hotels4humanity.com/"}, "sync_result": {"status": "queued", "sync_run_id": 99, "row_count": 0}}
        with patch("app.fulcrum.routes.apply_runtime_schema"), patch("app.fulcrum.routes._require_store_allowed"), patch("app.fulcrum.routes.select_google_resource", return_value=result):
            response = self.client.post("/fulcrum/integrations/gsc/select", data={"store_hash": "99oa2tso", "selected_resource_id": "https://www.hotels4humanity.com/"}, headers={"X-Requested-With": "XMLHttpRequest", "Accept": "application/json"})
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["integration_key"], "gsc")
        self.assertEqual(payload["sync_status"], "queued")
        self.assertEqual(payload["sync_run_id"], 99)

    def test_select_google_resource_redirects_to_setup_connections_for_non_ajax_requests(self):
        result = {"status": "ok", "selected": {"id": "https://www.hotels4humanity.com/", "label": "https://www.hotels4humanity.com/"}, "sync_result": {"row_count": 42}}
        with patch("app.fulcrum.routes.apply_runtime_schema"), patch("app.fulcrum.routes._require_store_allowed"), patch("app.fulcrum.routes.select_google_resource", return_value=result):
            response = self.client.post("/fulcrum/integrations/gsc/select", data={"store_hash": "99oa2tso", "selected_resource_id": "https://www.hotels4humanity.com/"})
        self.assertEqual(response.status_code, 302)
        self.assertIn("/fulcrum/setup?store_hash=99oa2tso#connections", response.headers["Location"])

    def test_integration_sync_retry_enqueues_background_sync(self):
        with patch("app.fulcrum.routes.apply_runtime_schema"), patch("app.fulcrum.routes._require_store_allowed"), patch("app.fulcrum.routes.enqueue_integration_sync", return_value={"status": "queued", "sync_run_id": 456}) as enqueue_mock:
            response = self.client.post("/fulcrum/integrations/gsc/sync", data={"store_hash": "99oa2tso"}, headers={"X-Requested-With": "XMLHttpRequest", "Accept": "application/json"})
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["status"], "queued")
        self.assertEqual(payload["sync_run_id"], 456)
        enqueue_mock.assert_called_once()

    def test_health_does_not_refresh_store_readiness(self):
        with patch("app.fulcrum.routes._require_store_allowed"), patch("app.fulcrum.routes.refresh_store_readiness") as refresh_mock:
            response = self.client.get("/fulcrum/health?store_hash=99oa2tso")
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["store_hash"], "99oa2tso")
        self.assertFalse(refresh_mock.called)

    def test_readiness_endpoint_returns_structured_snapshot(self):
        snapshot = {"status": "needs_attention", "store_hash": "99oa2tso", "checks": {"gsc": {"ready": False}}}
        with patch("app.fulcrum.routes.apply_runtime_schema"), patch("app.fulcrum.routes._require_store_allowed"), patch("app.fulcrum.routes.build_store_readiness_snapshot", return_value=snapshot):
            response = self.client.get("/fulcrum/readiness?store_hash=99oa2tso")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json(), snapshot)


if __name__ == "__main__":
    unittest.main()

