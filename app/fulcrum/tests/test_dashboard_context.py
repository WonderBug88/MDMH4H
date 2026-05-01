import sys
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock


ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

APP_ROOT = ROOT / "MDMH4H"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from app.fulcrum import dashboard_context


class FulcrumDashboardContextTests(unittest.TestCase):
    def test_admin_context_defaults_normalizes_paging_fields(self):
        context = dashboard_context.admin_context_defaults(
            pending_count=7,
            changed_route_search="  hookless  ",
            changed_route_sort="incorrect_first",
            changed_route_page=0,
            changed_route_page_size=0,
        )

        self.assertEqual(context["pending_count"], 7)
        self.assertEqual(context["changed_route_search"], "hookless")
        self.assertEqual(context["changed_route_page"], 1)
        self.assertEqual(context["changed_route_page_size"], 25)
        self.assertEqual(context["changed_route_total_count"], 0)

    def test_build_public_dashboard_data_decorates_rows_and_reviews(self):
        publications = [
            {
                "source_entity_type": "category",
                "source_entity_id": 5450,
                "source_url": "/hotel-shower-curtains/",
            }
        ]
        review_bucket_requests = [
            {
                "request_id": 14,
                "source_url": "/merchant-review-queue/",
                "target_url": "/merchant-review-target/",
                "metadata": {"audit_status": "ok", "live_block_paused": True},
                "created_at": datetime(2026, 4, 15, 12, 0, 0),
            }
        ]

        result = dashboard_context.build_public_dashboard_data(
            store_hash="99oa2tso",
            include_admin=False,
            latest_gate_run_id=44,
            latest_gate_cluster="bath",
            publications=publications,
            review_bucket_requests=review_bucket_requests,
            list_query_gate_records_fn=lambda store_hash, disposition=None, limit=None, run_id=None: [
                {
                    "gate_record_id": 8,
                    "disposition": "pass",
                    "source_entity_type": "category",
                    "current_page_type": "category",
                    "source_entity_id": 5450,
                    "source_url": "/hotel-shower-curtains/",
                    "suggested_target": {"url": "/hookless-shower-curtains/"},
                    "second_option": {"url": "/alternate-curtains/"},
                }
            ],
            annotate_query_gate_rows_with_suggestions_fn=lambda store_hash, rows, cluster=None: rows,
            attach_cached_query_gate_suggestions_fn=lambda rows: rows,
            extract_storefront_channel_id_fn=lambda *args, **kwargs: 7,
            build_storefront_url_fn=lambda store_hash, path, channel_id=None: f"https://example.com{path}",
            summarize_suggested_target_types_fn=lambda rows: {"category": len(rows)},
            publication_posting_label_fn=lambda row: "Published block live",
            format_timestamp_display_fn=lambda value: "display",
            format_relative_time_fn=lambda value: "relative",
            summarize_edge_case_requests_fn=lambda rows: {"count": len(rows)},
        )

        self.assertEqual(result["suggested_target_type_summary"]["category"], 1)
        self.assertEqual(result["query_family_review"][0]["source_live_url"], "https://example.com/hotel-shower-curtains/")
        self.assertEqual(result["query_family_review"][0]["suggested_target"]["live_url"], "https://example.com/hookless-shower-curtains/")
        self.assertEqual(result["query_family_review"][0]["second_option"]["live_url"], "https://example.com/alternate-curtains/")
        self.assertEqual(result["query_family_review"][0]["gate_status"], "pass")
        self.assertEqual(result["query_family_review"][0]["gate_status_label"], "Pass")
        self.assertEqual(result["query_family_review"][0]["publish_status"], "published")
        self.assertEqual(result["query_family_review"][0]["publish_reason_key"], "published")
        self.assertEqual(result["query_family_review"][0]["publish_status_label"], "Published")
        self.assertEqual(result["query_family_review"][0]["live_status_label"], "Published")
        self.assertEqual(result["pass_count"], 1)
        self.assertEqual(result["hold_count"], 0)
        self.assertEqual(result["reject_count"], 0)
        self.assertEqual(result["published_count"], 1)
        self.assertEqual(result["not_published_count"], 0)
        self.assertEqual(result["results_report"]["published_query_count"], 1)
        self.assertEqual(result["results_report"]["not_published_query_count"], 0)
        self.assertEqual(result["results_report"]["pass_published_query_count"], 1)
        self.assertEqual(result["results_report"]["pass_not_published_query_count"], 0)
        self.assertEqual(result["results_report"]["review_bucket_query_count"], 1)
        self.assertEqual(result["results_report"]["family_count"], 1)
        self.assertEqual(result["results_report"]["raw_query_count"], 1)
        self.assertEqual(result["publications"][0]["posting_label"], "Published block live")
        self.assertEqual(result["review_bucket_requests"][0]["review_status_label"], "Agent diagnosis ready for support")
        self.assertEqual(result["review_bucket_requests"][0]["applied_status_label"], "Live block paused for review")
        self.assertEqual(result["review_bucket_summary"]["count"], 1)



    def test_build_public_dashboard_data_matches_live_category_publication_by_bc_id_metadata(self):
        result = dashboard_context.build_public_dashboard_data(
            store_hash="99oa2tso",
            include_admin=False,
            latest_gate_run_id=44,
            latest_gate_cluster="bath",
            publications=[
                {
                    "source_entity_type": "category",
                    "source_entity_id": -1000005450,
                    "source_url": "/hotel-shower-curtains/",
                    "metadata": {"bc_category_id": 5450},
                }
            ],
            review_bucket_requests=[],
            list_query_gate_records_fn=lambda store_hash, disposition=None, limit=None, run_id=None: [
                {
                    "gate_record_id": 8,
                    "disposition": "pass",
                    "source_entity_type": "category",
                    "current_page_type": "category",
                    "source_entity_id": 5450,
                    "source_url": "/hotel-shower-curtains/",
                    "suggested_target": {"url": "/hookless-shower-curtains/"},
                    "second_option": {"url": "/alternate-curtains/"},
                }
            ],
            annotate_query_gate_rows_with_suggestions_fn=lambda store_hash, rows, cluster=None: rows,
            attach_cached_query_gate_suggestions_fn=lambda rows: rows,
            extract_storefront_channel_id_fn=lambda *args, **kwargs: 7,
            build_storefront_url_fn=lambda store_hash, path, channel_id=None: f"https://example.com{path}",
            summarize_suggested_target_types_fn=lambda rows: {"category": len(rows)},
            publication_posting_label_fn=lambda row: "Published block live",
            format_timestamp_display_fn=lambda value: "display",
            format_relative_time_fn=lambda value: "relative",
            summarize_edge_case_requests_fn=lambda rows: {"count": len(rows)},
        )

        self.assertTrue(result["query_family_review"][0]["is_live_result"])
        self.assertEqual(result["query_family_review"][0]["publish_status"], "published")
        self.assertEqual(result["query_family_review"][0]["publish_status_label"], "Published")

    def test_build_public_dashboard_data_counts_gate_and_publish_statuses(self):
        result = dashboard_context.build_public_dashboard_data(
            store_hash="99oa2tso",
            include_admin=False,
            latest_gate_run_id=44,
            latest_gate_cluster="bath",
            publications=[
                {
                    "source_entity_type": "category",
                    "source_entity_id": 5450,
                    "source_url": "/hotel-shower-curtains/",
                }
            ],
            review_bucket_requests=[],
            list_query_gate_records_fn=lambda store_hash, disposition=None, limit=None, run_id=None: [
                {
                    "gate_record_id": 8,
                    "disposition": "pass",
                    "source_entity_type": "category",
                    "current_page_type": "category",
                    "source_entity_id": 5450,
                    "source_url": "/hotel-shower-curtains/",
                    "suggested_target": {"entity_type": "category", "entity_id": 5455, "url": "/hookless-shower-curtains/"},
                },
                {
                    "gate_record_id": 9,
                    "disposition": "hold",
                    "source_entity_type": "category",
                    "current_page_type": "category",
                    "source_entity_id": 5451,
                    "source_url": "/luxury-bath-towels/",
                },
                {
                    "gate_record_id": 10,
                    "disposition": "reject",
                    "source_entity_type": "category",
                    "current_page_type": "category",
                    "source_entity_id": 5452,
                    "source_url": "/blankets/",
                },
            ],
            annotate_query_gate_rows_with_suggestions_fn=lambda store_hash, rows, cluster=None: rows,
            attach_cached_query_gate_suggestions_fn=lambda rows: rows,
            extract_storefront_channel_id_fn=lambda *args, **kwargs: 7,
            build_storefront_url_fn=lambda store_hash, path, channel_id=None: f"https://example.com{path}" if path else None,
            summarize_suggested_target_types_fn=lambda rows: {"rows": len(rows)},
            publication_posting_label_fn=lambda row: "Published block live",
            format_timestamp_display_fn=lambda value: "display",
            format_relative_time_fn=lambda value: "relative",
            summarize_edge_case_requests_fn=lambda rows: {"count": len(rows)},
        )

        self.assertEqual(result["pass_count"], 1)
        self.assertEqual(result["hold_count"], 1)
        self.assertEqual(result["reject_count"], 1)
        self.assertEqual(result["published_count"], 1)
        self.assertEqual(result["not_published_count"], 2)
        self.assertEqual(result["query_family_review"][0]["publish_status"], "published")
        self.assertEqual(result["query_family_review"][1]["publish_status"], "not_published")
        self.assertEqual(result["query_family_review"][2]["gate_status_label"], "Reject")

    def test_build_public_dashboard_data_builds_results_report_from_raw_query_variants(self):
        result = dashboard_context.build_public_dashboard_data(
            store_hash="99oa2tso",
            include_admin=False,
            latest_gate_run_id=44,
            latest_gate_cluster="bath",
            publications=[
                {
                    "source_entity_type": "category",
                    "source_entity_id": 5450,
                    "source_url": "/hotel-shower-curtains/",
                }
            ],
            review_bucket_requests=[
                {
                    "request_id": 41,
                    "source_entity_type": "category",
                    "source_entity_id": 999,
                    "source_url": "/review-source/",
                    "metadata": {
                        "query_variants": [
                            {"query": "review one"},
                            {"query": "review two"},
                        ]
                    },
                    "created_at": datetime(2026, 4, 15, 12, 0, 0),
                }
            ],
            list_query_gate_records_fn=lambda store_hash, disposition=None, limit=None, run_id=None: [
                {
                    "gate_record_id": 8,
                    "disposition": "pass",
                    "source_entity_type": "category",
                    "current_page_type": "category",
                    "source_entity_id": 5450,
                    "source_url": "/hotel-shower-curtains/",
                    "metadata": {
                        "query_variants": [
                            {"query": "published one"},
                            {"query": "published two"},
                            {"query": "published three"},
                        ]
                    },
                    "suggested_target": {"url": "/hookless-shower-curtains/"},
                },
                {
                    "gate_record_id": 9,
                    "disposition": "hold",
                    "source_entity_type": "category",
                    "current_page_type": "category",
                    "source_entity_id": 5451,
                    "source_url": "/luxury-bath-towels/",
                    "metadata": {
                        "query_variants": [
                            {"query": "not published one"},
                            {"query": "not published two"},
                        ]
                    },
                },
                {
                    "gate_record_id": 10,
                    "disposition": "pass",
                    "source_entity_type": "category",
                    "current_page_type": "category",
                    "source_entity_id": 5452,
                    "source_url": "/targeted-not-live/",
                    "metadata": {},
                    "suggested_target": {"url": "/targeted-page/"},
                },
            ],
            annotate_query_gate_rows_with_suggestions_fn=lambda store_hash, rows, cluster=None: rows,
            attach_cached_query_gate_suggestions_fn=lambda rows: rows,
            extract_storefront_channel_id_fn=lambda *args, **kwargs: 7,
            build_storefront_url_fn=lambda store_hash, path, channel_id=None: f"https://example.com{path}" if path else None,
            summarize_suggested_target_types_fn=lambda rows: {"rows": len(rows)},
            publication_posting_label_fn=lambda row: "Published block live",
            format_timestamp_display_fn=lambda value: "display",
            format_relative_time_fn=lambda value: "relative",
            summarize_edge_case_requests_fn=lambda rows: {"count": len(rows)},
        )

        self.assertEqual(result["query_family_review"][0]["raw_query_count"], 3)
        self.assertEqual(result["query_family_review"][1]["raw_query_count"], 2)
        self.assertEqual(result["query_family_review"][2]["raw_query_count"], 1)
        self.assertEqual(result["results_report"]["published_query_count"], 4)
        self.assertEqual(result["results_report"]["not_published_query_count"], 2)
        self.assertEqual(result["results_report"]["pass_published_query_count"], 3)
        self.assertEqual(result["results_report"]["pass_not_published_query_count"], 1)
        self.assertEqual(result["results_report"]["review_bucket_query_count"], 2)
        self.assertEqual(result["results_report"]["family_count"], 3)
        self.assertEqual(result["results_report"]["raw_query_count"], 6)

    def test_build_public_dashboard_data_counts_not_published_queries_from_hold_and_reject(self):
        result = dashboard_context.build_public_dashboard_data(
            store_hash="99oa2tso",
            include_admin=False,
            latest_gate_run_id=44,
            latest_gate_cluster="bath",
            publications=[],
            review_bucket_requests=[],
            list_query_gate_records_fn=lambda store_hash, disposition=None, limit=None, run_id=None: [
                {
                    "gate_record_id": 20,
                    "disposition": "pass",
                    "source_entity_type": "category",
                    "current_page_type": "category",
                    "source_entity_id": 600,
                    "source_url": "/pass-row/",
                    "metadata": {"query_variants": [{"query": "pass one"}, {"query": "pass two"}]},
                    "suggested_target": {"entity_type": "category", "entity_id": 650, "url": "/pass-target/"},
                },
                {
                    "gate_record_id": 21,
                    "disposition": "hold",
                    "source_entity_type": "category",
                    "current_page_type": "category",
                    "source_entity_id": 601,
                    "source_url": "/hold-row/",
                    "metadata": {"query_variants": [{"query": "hold one"}, {"query": "hold two"}, {"query": "hold three"}]},
                },
                {
                    "gate_record_id": 22,
                    "disposition": "reject",
                    "source_entity_type": "category",
                    "current_page_type": "category",
                    "source_entity_id": 602,
                    "source_url": "/reject-row/",
                    "metadata": {"query_variants": [{"query": "reject one"}]},
                },
            ],
            annotate_query_gate_rows_with_suggestions_fn=lambda store_hash, rows, cluster=None: rows,
            attach_cached_query_gate_suggestions_fn=lambda rows: rows,
            extract_storefront_channel_id_fn=lambda *args, **kwargs: 7,
            build_storefront_url_fn=lambda store_hash, path, channel_id=None: f"https://example.com{path}" if path else None,
            summarize_suggested_target_types_fn=lambda rows: {"rows": len(rows)},
            publication_posting_label_fn=lambda row: "Published block live",
            format_timestamp_display_fn=lambda value: "display",
            format_relative_time_fn=lambda value: "relative",
            summarize_edge_case_requests_fn=lambda rows: {"count": len(rows)},
        )

        self.assertEqual(result["results_report"]["published_query_count"], 2)
        self.assertEqual(result["results_report"]["not_published_query_count"], 4)
        self.assertEqual(result["results_report"]["raw_query_count"], 6)

    def test_build_public_dashboard_data_labels_same_page_winner(self):
        result = dashboard_context.build_public_dashboard_data(
            store_hash="99oa2tso",
            include_admin=False,
            latest_gate_run_id=44,
            latest_gate_cluster="bath",
            publications=[],
            review_bucket_requests=[],
            list_query_gate_records_fn=lambda store_hash, disposition=None, limit=None, run_id=None: [
                {
                    "gate_record_id": 8,
                    "disposition": "pass",
                    "source_entity_type": "category",
                    "current_page_type": "category",
                    "source_entity_id": 5450,
                    "source_url": "/hotel-shower-curtains/",
                    "suggested_target": {
                        "entity_type": "category",
                        "entity_id": 5450,
                        "url": "/hotel-shower-curtains/",
                    },
                }
            ],
            annotate_query_gate_rows_with_suggestions_fn=lambda store_hash, rows, cluster=None: rows,
            attach_cached_query_gate_suggestions_fn=lambda rows: rows,
            extract_storefront_channel_id_fn=lambda *args, **kwargs: 7,
            build_storefront_url_fn=lambda store_hash, path, channel_id=None: f"https://example.com{path}" if path else None,
            summarize_suggested_target_types_fn=lambda rows: {"rows": len(rows)},
            publication_posting_label_fn=lambda row: "Published block live",
            format_timestamp_display_fn=lambda value: "display",
            format_relative_time_fn=lambda value: "relative",
            summarize_edge_case_requests_fn=lambda rows: {"count": len(rows)},
        )

        row = result["query_family_review"][0]
        self.assertFalse(row["is_live_result"])
        self.assertTrue(row["is_same_page_winner"])
        self.assertEqual(row["publish_status"], "not_published")
        self.assertEqual(row["publish_reason_key"], "routing_same_page_winner")
        self.assertEqual(row["publish_status_label"], "Routing - Same Page Winner")
        self.assertEqual(row["live_status_label"], "Routing - Same Page Winner")

    def test_build_public_dashboard_data_labels_hold_same_page_as_gating_low_clarity(self):
        result = dashboard_context.build_public_dashboard_data(
            store_hash="99oa2tso",
            include_admin=False,
            latest_gate_run_id=44,
            latest_gate_cluster="bath",
            publications=[],
            review_bucket_requests=[],
            list_query_gate_records_fn=lambda store_hash, disposition=None, limit=None, run_id=None: [
                {
                    "gate_record_id": 9,
                    "disposition": "hold",
                    "source_entity_type": "product",
                    "current_page_type": "product",
                    "source_entity_id": 112351,
                    "source_url": "/oxford-reserve-luxury-hotel-spa-towels/",
                    "suggested_target": {
                        "entity_type": "product",
                        "entity_id": 112351,
                        "url": "/oxford-reserve-luxury-hotel-spa-towels/",
                        "is_current_page": True,
                    },
                }
            ],
            annotate_query_gate_rows_with_suggestions_fn=lambda store_hash, rows, cluster=None: rows,
            attach_cached_query_gate_suggestions_fn=lambda rows: rows,
            extract_storefront_channel_id_fn=lambda *args, **kwargs: 7,
            build_storefront_url_fn=lambda store_hash, path, channel_id=None: f"https://example.com{path}" if path else None,
            summarize_suggested_target_types_fn=lambda rows: {"rows": len(rows)},
            publication_posting_label_fn=lambda row: "Published block live",
            format_timestamp_display_fn=lambda value: "display",
            format_relative_time_fn=lambda value: "relative",
            summarize_edge_case_requests_fn=lambda rows: {"count": len(rows)},
        )

        row = result["query_family_review"][0]
        self.assertEqual(row["gate_status"], "hold")
        self.assertFalse(row["is_live_result"])
        self.assertTrue(row["is_same_page_winner"])
        self.assertEqual(row["publish_status"], "not_published")
        self.assertEqual(row["publish_reason_key"], "gating_low_clarity")
        self.assertEqual(row["publish_status_label"], "Gating - Low Clarity")
        self.assertEqual(row["live_status_label"], "Gating - Low Clarity")

    def test_build_public_dashboard_data_does_not_mark_hold_live_same_page_as_published(self):
        result = dashboard_context.build_public_dashboard_data(
            store_hash="99oa2tso",
            include_admin=False,
            latest_gate_run_id=44,
            latest_gate_cluster="bath",
            publications=[
                {
                    "source_entity_type": "category",
                    "source_entity_id": 5468,
                    "source_url": "/hotel-luggage-racks/",
                }
            ],
            review_bucket_requests=[],
            list_query_gate_records_fn=lambda store_hash, disposition=None, limit=None, run_id=None: [
                {
                    "gate_record_id": 16,
                    "disposition": "hold",
                    "source_entity_type": "category",
                    "current_page_type": "category",
                    "source_entity_id": 5468,
                    "source_url": "/hotel-luggage-racks/",
                    "suggested_target": {
                        "entity_type": "category",
                        "entity_id": 5468,
                        "url": "/hotel-luggage-racks/",
                        "is_current_page": True,
                    },
                }
            ],
            annotate_query_gate_rows_with_suggestions_fn=lambda store_hash, rows, cluster=None: rows,
            attach_cached_query_gate_suggestions_fn=lambda rows: rows,
            extract_storefront_channel_id_fn=lambda *args, **kwargs: 7,
            build_storefront_url_fn=lambda store_hash, path, channel_id=None: f"https://example.com{path}" if path else None,
            summarize_suggested_target_types_fn=lambda rows: {"rows": len(rows)},
            publication_posting_label_fn=lambda row: "Published block live",
            format_timestamp_display_fn=lambda value: "display",
            format_relative_time_fn=lambda value: "relative",
            summarize_edge_case_requests_fn=lambda rows: {"count": len(rows)},
        )

        row = result["query_family_review"][0]
        self.assertEqual(row["gate_status"], "hold")
        self.assertTrue(row["is_live_result"])
        self.assertTrue(row["is_same_page_winner"])
        self.assertEqual(row["publish_status"], "not_published")
        self.assertEqual(row["publish_reason_key"], "gating_low_clarity")
        self.assertEqual(row["publish_status_label"], "Gating - Low Clarity")

    def test_build_public_dashboard_data_does_not_mark_pass_live_same_page_as_published(self):
        result = dashboard_context.build_public_dashboard_data(
            store_hash="99oa2tso",
            include_admin=False,
            latest_gate_run_id=44,
            latest_gate_cluster="bath",
            publications=[
                {
                    "source_entity_type": "category",
                    "source_entity_id": 5450,
                    "source_url": "/hotel-shower-curtains/",
                }
            ],
            review_bucket_requests=[],
            list_query_gate_records_fn=lambda store_hash, disposition=None, limit=None, run_id=None: [
                {
                    "gate_record_id": 17,
                    "disposition": "pass",
                    "source_entity_type": "category",
                    "current_page_type": "category",
                    "source_entity_id": 5450,
                    "source_url": "/hotel-shower-curtains/",
                    "suggested_target": {
                        "entity_type": "category",
                        "entity_id": 5450,
                        "url": "/hotel-shower-curtains/",
                        "is_current_page": True,
                    },
                }
            ],
            annotate_query_gate_rows_with_suggestions_fn=lambda store_hash, rows, cluster=None: rows,
            attach_cached_query_gate_suggestions_fn=lambda rows: rows,
            extract_storefront_channel_id_fn=lambda *args, **kwargs: 7,
            build_storefront_url_fn=lambda store_hash, path, channel_id=None: f"https://example.com{path}" if path else None,
            summarize_suggested_target_types_fn=lambda rows: {"rows": len(rows)},
            publication_posting_label_fn=lambda row: "Published block live",
            format_timestamp_display_fn=lambda value: "display",
            format_relative_time_fn=lambda value: "relative",
            summarize_edge_case_requests_fn=lambda rows: {"count": len(rows)},
        )

        row = result["query_family_review"][0]
        self.assertEqual(row["gate_status"], "pass")
        self.assertTrue(row["is_live_result"])
        self.assertTrue(row["is_same_page_winner"])
        self.assertEqual(row["publish_status"], "not_published")
        self.assertEqual(row["publish_reason_key"], "routing_same_page_winner")
        self.assertEqual(row["publish_status_label"], "Routing - Same Page Winner")

    def test_build_public_dashboard_data_explains_pass_not_published_reasons(self):
        result = dashboard_context.build_public_dashboard_data(
            store_hash="99oa2tso",
            include_admin=False,
            latest_gate_run_id=44,
            latest_gate_cluster="bath",
            publications=[],
            review_bucket_requests=[
                {
                    "request_id": 41,
                    "gate_record_id": 999,
                    "source_entity_type": "product",
                    "source_entity_id": 200,
                    "source_url": "/blocked-product/",
                    "metadata": {},
                    "created_at": datetime(2026, 4, 15, 12, 0, 0),
                }
            ],
            category_publishing_enabled=False,
            list_query_gate_records_fn=lambda store_hash, disposition=None, limit=None, run_id=None: [
                {
                    "gate_record_id": 11,
                    "disposition": "pass",
                    "source_entity_type": "product",
                    "current_page_type": "product",
                    "source_entity_id": 100,
                    "source_url": "/awaiting-product/",
                    "suggested_target": {"entity_type": "product", "entity_id": 150, "url": "/target-product/"},
                },
                {
                    "gate_record_id": 12,
                    "disposition": "pass",
                    "source_entity_type": "product",
                    "current_page_type": "product",
                    "source_entity_id": 200,
                    "source_url": "/blocked-product/",
                    "suggested_target": {"entity_type": "product", "entity_id": 250, "url": "/blocked-target/"},
                },
                {
                    "gate_record_id": 13,
                    "disposition": "pass",
                    "source_entity_type": "category",
                    "current_page_type": "category",
                    "source_entity_id": 300,
                    "source_url": "/category-source/",
                    "suggested_target": {"entity_type": "category", "entity_id": 350, "url": "/category-target/"},
                },
                {
                    "gate_record_id": 14,
                    "disposition": "pass",
                    "source_entity_type": "content",
                    "current_page_type": "content",
                    "source_entity_id": 400,
                    "source_url": "/content-source/",
                    "suggested_target": {"entity_type": "content", "entity_id": 450, "url": "/content-target/"},
                },
            ],
            annotate_query_gate_rows_with_suggestions_fn=lambda store_hash, rows, cluster=None: rows,
            attach_cached_query_gate_suggestions_fn=lambda rows: rows,
            extract_storefront_channel_id_fn=lambda *args, **kwargs: 7,
            build_storefront_url_fn=lambda store_hash, path, channel_id=None: f"https://example.com{path}" if path else None,
            summarize_suggested_target_types_fn=lambda rows: {"rows": len(rows)},
            publication_posting_label_fn=lambda row: "Published block live",
            format_timestamp_display_fn=lambda value: "display",
            format_relative_time_fn=lambda value: "relative",
            summarize_edge_case_requests_fn=lambda rows: {"count": len(rows)},
        )

        labels = {row["gate_record_id"]: row["publish_status_label"] for row in result["query_family_review"]}
        reasons = {row["gate_record_id"]: row["publish_reason_key"] for row in result["query_family_review"]}
        self.assertEqual(labels[11], "Awaiting publish")
        self.assertEqual(labels[13], "Category publishing off")
        self.assertEqual(labels[14], "Source type not publishable")
        self.assertEqual(reasons[11], "awaiting_publish")
        self.assertEqual(reasons[13], "category_publishing_off")
        self.assertEqual(reasons[14], "source_type_not_publishable")
        self.assertNotIn(12, labels)
        self.assertNotIn(12, reasons)

    def test_build_public_dashboard_data_labels_top_ten_rank_hold(self):
        result = dashboard_context.build_public_dashboard_data(
            store_hash="99oa2tso",
            include_admin=False,
            latest_gate_run_id=44,
            latest_gate_cluster="bath",
            publications=[],
            review_bucket_requests=[],
            list_query_gate_records_fn=lambda store_hash, disposition=None, limit=None, run_id=None: [
                {
                    "gate_record_id": 15,
                    "disposition": "hold",
                    "reason_summary": "query already ranks in the top 10, so routing stays on hold by default; avg position 5.0; broad product family",
                    "source_entity_type": "category",
                    "current_page_type": "category",
                    "source_entity_id": 500,
                    "source_url": "/blankets/",
                    "suggested_target": {"entity_type": "category", "entity_id": 550, "url": "/hotel-duvet-insert-filled-blankets/"},
                },
            ],
            annotate_query_gate_rows_with_suggestions_fn=lambda store_hash, rows, cluster=None: rows,
            attach_cached_query_gate_suggestions_fn=lambda rows: rows,
            extract_storefront_channel_id_fn=lambda *args, **kwargs: 7,
            build_storefront_url_fn=lambda store_hash, path, channel_id=None: f"https://example.com{path}" if path else None,
            summarize_suggested_target_types_fn=lambda rows: {"rows": len(rows)},
            publication_posting_label_fn=lambda row: "Published block live",
            format_timestamp_display_fn=lambda value: "display",
            format_relative_time_fn=lambda value: "relative",
            summarize_edge_case_requests_fn=lambda rows: {"count": len(rows)},
        )

        row = result["query_family_review"][0]
        self.assertEqual(row["gate_status"], "hold")
        self.assertFalse(row["is_live_result"])
        self.assertFalse(row["is_same_page_winner"])
        self.assertEqual(row["publish_status"], "not_published")
        self.assertEqual(row["publish_reason_key"], "gating_top_ten")
        self.assertEqual(row["publish_status_label"], "Gating - Top-10")
        self.assertEqual(row["live_status_label"], "Gating - Top-10")

    def test_build_public_dashboard_data_does_not_treat_current_page_preservation_as_same_page_winner(self):
        result = dashboard_context.build_public_dashboard_data(
            store_hash="99oa2tso",
            include_admin=False,
            latest_gate_run_id=44,
            latest_gate_cluster="bath",
            publications=[
                {
                    "source_entity_type": "product",
                    "source_entity_id": 100726,
                    "source_url": "/1888-mills-best-gym-towels-for-sports-fitness/",
                }
            ],
            review_bucket_requests=[],
            list_query_gate_records_fn=lambda store_hash, disposition=None, limit=None, run_id=None: [
                {
                    "gate_record_id": 18,
                    "disposition": "hold",
                    "reason_summary": "Google already aligns this query to the current page, so routing stays on hold; avg position 10.3; broad product family",
                    "source_entity_type": "product",
                    "current_page_type": "product",
                    "source_entity_id": 100726,
                    "source_url": "/1888-mills-best-gym-towels-for-sports-fitness/",
                    "metadata": {
                        "current_page_preservation_guard": {
                            "active": True,
                            "preserves_head_term": True,
                            "preserves_leading_qualifiers": True,
                        }
                    },
                    "suggested_target": {"entity_type": "category", "entity_id": 5436, "url": "/gym-towels/"},
                },
            ],
            annotate_query_gate_rows_with_suggestions_fn=lambda store_hash, rows, cluster=None: rows,
            attach_cached_query_gate_suggestions_fn=lambda rows: rows,
            extract_storefront_channel_id_fn=lambda *args, **kwargs: 7,
            build_storefront_url_fn=lambda store_hash, path, channel_id=None: f"https://example.com{path}" if path else None,
            summarize_suggested_target_types_fn=lambda rows: {"rows": len(rows)},
            publication_posting_label_fn=lambda row: "Published block live",
            format_timestamp_display_fn=lambda value: "display",
            format_relative_time_fn=lambda value: "relative",
            summarize_edge_case_requests_fn=lambda rows: {"count": len(rows)},
        )

        row = result["query_family_review"][0]
        self.assertEqual(row["gate_status"], "hold")
        self.assertTrue(row["is_live_result"])
        self.assertFalse(row["is_same_page_winner"])
        self.assertEqual(row["publish_status"], "not_published")
        self.assertEqual(row["publish_reason_key"], "gating_low_clarity")
        self.assertEqual(row["publish_status_label"], "Gating - Low Clarity")
        self.assertEqual(row["live_status_label"], "Gating - Low Clarity")

    def test_build_public_dashboard_data_hides_rows_already_in_review_bucket(self):
        result = dashboard_context.build_public_dashboard_data(
            store_hash="99oa2tso",
            include_admin=False,
            latest_gate_run_id=44,
            latest_gate_cluster="bath",
            publications=[
                {
                    "source_entity_type": "category",
                    "source_entity_id": 0,
                    "source_url": "/show-this-row/",
                }
            ],
            review_bucket_requests=[
                {
                    "request_id": 14,
                    "gate_record_id": 8,
                    "metadata": {},
                    "created_at": datetime(2026, 4, 15, 12, 0, 0),
                }
            ],
            list_query_gate_records_fn=lambda store_hash, disposition=None, limit=None, run_id=None: [
                {"gate_record_id": 8, "representative_query": "hide this row", "source_url": "/hide-this-row/"},
                {"gate_record_id": 9, "representative_query": "show this row", "source_url": "/show-this-row/"},
            ],
            annotate_query_gate_rows_with_suggestions_fn=lambda store_hash, rows, cluster=None: rows,
            attach_cached_query_gate_suggestions_fn=lambda rows: rows,
            extract_storefront_channel_id_fn=lambda *args, **kwargs: 7,
            build_storefront_url_fn=lambda store_hash, path, channel_id=None: f"https://example.com{path}" if path else None,
            summarize_suggested_target_types_fn=lambda rows: {"rows": len(rows)},
            publication_posting_label_fn=lambda row: "Published block live",
            format_timestamp_display_fn=lambda value: "display",
            format_relative_time_fn=lambda value: "relative",
            summarize_edge_case_requests_fn=lambda rows: {"count": len(rows)},
        )

        self.assertEqual(len(result["query_family_review"]), 1)
        self.assertEqual(result["query_family_review"][0]["gate_record_id"], 9)

    def test_build_public_dashboard_data_hides_rows_for_sources_already_in_review_bucket(self):
        result = dashboard_context.build_public_dashboard_data(
            store_hash="99oa2tso",
            include_admin=False,
            latest_gate_run_id=44,
            latest_gate_cluster="bath",
            publications=[],
            review_bucket_requests=[
                {
                    "request_id": 14,
                    "gate_record_id": 8,
                    "source_entity_type": "category",
                    "source_entity_id": 24,
                    "source_url": "/spa-hotel-bath-robes/",
                    "metadata": {},
                    "created_at": datetime(2026, 4, 15, 12, 0, 0),
                }
            ],
            list_query_gate_records_fn=lambda store_hash, disposition=None, limit=None, run_id=None: [
                {"gate_record_id": 8, "representative_query": "luxury hotel bathrobes", "source_entity_type": "category", "current_page_type": "category", "source_entity_id": 24, "source_url": "/spa-hotel-bath-robes/"},
                {"gate_record_id": 9, "representative_query": "luxury hotel robes", "source_entity_type": "category", "current_page_type": "category", "source_entity_id": 24, "source_url": "/spa-hotel-bath-robes/"},
                {"gate_record_id": 10, "representative_query": "show this row", "source_entity_type": "category", "current_page_type": "category", "source_entity_id": 31, "source_url": "/hotel-slippers/"},
            ],
            annotate_query_gate_rows_with_suggestions_fn=lambda store_hash, rows, cluster=None: rows,
            attach_cached_query_gate_suggestions_fn=lambda rows: rows,
            extract_storefront_channel_id_fn=lambda *args, **kwargs: 7,
            build_storefront_url_fn=lambda store_hash, path, channel_id=None: f"https://example.com{path}" if path else None,
            summarize_suggested_target_types_fn=lambda rows: {"rows": len(rows)},
            publication_posting_label_fn=lambda row: "Published block live",
            format_timestamp_display_fn=lambda value: "display",
            format_relative_time_fn=lambda value: "relative",
            summarize_edge_case_requests_fn=lambda rows: {"count": len(rows)},
        )

        self.assertEqual(len(result["query_family_review"]), 1)
        self.assertEqual(result["query_family_review"][0]["gate_record_id"], 10)

    def test_build_public_dashboard_data_uses_cached_snapshots_before_fresh_annotation(self):
        annotate_mock = Mock(side_effect=AssertionError("fresh annotation should not run when cached snapshots exist"))

        result = dashboard_context.build_public_dashboard_data(
            store_hash="99oa2tso",
            include_admin=False,
            latest_gate_run_id=44,
            latest_gate_cluster="bath",
            publications=[
                {
                    "source_entity_type": "category",
                    "source_entity_id": 5450,
                    "source_url": "/hotel-shower-curtains/",
                }
            ],
            review_bucket_requests=[],
            list_query_gate_records_fn=lambda store_hash, disposition=None, limit=None, run_id=None: [
                {
                    "gate_record_id": 8,
                    "source_entity_type": "category",
                    "current_page_type": "category",
                    "source_entity_id": 5450,
                    "source_url": "/hotel-shower-curtains/",
                    "metadata": {
                        "suggested_target_snapshot": {"url": "/hookless-shower-curtains/"},
                        "second_option_snapshot": {"url": "/alternate-curtains/"},
                    },
                }
            ],
            annotate_query_gate_rows_with_suggestions_fn=annotate_mock,
            attach_cached_query_gate_suggestions_fn=lambda rows: [
                dict(
                    row,
                    suggested_target=dict((row.get("metadata") or {}).get("suggested_target_snapshot") or {}) or None,
                    second_option=dict((row.get("metadata") or {}).get("second_option_snapshot") or {}) or None,
                    target_override=None,
                )
                for row in rows
            ],
            extract_storefront_channel_id_fn=lambda *args, **kwargs: 7,
            build_storefront_url_fn=lambda store_hash, path, channel_id=None: f"https://example.com{path}" if path else None,
            summarize_suggested_target_types_fn=lambda rows: {"rows": len(rows)},
            publication_posting_label_fn=lambda row: "Published block live",
            format_timestamp_display_fn=lambda value: "display",
            format_relative_time_fn=lambda value: "relative",
            summarize_edge_case_requests_fn=lambda rows: {"count": len(rows)},
        )

        self.assertEqual(result["query_family_review"][0]["suggested_target"]["live_url"], "https://example.com/hookless-shower-curtains/")
        self.assertEqual(result["query_family_review"][0]["second_option"]["live_url"], "https://example.com/alternate-curtains/")
        annotate_mock.assert_not_called()

    def test_populate_changed_route_admin_context_applies_search_and_pagination(self):
        admin_context = dashboard_context.admin_context_defaults(
            pending_count=3,
            changed_route_search="hookless",
            changed_route_sort="query_asc",
            changed_route_page=1,
            changed_route_page_size=1,
        )
        changed_rows = [
            {
                "gate_record_id": 8,
                "representative_query": "hookless shower curtains",
                "source_entity_type": "category",
                "source_entity_id": 5450,
                "source_url": "/hotel-shower-curtains/",
            },
            {
                "gate_record_id": 9,
                "representative_query": "luxury bath towels",
                "source_entity_type": "category",
                "source_entity_id": 5451,
                "source_url": "/luxury-bath-towels/",
            },
        ]
        review_map = {
            8: {"verdict": "incorrect"},
            9: {"verdict": "correct"},
        }

        result = dashboard_context.populate_changed_route_admin_context(
            admin_context,
            store_hash="99oa2tso",
            latest_gate_run_id=44,
            publications=[
                {
                    "source_entity_type": "category",
                    "source_entity_id": 5450,
                    "source_url": "/hotel-shower-curtains/",
                }
            ],
            changed_route_search="hookless",
            changed_route_sort="query_asc",
            changed_route_page=1,
            changed_route_page_size=1,
            get_cached_changed_route_results_fn=lambda store_hash, run_id=None, limit=None: list(changed_rows),
            summarize_changed_route_rows_fn=lambda rows: {"changed_count": len(rows)},
            gate_review_map_for_ids_fn=lambda store_hash, gate_record_ids, run_id=None: review_map,
            attach_changed_route_agent_reviews_fn=lambda rows, review_map: [
                dict(row, agent_review=review_map.get(int(row.get("gate_record_id") or 0)))
                for row in rows
            ],
            summarize_changed_route_agent_reviews_fn=lambda rows, review_map: {"incorrect": 1, "correct": 1, "unclear": 0},
            get_cached_changed_route_review_reasoning_fn=lambda *args, **kwargs: {"status": "ok", "summary_text": "Looks useful"},
            build_query_gate_human_review_mailto_fn=lambda store_hash, gate_row=None, review=None, **kwargs: f"mailto:{gate_row.get('gate_record_id')}",
            matches_changed_route_search_fn=lambda row, search_text: search_text.lower() in (row.get("representative_query") or "").lower(),
            sorted_changed_route_rows_fn=lambda rows, sort_key: list(rows),
        )

        self.assertEqual(result["changed_route_total_count"], 2)
        self.assertEqual(result["changed_route_filtered_count"], 1)
        self.assertEqual(result["changed_route_page_count"], 1)
        self.assertEqual(len(result["changed_route_results"]), 1)
        self.assertEqual(result["changed_route_results"][0]["gate_record_id"], 8)
        self.assertEqual(result["changed_route_results"][0]["live_status_label"], "Published live")
        self.assertEqual(result["changed_route_results"][0]["human_review_mailto"], "mailto:8")
        self.assertEqual(result["changed_route_review_reasoning"]["status"], "ok")

    def test_populate_edge_case_admin_context_applies_requested_and_resolved_labels(self):
        admin_context = dashboard_context.admin_context_defaults(
            pending_count=3,
            changed_route_search="",
            changed_route_sort="score_desc",
            changed_route_page=1,
            changed_route_page_size=25,
        )
        request_rows = [
            {
                "request_id": 14,
                "gate_record_id": 8,
                "run_id": 44,
                "source_url": "/merchant-review-queue/",
                "target_url": "/merchant-review-target/",
                "metadata": {"live_block_paused": True},
                "created_at": datetime(2026, 4, 15, 12, 0, 0),
            }
        ]
        resolved_rows = [
            {
                "request_id": 15,
                "gate_record_id": 9,
                "run_id": 44,
                "source_url": "/luxury-bath-towels/",
                "target_url": "/luxury-towels/",
                "metadata": {
                    "live_block_restored": True,
                    "resolved_at": "2026-04-15T13:00:00",
                },
                "created_at": datetime(2026, 4, 15, 11, 0, 0),
                "allow_live_approval": True,
            }
        ]
        review_map = {
            8: {"metadata": {"winner": {"name": "Hookless Winner", "url": "/hookless-shower-curtains/"}}},
            9: {"metadata": {"winner": {"name": "Luxury Winner", "url": "/luxury-towels/"}}},
        }
        gate_row_map = {
            8: {"suggested_target": {"name": "Hookless Winner", "url": "/hookless-shower-curtains/"}},
            9: {"suggested_target": {"name": "Luxury Winner", "url": "/luxury-towels/"}},
        }

        result = dashboard_context.populate_edge_case_admin_context(
            admin_context,
            store_hash="99oa2tso",
            list_query_gate_review_requests_fn=lambda store_hash, request_status=None, limit=None: (
                [dict(row) for row in request_rows] if request_status == "requested" else [dict(row) for row in resolved_rows]
            ),
            gate_review_map_for_ids_fn=lambda store_hash, gate_record_ids, run_ids=None: review_map,
            query_gate_record_map_for_ids_fn=lambda store_hash, gate_record_ids, run_ids=None, fresh_suggestions=False: gate_row_map,
            extract_storefront_channel_id_fn=lambda *args, **kwargs: 7,
            build_storefront_url_fn=lambda store_hash, path, channel_id=None: f"https://example.com{path}",
            merge_fresh_gate_context_into_review_row_fn=lambda row, gate_row=None: row.update({"target_matches_current": row.get("request_id") == 15}),
            build_query_gate_human_review_mailto_fn=lambda store_hash, gate_row=None, request_row=None, review=None, **kwargs: f"mailto:{request_row.get('request_id')}",
            apply_review_target_display_fn=lambda row: row.update({"target_display_applied": True}),
            format_timestamp_display_fn=lambda value: "display",
            format_relative_time_fn=lambda value: "relative",
            summarize_edge_case_requests_fn=lambda rows: {"count": len(rows)},
        )

        self.assertEqual(result["edge_case_summary"]["count"], 1)
        self.assertEqual(result["resolved_edge_case_summary"]["recent_count"], 0)
        self.assertEqual(result["resolved_edge_case_summary"]["pending_approval_count"], 0)
        self.assertEqual(result["resolved_edge_case_summary"]["approved_live_count"], 1)
        self.assertEqual(result["edge_case_requests"][0]["applied_status_label"], "Live block paused for review")
        self.assertEqual(result["edge_case_requests"][0]["resolution_status_label"], "Waiting for support investigation or live fix")
        self.assertEqual(result["edge_case_requests"][0]["human_review_mailto"], "mailto:14")
        self.assertEqual(result["resolved_edge_case_requests"], [])

    def test_populate_edge_case_admin_context_keeps_only_resolved_rows_that_need_approval(self):
        admin_context = dashboard_context.admin_context_defaults(
            pending_count=3,
            changed_route_search="",
            changed_route_sort="score_desc",
            changed_route_page=1,
            changed_route_page_size=25,
        )
        resolved_rows = [
            {
                "request_id": 16,
                "gate_record_id": 10,
                "run_id": 44,
                "source_url": "/egyptian-cotton-towels/",
                "target_url": "/organic-towels/",
                "metadata": {
                    "live_block_paused": True,
                    "resolved_at": "2026-04-15T14:00:00",
                },
                "created_at": datetime(2026, 4, 15, 12, 0, 0),
                "allow_live_approval": True,
            }
        ]

        result = dashboard_context.populate_edge_case_admin_context(
            admin_context,
            store_hash="99oa2tso",
            list_query_gate_review_requests_fn=lambda store_hash, request_status=None, limit=None: (
                [] if request_status == "requested" else [dict(row) for row in resolved_rows]
            ),
            gate_review_map_for_ids_fn=lambda store_hash, gate_record_ids, run_ids=None: {},
            query_gate_record_map_for_ids_fn=lambda store_hash, gate_record_ids, run_ids=None, fresh_suggestions=False: {},
            extract_storefront_channel_id_fn=lambda *args, **kwargs: 7,
            build_storefront_url_fn=lambda store_hash, path, channel_id=None: f"https://example.com{path}",
            merge_fresh_gate_context_into_review_row_fn=lambda row, gate_row=None: row.update({"target_matches_current": False}),
            build_query_gate_human_review_mailto_fn=lambda store_hash, gate_row=None, request_row=None, review=None, **kwargs: f"mailto:{request_row.get('request_id')}",
            apply_review_target_display_fn=lambda row: row.update({"target_display_applied": True}),
            format_timestamp_display_fn=lambda value: "display",
            format_relative_time_fn=lambda value: "relative",
            summarize_edge_case_requests_fn=lambda rows: {"count": len(rows)},
        )

        self.assertEqual(result["resolved_edge_case_summary"]["recent_count"], 1)
        self.assertEqual(result["resolved_edge_case_summary"]["pending_approval_count"], 1)
        self.assertEqual(len(result["resolved_edge_case_requests"]), 1)
        self.assertTrue(result["resolved_edge_case_requests"][0]["can_approve_live"])
        self.assertEqual(result["resolved_edge_case_requests"][0]["resolution_status_label"], "Investigation complete")
        self.assertTrue(result["resolved_edge_case_requests"][0]["target_display_applied"])

    def test_populate_edge_case_admin_context_hides_resolved_rows_after_approval_completed_without_restore(self):
        admin_context = dashboard_context.admin_context_defaults(
            pending_count=3,
            changed_route_search="",
            changed_route_sort="score_desc",
            changed_route_page=1,
            changed_route_page_size=25,
        )
        resolved_rows = [
            {
                "request_id": 17,
                "gate_record_id": 12,
                "run_id": 44,
                "source_url": "/housekeeping-janitorial/",
                "target_url": "/cleaning-supplies/",
                "metadata": {
                    "resolved_at": "2026-04-15T14:00:00",
                    "live_block_restored": False,
                    "live_approval_completed": True,
                },
                "created_at": datetime(2026, 4, 15, 12, 0, 0),
                "allow_live_approval": True,
            }
        ]

        result = dashboard_context.populate_edge_case_admin_context(
            admin_context,
            store_hash="99oa2tso",
            list_query_gate_review_requests_fn=lambda store_hash, request_status=None, limit=None: (
                [] if request_status == "requested" else [dict(row) for row in resolved_rows]
            ),
            gate_review_map_for_ids_fn=lambda store_hash, gate_record_ids, run_ids=None: {},
            query_gate_record_map_for_ids_fn=lambda store_hash, gate_record_ids, run_ids=None, fresh_suggestions=False: {},
            extract_storefront_channel_id_fn=lambda *args, **kwargs: 7,
            build_storefront_url_fn=lambda store_hash, path, channel_id=None: f"https://example.com{path}",
            merge_fresh_gate_context_into_review_row_fn=lambda row, gate_row=None: row.update({"target_matches_current": False}),
            build_query_gate_human_review_mailto_fn=lambda store_hash, gate_row=None, request_row=None, review=None, **kwargs: f"mailto:{request_row.get('request_id')}",
            apply_review_target_display_fn=lambda row: row.update({"target_display_applied": True}),
            format_timestamp_display_fn=lambda value: "display",
            format_relative_time_fn=lambda value: "relative",
            summarize_edge_case_requests_fn=lambda rows: {"count": len(rows)},
        )

        self.assertEqual(result["resolved_edge_case_summary"]["recent_count"], 0)
        self.assertEqual(result["resolved_edge_case_summary"]["pending_approval_count"], 0)
        self.assertEqual(result["resolved_edge_case_summary"]["approved_live_count"], 0)
        self.assertEqual(result["resolved_edge_case_requests"], [])

    def test_build_dashboard_context_orchestrates_public_admin_and_quality_sections(self):
        result = dashboard_context.build_dashboard_context(
            "99oa2tso",
            include_admin=True,
            include_quality=True,
            changed_route_search="hookless",
            changed_route_sort="score_desc",
            changed_route_page=2,
            changed_route_page_size=10,
            generation_active_statuses={"running"},
            list_runs_fn=lambda store_hash: [
                {"run_id": 44, "status": "running", "filters": {"cluster": "bath"}},
                {"run_id": 43, "status": "completed"},
            ],
            get_store_profile_summary_fn=lambda store_hash: {"profile_count": 12},
            refresh_store_readiness_fn=lambda store_hash: {"updated_at": datetime(2026, 4, 15, 12, 0, 0), "metadata": {"feature_flags": {"cats": True}}},
            list_query_gate_review_requests_fn=lambda store_hash, request_status=None, limit=None: [],
            summarize_query_gate_dispositions_fn=lambda store_hash: {"run_id": 44, "pass": 7},
            list_publications_fn=lambda store_hash, active_only=True, limit=None: [{"source_url": "/hotel-shower-curtains/"}],
            summarize_live_publications_fn=lambda store_hash: {"total_live_blocks": 5},
            build_public_dashboard_data_fn=lambda **kwargs: {
                "query_family_review": [{"gate_record_id": 8}],
                "suggested_target_type_summary": {"category": 1},
                "publications": list(kwargs["publications"]),
                "review_bucket_requests": [],
                "review_bucket_summary": {"count": 0},
            },
            count_pending_candidates_fn=lambda store_hash: 3,
            admin_context_defaults_fn=lambda **kwargs: {"pending_count": kwargs["pending_count"], "edge_case_requests": []},
            populate_edge_case_admin_context_fn=lambda admin_context, **kwargs: dict(admin_context, edge_case_requests=[{"request_id": 1}]),
            summarize_gsc_routing_coverage_fn=lambda store_hash, run_id=None: {"run_id": run_id, "family_count": 5},
            populate_changed_route_admin_context_fn=lambda admin_context, **kwargs: dict(admin_context, changed_route_results=[{"gate_record_id": 9}]),
            summarize_blocked_gate_families_fn=lambda store_hash, run_id=None: {"run_id": run_id, "categories": []},
            get_cached_live_gsc_performance_fn=lambda store_hash: {"page_count": 2},
            build_operational_snapshot_fn=lambda store_hash, **kwargs: {"overall_status": "healthy"},
            get_logic_change_summary_fn=lambda limit=0: {"revision_count": 2},
            summarize_query_gate_agent_reviews_fn=lambda store_hash, run_id=None: {"correct": 1},
            list_query_gate_agent_review_clusters_fn=lambda store_hash, run_id=None, limit=0: [{"cluster": "bath"}],
            count_query_gate_review_requests_fn=lambda store_hash, request_status=None: 4,
            count_publications_fn=lambda store_hash, active_only=True: 6,
            theme_hook_present_fn=lambda: True,
            category_theme_hook_present_fn=lambda: False,
            format_timestamp_display_fn=lambda value: "display",
            format_relative_time_fn=lambda value: "relative",
        )

        self.assertEqual(result["active_run"]["run_id"], 44)
        self.assertEqual(result["pending_count"], 3)
        self.assertEqual(result["gsc_routing_coverage"]["run_id"], 44)
        self.assertEqual(result["operational_snapshot"]["overall_status"], "healthy")
        self.assertEqual(result["logic_change_summary"]["revision_count"], 2)
        self.assertEqual(result["gate_agent_review_clusters"][0]["cluster"], "bath")
        self.assertEqual(result["review_bucket_count"], 4)
        self.assertEqual(result["publication_count"], 6)
        self.assertEqual(result["feature_flags"]["cats"], True)
        self.assertEqual(result["last_checked_display"], "display")
        self.assertEqual(result["gsc_performance_summary"]["page_count"], 2)

    def test_build_dashboard_context_public_skips_expensive_profile_and_readiness_calls(self):
        result = dashboard_context.build_dashboard_context(
            "99oa2tso",
            include_admin=False,
            include_quality=False,
            generation_active_statuses={"running"},
            list_runs_fn=lambda store_hash: [
                {"run_id": 44, "status": "running", "filters": {"cluster": "bath"}},
            ],
            get_store_profile_summary_fn=lambda store_hash: (_ for _ in ()).throw(AssertionError("profile summary should not run")),
            refresh_store_readiness_fn=lambda store_hash: (_ for _ in ()).throw(AssertionError("readiness refresh should not run")),
            list_query_gate_review_requests_fn=lambda store_hash, request_status=None, limit=None: [],
            summarize_query_gate_dispositions_fn=lambda store_hash: {"run_id": 44, "pass": 7},
            list_publications_fn=lambda store_hash, active_only=True, limit=None: [{"source_url": "/hotel-shower-curtains/"}],
            summarize_live_publications_fn=lambda store_hash: {"total_live_blocks": 5},
            build_public_dashboard_data_fn=lambda **kwargs: {
                "query_family_review": [{"gate_record_id": 8}],
                "suggested_target_type_summary": {"category": 1},
                "results_report": {"raw_query_count": 7},
                "publications": list(kwargs["publications"]),
                "review_bucket_requests": [],
                "review_bucket_summary": {"count": 0},
            },
            count_pending_candidates_fn=lambda store_hash: (_ for _ in ()).throw(AssertionError("pending candidate count should not run")),
            admin_context_defaults_fn=lambda **kwargs: {"pending_count": kwargs["pending_count"], "edge_case_requests": []},
            populate_edge_case_admin_context_fn=lambda admin_context, **kwargs: admin_context,
            summarize_gsc_routing_coverage_fn=lambda store_hash, run_id=None: {},
            populate_changed_route_admin_context_fn=lambda admin_context, **kwargs: admin_context,
            summarize_blocked_gate_families_fn=lambda store_hash, run_id=None: {},
            get_cached_live_gsc_performance_fn=lambda store_hash: {"page_count": 3},
            build_operational_snapshot_fn=lambda store_hash, **kwargs: {},
            get_logic_change_summary_fn=lambda limit=0: {},
            summarize_query_gate_agent_reviews_fn=lambda store_hash, run_id=None: {},
            list_query_gate_agent_review_clusters_fn=lambda store_hash, run_id=None, limit=0: [],
            count_query_gate_review_requests_fn=lambda store_hash, request_status=None: 0,
            count_publications_fn=lambda store_hash, active_only=True: 1,
            theme_hook_present_fn=lambda: (_ for _ in ()).throw(AssertionError("theme hook check should not run")),
            category_theme_hook_present_fn=lambda: (_ for _ in ()).throw(AssertionError("category hook check should not run")),
            format_timestamp_display_fn=lambda value: "display",
            format_relative_time_fn=lambda value: "relative",
        )

        self.assertEqual(result["active_run"]["run_id"], 44)
        self.assertEqual(result["profile_summary"], {})
        self.assertEqual(result["readiness"], {})
        self.assertEqual(result["pending_count"], 0)
        self.assertFalse(result["theme_hook_present"])
        self.assertFalse(result["category_theme_hook_present"])
        self.assertEqual(result["gsc_performance_summary"]["page_count"], 3)
        self.assertEqual(result["results_report"]["raw_query_count"], 7)


if __name__ == "__main__":
    unittest.main()



