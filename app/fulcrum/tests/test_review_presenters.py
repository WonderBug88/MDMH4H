import sys
import unittest
from datetime import datetime
from pathlib import Path
from urllib.parse import parse_qs, urlparse


ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

APP_ROOT = ROOT / "MDMH4H"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from app.fulcrum import review_presenters


class FulcrumReviewPresentersTests(unittest.TestCase):
    def test_merge_fresh_gate_context_into_review_row_copies_known_fields(self):
        row = {"source_name": "Old Name"}
        review_presenters.merge_fresh_gate_context_into_review_row(
            row,
            {"source_name": "Fresh Name", "normalized_query_key": "hookless curtain", "ignored": "value"},
        )

        self.assertEqual(row["source_name"], "Fresh Name")
        self.assertEqual(row["normalized_query_key"], "hookless curtain")
        self.assertNotIn("ignored", row)

    def test_build_query_gate_human_review_mailto_includes_review_and_admin_link(self):
        mailto = review_presenters.build_query_gate_human_review_mailto(
            "Stores/99OA2TSO",
            app_base_url="https://fulcrum.example.com",
            get_store_owner_email_fn=lambda store_hash: "owner@example.com",
            normalize_store_hash_fn=lambda store_hash: "99oa2tso",
            gate_row={
                "gate_record_id": 8,
                "representative_query": "hookless shower curtains",
                "normalized_query_key": "hookless curtain",
                "source_name": "Hotel Shower Curtains",
                "source_url": "/hotel-shower-curtains/",
                "source_entity_type": "category",
                "suggested_target": {"name": "Hookless Curtains", "url": "/hookless-shower-curtains/", "entity_type": "category"},
                "reason_summary": "Winner looks cleaner",
            },
            review={"verdict": "incorrect", "issue_type": "needs_human_review", "recommended_action": "manual_review", "rationale": "Please inspect"},
        )

        parsed = urlparse(mailto)
        params = parse_qs(parsed.query)

        self.assertEqual(parsed.scheme, "mailto")
        self.assertEqual(parsed.path, "owner@example.com")
        self.assertIn("Gate #8", params["subject"][0])
        self.assertIn("Admin page: https://fulcrum.example.com/fulcrum/admin?store_hash=99oa2tso", params["body"][0])
        self.assertIn("AI review:", params["body"][0])

    def test_publication_posting_label_uses_entity_type_and_metafield(self):
        self.assertEqual(
            review_presenters.publication_posting_label({"source_entity_type": "category", "metafield_key": "internal_product_links_html"}),
            "Category page posting product families",
        )

    def test_summarize_edge_case_requests_counts_statuses_and_dates(self):
        summary = review_presenters.summarize_edge_case_requests(
            [
                {
                    "metadata": {"live_block_paused": True, "audit_status": "ok"},
                    "agent_review": {"issue_type": "needs_human_review", "recommended_action": "manual_review"},
                    "created_at": datetime(2026, 4, 15, 12, 0, 0),
                },
                {
                    "metadata": {},
                    "created_at": datetime(2026, 4, 15, 13, 0, 0),
                },
            ],
            format_timestamp_display_fn=lambda value: value.strftime("%H:%M") if value else "",
            format_relative_time_fn=lambda value: "relative" if value else "",
        )

        self.assertEqual(summary["open_count"], 2)
        self.assertEqual(summary["paused_live_blocks"], 1)
        self.assertEqual(summary["audited_count"], 1)
        self.assertEqual(summary["needs_human_review_count"], 1)
        self.assertEqual(summary["oldest_request_display"], "12:00")
        self.assertEqual(summary["newest_request_display"], "13:00")

    def test_apply_review_target_display_handles_use_original_and_sets_match_flags(self):
        row = {
            "source_name": "Hotel Shower Curtains",
            "source_url": "/hotel-shower-curtains/",
            "source_live_url": "https://example.com/hotel-shower-curtains/",
            "target_name": "Hookless Curtains",
            "target_url": "/hookless-shower-curtains/",
            "target_live_url": "https://example.com/hookless-shower-curtains/",
            "agent_review": {"recommended_action": "use_original"},
        }

        review_presenters.apply_review_target_display(
            row,
            normalize_storefront_path_fn=lambda value: str(value or "").strip(),
        )

        self.assertEqual(row["display_target_name"], "Hotel Shower Curtains")
        self.assertTrue(row["target_matches_current"])
        self.assertFalse(row["allow_live_approval"])


if __name__ == "__main__":
    unittest.main()
