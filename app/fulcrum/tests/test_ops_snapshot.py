import sys
import unittest
from datetime import datetime, timedelta
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

APP_ROOT = ROOT / "MDMH4H"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from app.fulcrum import ops_snapshot


class FulcrumOpsSnapshotTests(unittest.TestCase):
    def test_format_helpers_return_display_strings(self):
        timestamp = datetime.now().astimezone() - timedelta(hours=2)

        self.assertTrue(ops_snapshot.format_timestamp_display(timestamp))
        self.assertEqual(ops_snapshot.format_relative_time(timestamp), "2h ago")
        self.assertEqual(ops_snapshot.alert_severity_rank("urgent"), 3)
        self.assertEqual(ops_snapshot.alert_tone_for_severity("watch"), "amber")

    def test_build_operational_snapshot_surfaces_urgent_and_watch_alerts(self):
        now = datetime.now().astimezone()
        snapshot = ops_snapshot.build_operational_snapshot(
            "Stores/99OA2TSO",
            runs=[
                {"run_id": 14, "status": "failed", "started_at": now - timedelta(hours=3), "notes": "network timeout"},
                {"run_id": 12, "status": "completed", "completed_at": now - timedelta(days=10), "started_at": now - timedelta(days=10, hours=1)},
            ],
            readiness={
                "catalog_synced": False,
                "theme_hook_ready": False,
                "auto_publish_ready": False,
                "updated_at": now - timedelta(hours=1),
                "unresolved_option_name_count": 2,
                "unresolved_option_value_count": 1,
            },
            publication_summary={"total_live_blocks": 0},
            edge_case_requests=[{"request_id": 1}] * 4,
            gate_summary={"pass": 7},
            normalize_store_hash_fn=lambda store_hash: "99oa2tso",
            list_runs_fn=lambda store_hash, limit=10: [],
            refresh_store_readiness_fn=lambda store_hash: {},
            summarize_live_publications_fn=lambda store_hash: {},
            summarize_query_gate_dispositions_fn=lambda store_hash: {},
            category_publishing_enabled_for_store_fn=lambda store_hash: True,
            category_theme_hook_present_fn=lambda: False,
            generation_active_statuses={"queued", "running"},
            active_run_watch_after=timedelta(minutes=15),
            active_run_urgent_after=timedelta(minutes=45),
            completed_run_watch_after=timedelta(days=8),
            completed_run_urgent_after=timedelta(days=14),
            edge_case_watch_count=3,
            edge_case_urgent_count=8,
        )

        self.assertEqual(snapshot["overall_status"], "urgent")
        alert_titles = [item["title"] for item in snapshot["alerts"]]
        self.assertIn("Catalog is not synced", alert_titles)
        self.assertIn("Product theme hook is missing", alert_titles)
        self.assertIn("Review queue needs attention", alert_titles)
        self.assertIn("Recent generation failures detected", alert_titles)
        self.assertEqual(snapshot["system_cards"][0]["value"], "Needs sync")


if __name__ == "__main__":
    unittest.main()
