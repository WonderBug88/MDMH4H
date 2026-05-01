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

from app.fulcrum import query_suggestions


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


class FulcrumQuerySuggestionsTests(unittest.TestCase):
    def test_query_target_override_key_normalizes_query_and_source(self):
        key = query_suggestions.query_target_override_key(
            "Hookless Curtain",
            " /hotel-shower-curtains/ ",
            normalize_query_family_key_fn=lambda value: str(value or "").strip().lower(),
            normalize_storefront_path_fn=lambda value: str(value or "").strip(),
        )

        self.assertEqual(key, ("hookless curtain", "/hotel-shower-curtains/"))

    def test_load_and_set_query_target_overrides_use_db(self):
        load_cursor = _FakeCursor(
            fetchall_results=[
                [
                    {"normalized_query_key": "hookless curtain", "source_url": "/hotel-shower-curtains/", "override_id": 8},
                ]
            ]
        )
        set_cursor = _FakeCursor(
            fetchone_results=[
                {"override_id": 9, "normalized_query_key": "hookless curtain", "source_url": "/hotel-shower-curtains/", "metadata": {"target_name": "Hookless"}},
            ]
        )

        with patch(
            "app.fulcrum.query_suggestions.get_pg_conn",
            side_effect=[_FakeConnection([load_cursor]), _FakeConnection([set_cursor])],
        ):
            loaded = query_suggestions.load_query_target_overrides(
                "99oa2tso",
                query_target_override_key_fn=lambda normalized_query_key, source_url: (normalized_query_key or "", source_url or ""),
            )
            stored = query_suggestions.set_query_target_override(
                "99oa2tso",
                "Hookless Curtain",
                "/hotel-shower-curtains/",
                "category",
                10,
                "product",
                22,
                query_target_override_key_fn=lambda normalized_query_key, source_url: ("hookless curtain", "/hotel-shower-curtains/"),
                created_by="alice@example.com",
                metadata={"target_name": "Hookless"},
            )

        self.assertIn(("hookless curtain", "/hotel-shower-curtains/"), loaded)
        self.assertEqual(stored["override_id"], 9)
        self.assertEqual(set_cursor.executions[0][1][0], "99oa2tso")

    def test_attach_cached_query_gate_suggestions_and_snapshot_serialization(self):
        snapshot = query_suggestions.serialize_query_gate_target_snapshot(
            {"entity_type": "product", "entity_id": 22, "name": "Hookless", "url": "/hookless/", "score": 88.4, "ignored": "x"}
        )
        self.assertEqual(snapshot["entity_type"], "product")
        self.assertNotIn("ignored", snapshot)

        attached = query_suggestions.attach_cached_query_gate_suggestions(
            [
                {
                    "gate_record_id": 8,
                    "metadata": {
                        "suggested_target_snapshot": {"entity_id": 22},
                        "second_option_snapshot": {"entity_id": 23},
                        "target_override_snapshot": {"entity_id": 24},
                    },
                }
            ]
        )
        self.assertEqual(attached[0]["suggested_target"]["entity_id"], 22)
        self.assertEqual(attached[0]["second_option"]["entity_id"], 23)
        self.assertEqual(attached[0]["target_override"]["entity_id"], 24)

    def test_annotate_query_gate_rows_with_suggestions_uses_injected_dependencies(self):
        refreshed = {"gate_record_id": 8, "normalized_query_key": "hookless curtain", "source_url": "/hotel-shower-curtains/", "metadata": {}}
        ranked = [
            {"entity_id": 22, "entity_type": "product"},
            {"entity_id": 23, "entity_type": "category"},
        ]

        result = query_suggestions.annotate_query_gate_rows_with_suggestions(
            "99oa2tso",
            [{"gate_record_id": 8}],
            cache_snapshots=True,
            build_unified_entity_index_fn=lambda store_hash, cluster=None: {"sources": {"a": {}}, "targets": [{"bc_entity_id": 22}]},
            load_query_target_overrides_fn=lambda store_hash: {("hookless curtain", "/hotel-shower-curtains/"): {"override_id": 1}},
            load_review_feedback_maps_fn=lambda store_hash: {"pair": {}, "family_target": {}, "target": {}},
            build_store_signal_library_fn=lambda store_hash: {"signals": True},
            refresh_query_gate_row_live_state_fn=lambda **kwargs: dict(refreshed),
            rank_target_options_for_gate_row_fn=lambda **kwargs: list(ranked),
            query_target_override_key_fn=lambda normalized_query_key, source_url: (normalized_query_key or "", source_url or ""),
            serialize_query_gate_target_snapshot_fn=lambda target: {"entity_id": target.get("entity_id"), "override_id": target.get("override_id")} if target else None,
        )

        self.assertEqual(result[0]["suggested_target"]["entity_id"], 22)
        self.assertEqual(result[0]["second_option"]["entity_id"], 23)
        self.assertEqual(result[0]["target_override"]["override_id"], 1)
        self.assertEqual(result[0]["metadata"]["suggested_target_snapshot"]["entity_id"], 22)
        self.assertEqual(result[0]["metadata"]["target_override_snapshot"]["override_id"], 1)

    def test_refresh_query_gate_suggestion_cache_coordinates_update_flow(self):
        annotate = Mock(return_value=[{"gate_record_id": 8}, {"gate_record_id": 9}])
        store_records = Mock()

        result = query_suggestions.refresh_query_gate_suggestion_cache(
            "99oa2tso",
            latest_gate_run_id_fn=lambda store_hash: 44,
            list_runs_fn=lambda store_hash, limit=20: [{"run_id": 44, "filters": {"cluster": "bath"}}],
            list_query_gate_records_fn=lambda store_hash, disposition=None, limit=None, run_id=None: [{"gate_record_id": 8}, {"gate_record_id": 9}],
            build_unified_entity_index_fn=lambda store_hash, cluster=None: {"sources": {"a": {}}, "targets": [{"bc_entity_id": 22}]},
            build_store_signal_library_fn=lambda store_hash: {"signals": True},
            load_query_target_overrides_fn=lambda store_hash: {},
            load_review_feedback_maps_fn=lambda store_hash: {},
            annotate_query_gate_rows_with_suggestions_fn=annotate,
            store_query_gate_records_fn=store_records,
        )

        self.assertEqual(result, {"status": "ok", "run_id": 44, "updated_count": 2})
        annotate.assert_called_once()
        store_records.assert_called_once_with(44, "99oa2tso", [{"gate_record_id": 8}, {"gate_record_id": 9}])


if __name__ == "__main__":
    unittest.main()
