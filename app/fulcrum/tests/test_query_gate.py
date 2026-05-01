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

from app.fulcrum import query_gate


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


class FulcrumQueryGateTests(unittest.TestCase):
    def test_build_query_gate_records_groups_variants_and_sorts_rows(self):
        build_query_gate_record = Mock(
            side_effect=[
                {
                    "normalized_query_key": "luxury towel",
                    "representative_query": "luxury towels",
                    "disposition": "hold",
                    "opportunity_score": 45,
                    "demand_score": 10,
                },
                {
                    "normalized_query_key": "bath towel",
                    "representative_query": "bath towels",
                    "disposition": "pass",
                    "opportunity_score": 20,
                    "demand_score": 30,
                },
            ]
        )

        result = query_gate.build_query_gate_records(
            "99oa2tso",
            source_profiles={
                "/bath-towels/": {"entity_type": "product"},
                "/luxury-towels/": {"entity_type": "category"},
                "/brand-page/": {"entity_type": "brand"},
            },
            target_entities=[{"entity_type": "product", "bc_entity_id": 1}],
            min_hit_count=2,
            limit_total=5,
            build_store_signal_library=lambda store_hash: {"signals": True},
            fetch_gsc_query_page_evidence=lambda *args, **kwargs: [
                {"query": "bath towels", "source_url": "/bath-towels/", "clicks_90d": 3, "impressions_90d": 10, "avg_position_90d": 2.5},
                {"query": "bath towels sale", "source_url": "/bath-towels/", "clicks_90d": 2, "impressions_90d": 9, "avg_position_90d": 3.0},
                {"query": "luxury towels", "source_url": "/luxury-towels/", "clicks_90d": 1, "impressions_90d": 12, "avg_position_90d": 4.0},
                {"query": "brand towels", "source_url": "/brand-page/", "clicks_90d": 9, "impressions_90d": 20, "avg_position_90d": 1.0},
            ],
            normalize_storefront_path=lambda value: str(value or "").strip().lower(),
            normalize_query_family_key=lambda query: "bath towel" if "bath" in str(query).lower() else ("luxury towel" if "luxury" in str(query).lower() else ""),
            build_query_gate_record=build_query_gate_record,
        )

        self.assertEqual([row["normalized_query_key"] for row in result], ["bath towel", "luxury towel"])
        first_call = build_query_gate_record.call_args_list[0].kwargs
        second_call = build_query_gate_record.call_args_list[1].kwargs
        self.assertEqual(first_call["family_key"], "bath towel")
        self.assertEqual(first_call["representative_query"], "bath towels")
        self.assertEqual(len(first_call["evidence_rows"]), 2)
        self.assertEqual(second_call["family_key"], "luxury towel")

    def test_store_query_gate_records_persists_rows_and_commits(self):
        fake_cursor = _FakeCursor()
        fake_conn = _FakeConnection([fake_cursor])
        execute_batch_mock = Mock()

        with (
            patch("app.fulcrum.query_gate.get_pg_conn", return_value=fake_conn),
            patch("app.fulcrum.query_gate.execute_batch", execute_batch_mock),
        ):
            query_gate.store_query_gate_records(
                12,
                "99oa2tso",
                [
                    {
                        "normalized_query_key": "bath towel",
                        "representative_query": "bath towels",
                        "source_url": "/bath-towels/",
                        "source_name": "Bath Towel",
                        "source_entity_type": "product",
                        "source_entity_id": 5,
                        "current_page_type": "product",
                        "query_intent_scope": "specific_product",
                        "preferred_entity_type": "product",
                        "clicks_28d": 1,
                        "impressions_28d": 10,
                        "ctr_28d": 0.1,
                        "avg_position_28d": 2.0,
                        "clicks_90d": 3,
                        "impressions_90d": 30,
                        "ctr_90d": 0.1,
                        "avg_position_90d": 2.5,
                        "demand_score": 12.5,
                        "opportunity_score": 22.5,
                        "intent_clarity_score": 55.0,
                        "noise_penalty": 1.5,
                        "freshness_context": {"days": 14},
                        "disposition": "pass",
                        "reason_summary": "Strong route",
                        "metadata": {"winner": "product"},
                    }
                ],
            )

        self.assertTrue(fake_conn.committed)
        execute_batch_mock.assert_called_once()
        records = execute_batch_mock.call_args.args[2]
        self.assertEqual(records[0][0], 12)
        self.assertEqual(records[0][1], "99oa2tso")
        self.assertEqual(records[0][2], "bath towel")

    def test_list_and_summarize_query_gate_records_use_latest_run_when_needed(self):
        summarize_cursor = _FakeCursor(fetchall_results=[[("pass", 2), ("hold", 1)]])
        list_cursor = _FakeCursor(
            fetchall_results=[
                [
                    {
                        "gate_record_id": 8,
                        "run_id": 44,
                        "normalized_query_key": "bath towel",
                        "representative_query": "bath towels",
                        "source_url": "/bath-towels/",
                        "disposition": "pass",
                        "metadata": {},
                    }
                ]
            ]
        )

        with (
            patch("app.fulcrum.query_gate.latest_gate_run_id", return_value=44),
            patch("app.fulcrum.query_gate.get_pg_conn", side_effect=[_FakeConnection([summarize_cursor]), _FakeConnection([list_cursor])]),
        ):
            summary = query_gate.summarize_query_gate_dispositions("99oa2tso")
            rows = query_gate.list_query_gate_records("99oa2tso", disposition="pass", limit=20)

        self.assertEqual(summary, {"run_id": 44, "pass": 2, "hold": 1, "reject": 0})
        self.assertEqual(rows[0]["gate_record_id"], 8)
        self.assertEqual(list_cursor.executions[0][1], ("99oa2tso", 44, "pass", "pass", 20))

    def test_query_gate_record_map_for_ids_uses_cached_or_fresh_annotations(self):
        cached_cursor = _FakeCursor(
            fetchall_results=[
                [
                    {"gate_record_id": 7, "metadata": {}, "source_url": "/bath-towels/"},
                    {"gate_record_id": 9, "metadata": {}, "source_url": "/luxury-towels/"},
                ]
            ]
        )
        cached_attach = Mock(
            return_value=[
                {"gate_record_id": 7, "label": "cached"},
                {"gate_record_id": 9, "label": "cached"},
            ]
        )
        with patch("app.fulcrum.query_gate.get_pg_conn", return_value=_FakeConnection([cached_cursor])):
            result = query_gate.query_gate_record_map_for_ids(
                "99oa2tso",
                {7, 9},
                attach_cached_query_gate_suggestions=cached_attach,
            )

        self.assertEqual(sorted(result.keys()), [7, 9])
        cached_attach.assert_called_once()

        fresh_cursor = _FakeCursor(fetchall_results=[[{"gate_record_id": 11, "metadata": {}, "source_url": "/bath-towels/"}]])
        annotate = Mock(return_value=[{"gate_record_id": 11, "label": "fresh"}])
        with patch("app.fulcrum.query_gate.get_pg_conn", return_value=_FakeConnection([fresh_cursor])):
            fresh_result = query_gate.query_gate_record_map_for_ids(
                "99oa2tso",
                {11},
                fresh_suggestions=True,
                build_unified_entity_index=lambda store_hash: {"sources": {}, "targets": []},
                load_query_target_overrides=lambda store_hash: {},
                load_review_feedback_maps=lambda store_hash: {},
                build_store_signal_library=lambda store_hash: {},
                annotate_query_gate_rows_with_suggestions=annotate,
            )

        self.assertEqual(fresh_result[11]["label"], "fresh")
        annotate.assert_called_once()


if __name__ == "__main__":
    unittest.main()
