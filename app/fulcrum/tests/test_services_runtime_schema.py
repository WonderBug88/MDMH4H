import unittest
from pathlib import Path
from unittest.mock import Mock, patch
import sys


ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

APP_ROOT = ROOT / "MDMH4H"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from app.fulcrum import services


class _FakeCursor:
    def __init__(self, rows=None):
        self.rows = list(rows or [])
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchall(self):
        return list(self.rows)


class _FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor
        self.committed = False
        self.rolled_back = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        return self._cursor

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


class FulcrumRuntimeSchemaTests(unittest.TestCase):
    def setUp(self):
        self._original_runtime_schema_applied = services._RUNTIME_SCHEMA_APPLIED
        services._RUNTIME_SCHEMA_APPLIED = False

    def tearDown(self):
        services._RUNTIME_SCHEMA_APPLIED = self._original_runtime_schema_applied

    def test_apply_runtime_schema_skips_ddl_when_schema_is_already_current(self):
        rows = []
        for table_name in services._RUNTIME_SCHEMA_REQUIRED_TABLES:
            required_columns = services._RUNTIME_SCHEMA_REQUIRED_COLUMNS.get(table_name)
            if required_columns:
                rows.extend((table_name, column_name) for column_name in required_columns)
            else:
                rows.append((table_name, "id"))

        probe_cursor = _FakeCursor(rows)
        probe_conn = _FakeConnection(probe_cursor)

        with (
            patch("app.fulcrum.services.get_pg_conn", return_value=probe_conn),
            patch("app.fulcrum.services.RUNTIME_SQL_PATH", Mock(read_text=Mock(side_effect=AssertionError("DDL should not be replayed")))),
        ):
            services.apply_runtime_schema()

        self.assertTrue(services._RUNTIME_SCHEMA_APPLIED)
        self.assertEqual(len(probe_cursor.executed), 1)
        self.assertIn("information_schema.columns", probe_cursor.executed[0][0])
        self.assertFalse(probe_conn.committed)

    def test_apply_runtime_schema_replays_ddl_when_schema_probe_is_incomplete(self):
        probe_cursor = _FakeCursor([("store_installations", "installation_id")])
        probe_conn = _FakeConnection(probe_cursor)
        ddl_cursor = _FakeCursor()
        ddl_conn = _FakeConnection(ddl_cursor)

        with (
            patch("app.fulcrum.services.get_pg_conn", side_effect=[probe_conn, ddl_conn]),
            patch("app.fulcrum.services.RUNTIME_SQL_PATH", Mock(read_text=Mock(return_value="SELECT 1; SELECT 2;"))),
        ):
            services.apply_runtime_schema()

        self.assertTrue(services._RUNTIME_SCHEMA_APPLIED)
        self.assertEqual([sql for sql, _ in ddl_cursor.executed], ["SELECT 1", "SELECT 2"])
        self.assertTrue(ddl_conn.committed)

    def test_apply_runtime_schema_runs_targeted_submission_table_migration(self):
        rows = []
        for table_name in services._RUNTIME_SCHEMA_REQUIRED_TABLES.difference({"query_gate_review_submissions"}):
            required_columns = services._RUNTIME_SCHEMA_REQUIRED_COLUMNS.get(table_name)
            if required_columns:
                rows.extend((table_name, column_name) for column_name in required_columns)
            else:
                rows.append((table_name, "id"))

        probe_cursor = _FakeCursor(rows)
        probe_conn = _FakeConnection(probe_cursor)
        migration_cursor = _FakeCursor()
        migration_conn = _FakeConnection(migration_cursor)

        with (
            patch("app.fulcrum.services.get_pg_conn", side_effect=[probe_conn, migration_conn]),
            patch("app.fulcrum.services.RUNTIME_SQL_PATH", Mock(read_text=Mock(side_effect=AssertionError("full DDL should not run")))),
        ):
            services.apply_runtime_schema()

        self.assertTrue(services._RUNTIME_SCHEMA_APPLIED)
        self.assertEqual(
            [sql for sql, _ in migration_cursor.executed],
            services._RUNTIME_SCHEMA_QUERY_GATE_REVIEW_SUBMISSIONS_STATEMENTS,
        )
        self.assertTrue(migration_conn.committed)


if __name__ == "__main__":
    unittest.main()
