import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

APP_ROOT = ROOT / "MDMH4H"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from app.fulcrum.publishing import (
    _publication_source_match_ids,
    list_publications,
    publish_approved_entities,
    summarize_live_publications,
    unpublish_entities,
)


class _FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self._fetchall_result = conn.fetchall_queue.pop(0) if conn.fetchall_queue else []
        self._fetchone_result = conn.fetchone_queue.pop(0) if conn.fetchone_queue else None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.conn.executed.append((sql, params))

    def fetchall(self):
        return self._fetchall_result

    def fetchone(self):
        return self._fetchone_result


class _FakeConn:
    def __init__(self, *, fetchall_queue=None, fetchone_queue=None):
        self.fetchall_queue = list(fetchall_queue or [])
        self.fetchone_queue = list(fetchone_queue or [])
        self.executed = []
        self.commit_count = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self, cursor_factory=None):
        return _FakeCursor(self)

    def commit(self):
        self.commit_count += 1


class _FakeResponse:
    def __init__(self, *, status_code=204):
        self.status_code = status_code

    def raise_for_status(self):
        return None


class _FakeRequests:
    def __init__(self):
        self.delete_calls = []
        self.put_calls = []

    def delete(self, url, headers=None, timeout=None):
        self.delete_calls.append(
            {
                "url": url,
                "headers": headers,
                "timeout": timeout,
            }
        )
        return _FakeResponse(status_code=204)

    def put(self, url, headers=None, json=None, timeout=None):
        self.put_calls.append(
            {
                "url": url,
                "headers": headers,
                "json": json,
                "timeout": timeout,
            }
        )
        return _FakeResponse(status_code=200)


class _ForbiddenDeleteRequests(_FakeRequests):
    def delete(self, url, headers=None, timeout=None):
        self.delete_calls.append(
            {
                "url": url,
                "headers": headers,
                "timeout": timeout,
            }
        )
        return _FakeResponse(status_code=403)


class FulcrumPublishingTests(unittest.TestCase):
    def test_publication_source_match_ids_includes_legacy_category_id(self):
        self.assertEqual(
            _publication_source_match_ids(
                source_entity_type="category",
                source_entity_id=-1000005388,
                live_entity_id=5388,
            ),
            [-1000005388, -5388],
        )

    def test_publish_approved_entities_builds_two_category_metafields(self):
        conn = _FakeConn(
            fetchall_queue=[
                [
                    {
                        "source_entity_type": "category",
                        "source_product_id": 101,
                        "source_name": "Rollaway Beds",
                        "source_url": "/rollaway-beds/",
                    }
                ]
            ]
        )
        upsert_calls = []
        invalidations = []

        def _get_pg_conn():
            return conn

        def _get_rows(store_hash, source_product_id, source_entity_type):
            self.assertEqual(store_hash, "abc123")
            self.assertEqual(source_product_id, 101)
            self.assertEqual(source_entity_type, "category")
            return [
                {
                    "target_entity_type": "category",
                    "target_url": "/folding-beds/",
                    "metadata": {"query_intent_scope": "commercial_topic", "preferred_entity_type": "category"},
                },
                {
                    "target_entity_type": "brand",
                    "target_url": "/downlite/",
                    "metadata": {
                        "query_intent_scope": "brand_navigation",
                        "preferred_entity_type": "brand",
                        "query_target_tokens": ["downlite"],
                    },
                },
                {
                    "target_entity_type": "product",
                    "target_url": "/portable-bed-frame/",
                    "metadata": {"query_intent_scope": "specific_product", "preferred_entity_type": "product"},
                },
            ]

        def _build_links_html(rows, section_title="Related options"):
            if not rows:
                return None
            return f"{section_title}:{','.join(row['target_url'] for row in rows)}"

        def _upsert(store_hash, entity_type, entity_id, key, html):
            upsert_calls.append((store_hash, entity_type, entity_id, key, html))
            return {"action": "updated", "metafield_id": 900 + len(upsert_calls)}

        def _invalidate(store_hash, metric_keys=None):
            invalidations.append((store_hash, metric_keys))

        publications = publish_approved_entities(
            store_hash="abc123",
            source_entity_ids=None,
            run_id=55,
            get_pg_conn=_get_pg_conn,
            get_approved_rows_for_source=_get_rows,
            resolve_store_category_id_by_url=lambda store_hash, url: (777, "Live Rollaway Beds"),
            resolve_store_product_id_by_url=lambda store_hash, url: (None, None),
            build_links_html=_build_links_html,
            upsert_entity_metafield=_upsert,
            invalidate_admin_metric_cache=_invalidate,
        )

        self.assertGreaterEqual(conn.commit_count, 1)
        self.assertEqual(len(upsert_calls), 2)
        self.assertEqual(
            [call[3] for call in upsert_calls],
            ["internal_category_links_html", "internal_product_links_html"],
        )
        self.assertIn("/downlite/", upsert_calls[0][4])
        self.assertEqual(
            [item["metafield_key"] for item in publications],
            ["internal_category_links_html", "internal_product_links_html"],
        )
        self.assertEqual(invalidations, [("abc123", ["live_gsc_performance", "live_gsc_performance_store_scoped_v2"])])

    def test_publish_approved_entities_skips_brand_query_category_target(self):
        conn = _FakeConn(
            fetchall_queue=[
                [
                    {
                        "source_entity_type": "product",
                        "source_product_id": 112556,
                        "source_name": "Cloud Top",
                        "source_url": "/cloud-top/",
                    }
                ]
            ]
        )
        upsert_calls = []

        publications = publish_approved_entities(
            store_hash="abc123",
            source_entity_ids=None,
            run_id=None,
            get_pg_conn=lambda: conn,
            get_approved_rows_for_source=lambda store_hash, source_product_id, source_entity_type: [
                {
                    "target_entity_type": "category",
                    "target_url": "/hotel-bedding-supply/",
                    "metadata": {
                        "query_intent_scope": "brand_navigation",
                        "preferred_entity_type": "brand",
                        "query_target_tokens": ["downlite"],
                    },
                }
            ],
            resolve_store_category_id_by_url=lambda store_hash, url: (None, None),
            resolve_store_product_id_by_url=lambda store_hash, url: (112556, "Cloud Top"),
            build_links_html=lambda rows, section_title="Related options": "html" if rows else None,
            upsert_entity_metafield=lambda *args: upsert_calls.append(args) or {"action": "updated", "metafield_id": 1},
            invalidate_admin_metric_cache=lambda store_hash, metric_keys=None: None,
        )

        self.assertEqual(publications, [])
        self.assertEqual(upsert_calls, [])

    def test_unpublish_entities_deletes_category_metafield_and_marks_row_unpublished(self):
        conn = _FakeConn(
            fetchall_queue=[
                [
                    {
                        "publication_id": 7,
                        "source_entity_type": "category",
                        "source_product_id": 101,
                        "source_url": "/rollaway-beds/",
                        "metafield_id": 333,
                        "metafield_key": "internal_category_links_html",
                    }
                ]
            ]
        )
        requests_module = _FakeRequests()
        invalidations = []

        def _invalidate(store_hash, metric_keys=None):
            invalidations.append((store_hash, metric_keys))

        results = unpublish_entities(
            store_hash="AbC123",
            source_entity_ids=[101],
            get_pg_conn=lambda: conn,
            get_bc_headers=lambda store_hash: {"X-Auth-Token": "secret"},
            normalize_store_hash=lambda store_hash: str(store_hash).lower(),
            resolve_store_category_id_by_url=lambda store_hash, url: (888, "Live Category"),
            resolve_store_product_id_by_url=lambda store_hash, url: (None, None),
            invalidate_admin_metric_cache=_invalidate,
            requests_module=requests_module,
        )

        self.assertEqual(conn.commit_count, 1)
        self.assertEqual(len(requests_module.delete_calls), 1)
        self.assertIn("/catalog/categories/888/metafields/333", requests_module.delete_calls[0]["url"])
        self.assertEqual(results[0]["status"], "unpublished")
        self.assertEqual(invalidations, [("AbC123", ["live_gsc_performance", "live_gsc_performance_store_scoped_v2"])])

    def test_unpublish_entities_blanks_metafield_when_delete_is_forbidden(self):
        conn = _FakeConn(
            fetchall_queue=[
                [
                    {
                        "publication_id": 7,
                        "source_entity_type": "category",
                        "source_product_id": 101,
                        "source_url": "/rollaway-beds/",
                        "metafield_id": 333,
                        "metafield_key": "internal_category_links_html",
                    }
                ]
            ]
        )
        requests_module = _ForbiddenDeleteRequests()

        results = unpublish_entities(
            store_hash="AbC123",
            source_entity_ids=[101],
            get_pg_conn=lambda: conn,
            get_bc_headers=lambda store_hash: {"X-Auth-Token": "secret"},
            normalize_store_hash=lambda store_hash: str(store_hash).lower(),
            resolve_store_category_id_by_url=lambda store_hash, url: (888, "Live Category"),
            resolve_store_product_id_by_url=lambda store_hash, url: (None, None),
            invalidate_admin_metric_cache=lambda store_hash, metric_keys=None: None,
            requests_module=requests_module,
        )

        self.assertEqual(len(requests_module.delete_calls), 1)
        self.assertEqual(len(requests_module.put_calls), 1)
        self.assertEqual(requests_module.put_calls[0]["json"]["value"], "<!-- Fulcrum unpublished -->")
        self.assertEqual(results[0]["status"], "unpublished")

    def test_summarize_live_publications_returns_ints(self):
        conn = _FakeConn(
            fetchone_queue=[
                {
                    "product_page_blocks": 2,
                    "category_product_blocks": 3,
                    "category_category_blocks": 4,
                    "total_live_blocks": 9,
                }
            ]
        )

        summary = summarize_live_publications(
            store_hash="AbC123",
            get_pg_conn=lambda: conn,
            normalize_store_hash=lambda store_hash: str(store_hash).lower(),
        )

        self.assertEqual(
            summary,
            {
                "product_page_blocks": 2,
                "category_product_blocks": 3,
                "category_category_blocks": 4,
                "total_live_blocks": 9,
            },
        )
        self.assertIn("abc123", conn.executed[0][1])

    def test_list_publications_returns_metadata(self):
        conn = _FakeConn(
            fetchall_queue=[
                [
                    {
                        "publication_id": 17,
                        "source_entity_type": "category",
                        "source_entity_id": -1000005450,
                        "source_product_id": -1000005450,
                        "source_name": "Fabric Curtains",
                        "source_url": "/fabric-curtains/",
                        "metafield_id": 333,
                        "metafield_key": "internal_category_links_html",
                        "metadata": {"bc_category_id": 5450},
                        "publication_status": "published",
                        "published_at": "2026-04-19T00:00:00Z",
                        "unpublished_at": None,
                    }
                ]
            ]
        )

        rows = list_publications(
            store_hash="abc123",
            active_only=True,
            limit=10,
            get_pg_conn=lambda: conn,
        )

        self.assertEqual(rows[0]["metadata"]["bc_category_id"], 5450)


if __name__ == "__main__":
    unittest.main()
