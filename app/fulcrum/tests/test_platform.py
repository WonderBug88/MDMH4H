import base64
import hashlib
import hmac
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

from app.fulcrum import platform


def _base64url_encode(payload) -> str:
    raw = payload if isinstance(payload, bytes) else json.dumps(payload).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")


class _FakeCursor:
    def __init__(self):
        self.executions = []

    def execute(self, sql, params=None):
        self.executions.append((sql, params))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeConnection:
    def __init__(self):
        self.cursor_instance = _FakeCursor()
        self.committed = False

    def cursor(self, *args, **kwargs):
        return self.cursor_instance

    def commit(self):
        self.committed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FulcrumPlatformTests(unittest.TestCase):
    def test_normalize_store_hash_handles_context_noise(self):
        self.assertEqual(platform.normalize_store_hash("stores/99OA2TSO?v=1#abc"), "99oa2tso")
        self.assertEqual(platform.normalize_store_hash("99oa2tso:admin"), "99oa2tso")
        self.assertEqual(platform.normalize_store_hash(None), "")

    def test_decode_signed_payload_validates_signature(self):
        secret = "super-secret"
        header = _base64url_encode({"alg": "HS256", "typ": "JWT"})
        payload = _base64url_encode({"context": "stores/99oa2tso"})
        signing_input = f"{header}.{payload}".encode("utf-8")
        signature = _base64url_encode(hmac.new(secret.encode("utf-8"), signing_input, hashlib.sha256).digest())
        token = f"{header}.{payload}.{signature}"

        decoded = platform.decode_signed_payload(token, secret)
        self.assertEqual(decoded["context"], "stores/99oa2tso")

        with self.assertRaises(ValueError):
            platform.decode_signed_payload(f"{header}.{payload}.invalid", secret)

    def test_flatten_option_pairs_supports_named_and_value_options(self):
        rows = platform._flatten_option_pairs(
            [
                {"display_name": "Color", "option_values": [{"label": "White"}, {"value": "Blue"}]},
                {"name": "Size", "option_values": []},
            ]
        )
        self.assertEqual(rows, [("Color", "White"), ("Color", "Blue"), ("Size", "Size")])

    def test_sync_store_storefront_sites_uses_callbacks_and_returns_summary(self):
        fake_conn = _FakeConnection()
        execute_batch_mock = Mock()
        clear_cache_mock = Mock()
        resolve_default_url_mock = Mock(return_value="https://store.example.com")

        with (
            patch("app.fulcrum.platform.list_store_channels", return_value=[{"id": 1, "type": "storefront", "name": "Main", "platform": "bc", "status": "active", "is_enabled": True}]),
            patch("app.fulcrum.platform.list_store_sites", return_value=[{"id": 10, "channel_id": 1, "url": "https://store.example.com", "urls": [{"type": "primary", "url": "https://store.example.com"}, {"type": "checkout", "url": "https://checkout.example.com"}]}]),
            patch("app.fulcrum.platform.get_pg_conn", return_value=fake_conn),
            patch("app.fulcrum.platform.execute_batch", execute_batch_mock),
        ):
            result = platform.sync_store_storefront_sites(
                "Stores/99OA2TSO",
                initiated_by="test",
                clear_storefront_site_caches=clear_cache_mock,
                resolve_default_base_url=resolve_default_url_mock,
            )

        self.assertEqual(result["store_hash"], "99oa2tso")
        self.assertEqual(result["synced_site_count"], 1)
        self.assertEqual(result["storefront_site_count"], 1)
        self.assertEqual(result["default_base_url"], "https://store.example.com")
        self.assertTrue(fake_conn.committed)
        clear_cache_mock.assert_called_once_with()
        resolve_default_url_mock.assert_called_once_with("99oa2tso")
        execute_batch_mock.assert_called_once()
        self.assertEqual(fake_conn.cursor_instance.executions[-1][1], ("99oa2tso",))


if __name__ == "__main__":
    unittest.main()
