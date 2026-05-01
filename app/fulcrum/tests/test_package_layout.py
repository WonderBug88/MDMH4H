import sys
import unittest
from pathlib import Path

from werkzeug.middleware.proxy_fix import ProxyFix

ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

APP_ROOT = ROOT / "MDMH4H"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from app import create_app as create_main_app
from app.fulcrum import create_fulcrum_app
from app.fulcrum import services
from app.fulcrum.config import Config as FulcrumConfig, load_config
from app.fulcrum.routes import fulcrum_bp


class FulcrumPackageLayoutTests(unittest.TestCase):
    def test_blueprint_uses_local_template_folder(self):
        self.assertEqual(fulcrum_bp.template_folder, "templates")

    def test_local_templates_exist(self):
        template_root = APP_ROOT / "app" / "fulcrum" / "templates" / "fulcrum"
        self.assertTrue((template_root / "base.html").exists())
        self.assertTrue((template_root / "setup.html").exists())
        self.assertTrue((template_root / "connections.html").exists())
        self.assertTrue((template_root / "store_setup.html").exists())
        self.assertTrue((template_root / "dashboard.html").exists())
        self.assertTrue((template_root / "review.html").exists())
        self.assertTrue((template_root / "settings.html").exists())
        self.assertTrue((template_root / "admin.html").exists())
        self.assertTrue((template_root / "admin_developer.html").exists())
        self.assertTrue((template_root / "admin_quality.html").exists())
        self.assertTrue((template_root / "privacy.html").exists())
        self.assertTrue((template_root / "support.html").exists())
        self.assertTrue((template_root / "terms.html").exists())
        self.assertTrue((template_root / "preview.html").exists())

    def test_local_config_facade_exposes_fulcrum_settings(self):
        self.assertTrue(hasattr(FulcrumConfig, "FULCRUM_ENV_PATH"))
        self.assertTrue(hasattr(FulcrumConfig, "FULCRUM_ALLOWED_STORES"))
        self.assertIsInstance(load_config, dict)

    def test_runtime_schema_source_is_local(self):
        local_schema = APP_ROOT / "app" / "fulcrum" / "sql" / "fulcrum_runtime.sql"
        legacy_schema = APP_ROOT / "db" / "fulcrum_runtime.sql"
        self.assertEqual(services.RUNTIME_SQL_PATH, local_schema)
        self.assertTrue(local_schema.exists())
        self.assertTrue(legacy_schema.exists())
        self.assertEqual(local_schema.read_text(encoding="utf-8"), legacy_schema.read_text(encoding="utf-8"))

    def test_generation_worker_script_is_local(self):
        worker_script = services._generation_worker_script_path()
        self.assertEqual(worker_script, APP_ROOT / "app" / "fulcrum" / "generation_job.py")
        self.assertTrue(worker_script.exists())

    def test_standalone_fulcrum_app_registers_fulcrum_blueprint(self):
        app = create_fulcrum_app(load_config["testing"])
        self.assertIn("fulcrum", app.blueprints)
        client = app.test_client()
        response = client.get("/")
        self.assertEqual(response.status_code, 302)
        self.assertIn("/fulcrum", response.headers["Location"])

    def test_standalone_fulcrum_app_uses_proxy_fix(self):
        app = create_fulcrum_app(load_config["testing"])
        self.assertIsInstance(app.wsgi_app, ProxyFix)

    def test_shared_pam_app_no_longer_registers_fulcrum_blueprint(self):
        app = create_main_app()
        self.assertNotIn("fulcrum", app.blueprints)


if __name__ == "__main__":
    unittest.main()




