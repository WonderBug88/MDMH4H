import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

APP_ROOT = ROOT / "MDMH4H"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from app.fulcrum.rendering import (
    build_links_html,
    build_preview_payload,
    category_theme_hook_present,
    theme_hook_present,
)


class FulcrumRenderingTests(unittest.TestCase):
    def test_build_links_html_prefers_explicit_anchor_label(self):
        calls: list[tuple[tuple, dict]] = []

        def _builder(*args, **kwargs):
            calls.append((args, kwargs))
            return "Fallback Label"

        html = build_links_html(
            [
                {
                    "target_url": "/target-path/",
                    "anchor_label": "Chosen Label",
                }
            ],
            build_anchor_label=_builder,
        )

        self.assertIn("Chosen Label", html)
        self.assertEqual(calls, [])

    def test_build_preview_payload_for_category_combines_category_and_product_sections(self):
        def _link_builder(rows, section_title="Related options"):
            if not rows:
                return None
            urls = ",".join(row["target_url"] for row in rows)
            return f"{section_title}:{urls}"

        payload = build_preview_payload(
            source_product_id=42,
            source_entity_type="category",
            rows=[
                {"target_entity_type": "category", "target_url": "/category-a/"},
                {"target_entity_type": "product", "target_url": "/product-a/"},
            ],
            build_links_html=_link_builder,
        )

        self.assertEqual(payload["source_product_id"], 42)
        self.assertEqual(payload["row_count"], 2)
        self.assertEqual(
            payload["html"],
            "Related Categories:/category-a/\nShop Matching Products:/product-a/",
        )

    def test_theme_hook_checks_expected_metafields(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            product_template = tmp_path / "product.html"
            category_template = tmp_path / "category.html"

            product_template.write_text(
                '{{product.metafields(namespace: "h4h")}}{{product.metafields.internal_links_html}}',
                encoding="utf-8",
            )
            category_template.write_text(
                '{{category.metafields(namespace: "h4h")}}'
                '{{category.metafields.internal_category_links_html}}'
                '{{category.metafields.internal_product_links_html}}',
                encoding="utf-8",
            )

            self.assertTrue(theme_hook_present(product_template))
            self.assertTrue(category_theme_hook_present(category_template))

    def test_theme_hook_checks_fail_when_template_is_missing_fields(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            incomplete_template = tmp_path / "missing.html"
            incomplete_template.write_text("{{product.name}}", encoding="utf-8")

            self.assertFalse(theme_hook_present(incomplete_template))
            self.assertFalse(category_theme_hook_present(incomplete_template))


if __name__ == "__main__":
    unittest.main()
