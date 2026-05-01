"""Rendering helpers for Fulcrum preview and published HTML blocks."""

from pathlib import Path
from typing import Any, Callable


AnchorLabelBuilder = Callable[..., str]
LinkHtmlBuilder = Callable[[list[dict[str, Any]], str], str | None]


def build_links_html(
    rows: list[dict[str, Any]],
    *,
    build_anchor_label: AnchorLabelBuilder,
    section_title: str = "Related options",
) -> str | None:
    if not rows:
        return None

    lines = []
    lines.append("<!-- Fulcrum auto-generated internal links -->")
    lines.append("<style>")
    lines.append(".h4h-internal-links { margin: 2rem 0 1.5rem 0; padding: 0.75rem 0; border-top: 1px solid #e5e5e5; border-bottom: 1px solid #e5e5e5; }")
    lines.append(".h4h-internal-links h3 { margin: 0 0 0.5rem 0; font-size: 0.95rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.04em; color: #555; }")
    lines.append(".h4h-internal-links ul { list-style: none; margin: 0; padding: 0; display: flex; flex-wrap: wrap; gap: 0.5rem 0.75rem; align-items: center; }")
    lines.append(".h4h-internal-links li { margin: 0; padding: 0; }")
    lines.append(".h4h-internal-links a { display: inline-block; padding: 0.35rem 0.85rem; border-radius: 999px; border: 1px solid #e0e0e0; font-size: 0.9rem; text-decoration: none; color: #333; background: #fff; }")
    lines.append(".h4h-internal-links a:hover { border-color: #ccc; background: #f5f5f5; }")
    lines.append("</style>")
    lines.append('<section class="h4h-internal-links">')
    lines.append(f"  <h3>{section_title}</h3>")
    lines.append("  <ul>")
    for row in rows:
        label = row.get("anchor_label") or build_anchor_label(
            row.get("relation_type"),
            row.get("example_query"),
            row.get("target_url"),
            target_name=row.get("target_name"),
            source_name=row.get("source_name"),
        )
        lines.append(f'    <li><a href="{row["target_url"]}">{label}</a></li>')
    lines.append("  </ul>")
    lines.append("</section>")
    return "\n".join(lines)


def build_preview_payload(
    *,
    source_product_id: int,
    source_entity_type: str,
    rows: list[dict[str, Any]],
    build_links_html: LinkHtmlBuilder,
) -> dict[str, Any]:
    category_rows = [row for row in rows if (row.get("target_entity_type") or "product") == "category"]
    product_rows = [row for row in rows if (row.get("target_entity_type") or "product") == "product"]
    if source_entity_type == "category":
        html = "\n".join(
            html
            for html in [
                build_links_html(category_rows, "Related Categories"),
                build_links_html(product_rows, "Shop Matching Products"),
            ]
            if html
        ) or None
    else:
        html = build_links_html(rows, "Related options")

    return {
        "source_product_id": source_product_id,
        "source_entity_type": source_entity_type,
        "row_count": len(rows),
        "rows": rows,
        "html": html,
    }


def theme_hook_present(template_path: str | Path) -> bool:
    path = Path(template_path)
    if not path.exists():
        return False
    text = path.read_text(encoding="utf-8", errors="ignore")
    return "internal_links_html" in text and 'metafields(namespace: "h4h"' in text


def category_theme_hook_present(template_path: str | Path) -> bool:
    category_template = Path(template_path)
    if not category_template.exists():
        return False
    text = category_template.read_text(encoding="utf-8", errors="ignore")
    return (
        "internal_category_links_html" in text
        and "internal_product_links_html" in text
        and 'metafields(namespace: "h4h"' in text
    )
