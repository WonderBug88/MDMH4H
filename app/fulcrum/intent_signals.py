"""Intent-signal enrichment helpers for Fulcrum."""

from __future__ import annotations

from collections import defaultdict
import json
import os
from typing import Any, Callable

from psycopg2.extras import RealDictCursor, execute_batch


def replace_store_intent_signal_enrichments(
    store_hash: str,
    rows: list[dict[str, Any]],
    *,
    normalize_store_hash_fn: Callable[[str | None], str],
    dedupe_intent_signal_rows_fn: Callable[[list[dict[str, Any]]], list[dict[str, Any]]],
    get_pg_conn_fn: Callable[[], Any],
) -> int:
    normalized_hash = normalize_store_hash_fn(store_hash)
    deduped_rows = dedupe_intent_signal_rows_fn(rows)
    delete_sql = """
        DELETE FROM app_runtime.store_intent_signal_enrichments
        WHERE store_hash = %s
          AND source <> 'manual';
    """
    insert_sql = """
        INSERT INTO app_runtime.store_intent_signal_enrichments (
            store_hash,
            signal_kind,
            raw_label,
            normalized_label,
            scope_kind,
            entity_type,
            entity_id,
            confidence,
            source,
            status,
            metadata,
            created_at,
            updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW(), NOW()
        );
    """
    records = [
        (
            normalized_hash,
            row.get("signal_kind"),
            row.get("raw_label"),
            row.get("normalized_label"),
            row.get("scope_kind"),
            row.get("entity_type"),
            row.get("entity_id"),
            round(float(row.get("confidence") or 0.0), 2),
            row.get("source") or "deterministic",
            row.get("status") or "active",
            json.dumps(row.get("metadata") or {}),
        )
        for row in deduped_rows
    ]
    with get_pg_conn_fn() as conn:
        with conn.cursor() as cur:
            cur.execute(delete_sql, (normalized_hash,))
            if records:
                execute_batch(cur, insert_sql, records, page_size=500)
        conn.commit()
    return len(deduped_rows)


def load_store_intent_signal_enrichments(
    store_hash: str,
    active_only: bool = True,
    *,
    normalize_store_hash_fn: Callable[[str | None], str],
    get_pg_conn_fn: Callable[[], Any],
    tokenize_intent_text_fn: Callable[[str | None], set[str]],
) -> list[dict[str, Any]]:
    normalized_hash = normalize_store_hash_fn(store_hash)
    sql = """
        SELECT
            enrichment_id,
            store_hash,
            signal_kind,
            raw_label,
            normalized_label,
            scope_kind,
            entity_type,
            entity_id,
            confidence,
            source,
            status,
            metadata,
            created_at,
            updated_at
        FROM app_runtime.store_intent_signal_enrichments
        WHERE store_hash = %s
          AND (%s = FALSE OR status = 'active')
        ORDER BY
            CASE source
                WHEN 'manual' THEN 0
                WHEN 'deterministic' THEN 1
                WHEN 'agent' THEN 2
                ELSE 3
            END,
            confidence DESC,
            updated_at DESC;
    """
    with get_pg_conn_fn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (normalized_hash, active_only))
            rows = [dict(row) for row in cur.fetchall()]
    for row in rows:
        row["tokens"] = tokenize_intent_text_fn(row.get("normalized_label") or row.get("raw_label"))
    return rows


def load_store_variant_sku_rows(
    product_ids: list[int],
    *,
    get_pg_conn_fn: Callable[[], Any],
) -> list[dict[str, Any]]:
    normalized_ids = sorted({int(product_id) for product_id in product_ids if int(product_id or 0)})
    if not normalized_ids:
        return []
    sql = """
        SELECT product_id, sku, 'product' AS scope_kind
        FROM h4h_import2.product_sku_mapping
        WHERE product_id = ANY(%s)
          AND sku IS NOT NULL
          AND sku <> ''
        UNION ALL
        SELECT product_id, sku, 'variant' AS scope_kind
        FROM h4h_import2.variants
        WHERE product_id = ANY(%s)
          AND sku IS NOT NULL
          AND sku <> ''
          AND COALESCE(deleted, FALSE) = FALSE;
    """
    with get_pg_conn_fn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (normalized_ids, normalized_ids))
            return [dict(row) for row in cur.fetchall()]


def valid_brand_alias_token(
    token: str,
    *,
    generic_brand_alias_tokens: set[str],
    topic_priority: set[str],
    material_tokens: set[str],
    form_tokens: set[str],
    generic_routing_tokens: set[str],
    query_noise_words: set[str],
) -> bool:
    normalized = (token or "").strip().lower()
    if not normalized:
        return False
    if normalized in generic_brand_alias_tokens:
        return False
    if normalized in topic_priority or normalized in material_tokens or normalized in form_tokens:
        return False
    if normalized in generic_routing_tokens or normalized in query_noise_words:
        return False
    return len(normalized) >= 5 or any(char.isdigit() for char in normalized)


def derive_collection_seed_from_product(
    profile: dict[str, Any],
    category_topic_tokens: set[str],
    *,
    ordered_intent_tokens_fn: Callable[[str | None], list[str]],
    tokenize_intent_text_fn: Callable[[str | None], set[str]],
    has_model_or_sku_signal_fn: Callable[[str | None], bool],
    topic_priority: set[str],
    topic_display_map: dict[str, str],
    generic_routing_tokens: set[str],
    context_keep_tokens: set[str],
    query_noise_words: set[str],
    material_tokens: set[str],
    form_tokens: set[str],
) -> str:
    ordered_tokens = ordered_intent_tokens_fn(profile.get("name") or "")
    if not ordered_tokens:
        return ""
    brand_tokens = tokenize_intent_text_fn(profile.get("brand_name"))
    attr_tokens: set[str] = set()
    for bucket, values in (profile.get("attributes") or {}).items():
        for value in values or []:
            attr_tokens |= tokenize_intent_text_fn(value)
        if bucket in {"size", "pack_size"}:
            attr_tokens |= set(values or [])
    skip_tokens = (
        brand_tokens
        | attr_tokens
        | category_topic_tokens
        | topic_priority
        | set(topic_display_map.keys())
        | generic_routing_tokens
        | context_keep_tokens
        | query_noise_words
        | material_tokens
        | form_tokens
    )
    for token in ordered_tokens:
        if token in skip_tokens or token.isdigit():
            continue
        if has_model_or_sku_signal_fn(token):
            continue
        if len(token) < 4:
            continue
        return token
    return ""


def label_ambiguous_intent_signals_with_agent(
    store_hash: str,
    ambiguous_items: list[dict[str, Any]],
    *,
    intent_signal_row_fn: Callable[..., dict[str, Any]],
    intent_signal_agent_auto_apply_confidence: float,
) -> list[dict[str, Any]]:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key or not ambiguous_items:
        return []

    try:
        from openai import OpenAI

        client = OpenAI(api_key=api_key)
        results: list[dict[str, Any]] = []
        model = os.getenv("FULCRUM_INTENT_SIGNAL_AGENT_MODEL", "gpt-4o-mini")
        for batch_start in range(0, len(ambiguous_items), 20):
            batch = ambiguous_items[batch_start:batch_start + 20]
            prompt = {
                "task": "Classify ambiguous ecommerce routing labels for a BigCommerce catalog.",
                "allowed_signal_kinds": ["brand_alias", "hard_attribute", "soft_attribute", "collection", "topic_token", "sku_pattern"],
                "instructions": [
                    "Return JSON only.",
                    "Choose exactly one signal_kind for each row.",
                    "Use collection for product-line labels like Courtyard or Dynasty.",
                    "Use hard_attribute for size, pack size, quantity, or variant-defining selectors.",
                    "Use soft_attribute for color, material, form, finish, or style-like modifiers.",
                    "Use topic_token for product family/category taxonomy labels.",
                    "Use brand_alias for brand-led labels.",
                    "Use sku_pattern only for model/SKU-like strings.",
                ],
                "rows": batch,
            }
            response = client.chat.completions.create(
                model=model,
                temperature=0,
                messages=[
                    {
                        "role": "system",
                        "content": "You classify ambiguous ecommerce catalog labels into routing signal kinds. Reply with valid JSON only.",
                    },
                    {"role": "user", "content": json.dumps(prompt)},
                ],
            )
            choice = response.choices[0] if getattr(response, "choices", None) else None
            message = getattr(choice, "message", None)
            content = getattr(message, "content", None) or "[]"
            parsed = json.loads(content)
            for item in parsed if isinstance(parsed, list) else []:
                raw_label = (item.get("raw_label") or "").strip()
                signal_kind = (item.get("signal_kind") or "").strip().lower()
                if signal_kind not in {"brand_alias", "hard_attribute", "soft_attribute", "collection", "topic_token", "sku_pattern"}:
                    continue
                confidence = float(item.get("confidence") or 0.0)
                status = "active" if confidence >= intent_signal_agent_auto_apply_confidence else "inactive"
                results.append(
                    intent_signal_row_fn(
                        store_hash=store_hash,
                        signal_kind=signal_kind,
                        raw_label=raw_label,
                        normalized_label=item.get("normalized_label") or raw_label,
                        scope_kind=item.get("scope_kind") or "option_value",
                        entity_type=item.get("entity_type"),
                        entity_id=item.get("entity_id"),
                        confidence=confidence,
                        source="agent",
                        status=status,
                        metadata={
                            "rationale": item.get("rationale") or "",
                            "candidate_signal_kinds": item.get("candidate_signal_kinds") or [],
                        },
                    )
                )
        return results
    except Exception:
        return []


def refresh_store_intent_signal_enrichments(
    store_hash: str,
    initiated_by: str | None = None,
    *,
    normalize_store_hash_fn: Callable[[str | None], str],
    load_all_store_product_profiles_fn: Callable[[str], list[dict[str, Any]]],
    load_store_category_profiles_fn: Callable[[str], dict[str, dict[str, Any]]],
    load_store_brand_profiles_fn: Callable[[str], dict[str, dict[str, Any]]],
    normalize_signal_label_fn: Callable[[str | None], str],
    intent_signal_row_fn: Callable[..., dict[str, Any]],
    tokenize_intent_text_fn: Callable[[str | None], set[str]],
    non_generic_signal_tokens_fn: Callable[[set[str]], set[str]],
    valid_brand_alias_token_fn: Callable[[str], bool],
    infer_bucket_from_option_name_fn: Callable[[str | None, list[str] | None], tuple[str | None, float]],
    signal_kind_from_bucket_fn: Callable[[str | None], str | None],
    canonicalize_attribute_value_fn: Callable[[str, str | None], str],
    derive_collection_seed_from_product_fn: Callable[[dict[str, Any], set[str]], str],
    load_store_variant_sku_rows_fn: Callable[[list[int]], list[dict[str, Any]]],
    semantic_builtin_enrichment_rows_fn: Callable[[str], list[dict[str, Any]]],
    label_ambiguous_intent_signals_with_agent_fn: Callable[[str, list[dict[str, Any]]], list[dict[str, Any]]],
    replace_store_intent_signal_enrichments_fn: Callable[[str, list[dict[str, Any]]], int],
    dedupe_intent_signal_rows_fn: Callable[[list[dict[str, Any]]], list[dict[str, Any]]],
    intent_signal_collection_min_repeat: int,
) -> dict[str, Any]:
    normalized_hash = normalize_store_hash_fn(store_hash)
    product_profiles = load_all_store_product_profiles_fn(normalized_hash)
    category_profiles = list(load_store_category_profiles_fn(normalized_hash).values())
    brand_profiles = list(load_store_brand_profiles_fn(normalized_hash).values())

    deterministic_rows: list[dict[str, Any]] = []
    ambiguous_items: list[dict[str, Any]] = []
    category_topic_tokens: set[str] = set()

    for category_profile in category_profiles:
        category_name = category_profile.get("name") or ""
        normalized_label = normalize_signal_label_fn(category_name)
        if normalized_label:
            deterministic_rows.append(
                intent_signal_row_fn(
                    store_hash=normalized_hash,
                    signal_kind="topic_token",
                    raw_label=category_name,
                    normalized_label=normalized_label,
                    scope_kind="category_name",
                    entity_type="category",
                    entity_id=category_profile.get("bc_category_id"),
                    confidence=0.96,
                    source="deterministic",
                    status="active",
                    metadata={"initiated_by": initiated_by or "fulcrum", "topic_tokens": sorted(tokenize_intent_text_fn(normalized_label))},
                )
            )
            category_topic_tokens |= non_generic_signal_tokens_fn(tokenize_intent_text_fn(normalized_label))

    for brand_profile in brand_profiles:
        brand_name = brand_profile.get("name") or brand_profile.get("brand_name") or ""
        if not brand_name:
            continue
        normalized_label = normalize_signal_label_fn(brand_name)
        if normalized_label:
            deterministic_rows.append(
                intent_signal_row_fn(
                    store_hash=normalized_hash,
                    signal_kind="brand_alias",
                    raw_label=brand_name,
                    normalized_label=normalized_label,
                    scope_kind="brand_name",
                    entity_type="brand",
                    entity_id=brand_profile.get("bc_entity_id"),
                    confidence=0.98,
                    source="deterministic",
                    status="active",
                    metadata={"initiated_by": initiated_by or "fulcrum"},
                )
            )
        for token in tokenize_intent_text_fn(brand_name):
            if not valid_brand_alias_token_fn(token):
                continue
            deterministic_rows.append(
                intent_signal_row_fn(
                    store_hash=normalized_hash,
                    signal_kind="brand_alias",
                    raw_label=token,
                    normalized_label=token,
                    scope_kind="brand_name",
                    entity_type="brand",
                    entity_id=brand_profile.get("bc_entity_id"),
                    confidence=0.9,
                    source="deterministic",
                    status="active",
                    metadata={"alias_of": brand_name, "initiated_by": initiated_by or "fulcrum"},
                )
            )

    collection_candidates: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    for product_profile in product_profiles:
        product_id = int(product_profile.get("bc_product_id") or 0)
        source_data = product_profile.get("source_data") or {}
        option_pairs = source_data.get("option_pairs") or []
        for pair in option_pairs:
            raw_name = (pair.get("name") or "").strip()
            raw_value = (pair.get("value") or "").strip()
            if not raw_value:
                continue
            bucket_key, bucket_confidence = infer_bucket_from_option_name_fn(raw_name, [raw_value])
            signal_kind = signal_kind_from_bucket_fn(bucket_key)
            canonical_value = canonicalize_attribute_value_fn(bucket_key, raw_value) if bucket_key else ""
            if signal_kind in {"hard_attribute", "soft_attribute"} and canonical_value:
                deterministic_rows.append(
                    intent_signal_row_fn(
                        store_hash=normalized_hash,
                        signal_kind=signal_kind,
                        raw_label=raw_value,
                        normalized_label=canonical_value,
                        scope_kind="option_value",
                        entity_type="product",
                        entity_id=product_id,
                        confidence=max(0.72, float(bucket_confidence or 0.0)),
                        source="deterministic",
                        status="active",
                        metadata={
                            "bucket_key": bucket_key,
                            "option_name": raw_name,
                            "initiated_by": initiated_by or "fulcrum",
                        },
                    )
                )
                continue
            option_name_tokens = tokenize_intent_text_fn(raw_name)
            if option_name_tokens & {"collection", "series", "program", "style"}:
                normalized_collection = normalize_signal_label_fn(raw_value)
                if normalized_collection:
                    deterministic_rows.append(
                        intent_signal_row_fn(
                            store_hash=normalized_hash,
                            signal_kind="collection",
                            raw_label=raw_value,
                            normalized_label=normalized_collection,
                            scope_kind="option_value",
                            entity_type="product",
                            entity_id=product_id,
                            confidence=0.97,
                            source="deterministic",
                            status="active",
                            metadata={"option_name": raw_name, "initiated_by": initiated_by or "fulcrum"},
                        )
                    )
                    continue
            raw_value_tokens = tokenize_intent_text_fn(raw_value)
            if raw_value_tokens and not signal_kind:
                ambiguous_items.append(
                    {
                        "raw_label": raw_value,
                        "scope_kind": "option_value",
                        "entity_type": "product",
                        "entity_id": product_id,
                        "candidate_signal_kinds": ["hard_attribute", "soft_attribute", "collection", "topic_token"],
                        "context": {
                            "option_name": raw_name,
                            "product_name": product_profile.get("name"),
                            "brand_name": product_profile.get("brand_name"),
                        },
                    }
                )

        for bucket_key, values in (product_profile.get("attributes") or {}).items():
            signal_kind = signal_kind_from_bucket_fn(bucket_key)
            if signal_kind not in {"hard_attribute", "soft_attribute"}:
                continue
            for value in values or []:
                deterministic_rows.append(
                    intent_signal_row_fn(
                        store_hash=normalized_hash,
                        signal_kind=signal_kind,
                        raw_label=value,
                        normalized_label=value,
                        scope_kind="product_title",
                        entity_type="product",
                        entity_id=product_id,
                        confidence=0.74,
                        source="deterministic",
                        status="active",
                        metadata={"bucket_key": bucket_key, "initiated_by": initiated_by or "fulcrum"},
                    )
                )

        collection_seed = derive_collection_seed_from_product_fn(product_profile, category_topic_tokens)
        if collection_seed:
            collection_candidates[collection_seed].append(
                {
                    "entity_id": product_id,
                    "product_name": product_profile.get("name"),
                    "brand_name": product_profile.get("brand_name"),
                }
            )

    for collection_label, owners in collection_candidates.items():
        if len(owners) >= intent_signal_collection_min_repeat:
            for owner in owners:
                deterministic_rows.append(
                    intent_signal_row_fn(
                        store_hash=normalized_hash,
                        signal_kind="collection",
                        raw_label=collection_label,
                        normalized_label=collection_label,
                        scope_kind="product_title",
                        entity_type="product",
                        entity_id=owner.get("entity_id"),
                        confidence=0.86,
                        source="deterministic",
                        status="active",
                        metadata={
                            "support_count": len(owners),
                            "product_name": owner.get("product_name"),
                            "brand_name": owner.get("brand_name"),
                            "initiated_by": initiated_by or "fulcrum",
                        },
                    )
                )
        elif len(collection_label) >= 6:
            owner = owners[0]
            deterministic_rows.append(
                intent_signal_row_fn(
                    store_hash=normalized_hash,
                    signal_kind="collection",
                    raw_label=collection_label,
                    normalized_label=collection_label,
                    scope_kind="product_title",
                    entity_type="product",
                    entity_id=owner.get("entity_id"),
                    confidence=0.72,
                    source="deterministic",
                    status="active",
                    metadata={
                        "support_count": len(owners),
                        "product_name": owner.get("product_name"),
                        "brand_name": owner.get("brand_name"),
                        "initiated_by": initiated_by or "fulcrum",
                        "single_product_seed": True,
                    },
                )
            )
        else:
            owner = owners[0]
            ambiguous_items.append(
                {
                    "raw_label": collection_label,
                    "scope_kind": "product_title",
                    "entity_type": "product",
                    "entity_id": owner.get("entity_id"),
                    "candidate_signal_kinds": ["collection", "topic_token", "brand_alias"],
                    "context": {
                        "product_name": owner.get("product_name"),
                        "brand_name": owner.get("brand_name"),
                    },
                }
            )

    sku_rows = load_store_variant_sku_rows_fn([int(profile.get("bc_product_id") or 0) for profile in product_profiles])
    for sku_row in sku_rows:
        sku = (sku_row.get("sku") or "").strip()
        normalized_sku = sku.lower()
        if not normalized_sku:
            continue
        deterministic_rows.append(
            intent_signal_row_fn(
                store_hash=normalized_hash,
                signal_kind="sku_pattern",
                raw_label=sku,
                normalized_label=normalized_sku,
                scope_kind="sku",
                entity_type="product",
                entity_id=sku_row.get("product_id"),
                confidence=0.99,
                source="deterministic",
                status="active",
                metadata={"sku_scope": sku_row.get("scope_kind"), "initiated_by": initiated_by or "fulcrum"},
            )
        )

    deterministic_rows.extend(semantic_builtin_enrichment_rows_fn(normalized_hash))
    agent_rows = label_ambiguous_intent_signals_with_agent_fn(normalized_hash, ambiguous_items)
    stored_count = replace_store_intent_signal_enrichments_fn(normalized_hash, deterministic_rows + agent_rows)
    return {
        "store_hash": normalized_hash,
        "stored_signals": stored_count,
        "deterministic_signal_count": len(dedupe_intent_signal_rows_fn(deterministic_rows)),
        "agent_signal_count": len(dedupe_intent_signal_rows_fn(agent_rows)),
        "ambiguous_signal_count": len(ambiguous_items),
    }


__all__ = [
    "derive_collection_seed_from_product",
    "label_ambiguous_intent_signals_with_agent",
    "load_store_intent_signal_enrichments",
    "load_store_variant_sku_rows",
    "refresh_store_intent_signal_enrichments",
    "replace_store_intent_signal_enrichments",
    "valid_brand_alias_token",
]
