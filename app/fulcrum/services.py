from datetime import datetime, timedelta
from functools import lru_cache
import json
import os
import re
import subprocess
import sys
import threading
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import psycopg2
from psycopg2.extras import RealDictCursor, execute_batch

from app.fulcrum.admin_cache import (
    invalidate_admin_metric_cache as cache_invalidate_admin_metric_cache,
    json_cache_safe as cache_json_cache_safe,
    load_admin_metric_cache as cache_load_admin_metric_cache,
    store_admin_metric_cache as cache_store_admin_metric_cache,
)
from app.fulcrum.admin_metrics import (
    candidate_gsc_page_values as metrics_candidate_gsc_page_values,
    format_change_value as metrics_format_change_value,
    get_cached_live_gsc_performance as metrics_get_cached_live_gsc_performance,
    percent_share as metrics_percent_share,
    summarize_blocked_gate_families as metrics_summarize_blocked_gate_families,
    summarize_gsc_alignment as metrics_summarize_gsc_alignment,
    summarize_gsc_routing_coverage as metrics_summarize_gsc_routing_coverage,
    summarize_live_gsc_performance as metrics_summarize_live_gsc_performance,
)
from app.fulcrum.agent_review import (
    agent_review_signal_snapshot as review_agent_review_signal_snapshot,
    gate_review_cluster_values as review_gate_review_cluster_values,
    list_query_gate_agent_review_clusters as review_list_query_gate_agent_review_clusters,
    list_query_gate_agent_reviews as review_list_query_gate_agent_reviews,
    load_query_gate_agent_review_map as review_load_query_gate_agent_review_map,
    normalize_gate_review_item as review_normalize_gate_review_item,
    parse_agent_json_list as review_parse_agent_json_list,
    postprocess_gate_agent_reviews as review_postprocess_gate_agent_reviews,
    review_query_gate_rows_with_agent as review_query_gate_rows_with_agent_impl,
    run_query_gate_agent_review as review_run_query_gate_agent_review,
    serialize_query_gate_row_for_agent_review as review_serialize_query_gate_row_for_agent_review,
    store_query_gate_agent_reviews as review_store_query_gate_agent_reviews,
    summarize_query_gate_agent_reviews as review_summarize_query_gate_agent_reviews,
)
from app.fulcrum.anchor_labels import (
    canonical_word_token as anchor_canonical_word_token,
    extract_label_candidates as anchor_extract_label_candidates,
    is_noise_fragment as anchor_is_noise_fragment,
    label_from_target_url as anchor_label_from_target_url,
    legacy_fallback_anchor_label as anchor_legacy_fallback_anchor_label,
    looks_generic_phrase as anchor_looks_generic_phrase,
    normalize_anchor_text as anchor_normalize_anchor_text,
    ordered_focus_terms as anchor_ordered_focus_terms,
    ordered_size_terms as anchor_ordered_size_terms,
    select_anchor_label as anchor_select_anchor_label,
    select_category_product_anchor_label as anchor_select_category_product_anchor_label,
    title_case_anchor as anchor_title_case_anchor,
    trim_phrase_tokens as anchor_trim_phrase_tokens,
)
from app.fulcrum.changed_route_review import (
    attach_changed_route_agent_reviews as review_attach_changed_route_agent_reviews,
    changed_route_review_next_step_label as review_changed_route_review_next_step_label,
    fallback_changed_route_review_reasoning as review_fallback_changed_route_review_reasoning,
    get_cached_changed_route_review_reasoning as review_get_cached_changed_route_review_reasoning,
    reason_about_changed_route_reviews_with_agent as review_reason_about_changed_route_reviews_with_agent,
    run_changed_route_agent_review as review_run_changed_route_agent_review,
    summarize_changed_route_agent_reviews as review_summarize_changed_route_agent_reviews,
)
from app.fulcrum.dashboard_read_model import (
    gate_review_map_for_ids as dashboard_gate_review_map_for_ids,
    get_cached_changed_route_results as dashboard_get_cached_changed_route_results,
    list_changed_route_results as dashboard_list_changed_route_results,
    matches_changed_route_search as dashboard_matches_changed_route_search,
    sorted_changed_route_rows as dashboard_sorted_changed_route_rows,
    summarize_changed_route_rows as dashboard_summarize_changed_route_rows,
)
from app.fulcrum.dashboard_context import (
    admin_context_defaults as dashboard_admin_context_defaults,
    build_dashboard_context as dashboard_build_dashboard_context,
    build_public_dashboard_data as dashboard_build_public_dashboard_data,
    populate_edge_case_admin_context as dashboard_populate_edge_case_admin_context,
    populate_changed_route_admin_context as dashboard_populate_changed_route_admin_context,
)
from app.fulcrum.merchant_setup import (
    apply_theme_automatic_fix as merchant_apply_theme_automatic_fix,
    build_store_readiness_snapshot as merchant_build_store_readiness_snapshot,
    build_google_authorization_url as merchant_build_google_authorization_url,
    build_setup_context as merchant_build_setup_context,
    complete_google_oauth as merchant_complete_google_oauth,
    decode_google_oauth_state as merchant_decode_google_oauth_state,
    enqueue_integration_sync as merchant_enqueue_integration_sync,
    evaluate_theme_verification as merchant_evaluate_theme_verification,
    get_store_integration_data_summary as merchant_get_store_integration_data_summary,
    get_store_publish_settings as merchant_get_store_publish_settings,
    list_store_integrations as merchant_list_store_integrations,
    merchant_landing_path as merchant_landing_path_impl,
    process_queued_integration_syncs as merchant_process_queued_integration_syncs,
    purge_store_data_on_uninstall as merchant_purge_store_data_on_uninstall,
    run_integration_sync_run as merchant_run_integration_sync_run,
    select_google_resource as merchant_select_google_resource,
    sync_bigcommerce_integration as merchant_sync_bigcommerce_integration,
    upsert_store_publish_settings as merchant_upsert_store_publish_settings,
)
from app.fulcrum.ops_snapshot import (
    alert_severity_rank as snapshot_alert_severity_rank,
    alert_tone_for_severity as snapshot_alert_tone_for_severity,
    build_operational_snapshot as snapshot_build_operational_snapshot,
    format_relative_time as snapshot_format_relative_time,
    format_timestamp_display as snapshot_format_timestamp_display,
)
from app.fulcrum.ga4_signals import build_ga4_signal as ga4_build_ga4_signal
from app.fulcrum.config import Config
from app.fulcrum.catalog import sync_store_catalog_profiles as catalog_sync_store_catalog_profiles
from app.fulcrum.candidate_runs import (
    auto_approve_and_publish_run as runs_auto_approve_and_publish_run,
    candidate_source_key as runs_candidate_source_key,
    eligible_auto_publish_candidates as runs_eligible_auto_publish_candidates,
    execute_candidate_run as runs_execute_candidate_run,
    execute_candidate_run_impl as runs_execute_candidate_run_impl,
    generate_candidate_run as runs_generate_candidate_run,
    publish_all_current_results as runs_publish_all_current_results,
    queue_candidate_run as runs_queue_candidate_run,
    review_request_source_key as runs_review_request_source_key,
)
from app.fulcrum.cluster_profile import build_cluster_profile as cluster_build_cluster_profile
from app.fulcrum.candidates import (
    count_candidates_by_statuses as candidates_count_candidates_by_statuses,
    count_pending_candidates as candidates_count_pending_candidates,
    get_approved_rows_for_source as candidates_get_approved_rows_for_source,
    include_dashboard_candidate as candidates_include_dashboard_candidate,
    latest_candidate_rows_for_store as candidates_latest_candidate_rows_for_store,
    list_approved_sources as candidates_list_approved_sources,
    list_candidates as candidates_list_candidates,
    review_candidates as candidates_review_candidates,
)
from app.fulcrum.platform import (
    _resolve_store_token as platform_resolve_store_token,
    decode_signed_payload as platform_decode_signed_payload,
    exchange_auth_code as platform_exchange_auth_code,
    get_bc_headers,
    get_pg_conn,
    get_store_owner_email,
    list_store_brands as platform_list_store_brands,
    mark_store_uninstalled as platform_mark_store_uninstalled,
    merge_store_installation_metadata as platform_merge_store_installation_metadata,
    normalize_store_hash as platform_normalize_store_hash,
    resolve_store_category_id_by_url,
    resolve_store_product_id_by_url,
    sync_store_storefront_sites as platform_sync_store_storefront_sites,
    upsert_store_installation as platform_upsert_store_installation,
)
from app.fulcrum.profile_loaders import (
    dedupe_entity_profiles as loaders_dedupe_entity_profiles,
    humanize_url_path_title as loaders_humanize_url_path_title,
    load_ga4_page_metrics as loaders_load_ga4_page_metrics,
    load_product_profiles as loaders_load_product_profiles,
    load_reserved_storefront_urls as loaders_load_reserved_storefront_urls,
    load_store_brand_profiles as loaders_load_store_brand_profiles,
    load_store_category_profiles as loaders_load_store_category_profiles,
    load_store_content_profiles as loaders_load_store_content_profiles,
    load_store_product_profiles as loaders_load_store_product_profiles,
    looks_like_content_path as loaders_looks_like_content_path,
    synthetic_content_entity_id as loaders_synthetic_content_entity_id,
)
from app.fulcrum.query_suggestions import (
    annotate_query_gate_rows_with_suggestions as suggestions_annotate_query_gate_rows_with_suggestions,
    attach_cached_query_gate_suggestions as suggestions_attach_cached_query_gate_suggestions,
    load_query_target_overrides as suggestions_load_query_target_overrides,
    query_target_override_key as suggestions_query_target_override_key,
    refresh_query_gate_suggestion_cache as suggestions_refresh_query_gate_suggestion_cache,
    serialize_query_gate_target_snapshot as suggestions_serialize_query_gate_target_snapshot,
    set_query_target_override as suggestions_set_query_target_override,
)
from app.fulcrum.query_signal_analysis import (
    build_fallback_query_signal_context as signal_build_fallback_query_signal_context,
    build_query_semantics_analysis as signal_build_query_semantics_analysis,
    classify_query_intent_from_signals as signal_classify_query_intent_from_signals,
    classify_query_intent_scope as signal_classify_query_intent_scope,
    match_semantic_signal_entries as signal_match_semantic_signal_entries,
    match_store_signal_entries as signal_match_store_signal_entries,
    query_has_exact_brand_phrase as signal_query_has_exact_brand_phrase,
    query_is_broad_descriptive as signal_query_is_broad_descriptive,
    resolve_query_signal_context as signal_resolve_query_signal_context,
    semantic_family_candidate_tokens as signal_semantic_family_candidate_tokens,
    semantic_head_family as signal_semantic_head_family,
    semantic_head_term as signal_semantic_head_term,
    semantic_head_term_from_phrases as signal_semantic_head_term_from_phrases,
    semantic_token_roles as signal_semantic_token_roles,
)
from app.fulcrum.query_gate_builder import (
    build_query_gate_record as gate_build_query_gate_record,
)
from app.fulcrum.intent_profile import build_intent_profile as intent_build_intent_profile
from app.fulcrum.intent_signals import (
    derive_collection_seed_from_product as signals_derive_collection_seed_from_product,
    label_ambiguous_intent_signals_with_agent as signals_label_ambiguous_intent_signals_with_agent,
    load_store_intent_signal_enrichments as signals_load_store_intent_signal_enrichments,
    load_store_variant_sku_rows as signals_load_store_variant_sku_rows,
    refresh_store_intent_signal_enrichments as signals_refresh_store_intent_signal_enrichments,
    replace_store_intent_signal_enrichments as signals_replace_store_intent_signal_enrichments,
    valid_brand_alias_token as signals_valid_brand_alias_token,
)
from app.fulcrum.direct_routing import (
    direct_route_candidates_from_gsc as direct_direct_route_candidates_from_gsc,
    entity_type_fit_adjustment as direct_entity_type_fit_adjustment,
    looks_informational_query as direct_looks_informational_query,
    target_prefilter as direct_target_prefilter,
)
from app.fulcrum.entity_index import (
    build_unified_entity_index as entity_build_unified_entity_index,
    load_all_store_product_profiles as entity_load_all_store_product_profiles,
    profile_matches_cluster as entity_profile_matches_cluster,
)
from app.fulcrum.category_sources import (
    build_category_descendants as category_build_category_descendants,
    build_pdp_category_competition_candidate as category_build_pdp_category_competition_candidate,
    category_anchor_label_for_cluster as category_category_anchor_label_for_cluster,
    category_competition_specificity_bonus as category_category_competition_specificity_bonus,
    generate_category_source_candidates as category_generate_category_source_candidates,
    load_canonical_cluster_categories as category_load_canonical_cluster_categories,
    shared_subclusters as category_shared_subclusters,
    store_category_competition_enabled as category_store_category_competition_enabled,
)
from app.fulcrum.review_feedback import (
    increment_review_feedback_bucket as feedback_increment_review_feedback_bucket,
    load_review_feedback_maps as feedback_load_review_feedback_maps,
)
from app.fulcrum.review_sessions import (
    create_query_gate_review_submission as sessions_create_query_gate_review_submission,
)
from app.fulcrum.routing_semantics import (
    apply_semantics_control_to_ranked_targets as semantics_apply_semantics_control_to_ranked_targets,
    gate_row_current_page_snapshot as semantics_gate_row_current_page_snapshot,
    gate_row_query_signal_context as semantics_gate_row_query_signal_context,
    gate_row_semantics_analysis as semantics_gate_row_semantics_analysis,
    semantics_target_block_reason as semantics_semantics_target_block_reason,
)
from app.fulcrum.routing_ranker import (
    append_reason_summary as ranker_append_reason_summary,
    build_review_feedback_signal as ranker_build_review_feedback_signal,
    rank_target_options_for_gate_row as ranker_rank_target_options_for_gate_row,
    refresh_query_gate_row_live_state as ranker_refresh_query_gate_row_live_state,
)
from app.fulcrum.routing_text import (
    build_fuzzy_signal as text_build_fuzzy_signal,
    fuzzy_candidate_kind as text_fuzzy_candidate_kind,
    fuzzy_match_score as text_fuzzy_match_score,
    normalize_phrase_for_match as text_normalize_phrase_for_match,
    normalize_query_family_key as text_normalize_query_family_key,
    normalize_signal_label as text_normalize_signal_label,
    ordered_intent_tokens as text_ordered_intent_tokens,
    profile_brand_label as text_profile_brand_label,
    profile_topic_label as text_profile_topic_label,
    semantic_pluralize as text_semantic_pluralize,
    tokenize_intent_text as text_tokenize_intent_text,
)
from app.fulcrum.publishing import (
    count_publications as publishing_count_publications,
    list_publications as publishing_list_publications,
    publish_approved_entities as publishing_publish_approved_entities,
    summarize_live_publications as publishing_summarize_live_publications,
    unpublish_entities as publishing_unpublish_entities,
    upsert_entity_metafield as publishing_upsert_entity_metafield,
)
from app.fulcrum.query_gate import (
    build_query_gate_records as gate_build_query_gate_records,
    get_query_gate_record_by_id as gate_get_query_gate_record_by_id,
    latest_gate_run_id as gate_latest_gate_run_id,
    list_query_gate_records as gate_list_query_gate_records,
    list_runs as gate_list_runs,
    query_gate_record_map_for_ids as gate_query_gate_record_map_for_ids,
    store_query_gate_records as gate_store_query_gate_records,
    summarize_query_gate_dispositions as gate_summarize_query_gate_dispositions,
)
from app.fulcrum.quality_reporting import (
    format_logic_change_timestamp as reporting_format_logic_change_timestamp,
    format_logic_validation_status as reporting_format_logic_validation_status,
    get_entity_coverage_summary as reporting_get_entity_coverage_summary,
    get_logic_change_summary as reporting_get_logic_change_summary,
    load_logic_change_log as reporting_load_logic_change_log,
)
from app.fulcrum.review_workflow import (
    candidate_matches_review_source as workflow_candidate_matches_review_source,
    candidate_source_bc_entity_id as workflow_candidate_source_bc_entity_id,
    candidate_matches_review_target as workflow_candidate_matches_review_target,
    candidate_target_bc_entity_id as workflow_candidate_target_bc_entity_id,
    count_query_gate_review_requests as workflow_count_query_gate_review_requests,
    get_query_gate_review_request_by_id as workflow_get_query_gate_review_request_by_id,
    list_query_gate_review_requests as workflow_list_query_gate_review_requests,
    pause_source_for_review as workflow_pause_source_for_review,
    request_query_gate_review as workflow_request_query_gate_review,
    resolve_query_gate_review_request as workflow_resolve_query_gate_review_request,
    restore_source_after_review as workflow_restore_source_after_review,
    review_all_edge_cases as workflow_review_all_edge_cases,
    update_query_gate_review_request_metadata as workflow_update_query_gate_review_request_metadata,
)
from app.fulcrum.review_presenters import (
    apply_review_target_display as presenters_apply_review_target_display,
    build_query_gate_human_review_mailto as presenters_build_query_gate_human_review_mailto,
    merge_fresh_gate_context_into_review_row as presenters_merge_fresh_gate_context_into_review_row,
    publication_posting_label as presenters_publication_posting_label,
    summarize_edge_case_requests as presenters_summarize_edge_case_requests,
)
from app.fulcrum.readiness import (
    _normalize_mapping_review_statuses as readiness_normalize_mapping_review_statuses,
    auto_resolve_pending_mappings as readiness_auto_resolve_pending_mappings,
    category_publishing_enabled_for_store as readiness_category_publishing_enabled_for_store,
    get_store_readiness as readiness_get_store_readiness,
    list_pending_mapping_reviews as readiness_list_pending_mapping_reviews,
    refresh_store_readiness as readiness_refresh_store_readiness,
    review_mapping_rows as readiness_review_mapping_rows,
)
from app.fulcrum.rendering import (
    build_links_html as render_build_links_html,
    build_preview_payload,
    category_theme_hook_present as render_category_theme_hook_present,
    theme_hook_present as render_theme_hook_present,
)
from app.fulcrum.storefront import (
    clear_storefront_site_caches as storefront_clear_storefront_site_caches,
    extract_storefront_channel_id as storefront_extract_storefront_channel_id,
    get_store_profile_summary as storefront_get_store_profile_summary,
    get_storefront_base_url as storefront_get_storefront_base_url,
    get_storefront_base_url_from_db as storefront_get_storefront_base_url_from_db,
    list_storefront_base_urls as storefront_list_storefront_base_urls,
    load_storefront_site_rows as storefront_load_storefront_site_rows,
    select_storefront_site_row as storefront_select_storefront_site_row,
)


FULCRUM_DIR = Path(__file__).resolve().parent
ROOT_DIR = Path(Config.FULCRUM_ENV_PATH).resolve().parent
RUNTIME_SQL_PATH = FULCRUM_DIR / "sql" / "fulcrum_runtime.sql"
LOGIC_CHANGELOG_PATH = Path(Config.BASE_DIR) / "docs" / "FULCRUM_LOGIC_CHANGELOG.json"
INTENT_SIGNAL_AGENT_AUTO_APPLY_CONFIDENCE = 0.82
INTENT_SIGNAL_COLLECTION_MIN_REPEAT = 2
ENTITY_STORAGE_OFFSETS = {
    "category": 1_000_000_000,
    "brand": 2_000_000_000,
    "content": 3_000_000_000,
}
PG_INT_MAX = 2_147_483_647
PG_INT_MIN = -2_147_483_648
_RUNTIME_SCHEMA_APPLIED = False
_RUNTIME_SCHEMA_LOCK = threading.Lock()
_RUNTIME_SCHEMA_REQUIRED_TABLES = {
    "store_installations",
    "store_integrations",
    "integration_sync_runs",
    "store_publish_settings",
    "store_theme_verifications",
    "store_readiness",
    "store_storefront_sites",
    "store_attribute_buckets",
    "store_option_name_mappings",
    "store_option_value_mappings",
    "store_product_profiles",
    "store_category_profiles",
    "store_gsc_daily",
    "store_ga4_pages_daily",
    "store_intent_signal_enrichments",
    "store_cluster_rules",
    "link_runs",
    "query_gate_records",
    "query_gate_agent_reviews",
    "query_gate_review_requests",
    "query_gate_review_submissions",
    "query_gate_decision_feedback",
    "query_target_overrides",
    "link_candidates",
    "link_reviews",
    "link_publications",
    "admin_metric_cache",
}
_RUNTIME_SCHEMA_REQUIRED_COLUMNS = {
    "integration_sync_runs": {
        "sync_run_id",
        "store_hash",
        "integration_key",
        "selected_resource_id",
        "status",
        "triggered_by",
        "row_count",
        "start_date",
        "end_date",
        "error_message",
        "metadata",
        "queued_at",
        "started_at",
        "finished_at",
        "updated_at",
    },
    "store_product_profiles": {
        "canonical_group_key",
        "is_canonical_target",
        "is_visible",
        "availability",
        "is_price_hidden",
        "eligible_for_routing",
    },
    "store_category_profiles": {
        "is_visible",
        "eligible_for_routing",
    },
    "link_candidates": {
        "source_entity_type",
        "target_entity_type",
        "source_entity_id",
        "target_entity_id",
    },
    "link_publications": {
        "source_entity_type",
        "source_entity_id",
        "metafield_key",
    },
}
_RUNTIME_SCHEMA_QUERY_GATE_REVIEW_SUBMISSIONS_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS app_runtime.query_gate_review_submissions (
        submission_id BIGSERIAL PRIMARY KEY,
        store_hash TEXT NOT NULL,
        run_id BIGINT REFERENCES app_runtime.link_runs(run_id) ON DELETE SET NULL,
        submitted_by TEXT,
        total_result_count INTEGER NOT NULL DEFAULT 0,
        cleared_count INTEGER NOT NULL DEFAULT 0,
        review_bucket_count INTEGER NOT NULL DEFAULT 0,
        remaining_count INTEGER NOT NULL DEFAULT 0,
        metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """.strip(),
    """
    CREATE INDEX IF NOT EXISTS idx_query_gate_review_submissions_store_run
        ON app_runtime.query_gate_review_submissions (store_hash, run_id, created_at DESC)
    """.strip(),
]
GENERATION_ACTIVE_STATUSES = {"queued", "running"}
ACTIVE_RUN_STALE_AFTER = timedelta(hours=6)
ACTIVE_RUN_WATCH_AFTER = timedelta(minutes=15)
ACTIVE_RUN_URGENT_AFTER = timedelta(minutes=45)
COMPLETED_RUN_WATCH_AFTER = timedelta(days=8)
COMPLETED_RUN_URGENT_AFTER = timedelta(days=14)
EDGE_CASE_WATCH_COUNT = 3
EDGE_CASE_URGENT_COUNT = 8
ADMIN_METRIC_CACHE_TTL = timedelta(minutes=30)

ANCHOR_SMALL_WORDS = {"and", "or", "for", "of", "the", "with", "to", "by", "in", "on"}
ANCHOR_GENERIC_WORDS = {
    *ANCHOR_SMALL_WORDS,
    "hotel",
    "hospitality",
    "commercial",
    "quality",
    "dependable",
    "premium",
    "best",
    "top",
    "luxury",
    "choice",
    "collection",
    "series",
    "style",
    "styles",
    "decking",
    "deckings",
    "all",
    "new",
    "direct",
}
INTENT_STOPWORDS = ANCHOR_GENERIC_WORDS | {
    "a",
    "an",
    "your",
    "our",
    "this",
    "that",
    "these",
    "those",
    "from",
    "into",
    "room",
    "rooms",
    "guest",
    "guests",
    "supply",
    "supplies",
}
CONTEXT_KEEP_TOKENS = {"hotel", "hospitality"}
CONTEXT_TOKEN_ALIASES = {"hospitality": "hotel"}
QUERY_NOISE_WORDS = {
    "best",
    "cheap",
    "top",
    "review",
    "reviews",
    "vs",
    "compare",
    "comparison",
    "ideas",
    "inspiration",
    "sale",
    "discount",
    "wholesale",
}
FRAGMENT_NOISE_PATTERNS = (
    re.compile(r"^pack of \d+$", re.I),
    re.compile(r"^priced by the case$", re.I),
    re.compile(r"^made in usa$", re.I),
    re.compile(r"^made in the usa$", re.I),
    re.compile(r"^wholesale$", re.I),
    re.compile(r"^all sizes(?: and colors)?!?$", re.I),
    re.compile(r"^all styles(?: and sizes)?!?$", re.I),
    re.compile(r"^all sizes and colors!?$", re.I),
    re.compile(r"^\d+ ?(?:lb|lbs|oz|x\d+).*$", re.I),
)
TOPIC_PRIORITY = {
    "rollaway",
    "fold-up",
    "fold-out",
    "portable",
    "mobile",
    "mattress",
    "bed",
    "sleeper",
    "luggage",
    "bellman",
    "bellhop",
    "cart",
    "carrier",
    "rack",
    "towel",
    "bath",
    "pool",
    "cotton",
    "sheet",
    "pillow",
    "blanket",
    "duvet",
    "linen",
    "curtain",
    "shower",
    "hookless",
    "vinyl",
    "fabric",
    "polyester",
    "laminated",
}
REPLACEMENT_INTENT_TOKENS = {
    "replacement",
    "replace",
    "replacing",
    "part",
    "parts",
    "accessory",
    "accessories",
    "component",
    "components",
}
NARROW_ACCESSORY_TARGET_TOKENS = REPLACEMENT_INTENT_TOKENS | {
    "mattress-pad",
    "pad",
    "cover",
}
SIZE_TOKENS = {"twin", "full", "queen", "king", "twin-size", "full-size", "queen-size", "king-size"}
COLOR_TOKENS = {
    "white",
    "black",
    "silver",
    "gold",
    "brown",
    "gray",
    "grey",
    "beige",
    "ivory",
    "blue",
    "navy",
    "red",
    "green",
    "tan",
    "bronze",
    "mahogany",
    "chrome",
}
MATERIAL_TOKENS = {
    "cotton",
    "poly",
    "polyester",
    "microfiber",
    "foam",
    "spring",
    "metal",
    "steel",
    "wood",
    "rubber",
    "terry",
    "linen",
    "vinyl",
    "fabric",
    "brass",
}
FORM_TOKENS = {
    "bath-towel",
    "hand-towel",
    "washcloth",
    "bath-mat",
    "pool-towel",
    "bath-sheet",
    "flat-sheet",
    "fitted-sheet",
    "duvet-cover",
    "pillow-case",
    "shower-curtain",
    "hookless-shower-curtain",
}
GENERIC_ROUTING_TOKENS = {
    "hotel",
    "hotels",
    "hospitality",
    "wholesale",
    "bulk",
    "supplier",
    "vendor",
    "manufacturer",
    "distributor",
    "wholesaler",
    "procurement",
    "sourcing",
    "supply",
    "supplies",
    "product",
    "products",
    "collection",
    "collections",
}
GENERIC_BRAND_ALIAS_TOKENS = {
    "appliance",
    "appliances",
    "bath",
    "bed",
    "beds",
    "bedding",
    "frame",
    "frames",
    "home",
    "hospitality",
    "hotel",
    "linen",
    "linens",
    "product",
    "products",
    "supply",
    "supplies",
}
SEMANTIC_STATIC_SIGNAL_ROWS = (
    {
        "signal_kind": "protected_phrase",
        "raw_label": "5 star",
        "normalized_label": "5 star",
        "scope_kind": "phrase",
        "confidence": 0.98,
        "metadata": {"role": "quality_modifier", "head_term": "pillow", "head_family": "five star hotel pillows"},
    },
    {
        "signal_kind": "protected_phrase",
        "raw_label": "five star",
        "normalized_label": "5 star",
        "scope_kind": "phrase",
        "confidence": 0.98,
        "metadata": {"role": "quality_modifier", "head_term": "pillow", "head_family": "five star hotel pillows"},
    },
    {
        "signal_kind": "protected_phrase",
        "raw_label": "king size",
        "normalized_label": "king-size",
        "scope_kind": "phrase",
        "confidence": 0.99,
        "metadata": {"role": "size_attribute", "head_term": "bed"},
    },
    {
        "signal_kind": "protected_phrase",
        "raw_label": "pillow case",
        "normalized_label": "pillow case",
        "scope_kind": "phrase",
        "confidence": 0.98,
        "metadata": {"role": "head_product", "head_term": "pillow-case", "head_family": "pillow cases"},
    },
    {
        "signal_kind": "protected_phrase",
        "raw_label": "pillow cases",
        "normalized_label": "pillow case",
        "scope_kind": "phrase",
        "confidence": 0.98,
        "metadata": {"role": "head_product", "head_term": "pillow-case", "head_family": "pillow cases"},
    },
    {
        "signal_kind": "protected_phrase",
        "raw_label": "bellhop cart",
        "normalized_label": "bellhop cart",
        "scope_kind": "phrase",
        "confidence": 0.98,
        "metadata": {"role": "taxonomy_alias", "head_term": "cart", "head_family": "bellman carts"},
    },
    {
        "signal_kind": "protected_phrase",
        "raw_label": "box spring cover",
        "normalized_label": "box spring cover",
        "scope_kind": "phrase",
        "confidence": 0.98,
        "metadata": {"role": "head_product", "head_term": "cover", "head_family": "box spring covers"},
    },
    {
        "signal_kind": "protected_phrase",
        "raw_label": "box spring covers",
        "normalized_label": "box spring cover",
        "scope_kind": "phrase",
        "confidence": 0.98,
        "metadata": {"role": "head_product", "head_term": "cover", "head_family": "box spring covers"},
    },
    {
        "signal_kind": "protected_phrase",
        "raw_label": "rollaway bed",
        "normalized_label": "rollaway bed",
        "scope_kind": "phrase",
        "confidence": 0.98,
        "metadata": {"role": "head_product", "head_term": "bed", "head_family": "rollaway beds"},
    },
    {
        "signal_kind": "protected_phrase",
        "raw_label": "suite touch",
        "normalized_label": "suite touch",
        "scope_kind": "phrase",
        "confidence": 0.99,
        "metadata": {"role": "brand_candidate", "protected_brand_phrase": True},
    },
    {
        "signal_kind": "taxonomy_alias",
        "raw_label": "bellhop",
        "normalized_label": "bellhop",
        "scope_kind": "token",
        "confidence": 0.96,
        "metadata": {"role": "taxonomy_alias", "canonical_tokens": ["bellman", "birdcage", "luggage"]},
    },
    {
        "signal_kind": "taxonomy_alias",
        "raw_label": "bellman",
        "normalized_label": "bellman",
        "scope_kind": "token",
        "confidence": 0.96,
        "metadata": {"role": "taxonomy_alias", "canonical_tokens": ["bellhop", "birdcage", "luggage"]},
    },
    {
        "signal_kind": "ambiguous_modifier",
        "raw_label": "suite",
        "normalized_label": "suite",
        "scope_kind": "token",
        "confidence": 0.94,
        "metadata": {"role": "ambiguous_modifier", "alternate_roles": ["brand_candidate", "descriptive_modifier"]},
    },
    {
        "signal_kind": "ambiguous_modifier",
        "raw_label": "style",
        "normalized_label": "style",
        "scope_kind": "token",
        "confidence": 0.9,
        "metadata": {"role": "ambiguous_modifier", "alternate_roles": ["quality_modifier", "descriptive_modifier"]},
    },
    {
        "signal_kind": "ambiguous_modifier",
        "raw_label": "luxury",
        "normalized_label": "luxury",
        "scope_kind": "token",
        "confidence": 0.9,
        "metadata": {"role": "quality_modifier", "alternate_roles": ["descriptive_modifier"]},
    },
)
SEMANTIC_ALLOWED_PAGE_TYPES = {"product", "category", "brand", "content"}
SEMANTIC_ACCESSORY_BLOCK_RULES = {
    "pillow": {
        "blocked_tokens": {"pillow-case", "case", "cases"},
        "unless_query_tokens": {"case", "cases", "sham", "shams"},
        "message": "Explicit pillows suppress pillow cases unless case/cases is present",
    },
}
SEMANTIC_SUBTYPE_CONSTRAINTS = {
    "bellhop": {
        "head_term": "cart",
        "allowed_target_tokens": {"bellhop", "bellman", "birdcage", "luggage"},
        "blocked_target_tokens": {"platform"},
        "message": "Bellhop cart queries should stay in the bellman or birdcage cart family",
    },
    "bellman": {
        "head_term": "cart",
        "allowed_target_tokens": {"bellhop", "bellman", "birdcage", "luggage"},
        "blocked_target_tokens": {"platform"},
        "message": "Bellman cart queries should stay in the bellman or birdcage cart family",
    },
}
SEMANTIC_BRAND_FAMILY_ALIASES = {
    "blanket": {"blanket", "blankets", "duvet", "duvets", "insert", "inserts", "comforter", "comforters", "throw", "throws", "quilt", "quilts"},
}
SEMANTIC_BROAD_DESCRIPTIVE_PATTERNS = (
    re.compile(r"\bhow\b", re.I),
    re.compile(r"\bwhat\b", re.I),
    re.compile(r"\bwhy\b", re.I),
    re.compile(r"\bwhich\b", re.I),
    re.compile(r"\bwhere\b", re.I),
    re.compile(r"\bwith\b", re.I),
)
TOPIC_DISPLAY_MAP = {
    "bed": "Beds",
    "mattress": "Mattresses",
    "frame": "Bed Frames",
    "sleeper": "Sleepers",
    "cart": "Carts",
    "carrier": "Carriers",
    "rack": "Racks",
    "towel": "Towels",
    "pillow": "Pillows",
    "blanket": "Blankets",
    "comforter": "Comforters",
    "bedspread": "Bedspreads",
    "scarf": "Bed Scarves",
    "cover": "Covers",
    "linen": "Linens",
    "curtain": "Shower Curtains",
    "hookless": "Hookless Shower Curtains",
}
FORM_DISPLAY_MAP = {
    "bath-towel": "Bath Towels",
    "hand-towel": "Hand Towels",
    "pool-towel": "Pool Towels",
    "washcloth": "Washcloths",
    "bath-mat": "Bath Mats",
    "bath-sheet": "Bath Sheets",
    "flat-sheet": "Flat Sheets",
    "fitted-sheet": "Fitted Sheets",
    "duvet-cover": "Duvet Covers",
    "pillow-case": "Pillow Cases",
    "shower-curtain": "Shower Curtains",
    "hookless-shower-curtain": "Hookless Shower Curtains",
}
ATTRIBUTE_PHRASES = {
    "twin xl": ("size", "twin-xl"),
    "twin-size": ("size", "twin-size"),
    "full size": ("size", "full-size"),
    "queen size": ("size", "queen-size"),
    "king size": ("size", "king-size"),
    "bath towel": ("form", "bath-towel"),
    "hand towel": ("form", "hand-towel"),
    "pool towel": ("form", "pool-towel"),
    "bath mat": ("form", "bath-mat"),
    "bath sheet": ("form", "bath-sheet"),
    "shower curtain": ("form", "shower-curtain"),
    "shower curtains": ("form", "shower-curtain"),
    "hookless shower curtain": ("form", "hookless-shower-curtain"),
    "hookless shower curtains": ("form", "hookless-shower-curtain"),
    "sheet set": ("form", "sheet-set"),
    "pack of 6": ("pack_size", "6"),
    "pack of 10": ("pack_size", "10"),
    "pack of 12": ("pack_size", "12"),
    "pack of 24": ("pack_size", "24"),
    "pack of 48": ("pack_size", "48"),
    "pillow top": ("material", "pillow-top"),
    "coil spring": ("material", "coil-spring"),
}
ANCHOR_PHRASE_REPLACEMENTS = (
    ("bagage", "baggage"),
    ("bellman's", "bellman"),
    ("bellmans", "bellman"),
    ("bell hop", "bellhop"),
    ("roll away", "rollaway"),
    ("fold away", "fold-away"),
    ("fold out", "fold-out"),
    ("fold up", "fold-up"),
    ("full size", "full-size"),
    ("twin size", "twin-size"),
    ("queen size", "queen-size"),
    ("king size", "king-size"),
)

CLUSTER_PATTERNS = {
    "rollaway": ["%rollaway%", "%fold-up-bed%", "%portable-bed%", "%mobile-sleeper%"],
    "luggage": ["%luggage-cart%", "%bellman%", "%bellmans%", "%luggage-carrier%"],
    "towels": ["%towel%", "%bath-towel%", "%bathroom-supplies%", "%martex%", "%1888-mills%"],
    "shower-curtains": ["%shower-curtain%", "%hookless-shower%", "%fabric-curtain%", "%vinyl-curtain%", "%laminated-curtain%"],
}
STORE_CATEGORY_COMPETITION = {
    "pdwzti0dpv": {"rollaway"},
    "99oa2tso": {"rollaway", "towels", "shower-curtains", "luggage"},
}
CATEGORY_CLUSTER_LABELS = {
    "rollaway": "Rollaway Beds",
    "luggage": "Luggage Carts",
    "towels": "Hotel Towels",
    "shower-curtains": "Shower Curtains",
}
CATEGORY_COMPETITION_URL_HINTS = {
    "rollaway": ("rollaway-portable-foldable-beds-for-hotels",),
    "luggage": (
        "hotel-luggage-carts",
        "compact-luggage-carts",
        "bellman-birdcage-carts",
        "hotel-luggage-racks",
        "metal-luggage-racks",
        "wooden-luggage-rack",
    ),
    "towels": (
        "hotel-towels",
        "1888-mills-towels",
        "bath-spa-towels",
        "pool-towels-beach-towels",
    ),
    "shower-curtains": (
        "hotel-shower-curtains",
        "fabric-curtains",
        "hookless-shower-curtains",
        "vinyl-curtains",
        "laminated-curtains",
    ),
}
CATEGORY_COMPETITION_SPECIFIC_HINTS = {
    "rollaway": (
        {"fragment": "rollaway-portable-foldable-beds-for-hotels", "tokens": set(), "subclusters": set()},
    ),
    "luggage": (
        {"fragment": "bellman-birdcage-carts", "tokens": {"bell", "bellman", "bellhop", "birdcage"}, "subclusters": set()},
        {"fragment": "compact-luggage-carts", "tokens": {"compact", "nestable"}, "subclusters": set()},
        {"fragment": "hotel-luggage-racks", "tokens": {"rack", "racks"}, "subclusters": set()},
        {"fragment": "hotel-luggage-carts", "tokens": set(), "subclusters": set()},
    ),
    "towels": (
        {"fragment": "hotel-towels", "tokens": set(), "subclusters": set()},
    ),
    "shower-curtains": (
        {"fragment": "hookless-shower-curtains", "tokens": {"hookless"}, "subclusters": {"hookless-shower-curtains"}},
        {"fragment": "fabric-curtains", "tokens": {"fabric", "polyester"}, "subclusters": {"fabric-shower-curtains"}},
        {"fragment": "vinyl-curtains", "tokens": {"vinyl"}, "subclusters": {"vinyl-shower-curtains"}},
        {"fragment": "laminated-curtains", "tokens": {"laminated"}, "subclusters": {"laminated-shower-curtains"}},
        {"fragment": "hotel-shower-curtains", "tokens": set(), "subclusters": set()},
    ),
}

DEFAULT_ATTRIBUTE_BUCKETS = (
    ("size", "Size"),
    ("color", "Color"),
    ("material", "Material"),
    ("form", "Form"),
    ("finish", "Finish"),
    ("pack_size", "Pack Size"),
    ("collection", "Collection"),
    ("brand", "Brand"),
    ("topic", "Topic"),
)

OPTION_BUCKET_RULES = {
    "size": ("size", "sizes", "mattress size", "mattress sizes", "sheet size", "towel size", "dimension"),
    "color": ("color", "colors", "colour"),
    "material": ("material", "fabric", "fill", "construction"),
    "form": ("type", "item", "product type", "towel type", "sheet type", "style"),
    "finish": ("finish", "frame finish", "metal finish"),
    "pack_size": ("pack", "case pack", "qty", "quantity", "count"),
    "collection": ("collection", "series", "line"),
    "brand": ("brand",),
}

CANONICAL_VALUE_MAP = {
    "size": {
        "twin": "twin-size",
        "twin size": "twin-size",
        "twin-size": "twin-size",
        "twin xl": "twin-xl",
        "twin-xl": "twin-xl",
        "full": "full-size",
        "full size": "full-size",
        "full-size": "full-size",
        "queen": "queen-size",
        "queen size": "queen-size",
        "queen-size": "queen-size",
        "king": "king-size",
        "king size": "king-size",
        "king-size": "king-size",
    },
    "form": {
        "bath towel": "bath-towel",
        "bath towels": "bath-towel",
        "hand towel": "hand-towel",
        "hand towels": "hand-towel",
        "pool towel": "pool-towel",
        "pool towels": "pool-towel",
        "wash cloth": "washcloth",
        "washcloth": "washcloth",
        "wash cloths": "washcloth",
        "washcloths": "washcloth",
        "bath mat": "bath-mat",
        "bath mats": "bath-mat",
        "bath sheet": "bath-sheet",
        "bath sheets": "bath-sheet",
        "flat sheet": "flat-sheet",
        "flat sheets": "flat-sheet",
        "fitted sheet": "fitted-sheet",
        "fitted sheets": "fitted-sheet",
        "duvet cover": "duvet-cover",
        "duvet covers": "duvet-cover",
        "pillow case": "pillow-case",
        "pillow cases": "pillow-case",
        "sheet set": "sheet-set",
        "sheet sets": "sheet-set",
    },
    "material": {
        "100 cotton": "cotton",
        "100% cotton": "cotton",
        "cotton": "cotton",
        "egyptian cotton": "egyptian-cotton",
        "supima cotton": "supima-cotton",
        "microfiber": "microfiber",
        "polyester": "polyester",
        "poly": "polyester",
        "terry": "terry",
        "memory foam": "memory-foam",
        "foam": "foam",
        "coil spring": "coil-spring",
        "steel": "steel",
        "chrome": "chrome",
        "brass": "brass",
    },
    "color": {
        "off white": "ivory",
        "ivory": "ivory",
        "white": "white",
        "black": "black",
        "silver": "silver",
        "gold": "gold",
        "brown": "brown",
        "gray": "gray",
        "grey": "gray",
        "beige": "beige",
        "blue": "blue",
        "navy": "navy",
        "red": "red",
        "green": "green",
        "tan": "tan",
        "bronze": "bronze",
        "mahogany": "mahogany",
    },
}

TOWEL_SUBCLUSTER_MAP = {
    "bath-towel": "bath-towels",
    "hand-towel": "hand-towels",
    "pool-towel": "pool-towels",
    "washcloth": "washcloths",
    "bath-mat": "bath-mats",
    "bath-sheet": "bath-sheets",
}

SHOWER_CURTAIN_SUBCLUSTER_MAP = {
    "shower-curtain": "shower-curtains",
    "hookless-shower-curtain": "hookless-shower-curtains",
}

SIGNAL_SOURCE_PRIORITY = {
    "manual": 3,
    "deterministic": 2,
    "agent": 1,
    "fallback": 0,
}

FORM_FAMILY_TOKENS = {
    "bath",
    "bed",
    "blanket",
    "cart",
    "comforter",
    "cover",
    "crib",
    "curtain",
    "duvet",
    "frame",
    "linen",
    "luggage",
    "mattress",
    "pillow",
    "rack",
    "rollaway",
    "sheet",
    "shower",
    "sleep",
    "towel",
}
PACK_SIZE_HINT_TOKENS = {"pack", "case", "count", "counts", "qty", "quantity", "per", "dz", "dozen", "dozens"}
SIZE_HINT_TOKENS = SIZE_TOKENS | {"xl", "xxl", "california"}
GENERIC_ATTRIBUTE_MATCH_TOKENS = {"box", "cover", "covers", "wrap", "wraps"}
AMBIGUOUS_MATERIAL_TOKENS = {"spring"}


def apply_runtime_schema() -> None:
    global _RUNTIME_SCHEMA_APPLIED
    if _RUNTIME_SCHEMA_APPLIED:
        return

    with _RUNTIME_SCHEMA_LOCK:
        if _RUNTIME_SCHEMA_APPLIED:
            return

        # Most requests hit an already-provisioned runtime schema, so avoid
        # replaying the full DDL file when the expected tables and columns exist.
        with get_pg_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT table_name, column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'app_runtime'
                    """
                )
                table_columns: dict[str, set[str]] = {}
                for table_name, column_name in cur.fetchall() or []:
                    table_columns.setdefault((table_name or "").strip(), set()).add((column_name or "").strip())
        missing_tables = _RUNTIME_SCHEMA_REQUIRED_TABLES.difference(table_columns)
        missing_columns = {
            table_name: sorted(required_columns.difference(table_columns.get(table_name, set())))
            for table_name, required_columns in _RUNTIME_SCHEMA_REQUIRED_COLUMNS.items()
            if not required_columns.issubset(table_columns.get(table_name, set()))
        }
        if not missing_tables and not missing_columns:
            _RUNTIME_SCHEMA_APPLIED = True
            return

        if missing_tables == {"query_gate_review_submissions"} and not missing_columns:
            with get_pg_conn() as conn:
                with conn.cursor() as cur:
                    for statement in _RUNTIME_SCHEMA_QUERY_GATE_REVIEW_SUBMISSIONS_STATEMENTS:
                        cur.execute(statement)
                conn.commit()
            _RUNTIME_SCHEMA_APPLIED = True
            return

        sql = RUNTIME_SQL_PATH.read_text(encoding="utf-8")
        statements = [statement.strip() for statement in sql.split(";") if statement.strip()]
        with get_pg_conn() as conn:
            with conn.cursor() as cur:
                for statement in statements:
                    try:
                        cur.execute(statement)
                    except psycopg2.Error as exc:
                        conn.rollback()
                        if "pg_namespace_nspname_index" in str(exc) or "pg_type_typname_nsp_index" in str(exc):
                            continue
                        raise
            conn.commit()
        _RUNTIME_SCHEMA_APPLIED = True


def _intent_signal_row(
    store_hash: str,
    signal_kind: str,
    raw_label: str,
    normalized_label: str,
    scope_kind: str,
    entity_type: str | None = None,
    entity_id: int | None = None,
    confidence: float = 0.0,
    source: str = "deterministic",
    status: str = "active",
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "store_hash": normalize_store_hash(store_hash),
        "signal_kind": (signal_kind or "").strip().lower(),
        "raw_label": (raw_label or "").strip(),
        "normalized_label": _normalize_signal_label(normalized_label or raw_label),
        "scope_kind": (scope_kind or "").strip().lower(),
        "entity_type": ((entity_type or "").strip().lower() or None),
        "entity_id": int(entity_id) if entity_id is not None else None,
        "confidence": round(float(confidence or 0.0), 2),
        "source": (source or "deterministic").strip().lower(),
        "status": (status or "active").strip().lower(),
        "metadata": dict(metadata or {}),
    }


def _dedupe_intent_signal_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        normalized_label = _normalize_signal_label(row.get("normalized_label") or row.get("raw_label"))
        raw_label = (row.get("raw_label") or "").strip()
        if not normalized_label or not raw_label:
            continue
        key = (
            normalize_store_hash(row.get("store_hash")),
            (row.get("signal_kind") or "").strip().lower(),
            raw_label.lower(),
            normalized_label,
            (row.get("scope_kind") or "").strip().lower(),
            ((row.get("entity_type") or "").strip().lower() or ""),
            int(row.get("entity_id") or 0),
            (row.get("source") or "deterministic").strip().lower(),
        )
        existing = deduped.get(key)
        if not existing:
            deduped[key] = {**row, "normalized_label": normalized_label}
            continue
        existing_priority = (_signal_source_priority(existing.get("source")), float(existing.get("confidence") or 0.0))
        row_priority = (_signal_source_priority(row.get("source")), float(row.get("confidence") or 0.0))
        chosen = row if row_priority >= existing_priority else existing
        merged_metadata = dict(existing.get("metadata") or {})
        merged_metadata.update(row.get("metadata") or {})
        deduped[key] = {
            **chosen,
            "normalized_label": normalized_label,
            "metadata": merged_metadata,
            "status": "active" if "active" in {existing.get("status"), row.get("status")} else chosen.get("status", "active"),
        }
    return list(deduped.values())


def _replace_store_intent_signal_enrichments(store_hash: str, rows: list[dict[str, Any]]) -> int:
    return signals_replace_store_intent_signal_enrichments(
        store_hash,
        rows,
        normalize_store_hash_fn=normalize_store_hash,
        dedupe_intent_signal_rows_fn=_dedupe_intent_signal_rows,
        get_pg_conn_fn=get_pg_conn,
    )


def _load_store_intent_signal_enrichments(store_hash: str, active_only: bool = True) -> list[dict[str, Any]]:
    return signals_load_store_intent_signal_enrichments(
        store_hash,
        active_only=active_only,
        normalize_store_hash_fn=normalize_store_hash,
        get_pg_conn_fn=get_pg_conn,
        tokenize_intent_text_fn=_tokenize_intent_text,
    )


def _load_store_variant_sku_rows(product_ids: list[int]) -> list[dict[str, Any]]:
    return signals_load_store_variant_sku_rows(
        product_ids,
        get_pg_conn_fn=get_pg_conn,
    )


def _valid_brand_alias_token(token: str) -> bool:
    return signals_valid_brand_alias_token(
        token,
        generic_brand_alias_tokens=GENERIC_BRAND_ALIAS_TOKENS,
        topic_priority=TOPIC_PRIORITY,
        material_tokens=MATERIAL_TOKENS,
        form_tokens=FORM_TOKENS,
        generic_routing_tokens=GENERIC_ROUTING_TOKENS,
        query_noise_words=QUERY_NOISE_WORDS,
    )


def _derive_collection_seed_from_product(profile: dict[str, Any], category_topic_tokens: set[str]) -> str:
    return signals_derive_collection_seed_from_product(
        profile,
        category_topic_tokens,
        ordered_intent_tokens_fn=_ordered_intent_tokens,
        tokenize_intent_text_fn=_tokenize_intent_text,
        has_model_or_sku_signal_fn=_has_model_or_sku_signal,
        topic_priority=TOPIC_PRIORITY,
        topic_display_map=TOPIC_DISPLAY_MAP,
        generic_routing_tokens=GENERIC_ROUTING_TOKENS,
        context_keep_tokens=CONTEXT_KEEP_TOKENS,
        query_noise_words=QUERY_NOISE_WORDS,
        material_tokens=MATERIAL_TOKENS,
        form_tokens=FORM_TOKENS,
    )


def _label_ambiguous_intent_signals_with_agent(
    store_hash: str,
    ambiguous_items: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    return signals_label_ambiguous_intent_signals_with_agent(
        store_hash,
        ambiguous_items,
        intent_signal_row_fn=_intent_signal_row,
        intent_signal_agent_auto_apply_confidence=INTENT_SIGNAL_AGENT_AUTO_APPLY_CONFIDENCE,
    )


def refresh_store_intent_signal_enrichments(store_hash: str, initiated_by: str | None = None) -> dict[str, Any]:
    return signals_refresh_store_intent_signal_enrichments(
        store_hash,
        initiated_by=initiated_by,
        normalize_store_hash_fn=normalize_store_hash,
        load_all_store_product_profiles_fn=_load_all_store_product_profiles,
        load_store_category_profiles_fn=lambda normalized_hash: load_store_category_profiles(normalized_hash, canonical_only=False),
        load_store_brand_profiles_fn=load_store_brand_profiles,
        normalize_signal_label_fn=_normalize_signal_label,
        intent_signal_row_fn=_intent_signal_row,
        tokenize_intent_text_fn=_tokenize_intent_text,
        non_generic_signal_tokens_fn=_non_generic_signal_tokens,
        valid_brand_alias_token_fn=_valid_brand_alias_token,
        infer_bucket_from_option_name_fn=_infer_bucket_from_option_name,
        signal_kind_from_bucket_fn=_signal_kind_from_bucket,
        canonicalize_attribute_value_fn=_canonicalize_attribute_value,
        derive_collection_seed_from_product_fn=_derive_collection_seed_from_product,
        load_store_variant_sku_rows_fn=_load_store_variant_sku_rows,
        semantic_builtin_enrichment_rows_fn=_semantic_builtin_enrichment_rows,
        label_ambiguous_intent_signals_with_agent_fn=_label_ambiguous_intent_signals_with_agent,
        replace_store_intent_signal_enrichments_fn=_replace_store_intent_signal_enrichments,
        dedupe_intent_signal_rows_fn=_dedupe_intent_signal_rows,
        intent_signal_collection_min_repeat=INTENT_SIGNAL_COLLECTION_MIN_REPEAT,
    )


def _title_case_anchor(text: str) -> str:
    return anchor_title_case_anchor(text, anchor_small_words=ANCHOR_SMALL_WORDS)


def _normalize_anchor_text(raw_text: str) -> str:
    return anchor_normalize_anchor_text(
        raw_text,
        title_case_anchor_fn=_title_case_anchor,
        anchor_phrase_replacements=ANCHOR_PHRASE_REPLACEMENTS,
    )


def _label_from_target_url(target_url: str) -> str:
    return anchor_label_from_target_url(
        target_url,
        normalize_anchor_text_fn=_normalize_anchor_text,
    )


def _legacy_fallback_anchor_label(
    relation_type: str | None,
    example_query: str | None,
    target_url: str,
) -> str | None:
    return anchor_legacy_fallback_anchor_label(
        relation_type,
        example_query,
        target_url,
        label_from_target_url_fn=_label_from_target_url,
        normalize_anchor_text_fn=_normalize_anchor_text,
    )


def _tokenize_intent_text(raw_text: str | None) -> set[str]:
    return text_tokenize_intent_text(
        raw_text,
        anchor_phrase_replacements=ANCHOR_PHRASE_REPLACEMENTS,
        context_token_aliases=CONTEXT_TOKEN_ALIASES,
        intent_stopwords=INTENT_STOPWORDS,
        context_keep_tokens=CONTEXT_KEEP_TOKENS,
    )


def _ordered_intent_tokens(raw_text: str | None) -> list[str]:
    return text_ordered_intent_tokens(
        raw_text,
        anchor_phrase_replacements=ANCHOR_PHRASE_REPLACEMENTS,
        context_token_aliases=CONTEXT_TOKEN_ALIASES,
        intent_stopwords=INTENT_STOPWORDS,
        context_keep_tokens=CONTEXT_KEEP_TOKENS,
    )


def _normalize_signal_label(raw_label: str | None) -> str:
    return text_normalize_signal_label(raw_label, ordered_intent_tokens_fn=_ordered_intent_tokens)


def _signal_kind_from_bucket(bucket_key: str | None) -> str | None:
    bucket = (bucket_key or "").strip().lower()
    if bucket in {"size", "pack_size"}:
        return "hard_attribute"
    if bucket in {"color", "material", "form", "finish"}:
        return "soft_attribute"
    if bucket == "collection":
        return "collection"
    if bucket == "brand":
        return "brand_alias"
    if bucket == "topic":
        return "topic_token"
    return None


def _signal_source_priority(source: str | None) -> int:
    return SIGNAL_SOURCE_PRIORITY.get((source or "fallback").strip().lower(), 0)


def _non_generic_signal_tokens(tokens: set[str]) -> set[str]:
    return {
        token
        for token in tokens
        if token not in GENERIC_ROUTING_TOKENS
        and token not in CONTEXT_KEEP_TOKENS
        and token not in QUERY_NOISE_WORDS
    }


def _expand_signal_tokens(tokens: set[str] | list[str]) -> set[str]:
    expanded: set[str] = set()
    for token in tokens or []:
        normalized = (token or "").strip().lower()
        if not normalized:
            continue
        expanded.add(normalized)
        expanded |= {part for part in normalized.split("-") if part}
    return expanded


def _match_has_specific_attribute_tokens(
    query_tokens: set[str],
    match: dict[str, Any],
    signal_kind: str,
) -> bool:
    label_tokens = _expand_signal_tokens(
        _tokenize_intent_text(match.get("normalized_label") or match.get("label"))
    )
    if not label_tokens:
        return False

    ignored_tokens = (
        GENERIC_ROUTING_TOKENS
        | CONTEXT_KEEP_TOKENS
        | QUERY_NOISE_WORDS
        | TOPIC_PRIORITY
        | set(TOPIC_DISPLAY_MAP.keys())
        | GENERIC_ATTRIBUTE_MATCH_TOKENS
    )
    bucket_key = (match.get("bucket_key") or "").strip().lower()
    numeric_query_tokens = {token for token in query_tokens if any(char.isdigit() for char in token)}
    if signal_kind == "hard_attribute":
        ignored_tokens |= MATERIAL_TOKENS | FORM_TOKENS | FORM_FAMILY_TOKENS
        if bucket_key == "pack_size":
            return bool(query_tokens & PACK_SIZE_HINT_TOKENS or numeric_query_tokens)
        if bucket_key == "size":
            return bool(query_tokens & SIZE_HINT_TOKENS or numeric_query_tokens)
    elif signal_kind == "soft_attribute" and bucket_key == "form":
        ignored_tokens |= FORM_FAMILY_TOKENS

    specific_tokens = {token for token in label_tokens if token not in ignored_tokens}
    if signal_kind == "soft_attribute" and bucket_key == "material":
        specific_tokens -= AMBIGUOUS_MATERIAL_TOKENS
    return bool(specific_tokens & query_tokens)


def _query_has_explicit_attribute_intent(
    query_attrs: dict[str, set[str]],
    bucket_key: str,
    query_tokens: set[str],
) -> bool:
    values = set(query_attrs.get(bucket_key) or set())
    numeric_query_tokens = {token for token in query_tokens if any(char.isdigit() for char in token)}
    if bucket_key == "pack_size":
        return bool(values or query_tokens & PACK_SIZE_HINT_TOKENS or numeric_query_tokens)
    if bucket_key == "size":
        return bool(values or query_tokens & SIZE_HINT_TOKENS or numeric_query_tokens)
    if bucket_key == "material":
        return bool({value for value in values if value not in AMBIGUOUS_MATERIAL_TOKENS})
    return bool(values)


def _extract_attribute_terms(raw_text: str | None) -> dict[str, set[str]]:
    text = str(raw_text or "").lower()
    attrs = {
        "size": set(),
        "color": set(),
        "material": set(),
        "form": set(),
        "pack_size": set(),
    }
    for phrase, (bucket, canonical) in ATTRIBUTE_PHRASES.items():
        if phrase in text:
            attrs[bucket].add(canonical)

    for match in re.finditer(r"\b(?:pack|case)\s+of\s+(\d+)\b", text):
        attrs["pack_size"].add(match.group(1))
    for match in re.finditer(r"\b(\d+)\s*-\s*pack\b|\b(\d+)\s*pack\b|\b(\d+)\s*count\b", text):
        quantity = next((group for group in match.groups() if group), "")
        if quantity:
            attrs["pack_size"].add(quantity)

    tokens = _tokenize_intent_text(text)
    for token in tokens:
        if token in SIZE_TOKENS:
            attrs["size"].add(token)
        if token in COLOR_TOKENS:
            attrs["color"].add(token)
        if token in MATERIAL_TOKENS:
            attrs["material"].add(token)
        if token in FORM_TOKENS:
            attrs["form"].add(token)
    return attrs


def _has_model_or_sku_signal(raw_text: str | None) -> bool:
    if not raw_text:
        return False
    text = str(raw_text).lower()
    for token in re.findall(r"[a-z0-9-]+", text):
        compact = token.strip("-")
        if len(compact) < 3:
            continue
        has_digit = any(char.isdigit() for char in compact)
        has_alpha = any(char.isalpha() for char in compact)
        if has_digit and has_alpha:
            return True
    return False


def _serialize_query_signal_matches(matches: list[dict[str, Any]]) -> list[dict[str, Any]]:
    serialized: list[dict[str, Any]] = []
    for match in matches:
        serialized.append(
            {
                "label": match.get("label") or "",
                "normalized_label": match.get("normalized_label") or "",
                "source": match.get("source") or "deterministic",
                "confidence": round(float(match.get("confidence") or 0.0), 2),
                "matched_tokens": sorted(match.get("matched_tokens") or []),
                "bucket_key": (match.get("bucket_key") or "").strip().lower() or None,
                "scope_kind": match.get("scope_kind") or "",
                "entity_type": match.get("entity_type") or "",
                "entity_id": int(match.get("entity_id") or 0),
            }
        )
    return serialized


def _fallback_signal_match(
    signal_kind: str,
    label: str,
    normalized_label: str | None = None,
    matched_tokens: set[str] | None = None,
    confidence: float = 0.42,
    bucket_key: str | None = None,
) -> dict[str, Any]:
    return {
        "label": label,
        "normalized_label": _normalize_signal_label(normalized_label or label),
        "source": "fallback",
        "confidence": float(confidence),
        "matched_tokens": set(matched_tokens or _tokenize_intent_text(label)),
        "bucket_key": bucket_key,
        "scope_kind": "fallback",
        "entity_type": "",
        "entity_id": 0,
    }


def _semantic_builtin_enrichment_rows(store_hash: str) -> list[dict[str, Any]]:
    normalized_hash = normalize_store_hash(store_hash)
    rows: list[dict[str, Any]] = []
    for item in SEMANTIC_STATIC_SIGNAL_ROWS:
        rows.append(
            _intent_signal_row(
                store_hash=normalized_hash,
                signal_kind=item["signal_kind"],
                raw_label=item["raw_label"],
                normalized_label=item["normalized_label"],
                scope_kind=item["scope_kind"],
                confidence=float(item.get("confidence") or 0.0),
                source="deterministic",
                status="active",
                metadata=dict(item.get("metadata") or {}),
            )
        )
    return rows


def _build_store_signal_library(store_hash: str) -> dict[str, list[dict[str, Any]]]:
    library: dict[str, list[dict[str, Any]]] = {
        "brand_alias": [],
        "hard_attribute": [],
        "soft_attribute": [],
        "collection": [],
        "topic_token": [],
        "sku_pattern": [],
        "protected_phrase": [],
        "taxonomy_alias": [],
        "ambiguous_modifier": [],
        "negative_constraint": [],
    }
    for row in _load_store_intent_signal_enrichments(store_hash, active_only=True):
        signal_kind = (row.get("signal_kind") or "").strip().lower()
        if signal_kind not in library:
            continue
        library[signal_kind].append(row)
    for row in _semantic_builtin_enrichment_rows(store_hash):
        signal_kind = (row.get("signal_kind") or "").strip().lower()
        if signal_kind not in library:
            continue
        row["tokens"] = _tokenize_intent_text(row.get("normalized_label") or row.get("raw_label"))
        library[signal_kind].append(row)
    return library


def _match_store_signal_entries(
    query: str | None,
    query_tokens: set[str],
    entries: list[dict[str, Any]],
    signal_kind: str,
) -> list[dict[str, Any]]:
    return signal_match_store_signal_entries(
        query,
        query_tokens,
        entries,
        signal_kind,
        normalize_signal_label_fn=_normalize_signal_label,
        tokenize_intent_text_fn=_tokenize_intent_text,
        non_generic_signal_tokens_fn=_non_generic_signal_tokens,
        signal_source_priority_fn=_signal_source_priority,
        topic_priority=TOPIC_PRIORITY,
        topic_display_map=TOPIC_DISPLAY_MAP,
        form_family_tokens=FORM_FAMILY_TOKENS,
    )


def _match_semantic_signal_entries(
    query: str | None,
    query_tokens: set[str],
    entries: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    return signal_match_semantic_signal_entries(
        query,
        query_tokens,
        entries,
        normalize_signal_label_fn=_normalize_signal_label,
        tokenize_intent_text_fn=_tokenize_intent_text,
    )


def _semantic_pluralize(term: str | None) -> str:
    return text_semantic_pluralize(term)


def _semantic_head_term_from_phrases(bound_phrase_matches: list[dict[str, Any]]) -> str:
    return signal_semantic_head_term_from_phrases(
        bound_phrase_matches,
        normalize_signal_label_fn=_normalize_signal_label,
    )


def _semantic_head_term(
    query: str | None,
    query_tokens: set[str],
    bound_phrase_matches: list[dict[str, Any]],
    resolved_signals: dict[str, Any],
) -> str:
    return signal_semantic_head_term(
        query,
        query_tokens,
        bound_phrase_matches,
        resolved_signals,
        semantic_head_term_from_phrases_fn=_semantic_head_term_from_phrases,
        ordered_intent_tokens_fn=_ordered_intent_tokens,
        canonical_word_token_fn=_canonical_word_token,
        topic_priority=TOPIC_PRIORITY,
        query_noise_words=QUERY_NOISE_WORDS,
        generic_routing_tokens=GENERIC_ROUTING_TOKENS,
        context_keep_tokens=CONTEXT_KEEP_TOKENS,
    )


def _semantic_head_family(
    head_term: str,
    query_tokens: set[str],
    bound_phrase_matches: list[dict[str, Any]],
    taxonomy_alias_matches: list[dict[str, Any]],
) -> str:
    return signal_semantic_head_family(
        head_term,
        query_tokens,
        bound_phrase_matches,
        taxonomy_alias_matches,
        normalize_signal_label_fn=_normalize_signal_label,
        semantic_pluralize_fn=_semantic_pluralize,
    )


def _semantic_family_candidate_tokens(
    head_term: str,
    head_family: str,
    taxonomy_alias_matches: list[dict[str, Any]],
) -> set[str]:
    return signal_semantic_family_candidate_tokens(
        head_term,
        head_family,
        taxonomy_alias_matches,
        canonical_word_token_fn=_canonical_word_token,
        semantic_pluralize_fn=_semantic_pluralize,
        tokenize_intent_text_fn=_tokenize_intent_text,
        semantic_brand_family_aliases=SEMANTIC_BRAND_FAMILY_ALIASES,
        generic_routing_tokens=GENERIC_ROUTING_TOKENS,
    )


@lru_cache(maxsize=256)
def _brand_family_catalog_evidence(
    store_hash: str,
    brand_label: str,
    family_token_key: tuple[str, ...],
) -> dict[str, Any]:
    normalized_store_hash = normalize_store_hash(store_hash)
    normalized_brand = _normalize_signal_label(brand_label)
    brand_label_tokens = _tokenize_intent_text(normalized_brand)
    family_tokens = {token for token in family_token_key if token}
    if not normalized_store_hash or not normalized_brand or not family_tokens or not brand_label_tokens:
        return {"matching_product_count": 0, "matching_product_urls": []}

    def _profile_category_ids(profile: dict[str, Any]) -> set[int]:
        source_data = profile.get("source_data") if isinstance(profile.get("source_data"), dict) else {}
        product_data = source_data.get("product") if isinstance(source_data.get("product"), dict) else {}
        category_values = product_data.get("categories") or []
        category_ids: set[int] = set()
        for value in category_values:
            try:
                category_id = int(value or 0)
            except (TypeError, ValueError):
                continue
            if category_id > 0:
                category_ids.add(category_id)
        return category_ids

    matching_urls: list[str] = []
    matching_category_ids: set[int] = set()
    brand_category_counts: dict[int, int] = {}
    category_total_counts: dict[int, int] = {}
    profiles = _load_all_store_product_profiles(normalized_store_hash)
    for profile in profiles:
        category_ids = _profile_category_ids(profile)
        for category_id in category_ids:
            category_total_counts[category_id] = category_total_counts.get(category_id, 0) + 1
        product_brand_tokens = _tokenize_intent_text(_normalize_signal_label(profile.get("brand_name")))
        brand_matches = brand_label_tokens <= product_brand_tokens
        if brand_matches:
            for category_id in category_ids:
                brand_category_counts[category_id] = brand_category_counts.get(category_id, 0) + 1
        if not brand_matches:
            continue
        product_tokens = {_canonical_word_token(token) for token in set(profile.get("tokens") or []) if _canonical_word_token(token)}
        if product_tokens & family_tokens:
            matching_urls.append(profile.get("url") or "")
            matching_category_ids.update(category_ids)

    deduped_urls = sorted({url for url in matching_urls if url})
    category_depth_rows: list[dict[str, Any]] = []
    for category_id in sorted(matching_category_ids):
        total_count = int(category_total_counts.get(category_id) or 0)
        brand_count = int(brand_category_counts.get(category_id) or 0)
        share = round((brand_count / total_count), 4) if total_count > 0 else 0.0
        category_depth_rows.append(
            {
                "category_id": category_id,
                "brand_product_count": brand_count,
                "category_product_count": total_count,
                "brand_category_share": share,
            }
        )
    best_category_depth = max(
        category_depth_rows,
        key=lambda row: (float(row.get("brand_category_share") or 0.0), int(row.get("brand_product_count") or 0)),
        default={},
    )
    return {
        "matching_product_count": len(deduped_urls),
        "matching_product_urls": deduped_urls[:12],
        "matching_category_ids": sorted(matching_category_ids),
        "category_depth": category_depth_rows[:12],
        "best_brand_category_share": float(best_category_depth.get("brand_category_share") or 0.0),
        "best_brand_category_product_count": int(best_category_depth.get("brand_product_count") or 0),
        "best_category_product_count": int(best_category_depth.get("category_product_count") or 0),
        "best_category_id": int(best_category_depth.get("category_id") or 0) or None,
    }


def _query_has_exact_brand_phrase(query: str | None, brand_signals: list[dict[str, Any]]) -> float:
    return signal_query_has_exact_brand_phrase(
        query,
        brand_signals,
        normalize_signal_label_fn=_normalize_signal_label,
        tokenize_intent_text_fn=_tokenize_intent_text,
    )


def _semantic_token_roles(
    query: str | None,
    head_term: str,
    resolved_signals: dict[str, Any],
    taxonomy_alias_matches: list[dict[str, Any]],
    ambiguous_modifier_matches: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    return signal_semantic_token_roles(
        query,
        head_term,
        resolved_signals,
        taxonomy_alias_matches,
        ambiguous_modifier_matches,
        ordered_intent_tokens_fn=_ordered_intent_tokens,
        expand_signal_tokens_fn=_expand_signal_tokens,
        canonical_word_token_fn=_canonical_word_token,
        context_keep_tokens=CONTEXT_KEEP_TOKENS,
        topic_priority=TOPIC_PRIORITY,
        query_noise_words=QUERY_NOISE_WORDS,
        generic_routing_tokens=GENERIC_ROUTING_TOKENS,
        size_tokens=SIZE_TOKENS,
    )


def _query_is_broad_descriptive(
    query: str | None,
    query_tokens: set[str],
    resolved_signals: dict[str, Any],
) -> bool:
    return signal_query_is_broad_descriptive(
        query,
        query_tokens,
        resolved_signals,
        semantic_broad_descriptive_patterns=SEMANTIC_BROAD_DESCRIPTIVE_PATTERNS,
    )


def _build_query_semantics_analysis(
    store_hash: str | None,
    example_query: str | None,
    resolved_signals: dict[str, Any],
    *,
    signal_library: dict[str, list[dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    return signal_build_query_semantics_analysis(
        store_hash,
        example_query,
        resolved_signals,
        signal_library=signal_library,
        build_store_signal_library_fn=_build_store_signal_library,
        ordered_intent_tokens_fn=_ordered_intent_tokens,
        expand_signal_tokens_fn=_expand_signal_tokens,
        tokenize_intent_text_fn=_tokenize_intent_text,
        non_generic_signal_tokens_fn=_non_generic_signal_tokens,
        match_semantic_signal_entries_fn=_match_semantic_signal_entries,
        semantic_head_term_fn=_semantic_head_term,
        semantic_head_family_fn=_semantic_head_family,
        query_has_exact_brand_phrase_fn=_query_has_exact_brand_phrase,
        query_is_broad_descriptive_fn=_query_is_broad_descriptive,
        semantic_family_candidate_tokens_fn=_semantic_family_candidate_tokens,
        normalize_signal_label_fn=_normalize_signal_label,
        brand_family_catalog_evidence_fn=_brand_family_catalog_evidence,
        semantic_head_term_from_phrases_fn=_semantic_head_term_from_phrases,
        semantic_token_roles_fn=_semantic_token_roles,
        generic_brand_alias_tokens=GENERIC_BRAND_ALIAS_TOKENS,
        semantic_allowed_page_types=SEMANTIC_ALLOWED_PAGE_TYPES,
        semantic_accessory_block_rules=SEMANTIC_ACCESSORY_BLOCK_RULES,
        semantic_subtype_constraints=SEMANTIC_SUBTYPE_CONSTRAINTS,
        context_keep_tokens=CONTEXT_KEEP_TOKENS,
        query_noise_words=QUERY_NOISE_WORDS,
        generic_routing_tokens=GENERIC_ROUTING_TOKENS,
    )


def _build_fallback_query_signal_context(
    example_query: str | None,
    query_tokens: set[str],
    source_profile: dict[str, Any] | None = None,
    target_profile: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return signal_build_fallback_query_signal_context(
        example_query,
        query_tokens,
        source_profile=source_profile,
        target_profile=target_profile,
        extract_attribute_terms_fn=_extract_attribute_terms,
        tokenize_intent_text_fn=_tokenize_intent_text,
        fallback_signal_match_fn=_fallback_signal_match,
        has_model_or_sku_signal_fn=_has_model_or_sku_signal,
        serialize_query_signal_matches_fn=_serialize_query_signal_matches,
        signal_kind_from_bucket_fn=_signal_kind_from_bucket,
        expand_signal_tokens_fn=_expand_signal_tokens,
        non_generic_signal_tokens_fn=_non_generic_signal_tokens,
        topic_priority=TOPIC_PRIORITY,
        topic_display_map=TOPIC_DISPLAY_MAP,
        material_tokens=MATERIAL_TOKENS,
        form_tokens=FORM_TOKENS,
    )


def _resolve_query_signal_context(
    store_hash: str | None,
    example_query: str | None,
    signal_library: dict[str, list[dict[str, Any]]] | None = None,
    source_profile: dict[str, Any] | None = None,
    target_profile: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return signal_resolve_query_signal_context(
        store_hash,
        example_query,
        signal_library=signal_library,
        source_profile=source_profile,
        target_profile=target_profile,
        tokenize_intent_text_fn=_tokenize_intent_text,
        build_store_signal_library_fn=_build_store_signal_library,
        match_store_signal_entries_fn=_match_store_signal_entries,
        extract_attribute_terms_fn=_extract_attribute_terms,
        query_has_explicit_attribute_intent_fn=_query_has_explicit_attribute_intent,
        build_fallback_query_signal_context_fn=_build_fallback_query_signal_context,
        match_has_specific_attribute_tokens_fn=_match_has_specific_attribute_tokens,
        expand_signal_tokens_fn=_expand_signal_tokens,
        serialize_query_signal_matches_fn=_serialize_query_signal_matches,
    )


def _classify_query_intent_from_signals(
    example_query: str | None,
    resolved_signals: dict[str, Any],
) -> tuple[str, str]:
    return signal_classify_query_intent_from_signals(
        example_query,
        resolved_signals,
        ordered_intent_tokens_fn=_ordered_intent_tokens,
        expand_signal_tokens_fn=_expand_signal_tokens,
        tokenize_intent_text_fn=_tokenize_intent_text,
        non_generic_signal_tokens_fn=_non_generic_signal_tokens,
        looks_informational_query_fn=_looks_informational_query,
        generic_brand_alias_tokens=GENERIC_BRAND_ALIAS_TOKENS,
    )


def _classify_query_intent_scope(
    example_query: str | None,
    query_tokens: set[str],
    query_attrs: dict[str, set[str]],
    query_brand_tokens: set[str] | None = None,
    resolved_signals: dict[str, Any] | None = None,
) -> tuple[str, str]:
    return signal_classify_query_intent_scope(
        example_query,
        query_tokens,
        query_attrs,
        query_brand_tokens=query_brand_tokens,
        resolved_signals=resolved_signals,
        classify_query_intent_from_signals_fn=_classify_query_intent_from_signals,
        looks_informational_query_fn=_looks_informational_query,
        has_model_or_sku_signal_fn=_has_model_or_sku_signal,
        topic_priority=TOPIC_PRIORITY,
        topic_display_map=TOPIC_DISPLAY_MAP,
    )


def _is_replacement_or_accessory_target(
    query_tokens: set[str],
    target_tokens: set[str],
    target_name: str | None,
) -> bool:
    lowered_name = (target_name or "").lower()
    target_name_tokens = _tokenize_intent_text(target_name or "")
    if "replacement" in lowered_name:
        return True
    if target_name_tokens & NARROW_ACCESSORY_TARGET_TOKENS:
        return True
    # Mattress-only / parts-style targets are too narrow unless the query explicitly asks for them.
    if "mattress" in target_name_tokens and "bed" not in query_tokens and "rollaway" not in query_tokens:
        return True
    return False


def _attribute_sets_to_list(attrs: dict[str, set[str]]) -> dict[str, list[str]]:
    return {key: sorted(values) for key, values in attrs.items() if values}


def _build_ga4_signal(
    target_profile: dict[str, Any] | None,
    target_entity_type: str,
    query_intent_scope: str | None = None,
) -> dict[str, Any]:
    return ga4_build_ga4_signal(
        target_profile,
        target_entity_type,
        query_intent_scope,
    )


def _increment_review_feedback_bucket(
    buckets: dict[Any, dict[str, int]],
    key: Any,
    review_status: str | None,
) -> None:
    feedback_increment_review_feedback_bucket(buckets, key, review_status)


def _load_review_feedback_maps(store_hash: str) -> dict[str, dict[Any, dict[str, int]]]:
    return feedback_load_review_feedback_maps(
        store_hash,
        normalize_query_family_key_fn=_normalize_query_family_key,
    )


def _build_review_feedback_signal(
    query: str | None,
    source_entity_type: str,
    source_entity_id: int | None,
    target_entity_type: str,
    target_entity_id: int | None,
    feedback_maps: dict[str, dict[Any, dict[str, int]]] | None = None,
) -> dict[str, Any]:
    return ranker_build_review_feedback_signal(
        query,
        source_entity_type,
        source_entity_id,
        target_entity_type,
        target_entity_id,
        feedback_maps=feedback_maps,
        normalize_query_family_key_fn=_normalize_query_family_key,
    )


def _append_reason_summary(base_summary: str | None, extra_summary: str | None) -> str:
    return ranker_append_reason_summary(base_summary, extra_summary)


def _profile_topic_label(profile: dict[str, Any] | None) -> str:
    return text_profile_topic_label(
        profile,
        form_display_map=FORM_DISPLAY_MAP,
        topic_display_map=TOPIC_DISPLAY_MAP,
        normalize_anchor_text_fn=_normalize_anchor_text,
        tokenize_intent_text_fn=_tokenize_intent_text,
    )


def _profile_brand_label(profile: dict[str, Any] | None) -> str:
    return text_profile_brand_label(profile, normalize_anchor_text_fn=_normalize_anchor_text)


def _normalize_phrase_for_match(text: str | None) -> str:
    return text_normalize_phrase_for_match(text, normalize_anchor_text_fn=_normalize_anchor_text)


def _fuzzy_match_score(left: str | None, right: str | None) -> float:
    return text_fuzzy_match_score(
        left,
        right,
        normalize_phrase_for_match_fn=_normalize_phrase_for_match,
        tokenize_intent_text_fn=_tokenize_intent_text,
    )


def _fuzzy_candidate_kind(label_source: str) -> str:
    return text_fuzzy_candidate_kind(label_source)


def _build_fuzzy_signal(
    example_query: str | None,
    target_name: str | None,
    target_url: str,
    target_profile: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return text_build_fuzzy_signal(
        example_query,
        target_name,
        target_url,
        target_profile=target_profile,
        normalize_phrase_for_match_fn=_normalize_phrase_for_match,
        profile_brand_label_fn=_profile_brand_label,
        extract_label_candidates_fn=_extract_label_candidates,
        fuzzy_match_score_fn=_fuzzy_match_score,
        normalize_anchor_text_fn=_normalize_anchor_text,
        fuzzy_candidate_kind_fn=_fuzzy_candidate_kind,
    )


def _normalize_query_family_key(query: str | None) -> str:
    return text_normalize_query_family_key(
        query,
        normalize_phrase_for_match_fn=_normalize_phrase_for_match,
        tokenize_intent_text_fn=_tokenize_intent_text,
        query_noise_words=QUERY_NOISE_WORDS,
    )


def _expected_ctr_for_position(avg_position: float) -> float:
    position = float(avg_position or 0.0)
    if position <= 0:
        return 0.0
    if position <= 3:
        return 0.16
    if position <= 5:
        return 0.09
    if position <= 10:
        return 0.045
    if position <= 20:
        return 0.02
    return 0.01


def _build_freshness_context(
    clicks_28d: int,
    impressions_28d: int,
    clicks_90d: int,
    impressions_90d: int,
) -> dict[str, Any]:
    expected_clicks_28d = (float(clicks_90d or 0) * 28.0) / 90.0
    expected_impressions_28d = (float(impressions_90d or 0) * 28.0) / 90.0

    def _pct_delta(current: float, expected: float) -> float:
        baseline = max(expected, 1.0)
        return round(((current - expected) / baseline) * 100.0, 2)

    click_delta = _pct_delta(float(clicks_28d or 0), expected_clicks_28d)
    impression_delta = _pct_delta(float(impressions_28d or 0), expected_impressions_28d)
    if click_delta >= 20 or impression_delta >= 20:
        trend_label = "rising"
    elif click_delta <= -20 or impression_delta <= -20:
        trend_label = "softening"
    else:
        trend_label = "stable"
    return {
        "trend_label": trend_label,
        "click_delta_pct": click_delta,
        "impression_delta_pct": impression_delta,
        "expected_clicks_28d": round(expected_clicks_28d, 2),
        "expected_impressions_28d": round(expected_impressions_28d, 2),
    }


def _build_query_gate_record(
    store_hash: str,
    family_key: str,
    representative_query: str,
    evidence_rows: list[dict[str, Any]],
    source_profiles: dict[str, dict[str, Any]],
    target_entities: list[dict[str, Any]],
    signal_library: dict[str, list[dict[str, Any]]] | None = None,
) -> dict[str, Any] | None:
    return gate_build_query_gate_record(
        store_hash,
        family_key,
        representative_query,
        evidence_rows,
        source_profiles,
        target_entities,
        signal_library,
        normalize_storefront_path_fn=_normalize_storefront_path,
        tokenize_intent_text_fn=_tokenize_intent_text,
        resolve_query_signal_context_fn=_resolve_query_signal_context,
        classify_query_intent_scope_fn=_classify_query_intent_scope,
        build_query_semantics_analysis_fn=_build_query_semantics_analysis,
        fuzzy_match_score_fn=_fuzzy_match_score,
        expected_ctr_for_position_fn=_expected_ctr_for_position,
        build_freshness_context_fn=_build_freshness_context,
        query_noise_words=QUERY_NOISE_WORDS,
    )


def load_product_profiles(product_urls: list[str]) -> dict[str, dict[str, Any]]:
    return loaders_load_product_profiles(
        product_urls,
        get_pg_conn_fn=get_pg_conn,
        extract_attribute_terms_fn=_extract_attribute_terms,
        tokenize_intent_text_fn=_tokenize_intent_text,
        build_cluster_profile_fn=_build_cluster_profile,
    )


def _slugify_value(raw_value: str | None) -> str:
    value = re.sub(r"[^a-z0-9]+", "-", str(raw_value or "").lower()).strip("-")
    return re.sub(r"-{2,}", "-", value)


def _normalize_option_name(raw_option_name: str | None) -> str:
    return re.sub(r"\s+", " ", str(raw_option_name or "").strip()).lower()


def _infer_bucket_from_option_name(raw_option_name: str | None, raw_values: list[str] | None = None) -> tuple[str | None, float]:
    option_name = _normalize_option_name(raw_option_name)
    value_text = " ".join(raw_values or []).lower()
    if not option_name and not value_text:
        return None, 0.0

    for bucket_key, patterns in OPTION_BUCKET_RULES.items():
        for pattern in patterns:
            if pattern in option_name:
                return bucket_key, 0.96

    if re.search(r"\b(pack|case|qty|quantity|count)\b", value_text):
        return "pack_size", 0.76
    if _extract_attribute_terms(value_text).get("form"):
        return "form", 0.72
    if _extract_attribute_terms(value_text).get("size"):
        return "size", 0.72
    if _extract_attribute_terms(value_text).get("color"):
        return "color", 0.68
    if _extract_attribute_terms(value_text).get("material"):
        return "material", 0.68
    return None, 0.0


def _canonicalize_attribute_value(bucket_key: str, raw_value: str | None) -> str:
    text = _normalize_option_name(raw_value)
    if not text:
        return ""

    if bucket_key == "pack_size":
        match = re.search(r"\b(\d+)\b", text)
        return match.group(1) if match else ""

    if bucket_key == "size" and text in {"xs", "s", "m", "l", "xl", "xxl"}:
        return ""

    mapped = CANONICAL_VALUE_MAP.get(bucket_key, {}).get(text)
    if mapped:
        return mapped

    for pattern, (bucket, canonical) in ATTRIBUTE_PHRASES.items():
        if bucket == bucket_key and pattern in text:
            return canonical

    extracted = _extract_attribute_terms(text).get(bucket_key, set())
    if extracted:
        return sorted(extracted)[0]

    if bucket_key in {"finish", "collection", "brand", "topic"}:
        return _slugify_value(text)

    return _slugify_value(text)


def _serialize_attribute_profile(attribute_profile: dict[str, set[str]]) -> dict[str, list[str]]:
    return {bucket: sorted(values) for bucket, values in attribute_profile.items() if values}


def _build_cluster_profile(
    product_name: str | None,
    product_url: str | None,
    brand_name: str | None,
    search_keywords: str | None,
    attribute_profile: dict[str, set[str]],
) -> dict[str, Any]:
    return cluster_build_cluster_profile(
        product_name,
        product_url,
        brand_name,
        search_keywords,
        attribute_profile,
        tokenize_intent_text_fn=_tokenize_intent_text,
        shower_curtain_subcluster_map=SHOWER_CURTAIN_SUBCLUSTER_MAP,
        towel_subcluster_map=TOWEL_SUBCLUSTER_MAP,
        topic_priority=TOPIC_PRIORITY,
    )


def _looks_generic_phrase(text: str) -> bool:
    return anchor_looks_generic_phrase(
        text,
        tokenize_intent_text_fn=_tokenize_intent_text,
        topic_priority=TOPIC_PRIORITY,
        anchor_generic_words=ANCHOR_GENERIC_WORDS,
    )


def _is_noise_fragment(text: str) -> bool:
    return anchor_is_noise_fragment(
        text,
        normalize_anchor_text_fn=_normalize_anchor_text,
        anchor_small_words=ANCHOR_SMALL_WORDS,
        fragment_noise_patterns=FRAGMENT_NOISE_PATTERNS,
    )


def _trim_phrase_tokens(text: str, max_words: int = 5) -> str:
    return anchor_trim_phrase_tokens(
        text,
        canonical_word_token_fn=_canonical_word_token,
        anchor_small_words=ANCHOR_SMALL_WORDS,
        max_words=max_words,
    )


def _canonical_word_token(word: str) -> str:
    return anchor_canonical_word_token(word)


def _ordered_focus_terms(fragment: str, max_terms: int = 2) -> list[str]:
    return anchor_ordered_focus_terms(
        fragment,
        canonical_word_token_fn=_canonical_word_token,
        topic_priority=TOPIC_PRIORITY,
        anchor_generic_words=ANCHOR_GENERIC_WORDS,
        size_tokens=SIZE_TOKENS,
        max_terms=max_terms,
    )


def _ordered_size_terms(fragment: str, max_terms: int = 1) -> list[str]:
    return anchor_ordered_size_terms(
        fragment,
        canonical_word_token_fn=_canonical_word_token,
        size_tokens=SIZE_TOKENS,
        max_terms=max_terms,
    )


def _extract_label_candidates(
    target_name: str | None,
    target_url: str,
    example_query: str | None = None,
    target_profile: dict[str, Any] | None = None,
) -> list[tuple[str, str]]:
    return anchor_extract_label_candidates(
        target_name,
        target_url,
        example_query,
        target_profile,
        normalize_anchor_text_fn=_normalize_anchor_text,
        trim_phrase_tokens_fn=_trim_phrase_tokens,
        tokenize_intent_text_fn=_tokenize_intent_text,
        is_noise_fragment_fn=_is_noise_fragment,
        ordered_size_terms_fn=_ordered_size_terms,
        ordered_focus_terms_fn=_ordered_focus_terms,
        profile_topic_label_fn=_profile_topic_label,
        profile_brand_label_fn=_profile_brand_label,
        extract_attribute_terms_fn=_extract_attribute_terms,
        label_from_target_url_fn=_label_from_target_url,
        looks_generic_phrase_fn=_looks_generic_phrase,
        query_noise_words=QUERY_NOISE_WORDS,
        size_tokens=SIZE_TOKENS,
        topic_priority=TOPIC_PRIORITY,
        form_tokens=FORM_TOKENS,
        topic_display_map=TOPIC_DISPLAY_MAP,
        form_display_map=FORM_DISPLAY_MAP,
    )


def _select_anchor_label(
    relation_type: str,
    example_query: str | None,
    target_url: str,
    target_name: str | None = None,
    source_name: str | None = None,
    source_profile: dict[str, Any] | None = None,
    target_profile: dict[str, Any] | None = None,
    used_labels: set[str] | None = None,
) -> dict[str, Any]:
    return anchor_select_anchor_label(
        relation_type,
        example_query,
        target_url,
        target_name,
        source_name,
        source_profile,
        target_profile,
        used_labels,
        legacy_fallback_anchor_label_fn=_legacy_fallback_anchor_label,
        label_from_target_url_fn=_label_from_target_url,
        looks_generic_phrase_fn=_looks_generic_phrase,
        tokenize_intent_text_fn=_tokenize_intent_text,
        extract_attribute_terms_fn=_extract_attribute_terms,
        profile_topic_label_fn=_profile_topic_label,
        profile_brand_label_fn=_profile_brand_label,
        extract_label_candidates_fn=_extract_label_candidates,
        query_noise_words=QUERY_NOISE_WORDS,
        size_tokens=SIZE_TOKENS,
        topic_priority=TOPIC_PRIORITY,
        form_family_tokens=FORM_FAMILY_TOKENS,
        generic_routing_tokens=GENERIC_ROUTING_TOKENS,
        topic_display_map=TOPIC_DISPLAY_MAP,
    )


def _select_category_product_anchor_label(
    target_name: str | None,
    target_url: str,
    source_name: str | None = None,
    source_profile: dict[str, Any] | None = None,
    target_profile: dict[str, Any] | None = None,
    used_labels: set[str] | None = None,
) -> dict[str, Any]:
    return anchor_select_category_product_anchor_label(
        target_name,
        target_url,
        source_name,
        source_profile,
        target_profile,
        used_labels,
        tokenize_intent_text_fn=_tokenize_intent_text,
        profile_brand_label_fn=_profile_brand_label,
        profile_topic_label_fn=_profile_topic_label,
        extract_label_candidates_fn=_extract_label_candidates,
        looks_generic_phrase_fn=_looks_generic_phrase,
        select_anchor_label_fn=_select_anchor_label,
        size_tokens=SIZE_TOKENS,
    )


def build_anchor_label(
    relation_type: str,
    example_query: str,
    target_url: str,
    target_name: str | None = None,
    source_name: str | None = None,
    source_profile: dict[str, Any] | None = None,
    target_profile: dict[str, Any] | None = None,
    used_labels: set[str] | None = None,
) -> str:
    return _select_anchor_label(
        relation_type=relation_type,
        example_query=example_query,
        target_url=target_url,
        target_name=target_name,
        source_name=source_name,
        source_profile=source_profile,
        target_profile=target_profile,
        used_labels=used_labels,
    )["label"]


def build_intent_profile(
    source_name: str | None,
    source_url: str | None,
    target_name: str | None,
    target_url: str,
    example_query: str | None,
    relation_type: str,
    hit_count: int,
    source_profile: dict[str, Any] | None = None,
    target_profile: dict[str, Any] | None = None,
    used_labels: set[str] | None = None,
    query_signal_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return intent_build_intent_profile(
        source_name,
        source_url,
        target_name,
        target_url,
        example_query,
        relation_type,
        hit_count,
        source_profile=source_profile,
        target_profile=target_profile,
        used_labels=used_labels,
        query_signal_context=query_signal_context,
        tokenize_intent_text_fn=_tokenize_intent_text,
        extract_attribute_terms_fn=_extract_attribute_terms,
        resolve_query_signal_context_fn=_resolve_query_signal_context,
        build_fuzzy_signal_fn=_build_fuzzy_signal,
        classify_query_intent_scope_fn=_classify_query_intent_scope,
        select_anchor_label_fn=_select_anchor_label,
        build_ga4_signal_fn=_build_ga4_signal,
        is_replacement_or_accessory_target_fn=_is_replacement_or_accessory_target,
        attribute_sets_to_list_fn=_attribute_sets_to_list,
        topic_priority=TOPIC_PRIORITY,
        topic_display_map=TOPIC_DISPLAY_MAP,
        form_family_tokens=FORM_FAMILY_TOKENS,
        generic_routing_tokens=GENERIC_ROUTING_TOKENS,
        query_noise_words=QUERY_NOISE_WORDS,
        intent_stopwords=INTENT_STOPWORDS,
        context_keep_tokens=CONTEXT_KEEP_TOKENS,
        narrow_accessory_target_tokens=NARROW_ACCESSORY_TARGET_TOKENS,
        replacement_intent_tokens=REPLACEMENT_INTENT_TOKENS,
    )


def sync_store_storefront_sites(store_hash: str, initiated_by: str | None = None) -> dict[str, Any]:
    return platform_sync_store_storefront_sites(
        store_hash,
        initiated_by=initiated_by,
        clear_storefront_site_caches=_clear_storefront_site_caches,
        resolve_default_base_url=_storefront_base_url_from_db,
    )


def normalize_store_hash(value: str | None) -> str:
    return platform_normalize_store_hash(value)


def _resolve_store_token(store_hash: str) -> str:
    return platform_resolve_store_token(store_hash)


def decode_signed_payload(token: str, secret: str) -> dict[str, Any]:
    return platform_decode_signed_payload(token, secret)


def exchange_auth_code(code: str, scope: str, context: str) -> dict[str, Any]:
    return platform_exchange_auth_code(code, scope, context)


def upsert_store_installation(
    store_hash: str,
    context: str,
    access_token: str | None,
    scope: str | None,
    user_id: str | None = None,
    owner_email: str | None = None,
    install_source: str = "oauth",
    metadata: dict[str, Any] | None = None,
) -> None:
    return platform_upsert_store_installation(
        store_hash,
        context,
        access_token,
        scope,
        user_id=user_id,
        owner_email=owner_email,
        install_source=install_source,
        metadata=metadata,
    )


def mark_store_uninstalled(store_hash: str, metadata: dict[str, Any] | None = None) -> None:
    return platform_mark_store_uninstalled(store_hash, metadata=metadata)


def merge_store_installation_metadata(
    store_hash: str,
    metadata: dict[str, Any] | None = None,
    *,
    context: str | None = None,
    scope: str | None = None,
    user_id: str | None = None,
    owner_email: str | None = None,
    install_source: str | None = None,
) -> None:
    return platform_merge_store_installation_metadata(
        store_hash,
        metadata=metadata,
        context=context,
        scope=scope,
        user_id=user_id,
        owner_email=owner_email,
        install_source=install_source,
    )


def _seed_store_attribute_buckets(store_hash: str) -> None:
    sql = """
        INSERT INTO app_runtime.store_attribute_buckets (store_hash, bucket_key, bucket_label, is_active, config, updated_at)
        VALUES (%s, %s, %s, TRUE, '{}'::jsonb, NOW())
        ON CONFLICT (store_hash, bucket_key) DO UPDATE SET
            bucket_label = EXCLUDED.bucket_label,
            updated_at = NOW();
    """
    values = [(store_hash, bucket_key, bucket_label) for bucket_key, bucket_label in DEFAULT_ATTRIBUTE_BUCKETS]
    if not values:
        return
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            execute_batch(cur, sql, values, page_size=100)
        conn.commit()


def _seed_store_cluster_rules(store_hash: str) -> None:
    default_rules = [
        (store_hash, "rollaway", "Rollaway Beds", "topic", "rollaway", 10, "auto", True, json.dumps({"source": "default"})),
        (store_hash, "luggage", "Luggage Carts", "topic", "luggage", 10, "auto", True, json.dumps({"source": "default"})),
        (store_hash, "towels", "Hotel Towels", "topic", "towels", 10, "auto", True, json.dumps({"source": "default"})),
        (store_hash, "bedding", "Hotel Bedding", "topic", "bedding", 20, "auto", True, json.dumps({"source": "default"})),
    ]
    default_rules.extend(
        (
            store_hash,
            "towels",
            cluster_label.replace("-", " ").title(),
            "form",
            canonical_value,
            30,
            "auto",
            True,
            json.dumps({"subcluster": cluster_label}),
        )
        for canonical_value, cluster_label in TOWEL_SUBCLUSTER_MAP.items()
    )
    sql = """
        INSERT INTO app_runtime.store_cluster_rules (
            store_hash, cluster_key, cluster_label, bucket_key, canonical_value, priority, source, is_active, metadata, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW())
        ON CONFLICT DO NOTHING;
    """
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            execute_batch(cur, sql, default_rules, page_size=100)
        conn.commit()


def _canonical_category_group_key(
    category: dict[str, Any],
    known_urls: set[str] | None = None,
) -> str:
    category_url = (((category.get("custom_url") or {}).get("url")) or category.get("category_url") or "").strip()
    normalized_url = _normalize_storefront_path(category_url)
    if normalized_url and known_urls:
        base_url = _duplicate_suffix_base_url(normalized_url, known_urls)
        if base_url != normalized_url:
            return f"url:{base_url}"
        duplicate_pattern = re.compile(rf"^{re.escape(normalized_url[:-1])}-\d+/$")
        if any(duplicate_pattern.match(candidate) for candidate in known_urls if candidate != normalized_url):
            return f"url:{normalized_url}"
    name_key = _slugify_value(category.get("name") or category.get("category_name") or "")
    parent_key = str(category.get("parent_id") or category.get("parent_category_id") or "root")
    return f"{parent_key}:{name_key}" if name_key else parent_key


def _duplicate_suffix_base_url(url: str | None, known_urls: set[str] | None = None) -> str:
    normalized_url = _normalize_storefront_path(url)
    if not normalized_url:
        return ""
    match = re.match(r"^(?P<base>.+)-(?P<suffix>\d+)/$", normalized_url)
    if not match:
        return normalized_url
    candidate_base = f"{match.group('base')}/"
    if known_urls and candidate_base not in known_urls:
        return normalized_url
    return candidate_base


def _looks_like_placeholder_entity(name: str | None, url: str | None) -> bool:
    lowered_name = (name or "").strip().lower()
    normalized_url = _normalize_storefront_path(url)
    return (
        lowered_name.startswith("[sample]")
        or lowered_name.startswith("sample ")
        or "/sample-" in normalized_url
        or normalized_url.endswith("/sample/")
        or "/test-" in normalized_url
    )


def _canonical_product_group_key(product: dict[str, Any], known_urls: set[str]) -> str:
    product_url = (((product.get("custom_url") or {}).get("url")) or product.get("product_url") or "").strip()
    normalized_url = _normalize_storefront_path(product_url)
    if normalized_url:
        return _duplicate_suffix_base_url(normalized_url, known_urls)
    brand_key = _slugify_value(product.get("brand_name") or "")
    name_key = _slugify_value(product.get("name") or product.get("product_name") or "")
    return f"{brand_key}:{name_key}" if brand_key or name_key else str(product.get("id") or product.get("bc_product_id") or "")


def _product_quality_score(product: dict[str, Any], known_urls: set[str]) -> tuple[int, int, int, int, int, int]:
    custom_url = (product.get("custom_url") or {}) if isinstance(product.get("custom_url"), dict) else {}
    url = (custom_url.get("url") or product.get("product_url") or "").strip().lower()
    normalized_url = _normalize_storefront_path(url)
    is_visible = bool(product.get("is_visible", True))
    availability = (product.get("availability") or "").strip().lower()
    is_price_hidden = bool(product.get("is_price_hidden", False))
    duplicate_suffix_penalty = 1 if _duplicate_suffix_base_url(normalized_url, known_urls) != normalized_url else 0
    customized_url = 1 if custom_url.get("is_customized") else 0
    return (
        1 if is_visible else 0,
        1 if availability in {"", "available", "preorder"} else 0,
        1 if not is_price_hidden else 0,
        1 if not _looks_like_placeholder_entity(product.get("name") or product.get("product_name"), normalized_url) else 0,
        customized_url,
        -duplicate_suffix_penalty,
    )


def _pick_canonical_product_ids(products: list[dict[str, Any]]) -> set[int]:
    known_urls = {
        _normalize_storefront_path(((product.get("custom_url") or {}).get("url")) or product.get("product_url") or "")
        for product in products
        if (((product.get("custom_url") or {}).get("url")) or product.get("product_url") or "").strip()
    }
    grouped: dict[str, list[dict[str, Any]]] = {}
    for product in products:
        grouped.setdefault(_canonical_product_group_key(product, known_urls), []).append(product)

    canonical_ids: set[int] = set()
    for group_rows in grouped.values():
        best = max(group_rows, key=lambda row: _product_quality_score(row, known_urls))
        best_id = best.get("id") if best.get("id") is not None else best.get("bc_product_id")
        if best_id is not None:
            canonical_ids.add(int(best_id))
    return canonical_ids


def _product_eligible_for_routing(product: dict[str, Any]) -> bool:
    custom_url = (product.get("custom_url") or {}) if isinstance(product.get("custom_url"), dict) else {}
    product_url = (custom_url.get("url") or product.get("product_url") or "").strip()
    if not product_url:
        return False
    if not bool(product.get("is_visible", True)):
        return False
    availability = (product.get("availability") or "").strip().lower()
    if availability and availability not in {"available", "preorder"}:
        return False
    if bool(product.get("is_price_hidden", False)):
        return False
    if _looks_like_placeholder_entity(product.get("name") or product.get("product_name"), product_url):
        return False
    return True


def _category_eligible_for_routing(category: dict[str, Any]) -> bool:
    category_url = (((category.get("custom_url") or {}).get("url")) or category.get("category_url") or "").strip()
    if not category_url:
        return False
    if category.get("is_visible") is not None and not bool(category.get("is_visible")):
        return False
    if _looks_like_placeholder_entity(category.get("name") or category.get("category_name"), category_url):
        return False
    return True


def _dedupe_entity_profiles(
    profiles: list[dict[str, Any]],
    *,
    prefer_canonical: bool = True,
) -> list[dict[str, Any]]:
    return loaders_dedupe_entity_profiles(
        profiles,
        normalize_storefront_path_fn=_normalize_storefront_path,
        duplicate_suffix_base_url_fn=_duplicate_suffix_base_url,
        prefer_canonical=prefer_canonical,
    )


def _category_quality_score(category: dict[str, Any]) -> tuple[int, int, int, int, int]:
    url = (((category.get("custom_url") or {}).get("url")) or category.get("category_url") or "").strip().lower()
    page_title = (category.get("page_title") or "").strip()
    description = re.sub(r"<[^>]+>", "", category.get("description") or "").strip()
    keyword_count = len(category.get("meta_keywords") or [])
    numeric_suffix_penalty = 1 if re.search(r"-\d+/?$", url) else 0
    return (
        1 if page_title else 0,
        1 if len(description) >= 40 else 0,
        keyword_count,
        -numeric_suffix_penalty,
        -len(url),
    )


def _pick_canonical_category_ids(categories: list[dict[str, Any]]) -> set[int]:
    known_urls = {
        _normalize_storefront_path((((category.get("custom_url") or {}).get("url")) or category.get("category_url") or ""))
        for category in categories
        if (((category.get("custom_url") or {}).get("url")) or category.get("category_url") or "").strip()
    }
    grouped: dict[str, list[dict[str, Any]]] = {}
    for category in categories:
        grouped.setdefault(_canonical_category_group_key(category, known_urls), []).append(category)

    canonical_ids: set[int] = set()
    for group_rows in grouped.values():
        best = max(group_rows, key=_category_quality_score)
        if best.get("id") is not None:
            canonical_ids.add(int(best["id"]))
    return canonical_ids


def _entity_storage_id(entity_type: str, bc_entity_id: int | None) -> int | None:
    if bc_entity_id is None:
        return None
    numeric_id = abs(int(bc_entity_id))
    if entity_type == "product":
        return numeric_id if numeric_id <= PG_INT_MAX else None
    offset = ENTITY_STORAGE_OFFSETS.get(entity_type)
    if offset is None:
        candidate = -numeric_id
    else:
        candidate = -(offset + numeric_id)
    if candidate < PG_INT_MIN or candidate > PG_INT_MAX:
        return None
    return candidate


def _entity_bc_id(entity_type: str, storage_id: int | None) -> int | None:
    if storage_id is None:
        return None
    numeric_id = int(storage_id)
    if entity_type == "product":
        return numeric_id
    offset = ENTITY_STORAGE_OFFSETS.get(entity_type)
    if offset is None:
        return abs(numeric_id)
    return abs(numeric_id) - offset


def _normalize_mapping_review_statuses(store_hash: str) -> dict[str, int]:
    return readiness_normalize_mapping_review_statuses(store_hash)


def list_pending_mapping_reviews(store_hash: str, limit: int = 100) -> list[dict[str, Any]]:
    return readiness_list_pending_mapping_reviews(store_hash, limit=limit)


def auto_resolve_pending_mappings(store_hash: str, reviewed_by: str | None = None) -> dict[str, Any]:
    return readiness_auto_resolve_pending_mappings(
        store_hash,
        reviewed_by=reviewed_by,
        get_store_profile_summary=get_store_profile_summary,
    )


def review_mapping_rows(
    store_hash: str,
    mapping_refs: list[str],
    review_status: str,
    reviewed_by: str | None = None,
    note: str | None = None,
) -> dict[str, int]:
    return readiness_review_mapping_rows(
        store_hash,
        mapping_refs,
        review_status,
        reviewed_by=reviewed_by,
        note=note,
        get_store_profile_summary=get_store_profile_summary,
    )


def get_store_readiness(store_hash: str) -> dict[str, Any]:
    return readiness_get_store_readiness(store_hash)


def category_publishing_enabled_for_store(store_hash: str) -> bool:
    return readiness_category_publishing_enabled_for_store(store_hash)


def refresh_store_readiness(store_hash: str) -> dict[str, Any]:
    return readiness_refresh_store_readiness(
        store_hash,
        get_store_profile_summary=get_store_profile_summary,
    )


def sync_store_catalog_profiles(store_hash: str, initiated_by: str | None = None) -> dict[str, Any]:
    return catalog_sync_store_catalog_profiles(
        store_hash,
        initiated_by=initiated_by,
        seed_store_attribute_buckets=_seed_store_attribute_buckets,
        seed_store_cluster_rules=_seed_store_cluster_rules,
        sync_store_storefront_sites=sync_store_storefront_sites,
        normalize_storefront_path=_normalize_storefront_path,
        pick_canonical_product_ids=_pick_canonical_product_ids,
        pick_canonical_category_ids=_pick_canonical_category_ids,
        infer_bucket_from_option_name=_infer_bucket_from_option_name,
        canonicalize_attribute_value=_canonicalize_attribute_value,
        extract_attribute_terms=_extract_attribute_terms,
        slugify_value=_slugify_value,
        build_cluster_profile=_build_cluster_profile,
        canonical_product_group_key=_canonical_product_group_key,
        product_eligible_for_routing=_product_eligible_for_routing,
        canonical_category_group_key=_canonical_category_group_key,
        category_eligible_for_routing=_category_eligible_for_routing,
        serialize_attribute_profile=_serialize_attribute_profile,
        refresh_store_intent_signal_enrichments=refresh_store_intent_signal_enrichments,
        normalize_mapping_review_statuses=_normalize_mapping_review_statuses,
        refresh_store_readiness=refresh_store_readiness,
    )


def sync_bigcommerce_integration(store_hash: str) -> dict[str, Any]:
    return merchant_sync_bigcommerce_integration(store_hash)


def get_store_publish_settings(store_hash: str) -> dict[str, Any]:
    return merchant_get_store_publish_settings(store_hash)


def upsert_store_publish_settings(
    store_hash: str,
    *,
    publishing_enabled: bool | None = None,
    category_publishing_enabled: bool | None = None,
    metadata_updates: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return merchant_upsert_store_publish_settings(
        store_hash,
        publishing_enabled=publishing_enabled,
        category_publishing_enabled=category_publishing_enabled,
        metadata_updates=metadata_updates,
    )


def build_setup_context(store_hash: str) -> dict[str, Any]:
    return merchant_build_setup_context(store_hash)


def build_store_readiness_snapshot(store_hash: str) -> dict[str, Any]:
    return merchant_build_store_readiness_snapshot(store_hash)


def merchant_landing_path(store_hash: str) -> str:
    return merchant_landing_path_impl(store_hash)


def build_google_authorization_url(integration_key: str, store_hash: str | None = None) -> tuple[str, str]:
    return merchant_build_google_authorization_url(integration_key, store_hash=store_hash)


def decode_google_oauth_state(state: str | None) -> dict[str, str]:
    return merchant_decode_google_oauth_state(state)


def complete_google_oauth(
    integration_key: str,
    *,
    store_hash: str,
    state: str,
    authorization_response: str,
) -> dict[str, Any]:
    return merchant_complete_google_oauth(
        integration_key,
        store_hash=store_hash,
        state=state,
        authorization_response=authorization_response,
    )


def select_google_resource(
    store_hash: str,
    *,
    integration_key: str,
    selected_resource_id: str,
) -> dict[str, Any]:
    return merchant_select_google_resource(
        store_hash,
        integration_key=integration_key,
        selected_resource_id=selected_resource_id,
    )


def enqueue_integration_sync(
    store_hash: str,
    integration_key: str,
    *,
    triggered_by: str = "merchant",
    selected_resource_id: str | None = None,
    metadata_updates: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return merchant_enqueue_integration_sync(
        store_hash,
        integration_key,
        triggered_by=triggered_by,
        selected_resource_id=selected_resource_id,
        metadata_updates=metadata_updates,
    )


def run_integration_sync_run(sync_run_id: int) -> dict[str, Any]:
    return merchant_run_integration_sync_run(sync_run_id)


def process_queued_integration_syncs(
    *,
    limit: int = 10,
    store_hash: str | None = None,
    integration_key: str | None = None,
    expire_running_after_minutes: int = 30,
) -> dict[str, Any]:
    return merchant_process_queued_integration_syncs(
        limit=limit,
        store_hash=store_hash,
        integration_key=integration_key,
        expire_running_after_minutes=expire_running_after_minutes,
    )


def get_store_integration_data_summary(
    store_hash: str,
    integration_key: str,
    selected_resource_id: str | None = None,
) -> dict[str, Any]:
    return merchant_get_store_integration_data_summary(
        store_hash,
        integration_key,
        selected_resource_id=selected_resource_id,
    )


def list_store_integrations(store_hash: str) -> list[dict[str, Any]]:
    return merchant_list_store_integrations(store_hash)


def evaluate_theme_verification(store_hash: str, *, persist: bool = False) -> dict[str, Any]:
    return merchant_evaluate_theme_verification(store_hash, persist=persist)


def apply_theme_automatic_fix(store_hash: str) -> dict[str, Any]:
    return merchant_apply_theme_automatic_fix(store_hash)


def purge_store_data_on_uninstall(store_hash: str) -> dict[str, int]:
    return merchant_purge_store_data_on_uninstall(store_hash)


def load_store_product_profiles(store_hash: str, product_urls: list[str]) -> dict[str, dict[str, Any]]:
    return loaders_load_store_product_profiles(
        store_hash,
        product_urls,
        get_pg_conn_fn=get_pg_conn,
        normalize_store_hash_fn=normalize_store_hash,
        tokenize_intent_text_fn=_tokenize_intent_text,
        extract_attribute_terms_fn=_extract_attribute_terms,
    )


def load_store_category_profiles(store_hash: str, category_urls: list[str] | None = None, canonical_only: bool = False) -> dict[str, dict[str, Any]]:
    return loaders_load_store_category_profiles(
        store_hash,
        category_urls,
        canonical_only,
        get_pg_conn_fn=get_pg_conn,
        normalize_store_hash_fn=normalize_store_hash,
        tokenize_intent_text_fn=_tokenize_intent_text,
        extract_attribute_terms_fn=_extract_attribute_terms,
    )


def _normalize_storefront_path(url: str | None) -> str:
    raw = (url or "").strip()
    if not raw:
        return ""
    if raw.startswith("/"):
        path = raw
    else:
        parsed = urlparse(raw)
        path = parsed.path or raw
    path = re.sub(r"/{2,}", "/", path).strip()
    if not path.startswith("/"):
        path = "/" + path
    if path != "/" and not path.endswith("/"):
        path += "/"
    return path.lower()


def _humanize_url_path_title(url_path: str | None) -> str:
    return loaders_humanize_url_path_title(
        url_path,
        normalize_storefront_path_fn=_normalize_storefront_path,
    )


def _looks_like_content_path(url_path: str | None) -> bool:
    return loaders_looks_like_content_path(
        url_path,
        normalize_storefront_path_fn=_normalize_storefront_path,
    )


def _synthetic_content_entity_id(url_path: str | None) -> int:
    return loaders_synthetic_content_entity_id(
        url_path,
        normalize_storefront_path_fn=_normalize_storefront_path,
    )


def _load_ga4_page_metrics(store_hash: str, urls: list[str], days: int = 90) -> dict[str, dict[str, Any]]:
    return loaders_load_ga4_page_metrics(
        store_hash,
        urls,
        days=days,
        get_pg_conn_fn=get_pg_conn,
        normalize_store_hash_fn=normalize_store_hash,
        normalize_storefront_path_fn=_normalize_storefront_path,
    )


def load_store_brand_profiles(store_hash: str) -> dict[str, dict[str, Any]]:
    return loaders_load_store_brand_profiles(
        store_hash,
        get_pg_conn_fn=get_pg_conn,
        normalize_storefront_path_fn=_normalize_storefront_path,
        extract_attribute_terms_fn=_extract_attribute_terms,
        tokenize_intent_text_fn=_tokenize_intent_text,
        build_cluster_profile_fn=_build_cluster_profile,
        dedupe_entity_profiles_fn=_dedupe_entity_profiles,
        list_store_brands_fn=platform_list_store_brands,
    )


def _load_reserved_storefront_urls(store_hash: str) -> set[str]:
    return loaders_load_reserved_storefront_urls(
        store_hash,
        get_pg_conn_fn=get_pg_conn,
        normalize_store_hash_fn=normalize_store_hash,
        normalize_storefront_path_fn=_normalize_storefront_path,
    )


def load_store_content_profiles(store_hash: str, include_backlog: bool = False) -> dict[str, dict[str, Any]]:
    return loaders_load_store_content_profiles(
        store_hash,
        include_backlog,
        get_pg_conn_fn=get_pg_conn,
        normalize_storefront_path_fn=_normalize_storefront_path,
        looks_like_content_path_fn=_looks_like_content_path,
        load_reserved_storefront_urls_fn=_load_reserved_storefront_urls,
        extract_attribute_terms_fn=_extract_attribute_terms,
        tokenize_intent_text_fn=_tokenize_intent_text,
        build_cluster_profile_fn=_build_cluster_profile,
        synthetic_content_entity_id_fn=_synthetic_content_entity_id,
        humanize_url_path_title_fn=_humanize_url_path_title,
        dedupe_entity_profiles_fn=_dedupe_entity_profiles,
    )


def _build_unified_entity_index(store_hash: str, cluster: str | None = None) -> dict[str, Any]:
    return entity_build_unified_entity_index(
        store_hash,
        cluster=cluster,
        load_all_store_product_profiles_fn=lambda normalized_store_hash, cluster_name=None: _load_all_store_product_profiles(
            normalized_store_hash,
            cluster=cluster_name,
        ),
        load_store_category_profiles_fn=load_store_category_profiles,
        load_store_brand_profiles_fn=load_store_brand_profiles,
        load_store_content_profiles_fn=load_store_content_profiles,
        normalize_storefront_path_fn=_normalize_storefront_path,
        profile_matches_cluster_fn=_profile_matches_cluster,
        load_ga4_page_metrics_fn=lambda urls, days: _load_ga4_page_metrics(store_hash, urls, days=days),
    )


def load_best_product_profiles(store_hash: str, product_urls: list[str]) -> dict[str, dict[str, Any]]:
    store_profiles = load_store_product_profiles(store_hash, product_urls)
    missing_urls = [url for url in product_urls if url and url not in store_profiles]
    if not missing_urls:
        return store_profiles

    fallback_profiles = load_product_profiles(missing_urls)
    return {**fallback_profiles, **store_profiles}


def get_store_profile_summary(store_hash: str) -> dict[str, Any]:
    return storefront_get_store_profile_summary(store_hash)


def _cluster_filters_sql(cluster: str | None) -> tuple[str, list[Any]]:
    if not cluster:
        return "", []

    patterns = CLUSTER_PATTERNS.get(cluster.lower())
    if not patterns:
        return "", []

    condition = " AND (" + " OR ".join(["sp.product_custom_url ILIKE %s"] * len(patterns)) + ")"
    return condition, patterns


def _profile_matches_cluster(profile: dict[str, Any] | None, cluster: str | None) -> bool:
    return entity_profile_matches_cluster(profile, cluster)


def _store_category_competition_enabled(store_hash: str, cluster: str | None) -> bool:
    return category_store_category_competition_enabled(
        store_hash,
        cluster,
        normalize_store_hash_fn=normalize_store_hash,
        store_category_competition=STORE_CATEGORY_COMPETITION,
    )


def _load_canonical_cluster_categories(store_hash: str, cluster: str | None) -> list[dict[str, Any]]:
    return category_load_canonical_cluster_categories(
        store_hash,
        cluster,
        load_store_category_profiles_fn=load_store_category_profiles,
        profile_matches_cluster_fn=_profile_matches_cluster,
        category_competition_url_hints=CATEGORY_COMPETITION_URL_HINTS,
    )


def _category_anchor_label_for_cluster(cluster: str | None, category_profile: dict[str, Any]) -> str:
    return category_category_anchor_label_for_cluster(
        cluster,
        category_profile,
        category_competition_specific_hints=CATEGORY_COMPETITION_SPECIFIC_HINTS,
        category_cluster_labels=CATEGORY_CLUSTER_LABELS,
        normalize_anchor_text_fn=_normalize_anchor_text,
        label_from_target_url_fn=_label_from_target_url,
    )


def _category_competition_specificity_bonus(
    cluster: str | None,
    source_row: dict[str, Any],
    source_profile: dict[str, Any],
    category_profile: dict[str, Any],
) -> tuple[float, str | None]:
    return category_category_competition_specificity_bonus(
        cluster,
        source_row,
        source_profile,
        category_profile,
        category_competition_specific_hints=CATEGORY_COMPETITION_SPECIFIC_HINTS,
        tokenize_intent_text_fn=_tokenize_intent_text,
    )


def _build_pdp_category_competition_candidate(
    store_hash: str,
    cluster: str | None,
    source_row: dict[str, Any],
    source_profile: dict[str, Any],
    broad_query_profile: dict[str, Any],
    category_profile: dict[str, Any],
) -> dict[str, Any] | None:
    return category_build_pdp_category_competition_candidate(
        store_hash,
        cluster,
        source_row,
        source_profile,
        broad_query_profile,
        category_profile,
        build_intent_profile_fn=build_intent_profile,
        entity_storage_id_fn=_entity_storage_id,
        category_competition_specificity_bonus_fn=_category_competition_specificity_bonus,
        category_anchor_label_for_cluster_fn=_category_anchor_label_for_cluster,
    )


def _load_all_store_product_profiles(store_hash: str, cluster: str | None = None) -> list[dict[str, Any]]:
    return entity_load_all_store_product_profiles(
        store_hash,
        cluster=cluster,
        profile_matches_cluster_fn=_profile_matches_cluster,
        tokenize_intent_text_fn=_tokenize_intent_text,
        extract_attribute_terms_fn=_extract_attribute_terms,
    )


def _build_category_descendants(category_profiles: list[dict[str, Any]]) -> dict[int, set[int]]:
    return category_build_category_descendants(category_profiles)


def _shared_subclusters(source_profile: dict[str, Any], target_profile: dict[str, Any]) -> set[str]:
    return category_shared_subclusters(source_profile, target_profile)


def _generate_category_source_candidates(store_hash: str, cluster: str | None = None) -> list[dict[str, Any]]:
    return category_generate_category_source_candidates(
        store_hash,
        cluster=cluster,
        load_store_category_profiles_fn=load_store_category_profiles,
        load_all_store_product_profiles_fn=lambda normalized_store_hash, cluster_name=None: _load_all_store_product_profiles(
            normalized_store_hash,
            cluster=cluster_name,
        ),
        profile_matches_cluster_fn=_profile_matches_cluster,
        build_category_descendants_fn=_build_category_descendants,
        entity_storage_id_fn=_entity_storage_id,
        shared_subclusters_fn=_shared_subclusters,
        build_intent_profile_fn=build_intent_profile,
        select_category_product_anchor_label_fn=_select_category_product_anchor_label,
    )


def _fetch_gsc_query_page_evidence(
    store_hash: str,
    source_urls: list[str],
    min_hit_count: int = 3,
    limit_total: int = 300,
) -> list[dict[str, Any]]:
    normalized_store_hash = normalize_store_hash(store_hash)
    normalized_urls = sorted({_normalize_storefront_path(url) for url in source_urls if url})
    if not normalized_urls:
        return []

    # Keep small scoped runs actually small. We still over-fetch for ranking,
    # but not so aggressively that a family-level dry run balloons into a
    # full-store pass.
    fetch_limit = max(limit_total * 2, 80)
    sql = """
        WITH raw_pages AS (
            SELECT
                CASE
                    WHEN page ~ '^https?://' THEN regexp_replace(page, '^https?://[^/]+', '')
                    ELSE page
                END AS raw_path,
                query,
                date,
                clicks,
                impressions,
                position
            FROM app_runtime.store_gsc_daily
            WHERE store_hash = %s
              AND date >= CURRENT_DATE - INTERVAL '90 days'
              AND query IS NOT NULL
              AND page IS NOT NULL
        ),
        evidence AS (
            SELECT
                lower(
                    CASE
                        WHEN raw_path = '/' THEN '/'
                        WHEN right(raw_path, 1) = '/' THEN raw_path
                        ELSE raw_path || '/'
                    END
                ) AS source_url,
                query,
                SUM(clicks) FILTER (WHERE date >= CURRENT_DATE - INTERVAL '28 days') AS clicks_28d,
                SUM(impressions) FILTER (WHERE date >= CURRENT_DATE - INTERVAL '28 days') AS impressions_28d,
                CASE
                    WHEN SUM(impressions) FILTER (WHERE date >= CURRENT_DATE - INTERVAL '28 days') > 0
                        THEN SUM(clicks) FILTER (WHERE date >= CURRENT_DATE - INTERVAL '28 days')::double precision
                             / SUM(impressions) FILTER (WHERE date >= CURRENT_DATE - INTERVAL '28 days')
                    ELSE 0
                END AS ctr_28d,
                AVG(position) FILTER (WHERE date >= CURRENT_DATE - INTERVAL '28 days') AS avg_position_28d,
                SUM(clicks) AS clicks_90d,
                SUM(impressions) AS impressions_90d,
                CASE
                    WHEN SUM(impressions) > 0 THEN SUM(clicks)::double precision / SUM(impressions)
                    ELSE 0
                END AS ctr_90d,
                AVG(position) AS avg_position_90d
            FROM raw_pages
            GROUP BY 1, 2
        )
        SELECT
            source_url,
            query,
            clicks_28d,
            impressions_28d,
            ctr_28d,
            avg_position_28d,
            clicks_90d,
            impressions_90d,
            ctr_90d,
            avg_position_90d
        FROM evidence
        WHERE source_url = ANY(%s)
          AND (clicks_90d >= %s OR impressions_90d >= %s)
        ORDER BY impressions_90d DESC, clicks_90d DESC, avg_position_90d ASC
        LIMIT %s;
    """
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                sql,
                (
                    normalized_store_hash,
                    normalized_urls,
                    min_hit_count,
                    max(min_hit_count, 1),
                    fetch_limit,
                ),
            )
            return [dict(row) for row in cur.fetchall()]


def _build_query_gate_records(
    store_hash: str,
    source_profiles: dict[str, dict[str, Any]],
    target_entities: list[dict[str, Any]],
    min_hit_count: int = 3,
    limit_total: int = 300,
) -> list[dict[str, Any]]:
    return gate_build_query_gate_records(
        store_hash,
        source_profiles,
        target_entities,
        min_hit_count=min_hit_count,
        limit_total=limit_total,
        build_store_signal_library=_build_store_signal_library,
        fetch_gsc_query_page_evidence=lambda source_urls, min_hit_count=3, limit_total=300: _fetch_gsc_query_page_evidence(
            store_hash,
            source_urls,
            min_hit_count=min_hit_count,
            limit_total=limit_total,
        ),
        normalize_storefront_path=_normalize_storefront_path,
        normalize_query_family_key=_normalize_query_family_key,
        build_query_gate_record=_build_query_gate_record,
    )


def _store_query_gate_records(run_id: int, store_hash: str, gate_rows: list[dict[str, Any]]) -> None:
    gate_store_query_gate_records(run_id, store_hash, gate_rows)


def _looks_informational_query(query: str | None) -> bool:
    return direct_looks_informational_query(query, tokenize_intent_text_fn=_tokenize_intent_text)


def _entity_type_fit_adjustment(
    query: str | None,
    preferred_entity_type: str | None,
    target_entity_type: str,
    fuzzy_signal: dict[str, Any] | None = None,
    current_page: bool = False,
    source_query_topic_match_count: int = 0,
    has_brand_signal: bool = False,
    has_collection_signal: bool = False,
    has_sku_signal: bool = False,
) -> tuple[float, str | None]:
    return direct_entity_type_fit_adjustment(
        query=query,
        preferred_entity_type=preferred_entity_type,
        target_entity_type=target_entity_type,
        fuzzy_signal=fuzzy_signal,
        current_page=current_page,
        source_query_topic_match_count=source_query_topic_match_count,
        has_brand_signal=has_brand_signal,
        has_collection_signal=has_collection_signal,
        has_sku_signal=has_sku_signal,
        looks_informational_query_fn=_looks_informational_query,
    )


def _target_prefilter(
    query: str,
    target_profile: dict[str, Any],
) -> bool:
    return direct_target_prefilter(
        query,
        target_profile,
        tokenize_intent_text_fn=_tokenize_intent_text,
        extract_attribute_terms_fn=_extract_attribute_terms,
        fuzzy_match_score_fn=_fuzzy_match_score,
        generic_routing_tokens=GENERIC_ROUTING_TOKENS,
    )


def _direct_route_candidates_from_gsc(
    store_hash: str,
    cluster: str | None = None,
    min_hit_count: int = 3,
    limit_total: int = 300,
    entity_index: dict[str, Any] | None = None,
    gate_rows: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    return direct_direct_route_candidates_from_gsc(
        store_hash,
        cluster=cluster,
        min_hit_count=min_hit_count,
        limit_total=limit_total,
        entity_index=entity_index,
        gate_rows=gate_rows,
        build_unified_entity_index_fn=lambda normalized_store_hash, cluster_name=None: _build_unified_entity_index(
            normalized_store_hash,
            cluster=cluster_name,
        ),
        load_query_target_overrides_fn=_load_query_target_overrides,
        load_review_feedback_maps_fn=_load_review_feedback_maps,
        build_query_gate_records_fn=_build_query_gate_records,
        normalize_storefront_path_fn=_normalize_storefront_path,
        entity_storage_id_fn=_entity_storage_id,
        gate_row_query_signal_context_fn=_gate_row_query_signal_context,
        query_target_override_key_fn=_query_target_override_key,
        target_prefilter_fn=_target_prefilter,
        build_intent_profile_fn=build_intent_profile,
        build_review_feedback_signal_fn=_build_review_feedback_signal,
        entity_type_fit_adjustment_fn=_entity_type_fit_adjustment,
        append_reason_summary_fn=_append_reason_summary,
    )


def create_run(
    store_hash: str,
    initiated_by: str | None,
    run_source: str = "manual",
    filters: dict[str, Any] | None = None,
    status: str = "running",
) -> int:
    filters = filters or {}
    sql = """
        INSERT INTO app_runtime.link_runs (store_hash, initiated_by, run_source, status, filters, started_at)
        VALUES (%s, %s, %s, %s, %s::jsonb, NOW())
        RETURNING run_id;
    """
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (store_hash, initiated_by, run_source, status, json.dumps(filters)))
            run_id = cur.fetchone()[0]
        conn.commit()
    return run_id


def complete_run(run_id: int, status: str, notes: str | None = None) -> None:
    sql = """
        UPDATE app_runtime.link_runs
        SET status = %s,
            notes = COALESCE(%s, notes),
            completed_at = NOW()
        WHERE run_id = %s;
    """
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (status, notes, run_id))
        conn.commit()


def mark_run_running(run_id: int) -> None:
    sql = """
        UPDATE app_runtime.link_runs
        SET status = 'running',
            completed_at = NULL
        WHERE run_id = %s;
    """
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (run_id,))
        conn.commit()


def get_run(run_id: int) -> dict[str, Any] | None:
    sql = """
        SELECT run_id, store_hash, initiated_by, run_source, status, filters, notes, started_at, completed_at
        FROM app_runtime.link_runs
        WHERE run_id = %s
        LIMIT 1;
    """
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (run_id,))
            row = cur.fetchone()
    return dict(row) if row else None


def find_active_run(store_hash: str) -> dict[str, Any] | None:
    sql = """
        SELECT run_id, store_hash, initiated_by, run_source, status, filters, notes, started_at, completed_at
        FROM app_runtime.link_runs
        WHERE store_hash = %s
          AND status = ANY(%s)
        ORDER BY started_at DESC
        LIMIT 10;
    """
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (normalize_store_hash(store_hash), list(GENERATION_ACTIVE_STATUSES)))
            rows = [dict(row) for row in cur.fetchall()]

    now_utc = datetime.now().astimezone()
    for row in rows:
        started_at = row.get("started_at")
        if isinstance(started_at, datetime) and (now_utc - started_at) > ACTIVE_RUN_STALE_AFTER:
            complete_run(
                int(row["run_id"]),
                "failed",
                notes=f"Marked stale after exceeding {int(ACTIVE_RUN_STALE_AFTER.total_seconds() // 3600)} hours without completion.",
            )
            continue
        return row
    return None


def _generation_worker_script_path() -> Path:
    return FULCRUM_DIR / "generation_job.py"


def start_generation_worker(run_id: int) -> dict[str, Any]:
    script_path = _generation_worker_script_path()
    if not script_path.exists():
        return {"started": False, "reason": f"Missing worker script at {script_path}"}

    creationflags = 0
    popen_kwargs: dict[str, Any] = {
        "cwd": str(Path(Config.BASE_DIR)),
        "stdout": subprocess.DEVNULL,
        "stderr": subprocess.DEVNULL,
    }
    if os.name == "nt":
        creationflags |= getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        creationflags |= getattr(subprocess, "DETACHED_PROCESS", 0)
        creationflags |= getattr(subprocess, "CREATE_NO_WINDOW", 0)
        popen_kwargs["creationflags"] = creationflags
    else:
        popen_kwargs["start_new_session"] = True

    process = subprocess.Popen(
        [sys.executable, str(script_path), "--run-id", str(run_id)],
        **popen_kwargs,
    )
    return {"started": True, "pid": int(process.pid)}


def queue_candidate_run(
    store_hash: str,
    initiated_by: str | None = None,
    cluster: str | None = None,
    max_links_per_product: int = 4,
    min_hit_count: int = 3,
    limit_total: int = 300,
    run_source: str = "manual",
) -> dict[str, Any]:
    return runs_queue_candidate_run(
        store_hash,
        initiated_by=initiated_by,
        cluster=cluster,
        max_links_per_product=max_links_per_product,
        min_hit_count=min_hit_count,
        limit_total=limit_total,
        run_source=run_source,
        normalize_store_hash_fn=normalize_store_hash,
        find_active_run_fn=find_active_run,
        create_run_fn=create_run,
        start_generation_worker_fn=start_generation_worker,
        complete_run_fn=complete_run,
    )


def _eligible_auto_publish_candidates(store_hash: str, run_id: int) -> list[dict[str, Any]]:
    return runs_eligible_auto_publish_candidates(
        store_hash,
        run_id,
        refresh_store_readiness_fn=refresh_store_readiness,
        get_pg_conn_fn=get_pg_conn,
        normalize_store_hash_fn=normalize_store_hash,
        category_publishing_enabled_for_store_fn=category_publishing_enabled_for_store,
        auto_publish_min_score=float(Config.FULCRUM_AUTO_PUBLISH_MIN_SCORE),
        auto_publish_max_links_per_source=int(Config.FULCRUM_AUTO_PUBLISH_MAX_LINKS_PER_SOURCE),
    )


def auto_approve_and_publish_run(store_hash: str, run_id: int) -> dict[str, Any]:
    return runs_auto_approve_and_publish_run(
        store_hash,
        run_id,
        auto_publish_enabled=bool(Config.FULCRUM_AUTO_PUBLISH_ENABLED),
        refresh_store_readiness_fn=refresh_store_readiness,
        publish_all_current_results_fn=lambda normalized_store_hash, initiated_by=None: runs_publish_all_current_results(
            normalized_store_hash,
            initiated_by=initiated_by,
            normalize_store_hash_fn=normalize_store_hash,
            category_publishing_enabled_for_store_fn=category_publishing_enabled_for_store,
            list_query_gate_review_requests_fn=list_query_gate_review_requests,
            latest_candidate_rows_for_store_fn=_latest_candidate_rows_for_store,
            include_dashboard_candidate_fn=_include_dashboard_candidate,
            review_candidates_fn=review_candidates,
            publish_approved_entities_fn=publish_approved_entities,
        ),
    )


def _execute_candidate_run(
    run_id: int,
    *,
    store_hash: str,
    cluster: str | None = None,
    max_links_per_product: int = 4,
    min_hit_count: int = 3,
    limit_total: int = 300,
) -> dict[str, Any]:
    return runs_execute_candidate_run_impl(
        run_id,
        store_hash=store_hash,
        cluster=cluster,
        max_links_per_product=max_links_per_product,
        min_hit_count=min_hit_count,
        limit_total=limit_total,
        refresh_store_readiness_fn=refresh_store_readiness,
        build_unified_entity_index_fn=_build_unified_entity_index,
        build_query_gate_records_fn=_build_query_gate_records,
        annotate_query_gate_rows_with_suggestions_fn=_annotate_query_gate_rows_with_suggestions,
        store_query_gate_records_fn=_store_query_gate_records,
        load_query_target_overrides_fn=_load_query_target_overrides,
        load_review_feedback_maps_fn=_load_review_feedback_maps,
        build_store_signal_library_fn=_build_store_signal_library,
        direct_route_candidates_from_gsc_fn=_direct_route_candidates_from_gsc,
        rank_source_rows_fn=_rank_source_rows,
        get_pg_conn_fn=get_pg_conn,
        auto_approve_and_publish_run_fn=auto_approve_and_publish_run,
        complete_run_fn=complete_run,
    )


def execute_candidate_run(run_id: int) -> dict[str, Any]:
    return runs_execute_candidate_run(
        run_id,
        get_run_fn=get_run,
        normalize_store_hash_fn=normalize_store_hash,
        mark_run_running_fn=mark_run_running,
        execute_candidate_run_impl_fn=_execute_candidate_run,
    )


def generate_candidate_run(
    store_hash: str,
    initiated_by: str | None = None,
    cluster: str | None = None,
    max_links_per_product: int = 4,
    min_hit_count: int = 3,
    limit_total: int = 300,
) -> dict[str, Any]:
    return runs_generate_candidate_run(
        store_hash,
        initiated_by=initiated_by,
        cluster=cluster,
        max_links_per_product=max_links_per_product,
        min_hit_count=min_hit_count,
        limit_total=limit_total,
        create_run_fn=create_run,
        execute_candidate_run_impl_fn=_execute_candidate_run,
    )


def list_runs(store_hash: str, limit: int = 10) -> list[dict[str, Any]]:
    return gate_list_runs(store_hash, limit=limit)


def _latest_gate_run_id(store_hash: str) -> int | None:
    return gate_latest_gate_run_id(store_hash)


def summarize_query_gate_dispositions(store_hash: str, run_id: int | None = None) -> dict[str, Any]:
    return gate_summarize_query_gate_dispositions(
        store_hash,
        run_id=run_id,
        latest_gate_run_id_fn=_latest_gate_run_id,
    )


def list_query_gate_records(
    store_hash: str,
    disposition: str | None = None,
    limit: int = 100,
    run_id: int | None = None,
) -> list[dict[str, Any]]:
    return gate_list_query_gate_records(
        store_hash,
        disposition=disposition,
        limit=limit,
        run_id=run_id,
        latest_gate_run_id_fn=_latest_gate_run_id,
    )


def get_query_gate_record_by_id(store_hash: str, gate_record_id: int) -> dict[str, Any] | None:
    return gate_get_query_gate_record_by_id(store_hash, gate_record_id)


def _query_gate_record_map_for_ids(
    store_hash: str,
    gate_record_ids: set[int],
    *,
    run_ids: set[int] | None = None,
    fresh_suggestions: bool = False,
) -> dict[int, dict[str, Any]]:
    return gate_query_gate_record_map_for_ids(
        store_hash,
        gate_record_ids,
        run_ids=run_ids,
        fresh_suggestions=fresh_suggestions,
        build_unified_entity_index=_build_unified_entity_index,
        load_query_target_overrides=_load_query_target_overrides,
        load_review_feedback_maps=_load_review_feedback_maps,
        build_store_signal_library=_build_store_signal_library,
        annotate_query_gate_rows_with_suggestions=_annotate_query_gate_rows_with_suggestions,
        attach_cached_query_gate_suggestions=_attach_cached_query_gate_suggestions,
    )


def request_query_gate_review(
    store_hash: str,
    gate_record_id: int,
    target_entity_type: str | None,
    target_entity_id: int | None,
    target_name: str | None,
    target_url: str | None,
    reason_summary: str | None,
    requested_by: str | None,
    note: str | None = None,
) -> dict[str, Any] | None:
    apply_runtime_schema()
    return workflow_request_query_gate_review(
        store_hash,
        gate_record_id,
        target_entity_type,
        target_entity_id,
        target_name,
        target_url,
        reason_summary,
        requested_by,
        note=note,
        get_query_gate_record_by_id_fn=get_query_gate_record_by_id,
    )


def submit_query_gate_review_session(
    store_hash: str,
    *,
    run_id: int | None,
    submitted_by: str | None,
    all_gate_record_ids: list[Any] | tuple[Any, ...] | set[Any] | None,
    cleared_gate_record_ids: list[Any] | tuple[Any, ...] | set[Any] | None,
    review_bucket_gate_record_ids: list[Any] | tuple[Any, ...] | set[Any] | None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    apply_runtime_schema()
    return sessions_create_query_gate_review_submission(
        store_hash,
        run_id=run_id,
        submitted_by=submitted_by,
        all_gate_record_ids=all_gate_record_ids,
        cleared_gate_record_ids=cleared_gate_record_ids,
        review_bucket_gate_record_ids=review_bucket_gate_record_ids,
        metadata=metadata,
    )


def update_query_gate_review_request_metadata(
    store_hash: str,
    request_id: int,
    metadata_updates: dict[str, Any],
) -> dict[str, Any] | None:
    return workflow_update_query_gate_review_request_metadata(store_hash, request_id, metadata_updates)


def _merge_fresh_gate_context_into_review_row(row: dict[str, Any], gate_row: dict[str, Any] | None) -> None:
    presenters_merge_fresh_gate_context_into_review_row(row, gate_row)


def build_query_gate_human_review_mailto(
    store_hash: str,
    *,
    gate_row: dict[str, Any] | None = None,
    request_row: dict[str, Any] | None = None,
    review: dict[str, Any] | None = None,
    email_to: str | None = None,
) -> str:
    return presenters_build_query_gate_human_review_mailto(
        store_hash,
        app_base_url=Config.FULCRUM_APP_BASE_URL,
        get_store_owner_email_fn=get_store_owner_email,
        normalize_store_hash_fn=normalize_store_hash,
        gate_row=gate_row,
        request_row=request_row,
        review=review,
        email_to=email_to,
    )


def pause_source_for_review(
    store_hash: str,
    source_entity_id: int | None,
    source_entity_type: str = "product",
    reviewed_by: str | None = None,
    note: str | None = None,
) -> dict[str, Any]:
    return workflow_pause_source_for_review(
        store_hash,
        source_entity_id,
        source_entity_type=source_entity_type,
        reviewed_by=reviewed_by,
        note=note,
        latest_candidate_rows_for_store=_latest_candidate_rows_for_store,
        normalize_storefront_path_fn=_normalize_storefront_path,
        entity_bc_id_fn=_entity_bc_id,
        review_candidates_fn=review_candidates,
        unpublish_entities_fn=lambda normalized_store_hash, source_entity_ids: unpublish_entities(
            normalized_store_hash,
            source_entity_ids=source_entity_ids,
        ),
    )


def list_query_gate_review_requests(
    store_hash: str,
    request_status: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    return workflow_list_query_gate_review_requests(store_hash, request_status=request_status, limit=limit)


def get_query_gate_review_request_by_id(store_hash: str, request_id: int) -> dict[str, Any] | None:
    return workflow_get_query_gate_review_request_by_id(store_hash, request_id)


def count_query_gate_review_requests(store_hash: str, request_status: str | None = None) -> int:
    return workflow_count_query_gate_review_requests(store_hash, request_status=request_status)


def resolve_query_gate_review_request(
    store_hash: str,
    request_id: int,
    *,
    resolved_by: str | None = None,
    resolution_note: str | None = None,
    metadata_updates: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    return workflow_resolve_query_gate_review_request(
        store_hash,
        request_id,
        resolved_by=resolved_by,
        resolution_note=resolution_note,
        metadata_updates=metadata_updates,
        get_query_gate_review_request_by_id_fn=get_query_gate_review_request_by_id,
    )


def _review_request_source_key(row: dict[str, Any]) -> tuple[str, int]:
    return runs_review_request_source_key(row)


def _candidate_source_key(row: dict[str, Any]) -> tuple[str, int]:
    return runs_candidate_source_key(row)


def review_all_edge_cases(store_hash: str, initiated_by: str | None = None) -> dict[str, Any]:
    return workflow_review_all_edge_cases(
        store_hash,
        initiated_by=initiated_by,
        list_query_gate_review_requests_fn=list_query_gate_review_requests,
        run_query_gate_agent_review_fn=run_query_gate_agent_review,
    )


def publish_all_current_results(store_hash: str, initiated_by: str | None = None) -> dict[str, Any]:
    return runs_publish_all_current_results(
        store_hash,
        initiated_by=initiated_by,
        normalize_store_hash_fn=normalize_store_hash,
        category_publishing_enabled_for_store_fn=category_publishing_enabled_for_store,
        list_query_gate_review_requests_fn=list_query_gate_review_requests,
        latest_candidate_rows_for_store_fn=_latest_candidate_rows_for_store,
        include_dashboard_candidate_fn=_include_dashboard_candidate,
        review_candidates_fn=review_candidates,
        publish_approved_entities_fn=publish_approved_entities,
    )


def restore_source_after_review(
    store_hash: str,
    source_entity_id: int | None,
    source_entity_type: str,
    *,
    target_entity_type: str | None = None,
    target_entity_id: int | None = None,
    target_url: str | None = None,
    reviewed_by: str | None = None,
    note: str | None = None,
) -> dict[str, Any]:
    return workflow_restore_source_after_review(
        store_hash,
        source_entity_id,
        source_entity_type,
        target_entity_type=target_entity_type,
        target_entity_id=target_entity_id,
        target_url=target_url,
        reviewed_by=reviewed_by,
        note=note,
        latest_candidate_rows_for_store=_latest_candidate_rows_for_store,
        normalize_storefront_path_fn=_normalize_storefront_path,
        entity_bc_id_fn=_entity_bc_id,
        review_candidates_fn=review_candidates,
        publish_approved_entities_fn=lambda normalized_store_hash, source_entity_ids: publish_approved_entities(
            normalized_store_hash,
            source_entity_ids=source_entity_ids,
        ),
    )


def _candidate_target_bc_entity_id(row: dict[str, Any]) -> int | None:
    return workflow_candidate_target_bc_entity_id(row, entity_bc_id_fn=_entity_bc_id)


def _candidate_source_bc_entity_id(row: dict[str, Any]) -> int | None:
    return workflow_candidate_source_bc_entity_id(row, entity_bc_id_fn=_entity_bc_id)


def _candidate_matches_review_source(
    row: dict[str, Any],
    *,
    source_entity_type: str | None = None,
    source_entity_id: int | None = None,
    source_url: str | None = None,
) -> bool:
    return workflow_candidate_matches_review_source(
        row,
        source_entity_type=source_entity_type,
        source_entity_id=source_entity_id,
        source_url=source_url,
        normalize_storefront_path_fn=_normalize_storefront_path,
        entity_bc_id_fn=_entity_bc_id,
    )


def _candidate_matches_review_target(
    row: dict[str, Any],
    *,
    target_entity_type: str | None = None,
    target_entity_id: int | None = None,
    target_url: str | None = None,
) -> bool:
    return workflow_candidate_matches_review_target(
        row,
        target_entity_type=target_entity_type,
        target_entity_id=target_entity_id,
        target_url=target_url,
        normalize_storefront_path_fn=_normalize_storefront_path,
        entity_bc_id_fn=_entity_bc_id,
    )


def _gate_review_cluster_values(
    verdict: str | None,
    issue_type: str | None,
    recommended_action: str | None,
) -> tuple[str, str]:
    return review_gate_review_cluster_values(verdict, issue_type, recommended_action)


def _agent_review_signal_snapshot(signals: dict[str, Any]) -> dict[str, list[str]]:
    return review_agent_review_signal_snapshot(signals)


def _serialize_query_gate_row_for_agent_review(row: dict[str, Any]) -> dict[str, Any]:
    return review_serialize_query_gate_row_for_agent_review(row)


def _parse_agent_json_list(raw_content: str | None) -> list[dict[str, Any]]:
    return review_parse_agent_json_list(raw_content)


def _normalize_gate_review_item(item: dict[str, Any]) -> dict[str, Any]:
    return review_normalize_gate_review_item(item)


def _review_query_gate_rows_with_agent(
    store_hash: str,
    annotated_rows: list[dict[str, Any]],
    initiated_by: str | None = None,
) -> dict[str, Any]:
    return review_query_gate_rows_with_agent_impl(store_hash, annotated_rows, initiated_by=initiated_by)


def _store_query_gate_agent_reviews(
    store_hash: str,
    run_id: int,
    reviews: list[dict[str, Any]],
    gate_rows: dict[int, dict[str, Any]],
    model_name: str | None = None,
    created_by: str | None = None,
) -> int:
    return review_store_query_gate_agent_reviews(
        store_hash,
        run_id,
        reviews,
        gate_rows,
        model_name=model_name,
        created_by=created_by,
    )


def list_query_gate_agent_reviews(
    store_hash: str,
    run_id: int | None = None,
    verdict: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    return review_list_query_gate_agent_reviews(
        store_hash,
        run_id=run_id,
        verdict=verdict,
        limit=limit,
        apply_runtime_schema_fn=apply_runtime_schema,
        latest_gate_run_id_fn=_latest_gate_run_id,
    )


def summarize_query_gate_agent_reviews(store_hash: str, run_id: int | None = None) -> dict[str, Any]:
    return review_summarize_query_gate_agent_reviews(
        store_hash,
        run_id=run_id,
        apply_runtime_schema_fn=apply_runtime_schema,
        latest_gate_run_id_fn=_latest_gate_run_id,
    )


def list_query_gate_agent_review_clusters(
    store_hash: str,
    run_id: int | None = None,
    limit: int = 10,
) -> list[dict[str, Any]]:
    return review_list_query_gate_agent_review_clusters(
        store_hash,
        run_id=run_id,
        limit=limit,
        apply_runtime_schema_fn=apply_runtime_schema,
        latest_gate_run_id_fn=_latest_gate_run_id,
    )


def _load_query_gate_agent_review_map(store_hash: str, run_id: int | None = None) -> dict[int, dict[str, Any]]:
    return review_load_query_gate_agent_review_map(
        store_hash,
        run_id=run_id,
        list_query_gate_agent_reviews_fn=list_query_gate_agent_reviews,
        query_gate_record_map_for_ids_fn=_query_gate_record_map_for_ids,
        gate_row_semantics_analysis_fn=_gate_row_semantics_analysis,
    )


def _postprocess_gate_agent_reviews(
    reviews: list[dict[str, Any]],
    gate_row_map: dict[int, dict[str, Any]],
) -> list[dict[str, Any]]:
    return review_postprocess_gate_agent_reviews(
        reviews,
        gate_row_map,
        gate_row_semantics_analysis_fn=_gate_row_semantics_analysis,
    )


def run_query_gate_agent_review(
    store_hash: str,
    run_id: int | None = None,
    disposition: str | None = None,
    limit: int = 40,
    cluster: str | None = None,
    initiated_by: str | None = None,
    gate_record_ids: list[int] | None = None,
) -> dict[str, Any]:
    return review_run_query_gate_agent_review(
        store_hash,
        run_id=run_id,
        disposition=disposition,
        limit=limit,
        cluster=cluster,
        initiated_by=initiated_by,
        gate_record_ids=gate_record_ids,
        apply_runtime_schema_fn=apply_runtime_schema,
        get_query_gate_record_by_id_fn=get_query_gate_record_by_id,
        latest_gate_run_id_fn=_latest_gate_run_id,
        list_query_gate_records_fn=list_query_gate_records,
        annotate_query_gate_rows_with_suggestions_fn=_annotate_query_gate_rows_with_suggestions,
        summarize_query_gate_agent_reviews_fn=summarize_query_gate_agent_reviews,
        list_query_gate_agent_review_clusters_fn=list_query_gate_agent_review_clusters,
        gate_row_semantics_analysis_fn=_gate_row_semantics_analysis,
        review_query_gate_rows_with_agent_fn=_review_query_gate_rows_with_agent,
        store_query_gate_agent_reviews_fn=_store_query_gate_agent_reviews,
    )


def _query_target_override_key(normalized_query_key: str | None, source_url: str | None) -> tuple[str, str]:
    return suggestions_query_target_override_key(
        normalized_query_key,
        source_url,
        normalize_query_family_key_fn=_normalize_query_family_key,
        normalize_storefront_path_fn=_normalize_storefront_path,
    )


def _gate_row_query_signal_context(gate_row: dict[str, Any]) -> dict[str, Any] | None:
    return semantics_gate_row_query_signal_context(gate_row)


def _gate_row_semantics_analysis(
    gate_row: dict[str, Any],
    store_hash: str,
    *,
    signal_library: dict[str, list[dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    return semantics_gate_row_semantics_analysis(
        gate_row,
        store_hash,
        signal_library=signal_library,
        gate_row_query_signal_context_fn=_gate_row_query_signal_context,
        resolve_query_signal_context_fn=_resolve_query_signal_context,
        build_query_semantics_analysis_fn=_build_query_semantics_analysis,
    )


def _gate_row_current_page_snapshot(
    gate_row: dict[str, Any],
    source_profile: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    return semantics_gate_row_current_page_snapshot(
        gate_row,
        source_profile=source_profile,
        normalize_storefront_path_fn=_normalize_storefront_path,
    )


def _semantics_target_block_reason(
    semantics_analysis: dict[str, Any],
    target_profile: dict[str, Any] | None,
) -> str | None:
    return semantics_semantics_target_block_reason(
        semantics_analysis,
        target_profile,
        tokenize_intent_text_fn=_tokenize_intent_text,
        normalize_signal_label_fn=_normalize_signal_label,
        semantic_pluralize_fn=_semantic_pluralize,
    )


def _apply_semantics_control_to_ranked_targets(
    gate_row: dict[str, Any],
    ranked_targets: list[dict[str, Any]],
    *,
    store_hash: str,
    source_profile: dict[str, Any] | None = None,
    target_entities_by_key: dict[tuple[str, int], dict[str, Any]] | None = None,
    source_profiles: dict[str, dict[str, Any]] | None = None,
    target_entities: list[dict[str, Any]] | None = None,
    overrides: dict[tuple[str, str], dict[str, Any]] | None = None,
    review_feedback_maps: dict[str, dict[Any, dict[str, int]]] | None = None,
    signal_library: dict[str, list[dict[str, Any]]] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    return semantics_apply_semantics_control_to_ranked_targets(
        gate_row,
        ranked_targets,
        store_hash=store_hash,
        source_profile=source_profile,
        target_entities_by_key=target_entities_by_key,
        source_profiles=source_profiles,
        target_entities=target_entities,
        overrides=overrides,
        review_feedback_maps=review_feedback_maps,
        signal_library=signal_library,
        gate_row_semantics_analysis_fn=_gate_row_semantics_analysis,
        gate_row_current_page_snapshot_fn=_gate_row_current_page_snapshot,
        semantics_target_block_reason_fn=_semantics_target_block_reason,
        rank_target_options_for_gate_row_fn=_rank_target_options_for_gate_row,
    )


def _load_query_target_overrides(store_hash: str) -> dict[tuple[str, str], dict[str, Any]]:
    return suggestions_load_query_target_overrides(
        store_hash,
        query_target_override_key_fn=_query_target_override_key,
    )


def set_query_target_override(
    store_hash: str,
    normalized_query_key: str,
    source_url: str,
    source_entity_type: str,
    source_entity_id: int | None,
    target_entity_type: str,
    target_entity_id: int,
    created_by: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return suggestions_set_query_target_override(
        store_hash,
        normalized_query_key,
        source_url,
        source_entity_type,
        source_entity_id,
        target_entity_type,
        target_entity_id,
        query_target_override_key_fn=_query_target_override_key,
        created_by=created_by,
        metadata=metadata,
    )


def _rank_target_options_for_gate_row(
    gate_row: dict[str, Any],
    source_profiles: dict[str, dict[str, Any]],
    target_entities: list[dict[str, Any]],
    overrides: dict[tuple[str, str], dict[str, Any]] | None = None,
    review_feedback_maps: dict[str, dict[Any, dict[str, int]]] | None = None,
    limit: int = 2,
    *,
    apply_semantics_control: bool = True,
    semantics_analysis: dict[str, Any] | None = None,
    target_entities_by_key: dict[tuple[str, int], dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    return ranker_rank_target_options_for_gate_row(
        gate_row,
        source_profiles,
        target_entities,
        overrides=overrides,
        review_feedback_maps=review_feedback_maps,
        limit=limit,
        apply_semantics_control=apply_semantics_control,
        semantics_analysis=semantics_analysis,
        target_entities_by_key=target_entities_by_key,
        normalize_storefront_path_fn=_normalize_storefront_path,
        gate_row_query_signal_context_fn=_gate_row_query_signal_context,
        tokenize_intent_text_fn=_tokenize_intent_text,
        gate_row_semantics_analysis_fn=_gate_row_semantics_analysis,
        query_target_override_key_fn=_query_target_override_key,
        semantics_target_block_reason_fn=_semantics_target_block_reason,
        target_prefilter_fn=_target_prefilter,
        build_intent_profile_fn=build_intent_profile,
        build_review_feedback_signal_fn=_build_review_feedback_signal,
        entity_type_fit_adjustment_fn=_entity_type_fit_adjustment,
        append_reason_summary_fn=_append_reason_summary,
        apply_semantics_control_to_ranked_targets_fn=_apply_semantics_control_to_ranked_targets,
    )


def _refresh_query_gate_row_live_state(
    store_hash: str,
    gate_row: dict[str, Any],
    source_profiles: dict[str, dict[str, Any]],
    target_entities: list[dict[str, Any]],
    signal_library: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return ranker_refresh_query_gate_row_live_state(
        store_hash,
        gate_row,
        source_profiles,
        target_entities,
        signal_library=signal_library,
        normalize_storefront_path_fn=_normalize_storefront_path,
        build_query_gate_record_fn=_build_query_gate_record,
        build_store_signal_library_fn=_build_store_signal_library,
    )


def _annotate_query_gate_rows_with_suggestions(
    store_hash: str,
    gate_rows: list[dict[str, Any]],
    cluster: str | None = None,
    *,
    source_profiles: dict[str, dict[str, Any]] | None = None,
    target_entities: list[dict[str, Any]] | None = None,
    overrides: dict[tuple[str, str], dict[str, Any]] | None = None,
    review_feedback_maps: dict[str, dict[Any, dict[str, int]]] | None = None,
    signal_library: dict[str, Any] | None = None,
    cache_snapshots: bool = False,
) -> list[dict[str, Any]]:
    return suggestions_annotate_query_gate_rows_with_suggestions(
        store_hash,
        gate_rows,
        cluster=cluster,
        source_profiles=source_profiles,
        target_entities=target_entities,
        overrides=overrides,
        review_feedback_maps=review_feedback_maps,
        signal_library=signal_library,
        cache_snapshots=cache_snapshots,
        build_unified_entity_index_fn=_build_unified_entity_index,
        load_query_target_overrides_fn=_load_query_target_overrides,
        load_review_feedback_maps_fn=_load_review_feedback_maps,
        build_store_signal_library_fn=_build_store_signal_library,
        refresh_query_gate_row_live_state_fn=_refresh_query_gate_row_live_state,
        rank_target_options_for_gate_row_fn=_rank_target_options_for_gate_row,
        query_target_override_key_fn=_query_target_override_key,
        serialize_query_gate_target_snapshot_fn=_serialize_query_gate_target_snapshot,
    )


def _annotate_query_gate_rows_with_agent_reviews(
    gate_rows: list[dict[str, Any]],
    review_map: dict[int, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    review_map = review_map or {}
    annotated_rows: list[dict[str, Any]] = []
    for row in gate_rows:
        annotated = dict(row)
        gate_record_id = int(row.get("gate_record_id") or 0)
        review = review_map.get(gate_record_id)
        if review:
            processed_reviews = _postprocess_gate_agent_reviews([review], {gate_record_id: annotated})
            review = processed_reviews[0] if processed_reviews else review
            winner = annotated.get("suggested_target") or {}
            winner_target_key = (
                (winner.get("entity_type") or "").strip().lower(),
                int(winner.get("entity_id") or 0),
            )
            review_target_key = (
                (review.get("target_entity_type") or "").strip().lower(),
                int(review.get("target_entity_id") or 0),
            )
            if all(winner_target_key) and all(review_target_key) and winner_target_key != review_target_key:
                review = None
        annotated["agent_review"] = review
        annotated_rows.append(annotated)
    return annotated_rows


def _serialize_query_gate_target_snapshot(target: dict[str, Any] | None) -> dict[str, Any] | None:
    return suggestions_serialize_query_gate_target_snapshot(target)


def _attach_cached_query_gate_suggestions(gate_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return suggestions_attach_cached_query_gate_suggestions(gate_rows)


def refresh_query_gate_suggestion_cache(
    store_hash: str,
    run_id: int | None = None,
    gate_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return suggestions_refresh_query_gate_suggestion_cache(
        store_hash,
        run_id=run_id,
        gate_rows=gate_rows,
        latest_gate_run_id_fn=_latest_gate_run_id,
        list_runs_fn=list_runs,
        list_query_gate_records_fn=list_query_gate_records,
        build_unified_entity_index_fn=_build_unified_entity_index,
        build_store_signal_library_fn=_build_store_signal_library,
        load_query_target_overrides_fn=_load_query_target_overrides,
        load_review_feedback_maps_fn=_load_review_feedback_maps,
        annotate_query_gate_rows_with_suggestions_fn=_annotate_query_gate_rows_with_suggestions,
        store_query_gate_records_fn=_store_query_gate_records,
    )


def _latest_candidate_rows_for_store(
    store_hash: str,
    review_status: str | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    return candidates_latest_candidate_rows_for_store(store_hash, review_status=review_status, limit=limit)


def _include_dashboard_candidate(row: dict[str, Any], review_status: str, category_enabled: bool) -> bool:
    return candidates_include_dashboard_candidate(row, review_status, category_enabled)


def count_pending_candidates(store_hash: str) -> int:
    return candidates_count_pending_candidates(
        store_hash,
        category_publishing_enabled_for_store_fn=category_publishing_enabled_for_store,
    )


def count_candidates_by_statuses(store_hash: str, review_statuses: list[str]) -> int:
    return candidates_count_candidates_by_statuses(
        store_hash,
        review_statuses,
        category_publishing_enabled_for_store_fn=category_publishing_enabled_for_store,
    )


def review_candidates(candidate_ids: list[int], review_status: str, reviewed_by: str | None, note: str | None = None) -> int:
    return candidates_review_candidates(candidate_ids, review_status, reviewed_by, note=note)


def _rank_source_rows(rows: list[dict[str, Any]], source_entity_type: str = "product") -> list[dict[str, Any]]:
    max_links = max(int(Config.FULCRUM_AUTO_PUBLISH_MAX_LINKS_PER_SOURCE or 4), 1)

    def sort_key(row: dict[str, Any]) -> tuple[int, int, float, int]:
        metadata = row.get("metadata") or {}
        target_entity_type = row.get("target_entity_type") or "product"
        query_scope = metadata.get("query_intent_scope")
        preferred_entity_type = metadata.get("preferred_entity_type")
        block_type = metadata.get("block_type")
        query_tokens = _tokenize_intent_text(row.get("example_query"))
        target_tokens = _tokenize_intent_text(f"{row.get('target_name') or ''} {row.get('target_url') or ''}")
        narrow_accessory_target = _is_replacement_or_accessory_target(query_tokens, target_tokens, row.get("target_name"))

        if source_entity_type == "product" and target_entity_type == "category":
            priority_bucket = 0 if block_type == "pdp_category_competition" or preferred_entity_type == "category" else 1
        elif query_scope == "specific_product":
            priority_bucket = 1
        elif query_scope == "broad_product_family":
            priority_bucket = 2
        else:
            priority_bucket = 3

        if query_scope == "broad_product_family" and narrow_accessory_target and not (query_tokens & REPLACEMENT_INTENT_TOKENS):
            priority_bucket += 2

        # Prefer rows that explicitly preserve the commercial head term in the anchor.
        anchor_label = (row.get("anchor_label") or "").lower()
        head_term_bonus = 0
        if "rollaway" in anchor_label or "bed" in anchor_label:
            head_term_bonus = 1

        return (
            priority_bucket,
            -head_term_bonus,
            -float(row.get("score") or 0),
            int(row.get("target_product_id") or 0),
        )

    rows.sort(key=sort_key)
    selected: list[dict[str, Any]] = []
    selected_candidate_keys: set[tuple[int, str, int, str]] = set()
    category_slots_used = 0

    if source_entity_type == "category":
        best_category_row = next(
            (
                row
                for row in rows
                if (row.get("target_entity_type") or "product") == "category"
            ),
            None,
        )
        if best_category_row:
            selected.append(best_category_row)
            selected_candidate_keys.add(
                (
                    int(best_category_row.get("candidate_id") or 0),
                    (best_category_row.get("target_entity_type") or "product"),
                    int(best_category_row.get("target_product_id") or 0),
                    best_category_row.get("target_url") or "",
                )
            )

    for row in rows:
        target_entity_type = row.get("target_entity_type") or "product"
        row_key = (
            int(row.get("candidate_id") or 0),
            target_entity_type,
            int(row.get("target_product_id") or 0),
            row.get("target_url") or "",
        )
        if row_key in selected_candidate_keys:
            continue
        if source_entity_type == "product" and target_entity_type == "category":
            if category_slots_used >= 1:
                continue
            category_slots_used += 1
        selected.append(row)
        selected_candidate_keys.add(row_key)
        if len(selected) >= max_links:
            break
    return selected


def get_approved_rows_for_source(store_hash: str, source_product_id: int, source_entity_type: str = "product") -> list[dict[str, Any]]:
    return candidates_get_approved_rows_for_source(
        store_hash,
        source_product_id,
        source_entity_type=source_entity_type,
        latest_candidate_rows_for_store_fn=_latest_candidate_rows_for_store,
        rank_source_rows_fn=_rank_source_rows,
    )


def list_candidates(store_hash: str, review_status: str = "pending", limit: int = 200) -> list[dict[str, Any]]:
    return candidates_list_candidates(
        store_hash,
        review_status=review_status,
        limit=limit,
        latest_candidate_rows_for_store_fn=_latest_candidate_rows_for_store,
        category_publishing_enabled_for_store_fn=category_publishing_enabled_for_store,
        include_dashboard_candidate_fn=_include_dashboard_candidate,
        rank_source_rows_fn=_rank_source_rows,
    )


def list_approved_sources(store_hash: str, limit: int = 100) -> list[dict[str, Any]]:
    return candidates_list_approved_sources(
        store_hash,
        limit=limit,
        list_candidates_fn=list_candidates,
    )


def _extract_approved_sources(rows: list[dict[str, Any]], limit: int = 100) -> list[dict[str, Any]]:
    seen: set[tuple[str, int]] = set()
    sources: list[dict[str, Any]] = []
    for row in rows:
        key = (
            row.get("source_entity_type") or "product",
            int(row.get("source_product_id") or 0),
        )
        if key in seen:
            continue
        seen.add(key)
        sources.append(
            {
                "source_entity_type": row.get("source_entity_type") or "product",
                "source_product_id": row.get("source_product_id"),
                "source_name": row.get("source_name"),
                "source_url": row.get("source_url"),
            }
        )
        if len(sources) >= limit:
            break
    return sources


def build_links_html(rows: list[dict[str, Any]], section_title: str = "Related options") -> str | None:
    return render_build_links_html(
        rows,
        build_anchor_label=build_anchor_label,
        section_title=section_title,
    )


def preview_product_html(store_hash: str, source_product_id: int, source_entity_type: str = "product") -> dict[str, Any]:
    rows = get_approved_rows_for_source(store_hash, source_product_id, source_entity_type=source_entity_type)
    return build_preview_payload(
        source_product_id=source_product_id,
        source_entity_type=source_entity_type,
        rows=rows,
        build_links_html=build_links_html,
    )


def _upsert_entity_metafield(store_hash: str, entity_type: str, entity_id: int, key: str, html: str) -> dict[str, Any]:
    return publishing_upsert_entity_metafield(
        store_hash=store_hash,
        entity_type=entity_type,
        entity_id=entity_id,
        key=key,
        html=html,
        get_bc_headers=get_bc_headers,
        normalize_store_hash=normalize_store_hash,
    )


def resolve_publish_source_entity_ids(
    store_hash: str,
    *,
    source_entity_type: str | None = None,
    source_entity_id: int | None = None,
    source_url: str | None = None,
) -> list[int]:
    normalized_store_hash = normalize_store_hash(store_hash)
    normalized_source_type = (source_entity_type or "").strip().lower() or None
    normalized_source_url = _normalize_storefront_path(source_url)
    resolved_source_id = int(source_entity_id or 0)
    if not normalized_source_type and resolved_source_id == 0 and not normalized_source_url:
        return []

    return sorted(
        {
            int(row.get("source_product_id") or row.get("source_entity_id") or 0)
            for row in _latest_candidate_rows_for_store(normalized_store_hash, review_status="approved", limit=None)
            if int(row.get("source_product_id") or row.get("source_entity_id") or 0) != 0
            and _candidate_matches_review_source(
                row,
                source_entity_type=normalized_source_type,
                source_entity_id=resolved_source_id or None,
                source_url=normalized_source_url or None,
            )
        }
    )


def publish_approved_entities(store_hash: str, source_entity_ids: list[int] | None = None, run_id: int | None = None) -> list[dict[str, Any]]:
    return publishing_publish_approved_entities(
        store_hash=store_hash,
        source_entity_ids=source_entity_ids,
        run_id=run_id,
        get_pg_conn=get_pg_conn,
        get_approved_rows_for_source=get_approved_rows_for_source,
        resolve_store_category_id_by_url=resolve_store_category_id_by_url,
        resolve_store_product_id_by_url=resolve_store_product_id_by_url,
        build_links_html=build_links_html,
        upsert_entity_metafield=_upsert_entity_metafield,
        invalidate_admin_metric_cache=invalidate_admin_metric_cache,
    )


def publish_approved_products(store_hash: str, source_product_ids: list[int] | None = None, run_id: int | None = None) -> list[dict[str, Any]]:
    return publish_approved_entities(store_hash, source_entity_ids=source_product_ids, run_id=run_id)


def unpublish_entities(store_hash: str, source_entity_ids: list[int]) -> list[dict[str, Any]]:
    return publishing_unpublish_entities(
        store_hash=store_hash,
        source_entity_ids=source_entity_ids,
        get_pg_conn=get_pg_conn,
        get_bc_headers=get_bc_headers,
        normalize_store_hash=normalize_store_hash,
        resolve_store_category_id_by_url=resolve_store_category_id_by_url,
        resolve_store_product_id_by_url=resolve_store_product_id_by_url,
        invalidate_admin_metric_cache=invalidate_admin_metric_cache,
    )


def unpublish_products(store_hash: str, source_product_ids: list[int]) -> list[dict[str, Any]]:
    return unpublish_entities(store_hash, source_entity_ids=source_product_ids)


def list_publications(store_hash: str, active_only: bool = True, limit: int = 100) -> list[dict[str, Any]]:
    return publishing_list_publications(
        store_hash=store_hash,
        active_only=active_only,
        limit=limit,
        get_pg_conn=get_pg_conn,
    )


def count_publications(store_hash: str, active_only: bool = True) -> int:
    return publishing_count_publications(
        store_hash=store_hash,
        active_only=active_only,
        get_pg_conn=get_pg_conn,
    )


def summarize_live_publications(store_hash: str) -> dict[str, int]:
    return publishing_summarize_live_publications(
        store_hash=store_hash,
        get_pg_conn=get_pg_conn,
        normalize_store_hash=normalize_store_hash,
    )


def _format_timestamp_display(value: Any) -> str:
    return snapshot_format_timestamp_display(value)


def _format_relative_time(value: Any) -> str:
    return snapshot_format_relative_time(value)


def _alert_severity_rank(severity: str) -> int:
    return snapshot_alert_severity_rank(severity)


def _alert_tone_for_severity(severity: str) -> str:
    return snapshot_alert_tone_for_severity(severity)


def build_operational_snapshot(
    store_hash: str,
    *,
    runs: list[dict[str, Any]] | None = None,
    active_run: dict[str, Any] | None = None,
    readiness: dict[str, Any] | None = None,
    publication_summary: dict[str, int] | None = None,
    edge_case_requests: list[dict[str, Any]] | None = None,
    gate_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return snapshot_build_operational_snapshot(
        store_hash,
        runs=runs,
        active_run=active_run,
        readiness=readiness,
        publication_summary=publication_summary,
        edge_case_requests=edge_case_requests,
        gate_summary=gate_summary,
        normalize_store_hash_fn=normalize_store_hash,
        list_runs_fn=list_runs,
        refresh_store_readiness_fn=refresh_store_readiness,
        summarize_live_publications_fn=summarize_live_publications,
        summarize_query_gate_dispositions_fn=summarize_query_gate_dispositions,
        category_publishing_enabled_for_store_fn=category_publishing_enabled_for_store,
        category_theme_hook_present_fn=category_theme_hook_present,
        generation_active_statuses=GENERATION_ACTIVE_STATUSES,
        active_run_watch_after=ACTIVE_RUN_WATCH_AFTER,
        active_run_urgent_after=ACTIVE_RUN_URGENT_AFTER,
        completed_run_watch_after=COMPLETED_RUN_WATCH_AFTER,
        completed_run_urgent_after=COMPLETED_RUN_URGENT_AFTER,
        edge_case_watch_count=EDGE_CASE_WATCH_COUNT,
        edge_case_urgent_count=EDGE_CASE_URGENT_COUNT,
    )


def _publication_posting_label(row: dict[str, Any]) -> str:
    return presenters_publication_posting_label(row)


def summarize_edge_case_requests(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return presenters_summarize_edge_case_requests(
        rows,
        format_timestamp_display_fn=_format_timestamp_display,
        format_relative_time_fn=_format_relative_time,
    )


def _apply_review_target_display(row: dict[str, Any]) -> None:
    presenters_apply_review_target_display(
        row,
        normalize_storefront_path_fn=_normalize_storefront_path,
    )


def get_entity_coverage_summary(store_hash: str) -> dict[str, int]:
    return reporting_get_entity_coverage_summary(
        store_hash,
        load_store_brand_profiles_fn=load_store_brand_profiles,
        load_store_content_profiles_fn=load_store_content_profiles,
    )


def _format_logic_change_timestamp(value: str | None) -> str:
    return reporting_format_logic_change_timestamp(value)


def _format_logic_validation_status(status: str | None) -> str:
    return reporting_format_logic_validation_status(status)


def _coerce_channel_id(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _clear_storefront_site_caches() -> None:
    storefront_clear_storefront_site_caches()


def _load_storefront_site_rows(store_hash: str) -> list[dict[str, Any]]:
    return storefront_load_storefront_site_rows(store_hash)


def _select_storefront_site_row(site_rows: list[dict[str, Any]], channel_id: int | None = None) -> dict[str, Any] | None:
    return storefront_select_storefront_site_row(site_rows, channel_id=channel_id)


def _extract_storefront_channel_id(*objects: Any) -> int | None:
    return storefront_extract_storefront_channel_id(*objects)


def _storefront_base_url(store_hash: str, channel_id: int | None = None) -> str:
    return storefront_get_storefront_base_url(store_hash, channel_id=channel_id)


def _storefront_base_url_from_db(store_hash: str, channel_id: int | None = None) -> str:
    return storefront_get_storefront_base_url_from_db(store_hash, channel_id=channel_id)


def build_storefront_url(store_hash: str, url_path: str | None, channel_id: int | None = None) -> str:
    raw = (url_path or "").strip()
    if not raw:
        return ""
    if raw.startswith("http://") or raw.startswith("https://"):
        return raw
    normalized_path = _normalize_storefront_path(raw)
    if not normalized_path:
        return ""
    return f"{_storefront_base_url(store_hash, channel_id=channel_id).rstrip('/')}{normalized_path}"


def _storefront_base_urls(store_hash: str) -> list[str]:
    return storefront_list_storefront_base_urls(store_hash)


def _candidate_gsc_page_values(store_hash: str, paths: list[str]) -> list[str]:
    return metrics_candidate_gsc_page_values(
        store_hash,
        paths,
        normalize_storefront_path_fn=_normalize_storefront_path,
        storefront_base_urls_fn=_storefront_base_urls,
    )


def load_logic_change_log(limit: int | None = None) -> list[dict[str, Any]]:
    return reporting_load_logic_change_log(changelog_path=LOGIC_CHANGELOG_PATH, limit=limit)


def get_logic_change_summary(limit: int = 5) -> dict[str, Any]:
    return reporting_get_logic_change_summary(changelog_path=LOGIC_CHANGELOG_PATH, limit=limit)


def summarize_suggested_target_types(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts = {"product": 0, "category": 0, "brand": 0, "content": 0}
    for row in rows:
        target_type = ((row.get("suggested_target") or {}).get("entity_type") or "").strip().lower()
        if target_type in counts:
            counts[target_type] += 1
    return counts


def _row_current_page_matches_winner(row: dict[str, Any]) -> tuple[bool, bool]:
    winner = dict(row.get("suggested_target") or {})
    if not winner:
        return False, False
    source_type = ((row.get("source_entity_type") or row.get("current_page_type") or "")).strip().lower()
    winner_type = ((winner.get("entity_type") or "")).strip().lower()
    source_url = _normalize_storefront_path(row.get("source_url"))
    winner_url = _normalize_storefront_path(winner.get("url"))
    source_entity_id = int(row.get("source_entity_id") or 0)
    winner_entity_id = int(winner.get("entity_id") or 0)
    same_type = bool(source_type and winner_type and source_type == winner_type)
    same_entity = bool(source_entity_id and winner_entity_id and source_entity_id == winner_entity_id and same_type)
    same_url = bool(source_url and winner_url and source_url == winner_url)
    is_exact_match = same_entity or same_url
    return is_exact_match, same_type


def summarize_changed_route_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return dashboard_summarize_changed_route_rows(
        rows,
        row_current_page_matches_winner_fn=_row_current_page_matches_winner,
    )


def _matches_changed_route_search(row: dict[str, Any], search_text: str) -> bool:
    return dashboard_matches_changed_route_search(row, search_text)


def _sorted_changed_route_rows(rows: list[dict[str, Any]], sort_key: str) -> list[dict[str, Any]]:
    return dashboard_sorted_changed_route_rows(rows, sort_key)


def _gate_review_map_for_ids(
    store_hash: str,
    gate_record_ids: set[int],
    *,
    run_id: int | None = None,
    run_ids: set[int] | None = None,
) -> dict[int, dict[str, Any]]:
    return dashboard_gate_review_map_for_ids(
        store_hash,
        gate_record_ids,
        run_id=run_id,
        run_ids=run_ids,
        query_gate_record_map_for_ids_fn=_query_gate_record_map_for_ids,
        list_query_gate_agent_reviews_fn=list_query_gate_agent_reviews,
        postprocess_gate_agent_reviews_fn=_postprocess_gate_agent_reviews,
    )


def _attach_changed_route_agent_reviews(
    rows: list[dict[str, Any]],
    review_map: dict[int, dict[str, Any]],
) -> list[dict[str, Any]]:
    return review_attach_changed_route_agent_reviews(rows, review_map)


def summarize_changed_route_agent_reviews(
    rows: list[dict[str, Any]],
    review_map: dict[int, dict[str, Any]],
) -> dict[str, Any]:
    return review_summarize_changed_route_agent_reviews(rows, review_map)


def _changed_route_review_next_step_label(action: str | None) -> str:
    return review_changed_route_review_next_step_label(action)


def _fallback_changed_route_review_reasoning(
    rows: list[dict[str, Any]],
    review_map: dict[int, dict[str, Any]],
) -> dict[str, Any]:
    return review_fallback_changed_route_review_reasoning(rows, review_map)


def _reason_about_changed_route_reviews_with_agent(
    rows: list[dict[str, Any]],
    review_map: dict[int, dict[str, Any]],
    *,
    initiated_by: str | None = None,
) -> dict[str, Any]:
    return review_reason_about_changed_route_reviews_with_agent(
        rows,
        review_map,
        initiated_by=initiated_by,
    )


def get_cached_changed_route_review_reasoning(
    store_hash: str,
    *,
    run_id: int | None = None,
    rows: list[dict[str, Any]] | None = None,
    review_map: dict[int, dict[str, Any]] | None = None,
    force_refresh: bool = False,
    initiated_by: str | None = None,
) -> dict[str, Any]:
    return review_get_cached_changed_route_review_reasoning(
        store_hash,
        run_id=run_id,
        rows=rows,
        review_map=review_map,
        force_refresh=force_refresh,
        initiated_by=initiated_by,
        latest_gate_run_id_fn=_latest_gate_run_id,
        load_admin_metric_cache_fn=_load_admin_metric_cache,
        store_admin_metric_cache_fn=_store_admin_metric_cache,
        gate_review_map_for_ids_fn=_gate_review_map_for_ids,
        reason_about_changed_route_reviews_with_agent_fn=_reason_about_changed_route_reviews_with_agent,
    )


def run_changed_route_agent_review(
    store_hash: str,
    *,
    run_id: int | None = None,
    limit: int = 25,
    initiated_by: str | None = None,
) -> dict[str, Any]:
    return review_run_changed_route_agent_review(
        store_hash,
        run_id=run_id,
        limit=limit,
        initiated_by=initiated_by,
        latest_gate_run_id_fn=_latest_gate_run_id,
        list_changed_route_results_fn=list_changed_route_results,
        run_query_gate_agent_review_fn=run_query_gate_agent_review,
        gate_review_map_for_ids_fn=_gate_review_map_for_ids,
        get_cached_changed_route_review_reasoning_fn=get_cached_changed_route_review_reasoning,
        summarize_changed_route_agent_reviews_fn=summarize_changed_route_agent_reviews,
    )


def _percent_share(numerator: int | float, denominator: int | float) -> float:
    return metrics_percent_share(numerator, denominator)


def summarize_gsc_routing_coverage(store_hash: str, run_id: int | None = None) -> dict[str, Any]:
    return metrics_summarize_gsc_routing_coverage(
        store_hash,
        run_id=run_id,
        latest_gate_run_id_fn=_latest_gate_run_id,
    )


def list_changed_route_results(store_hash: str, run_id: int | None = None, limit: int = 25) -> list[dict[str, Any]]:
    return dashboard_list_changed_route_results(
        store_hash,
        run_id=run_id,
        limit=limit,
        latest_gate_run_id_fn=_latest_gate_run_id,
        list_query_gate_records_fn=list_query_gate_records,
        attach_cached_query_gate_suggestions_fn=_attach_cached_query_gate_suggestions,
        row_current_page_matches_winner_fn=_row_current_page_matches_winner,
        extract_storefront_channel_id_fn=_extract_storefront_channel_id,
        build_storefront_url_fn=build_storefront_url,
    )


def get_cached_changed_route_results(
    store_hash: str,
    *,
    run_id: int | None = None,
    limit: int = 25,
    force_refresh: bool = False,
) -> list[dict[str, Any]]:
    return dashboard_get_cached_changed_route_results(
        store_hash,
        run_id=run_id,
        limit=limit,
        force_refresh=force_refresh,
        latest_gate_run_id_fn=_latest_gate_run_id,
        load_admin_metric_cache_fn=_load_admin_metric_cache,
        store_admin_metric_cache_fn=_store_admin_metric_cache,
        list_changed_route_results_fn=list_changed_route_results,
    )


def summarize_blocked_gate_families(store_hash: str, run_id: int | None = None, limit: int = 2000) -> dict[str, Any]:
    return metrics_summarize_blocked_gate_families(
        store_hash,
        run_id=run_id,
        limit=limit,
        latest_gate_run_id_fn=_latest_gate_run_id,
        list_query_gate_records_fn=list_query_gate_records,
    )


def summarize_gsc_alignment(store_hash: str, run_id: int | None = None) -> dict[str, Any]:
    return metrics_summarize_gsc_alignment(
        store_hash,
        run_id=run_id,
        latest_gate_run_id_fn=_latest_gate_run_id,
        list_query_gate_records_fn=list_query_gate_records,
        attach_cached_query_gate_suggestions_fn=_attach_cached_query_gate_suggestions,
        row_current_page_matches_winner_fn=_row_current_page_matches_winner,
    )


def _format_change_value(value: float | int | None, *, suffix: str = "", invert_good: bool = False) -> dict[str, Any]:
    return metrics_format_change_value(value, suffix=suffix, invert_good=invert_good)


def summarize_live_gsc_performance(store_hash: str) -> dict[str, Any]:
    return metrics_summarize_live_gsc_performance(
        store_hash,
        list_publications_fn=list_publications,
        normalize_storefront_path_fn=_normalize_storefront_path,
        candidate_gsc_page_values_fn=_candidate_gsc_page_values,
        format_timestamp_display_fn=_format_timestamp_display,
    )


def invalidate_admin_metric_cache(store_hash: str, metric_keys: list[str] | None = None) -> None:
    return cache_invalidate_admin_metric_cache(
        store_hash,
        metric_keys=metric_keys,
        apply_runtime_schema_fn=apply_runtime_schema,
    )


def _load_admin_metric_cache(
    store_hash: str,
    metric_key: str,
    *,
    max_age: timedelta = ADMIN_METRIC_CACHE_TTL,
) -> dict[str, Any] | None:
    return cache_load_admin_metric_cache(
        store_hash,
        metric_key,
        max_age=max_age,
        apply_runtime_schema_fn=apply_runtime_schema,
        format_timestamp_display_fn=_format_timestamp_display,
        format_relative_time_fn=_format_relative_time,
    )


def _json_cache_safe(value: Any) -> Any:
    return cache_json_cache_safe(value)


def _store_admin_metric_cache(store_hash: str, metric_key: str, payload: dict[str, Any]) -> dict[str, Any]:
    return cache_store_admin_metric_cache(
        store_hash,
        metric_key,
        payload,
        apply_runtime_schema_fn=apply_runtime_schema,
        format_timestamp_display_fn=_format_timestamp_display,
        format_relative_time_fn=_format_relative_time,
    )


def get_cached_live_gsc_performance(store_hash: str, *, force_refresh: bool = False) -> dict[str, Any]:
    return metrics_get_cached_live_gsc_performance(
        store_hash,
        force_refresh=force_refresh,
        load_admin_metric_cache_fn=_load_admin_metric_cache,
        store_admin_metric_cache_fn=_store_admin_metric_cache,
        summarize_live_gsc_performance_fn=summarize_live_gsc_performance,
    )


def theme_hook_present() -> bool:
    return render_theme_hook_present(Config.FULCRUM_THEME_PRODUCT_TEMPLATE)


def category_theme_hook_present() -> bool:
    return render_category_theme_hook_present(Config.FULCRUM_THEME_CATEGORY_TEMPLATE)


def get_dashboard_context(
    store_hash: str,
    *,
    include_admin: bool = False,
    include_quality: bool = False,
    changed_route_search: str | None = None,
    changed_route_sort: str = "score_desc",
    changed_route_page: int = 1,
    changed_route_page_size: int = 25,
) -> dict[str, Any]:
    return dashboard_build_dashboard_context(
        store_hash,
        include_admin=include_admin,
        include_quality=include_quality,
        changed_route_search=changed_route_search,
        changed_route_sort=changed_route_sort,
        changed_route_page=changed_route_page,
        changed_route_page_size=changed_route_page_size,
        generation_active_statuses=GENERATION_ACTIVE_STATUSES,
        list_runs_fn=list_runs,
        get_store_profile_summary_fn=get_store_profile_summary,
        refresh_store_readiness_fn=refresh_store_readiness,
        list_query_gate_review_requests_fn=list_query_gate_review_requests,
        summarize_query_gate_dispositions_fn=summarize_query_gate_dispositions,
        list_publications_fn=list_publications,
        summarize_live_publications_fn=summarize_live_publications,
        build_public_dashboard_data_fn=lambda **kwargs: dashboard_build_public_dashboard_data(
            **kwargs,
            category_publishing_enabled=category_publishing_enabled_for_store(kwargs["store_hash"]),
            list_query_gate_records_fn=list_query_gate_records,
            annotate_query_gate_rows_with_suggestions_fn=_annotate_query_gate_rows_with_suggestions,
            attach_cached_query_gate_suggestions_fn=_attach_cached_query_gate_suggestions,
            extract_storefront_channel_id_fn=_extract_storefront_channel_id,
            build_storefront_url_fn=build_storefront_url,
            summarize_suggested_target_types_fn=summarize_suggested_target_types,
            publication_posting_label_fn=_publication_posting_label,
            format_timestamp_display_fn=_format_timestamp_display,
            format_relative_time_fn=_format_relative_time,
            summarize_edge_case_requests_fn=summarize_edge_case_requests,
        ),
        count_pending_candidates_fn=count_pending_candidates,
        admin_context_defaults_fn=dashboard_admin_context_defaults,
        populate_edge_case_admin_context_fn=lambda admin_context, **kwargs: dashboard_populate_edge_case_admin_context(
            admin_context,
            **kwargs,
            list_query_gate_review_requests_fn=list_query_gate_review_requests,
            gate_review_map_for_ids_fn=_gate_review_map_for_ids,
            query_gate_record_map_for_ids_fn=_query_gate_record_map_for_ids,
            extract_storefront_channel_id_fn=_extract_storefront_channel_id,
            build_storefront_url_fn=build_storefront_url,
            merge_fresh_gate_context_into_review_row_fn=_merge_fresh_gate_context_into_review_row,
            build_query_gate_human_review_mailto_fn=build_query_gate_human_review_mailto,
            apply_review_target_display_fn=_apply_review_target_display,
            format_timestamp_display_fn=_format_timestamp_display,
            format_relative_time_fn=_format_relative_time,
            summarize_edge_case_requests_fn=summarize_edge_case_requests,
        ),
        summarize_gsc_routing_coverage_fn=summarize_gsc_routing_coverage,
        populate_changed_route_admin_context_fn=lambda admin_context, **kwargs: dashboard_populate_changed_route_admin_context(
            admin_context,
            **kwargs,
            get_cached_changed_route_results_fn=get_cached_changed_route_results,
            summarize_changed_route_rows_fn=summarize_changed_route_rows,
            gate_review_map_for_ids_fn=_gate_review_map_for_ids,
            attach_changed_route_agent_reviews_fn=_attach_changed_route_agent_reviews,
            summarize_changed_route_agent_reviews_fn=summarize_changed_route_agent_reviews,
            get_cached_changed_route_review_reasoning_fn=get_cached_changed_route_review_reasoning,
            build_query_gate_human_review_mailto_fn=build_query_gate_human_review_mailto,
            matches_changed_route_search_fn=_matches_changed_route_search,
            sorted_changed_route_rows_fn=_sorted_changed_route_rows,
        ),
        summarize_blocked_gate_families_fn=summarize_blocked_gate_families,
        get_cached_live_gsc_performance_fn=get_cached_live_gsc_performance,
        build_operational_snapshot_fn=build_operational_snapshot,
        get_logic_change_summary_fn=get_logic_change_summary,
        summarize_query_gate_agent_reviews_fn=summarize_query_gate_agent_reviews,
        list_query_gate_agent_review_clusters_fn=list_query_gate_agent_review_clusters,
        count_query_gate_review_requests_fn=count_query_gate_review_requests,
        count_publications_fn=count_publications,
        theme_hook_present_fn=theme_hook_present,
        category_theme_hook_present_fn=category_theme_hook_present,
        format_timestamp_display_fn=_format_timestamp_display,
        format_relative_time_fn=_format_relative_time,
    )
