CREATE SCHEMA IF NOT EXISTS app_runtime;

CREATE TABLE IF NOT EXISTS app_runtime.store_installations (
    installation_id BIGSERIAL PRIMARY KEY,
    store_hash TEXT NOT NULL UNIQUE,
    context TEXT NOT NULL,
    account_uuid TEXT,
    access_token TEXT,
    scope TEXT,
    user_id TEXT,
    owner_email TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    install_source TEXT NOT NULL DEFAULT 'oauth',
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    installed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    uninstalled_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS app_runtime.store_readiness (
    store_hash TEXT PRIMARY KEY,
    catalog_synced BOOLEAN NOT NULL DEFAULT FALSE,
    attribute_mappings_ready BOOLEAN NOT NULL DEFAULT FALSE,
    theme_hook_ready BOOLEAN NOT NULL DEFAULT FALSE,
    auto_publish_ready BOOLEAN NOT NULL DEFAULT FALSE,
    category_beta_ready BOOLEAN NOT NULL DEFAULT FALSE,
    unresolved_option_name_count INTEGER NOT NULL DEFAULT 0,
    unresolved_option_value_count INTEGER NOT NULL DEFAULT 0,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS app_runtime.store_integrations (
    store_hash TEXT NOT NULL,
    integration_key TEXT NOT NULL,
    connection_status TEXT NOT NULL DEFAULT 'not_connected',
    configuration_status TEXT NOT NULL DEFAULT 'not_configured',
    selected_resource_id TEXT,
    selected_resource_label TEXT,
    auth_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    last_success_at TIMESTAMPTZ,
    last_error_at TIMESTAMPTZ,
    last_error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (store_hash, integration_key)
);

CREATE TABLE IF NOT EXISTS app_runtime.integration_sync_runs (
    sync_run_id BIGSERIAL PRIMARY KEY,
    store_hash TEXT NOT NULL,
    integration_key TEXT NOT NULL,
    selected_resource_id TEXT,
    status TEXT NOT NULL DEFAULT 'queued',
    triggered_by TEXT NOT NULL DEFAULT 'manual',
    row_count INTEGER NOT NULL DEFAULT 0,
    start_date TEXT,
    end_date TEXT,
    error_message TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    queued_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_integration_sync_runs_queue
    ON app_runtime.integration_sync_runs (status, queued_at, sync_run_id);

CREATE INDEX IF NOT EXISTS idx_integration_sync_runs_store_key
    ON app_runtime.integration_sync_runs (store_hash, integration_key, queued_at DESC);

CREATE TABLE IF NOT EXISTS app_runtime.store_publish_settings (
    store_hash TEXT PRIMARY KEY,
    publishing_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    category_publishing_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS app_runtime.store_theme_verifications (
    verification_id BIGSERIAL PRIMARY KEY,
    store_hash TEXT NOT NULL,
    verification_status TEXT NOT NULL DEFAULT 'not_checked',
    failure_classification TEXT,
    summary TEXT,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_store_theme_verifications_store_created
    ON app_runtime.store_theme_verifications (store_hash, created_at DESC);

CREATE TABLE IF NOT EXISTS app_runtime.store_gsc_daily (
    store_hash TEXT NOT NULL,
    property_site_url TEXT NOT NULL,
    page TEXT NOT NULL,
    query TEXT NOT NULL,
    date DATE NOT NULL,
    clicks BIGINT NOT NULL DEFAULT 0,
    impressions BIGINT NOT NULL DEFAULT 0,
    ctr DOUBLE PRECISION NOT NULL DEFAULT 0,
    position DOUBLE PRECISION NOT NULL DEFAULT 0,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (store_hash, property_site_url, page, query, date)
);

CREATE INDEX IF NOT EXISTS idx_store_gsc_daily_store_page_date
    ON app_runtime.store_gsc_daily (store_hash, page, date DESC);

CREATE TABLE IF NOT EXISTS app_runtime.store_ga4_pages_daily (
    store_hash TEXT NOT NULL,
    property_id TEXT NOT NULL,
    date DATE NOT NULL,
    page_path TEXT NOT NULL,
    channel_group TEXT NOT NULL DEFAULT '',
    sessions BIGINT NOT NULL DEFAULT 0,
    total_users BIGINT NOT NULL DEFAULT 0,
    engaged_sessions BIGINT NOT NULL DEFAULT 0,
    add_to_carts BIGINT NOT NULL DEFAULT 0,
    ecommerce_purchases BIGINT NOT NULL DEFAULT 0,
    purchase_revenue DOUBLE PRECISION NOT NULL DEFAULT 0,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (store_hash, property_id, date, page_path, channel_group)
);

CREATE INDEX IF NOT EXISTS idx_store_ga4_pages_daily_store_page_date
    ON app_runtime.store_ga4_pages_daily (store_hash, page_path, date DESC);

CREATE TABLE IF NOT EXISTS app_runtime.store_storefront_sites (
    store_hash TEXT NOT NULL,
    channel_id INTEGER NOT NULL,
    site_id INTEGER NOT NULL,
    channel_name TEXT,
    channel_platform TEXT,
    channel_type TEXT,
    channel_status TEXT,
    is_channel_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    site_url TEXT,
    primary_url TEXT,
    canonical_url TEXT,
    checkout_url TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (store_hash, channel_id, site_id)
);

CREATE INDEX IF NOT EXISTS idx_store_storefront_sites_store_channel
    ON app_runtime.store_storefront_sites (store_hash, channel_id, last_synced_at DESC);

CREATE TABLE IF NOT EXISTS app_runtime.store_attribute_buckets (
    store_hash TEXT NOT NULL,
    bucket_key TEXT NOT NULL,
    bucket_label TEXT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    config JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (store_hash, bucket_key)
);

CREATE TABLE IF NOT EXISTS app_runtime.store_option_name_mappings (
    mapping_id BIGSERIAL PRIMARY KEY,
    store_hash TEXT NOT NULL,
    raw_option_name TEXT NOT NULL,
    bucket_key TEXT NOT NULL,
    normalized_name TEXT,
    confidence NUMERIC(5,2) NOT NULL DEFAULT 0,
    source TEXT NOT NULL DEFAULT 'auto',
    review_status TEXT NOT NULL DEFAULT 'auto_approved',
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (store_hash, raw_option_name)
);

CREATE TABLE IF NOT EXISTS app_runtime.store_option_value_mappings (
    value_mapping_id BIGSERIAL PRIMARY KEY,
    store_hash TEXT NOT NULL,
    raw_option_name TEXT,
    raw_option_value TEXT NOT NULL,
    bucket_key TEXT NOT NULL,
    canonical_value TEXT NOT NULL,
    confidence NUMERIC(5,2) NOT NULL DEFAULT 0,
    source TEXT NOT NULL DEFAULT 'auto',
    review_status TEXT NOT NULL DEFAULT 'auto_approved',
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (store_hash, raw_option_name, raw_option_value)
);

CREATE TABLE IF NOT EXISTS app_runtime.store_product_profiles (
    profile_id BIGSERIAL PRIMARY KEY,
    store_hash TEXT NOT NULL,
    bc_product_id INTEGER NOT NULL,
    product_name TEXT,
    product_url TEXT NOT NULL,
    brand_name TEXT,
    search_keywords TEXT,
    source_data JSONB NOT NULL DEFAULT '{}'::jsonb,
    attribute_profile JSONB NOT NULL DEFAULT '{}'::jsonb,
    cluster_profile JSONB NOT NULL DEFAULT '{}'::jsonb,
    canonical_group_key TEXT,
    is_canonical_target BOOLEAN NOT NULL DEFAULT TRUE,
    is_visible BOOLEAN NOT NULL DEFAULT TRUE,
    availability TEXT,
    is_price_hidden BOOLEAN NOT NULL DEFAULT FALSE,
    eligible_for_routing BOOLEAN NOT NULL DEFAULT TRUE,
    sync_source TEXT NOT NULL DEFAULT 'api',
    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (store_hash, bc_product_id),
    UNIQUE (store_hash, product_url)
);

ALTER TABLE app_runtime.store_product_profiles
    ADD COLUMN IF NOT EXISTS canonical_group_key TEXT;

ALTER TABLE app_runtime.store_product_profiles
    ADD COLUMN IF NOT EXISTS is_canonical_target BOOLEAN NOT NULL DEFAULT TRUE;

ALTER TABLE app_runtime.store_product_profiles
    ADD COLUMN IF NOT EXISTS is_visible BOOLEAN NOT NULL DEFAULT TRUE;

ALTER TABLE app_runtime.store_product_profiles
    ADD COLUMN IF NOT EXISTS availability TEXT;

ALTER TABLE app_runtime.store_product_profiles
    ADD COLUMN IF NOT EXISTS is_price_hidden BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE app_runtime.store_product_profiles
    ADD COLUMN IF NOT EXISTS eligible_for_routing BOOLEAN NOT NULL DEFAULT TRUE;

CREATE INDEX IF NOT EXISTS idx_store_product_profiles_url
    ON app_runtime.store_product_profiles (store_hash, product_url);

CREATE TABLE IF NOT EXISTS app_runtime.store_category_profiles (
    category_profile_id BIGSERIAL PRIMARY KEY,
    store_hash TEXT NOT NULL,
    bc_category_id INTEGER NOT NULL,
    parent_category_id INTEGER,
    category_name TEXT,
    category_url TEXT NOT NULL,
    page_title TEXT,
    description TEXT,
    meta_keywords JSONB NOT NULL DEFAULT '[]'::jsonb,
    source_data JSONB NOT NULL DEFAULT '{}'::jsonb,
    attribute_profile JSONB NOT NULL DEFAULT '{}'::jsonb,
    cluster_profile JSONB NOT NULL DEFAULT '{}'::jsonb,
    canonical_group_key TEXT,
    is_canonical_target BOOLEAN NOT NULL DEFAULT TRUE,
    is_visible BOOLEAN NOT NULL DEFAULT TRUE,
    eligible_for_routing BOOLEAN NOT NULL DEFAULT TRUE,
    sync_source TEXT NOT NULL DEFAULT 'api',
    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (store_hash, bc_category_id),
    UNIQUE (store_hash, category_url)
);

ALTER TABLE app_runtime.store_category_profiles
    ADD COLUMN IF NOT EXISTS is_visible BOOLEAN NOT NULL DEFAULT TRUE;

ALTER TABLE app_runtime.store_category_profiles
    ADD COLUMN IF NOT EXISTS eligible_for_routing BOOLEAN NOT NULL DEFAULT TRUE;

CREATE INDEX IF NOT EXISTS idx_store_category_profiles_url
    ON app_runtime.store_category_profiles (store_hash, category_url);

CREATE TABLE IF NOT EXISTS app_runtime.store_intent_signal_enrichments (
    enrichment_id BIGSERIAL PRIMARY KEY,
    store_hash TEXT NOT NULL,
    signal_kind TEXT NOT NULL,
    raw_label TEXT NOT NULL,
    normalized_label TEXT NOT NULL,
    scope_kind TEXT NOT NULL,
    entity_type TEXT,
    entity_id INTEGER,
    confidence NUMERIC(5,2) NOT NULL DEFAULT 0,
    source TEXT NOT NULL DEFAULT 'deterministic',
    status TEXT NOT NULL DEFAULT 'active',
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_store_intent_signal_enrichments_store_kind
    ON app_runtime.store_intent_signal_enrichments (store_hash, signal_kind, status, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_store_intent_signal_enrichments_store_source
    ON app_runtime.store_intent_signal_enrichments (store_hash, source, updated_at DESC);

CREATE TABLE IF NOT EXISTS app_runtime.store_cluster_rules (
    rule_id BIGSERIAL PRIMARY KEY,
    store_hash TEXT NOT NULL,
    cluster_key TEXT NOT NULL,
    cluster_label TEXT NOT NULL,
    bucket_key TEXT,
    canonical_value TEXT,
    priority INTEGER NOT NULL DEFAULT 100,
    source TEXT NOT NULL DEFAULT 'auto',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS app_runtime.link_runs (
    run_id BIGSERIAL PRIMARY KEY,
    store_hash TEXT NOT NULL,
    initiated_by TEXT,
    run_source TEXT NOT NULL DEFAULT 'manual',
    status TEXT NOT NULL DEFAULT 'pending',
    filters JSONB NOT NULL DEFAULT '{}'::jsonb,
    notes TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS app_runtime.query_gate_records (
    gate_record_id BIGSERIAL PRIMARY KEY,
    run_id BIGINT NOT NULL REFERENCES app_runtime.link_runs(run_id) ON DELETE CASCADE,
    store_hash TEXT NOT NULL,
    normalized_query_key TEXT NOT NULL,
    representative_query TEXT NOT NULL,
    source_url TEXT NOT NULL,
    source_name TEXT,
    source_entity_type TEXT NOT NULL DEFAULT 'product',
    source_entity_id INTEGER,
    current_page_type TEXT,
    query_intent_scope TEXT,
    preferred_entity_type TEXT,
    clicks_28d INTEGER NOT NULL DEFAULT 0,
    impressions_28d INTEGER NOT NULL DEFAULT 0,
    ctr_28d NUMERIC(10,6) NOT NULL DEFAULT 0,
    avg_position_28d NUMERIC(10,4) NOT NULL DEFAULT 0,
    clicks_90d INTEGER NOT NULL DEFAULT 0,
    impressions_90d INTEGER NOT NULL DEFAULT 0,
    ctr_90d NUMERIC(10,6) NOT NULL DEFAULT 0,
    avg_position_90d NUMERIC(10,4) NOT NULL DEFAULT 0,
    demand_score NUMERIC(8,2) NOT NULL DEFAULT 0,
    opportunity_score NUMERIC(8,2) NOT NULL DEFAULT 0,
    intent_clarity_score NUMERIC(8,2) NOT NULL DEFAULT 0,
    noise_penalty NUMERIC(8,2) NOT NULL DEFAULT 0,
    freshness_context JSONB NOT NULL DEFAULT '{}'::jsonb,
    disposition TEXT NOT NULL,
    reason_summary TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (run_id, normalized_query_key, source_url)
);

CREATE INDEX IF NOT EXISTS idx_query_gate_records_run_disposition
    ON app_runtime.query_gate_records (run_id, disposition);

CREATE INDEX IF NOT EXISTS idx_query_gate_records_store_disposition
    ON app_runtime.query_gate_records (store_hash, disposition, created_at DESC);

CREATE TABLE IF NOT EXISTS app_runtime.query_gate_agent_reviews (
    review_id BIGSERIAL PRIMARY KEY,
    gate_record_id BIGINT NOT NULL REFERENCES app_runtime.query_gate_records(gate_record_id) ON DELETE CASCADE,
    run_id BIGINT NOT NULL REFERENCES app_runtime.link_runs(run_id) ON DELETE CASCADE,
    store_hash TEXT NOT NULL,
    normalized_query_key TEXT NOT NULL,
    representative_query TEXT NOT NULL,
    source_url TEXT NOT NULL,
    source_entity_type TEXT NOT NULL DEFAULT 'product',
    source_entity_id INTEGER,
    target_entity_type TEXT,
    target_entity_id INTEGER,
    verdict TEXT NOT NULL,
    issue_type TEXT NOT NULL,
    recommended_action TEXT NOT NULL,
    confidence NUMERIC(6,4) NOT NULL DEFAULT 0,
    cluster_key TEXT NOT NULL,
    cluster_label TEXT NOT NULL,
    rationale TEXT,
    model_name TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_by TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (gate_record_id)
);

CREATE INDEX IF NOT EXISTS idx_query_gate_agent_reviews_store_run
    ON app_runtime.query_gate_agent_reviews (store_hash, run_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_query_gate_agent_reviews_cluster
    ON app_runtime.query_gate_agent_reviews (store_hash, run_id, cluster_key, verdict);

CREATE TABLE IF NOT EXISTS app_runtime.query_gate_review_requests (
    request_id BIGSERIAL PRIMARY KEY,
    gate_record_id BIGINT NOT NULL REFERENCES app_runtime.query_gate_records(gate_record_id) ON DELETE CASCADE,
    run_id BIGINT NOT NULL REFERENCES app_runtime.link_runs(run_id) ON DELETE CASCADE,
    store_hash TEXT NOT NULL,
    normalized_query_key TEXT NOT NULL,
    representative_query TEXT NOT NULL,
    source_url TEXT NOT NULL,
    source_name TEXT,
    source_entity_type TEXT NOT NULL DEFAULT 'product',
    source_entity_id INTEGER,
    current_page_type TEXT,
    target_entity_type TEXT,
    target_entity_id INTEGER,
    target_name TEXT,
    target_url TEXT,
    reason_summary TEXT,
    request_status TEXT NOT NULL DEFAULT 'requested',
    request_note TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    requested_by TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (gate_record_id)
);

CREATE INDEX IF NOT EXISTS idx_query_gate_review_requests_store_status
    ON app_runtime.query_gate_review_requests (store_hash, request_status, updated_at DESC);

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
);

CREATE INDEX IF NOT EXISTS idx_query_gate_review_submissions_store_run
    ON app_runtime.query_gate_review_submissions (store_hash, run_id, created_at DESC);

CREATE TABLE IF NOT EXISTS app_runtime.query_gate_decision_feedback (
    feedback_id BIGSERIAL PRIMARY KEY,
    store_hash TEXT NOT NULL,
    gate_record_id BIGINT NOT NULL REFERENCES app_runtime.query_gate_records(gate_record_id) ON DELETE CASCADE,
    run_id BIGINT REFERENCES app_runtime.link_runs(run_id) ON DELETE SET NULL,
    action TEXT NOT NULL,
    feedback_status TEXT NOT NULL DEFAULT 'recorded',
    request_id BIGINT REFERENCES app_runtime.query_gate_review_requests(request_id) ON DELETE SET NULL,
    diagnosis_category TEXT,
    recommended_action TEXT,
    admin_decision TEXT,
    decision_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    submitted_by TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (store_hash, gate_record_id, action)
);

CREATE INDEX IF NOT EXISTS idx_query_gate_decision_feedback_store_action
    ON app_runtime.query_gate_decision_feedback (store_hash, action, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_query_gate_decision_feedback_diagnosis
    ON app_runtime.query_gate_decision_feedback (store_hash, diagnosis_category, feedback_status);

CREATE TABLE IF NOT EXISTS app_runtime.query_target_overrides (
    override_id BIGSERIAL PRIMARY KEY,
    store_hash TEXT NOT NULL,
    normalized_query_key TEXT NOT NULL,
    source_url TEXT NOT NULL,
    source_entity_type TEXT NOT NULL DEFAULT 'product',
    source_entity_id INTEGER,
    target_entity_type TEXT NOT NULL,
    target_entity_id INTEGER NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_by TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (store_hash, normalized_query_key, source_url)
);

CREATE INDEX IF NOT EXISTS idx_query_target_overrides_store_updated
    ON app_runtime.query_target_overrides (store_hash, updated_at DESC);

CREATE TABLE IF NOT EXISTS app_runtime.link_candidates (
    candidate_id BIGSERIAL PRIMARY KEY,
    run_id BIGINT NOT NULL REFERENCES app_runtime.link_runs(run_id) ON DELETE CASCADE,
    store_hash TEXT NOT NULL,
    source_product_id INTEGER NOT NULL,
    source_name TEXT,
    source_url TEXT NOT NULL,
    target_product_id INTEGER NOT NULL,
    target_name TEXT,
    target_url TEXT NOT NULL,
    relation_type TEXT NOT NULL,
    example_query TEXT,
    anchor_label TEXT,
    hit_count INTEGER NOT NULL DEFAULT 0,
    score NUMERIC(8,2) NOT NULL DEFAULT 0,
    review_status TEXT NOT NULL DEFAULT 'pending',
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (run_id, source_product_id, target_product_id)
);

ALTER TABLE app_runtime.link_candidates
    ADD COLUMN IF NOT EXISTS source_entity_type TEXT NOT NULL DEFAULT 'product';

ALTER TABLE app_runtime.link_candidates
    ADD COLUMN IF NOT EXISTS target_entity_type TEXT NOT NULL DEFAULT 'product';

ALTER TABLE app_runtime.link_candidates
    ADD COLUMN IF NOT EXISTS source_entity_id INTEGER;

ALTER TABLE app_runtime.link_candidates
    ADD COLUMN IF NOT EXISTS target_entity_id INTEGER;

UPDATE app_runtime.link_candidates
SET source_entity_id = source_product_id
WHERE source_entity_id IS NULL;

UPDATE app_runtime.link_candidates
SET target_entity_id = target_product_id
WHERE target_entity_id IS NULL;

CREATE INDEX IF NOT EXISTS idx_link_candidates_run_status
    ON app_runtime.link_candidates (run_id, review_status);

CREATE INDEX IF NOT EXISTS idx_link_candidates_source
    ON app_runtime.link_candidates (store_hash, source_product_id);

CREATE TABLE IF NOT EXISTS app_runtime.link_reviews (
    review_id BIGSERIAL PRIMARY KEY,
    candidate_id BIGINT NOT NULL REFERENCES app_runtime.link_candidates(candidate_id) ON DELETE CASCADE,
    review_status TEXT NOT NULL,
    reviewed_by TEXT,
    review_note TEXT,
    reviewed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS app_runtime.link_publications (
    publication_id BIGSERIAL PRIMARY KEY,
    store_hash TEXT NOT NULL,
    source_product_id INTEGER NOT NULL,
    source_name TEXT,
    source_url TEXT NOT NULL,
    metafield_id BIGINT,
    html_hash TEXT,
    html_snapshot TEXT,
    publication_status TEXT NOT NULL DEFAULT 'published',
    run_id BIGINT REFERENCES app_runtime.link_runs(run_id) ON DELETE SET NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    published_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    unpublished_at TIMESTAMPTZ
);

ALTER TABLE app_runtime.link_publications
    ADD COLUMN IF NOT EXISTS source_entity_type TEXT NOT NULL DEFAULT 'product';

ALTER TABLE app_runtime.link_publications
    ADD COLUMN IF NOT EXISTS source_entity_id INTEGER;

ALTER TABLE app_runtime.link_publications
    ADD COLUMN IF NOT EXISTS metafield_key TEXT;

UPDATE app_runtime.link_publications
SET source_entity_id = source_product_id
WHERE source_entity_id IS NULL;

DROP INDEX IF EXISTS app_runtime.idx_link_publications_active;

CREATE UNIQUE INDEX idx_link_publications_active
    ON app_runtime.link_publications (store_hash, source_entity_type, source_entity_id, COALESCE(metafield_key, 'internal_links_html'))
    WHERE unpublished_at IS NULL;

CREATE TABLE IF NOT EXISTS app_runtime.admin_metric_cache (
    store_hash TEXT NOT NULL,
    metric_key TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (store_hash, metric_key)
);
