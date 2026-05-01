"""Shared constants for Fulcrum domain modules."""

GATE_REVIEW_AGENT_BATCH_SIZE = 20
GATE_REVIEW_AGENT_VERDICTS = {"correct", "incorrect", "unclear"}
GATE_REVIEW_AGENT_ISSUE_TYPES = {
    "looks_correct",
    "wrong_page_type",
    "too_broad",
    "too_narrow",
    "attribute_mismatch",
    "brand_confusion",
    "taxonomy_conflict",
    "content_misroute",
    "weak_signal",
    "needs_human_review",
}
GATE_REVIEW_AGENT_ACTIONS = {
    "keep_winner",
    "use_original",
    "use_alternate",
    "tune_logic",
    "manual_review",
}

MAPPING_AUTO_APPROVE_MIN_CONFIDENCE = 0.80
MAPPING_PENDING_STATUS = "pending_review"
MAPPING_APPROVED_STATUSES = {"auto_approved", "approved"}
MAPPING_MANUAL_STATUSES = {"approved", "ignored"}

ALPHA_DEFAULT_BUCKETS = (
    "size",
    "color",
    "material",
    "form",
    "finish",
    "pack_size",
    "brand",
    "collection",
)


__all__ = [
    "ALPHA_DEFAULT_BUCKETS",
    "GATE_REVIEW_AGENT_ACTIONS",
    "GATE_REVIEW_AGENT_BATCH_SIZE",
    "GATE_REVIEW_AGENT_ISSUE_TYPES",
    "GATE_REVIEW_AGENT_VERDICTS",
    "MAPPING_APPROVED_STATUSES",
    "MAPPING_AUTO_APPROVE_MIN_CONFIDENCE",
    "MAPPING_MANUAL_STATUSES",
    "MAPPING_PENDING_STATUS",
]
