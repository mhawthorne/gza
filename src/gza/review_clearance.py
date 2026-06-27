"""Shared review-clearance constants used by runner and lifecycle code."""

REVIEW_CLEARANCE_ARTIFACT_KIND = "review_clearance"
VERIFY_ONLY_NOOP_REVIEW_CLEARANCE_KIND = "verify_only_noop_recovered"
VERIFY_ONLY_NOOP_REVIEW_CLEARANCE_STATUS = "passed"
_LEGACY_VERIFY_ONLY_NOOP_REVIEW_CLEARANCE_STATUS = "cleared"


def is_verify_only_noop_review_clearance_status(status: str | None) -> bool:
    """Return whether an artifact status represents verify-only noop clearance."""
    return status in {
        VERIFY_ONLY_NOOP_REVIEW_CLEARANCE_STATUS,
        _LEGACY_VERIFY_ONLY_NOOP_REVIEW_CLEARANCE_STATUS,
    }
