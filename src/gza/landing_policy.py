"""Pure landing-policy refusal classification helpers."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

LandingPolicyName = Literal["guarded", "strict"]
LandingReviewMode = Literal["plain_full", "resolution", "spec_coherence", "unknown"]
LandingJudgeVerdict = Literal["LAND", "BLOCK", "NEEDS_HUMAN"]
LandingReviewVerdict = Literal["APPROVED", "APPROVED_WITH_FOLLOWUPS", "CHANGES_REQUESTED", "NEEDS_DISCUSSION"]
LandingRefusalReasonCode = Literal[
    "required-review-unavailable",
    "nondeferrable-blocker",
    "policy-or-judge-refused",
]


@dataclass(frozen=True)
class LandingReviewEvidence:
    """Decision-bearing review evidence after identity/currentness checks."""

    required: bool = True
    status: Literal["completed", "failed", "pending", "in_progress", "unavailable"] = "completed"
    mode: LandingReviewMode = "plain_full"
    verdict: LandingReviewVerdict | None = "APPROVED"
    current: bool = True
    parseable: bool = True
    identity_matched: bool = True


@dataclass(frozen=True)
class LandingOpenBlocker:
    """A current review blocker and whether guarded policy may defer it."""

    finding_id: str
    deferrable: bool


@dataclass(frozen=True)
class LandingPolicyDecision:
    """Classification result for the review/policy portion of landing refusal precedence."""

    allowed: bool
    reason_code: LandingRefusalReasonCode | None = None
    judgment_verdict: LandingJudgeVerdict | None = None


LandingJudge = Callable[[], LandingJudgeVerdict]


def classify_landing_review_policy(
    *,
    policy: LandingPolicyName,
    review: LandingReviewEvidence | None,
    open_blockers: tuple[LandingOpenBlocker, ...] = (),
    guarded_judgment_enabled: bool = True,
    judge: LandingJudge | None = None,
) -> LandingPolicyDecision:
    """Classify the review/policy portion of landing refusal precedence.

    This function owns the item 5/6/7 split for landing. Invalid or unusable review
    evidence is item 5. A current, parseable plain-full or resolution
    ``CHANGES_REQUESTED`` review is usable evidence; strict mode stops it at item 6,
    while guarded mode may advance it to the single landing judgment when deterministic
    guarded eligibility has already been established by the caller.
    """

    if review is None:
        return LandingPolicyDecision(False, "required-review-unavailable")
    if not review.required:
        return LandingPolicyDecision(True)
    if (
        review.status != "completed"
        or not review.current
        or not review.parseable
        or not review.identity_matched
        or review.verdict is None
        or review.verdict == "NEEDS_DISCUSSION"
        or review.mode == "unknown"
    ):
        return LandingPolicyDecision(False, "required-review-unavailable")
    if review.mode == "spec_coherence" and review.verdict == "CHANGES_REQUESTED":
        return LandingPolicyDecision(False, "required-review-unavailable")
    if review.verdict in {"APPROVED", "APPROVED_WITH_FOLLOWUPS"}:
        return LandingPolicyDecision(True)
    if review.verdict != "CHANGES_REQUESTED":
        return LandingPolicyDecision(False, "required-review-unavailable")
    if review.mode not in {"plain_full", "resolution"}:
        return LandingPolicyDecision(False, "required-review-unavailable")
    if policy == "strict":
        return LandingPolicyDecision(False, "nondeferrable-blocker")
    if any(not blocker.deferrable for blocker in open_blockers):
        return LandingPolicyDecision(False, "nondeferrable-blocker")
    if not guarded_judgment_enabled or judge is None:
        return LandingPolicyDecision(False, "policy-or-judge-refused")

    judgment = judge()
    if judgment == "LAND":
        return LandingPolicyDecision(True, judgment_verdict=judgment)
    return LandingPolicyDecision(False, "policy-or-judge-refused", judgment_verdict=judgment)
