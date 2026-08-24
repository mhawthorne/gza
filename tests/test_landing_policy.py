from __future__ import annotations

from collections.abc import Callable

import pytest

from gza.landing_policy import (
    LandingJudgeVerdict,
    LandingOpenBlocker,
    LandingReviewEvidence,
    classify_landing_review_policy,
)


def _counting_judge(verdict: LandingJudgeVerdict) -> tuple[Callable[[], LandingJudgeVerdict], list[str]]:
    calls: list[str] = []

    def judge() -> LandingJudgeVerdict:
        calls.append(verdict)
        return verdict

    return judge, calls


def test_guarded_current_changes_requested_reaches_exactly_one_judge() -> None:
    judge, calls = _counting_judge("LAND")

    decision = classify_landing_review_policy(
        policy="guarded",
        review=LandingReviewEvidence(mode="plain_full", verdict="CHANGES_REQUESTED"),
        open_blockers=(LandingOpenBlocker("B1", deferrable=True),),
        judge=judge,
    )

    assert decision.allowed is True
    assert decision.reason_code is None
    assert decision.judgment_verdict == "LAND"
    assert calls == ["LAND"]


def test_strict_current_changes_requested_is_nondeferrable_without_judge() -> None:
    judge, calls = _counting_judge("LAND")

    decision = classify_landing_review_policy(
        policy="strict",
        review=LandingReviewEvidence(mode="resolution", verdict="CHANGES_REQUESTED"),
        open_blockers=(LandingOpenBlocker("B1", deferrable=True),),
        judge=judge,
    )

    assert decision.allowed is False
    assert decision.reason_code == "nondeferrable-blocker"
    assert calls == []


def test_guarded_nondeferrable_blocker_stops_before_judge() -> None:
    judge, calls = _counting_judge("LAND")

    decision = classify_landing_review_policy(
        policy="guarded",
        review=LandingReviewEvidence(mode="plain_full", verdict="CHANGES_REQUESTED"),
        open_blockers=(LandingOpenBlocker("B1", deferrable=False),),
        judge=judge,
    )

    assert decision.allowed is False
    assert decision.reason_code == "nondeferrable-blocker"
    assert calls == []


@pytest.mark.parametrize(
    ("judge_verdict", "allowed", "reason_code"),
    [
        ("LAND", True, None),
        ("BLOCK", False, "policy-or-judge-refused"),
        ("NEEDS_HUMAN", False, "policy-or-judge-refused"),
    ],
)
def test_guarded_judge_verdicts_map_to_declared_outcomes(
    judge_verdict: LandingJudgeVerdict,
    allowed: bool,
    reason_code: str | None,
) -> None:
    judge, calls = _counting_judge(judge_verdict)

    decision = classify_landing_review_policy(
        policy="guarded",
        review=LandingReviewEvidence(mode="resolution", verdict="CHANGES_REQUESTED"),
        open_blockers=(LandingOpenBlocker("B1", deferrable=True),),
        judge=judge,
    )

    assert decision.allowed is allowed
    assert decision.reason_code == reason_code
    assert decision.judgment_verdict == judge_verdict
    assert calls == [judge_verdict]


@pytest.mark.parametrize(
    "review",
    [
        None,
        LandingReviewEvidence(current=False, verdict="CHANGES_REQUESTED"),
        LandingReviewEvidence(parseable=False, verdict="CHANGES_REQUESTED"),
        LandingReviewEvidence(mode="unknown", verdict="CHANGES_REQUESTED"),
        LandingReviewEvidence(verdict="NEEDS_DISCUSSION"),
        LandingReviewEvidence(status="failed", verdict="CHANGES_REQUESTED"),
        LandingReviewEvidence(status="unavailable", verdict="CHANGES_REQUESTED"),
        LandingReviewEvidence(identity_matched=False, verdict="CHANGES_REQUESTED"),
        LandingReviewEvidence(verdict=None),
    ],
)
def test_unusable_reviews_remain_required_review_unavailable_without_judge(
    review: LandingReviewEvidence | None,
) -> None:
    judge, calls = _counting_judge("LAND")

    decision = classify_landing_review_policy(
        policy="guarded",
        review=review,
        open_blockers=(LandingOpenBlocker("B1", deferrable=True),),
        judge=judge,
    )

    assert decision.allowed is False
    assert decision.reason_code == "required-review-unavailable"
    assert calls == []
