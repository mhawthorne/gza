from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from gza.landing_policy import (
    LandingJudgment,
    LandingJudgeVerdict,
    LandingOpenBlocker,
    LandingReviewEvidence,
    classify_landing_review_policy,
)


def _counting_judge(
    verdict: LandingJudgeVerdict,
) -> tuple[Callable[[], LandingJudgment | LandingJudgeVerdict], list[str]]:
    calls: list[str] = []

    def judge() -> LandingJudgment | LandingJudgeVerdict:
        calls.append(verdict)
        if verdict == "LAND":
            return LandingJudgment("LAND", artifact_id="judge-artifact", key="judge-key")
        return verdict

    return judge, calls


def _blocker(
    finding_id: str,
    *,
    deferrable: bool,
    blocker_class: str = "adjacent",
    fingerprint: str | None = None,
) -> LandingOpenBlocker:
    return LandingOpenBlocker(
        finding_id,
        deferrable=deferrable,
        blocker_class=blocker_class,  # type: ignore[arg-type]
        source="review:gza-200",
        fingerprint=fingerprint or f"blocker:{finding_id}:normalized",
    )


def _review(**overrides: Any) -> LandingReviewEvidence:
    values: dict[str, Any] = {
        "status": "completed",
        "mode": "plain_full",
        "verdict": "APPROVED",
        "current": True,
        "parseable": True,
        "identity_matched": True,
        "review_id": "gza-200",
        "reviewed_head": "source",
    }
    values.update(overrides)
    return LandingReviewEvidence(**values)


def test_guarded_current_changes_requested_reaches_exactly_one_judge() -> None:
    judge, calls = _counting_judge("LAND")

    decision = classify_landing_review_policy(
        policy="guarded",
        review=_review(mode="plain_full", verdict="CHANGES_REQUESTED"),
        open_blockers=(_blocker("B1", deferrable=True, blocker_class="adjacent"),),
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
        review=_review(mode="resolution", verdict="CHANGES_REQUESTED"),
        open_blockers=(_blocker("B1", deferrable=True, blocker_class="adjacent"),),
        judge=judge,
    )

    assert decision.allowed is False
    assert decision.reason_code == "nondeferrable-blocker"
    assert calls == []


def test_guarded_nondeferrable_blocker_stops_before_judge() -> None:
    judge, calls = _counting_judge("LAND")

    decision = classify_landing_review_policy(
        policy="guarded",
        review=_review(mode="plain_full", verdict="CHANGES_REQUESTED"),
        open_blockers=(_blocker("B1", deferrable=False, blocker_class="correctness"),),
        judge=judge,
    )

    assert decision.allowed is False
    assert decision.reason_code == "nondeferrable-blocker"
    assert calls == []


@pytest.mark.parametrize(
    "blocker",
    (
        _blocker("B1", deferrable=True, blocker_class="unknown"),
        _blocker("B2", deferrable=True, blocker_class="unknown"),
    ),
)
def test_guarded_omitted_or_unknown_blocker_class_stops_before_judge(
    blocker: LandingOpenBlocker,
) -> None:
    judge, calls = _counting_judge("LAND")

    decision = classify_landing_review_policy(
        policy="guarded",
        review=_review(mode="plain_full", verdict="CHANGES_REQUESTED"),
        open_blockers=(blocker,),
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
        review=_review(mode="resolution", verdict="CHANGES_REQUESTED"),
        open_blockers=(_blocker("B1", deferrable=True, blocker_class="adjacent"),),
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
        _review(current=False, verdict="CHANGES_REQUESTED"),
        _review(parseable=False, verdict="CHANGES_REQUESTED"),
        _review(mode="unknown", verdict="CHANGES_REQUESTED"),
        _review(verdict="NEEDS_DISCUSSION"),
        _review(status="failed", verdict="CHANGES_REQUESTED"),
        _review(status="unavailable", verdict="CHANGES_REQUESTED"),
        _review(identity_matched=False, verdict="CHANGES_REQUESTED"),
        _review(review_id=None, verdict="CHANGES_REQUESTED"),
        _review(reviewed_head=None, verdict="CHANGES_REQUESTED"),
        _review(reviewed_head="other", verdict="CHANGES_REQUESTED"),
        _review(verdict=None),
    ],
)
def test_unusable_reviews_remain_required_review_unavailable_without_judge(
    review: LandingReviewEvidence | None,
) -> None:
    judge, calls = _counting_judge("LAND")

    decision = classify_landing_review_policy(
        policy="guarded",
        review=review,
        open_blockers=(_blocker("B1", deferrable=True, blocker_class="adjacent"),),
        judge=judge,
    )

    assert decision.allowed is False
    assert decision.reason_code == "required-review-unavailable"
    assert calls == []
