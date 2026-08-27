from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest

from gza.config import Config
from gza.db import SqliteTaskStore, Task
from gza.landing import (
    LANDING_PHASES,
    LandingPostRebaseReviewRequest,
    LandingFollowupFinding,
    LandingFollowupMaterializationIdentity,
    LandingOpenBlocker,
    LandingJudgment,
    LandingJudgeVerdict,
    LandingPolicyDecision,
    LandingPolicyFacts,
    LandingRebaseOutcomeIdentity,
    LandingRebaseFingerprint,
    LandingReviewEvidence,
    LandingSpecCoherenceEvidence,
    LandingSpecCoherenceFingerprint,
    LandingStateFingerprint,
    LandingVerifyEvidence,
    LandBlocked,
    LandPostMergeVerifyFailure,
    LandRequest,
    LandResult,
    LandStep,
    acquire_landing_verify_evidence,
    acquire_one_post_rebase_review,
    dry_run_steps_until_boundary,
    evaluate_landing_policy,
    inspect_current_landing_verify_evidence,
)
from gza.review_scope import build_resolution_review_scope
from gza.review_tasks import DuplicateReviewError
from gza.review_verify_state import (
    VerifyGateDecision,
    VerifyGateLookup,
    VerifyGateResult,
    make_verify_epoch,
    persist_verify_gate_artifact,
    persist_recredited_verify_gate_artifact,
)
from gza.runner import (
    LifecycleVerifyExecution,
    ProjectVerificationResult,
    ReviewVerifyResult,
    _persist_lifecycle_verify_execution,
)


TREE_A = "a" * 64
TREE_B = "b" * 64


def _review(**overrides: Any) -> LandingReviewEvidence:
    values: dict[str, Any] = {
        "required": True,
        "status": "completed",
        "mode": "plain_full",
        "verdict": "APPROVED",
        "current": True,
        "parseable": True,
        "identity_matched": True,
        "review_id": "gza-200",
        "reviewed_head": "source-a",
    }
    values.update(overrides)
    return LandingReviewEvidence(**values)


def _verify(**overrides: Any) -> LandingVerifyEvidence:
    values: dict[str, Any] = {
        "status": "passed",
        "current": True,
        "identity_matched": True,
        "epoch": "verify-1",
        "gate_identity": "gate-a",
        "tree_fingerprint": TREE_A,
    }
    values.update(overrides)
    return LandingVerifyEvidence(**values)


def _green_facts(**overrides: Any) -> LandingPolicyFacts:
    values: dict[str, Any] = {
        "task_id": "gza-100",
        "merge_unit_state": "unmerged",
        "representative_status": "completed",
        "has_active_merge_unit": True,
        "has_local_source": True,
        "target_matches_checkout": True,
        "dependency_ready": True,
        "project_scope_ok": True,
        "checkout_clean": True,
        "source_head": "source-a",
        "target_head": "target-a",
        "clean_merge": True,
        "ancestry_proof_available": True,
        "rebase_status": "none",
        "rebase_resolution_kind": "none",
        "rebase_target_contained": True,
        "verify": _verify(),
        "review": _review(),
    }
    values.update(overrides)
    return LandingPolicyFacts(**values)


def _recording_judge(
    calls: list[str],
    verdict: LandingJudgeVerdict,
) -> Callable[[], LandingJudgment | LandingJudgeVerdict]:
    def judge() -> LandingJudgment | LandingJudgeVerdict:
        calls.append("called")
        if verdict == "LAND":
            return LandingJudgment("LAND", artifact_id="judge-artifact", key="judge-key")
        return verdict

    return judge


def _landing_judgment() -> LandingJudgment:
    return LandingJudgment("LAND", artifact_id="judge-artifact", key="judge-key")


def _blocker(
    finding_id: str,
    *,
    deferrable: bool,
    blocker_class: str = "out_of_scope",
    source: str = "review:gza-200",
    fingerprint: str | None = None,
) -> LandingOpenBlocker:
    return LandingOpenBlocker(
        finding_id,
        deferrable=deferrable,
        blocker_class=blocker_class,  # type: ignore[arg-type]
        source=source,
        fingerprint=fingerprint or f"blocker:{finding_id}:normalized",
    )


@pytest.mark.parametrize(
    ("facts", "reason_code"),
    (
        (LandingPolicyFacts(task_id="gza-100"), "identity-proof-unavailable"),
        (_green_facts(review=_review(review_id=None)), "required-review-unavailable"),
        (_green_facts(review=_review(reviewed_head=None)), "required-review-unavailable"),
        (_green_facts(review=_review(reviewed_head="old-head")), "required-review-unavailable"),
        (_green_facts(verify=_verify(epoch=None)), "verify-unavailable-or-red"),
        (_green_facts(verify=_verify(gate_identity=None)), "verify-unavailable-or-red"),
        (_green_facts(verify=_verify(tree_fingerprint=None)), "verify-unavailable-or-red"),
        (_green_facts(rebase_status="pending", rebase_resolution_kind="none"), "rebase-or-conflict"),
        (_green_facts(rebase_status="in_progress", rebase_resolution_kind="none"), "rebase-or-conflict"),
        (_green_facts(rebase_status="completed", rebase_resolution_kind="none"), "rebase-or-conflict"),
        (_green_facts(rebase_status="none", rebase_resolution_kind="mechanical"), "rebase-or-conflict"),
        (
            _green_facts(rebase_status="none", rebase_resolution_kind="none", rebase_target_contained=False),
            "rebase-or-conflict",
        ),
        (
            _green_facts(rebase_status="none", rebase_resolution_kind="none", rebase_target_contained=None),
            "rebase-or-conflict",
        ),
        (
            _green_facts(
                spec_coherence=LandingSpecCoherenceEvidence(
                    required=True,
                    status="unavailable",
                    verdict=None,
                    current=False,
                    identity_matched=False,
                ),
            ),
            "required-review-unavailable",
        ),
        (
            _green_facts(
                spec_coherence=LandingSpecCoherenceEvidence(
                    required=True,
                    status="completed",
                    verdict="CHANGES_REQUESTED",
                    current=True,
                    identity_matched=True,
                    evidence_id="spec-1",
                    reviewed_head="source-a",
                    changed_paths_fingerprint="paths-a",
                ),
            ),
            "required-review-unavailable",
        ),
        (
            _green_facts(
                spec_coherence=LandingSpecCoherenceEvidence(
                    required=True,
                    status="completed",
                    verdict="APPROVED",
                    current=False,
                    identity_matched=True,
                    evidence_id="spec-1",
                    reviewed_head="source-a",
                    changed_paths_fingerprint="paths-a",
                ),
            ),
            "required-review-unavailable",
        ),
        (_green_facts(actionable_lifecycle_work=("verify-fix:gza-300",)), "verify-unavailable-or-red"),
        (
            _green_facts(
                open_blockers=(_blocker("B1", deferrable=True, blocker_class="out_of_scope"),),
            ),
            "nondeferrable-blocker",
        ),
    ),
)
def test_landing_policy_fail_closed_identity_table(
    facts: LandingPolicyFacts,
    reason_code: str,
) -> None:
    calls: list[str] = []

    decision = evaluate_landing_policy(
        policy="guarded",
        facts=facts,
        judge=_recording_judge(calls, "LAND"),
    )

    assert decision.allowed is False
    assert decision.blocked is not None
    assert decision.blocked.reason_code == reason_code
    assert decision.blocked.evidence_refs
    assert calls == []


@pytest.mark.parametrize(
    ("work", "expected_reason"),
    (
        ("rebase:gza-201", "rebase-or-conflict"),
        ("verify:gza-202", "verify-unavailable-or-red"),
        ("review:gza-203", "required-review-unavailable"),
        ("spec-coherence:gza-204", "required-review-unavailable"),
    ),
)
def test_landing_policy_maps_exact_matching_lifecycle_work_to_phase_refusal(
    work: str,
    expected_reason: str,
) -> None:
    calls: list[str] = []

    decision = evaluate_landing_policy(
        policy="guarded",
        facts=_green_facts(actionable_lifecycle_work=(work,)),
        judge=_recording_judge(calls, "LAND"),
    )

    assert decision.allowed is False
    assert decision.blocked is not None
    assert decision.blocked.reason_code == expected_reason
    assert decision.blocked.evidence_refs == ("gza-100", work)
    assert calls == []


@pytest.mark.parametrize(
    "work",
    (
        "active-work-identity-mismatch:gza-205",
        "ambiguous-active-work:gza-206",
        "stale-active-work:gza-207",
    ),
)
def test_landing_policy_reserves_identity_refusal_for_active_work_identity_mismatch(
    work: str,
) -> None:
    calls: list[str] = []

    decision = evaluate_landing_policy(
        policy="guarded",
        facts=_green_facts(actionable_lifecycle_work=(work,)),
        judge=_recording_judge(calls, "LAND"),
    )

    assert decision.allowed is False
    assert decision.blocked is not None
    assert decision.blocked.reason_code == "identity-proof-unavailable"
    assert decision.blocked.evidence_refs == ("gza-100", work)
    assert calls == []


@pytest.mark.parametrize("representative_status", ("completed", "unmerged"))
def test_landing_policy_allows_exact_identity_and_compat_unmerged_status(
    representative_status: str,
) -> None:
    decision = evaluate_landing_policy(
        policy="guarded",
        facts=_green_facts(representative_status=representative_status),
    )

    assert decision.allowed is True
    assert decision.allowed_overrides == ()


def test_landing_policy_preserves_non_escalated_review_disabled_path() -> None:
    allowed = evaluate_landing_policy(
        policy="guarded",
        facts=_green_facts(review=_review(required=False)),
    )
    blocked = evaluate_landing_policy(
        policy="guarded",
        facts=_green_facts(
            review=_review(required=False),
            parked_reason="improve-no-op",
        ),
        judge=_landing_judgment,
    )

    assert allowed.allowed is True
    assert blocked.allowed is False
    assert blocked.blocked is not None
    assert blocked.blocked.reason_code == "policy-or-judge-refused"


def test_review_disabled_approved_with_followups_without_findings_is_not_merge_permitting() -> None:
    decision = evaluate_landing_policy(
        policy="guarded",
        facts=_green_facts(review=_review(required=False, verdict="APPROVED_WITH_FOLLOWUPS")),
    )

    assert decision.allowed is False
    assert decision.blocked is not None
    assert decision.blocked.reason_code == "required-review-unavailable"
    assert "approved-with-followups review has no valid follow-up" in decision.blocked.fact
    assert decision.followup_materialization_identities == ()


def test_review_disabled_approved_with_malformed_followup_identity_is_not_merge_permitting() -> None:
    decision = evaluate_landing_policy(
        policy="guarded",
        facts=_green_facts(
            review=_review(
                required=False,
                verdict="APPROVED_WITH_FOLLOWUPS",
                followup_findings=(
                    LandingFollowupFinding("F1", fingerprint="followup:f1", source=None),
                ),
            )
        ),
    )

    assert decision.allowed is False
    assert decision.blocked is not None
    assert decision.blocked.reason_code == "required-review-unavailable"
    assert decision.followup_materialization_identities == ()


def test_review_disabled_approved_with_followups_is_not_merge_permitting() -> None:
    decision = evaluate_landing_policy(
        policy="guarded",
        facts=_green_facts(
            review=_review(
                required=False,
                verdict="APPROVED",
                followup_findings=(
                    LandingFollowupFinding("F1", fingerprint="followup:f1", source="review:gza-200"),
                ),
            )
        ),
    )

    assert decision.allowed is False
    assert decision.blocked is not None
    assert decision.blocked.reason_code == "required-review-unavailable"
    assert "approved review contradicts parsed follow-up" in decision.blocked.fact


def test_review_disabled_approved_with_followups_preserves_exact_identities_and_fingerprint() -> None:
    facts_without_followups = _green_facts(review=_review(required=False))
    facts_with_followups = _green_facts(
        review=_review(
            required=False,
            verdict="APPROVED_WITH_FOLLOWUPS",
            followup_findings=(
                LandingFollowupFinding("F2", fingerprint="followup:b", source="review:gza-200"),
                LandingFollowupFinding("F1", fingerprint="followup:a", source="review:gza-200"),
            ),
        )
    )

    decision = evaluate_landing_policy(policy="guarded", facts=facts_with_followups)

    assert decision.allowed is True
    assert decision.blocked is None
    assert decision.followup_materialization_identities == (
        LandingFollowupMaterializationIdentity(
            review_id="gza-200",
            source="review:gza-200",
            finding_id="F1",
            fingerprint="followup:a",
        ),
        LandingFollowupMaterializationIdentity(
            review_id="gza-200",
            source="review:gza-200",
            finding_id="F2",
            fingerprint="followup:b",
        ),
    )
    assert LandingStateFingerprint.from_facts(facts_with_followups) != LandingStateFingerprint.from_facts(
        facts_without_followups
    )


def test_approved_with_followups_without_followup_evidence_is_not_merge_permitting() -> None:
    decision = evaluate_landing_policy(
        policy="guarded",
        facts=_green_facts(review=_review(verdict="APPROVED_WITH_FOLLOWUPS")),
    )

    assert decision.allowed is False
    assert decision.blocked is not None
    assert decision.blocked.reason_code == "required-review-unavailable"
    assert "approved-with-followups review has no valid follow-up" in decision.blocked.fact


def test_approved_with_followups_with_valid_followup_evidence_is_merge_permitting() -> None:
    decision = evaluate_landing_policy(
        policy="guarded",
        facts=_green_facts(
            review=_review(
                verdict="APPROVED_WITH_FOLLOWUPS",
                followup_findings=(
                    LandingFollowupFinding("F1", fingerprint="followup:f1", source="review:gza-200"),
                ),
            )
        ),
    )

    assert decision.allowed is True
    assert decision.blocked is None
    assert decision.followup_materialization_identities == (
        LandingFollowupMaterializationIdentity(
            review_id="gza-200",
            source="review:gza-200",
            finding_id="F1",
            fingerprint="followup:f1",
        ),
    )


def test_approved_with_followups_exposes_multiple_exact_materialization_identities() -> None:
    decision = evaluate_landing_policy(
        policy="guarded",
        facts=_green_facts(
            review=_review(
                verdict="APPROVED_WITH_FOLLOWUPS",
                followup_findings=(
                    LandingFollowupFinding("F2", fingerprint="same-content", source="review:gza-200"),
                    LandingFollowupFinding("F1", fingerprint="same-content", source="review:gza-200"),
                ),
            )
        ),
    )

    assert decision.allowed is True
    assert decision.followup_materialization_identities == (
        LandingFollowupMaterializationIdentity(
            review_id="gza-200",
            source="review:gza-200",
            finding_id="F1",
            fingerprint="same-content",
        ),
        LandingFollowupMaterializationIdentity(
            review_id="gza-200",
            source="review:gza-200",
            finding_id="F2",
            fingerprint="same-content",
        ),
    )


def test_approved_with_followups_without_review_identity_is_not_merge_permitting() -> None:
    decision = evaluate_landing_policy(
        policy="guarded",
        facts=_green_facts(
            review=_review(
                verdict="APPROVED_WITH_FOLLOWUPS",
                review_id=None,
                followup_findings=(
                    LandingFollowupFinding("F1", fingerprint="followup:f1", source="review:gza-200"),
                ),
            )
        ),
    )

    assert decision.allowed is False
    assert decision.blocked is not None
    assert decision.blocked.reason_code == "required-review-unavailable"


@pytest.mark.parametrize(
    "review",
    (
        _review(
            verdict="APPROVED_WITH_FOLLOWUPS",
            review_id="   ",
            followup_findings=(LandingFollowupFinding("F1", fingerprint="followup:f1", source="review:gza-200"),),
        ),
        _review(
            verdict="APPROVED_WITH_FOLLOWUPS",
            followup_findings=(LandingFollowupFinding("F1", fingerprint="followup:f1", source=None),),
        ),
        _review(
            verdict="APPROVED_WITH_FOLLOWUPS",
            followup_findings=(LandingFollowupFinding("F1", fingerprint="followup:f1", source="   "),),
        ),
        _review(
            verdict="APPROVED_WITH_FOLLOWUPS",
            followup_findings=(LandingFollowupFinding("   ", fingerprint="followup:f1", source="review:gza-200"),),
        ),
        _review(
            verdict="APPROVED_WITH_FOLLOWUPS",
            followup_findings=(LandingFollowupFinding("F1", fingerprint=None, source="review:gza-200"),),
        ),
        _review(
            verdict="APPROVED_WITH_FOLLOWUPS",
            followup_findings=(LandingFollowupFinding("F1", fingerprint="   ", source="review:gza-200"),),
        ),
    ),
)
def test_approved_with_followups_with_malformed_identity_is_typed_refusal(
    review: LandingReviewEvidence,
) -> None:
    decision = evaluate_landing_policy(
        policy="guarded",
        facts=_green_facts(review=review),
    )

    assert decision.allowed is False
    assert decision.blocked is not None
    assert decision.blocked.reason_code == "required-review-unavailable"


def test_followup_identity_canonicalizes_normalization_equivalent_inputs() -> None:
    decision_a = evaluate_landing_policy(
        policy="guarded",
        facts=_green_facts(
            review=_review(
                review_id=" gza-200 ",
                verdict="APPROVED_WITH_FOLLOWUPS",
                followup_findings=(
                    LandingFollowupFinding(" F1 ", fingerprint=" followup:f1 ", source=" review:gza-200 "),
                ),
            )
        ),
    )
    decision_b = evaluate_landing_policy(
        policy="guarded",
        facts=_green_facts(
            review=_review(
                review_id="gza-200",
                verdict="APPROVED_WITH_FOLLOWUPS",
                followup_findings=(
                    LandingFollowupFinding("F1", fingerprint="followup:f1", source="review:gza-200"),
                ),
            )
        ),
    )

    assert decision_a.allowed is True
    assert decision_a.followup_materialization_identities == decision_b.followup_materialization_identities


def test_approved_with_followups_with_duplicate_durable_finding_ids_is_not_merge_permitting() -> None:
    for followups in (
        (
            LandingFollowupFinding("F1", fingerprint="followup:a", source="review:gza-200"),
            LandingFollowupFinding("F1", fingerprint="followup:b", source="review:gza-200"),
        ),
        (
            LandingFollowupFinding("F1", fingerprint="followup:a", source="source:a"),
            LandingFollowupFinding("F1", fingerprint="followup:b", source="source:b"),
        ),
    ):
        decision = evaluate_landing_policy(
            policy="guarded",
            facts=_green_facts(
                review=_review(
                    verdict="APPROVED_WITH_FOLLOWUPS",
                    followup_findings=followups,
                )
            ),
        )

        assert decision.allowed is False
        assert decision.blocked is not None
        assert decision.blocked.reason_code == "required-review-unavailable"


def test_approved_with_followup_evidence_is_rejected_as_inconsistent() -> None:
    decision = evaluate_landing_policy(
        policy="guarded",
        facts=_green_facts(
            review=_review(
                verdict="APPROVED",
                followup_findings=(LandingFollowupFinding("F1", fingerprint="followup:f1", source="review:gza-200"),),
            )
        ),
    )

    assert decision.allowed is False
    assert decision.blocked is not None
    assert decision.blocked.reason_code == "required-review-unavailable"
    assert "approved review contradicts parsed follow-up" in decision.blocked.fact


def test_guarded_allows_exact_review_churn_park_and_deferred_blocker_override() -> None:
    decision = evaluate_landing_policy(
        policy="guarded",
        facts=_green_facts(
            parked_reason="review-max-cycles-reached",
            review=_review(verdict="CHANGES_REQUESTED"),
            open_blockers=(_blocker("B1", deferrable=True, blocker_class="out_of_scope"),),
        ),
        judge=_landing_judgment,
    )

    assert decision.allowed is True
    assert decision.blocked is None
    assert decision.allowed_overrides == (
        "defer-review-blockers",
        "parked:review-max-cycles-reached",
    )
    assert decision.judgment_verdict == "LAND"


def test_guarded_changes_requested_preserves_judgment_and_followup_identities() -> None:
    decision = evaluate_landing_policy(
        policy="guarded",
        facts=_green_facts(
            parked_reason="review-max-cycles-reached",
            review=_review(
                verdict="CHANGES_REQUESTED",
                followup_findings=(
                    LandingFollowupFinding("F2", fingerprint="followup:b", source="review:gza-200"),
                    LandingFollowupFinding("F1", fingerprint="followup:a", source="review:gza-200"),
                ),
            ),
            open_blockers=(_blocker("B1", deferrable=True, blocker_class="out_of_scope"),),
        ),
        judge=_landing_judgment,
    )

    assert decision.allowed is True
    assert decision.blocked is None
    assert decision.allowed_overrides == (
        "defer-review-blockers",
        "parked:review-max-cycles-reached",
    )
    assert decision.judgment_verdict == "LAND"
    assert decision.judgment_artifact_id == "judge-artifact"
    assert decision.judgment_key == "judge-key"
    assert decision.followup_materialization_identities == (
        LandingFollowupMaterializationIdentity(
            review_id="gza-200",
            source="review:gza-200",
            finding_id="F1",
            fingerprint="followup:a",
        ),
        LandingFollowupMaterializationIdentity(
            review_id="gza-200",
            source="review:gza-200",
            finding_id="F2",
            fingerprint="followup:b",
        ),
    )


@pytest.mark.parametrize(
    "review",
    (
        _review(
            verdict="CHANGES_REQUESTED",
            followup_findings=(LandingFollowupFinding("F1", fingerprint="followup:f1", source=None),),
        ),
        _review(
            verdict="CHANGES_REQUESTED",
            followup_findings=(LandingFollowupFinding("F1", fingerprint="followup:f1", source="   "),),
        ),
        _review(
            verdict="CHANGES_REQUESTED",
            followup_findings=(LandingFollowupFinding("F1", fingerprint=None, source="review:gza-200"),),
        ),
        _review(
            verdict="CHANGES_REQUESTED",
            followup_findings=(LandingFollowupFinding("F1", fingerprint="   ", source="review:gza-200"),),
        ),
        _review(
            verdict="CHANGES_REQUESTED",
            followup_findings=(LandingFollowupFinding("   ", fingerprint="followup:f1", source="review:gza-200"),),
        ),
        _review(
            verdict="CHANGES_REQUESTED",
            review_id=None,
            followup_findings=(LandingFollowupFinding("F1", fingerprint="followup:f1", source="review:gza-200"),),
        ),
        _review(
            verdict="CHANGES_REQUESTED",
            followup_findings=(
                LandingFollowupFinding("F1", fingerprint="followup:a", source="review:gza-200"),
                LandingFollowupFinding("F1", fingerprint="followup:b", source="review:gza-200"),
            ),
        ),
    ),
)
def test_guarded_changes_requested_with_malformed_followups_refuses_before_judge(
    review: LandingReviewEvidence,
) -> None:
    calls: list[str] = []

    decision = evaluate_landing_policy(
        policy="guarded",
        facts=_green_facts(
            parked_reason="review-max-cycles-reached",
            review=review,
            open_blockers=(_blocker("B1", deferrable=True, blocker_class="out_of_scope"),),
        ),
        judge=_recording_judge(calls, "LAND"),
    )

    assert decision.allowed is False
    assert decision.blocked is not None
    assert decision.blocked.reason_code == "required-review-unavailable"
    assert calls == []
    assert decision.judgment_verdict is None
    assert decision.judgment_artifact_id is None
    assert decision.judgment_key is None
    assert decision.followup_materialization_identities == ()


@pytest.mark.parametrize(
    "blocker",
    (
        _blocker("B1", deferrable=True, blocker_class="unknown"),
        _blocker("B2", deferrable=True, blocker_class="unknown"),
    ),
)
def test_changes_requested_with_omitted_or_unknown_blocker_class_never_calls_judge(
    blocker: LandingOpenBlocker,
) -> None:
    calls: list[str] = []

    decision = evaluate_landing_policy(
        policy="guarded",
        facts=_green_facts(
            parked_reason="review-max-cycles-reached",
            review=_review(verdict="CHANGES_REQUESTED"),
            open_blockers=(blocker,),
        ),
        judge=_recording_judge(calls, "LAND"),
    )

    assert decision.allowed is False
    assert decision.blocked is not None
    assert decision.blocked.reason_code == "nondeferrable-blocker"
    assert calls == []


@pytest.mark.parametrize(
    "parked_reason",
    (
        "review-max-cycles-reached",
        "duplicate-blocker-no-progress",
        "improve-no-op",
        "review-blocker-adjudication-needed",
    ),
)
def test_changes_requested_with_eligible_park_and_no_blocker_records_never_calls_judge(
    parked_reason: str,
) -> None:
    calls: list[str] = []

    decision = evaluate_landing_policy(
        policy="guarded",
        facts=_green_facts(
            parked_reason=parked_reason,
            review_blocker_adjudication_evidence_complete=True,
            review=_review(verdict="CHANGES_REQUESTED"),
            open_blockers=(),
        ),
        judge=_recording_judge(calls, "LAND"),
    )

    assert decision.allowed is False
    assert decision.blocked is not None
    assert decision.blocked.reason_code == "nondeferrable-blocker"
    assert calls == []


@pytest.mark.parametrize(
    "parked_reason",
    (
        "review-max-cycles-reached",
        "duplicate-blocker-no-progress",
        "improve-no-op",
    ),
)
@pytest.mark.parametrize("judgment", ("BLOCK", "NEEDS_HUMAN"))
def test_approved_review_plus_eligible_park_requires_one_land_judgment(
    parked_reason: str,
    judgment: str,
) -> None:
    calls: list[str] = []
    decision = evaluate_landing_policy(
        policy="guarded",
        facts=_green_facts(parked_reason=parked_reason),
        judge=_recording_judge(calls, judgment),  # type: ignore[arg-type]
    )

    assert calls == ["called"]
    assert decision.allowed is False
    assert decision.blocked is not None
    assert decision.blocked.reason_code == "policy-or-judge-refused"
    assert decision.judgment_verdict == judgment


def test_approved_review_plus_eligible_park_allows_only_after_land_judgment() -> None:
    calls: list[str] = []

    decision = evaluate_landing_policy(
        policy="guarded",
        facts=_green_facts(parked_reason="improve-no-op"),
        judge=_recording_judge(calls, "LAND"),
    )

    assert calls == ["called"]
    assert decision.allowed is True
    assert decision.allowed_overrides == ("parked:improve-no-op",)
    assert decision.judgment_artifact_id == "judge-artifact"
    assert decision.judgment_key == "judge-key"


def test_unsupported_incomplete_and_review_disabled_parks_do_not_call_judge() -> None:
    for facts in (
        _green_facts(parked_reason="needs-discussion"),
        _green_facts(parked_reason="review-blocker-adjudication-needed"),
        _green_facts(review=_review(required=False), parked_reason="improve-no-op"),
    ):
        calls: list[str] = []
        decision = evaluate_landing_policy(
            policy="guarded",
            facts=facts,
            judge=_recording_judge(calls, "LAND"),
        )

        assert decision.allowed is False
        assert decision.blocked is not None
        assert calls == []


@pytest.mark.parametrize(
    ("facts", "expected_reason"),
    (
        (_green_facts(actionable_lifecycle_work=("review:gza-201",), checkout_clean=False), "dirty-checkout"),
        (
            _green_facts(
                actionable_lifecycle_work=("rebase:gza-202",),
                rebase_status="failed",
                rebase_resolution_kind="none",
            ),
            "rebase-or-conflict",
        ),
        (
            _green_facts(actionable_lifecycle_work=("verify:gza-203",), verify=_verify(status="failed")),
            "verify-unavailable-or-red",
        ),
        (
            _green_facts(actionable_lifecycle_work=("review:gza-204",), verify=_verify(status="failed")),
            "verify-unavailable-or-red",
        ),
        (
            _green_facts(
                actionable_lifecycle_work=("spec-coherence:gza-205",),
                review=_review(verdict="CHANGES_REQUESTED"),
                open_blockers=(_blocker("B1", deferrable=False, blocker_class="correctness"),),
            ),
            "required-review-unavailable",
        ),
        (
            _green_facts(
                actionable_lifecycle_work=("active-work-identity-mismatch:gza-206",),
                checkout_clean=False,
            ),
            "identity-proof-unavailable",
        ),
        (_green_facts(review=_review(current=False), parked_reason="needs-discussion"), "required-review-unavailable"),
        (
            _green_facts(
                review=_review(parseable=False),
                parked_reason="review-blocker-adjudication-needed",
            ),
            "required-review-unavailable",
        ),
        (
            _green_facts(
                review=_review(verdict="CHANGES_REQUESTED"),
                open_blockers=(_blocker("B1", deferrable=False, blocker_class="correctness"),),
                parked_reason="review-blocker-adjudication-needed",
            ),
            "nondeferrable-blocker",
        ),
    ),
)
def test_landing_policy_selects_declared_precedence_for_overlapping_refusals(
    facts: LandingPolicyFacts,
    expected_reason: str,
) -> None:
    calls: list[str] = []

    decision = evaluate_landing_policy(
        policy="guarded",
        facts=facts,
        judge=_recording_judge(calls, "LAND"),
    )

    assert decision.allowed is False
    assert decision.blocked is not None
    assert decision.blocked.reason_code == expected_reason
    assert calls == []


def test_adjudication_park_requires_complete_evidence_and_successful_judgment() -> None:
    calls: list[str] = []
    decision = evaluate_landing_policy(
        policy="guarded",
        facts=_green_facts(
            parked_reason="review-blocker-adjudication-needed",
            review_blocker_adjudication_evidence_complete=True,
        ),
        judge=_recording_judge(calls, "LAND"),
    )

    assert calls == ["called"]
    assert decision.allowed is True
    assert decision.allowed_overrides == ("parked:review-blocker-adjudication-needed",)


def test_strict_refuses_changes_requested_without_judge() -> None:
    calls: list[str] = []

    decision = evaluate_landing_policy(
        policy="strict",
        facts=_green_facts(
            review=_review(mode="resolution", verdict="CHANGES_REQUESTED"),
            open_blockers=(_blocker("B1", deferrable=True, blocker_class="out_of_scope"),),
        ),
        judge=_recording_judge(calls, "LAND"),
    )

    assert decision.allowed is False
    assert decision.blocked is not None
    assert decision.blocked.reason_code == "nondeferrable-blocker"
    assert decision.blocked.evidence_refs
    assert calls == []


def test_spec_coherence_review_cannot_be_used_as_code_review_evidence() -> None:
    decision = evaluate_landing_policy(
        policy="guarded",
        facts=_green_facts(review=_review(mode="spec_coherence", verdict="CHANGES_REQUESTED")),
        judge=_landing_judgment,
    )

    assert decision.allowed is False
    assert decision.blocked is not None
    assert decision.blocked.reason_code == "required-review-unavailable"


def test_conflict_resolution_and_correctness_blockers_are_nondeferrable() -> None:
    for blocker in (
        _blocker("B1", deferrable=True, blocker_class="conflict_resolution"),
        _blocker("B2", deferrable=True, blocker_class="correctness"),
    ):
        decision = evaluate_landing_policy(
            policy="guarded",
            facts=_green_facts(
                review=_review(mode="resolution", verdict="CHANGES_REQUESTED"),
                open_blockers=(blocker,),
            ),
            judge=_landing_judgment,
        )
        assert decision.allowed is False
        assert decision.blocked is not None
        assert decision.blocked.reason_code == "nondeferrable-blocker"
        assert decision.blocked.evidence_refs == (blocker.finding_id, blocker.source, blocker.fingerprint)


def test_landing_phase_order_matches_verify_spec_coherence_review_contract() -> None:
    assert LANDING_PHASES == (
        "resolve",
        "rebase",
        "verify",
        "spec_coherence",
        "post_rebase_review",
        "judge",
        "defer_blockers",
        "merge",
        "post_merge_verify",
    )


@pytest.mark.parametrize("phase", ("verify", "spec_coherence"))
def test_dry_run_steps_stop_at_verify_and_spec_coherence_boundaries(phase: str) -> None:
    steps = dry_run_steps_until_boundary(
        resolved=True,
        first_execution_required_phase=phase,  # type: ignore[arg-type]
    )

    assert steps[-1] == LandStep(
        phase=phase,  # type: ignore[arg-type]
        status="conditional",
        summary="execution required before later outcomes are knowable",
    )
    assert [step.phase for step in steps] == list(LANDING_PHASES[: LANDING_PHASES.index(phase) + 1])


@pytest.mark.parametrize(
    "facts",
    (
        _green_facts(
            rebase_status="completed",
            rebase_resolution_kind="mechanical",
            rebase_changed_diff=False,
            rebase_outcome_id="rebase-1",
            rebase_attempted_source_head="source-a",
            rebase_attempted_target_head="target-a",
            rebase_target_contained=True,
        ),
        _green_facts(
            rebase_status="completed",
            rebase_resolution_kind="provider_resolved",
            rebase_changed_diff=False,
            rebase_outcome_id="rebase-2",
            rebase_attempted_source_head="source-a",
            rebase_attempted_target_head="target-a",
            rebase_target_contained=True,
            rebase_provider_resolution_proof=True,
        ),
        _green_facts(
            rebase_status="completed",
            rebase_resolution_kind="no_op",
            rebase_no_op_subtype="already_contained",
            rebase_changed_diff=False,
            rebase_outcome_id="rebase-3",
            rebase_attempted_source_head="source-a",
            rebase_attempted_target_head="target-a",
            rebase_target_contained=True,
            rebase_provider_resolution_proof=False,
        ),
        _green_facts(
            rebase_status="completed",
            rebase_resolution_kind="no_op",
            rebase_no_op_subtype="superseded_contained",
            rebase_outcome_id="rebase-4",
            rebase_attempted_source_head="old-source",
            rebase_attempted_target_head="target-a",
            rebase_target_contained=True,
            rebase_provider_resolution_proof=False,
        ),
        _green_facts(
            rebase_status="completed",
            rebase_resolution_kind="no_op",
            rebase_no_op_subtype="unchanged_target",
            rebase_changed_diff=False,
            rebase_outcome_id="rebase-5",
            rebase_attempted_source_head="source-a",
            rebase_attempted_target_head="target-a",
            rebase_target_contained=True,
            rebase_provider_resolution_proof=False,
        ),
        _green_facts(
            rebase_status="completed",
            rebase_resolution_kind="no_op",
            rebase_no_op_subtype="moot",
            rebase_changed_diff=False,
            rebase_outcome_id="rebase-6",
            rebase_attempted_source_head="source-a",
            rebase_attempted_target_head="target-a",
            rebase_target_contained=True,
            rebase_provider_resolution_proof=False,
        ),
    ),
)
def test_supported_rebase_outcomes_are_decision_representable(
    facts: LandingPolicyFacts,
) -> None:
    decision = evaluate_landing_policy(policy="guarded", facts=facts)

    assert decision.allowed is True


@pytest.mark.parametrize(
    "overrides",
    (
        {
            "rebase_resolution_kind": "mechanical",
            "rebase_changed_diff": False,
            "rebase_attempted_source_head": "source-a",
            "rebase_attempted_target_head": "target-a",
            "rebase_target_contained": True,
        },
        {
            "rebase_resolution_kind": "provider_resolved",
            "rebase_changed_diff": False,
            "rebase_attempted_source_head": "source-a",
            "rebase_attempted_target_head": "target-a",
            "rebase_target_contained": True,
            "rebase_provider_resolution_proof": True,
        },
        {
            "rebase_resolution_kind": "no_op",
            "rebase_no_op_subtype": "already_contained",
            "rebase_changed_diff": False,
            "rebase_attempted_source_head": "source-a",
            "rebase_attempted_target_head": "target-a",
            "rebase_target_contained": True,
            "rebase_provider_resolution_proof": False,
        },
        {
            "rebase_resolution_kind": "no_op",
            "rebase_no_op_subtype": "superseded_contained",
            "rebase_attempted_source_head": "old-source",
            "rebase_attempted_target_head": "target-a",
            "rebase_target_contained": True,
            "rebase_provider_resolution_proof": False,
        },
        {
            "rebase_resolution_kind": "no_op",
            "rebase_no_op_subtype": "unchanged_target",
            "rebase_changed_diff": False,
            "rebase_attempted_source_head": "source-a",
            "rebase_attempted_target_head": "target-a",
            "rebase_target_contained": True,
            "rebase_provider_resolution_proof": False,
        },
        {
            "rebase_resolution_kind": "no_op",
            "rebase_no_op_subtype": "moot",
            "rebase_changed_diff": False,
            "rebase_attempted_source_head": "source-a",
            "rebase_attempted_target_head": "target-a",
            "rebase_target_contained": True,
            "rebase_provider_resolution_proof": False,
        },
    ),
)
def test_completed_rebase_outcomes_require_durable_outcome_id_without_judge(
    overrides: dict[str, Any],
) -> None:
    calls: list[str] = []

    decision = evaluate_landing_policy(
        policy="guarded",
        facts=_green_facts(rebase_status="completed", rebase_outcome_id=None, **overrides),
        judge=_recording_judge(calls, "LAND"),
    )

    assert decision.allowed is False
    assert decision.blocked is not None
    assert decision.blocked.reason_code == "rebase-or-conflict"
    assert calls == []


def test_rebase_refusal_evidence_includes_present_outcome_id() -> None:
    decision = evaluate_landing_policy(
        policy="guarded",
        facts=_green_facts(
            rebase_status="completed",
            rebase_resolution_kind="mechanical",
            rebase_changed_diff=False,
            rebase_outcome_id="rebase-evidence",
            rebase_attempted_source_head="source-a",
            rebase_attempted_target_head="old-target",
            rebase_target_contained=True,
        ),
    )

    assert decision.allowed is False
    assert decision.blocked is not None
    assert decision.blocked.reason_code == "rebase-or-conflict"
    assert "rebase-evidence" in decision.blocked.evidence_refs


@pytest.mark.parametrize(
    "facts",
    (
        _green_facts(
            rebase_status="completed",
            rebase_resolution_kind="no_op",
            rebase_no_op_subtype=None,
            rebase_attempted_target_head="target-a",
            rebase_target_contained=True,
            rebase_provider_resolution_proof=False,
        ),
        _green_facts(
            rebase_status="completed",
            rebase_resolution_kind="no_op",
            rebase_no_op_subtype="already_contained",
            rebase_changed_diff=False,
            rebase_attempted_source_head="other-source",
            rebase_attempted_target_head="target-a",
            rebase_target_contained=True,
            rebase_provider_resolution_proof=False,
        ),
        _green_facts(
            rebase_status="completed",
            rebase_resolution_kind="no_op",
            rebase_no_op_subtype="moot",
            rebase_changed_diff=False,
            rebase_attempted_source_head="source-a",
            rebase_attempted_target_head="old-target",
            rebase_target_contained=True,
            rebase_provider_resolution_proof=False,
        ),
        _green_facts(
            rebase_status="completed",
            rebase_resolution_kind="no_op",
            rebase_no_op_subtype="moot",
            rebase_changed_diff=False,
            rebase_attempted_source_head="source-a",
            rebase_attempted_target_head="target-a",
            rebase_target_contained=False,
            rebase_provider_resolution_proof=False,
        ),
        _green_facts(
            rebase_status="completed",
            rebase_resolution_kind="no_op",
            rebase_no_op_subtype="moot",
            rebase_changed_diff=False,
            rebase_attempted_source_head="source-a",
            rebase_attempted_target_head="target-a",
            rebase_target_contained=True,
            rebase_provider_resolution_proof=True,
        ),
    ),
)
def test_malformed_or_mismatched_no_op_rebase_proof_fails_closed(
    facts: LandingPolicyFacts,
) -> None:
    decision = evaluate_landing_policy(policy="guarded", facts=facts)

    assert decision.allowed is False
    assert decision.blocked is not None
    assert decision.blocked.reason_code == "rebase-or-conflict"
    assert decision.blocked.evidence_refs


@pytest.mark.parametrize("resolution_kind", ("mechanical", "provider_resolved"))
@pytest.mark.parametrize(
    "overrides",
    (
        {"rebase_attempted_source_head": None},
        {"rebase_attempted_target_head": None},
        {"rebase_attempted_target_head": "old-target"},
        {"rebase_target_contained": None},
        {"rebase_target_contained": False},
    ),
)
def test_completed_rebase_outcomes_require_exact_attempt_identity_and_target_containment(
    resolution_kind: str,
    overrides: dict[str, Any],
) -> None:
    values: dict[str, Any] = {
        "rebase_status": "completed",
        "rebase_resolution_kind": resolution_kind,
        "rebase_changed_diff": False,
        "rebase_outcome_id": "rebase-exact",
        "rebase_attempted_source_head": "source-a",
        "rebase_attempted_target_head": "target-a",
        "rebase_target_contained": True,
    }
    if resolution_kind == "provider_resolved":
        values["rebase_provider_resolution_proof"] = True
    values.update(overrides)

    decision = evaluate_landing_policy(policy="guarded", facts=_green_facts(**values))

    assert decision.allowed is False
    assert decision.blocked is not None
    assert decision.blocked.reason_code == "rebase-or-conflict"
    assert decision.blocked.evidence_refs


@pytest.mark.parametrize("provider_proof", (None, False))
def test_provider_resolved_rebase_requires_affirmative_provider_resolution_proof(
    provider_proof: bool | None,
) -> None:
    decision = evaluate_landing_policy(
        policy="guarded",
        facts=_green_facts(
            rebase_status="completed",
            rebase_resolution_kind="provider_resolved",
            rebase_changed_diff=False,
            rebase_outcome_id="rebase-provider",
            rebase_attempted_source_head="source-a",
            rebase_attempted_target_head="target-a",
            rebase_target_contained=True,
            rebase_provider_resolution_proof=provider_proof,
        ),
    )

    assert decision.allowed is False
    assert decision.blocked is not None
    assert decision.blocked.reason_code == "rebase-or-conflict"


def test_mechanical_rebase_rejects_provider_resolution_proof() -> None:
    decision = evaluate_landing_policy(
        policy="guarded",
        facts=_green_facts(
            rebase_status="completed",
            rebase_resolution_kind="mechanical",
            rebase_changed_diff=False,
            rebase_outcome_id="rebase-mechanical",
            rebase_attempted_source_head="source-a",
            rebase_attempted_target_head="target-a",
            rebase_target_contained=True,
            rebase_provider_resolution_proof=True,
        ),
    )

    assert decision.allowed is False
    assert decision.blocked is not None
    assert decision.blocked.reason_code == "rebase-or-conflict"


def test_landing_state_fingerprint_includes_rebase_outcome_and_canonicalizes_sets() -> None:
    facts_a = _green_facts(
        rebase_status="completed",
        rebase_resolution_kind="mechanical",
        rebase_changed_diff=False,
        rebase_outcome_id="rebase-a",
        rebase_attempted_source_head="source-a",
        rebase_attempted_target_head="target-a",
        rebase_target_contained=True,
        open_blockers=(
            _blocker("B2", deferrable=True, fingerprint="blocker-b"),
            _blocker("B1", deferrable=True, fingerprint="blocker-a"),
        ),
    )
    facts_b = _green_facts(
        rebase_status="completed",
        rebase_resolution_kind="provider_resolved",
        rebase_changed_diff=False,
        rebase_outcome_id="rebase-b",
        rebase_attempted_source_head="source-a",
        rebase_attempted_target_head="target-a",
        rebase_target_contained=True,
        rebase_provider_resolution_proof=True,
        open_blockers=(
            _blocker("B1", deferrable=True, fingerprint="blocker-a"),
            _blocker("B2", deferrable=True, fingerprint="blocker-b"),
        ),
    )

    fingerprint_a = LandingStateFingerprint.from_facts(
        facts_a,
        policy_judgment_identity="judge-key-a",
        adjudication_fingerprints=("adjudication-b", "adjudication-a"),
    )
    fingerprint_b = LandingStateFingerprint.from_facts(
        facts_b,
        policy_judgment_identity="judge-key-a",
        adjudication_fingerprints=("adjudication-a", "adjudication-b"),
    )
    fingerprint_a_reordered = LandingStateFingerprint.from_facts(
        facts_a,
        policy_judgment_identity="judge-key-a",
        adjudication_fingerprints=("adjudication-a", "adjudication-b"),
    )

    assert fingerprint_a.rebase.resolution_kind == "mechanical"
    assert fingerprint_b.rebase.resolution_kind == "provider_resolved"
    assert fingerprint_a != fingerprint_b
    assert fingerprint_a == fingerprint_a_reordered
    assert fingerprint_a.blocker_fingerprints == ("blocker-a", "blocker-b")
    assert fingerprint_a.adjudication_fingerprints == ("adjudication-a", "adjudication-b")


def test_landing_state_fingerprint_differs_when_only_rebase_outcome_id_changes() -> None:
    facts_a = _green_facts(
        rebase_status="completed",
        rebase_resolution_kind="mechanical",
        rebase_changed_diff=False,
        rebase_outcome_id="rebase-a",
        rebase_attempted_source_head="source-a",
        rebase_attempted_target_head="target-a",
        rebase_target_contained=True,
    )
    facts_b = _green_facts(
        rebase_status="completed",
        rebase_resolution_kind="mechanical",
        rebase_changed_diff=False,
        rebase_outcome_id="rebase-b",
        rebase_attempted_source_head="source-a",
        rebase_attempted_target_head="target-a",
        rebase_target_contained=True,
    )

    assert LandingStateFingerprint.from_facts(facts_a) != LandingStateFingerprint.from_facts(facts_b)


def test_landing_state_fingerprint_changes_when_same_finding_id_has_new_blocker_content() -> None:
    facts_a = _green_facts(
        open_blockers=(
            _blocker(
                "B1",
                deferrable=True,
                blocker_class="out_of_scope",
                source="artifact:review-a",
                fingerprint="normalized:old-evidence",
            ),
        ),
    )
    facts_b = _green_facts(
        open_blockers=(
            _blocker(
                "B1",
                deferrable=True,
                blocker_class="out_of_scope",
                source="artifact:review-a",
                fingerprint="normalized:new-evidence",
            ),
        ),
    )

    fingerprint_a = LandingStateFingerprint.from_facts(facts_a)
    fingerprint_b = LandingStateFingerprint.from_facts(facts_b)

    assert fingerprint_a != fingerprint_b
    assert fingerprint_a.blocker_fingerprints == ("normalized:old-evidence",)
    assert fingerprint_b.blocker_fingerprints == ("normalized:new-evidence",)


def test_landing_state_fingerprint_includes_followup_finding_identities() -> None:
    facts_a = _green_facts(
        review=_review(
            verdict="APPROVED_WITH_FOLLOWUPS",
            followup_findings=(
                LandingFollowupFinding("F2", fingerprint="followup:b", source="review:gza-200"),
                LandingFollowupFinding("F1", fingerprint="followup:a", source="review:gza-200"),
            ),
        )
    )
    facts_b = _green_facts(
        review=_review(
            verdict="APPROVED_WITH_FOLLOWUPS",
            followup_findings=(
                LandingFollowupFinding("F1", fingerprint="followup:a", source="review:gza-200"),
                LandingFollowupFinding("F2", fingerprint="followup:b", source="review:gza-200"),
            ),
        )
    )
    facts_c = _green_facts(
        review=_review(
            verdict="APPROVED_WITH_FOLLOWUPS",
            followup_findings=(
                LandingFollowupFinding("F1", fingerprint="followup:a", source="review:gza-200"),
                LandingFollowupFinding("F3", fingerprint="followup:c", source="review:gza-200"),
            ),
        )
    )
    facts_d = _green_facts(
        review=_review(
            verdict="APPROVED_WITH_FOLLOWUPS",
            followup_findings=(
                LandingFollowupFinding("F1", fingerprint="followup:a", source="source:a"),
                LandingFollowupFinding("F2", fingerprint="followup:b", source="review:gza-200"),
            ),
        )
    )
    facts_e = _green_facts(
        review=_review(
            verdict="APPROVED_WITH_FOLLOWUPS",
            followup_findings=(
                LandingFollowupFinding("F1", fingerprint="followup:z", source="review:gza-200"),
                LandingFollowupFinding("F2", fingerprint="followup:b", source="review:gza-200"),
            ),
        )
    )

    fingerprint_a = LandingStateFingerprint.from_facts(facts_a)
    fingerprint_b = LandingStateFingerprint.from_facts(facts_b)
    fingerprint_c = LandingStateFingerprint.from_facts(facts_c)
    fingerprint_d = LandingStateFingerprint.from_facts(facts_d)
    fingerprint_e = LandingStateFingerprint.from_facts(facts_e)

    assert fingerprint_a == fingerprint_b
    assert fingerprint_a.review.followup_fingerprints == (
        '{"content":"followup:a","finding":"F1","review":"gza-200","source":"review:gza-200"}',
        '{"content":"followup:b","finding":"F2","review":"gza-200","source":"review:gza-200"}',
    )
    assert fingerprint_a != fingerprint_c
    assert fingerprint_a != fingerprint_d
    assert fingerprint_a != fingerprint_e


@pytest.mark.parametrize(
    ("field", "replacement"),
    (
        ("review_id", "gza-201"),
        ("source", "review:gza-201"),
        ("finding_id", "F2"),
        ("fingerprint", "followup:f2"),
    ),
)
def test_followup_identity_changes_materialization_inputs_and_landing_fingerprint(
    field: str,
    replacement: str,
) -> None:
    base_review = {
        "review_id": "gza-200",
        "verdict": "APPROVED_WITH_FOLLOWUPS",
        "followup_findings": (
            LandingFollowupFinding("F1", fingerprint="followup:f1", source="review:gza-200"),
        ),
    }
    changed_review = dict(base_review)
    if field == "review_id":
        changed_review["review_id"] = replacement
    else:
        changed_review["followup_findings"] = (
            LandingFollowupFinding(
                replacement if field == "finding_id" else "F1",
                fingerprint=replacement if field == "fingerprint" else "followup:f1",
                source=replacement if field == "source" else "review:gza-200",
            ),
        )
    facts_a = _green_facts(review=_review(**base_review))
    facts_b = _green_facts(review=_review(**changed_review))
    decision_a = evaluate_landing_policy(policy="guarded", facts=facts_a)
    decision_b = evaluate_landing_policy(policy="guarded", facts=facts_b)

    assert decision_a.allowed is True
    assert decision_b.allowed is True
    assert decision_a.followup_materialization_identities != decision_b.followup_materialization_identities
    assert LandingStateFingerprint.from_facts(facts_a) != LandingStateFingerprint.from_facts(facts_b)


def test_delimiter_bearing_followup_identities_do_not_collide() -> None:
    identity_a = LandingFollowupMaterializationIdentity(
        review_id="a|source=b",
        source="c",
        finding_id="d",
        fingerprint="e",
    )
    identity_b = LandingFollowupMaterializationIdentity(
        review_id="a",
        source="source=b|finding=c",
        finding_id="d",
        fingerprint="e",
    )

    assert identity_a.fingerprint_key != identity_b.fingerprint_key


@pytest.mark.parametrize(
    ("field", "replacement"),
    (
        ("evidence_id", "spec-b"),
        ("status", "failed"),
        ("verdict", "CHANGES_REQUESTED"),
        ("reviewed_head", "source-b"),
        ("changed_paths_fingerprint", "paths-b"),
    ),
)
def test_landing_state_fingerprint_differs_for_each_spec_coherence_identity_field(
    field: str,
    replacement: Any,
) -> None:
    base_spec = {
        "required": True,
        "status": "completed",
        "verdict": "APPROVED",
        "current": True,
        "identity_matched": True,
        "evidence_id": "spec-a",
        "reviewed_head": "source-a",
        "changed_paths_fingerprint": "paths-a",
    }
    changed_spec = {**base_spec, field: replacement}
    facts_a = _green_facts(spec_coherence=LandingSpecCoherenceEvidence(**base_spec))
    facts_b = _green_facts(spec_coherence=LandingSpecCoherenceEvidence(**changed_spec))

    assert LandingStateFingerprint.from_facts(facts_a) != LandingStateFingerprint.from_facts(facts_b)


def test_landing_state_fingerprint_preserves_spec_coherence_facts_and_canonicalizes_sets() -> None:
    facts = _green_facts(
        spec_coherence=LandingSpecCoherenceEvidence(
            required=True,
            status="completed",
            verdict="APPROVED",
            current=True,
            identity_matched=True,
            evidence_id="spec-a",
            reviewed_head="source-a",
            changed_paths_fingerprint="paths-a",
        )
    )

    fingerprint = LandingStateFingerprint.from_facts(
        facts,
        adjudication_fingerprints=("adjudication-b", "adjudication-a"),
    )

    assert fingerprint.spec_coherence == LandingSpecCoherenceFingerprint(
        task_or_artifact_id="spec-a",
        status="completed",
        verdict="APPROVED",
        reviewed_head="source-a",
        changed_paths_fingerprint="paths-a",
    )
    assert fingerprint.adjudication_fingerprints == ("adjudication-a", "adjudication-b")


def test_landing_state_fingerprint_rejects_empty_supplied_fingerprints_that_discard_facts() -> None:
    facts = _green_facts(
        rebase_status="completed",
        rebase_resolution_kind="mechanical",
        rebase_changed_diff=False,
        rebase_outcome_id="rebase-a",
        rebase_attempted_source_head="source-a",
        rebase_attempted_target_head="target-a",
        rebase_target_contained=True,
        spec_coherence=LandingSpecCoherenceEvidence(
            required=True,
            status="completed",
            verdict="APPROVED",
            current=True,
            identity_matched=True,
            evidence_id="spec-a",
            reviewed_head="source-a",
            changed_paths_fingerprint="paths-a",
        ),
    )

    with pytest.raises(ValueError):
        LandingStateFingerprint.from_facts(facts, rebase=LandingRebaseFingerprint())
    with pytest.raises(ValueError):
        LandingStateFingerprint.from_facts(facts, spec_coherence=LandingSpecCoherenceFingerprint())


def test_landing_state_fingerprint_accepts_matching_supplied_fact_derived_fingerprints() -> None:
    facts = _green_facts(
        rebase_status="completed",
        rebase_resolution_kind="provider_resolved",
        rebase_changed_diff=True,
        rebase_outcome_id="rebase-a",
        rebase_attempted_source_head="source-a",
        rebase_attempted_target_head="target-a",
        rebase_target_contained=True,
        rebase_provider_resolution_proof=True,
        spec_coherence=LandingSpecCoherenceEvidence(
            required=True,
            status="completed",
            verdict="APPROVED",
            current=True,
            identity_matched=True,
            evidence_id="spec-a",
            reviewed_head="source-a",
            changed_paths_fingerprint="paths-a",
        ),
    )
    rebase = LandingRebaseFingerprint(
        outcome_id="rebase-a",
        status="completed",
        changed_diff=True,
        resolution_kind="provider_resolved",
        no_op_subtype=None,
        attempted_source_head="source-a",
        attempted_target_head="target-a",
        target_contained=True,
        provider_resolution_proof=True,
    )
    spec = LandingSpecCoherenceFingerprint(
        task_or_artifact_id="spec-a",
        status="completed",
        verdict="APPROVED",
        reviewed_head="source-a",
        changed_paths_fingerprint="paths-a",
    )

    fingerprint = LandingStateFingerprint.from_facts(facts, rebase=rebase, spec_coherence=spec)

    assert fingerprint.rebase == rebase
    assert fingerprint.spec_coherence == spec


@pytest.mark.parametrize(
    ("field", "replacement"),
    (
        ("outcome_id", "rebase-b"),
        ("status", "failed"),
        ("changed_diff", False),
        ("resolution_kind", "mechanical"),
        ("no_op_subtype", "moot"),
        ("attempted_source_head", "source-b"),
        ("attempted_target_head", "target-b"),
        ("target_contained", False),
        ("provider_resolution_proof", False),
    ),
)
def test_supplied_rebase_fingerprint_must_match_every_fact_identity_field(
    field: str,
    replacement: Any,
) -> None:
    facts = _green_facts(
        rebase_status="completed",
        rebase_resolution_kind="provider_resolved",
        rebase_changed_diff=True,
        rebase_outcome_id="rebase-a",
        rebase_attempted_source_head="source-a",
        rebase_attempted_target_head="target-a",
        rebase_target_contained=True,
        rebase_provider_resolution_proof=True,
    )
    values = {
        "outcome_id": "rebase-a",
        "status": "completed",
        "changed_diff": True,
        "resolution_kind": "provider_resolved",
        "no_op_subtype": None,
        "attempted_source_head": "source-a",
        "attempted_target_head": "target-a",
        "target_contained": True,
        "provider_resolution_proof": True,
    }
    values[field] = replacement

    with pytest.raises(ValueError):
        LandingStateFingerprint.from_facts(facts, rebase=LandingRebaseFingerprint(**values))


@pytest.mark.parametrize(
    ("field", "replacement"),
    (
        ("task_or_artifact_id", "spec-b"),
        ("status", "failed"),
        ("verdict", "CHANGES_REQUESTED"),
        ("reviewed_head", "source-b"),
        ("changed_paths_fingerprint", "paths-b"),
    ),
)
def test_supplied_spec_coherence_fingerprint_must_match_every_fact_identity_field(
    field: str,
    replacement: Any,
) -> None:
    facts = _green_facts(
        spec_coherence=LandingSpecCoherenceEvidence(
            required=True,
            status="completed",
            verdict="APPROVED",
            current=True,
            identity_matched=True,
            evidence_id="spec-a",
            reviewed_head="source-a",
            changed_paths_fingerprint="paths-a",
        ),
    )
    values = {
        "task_or_artifact_id": "spec-a",
        "status": "completed",
        "verdict": "APPROVED",
        "reviewed_head": "source-a",
        "changed_paths_fingerprint": "paths-a",
    }
    values[field] = replacement

    with pytest.raises(ValueError):
        LandingStateFingerprint.from_facts(
            facts,
            spec_coherence=LandingSpecCoherenceFingerprint(**values),
        )


def test_supplied_fingerprints_remain_supported_when_facts_have_no_identity() -> None:
    rebase = LandingRebaseFingerprint(outcome_id="external-rebase")
    spec = LandingSpecCoherenceFingerprint(task_or_artifact_id="external-spec")

    fingerprint = LandingStateFingerprint.from_facts(
        LandingPolicyFacts(task_id="gza-100"),
        rebase=rebase,
        spec_coherence=spec,
    )

    assert fingerprint.rebase == rebase
    assert fingerprint.spec_coherence == spec


@pytest.mark.parametrize(
    "facts",
    (
        _green_facts(source_head=None),
        _green_facts(checkout_clean=False),
        _green_facts(clean_merge=False),
        _green_facts(verify=_verify(status="failed")),
        _green_facts(review=_review(status="failed")),
        _green_facts(
            review=_review(verdict="CHANGES_REQUESTED"),
            open_blockers=(_blocker("B1", deferrable=False, blocker_class="correctness"),),
        ),
        _green_facts(
            review=_review(verdict="CHANGES_REQUESTED"),
            open_blockers=(_blocker("B1", deferrable=True, blocker_class="out_of_scope"),),
            guarded_judgment_enabled=False,
        ),
        _green_facts(parked_reason="improve-no-op", review=_review(required=False)),
    ),
)
def test_policy_refusals_have_non_empty_durable_evidence(facts: LandingPolicyFacts) -> None:
    decision = evaluate_landing_policy(policy="guarded", facts=facts)

    assert decision.allowed is False
    assert decision.blocked is not None
    assert decision.blocked.evidence_refs


@pytest.mark.parametrize("status", ("failed", "unavailable", "stale", "malformed", "missing"))
def test_post_merge_verify_failure_renders_truthful_merged_non_success(status: str) -> None:
    failure = LandPostMergeVerifyFailure(
        status=status,  # type: ignore[arg-type]
        fact=f"post-merge checkpoint is {status}",
        checkpoint_id="checkpoint-1",
        target_head="target-after",
        gate_identity="main-verify",
    )
    result = LandResult(
        request=LandRequest(task_id="gza-100"),
        owner_task_id="gza-100",
        target_branch="main",
        source_ref="feature/example",
        merged=True,
        merge_provenance="manual_land",
        post_merge_verify_failure=failure,
    )

    assert result.merged is True
    assert result.blocked is None
    sentence = failure.terminal_sentence("gza-100")
    assert sentence == f"Merged gza-100, but integration verification failed: post-merge checkpoint is {status}."
    assert "Cannot land" not in sentence
    assert failure.evidence_refs


@pytest.mark.parametrize("status", ("failed", "unavailable", "stale", "malformed", "missing"))
def test_already_merged_post_merge_verify_failure_needs_no_new_provenance(
    status: str,
) -> None:
    failure = LandPostMergeVerifyFailure(
        status=status,  # type: ignore[arg-type]
        fact=f"post-merge checkpoint is {status}",
        checkpoint_id="checkpoint-1",
        target_head="target-after",
        gate_identity="main-verify",
    )
    result = LandResult(
        request=LandRequest(task_id="gza-100"),
        owner_task_id="gza-100",
        target_branch="main",
        source_ref="feature/example",
        already_merged=True,
        post_merge_verify_failure=failure,
    )

    assert result.already_merged is True
    assert result.merged is False
    assert result.merge_provenance is None
    assert failure.terminal_sentence("gza-100") == (
        f"Merged gza-100, but integration verification failed: post-merge checkpoint is {status}."
    )


def test_landblocked_rejects_empty_evidence_refs() -> None:
    with pytest.raises(ValueError):
        LandBlocked("identity-proof-unavailable", "source proof is unavailable")
    with pytest.raises(ValueError):
        LandBlocked("identity-proof-unavailable", "source proof is unavailable", (" ",))
    with pytest.raises(ValueError):
        LandPostMergeVerifyFailure(status="missing", fact="missing", evidence_refs=("checkpoint-1", "\t"))
    with pytest.raises(ValueError):
        LandStep("resolve", "blocked", "blocked", evidence_refs=("gza-100", ""))


def test_open_blockers_require_durable_provenance_and_normalized_fingerprint() -> None:
    with pytest.raises(ValueError):
        LandingOpenBlocker("", deferrable=True, source="review:gza-200", fingerprint="blocker")
    with pytest.raises(ValueError):
        LandingOpenBlocker("B1", deferrable=True, source=" ", fingerprint="blocker")
    with pytest.raises(ValueError):
        LandingOpenBlocker("B1", deferrable=True, source="review:gza-200", fingerprint="\n")

    blocker = LandingOpenBlocker(" B1 ", deferrable=True, source=" review:gza-200 ", fingerprint=" blocker:a ")

    assert blocker.finding_id == "B1"
    assert blocker.source == "review:gza-200"
    assert blocker.fingerprint == "blocker:a"


def test_landing_policy_decision_rejects_contradictory_direct_construction() -> None:
    blocked = LandBlocked("policy-or-judge-refused", "judge refused", ("judge-1",))

    with pytest.raises(ValueError):
        LandingPolicyDecision(True, blocked=blocked)
    with pytest.raises(ValueError):
        LandingPolicyDecision(False)
    with pytest.raises(ValueError):
        LandingPolicyDecision(
            False,
            blocked=blocked,
            allowed_overrides=("defer-review-blockers",),
        )
    with pytest.raises(ValueError):
        LandingPolicyDecision(True, allowed_overrides=("defer-review-blockers",))
    with pytest.raises(ValueError):
        LandingPolicyDecision(
            True,
            allowed_overrides=("defer-review-blockers",),
            judgment_verdict="BLOCK",
            judgment_artifact_id="judge-artifact",
            judgment_key="judge-key",
        )
    with pytest.raises(ValueError):
        LandingPolicyDecision(
            True,
            allowed_overrides=("defer-review-blockers",),
            judgment_verdict="NEEDS_HUMAN",
            judgment_artifact_id="judge-artifact",
            judgment_key="judge-key",
        )
    with pytest.raises(ValueError):
        LandingPolicyDecision(
            True,
            allowed_overrides=("defer-review-blockers",),
            judgment_verdict="LAND",
            judgment_artifact_id="judge-artifact",
        )

    accepted = LandingPolicyDecision(
        True,
        allowed_overrides=("defer-review-blockers",),
        judgment_verdict="LAND",
        judgment_artifact_id=" judge-artifact ",
        judgment_key=" judge-key ",
    )

    assert accepted.judgment_artifact_id == "judge-artifact"
    assert accepted.judgment_key == "judge-key"


def test_land_result_requires_new_merge_provenance_but_allows_already_merged() -> None:
    already = LandResult(
        request=LandRequest(task_id="gza-100"),
        owner_task_id="gza-100",
        target_branch="main",
        source_ref="feature/example",
        already_merged=True,
    )

    assert already.already_merged is True

    with pytest.raises(ValueError):
        LandResult(
            request=LandRequest(task_id="gza-100"),
            owner_task_id="gza-100",
            target_branch="main",
            source_ref="feature/example",
            merged=True,
        )


def test_land_result_requires_escalated_provenance_for_deferred_blocker_ids() -> None:
    with pytest.raises(ValueError):
        LandResult(
            request=LandRequest(task_id="gza-100"),
            owner_task_id="gza-100",
            target_branch="main",
            source_ref="feature/example",
            merged=True,
            merge_provenance="manual_land",
            deferred_task_ids=("gza-300",),
        )

    escalated = LandResult(
        request=LandRequest(task_id="gza-100"),
        owner_task_id="gza-100",
        target_branch="main",
        source_ref="feature/example",
        merged=True,
        merge_provenance="manual_land_escalated",
        judgment_artifact_id="judge-artifact",
        judgment_key="judge-key",
        deferred_task_ids=("gza-300",),
    )
    with pytest.raises(ValueError):
        LandResult(
            request=LandRequest(task_id="gza-101"),
            owner_task_id="gza-101",
            target_branch="main",
            source_ref="feature/park-only",
            merged=True,
            merge_provenance="manual_land_escalated",
        )
    with pytest.raises(ValueError):
        LandResult(
            request=LandRequest(task_id="gza-101"),
            owner_task_id="gza-101",
            target_branch="main",
            source_ref="feature/park-only",
            merged=True,
            merge_provenance="manual_land_escalated",
            judgment_artifact_id="judge-artifact",
        )

    assert escalated.deferred_task_ids == ("gza-300",)
    assert escalated.judgment_artifact_id == "judge-artifact"
    assert escalated.judgment_key == "judge-key"


def test_land_result_rejects_contradictory_terminal_state_combinations() -> None:
    blocked = evaluate_landing_policy(policy="guarded", facts=_green_facts(checkout_clean=False)).blocked
    assert blocked is not None
    failure = LandPostMergeVerifyFailure(
        status="missing",
        fact="post-merge checkpoint is missing",
        checkpoint_id="checkpoint-1",
    )

    with pytest.raises(ValueError):
        LandResult(
            request=LandRequest(task_id="gza-100"),
            owner_task_id="gza-100",
            target_branch="main",
            source_ref="feature/example",
            merged=True,
            already_merged=True,
        )
    with pytest.raises(ValueError):
        LandResult(
            request=LandRequest(task_id="gza-100"),
            owner_task_id="gza-100",
            target_branch="main",
            source_ref="feature/example",
            merged=True,
            blocked=blocked,
        )
    with pytest.raises(ValueError):
        LandResult(
            request=LandRequest(task_id="gza-100"),
            owner_task_id="gza-100",
            target_branch="main",
            source_ref="feature/example",
            already_merged=True,
            merge_provenance="manual_land",
        )
    with pytest.raises(ValueError):
        LandResult(
            request=LandRequest(task_id="gza-100"),
            owner_task_id="gza-100",
            target_branch="main",
            source_ref="feature/example",
            post_merge_verify_failure=failure,
        )
    with pytest.raises(ValueError):
        LandResult(
            request=LandRequest(task_id="gza-100"),
            owner_task_id="gza-100",
            target_branch="main",
            source_ref="feature/example",
            already_merged=True,
            merge_provenance="manual_land",
            post_merge_verify_failure=failure,
        )
    with pytest.raises(ValueError):
        LandResult(
            request=LandRequest(task_id="gza-100"),
            owner_task_id="gza-100",
            target_branch="main",
            source_ref="feature/example",
            already_merged=True,
            blocked=blocked,
            post_merge_verify_failure=failure,
        )
    with pytest.raises(ValueError):
        LandResult(
            request=LandRequest(task_id="gza-100"),
            owner_task_id="gza-100",
            target_branch="main",
            source_ref="feature/example",
            blocked=blocked,
            deferred_task_ids=("gza-300",),
        )
    with pytest.raises(ValueError):
        LandResult(
            request=LandRequest(task_id="gza-100"),
            owner_task_id="gza-100",
            target_branch="main",
            source_ref="feature/example",
            blocked=blocked,
            followup_task_ids=("gza-301",),
        )


def test_land_result_carries_typed_blocking_fact_and_terminal_sentence() -> None:
    decision = evaluate_landing_policy(policy="guarded", facts=_green_facts(checkout_clean=False))
    assert decision.blocked is not None
    result = LandResult(
        request=LandRequest(task_id="gza-100", policy="guarded"),
        owner_task_id="gza-100",
        target_branch="main",
        source_ref="feature/example",
        blocked=decision.blocked,
    )

    assert result.blocked is not None
    assert result.blocked.reason_code == "dirty-checkout"
    assert result.blocked.evidence_refs
    assert result.blocked.terminal_sentence("gza-100") == "Cannot land gza-100: tracked checkout is not clean."


class _FakeGit:
    def __init__(self, heads: dict[str, str]) -> None:
        self.heads = heads

    def rev_parse_if_exists(self, ref: str) -> str | None:
        return self.heads.get(ref)


def _verify_config(tmp_path) -> Config:
    config = Config(project_dir=tmp_path, project_name="test-project")
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0
    return config


def _verify_result(
    *,
    status: str = "passed",
    head: str = "head-a",
    tree_fingerprint: str | None = TREE_A,
) -> SimpleNamespace:
    output = "verify output\n"
    if tree_fingerprint is not None:
        output += f"gza-verify phase=passed name=unit duration_seconds=1.0 tree_fingerprint={tree_fingerprint}\n"
    return SimpleNamespace(
        command="./bin/tests",
        status=status,
        exit_status="0" if status == "passed" else "1",
        captured_at=datetime(2026, 8, 26, 12, 0, tzinfo=UTC),
        reviewed_branch="feature/landing",
        reviewed_head_sha=head,
        reviewed_base_sha="base-a",
        working_directory="/tmp/worktree",
        failure=None if status == "passed" else "failed",
        output=output,
    )


def _decision(state: str, *, head: str = "head-a") -> VerifyGateDecision:
    epoch = make_verify_epoch(
        reviewed_branch="feature/landing",
        reviewed_head_sha=head,
        verify_command="./bin/tests",
        verify_timeout_seconds=120,
        verify_timeout_grace_seconds=5.0,
    )
    result = None if state in {"missing", "stale"} else _verify_result(status=state, head=head)
    return VerifyGateDecision(
        owner_task_id="gza-1",
        current_epoch=epoch,
        lookup=VerifyGateLookup(
            result=result,
            source="owner_artifact" if result is not None else None,
            is_current=state != "stale" and result is not None,
            has_owner_artifact=result is not None,
            artifact_metadata={"tree_fingerprint": TREE_A} if result is not None else None,
        ),
        state=state,  # type: ignore[arg-type]
    )


def test_inspect_current_landing_verify_requires_canonical_owner_artifact_not_rebase_verify(tmp_path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    config = _verify_config(tmp_path)
    impl = store.add("Implement landing verify", task_type="implement")
    impl.status = "completed"
    impl.branch = "feature/landing"
    store.update(impl)
    rebase = store.add("Rebase provider verify", task_type="rebase", based_on=impl.id, same_branch=True)
    rebase.review_verify_status = "passed"
    rebase.review_verify_command = "./bin/tests"
    rebase.review_verify_exit_status = "0"
    rebase.review_verify_captured_at = datetime(2026, 8, 26, 12, 0, tzinfo=UTC)
    rebase.review_verify_branch = "feature/landing"
    rebase.review_verify_head_sha = "head-a"
    store.update(rebase)

    evidence = inspect_current_landing_verify_evidence(
        store,
        impl,
        config=config,
        git=_FakeGit({"feature/landing": "head-a"}),
        source_head="head-a",
    )

    assert evidence.status == "missing"
    assert evidence.current is False
    assert evidence.identity_matched is False


def _persist_lifecycle_verify_for_landing(
    store: SqliteTaskStore,
    config: Config,
    impl,
    *,
    aggregate_tree: str | None = TREE_A,
    project_trees: tuple[str | None, ...] = (),
    consumed_verify_fix_task=None,
) -> None:
    aggregate = ReviewVerifyResult(
        command="./bin/tests",
        status="passed",
        exit_status="0",
        captured_at=datetime(2026, 8, 26, 12, 0, tzinfo=UTC),
        reviewed_branch="feature/landing",
        reviewed_head_sha="head-a",
        reviewed_base_sha="base-a",
        working_directory="/tmp/worktree",
        failure=None,
        output=(
            "verify output\n"
            f"gza-verify phase=passed name=unit duration_seconds=1.0 tree_fingerprint={aggregate_tree}\n"
            if aggregate_tree is not None
            else "verify output\n"
        ),
    )
    project_results = tuple(
        ProjectVerificationResult(
            project=None,
            scope=f"project-{index}",
            working_directory=f"/tmp/worktree/project-{index}",
            result=ReviewVerifyResult(
                command=f"./bin/tests project-{index}",
                status="passed",
                exit_status="0",
                captured_at=datetime(2026, 8, 26, 12, index, tzinfo=UTC),
                reviewed_branch="feature/landing",
                reviewed_head_sha="head-a",
                reviewed_base_sha="base-a",
                working_directory=f"/tmp/worktree/project-{index}",
                failure=None,
                output=(
                    "verify output\n"
                    f"gza-verify phase=passed name=unit duration_seconds=1.0 tree_fingerprint={tree}\n"
                    if tree is not None
                    else "verify output\n"
                ),
            ),
        )
        for index, tree in enumerate(project_trees)
    )
    _persist_lifecycle_verify_execution(
        config,
        store,
        impl,
        LifecycleVerifyExecution(
            markdown="verify passed",
            aggregate_result=aggregate,
            project_results=project_results,
        ),
        producer="advance_verify_gate",
        timeout_seconds=120,
        timeout_grace_seconds=5.0,
        consumed_verify_fix_task=consumed_verify_fix_task,
        consumed_verify_fix_no_source_changes=False if consumed_verify_fix_task is not None else None,
        consumed_verify_fix_completion_head_sha="head-a" if consumed_verify_fix_task is not None else None,
    )


def test_inspect_current_landing_verify_accepts_production_single_project_exact_head_gate_and_tree(tmp_path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    config = _verify_config(tmp_path)
    impl = store.add("Implement landing verify", task_type="implement")
    impl.status = "completed"
    impl.branch = "feature/landing"
    store.update(impl)
    _persist_lifecycle_verify_for_landing(store, config, impl)
    gate = '{"command":"./bin/tests","grace":5.0,"timeout":120}'

    evidence = inspect_current_landing_verify_evidence(
        store,
        impl,
        config=config,
        git=_FakeGit({"feature/landing": "head-a"}),
        source_head="head-a",
        gate_identity=gate,
        tree_fingerprint=TREE_A,
    )

    assert evidence.status == "passed"
    assert evidence.current is True
    assert evidence.identity_matched is True
    assert evidence.gate_identity == gate
    assert evidence.tree_fingerprint == TREE_A


def test_inspect_current_landing_verify_accepts_production_cross_project_aggregate_tree(tmp_path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    config = _verify_config(tmp_path)
    impl = store.add("Implement landing verify", task_type="implement")
    impl.status = "completed"
    impl.branch = "feature/landing"
    store.update(impl)
    _persist_lifecycle_verify_for_landing(store, config, impl, aggregate_tree=None, project_trees=(TREE_A, TREE_A))

    evidence = inspect_current_landing_verify_evidence(
        store,
        impl,
        config=config,
        git=_FakeGit({"feature/landing": "head-a"}),
        source_head="head-a",
        tree_fingerprint=TREE_A,
    )

    assert evidence.status == "passed"
    assert evidence.current is True
    assert evidence.identity_matched is True
    assert evidence.tree_fingerprint == TREE_A


@pytest.mark.parametrize("project_trees", ((TREE_A, None), (None, TREE_A), (None, None)))
def test_inspect_current_landing_verify_rejects_incomplete_cross_project_aggregate_tree(
    tmp_path,
    project_trees: tuple[str | None, ...],
) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    config = _verify_config(tmp_path)
    impl = store.add("Implement landing verify", task_type="implement")
    impl.status = "completed"
    impl.branch = "feature/landing"
    store.update(impl)
    _persist_lifecycle_verify_for_landing(store, config, impl, aggregate_tree=None, project_trees=project_trees)

    artifact = next(
        artifact
        for artifact in store.list_artifacts(impl.id)
        if artifact.metadata is not None and "aggregate_details" in artifact.metadata
    )
    assert artifact.metadata is not None
    aggregate_details = artifact.metadata["aggregate_details"]
    assert aggregate_details["runnable_count"] == len(project_trees)
    assert aggregate_details["tree_fingerprint"] is None
    assert aggregate_details["tree_fingerprint_complete"] is False
    assert aggregate_details["tree_fingerprint_missing_count"] == project_trees.count(None)

    evidence = inspect_current_landing_verify_evidence(
        store,
        impl,
        config=config,
        git=_FakeGit({"feature/landing": "head-a"}),
        source_head="head-a",
        tree_fingerprint=TREE_A,
    )

    assert evidence.status == "passed"
    assert evidence.current is True
    assert evidence.identity_matched is False
    assert evidence.tree_fingerprint is None


def test_inspect_current_landing_verify_blocks_inconsistent_cross_project_tree_proof(tmp_path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    config = _verify_config(tmp_path)
    impl = store.add("Implement landing verify", task_type="implement")
    impl.status = "completed"
    impl.branch = "feature/landing"
    store.update(impl)
    _persist_lifecycle_verify_for_landing(store, config, impl, aggregate_tree=None, project_trees=(TREE_A, TREE_B))

    evidence = inspect_current_landing_verify_evidence(
        store,
        impl,
        config=config,
        git=_FakeGit({"feature/landing": "head-a"}),
        source_head="head-a",
        tree_fingerprint=TREE_A,
    )

    assert evidence.status == "passed"
    assert evidence.current is True
    assert evidence.identity_matched is False
    assert evidence.tree_fingerprint is None


def test_inspect_current_landing_verify_accepts_recredited_production_tree(tmp_path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    config = _verify_config(tmp_path)
    evidence_holder = store.add("Evidence holder", task_type="implement")
    evidence_holder.status = "completed"
    evidence_holder.branch = "feature/landing"
    store.update(evidence_holder)
    credited = store.add("Credited owner", task_type="implement")
    credited.status = "completed"
    credited.branch = "feature/landing"
    store.update(credited)
    _persist_lifecycle_verify_for_landing(store, config, evidence_holder)
    source = inspect_current_landing_verify_evidence(
        store,
        evidence_holder,
        config=config,
        git=_FakeGit({"feature/landing": "head-a"}),
        source_head="head-a",
    )
    assert source.status == "passed"

    latest = store.list_artifacts(evidence_holder.id, kind="verify_gate_result")[0]
    persist_recredited_verify_gate_artifact(
        store,
        config,
        owner_task=credited,
        evidence_holder_task=evidence_holder,
        result=VerifyGateResult(
            command="./bin/tests",
            status="passed",
            exit_status="0",
            captured_at=datetime(2026, 8, 26, 12, 0, tzinfo=UTC),
            reviewed_branch="feature/landing",
            reviewed_head_sha="head-a",
            reviewed_base_sha="base-a",
            working_directory="/tmp/worktree",
            failure=None,
        ),
        source_metadata=latest.metadata,
        producer="advance_verify_gate_recredit",
    )

    evidence = inspect_current_landing_verify_evidence(
        store,
        credited,
        config=config,
        git=_FakeGit({"feature/landing": "head-a"}),
        source_head="head-a",
        tree_fingerprint=TREE_A,
    )

    assert evidence.status == "passed"
    assert evidence.current is True
    assert evidence.identity_matched is True
    assert evidence.tree_fingerprint == TREE_A


@pytest.mark.parametrize("expected_tree", (TREE_B, None))
def test_inspect_current_landing_verify_blocks_mismatched_or_absent_tree_proof(tmp_path, expected_tree: str | None) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    config = _verify_config(tmp_path)
    impl = store.add("Implement landing verify", task_type="implement")
    impl.status = "completed"
    impl.branch = "feature/landing"
    store.update(impl)
    _persist_lifecycle_verify_for_landing(store, config, impl, aggregate_tree=expected_tree)

    evidence = inspect_current_landing_verify_evidence(
        store,
        impl,
        config=config,
        git=_FakeGit({"feature/landing": "head-a"}),
        source_head="head-a",
        tree_fingerprint=TREE_A,
    )

    assert evidence.status == "passed"
    assert evidence.current is True
    assert evidence.identity_matched is False


def test_acquire_landing_verify_runs_shared_direct_action_then_reevaluates(monkeypatch, tmp_path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    config = _verify_config(tmp_path)
    impl = store.add("Implement landing verify", task_type="implement")
    impl.status = "completed"
    impl.branch = "feature/landing"
    store.update(impl)
    calls: list[str] = []
    decisions = [_decision("missing"), _decision("passed")]

    def fake_resolve(*_args: Any, **_kwargs: Any) -> VerifyGateDecision:
        return decisions.pop(0)

    def fake_plan(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        return {"type": "verify_gate", "description": "Run verify gate before merge"}

    def fake_execute(_task: Any, action: dict[str, Any], _context: Any) -> Any:
        calls.append(str(action["type"]))
        return SimpleNamespace(action_type="verify_gate", status="success")

    monkeypatch.setattr("gza.landing.resolve_verify_gate_decision", fake_resolve)
    monkeypatch.setattr("gza.landing.plan_manual_verify_gate_action", fake_plan)

    result = acquire_landing_verify_evidence(
        store,
        impl,
        config=config,
        git=_FakeGit({"feature/landing": "head-a"}),
        target_branch="main",
        source_head="head-a",
        context=SimpleNamespace(),  # type: ignore[arg-type]
        execute_action=fake_execute,  # type: ignore[arg-type]
    )

    assert result.status == "ran_verify"
    assert result.evidence.status == "passed"
    assert calls == ["verify_gate"]


def test_acquire_landing_verify_blocks_red_without_verify_fix_or_improve(monkeypatch, tmp_path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    config = _verify_config(tmp_path)
    impl = store.add("Implement landing verify", task_type="implement")
    impl.status = "completed"
    impl.branch = "feature/landing"
    store.update(impl)
    monkeypatch.setattr("gza.landing.resolve_verify_gate_decision", lambda *_args, **_kwargs: _decision("failed"))

    result = acquire_landing_verify_evidence(
        store,
        impl,
        config=config,
        git=_FakeGit({"feature/landing": "head-a"}),
        target_branch="main",
        source_head="head-a",
        context=SimpleNamespace(),  # type: ignore[arg-type]
        execute_action=lambda *_args: (_ for _ in ()).throw(AssertionError("must not execute")),  # type: ignore[arg-type]
    )

    assert result.status == "blocked"
    assert result.blocked is not None
    assert result.blocked.reason_code == "verify-unavailable-or-red"


def _completed_impl_for_landing_review(tmp_path):
    store = SqliteTaskStore(tmp_path / "test.db")
    impl = store.add("Implement landing review", task_type="implement")
    impl.status = "completed"
    impl.branch = "feature/landing"
    store.update(impl)
    return store, impl


def _rebase_identity(
    *,
    outcome_kind: str = "mechanical",
    attempted_source_head: str = "head-a",
    attempted_target_head: str = "target-a",
    live_source_head: str = "head-a",
    live_target_head: str = "target-a",
    target_contained: bool = True,
    provider_resolution_proof: bool = False,
    changed_diff: bool | None = False,
    no_op_subtype: str | None = None,
) -> LandingRebaseOutcomeIdentity:
    return LandingRebaseOutcomeIdentity(
        outcome_id=f"outcome-{outcome_kind}-{no_op_subtype or 'default'}",
        outcome_kind=outcome_kind,
        attempted_source_head=attempted_source_head,
        attempted_target_head=attempted_target_head,
        live_source_head=live_source_head,
        live_target_head=live_target_head,
        target_contained=target_contained,
        provider_resolution_proof=provider_resolution_proof,
        changed_diff=changed_diff,
        no_op_subtype=no_op_subtype,
    )


def _review_report(verdict: str) -> str:
    blockers = "None."
    if verdict == "CHANGES_REQUESTED":
        blockers = "### B1 Correctness bug\nEvidence: x\nImpact: y\nRequired fix: z"
    return (
        "## Summary\n\nReview result.\n\n"
        f"## Blockers\n\n{blockers}\n\n"
        "## Follow-Ups\n\nNone.\n\n"
        "## Questions / Assumptions\n\nNone.\n\n"
        f"## Verdict\n\n{verdict}\n"
    )


def _completed_full_review(
    store: SqliteTaskStore,
    impl,
    *,
    head: str,
    verdict: str = "APPROVED",
    completed_at: datetime | None = None,
):
    review = store.add("Completed full review", task_type="review", depends_on=impl.id, based_on=impl.id)
    review.status = "completed"
    review.completed_at = completed_at or datetime(2026, 8, 26, 12, 0, tzinfo=UTC)
    review.review_verify_head_sha = head
    review.output_content = _review_report(verdict)
    store.update(review)
    return review


def _resolution_review(
    store: SqliteTaskStore,
    impl,
    rebase,
    *,
    status: str,
    resolved_head: str,
    target: str,
    verify_head: str | None = None,
    verdict: str = "APPROVED",
    completed_at: datetime | None = None,
):
    review = store.add("Resolution review", task_type="review", depends_on=impl.id, based_on=impl.id)
    review.status = status
    if status == "completed":
        review.completed_at = completed_at or datetime(2026, 8, 26, 12, 0, tzinfo=UTC)
        review.output_content = _review_report(verdict)
    review.review_scope = build_resolution_review_scope(
        implementation_task_id=impl.id,
        rebase_task_id=rebase.id,
        resolved_head_sha=resolved_head,
        resolved_target_sha=target,
    )
    review.review_verify_head_sha = verify_head
    store.update(review)
    return review


def test_post_rebase_review_not_required_for_mechanical_unchanged_diff_with_rewritten_live_head(tmp_path) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)

    result = acquire_one_post_rebase_review(
        store,
        LandingPostRebaseReviewRequest(
            impl_task=impl,
            source_head="head-b",
            target_head="target-a",
            pre_rebase_source_head="head-a",
            rebase_outcome_identity=_rebase_identity(
                attempted_source_head="head-a",
                live_source_head="head-b",
            ),
            rebase_outcome_kind="mechanical",
            changed_diff=False,
            conflict_resolved=False,
        ),
    )

    assert result.status == "not_required"
    assert result.review_budget_used is False


def test_post_rebase_review_not_required_preserves_spent_budget(tmp_path) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)

    result = acquire_one_post_rebase_review(
        store,
        LandingPostRebaseReviewRequest(
            impl_task=impl,
            source_head="head-a",
            target_head="target-a",
            pre_rebase_source_head="head-a",
            rebase_outcome_identity=_rebase_identity(),
            rebase_outcome_kind="mechanical",
            changed_diff=False,
            conflict_resolved=False,
            review_budget_used=True,
        ),
    )

    assert result.status == "not_required"
    assert result.review_budget_used is True


@pytest.mark.parametrize("blank_field", ("source_head", "target_head"))
def test_post_rebase_review_blocks_blank_live_head_before_mechanical_carry_forward(
    tmp_path,
    blank_field: str,
) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)
    calls: list[str] = []
    values = {"source_head": "head-a", "target_head": "target-a"}
    values[blank_field] = " "

    result = acquire_one_post_rebase_review(
        store,
        LandingPostRebaseReviewRequest(
            impl_task=impl,
            source_head=values["source_head"],
            target_head=values["target_head"],
            pre_rebase_source_head="head-a",
            rebase_outcome_identity=_rebase_identity(),
            rebase_outcome_kind="mechanical",
            changed_diff=False,
        ),
        create_full_review=lambda *_args, **_kwargs: calls.append("created") or impl,
    )

    assert result.status == "blocked"
    assert result.review_budget_used is False
    assert result.blocked is not None
    assert result.blocked.reason_code == "required-review-unavailable"
    assert calls == []


@pytest.mark.parametrize(
    "no_op_subtype",
    ("already_contained", "superseded_contained", "unchanged_target", "moot"),
)
def test_post_rebase_review_not_required_for_supported_no_op_with_exact_proof(
    tmp_path,
    no_op_subtype: str,
) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)

    result = acquire_one_post_rebase_review(
        store,
        LandingPostRebaseReviewRequest(
            impl_task=impl,
            source_head="head-a",
            target_head="target-a",
            rebase_outcome_identity=_rebase_identity(outcome_kind="no_op", no_op_subtype=no_op_subtype),
            rebase_outcome_kind="no_op",
            changed_diff=False,
        ),
    )

    assert result.status == "not_required"
    assert result.review_budget_used is False


@pytest.mark.parametrize(
    "identity",
    (
        None,
        _rebase_identity(attempted_source_head="old-head"),
        _rebase_identity(attempted_target_head="old-target"),
        _rebase_identity(live_source_head="other-head"),
        _rebase_identity(live_target_head="other-target"),
        _rebase_identity(target_contained=False),
        _rebase_identity(provider_resolution_proof=True),
        _rebase_identity(changed_diff=None),
        _rebase_identity(outcome_kind="no_op", no_op_subtype=None),
        _rebase_identity(outcome_kind="no_op", no_op_subtype="unsupported"),
    ),
)
def test_post_rebase_review_refreshes_once_for_missing_or_mismatched_carry_forward_proof(
    tmp_path,
    identity: LandingRebaseOutcomeIdentity | None,
) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)
    calls: list[str] = []

    def fake_full_review(*_args: Any, **_kwargs: Any):
        calls.append("created")
        review = store.add("Created full review", task_type="review", depends_on=impl.id, based_on=impl.id)
        review.status = "pending"
        review.review_verify_head_sha = "head-a"
        store.update(review)
        return review

    result = acquire_one_post_rebase_review(
        store,
        LandingPostRebaseReviewRequest(
            impl_task=impl,
            source_head="head-a",
            target_head="target-a",
            pre_rebase_source_head="head-a",
            rebase_outcome_identity=identity,
            rebase_outcome_kind="mechanical" if identity is None or identity.outcome_kind == "mechanical" else "no_op",
            changed_diff=False,
        ),
        create_full_review=fake_full_review,
    )

    assert result.status == "created"
    assert result.need == "full"
    assert result.review_budget_used is True
    assert calls == ["created"]


def test_post_rebase_review_blocks_without_creation_when_carry_forward_proof_invalid_and_budget_spent(
    tmp_path,
) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)
    calls: list[str] = []

    result = acquire_one_post_rebase_review(
        store,
        LandingPostRebaseReviewRequest(
            impl_task=impl,
            source_head="head-a",
            target_head="target-a",
            pre_rebase_source_head="head-a",
            rebase_outcome_identity=_rebase_identity(attempted_source_head="old-head"),
            rebase_outcome_kind="mechanical",
            changed_diff=False,
            review_budget_used=True,
        ),
        create_full_review=lambda *_args, **_kwargs: calls.append("created") or impl,
    )

    assert result.status == "blocked"
    assert result.review_budget_used is True
    assert result.blocked is not None
    assert result.blocked.reason_code == "bounded-attempt-exhausted"
    assert calls == []


def test_conflict_resolved_rebase_requires_one_resolution_review_even_when_diff_unchanged(tmp_path) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)
    rebase = store.add("Rebase landing review", task_type="rebase", based_on=impl.id, same_branch=True)
    created: list[str] = []

    def fake_resolution(*_args: Any, **kwargs: Any):
        created.append("resolution")
        review = store.add("Resolution review", task_type="review", depends_on=impl.id, based_on=impl.id)
        review.status = "pending"
        review.review_scope = "resolution-review"
        review.review_verify_head_sha = kwargs["resolved_head_sha"]
        store.update(review)
        return review

    result = acquire_one_post_rebase_review(
        store,
        LandingPostRebaseReviewRequest(
            impl_task=impl,
            rebase_task=rebase,
            source_head="head-a",
            target_head="target-a",
            rebase_outcome_kind="provider_resolved",
            changed_diff=False,
            conflict_resolved=True,
        ),
        create_resolution_review=fake_resolution,
    )

    assert result.status == "created"
    assert result.need == "resolution"
    assert result.review_budget_used is True
    assert created == ["resolution"]


@pytest.mark.parametrize("outcome_kind", ("recovered", "resumed"))
@pytest.mark.parametrize("provenance_complete", (True, False))
def test_recovered_and_resumed_rebases_require_one_review_even_when_diff_unchanged(
    tmp_path,
    outcome_kind: str,
    provenance_complete: bool,
) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)
    rebase = store.add("Rebase landing review", task_type="rebase", based_on=impl.id, same_branch=True)
    created: list[str] = []

    def fake_resolution(*_args: Any, **kwargs: Any):
        created.append("resolution")
        review = store.add("Resolution review", task_type="review", depends_on=impl.id, based_on=impl.id)
        review.status = "pending"
        review.review_scope = build_resolution_review_scope(
            implementation_task_id=impl.id,
            rebase_task_id=rebase.id,
            resolved_head_sha=kwargs["resolved_head_sha"],
            resolved_target_sha=kwargs["resolved_target_sha"],
        )
        review.review_verify_head_sha = kwargs["resolved_head_sha"]
        store.update(review)
        return review

    def fake_full(*_args: Any, **_kwargs: Any):
        created.append("full")
        review = store.add("Full review", task_type="review", depends_on=impl.id, based_on=impl.id)
        review.status = "pending"
        store.update(review)
        return review

    result = acquire_one_post_rebase_review(
        store,
        LandingPostRebaseReviewRequest(
            impl_task=impl,
            rebase_task=rebase,
            source_head="head-a",
            target_head="target-a",
            rebase_outcome_kind=outcome_kind,
            changed_diff=False,
            conflict_resolved=False,
            resolution_provenance_complete=provenance_complete,
        ),
        create_full_review=fake_full,
        create_resolution_review=fake_resolution,
    )

    assert result.status == "created"
    assert result.need == ("resolution" if provenance_complete else "full")
    assert result.review_budget_used is True
    assert created == ["resolution" if provenance_complete else "full"]
    assert result.review_task is not None
    if provenance_complete:
        assert result.review_task.review_scope is not None
    else:
        assert result.review_task.review_verify_head_sha == "head-a"


@pytest.mark.parametrize("outcome_kind", (None, "unexpected"))
def test_missing_or_malformed_rebase_outcome_with_unchanged_diff_uses_full_current_head_fallback(
    tmp_path,
    outcome_kind: str | None,
) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)

    result = acquire_one_post_rebase_review(
        store,
        LandingPostRebaseReviewRequest(
            impl_task=impl,
            source_head="head-a",
            target_head="target-a",
            rebase_outcome_kind=outcome_kind,
            changed_diff=False,
            conflict_resolved=False,
        ),
    )

    assert result.status == "created"
    assert result.need == "full"
    assert result.review_budget_used is True
    assert result.review_task is not None
    assert result.review_task.review_verify_head_sha == "head-a"


def test_changed_unknown_diff_falls_back_to_full_current_head_review_when_resolution_provenance_missing(tmp_path) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)

    result = acquire_one_post_rebase_review(
        store,
        LandingPostRebaseReviewRequest(
            impl_task=impl,
            source_head="head-a",
            target_head="target-a",
            changed_diff=None,
            resolution_provenance_complete=False,
        ),
    )

    assert result.status == "created"
    assert result.need == "full"
    assert result.review_task is not None
    assert result.review_task.review_verify_head_sha == "head-a"
    assert result.review_budget_used is True


def test_post_rebase_review_reuses_only_exact_pending_mode_and_head_identity(tmp_path) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)
    stale = store.add("Stale full review", task_type="review", depends_on=impl.id, based_on=impl.id)
    stale.status = "pending"
    stale.review_verify_head_sha = "old-head"
    store.update(stale)

    result = acquire_one_post_rebase_review(
        store,
        LandingPostRebaseReviewRequest(
            impl_task=impl,
            source_head="head-a",
            target_head="target-a",
            changed_diff=None,
            resolution_provenance_complete=False,
        ),
    )

    assert result.status == "blocked"
    assert result.blocked is not None
    assert result.blocked.reason_code == "required-review-unavailable"
    assert "does not match" in result.blocked.fact


@pytest.mark.parametrize("active_status", ("pending", "in_progress"))
def test_active_resolution_review_with_contradictory_reviewed_head_blocks_without_creation(
    tmp_path,
    active_status: str,
) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)
    rebase = store.add("Rebase landing review", task_type="rebase", based_on=impl.id, same_branch=True)
    active = _resolution_review(
        store,
        impl,
        rebase,
        status=active_status,
        resolved_head="head-a",
        target="target-a",
        verify_head="old-head",
    )

    result = acquire_one_post_rebase_review(
        store,
        LandingPostRebaseReviewRequest(
            impl_task=impl,
            rebase_task=rebase,
            source_head="head-a",
            target_head="target-a",
            changed_diff=True,
            resolution_provenance_complete=True,
        ),
        create_resolution_review=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("no create")),
    )

    assert result.status == "blocked"
    assert result.review_budget_used is False
    assert result.blocked is not None
    assert result.blocked.reason_code == "required-review-unavailable"
    assert active.id in result.blocked.evidence_refs


def test_full_post_rebase_review_rejects_blank_source_head(tmp_path) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)
    review = _completed_full_review(store, impl, head="arbitrary-head")
    calls: list[str] = []

    result = acquire_one_post_rebase_review(
        store,
        LandingPostRebaseReviewRequest(
            impl_task=impl,
            source_head="   ",
            target_head="target-a",
            changed_diff=None,
            resolution_provenance_complete=False,
            review_budget_used=True,
        ),
        create_full_review=lambda *_args, **_kwargs: calls.append("created") or review,
    )

    assert result.status == "blocked"
    assert result.review_budget_used is True
    assert result.blocked is not None
    assert result.blocked.reason_code == "required-review-unavailable"
    assert calls == []


def test_completed_resolution_reviews_require_actual_reviewed_head(tmp_path) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)
    rebase = store.add("Rebase landing review", task_type="rebase", based_on=impl.id, same_branch=True)
    missing = _resolution_review(
        store,
        impl,
        rebase,
        status="completed",
        resolved_head="head-a",
        target="target-a",
        verify_head=None,
        completed_at=datetime(2026, 8, 26, 12, 0, tzinfo=UTC),
    )
    mismatched = _resolution_review(
        store,
        impl,
        rebase,
        status="completed",
        resolved_head="head-a",
        target="target-a",
        verify_head="old-head",
        completed_at=datetime(2026, 8, 26, 12, 1, tzinfo=UTC),
    )
    exact = _resolution_review(
        store,
        impl,
        rebase,
        status="completed",
        resolved_head="head-a",
        target="target-a",
        verify_head="head-a",
        completed_at=datetime(2026, 8, 26, 12, 2, tzinfo=UTC),
    )
    calls: list[str] = []

    result = acquire_one_post_rebase_review(
        store,
        LandingPostRebaseReviewRequest(
            impl_task=impl,
            rebase_task=rebase,
            source_head="head-a",
            target_head="target-a",
            changed_diff=True,
            resolution_provenance_complete=True,
        ),
        create_resolution_review=lambda *_args, **_kwargs: calls.append("created") or missing,
    )

    assert result.status == "reused_completed"
    assert result.review_task == exact
    assert result.review_task is not None
    assert result.review_task.id not in {missing.id, mismatched.id}
    assert calls == []


@pytest.mark.parametrize("missing_head", (None, "old-head"))
def test_completed_resolution_review_with_missing_or_mismatched_actual_head_is_not_reused(
    tmp_path,
    missing_head: str | None,
) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)
    rebase = store.add("Rebase landing review", task_type="rebase", based_on=impl.id, same_branch=True)
    stale = _resolution_review(
        store,
        impl,
        rebase,
        status="completed",
        resolved_head="head-a",
        target="target-a",
        verify_head=missing_head,
    )
    created: list[str] = []

    result = acquire_one_post_rebase_review(
        store,
        LandingPostRebaseReviewRequest(
            impl_task=impl,
            rebase_task=rebase,
            source_head="head-a",
            target_head="target-a",
            changed_diff=True,
            resolution_provenance_complete=True,
        ),
        create_resolution_review=lambda *_args, **_kwargs: created.append("created") or stale,
    )

    assert result.status == "created"
    assert created == ["created"]


@pytest.mark.parametrize("need", ("full", "resolution"))
@pytest.mark.parametrize("active_status", ("pending", "in_progress"))
def test_active_incompatible_post_rebase_review_blocks_older_completed_reuse(
    tmp_path,
    need: str,
    active_status: str,
) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)
    rebase = store.add("Rebase landing review", task_type="rebase", based_on=impl.id, same_branch=True)
    _completed_full_review(store, impl, head="head-a", completed_at=datetime(2026, 8, 26, 12, 0, tzinfo=UTC))
    if need == "resolution":
        _resolution_review(
            store,
            impl,
            rebase,
            status="completed",
            resolved_head="head-a",
            target="target-a",
            verify_head="head-a",
            completed_at=datetime(2026, 8, 26, 12, 0, tzinfo=UTC),
        )
        active = _resolution_review(
            store,
            impl,
            rebase,
            status=active_status,
            resolved_head="other-head",
            target="target-a",
        )
        request = LandingPostRebaseReviewRequest(
            impl_task=impl,
            rebase_task=rebase,
            source_head="head-a",
            target_head="target-a",
            changed_diff=True,
            resolution_provenance_complete=True,
        )
    else:
        active = store.add("Incompatible full review", task_type="review", depends_on=impl.id, based_on=impl.id)
        active.status = active_status
        active.review_verify_head_sha = "other-head"
        store.update(active)
        request = LandingPostRebaseReviewRequest(
            impl_task=impl,
            source_head="head-a",
            target_head="target-a",
            changed_diff=True,
            resolution_provenance_complete=False,
        )

    result = acquire_one_post_rebase_review(store, request)

    assert result.status == "blocked"
    assert result.review_budget_used is False
    assert result.blocked is not None
    assert result.blocked.reason_code == "required-review-unavailable"
    assert active.id in result.blocked.evidence_refs


@pytest.mark.parametrize("need", ("full", "resolution"))
@pytest.mark.parametrize("active_status", ("pending", "in_progress"))
def test_exact_active_post_rebase_review_is_reused_or_waited_with_spent_budget(
    tmp_path,
    need: str,
    active_status: str,
) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)
    rebase = store.add("Rebase landing review", task_type="rebase", based_on=impl.id, same_branch=True)
    if need == "resolution":
        active = _resolution_review(
            store,
            impl,
            rebase,
            status=active_status,
            resolved_head="head-a",
            target="target-a",
        )
        request = LandingPostRebaseReviewRequest(
            impl_task=impl,
            rebase_task=rebase,
            source_head="head-a",
            target_head="target-a",
            changed_diff=True,
            resolution_provenance_complete=True,
            review_budget_used=True,
        )
    else:
        active = store.add("Exact full review", task_type="review", depends_on=impl.id, based_on=impl.id)
        active.status = active_status
        active.review_verify_head_sha = "head-a"
        store.update(active)
        request = LandingPostRebaseReviewRequest(
            impl_task=impl,
            source_head="head-a",
            target_head="target-a",
            changed_diff=True,
            resolution_provenance_complete=False,
            review_budget_used=True,
        )

    result = acquire_one_post_rebase_review(
        store,
        request,
        create_full_review=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("no create")),
        create_resolution_review=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("no create")),
    )

    assert result.status == active_status
    assert result.review_task == active
    assert result.review_budget_used is True


def test_incompatible_active_review_blocks_even_when_exact_active_review_exists(tmp_path) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)
    exact = store.add("Exact full review", task_type="review", depends_on=impl.id, based_on=impl.id)
    exact.status = "pending"
    exact.review_verify_head_sha = "head-a"
    store.update(exact)
    incompatible = store.add("Incompatible full review", task_type="review", depends_on=impl.id, based_on=impl.id)
    incompatible.status = "in_progress"
    incompatible.review_verify_head_sha = "other-head"
    store.update(incompatible)

    result = acquire_one_post_rebase_review(
        store,
        LandingPostRebaseReviewRequest(
            impl_task=impl,
            source_head="head-a",
            target_head="target-a",
            changed_diff=True,
            resolution_provenance_complete=False,
        ),
    )

    assert result.status == "blocked"
    assert result.blocked is not None
    assert result.blocked.reason_code == "required-review-unavailable"
    assert incompatible.id in result.blocked.evidence_refs


def test_completed_changes_requested_post_rebase_review_returns_without_second_review_or_improve(tmp_path) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)
    review = store.add("Completed full review", task_type="review", depends_on=impl.id, based_on=impl.id)
    review.status = "completed"
    review.completed_at = datetime(2026, 8, 26, 12, 0, tzinfo=UTC)
    review.review_verify_head_sha = "head-a"
    review.output_content = (
        "## Summary\n\nChanges requested.\n\n"
        "## Blockers\n\n### B1 Correctness bug\nEvidence: x\nImpact: y\nRequired fix: z\n\n"
        "## Follow-Ups\n\nNone.\n\n"
        "## Questions / Assumptions\n\nNone.\n\n"
        "## Verdict\n\nCHANGES_REQUESTED\n"
    )
    store.update(review)
    calls: list[str] = []

    result = acquire_one_post_rebase_review(
        store,
        LandingPostRebaseReviewRequest(
            impl_task=impl,
            source_head="head-a",
            target_head="target-a",
            changed_diff=None,
            resolution_provenance_complete=False,
        ),
        create_full_review=lambda *_args, **_kwargs: calls.append("created") or review,
    )

    assert result.status == "reused_completed"
    assert result.review_task == review
    assert result.review_budget_used is False
    assert calls == []


def test_malformed_completed_post_rebase_review_blocks(tmp_path) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)
    review = store.add("Malformed full review", task_type="review", depends_on=impl.id, based_on=impl.id)
    review.status = "completed"
    review.completed_at = datetime(2026, 8, 26, 12, 0, tzinfo=UTC)
    review.review_verify_head_sha = "head-a"
    review.output_content = "not a valid review verdict"
    store.update(review)

    result = acquire_one_post_rebase_review(
        store,
        LandingPostRebaseReviewRequest(
            impl_task=impl,
            source_head="head-a",
            target_head="target-a",
            changed_diff=None,
            resolution_provenance_complete=False,
        ),
    )

    assert result.status == "blocked"
    assert result.blocked is not None
    assert result.blocked.reason_code == "required-review-unavailable"


@pytest.mark.parametrize("need", ("full", "resolution"))
@pytest.mark.parametrize("terminal_status", ("failed", "stopped"))
def test_latest_exact_terminal_post_rebase_review_blocks_without_second_creation(
    tmp_path,
    need: str,
    terminal_status: str,
) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)
    rebase = store.add("Rebase landing review", task_type="rebase", based_on=impl.id, same_branch=True)
    _completed_full_review(
        store,
        impl,
        head="head-a",
        verdict="APPROVED",
        completed_at=datetime(2026, 8, 26, 12, 0, tzinfo=UTC),
    )
    if need == "resolution":
        _resolution_review(
            store,
            impl,
            rebase,
            status="completed",
            resolved_head="head-a",
            target="target-a",
            verify_head="head-a",
            completed_at=datetime(2026, 8, 26, 12, 0, tzinfo=UTC),
        )
        terminal = _resolution_review(
            store,
            impl,
            rebase,
            status=terminal_status,
            resolved_head="head-a",
            target="target-a",
            verify_head="head-a",
        )
        terminal.completed_at = datetime(2026, 8, 26, 12, 1, tzinfo=UTC)
        store.update(terminal)
        request = LandingPostRebaseReviewRequest(
            impl_task=impl,
            rebase_task=rebase,
            source_head="head-a",
            target_head="target-a",
            changed_diff=True,
            resolution_provenance_complete=True,
        )
        create = {
            "create_resolution_review": lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("no create")),
        }
    else:
        terminal = store.add("Terminal full review", task_type="review", depends_on=impl.id, based_on=impl.id)
        terminal.status = terminal_status
        terminal.review_verify_head_sha = "head-a"
        terminal.completed_at = datetime(2026, 8, 26, 12, 1, tzinfo=UTC)
        store.update(terminal)
        request = LandingPostRebaseReviewRequest(
            impl_task=impl,
            source_head="head-a",
            target_head="target-a",
            changed_diff=True,
            resolution_provenance_complete=False,
        )
        create = {
            "create_full_review": lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("no create")),
        }

    result = acquire_one_post_rebase_review(store, request, **create)

    assert result.status == "blocked"
    assert result.review_task == terminal
    assert result.blocked is not None
    assert result.blocked.reason_code == "required-review-unavailable"
    assert terminal.id in result.blocked.evidence_refs


def test_post_rebase_review_budget_blocks_second_creation(tmp_path) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)

    result = acquire_one_post_rebase_review(
        store,
        LandingPostRebaseReviewRequest(
            impl_task=impl,
            source_head="head-a",
            target_head="target-a",
            changed_diff=True,
            resolution_provenance_complete=False,
            review_budget_used=True,
        ),
        create_full_review=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("budget spent")),
    )

    assert result.status == "blocked"
    assert result.blocked is not None
    assert result.blocked.reason_code == "bounded-attempt-exhausted"


@pytest.mark.parametrize(
    ("case", "expected_status"),
    (
        ("not_required", "not_required"),
        ("blank_source", "blocked"),
        ("reused_completed_approved", "reused_completed"),
        ("reused_completed_changes_requested", "reused_completed"),
        ("malformed_completed", "blocked"),
        ("exact_pending", "pending"),
        ("exact_in_progress", "in_progress"),
        ("exhausted_no_reuse", "blocked"),
    ),
)
def test_post_rebase_review_result_budget_is_monotonic_after_spent_entry(
    tmp_path,
    case: str,
    expected_status: str,
) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)
    request = LandingPostRebaseReviewRequest(
        impl_task=impl,
        source_head="head-a",
        target_head="target-a",
        changed_diff=True,
        resolution_provenance_complete=False,
        review_budget_used=True,
    )
    if case == "not_required":
        request = LandingPostRebaseReviewRequest(
            impl_task=impl,
            source_head="head-a",
            target_head="target-a",
            pre_rebase_source_head="head-a",
            rebase_outcome_identity=_rebase_identity(),
            rebase_outcome_kind="mechanical",
            changed_diff=False,
            review_budget_used=True,
        )
    elif case == "blank_source":
        request = LandingPostRebaseReviewRequest(
            impl_task=impl,
            source_head=" ",
            target_head="target-a",
            changed_diff=True,
            resolution_provenance_complete=False,
            review_budget_used=True,
        )
    elif case == "reused_completed_approved":
        _completed_full_review(store, impl, head="head-a", verdict="APPROVED")
    elif case == "reused_completed_changes_requested":
        _completed_full_review(store, impl, head="head-a", verdict="CHANGES_REQUESTED")
    elif case == "malformed_completed":
        review = store.add("Malformed full review", task_type="review", depends_on=impl.id, based_on=impl.id)
        review.status = "completed"
        review.completed_at = datetime(2026, 8, 26, 12, 0, tzinfo=UTC)
        review.review_verify_head_sha = "head-a"
        review.output_content = "not a valid review verdict"
        store.update(review)
    elif case in {"exact_pending", "exact_in_progress"}:
        review = store.add("Exact active review", task_type="review", depends_on=impl.id, based_on=impl.id)
        review.status = "pending" if case == "exact_pending" else "in_progress"
        review.review_verify_head_sha = "head-a"
        store.update(review)

    result = acquire_one_post_rebase_review(
        store,
        request,
        create_full_review=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("spent budget")),
    )

    assert result.status == expected_status
    assert result.review_budget_used is True


@pytest.mark.parametrize("active_status", ("pending", "in_progress"))
def test_duplicate_race_reuse_consumes_review_budget(active_status: str, tmp_path) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)
    active = Task(
        id="gza-duplicate",
        prompt="Duplicate active review",
        task_type="review",
        status=active_status,
        depends_on=impl.id,
        based_on=impl.id,
        review_verify_head_sha="head-a",
    )

    result = acquire_one_post_rebase_review(
        store,
        LandingPostRebaseReviewRequest(
            impl_task=impl,
            source_head="head-a",
            target_head="target-a",
            changed_diff=True,
            resolution_provenance_complete=False,
        ),
        create_full_review=lambda *_args, **_kwargs: (_ for _ in ()).throw(DuplicateReviewError(active)),
    )

    assert result.status == active_status
    assert result.review_task == active
    assert result.review_budget_used is True


def test_duplicate_race_identity_conflict_preserves_unspent_budget(tmp_path) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)
    active = Task(
        id="gza-duplicate",
        prompt="Duplicate stale review",
        task_type="review",
        status="pending",
        depends_on=impl.id,
        based_on=impl.id,
        review_verify_head_sha="old-head",
    )

    result = acquire_one_post_rebase_review(
        store,
        LandingPostRebaseReviewRequest(
            impl_task=impl,
            source_head="head-a",
            target_head="target-a",
            changed_diff=True,
            resolution_provenance_complete=False,
        ),
        create_full_review=lambda *_args, **_kwargs: (_ for _ in ()).throw(DuplicateReviewError(active)),
    )

    assert result.status == "blocked"
    assert result.review_budget_used is False
    assert result.blocked is not None
    assert result.blocked.reason_code == "required-review-unavailable"


def test_pending_reuse_consumes_budget_and_terminal_failure_blocks_second_creation(tmp_path) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)
    pending = store.add("Pending landing review", task_type="review", depends_on=impl.id, based_on=impl.id)
    pending.status = "pending"
    pending.review_verify_head_sha = "head-a"
    store.update(pending)
    calls: list[str] = []

    first = acquire_one_post_rebase_review(
        store,
        LandingPostRebaseReviewRequest(
            impl_task=impl,
            source_head="head-a",
            target_head="target-a",
            changed_diff=True,
            resolution_provenance_complete=False,
        ),
        create_full_review=lambda *_args, **_kwargs: calls.append("created") or pending,
    )

    assert first.status == "pending"
    assert first.review_budget_used is True
    assert calls == []

    pending.status = "failed"
    store.update(pending)
    second = acquire_one_post_rebase_review(
        store,
        LandingPostRebaseReviewRequest(
            impl_task=impl,
            source_head="head-a",
            target_head="target-a",
            changed_diff=True,
            resolution_provenance_complete=False,
            review_budget_used=first.review_budget_used,
        ),
        create_full_review=lambda *_args, **_kwargs: calls.append("created") or pending,
    )

    assert second.status == "blocked"
    assert second.review_task == pending
    assert second.review_budget_used is True
    assert second.blocked is not None
    assert second.blocked.reason_code == "required-review-unavailable"
    assert calls == []


def test_post_rebase_review_budget_sequence_never_allows_second_review_after_changes_requested(tmp_path) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)
    created: list[str] = []

    def create_first_review(*_args: Any, **_kwargs: Any):
        created.append("created")
        review = store.add("First landing review", task_type="review", depends_on=impl.id, based_on=impl.id)
        review.status = "pending"
        review.review_verify_head_sha = "head-a"
        store.update(review)
        return review

    first = acquire_one_post_rebase_review(
        store,
        LandingPostRebaseReviewRequest(
            impl_task=impl,
            source_head="head-a",
            target_head="target-a",
            changed_diff=True,
            resolution_provenance_complete=False,
        ),
        create_full_review=create_first_review,
    )
    assert first.status == "created"
    assert first.review_task is not None
    assert first.review_budget_used is True

    first.review_task.status = "completed"
    first.review_task.completed_at = datetime(2026, 8, 26, 12, 0, tzinfo=UTC)
    first.review_task.output_content = _review_report("CHANGES_REQUESTED")
    store.update(first.review_task)

    completed = acquire_one_post_rebase_review(
        store,
        LandingPostRebaseReviewRequest(
            impl_task=impl,
            source_head="head-a",
            target_head="target-a",
            changed_diff=True,
            resolution_provenance_complete=False,
            review_budget_used=first.review_budget_used,
        ),
        create_full_review=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("no second review")),
    )
    assert completed.status == "reused_completed"
    assert completed.review_budget_used is True

    first.review_task.status = "pending"
    first.review_task.completed_at = None
    store.update(first.review_task)

    pending = acquire_one_post_rebase_review(
        store,
        LandingPostRebaseReviewRequest(
            impl_task=impl,
            source_head="head-a",
            target_head="target-a",
            changed_diff=True,
            resolution_provenance_complete=False,
            review_budget_used=completed.review_budget_used,
        ),
        create_full_review=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("no second review")),
    )
    assert pending.status == "pending"
    assert pending.review_budget_used is True

    no_longer_required = acquire_one_post_rebase_review(
        store,
        LandingPostRebaseReviewRequest(
            impl_task=impl,
            source_head="head-a",
            target_head="target-a",
            pre_rebase_source_head="head-a",
            rebase_outcome_identity=_rebase_identity(),
            rebase_outcome_kind="mechanical",
            changed_diff=False,
            review_budget_used=pending.review_budget_used,
        ),
        create_full_review=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("no second review")),
    )
    assert no_longer_required.status == "not_required"
    assert no_longer_required.review_budget_used is True

    first.review_task.status = "completed"
    first.review_task.completed_at = datetime(2026, 8, 26, 12, 1, tzinfo=UTC)
    store.update(first.review_task)

    changed_identity = acquire_one_post_rebase_review(
        store,
        LandingPostRebaseReviewRequest(
            impl_task=impl,
            source_head="head-b",
            target_head="target-a",
            changed_diff=True,
            resolution_provenance_complete=False,
            review_budget_used=no_longer_required.review_budget_used,
        ),
        create_full_review=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("no second review")),
    )
    assert changed_identity.status == "blocked"
    assert changed_identity.review_budget_used is True
    assert changed_identity.blocked is not None
    assert changed_identity.blocked.reason_code == "bounded-attempt-exhausted"
    assert created == ["created"]
