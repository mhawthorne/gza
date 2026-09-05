from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from gza.config import Config
from gza.db import SqliteTaskStore, Task
from gza.db import WatchProgressObservation
from gza.landing import (
    LANDING_PHASES,
    LandBlocked,
    LandingCoordinator,
    LandingFollowupFinding,
    LandingFollowupMaterializationIdentity,
    LandingJudgeVerdict,
    LandingJudgment,
    LandingOpenBlocker,
    LandingPolicyDecision,
    LandingPolicyFacts,
    LandingPostRebaseReviewRequest,
    LandingRebaseFingerprint,
    LandingRebaseOutcomeIdentity,
    LandingReviewEvidence,
    LandingSpecCoherenceEvidence,
    LandingSpecCoherenceFingerprint,
    LandingStateFingerprint,
    LandingTransitionLimitPolicy,
    LandingVerifyAcquisitionResult,
    LandingVerifyEvidence,
    LandPostMergeVerifyFailure,
    LandRequest,
    LandResult,
    LandStep,
    acquire_landing_verify_evidence,
    acquire_one_post_rebase_review,
    dry_run_steps_until_boundary,
    evaluate_landing_policy,
    inspect_current_landing_verify_evidence,
    run_landing_post_rebase_review_transition,
)
from gza.review_scope import build_spec_coherence_review_scope
from gza.review_scope import build_resolution_review_scope
from gza.review_tasks import DuplicateReviewError
from gza.review_verdict import ParsedReviewReport, ReviewFinding
from gza.review_verify_state import (
    VerifyGateDecision,
    VerifyGateLookup,
    VerifyGateResult,
    make_verify_epoch,
    persist_recredited_verify_gate_artifact,
    persist_verify_gate_artifact,
)
from gza.runner import (
    LifecycleVerifyExecution,
    ProjectVerificationResult,
    ReviewVerifyResult,
    _persist_lifecycle_verify_execution,
)
from gza.rebase_service import RebaseServiceRequest, RebaseServiceResult
from gza.sync_ops import BranchSyncResult

TREE_A = "a" * 64
TREE_B = "b" * 64


def _assert_no_improve_rows(store: SqliteTaskStore) -> None:
    with store._connect() as conn:
        rows = conn.execute("SELECT id FROM tasks WHERE task_type = 'improve'").fetchall()
    assert rows == []


def _assert_no_improve_action(result: Any) -> None:
    action = getattr(result, "action", None)
    assert not isinstance(action, dict) or action.get("type") not in {
        "improve",
        "create_improve",
        "run_improve",
        "wait_improve",
        "resume",
        "resume_improve",
    }


def _fail_improve_or_review_route(*_args: Any, **_kwargs: Any) -> Task:
    raise AssertionError("no improve, second review, or fallback review route")


def _assert_no_review_or_improve_rows_after_landing_review(store: SqliteTaskStore, review_ids: set[str]) -> None:
    with store._connect() as conn:
        review_rows = conn.execute("SELECT id FROM tasks WHERE task_type = 'review'").fetchall()
        improve_rows = conn.execute("SELECT id FROM tasks WHERE task_type = 'improve'").fetchall()
    assert {row[0] for row in review_rows} <= review_ids
    assert improve_rows == []


def _landing_policy_facts_for_review(impl: Task, review: Task) -> LandingPolicyFacts:
    return _green_facts(
        task_id=impl.id or "unknown",
        source_head="head-a",
        target_head="target-a",
        parked_reason="review-max-cycles-reached",
        review=_review(
            verdict="APPROVED",
            review_id=review.id,
            reviewed_head=review.review_verify_head_sha,
        ),
        open_blockers=(_blocker("B1", deferrable=True, blocker_class="out_of_scope", source=f"review:{review.id}"),),
    )


def _run_landing_review_transition_with_poisoned_review_routes(
    store: SqliteTaskStore,
    impl: Task,
    request: LandingPostRebaseReviewRequest,
    review: Task,
) -> Any:
    transition = run_landing_post_rebase_review_transition(
        store,
        request,
        policy="guarded",
        facts=_landing_policy_facts_for_review(impl, review),
        judge=_landing_judgment,
        create_full_review=_fail_improve_or_review_route,
        create_resolution_review=_fail_improve_or_review_route,
    )

    result = transition.review_result
    _assert_no_improve_action(result)
    assert result.status == "reused_completed"
    assert result.review_task == review
    decision = transition.decision
    assert decision.allowed is True
    assert decision.allowed_overrides == (
        "defer-review-blockers",
        "parked:review-max-cycles-reached",
    )
    assert decision.judgment_verdict == "LAND"
    _assert_no_review_or_improve_rows_after_landing_review(store, {review.id or ""})
    return transition


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
    if field in {"review_id", "source"}:
        assert decision_b.allowed is False
        assert decision_b.blocked is not None
        assert decision_b.blocked.reason_code == "required-review-unavailable"
    else:
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
    def __init__(
        self,
        heads: dict[str, str],
        *,
        current_branch: str = "main",
        dirty: bool = False,
        merged_refs: set[tuple[str, str]] | None = None,
        ancestors: set[tuple[str, str]] | None = None,
        can_merge_refs: set[tuple[str, str]] | None = None,
        name_status: str = "",
    ) -> None:
        self.heads = heads
        self._current_branch = current_branch
        self.dirty = dirty
        self.merged_refs = merged_refs or set()
        self.ancestors = ancestors or set()
        self.can_merge_refs = can_merge_refs
        self.name_status = name_status
        self.merge_calls: list[tuple[str, str | None]] = []
        self.mutation_calls: list[str] = []

    def rev_parse_if_exists(self, ref: str) -> str | None:
        return self.heads.get(ref)

    def current_branch(self) -> str:
        return self._current_branch

    def has_changes(self, include_untracked: bool = False) -> bool:
        assert include_untracked is False
        return self.dirty

    def is_ancestor(self, ancestor: str, descendant: str) -> bool:
        return (ancestor, descendant) in self.ancestors

    def can_merge(self, branch: str, into: str | None = None) -> bool:
        target = into or self._current_branch
        if self.can_merge_refs is None:
            return True
        return (branch, target) in self.can_merge_refs

    def get_diff_name_status(self, revision_range: str, *, check: bool = True) -> str:
        assert check is True
        return self.name_status

    def is_merged(self, branch: str, into: str | None = None, use_cherry: bool = False) -> bool:
        del use_cherry
        target = into or self._current_branch
        self.merge_calls.append((branch, target))
        return (branch, target) in self.merged_refs

    def merge(self, *_args: object, **_kwargs: object) -> None:
        self.mutation_calls.append("merge")


class _LandingSourceGit(_FakeGit):
    def __init__(self, heads: dict[str, str], *, local_branches: set[str], **kwargs: Any) -> None:
        super().__init__(heads, **kwargs)
        self.local_branches = local_branches

    def branch_exists(self, branch: str) -> bool:
        return branch in self.local_branches

    def ref_exists(self, ref: str) -> bool:
        return ref in self.heads


def _coordinator_store(tmp_path) -> SqliteTaskStore:
    return SqliteTaskStore(tmp_path / "test.db")


def _set_landing_subdir_project_boundary(config: Config, tmp_path) -> None:
    from gza.runner import ProjectBoundary

    repo_root = tmp_path
    project_dir = tmp_path / "services" / "foo"
    project_dir.mkdir(parents=True, exist_ok=True)
    config.project_dir = project_dir
    config.enforce_project_scope = True
    setattr(
        config,
        "_project_boundary_cache",
        ProjectBoundary(
            repo_root=repo_root,
            scope_root=Path("services/foo"),
            local_dependencies=(),
        ),
    )


def _completed_impl(store: SqliteTaskStore, prompt: str, branch: str) -> Task:
    task = store.add(prompt, task_type="implement")
    store.mark_completed(task, has_commits=True, branch=branch)
    refreshed = store.get(task.id or "")
    assert refreshed is not None
    return refreshed


def _completed_impl_with_stored_unit(
    store: SqliteTaskStore,
    prompt: str,
    branch: str,
    *,
    target_branch: str,
) -> tuple[Task, Any]:
    task = store.add(prompt, task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime(2026, 8, 26, 12, 0, tzinfo=UTC)
    task.has_commits = True
    task.merge_status = "unmerged"
    task.branch = branch
    store.update(task)
    unit = store.create_merge_unit(
        source_branch=branch,
        target_branch=target_branch,
        owner_task_id=task.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(task.id, unit.id, "owner")
    refreshed = store.get(task.id)
    assert refreshed is not None
    return refreshed, unit


def _sqlite_task_snapshot(store: SqliteTaskStore) -> tuple[tuple[Any, ...], ...]:
    with store._connect() as conn:
        rows = conn.execute(
            """
            SELECT id, status, task_type, branch, merge_status, merged_at, completion_reason
            FROM tasks
            ORDER BY id
            """
        ).fetchall()
    return tuple(tuple(row) for row in rows)


def _sqlite_merge_unit_snapshot(store: SqliteTaskStore) -> tuple[tuple[Any, ...], ...]:
    with store._connect() as conn:
        rows = conn.execute(
            """
            SELECT id, source_branch, target_branch, state, owner_task_id, merged_at, merge_source
            FROM merge_units
            ORDER BY id
            """
        ).fetchall()
    return tuple(tuple(row) for row in rows)


def _assert_single_terminal_sentence(blocked: LandBlocked, task_id: str) -> None:
    sentence = blocked.terminal_sentence(task_id)
    assert "\n" not in sentence
    assert sentence.startswith(f"Cannot land {task_id}: ")
    assert sentence.endswith(".")
    assert sentence.count(".") == 1


def test_landing_coordinator_resolves_owner_descendant_and_review_to_canonical_unit(tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    owner = _completed_impl(store, "owner", "feature/landing")
    retry = store.add("retry", task_type="implement", based_on=owner.id, same_branch=True)
    store.mark_completed(retry, has_commits=True, branch="feature/landing")
    review = store.add("review", task_type="review", based_on=retry.id)
    assert retry.id is not None and review.id is not None
    unit = store.get_or_create_merge_unit_for_task(retry)
    assert unit is not None
    store.attach_task_to_merge_unit(review.id, unit.id, "review")
    git = _LandingSourceGit(
        {"feature/landing": "source-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "source-a")},
    )

    result = LandingCoordinator(store=store, git=git).run(LandRequest(task_id=review.id, dry_run=True))

    assert result.owner_task_id == retry.id
    assert result.source_ref == "feature/landing"
    assert result.target_branch == "main"
    assert result.steps[0].status == "completed"


@pytest.mark.parametrize("selection", ("owner", "descendant", "review"))
def test_landing_coordinator_uses_stored_unit_target_when_default_discovery_fails(
    tmp_path,
    selection: str,
) -> None:
    store = _coordinator_store(tmp_path)
    owner, unit = _completed_impl_with_stored_unit(
        store,
        "stored target owner",
        "feature/stored-target",
        target_branch="release",
    )
    assert owner.id is not None
    descendant = store.add("stored target descendant", task_type="implement", based_on=owner.id, same_branch=True)
    store.mark_completed(descendant, has_commits=True, branch="feature/stored-target")
    review = store.add("stored target review", task_type="review", depends_on=descendant.id, based_on=descendant.id)
    assert descendant.id is not None and review.id is not None
    store.attach_task_to_merge_unit(descendant.id, unit.id, "member")
    store.attach_task_to_merge_unit(review.id, unit.id, "review")

    def fail_default_target(*, strict: bool) -> str:
        assert strict is True
        raise RuntimeError("default target unavailable")

    setattr(store, "default_merge_target", fail_default_target)
    git = _LandingSourceGit(
        {"feature/stored-target": "source-a", "release": "target-a"},
        current_branch="release",
        local_branches={"feature/stored-target"},
        ancestors={("target-a", "source-a")},
    )

    selected_id = {
        "owner": owner.id,
        "descendant": descendant.id,
        "review": review.id,
    }[selection]
    result = LandingCoordinator(store=store, git=git).run(LandRequest(task_id=selected_id, dry_run=True))

    assert result.blocked is None
    assert result.owner_task_id == descendant.id
    assert result.source_ref == "feature/stored-target"
    assert result.target_branch == "release"
    assert result.steps[0].status == "completed"
    assert any(step.phase == "verify" and step.status == "conditional" for step in result.steps)


def test_landing_coordinator_keeps_sibling_merge_units_isolated(tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    first = _completed_impl(store, "first", "feature/first")
    second = _completed_impl(store, "second", "feature/second")
    first_unit = store.get_or_create_merge_unit_for_task(first)
    second_unit = store.get_or_create_merge_unit_for_task(second)
    assert first_unit is not None and second_unit is not None and first.id is not None
    git = _LandingSourceGit(
        {
            "feature/first": "source-first",
            "feature/second": "source-second",
            "main": "target-a",
        },
        local_branches={"feature/first", "feature/second"},
        ancestors={("target-a", "source-first"), ("target-a", "source-second")},
    )

    result = LandingCoordinator(store=store, git=git).run(LandRequest(task_id=first.id, dry_run=True))

    assert result.owner_task_id == first.id
    assert result.source_ref == "feature/first"
    assert result.steps[0].evidence_refs
    assert second_unit.id not in result.steps[0].evidence_refs


def test_landing_coordinator_missing_local_source_fails_closed(tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    impl = _completed_impl(store, "missing source", "feature/missing")
    unit = store.get_or_create_merge_unit_for_task(impl)
    assert unit is not None and impl.id is not None
    git = _LandingSourceGit({"main": "target-a"}, local_branches=set())

    result = LandingCoordinator(store=store, git=git).run(LandRequest(task_id=impl.id, dry_run=True))

    assert result.blocked is not None
    assert result.blocked.reason_code == "identity-proof-unavailable"
    assert result.blocked.terminal_sentence(impl.id).startswith(f"Cannot land {impl.id}: ")


def test_landing_coordinator_dirty_checkout_precedes_rebase_or_verify(tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    impl = _completed_impl(store, "dirty", "feature/dirty")
    unit = store.get_or_create_merge_unit_for_task(impl)
    assert unit is not None and impl.id is not None
    git = _LandingSourceGit(
        {"feature/dirty": "source-a", "main": "target-a"},
        local_branches={"feature/dirty"},
        dirty=True,
    )

    result = LandingCoordinator(store=store, git=git).run(LandRequest(task_id=impl.id))

    assert result.blocked is not None
    assert result.blocked.reason_code == "dirty-checkout"
    assert [step.phase for step in result.steps] == ["resolve", "resolve"]


@pytest.mark.parametrize("case", ("dependency", "scope_violation", "scope_inspection_failure"))
def test_landing_coordinator_cleanliness_probe_failure_does_not_mask_higher_priority_identity_facts(
    tmp_path,
    case: str,
) -> None:
    store = _coordinator_store(tmp_path)
    config: Config | None = None
    if case == "dependency":
        dependency = _completed_impl(store, "dependency", "feature/dependency")
        impl = store.add("dependent", task_type="implement", depends_on=dependency.id)
        store.mark_completed(impl, has_commits=True, branch="feature/dependent")
        branch = "feature/dependent"
        name_status = ""
    else:
        config = Config(project_dir=tmp_path, project_name="scope-project")
        _set_landing_subdir_project_boundary(config, tmp_path)
        impl = _completed_impl(store, f"dirty {case}", f"feature/{case}")
        branch = f"feature/{case}"
        name_status = (
            "M\tservices/bar/app.py\n"
            if case == "scope_violation"
            else "M\tservices/foo/app.py\n"
        )
    refreshed = store.get(impl.id or "")
    assert refreshed is not None and refreshed.id is not None
    before_tasks = _sqlite_task_snapshot(store)
    before_units = _sqlite_merge_unit_snapshot(store)

    class ScopeFailureGit(_LandingSourceGit):
        def has_changes(self, include_untracked: bool = False) -> bool:
            assert include_untracked is False
            raise RuntimeError("status failed. secondary diagnostic")

        def get_diff_name_status(self, revision_range: str, *, check: bool = True) -> str:
            if case == "scope_inspection_failure":
                raise RuntimeError("diff inspection failed. secondary diagnostic")
            return super().get_diff_name_status(revision_range, check=check)

    git = ScopeFailureGit(
        {branch: "source-a", "main": "target-a"},
        local_branches={branch},
        ancestors={("target-a", "source-a")},
        name_status=name_status,
    )

    result = LandingCoordinator(store=store, git=git, config=config).run(
        LandRequest(task_id=refreshed.id, dry_run=True)
    )

    assert result.blocked is not None
    assert result.blocked.reason_code == "identity-proof-unavailable"
    sentence = result.blocked.terminal_sentence(refreshed.id)
    assert "\n" not in sentence
    assert sentence.startswith(f"Cannot land {refreshed.id}: ")
    assert sentence.endswith(".")
    assert _sqlite_task_snapshot(store) == before_tasks
    assert _sqlite_merge_unit_snapshot(store) == before_units
    assert git.mutation_calls == []


def test_landing_coordinator_reconciles_already_landed_through_merge_truth(tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    impl = _completed_impl(store, "already landed", "feature/already")
    unit = store.get_or_create_merge_unit_for_task(impl)
    assert unit is not None and impl.id is not None
    git = _LandingSourceGit(
        {"feature/already": "source-a", "main": "target-a"},
        local_branches={"feature/already"},
        merged_refs={("feature/already", "main")},
        ancestors={("target-a", "source-a")},
    )

    result = LandingCoordinator(store=store, git=git).run(LandRequest(task_id=impl.id))
    refreshed_unit = store.get_merge_unit(unit.id)

    assert result.already_merged is True
    assert result.blocked is None
    assert refreshed_unit is not None
    assert refreshed_unit.state == "merged"


@pytest.mark.parametrize("dry_run", (False, True))
@pytest.mark.parametrize("reconcile_case", ("git_error", "unknown_classification"))
def test_landing_coordinator_refuses_stale_legacy_merged_without_current_target_proof(
    tmp_path,
    dry_run: bool,
    reconcile_case: str,
) -> None:
    store = _coordinator_store(tmp_path)
    impl = _completed_impl(store, f"stale legacy {reconcile_case}", f"feature/{reconcile_case}")
    unit = store.get_or_create_merge_unit_for_task(impl)
    assert unit is not None and impl.id is not None
    impl.merge_status = "merged"
    store.update(impl)
    before_tasks = _sqlite_task_snapshot(store)
    before_units = _sqlite_merge_unit_snapshot(store)
    git = _LandingSourceGit(
        {f"feature/{reconcile_case}": "source-a", "main": "target-a"},
        local_branches={f"feature/{reconcile_case}"},
        ancestors={("target-a", "source-a")},
    )
    persist_calls: list[bool] = []

    def stale_reconcile(*_args: Any, **kwargs: Any) -> BranchSyncResult:
        persist_calls.append(bool(kwargs.get("persist")))
        result = BranchSyncResult(
            branch=f"feature/{reconcile_case}",
            task_ids=(impl.id or "",),
            merge_status="merged",
            reconciled=True,
            head_sha="source-a",
            base_sha="target-a",
        )
        if reconcile_case == "git_error":
            result.errors.append("git is_merged failed")
        else:
            result.warnings.append("classification unknown; preserved existing merge state")
        return result

    result = LandingCoordinator(
        store=store,
        git=git,
        reconcile_merge_truth=stale_reconcile,
    ).run(LandRequest(task_id=impl.id, dry_run=dry_run))

    assert result.blocked is not None
    assert result.blocked.reason_code == "identity-proof-unavailable"
    assert result.already_merged is False
    assert persist_calls == [False]
    assert _sqlite_task_snapshot(store) == before_tasks
    assert _sqlite_merge_unit_snapshot(store) == before_units


def test_landing_coordinator_reports_persisted_merged_unit_after_source_ref_deleted(tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    impl = _completed_impl(store, "already landed deleted source", "feature/deleted")
    unit = store.get_or_create_merge_unit_for_task(impl)
    assert unit is not None and impl.id is not None
    store.set_merge_unit_state(unit.id, "merged")
    git = _LandingSourceGit({"main": "target-a"}, local_branches=set())

    result = LandingCoordinator(store=store, git=git).run(LandRequest(task_id=impl.id))

    assert result.already_merged is True
    assert result.blocked is None
    assert [step.phase for step in result.steps] == ["resolve", "merge"]
    assert git.mutation_calls == []
    with store._connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM tasks WHERE task_type IN ('rebase', 'review', 'improve')").fetchone()[0] == 0


def test_landing_coordinator_reports_persisted_merged_unit_with_non_actionable_representative(tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    impl = _completed_impl(store, "already landed failed representative", "feature/landed")
    unit = store.get_or_create_merge_unit_for_task(impl)
    assert unit is not None and impl.id is not None
    store.set_merge_unit_state(unit.id, "merged")
    impl.status = "failed"
    store.update(impl)
    git = _LandingSourceGit({"feature/landed": "source-a", "main": "target-a"}, local_branches={"feature/landed"})

    result = LandingCoordinator(store=store, git=git).run(LandRequest(task_id=impl.id))

    assert result.already_merged is True
    assert result.blocked is None
    assert [step.phase for step in result.steps] == ["resolve", "merge"]
    assert git.mutation_calls == []


def test_landing_coordinator_dry_run_has_zero_mutations(tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    impl = _completed_impl(store, "dry run", "feature/dry")
    unit = store.get_or_create_merge_unit_for_task(impl)
    assert unit is not None and impl.id is not None
    before_tasks = _sqlite_task_snapshot(store)
    before_units = _sqlite_merge_unit_snapshot(store)
    git = _LandingSourceGit(
        {"feature/dry": "source-a", "main": "target-a"},
        local_branches={"feature/dry"},
        ancestors=set(),
    )

    result = LandingCoordinator(store=store, git=git).run(LandRequest(task_id=impl.id, dry_run=True))

    assert result.blocked is None
    assert any(step.phase == "rebase" and step.status == "conditional" for step in result.steps)
    assert _sqlite_task_snapshot(store) == before_tasks
    assert _sqlite_merge_unit_snapshot(store) == before_units
    assert git.mutation_calls == []


def test_landing_coordinator_strict_target_resolution_failure_returns_typed_block(tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    impl = store.add("target failure", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime(2026, 8, 26, 12, 0, tzinfo=UTC)
    impl.has_commits = True
    impl.merge_status = "unmerged"
    impl.branch = "feature/target"
    store.update(impl)
    assert impl.id is not None

    def fail_default_target(*, strict: bool) -> str:
        assert strict is True
        raise RuntimeError("target unavailable\nsecondary diagnostic")

    setattr(store, "default_merge_target", fail_default_target)
    git = _LandingSourceGit({"feature/target": "source-a", "main": "target-a"}, local_branches={"feature/target"})

    result = LandingCoordinator(store=store, git=git).run(LandRequest(task_id=impl.id))

    assert result.blocked is not None
    assert result.blocked.reason_code == "identity-proof-unavailable"
    _assert_single_terminal_sentence(result.blocked, impl.id)


def test_landing_coordinator_has_changes_failure_returns_dirty_checkout(tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    impl = _completed_impl(store, "cleanliness failure", "feature/dirty-proof")
    assert impl.id is not None

    class FailingCleanGit(_LandingSourceGit):
        def has_changes(self, include_untracked: bool = False) -> bool:
            raise RuntimeError("status failed\nwith diagnostic")

    git = FailingCleanGit(
        {"feature/dirty-proof": "source-a", "main": "target-a"},
        local_branches={"feature/dirty-proof"},
    )

    result = LandingCoordinator(store=store, git=git).run(LandRequest(task_id=impl.id))

    assert result.blocked is not None
    assert result.blocked.reason_code == "dirty-checkout"
    _assert_single_terminal_sentence(result.blocked, impl.id)


def test_landing_coordinator_merge_truth_failure_returns_typed_identity_block(tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    impl = _completed_impl(store, "merge truth failure", "feature/truth")
    assert impl.id is not None
    git = _LandingSourceGit({"feature/truth": "source-a", "main": "target-a"}, local_branches={"feature/truth"})

    def fail_reconcile(*_args: Any, **_kwargs: Any) -> Any:
        raise RuntimeError("reconcile failed\nmore detail")

    result = LandingCoordinator(
        store=store,
        git=git,
        reconcile_merge_truth=fail_reconcile,
    ).run(LandRequest(task_id=impl.id))

    assert result.blocked is not None
    assert result.blocked.reason_code == "identity-proof-unavailable"
    _assert_single_terminal_sentence(result.blocked, impl.id)


def test_landing_coordinator_merge_truth_failure_precedes_dirty_checkout_when_both_fail(tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    impl = _completed_impl(store, "cleanliness and truth failure", "feature/dirty-and-truth")
    assert impl.id is not None

    class FailingCleanGit(_LandingSourceGit):
        def has_changes(self, include_untracked: bool = False) -> bool:
            raise RuntimeError("status failed")

    git = FailingCleanGit(
        {"feature/dirty-and-truth": "source-a", "main": "target-a"},
        local_branches={"feature/dirty-and-truth"},
    )

    def fail_reconcile(*_args: Any, **_kwargs: Any) -> Any:
        raise RuntimeError("reconcile failed")

    result = LandingCoordinator(
        store=store,
        git=git,
        reconcile_merge_truth=fail_reconcile,
    ).run(LandRequest(task_id=impl.id))

    assert result.blocked is not None
    assert result.blocked.reason_code == "identity-proof-unavailable"
    _assert_single_terminal_sentence(result.blocked, impl.id)


@pytest.mark.parametrize("select", ("review", "owner"))
def test_landing_coordinator_rejects_non_actionable_fallback_representative(tmp_path, select: str) -> None:
    store = _coordinator_store(tmp_path)
    branch = "feature/b1-fallback"
    impl = store.add("failed implement", task_type="implement")
    store.mark_failed(impl, has_commits=True, branch=branch)
    assert impl.id is not None
    review = store.add("completed review", task_type="review", depends_on=impl.id, based_on=impl.id)
    store.mark_completed(review, has_commits=True, branch=branch)
    assert review.id is not None

    git = _LandingSourceGit({branch: "source-a", "main": "target-a"}, local_branches={branch})

    task_id = review.id if select == "review" else impl.id
    result = LandingCoordinator(store=store, git=git).run(LandRequest(task_id=task_id, dry_run=True))

    assert result.blocked is not None
    assert result.blocked.reason_code == "identity-proof-unavailable"


def test_landing_coordinator_ancestry_failure_preserves_unavailable_proof(tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    impl = _completed_impl(store, "ancestry failure", "feature/ancestry")
    assert impl.id is not None

    class FailingAncestryGit(_LandingSourceGit):
        def is_ancestor(self, ancestor: str, descendant: str) -> bool:
            raise RuntimeError("cannot inspect ancestry\nnot a required rebase proof")

    git = FailingAncestryGit(
        {"feature/ancestry": "source-a", "main": "target-a"},
        local_branches={"feature/ancestry"},
    )

    result = LandingCoordinator(store=store, git=git).run(LandRequest(task_id=impl.id))

    assert result.blocked is not None
    assert result.blocked.reason_code == "rebase-or-conflict"
    assert "ancestry proof is unavailable" in result.blocked.fact
    assert "task-backed rebase execution is required" not in result.blocked.fact
    _assert_single_terminal_sentence(result.blocked, impl.id)


def test_landing_coordinator_manual_preflight_failure_returns_typed_rebase_block(monkeypatch, tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    impl = _completed_impl(store, "preflight failure", "feature/preflight")
    assert impl.id is not None
    git = _LandingSourceGit(
        {"feature/preflight": "source-a", "main": "target-a"},
        local_branches={"feature/preflight"},
        ancestors={("target-a", "source-a")},
    )

    def fail_preflight(*_args: Any, **_kwargs: Any) -> Any:
        raise RuntimeError("preflight failed\nmore detail")

    monkeypatch.setattr("gza.landing.check_manual_merge_preflight", fail_preflight)

    result = LandingCoordinator(store=store, git=git).run(LandRequest(task_id=impl.id))

    assert result.blocked is not None
    assert result.blocked.reason_code == "rebase-or-conflict"
    _assert_single_terminal_sentence(result.blocked, impl.id)


def test_landing_coordinator_late_dirty_preflight_keeps_dirty_checkout_precedence(monkeypatch, tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    impl = _completed_impl(store, "late dirty", "feature/late-dirty")
    assert impl.id is not None
    git = _LandingSourceGit(
        {"feature/late-dirty": "source-a", "main": "target-a"},
        local_branches={"feature/late-dirty"},
        ancestors={("target-a", "source-a")},
    )
    monkeypatch.setattr(
        "gza.landing.check_manual_merge_preflight",
        lambda *_args, **_kwargs: SimpleNamespace(ok=False, status="dirty_checkout"),
    )

    result = LandingCoordinator(store=store, git=git).run(LandRequest(task_id=impl.id))

    assert result.blocked is not None
    assert result.blocked.reason_code == "dirty-checkout"
    _assert_single_terminal_sentence(result.blocked, impl.id)


@pytest.mark.parametrize(
    ("failure_site", "expected_reason"),
    (
        ("target-resolution", "identity-proof-unavailable"),
        ("subject-resolution", "identity-proof-unavailable"),
        ("cleanliness", "dirty-checkout"),
        ("reconciliation", "identity-proof-unavailable"),
        ("ancestry", "rebase-or-conflict"),
        ("preflight", "rebase-or-conflict"),
    ),
)
def test_landing_coordinator_exception_facts_render_as_one_sentence(
    monkeypatch,
    tmp_path,
    failure_site: str,
    expected_reason: str,
) -> None:
    store = _coordinator_store(tmp_path)
    if failure_site == "target-resolution":
        impl = store.add(f"{failure_site} exception", task_type="implement")
        assert impl.id is not None
        impl.status = "completed"
        impl.completed_at = datetime(2026, 8, 26, 12, 0, tzinfo=UTC)
        impl.has_commits = True
        impl.merge_status = "unmerged"
        impl.branch = f"feature/{failure_site}"
        store.update(impl)
    else:
        impl = _completed_impl(store, f"{failure_site} exception", f"feature/{failure_site}")
    assert impl.id is not None
    diagnostic = f"{failure_site} failed. secondary diagnostic\nthird line"

    git: Any = _LandingSourceGit(
        {f"feature/{failure_site}": "source-a", "main": "target-a"},
        local_branches={f"feature/{failure_site}"},
        ancestors={("target-a", "source-a")},
    )
    coordinator_kwargs: dict[str, Any] = {}

    if failure_site == "target-resolution":
        def fail_default_target(*, strict: bool) -> str:
            assert strict is True
            raise RuntimeError(diagnostic)

        setattr(store, "default_merge_target", fail_default_target)
    elif failure_site == "subject-resolution":
        def fail_subject(*_args: Any, **_kwargs: Any) -> Any:
            raise RuntimeError(diagnostic)

        coordinator_kwargs["resolve_subject"] = fail_subject
    elif failure_site == "cleanliness":
        class FailingCleanGit(_LandingSourceGit):
            def has_changes(self, include_untracked: bool = False) -> bool:
                raise RuntimeError(diagnostic)

        git = FailingCleanGit(
            {"feature/cleanliness": "source-a", "main": "target-a"},
            local_branches={"feature/cleanliness"},
            ancestors={("target-a", "source-a")},
        )
    elif failure_site == "reconciliation":
        def fail_reconcile(*_args: Any, **_kwargs: Any) -> Any:
            raise RuntimeError(diagnostic)

        coordinator_kwargs["reconcile_merge_truth"] = fail_reconcile
    elif failure_site == "ancestry":
        class FailingAncestryGit(_LandingSourceGit):
            def is_ancestor(self, ancestor: str, descendant: str) -> bool:
                raise RuntimeError(diagnostic)

        git = FailingAncestryGit(
            {"feature/ancestry": "source-a", "main": "target-a"},
            local_branches={"feature/ancestry"},
        )
    elif failure_site == "preflight":
        monkeypatch.setattr(
            "gza.landing.check_manual_merge_preflight",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError(diagnostic)),
        )

    result = LandingCoordinator(store=store, git=git, **coordinator_kwargs).run(LandRequest(task_id=impl.id))

    assert result.blocked is not None
    assert result.blocked.reason_code == expected_reason
    assert "secondary diagnostic" not in result.blocked.terminal_sentence(impl.id)
    _assert_single_terminal_sentence(result.blocked, impl.id)


def test_landing_coordinator_dry_run_reuses_current_verify_and_review_evidence(tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "dry evidence", "feature/landing")
    unit = store.get_or_create_merge_unit_for_task(impl)
    assert unit is not None and impl.id is not None
    _persist_lifecycle_verify_for_landing(store, config, impl)
    review = _completed_full_review(store, impl, head="head-a", verdict="APPROVED")
    before_tasks = _sqlite_task_snapshot(store)
    before_units = _sqlite_merge_unit_snapshot(store)
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-a")},
    )

    result = LandingCoordinator(store=store, git=git, config=config).run(LandRequest(task_id=impl.id, dry_run=True))

    summaries = {step.phase: step.summary for step in result.steps}
    statuses = {step.phase: step.status for step in result.steps}
    assert statuses["verify"] == "completed"
    assert "passed for gate" in summaries["verify"]
    assert statuses["post_rebase_review"] == "completed"
    assert f"review {review.id} is APPROVED" in summaries["post_rebase_review"]
    assert statuses["merge"] == "conditional"
    assert _sqlite_task_snapshot(store) == before_tasks
    assert _sqlite_merge_unit_snapshot(store) == before_units
    assert git.mutation_calls == []


def test_landing_coordinator_strict_dry_run_never_advertises_judge_for_changes_requested(tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "strict changes requested", "feature/landing")
    unit = store.get_or_create_merge_unit_for_task(impl)
    assert unit is not None and impl.id is not None
    _persist_lifecycle_verify_for_landing(store, config, impl)
    _completed_full_review(store, impl, head="head-a", verdict="CHANGES_REQUESTED")
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-a")},
    )

    result = LandingCoordinator(store=store, git=git, config=config).run(
        LandRequest(task_id=impl.id, policy="strict", dry_run=True)
    )

    assert all(step.phase != "judge" for step in result.steps)
    assert result.blocked is not None
    assert result.blocked.reason_code == "nondeferrable-blocker"


def _review_report_with_findings(
    verdict: str,
    *,
    blockers: tuple[tuple[str, str, str], ...] = (),
    followups: tuple[tuple[str, str, str], ...] = (),
) -> str:
    blocker_body = "None."
    if blockers:
        blocker_body = "\n\n".join(
            (
                f"### {finding_id} {title}\n"
                f"Evidence: `{path}` shows the issue.\n"
                "Impact: The landing decision would be wrong.\n"
                f"Required fix: Resolve {title} at `{path}`.\n"
                "Required tests: Add store-backed landing coverage.\n"
                f"Open-state citation: `{path}`"
            )
            for finding_id, title, path in blockers
        )
    followup_body = "None."
    if followups:
        followup_body = "\n\n".join(
            (
                f"### {finding_id} {title}\n"
                f"Evidence: `{path}` needs later work.\n"
                "Impact: Follow-up work should be tracked.\n"
                f"Recommended follow-up: Track {title} at `{path}`.\n"
                "Recommended tests: Add focused coverage.\n"
                f"Open-state citation: `{path}`"
            )
            for finding_id, title, path in followups
        )
    return (
        "## Summary\n\nReview result.\n\n"
        f"## Blockers\n\n{blocker_body}\n\n"
        f"## Follow-Ups\n\n{followup_body}\n\n"
        "## Questions / Assumptions\n\nNone.\n\n"
        f"## Verdict\n\nVerdict: {verdict}\n"
    )


def _finding_fingerprint_metadata(title: str, path: str) -> dict[str, str]:
    return {
        "title": title.lower(),
        "anchor": path.lower(),
    }


def _add_review_blocker_resolution(
    store: SqliteTaskStore,
    *,
    impl: Task,
    review: Task,
    finding_id: str = "B1",
    title: str = "Out-of-scope polish debt",
    path: str = "docs/internal/landing.md:12",
    state: str = "invalid",
    head: str = "head-a",
    impl_task_id: str | None = None,
    review_task_id: str | None = None,
    target_head: str | None = "target-a",
    reason: str = "out_of_scope",
    metadata: dict[str, Any] | None = None,
    artifact_id: int | None = None,
) -> Any:
    payload = {
        "schema_version": 1,
        "state": state,
        "review_task_id": review_task_id if review_task_id is not None else review.id,
        "impl_task_id": impl_task_id if impl_task_id is not None else impl.id,
        "source_task_id": impl.id,
        "source_task_type": impl.task_type,
        "finding_id": finding_id,
        "finding_fingerprint": _finding_fingerprint_metadata(title, path),
        "head_sha": head,
        "target_head_sha": target_head,
        "reason": reason,
    }
    if metadata is not None:
        payload.update(metadata)
    body = json.dumps(payload, sort_keys=True)
    return store.add_artifact(
        review.id or "",
        kind="review_blocker_resolution",
        label=f"{state}-{finding_id}",
        path=f".gza/artifacts/{review.id}/resolution-{finding_id}-{state}-{len(body)}.json",
        byte_size=len(body.encode()),
        sha256=sha256(body.encode()).hexdigest(),
        metadata=payload,
        status=state,
        head_sha=head,
        artifact_id=artifact_id,
    )


def test_landing_coordinator_store_backed_dry_run_refuses_nondeferrable_blocker(tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "store-backed blocker", "feature/landing")
    assert impl.id is not None
    _persist_lifecycle_verify_for_landing(store, config, impl)
    review = _completed_full_review(store, impl, head="head-a", verdict="CHANGES_REQUESTED")
    review.output_content = _review_report_with_findings(
        "CHANGES_REQUESTED",
        blockers=(("B1", "Correctness nondeferrable blocker", "src/gza/landing.py:10"),),
    )
    store.update(review)
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-a")},
    )

    result = LandingCoordinator(store=store, git=git, config=config).run(
        LandRequest(task_id=impl.id, dry_run=True)
    )

    assert result.blocked is not None
    assert result.blocked.reason_code == "nondeferrable-blocker"
    assert result.blocked.fact == "review blocker B1 is non-deferable"
    assert "review:" + (review.id or "") in result.blocked.evidence_refs
    assert all(step.phase != "judge" for step in result.steps)


@pytest.mark.parametrize(
    "blocker_title",
    (
        "Correctness defect in out of scope path",
        "Integration-contract adjacent state corruption",
    ),
)
def test_landing_coordinator_store_backed_dry_run_does_not_infer_deferrable_from_prose(
    tmp_path,
    blocker_title: str,
) -> None:
    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "store-backed misleading prose", "feature/landing")
    assert impl.id is not None
    _persist_lifecycle_verify_for_landing(store, config, impl)
    review = _completed_full_review(store, impl, head="head-a", verdict="CHANGES_REQUESTED")
    review.output_content = _review_report_with_findings(
        "CHANGES_REQUESTED",
        blockers=(("B1", blocker_title, "src/gza/landing.py:10"),),
    )
    store.update(review)
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-a")},
    )

    result = LandingCoordinator(store=store, git=git, config=config).run(
        LandRequest(task_id=impl.id, dry_run=True)
    )

    assert result.blocked is not None
    assert result.blocked.reason_code == "nondeferrable-blocker"
    assert all(step.phase != "judge" for step in result.steps)


def test_landing_coordinator_store_backed_dry_run_requires_authoritative_deferrable_classification(
    tmp_path,
) -> None:
    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "store-backed missing classification", "feature/landing")
    assert impl.id is not None
    _persist_lifecycle_verify_for_landing(store, config, impl)
    review = _completed_full_review(store, impl, head="head-a", verdict="CHANGES_REQUESTED")
    review.output_content = _review_report_with_findings(
        "CHANGES_REQUESTED",
        blockers=(("B1", "Out-of-scope polish debt", "docs/internal/landing.md:12"),),
    )
    store.update(review)
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-a")},
    )

    result = LandingCoordinator(store=store, git=git, config=config).run(
        LandRequest(task_id=impl.id, dry_run=True)
    )

    assert result.blocked is not None
    assert result.blocked.reason_code == "nondeferrable-blocker"
    assert all(step.phase != "judge" for step in result.steps)


def test_landing_coordinator_store_backed_dry_run_advertises_judge_for_current_deferrable_classification(
    tmp_path,
) -> None:
    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "store-backed judge", "feature/landing")
    assert impl.id is not None
    _persist_lifecycle_verify_for_landing(store, config, impl)
    review = _completed_full_review(store, impl, head="head-a", verdict="CHANGES_REQUESTED")
    review.output_content = _review_report_with_findings(
        "CHANGES_REQUESTED",
        blockers=(
            ("B1", "Out-of-scope polish debt", "docs/internal/landing.md:12"),
        ),
    )
    store.update(review)
    _add_review_blocker_resolution(store, impl=impl, review=review)
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-a")},
    )

    result = LandingCoordinator(store=store, git=git, config=config).run(
        LandRequest(task_id=impl.id, dry_run=True)
    )

    statuses = {step.phase: step.status for step in result.steps}
    assert result.blocked is None
    assert statuses["judge"] == "conditional"
    assert "merge" not in statuses


def test_landing_coordinator_store_backed_dry_run_materializes_followups_before_merge(tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "store-backed followup", "feature/landing")
    assert impl.id is not None
    _persist_lifecycle_verify_for_landing(store, config, impl)
    review = _completed_full_review(store, impl, head="head-a", verdict="APPROVED_WITH_FOLLOWUPS")
    review.output_content = _review_report_with_findings(
        "APPROVED_WITH_FOLLOWUPS",
        followups=(("F1", "Follow-up materialization", "src/gza/landing.py:20"),),
    )
    store.update(review)
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-a")},
    )

    result = LandingCoordinator(store=store, git=git, config=config).run(
        LandRequest(task_id=impl.id, dry_run=True)
    )

    statuses = {step.phase: step.status for step in result.steps}
    assert result.blocked is None
    assert statuses["defer_blockers"] == "conditional"
    assert "merge" not in statuses


def _park_for_review_blocker_adjudication(store: SqliteTaskStore, impl: Task) -> None:
    store.upsert_watch_progress_observation(
        WatchProgressObservation(
            subject_kind="task",
            subject_id=impl.id or "",
            subject_task_id=impl.id,
            action_type="max_cycles_reached",
            action_reason="review-blocker-adjudication-needed",
            evidence_fingerprint="park-adjudication-needed",
            parked_reason="review-blocker-adjudication-needed",
            observed_at=datetime(2026, 8, 26, 12, 0, tzinfo=UTC),
        )
    )


def _blocked_adjudication_dry_run(
    tmp_path,
    *,
    resolution_kwargs: dict[str, Any] | None = None,
    blockers: tuple[tuple[str, str, str], ...] = (("B1", "Out-of-scope polish debt", "docs/internal/landing.md:12"),),
) -> tuple[Any, Any, Any, LandingCoordinator]:
    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "store-backed adjudication", "feature/landing")
    assert impl.id is not None
    _persist_lifecycle_verify_for_landing(store, config, impl)
    review = _completed_full_review(store, impl, head="head-a", verdict="CHANGES_REQUESTED")
    review.output_content = _review_report_with_findings("CHANGES_REQUESTED", blockers=blockers)
    store.update(review)
    _park_for_review_blocker_adjudication(store, impl)
    if resolution_kwargs is not None:
        _add_review_blocker_resolution(store, impl=impl, review=review, **resolution_kwargs)
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-a")},
    )
    coordinator = LandingCoordinator(store=store, git=git, config=config)
    result = coordinator.run(LandRequest(task_id=impl.id, dry_run=True))
    return result, impl, review, coordinator


@pytest.mark.parametrize(
    "resolution_kwargs",
    (
        {"state": "disputed"},
        {"impl_task_id": "gza-wrong"},
        {"review_task_id": "gza-wrong"},
        {"head": "old-head"},
        {"path": "docs/internal/other.md:99"},
        {"state": "needs_human"},
        {"metadata": {"finding_fingerprint": "malformed"}},
    ),
)
def test_landing_coordinator_store_backed_park_refuses_incomplete_adjudication_evidence(
    tmp_path,
    resolution_kwargs: dict[str, Any],
) -> None:
    result, impl, _review, coordinator = _blocked_adjudication_dry_run(
        tmp_path,
        resolution_kwargs=resolution_kwargs,
    )
    identity = coordinator._resolve_identity(LandRequest(task_id=impl.id), persist_reconciliation=False)
    assert not isinstance(identity, LandBlocked)
    facts = coordinator._landing_policy_facts(identity)

    assert result.blocked is not None
    assert result.blocked.reason_code in {"nondeferrable-blocker", "policy-or-judge-refused"}
    assert all(step.phase != "judge" for step in result.steps)
    assert facts.review_blocker_adjudication_evidence_complete is False
    assert any(
        item.startswith("review-blocker-resolution-incomplete") or item == "review-blocker-resolution-read-unavailable"
        for item in facts.adjudication_fingerprints
    )


def test_landing_coordinator_store_backed_park_refuses_partial_adjudication_set(tmp_path) -> None:
    result, impl, _review, coordinator = _blocked_adjudication_dry_run(
        tmp_path,
        blockers=(
            ("B1", "Out-of-scope polish debt", "docs/internal/landing.md:12"),
            ("B2", "Adjacent cleanup debt", "docs/internal/landing.md:18"),
        ),
        resolution_kwargs={"finding_id": "B1", "title": "Out-of-scope polish debt", "path": "docs/internal/landing.md:12"},
    )
    identity = coordinator._resolve_identity(LandRequest(task_id=impl.id), persist_reconciliation=False)
    assert not isinstance(identity, LandBlocked)
    facts = coordinator._landing_policy_facts(identity)

    assert result.blocked is not None
    assert all(step.phase != "judge" for step in result.steps)
    assert facts.review_blocker_adjudication_evidence_complete is False


def test_landing_coordinator_store_backed_current_complete_adjudication_fingerprint_is_exact(tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "store-backed complete adjudication", "feature/landing")
    assert impl.id is not None
    _persist_lifecycle_verify_for_landing(store, config, impl)
    review = _completed_full_review(store, impl, head="head-a", verdict="CHANGES_REQUESTED")
    review.output_content = _review_report_with_findings(
        "CHANGES_REQUESTED",
        blockers=(("B1", "Out-of-scope polish debt", "docs/internal/landing.md:12"),),
    )
    store.update(review)
    _park_for_review_blocker_adjudication(store, impl)
    artifact = _add_review_blocker_resolution(store, impl=impl, review=review)
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-a")},
    )
    coordinator = LandingCoordinator(store=store, git=git, config=config)
    identity = coordinator._resolve_identity(LandRequest(task_id=impl.id), persist_reconciliation=False)
    assert not isinstance(identity, LandBlocked)
    first_facts = coordinator._landing_policy_facts(identity)
    first_fingerprint = LandingStateFingerprint.from_facts(first_facts)

    _add_review_blocker_resolution(store, impl=impl, review=review, impl_task_id="gza-wrong", artifact_id=artifact.id)
    second_facts = coordinator._landing_policy_facts(identity)
    second_fingerprint = LandingStateFingerprint.from_facts(second_facts)

    assert first_facts.review_blocker_adjudication_evidence_complete is True
    assert second_facts.review_blocker_adjudication_evidence_complete is False
    assert first_fingerprint != second_fingerprint
    assert all(not item.startswith("review-blocker-resolution-incomplete") for item in first_facts.adjudication_fingerprints)


def test_landing_coordinator_store_backed_blocker_count_mismatch_fails_closed(monkeypatch, tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "store-backed count mismatch", "feature/landing")
    assert impl.id is not None
    _persist_lifecycle_verify_for_landing(store, config, impl)
    review = _completed_full_review(store, impl, head="head-a", verdict="CHANGES_REQUESTED")
    review.output_content = _review_report_with_findings(
        "CHANGES_REQUESTED",
        blockers=(("B1", "Out-of-scope blocker", "docs/internal/landing.md:12"),),
    )
    store.update(review)
    monkeypatch.setattr(
        "gza.landing.summarize_review_blockers",
        lambda _content: SimpleNamespace(blocker_count=2),
    )
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-a")},
    )
    coordinator = LandingCoordinator(store=store, git=git, config=config)

    result = coordinator.run(LandRequest(task_id=impl.id, dry_run=True))
    assert result.owner_task_id == impl.id
    identity = coordinator._resolve_identity(LandRequest(task_id=impl.id), persist_reconciliation=False)
    assert not isinstance(identity, LandBlocked)
    fingerprint = LandingStateFingerprint.from_facts(coordinator._landing_policy_facts(identity))

    assert result.blocked is not None
    assert result.blocked.reason_code == "nondeferrable-blocker"
    assert result.blocked.fact == "review blocker invalid-blocker-count-mismatch is non-deferable"
    assert f"invalid-review-blockers:{review.id}:blocker-count-mismatch" in fingerprint.blocker_fingerprints


def test_landing_coordinator_store_backed_followup_count_disagreement_fails_closed(tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "store-backed followup mismatch", "feature/landing")
    assert impl.id is not None
    _persist_lifecycle_verify_for_landing(store, config, impl)
    review = _completed_full_review(store, impl, head="head-a", verdict="APPROVED_WITH_FOLLOWUPS")
    review.output_content = _review_report_with_findings("APPROVED_WITH_FOLLOWUPS")
    store.update(review)
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-a")},
    )
    coordinator = LandingCoordinator(store=store, git=git, config=config)

    result = coordinator.run(LandRequest(task_id=impl.id, dry_run=True))
    assert result.owner_task_id == impl.id
    identity = coordinator._resolve_identity(LandRequest(task_id=impl.id), persist_reconciliation=False)
    assert not isinstance(identity, LandBlocked)
    fingerprint = LandingStateFingerprint.from_facts(coordinator._landing_policy_facts(identity))

    decision = evaluate_landing_policy(policy="guarded", facts=coordinator._landing_policy_facts(identity))
    assert decision.blocked is not None
    assert decision.blocked.reason_code == "required-review-unavailable"
    assert f"invalid-review-followups:{review.id}:followup-count-mismatch" in fingerprint.blocker_fingerprints


@pytest.mark.parametrize(
    ("findings", "expected_reason"),
    (
        (
            (ReviewFinding("", "FOLLOWUP", "Missing ID", "", "src/gza/landing.py:1", "impact", "fix", "tests"),),
            "followup-missing-finding-id",
        ),
        (
            (ReviewFinding("F1", "FOLLOWUP", "Missing fingerprint", "", "evidence", "impact", None, "tests", None),),
            "followup-missing-fingerprint",
        ),
        (
            (
                ReviewFinding("F1", "FOLLOWUP", "Duplicate ID A", "", "src/gza/landing.py:1", "impact", "fix", "tests", "src/gza/landing.py:1"),
                ReviewFinding("F1", "FOLLOWUP", "Duplicate ID B", "", "src/gza/landing.py:2", "impact", "fix", "tests", "src/gza/landing.py:2"),
            ),
            "followup-duplicate-finding-id",
        ),
        (
            (
                ReviewFinding("F1", "FOLLOWUP", "Duplicate followup", "", "src/gza/landing.py:1", "impact", "fix", "tests", "src/gza/landing.py:1"),
                ReviewFinding("F2", "FOLLOWUP", "Duplicate followup", "", "src/gza/landing.py:1", "impact", "fix", "tests", "src/gza/landing.py:1"),
            ),
            "followup-duplicate-fingerprint",
        ),
    ),
)
def test_landing_coordinator_store_backed_followup_invalid_identity_fails_closed(
    monkeypatch,
    tmp_path,
    findings: tuple[ReviewFinding, ...],
    expected_reason: str,
) -> None:
    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "store-backed followup invalid identity", "feature/landing")
    assert impl.id is not None
    _persist_lifecycle_verify_for_landing(store, config, impl)
    review = _completed_full_review(store, impl, head="head-a", verdict="APPROVED_WITH_FOLLOWUPS")
    review.output_content = _review_report_with_findings(
        "APPROVED_WITH_FOLLOWUPS",
        followups=(("F1", "placeholder", "src/gza/landing.py:1"),),
    )
    store.update(review)
    parsed = ParsedReviewReport(
        verdict="APPROVED_WITH_FOLLOWUPS",
        findings=findings,
        format_version="legacy",
    )
    monkeypatch.setattr("gza.landing._landing_review_report_from_task", lambda _config, _task: parsed)
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-a")},
    )
    coordinator = LandingCoordinator(store=store, git=git, config=config)

    result = coordinator.run(LandRequest(task_id=impl.id, dry_run=True))
    assert result.owner_task_id == impl.id
    identity = coordinator._resolve_identity(LandRequest(task_id=impl.id), persist_reconciliation=False)
    assert not isinstance(identity, LandBlocked)
    fingerprint = LandingStateFingerprint.from_facts(coordinator._landing_policy_facts(identity))

    decision = evaluate_landing_policy(policy="guarded", facts=coordinator._landing_policy_facts(identity))
    assert decision.blocked is not None
    assert decision.blocked.reason_code == "required-review-unavailable"
    assert f"invalid-review-followups:{review.id}:{expected_reason}" in fingerprint.blocker_fingerprints


@pytest.mark.parametrize(
    ("findings", "expected_reason"),
    (
        (
            (
                ReviewFinding("B1", "BLOCKER", "Duplicate ID A", "", "src/gza/landing.py:1", "impact", "fix", "tests", "src/gza/landing.py:1"),
                ReviewFinding("B1", "BLOCKER", "Duplicate ID B", "", "src/gza/landing.py:2", "impact", "fix", "tests", "src/gza/landing.py:2"),
            ),
            "blocker-duplicate-finding-id",
        ),
        (
            (
                ReviewFinding("B1", "BLOCKER", "Duplicate blocker", "", "src/gza/landing.py:1", "impact", "fix", "tests", "src/gza/landing.py:1"),
                ReviewFinding("B2", "BLOCKER", "Duplicate blocker", "", "src/gza/landing.py:1", "impact", "fix", "tests", "src/gza/landing.py:1"),
            ),
            "blocker-duplicate-fingerprint",
        ),
    ),
)
def test_landing_coordinator_store_backed_blocker_identity_mismatch_fails_closed(
    monkeypatch,
    tmp_path,
    findings: tuple[ReviewFinding, ...],
    expected_reason: str,
) -> None:
    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "store-backed blocker invalid identity", "feature/landing")
    assert impl.id is not None
    _persist_lifecycle_verify_for_landing(store, config, impl)
    review = _completed_full_review(store, impl, head="head-a", verdict="CHANGES_REQUESTED")
    review.output_content = _review_report_with_findings(
        "CHANGES_REQUESTED",
        blockers=(("B1", "placeholder", "src/gza/landing.py:1"),),
    )
    store.update(review)
    parsed = ParsedReviewReport(
        verdict="CHANGES_REQUESTED",
        findings=findings,
        format_version="legacy",
    )
    monkeypatch.setattr("gza.landing._landing_review_report_from_task", lambda _config, _task: parsed)
    monkeypatch.setattr(
        "gza.landing.summarize_review_blockers",
        lambda _content: SimpleNamespace(blocker_count=len(findings)),
    )
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-a")},
    )
    coordinator = LandingCoordinator(store=store, git=git, config=config)

    result = coordinator.run(LandRequest(task_id=impl.id, dry_run=True))
    identity = coordinator._resolve_identity(LandRequest(task_id=impl.id), persist_reconciliation=False)
    assert not isinstance(identity, LandBlocked)
    fingerprint = LandingStateFingerprint.from_facts(coordinator._landing_policy_facts(identity))

    decision = evaluate_landing_policy(policy="guarded", facts=coordinator._landing_policy_facts(identity))
    assert decision.blocked is not None
    assert decision.blocked.reason_code == "required-review-unavailable"
    assert f"invalid-review-blockers:{review.id}:{expected_reason}" in fingerprint.blocker_fingerprints


def test_landing_followup_materialization_rejects_source_review_identity_mismatch() -> None:
    review = _review(
        verdict="APPROVED_WITH_FOLLOWUPS",
        followup_findings=(LandingFollowupFinding("F1", fingerprint="followup:f1", source="review:gza-other"),),
    )

    decision = evaluate_landing_policy(policy="guarded", facts=_green_facts(review=review))

    assert decision.allowed is False
    assert decision.blocked is not None
    assert decision.blocked.reason_code == "required-review-unavailable"


def test_landing_coordinator_production_fingerprint_tracks_park_and_judgment_identity_changes(tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "store-backed fingerprint identity", "feature/landing")
    assert impl.id is not None
    _persist_lifecycle_verify_for_landing(store, config, impl)
    _completed_full_review(store, impl, head="head-a", verdict="APPROVED")
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-a")},
    )
    coordinator = LandingCoordinator(store=store, git=git, config=config)
    identity = coordinator._resolve_identity(LandRequest(task_id=impl.id), persist_reconciliation=False)
    assert not isinstance(identity, LandBlocked)

    first = LandingStateFingerprint.from_facts(coordinator._landing_policy_facts(identity))
    store.upsert_watch_progress_observation(
        WatchProgressObservation(
            subject_kind="task",
            subject_id=impl.id,
            subject_task_id=impl.id,
            action_type="max_cycles_reached",
            action_reason="review-max-cycles-reached",
            evidence_fingerprint="park-a",
            parked_reason="review-max-cycles-reached",
            observed_at=datetime(2026, 8, 26, 12, 0, tzinfo=UTC),
        )
    )
    payload = {"key": "judgment-key-a", "identity": {"source": "a"}}
    store.add_artifact(
        impl.id,
        kind="landing_judgment",
        label="landing_judgment",
        path=f".gza/artifacts/{impl.id}/landing-judgment.json",
        byte_size=len(json.dumps(payload).encode()),
        sha256=sha256(json.dumps(payload).encode()).hexdigest(),
        metadata=payload,
        status="LAND",
        head_sha="head-a",
    )
    second = LandingStateFingerprint.from_facts(coordinator._landing_policy_facts(identity))

    assert first != second
    assert first.parked_reason is None
    assert second.parked_reason == "review-max-cycles-reached"
    assert second.policy_judgment_identity is not None
    assert second.policy_judgment_identity.startswith("artifact:")


def test_landing_coordinator_dry_run_uses_merge_unit_attached_code_review_evidence(tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "merge-unit review", "feature/landing")
    sibling = store.add("sibling", task_type="implement", based_on=impl.id, same_branch=True)
    store.mark_completed(sibling, has_commits=True, branch="feature/landing")
    unit = store.get_or_create_merge_unit_for_task(impl)
    assert unit is not None and impl.id is not None and sibling.id is not None
    store.attach_task_to_merge_unit(sibling.id, unit.id, "member")
    _persist_lifecycle_verify_for_landing(store, config, impl)
    _persist_lifecycle_verify_for_landing(store, config, sibling)
    review = store.add("merge-unit attached review", task_type="review", depends_on=sibling.id, based_on=sibling.id)
    review.status = "completed"
    review.completed_at = datetime(2026, 8, 26, 12, 30, tzinfo=UTC)
    review.review_verify_head_sha = "head-a"
    review.output_content = _review_report("APPROVED")
    store.update(review)
    store.attach_task_to_merge_unit(review.id or "", unit.id, "review")
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-a")},
    )

    result = LandingCoordinator(store=store, git=git, config=config).run(LandRequest(task_id=impl.id, dry_run=True))

    statuses = {step.phase: step.status for step in result.steps}
    summaries = {step.phase: step.summary for step in result.steps}
    fingerprint = LandingStateFingerprint.from_facts(
        LandingCoordinator(store=store, git=git, config=config)._landing_policy_facts(
            LandingCoordinator(store=store, git=git, config=config)._resolve_identity(
                LandRequest(task_id=impl.id), persist_reconciliation=False
            )
        )
    )
    assert statuses["post_rebase_review"] == "completed"
    assert f"review {review.id} is APPROVED" in summaries["post_rebase_review"]
    assert fingerprint.review.review_id == review.id


def test_landing_coordinator_dry_run_uses_automatic_review_recovery_descendant(tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "review recovery", "feature/landing")
    assert impl.id is not None
    _persist_lifecycle_verify_for_landing(store, config, impl)
    failed = _completed_full_review(store, impl, head="old-head", verdict="APPROVED")
    retry = store.add("review retry", task_type="review", based_on=failed.id, recovery_origin="retry")
    retry.status = "completed"
    retry.completed_at = datetime(2026, 8, 26, 12, 30, tzinfo=UTC)
    retry.review_verify_head_sha = "head-a"
    retry.output_content = _review_report("APPROVED")
    store.update(retry)
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-a")},
    )
    coordinator = LandingCoordinator(store=store, git=git, config=config)

    result = coordinator.run(LandRequest(task_id=impl.id, dry_run=True))
    identity = coordinator._resolve_identity(LandRequest(task_id=impl.id), persist_reconciliation=False)
    assert not isinstance(identity, LandBlocked)
    fingerprint = LandingStateFingerprint.from_facts(coordinator._landing_policy_facts(identity))

    statuses = {step.phase: step.status for step in result.steps}
    assert statuses["post_rebase_review"] == "completed"
    assert fingerprint.review.review_id == retry.id


def test_landing_coordinator_dry_run_uses_merge_unit_attached_spec_review_evidence(tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    config.spec_coherence.enabled = True
    config.spec_coherence.paths = ("specs/behavior/**",)
    impl = _completed_impl(store, "spec merge-unit review", "feature/landing")
    unit = store.get_or_create_merge_unit_for_task(impl)
    assert unit is not None and impl.id is not None
    _persist_lifecycle_verify_for_landing(store, config, impl)
    _completed_full_review(store, impl, head="head-a", verdict="APPROVED")
    spec_review = _completed_spec_review(
        store,
        impl,
        head="head-a",
        changed_paths=("specs/behavior/lifecycle-engine.md",),
        verdict="APPROVED",
        completed_at=datetime(2026, 8, 26, 12, 30, tzinfo=UTC),
    )
    store.attach_task_to_merge_unit(spec_review.id or "", unit.id, "review")
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-a")},
        name_status="M\tspecs/behavior/lifecycle-engine.md\n",
    )
    coordinator = LandingCoordinator(store=store, git=git, config=config)

    result = coordinator.run(LandRequest(task_id=impl.id, dry_run=True))
    identity = coordinator._resolve_identity(LandRequest(task_id=impl.id), persist_reconciliation=False)
    assert not isinstance(identity, LandBlocked)
    fingerprint = LandingStateFingerprint.from_facts(coordinator._landing_policy_facts(identity))

    statuses = {step.phase: step.status for step in result.steps}
    assert statuses["spec_coherence"] == "completed"
    assert fingerprint.spec_coherence.task_or_artifact_id == spec_review.id


def test_landing_coordinator_fingerprint_uses_provider_resolved_rebase_retry_descendant(tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    impl = _completed_impl(store, "rebase retry", "feature/rebase-retry")
    assert impl.id is not None
    first = store.add("failed rebase", task_type="rebase", based_on=impl.id, same_branch=True)
    first.status = "failed"
    first.branch = "feature/rebase-retry"
    store.update(first)
    retry = store.add("retry rebase", task_type="rebase", based_on=first.id, same_branch=True, recovery_origin="retry")
    store.mark_completed(retry, has_commits=True, branch="feature/rebase-retry", changed_diff=False)
    artifact = _persist_landing_rebase_outcome(store, retry, impl)
    git = _LandingSourceGit(
        {"feature/rebase-retry": "source-a", "main": "target-a"},
        local_branches={"feature/rebase-retry"},
        ancestors={("target-a", "source-a")},
    )
    coordinator = LandingCoordinator(store=store, git=git)
    identity = coordinator._resolve_identity(LandRequest(task_id=impl.id), persist_reconciliation=False)
    assert not isinstance(identity, LandBlocked)

    fingerprint = LandingStateFingerprint.from_facts(coordinator._landing_policy_facts(identity))

    assert fingerprint.rebase.outcome_id == str(artifact.id)
    assert fingerprint.rebase.resolution_kind == "provider_resolved"
    assert fingerprint.rebase.attempted_source_head == "source-before"
    assert fingerprint.rebase.attempted_target_head == "target-a"


def test_landing_coordinator_dry_run_stops_at_stale_verify_before_review(tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    impl = _completed_impl(store, "stale verify", "feature/stale-verify")
    assert impl.id is not None
    _completed_full_review(store, impl, head="source-a", verdict="APPROVED")
    git = _LandingSourceGit(
        {"feature/stale-verify": "source-a", "main": "target-a"},
        local_branches={"feature/stale-verify"},
        ancestors={("target-a", "source-a")},
    )

    result = LandingCoordinator(store=store, git=git).run(LandRequest(task_id=impl.id, dry_run=True))

    assert any(step.phase == "verify" and step.status == "conditional" for step in result.steps)
    assert all(step.phase != "post_rebase_review" for step in result.steps)


def test_landing_coordinator_dry_run_stops_at_stale_review_after_green_verify(tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "stale review", "feature/landing")
    assert impl.id is not None
    _persist_lifecycle_verify_for_landing(store, config, impl)
    _completed_full_review(store, impl, head="old-head", verdict="APPROVED")
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-a")},
    )

    result = LandingCoordinator(store=store, git=git, config=config).run(LandRequest(task_id=impl.id, dry_run=True))

    statuses = {step.phase: step.status for step in result.steps}
    assert statuses["verify"] == "completed"
    assert statuses["post_rebase_review"] == "conditional"
    assert "review" not in {step.phase for step in result.steps if step.phase == "merge"}


def test_landing_coordinator_dry_run_stops_at_malformed_resolution_review_scope(
    tmp_path,
) -> None:
    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "malformed resolution scope", "feature/landing")
    assert impl.id is not None
    _persist_lifecycle_verify_for_landing(store, config, impl)
    rebase = store.add("Completed rebase", task_type="rebase", based_on=impl.id, same_branch=True)
    store.mark_completed(rebase, has_commits=True, branch="feature/landing", changed_diff=False)
    _persist_landing_rebase_outcome(store, rebase, impl)
    review = store.add("Malformed resolution review", task_type="review", depends_on=impl.id, based_on=impl.id)
    review.status = "completed"
    review.completed_at = datetime(2026, 8, 26, 12, 30, tzinfo=UTC)
    review.review_verify_head_sha = "head-a"
    review.review_scope = "\n".join(
        (
            "Review mode: resolution",
            f"Implementation task: {impl.id}",
            f"Rebase task: {rebase.id}",
            "Resolved head SHA: head-a",
            "",
            "Review only the conflict-resolution delta introduced by this rebase.",
        )
    )
    review.output_content = _review_report("APPROVED")
    store.update(review)
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-a")},
    )

    result = LandingCoordinator(store=store, git=git, config=config).run(
        LandRequest(task_id=impl.id, dry_run=True)
    )

    statuses = {step.phase: step.status for step in result.steps}
    assert statuses["verify"] == "completed"
    assert statuses["post_rebase_review"] == "conditional"
    assert all(step.phase != "merge" for step in result.steps)
    assert result.blocked is None


def test_landing_coordinator_dry_run_stops_at_resolution_review_with_stale_target_scope(
    tmp_path,
) -> None:
    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "stale resolution target", "feature/landing")
    assert impl.id is not None
    _persist_lifecycle_verify_for_landing(store, config, impl)
    rebase = store.add("Completed rebase", task_type="rebase", based_on=impl.id, same_branch=True)
    store.mark_completed(rebase, has_commits=True, branch="feature/landing", changed_diff=False)
    _persist_landing_rebase_outcome(store, rebase, impl)
    _resolution_review(
        store,
        impl,
        rebase,
        status="completed",
        resolved_head="head-a",
        target="target-old",
        verify_head="head-a",
        verdict="APPROVED",
        completed_at=datetime(2026, 8, 26, 12, 30, tzinfo=UTC),
    )
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-a")},
    )

    result = LandingCoordinator(store=store, git=git, config=config).run(
        LandRequest(task_id=impl.id, dry_run=True)
    )

    statuses = {step.phase: step.status for step in result.steps}
    assert statuses["verify"] == "completed"
    assert statuses["post_rebase_review"] == "conditional"
    assert all(step.phase != "merge" for step in result.steps)
    assert result.blocked is None


def test_landing_coordinator_repeated_fingerprint_returns_bounded_without_later_policy_simulation(tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    impl = _completed_impl(store, "repeat bounded", "feature/repeat-bounded")
    assert impl.id is not None
    before_tasks = _sqlite_task_snapshot(store)
    before_units = _sqlite_merge_unit_snapshot(store)
    git = _LandingSourceGit(
        {"feature/repeat-bounded": "source-a", "main": "target-a"},
        local_branches={"feature/repeat-bounded"},
        ancestors={("target-a", "source-a")},
    )
    facts = _green_facts(
        review=_review(verdict="CHANGES_REQUESTED"),
        open_blockers=(_blocker("B1", deferrable=True, blocker_class="out_of_scope"),),
        parked_reason="review-max-cycles-reached",
    )
    calls = 0

    def inspect(_identity: Any) -> LandingPolicyFacts:
        nonlocal calls
        calls += 1
        return facts

    result = LandingCoordinator(
        store=store,
        git=git,
        inspect_policy_facts=inspect,
        should_re_resolve=lambda *_args: True,
    ).run(LandRequest(task_id=impl.id))

    assert result.blocked is not None
    assert result.blocked.reason_code == "bounded-attempt-exhausted"
    assert "revisited the same decision state" in result.blocked.fact
    assert "judge" not in result.blocked.fact
    assert calls == 2
    assert _sqlite_task_snapshot(store) == before_tasks
    assert _sqlite_merge_unit_snapshot(store) == before_units
    assert git.mutation_calls == []
    _assert_single_terminal_sentence(result.blocked, impl.id)


def test_landing_coordinator_transition_cap_returns_bounded_without_later_policy_simulation(tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    impl = _completed_impl(store, "cap bounded", "feature/cap-bounded")
    assert impl.id is not None
    before_tasks = _sqlite_task_snapshot(store)
    before_units = _sqlite_merge_unit_snapshot(store)
    git = _LandingSourceGit(
        {"feature/cap-bounded": "source-a", "main": "target-a"},
        local_branches={"feature/cap-bounded"},
        ancestors={("target-a", "source-a")},
    )
    facts = [
        _green_facts(
            review=_review(verdict="CHANGES_REQUESTED", review_id="gza-200"),
            open_blockers=(_blocker("B1", deferrable=True, blocker_class="out_of_scope"),),
            parked_reason="review-max-cycles-reached",
        ),
        _green_facts(
            review=_review(verdict="CHANGES_REQUESTED", review_id="gza-201"),
            open_blockers=(_blocker("B1", deferrable=True, blocker_class="out_of_scope"),),
            parked_reason="review-max-cycles-reached",
        ),
    ]
    calls = 0

    def inspect(_identity: Any) -> LandingPolicyFacts:
        nonlocal calls
        value = facts[calls]
        calls += 1
        return value

    result = LandingCoordinator(
        store=store,
        git=git,
        transition_limit=LandingTransitionLimitPolicy(max_transitions=1),
        inspect_policy_facts=inspect,
        should_re_resolve=lambda *_args: True,
    ).run(LandRequest(task_id=impl.id))

    assert result.blocked is not None
    assert result.blocked.reason_code == "bounded-attempt-exhausted"
    assert "transition limit was exhausted" in result.blocked.fact
    assert "judge" not in result.blocked.fact
    assert calls == 2
    assert _sqlite_task_snapshot(store) == before_tasks
    assert _sqlite_merge_unit_snapshot(store) == before_units
    assert git.mutation_calls == []


def test_landing_coordinator_dry_run_blocks_unresolved_dependency_from_store(tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    dependency = _completed_impl(store, "dependency", "feature/dependency")
    impl = store.add("dependent", task_type="implement", depends_on=dependency.id)
    store.mark_completed(impl, has_commits=True, branch="feature/dependent")
    refreshed = store.get(impl.id or "")
    assert refreshed is not None and refreshed.id is not None
    git = _LandingSourceGit(
        {"feature/dependent": "source-a", "main": "target-a"},
        local_branches={"feature/dependent"},
        ancestors={("target-a", "source-a")},
    )

    result = LandingCoordinator(store=store, git=git).run(LandRequest(task_id=refreshed.id, dry_run=True))

    assert result.blocked is not None
    assert result.blocked.reason_code == "identity-proof-unavailable"
    assert any("dependency" in ref for ref in result.blocked.evidence_refs)
    assert git.mutation_calls == []


def test_landing_coordinator_dry_run_blocks_uninspectable_scope_from_shared_inspector(tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    config = Config(project_dir=tmp_path, project_name="scope-project")
    config.enforce_project_scope = True
    impl = _completed_impl(store, "scoped", "feature/scoped")
    assert impl.id is not None

    class FailingDiffGit(_LandingSourceGit):
        def get_diff_name_status(self, revision_range: str, *, check: bool = True) -> str:
            raise RuntimeError("diff read failed. secondary diagnostic")

    git = FailingDiffGit(
        {"feature/scoped": "source-a", "main": "target-a"},
        local_branches={"feature/scoped"},
        ancestors={("target-a", "source-a")},
    )

    result = LandingCoordinator(store=store, git=git, config=config).run(LandRequest(task_id=impl.id, dry_run=True))

    assert result.blocked is not None
    assert result.blocked.reason_code == "identity-proof-unavailable"
    assert any("project-scope" in ref for ref in result.blocked.evidence_refs)
    assert git.mutation_calls == []


def test_landing_coordinator_dry_run_requires_spec_review_for_spec_triggering_paths(tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    config.spec_coherence.enabled = True
    config.spec_coherence.paths = ("specs/behavior/**",)
    impl = _completed_impl(store, "spec change", "feature/landing")
    assert impl.id is not None
    _persist_lifecycle_verify_for_landing(store, config, impl)
    _completed_full_review(store, impl, head="head-a", verdict="APPROVED")
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-a")},
        name_status="M\tspecs/behavior/lifecycle-engine.md\n",
    )

    result = LandingCoordinator(store=store, git=git, config=config).run(LandRequest(task_id=impl.id, dry_run=True))

    statuses = {step.phase: step.status for step in result.steps}
    assert result.blocked is None
    assert statuses["spec_coherence"] == "conditional"
    assert "post_rebase_review" not in statuses
    assert git.mutation_calls == []


def test_landing_coordinator_dry_run_skips_disabled_code_review_and_reaches_merge_boundary(tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    config.require_review_before_merge = False
    impl = _completed_impl(store, "review disabled", "feature/landing")
    assert impl.id is not None
    _persist_lifecycle_verify_for_landing(store, config, impl)
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-a")},
    )

    result = LandingCoordinator(store=store, git=git, config=config).run(LandRequest(task_id=impl.id, dry_run=True))

    statuses = {step.phase: step.status for step in result.steps}
    assert result.blocked is None
    assert statuses["post_rebase_review"] == "skipped"
    assert statuses["judge"] == "skipped"
    assert statuses["merge"] == "conditional"
    assert git.mutation_calls == []


def test_landing_coordinator_review_disabled_ignores_stale_blocker_findings_without_guarded_escalation(tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    config.require_review_before_merge = False
    impl = _completed_impl(store, "review disabled stale blockers", "feature/landing")
    assert impl.id is not None
    _persist_lifecycle_verify_for_landing(store, config, impl)
    review = _completed_full_review(store, impl, head="stale-head", verdict="CHANGES_REQUESTED")
    review.output_content = _review_report_with_findings(
        "CHANGES_REQUESTED",
        blockers=(("B1", "Stale correctness blocker", "src/gza/landing.py:2564"),),
    )
    store.update(review)
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-a")},
    )
    coordinator = LandingCoordinator(store=store, git=git, config=config)

    result = coordinator.run(LandRequest(task_id=impl.id, dry_run=True))
    identity = coordinator._resolve_identity(LandRequest(task_id=impl.id), persist_reconciliation=False)
    assert not isinstance(identity, LandBlocked)
    facts = coordinator._landing_policy_facts(identity)
    decision = evaluate_landing_policy(
        policy="guarded",
        facts=facts,
        judge=lambda: (_ for _ in ()).throw(AssertionError("stale review must not invoke guarded escalation")),
    )

    statuses = {step.phase: step.status for step in result.steps}
    assert result.blocked is None
    assert facts.review is not None
    assert facts.review.identity_matched is False
    assert facts.open_blockers == ()
    assert decision.allowed is True
    assert decision.judgment_verdict is None
    assert statuses["post_rebase_review"] == "skipped"
    assert statuses["judge"] == "skipped"
    assert statuses["defer_blockers"] == "skipped"
    assert statuses["merge"] == "conditional"
    assert git.mutation_calls == []


def test_landing_coordinator_review_disabled_ignores_stale_followups_without_materialization(tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    config.require_review_before_merge = False
    impl = _completed_impl(store, "review disabled stale followups", "feature/landing")
    assert impl.id is not None
    _persist_lifecycle_verify_for_landing(store, config, impl)
    review = _completed_full_review(store, impl, head="stale-head", verdict="APPROVED_WITH_FOLLOWUPS")
    review.output_content = _review_report_with_findings(
        "APPROVED_WITH_FOLLOWUPS",
        followups=(("F1", "Stale follow-up", "src/gza/landing.py:4183"),),
    )
    store.update(review)
    before_tasks = _sqlite_task_snapshot(store)
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-a")},
    )
    coordinator = LandingCoordinator(store=store, git=git, config=config)

    result = coordinator.run(LandRequest(task_id=impl.id, dry_run=True))
    identity = coordinator._resolve_identity(LandRequest(task_id=impl.id), persist_reconciliation=False)
    assert not isinstance(identity, LandBlocked)
    facts = coordinator._landing_policy_facts(identity)
    decision = evaluate_landing_policy(policy="guarded", facts=facts)

    statuses = {step.phase: step.status for step in result.steps}
    assert result.blocked is None
    assert facts.review is not None
    assert facts.review.identity_matched is False
    assert facts.review.followup_findings == ()
    assert decision.allowed is True
    assert decision.followup_materialization_identities == ()
    assert statuses["post_rebase_review"] == "skipped"
    assert statuses["defer_blockers"] == "skipped"
    assert statuses["merge"] == "conditional"
    assert _sqlite_task_snapshot(store) == before_tasks
    assert git.mutation_calls == []


def test_landing_coordinator_dry_run_skips_historical_failed_spec_review_for_non_spec_diff(tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    config.spec_coherence.enabled = True
    config.spec_coherence.paths = ("specs/behavior/**",)
    impl = _completed_impl(store, "non spec diff", "feature/landing")
    assert impl.id is not None
    _persist_lifecycle_verify_for_landing(store, config, impl)
    _completed_full_review(store, impl, head="head-a", verdict="APPROVED")
    failed_spec = _completed_spec_review(
        store,
        impl,
        head="head-a",
        changed_paths=("specs/behavior/lifecycle-engine.md",),
        verdict="CHANGES_REQUESTED",
    )
    failed_spec.status = "failed"
    store.update(failed_spec)
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-a")},
        name_status="M\tsrc/gza/landing.py\n",
    )

    result = LandingCoordinator(store=store, git=git, config=config).run(LandRequest(task_id=impl.id, dry_run=True))

    statuses = {step.phase: step.status for step in result.steps}
    assert result.blocked is None
    assert statuses["spec_coherence"] == "skipped"
    assert statuses["merge"] == "conditional"
    assert git.mutation_calls == []


@pytest.mark.parametrize("terminal_case", ("failed", "malformed"))
def test_landing_coordinator_terminal_spec_evidence_blocks_at_spec_phase(tmp_path, terminal_case: str) -> None:
    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    config.spec_coherence.enabled = True
    config.spec_coherence.paths = ("specs/behavior/**",)
    impl = _completed_impl(store, f"terminal spec {terminal_case}", "feature/landing")
    assert impl.id is not None
    _persist_lifecycle_verify_for_landing(store, config, impl)
    _completed_full_review(store, impl, head="head-a", verdict="APPROVED")
    spec_review = _completed_spec_review(
        store,
        impl,
        head="head-a",
        changed_paths=("specs/behavior/lifecycle-engine.md",),
        verdict="APPROVED" if terminal_case == "failed" else "CHANGES_REQUESTED",
    )
    if terminal_case == "failed":
        spec_review.status = "failed"
        spec_review.output_content = ""
    else:
        spec_review.output_content = "not a parseable review"
    store.update(spec_review)
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-a")},
        name_status="M\tspecs/behavior/lifecycle-engine.md\n",
    )

    result = LandingCoordinator(store=store, git=git, config=config).run(LandRequest(task_id=impl.id, dry_run=True))

    assert result.blocked is not None
    assert result.blocked.reason_code == "required-review-unavailable"
    assert result.steps[-1].phase == "spec_coherence"
    assert git.mutation_calls == []


def test_landing_coordinator_review_read_failure_is_unavailable_not_skipped(monkeypatch, tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "review read failure", "feature/landing")
    assert impl.id is not None
    _persist_lifecycle_verify_for_landing(store, config, impl)
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-a")},
    )

    monkeypatch.setattr(store, "get_reviews_for_task", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("review read failed")))

    result = LandingCoordinator(store=store, git=git, config=config).run(LandRequest(task_id=impl.id, dry_run=True))

    statuses = {step.phase: step.status for step in result.steps}
    assert result.blocked is None
    assert statuses["post_rebase_review"] == "conditional"
    assert "defer_blockers" not in statuses
    assert git.mutation_calls == []


def test_landing_coordinator_rebase_read_failure_is_typed_unavailable(monkeypatch, tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    impl = _completed_impl(store, "rebase read failure", "feature/rebase-read")
    assert impl.id is not None
    git = _LandingSourceGit(
        {"feature/rebase-read": "source-a", "main": "target-a"},
        local_branches={"feature/rebase-read"},
        ancestors={("target-a", "source-a")},
    )

    monkeypatch.setattr(
        "gza.landing.get_same_branch_rebase_descendants_for_root",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("rebase read failed")),
    )

    result = LandingCoordinator(store=store, git=git).run(LandRequest(task_id=impl.id, dry_run=True))

    assert result.blocked is not None
    assert result.blocked.reason_code == "rebase-or-conflict"
    assert "rebase, ancestry, or clean-merge proof is unavailable" in result.blocked.fact
    assert git.mutation_calls == []


def test_landing_rebase_fingerprint_preserves_durable_attempt_heads_when_changed_diff_false(tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    impl = _completed_impl(store, "durable rebase", "feature/durable")
    assert impl.id is not None
    rebase = store.add("rebase", task_type="rebase", based_on=impl.id, same_branch=True)
    store.mark_completed(rebase, has_commits=True, branch="feature/durable", changed_diff=False)
    assert rebase.id is not None
    metadata = {
        "schema_version": 1,
        "parent_task_id": impl.id,
        "branch": "feature/durable",
        "target_ref": "main",
        "source_head_before": "attempted-source",
        "target_head_before": "attempted-target",
        "source_head_after": "live-source",
        "target_head_after": "live-target",
        "status": "provider_conflict_resolved",
        "changed_diff": False,
        "provider_conflict_resolved": True,
        "target_contained": True,
        "superseded": False,
        "completion_reason": None,
    }
    payload = json.dumps(metadata, sort_keys=True)
    artifact = store.add_artifact(
        rebase.id,
        kind="rebase_execution_outcome",
        label="rebase_execution_outcome",
        path=f".gza/artifacts/{rebase.id}/outcome.txt",
        byte_size=len(payload.encode()),
        sha256=sha256(payload.encode()).hexdigest(),
        metadata=metadata,
        status="provider_conflict_resolved",
        head_sha="live-source",
    )
    git = _LandingSourceGit(
        {"feature/durable": "live-source", "main": "live-target"},
        local_branches={"feature/durable"},
        ancestors={("live-target", "live-source")},
    )

    coordinator = LandingCoordinator(store=store, git=git)
    identity = coordinator._resolve_identity(LandRequest(task_id=impl.id), persist_reconciliation=False)
    assert not isinstance(identity, LandBlocked)
    fingerprint = LandingStateFingerprint.from_facts(coordinator._landing_policy_facts(identity))
    assert fingerprint.rebase.outcome_id == str(artifact.id)
    assert fingerprint.rebase.changed_diff is False
    assert fingerprint.rebase.resolution_kind == "provider_resolved"
    assert fingerprint.rebase.attempted_source_head == "attempted-source"
    assert fingerprint.rebase.attempted_target_head == "attempted-target"
    assert fingerprint.rebase.attempted_source_head != fingerprint.source_sha
    assert fingerprint.rebase.attempted_target_head != fingerprint.target_sha


@pytest.mark.parametrize(
    "facts",
    (
        _green_facts(review=_review(review_id="gza-201")),
        _green_facts(verify=_verify(epoch="verify-2")),
        _green_facts(
            rebase_status="completed",
            rebase_resolution_kind="mechanical",
            rebase_changed_diff=False,
            rebase_outcome_id="rebase-outcome-2",
            rebase_attempted_source_head="source-a",
            rebase_attempted_target_head="target-a",
            rebase_target_contained=True,
            rebase_provider_resolution_proof=False,
        ),
        _green_facts(open_blockers=(_blocker("B2", deferrable=True, blocker_class="out_of_scope"),)),
        _green_facts(policy_judgment_identity="judgment-key-2"),
        _green_facts(adjudication_fingerprints=("adjudication:2",)),
        _green_facts(
            spec_coherence=LandingSpecCoherenceEvidence(
                required=True,
                status="completed",
                verdict="APPROVED",
                current=True,
                identity_matched=True,
                evidence_id="spec-review-2",
                reviewed_head="source-a",
                changed_paths_fingerprint="specs/behavior/a.md",
            ),
        ),
    ),
)
def test_landing_coordinator_full_fingerprint_treats_decision_evidence_progress_as_distinct(
    facts: LandingPolicyFacts,
    tmp_path,
) -> None:
    store = _coordinator_store(tmp_path)
    impl = _completed_impl(store, "fingerprint progress", "feature/fingerprint")
    assert impl.id is not None
    git = _LandingSourceGit(
        {"feature/fingerprint": "source-a", "main": "target-a"},
        local_branches={"feature/fingerprint"},
        ancestors={("target-a", "source-a")},
    )
    calls = 0

    def inspect(_identity: Any) -> LandingPolicyFacts:
        nonlocal calls
        calls += 1
        return _green_facts() if calls == 1 else facts

    def re_resolve(_identity: Any, _fingerprint: LandingStateFingerprint, _steps: tuple[LandStep, ...]) -> bool:
        return calls == 1

    result = LandingCoordinator(
        store=store,
        git=git,
        inspect_policy_facts=inspect,
        should_re_resolve=re_resolve,
    ).run(LandRequest(task_id=impl.id, dry_run=True))

    if facts.open_blockers:
        # An open blocker against an otherwise-APPROVED review is a genuine
        # nondeferrable-blocker refusal (contradicts a merge-permitting review),
        # not a fingerprint-progress artifact.
        assert result.blocked is not None
        assert result.blocked.reason_code == "nondeferrable-blocker"
    else:
        assert result.blocked is None
    assert calls == 2


def test_landing_coordinator_exact_repeated_full_fingerprint_stops_before_later_side_effects(tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    impl = _completed_impl(store, "repeat fingerprint", "feature/repeat")
    assert impl.id is not None
    git = _LandingSourceGit(
        {"feature/repeat": "source-a", "main": "target-a"},
        local_branches={"feature/repeat"},
        ancestors={("target-a", "source-a")},
    )

    result = LandingCoordinator(
        store=store,
        git=git,
        inspect_policy_facts=lambda _identity: _green_facts(),
        should_re_resolve=lambda *_args: True,
    ).run(LandRequest(task_id=impl.id))

    assert result.blocked is not None
    assert result.blocked.reason_code == "bounded-attempt-exhausted"
    assert "revisited the same decision state" in result.blocked.fact
    assert git.mutation_calls == []


def test_landing_coordinator_distinct_full_fingerprints_stop_at_custom_transition_cap(tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    impl = _completed_impl(store, "transition cap", "feature/cap")
    assert impl.id is not None
    git = _LandingSourceGit(
        {"feature/cap": "source-a", "main": "target-a"},
        local_branches={"feature/cap"},
        ancestors={("target-a", "source-a")},
    )
    facts = [
        _green_facts(review=_review(review_id="gza-200")),
        _green_facts(review=_review(review_id="gza-201")),
        _green_facts(review=_review(review_id="gza-202")),
    ]
    calls = 0

    def inspect(_identity: Any) -> LandingPolicyFacts:
        nonlocal calls
        value = facts[calls]
        calls += 1
        return value

    result = LandingCoordinator(
        store=store,
        git=git,
        transition_limit=LandingTransitionLimitPolicy(max_transitions=2),
        inspect_policy_facts=inspect,
        should_re_resolve=lambda *_args: True,
    ).run(LandRequest(task_id=impl.id))

    assert result.blocked is not None
    assert result.blocked.reason_code == "bounded-attempt-exhausted"
    assert "transition limit was exhausted" in result.blocked.fact
    assert calls == 3
    assert git.mutation_calls == []


def _unused_rebase_factory(*_args: Any, **_kwargs: Any) -> Task:
    raise AssertionError("coordinator should not create a rebase task")


def _unused_rebase_executor(*_args: Any, **_kwargs: Any) -> int:
    raise AssertionError("coordinator should not run a rebase executor")


def test_landing_coordinator_skip_rebase_when_source_contains_target_tip(tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "contains target", "feature/contains-target")
    assert impl.id is not None
    git = _LandingSourceGit(
        {"feature/contains-target": "source-a", "main": "target-a"},
        local_branches={"feature/contains-target"},
        ancestors={("target-a", "source-a")},
    )
    service_calls: list[RebaseServiceRequest] = []

    def recording_service(**kwargs: Any) -> RebaseServiceResult:
        request = kwargs["request"]
        service_calls.append(request)
        return RebaseServiceResult(
            status="skipped",
            parent_task_id=request.parent_task_id,
            branch=request.branch,
            target_ref=request.target_branch,
            changed_diff=False,
            artifact_id=10,
            artifact_key="skip-key",
            source_head_before="source-a",
            target_head_before="target-a",
            source_head_after="source-a",
            target_head_after="target-a",
            fact="source already contains target",
        )

    result = LandingCoordinator(
        store=store,
        git=git,
        config=config,
        create_rebase_task=_unused_rebase_factory,
        rebase_executor=_unused_rebase_executor,
        execute_rebase_service=recording_service,
    ).run(LandRequest(task_id=impl.id))

    assert service_calls == []
    statuses = {step.phase: step.status for step in result.steps}
    assert statuses["rebase"] == "skipped"
    assert result.blocked is not None
    assert result.blocked.reason_code == "verify-unavailable-or-red"


def test_landing_coordinator_runs_one_task_backed_rebase_when_source_is_behind(tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "behind target", "feature/behind")
    assert impl.id is not None
    git = _LandingSourceGit(
        {"feature/behind": "source-before", "main": "target-a"},
        local_branches={"feature/behind"},
        ancestors=set(),
    )
    service_calls: list[RebaseServiceRequest] = []
    resolved_heads: list[tuple[str | None, str | None]] = []

    def inspect(identity: Any) -> LandingPolicyFacts:
        resolved_heads.append((identity.source_sha, identity.target_sha))
        return _green_facts(
            task_id=identity.owner_task_id,
            source_head=identity.source_sha,
            target_head=identity.target_sha,
            review=_review(reviewed_head=identity.source_sha),
            rebase_status="none",
            rebase_resolution_kind="none",
            rebase_target_contained=(identity.target_sha, identity.source_sha) in git.ancestors,
            ancestry_proof_available=True,
            clean_merge=True,
        )

    def recording_service(**kwargs: Any) -> RebaseServiceResult:
        request = kwargs["request"]
        service_calls.append(request)
        assert request.trigger_source == "manual_land"
        assert request.run is True
        assert request.skip_if_target_contained is True
        git.heads["feature/behind"] = "source-after"
        git.ancestors.add(("target-a", "source-after"))
        return RebaseServiceResult(
            status="completed_mechanical",
            parent_task_id=request.parent_task_id,
            branch=request.branch,
            target_ref=request.target_branch,
            rebase_task_id="gza-200",
            changed_diff=False,
            artifact_id=11,
            artifact_key="rebase-key",
            source_head_before="source-before",
            target_head_before="target-a",
            source_head_after="source-after",
            target_head_after="target-a",
        )

    result = LandingCoordinator(
        store=store,
        git=git,
        config=config,
        inspect_policy_facts=inspect,
        create_rebase_task=_unused_rebase_factory,
        rebase_executor=_unused_rebase_executor,
        execute_rebase_service=recording_service,
    ).run(LandRequest(task_id=impl.id))

    assert len(service_calls) == 1
    assert service_calls[0].parent_task_id == impl.id
    assert service_calls[0].branch == "feature/behind"
    assert service_calls[0].target_branch == "main"
    assert resolved_heads == [("source-before", "target-a"), ("source-after", "target-a")]
    assert any(step.phase == "rebase" and step.status == "completed" for step in result.steps)
    assert result.blocked is not None
    assert result.blocked.reason_code == "merge-failed"


def test_landing_coordinator_unknown_ancestry_blocks_without_rebase(tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "unknown ancestry", "feature/unknown-ancestry")
    assert impl.id is not None

    class FailingAncestryGit(_LandingSourceGit):
        def is_ancestor(self, ancestor: str, descendant: str) -> bool:
            raise RuntimeError(f"cannot prove {ancestor}->{descendant}")

    git = FailingAncestryGit(
        {"feature/unknown-ancestry": "source-a", "main": "target-a"},
        local_branches={"feature/unknown-ancestry"},
    )
    service_calls: list[RebaseServiceRequest] = []

    def unexpected_service(**kwargs: Any) -> RebaseServiceResult:
        service_calls.append(kwargs["request"])
        raise AssertionError("coordinator should not call rebase service without ancestry proof")

    result = LandingCoordinator(
        store=store,
        git=git,
        config=config,
        create_rebase_task=_unused_rebase_factory,
        rebase_executor=_unused_rebase_executor,
        execute_rebase_service=unexpected_service,
    ).run(LandRequest(task_id=impl.id))

    assert service_calls == []
    assert result.blocked is not None
    assert result.blocked.reason_code == "rebase-or-conflict"
    assert "ancestry proof is unavailable" in result.blocked.fact


def test_landing_coordinator_failed_rebase_blocks_once_without_merge(tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "failed rebase", "feature/failed-rebase")
    assert impl.id is not None
    git = _LandingSourceGit(
        {"feature/failed-rebase": "source-a", "main": "target-a"},
        local_branches={"feature/failed-rebase"},
    )
    service_calls = 0

    def failing_service(**kwargs: Any) -> RebaseServiceResult:
        nonlocal service_calls
        service_calls += 1
        request = kwargs["request"]
        return RebaseServiceResult(
            status="failed",
            parent_task_id=request.parent_task_id,
            branch=request.branch,
            target_ref=request.target_branch,
            rebase_task_id="gza-201",
            exit_code=1,
            artifact_id=12,
            artifact_key="failed-key",
            source_head_before="source-a",
            target_head_before="target-a",
            source_head_after="source-a",
            target_head_after="target-a",
            fact="AI conflict resolution could not complete",
        )

    result = LandingCoordinator(
        store=store,
        git=git,
        config=config,
        create_rebase_task=_unused_rebase_factory,
        rebase_executor=_unused_rebase_executor,
        execute_rebase_service=failing_service,
    ).run(LandRequest(task_id=impl.id))

    assert service_calls == 1
    assert result.blocked is not None
    assert result.blocked.reason_code == "rebase-or-conflict"
    assert result.blocked.fact == "AI conflict resolution could not complete"
    assert sum(1 for step in result.steps if step.phase == "rebase" and step.status == "blocked") == 1
    assert git.mutation_calls == []


def test_landing_coordinator_stale_target_after_rebase_blocks_without_second_rebase(tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "stale target", "feature/stale-target")
    assert impl.id is not None
    git = _LandingSourceGit(
        {"feature/stale-target": "source-before", "main": "target-a"},
        local_branches={"feature/stale-target"},
    )
    service_calls = 0
    facts_calls = 0

    def inspect(identity: Any) -> LandingPolicyFacts:
        nonlocal facts_calls
        facts_calls += 1
        if facts_calls == 1:
            return _green_facts(
                task_id=identity.owner_task_id,
                source_head=identity.source_sha,
                target_head=identity.target_sha,
                rebase_status="none",
                rebase_resolution_kind="none",
                rebase_target_contained=False,
                ancestry_proof_available=True,
                clean_merge=False,
            )
        return _green_facts(
            task_id=identity.owner_task_id,
            source_head=identity.source_sha,
            target_head=identity.target_sha,
            rebase_status="completed",
            rebase_resolution_kind="mechanical",
            rebase_changed_diff=False,
            rebase_outcome_id="artifact-13",
            rebase_attempted_source_head="source-before",
            rebase_attempted_target_head="target-a",
            rebase_target_contained=False,
            rebase_provider_resolution_proof=False,
            ancestry_proof_available=True,
            clean_merge=False,
        )

    def stale_target_service(**kwargs: Any) -> RebaseServiceResult:
        nonlocal service_calls
        service_calls += 1
        request = kwargs["request"]
        git.heads["feature/stale-target"] = "source-after"
        git.heads["main"] = "target-b"
        return RebaseServiceResult(
            status="completed_mechanical",
            parent_task_id=request.parent_task_id,
            branch=request.branch,
            target_ref=request.target_branch,
            rebase_task_id="gza-202",
            changed_diff=False,
            artifact_id=13,
            artifact_key="stale-key",
            source_head_before="source-before",
            target_head_before="target-a",
            source_head_after="source-after",
            target_head_after="target-a",
        )

    result = LandingCoordinator(
        store=store,
        git=git,
        config=config,
        inspect_policy_facts=inspect,
        create_rebase_task=_unused_rebase_factory,
        rebase_executor=_unused_rebase_executor,
        execute_rebase_service=stale_target_service,
    ).run(LandRequest(task_id=impl.id))

    assert service_calls == 1
    assert facts_calls == 2
    assert result.blocked is not None
    assert result.blocked.reason_code == "rebase-or-conflict"
    assert git.mutation_calls == []


def test_landing_coordinator_invokes_canonical_verify_acquisition(monkeypatch, tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "verify acquisition", "feature/landing")
    assert impl.id is not None
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-a")},
    )
    calls: list[tuple[str | None, str | None]] = []
    facts_calls = 0

    def inspect(identity: Any) -> LandingPolicyFacts:
        nonlocal facts_calls
        facts_calls += 1
        return _green_facts(
            task_id=identity.owner_task_id,
            source_head=identity.source_sha,
            target_head=identity.target_sha,
            review=_review(reviewed_head=identity.source_sha),
            verify=_verify(status="missing", current=False, identity_matched=False)
            if facts_calls == 1
            else _verify(),
        )

    def acquire(_store: Any, owner_task: Task, **kwargs: Any) -> LandingVerifyAcquisitionResult:
        calls.append((owner_task.id, kwargs["source_head"]))
        return LandingVerifyAcquisitionResult("ran_verify", _verify(), execution=SimpleNamespace(status="success"))

    monkeypatch.setattr("gza.landing.acquire_landing_verify_evidence", acquire)

    result = LandingCoordinator(
        store=store,
        git=git,
        config=config,
        inspect_policy_facts=inspect,
        verify_action_context=SimpleNamespace(),  # type: ignore[arg-type]
    ).run(LandRequest(task_id=impl.id))

    assert calls == [(impl.id, "head-a")]
    assert any(step.phase == "verify" and step.status == "completed" for step in result.steps)
    assert result.blocked is not None
    assert result.blocked.reason_code == "merge-failed"


def test_landing_coordinator_refuses_stale_verify_after_acquisition(monkeypatch, tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "stale verify acquisition", "feature/landing")
    assert impl.id is not None
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-a")},
    )
    blocked = LandBlocked(
        "verify-unavailable-or-red",
        "current green source verify evidence is unavailable after shared verify",
        (impl.id, "old-verify", "head-a"),
    )

    def acquire(*_args: Any, **_kwargs: Any) -> LandingVerifyAcquisitionResult:
        return LandingVerifyAcquisitionResult(
            "blocked",
            _verify(status="stale", current=False, identity_matched=False, epoch="old-verify"),
            execution=SimpleNamespace(status="success"),
            blocked=blocked,
        )

    monkeypatch.setattr("gza.landing.acquire_landing_verify_evidence", acquire)

    result = LandingCoordinator(
        store=store,
        git=git,
        config=config,
        inspect_policy_facts=lambda identity: _green_facts(
            task_id=identity.owner_task_id,
            source_head=identity.source_sha,
            target_head=identity.target_sha,
            verify=_verify(status="stale", current=False, identity_matched=False),
        ),
        verify_action_context=SimpleNamespace(),  # type: ignore[arg-type]
        create_full_review=_fail_improve_or_review_route,
        create_resolution_review=_fail_improve_or_review_route,
    ).run(LandRequest(task_id=impl.id))

    assert result.blocked == blocked
    assert result.steps[-1].phase == "verify"
    _assert_no_improve_rows(store)


def test_landing_coordinator_preserves_review_after_mechanical_unchanged_rebase(tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "mechanical unchanged", "feature/landing")
    assert impl.id is not None
    review = _completed_full_review(store, impl, head="head-before", verdict="APPROVED")
    git = _LandingSourceGit(
        {"feature/landing": "head-after", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-after")},
    )

    facts = _green_facts(
        task_id=impl.id,
        source_head="head-after",
        target_head="target-a",
        rebase_status="completed",
        rebase_resolution_kind="mechanical",
        rebase_changed_diff=False,
        rebase_outcome_id="rebase-outcome",
        rebase_attempted_source_head="head-before",
        rebase_attempted_target_head="target-a",
        rebase_target_contained=True,
        rebase_provider_resolution_proof=False,
        review=_review(
            review_id=review.id,
            reviewed_head="head-before",
            current=False,
            identity_matched=False,
        ),
    )

    result = LandingCoordinator(
        store=store,
        git=git,
        config=config,
        inspect_policy_facts=lambda _identity: facts,
        create_full_review=_fail_improve_or_review_route,
        create_resolution_review=_fail_improve_or_review_route,
    ).run(LandRequest(task_id=impl.id))

    statuses = {step.phase: step.status for step in result.steps}
    assert statuses["post_rebase_review"] == "skipped"
    assert result.blocked is not None
    assert result.blocked.reason_code == "merge-failed"
    _assert_no_review_or_improve_rows_after_landing_review(store, {review.id or ""})


def test_landing_coordinator_requires_one_resolution_review_after_provider_rebase(tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "provider resolved", "feature/landing")
    assert impl.id is not None
    _persist_lifecycle_verify_for_landing(store, config, impl)
    rebase = store.add("provider rebase", task_type="rebase", based_on=impl.id, same_branch=True)
    store.mark_completed(rebase, has_commits=True, branch="feature/landing", changed_diff=False)
    _persist_landing_rebase_outcome(store, rebase, impl, source_after="head-a")
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-a")},
    )
    created: list[str] = []

    def create_resolution(*_args: Any, **kwargs: Any) -> Task:
        created.append(kwargs["resolved_head_sha"])
        review = store.add("landing resolution review", task_type="review", depends_on=impl.id, based_on=impl.id)
        review.status = "pending"
        review.review_verify_head_sha = kwargs["resolved_head_sha"]
        store.update(review)
        return review

    result = LandingCoordinator(
        store=store,
        git=git,
        config=config,
        create_resolution_review=create_resolution,
        create_full_review=_fail_improve_or_review_route,
    ).run(LandRequest(task_id=impl.id))

    assert created == ["head-a"]
    assert result.blocked is not None
    assert result.blocked.reason_code == "required-review-unavailable"
    assert sum(1 for task in store.get_all() if task.task_type == "review") == 1
    _assert_no_improve_rows(store)


def test_landing_coordinator_falls_back_to_one_full_review_when_resolution_provenance_is_incomplete(tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "fallback full", "feature/landing")
    assert impl.id is not None
    git = _LandingSourceGit(
        {"feature/landing": "source-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "source-a")},
    )
    created: list[str] = []

    def create_full(_store: Any, _impl: Task, **_kwargs: Any) -> Task:
        created.append("full")
        review = store.add("landing full review", task_type="review", depends_on=impl.id, based_on=impl.id)
        review.status = "pending"
        store.update(review)
        return review

    result = LandingCoordinator(
        store=store,
        git=git,
        config=config,
        inspect_policy_facts=lambda identity: _green_facts(
            task_id=identity.owner_task_id,
            source_head=identity.source_sha,
            target_head=identity.target_sha,
            rebase_status="completed",
            rebase_resolution_kind="provider_resolved",
            rebase_changed_diff=True,
            rebase_outcome_id="legacy-outcome",
            rebase_attempted_source_head="source-before",
            rebase_attempted_target_head="target-a",
            rebase_target_contained=True,
            rebase_provider_resolution_proof=True,
            review=_review(status="unavailable", current=False, parseable=False, identity_matched=False),
        ),
        create_full_review=create_full,
        create_resolution_review=_fail_improve_or_review_route,
    ).run(LandRequest(task_id=impl.id))

    assert created == ["full"]
    assert result.blocked is not None
    assert result.blocked.reason_code == "required-review-unavailable"
    assert sum(1 for task in store.get_all() if task.task_type == "review") == 1
    _assert_no_improve_rows(store)


def test_landing_coordinator_strict_changes_requested_stops_without_review_or_improve(tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "strict current changes requested", "feature/landing")
    assert impl.id is not None
    _persist_lifecycle_verify_for_landing(store, config, impl)
    review = _completed_full_review(store, impl, head="head-a", verdict="CHANGES_REQUESTED")
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-a")},
    )

    result = LandingCoordinator(
        store=store,
        git=git,
        config=config,
        create_full_review=_fail_improve_or_review_route,
        create_resolution_review=_fail_improve_or_review_route,
    ).run(LandRequest(task_id=impl.id, policy="strict"))

    assert result.blocked is not None
    assert result.blocked.reason_code == "nondeferrable-blocker"
    assert all(step.phase != "post_rebase_review" for step in result.steps)
    _assert_no_review_or_improve_rows_after_landing_review(store, {review.id or ""})


def test_landing_coordinator_guarded_defers_blockers_after_final_preflight_and_merges(tmp_path) -> None:
    from gza.merge_services import ManualMergeExecutionResult

    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "guarded deferral", "feature/landing")
    assert impl.id is not None
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-a")},
    )
    order: list[str] = []

    def inspect(identity: Any) -> LandingPolicyFacts:
        return _green_facts(
            task_id=identity.owner_task_id,
            source_head=identity.source_sha,
            target_head=identity.target_sha,
            parked_reason="review-max-cycles-reached",
            review=_review(verdict="CHANGES_REQUESTED", reviewed_head=identity.source_sha),
            open_blockers=(_blocker("B1", deferrable=True, blocker_class="out_of_scope"),),
        )

    def judge() -> LandingJudgment:
        order.append("judge")
        return LandingJudgment("LAND", artifact_id="judge-artifact", key="judge-key")

    def merge(identity: Any, decision: LandingPolicyDecision, provenance: str) -> ManualMergeExecutionResult:
        order.append("merge")
        assert order == ["judge", "merge"]
        assert provenance == "manual_land_escalated"
        assert decision.allowed_overrides == (
            "defer-review-blockers",
            "parked:review-max-cycles-reached",
        )
        blocker = store.add("deferred B1", task_type="implement", depends_on=impl.id, create_pr=True, urgent=True)
        return ManualMergeExecutionResult(
            rc=0,
            status="merged",
            created_deferred_blockers=[blocker],
        )

    result = LandingCoordinator(
        store=store,
        git=git,
        config=config,
        inspect_policy_facts=inspect,
        landing_judge=judge,
        execute_merge=merge,
    ).run(LandRequest(task_id=impl.id))

    assert result.blocked is None
    assert result.merged is True
    assert result.merge_provenance == "manual_land_escalated"
    assert result.judgment_artifact_id == "judge-artifact"
    assert result.judgment_key == "judge-key"
    assert result.deferred_task_ids
    deferred = store.get(result.deferred_task_ids[0])
    assert deferred is not None
    assert deferred.urgent is True
    assert deferred.create_pr is True
    assert [step.phase for step in result.steps[-3:]] == ["judge", "defer_blockers", "merge"]


@pytest.mark.parametrize("changed_ref", ("source", "target"))
def test_landing_coordinator_final_head_change_blocks_before_deferred_materialization(
    tmp_path,
    changed_ref: str,
) -> None:
    from gza.merge_services import ManualMergeExecutionResult

    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "head invalidation", "feature/landing")
    assert impl.id is not None
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-a")},
    )

    def inspect(identity: Any) -> LandingPolicyFacts:
        return _green_facts(
            task_id=identity.owner_task_id,
            source_head=identity.source_sha,
            target_head=identity.target_sha,
            parked_reason="review-max-cycles-reached",
            review=_review(verdict="CHANGES_REQUESTED", reviewed_head=identity.source_sha),
            open_blockers=(_blocker("B1", deferrable=True, blocker_class="out_of_scope"),),
        )

    def judge() -> LandingJudgment:
        if changed_ref == "source":
            git.heads["feature/landing"] = "head-b"
        else:
            git.heads["main"] = "target-b"
        return LandingJudgment("LAND", artifact_id="judge-artifact", key="judge-key")

    def merge(*_args: Any, **_kwargs: Any) -> ManualMergeExecutionResult:
        raise AssertionError("deferred blockers and merge must not run after head invalidation")

    result = LandingCoordinator(
        store=store,
        git=git,
        config=config,
        inspect_policy_facts=inspect,
        landing_judge=judge,
        execute_merge=merge,
    ).run(LandRequest(task_id=impl.id))

    assert result.blocked is not None
    assert result.blocked.reason_code == "identity-proof-unavailable"
    assert f"{changed_ref} head changed" in result.blocked.fact
    assert all(step.phase != "defer_blockers" for step in result.steps)
    assert all(task.prompt != "deferred B1" for task in store.get_all())


def test_land_cli_prints_concrete_dry_run_evidence(monkeypatch, capsys, tmp_path) -> None:
    from gza.cli import land as land_cli

    class FakeCoordinator:
        def __init__(self, *, store: Any, git: Any, config: Any, **_kwargs: Any) -> None:
            self.store = store
            self.git = git
            self.config = config

        def run(self, request: LandRequest) -> LandResult:
            return LandResult(
                request=request,
                owner_task_id="gza-9316",
                target_branch="main",
                source_ref="feature/landing",
                steps=(
                    LandStep("resolve", "completed", "resolved gza-9316 to owner gza-9316 on feature/landing -> main"),
                    LandStep("verify", "completed", "current green source verify evidence verify-1 passed for gate gate-a"),
                    LandStep("post_rebase_review", "completed", "current plain_full review gza-10158 is APPROVED"),
                    LandStep("merge", "conditional", "execution required before later outcomes are knowable"),
                ),
            )

    monkeypatch.setattr(land_cli.Config, "load", lambda project_dir: SimpleNamespace(project_dir=project_dir))
    monkeypatch.setattr(land_cli, "get_store", lambda _config, open_mode: SimpleNamespace(open_mode=open_mode))
    monkeypatch.setattr(land_cli, "Git", lambda project_dir: SimpleNamespace(project_dir=project_dir))
    monkeypatch.setattr(land_cli, "resolve_id", lambda _config, task_id: task_id)
    monkeypatch.setattr("gza.landing.LandingCoordinator", FakeCoordinator)

    status = land_cli.cmd_land(
        land_cli.argparse.Namespace(project_dir=tmp_path, task_id="gza-9316", policy="guarded", dry_run=True)
    )
    output = capsys.readouterr().out

    assert status == 0
    assert "verify-1 passed for gate gate-a" in output
    assert "review gza-10158 is APPROVED" in output


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


@pytest.mark.parametrize("expected_tree", (None, "   "))
def test_inspect_current_landing_verify_blocks_omitted_or_blank_live_tree(tmp_path, expected_tree: str | None) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    config = _verify_config(tmp_path)
    impl = store.add("Implement landing verify", task_type="implement")
    impl.status = "completed"
    impl.branch = "feature/landing"
    store.update(impl)
    _persist_lifecycle_verify_for_landing(store, config, impl)

    evidence = inspect_current_landing_verify_evidence(
        store,
        impl,
        config=config,
        git=_FakeGit({"feature/landing": "head-a"}),
        source_head="head-a",
        tree_fingerprint=expected_tree,
    )

    assert evidence.status == "passed"
    assert evidence.current is True
    assert evidence.identity_matched is False
    assert evidence.tree_fingerprint == TREE_A


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


def test_inspect_current_landing_verify_rejects_cross_project_tree_fallback(tmp_path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    config = _verify_config(tmp_path)
    impl = store.add("Implement landing verify", task_type="implement")
    impl.status = "completed"
    impl.branch = "feature/landing"
    store.update(impl)
    persist_verify_gate_artifact(
        store,
        config,
        owner_task=impl,
        source_task=impl,
        result=ReviewVerifyResult(
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
                f"gza-verify phase=passed name=unit duration_seconds=1.0 tree_fingerprint={TREE_A}\n"
            ),
        ),
        verify_timeout_seconds=120,
        verify_timeout_grace_seconds=5.0,
        producer="advance_verify_gate",
        provenance={"tree_fingerprint": TREE_A},
        aggregate_details={
            "runnable_count": 2,
            "tree_fingerprint": None,
            "tree_fingerprint_complete": False,
            "tree_fingerprint_missing_count": 1,
            "scopes": [],
        },
    )

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


def test_acquire_landing_verify_runs_shared_direct_action_then_reevaluates_stable_live_tree(monkeypatch, tmp_path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    config = _verify_config(tmp_path)
    impl = store.add("Implement landing verify", task_type="implement")
    impl.status = "completed"
    impl.branch = "feature/landing"
    store.update(impl)
    calls: list[str] = []
    live_tree_calls: list[str] = []

    def fake_plan(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        return {"type": "verify_gate", "description": "Run verify gate before merge"}

    def fake_execute(_task: Any, action: dict[str, Any], _context: Any) -> Any:
        calls.append(str(action["type"]))
        _persist_lifecycle_verify_for_landing(store, config, impl)
        return SimpleNamespace(action_type="verify_gate", status="success")

    def live_tree() -> str:
        live_tree_calls.append("resolved")
        return TREE_A

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
        live_tree_fingerprint_resolver=live_tree,
    )

    assert result.status == "ran_verify"
    assert result.evidence.status == "passed"
    assert calls == ["verify_gate"]
    assert live_tree_calls == ["resolved", "resolved"]


@pytest.mark.parametrize("live_tree", (None, "   ", TREE_B))
def test_acquire_landing_verify_blocks_without_exact_live_tree_before_work(live_tree: str | None, tmp_path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    config = _verify_config(tmp_path)
    impl = store.add("Implement landing verify", task_type="implement")
    impl.status = "completed"
    impl.branch = "feature/landing"
    store.update(impl)
    _persist_lifecycle_verify_for_landing(store, config, impl)

    result = acquire_landing_verify_evidence(
        store,
        impl,
        config=config,
        git=_FakeGit({"feature/landing": "head-a"}),
        target_branch="main",
        source_head="head-a",
        context=SimpleNamespace(),  # type: ignore[arg-type]
        execute_action=lambda *_args: (_ for _ in ()).throw(AssertionError("must not execute")),  # type: ignore[arg-type]
        live_tree_fingerprint_resolver=lambda: live_tree,
    )

    assert result.status == "blocked"
    assert result.evidence.status == "passed"
    assert result.evidence.identity_matched is False
    assert result.blocked is not None
    assert result.blocked.reason_code == "verify-unavailable-or-red"


def test_acquire_landing_verify_blocks_when_post_execution_live_tree_changed(monkeypatch, tmp_path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    config = _verify_config(tmp_path)
    impl = store.add("Implement landing verify", task_type="implement")
    impl.status = "completed"
    impl.branch = "feature/landing"
    store.update(impl)
    calls: list[str] = []
    live_trees = [TREE_A, TREE_B]

    def fake_plan(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        return {"type": "verify_gate", "description": "Run verify gate before merge"}

    def fake_execute(_task: Any, action: dict[str, Any], _context: Any) -> Any:
        calls.append(str(action["type"]))
        _persist_lifecycle_verify_for_landing(store, config, impl)
        return SimpleNamespace(action_type="verify_gate", status="success")

    def live_tree() -> str:
        return live_trees.pop(0)

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
        live_tree_fingerprint_resolver=live_tree,
    )

    assert result.status == "blocked"
    assert result.evidence.status == "passed"
    assert result.evidence.identity_matched is False
    assert result.blocked is not None
    assert result.blocked.reason_code == "verify-unavailable-or-red"
    assert calls == ["verify_gate"]
    assert live_trees == []


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
        live_tree_fingerprint_resolver=lambda: TREE_A,
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


def _completed_spec_review(
    store: SqliteTaskStore,
    impl,
    *,
    head: str,
    changed_paths: tuple[str, ...],
    verdict: str = "APPROVED",
    completed_at: datetime | None = None,
):
    review = store.add("Completed spec review", task_type="review", depends_on=impl.id, based_on=impl.id)
    review.status = "completed"
    review.completed_at = completed_at or datetime(2026, 8, 26, 12, 0, tzinfo=UTC)
    review.review_verify_head_sha = head
    review.review_scope = build_spec_coherence_review_scope(
        implementation_task_id=impl.id,
        reviewed_head_sha=head,
        changed_paths=changed_paths,
    )
    review.output_content = _review_report(verdict)
    store.update(review)
    return review


def _persist_landing_rebase_outcome(
    store: SqliteTaskStore,
    rebase: Task,
    impl: Task,
    *,
    source_before: str = "source-before",
    target_before: str = "target-a",
    source_after: str = "source-a",
    target_after: str = "target-a",
    status: str = "provider_conflict_resolved",
    changed_diff: bool = False,
    provider_conflict_resolved: bool = True,
) -> Any:
    metadata = {
        "schema_version": 1,
        "parent_task_id": impl.id,
        "branch": impl.branch,
        "target_ref": "main",
        "source_head_before": source_before,
        "target_head_before": target_before,
        "source_head_after": source_after,
        "target_head_after": target_after,
        "status": status,
        "changed_diff": changed_diff,
        "provider_conflict_resolved": provider_conflict_resolved,
        "target_contained": True,
        "superseded": False,
        "completion_reason": None,
    }
    payload = json.dumps(metadata, sort_keys=True)
    return store.add_artifact(
        rebase.id or "",
        kind="rebase_execution_outcome",
        label="rebase_execution_outcome",
        path=f".gza/artifacts/{rebase.id}/outcome.txt",
        byte_size=len(payload.encode()),
        sha256=sha256(payload.encode()).hexdigest(),
        metadata=metadata,
        status=status,
        head_sha=source_after,
    )


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

    transition = _run_landing_review_transition_with_poisoned_review_routes(
        store,
        impl,
        LandingPostRebaseReviewRequest(
            impl_task=impl,
            source_head="head-a",
            target_head="target-a",
            changed_diff=None,
            resolution_provenance_complete=False,
        ),
        review,
    )

    result = transition.review_result
    assert result.status == "reused_completed"
    assert result.review_task == review
    assert result.review_budget_used is False


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
        create_resolution_review=_fail_improve_or_review_route,
    )
    assert first.status == "created"
    assert first.review_task is not None
    assert first.review_budget_used is True
    _assert_no_improve_action(first)
    _assert_no_improve_rows(store)

    first.review_task.status = "completed"
    first.review_task.completed_at = datetime(2026, 8, 26, 12, 0, tzinfo=UTC)
    first.review_task.output_content = _review_report("CHANGES_REQUESTED")
    store.update(first.review_task)

    completed = run_landing_post_rebase_review_transition(
        store,
        LandingPostRebaseReviewRequest(
            impl_task=impl,
            source_head="head-a",
            target_head="target-a",
            changed_diff=True,
            resolution_provenance_complete=False,
            review_budget_used=first.review_budget_used,
        ),
        policy="guarded",
        facts=_landing_policy_facts_for_review(impl, first.review_task),
        judge=_landing_judgment,
        create_full_review=_fail_improve_or_review_route,
        create_resolution_review=_fail_improve_or_review_route,
    )
    assert completed.review_result.status == "reused_completed"
    assert completed.review_result.review_budget_used is True
    assert completed.review_result.review_task == first.review_task
    _assert_no_improve_action(completed.review_result)
    assert completed.decision.allowed is True
    assert completed.decision.allowed_overrides == (
        "defer-review-blockers",
        "parked:review-max-cycles-reached",
    )
    assert completed.decision.judgment_verdict == "LAND"
    _assert_no_review_or_improve_rows_after_landing_review(store, {first.review_task.id or ""})

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
            review_budget_used=completed.review_result.review_budget_used,
        ),
        create_full_review=_fail_improve_or_review_route,
        create_resolution_review=_fail_improve_or_review_route,
    )
    assert pending.status == "pending"
    assert pending.review_budget_used is True
    _assert_no_improve_action(pending)
    _assert_no_improve_rows(store)

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
        create_full_review=_fail_improve_or_review_route,
        create_resolution_review=_fail_improve_or_review_route,
    )
    assert no_longer_required.status == "not_required"
    assert no_longer_required.review_budget_used is True
    _assert_no_improve_action(no_longer_required)
    _assert_no_improve_rows(store)

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
        create_full_review=_fail_improve_or_review_route,
        create_resolution_review=_fail_improve_or_review_route,
    )
    assert changed_identity.status == "blocked"
    assert changed_identity.review_budget_used is True
    assert changed_identity.blocked is not None
    assert changed_identity.blocked.reason_code == "bounded-attempt-exhausted"
    assert created == ["created"]
    _assert_no_improve_action(changed_identity)
    _assert_no_improve_rows(store)
