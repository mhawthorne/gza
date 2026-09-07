from __future__ import annotations

import argparse
import json
from collections.abc import Callable
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Literal

import pytest

import gza.landing as landing_module
from gza.config import Config
from gza.db import SqliteTaskStore, Task, WatchProgressObservation
from gza.landing import (
    LANDING_PHASES,
    LandBlocked,
    LandingCollaborators,
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
    LandingResolvedIdentity,
    LandingSpecCoherenceEvidence,
    LandingSpecCoherenceFingerprint,
    LandingStateFingerprint,
    LandingTransitionLimitPolicy,
    LandingVerifyAcquisitionResult,
    LandingVerifyEvidence,
    LandPostMergeVerifyFailure,
    LandPostMergeVerifySuccess,
    LandRequest,
    LandResult,
    LandStep,
    MergeLandingAuthorization,
    MergeUnitProofIdentity,
    TerminalProof,
    acquire_landing_verify_evidence,
    acquire_one_post_rebase_review,
    create_production_landing_coordinator,
    dry_run_steps_until_boundary,
    evaluate_landing_policy,
    inspect_current_landing_verify_evidence,
    refresh_landing_authorization,
    run_landing_post_rebase_review_transition,
)
from gza.merge_services import ManualMergeExecutionResult, MergeLandingAuthorization
from gza.rebase_service import RebaseServiceRequest, RebaseServiceResult
from gza.rebase_diff import RebaseDiffBaseline, build_rebase_diff_provenance
from gza.review_scope import build_resolution_review_scope, build_spec_coherence_review_scope
from gza.review_tasks import DuplicateReviewError, build_deferred_blocker_prompt, format_blocker_finding_context
from gza.review_verdict import ParsedReviewReport, ReviewFinding, parse_review_report
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


def _post_merge_success(
    identity: Any,
    *,
    target_head: str = "merge-a",
    checkpoint_id: str = "checkpoint-green",
) -> LandPostMergeVerifySuccess:
    return LandPostMergeVerifySuccess(
        checkpoint_id=checkpoint_id,
        target_head=target_head,
        tree_fingerprint=TREE_A,
        gate_identity=identity.target_branch,
    )


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
        deferred_task_prompt_sha256=f"sha256:prompt-{finding_id}",
        deferred_task_review_scope_sha256=f"sha256:scope-{finding_id}",
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
def test_post_merge_verify_failure_renders_pending_finalization_non_success(status: str) -> None:
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
        post_merge_verify_failure=failure,
    )

    assert result.merged is False
    assert result.already_merged is False
    assert result.merge_provenance is None
    assert result.blocked is None
    sentence = failure.terminal_sentence("gza-100")
    assert sentence == (
        "Git merged gza-100, but integration verification failed before recording merged state: "
        f"post-merge checkpoint is {status}."
    )
    assert "Cannot land" not in sentence
    assert failure.evidence_refs


@pytest.mark.parametrize("status", ("failed", "unavailable", "stale", "malformed", "missing"))
def test_post_merge_verify_failure_rejects_authoritative_terminal_state(
    status: str,
) -> None:
    failure = LandPostMergeVerifyFailure(
        status=status,  # type: ignore[arg-type]
        fact=f"post-merge checkpoint is {status}",
        checkpoint_id="checkpoint-1",
        target_head="target-after",
        gate_identity="main-verify",
    )
    with pytest.raises(ValueError):
        LandResult(
            request=LandRequest(task_id="gza-100"),
            owner_task_id="gza-100",
            target_branch="main",
            source_ref="feature/example",
            merged=True,
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
            post_merge_verify_failure=failure,
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
    pending_finalization = LandResult(
        request=LandRequest(task_id="gza-100"),
        owner_task_id="gza-100",
        target_branch="main",
        source_ref="feature/example",
        post_merge_verify_failure=failure,
    )
    assert pending_finalization.merged is False
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
        trees: dict[str, str] | None = None,
        current_branch: str = "main",
        dirty: bool = False,
        merged_refs: set[tuple[str, str]] | None = None,
        ancestors: set[tuple[str, str]] | None = None,
        can_merge_refs: set[tuple[str, str]] | None = None,
        name_status: str = "",
        diff: str = "diff --git a/example b/example\n+landing change\n",
    ) -> None:
        self.heads = heads
        self.trees = trees or {}
        self._current_branch = current_branch
        self.dirty = dirty
        self.merged_refs = merged_refs or set()
        self.ancestors = ancestors or set()
        self.can_merge_refs = can_merge_refs
        self.name_status = name_status
        self.diff = diff
        self.merge_calls: list[tuple[str, str | None]] = []
        self.mutation_calls: list[str] = []

    def rev_parse_if_exists(self, ref: str) -> str | None:
        return self.heads.get(ref)

    def resolve_refs(self, refs: tuple[str, ...] | list[str], peel: str = "commit") -> dict[str, str | None]:
        source = self.trees if peel == "tree" else self.heads
        return {ref: source.get(ref) for ref in refs}

    def current_branch(self) -> str:
        return self._current_branch

    def default_branch(self) -> str:
        return "main"

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

    def get_diff(self, revision_range: str) -> str:
        return self.diff

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


def _simulate_no_ff_landing_git_merge(
    git: _LandingSourceGit,
    *,
    source_ref: str = "feature/landing",
    target_branch: str = "main",
    merge_sha: str = "merge-a",
) -> None:
    git.heads[target_branch] = merge_sha
    git.merged_refs.add((source_ref, target_branch))
    source_sha = git.heads.get(source_ref)
    if source_sha is not None:
        git.ancestors.add((source_sha, merge_sha))


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


def _finalize_landing_merge_state(
    store: SqliteTaskStore,
    identity: Any,
    _decision: Any,
    provenance: str,
) -> Any:
    from gza.merge_services import ManualMergeExecutionResult, mark_merge_subject_merged

    mark_merge_subject_merged(
        store,
        merge_subject=identity.owner_task,
        merge_unit_id=identity.merge_unit_id,
        merge_source=provenance,
    )
    return ManualMergeExecutionResult(rc=0, status="merged")


def _pending_finalization_identity_and_authorization(
    store: SqliteTaskStore,
    impl: Task,
    *,
    current_target_sha: str = "merge-a",
    prepared_target_sha: str = "target-a",
) -> tuple[Any, MergeLandingAuthorization]:
    assert impl.id is not None
    unit = store.resolve_merge_unit_for_task(impl.id)
    assert unit is not None
    identity = LandingResolvedIdentity(
        selected_task_id=impl.id,
        owner_task=impl,
        representative_task=impl,
        merge_unit_id=unit.id,
        merge_unit_state="unmerged",
        source_branch="feature/landing",
        source_ref="feature/landing",
        source_sha="head-a",
        target_branch="main",
        target_sha=current_target_sha,
        current_branch="main",
        member_task_ids=(impl.id,),
        already_merged=True,
    )
    authorization = MergeLandingAuthorization(
        owner_task_id=impl.id,
        merge_unit_id=unit.id,
        source_branch="feature/landing",
        source_ref="feature/landing",
        target_branch="main",
        source_sha="head-a",
        target_sha=prepared_target_sha,
        representative_task_id=impl.id,
        member_task_ids=(impl.id,),
        review_id="gza-review",
        reviewed_head="head-a",
        review_mode="plain_full",
        review_verdict="APPROVED",
        verify_epoch="verify-1",
        verify_verdict="passed",
        verify_gate_identity="main",
    )
    return identity, authorization


def _pending_finalization_metadata(
    authorization: MergeLandingAuthorization,
    *,
    stage: Any = "prepared",
    prepared_target_sha: Any = "target-a",
    post_merge_target_sha: Any = None,
) -> dict[str, Any]:
    metadata = {
        "kind": "landing_pending_finalization",
        "stage": stage,
        "authorization": authorization.__dict__,
        "provenance": "manual_land",
        "prepared_target_sha": prepared_target_sha,
        "post_merge_target_sha": post_merge_target_sha,
        "deferred_task_ids": (),
        "followup_task_ids": (),
    }
    if stage == "__missing__":
        metadata.pop("stage")
    return metadata


def _persist_pending_finalization_artifact(
    store: SqliteTaskStore,
    impl: Task,
    metadata: dict[str, Any],
) -> None:
    assert impl.id is not None
    body = json.dumps(metadata, sort_keys=True, default=str)
    digest = sha256(body.encode()).hexdigest()
    store.add_artifact(
        impl.id,
        kind="landing_pending_finalization",
        label="landing_pending_finalization",
        path=f".gza/artifacts/{impl.id}/landing-pending-finalization-test-{digest}.json",
        content_type="application/json",
        byte_size=len(body.encode()),
        sha256=digest,
        producer="test",
        status="pending",
        head_sha="head-a",
        metadata=metadata,
    )


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


def _sqlite_table_snapshot(store: SqliteTaskStore, table: str) -> tuple[tuple[Any, ...], ...]:
    if table not in {"tasks", "merge_units", "merge_unit_tasks", "task_artifacts"}:
        raise ValueError(f"unsupported snapshot table: {table}")
    with store._connect() as conn:
        columns = tuple(str(row["name"]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall())
        assert columns
        selected = ", ".join(f'"{column}"' for column in columns)
        rows = conn.execute(f"SELECT {selected} FROM {table} ORDER BY rowid").fetchall()
    return tuple(tuple(row[column] for column in columns) for row in rows)


def _landing_durable_snapshot(store: SqliteTaskStore) -> dict[str, tuple[tuple[Any, ...], ...]]:
    return {
        table: _sqlite_table_snapshot(store, table)
        for table in ("tasks", "merge_units", "merge_unit_tasks", "task_artifacts")
    }


def _sqlite_task_snapshot(store: SqliteTaskStore) -> tuple[tuple[Any, ...], ...]:
    return _sqlite_table_snapshot(store, "tasks")


def _sqlite_merge_unit_snapshot(store: SqliteTaskStore) -> tuple[tuple[Any, ...], ...]:
    return _sqlite_table_snapshot(store, "merge_units")


def _sqlite_artifact_snapshot(store: SqliteTaskStore) -> tuple[tuple[Any, ...], ...]:
    return _sqlite_table_snapshot(store, "task_artifacts")


def _persist_exact_landing_pending_finalization(
    store: SqliteTaskStore,
    *,
    impl: Task,
    unit_id: str,
    source_ref: str,
    source_sha: str,
    target_branch: str,
    target_sha: str,
    provenance: Literal["manual_land", "manual_land_escalated"] = "manual_land",
    deferred_task_ids: tuple[str, ...] = (),
    followup_task_ids: tuple[str, ...] = (),
) -> Any:
    assert impl.id is not None
    authorization = MergeLandingAuthorization(
        owner_task_id=impl.id,
        merge_unit_id=unit_id,
        source_branch=source_ref,
        source_ref=source_ref,
        target_branch=target_branch,
        source_sha=source_sha,
        target_sha=target_sha,
        merge_unit_head_sha=None,
        merge_unit_base_sha=None,
        representative_task_id=impl.id,
        member_task_ids=(impl.id,),
        policy_version="strict.v1" if provenance == "manual_land" else "guarded.v1",
        schema_version="landing.strict.v1" if provenance == "manual_land" else "landing_judge.v1",
        allowed_overrides=()
        if provenance == "manual_land"
        else ("defer-review-blockers", "parked:review-max-cycles-reached"),
        judgment_artifact_id=None if provenance == "manual_land" else "judge-artifact",
        judgment_key=None if provenance == "manual_land" else "judge-key",
    )
    payload = {
        "kind": "landing_pending_finalization",
        "authorization": authorization.__dict__,
        "provenance": provenance,
        "post_merge_target_sha": target_sha,
        "deferred_task_ids": deferred_task_ids,
        "followup_task_ids": followup_task_ids,
    }
    body = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return store.add_artifact(
        impl.id,
        kind="landing_pending_finalization",
        label="landing_pending_finalization",
        path=f".gza/artifacts/{impl.id}/landing-pending-finalization-test.json",
        byte_size=len(body.encode()),
        sha256=sha256(body.encode()).hexdigest(),
        metadata=payload,
        status="pending",
        head_sha=target_sha,
    )


def _persist_pending_finalization_with_authorization(
    store: SqliteTaskStore,
    *,
    impl: Task,
    authorization: MergeLandingAuthorization,
    target_sha: str,
    provenance: Literal["manual_land", "manual_land_escalated"] = "manual_land_escalated",
    deferred_task_ids: tuple[str, ...] = (),
    followup_task_ids: tuple[str, ...] = (),
) -> Any:
    assert impl.id is not None
    payload = {
        "kind": "landing_pending_finalization",
        "authorization": authorization.__dict__,
        "provenance": provenance,
        "post_merge_target_sha": target_sha,
        "deferred_task_ids": deferred_task_ids,
        "followup_task_ids": followup_task_ids,
    }
    body = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return store.add_artifact(
        impl.id,
        kind="landing_pending_finalization",
        label="landing_pending_finalization",
        path=f".gza/artifacts/{impl.id}/landing-pending-finalization-authorized-test.json",
        byte_size=len(body.encode()),
        sha256=sha256(body.encode()).hexdigest(),
        metadata=payload,
        status="pending",
        head_sha=target_sha,
    )


def _set_merge_unit_proof_fields(
    store: SqliteTaskStore,
    unit_id: str,
    *,
    source_branch: str = "feature/landing",
    target_branch: str = "main",
    state: str = "unmerged",
    owner_task_id: str | None | object = None,
    head_sha: str | None = "head-a",
    base_sha: str | None = "base-a",
) -> None:
    values = {
        "source_branch": source_branch,
        "target_branch": target_branch,
        "state": state,
        "head_sha": head_sha,
        "base_sha": base_sha,
    }
    assignments = [f"{name} = ?" for name in values]
    params: list[Any] = list(values.values())
    if owner_task_id is not None:
        assignments.append("owner_task_id = ?")
        params.append(owner_task_id)
    params.extend([store._project_id, unit_id])
    with store._write_transaction() as conn:
        conn.execute(
            f"UPDATE merge_units SET {', '.join(assignments)} WHERE project_id = ? AND id = ?",
            tuple(params),
        )


def _authorized_pending_deferred_task(
    store: SqliteTaskStore,
    *,
    impl: Task,
    review: Task,
    finding: ReviewFinding,
) -> tuple[Task, str]:
    from gza.review_tasks import (
        build_deferred_blocker_prompt,
        format_blocker_finding_context,
    )

    assert impl.id is not None
    assert review.id is not None
    prompt = build_deferred_blocker_prompt(review.id, impl.id, finding)
    review_scope = format_blocker_finding_context(finding)
    task = store.add(
        prompt,
        task_type="implement",
        based_on=review.id,
        depends_on=impl.id,
        review_scope=review_scope,
        urgent=True,
        create_pr=True,
    )
    payload = {
        "finding_id": finding.id,
        "review_id": review.id,
        "impl_task_id": impl.id,
        "prompt_sha256": "sha256:" + sha256(prompt.encode()).hexdigest(),
        "review_scope_sha256": "sha256:" + sha256(review_scope.encode()).hexdigest(),
    }
    return task, json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _authorized_pending_followup_task(
    store: SqliteTaskStore,
    *,
    config: Config | None = None,
    impl: Task,
    review: Task,
    finding: ReviewFinding,
    escalated: bool = False,
) -> tuple[Task, str]:
    from gza.review_tasks import create_or_reuse_followup_task

    assert impl.id is not None
    assert review.id is not None
    task, _created = create_or_reuse_followup_task(
        store,
        config=None,
        review_task=review,
        impl_task=impl,
        finding=finding,
        trigger_source="manual_land",
    )
    if escalated:
        task.urgent = True
        task.create_pr = True
        store.update(task)
        refreshed = store.get(task.id)
        assert refreshed is not None
        task = refreshed
    payload = {
        "review": review.id,
        "source": f"review:{review.id}",
        "finding": finding.id,
        "content": _landing_review_finding_fingerprint_for_test(finding),
    }
    return task, json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _pending_replay_authorization(
    *,
    impl: Task,
    unit_id: str,
    deferred_identity: str | None = None,
    followup_identity: str | None = None,
    review_id: str,
    review_verdict: str = "CHANGES_REQUESTED",
    source_sha: str = "head-a",
    target_sha: str = "merge-a",
    source_branch: str = "feature/landing",
    source_ref: str = "feature/landing",
    merge_unit_head_sha: str | None = "head-a",
    merge_unit_base_sha: str | None = "base-a",
) -> MergeLandingAuthorization:
    assert impl.id is not None
    return MergeLandingAuthorization(
        owner_task_id=impl.id,
        merge_unit_id=unit_id,
        source_branch=source_branch,
        source_ref=source_ref,
        target_branch="main",
        source_sha=source_sha,
        target_sha="target-a",
        merge_unit_head_sha=merge_unit_head_sha,
        merge_unit_base_sha=merge_unit_base_sha,
        representative_task_id=impl.id,
        member_task_ids=(impl.id,),
        policy_version="guarded.v1" if review_verdict == "CHANGES_REQUESTED" else "strict.v1",
        schema_version="landing_judge.v1" if review_verdict == "CHANGES_REQUESTED" else "landing.strict.v1",
        allowed_overrides=("defer-review-blockers", "parked:review-max-cycles-reached")
        if review_verdict == "CHANGES_REQUESTED"
        else (),
        judgment_artifact_id="judge-artifact" if review_verdict == "CHANGES_REQUESTED" else None,
        judgment_key="judge-key" if review_verdict == "CHANGES_REQUESTED" else None,
        review_id=review_id,
        review_verdict=review_verdict,
        blocker_identities=(
            json.dumps(
                {
                    "finding_id": "B1",
                    "fingerprint": "blocker:B1:normalized",
                    "source": f"review:{review_id}",
                    "class": "out_of_scope",
                    "deferrable": True,
                },
                sort_keys=True,
                separators=(",", ":"),
            ),
        )
        if deferred_identity is not None
        else (),
        deferred_blocker_task_identities=(deferred_identity,) if deferred_identity is not None else (),
        followup_identities=(followup_identity,) if followup_identity is not None else (),
        followup_fingerprints=(followup_identity,) if followup_identity is not None else (),
    )


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


def test_landing_coordinator_checkpoint_gates_unmerged_already_landed_merge_truth(tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "already landed", "feature/already")
    unit = store.get_or_create_merge_unit_for_task(impl)
    assert unit is not None and impl.id is not None
    git = _LandingSourceGit(
        {"feature/already": "source-a", "main": "target-a"},
        local_branches={"feature/already"},
        merged_refs={("feature/already", "main")},
        ancestors={("target-a", "source-a")},
    )
    merge_calls: list[str] = []
    finalizations: list[str] = []

    def inspect(identity: Any) -> LandingPolicyFacts:
        return _green_facts(
            task_id=identity.owner_task_id,
            source_head=identity.source_sha,
            target_head=identity.target_sha,
            review=_review(verdict="APPROVED", reviewed_head=identity.source_sha),
            open_blockers=(),
        )

    def merge(*_args: Any, **_kwargs: Any) -> Any:
        merge_calls.append("git_merge")
        raise AssertionError("already-merged git truth must not merge again")

    def post_merge_verify(identity: Any) -> LandPostMergeVerifySuccess:
        assert store.get_merge_unit(unit.id).state == "unmerged"  # type: ignore[union-attr]
        return _post_merge_success(identity)

    def finalize(identity: Any, decision: Any, provenance: str) -> Any:
        finalizations.append(provenance)
        return _finalize_landing_merge_state(store, identity, decision, provenance)

    result = LandingCoordinator(
        store=store,
        git=git,
        inspect_policy_facts=inspect,
        execute_merge=merge,
        finalize_merge=finalize,
        post_merge_verifier=post_merge_verify,
    ).run(LandRequest(task_id=impl.id))
    refreshed_unit = store.get_merge_unit(unit.id)

    assert result.already_merged is False
    assert result.merged is False
    assert result.blocked is not None
    assert result.blocked.reason_code == "merge-proof-unavailable"
    assert merge_calls == []
    assert finalizations == []
    assert refreshed_unit is not None
    assert refreshed_unit.state == "unmerged"
    assert refreshed_unit.merge_source is None


@pytest.mark.parametrize("state", ("empty", "redundant"))
def test_landing_coordinator_completed_rebase_reroutes_unmerged_terminal_no_work_before_downstream_phases(
    tmp_path,
    state: str,
) -> None:
    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, f"post rebase {state}", f"feature/post-rebase-{state}")
    unit = store.get_or_create_merge_unit_for_task(impl)
    assert unit is not None and impl.id is not None
    git = _LandingSourceGit(
        {impl.branch or "": "source-before", "main": "target-a"},
        local_branches={impl.branch or ""},
        ancestors=set(),
    )
    rebase_done = False
    rebase_calls: list[RebaseServiceRequest] = []

    def _reconcile(_store: Any, current_unit: Any) -> TerminalProof | None:
        if not rebase_done:
            return None
        return TerminalProof(
            state=state,  # type: ignore[arg-type]
            identity=MergeUnitProofIdentity(
                source_branch=current_unit.source_branch,
                target_branch=current_unit.target_branch,
                state=current_unit.state,
                owner_task_id=current_unit.owner_task_id,
                head_sha=current_unit.head_sha,
                base_sha=current_unit.base_sha,
            ),
            source_sha="source-after",
            target_sha="target-a",
        )

    def _facts(identity: Any) -> LandingPolicyFacts:
        return _green_facts(
            task_id=identity.owner_task_id,
            source_head=identity.source_sha,
            target_head=identity.target_sha,
            rebase_target_contained=(identity.target_sha, identity.source_sha) in git.ancestors,
        )

    def _execute_rebase_service(**kwargs: Any) -> RebaseServiceResult:
        nonlocal rebase_done
        request = kwargs["request"]
        rebase_calls.append(request)
        rebase_done = True
        git.heads[request.branch] = "source-after"
        return RebaseServiceResult(
            status="completed_mechanical",
            parent_task_id=request.parent_task_id,
            branch=request.branch,
            target_ref=request.target_branch,
            rebase_task_id="gza-9999",
            changed_diff=False,
            artifact_id=42,
            artifact_key="rebase-outcome",
            source_head_before="source-before",
            target_head_before="target-a",
            source_head_after="source-after",
            target_head_after="target-a",
        )

    collaborators = LandingCollaborators(reconcile_terminal_state=_reconcile)

    result = LandingCoordinator(
        store=store,
        git=git,
        config=config,
        inspect_policy_facts=_facts,
        create_rebase_task=_unused_rebase_factory,
        rebase_executor=_unused_rebase_executor,
        execute_rebase_service=_execute_rebase_service,
        collaborators=collaborators,
    ).run(LandRequest(task_id=impl.id))

    refreshed_unit = store.get_merge_unit(unit.id)
    assert result.blocked is None
    assert result.terminal_outcome == state
    assert result.terminal_reconciled is True
    assert refreshed_unit is not None
    assert refreshed_unit.state == state
    assert rebase_calls and rebase_calls[0].parent_task_id == impl.id
    assert [step.phase for step in result.steps] == ["resolve", "rebase", "resolve", "merge"]
    assert git.mutation_calls == []


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


def _landing_review_finding_fingerprint_for_test(finding: ReviewFinding) -> str:
    from gza.review_verdict import get_review_finding_fingerprint

    fingerprint = get_review_finding_fingerprint(finding)
    assert fingerprint is not None
    title, anchor = fingerprint
    return json.dumps({"title": title, "anchor": anchor}, sort_keys=True, separators=(",", ":"))


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


def _park_for_review_max_cycles(store: SqliteTaskStore, impl: Task) -> None:
    store.upsert_watch_progress_observation(
        WatchProgressObservation(
            subject_kind="task",
            subject_id=impl.id or "",
            subject_task_id=impl.id,
            action_type="max_cycles_reached",
            action_reason="review-max-cycles-reached",
            evidence_fingerprint="park-review-max-cycles",
            parked_reason="review-max-cycles-reached",
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

    coordinator.run(LandRequest(task_id=impl.id, dry_run=True))
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
    assert second.policy_judgment_identity.startswith("sha256:")


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


def _store_backed_post_rebase_resolution_case(
    tmp_path,
    *,
    rebase_case: str | None = None,
    review_scope_overrides: dict[str, Any] | None = None,
) -> tuple[SqliteTaskStore, Config, Task, Task, Task, _LandingSourceGit]:
    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "store-backed post-rebase review binding", "feature/landing")
    assert impl.id is not None
    _persist_lifecycle_verify_for_landing(store, config, impl)
    rebase = _landing_rebase(store, impl)
    _persist_landing_rebase_outcome(
        store,
        rebase,
        impl,
        source_before="pre-head-a",
        target_before="target-a",
        merge_base_before="merge-base-a",
        source_after="head-a",
        target_after="target-a",
        status="provider_conflict_resolved",
        changed_diff=True,
        provider_conflict_resolved=True,
    )
    if rebase_case == "wrong-task-type":
        rebase.task_type = "implement"
        store.update(rebase)
    elif rebase_case == "wrong-lineage":
        other = store.add("Other implementation", task_type="implement")
        rebase.based_on = other.id
        store.update(rebase)
    elif rebase_case == "missing-provenance":
        rebase.review_scope = None
        store.update(rebase)
    elif rebase_case == "malformed-provenance":
        rebase.review_scope = "Rebase diff provenance: no"
        store.update(rebase)
    elif rebase_case == "incomplete-provenance":
        rebase.review_scope = build_rebase_diff_provenance(
            baseline=RebaseDiffBaseline(
                old_tip="pre-head-a",
                target_at_start="target-a",
                merge_base_at_start=None,
            ),
            resolved_head_sha="head-a",
            resolved_target_sha="target-a",
        )
        store.update(rebase)
    elif rebase_case == "head-mismatched":
        rebase.review_scope = build_rebase_diff_provenance(
            baseline=RebaseDiffBaseline(
                old_tip="pre-head-a",
                target_at_start="target-a",
                merge_base_at_start="merge-base-a",
            ),
            resolved_head_sha="other-head",
            resolved_target_sha="target-a",
        )
        store.update(rebase)
    elif rebase_case == "target-mismatched":
        rebase.review_scope = build_rebase_diff_provenance(
            baseline=RebaseDiffBaseline(
                old_tip="pre-head-a",
                target_at_start="target-a",
                merge_base_at_start="merge-base-a",
            ),
            resolved_head_sha="head-a",
            resolved_target_sha="other-target",
        )
        store.update(rebase)
    elif rebase_case is not None:
        raise AssertionError(f"unknown rebase case: {rebase_case}")

    review_kwargs = {
        "resolved_head": "head-a",
        "target": "target-a",
        "verify_head": "head-a",
        "pre_rebase_head": "pre-head-a",
        "pre_rebase_target": "target-a",
        "pre_rebase_merge_base": "merge-base-a",
        "completed_at": datetime(2026, 8, 26, 12, 30, tzinfo=UTC),
    }
    if review_scope_overrides:
        review_kwargs.update(review_scope_overrides)
    stale = _resolution_review(store, impl, rebase, status="completed", **review_kwargs)
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-a")},
    )
    return store, config, impl, rebase, stale, git


@pytest.mark.parametrize(
    "rebase_case",
    (
        "wrong-task-type",
        "wrong-lineage",
        "missing-provenance",
        "malformed-provenance",
        "incomplete-provenance",
        "head-mismatched",
        "target-mismatched",
    ),
)
def test_landing_default_inspector_rejects_resolution_review_when_rebase_identity_unbindable(
    tmp_path,
    rebase_case: str,
) -> None:
    store, config, impl, _rebase, stale, git = _store_backed_post_rebase_resolution_case(
        tmp_path,
        rebase_case=rebase_case,
    )
    coordinator = LandingCoordinator(store=store, git=git, config=config)

    identity = coordinator._resolve_identity(LandRequest(task_id=impl.id), persist_reconciliation=False)
    assert not isinstance(identity, LandBlocked)
    facts = coordinator._landing_policy_facts(identity)

    assert facts.review is not None
    assert facts.review.review_id == stale.id
    assert facts.review.current is False
    assert facts.review.identity_matched is False
    assert coordinator._first_execution_required_phase(identity, facts, policy="guarded") == "post_rebase_review"


def test_landing_default_inspector_rejects_resolution_review_referencing_missing_rebase_row(tmp_path) -> None:
    store, config, impl, rebase, stale, git = _store_backed_post_rebase_resolution_case(tmp_path)
    stale.review_scope = build_resolution_review_scope(
        implementation_task_id=impl.id,
        rebase_task_id="gza-missing-rebase",
        resolved_head_sha="head-a",
        resolved_target_sha="target-a",
        pre_rebase_head_sha="pre-head-a",
        pre_rebase_target_sha="target-a",
        pre_rebase_merge_base_sha="merge-base-a",
    )
    store.update(stale)
    coordinator = LandingCoordinator(store=store, git=git, config=config)

    identity = coordinator._resolve_identity(LandRequest(task_id=impl.id), persist_reconciliation=False)
    assert not isinstance(identity, LandBlocked)
    facts = coordinator._landing_policy_facts(identity)

    assert rebase.id != "gza-missing-rebase"
    assert facts.review is not None
    assert facts.review.review_id == stale.id
    assert facts.review.current is False
    assert facts.review.identity_matched is False
    assert coordinator._first_execution_required_phase(identity, facts, policy="guarded") == "post_rebase_review"


@pytest.mark.parametrize("field", ("head", "target", "merge_base"))
@pytest.mark.parametrize("case", ("missing", "mismatched"))
def test_landing_default_inspector_rejects_resolution_review_with_stale_pre_rebase_scope(
    tmp_path,
    field: str,
    case: str,
) -> None:
    values = {
        "pre_rebase_head": "pre-head-a",
        "pre_rebase_target": "target-a",
        "pre_rebase_merge_base": "merge-base-a",
    }
    key = f"pre_rebase_{field}"
    values[key] = None if case == "missing" else f"other-{field}"
    store, config, impl, _rebase, stale, git = _store_backed_post_rebase_resolution_case(
        tmp_path,
        review_scope_overrides=values,
    )
    coordinator = LandingCoordinator(store=store, git=git, config=config)

    identity = coordinator._resolve_identity(LandRequest(task_id=impl.id), persist_reconciliation=False)
    assert not isinstance(identity, LandBlocked)
    facts = coordinator._landing_policy_facts(identity)

    assert facts.review is not None
    assert facts.review.review_id == stale.id
    assert facts.review.current is False
    assert facts.review.identity_matched is False
    assert coordinator._first_execution_required_phase(identity, facts, policy="guarded") == "post_rebase_review"


@pytest.mark.parametrize(
    "rebase_case",
    (
        "wrong-task-type",
        "wrong-lineage",
        "missing-provenance",
        "malformed-provenance",
        "incomplete-provenance",
        "head-mismatched",
        "target-mismatched",
    ),
)
def test_landing_coordinator_creates_one_full_fallback_when_rebase_identity_unbindable(
    tmp_path,
    rebase_case: str,
) -> None:
    store, config, impl, _rebase, stale, git = _store_backed_post_rebase_resolution_case(
        tmp_path,
        rebase_case=rebase_case,
    )
    created: list[Task] = []

    def create_full(*_args: Any, **_kwargs: Any) -> Task:
        review = store.add("Created full fallback", task_type="review", depends_on=impl.id, based_on=impl.id)
        review.status = "pending"
        review.review_verify_head_sha = "head-a"
        store.update(review)
        created.append(review)
        return review

    coordinator = LandingCoordinator(
        store=store,
        git=git,
        config=config,
        create_full_review=create_full,
        create_resolution_review=_fail_improve_or_review_route,
        execute_merge=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not merge")),
    )
    identity = coordinator._resolve_identity(LandRequest(task_id=impl.id), persist_reconciliation=False)
    assert not isinstance(identity, LandBlocked)
    facts = coordinator._landing_policy_facts(identity)

    step, blocked, _decision, _selected_facts = coordinator._run_post_rebase_review_phase(
        identity,
        facts,
        policy="guarded",
    )

    assert blocked is not None
    assert blocked.reason_code == "required-review-unavailable"
    assert step.phase == "post_rebase_review"
    assert len(created) == 1
    assert created[0].review_verify_head_sha == "head-a"
    assert created[0].review_scope is None
    assert created[0].id != stale.id


@pytest.mark.parametrize("field", ("head", "target", "merge_base"))
@pytest.mark.parametrize("case", ("missing", "mismatched"))
def test_landing_coordinator_creates_one_replacement_resolution_review_when_pre_rebase_scope_stale(
    tmp_path,
    field: str,
    case: str,
) -> None:
    values = {
        "pre_rebase_head": "pre-head-a",
        "pre_rebase_target": "target-a",
        "pre_rebase_merge_base": "merge-base-a",
    }
    values[f"pre_rebase_{field}"] = None if case == "missing" else f"other-{field}"
    store, config, impl, rebase, stale, git = _store_backed_post_rebase_resolution_case(
        tmp_path,
        review_scope_overrides=values,
    )
    created: list[Task] = []

    def create_resolution(*_args: Any, **kwargs: Any) -> Task:
        review = _resolution_review(
            store,
            impl,
            rebase,
            status="pending",
            resolved_head=kwargs["resolved_head_sha"],
            target=kwargs["resolved_target_sha"],
            verify_head=kwargs["resolved_head_sha"],
            pre_rebase_target="target-a",
        )
        created.append(review)
        return review

    coordinator = LandingCoordinator(
        store=store,
        git=git,
        config=config,
        create_full_review=_fail_improve_or_review_route,
        create_resolution_review=create_resolution,
        execute_merge=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not merge")),
    )
    identity = coordinator._resolve_identity(LandRequest(task_id=impl.id), persist_reconciliation=False)
    assert not isinstance(identity, LandBlocked)
    facts = coordinator._landing_policy_facts(identity)

    step, blocked, _decision, _selected_facts = coordinator._run_post_rebase_review_phase(
        identity,
        facts,
        policy="guarded",
    )

    assert blocked is not None
    assert blocked.reason_code == "required-review-unavailable"
    assert step.phase == "post_rebase_review"
    assert len(created) == 1
    assert created[0].id != stale.id
    assert created[0].review_verify_head_sha == "head-a"
    assert created[0].review_scope is not None


def test_landing_coordinator_reuses_exact_full_fallback_when_unbindable_rebase_review_budget_spent(
    tmp_path,
) -> None:
    store, config, impl, _rebase, stale, git = _store_backed_post_rebase_resolution_case(
        tmp_path,
        rebase_case="missing-provenance",
    )
    full = _completed_full_review(
        store,
        impl,
        head="head-a",
        completed_at=datetime(2026, 8, 26, 12, 10, tzinfo=UTC),
    )
    coordinator = LandingCoordinator(
        store=store,
        git=git,
        config=config,
        create_full_review=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("no create")),
        create_resolution_review=_fail_improve_or_review_route,
        execute_merge=lambda *_args, **_kwargs: ManualMergeExecutionResult(success=False, message="stop before merge"),
    )
    coordinator.post_rebase_review_budget_used = True

    identity = coordinator._resolve_identity(LandRequest(task_id=impl.id), persist_reconciliation=False)
    assert not isinstance(identity, LandBlocked)
    facts = coordinator._landing_policy_facts(identity)

    step, blocked, decision, selected_facts = coordinator._run_post_rebase_review_phase(
        identity,
        facts,
        policy="guarded",
    )

    assert blocked is None
    assert decision is not None
    assert decision.allowed is True
    assert selected_facts.review is not None
    assert selected_facts.review.review_id == full.id
    assert selected_facts.review.mode == "plain_full"
    assert selected_facts.review.verdict == "APPROVED"
    assert selected_facts.review.followup_findings == ()
    assert selected_facts.open_blockers == ()
    authorization = landing_module.landing_merge_authorization_from_facts(
        identity=identity,
        facts=selected_facts,
        decision=decision,
    )
    assert authorization.review_id == full.id
    assert authorization.review_mode == "plain_full"
    assert authorization.review_verdict == "APPROVED"
    assert authorization.blocker_identities == ()
    assert step.status == "completed"
    assert str(full.id) in step.summary
    assert str(stale.id) not in step.summary


def test_landing_coordinator_blocks_unbindable_rebase_full_fallback_when_budget_spent(
    tmp_path,
) -> None:
    store, config, impl, _rebase, _stale, git = _store_backed_post_rebase_resolution_case(
        tmp_path,
        rebase_case="missing-provenance",
    )
    coordinator = LandingCoordinator(
        store=store,
        git=git,
        config=config,
        create_full_review=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("budget spent")),
        create_resolution_review=_fail_improve_or_review_route,
        execute_merge=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not merge")),
    )
    coordinator.post_rebase_review_budget_used = True

    identity = coordinator._resolve_identity(LandRequest(task_id=impl.id), persist_reconciliation=False)
    assert not isinstance(identity, LandBlocked)
    facts = coordinator._landing_policy_facts(identity)

    _step, blocked, _decision, _selected_facts = coordinator._run_post_rebase_review_phase(
        identity,
        facts,
        policy="guarded",
    )

    assert blocked is not None
    assert blocked.reason_code == "bounded-attempt-exhausted"


def test_landing_coordinator_reuses_exact_resolution_replacement_when_pre_rebase_scope_stale_budget_spent(
    tmp_path,
) -> None:
    store, config, impl, rebase, stale, git = _store_backed_post_rebase_resolution_case(
        tmp_path,
        review_scope_overrides={"pre_rebase_merge_base": "other-merge-base"},
    )
    exact = _resolution_review(
        store,
        impl,
        rebase,
        status="completed",
        resolved_head="head-a",
        target="target-a",
        verify_head="head-a",
        pre_rebase_target="target-a",
        completed_at=datetime(2026, 8, 26, 12, 10, tzinfo=UTC),
    )
    coordinator = LandingCoordinator(
        store=store,
        git=git,
        config=config,
        create_full_review=_fail_improve_or_review_route,
        create_resolution_review=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("no create")),
        execute_merge=lambda *_args, **_kwargs: ManualMergeExecutionResult(success=False, message="stop before merge"),
    )
    coordinator.post_rebase_review_budget_used = True

    identity = coordinator._resolve_identity(LandRequest(task_id=impl.id), persist_reconciliation=False)
    assert not isinstance(identity, LandBlocked)
    facts = coordinator._landing_policy_facts(identity)

    step, blocked, _decision, _selected_facts = coordinator._run_post_rebase_review_phase(
        identity,
        facts,
        policy="guarded",
    )

    assert blocked is None
    assert step.status == "completed"
    assert str(exact.id) in step.summary
    assert str(stale.id) not in step.summary


def test_landing_coordinator_blocks_stale_pre_rebase_resolution_replacement_when_budget_spent(
    tmp_path,
) -> None:
    store, config, impl, _rebase, _stale, git = _store_backed_post_rebase_resolution_case(
        tmp_path,
        review_scope_overrides={"pre_rebase_merge_base": "other-merge-base"},
    )
    coordinator = LandingCoordinator(
        store=store,
        git=git,
        config=config,
        create_full_review=_fail_improve_or_review_route,
        create_resolution_review=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("budget spent")),
        execute_merge=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not merge")),
    )
    coordinator.post_rebase_review_budget_used = True

    identity = coordinator._resolve_identity(LandRequest(task_id=impl.id), persist_reconciliation=False)
    assert not isinstance(identity, LandBlocked)
    facts = coordinator._landing_policy_facts(identity)

    _step, blocked, _decision, _selected_facts = coordinator._run_post_rebase_review_phase(
        identity,
        facts,
        policy="guarded",
    )

    assert blocked is not None
    assert blocked.reason_code == "bounded-attempt-exhausted"


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


@pytest.mark.parametrize("wrong_review_mode", ("unrelated_head", "resolution"))
def test_landing_coordinator_preserves_exact_carry_forward_changes_requested_over_newer_approval(
    tmp_path,
    wrong_review_mode: str,
) -> None:
    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "strict carry-forward chronology", "feature/landing")
    assert impl.id is not None
    _persist_lifecycle_verify_for_landing(store, config, impl, reviewed_head="head-b")
    rebase = _landing_rebase(
        store,
        impl,
        old_tip="head-a",
        target_at_start="target-a",
        merge_base_at_start="base-a",
        resolved_head="head-b",
        resolved_target="target-a",
    )
    _persist_landing_rebase_outcome(
        store,
        rebase,
        impl,
        source_before="head-a",
        target_before="target-a",
        merge_base_before="base-a",
        source_after="head-b",
        target_after="target-a",
        status="completed_mechanical",
        changed_diff=False,
        provider_conflict_resolved=False,
    )
    exact = _completed_full_review(
        store,
        impl,
        head="head-a",
        verdict="CHANGES_REQUESTED",
        completed_at=datetime(2026, 8, 26, 12, 0, tzinfo=UTC),
    )
    if wrong_review_mode == "unrelated_head":
        wrong = _completed_full_review(
            store,
            impl,
            head="other-head",
            verdict="APPROVED",
            completed_at=datetime(2026, 8, 26, 12, 1, tzinfo=UTC),
        )
    else:
        wrong = _resolution_review(
            store,
            impl,
            rebase,
            status="completed",
            resolved_head="head-b",
            target="target-a",
            verify_head="head-b",
            pre_rebase_head="head-a",
            pre_rebase_target="target-a",
            pre_rebase_merge_base="base-a",
            verdict="APPROVED",
            completed_at=datetime(2026, 8, 26, 12, 1, tzinfo=UTC),
        )
    git = _LandingSourceGit(
        {"feature/landing": "head-b", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-b")},
    )

    def inspect(identity: Any) -> LandingPolicyFacts:
        return _green_facts(
            task_id=identity.owner_task_id,
            source_head=identity.source_sha,
            target_head=identity.target_sha,
            rebase_status="completed",
            rebase_resolution_kind="mechanical",
            rebase_changed_diff=False,
            rebase_outcome_id="mechanical-a-to-b",
            rebase_attempted_source_head="head-a",
            rebase_attempted_target_head="target-a",
            rebase_target_contained=True,
            rebase_provider_resolution_proof=False,
            review=_review(
                review_id=wrong.id,
                verdict="APPROVED",
                current=False,
                identity_matched=False,
                reviewed_head=wrong.review_verify_head_sha,
            ),
        )

    result = LandingCoordinator(
        store=store,
        git=git,
        inspect_policy_facts=inspect,
        create_full_review=_fail_improve_or_review_route,
        create_resolution_review=_fail_improve_or_review_route,
    ).run(LandRequest(task_id=impl.id, policy="strict"))

    assert result.blocked is not None
    assert result.blocked.reason_code == "nondeferrable-blocker"
    assert f"review:{exact.id}" in result.blocked.evidence_refs
    assert f"review:{wrong.id}" not in result.blocked.evidence_refs
    assert all(step.phase != "merge" for step in result.steps)

    production = LandingCoordinator(
        store=store,
        git=git,
        config=config,
        live_tree_fingerprint_resolver=lambda: TREE_A,
        create_full_review=_fail_improve_or_review_route,
        create_resolution_review=_fail_improve_or_review_route,
    )
    identity = production._resolve_identity(LandRequest(task_id=impl.id, policy="strict"), persist_reconciliation=False)
    assert not isinstance(identity, LandBlocked)
    assert refresh_landing_authorization(
        production,
        identity,
        LandingPolicyDecision(True),
        policy="strict",
    ) is None


def test_landing_coordinator_preserves_exact_carry_forward_approval_over_newer_changes_requested(
    tmp_path,
) -> None:
    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "strict carry-forward inverse chronology", "feature/landing")
    assert impl.id is not None
    _persist_lifecycle_verify_for_landing(store, config, impl, reviewed_head="head-b")
    rebase = _landing_rebase(
        store,
        impl,
        old_tip="head-a",
        target_at_start="target-a",
        merge_base_at_start="base-a",
        resolved_head="head-b",
        resolved_target="target-a",
    )
    _persist_landing_rebase_outcome(
        store,
        rebase,
        impl,
        source_before="head-a",
        target_before="target-a",
        merge_base_before="base-a",
        source_after="head-b",
        target_after="target-a",
        status="completed_mechanical",
        changed_diff=False,
        provider_conflict_resolved=False,
    )
    exact = _completed_full_review(
        store,
        impl,
        head="head-a",
        verdict="APPROVED",
        completed_at=datetime(2026, 8, 26, 12, 0, tzinfo=UTC),
    )
    wrong = _completed_full_review(
        store,
        impl,
        head="other-head",
        verdict="CHANGES_REQUESTED",
        completed_at=datetime(2026, 8, 26, 12, 1, tzinfo=UTC),
    )
    git = _LandingSourceGit(
        {"feature/landing": "head-b", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-b")},
    )
    merged: list[tuple[str | None, str | None]] = []

    def inspect(identity: Any) -> LandingPolicyFacts:
        return _green_facts(
            task_id=identity.owner_task_id,
            source_head=identity.source_sha,
            target_head=identity.target_sha,
            rebase_status="completed",
            rebase_resolution_kind="mechanical",
            rebase_changed_diff=False,
            rebase_outcome_id="mechanical-a-to-b",
            rebase_attempted_source_head="head-a",
            rebase_attempted_target_head="target-a",
            rebase_target_contained=True,
            rebase_provider_resolution_proof=False,
            review=_review(
                review_id=wrong.id,
                verdict="CHANGES_REQUESTED",
                current=False,
                identity_matched=False,
                reviewed_head=wrong.review_verify_head_sha,
            ),
        )

    def merge(identity: Any, decision: LandingPolicyDecision, provenance: str) -> ManualMergeExecutionResult:
        assert provenance == "manual_land"
        merged.append((exact.id, "APPROVED"))
        _simulate_no_ff_landing_git_merge(git)
        return ManualMergeExecutionResult(rc=0, status="merged")

    production = LandingCoordinator(
        store=store,
        git=git,
        config=config,
        live_tree_fingerprint_resolver=lambda: TREE_A,
        create_full_review=_fail_improve_or_review_route,
        create_resolution_review=_fail_improve_or_review_route,
    )
    identity = production._resolve_identity(LandRequest(task_id=impl.id, policy="strict"), persist_reconciliation=False)
    assert not isinstance(identity, LandBlocked)
    refreshed = refresh_landing_authorization(
        production,
        identity,
        LandingPolicyDecision(True),
        policy="strict",
    )
    assert refreshed is not None
    assert refreshed.review_id == exact.id
    assert refreshed.review_verdict == "APPROVED"
    assert refreshed.review_mode == "plain_full"

    result = LandingCoordinator(
        store=store,
        git=git,
        inspect_policy_facts=inspect,
        create_full_review=_fail_improve_or_review_route,
        create_resolution_review=_fail_improve_or_review_route,
        execute_merge=merge,
        finalize_merge=lambda identity, decision, provenance: _finalize_landing_merge_state(
            store, identity, decision, provenance
        ),
        post_merge_verifier=lambda identity: _post_merge_success(identity),
    ).run(LandRequest(task_id=impl.id, policy="strict"))

    assert result.blocked is None
    assert result.merged is True
    assert merged == [(exact.id, "APPROVED")]
    post_rebase_step = next(step for step in result.steps if step.phase == "post_rebase_review")
    assert str(exact.id) in post_rebase_step.evidence_refs
    assert str(wrong.id) not in post_rebase_step.evidence_refs
    pending_artifacts = store.list_artifacts(impl.id, kind="landing_pending_finalization")
    assert pending_artifacts
    authorization = pending_artifacts[0].metadata["authorization"]
    assert authorization["review_id"] == exact.id
    assert authorization["review_verdict"] == "APPROVED"
    assert authorization["review_mode"] == "plain_full"


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
        _simulate_no_ff_landing_git_merge(git)
        return ManualMergeExecutionResult(
            rc=0,
            status="merged",
            created_deferred_blockers=[blocker],
        )

    def post_merge_verify(identity: Any) -> LandPostMergeVerifySuccess:
        order.append("post_merge_verify")
        return _post_merge_success(identity)

    result = LandingCoordinator(
        store=store,
        git=git,
        config=config,
        inspect_policy_facts=inspect,
        landing_judge=judge,
        execute_merge=merge,
        finalize_merge=lambda identity, decision, provenance: _finalize_landing_merge_state(
            store, identity, decision, provenance
        ),
        post_merge_verifier=post_merge_verify,
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
    assert [step.phase for step in result.steps[-4:]] == ["judge", "defer_blockers", "merge", "post_merge_verify"]


def _motivating_full_cycle_landing_fixture(tmp_path) -> dict[str, Any]:
    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "motivating guarded landing", "feature/landing")
    assert impl.id is not None
    unit = store.resolve_merge_unit_for_task(impl.id)
    assert unit is not None
    stale_review = _completed_full_review(store, impl, head="source-before", verdict="APPROVED")
    _park_for_review_max_cycles(store, impl)
    git = _LandingSourceGit(
        {"feature/landing": "source-before", "main": "target-a"},
        local_branches={"feature/landing"},
        can_merge_refs=set(),
    )
    state: dict[str, Any] = {
        "store": store,
        "config": config,
        "impl": impl,
        "unit": unit,
        "git": git,
        "stale_review_id": stale_review.id,
        "rebase_done": False,
        "coordinator": None,
        "latest_identity": None,
        "latest_facts": None,
        "review_executor_calls": [],
        "order": [],
    }

    def inspect(identity: Any) -> LandingPolicyFacts:
        coordinator = state["coordinator"]
        assert coordinator is not None
        saved = coordinator.inspect_policy_facts
        coordinator.inspect_policy_facts = None
        try:
            facts = coordinator._landing_policy_facts(identity)
        finally:
            coordinator.inspect_policy_facts = saved
        state["latest_identity"] = identity
        state["latest_facts"] = facts
        return facts

    def execute_rebase_service(**kwargs: Any) -> RebaseServiceResult:
        state["order"].append("rebase")
        request = kwargs["request"]
        rebase = store.add("landing conflict rebase", task_type="rebase", based_on=impl.id, same_branch=True)
        store.mark_completed(rebase, has_commits=True, branch="feature/landing", changed_diff=False)
        _persist_landing_rebase_outcome(
            store,
            rebase,
            impl,
            source_before="source-before",
            target_before="target-a",
            source_after="source-after",
            target_after="target-a",
            status="provider_conflict_resolved",
            changed_diff=False,
            provider_conflict_resolved=True,
        )
        state["rebase_done"] = True
        git.heads["feature/landing"] = "source-after"
        git.ancestors.add(("target-a", "source-after"))
        git.can_merge_refs = {("feature/landing", "main")}
        _persist_lifecycle_verify_for_landing(store, config, impl, reviewed_head="source-after")
        return RebaseServiceResult(
            status="provider_conflict_resolved",
            parent_task_id=request.parent_task_id,
            branch=request.branch,
            target_ref=request.target_branch,
            rebase_task_id=rebase.id,
            changed_diff=False,
            artifact_id=1,
            artifact_key="rebase-outcome-1",
            source_head_before="source-before",
            target_head_before="target-a",
            source_head_after="source-after",
            target_head_after="target-a",
        )

    def review_executor(_config: Any, review_id: str) -> int:
        state["order"].append("review")
        state["review_executor_calls"].append(review_id)
        review = store.get(review_id)
        assert review is not None
        assert review.task_type == "review"
        assert review.status == "pending"
        assert review.review_scope is not None and "resolution" in review.review_scope
        assert review.review_verify_head_sha is None
        review.status = "completed"
        review.completed_at = datetime(2026, 8, 26, 12, 0, tzinfo=UTC)
        review.review_verify_head_sha = "source-after"
        review.output_content = _review_report_with_findings(
            "CHANGES_REQUESTED",
            blockers=(("B1", "Out-of-scope landing polish", "docs/internal/landing.md:12"),),
        )
        store.update(review)
        _add_review_blocker_resolution(
            store,
            impl=impl,
            review=review,
            title="Out-of-scope landing polish",
            path="docs/internal/landing.md:12",
            head="source-after",
            target_head="target-a",
            reason="out_of_scope",
        )
        return 0

    def judge() -> LandingJudgment:
        state["order"].append("judge")
        identity = state["latest_identity"]
        facts = state["latest_facts"]
        assert identity is not None
        assert facts is not None
        assert facts.review is not None
        assert facts.review.review_id is not None
        review = store.get(facts.review.review_id)
        assert review is not None
        from gza.landing import _landing_judge_evidence
        from gza.landing_judge import LandingJudgeBlockerIdentity, LandingJudgeIdentity, build_landing_judge_prompt

        evidence = _landing_judge_evidence(store, config, git, identity, facts, review)
        judge_identity = LandingJudgeIdentity(
            implementation_id=identity.owner_task_id,
            merge_unit_id=identity.merge_unit_id,
            review_id=facts.review.review_id,
            reviewed_head=facts.review.reviewed_head,
            source_head=identity.source_sha,
            target_head=identity.target_sha,
            verify_identity=evidence["verify_identity"],
            authoritative_scope_identity=evidence["authoritative_scope_identity"],
            adjudication_artifact_identities=facts.adjudication_fingerprints,
            adjudication_content_identity=evidence["adjudication_content_identity"],
            blocker_identities=tuple(
                LandingJudgeBlockerIdentity(blocker.finding_id, blocker.fingerprint or "")
                for blocker in facts.open_blockers
            ),
            decision_context_digest=evidence["context"].digest,
        )
        prompt = build_landing_judge_prompt(
            identity=judge_identity,
            task_prompt=evidence["context"].task_prompt,
            authoritative_review_scope=evidence["context"].authoritative_review_scope,
            plan_context=evidence["context"].plan_context,
            implementation_summary=evidence["context"].implementation_summary,
            review_output=evidence["context"].review_output,
            verify_evidence=evidence["context"].verify_evidence,
            diff_context=evidence["context"].diff_context,
            adjudication_context=evidence["context"].adjudication_context,
            blockers=evidence["context"].blockers,
        )
        artifact = _persist_test_landing_judgment(store, impl, review, judge_identity, prompt)
        return LandingJudgment("LAND", artifact_id=str(artifact.id), key=judge_identity.key)

    def merge(identity: Any, decision: LandingPolicyDecision, provenance: str) -> ManualMergeExecutionResult:
        from gza.cli.git_ops import _create_or_reuse_deferred_blocker_tasks

        state["order"].append("merge")
        assert provenance == "manual_land_escalated"
        assert decision.allowed_overrides == (
            "defer-review-blockers",
            "parked:review-max-cycles-reached",
        )
        facts = state["latest_facts"]
        assert facts is not None
        assert facts.review is not None
        review_id = facts.review.review_id
        assert review_id is not None
        review = store.get(review_id)
        assert review is not None
        findings = tuple(parse_review_report(review.output_content).findings)
        created, reused = _create_or_reuse_deferred_blocker_tasks(
            store,
            config=config,
            review_task=review,
            impl_task=identity.owner_task,
            findings=findings,
            trigger_source="manual_land",
        )
        _simulate_no_ff_landing_git_merge(git, merge_sha="target-after")
        return ManualMergeExecutionResult(
            rc=0,
            status="merged",
            created_deferred_blockers=created,
            reused_deferred_blockers=reused,
        )

    def post_merge_verify(identity: Any) -> LandPostMergeVerifySuccess:
        from gza.main_integration_verify import run_main_integration_verify

        state["order"].append("post_merge_verify")
        checkpoint = run_main_integration_verify(
            config,
            store,
            git,
            reason="manual_land",
            resolved_head_sha=git.heads["main"],
        )
        return LandPostMergeVerifySuccess(
            checkpoint_id=checkpoint.task.id if checkpoint.task is not None else None,
            target_head=checkpoint.head_sha,
            tree_fingerprint=checkpoint.tree_fingerprint,
            gate_identity=identity.target_branch,
        )

    def finalize(identity: Any, decision: Any, provenance: str) -> ManualMergeExecutionResult:
        state["order"].append("finalize")
        return _finalize_landing_merge_state(store, identity, decision, provenance)

    state.update(
        {
            "inspect": inspect,
            "execute_rebase_service": execute_rebase_service,
            "review_executor": review_executor,
            "judge": judge,
            "merge": merge,
            "post_merge_verify": post_merge_verify,
            "finalize": finalize,
        }
    )
    return state


def test_landing_coordinator_full_cycle_guarded_conflict_resolution_lands_with_post_merge_verify(
    monkeypatch,
    tmp_path,
) -> None:
    fixture = _motivating_full_cycle_landing_fixture(tmp_path)
    store: SqliteTaskStore = fixture["store"]
    impl: Task = fixture["impl"]
    unit = fixture["unit"]
    git: _LandingSourceGit = fixture["git"]
    git.repo_dir = tmp_path
    git.trees["target-after"] = TREE_A

    def verify_command(*_args: Any, **_kwargs: Any) -> Any:
        return ReviewVerifyResult(
            command="./bin/tests",
            status="passed",
            exit_status="0",
            captured_at=datetime(2026, 8, 26, 12, 0, tzinfo=UTC),
            reviewed_branch="main",
            reviewed_head_sha="target-after",
            reviewed_base_sha="target-a",
            working_directory=str(tmp_path),
            failure=None,
            output=(
                "verify output\n"
                f"gza-verify phase=passed name=unit duration_seconds=1.0 tree_fingerprint={TREE_A}\n"
            ),
        )

    monkeypatch.setattr("gza.main_integration_verify._run_review_verify_command", verify_command)

    coordinator = LandingCoordinator(
        store=store,
        git=git,
        config=fixture["config"],
        inspect_policy_facts=fixture["inspect"],
        create_rebase_task=_unused_rebase_factory,
        rebase_executor=_unused_rebase_executor,
        execute_rebase_service=fixture["execute_rebase_service"],
        review_executor=fixture["review_executor"],
        landing_judge=fixture["judge"],
        execute_merge=fixture["merge"],
        finalize_merge=fixture["finalize"],
        post_merge_verifier=fixture["post_merge_verify"],
    )
    fixture["coordinator"] = coordinator

    result = coordinator.run(LandRequest(task_id=impl.id))

    assert result.blocked is None
    assert result.post_merge_verify_failure is None
    assert result.merged is True
    assert result.merge_provenance == "manual_land_escalated"
    assert result.judgment_artifact_id is not None
    assert result.judgment_key is not None
    assert fixture["order"] == ["rebase", "review", "judge", "merge", "post_merge_verify", "finalize"]
    persisted_unit = store.get_merge_unit(unit.id)
    assert persisted_unit is not None
    assert persisted_unit.state == "merged"
    assert persisted_unit.merge_source == "manual_land_escalated"
    review_tasks = [task for task in store.get_all() if task.task_type == "review"]
    resolution_reviews = [task for task in review_tasks if task.review_scope and "resolution" in task.review_scope]
    assert len(resolution_reviews) == 1
    assert len(review_tasks) == 2
    resolution_review = store.get(resolution_reviews[0].id or "")
    assert resolution_review is not None
    assert fixture["review_executor_calls"] == [resolution_review.id]
    assert resolution_review.status == "completed"
    assert resolution_review.review_verify_head_sha == "source-after"
    assert "Out-of-scope landing polish" in (resolution_review.output_content or "")
    deferred = store.get(result.deferred_task_ids[0])
    assert deferred is not None
    assert deferred.urgent is True
    assert deferred.create_pr is True
    assert deferred.depends_on == impl.id
    assert deferred.based_on == resolution_review.id
    judgment_artifact = store.get_artifact(int(result.judgment_artifact_id), task_id=impl.id)
    assert judgment_artifact is not None
    assert judgment_artifact.kind == "landing_judgment"
    assert judgment_artifact.status == "LAND"
    assert judgment_artifact.head_sha == "source-after"
    assert judgment_artifact.metadata is not None
    assert judgment_artifact.metadata["key"] == result.judgment_key
    pending_artifacts = store.list_artifacts(impl.id, kind="landing_pending_finalization")
    assert pending_artifacts
    assert pending_artifacts[0].metadata is not None
    assert pending_artifacts[0].metadata["post_merge_target_sha"] == "target-after"
    from gza.main_integration_verify import load_main_integration_verify_state

    checkpoint = load_main_integration_verify_state(store)
    assert checkpoint is not None
    assert checkpoint.task is not None
    assert checkpoint.head_sha == "target-after"
    assert checkpoint.tree_fingerprint == TREE_A
    assert any(
        step.phase == "post_merge_verify"
        and checkpoint.task.id in step.evidence_refs
        and "target-after" in step.evidence_refs
        for step in result.steps
    )
    assert [step.phase for step in result.steps].count("post_rebase_review") == 1
    assert any(
        step.phase == "post_rebase_review"
        and step.status == "completed"
        and resolution_review.id in step.summary
        for step in result.steps
    )
    _assert_no_improve_rows(store)
    before_rerun = _landing_durable_snapshot(store)

    rerun = LandingCoordinator(
        store=store,
        git=git,
        config=fixture["config"],
        inspect_policy_facts=lambda *_args: (_ for _ in ()).throw(AssertionError("rerun must be terminal")),
        execute_rebase_service=lambda **_kwargs: (_ for _ in ()).throw(AssertionError("rebase must not rerun")),
        review_executor=lambda *_args: (_ for _ in ()).throw(AssertionError("review must not rerun")),
        landing_judge=lambda: (_ for _ in ()).throw(AssertionError("judge must not rerun")),
        execute_merge=lambda *_args: (_ for _ in ()).throw(AssertionError("merge must not rerun")),
        post_merge_verifier=lambda *_args: (_ for _ in ()).throw(AssertionError("verify must not rerun")),
    ).run(LandRequest(task_id=impl.id))

    assert rerun.blocked is None
    assert rerun.already_merged is True
    assert rerun.terminal_outcome == "merged"
    assert _landing_durable_snapshot(store) == before_rerun


def test_landing_coordinator_full_cycle_dry_run_has_zero_mutation(tmp_path) -> None:
    fixture = _motivating_full_cycle_landing_fixture(tmp_path)
    store: SqliteTaskStore = fixture["store"]
    impl: Task = fixture["impl"]
    git: _LandingSourceGit = fixture["git"]
    before = _landing_durable_snapshot(store)
    before_heads = dict(git.heads)

    coordinator = LandingCoordinator(
        store=store,
        git=git,
        config=fixture["config"],
        inspect_policy_facts=fixture["inspect"],
        create_rebase_task=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("no dry-run rebase task")),
        rebase_executor=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("no dry-run rebase executor")),
        execute_rebase_service=lambda **_kwargs: (_ for _ in ()).throw(AssertionError("no dry-run rebase service")),
        landing_judge=lambda: (_ for _ in ()).throw(AssertionError("no dry-run judge")),
        execute_merge=lambda *_args: (_ for _ in ()).throw(AssertionError("no dry-run merge")),
        post_merge_verifier=lambda *_args: (_ for _ in ()).throw(AssertionError("no dry-run post-merge verify")),
    )
    fixture["coordinator"] = coordinator
    result = coordinator.run(LandRequest(task_id=impl.id, dry_run=True))

    assert result.blocked is None
    assert result.merged is False
    assert result.steps[-1].phase == "rebase"
    assert result.steps[-1].status == "conditional"
    assert "execution required" in result.steps[-1].summary
    assert _landing_durable_snapshot(store) == before
    assert git.heads == before_heads
    assert git.mutation_calls == []
    assert fixture["order"] == []


def test_landing_coordinator_target_advancement_after_judgment_stops_without_second_budget(tmp_path) -> None:
    fixture = _motivating_full_cycle_landing_fixture(tmp_path)
    store: SqliteTaskStore = fixture["store"]
    impl: Task = fixture["impl"]
    git: _LandingSourceGit = fixture["git"]
    original_judge = fixture["judge"]

    def advancing_judge() -> LandingJudgment:
        judgment = original_judge()
        git.heads["main"] = "target-b"
        return judgment

    coordinator = LandingCoordinator(
        store=store,
        git=git,
        config=fixture["config"],
        inspect_policy_facts=fixture["inspect"],
        create_rebase_task=_unused_rebase_factory,
        rebase_executor=_unused_rebase_executor,
        execute_rebase_service=fixture["execute_rebase_service"],
        review_executor=fixture["review_executor"],
        landing_judge=advancing_judge,
        execute_merge=lambda *_args: (_ for _ in ()).throw(AssertionError("merge must not run after target moves")),
        finalize_merge=fixture["finalize"],
        post_merge_verifier=fixture["post_merge_verify"],
    )
    fixture["coordinator"] = coordinator
    result = coordinator.run(LandRequest(task_id=impl.id))

    assert result.blocked is not None
    assert result.blocked.reason_code == "identity-proof-unavailable"
    assert result.blocked.fact == "target head changed after landing authorization"
    assert fixture["order"] == ["rebase", "review", "judge"]
    assert [step.phase for step in result.steps].count("rebase") == 1
    assert [step.phase for step in result.steps].count("post_rebase_review") == 1
    assert all(step.phase not in {"defer_blockers", "merge", "post_merge_verify"} for step in result.steps)
    assert store.list_artifacts(impl.id, kind="landing_pending_finalization") == []
    assert store.get_merge_unit(fixture["unit"].id).state == "unmerged"  # type: ignore[union-attr]
    assert len([task for task in store.get_all() if task.task_type == "review"]) == 2
    _assert_no_improve_rows(store)


def test_landing_coordinator_requires_post_merge_verifier_before_merge(tmp_path) -> None:
    from gza.merge_services import ManualMergeExecutionResult

    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "missing post merge verifier", "feature/landing")
    assert impl.id is not None
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-a")},
    )
    merge_calls: list[str] = []

    def inspect(identity: Any) -> LandingPolicyFacts:
        return _green_facts(
            task_id=identity.owner_task_id,
            source_head=identity.source_sha,
            target_head=identity.target_sha,
            review=_review(verdict="APPROVED", reviewed_head=identity.source_sha),
            open_blockers=(),
        )

    def merge(*_args: Any, **_kwargs: Any) -> ManualMergeExecutionResult:
        merge_calls.append("merge")
        return ManualMergeExecutionResult(rc=0, status="merged")

    result = LandingCoordinator(
        store=store,
        git=git,
        config=config,
        inspect_policy_facts=inspect,
        execute_merge=merge,
    ).run(LandRequest(task_id=impl.id))

    assert result.blocked is not None
    assert result.blocked.reason_code == "verify-unavailable-or-red"
    assert result.blocked.fact == "canonical post-merge target verifier is unavailable"
    assert result.merged is False
    assert result.merge_provenance is None
    assert merge_calls == []
    assert result.steps[-1].phase == "post_merge_verify"
    assert result.steps[-1].status == "blocked"


def test_landing_coordinator_runs_post_merge_verify_after_normal_merge(tmp_path) -> None:
    from gza.merge_services import ManualMergeExecutionResult

    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "normal landing", "feature/landing")
    assert impl.id is not None
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-a")},
    )
    calls: list[str] = []

    def inspect(identity: Any) -> LandingPolicyFacts:
        return _green_facts(
            task_id=identity.owner_task_id,
            source_head=identity.source_sha,
            target_head=identity.target_sha,
            review=_review(verdict="APPROVED", reviewed_head=identity.source_sha),
            open_blockers=(),
        )

    def merge(*_args: Any, **_kwargs: Any) -> ManualMergeExecutionResult:
        calls.append("merge")
        _simulate_no_ff_landing_git_merge(git)
        return ManualMergeExecutionResult(rc=0, status="merged")

    def post_merge_verify(identity: Any) -> LandPostMergeVerifySuccess:
        calls.append(f"verify:{identity.target_branch}")
        return _post_merge_success(identity)

    result = LandingCoordinator(
        store=store,
        git=git,
        config=config,
        inspect_policy_facts=inspect,
        execute_merge=merge,
        finalize_merge=lambda identity, decision, provenance: _finalize_landing_merge_state(
            store, identity, decision, provenance
        ),
        post_merge_verifier=post_merge_verify,
    ).run(LandRequest(task_id=impl.id))

    assert result.blocked is None
    assert result.post_merge_verify_failure is None
    assert result.merged is True
    assert calls == ["merge", "verify:main"]
    assert result.steps[-1].phase == "post_merge_verify"
    assert result.steps[-1].status == "completed"


def test_landing_coordinator_verifies_before_persisting_merged_state(tmp_path) -> None:
    from gza.merge_services import ManualMergeExecutionResult

    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "ordered landing", "feature/landing")
    assert impl.id is not None
    unit = store.resolve_merge_unit_for_task(impl.id)
    assert unit is not None
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
            review=_review(verdict="APPROVED", reviewed_head=identity.source_sha),
            open_blockers=(),
        )

    def merge(_identity: Any, _decision: Any, _provenance: str) -> ManualMergeExecutionResult:
        order.append("git_merge")
        assert store.get_merge_unit(unit.id).state == "unmerged"  # type: ignore[union-attr]
        _simulate_no_ff_landing_git_merge(git)
        return ManualMergeExecutionResult(rc=0, status="merged")

    def post_merge_verify(identity: Any) -> LandPostMergeVerifySuccess:
        order.append("post_merge_verify")
        assert store.get_merge_unit(unit.id).state == "unmerged"  # type: ignore[union-attr]
        assert store.get_merge_unit(unit.id).merge_source is None  # type: ignore[union-attr]
        return _post_merge_success(identity)

    def finalize(identity: Any, decision: Any, provenance: str) -> ManualMergeExecutionResult:
        order.append(f"finalize:{provenance}")
        assert order == ["git_merge", "post_merge_verify", "finalize:manual_land"]
        return _finalize_landing_merge_state(store, identity, decision, provenance)

    result = LandingCoordinator(
        store=store,
        git=git,
        config=config,
        inspect_policy_facts=inspect,
        execute_merge=merge,
        finalize_merge=finalize,
        post_merge_verifier=post_merge_verify,
    ).run(LandRequest(task_id=impl.id))

    refreshed = store.get_merge_unit(unit.id)
    assert result.blocked is None
    assert result.post_merge_verify_failure is None
    assert result.merged is True
    assert order == ["git_merge", "post_merge_verify", "finalize:manual_land"]
    assert refreshed is not None
    assert refreshed.state == "merged"
    assert refreshed.merge_source == "manual_land"


@pytest.mark.parametrize("status", ("failed", "stale", "unavailable", "malformed", "missing"))
def test_landing_coordinator_post_merge_verify_failure_leaves_unit_unfinalized(
    tmp_path,
    status: str,
) -> None:
    from gza.merge_services import ManualMergeExecutionResult

    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "post merge verify red", "feature/landing")
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
            review=_review(verdict="APPROVED", reviewed_head=identity.source_sha),
            open_blockers=(),
        )

    def merge(*_args: Any, **_kwargs: Any) -> ManualMergeExecutionResult:
        return ManualMergeExecutionResult(rc=0, status="merged")

    def post_merge_verify(_identity: Any) -> LandPostMergeVerifyFailure:
        return LandPostMergeVerifyFailure(
            status=status,  # type: ignore[arg-type]
            fact=f"post-merge checkpoint is {status}",
            checkpoint_id="checkpoint-1",
            target_head="target-after",
            gate_identity="main-verify",
        )

    result = LandingCoordinator(
        store=store,
        git=git,
        config=config,
        inspect_policy_facts=inspect,
        execute_merge=merge,
        post_merge_verifier=post_merge_verify,
    ).run(LandRequest(task_id=impl.id))

    assert result.blocked is None
    assert result.merged is False
    assert result.merge_provenance is None
    assert result.post_merge_verify_failure is not None
    assert result.post_merge_verify_failure.status == status
    unit = store.resolve_merge_unit_for_task(impl.id)
    assert unit is not None
    assert unit.state == "unmerged"
    assert unit.merge_source is None
    assert result.steps[-1].phase == "post_merge_verify"
    assert result.steps[-1].status == "blocked"


def test_landing_coordinator_reports_raised_post_merge_verify_after_authoritative_merge(tmp_path) -> None:
    from gza.merge_services import ManualMergeExecutionResult

    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "raised post merge verify", "feature/landing")
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
            review=_review(verdict="APPROVED", reviewed_head=identity.source_sha),
            open_blockers=(),
        )

    def merge(*_args: Any, **_kwargs: Any) -> ManualMergeExecutionResult:
        return ManualMergeExecutionResult(rc=0, status="merged")

    def post_merge_verify(_identity: Any) -> LandPostMergeVerifySuccess:
        raise RuntimeError("checkpoint backend offline")

    result = LandingCoordinator(
        store=store,
        git=git,
        config=config,
        inspect_policy_facts=inspect,
        execute_merge=merge,
        post_merge_verifier=post_merge_verify,
    ).run(LandRequest(task_id=impl.id))

    assert result.blocked is None
    assert result.merged is False
    assert result.merge_provenance is None
    assert result.post_merge_verify_failure is not None
    assert result.post_merge_verify_failure.status == "unavailable"
    unit = store.resolve_merge_unit_for_task(impl.id)
    assert unit is not None
    assert unit.state == "unmerged"
    assert unit.merge_source is None
    assert "checkpoint backend offline" in result.post_merge_verify_failure.fact
    assert result.steps[-1].phase == "post_merge_verify"
    assert result.steps[-1].status == "blocked"


@pytest.mark.parametrize("verifier_result", (None, object()))
def test_landing_coordinator_malformed_post_merge_verify_result_leaves_unit_unfinalized(
    tmp_path,
    verifier_result: Any,
) -> None:
    from gza.merge_services import ManualMergeExecutionResult

    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "malformed post merge verify", "feature/landing")
    assert impl.id is not None
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-a")},
    )
    finalizations: list[str] = []

    def inspect(identity: Any) -> LandingPolicyFacts:
        return _green_facts(
            task_id=identity.owner_task_id,
            source_head=identity.source_sha,
            target_head=identity.target_sha,
            review=_review(verdict="APPROVED", reviewed_head=identity.source_sha),
            open_blockers=(),
        )

    def merge(*_args: Any, **_kwargs: Any) -> ManualMergeExecutionResult:
        _simulate_no_ff_landing_git_merge(git)
        return ManualMergeExecutionResult(rc=0, status="merged")

    def finalize(*_args: Any, **_kwargs: Any) -> ManualMergeExecutionResult:
        finalizations.append("finalize")
        raise AssertionError("malformed post-merge verify output must not finalize merged state")

    result = LandingCoordinator(
        store=store,
        git=git,
        config=config,
        inspect_policy_facts=inspect,
        execute_merge=merge,
        finalize_merge=finalize,
        post_merge_verifier=lambda _identity: verifier_result,
    ).run(LandRequest(task_id=impl.id))

    unit = store.resolve_merge_unit_for_task(impl.id)
    assert result.blocked is None
    assert result.merged is False
    assert result.merge_provenance is None
    assert result.post_merge_verify_failure is not None
    assert result.post_merge_verify_failure.status == "malformed"
    assert "malformed result" in result.post_merge_verify_failure.fact
    assert result.steps[-1].phase == "post_merge_verify"
    assert result.steps[-1].status == "blocked"
    assert finalizations == []
    assert unit is not None
    assert unit.state == "unmerged"
    assert unit.merge_source is None


def test_landing_coordinator_reruns_partial_git_merge_checkpoint_and_finalizes_once(tmp_path) -> None:
    from gza.merge_services import ManualMergeExecutionResult

    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "partial landing rerun", "feature/landing")
    assert impl.id is not None
    unit = store.resolve_merge_unit_for_task(impl.id)
    assert unit is not None
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-a")},
    )
    merge_calls: list[str] = []
    verify_results = [
        LandPostMergeVerifyFailure(
            status="failed",
            fact="post-merge checkpoint is red",
            checkpoint_id="checkpoint-1",
            target_head="merge-a",
            gate_identity="main",
        ),
        LandPostMergeVerifyFailure(
            status="failed",
            fact="post-merge checkpoint is still red",
            checkpoint_id="checkpoint-2",
            target_head="merge-a",
            gate_identity="main",
        ),
        _post_merge_success(SimpleNamespace(target_branch="main")),
    ]
    finalizations: list[str] = []

    def inspect(identity: Any) -> LandingPolicyFacts:
        return _green_facts(
            task_id=identity.owner_task_id,
            source_head=identity.source_sha,
            target_head=identity.target_sha,
            review=_review(verdict="APPROVED", reviewed_head=identity.source_sha),
            open_blockers=(),
        )

    def merge(_identity: Any, _decision: Any, _provenance: str) -> ManualMergeExecutionResult:
        merge_calls.append("git_merge")
        _simulate_no_ff_landing_git_merge(git, merge_sha="merge-a")
        return ManualMergeExecutionResult(rc=0, status="merged")

    def post_merge_verify(_identity: Any) -> LandPostMergeVerifyFailure | LandPostMergeVerifySuccess:
        return verify_results.pop(0)

    def finalize(identity: Any, decision: Any, provenance: str) -> ManualMergeExecutionResult:
        finalizations.append(provenance)
        return _finalize_landing_merge_state(store, identity, decision, provenance)

    coordinator = LandingCoordinator(
        store=store,
        git=git,
        config=config,
        inspect_policy_facts=inspect,
        execute_merge=merge,
        finalize_merge=finalize,
        post_merge_verifier=post_merge_verify,
    )

    first = coordinator.run(LandRequest(task_id=impl.id))
    second = coordinator.run(LandRequest(task_id=impl.id))
    third = coordinator.run(LandRequest(task_id=impl.id))

    refreshed = store.get_merge_unit(unit.id)
    assert first.post_merge_verify_failure is not None
    assert second.post_merge_verify_failure is not None
    assert first.merged is False
    assert second.merged is False
    assert third.post_merge_verify_failure is None
    assert third.merged is True
    assert merge_calls == ["git_merge"]
    assert finalizations == ["manual_land"]
    assert refreshed is not None
    assert refreshed.state == "merged"
    assert refreshed.merge_source == "manual_land"


@pytest.mark.parametrize(
    ("case_name", "metadata_overrides", "expected_loaded"),
    [
        (
            "valid_prepared",
            {"stage": "prepared", "prepared_target_sha": "target-a", "post_merge_target_sha": None},
            True,
        ),
        (
            "valid_pending",
            {"stage": "pending", "prepared_target_sha": "target-a", "post_merge_target_sha": "merge-a"},
            True,
        ),
        (
            "valid_legacy_pending",
            {"stage": "__missing__", "prepared_target_sha": None, "post_merge_target_sha": "merge-a"},
            True,
        ),
        (
            "missing_stage_without_post_merge_sha",
            {"stage": "__missing__", "prepared_target_sha": "target-a", "post_merge_target_sha": None},
            False,
        ),
        (
            "unknown_stage",
            {"stage": "complete", "prepared_target_sha": "target-a", "post_merge_target_sha": "merge-a"},
            False,
        ),
        (
            "pending_null_post_merge_sha",
            {"stage": "pending", "prepared_target_sha": "target-a", "post_merge_target_sha": None},
            False,
        ),
        (
            "prepared_with_post_merge_sha",
            {"stage": "prepared", "prepared_target_sha": "target-a", "post_merge_target_sha": "merge-a"},
            False,
        ),
        (
            "non_string_post_merge_sha",
            {"stage": "pending", "prepared_target_sha": "target-a", "post_merge_target_sha": 123},
            False,
        ),
        (
            "non_string_prepared_target_sha",
            {"stage": "prepared", "prepared_target_sha": 123, "post_merge_target_sha": None},
            False,
        ),
        (
            "mismatched_prepared_target_sha",
            {"stage": "prepared", "prepared_target_sha": "target-b", "post_merge_target_sha": None},
            False,
        ),
    ],
)
def test_pending_finalization_parser_is_stage_discriminated_and_fail_closed(
    tmp_path,
    case_name: str,
    metadata_overrides: dict[str, Any],
    expected_loaded: bool,
) -> None:
    del case_name
    store = _coordinator_store(tmp_path)
    impl = _completed_impl(store, "parser replay", "feature/landing")
    identity, authorization = _pending_finalization_identity_and_authorization(store, impl)
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "merge-a"},
        local_branches={"feature/landing"},
        merged_refs={("feature/landing", "main")},
    )
    coordinator = LandingCoordinator(store=store, git=git)

    parsed = coordinator._pending_finalization_from_metadata(
        identity,
        "artifact-1",
        _pending_finalization_metadata(authorization, **metadata_overrides),
    )

    if expected_loaded:
        assert parsed is not None
        assert parsed.post_merge_target_sha == "merge-a"
        assert parsed.prepared_target_sha == metadata_overrides["prepared_target_sha"]
    else:
        assert parsed is None


@pytest.mark.parametrize(
    ("case_name", "metadata_overrides"),
    [
        (
            "missing_stage_without_post_merge_sha",
            {"stage": "__missing__", "prepared_target_sha": "target-a", "post_merge_target_sha": None},
        ),
        (
            "unknown_stage",
            {"stage": "complete", "prepared_target_sha": "target-a", "post_merge_target_sha": "merge-a"},
        ),
        (
            "pending_null_post_merge_sha",
            {"stage": "pending", "prepared_target_sha": "target-a", "post_merge_target_sha": None},
        ),
        (
            "prepared_with_post_merge_sha",
            {"stage": "prepared", "prepared_target_sha": "target-a", "post_merge_target_sha": "merge-a"},
        ),
        (
            "non_string_post_merge_sha",
            {"stage": "pending", "prepared_target_sha": "target-a", "post_merge_target_sha": ["merge-a"]},
        ),
        (
            "non_string_prepared_target_sha",
            {"stage": "prepared", "prepared_target_sha": ["target-a"], "post_merge_target_sha": None},
        ),
        (
            "mismatched_prepared_target_sha",
            {"stage": "prepared", "prepared_target_sha": "target-b", "post_merge_target_sha": None},
        ),
    ],
)
def test_landing_coordinator_rejects_malformed_pending_finalization_without_verify_or_finalize(
    tmp_path,
    case_name: str,
    metadata_overrides: dict[str, Any],
) -> None:
    del case_name
    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "malformed replay", "feature/landing")
    assert impl.id is not None
    unit = store.resolve_merge_unit_for_task(impl.id)
    assert unit is not None
    identity, authorization = _pending_finalization_identity_and_authorization(store, impl)
    _persist_pending_finalization_artifact(
        store,
        impl,
        _pending_finalization_metadata(authorization, **metadata_overrides),
    )
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "merge-a"},
        local_branches={"feature/landing"},
        merged_refs={("feature/landing", "main")},
        ancestors={("head-a", "merge-a")},
    )

    result = LandingCoordinator(
        store=store,
        git=git,
        config=config,
        post_merge_verifier=lambda _identity: (_ for _ in ()).throw(
            AssertionError("post-merge verification must not run for malformed replay proof")
        ),
        finalize_merge=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("merged state must not be finalized for malformed replay proof")
        ),
    ).run(LandRequest(task_id=identity.owner_task_id))

    refreshed = store.get_merge_unit(unit.id)
    assert result.blocked is not None
    assert result.blocked.reason_code == "merge-proof-unavailable"
    assert "no exact pending-finalization proof exists" in result.blocked.fact
    assert result.merged is False
    assert result.post_merge_verify_failure is None
    assert refreshed is not None
    assert refreshed.state == "unmerged"
    assert refreshed.merge_source is None


def test_landing_coordinator_guarded_rerun_replays_pending_no_ff_merge_without_duplicate_side_effects(
    tmp_path,
) -> None:
    from gza.merge_services import ManualMergeExecutionResult
    from gza.review_tasks import build_deferred_blocker_prompt, format_blocker_finding_context
    from gza.review_verdict import parse_review_report

    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "guarded partial landing rerun", "feature/landing")
    assert impl.id is not None
    unit = store.resolve_merge_unit_for_task(impl.id)
    assert unit is not None
    review = store.add("review", task_type="review", depends_on=impl.id, based_on=impl.id)
    review.status = "completed"
    review.output_content = _review_report_with_findings(
        "CHANGES_REQUESTED",
        blockers=(("B1", "Deferred blocker", "src/gza/landing.py:1"),),
    )
    store.update(review)
    assert review.id is not None
    finding = parse_review_report(review.output_content).findings[0]
    deferred_prompt = build_deferred_blocker_prompt(review.id, impl.id, finding)
    deferred_scope = format_blocker_finding_context(finding)
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-a")},
    )
    merge_calls: list[str] = []
    rebase_calls: list[str] = []
    judgment_calls: list[str] = []
    materialized: list[str] = []
    finalizations: list[str] = []

    def inspect(identity: Any) -> LandingPolicyFacts:
        return _green_facts(
            task_id=identity.owner_task_id,
            source_head=identity.source_sha,
            target_head=identity.target_sha,
            parked_reason="review-max-cycles-reached",
            review=_review(
                verdict="CHANGES_REQUESTED",
                reviewed_head=identity.source_sha,
                review_id=review.id,
            ),
            open_blockers=(
                LandingOpenBlocker(
                    "B1",
                    deferrable=True,
                    blocker_class="out_of_scope",
                    source=f"review:{review.id}",
                    fingerprint="blocker:B1:normalized",
                    deferred_task_prompt_sha256="sha256:" + sha256(deferred_prompt.encode()).hexdigest(),
                    deferred_task_review_scope_sha256="sha256:" + sha256(deferred_scope.encode()).hexdigest(),
                ),
            ),
        )

    def judge() -> LandingJudgment:
        judgment_calls.append("judge")
        return LandingJudgment("LAND", artifact_id="judge-artifact", key="judge-key")

    def merge(_identity: Any, _decision: Any, _provenance: str) -> ManualMergeExecutionResult:
        merge_calls.append("git_merge")
        _simulate_no_ff_landing_git_merge(git, merge_sha="merge-a")
        blocker = store.add(
            deferred_prompt,
            task_type="implement",
            based_on=review.id,
            depends_on=impl.id,
            review_scope=deferred_scope,
            create_pr=True,
            urgent=True,
        )
        assert blocker.id is not None
        materialized.append(blocker.id)
        return ManualMergeExecutionResult(rc=0, status="merged", created_deferred_blockers=[blocker])

    verify_results = [
        LandPostMergeVerifyFailure(
            status="failed",
            fact="post-merge checkpoint is red",
            checkpoint_id="checkpoint-red",
            target_head="merge-a",
            gate_identity="main",
        ),
        _post_merge_success(SimpleNamespace(target_branch="main")),
    ]

    def post_merge_verify(_identity: Any) -> LandPostMergeVerifyFailure | LandPostMergeVerifySuccess:
        return verify_results.pop(0)

    def finalize(identity: Any, decision: Any, provenance: str) -> ManualMergeExecutionResult:
        finalizations.append(provenance)
        return _finalize_landing_merge_state(store, identity, decision, provenance)

    first = LandingCoordinator(
        store=store,
        git=git,
        config=config,
        inspect_policy_facts=inspect,
        landing_judge=judge,
        execute_merge=merge,
        finalize_merge=finalize,
        post_merge_verifier=post_merge_verify,
        create_rebase_task=lambda *_args, **_kwargs: rebase_calls.append("rebase") or impl,
    ).run(LandRequest(task_id=impl.id))

    assert first.post_merge_verify_failure is not None
    assert git.heads["feature/landing"] == "head-a"
    assert git.heads["main"] == "merge-a"

    second = LandingCoordinator(
        store=store,
        git=git,
        config=config,
        inspect_policy_facts=lambda _identity: (_ for _ in ()).throw(AssertionError("policy facts must not reload")),
        landing_judge=lambda: (_ for _ in ()).throw(AssertionError("judgment must not rerun")),
        execute_merge=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("git merge must not rerun")),
        finalize_merge=finalize,
        post_merge_verifier=post_merge_verify,
        create_rebase_task=lambda *_args, **_kwargs: rebase_calls.append("rebase") or impl,
    ).run(LandRequest(task_id=impl.id))

    refreshed = store.get_merge_unit(unit.id)
    assert second.merged is True
    assert second.merge_provenance == "manual_land_escalated"
    assert second.judgment_artifact_id == "judge-artifact"
    assert second.judgment_key == "judge-key"
    assert second.deferred_task_ids == tuple(materialized)
    assert merge_calls == ["git_merge"]
    assert rebase_calls == []
    assert judgment_calls == ["judge"]
    assert finalizations == ["manual_land_escalated"]
    assert refreshed is not None
    assert refreshed.state == "merged"
    assert refreshed.merge_source == "manual_land_escalated"


def test_landing_coordinator_dry_run_reports_pending_finalization_without_replay_side_effects(
    tmp_path,
) -> None:
    from gza.merge_services import ManualMergeExecutionResult

    store = _coordinator_store(tmp_path)
    impl = _completed_impl(store, "pending dry-run", "feature/landing")
    assert impl.id is not None
    unit = store.resolve_merge_unit_for_task(impl.id)
    assert unit is not None
    deferred = store.add("Deferred B1", task_type="implement", depends_on=impl.id, urgent=True, create_pr=True)
    assert deferred.id is not None
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "merge-a"},
        local_branches={"feature/landing"},
        ancestors={("head-a", "merge-a")},
    )
    _persist_exact_landing_pending_finalization(
        store,
        impl=impl,
        unit_id=unit.id,
        source_ref="feature/landing",
        source_sha="head-a",
        target_branch="main",
        target_sha="merge-a",
        provenance="manual_land_escalated",
        deferred_task_ids=(deferred.id,),
    )
    before_tasks = _sqlite_task_snapshot(store)
    before_artifacts = _sqlite_artifact_snapshot(store)
    before_units = _sqlite_merge_unit_snapshot(store)
    before_refs = dict(git.heads)
    verifier_calls: list[str] = []
    finalizer_calls: list[str] = []

    def post_merge_verify(_identity: Any) -> LandPostMergeVerifySuccess:
        verifier_calls.append("verify")
        return LandPostMergeVerifySuccess(
            checkpoint_id="checkpoint-green",
            target_head="merge-a",
            tree_fingerprint=TREE_A,
            gate_identity="main",
        )

    def finalize(*_args: Any, **_kwargs: Any) -> ManualMergeExecutionResult:
        finalizer_calls.append("finalize")
        return ManualMergeExecutionResult(rc=0, status="merged")

    result = LandingCoordinator(
        store=store,
        git=git,
        post_merge_verifier=post_merge_verify,
        finalize_merge=finalize,
        inspect_policy_facts=lambda _identity: (_ for _ in ()).throw(
            AssertionError("pending dry-run must not reload policy facts")
        ),
    ).run(LandRequest(task_id=impl.id, dry_run=True))

    assert result.blocked is None
    assert result.post_merge_verify_failure is None
    assert result.merged is False
    assert result.deferred_task_ids == (deferred.id,)
    assert result.judgment_artifact_id == "judge-artifact"
    assert result.judgment_key == "judge-key"
    assert [step.phase for step in result.steps] == [
        "resolve",
        "merge",
        "post_merge_verify",
        "merge",
    ]
    assert result.steps[1].status == "completed"
    assert result.steps[2].status == "conditional"
    assert result.steps[3].status == "conditional"
    assert verifier_calls == []
    assert finalizer_calls == []
    assert _sqlite_task_snapshot(store) == before_tasks
    assert _sqlite_artifact_snapshot(store) == before_artifacts
    assert _sqlite_merge_unit_snapshot(store) == before_units
    assert git.heads == before_refs
    assert git.mutation_calls == []


def test_landing_coordinator_pending_replay_revalidates_exact_deferred_task_and_finalizes_once(
    tmp_path,
) -> None:
    from gza.merge_services import ManualMergeExecutionResult
    from gza.review_verdict import parse_review_report

    store = _coordinator_store(tmp_path)
    impl = _completed_impl(store, "pending exact replay", "feature/landing")
    assert impl.id is not None
    unit = store.resolve_merge_unit_for_task(impl.id)
    assert unit is not None
    review = store.add("review", task_type="review", depends_on=impl.id, based_on=impl.id)
    review.status = "completed"
    review.output_content = _review_report_with_findings(
        "CHANGES_REQUESTED",
        blockers=(("B1", "Deferred blocker", "src/gza/landing.py:1"),),
    )
    store.update(review)
    finding = parse_review_report(review.output_content).findings[0]
    _set_merge_unit_proof_fields(store, unit.id, owner_task_id=impl.id)
    deferred, deferred_identity = _authorized_pending_deferred_task(
        store,
        impl=impl,
        review=review,
        finding=finding,
    )
    authorization = _pending_replay_authorization(
        impl=impl,
        unit_id=unit.id,
        deferred_identity=deferred_identity,
        review_id=review.id,
    )
    _persist_pending_finalization_with_authorization(
        store,
        impl=impl,
        authorization=authorization,
        target_sha="merge-a",
        deferred_task_ids=(deferred.id,),
    )
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "merge-a"},
        local_branches={"feature/landing"},
        ancestors={("head-a", "merge-a")},
        merged_refs={("feature/landing", "main")},
    )
    finalizations: list[str] = []

    def finalize(identity: Any, decision: Any, provenance: str) -> ManualMergeExecutionResult:
        finalizations.append(provenance)
        return _finalize_landing_merge_state(store, identity, decision, provenance)

    result = LandingCoordinator(
        store=store,
        git=git,
        finalize_merge=finalize,
        post_merge_verifier=lambda identity: _post_merge_success(identity),
    ).run(LandRequest(task_id=impl.id))

    refreshed = store.get_merge_unit(unit.id)
    assert result.merged is True
    assert result.deferred_task_ids == (deferred.id,)
    assert finalizations == ["manual_land_escalated"]
    assert refreshed is not None
    assert refreshed.state == "merged"


def test_landing_coordinator_pending_replay_accepts_canonical_ordinary_followup_and_finalizes_once(
    tmp_path,
) -> None:
    from gza.merge_services import ManualMergeExecutionResult
    from gza.review_verdict import parse_review_report

    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "pending ordinary followup", "feature/landing")
    assert impl.id is not None
    unit = store.resolve_merge_unit_for_task(impl.id)
    assert unit is not None
    review = store.add("review", task_type="review", depends_on=impl.id, based_on=impl.id)
    review.status = "completed"
    review.output_content = _review_report_with_findings(
        "APPROVED_WITH_FOLLOWUPS",
        followups=(("F1", "Followup task", "src/gza/landing.py:2"),),
    )
    store.update(review)
    assert review.id is not None
    finding = parse_review_report(review.output_content).findings[0]
    _set_merge_unit_proof_fields(store, unit.id, owner_task_id=impl.id)
    followup, followup_identity = _authorized_pending_followup_task(
        store,
        config=config,
        impl=impl,
        review=review,
        finding=finding,
    )
    assert followup.id is not None
    assert followup.urgent is False
    assert followup.create_pr is False
    authorization = _pending_replay_authorization(
        impl=impl,
        unit_id=unit.id,
        review_id=review.id,
        review_verdict="APPROVED_WITH_FOLLOWUPS",
        followup_identity=followup_identity,
    )
    _persist_pending_finalization_with_authorization(
        store,
        impl=impl,
        authorization=authorization,
        target_sha="merge-a",
        provenance="manual_land",
        followup_task_ids=(followup.id,),
    )
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "merge-a"},
        local_branches={"feature/landing"},
        ancestors={("head-a", "merge-a")},
        merged_refs={("feature/landing", "main")},
    )
    finalizations: list[str] = []

    def finalize(identity: Any, decision: Any, provenance: str) -> ManualMergeExecutionResult:
        finalizations.append(provenance)
        return _finalize_landing_merge_state(store, identity, decision, provenance)

    result = LandingCoordinator(
        store=store,
        git=git,
        finalize_merge=finalize,
        post_merge_verifier=lambda identity: _post_merge_success(identity),
    ).run(LandRequest(task_id=impl.id))

    refreshed = store.get_merge_unit(unit.id)
    assert result.merged is True
    assert result.merge_provenance == "manual_land"
    assert result.followup_task_ids == (followup.id,)
    assert finalizations == ["manual_land"]
    assert refreshed is not None
    assert refreshed.state == "merged"
    assert refreshed.merge_source == "manual_land"


@pytest.mark.parametrize("field", ("urgent", "create_pr"))
def test_landing_coordinator_pending_replay_refuses_ordinary_followup_property_escalation(
    tmp_path,
    field: str,
) -> None:
    from gza.merge_services import ManualMergeExecutionResult
    from gza.review_verdict import parse_review_report

    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, f"pending ordinary followup mutation {field}", "feature/landing")
    assert impl.id is not None
    unit = store.resolve_merge_unit_for_task(impl.id)
    assert unit is not None
    review = store.add("review", task_type="review", depends_on=impl.id, based_on=impl.id)
    review.status = "completed"
    review.output_content = _review_report_with_findings(
        "APPROVED_WITH_FOLLOWUPS",
        followups=(("F1", "Followup task", "src/gza/landing.py:2"),),
    )
    store.update(review)
    assert review.id is not None
    finding = parse_review_report(review.output_content).findings[0]
    _set_merge_unit_proof_fields(store, unit.id, owner_task_id=impl.id)
    followup, followup_identity = _authorized_pending_followup_task(
        store,
        config=config,
        impl=impl,
        review=review,
        finding=finding,
    )
    assert followup.id is not None
    setattr(followup, field, True)
    store.update(followup)
    authorization = _pending_replay_authorization(
        impl=impl,
        unit_id=unit.id,
        review_id=review.id,
        review_verdict="APPROVED_WITH_FOLLOWUPS",
        followup_identity=followup_identity,
    )
    _persist_pending_finalization_with_authorization(
        store,
        impl=impl,
        authorization=authorization,
        target_sha="merge-a",
        provenance="manual_land",
        followup_task_ids=(followup.id,),
    )
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "merge-a"},
        local_branches={"feature/landing"},
        ancestors={("head-a", "merge-a")},
        merged_refs={("feature/landing", "main")},
    )

    def finalize(*_args: Any, **_kwargs: Any) -> ManualMergeExecutionResult:
        raise AssertionError("mutated ordinary follow-up must block finalization")

    result = LandingCoordinator(
        store=store,
        git=git,
        finalize_merge=finalize,
        post_merge_verifier=lambda identity: _post_merge_success(identity),
    ).run(LandRequest(task_id=impl.id))

    refreshed = store.get_merge_unit(unit.id)
    assert result.merged is False
    assert result.post_merge_verify_failure is not None
    assert result.post_merge_verify_failure.status == "final_preflight_failed"
    assert "pending follow-up task identity" in result.post_merge_verify_failure.fact
    assert field in result.post_merge_verify_failure.fact
    assert refreshed is not None
    assert refreshed.state == "unmerged"


def test_landing_coordinator_pending_replay_accepts_guarded_followup_true_properties(
    tmp_path,
) -> None:
    from gza.merge_services import ManualMergeExecutionResult
    from gza.review_verdict import parse_review_report

    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "pending escalated followup", "feature/landing")
    assert impl.id is not None
    unit = store.resolve_merge_unit_for_task(impl.id)
    assert unit is not None
    review = store.add("review", task_type="review", depends_on=impl.id, based_on=impl.id)
    review.status = "completed"
    review.output_content = _review_report_with_findings(
        "CHANGES_REQUESTED",
        blockers=(("B1", "Deferred blocker", "src/gza/landing.py:1"),),
        followups=(("F1", "Followup task", "src/gza/landing.py:2"),),
    )
    store.update(review)
    assert review.id is not None
    findings = parse_review_report(review.output_content).findings
    followup_finding = next(finding for finding in findings if finding.severity == "FOLLOWUP")
    _set_merge_unit_proof_fields(store, unit.id, owner_task_id=impl.id)
    followup, followup_identity = _authorized_pending_followup_task(
        store,
        config=config,
        impl=impl,
        review=review,
        finding=followup_finding,
        escalated=True,
    )
    assert followup.id is not None
    assert followup.urgent is True
    assert followup.create_pr is True
    authorization = _pending_replay_authorization(
        impl=impl,
        unit_id=unit.id,
        review_id=review.id,
        review_verdict="CHANGES_REQUESTED",
        followup_identity=followup_identity,
    )
    _persist_pending_finalization_with_authorization(
        store,
        impl=impl,
        authorization=authorization,
        target_sha="merge-a",
        provenance="manual_land_escalated",
        followup_task_ids=(followup.id,),
    )
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "merge-a"},
        local_branches={"feature/landing"},
        ancestors={("head-a", "merge-a")},
        merged_refs={("feature/landing", "main")},
    )
    finalizations: list[str] = []

    def finalize(identity: Any, decision: Any, provenance: str) -> ManualMergeExecutionResult:
        finalizations.append(provenance)
        return _finalize_landing_merge_state(store, identity, decision, provenance)

    result = LandingCoordinator(
        store=store,
        git=git,
        finalize_merge=finalize,
        post_merge_verifier=lambda identity: _post_merge_success(identity),
    ).run(LandRequest(task_id=impl.id))

    refreshed = store.get_merge_unit(unit.id)
    assert result.merged is True
    assert result.merge_provenance == "manual_land_escalated"
    assert result.followup_task_ids == (followup.id,)
    assert finalizations == ["manual_land_escalated"]
    assert refreshed is not None
    assert refreshed.state == "merged"
    assert refreshed.merge_source == "manual_land_escalated"


@pytest.mark.parametrize("field", ("urgent", "create_pr"))
def test_landing_coordinator_pending_replay_refuses_guarded_followup_missing_true_property(
    tmp_path,
    field: str,
) -> None:
    from gza.merge_services import ManualMergeExecutionResult
    from gza.review_verdict import parse_review_report

    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, f"pending escalated followup mutation {field}", "feature/landing")
    assert impl.id is not None
    unit = store.resolve_merge_unit_for_task(impl.id)
    assert unit is not None
    review = store.add("review", task_type="review", depends_on=impl.id, based_on=impl.id)
    review.status = "completed"
    review.output_content = _review_report_with_findings(
        "CHANGES_REQUESTED",
        blockers=(("B1", "Deferred blocker", "src/gza/landing.py:1"),),
        followups=(("F1", "Followup task", "src/gza/landing.py:2"),),
    )
    store.update(review)
    assert review.id is not None
    findings = parse_review_report(review.output_content).findings
    followup_finding = next(finding for finding in findings if finding.severity == "FOLLOWUP")
    _set_merge_unit_proof_fields(store, unit.id, owner_task_id=impl.id)
    followup, followup_identity = _authorized_pending_followup_task(
        store,
        config=config,
        impl=impl,
        review=review,
        finding=followup_finding,
        escalated=True,
    )
    assert followup.id is not None
    setattr(followup, field, False)
    store.update(followup)
    authorization = _pending_replay_authorization(
        impl=impl,
        unit_id=unit.id,
        review_id=review.id,
        review_verdict="CHANGES_REQUESTED",
        followup_identity=followup_identity,
    )
    _persist_pending_finalization_with_authorization(
        store,
        impl=impl,
        authorization=authorization,
        target_sha="merge-a",
        provenance="manual_land_escalated",
        followup_task_ids=(followup.id,),
    )
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "merge-a"},
        local_branches={"feature/landing"},
        ancestors={("head-a", "merge-a")},
        merged_refs={("feature/landing", "main")},
    )

    def finalize(*_args: Any, **_kwargs: Any) -> ManualMergeExecutionResult:
        raise AssertionError("mutated guarded follow-up must block finalization")

    result = LandingCoordinator(
        store=store,
        git=git,
        finalize_merge=finalize,
        post_merge_verifier=lambda identity: _post_merge_success(identity),
    ).run(LandRequest(task_id=impl.id))

    refreshed = store.get_merge_unit(unit.id)
    assert result.merged is False
    assert result.post_merge_verify_failure is not None
    assert result.post_merge_verify_failure.status == "final_preflight_failed"
    assert "pending follow-up task identity" in result.post_merge_verify_failure.fact
    assert field in result.post_merge_verify_failure.fact
    assert refreshed is not None
    assert refreshed.state == "unmerged"


def test_landing_coordinator_prepared_replay_recovers_deferred_blocker_after_proof_write_failure(
    tmp_path,
) -> None:
    from gza.merge_services import ManualMergeExecutionResult
    from gza.review_tasks import build_deferred_blocker_prompt, format_blocker_finding_context
    from gza.review_verdict import parse_review_report

    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "prepared deferred replay", "feature/landing")
    assert impl.id is not None
    unit = store.resolve_merge_unit_for_task(impl.id)
    assert unit is not None
    review = store.add("review", task_type="review", depends_on=impl.id, based_on=impl.id)
    review.status = "completed"
    review.output_content = _review_report_with_findings(
        "CHANGES_REQUESTED",
        blockers=(("B1", "Deferred blocker", "src/gza/landing.py:1"),),
    )
    store.update(review)
    assert review.id is not None
    finding = parse_review_report(review.output_content).findings[0]
    deferred_prompt = build_deferred_blocker_prompt(review.id, impl.id, finding)
    deferred_scope = format_blocker_finding_context(finding)
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-a")},
    )
    merge_calls: list[str] = []
    created_deferred_ids: list[str] = []
    finalizations: list[str] = []
    original_add_artifact = store.add_artifact
    artifact_stages: list[str] = []

    def flaky_add_artifact(*args: Any, **kwargs: Any) -> Any:
        metadata = kwargs.get("metadata")
        if isinstance(metadata, dict) and metadata.get("kind") == "landing_pending_finalization":
            artifact_stages.append(str(metadata.get("stage")))
            if metadata.get("stage") == "pending":
                raise RuntimeError("artifact store unavailable after merge")
        return original_add_artifact(*args, **kwargs)

    def inspect(identity: Any) -> LandingPolicyFacts:
        return _green_facts(
            task_id=identity.owner_task_id,
            source_head=identity.source_sha,
            target_head=identity.target_sha,
            parked_reason="review-max-cycles-reached",
            review=_review(
                verdict="CHANGES_REQUESTED",
                reviewed_head=identity.source_sha,
                review_id=review.id,
            ),
            open_blockers=(
                LandingOpenBlocker(
                    "B1",
                    deferrable=True,
                    blocker_class="out_of_scope",
                    source=f"review:{review.id}",
                    fingerprint="blocker:B1:normalized",
                    deferred_task_prompt_sha256="sha256:" + sha256(deferred_prompt.encode()).hexdigest(),
                    deferred_task_review_scope_sha256="sha256:" + sha256(deferred_scope.encode()).hexdigest(),
                ),
            ),
        )

    def merge(_identity: Any, _decision: Any, _provenance: str) -> ManualMergeExecutionResult:
        merge_calls.append("git_merge")
        _simulate_no_ff_landing_git_merge(git, merge_sha="merge-a")
        blocker = store.add(
            deferred_prompt,
            task_type="implement",
            based_on=review.id,
            depends_on=impl.id,
            review_scope=deferred_scope,
            create_pr=True,
            urgent=True,
        )
        assert blocker.id is not None
        created_deferred_ids.append(blocker.id)
        return ManualMergeExecutionResult(rc=0, status="merged", created_deferred_blockers=[blocker])

    verify_results: list[LandPostMergeVerifyFailure | LandPostMergeVerifySuccess] = [
        LandPostMergeVerifyFailure(
            status="failed",
            fact="post-merge checkpoint is red",
            checkpoint_id="checkpoint-red",
            target_head="merge-a",
            gate_identity="main",
        ),
        _post_merge_success(SimpleNamespace(target_branch="main")),
    ]

    def post_merge_verify(identity: Any) -> LandPostMergeVerifyFailure | LandPostMergeVerifySuccess:
        return verify_results.pop(0)

    def finalize(identity: Any, decision: Any, provenance: str) -> ManualMergeExecutionResult:
        finalizations.append(provenance)
        return _finalize_landing_merge_state(store, identity, decision, provenance)

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(store, "add_artifact", flaky_add_artifact)
    try:
        first = LandingCoordinator(
            store=store,
            git=git,
            inspect_policy_facts=inspect,
            landing_judge=_landing_judgment,
            execute_merge=merge,
            finalize_merge=finalize,
            post_merge_verifier=post_merge_verify,
        ).run(LandRequest(task_id=impl.id))
    finally:
        monkeypatch.undo()

    assert first.post_merge_verify_failure is not None
    assert "proof persistence failed" in first.post_merge_verify_failure.fact
    assert artifact_stages == ["prepared", "pending"]
    assert merge_calls == ["git_merge"]
    assert created_deferred_ids
    assert store.get_merge_unit(unit.id).state == "unmerged"  # type: ignore[union-attr]

    second = LandingCoordinator(
        store=store,
        git=git,
        inspect_policy_facts=lambda _identity: (_ for _ in ()).throw(AssertionError("policy facts must not reload")),
        landing_judge=lambda: (_ for _ in ()).throw(AssertionError("judgment must not rerun")),
        execute_merge=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("git merge must not rerun")),
        finalize_merge=finalize,
        post_merge_verifier=post_merge_verify,
    ).run(LandRequest(task_id=impl.id))

    assert second.post_merge_verify_failure is not None
    assert second.deferred_task_ids == tuple(created_deferred_ids)
    assert merge_calls == ["git_merge"]
    assert finalizations == []
    assert store.get_merge_unit(unit.id).state == "unmerged"  # type: ignore[union-attr]

    third = LandingCoordinator(
        store=store,
        git=git,
        inspect_policy_facts=lambda _identity: (_ for _ in ()).throw(AssertionError("policy facts must not reload")),
        landing_judge=lambda: (_ for _ in ()).throw(AssertionError("judgment must not rerun")),
        execute_merge=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("git merge must not rerun")),
        finalize_merge=finalize,
        post_merge_verifier=post_merge_verify,
    ).run(LandRequest(task_id=impl.id))

    refreshed = store.get_merge_unit(unit.id)
    assert third.merged is True
    assert third.deferred_task_ids == tuple(created_deferred_ids)
    assert merge_calls == ["git_merge"]
    assert finalizations == ["manual_land_escalated"]
    assert refreshed is not None
    assert refreshed.state == "merged"


def test_landing_coordinator_prepared_replay_recovers_followup_after_proof_write_failure(
    tmp_path,
) -> None:
    from gza.merge_services import ManualMergeExecutionResult
    from gza.review_tasks import create_or_reuse_followup_task
    from gza.review_verdict import parse_review_report

    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "prepared followup replay", "feature/landing")
    assert impl.id is not None
    unit = store.resolve_merge_unit_for_task(impl.id)
    assert unit is not None
    review = store.add("review", task_type="review", depends_on=impl.id, based_on=impl.id)
    review.status = "completed"
    review.output_content = _review_report_with_findings(
        "APPROVED_WITH_FOLLOWUPS",
        followups=(("F1", "Followup task", "src/gza/landing.py:2"),),
    )
    store.update(review)
    assert review.id is not None
    finding = parse_review_report(review.output_content).findings[0]
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-a")},
    )
    merge_calls: list[str] = []
    created_followup_ids: list[str] = []
    finalizations: list[str] = []
    original_add_artifact = store.add_artifact
    artifact_stages: list[str] = []

    def flaky_add_artifact(*args: Any, **kwargs: Any) -> Any:
        metadata = kwargs.get("metadata")
        if isinstance(metadata, dict) and metadata.get("kind") == "landing_pending_finalization":
            artifact_stages.append(str(metadata.get("stage")))
            if metadata.get("stage") == "pending":
                raise RuntimeError("artifact store unavailable after merge")
        return original_add_artifact(*args, **kwargs)

    def inspect(identity: Any) -> LandingPolicyFacts:
        return _green_facts(
            task_id=identity.owner_task_id,
            source_head=identity.source_sha,
            target_head=identity.target_sha,
            review=_review(
                verdict="APPROVED_WITH_FOLLOWUPS",
                reviewed_head=identity.source_sha,
                review_id=review.id,
                followup_findings=(
                    LandingFollowupFinding(
                        "F1",
                        fingerprint=_landing_review_finding_fingerprint_for_test(finding),
                        source=f"review:{review.id}",
                    ),
                ),
            ),
        )

    def merge(_identity: Any, _decision: Any, _provenance: str) -> ManualMergeExecutionResult:
        merge_calls.append("git_merge")
        _simulate_no_ff_landing_git_merge(git, merge_sha="merge-a")
        followup, _created = create_or_reuse_followup_task(
            store,
            config=None,
            review_task=review,
            impl_task=impl,
            finding=finding,
            trigger_source="manual_land",
        )
        assert followup.id is not None
        assert followup.urgent is False
        assert followup.create_pr is False
        created_followup_ids.append(followup.id)
        return ManualMergeExecutionResult(rc=0, status="merged", created_followups=[followup])

    verify_results: list[LandPostMergeVerifyFailure | LandPostMergeVerifySuccess] = [
        LandPostMergeVerifyFailure(
            status="failed",
            fact="post-merge checkpoint is red",
            checkpoint_id="checkpoint-red",
            target_head="merge-a",
            gate_identity="main",
        ),
        _post_merge_success(SimpleNamespace(target_branch="main")),
    ]

    def post_merge_verify(identity: Any) -> LandPostMergeVerifyFailure | LandPostMergeVerifySuccess:
        return verify_results.pop(0)

    def finalize(identity: Any, decision: Any, provenance: str) -> ManualMergeExecutionResult:
        finalizations.append(provenance)
        return _finalize_landing_merge_state(store, identity, decision, provenance)

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(store, "add_artifact", flaky_add_artifact)
    try:
        first = LandingCoordinator(
            store=store,
            git=git,
            config=config,
            inspect_policy_facts=inspect,
            execute_merge=merge,
            finalize_merge=finalize,
            post_merge_verifier=post_merge_verify,
        ).run(LandRequest(task_id=impl.id))
    finally:
        monkeypatch.undo()

    assert first.post_merge_verify_failure is not None
    assert "proof persistence failed" in first.post_merge_verify_failure.fact
    assert artifact_stages == ["prepared", "pending"]
    assert merge_calls == ["git_merge"]
    assert created_followup_ids
    assert store.get_merge_unit(unit.id).state == "unmerged"  # type: ignore[union-attr]

    second = LandingCoordinator(
        store=store,
        git=git,
        inspect_policy_facts=lambda _identity: (_ for _ in ()).throw(AssertionError("policy facts must not reload")),
        execute_merge=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("git merge must not rerun")),
        finalize_merge=finalize,
        post_merge_verifier=post_merge_verify,
    ).run(LandRequest(task_id=impl.id))

    assert second.post_merge_verify_failure is not None
    assert second.followup_task_ids == tuple(created_followup_ids)
    assert merge_calls == ["git_merge"]
    assert finalizations == []
    assert store.get_merge_unit(unit.id).state == "unmerged"  # type: ignore[union-attr]

    third = LandingCoordinator(
        store=store,
        git=git,
        inspect_policy_facts=lambda _identity: (_ for _ in ()).throw(AssertionError("policy facts must not reload")),
        execute_merge=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("git merge must not rerun")),
        finalize_merge=finalize,
        post_merge_verifier=post_merge_verify,
    ).run(LandRequest(task_id=impl.id))

    refreshed = store.get_merge_unit(unit.id)
    assert third.merged is True
    assert third.followup_task_ids == tuple(created_followup_ids)
    assert merge_calls == ["git_merge"]
    assert finalizations == ["manual_land"]
    assert refreshed is not None
    assert refreshed.state == "merged"


def test_landing_coordinator_prepared_replay_refuses_mutated_recovered_task(
    tmp_path,
) -> None:
    from gza.merge_services import ManualMergeExecutionResult
    from gza.review_tasks import create_or_reuse_followup_task
    from gza.review_verdict import parse_review_report

    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "prepared mutated followup replay", "feature/landing")
    assert impl.id is not None
    unit = store.resolve_merge_unit_for_task(impl.id)
    assert unit is not None
    review = store.add("review", task_type="review", depends_on=impl.id, based_on=impl.id)
    review.status = "completed"
    review.output_content = _review_report_with_findings(
        "APPROVED_WITH_FOLLOWUPS",
        followups=(("F1", "Followup task", "src/gza/landing.py:2"),),
    )
    store.update(review)
    assert review.id is not None
    finding = parse_review_report(review.output_content).findings[0]
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-a")},
    )
    original_add_artifact = store.add_artifact
    created_followup: list[Task] = []

    def flaky_add_artifact(*args: Any, **kwargs: Any) -> Any:
        metadata = kwargs.get("metadata")
        if isinstance(metadata, dict) and metadata.get("kind") == "landing_pending_finalization":
            if metadata.get("stage") == "pending":
                raise RuntimeError("artifact store unavailable after merge")
        return original_add_artifact(*args, **kwargs)

    def inspect(identity: Any) -> LandingPolicyFacts:
        return _green_facts(
            task_id=identity.owner_task_id,
            source_head=identity.source_sha,
            target_head=identity.target_sha,
            review=_review(
                verdict="APPROVED_WITH_FOLLOWUPS",
                reviewed_head=identity.source_sha,
                review_id=review.id,
                followup_findings=(
                    LandingFollowupFinding(
                        "F1",
                        fingerprint=_landing_review_finding_fingerprint_for_test(finding),
                        source=f"review:{review.id}",
                    ),
                ),
            ),
        )

    def merge(_identity: Any, _decision: Any, _provenance: str) -> ManualMergeExecutionResult:
        _simulate_no_ff_landing_git_merge(git, merge_sha="merge-a")
        followup, _created = create_or_reuse_followup_task(
            store,
            config=None,
            review_task=review,
            impl_task=impl,
            finding=finding,
            trigger_source="manual_land",
        )
        assert followup.urgent is False
        assert followup.create_pr is False
        created_followup.append(followup)
        return ManualMergeExecutionResult(rc=0, status="merged", created_followups=[followup])

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(store, "add_artifact", flaky_add_artifact)
    try:
        first = LandingCoordinator(
            store=store,
            git=git,
            config=config,
            inspect_policy_facts=inspect,
            execute_merge=merge,
            post_merge_verifier=lambda identity: _post_merge_success(identity),
        ).run(LandRequest(task_id=impl.id))
    finally:
        monkeypatch.undo()

    assert first.post_merge_verify_failure is not None
    assert created_followup and created_followup[0].id is not None
    created_followup[0].urgent = True
    store.update(created_followup[0])

    def finalize(*_args: Any, **_kwargs: Any) -> ManualMergeExecutionResult:
        raise AssertionError("mutated recovered task must block finalization")

    result = LandingCoordinator(
        store=store,
        git=git,
        inspect_policy_facts=lambda _identity: (_ for _ in ()).throw(AssertionError("policy facts must not reload")),
        execute_merge=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("git merge must not rerun")),
        finalize_merge=finalize,
        post_merge_verifier=lambda identity: _post_merge_success(identity),
    ).run(LandRequest(task_id=impl.id))

    refreshed = store.get_merge_unit(unit.id)
    assert result.merged is False
    assert result.post_merge_verify_failure is not None
    assert result.post_merge_verify_failure.status == "final_preflight_failed"
    assert "pending follow-up task identity" in result.post_merge_verify_failure.fact
    assert refreshed is not None
    assert refreshed.state == "unmerged"


@pytest.mark.parametrize("field", ("missing", "prompt", "review_scope", "urgent", "create_pr"))
def test_landing_coordinator_pending_replay_refuses_mutated_deferred_task_identity(
    tmp_path,
    field: str,
) -> None:
    from gza.merge_services import ManualMergeExecutionResult
    from gza.review_verdict import parse_review_report

    store = _coordinator_store(tmp_path)
    impl = _completed_impl(store, f"pending deferred mutation {field}", "feature/landing")
    assert impl.id is not None
    unit = store.resolve_merge_unit_for_task(impl.id)
    assert unit is not None
    review = store.add("review", task_type="review", depends_on=impl.id, based_on=impl.id)
    review.status = "completed"
    review.output_content = _review_report_with_findings(
        "CHANGES_REQUESTED",
        blockers=(("B1", "Deferred blocker", "src/gza/landing.py:1"),),
    )
    store.update(review)
    finding = parse_review_report(review.output_content).findings[0]
    _set_merge_unit_proof_fields(store, unit.id, owner_task_id=impl.id)
    deferred, deferred_identity = _authorized_pending_deferred_task(
        store,
        impl=impl,
        review=review,
        finding=finding,
    )
    authorization = _pending_replay_authorization(
        impl=impl,
        unit_id=unit.id,
        deferred_identity=deferred_identity,
        review_id=review.id,
    )
    _persist_pending_finalization_with_authorization(
        store,
        impl=impl,
        authorization=authorization,
        target_sha="merge-a",
        deferred_task_ids=(deferred.id,),
    )
    if field == "missing":
        with store._write_transaction() as conn:
            conn.execute("DELETE FROM tasks WHERE project_id = ? AND id = ?", (store._project_id, deferred.id))
    else:
        setattr(deferred, field, False if field in {"urgent", "create_pr"} else f"mutated {field}")
        store.update(deferred)
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "merge-a"},
        local_branches={"feature/landing"},
        ancestors={("head-a", "merge-a")},
        merged_refs={("feature/landing", "main")},
    )

    def finalize(*_args: Any, **_kwargs: Any) -> ManualMergeExecutionResult:
        raise AssertionError("mutated pending deferred task must block finalization")

    result = LandingCoordinator(
        store=store,
        git=git,
        finalize_merge=finalize,
        post_merge_verifier=lambda identity: _post_merge_success(identity),
    ).run(LandRequest(task_id=impl.id))

    refreshed = store.get_merge_unit(unit.id)
    assert result.merged is False
    assert result.post_merge_verify_failure is not None
    assert result.post_merge_verify_failure.status == "final_preflight_failed"
    assert "pending deferred blocker task identity" in result.post_merge_verify_failure.fact
    assert refreshed is not None
    assert refreshed.state == "unmerged"


@pytest.mark.parametrize(
    "mutation",
    ("membership", "state", "source_branch", "head_sha", "head_sha_cleared", "base_sha"),
)
def test_landing_coordinator_pending_replay_refuses_merge_unit_identity_change(
    tmp_path,
    mutation: str,
) -> None:
    from gza.merge_services import ManualMergeExecutionResult
    from gza.review_verdict import parse_review_report

    store = _coordinator_store(tmp_path)
    impl = _completed_impl(store, f"pending merge unit mutation {mutation}", "feature/landing")
    assert impl.id is not None
    unit = store.resolve_merge_unit_for_task(impl.id)
    assert unit is not None
    review = store.add("review", task_type="review", depends_on=impl.id, based_on=impl.id)
    review.status = "completed"
    review.output_content = _review_report_with_findings(
        "CHANGES_REQUESTED",
        blockers=(("B1", "Deferred blocker", "src/gza/landing.py:1"),),
    )
    store.update(review)
    finding = parse_review_report(review.output_content).findings[0]
    _set_merge_unit_proof_fields(store, unit.id, owner_task_id=impl.id)
    deferred, deferred_identity = _authorized_pending_deferred_task(
        store,
        impl=impl,
        review=review,
        finding=finding,
    )
    authorization = _pending_replay_authorization(
        impl=impl,
        unit_id=unit.id,
        deferred_identity=deferred_identity,
        review_id=review.id,
    )
    _persist_pending_finalization_with_authorization(
        store,
        impl=impl,
        authorization=authorization,
        target_sha="merge-a",
        deferred_task_ids=(deferred.id,),
    )
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "merge-a"},
        local_branches={"feature/landing"},
        ancestors={("head-a", "merge-a")},
        merged_refs={("feature/landing", "main")},
    )

    def finalize(*_args: Any, **_kwargs: Any) -> ManualMergeExecutionResult:
        raise AssertionError("mutated pending merge unit must block finalization")

    def post_merge_verify(identity: Any) -> LandPostMergeVerifySuccess:
        success = _post_merge_success(identity)
        if mutation == "membership":
            other = store.create_merge_unit(
                source_branch="feature/other",
                target_branch="main",
                owner_task_id=impl.id,
                state="unmerged",
            )
            store.attach_task_to_merge_unit(impl.id, other.id, "owner")
        elif mutation == "state":
            _set_merge_unit_proof_fields(store, unit.id, state="empty", owner_task_id=impl.id)
        elif mutation == "source_branch":
            _set_merge_unit_proof_fields(store, unit.id, source_branch="feature/other", owner_task_id=impl.id)
        elif mutation == "head_sha":
            _set_merge_unit_proof_fields(store, unit.id, head_sha="head-b", owner_task_id=impl.id)
        elif mutation == "head_sha_cleared":
            _set_merge_unit_proof_fields(store, unit.id, head_sha=None, owner_task_id=impl.id)
        else:
            _set_merge_unit_proof_fields(store, unit.id, base_sha="base-b", owner_task_id=impl.id)
        return success

    result = LandingCoordinator(
        store=store,
        git=git,
        finalize_merge=finalize,
        post_merge_verifier=post_merge_verify,
    ).run(LandRequest(task_id=impl.id))

    assert result.merged is False
    assert result.post_merge_verify_failure is not None
    assert result.post_merge_verify_failure.status == "final_preflight_failed"
    assert (
        "merge unit identity changed" in result.post_merge_verify_failure.fact
        or "selected task no longer resolves" in result.post_merge_verify_failure.fact
    )
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    if mutation == "state":
        assert refreshed_unit.state == "empty"
    else:
        assert refreshed_unit.state == "unmerged"


def test_landing_coordinator_pending_replay_allows_different_source_ref_for_same_durable_branch(
    tmp_path,
) -> None:
    from gza.merge_services import ManualMergeExecutionResult
    from gza.review_verdict import parse_review_report

    store = _coordinator_store(tmp_path)
    impl = _completed_impl(store, "pending source ref differs", "feature/landing")
    assert impl.id is not None
    unit = store.resolve_merge_unit_for_task(impl.id)
    assert unit is not None
    review = store.add("review", task_type="review", depends_on=impl.id, based_on=impl.id)
    review.status = "completed"
    review.output_content = _review_report_with_findings(
        "CHANGES_REQUESTED",
        blockers=(("B1", "Deferred blocker", "src/gza/landing.py:1"),),
    )
    store.update(review)
    finding = parse_review_report(review.output_content).findings[0]
    _set_merge_unit_proof_fields(store, unit.id, owner_task_id=impl.id)
    deferred, deferred_identity = _authorized_pending_deferred_task(
        store,
        impl=impl,
        review=review,
        finding=finding,
    )
    authorization = _pending_replay_authorization(
        impl=impl,
        unit_id=unit.id,
        deferred_identity=deferred_identity,
        review_id=review.id,
        source_branch="feature/landing",
        source_ref="refs/heads/feature/landing",
    )
    _persist_pending_finalization_with_authorization(
        store,
        impl=impl,
        authorization=authorization,
        target_sha="merge-a",
        deferred_task_ids=(deferred.id,),
    )
    git = _LandingSourceGit(
        {"refs/heads/feature/landing": "head-a", "main": "merge-a"},
        local_branches=set(),
        ancestors={("head-a", "merge-a")},
        merged_refs={("refs/heads/feature/landing", "main")},
    )

    def resolve_fresh_merge_source(_branch: str) -> str:
        return "refs/heads/feature/landing"

    git.resolve_fresh_merge_source = resolve_fresh_merge_source  # type: ignore[attr-defined]
    finalizations: list[str] = []

    def finalize(identity: Any, decision: Any, provenance: str) -> ManualMergeExecutionResult:
        finalizations.append(provenance)
        return _finalize_landing_merge_state(store, identity, decision, provenance)

    result = LandingCoordinator(
        store=store,
        git=git,
        finalize_merge=finalize,
        post_merge_verifier=lambda identity: _post_merge_success(identity),
    ).run(LandRequest(task_id=impl.id))

    refreshed = store.get_merge_unit(unit.id)
    assert result.merged is True
    assert finalizations == ["manual_land_escalated"]
    assert refreshed is not None
    assert refreshed.state == "merged"


def test_landing_coordinator_pending_replay_restores_when_handoff_mutates_during_finalizer(
    tmp_path,
) -> None:
    from gza.merge_services import ManualMergeExecutionResult
    from gza.review_verdict import parse_review_report

    store = _coordinator_store(tmp_path)
    impl = _completed_impl(store, "pending handoff write race", "feature/landing")
    assert impl.id is not None
    unit = store.resolve_merge_unit_for_task(impl.id)
    assert unit is not None
    review = store.add("review", task_type="review", depends_on=impl.id, based_on=impl.id)
    review.status = "completed"
    review.output_content = _review_report_with_findings(
        "CHANGES_REQUESTED",
        blockers=(("B1", "Deferred blocker", "src/gza/landing.py:1"),),
    )
    store.update(review)
    finding = parse_review_report(review.output_content).findings[0]
    _set_merge_unit_proof_fields(store, unit.id, owner_task_id=impl.id)
    deferred, deferred_identity = _authorized_pending_deferred_task(
        store,
        impl=impl,
        review=review,
        finding=finding,
    )
    authorization = _pending_replay_authorization(
        impl=impl,
        unit_id=unit.id,
        deferred_identity=deferred_identity,
        review_id=review.id,
    )
    _persist_pending_finalization_with_authorization(
        store,
        impl=impl,
        authorization=authorization,
        target_sha="merge-a",
        deferred_task_ids=(deferred.id,),
    )
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "merge-a"},
        local_branches={"feature/landing"},
        ancestors={("head-a", "merge-a")},
        merged_refs={("feature/landing", "main")},
    )

    def finalize(identity: Any, _decision: Any, provenance: str) -> ManualMergeExecutionResult:
        deferred.prompt = "mutated after final preflight"
        store.update(deferred)
        assert identity.merge_unit_proof is not None
        persisted = store.set_merge_unit_state_if_identity(
            identity.merge_unit_id,
            "merged",
            expected_identity=identity.merge_unit_proof,
            merge_source=provenance,
        )
        assert persisted is True
        return ManualMergeExecutionResult(rc=0, status="merged")

    result = LandingCoordinator(
        store=store,
        git=git,
        finalize_merge=finalize,
        post_merge_verifier=lambda identity: _post_merge_success(identity),
    ).run(LandRequest(task_id=impl.id))

    refreshed = store.get_merge_unit(unit.id)
    assert result.merged is False
    assert result.post_merge_verify_failure is not None
    assert result.post_merge_verify_failure.status == "state_persistence_failed"
    assert "pending deferred blocker task identity" in result.post_merge_verify_failure.fact
    assert refreshed is not None
    assert refreshed.state == "unmerged"
    assert refreshed.merge_source is None


def test_landing_coordinator_pending_replay_refuses_proof_mutation_during_finalizer(
    tmp_path,
) -> None:
    from gza.merge_services import ManualMergeExecutionResult
    from gza.review_verdict import parse_review_report

    store = _coordinator_store(tmp_path)
    impl = _completed_impl(store, "pending proof write race", "feature/landing")
    assert impl.id is not None
    unit = store.resolve_merge_unit_for_task(impl.id)
    assert unit is not None
    review = store.add("review", task_type="review", depends_on=impl.id, based_on=impl.id)
    review.status = "completed"
    review.output_content = _review_report_with_findings(
        "CHANGES_REQUESTED",
        blockers=(("B1", "Deferred blocker", "src/gza/landing.py:1"),),
    )
    store.update(review)
    finding = parse_review_report(review.output_content).findings[0]
    _set_merge_unit_proof_fields(store, unit.id, owner_task_id=impl.id)
    deferred, deferred_identity = _authorized_pending_deferred_task(
        store,
        impl=impl,
        review=review,
        finding=finding,
    )
    authorization = _pending_replay_authorization(
        impl=impl,
        unit_id=unit.id,
        deferred_identity=deferred_identity,
        review_id=review.id,
    )
    _persist_pending_finalization_with_authorization(
        store,
        impl=impl,
        authorization=authorization,
        target_sha="merge-a",
        deferred_task_ids=(deferred.id,),
    )
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "merge-a"},
        local_branches={"feature/landing"},
        ancestors={("head-a", "merge-a")},
        merged_refs={("feature/landing", "main")},
    )

    def finalize(identity: Any, _decision: Any, provenance: str) -> ManualMergeExecutionResult:
        _set_merge_unit_proof_fields(store, unit.id, head_sha="head-b", owner_task_id=impl.id)
        assert identity.merge_unit_proof is not None
        persisted = store.set_merge_unit_state_if_identity(
            identity.merge_unit_id,
            "merged",
            expected_identity=identity.merge_unit_proof,
            merge_source=provenance,
        )
        assert persisted is False
        return ManualMergeExecutionResult(
            rc=1,
            status="post_merge_state_persistence_failed",
            block_reason="landing merge state identity changed before persistence",
        )

    result = LandingCoordinator(
        store=store,
        git=git,
        finalize_merge=finalize,
        post_merge_verifier=lambda identity: _post_merge_success(identity),
    ).run(LandRequest(task_id=impl.id))

    refreshed = store.get_merge_unit(unit.id)
    assert result.merged is False
    assert result.post_merge_verify_failure is not None
    assert result.post_merge_verify_failure.status == "state_persistence_failed"
    assert "landing merge state identity changed" in result.post_merge_verify_failure.fact
    assert refreshed is not None
    assert refreshed.state == "unmerged"
    assert refreshed.merge_source is None


@pytest.mark.parametrize("mutation", ("target", "source", "checkout"))
def test_landing_coordinator_post_checkpoint_mutation_blocks_merged_state(
    tmp_path,
    mutation: str,
) -> None:
    from gza.merge_services import ManualMergeExecutionResult

    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, f"post checkpoint mutation {mutation}", "feature/landing")
    assert impl.id is not None
    unit = store.resolve_merge_unit_for_task(impl.id)
    assert unit is not None
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-a")},
    )
    finalizations: list[str] = []

    def inspect(identity: Any) -> LandingPolicyFacts:
        return _green_facts(
            task_id=identity.owner_task_id,
            source_head=identity.source_sha,
            target_head=identity.target_sha,
            review=_review(verdict="APPROVED", reviewed_head=identity.source_sha),
            open_blockers=(),
        )

    def merge(*_args: Any, **_kwargs: Any) -> ManualMergeExecutionResult:
        _simulate_no_ff_landing_git_merge(git, merge_sha="merge-a")
        return ManualMergeExecutionResult(rc=0, status="merged")

    def post_merge_verify(_identity: Any) -> LandPostMergeVerifySuccess:
        success = LandPostMergeVerifySuccess(
            checkpoint_id="checkpoint-green",
            target_head="merge-a",
            tree_fingerprint=TREE_A,
            gate_identity="main",
        )
        if mutation == "target":
            git.heads["main"] = "merge-b"
        elif mutation == "source":
            git.heads["feature/landing"] = "head-b"
        else:
            git.dirty = True
        return success

    def finalize(*_args: Any, **_kwargs: Any) -> ManualMergeExecutionResult:
        finalizations.append("finalize")
        raise AssertionError("merged state must not be finalized after post-checkpoint mutation")

    result = LandingCoordinator(
        store=store,
        git=git,
        config=config,
        inspect_policy_facts=inspect,
        execute_merge=merge,
        finalize_merge=finalize,
        post_merge_verifier=post_merge_verify,
    ).run(LandRequest(task_id=impl.id))

    refreshed = store.get_merge_unit(unit.id)
    assert result.blocked is None
    assert result.post_merge_verify_failure is not None
    assert result.post_merge_verify_failure.status == "final_preflight_failed"
    assert result.merged is False
    assert result.merge_provenance is None
    assert finalizations == []
    assert refreshed is not None
    assert refreshed.state == "unmerged"
    assert refreshed.merge_source is None


def test_landing_coordinator_finalizer_non_success_is_post_merge_failure_and_replays(
    tmp_path,
) -> None:
    from gza.merge_services import ManualMergeExecutionResult

    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "finalizer return failure", "feature/landing")
    assert impl.id is not None
    unit = store.resolve_merge_unit_for_task(impl.id)
    assert unit is not None
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-a")},
    )
    merge_calls: list[str] = []
    finalization_results = ["fail", "pass"]

    def inspect(identity: Any) -> LandingPolicyFacts:
        return _green_facts(
            task_id=identity.owner_task_id,
            source_head=identity.source_sha,
            target_head=identity.target_sha,
            review=_review(verdict="APPROVED", reviewed_head=identity.source_sha),
            open_blockers=(),
        )

    def merge(*_args: Any, **_kwargs: Any) -> ManualMergeExecutionResult:
        merge_calls.append("git_merge")
        _simulate_no_ff_landing_git_merge(git, merge_sha="merge-a")
        return ManualMergeExecutionResult(rc=0, status="merged")

    def finalize(identity: Any, decision: Any, provenance: str) -> ManualMergeExecutionResult:
        if finalization_results.pop(0) == "fail":
            return ManualMergeExecutionResult(
                rc=1,
                status="post_merge_state_persistence_failed",
                block_reason="state write failed",
            )
        return _finalize_landing_merge_state(store, identity, decision, provenance)

    first = LandingCoordinator(
        store=store,
        git=git,
        config=config,
        inspect_policy_facts=inspect,
        execute_merge=merge,
        finalize_merge=finalize,
        post_merge_verifier=lambda identity: _post_merge_success(identity),
    ).run(LandRequest(task_id=impl.id))

    assert first.blocked is None
    assert first.post_merge_verify_failure is not None
    assert first.post_merge_verify_failure.status == "state_persistence_failed"
    assert first.post_merge_verify_failure.fact == "state write failed"
    assert first.merge_provenance is None
    assert store.get_merge_unit(unit.id).state == "unmerged"  # type: ignore[union-attr]

    second = LandingCoordinator(
        store=store,
        git=git,
        config=config,
        inspect_policy_facts=lambda _identity: (_ for _ in ()).throw(AssertionError("policy facts must not reload")),
        execute_merge=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("git merge must not rerun")),
        finalize_merge=finalize,
        post_merge_verifier=lambda identity: _post_merge_success(identity),
    ).run(LandRequest(task_id=impl.id))

    assert second.merged is True
    assert second.merge_provenance == "manual_land"
    assert merge_calls == ["git_merge"]
    assert store.get_merge_unit(unit.id).state == "merged"  # type: ignore[union-attr]


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

    result = LandResult(
        request=LandRequest(task_id="gza-9316", policy="guarded", dry_run=True),
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
    monkeypatch.setattr("gza.landing.run_production_landing", lambda **_kwargs: result)

    status = land_cli.cmd_land(
        land_cli.argparse.Namespace(project_dir=tmp_path, task_id="gza-9316", policy="guarded", dry_run=True)
    )
    output = capsys.readouterr().out

    assert status == 0
    assert "verify-1 passed for gate gate-a" in output
    assert "review gza-10158 is APPROVED" in output


def test_production_landing_factory_wires_service_collaborators(monkeypatch, tmp_path) -> None:
    from gza.cli import _common as common_cli, git_ops

    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-a")},
    )

    def create_rebase(*_args: Any, **_kwargs: Any) -> Task:
        raise AssertionError("factory wiring test should not execute rebase creation")

    def run_rebase(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("factory wiring test should not execute rebase")

    monkeypatch.setattr(common_cli, "_create_rebase_task", create_rebase)
    monkeypatch.setattr(git_ops, "_run_task_backed_rebase", run_rebase)

    coordinator = create_production_landing_coordinator(
        config=config,
        store=store,
        git=git,
        policy="guarded",
    )

    assert coordinator.store is store
    assert coordinator.git is git
    assert coordinator.config is config
    assert coordinator.create_rebase_task is create_rebase
    assert coordinator.rebase_executor is run_rebase
    assert coordinator.inspect_policy_facts is not None
    assert coordinator.landing_judge is not None
    assert coordinator.execute_merge is not None
    assert coordinator.finalize_merge is not None
    assert coordinator.post_merge_verifier is not None
    assert coordinator.collaborators is None


def test_refresh_landing_authorization_rechecks_current_policy_facts_before_merge(tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "service authorization refresh", "feature/landing")
    assert impl.id is not None
    _persist_lifecycle_verify_for_landing(store, config, impl)
    review = _completed_full_review(store, impl, head="head-a", verdict="APPROVED")
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-a")},
    )
    coordinator = create_production_landing_coordinator(
        config=config,
        store=store,
        git=git,
        policy="strict",
    )
    identity = coordinator._resolve_identity(LandRequest(task_id=impl.id, policy="strict"), persist_reconciliation=False)
    assert not isinstance(identity, LandBlocked)
    facts = coordinator._landing_policy_facts(identity)
    decision = evaluate_landing_policy(policy="strict", facts=facts, judge=None)
    assert decision.allowed is True
    assert refresh_landing_authorization(coordinator, identity, decision, policy="strict") is not None

    review.output_content = _review_report_with_findings(
        "CHANGES_REQUESTED",
        blockers=(("B1", "Now-required current defect", "src/gza/landing.py:1"),),
    )
    store.update(review)

    assert refresh_landing_authorization(coordinator, identity, decision, policy="strict") is None


def test_cmd_land_guarded_uses_durable_judge_and_typed_authorization_to_reach_merge(
    monkeypatch,
    capsys,
    tmp_path,
) -> None:
    from gza.cli import land as land_cli
    from gza.cli.git_ops import _MergeSingleTaskResult
    from gza.landing_judge import LandingJudgeResult

    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "CLI guarded landing", "feature/landing")
    assert impl.id is not None
    _persist_lifecycle_verify_for_landing(store, config, impl)
    review = _completed_full_review(store, impl, head="head-a", verdict="CHANGES_REQUESTED")
    review.output_content = _review_report_with_findings(
        "CHANGES_REQUESTED",
        blockers=(("B1", "Out-of-scope polish debt", "docs/internal/landing.md:12"),),
    )
    store.update(review)
    _add_review_blocker_resolution(store, impl=impl, review=review)
    _park_for_review_blocker_adjudication(store, impl)
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-a")},
    )
    judge_calls: list[tuple[str, tuple[str, ...]]] = []
    merge_authorizations: list[Any] = []

    def obtain(**kwargs: Any) -> LandingJudgeResult:
        judge_calls.append((kwargs["identity"].key, tuple(blocker.finding_id for blocker in kwargs["blockers"])))
        artifact = _persist_test_landing_judgment(store, impl, review, kwargs["identity"], kwargs["prompt"])
        return LandingJudgeResult(
            judgment=LandingJudgment("LAND", artifact_id=str(artifact.id), key=kwargs["identity"].key),
            reused_artifact=True,
        )

    def merge_single(*_args: Any, **kwargs: Any) -> _MergeSingleTaskResult:
        authorization = kwargs["landing_authorization"]
        merge_authorizations.append(authorization)
        assert authorization.allowed_overrides == (
            "defer-review-blockers",
            "parked:review-blocker-adjudication-needed",
        )
        assert kwargs["load_landing_authorization"]() == authorization
        _simulate_no_ff_landing_git_merge(git, merge_sha="target-after")
        deferred = store.add("Deferred B1", task_type="implement", depends_on=impl.id, urgent=True, create_pr=True)
        return _MergeSingleTaskResult(rc=0, status="merged", created_deferred_blockers=(deferred,))

    main_verify_calls: list[str] = []

    def main_verify(*_args: Any, **_kwargs: Any) -> Any:
        main_verify_calls.append("called")
        state = SimpleNamespace(
            task=SimpleNamespace(id="gza-main-verify"),
            head_sha="target-after",
            tree_fingerprint=TREE_A,
            verify_status="passed",
            alert_message=None,
        )
        return SimpleNamespace(state=state, is_current=True, merges_halted=False, needs_attention=False)

    monkeypatch.setattr(land_cli.Config, "load", lambda _project_dir: config)
    monkeypatch.setattr(land_cli, "get_store", lambda _config, open_mode="readwrite": store)
    monkeypatch.setattr(land_cli, "resolve_id", lambda _config, task_id: task_id)
    monkeypatch.setattr(land_cli, "Git", lambda _project_dir: git)
    monkeypatch.setattr("gza.landing_judge.obtain_landing_judgment", obtain)
    monkeypatch.setattr("gza.cli.git_ops._merge_single_task", merge_single)
    monkeypatch.setattr("gza.main_integration_verify.check_main_integration_verify", main_verify)

    rc = land_cli.cmd_land(argparse.Namespace(project_dir=tmp_path, task_id=impl.id, policy="guarded", dry_run=False))

    assert rc == 0
    assert main_verify_calls == ["called"]
    assert judge_calls == [(merge_authorizations[0].judgment_key, ("B1",))]
    assert merge_authorizations[0].review_id == review.id
    assert merge_authorizations[0].blocker_fingerprints
    assert "Landed" in capsys.readouterr().out


def test_cmd_land_reports_red_post_merge_target_verify_as_non_success(
    monkeypatch,
    capsys,
    tmp_path,
) -> None:
    from gza.cli import land as land_cli
    from gza.cli.git_ops import _MergeSingleTaskResult

    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "CLI normal landing", "feature/landing")
    assert impl.id is not None
    _persist_lifecycle_verify_for_landing(store, config, impl)
    _completed_full_review(store, impl, head="head-a", verdict="APPROVED")
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-a")},
    )

    def merge_single(*_args: Any, **_kwargs: Any) -> _MergeSingleTaskResult:
        return _MergeSingleTaskResult(rc=0, status="merged")

    def main_verify(*_args: Any, **_kwargs: Any) -> Any:
        state = SimpleNamespace(
            task=SimpleNamespace(id="gza-main-verify"),
            head_sha="target-after",
            tree_fingerprint=TREE_A,
            verify_status="failed",
            alert_message="main verify RED - merges halted",
        )
        return SimpleNamespace(state=state, is_current=True, merges_halted=True, needs_attention=True)

    monkeypatch.setattr(land_cli.Config, "load", lambda _project_dir: config)
    monkeypatch.setattr(land_cli, "get_store", lambda _config, open_mode="readwrite": store)
    monkeypatch.setattr(land_cli, "resolve_id", lambda _config, task_id: task_id)
    monkeypatch.setattr(land_cli, "Git", lambda _project_dir: git)
    monkeypatch.setattr("gza.cli.git_ops._merge_single_task", merge_single)
    monkeypatch.setattr("gza.main_integration_verify.check_main_integration_verify", main_verify)

    rc = land_cli.cmd_land(argparse.Namespace(project_dir=tmp_path, task_id=impl.id, policy="guarded", dry_run=False))

    output = capsys.readouterr().out
    assert rc == 1
    assert "Git merged" in output
    assert "before recording merged state" in output
    assert "integration verification failed" in output
    assert "main verify RED" in output


def test_cmd_land_reports_state_persistence_failure_without_cannot_land_and_reruns(
    monkeypatch,
    capsys,
    tmp_path,
) -> None:
    from gza.cli import land as land_cli
    from gza.cli.git_ops import _MergeSingleTaskResult

    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "CLI persistence failure landing", "feature/landing")
    assert impl.id is not None
    unit = store.resolve_merge_unit_for_task(impl.id)
    assert unit is not None
    _persist_lifecycle_verify_for_landing(store, config, impl)
    _completed_full_review(store, impl, head="head-a", verdict="APPROVED")
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-a")},
    )
    merge_calls: list[str] = []
    conditional_persist_calls: list[str | None] = []

    def merge_single(*_args: Any, **_kwargs: Any) -> _MergeSingleTaskResult:
        merge_calls.append("git_merge")
        _simulate_no_ff_landing_git_merge(git, merge_sha="merge-a")
        return _MergeSingleTaskResult(rc=0, status="merged")

    def main_verify(*_args: Any, **_kwargs: Any) -> Any:
        state = SimpleNamespace(
            task=SimpleNamespace(id="gza-main-verify"),
            head_sha="merge-a",
            tree_fingerprint=TREE_A,
            verify_status="passed",
            alert_message=None,
        )
        return SimpleNamespace(state=state, is_current=True, merges_halted=False, needs_attention=False)

    original_set_merge_unit_state_if_identity = store.set_merge_unit_state_if_identity

    def set_merge_unit_state_if_identity(unit_id_arg: str, state_arg: str, **kwargs: Any) -> bool:
        conditional_persist_calls.append(kwargs.get("merge_source"))
        if len(conditional_persist_calls) == 1:
            raise RuntimeError("db locked")
        return original_set_merge_unit_state_if_identity(unit_id_arg, state_arg, **kwargs)

    monkeypatch.setattr(land_cli.Config, "load", lambda _project_dir: config)
    monkeypatch.setattr(land_cli, "get_store", lambda _config, open_mode="readwrite": store)
    monkeypatch.setattr(land_cli, "resolve_id", lambda _config, task_id: task_id)
    monkeypatch.setattr(land_cli, "Git", lambda _project_dir: git)
    monkeypatch.setattr(store, "set_merge_unit_state_if_identity", set_merge_unit_state_if_identity)
    monkeypatch.setattr("gza.cli.git_ops._merge_single_task", merge_single)
    monkeypatch.setattr("gza.main_integration_verify.check_main_integration_verify", main_verify)

    first_rc = land_cli.cmd_land(argparse.Namespace(project_dir=tmp_path, task_id=impl.id, policy="guarded", dry_run=False))
    first_output = capsys.readouterr().out

    assert first_rc == 1
    assert "Cannot land" not in first_output
    assert "Git merged" in first_output
    assert "merged-state finalization failed" in first_output
    assert "db locked" in first_output
    assert merge_calls == ["git_merge"]
    assert conditional_persist_calls == ["manual_land"]

    second_rc = land_cli.cmd_land(argparse.Namespace(project_dir=tmp_path, task_id=impl.id, policy="guarded", dry_run=False))
    second_output = capsys.readouterr().out

    assert second_rc == 0
    assert "Landed" in second_output
    assert "manual_land provenance" in second_output
    assert merge_calls == ["git_merge"]
    assert conditional_persist_calls == ["manual_land", "manual_land"]


def test_cmd_land_dry_run_reports_pending_finalization_without_replay_side_effects(
    monkeypatch,
    capsys,
    tmp_path,
) -> None:
    from gza.cli import land as land_cli

    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "CLI pending dry-run", "feature/landing")
    assert impl.id is not None
    unit = store.resolve_merge_unit_for_task(impl.id)
    assert unit is not None
    deferred = store.add("Deferred B1", task_type="implement", depends_on=impl.id, urgent=True, create_pr=True)
    assert deferred.id is not None
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "merge-a"},
        local_branches={"feature/landing"},
        ancestors={("head-a", "merge-a")},
    )
    _persist_exact_landing_pending_finalization(
        store,
        impl=impl,
        unit_id=unit.id,
        source_ref="feature/landing",
        source_sha="head-a",
        target_branch="main",
        target_sha="merge-a",
        provenance="manual_land_escalated",
        deferred_task_ids=(deferred.id,),
    )
    before_tasks = _sqlite_task_snapshot(store)
    before_artifacts = _sqlite_artifact_snapshot(store)
    before_units = _sqlite_merge_unit_snapshot(store)
    before_refs = dict(git.heads)
    opened_modes: list[str] = []
    verifier_calls: list[str] = []
    finalizer_calls: list[str] = []

    def get_store_query_only(_config: Config, open_mode: str = "readwrite") -> SqliteTaskStore:
        opened_modes.append(open_mode)
        return store

    def main_verify(*_args: Any, **_kwargs: Any) -> Any:
        verifier_calls.append("verify")
        raise AssertionError("dry-run pending finalization must not verify")

    def mark(*_args: Any, **_kwargs: Any) -> None:
        finalizer_calls.append("finalize")
        raise AssertionError("dry-run pending finalization must not finalize")

    monkeypatch.setattr(land_cli.Config, "load", lambda _project_dir: config)
    monkeypatch.setattr(land_cli, "get_store", get_store_query_only)
    monkeypatch.setattr(land_cli, "resolve_id", lambda _config, task_id: task_id)
    monkeypatch.setattr(land_cli, "Git", lambda _project_dir: git)
    monkeypatch.setattr("gza.landing.mark_merge_subject_merged", mark)
    monkeypatch.setattr("gza.main_integration_verify.check_main_integration_verify", main_verify)

    rc = land_cli.cmd_land(argparse.Namespace(project_dir=tmp_path, task_id=impl.id, policy="guarded", dry_run=True))
    output = capsys.readouterr().out

    assert rc == 0
    assert opened_modes == ["query_only"]
    assert "post_merge_verify: conditional" in output
    assert "would refresh the post-merge target verification checkpoint" in output
    assert "would finalize authoritative merged state only after a green post-merge checkpoint" in output
    assert "Dry run for" in output
    assert verifier_calls == []
    assert finalizer_calls == []
    assert _sqlite_task_snapshot(store) == before_tasks
    assert _sqlite_artifact_snapshot(store) == before_artifacts
    assert _sqlite_merge_unit_snapshot(store) == before_units
    assert git.heads == before_refs
    assert git.mutation_calls == []


def test_cmd_land_recovers_prepared_pending_identity_after_post_merge_artifact_failure(
    monkeypatch,
    capsys,
    tmp_path,
) -> None:
    from gza.cli import land as land_cli
    from gza.cli.git_ops import _MergeSingleTaskResult

    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "CLI prepared replay landing", "feature/landing")
    assert impl.id is not None
    unit = store.resolve_merge_unit_for_task(impl.id)
    assert unit is not None
    _persist_lifecycle_verify_for_landing(store, config, impl)
    _completed_full_review(store, impl, head="head-a", verdict="APPROVED")
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-a")},
    )
    merge_calls: list[str] = []
    mark_calls: list[str] = []
    verify_statuses = ["failed", "passed"]
    original_add_artifact = store.add_artifact
    artifact_calls: list[str] = []

    def flaky_add_artifact(*args: Any, **kwargs: Any) -> Any:
        metadata = kwargs.get("metadata")
        if isinstance(metadata, dict):
            artifact_calls.append(str(metadata.get("stage")))
        if len(artifact_calls) == 2:
            raise RuntimeError("artifact store unavailable after merge")
        return original_add_artifact(*args, **kwargs)

    def merge_single(*_args: Any, **_kwargs: Any) -> _MergeSingleTaskResult:
        merge_calls.append("git_merge")
        _simulate_no_ff_landing_git_merge(git, merge_sha="merge-a")
        return _MergeSingleTaskResult(rc=0, status="merged")

    def main_verify(*_args: Any, **_kwargs: Any) -> Any:
        status = verify_statuses.pop(0)
        state = SimpleNamespace(
            task=SimpleNamespace(id=f"gza-main-verify-{status}"),
            head_sha="merge-a",
            tree_fingerprint=TREE_A,
            verify_status=status,
            alert_message="main verify RED - merges halted" if status == "failed" else None,
        )
        return SimpleNamespace(
            state=state,
            is_current=True,
            merges_halted=status != "passed",
            needs_attention=status != "passed",
        )

    def mark(store_arg: Any, *, merge_subject: Any, merge_unit_id: str | None, merge_source: str) -> None:
        del store_arg, merge_subject, merge_unit_id
        mark_calls.append(merge_source)
        _finalize_landing_merge_state(store, SimpleNamespace(owner_task=impl, merge_unit_id=unit.id), None, merge_source)

    monkeypatch.setattr(land_cli.Config, "load", lambda _project_dir: config)
    monkeypatch.setattr(land_cli, "get_store", lambda _config, open_mode="readwrite": store)
    monkeypatch.setattr(land_cli, "resolve_id", lambda _config, task_id: task_id)
    monkeypatch.setattr(land_cli, "Git", lambda _project_dir: git)
    monkeypatch.setattr("gza.landing.mark_merge_subject_merged", mark)
    monkeypatch.setattr(store, "add_artifact", flaky_add_artifact)
    monkeypatch.setattr("gza.cli.git_ops._merge_single_task", merge_single)
    monkeypatch.setattr("gza.main_integration_verify.check_main_integration_verify", main_verify)

    first_rc = land_cli.cmd_land(argparse.Namespace(project_dir=tmp_path, task_id=impl.id, policy="guarded", dry_run=False))
    first_output = capsys.readouterr().out

    assert first_rc == 1
    assert "Git merged" in first_output
    assert "pending-finalization proof persistence failed" in first_output
    assert merge_calls == ["git_merge"]
    assert mark_calls == []
    assert store.get_merge_unit(unit.id).state == "unmerged"  # type: ignore[union-attr]
    assert artifact_calls == ["prepared", "pending"]

    second_rc = land_cli.cmd_land(argparse.Namespace(project_dir=tmp_path, task_id=impl.id, policy="guarded", dry_run=False))
    second_output = capsys.readouterr().out

    assert second_rc == 1
    assert "Git merged" in second_output
    assert "integration verification failed" in second_output
    assert "main verify RED" in second_output
    assert merge_calls == ["git_merge"]
    assert mark_calls == []
    assert store.get_merge_unit(unit.id).state == "unmerged"  # type: ignore[union-attr]

    third_rc = land_cli.cmd_land(argparse.Namespace(project_dir=tmp_path, task_id=impl.id, policy="guarded", dry_run=False))
    third_output = capsys.readouterr().out

    assert third_rc == 0
    assert "Landed" in third_output
    assert merge_calls == ["git_merge"]
    assert mark_calls == []
    refreshed = store.get_merge_unit(unit.id)
    assert refreshed is not None
    assert refreshed.state == "merged"
    assert refreshed.merge_source == "manual_land"


def _persist_test_landing_judgment(
    store: SqliteTaskStore,
    impl: Task,
    review: Task,
    identity: Any,
    prompt: str,
) -> Any:
    from gza.landing_judge import (
        _extract_prompt_context_envelope,
        create_or_reuse_landing_judge_task,
        parse_landing_judge_output,
        persist_landing_judgment_artifact,
    )

    blocker_ids = tuple(blocker.finding_id for blocker in identity.blocker_identities)
    envelope = _extract_prompt_context_envelope(prompt)
    decision_context = envelope["decision_context"]
    assert isinstance(decision_context, dict)
    review_output = decision_context["review_output"]
    assert isinstance(review_output, str)
    review.output_content = review_output
    store.update(review)
    payload = {
        "schema_version": "landing_judge.v1",
        "result": "LAND",
        "ask_met": True,
        "blocker_decisions": [
            {
                "finding_id": finding_id,
                "decision": "DEFERABLE",
                "citations": [f"blocker:{finding_id}", "review:current", "scope:authoritative"],
                "reason": "safe adjacent follow-up",
            }
            for finding_id in blocker_ids
        ],
        "citations": [
            "request:task",
            "plan:context",
            "scope:authoritative",
            "review:current",
            "diff:current",
            "verify:green",
            "adjudication:current",
        ],
        "blocking_fact": "none",
    }
    parsed = parse_landing_judge_output(
        json.dumps(payload),
        expected_blocker_ids=blocker_ids,
        allowed_citation_ids=tuple(
            sorted(
                {
                    "scope:authoritative",
                    "request:task",
                    "plan:context",
                    "diff:current",
                    "verify:green",
                    "review:current",
                    "adjudication:current",
                    *(f"blocker:{finding_id}" for finding_id in blocker_ids),
                }
            )
        ),
    )
    assert parsed is not None
    judge_task, _created = create_or_reuse_landing_judge_task(
        store,
        config=None,
        owner_task=impl,
        review_task=review,
        identity=identity,
        prompt=prompt,
        trigger_source="manual_land",
    )
    judge_task.status = "completed"
    judge_task.completed_at = datetime(2026, 8, 26, 12, 0, tzinfo=UTC)
    judge_task.output_content = json.dumps(payload)
    store.update(judge_task)
    return persist_landing_judgment_artifact(
        store,
        owner_task=impl,
        config=_verify_config(store.db_path.parent),
        identity=identity,
        parsed=parsed,
        judge_task_id=judge_task.id,
    )


@pytest.mark.parametrize(
    "mutation",
    ("review_output", "implementation_summary", "judge_status", "judge_output", "deleted_authorized_artifact"),
)
def test_cmd_land_guarded_final_reload_refuses_stale_or_missing_judgment_artifact(
    monkeypatch,
    capsys,
    tmp_path,
    mutation: str,
) -> None:
    from gza.cli import land as land_cli
    from gza.cli.git_ops import _MergeSingleTaskResult
    from gza.landing_judge import LandingJudgeResult

    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "CLI guarded stale evidence", "feature/landing")
    assert impl.id is not None
    _persist_lifecycle_verify_for_landing(store, config, impl)
    review = _completed_full_review(store, impl, head="head-a", verdict="CHANGES_REQUESTED")
    review.output_content = _review_report_with_findings(
        "CHANGES_REQUESTED",
        blockers=(("B1", "Out-of-scope polish debt", "docs/internal/landing.md:12"),),
    )
    store.update(review)
    _add_review_blocker_resolution(store, impl=impl, review=review)
    _park_for_review_blocker_adjudication(store, impl)
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-a")},
    )
    artifacts: list[Any] = []
    judge_tasks: list[str] = []

    def obtain(**kwargs: Any) -> LandingJudgeResult:
        artifact = _persist_test_landing_judgment(store, impl, review, kwargs["identity"], kwargs["prompt"])
        artifacts.append(artifact)
        judge_task_id = artifact.metadata["judge_task_id"]
        assert isinstance(judge_task_id, str)
        judge_tasks.append(judge_task_id)
        if mutation == "deleted_authorized_artifact":
            newer = _persist_test_landing_judgment(store, impl, review, kwargs["identity"], kwargs["prompt"])
            assert newer.id != artifact.id
            artifacts.append(newer)
        return LandingJudgeResult(
            judgment=LandingJudgment("LAND", artifact_id=str(artifact.id), key=kwargs["identity"].key),
            reused_artifact=True,
        )

    def mutate_current_evidence() -> None:
        if mutation == "review_output":
            review.output_content = _review_report_with_findings(
                "CHANGES_REQUESTED",
                blockers=(("B1", "Out-of-scope polish debt", "docs/internal/landing.md:12"),),
            ) + "\nAdditional evidence detail.\n"
            store.update(review)
        elif mutation == "implementation_summary":
            refreshed = store.get(impl.id or "")
            assert refreshed is not None
            refreshed.output_content = (refreshed.output_content or refreshed.prompt) + "\nCurrent summary changed.\n"
            store.update(refreshed)
        elif mutation == "judge_status":
            judge = store.get(judge_tasks[0])
            assert judge is not None
            judge.status = "failed"
            store.update(judge)
        elif mutation == "judge_output":
            judge = store.get(judge_tasks[0])
            assert judge is not None
            judge.output_content = json.dumps({"schema_version": "landing_judge.v1", "result": "LAND"})
            store.update(judge)
        elif mutation == "deleted_authorized_artifact":
            with store._connect() as conn:
                conn.execute(
                    "DELETE FROM task_artifacts WHERE project_id = ? AND id = ?",
                    (store._project_id, artifacts[0].id),
                )

    def merge_single(*_args: Any, **kwargs: Any) -> _MergeSingleTaskResult:
        mutate_current_evidence()
        assert kwargs["load_landing_authorization"]() is None
        return _MergeSingleTaskResult(
            rc=1,
            status="landing_authorization_changed",
            block_reason="landing authorization changed before merge side effects",
        )

    monkeypatch.setattr(land_cli.Config, "load", lambda _project_dir: config)
    monkeypatch.setattr(land_cli, "get_store", lambda _config, open_mode="readwrite": store)
    monkeypatch.setattr(land_cli, "resolve_id", lambda _config, task_id: task_id)
    monkeypatch.setattr(land_cli, "Git", lambda _project_dir: git)
    monkeypatch.setattr("gza.landing_judge.obtain_landing_judgment", obtain)
    monkeypatch.setattr("gza.cli.git_ops._merge_single_task", merge_single)

    rc = land_cli.cmd_land(argparse.Namespace(project_dir=tmp_path, task_id=impl.id, policy="guarded", dry_run=False))

    output = capsys.readouterr().out
    assert rc == 1
    assert "Cannot land" in output
    assert "landing authorization changed" in output
    assert [task for task in store.get_all() if task.task_type == "implement" and task.depends_on == impl.id] == []


def test_cmd_land_judge_receives_normalized_adjudication_content_and_actual_diff(
    monkeypatch,
    tmp_path,
) -> None:
    from gza.cli import land as land_cli
    from gza.landing_judge import LandingJudgeResult

    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "CLI guarded landing content", "feature/landing")
    assert impl.id is not None
    _persist_lifecycle_verify_for_landing(store, config, impl)
    review = _completed_full_review(store, impl, head="head-a", verdict="CHANGES_REQUESTED")
    review.output_content = _review_report_with_findings(
        "CHANGES_REQUESTED",
        blockers=(("B1", "Out-of-scope polish debt", "docs/internal/landing.md:12"),),
    )
    store.update(review)
    _add_review_blocker_resolution(store, impl=impl, review=review, reason="adjacent")
    _park_for_review_blocker_adjudication(store, impl)
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-a")},
        diff="diff --git a/docs/internal/landing.md b/docs/internal/landing.md\n+current patch\n",
    )
    captured: dict[str, Any] = {}

    def obtain(**kwargs: Any) -> LandingJudgeResult:
        captured["identity"] = kwargs["identity"]
        captured["prompt"] = kwargs["prompt"]
        return LandingJudgeResult(judgment=LandingJudgment("BLOCK", blocking_fact="captured context"))

    def merge_single(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("merge must not run when the judge refuses")

    monkeypatch.setattr(land_cli.Config, "load", lambda _project_dir: config)
    monkeypatch.setattr(land_cli, "get_store", lambda _config, open_mode="readwrite": store)
    monkeypatch.setattr(land_cli, "resolve_id", lambda _config, task_id: task_id)
    monkeypatch.setattr(land_cli, "Git", lambda _project_dir: git)
    monkeypatch.setattr("gza.landing_judge.obtain_landing_judgment", obtain)
    monkeypatch.setattr("gza.cli.git_ops._merge_single_task", merge_single)

    rc = land_cli.cmd_land(argparse.Namespace(project_dir=tmp_path, task_id=impl.id, policy="guarded", dry_run=False))

    assert rc == 1
    assert "normalized_reason" in captured["prompt"]
    assert "adjacent" in captured["prompt"]
    assert "diff --git a/docs/internal/landing.md b/docs/internal/landing.md" in captured["prompt"]
    assert captured["identity"].adjudication_content_identity.startswith("sha256:")


@pytest.mark.parametrize("failure", ("diff", "judge"))
def test_cmd_land_diff_and_judge_exceptions_are_typed_refusals_without_merge(
    monkeypatch,
    capsys,
    tmp_path,
    failure: str,
) -> None:
    from gza.cli import land as land_cli

    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "CLI guarded landing failure", "feature/landing")
    assert impl.id is not None
    _persist_lifecycle_verify_for_landing(store, config, impl)
    review = _completed_full_review(store, impl, head="head-a", verdict="CHANGES_REQUESTED")
    review.output_content = _review_report_with_findings(
        "CHANGES_REQUESTED",
        blockers=(("B1", "Out-of-scope polish debt", "docs/internal/landing.md:12"),),
    )
    store.update(review)
    _add_review_blocker_resolution(store, impl=impl, review=review)
    _park_for_review_blocker_adjudication(store, impl)
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-a")},
    )
    if failure == "diff":
        def bad_diff(_revision_range: str) -> str:
            raise RuntimeError("diff unavailable")

        git.get_diff = bad_diff  # type: ignore[method-assign]

    def obtain(**_kwargs: Any) -> Any:
        if failure == "judge":
            raise RuntimeError("judge unavailable")
        raise AssertionError("judge service must not run when diff evidence is unavailable")

    def merge_single(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("merge must not run after judge evidence failure")

    monkeypatch.setattr(land_cli.Config, "load", lambda _project_dir: config)
    monkeypatch.setattr(land_cli, "get_store", lambda _config, open_mode="readwrite": store)
    monkeypatch.setattr(land_cli, "resolve_id", lambda _config, task_id: task_id)
    monkeypatch.setattr(land_cli, "Git", lambda _project_dir: git)
    monkeypatch.setattr("gza.landing_judge.obtain_landing_judgment", obtain)
    monkeypatch.setattr("gza.cli.git_ops._merge_single_task", merge_single)

    rc = land_cli.cmd_land(argparse.Namespace(project_dir=tmp_path, task_id=impl.id, policy="guarded", dry_run=False))

    output = capsys.readouterr().out
    assert rc == 1
    assert "Cannot land" in output
    assert "guarded landing judgment" in output
    assert ("diff unavailable" in output) if failure == "diff" else ("judge unavailable" in output)


def test_landing_adjudication_identity_changes_when_reason_changes_with_stable_artifact_id(tmp_path) -> None:
    store = _coordinator_store(tmp_path)
    config = _verify_config(tmp_path)
    impl = _completed_impl(store, "adjudication reason identity", "feature/landing")
    assert impl.id is not None
    _persist_lifecycle_verify_for_landing(store, config, impl)
    review = _completed_full_review(store, impl, head="head-a", verdict="CHANGES_REQUESTED")
    review.output_content = _review_report_with_findings(
        "CHANGES_REQUESTED",
        blockers=(("B1", "Out-of-scope polish debt", "docs/internal/landing.md:12"),),
    )
    store.update(review)
    artifact = _add_review_blocker_resolution(store, impl=impl, review=review, reason="out_of_scope")
    git = _LandingSourceGit(
        {"feature/landing": "head-a", "main": "target-a"},
        local_branches={"feature/landing"},
        ancestors={("target-a", "head-a")},
    )
    coordinator = LandingCoordinator(store=store, git=git, config=config)
    identity = coordinator._resolve_identity(LandRequest(task_id=impl.id), persist_reconciliation=False)
    assert not isinstance(identity, LandBlocked)
    first = coordinator._landing_policy_facts(identity)

    _add_review_blocker_resolution(store, impl=impl, review=review, reason="adjacent", artifact_id=artifact.id)
    second = coordinator._landing_policy_facts(identity)

    assert first.adjudication_fingerprints != second.adjudication_fingerprints
    assert first.open_blockers[0].blocker_class == "out_of_scope"
    assert second.open_blockers[0].blocker_class == "adjacent"


def _verify_config(tmp_path) -> Config:
    config = Config(project_dir=tmp_path, project_name="test-project")
    config.model = "test-model"
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0
    return config


def _verify_result(
    *,
    status: str = "passed",
    head: str = "head-a",
    tree: str | None = None,
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
        reviewed_tree_sha=tree,
        reviewed_base_sha="base-a",
        working_directory="/tmp/worktree",
        failure=None if status == "passed" else "failed",
        output=output,
    )


def _decision(
    state: str,
    *,
    head: str = "head-a",
    tree: str | None = None,
    result_head: str | None = None,
    result_tree: str | None = None,
) -> VerifyGateDecision:
    epoch = make_verify_epoch(
        reviewed_branch="feature/landing",
        reviewed_head_sha=head,
        reviewed_tree_sha=tree,
        verify_command="./bin/tests",
        verify_timeout_seconds=120,
        verify_timeout_grace_seconds=5.0,
    )
    result = None if state in {"missing", "stale"} else _verify_result(
        status=state,
        head=result_head or head,
        tree=result_tree if result_tree is not None else tree,
    )
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


def test_inspect_current_landing_verify_accepts_same_tree_artifact_after_commit_rewrite(tmp_path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    config = _verify_config(tmp_path)
    impl = store.add("Implement landing verify same tree", task_type="implement")
    impl.status = "completed"
    impl.branch = "feature/landing"
    store.update(impl)
    _persist_lifecycle_verify_for_landing(
        store,
        config,
        impl,
        reviewed_head="head-a",
        reviewed_tree="tree-same",
    )

    evidence = inspect_current_landing_verify_evidence(
        store,
        impl,
        config=config,
        git=_FakeGit({"feature/landing": "head-b"}, trees={"feature/landing": "tree-same"}),
        source_head="head-b",
        tree_fingerprint=TREE_A,
    )

    assert evidence.status == "passed"
    assert evidence.current is True
    assert evidence.identity_matched is True
    assert '"head":"head-b"' in (evidence.epoch or "")
    assert '"tree":"tree-same"' in (evidence.epoch or "")


def test_inspect_current_landing_verify_rejects_legacy_missing_tree_after_commit_rewrite(tmp_path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    config = _verify_config(tmp_path)
    impl = store.add("Implement landing verify legacy stale", task_type="implement")
    impl.status = "completed"
    impl.branch = "feature/landing"
    store.update(impl)
    _persist_lifecycle_verify_for_landing(
        store,
        config,
        impl,
        reviewed_head="head-a",
        reviewed_tree=None,
    )

    evidence = inspect_current_landing_verify_evidence(
        store,
        impl,
        config=config,
        git=_FakeGit({"feature/landing": "head-b"}, trees={"feature/landing": "tree-same"}),
        source_head="head-b",
        tree_fingerprint=TREE_A,
    )

    assert evidence.status == "stale"
    assert evidence.current is False
    assert evidence.identity_matched is False


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
    reviewed_head: str = "head-a",
    reviewed_tree: str | None = None,
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
        reviewed_head_sha=reviewed_head,
        reviewed_tree_sha=reviewed_tree,
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
    merge_base_before: str = "merge-base-a",
    source_after: str = "source-a",
    target_after: str = "target-a",
    status: str = "provider_conflict_resolved",
    changed_diff: bool = False,
    provider_conflict_resolved: bool = True,
) -> Any:
    rebase.review_scope = build_rebase_diff_provenance(
        baseline=RebaseDiffBaseline(
            old_tip=source_before,
            target_at_start=target_before,
            merge_base_at_start="base-a",
        ),
        resolved_head_sha=source_after,
        resolved_target_sha=target_after,
        changed_diff_boundary_proven=True,
    )
    store.update(rebase)
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
    rebase.review_scope = build_rebase_diff_provenance(
        baseline=RebaseDiffBaseline(
            old_tip=source_before,
            target_at_start=target_before,
            merge_base_at_start=merge_base_before,
        ),
        resolved_head_sha=source_after,
        resolved_target_sha=target_after,
    )
    store.update(rebase)
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


def _landing_rebase(
    store: SqliteTaskStore,
    impl,
    *,
    status: str = "completed",
    task_type: str = "rebase",
    based_on: str | None = None,
    old_tip: str | None = "pre-head-a",
    target_at_start: str | None = "pre-target-a",
    merge_base_at_start: str | None = "merge-base-a",
    resolved_head: str | None = "head-a",
    resolved_target: str | None = "target-a",
):
    rebase = store.add(
        "Rebase landing review",
        task_type=task_type,
        based_on=based_on if based_on is not None else impl.id,
        same_branch=True,
    )
    rebase.status = status
    if status == "completed":
        rebase.completed_at = datetime(2026, 8, 26, 11, 0, tzinfo=UTC)
    rebase.review_scope = build_rebase_diff_provenance(
        baseline=RebaseDiffBaseline(
            old_tip=old_tip,
            target_at_start=target_at_start,
            merge_base_at_start=merge_base_at_start,
        ),
        resolved_head_sha=resolved_head,
        resolved_target_sha=resolved_target,
    )
    store.update(rebase)
    return rebase


def _unbindable_landing_rebase(store: SqliteTaskStore, impl, case: str) -> Task:
    if case == "missing-row":
        return Task(id="gza-missing-rebase", prompt="Missing rebase row", task_type="rebase", based_on=impl.id)
    if case == "wrong-task-type":
        return _landing_rebase(store, impl, task_type="implement")
    if case == "wrong-lineage":
        other = store.add("Other implementation", task_type="implement")
        return _landing_rebase(store, impl, based_on=other.id)
    if case == "missing-provenance":
        rebase = store.add("Rebase landing review", task_type="rebase", based_on=impl.id, same_branch=True)
        store.update(rebase)
        return rebase
    if case == "malformed-provenance":
        rebase = store.add("Rebase landing review", task_type="rebase", based_on=impl.id, same_branch=True)
        rebase.review_scope = "Rebase diff provenance: no"
        store.update(rebase)
        return rebase
    if case == "incomplete-provenance":
        return _landing_rebase(store, impl, merge_base_at_start=None)
    if case == "mismatched-head":
        return _landing_rebase(store, impl, resolved_head="other-head")
    if case == "mismatched-target":
        return _landing_rebase(store, impl, resolved_target="other-target")
    raise AssertionError(f"unknown unbindable rebase case: {case}")


def _resolution_review(
    store: SqliteTaskStore,
    impl,
    rebase,
    *,
    status: str,
    resolved_head: str,
    target: str,
    verify_head: str | None = None,
    pre_rebase_head: str | None = "pre-head-a",
    pre_rebase_target: str | None = "pre-target-a",
    pre_rebase_merge_base: str | None = "merge-base-a",
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
        pre_rebase_head_sha=pre_rebase_head,
        pre_rebase_target_sha=pre_rebase_target,
        pre_rebase_merge_base_sha=pre_rebase_merge_base,
    )
    review.review_verify_head_sha = verify_head
    store.update(review)
    return review


def test_post_rebase_review_creates_full_review_for_mechanical_unchanged_diff_without_prior_review(tmp_path) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)
    calls: list[str] = []

    def fake_full_review(*_args: Any, **_kwargs: Any):
        calls.append("created")
        review = store.add("Created full review", task_type="review", depends_on=impl.id, based_on=impl.id)
        review.status = "pending"
        review.review_verify_head_sha = "head-b"
        store.update(review)
        return review

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
        create_full_review=fake_full_review,
    )

    assert result.status == "created"
    assert result.need == "full"
    assert result.review_budget_used is True
    assert calls == ["created"]


def test_post_rebase_review_not_required_for_mechanical_unchanged_diff_with_valid_prior_review(tmp_path) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)
    prior = _completed_full_review(store, impl, head="head-a")

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
    assert prior.status == "completed"


def test_post_rebase_review_not_required_preserves_spent_budget(tmp_path) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)
    _completed_full_review(store, impl, head="head-a")

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
    _completed_full_review(store, impl, head="head-a")

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
    ("prior_head", "prior_output"),
    (
        ("old-head", _review_report("APPROVED")),
        ("head-a", "not a valid review verdict"),
    ),
)
def test_post_rebase_review_refreshes_for_stale_or_malformed_prior_mechanical_review(
    tmp_path,
    prior_head: str,
    prior_output: str,
) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)
    prior = _completed_full_review(store, impl, head=prior_head)
    prior.output_content = prior_output
    store.update(prior)
    calls: list[str] = []

    def fake_full_review(*_args: Any, **_kwargs: Any):
        calls.append("created")
        review = store.add("Created full review", task_type="review", depends_on=impl.id, based_on=impl.id)
        review.status = "pending"
        review.review_verify_head_sha = "head-b"
        store.update(review)
        return review

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
        create_full_review=fake_full_review,
    )

    assert result.status == "created"
    assert result.need == "full"
    assert result.review_budget_used is True
    assert calls == ["created"]


@pytest.mark.parametrize(
    ("outcome_kind", "no_op_subtype"),
    (
        ("mechanical", None),
        ("no_op", "already_contained"),
        ("no_op", "superseded_contained"),
        ("no_op", "unchanged_target"),
        ("no_op", "moot"),
    ),
)
@pytest.mark.parametrize("review_budget_used", (False, True))
def test_post_rebase_review_does_not_reuse_malformed_same_head_historical_carry_forward(
    tmp_path,
    outcome_kind: str,
    no_op_subtype: str | None,
    review_budget_used: bool,
) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)
    prior = _completed_full_review(store, impl, head="head-a")
    prior.output_content = "not a valid review verdict"
    store.update(prior)
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
            rebase_outcome_identity=_rebase_identity(outcome_kind=outcome_kind, no_op_subtype=no_op_subtype),
            rebase_outcome_kind=outcome_kind,
            changed_diff=False,
            conflict_resolved=False,
            review_budget_used=review_budget_used,
        ),
        create_full_review=fake_full_review,
    )

    if review_budget_used:
        assert result.status == "blocked"
        assert result.review_budget_used is True
        assert result.blocked is not None
        assert calls == []
    else:
        assert result.status == "created"
        assert result.need == "full"
        assert result.review_budget_used is True
        assert result.review_task is not prior
        assert calls == ["created"]


@pytest.mark.parametrize(
    "no_op_subtype",
    ("already_contained", "superseded_contained", "unchanged_target", "moot"),
)
@pytest.mark.parametrize("review_budget_used", (False, True))
def test_post_rebase_review_does_not_reuse_supported_no_op_same_head_review_when_identity_mismatches(
    tmp_path,
    no_op_subtype: str,
    review_budget_used: bool,
) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)
    prior = _completed_full_review(store, impl, head="head-a")
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
            rebase_outcome_identity=_rebase_identity(
                outcome_kind="no_op",
                no_op_subtype=no_op_subtype,
                attempted_source_head="other-head",
            ),
            rebase_outcome_kind="no_op",
            changed_diff=False,
            review_budget_used=review_budget_used,
        ),
        create_full_review=fake_full_review,
    )

    if review_budget_used:
        assert result.status == "blocked"
        assert result.blocked is not None
        assert result.blocked.reason_code == "bounded-attempt-exhausted"
        assert result.review_budget_used is True
        assert calls == []
    else:
        assert result.status == "created"
        assert result.need == "full"
        assert result.review_budget_used is True
        assert result.review_task is not prior
        assert calls == ["created"]


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
@pytest.mark.parametrize("review_budget_used", (False, True))
def test_post_rebase_review_does_not_reuse_same_head_review_when_carry_forward_proof_is_missing_or_mismatched(
    tmp_path,
    identity: LandingRebaseOutcomeIdentity | None,
    review_budget_used: bool,
) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)
    prior = _completed_full_review(store, impl, head="head-a")
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
            review_budget_used=review_budget_used,
        ),
        create_full_review=fake_full_review,
    )

    if review_budget_used:
        assert result.status == "blocked"
        assert result.blocked is not None
        assert result.blocked.reason_code == "bounded-attempt-exhausted"
        assert result.review_budget_used is True
        assert calls == []
    else:
        assert result.status == "created"
        assert result.need == "full"
        assert result.review_budget_used is True
        assert result.review_task is not prior
        assert calls == ["created"]


@pytest.mark.parametrize("older_status", ("completed", "failed"))
@pytest.mark.parametrize("review_budget_used", (False, True))
def test_post_rebase_review_excludes_stacked_malformed_same_head_historical_reviews(
    tmp_path,
    older_status: str,
    review_budget_used: bool,
) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)
    older = _completed_full_review(
        store,
        impl,
        head="head-a",
        completed_at=datetime(2026, 8, 26, 12, 0, tzinfo=UTC),
    )
    if older_status == "failed":
        older.status = "failed"
        older.output_content = None
        store.update(older)
    newest = _completed_full_review(
        store,
        impl,
        head="head-a",
        completed_at=datetime(2026, 8, 26, 12, 1, tzinfo=UTC),
    )
    newest.output_content = "not a valid review verdict"
    store.update(newest)
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
            rebase_outcome_identity=_rebase_identity(),
            rebase_outcome_kind="mechanical",
            changed_diff=False,
            conflict_resolved=False,
            review_budget_used=review_budget_used,
        ),
        create_full_review=fake_full_review,
    )

    if review_budget_used:
        assert result.status == "blocked"
        assert result.need == "full"
        assert result.review_budget_used is True
        assert result.blocked is not None
        assert result.blocked.reason_code == "bounded-attempt-exhausted"
        assert calls == []
    else:
        assert result.status == "created"
        assert result.need == "full"
        assert result.review_budget_used is True
        assert result.review_task is not None
        assert result.review_task.id not in {older.id, newest.id}
        assert calls == ["created"]


@pytest.mark.parametrize("review_budget_used", (False, True))
def test_post_rebase_review_reuses_unmarked_live_head_full_review_after_distinct_mechanical_rebase(
    tmp_path,
    review_budget_used: bool,
) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)
    live_review = _completed_full_review(store, impl, head="head-b", verdict="APPROVED")
    calls: list[str] = []

    result = acquire_one_post_rebase_review(
        store,
        LandingPostRebaseReviewRequest(
            impl_task=impl,
            source_head="head-b",
            target_head="target-a",
            pre_rebase_source_head="head-a",
            rebase_outcome_identity=_rebase_identity(
                attempted_source_head="old-head-without-eligible-review",
                live_source_head="head-b",
            ),
            rebase_outcome_kind="mechanical",
            changed_diff=False,
            conflict_resolved=False,
            review_budget_used=review_budget_used,
        ),
        create_full_review=lambda *_args, **_kwargs: calls.append("created") or impl,
    )

    assert result.status == "reused_completed"
    assert result.need == "full"
    assert result.review_task == live_review
    assert result.review_budget_used is review_budget_used
    assert calls == []


def test_post_rebase_review_rejects_malformed_unmarked_live_head_full_review_after_distinct_mechanical_rebase(
    tmp_path,
) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)
    live_review = _completed_full_review(store, impl, head="head-b", verdict="APPROVED")
    live_review.output_content = "not a valid review verdict"
    store.update(live_review)
    calls: list[str] = []

    result = acquire_one_post_rebase_review(
        store,
        LandingPostRebaseReviewRequest(
            impl_task=impl,
            source_head="head-b",
            target_head="target-a",
            pre_rebase_source_head="head-a",
            rebase_outcome_identity=_rebase_identity(
                attempted_source_head="old-head-without-eligible-review",
                live_source_head="head-b",
            ),
            rebase_outcome_kind="mechanical",
            changed_diff=False,
            conflict_resolved=False,
            review_budget_used=True,
        ),
        create_full_review=lambda *_args, **_kwargs: calls.append("created") or impl,
    )

    assert result.status == "blocked"
    assert result.need == "full"
    assert result.review_task == live_review
    assert result.review_budget_used is True
    assert result.blocked is not None
    assert result.blocked.reason_code == "required-review-unavailable"
    assert calls == []


def test_post_rebase_review_reuses_current_fallback_epoch_full_review_on_reentry(tmp_path) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)
    _completed_full_review(store, impl, head="head-a")
    created: list[Task] = []

    def fake_full_review(*_args: Any, **_kwargs: Any):
        review = store.add("Created full review", task_type="review", depends_on=impl.id, based_on=impl.id)
        review.status = "pending"
        review.review_verify_head_sha = "head-a"
        store.update(review)
        created.append(review)
        return review

    request = LandingPostRebaseReviewRequest(
        impl_task=impl,
        source_head="head-a",
        target_head="target-a",
        pre_rebase_source_head="head-a",
        rebase_outcome_identity=_rebase_identity(attempted_source_head="old-head"),
        rebase_outcome_kind="mechanical",
        changed_diff=False,
    )
    first = acquire_one_post_rebase_review(store, request, create_full_review=fake_full_review)

    assert first.status == "created"
    assert first.review_task is not None
    assert len(created) == 1

    created_review = first.review_task
    created_review.status = "completed"
    created_review.completed_at = datetime(2026, 8, 26, 12, 2, tzinfo=UTC)
    created_review.output_content = _review_report("APPROVED")
    store.update(created_review)

    second = acquire_one_post_rebase_review(
        store,
        LandingPostRebaseReviewRequest(
            impl_task=impl,
            source_head="head-a",
            target_head="target-a",
            pre_rebase_source_head="head-a",
            rebase_outcome_identity=_rebase_identity(attempted_source_head="old-head"),
            rebase_outcome_kind="mechanical",
            changed_diff=False,
            review_budget_used=first.review_budget_used,
        ),
        create_full_review=fake_full_review,
    )

    assert second.status == "reused_completed"
    assert second.review_task == created_review
    assert second.review_budget_used is True
    assert len(created) == 1


@pytest.mark.parametrize(
    "exc",
    (
        OSError("report file unavailable"),
        RuntimeError("parser exploded"),
    ),
)
def test_mechanical_carry_forward_review_read_failure_blocks_without_creation_or_budget(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    exc: Exception,
) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)
    _completed_full_review(store, impl, head="head-a")
    calls: list[str] = []

    def raise_read_failure(*_args: Any, **_kwargs: Any):
        raise exc

    monkeypatch.setattr(landing_module, "get_review_report", raise_read_failure)

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
        create_full_review=lambda *_args, **_kwargs: calls.append("full") or impl,
        create_resolution_review=lambda *_args, **_kwargs: calls.append("resolution") or impl,
    )

    assert result.status == "blocked"
    assert result.need == "full"
    assert result.review_budget_used is False
    assert result.blocked is not None
    assert result.blocked.reason_code == "required-review-unavailable"
    assert "could not be read" in result.blocked.fact
    assert calls == []


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
    rebase = _landing_rebase(store, impl)
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
    rebase = (
        _landing_rebase(store, impl)
        if provenance_complete
        else store.add("Rebase landing review", task_type="rebase", based_on=impl.id, same_branch=True)
    )
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


@pytest.mark.parametrize("outcome_kind", ("provider_resolved", "recovered", "resumed", None))
@pytest.mark.parametrize("rebase_identity", ("absent", "idless"))
def test_unbindable_complete_resolution_provenance_falls_back_to_one_full_current_head_review(
    tmp_path,
    outcome_kind: str | None,
    rebase_identity: str,
) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)
    rebase = None
    if rebase_identity == "idless":
        rebase = Task(id=None, prompt="ID-less rebase", task_type="rebase", based_on=impl.id, same_branch=True)
    created: list[str] = []

    def fake_full(*_args: Any, **_kwargs: Any):
        created.append("full")
        review = store.add("Full fallback", task_type="review", depends_on=impl.id, based_on=impl.id)
        review.status = "pending"
        review.review_verify_head_sha = "head-a"
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
            changed_diff=None if outcome_kind is None else True,
            conflict_resolved=outcome_kind == "provider_resolved",
            resolution_provenance_complete=True,
        ),
        create_full_review=fake_full,
        create_resolution_review=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("no resolution")),
    )

    assert result.status == "created"
    assert result.need == "full"
    assert result.review_budget_used is True
    assert result.review_task is not None
    assert result.review_task.review_verify_head_sha == "head-a"
    assert created == ["full"]


@pytest.mark.parametrize("outcome_kind", ("provider_resolved", "recovered", "resumed", None))
@pytest.mark.parametrize(
    "rebase_case",
    (
        "missing-row",
        "wrong-task-type",
        "wrong-lineage",
        "missing-provenance",
        "malformed-provenance",
        "incomplete-provenance",
        "mismatched-head",
        "mismatched-target",
    ),
)
def test_nonblank_unbindable_resolution_identity_falls_back_to_full_current_head_review(
    tmp_path,
    outcome_kind: str | None,
    rebase_case: str,
) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)
    rebase = _unbindable_landing_rebase(store, impl, rebase_case)
    created: list[str] = []

    def fake_full(*_args: Any, **_kwargs: Any):
        created.append("full")
        review = store.add("Full fallback", task_type="review", depends_on=impl.id, based_on=impl.id)
        review.status = "pending"
        review.review_verify_head_sha = "head-a"
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
            changed_diff=None if outcome_kind is None else True,
            conflict_resolved=outcome_kind == "provider_resolved",
            resolution_provenance_complete=True,
        ),
        create_full_review=fake_full,
        create_resolution_review=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("no resolution")),
    )

    assert result.status == "created"
    assert result.need == "full"
    assert result.review_budget_used is True
    assert result.review_task is not None
    assert result.review_task.review_verify_head_sha == "head-a"
    assert created == ["full"]


@pytest.mark.parametrize("rebase_status", ("pending", "in_progress", "failed", "stopped"))
def test_noncompleted_resolution_rebase_status_falls_back_to_full_current_head_review(
    tmp_path,
    rebase_status: str,
) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)
    rebase = _landing_rebase(store, impl, status=rebase_status)
    created: list[str] = []

    def fake_full(*_args: Any, **_kwargs: Any):
        created.append("full")
        review = store.add("Full fallback", task_type="review", depends_on=impl.id, based_on=impl.id)
        review.status = "pending"
        review.review_verify_head_sha = "head-a"
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
            changed_diff=True,
            conflict_resolved=True,
            resolution_provenance_complete=True,
        ),
        create_full_review=fake_full,
        create_resolution_review=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("no resolution")),
    )

    assert result.status == "created"
    assert result.need == "full"
    assert result.review_budget_used is True
    assert result.review_task is not None
    assert result.review_task.review_verify_head_sha == "head-a"
    assert created == ["full"]


def test_completed_resolution_rebase_status_keeps_resolution_review_available(tmp_path) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)
    rebase = _landing_rebase(store, impl, status="completed")
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
            pre_rebase_head_sha="pre-head-a",
            pre_rebase_target_sha="pre-target-a",
            pre_rebase_merge_base_sha="merge-base-a",
        )
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
            changed_diff=True,
            conflict_resolved=True,
            resolution_provenance_complete=True,
        ),
        create_resolution_review=fake_resolution,
    )

    assert result.status == "created"
    assert result.need == "resolution"
    assert result.review_budget_used is True
    assert created == ["resolution"]


@pytest.mark.parametrize("rebase_status", ("pending", "in_progress", "failed", "stopped"))
def test_noncompleted_resolution_rebase_status_reuses_exact_full_fallback_with_spent_budget(
    tmp_path,
    rebase_status: str,
) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)
    rebase = _landing_rebase(store, impl, status=rebase_status)
    full = _completed_full_review(store, impl, head="head-a")

    result = acquire_one_post_rebase_review(
        store,
        LandingPostRebaseReviewRequest(
            impl_task=impl,
            rebase_task=rebase,
            source_head="head-a",
            target_head="target-a",
            rebase_outcome_kind="provider_resolved",
            changed_diff=True,
            conflict_resolved=True,
            resolution_provenance_complete=True,
            review_budget_used=True,
        ),
        create_full_review=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("no create")),
        create_resolution_review=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("no resolution")),
    )

    assert result.status == "reused_completed"
    assert result.need == "full"
    assert result.review_task == full
    assert result.review_budget_used is True


@pytest.mark.parametrize("rebase_status", ("pending", "in_progress", "failed", "stopped"))
def test_noncompleted_resolution_rebase_status_blocks_full_fallback_creation_when_budget_spent(
    tmp_path,
    rebase_status: str,
) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)
    rebase = _landing_rebase(store, impl, status=rebase_status)

    result = acquire_one_post_rebase_review(
        store,
        LandingPostRebaseReviewRequest(
            impl_task=impl,
            rebase_task=rebase,
            source_head="head-a",
            target_head="target-a",
            rebase_outcome_kind="provider_resolved",
            changed_diff=True,
            conflict_resolved=True,
            resolution_provenance_complete=True,
            review_budget_used=True,
        ),
        create_full_review=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("budget spent")),
        create_resolution_review=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("no resolution")),
    )

    assert result.status == "blocked"
    assert result.need == "full"
    assert result.review_budget_used is True
    assert result.blocked is not None
    assert result.blocked.reason_code == "bounded-attempt-exhausted"


@pytest.mark.parametrize("outcome_kind", ("provider_resolved", "recovered", "resumed", None))
def test_unbindable_complete_resolution_provenance_reuses_exact_full_fallback_without_spending_again(
    tmp_path,
    outcome_kind: str | None,
) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)
    review = _completed_full_review(store, impl, head="head-a")

    result = acquire_one_post_rebase_review(
        store,
        LandingPostRebaseReviewRequest(
            impl_task=impl,
            source_head="head-a",
            target_head="target-a",
            rebase_outcome_kind=outcome_kind,
            changed_diff=None if outcome_kind is None else True,
            conflict_resolved=outcome_kind == "provider_resolved",
            resolution_provenance_complete=True,
            review_budget_used=True,
        ),
        create_full_review=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("no create")),
        create_resolution_review=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("no resolution")),
    )

    assert result.status == "reused_completed"
    assert result.need == "full"
    assert result.review_task == review
    assert result.review_budget_used is True


@pytest.mark.parametrize("outcome_kind", ("provider_resolved", "recovered", "resumed", None))
@pytest.mark.parametrize(
    "rebase_case",
    (
        "missing-row",
        "wrong-task-type",
        "wrong-lineage",
        "missing-provenance",
        "malformed-provenance",
        "incomplete-provenance",
        "mismatched-head",
        "mismatched-target",
    ),
)
def test_nonblank_unbindable_resolution_identity_reuses_exact_full_fallback_with_spent_budget(
    tmp_path,
    outcome_kind: str | None,
    rebase_case: str,
) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)
    rebase = _unbindable_landing_rebase(store, impl, rebase_case)
    review = _completed_full_review(store, impl, head="head-a")

    result = acquire_one_post_rebase_review(
        store,
        LandingPostRebaseReviewRequest(
            impl_task=impl,
            rebase_task=rebase,
            source_head="head-a",
            target_head="target-a",
            rebase_outcome_kind=outcome_kind,
            changed_diff=None if outcome_kind is None else True,
            conflict_resolved=outcome_kind == "provider_resolved",
            resolution_provenance_complete=True,
            review_budget_used=True,
        ),
        create_full_review=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("no create")),
        create_resolution_review=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("no resolution")),
    )

    assert result.status == "reused_completed"
    assert result.need == "full"
    assert result.review_task == review
    assert result.review_budget_used is True


@pytest.mark.parametrize("outcome_kind", ("provider_resolved", "recovered", "resumed", None))
@pytest.mark.parametrize("rebase_identity", ("absent", "idless"))
def test_unbindable_complete_resolution_provenance_blocks_full_fallback_when_budget_spent(
    tmp_path,
    outcome_kind: str | None,
    rebase_identity: str,
) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)
    rebase = None
    if rebase_identity == "idless":
        rebase = Task(id=None, prompt="ID-less rebase", task_type="rebase", based_on=impl.id, same_branch=True)

    result = acquire_one_post_rebase_review(
        store,
        LandingPostRebaseReviewRequest(
            impl_task=impl,
            rebase_task=rebase,
            source_head="head-a",
            target_head="target-a",
            rebase_outcome_kind=outcome_kind,
            changed_diff=None if outcome_kind is None else True,
            conflict_resolved=outcome_kind == "provider_resolved",
            resolution_provenance_complete=True,
            review_budget_used=True,
        ),
        create_full_review=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("budget spent")),
        create_resolution_review=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("no resolution")),
    )

    assert result.status == "blocked"
    assert result.need == "full"
    assert result.review_budget_used is True
    assert result.blocked is not None
    assert result.blocked.reason_code == "bounded-attempt-exhausted"


@pytest.mark.parametrize("outcome_kind", ("provider_resolved", "recovered", "resumed", None))
@pytest.mark.parametrize(
    "rebase_case",
    (
        "missing-row",
        "wrong-task-type",
        "wrong-lineage",
        "missing-provenance",
        "malformed-provenance",
        "incomplete-provenance",
        "mismatched-head",
        "mismatched-target",
    ),
)
def test_nonblank_unbindable_resolution_identity_blocks_full_fallback_when_budget_spent(
    tmp_path,
    outcome_kind: str | None,
    rebase_case: str,
) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)
    rebase = _unbindable_landing_rebase(store, impl, rebase_case)

    result = acquire_one_post_rebase_review(
        store,
        LandingPostRebaseReviewRequest(
            impl_task=impl,
            rebase_task=rebase,
            source_head="head-a",
            target_head="target-a",
            rebase_outcome_kind=outcome_kind,
            changed_diff=None if outcome_kind is None else True,
            conflict_resolved=outcome_kind == "provider_resolved",
            resolution_provenance_complete=True,
            review_budget_used=True,
        ),
        create_full_review=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("budget spent")),
        create_resolution_review=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("no resolution")),
    )

    assert result.status == "blocked"
    assert result.need == "full"
    assert result.review_budget_used is True
    assert result.blocked is not None
    assert result.blocked.reason_code == "bounded-attempt-exhausted"


@pytest.mark.parametrize("review_status", ("pending", "completed"))
def test_unbindable_resolution_review_scope_is_not_reused(
    tmp_path,
    review_status: str,
) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)
    rebase = _unbindable_landing_rebase(store, impl, "missing-provenance")
    resolution = _resolution_review(
        store,
        impl,
        rebase,
        status=review_status,
        resolved_head="head-a",
        target="target-a",
        verify_head="head-a",
    )
    full = _completed_full_review(store, impl, head="head-a")

    result = acquire_one_post_rebase_review(
        store,
        LandingPostRebaseReviewRequest(
            impl_task=impl,
            rebase_task=rebase,
            source_head="head-a",
            target_head="target-a",
            rebase_outcome_kind="provider_resolved",
            changed_diff=True,
            conflict_resolved=True,
            resolution_provenance_complete=True,
            review_budget_used=True,
        ),
        create_full_review=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("no create")),
        create_resolution_review=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("no resolution")),
    )

    if review_status == "completed":
        assert result.status == "reused_completed"
        assert result.review_task == full
    else:
        assert result.status == "blocked"
        assert result.review_task is None
        assert result.blocked is not None
        assert result.blocked.reason_code == "required-review-unavailable"
    assert result.review_task != resolution


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
    rebase = _landing_rebase(store, impl)
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
    rebase = _landing_rebase(store, impl)
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


@pytest.mark.parametrize("review_status", ("pending", "completed"))
@pytest.mark.parametrize("field", ("head", "target", "merge_base"))
@pytest.mark.parametrize("case", ("missing", "mismatched"))
def test_resolution_reviews_require_exact_pre_rebase_scope_provenance(
    tmp_path,
    review_status: str,
    field: str,
    case: str,
) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)
    rebase = _landing_rebase(store, impl)
    values = {
        "head": "pre-head-a",
        "target": "pre-target-a",
        "merge_base": "merge-base-a",
    }
    values[field] = None if case == "missing" else f"other-{field}"
    stale = _resolution_review(
        store,
        impl,
        rebase,
        status=review_status,
        resolved_head="head-a",
        target="target-a",
        verify_head="head-a",
        pre_rebase_head=values["head"],
        pre_rebase_target=values["target"],
        pre_rebase_merge_base=values["merge_base"],
    )
    created: list[str] = []

    def fake_resolution(*_args: Any, **kwargs: Any):
        created.append("created")
        review = store.add("Replacement resolution review", task_type="review", depends_on=impl.id, based_on=impl.id)
        review.status = "pending"
        review.review_scope = build_resolution_review_scope(
            implementation_task_id=impl.id,
            rebase_task_id=rebase.id,
            resolved_head_sha=kwargs["resolved_head_sha"],
            resolved_target_sha=kwargs["resolved_target_sha"],
            pre_rebase_head_sha="pre-head-a",
            pre_rebase_target_sha="pre-target-a",
            pre_rebase_merge_base_sha="merge-base-a",
        )
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
            changed_diff=True,
            resolution_provenance_complete=True,
        ),
        create_resolution_review=fake_resolution,
    )

    if review_status == "pending":
        assert result.status == "blocked"
        assert result.review_task is None
        assert result.blocked is not None
        assert result.blocked.reason_code == "required-review-unavailable"
        assert stale.id in result.blocked.evidence_refs
        assert created == []
    else:
        assert result.status == "created"
        assert result.review_task != stale
        assert created == ["created"]


def test_completed_resolution_review_reuses_exact_pre_and_post_rebase_scope(tmp_path) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)
    rebase = _landing_rebase(store, impl)
    exact = _resolution_review(
        store,
        impl,
        rebase,
        status="completed",
        resolved_head="head-a",
        target="target-a",
        verify_head="head-a",
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

    assert result.status == "reused_completed"
    assert result.review_task == exact
    assert result.review_budget_used is False


@pytest.mark.parametrize("missing_head", (None, "old-head"))
def test_completed_resolution_review_with_missing_or_mismatched_actual_head_is_not_reused(
    tmp_path,
    missing_head: str | None,
) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)
    rebase = _landing_rebase(store, impl)
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
    rebase = _landing_rebase(store, impl)
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
    rebase = _landing_rebase(store, impl)
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
    assert transition.decision.blocked is not None
    assert transition.decision.blocked.reason_code == "nondeferrable-blocker"
    assert f"review:{review.id}" in transition.decision.blocked.evidence_refs


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
@pytest.mark.parametrize(
    "exc",
    (
        OSError("report file unavailable"),
        RuntimeError("parser exploded"),
    ),
)
def test_exact_completed_post_rebase_review_read_failure_blocks_without_creation_or_budget(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    need: str,
    exc: Exception,
) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)
    rebase = _landing_rebase(store, impl)
    if need == "resolution":
        review = _resolution_review(
            store,
            impl,
            rebase,
            status="completed",
            resolved_head="head-a",
            target="target-a",
            verify_head="head-a",
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
        review = _completed_full_review(store, impl, head="head-a")
        request = LandingPostRebaseReviewRequest(
            impl_task=impl,
            source_head="head-a",
            target_head="target-a",
            changed_diff=True,
            resolution_provenance_complete=False,
        )
    calls: list[str] = []

    def raise_read_failure(*_args: Any, **_kwargs: Any):
        raise exc

    monkeypatch.setattr(landing_module, "get_review_report", raise_read_failure)

    result = acquire_one_post_rebase_review(
        store,
        request,
        create_full_review=lambda *_args, **_kwargs: calls.append("full") or review,
        create_resolution_review=lambda *_args, **_kwargs: calls.append("resolution") or review,
    )

    assert result.status == "blocked"
    assert result.review_task == review
    assert result.review_budget_used is False
    assert result.blocked is not None
    assert result.blocked.reason_code == "required-review-unavailable"
    assert review.id in result.blocked.evidence_refs
    assert "could not be read" in result.blocked.fact
    assert calls == []


@pytest.mark.parametrize("need", ("full", "resolution"))
@pytest.mark.parametrize("terminal_status", ("failed", "stopped"))
def test_latest_exact_terminal_post_rebase_review_blocks_without_second_creation(
    tmp_path,
    need: str,
    terminal_status: str,
) -> None:
    store, impl = _completed_impl_for_landing_review(tmp_path)
    rebase = _landing_rebase(store, impl)
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
        _completed_full_review(store, impl, head="head-a")
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
    assert completed.decision.blocked is not None
    assert completed.decision.blocked.reason_code == "nondeferrable-blocker"
    assert f"review:{first.review_task.id}" in completed.decision.blocked.evidence_refs
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

    first.review_task.status = "completed"
    first.review_task.completed_at = datetime(2026, 8, 26, 12, 1, tzinfo=UTC)
    store.update(first.review_task)

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
