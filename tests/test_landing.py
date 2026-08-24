from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from gza.landing import (
    LANDING_PHASES,
    LandingOpenBlocker,
    LandingJudgment,
    LandingJudgeVerdict,
    LandingPolicyDecision,
    LandingPolicyFacts,
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
    dry_run_steps_until_boundary,
    evaluate_landing_policy,
)


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
        "tree_fingerprint": "tree-a",
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
