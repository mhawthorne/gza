"""Shared landing request, result, fingerprint, and policy models."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal, cast

LandingPolicyName = Literal["guarded", "strict"]
LandingPhaseName = Literal[
    "resolve",
    "rebase",
    "verify",
    "spec_coherence",
    "post_rebase_review",
    "judge",
    "defer_blockers",
    "merge",
    "post_merge_verify",
]
LandingStepStatus = Literal["pending", "skipped", "conditional", "blocked", "completed"]
LandingReviewMode = Literal["plain_full", "resolution", "spec_coherence", "unknown"]
LandingReviewStatus = Literal["completed", "failed", "pending", "in_progress", "unavailable"]
LandingReviewVerdict = Literal[
    "APPROVED",
    "APPROVED_WITH_FOLLOWUPS",
    "CHANGES_REQUESTED",
    "NEEDS_DISCUSSION",
]
LandingVerifyStatus = Literal["passed", "failed", "stale", "malformed", "unavailable", "missing"]
LandingJudgeVerdict = Literal["LAND", "BLOCK", "NEEDS_HUMAN"]
LandingRebaseStatus = Literal["none", "pending", "in_progress", "completed", "failed", "unavailable"]
LandingRebaseResolutionKind = Literal["none", "mechanical", "provider_resolved", "no_op", "unknown"]
LandingRebaseNoOpSubtype = Literal[
    "already_contained",
    "superseded_contained",
    "unchanged_target",
    "moot",
]
LandingPostMergeVerifyStatus = Literal["failed", "unavailable", "stale", "malformed", "missing"]
LandingPolicyOverride = Literal[
    "parked:review-max-cycles-reached",
    "parked:duplicate-blocker-no-progress",
    "parked:improve-no-op",
    "parked:review-blocker-adjudication-needed",
    "defer-review-blockers",
]
LandingBlockerClass = Literal[
    "adjacent",
    "out_of_scope",
    "correctness",
    "regression",
    "repository_rule",
    "integration_contract",
    "conflict_resolution",
    "spec_coherence",
    "verify_failure",
    "unknown",
]
LandBlockedReasonCode = Literal[
    "identity-proof-unavailable",
    "dirty-checkout",
    "rebase-or-conflict",
    "verify-unavailable-or-red",
    "required-review-unavailable",
    "nondeferrable-blocker",
    "policy-or-judge-refused",
    "materialization-or-persistence-failed",
    "bounded-attempt-exhausted",
    "merge-failed",
]

LandingPolicyReasonCode = Literal[
    "required-review-unavailable",
    "nondeferrable-blocker",
    "policy-or-judge-refused",
]

LANDING_PHASES: tuple[LandingPhaseName, ...] = (
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
LANDING_POLICIES: tuple[LandingPolicyName, ...] = ("guarded", "strict")
LAND_BLOCKED_PRECEDENCE: tuple[LandBlockedReasonCode, ...] = (
    "identity-proof-unavailable",
    "dirty-checkout",
    "rebase-or-conflict",
    "verify-unavailable-or-red",
    "required-review-unavailable",
    "nondeferrable-blocker",
    "policy-or-judge-refused",
    "materialization-or-persistence-failed",
    "bounded-attempt-exhausted",
    "merge-failed",
)
GUARDED_PARK_OVERRIDES: dict[str, LandingPolicyOverride] = {
    "review-max-cycles-reached": "parked:review-max-cycles-reached",
    "duplicate-blocker-no-progress": "parked:duplicate-blocker-no-progress",
    "improve-no-op": "parked:improve-no-op",
}
NONDEFERRABLE_BLOCKER_CLASSES: frozenset[LandingBlockerClass] = frozenset(
    {
        "correctness",
        "regression",
        "repository_rule",
        "integration_contract",
        "conflict_resolution",
        "spec_coherence",
        "verify_failure",
        "unknown",
    }
)


@dataclass(frozen=True)
class LandRequest:
    """Operator request for a future landing coordinator invocation."""

    task_id: str
    policy: LandingPolicyName = "guarded"
    dry_run: bool = False


@dataclass(frozen=True)
class LandBlocked:
    """Typed pre-merge refusal fact selected by landing precedence."""

    reason_code: LandBlockedReasonCode
    fact: str
    evidence_refs: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.evidence_refs:
            raise ValueError("blocked landing result requires durable evidence")

    def terminal_sentence(self, task_id: str) -> str:
        return f"Cannot land {task_id}: {self.fact}."


@dataclass(frozen=True)
class LandPostMergeVerifyFailure:
    """Typed non-success after a merge mutation already occurred."""

    status: LandingPostMergeVerifyStatus
    fact: str
    checkpoint_id: str | None = None
    target_head: str | None = None
    gate_identity: str | None = None
    evidence_refs: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.evidence_refs:
            refs = tuple(
                ref
                for ref in (self.checkpoint_id, self.target_head, self.gate_identity)
                if ref
            )
            object.__setattr__(self, "evidence_refs", refs)
        if not self.evidence_refs:
            raise ValueError("post-merge verification failure requires checkpoint evidence")

    def terminal_sentence(self, task_id: str) -> str:
        return f"Merged {task_id}, but integration verification failed: {self.fact}."


@dataclass(frozen=True)
class LandStep:
    """A queryable or executed landing phase result."""

    phase: LandingPhaseName
    status: LandingStepStatus
    summary: str
    blocked: LandBlocked | None = None
    evidence_refs: tuple[str, ...] = ()


@dataclass(frozen=True)
class LandResult:
    """Shared command/coordinator result shape for landing."""

    request: LandRequest
    owner_task_id: str | None
    target_branch: str | None
    source_ref: str | None
    steps: tuple[LandStep, ...] = ()
    blocked: LandBlocked | None = None
    post_merge_verify_failure: LandPostMergeVerifyFailure | None = None
    merged: bool = False
    already_merged: bool = False
    merge_provenance: Literal["manual_land", "manual_land_escalated"] | None = None
    deferred_task_ids: tuple[str, ...] = ()
    followup_task_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.merged and self.already_merged:
            raise ValueError("merged and already_merged are mutually exclusive terminal states")
        if self.blocked is not None and (self.merged or self.already_merged or self.post_merge_verify_failure):
            raise ValueError("pre-merge blocked result cannot also be a merge terminal state")
        if self.blocked is not None and (
            self.merge_provenance is not None or self.deferred_task_ids or self.followup_task_ids
        ):
            raise ValueError("pre-merge blocked result cannot carry merge success metadata")
        if self.post_merge_verify_failure is not None and not (self.merged or self.already_merged):
            raise ValueError("post-merge verification failure requires a merged terminal state")
        if self.merge_provenance is not None and not self.merged:
            raise ValueError("merge provenance requires merged=True")
        if self.merged and self.merge_provenance is None:
            raise ValueError("newly merged result requires land provenance")
        if self.deferred_task_ids and self.merge_provenance != "manual_land_escalated":
            raise ValueError("deferred blocker task IDs require manual_land_escalated provenance")


@dataclass(frozen=True)
class LandingReviewEvidence:
    """Decision-bearing review evidence after identity/currentness checks."""

    required: bool = True
    status: LandingReviewStatus = "unavailable"
    mode: LandingReviewMode = "unknown"
    verdict: LandingReviewVerdict | None = None
    current: bool = False
    parseable: bool = False
    identity_matched: bool = False
    review_id: str | None = None
    reviewed_head: str | None = None


@dataclass(frozen=True)
class LandingVerifyEvidence:
    """Decision-bearing source verify evidence for the exact live source tree."""

    status: LandingVerifyStatus = "missing"
    current: bool = False
    identity_matched: bool = False
    epoch: str | None = None
    gate_identity: str | None = None
    tree_fingerprint: str | None = None


@dataclass(frozen=True)
class LandingOpenBlocker:
    """A current review blocker and its deterministic deferral classification."""

    finding_id: str
    deferrable: bool
    blocker_class: LandingBlockerClass = "unknown"
    fingerprint: str | None = None
    source: str | None = None


@dataclass(frozen=True)
class LandingSpecCoherenceEvidence:
    """Independent branch-local spec-coherence gate evidence."""

    required: bool = False
    status: LandingReviewStatus = "unavailable"
    verdict: LandingReviewVerdict | None = None
    current: bool = False
    identity_matched: bool = False
    evidence_id: str | None = None
    reviewed_head: str | None = None
    changed_paths_fingerprint: str | None = None


@dataclass(frozen=True)
class LandingPolicyFacts:
    """Injectable facts consumed by the deterministic landing policy evaluator."""

    task_id: str
    merge_unit_state: str = "unmerged"
    representative_status: str | None = None
    has_active_merge_unit: bool = False
    has_local_source: bool = False
    target_matches_checkout: bool = False
    dependency_ready: bool = False
    project_scope_ok: bool = False
    checkout_clean: bool = False
    source_head: str | None = None
    target_head: str | None = None
    clean_merge: bool = False
    ancestry_proof_available: bool = False
    rebase_status: LandingRebaseStatus = "unavailable"
    rebase_resolution_kind: LandingRebaseResolutionKind = "unknown"
    rebase_changed_diff: bool | None = None
    rebase_outcome_id: str | None = None
    rebase_no_op_subtype: LandingRebaseNoOpSubtype | None = None
    rebase_attempted_source_head: str | None = None
    rebase_attempted_target_head: str | None = None
    rebase_target_contained: bool | None = None
    rebase_provider_resolution_proof: bool | None = None
    verify: LandingVerifyEvidence | None = None
    spec_coherence: LandingSpecCoherenceEvidence | None = None
    review: LandingReviewEvidence | None = None
    open_blockers: tuple[LandingOpenBlocker, ...] = ()
    parked_reason: str | None = None
    review_blocker_adjudication_evidence_complete: bool = False
    guarded_judgment_enabled: bool = True
    actionable_lifecycle_work: tuple[str, ...] = ()


@dataclass(frozen=True)
class LandingPolicyDecision:
    """Typed policy outcome with exact allowed overrides or one blocking fact."""

    allowed: bool
    blocked: LandBlocked | None = None
    allowed_overrides: tuple[LandingPolicyOverride, ...] = ()
    judgment_verdict: LandingJudgeVerdict | None = None

    @property
    def reason_code(self) -> LandingPolicyReasonCode | None:
        if self.blocked is None:
            return None
        if self.blocked.reason_code in {
            "required-review-unavailable",
            "nondeferrable-blocker",
            "policy-or-judge-refused",
        }:
            return cast(LandingPolicyReasonCode, self.blocked.reason_code)
        return None

    def __post_init__(self) -> None:
        if self.allowed != (self.blocked is None):
            raise ValueError("landing policy allowed state must exactly match absence of a blocker")
        if not self.allowed and self.allowed_overrides:
            raise ValueError("denied landing policy decisions cannot carry allowed overrides")


@dataclass(frozen=True)
class LandingRebaseFingerprint:
    """Durable rebase outcome identity used in landing non-progress detection."""

    outcome_id: str | None = None
    status: LandingRebaseStatus = "none"
    changed_diff: bool | None = None
    resolution_kind: LandingRebaseResolutionKind = "none"
    no_op_subtype: LandingRebaseNoOpSubtype | None = None
    attempted_source_head: str | None = None
    attempted_target_head: str | None = None
    target_contained: bool | None = None
    provider_resolution_proof: bool | None = None


@dataclass(frozen=True)
class LandingReviewFingerprint:
    """Latest relevant review identity fields."""

    review_id: str | None = None
    verdict: LandingReviewVerdict | None = None
    reviewed_head: str | None = None
    mode: LandingReviewMode = "unknown"


@dataclass(frozen=True)
class LandingVerifyFingerprint:
    """Source verify identity fields."""

    epoch: str | None = None
    verdict: LandingVerifyStatus | None = None
    gate_identity: str | None = None
    tree_fingerprint: str | None = None


@dataclass(frozen=True)
class LandingSpecCoherenceFingerprint:
    """Spec-coherence evidence identity fields relevant to landing."""

    task_or_artifact_id: str | None = None
    status: LandingReviewStatus | None = None
    verdict: LandingReviewVerdict | None = None
    reviewed_head: str | None = None
    changed_paths_fingerprint: str | None = None


@dataclass(frozen=True)
class LandingStateFingerprint:
    """Exact identity of the decision-bearing landing state."""

    merge_unit_state: str
    source_sha: str | None
    target_sha: str | None
    review: LandingReviewFingerprint = LandingReviewFingerprint()
    verify: LandingVerifyFingerprint = LandingVerifyFingerprint()
    rebase: LandingRebaseFingerprint = LandingRebaseFingerprint()
    blocker_fingerprints: tuple[str, ...] = ()
    policy_judgment_identity: str | None = None
    adjudication_fingerprints: tuple[str, ...] = ()
    spec_coherence: LandingSpecCoherenceFingerprint = LandingSpecCoherenceFingerprint()

    @classmethod
    def from_facts(
        cls,
        facts: LandingPolicyFacts,
        *,
        rebase: LandingRebaseFingerprint | None = None,
        policy_judgment_identity: str | None = None,
        adjudication_fingerprints: tuple[str, ...] = (),
        spec_coherence: LandingSpecCoherenceFingerprint | None = None,
    ) -> LandingStateFingerprint:
        review = facts.review
        verify = facts.verify
        resolved_rebase = _select_rebase_fingerprint(facts=facts, supplied=rebase)
        resolved_spec = _select_spec_coherence_fingerprint(facts=facts, supplied=spec_coherence)
        return cls(
            merge_unit_state=facts.merge_unit_state,
            source_sha=facts.source_head,
            target_sha=facts.target_head,
            review=LandingReviewFingerprint(
                review_id=review.review_id if review is not None else None,
                verdict=review.verdict if review is not None else None,
                reviewed_head=review.reviewed_head if review is not None else None,
                mode=review.mode if review is not None else "unknown",
            ),
            verify=LandingVerifyFingerprint(
                epoch=verify.epoch if verify is not None else None,
                verdict=verify.status if verify is not None else None,
                gate_identity=verify.gate_identity if verify is not None else None,
                tree_fingerprint=verify.tree_fingerprint if verify is not None else None,
            ),
            rebase=resolved_rebase,
            blocker_fingerprints=tuple(
                sorted(
                    blocker.fingerprint or f"{blocker.finding_id}:{blocker.blocker_class}:{blocker.deferrable}"
                    for blocker in facts.open_blockers
                )
            ),
            policy_judgment_identity=policy_judgment_identity,
            adjudication_fingerprints=tuple(sorted(adjudication_fingerprints)),
            spec_coherence=resolved_spec,
        )


LandingJudge = Callable[[], LandingJudgeVerdict]


def dry_run_steps_until_boundary(
    *,
    resolved: bool,
    first_execution_required_phase: LandingPhaseName | None,
) -> tuple[LandStep, ...]:
    """Build query-only dry-run phase data without predicting execution outcomes."""

    steps: list[LandStep] = []
    for phase in LANDING_PHASES:
        if phase == "resolve":
            status: LandingStepStatus = "completed" if resolved else "blocked"
            steps.append(LandStep(phase=phase, status=status, summary="resolve current landing identity"))
            if not resolved:
                break
            continue
        if first_execution_required_phase == phase:
            steps.append(
                LandStep(
                    phase=phase,
                    status="conditional",
                    summary="execution required before later outcomes are knowable",
                )
            )
            break
        steps.append(LandStep(phase=phase, status="pending", summary="queryable prerequisite"))
    return tuple(steps)


def evaluate_landing_policy(
    *,
    policy: LandingPolicyName,
    facts: LandingPolicyFacts,
    judge: LandingJudge | None = None,
) -> LandingPolicyDecision:
    """Classify deterministic landing eligibility from supplied facts.

    The evaluator is deliberately fail-closed: missing, stale, malformed, or
    unavailable identity/evidence returns a typed ``LandBlocked`` fact. ``guarded``
    returns exact override tokens only for the park/blocker cases that this contract
    allows; callers never need to parse display text to infer eligibility.
    """

    blocked = _mechanical_blocked_fact(facts)
    if blocked is not None:
        return LandingPolicyDecision(False, blocked=blocked)

    review_availability_block = _review_availability_block(facts)
    if review_availability_block is not None:
        return LandingPolicyDecision(False, blocked=review_availability_block)

    review = facts.review
    nondeferrable = _first_nondeferrable_blocker(facts.open_blockers)
    if review is not None and review.required and review.verdict == "CHANGES_REQUESTED" and nondeferrable is not None:
        return LandingPolicyDecision(
            False,
            blocked=LandBlocked(
                "nondeferrable-blocker",
                f"review blocker {nondeferrable.finding_id} is non-deferable",
                _blocker_evidence(nondeferrable),
            ),
        )

    park_decision = _classify_park(policy, facts)
    if not park_decision.allowed:
        return park_decision

    review_decision = _evaluate_review_policy(
        policy=policy,
        facts=facts,
        judge=judge,
        park_overrides=park_decision.allowed_overrides,
    )
    if not review_decision.allowed:
        return review_decision

    overrides = list(review_decision.allowed_overrides)
    return LandingPolicyDecision(
        True,
        allowed_overrides=tuple(dict.fromkeys(overrides)),
        judgment_verdict=review_decision.judgment_verdict,
    )


def classify_landing_review_policy(
    *,
    policy: LandingPolicyName,
    review: LandingReviewEvidence | None,
    open_blockers: tuple[LandingOpenBlocker, ...] = (),
    guarded_judgment_enabled: bool = True,
    judge: LandingJudge | None = None,
) -> LandingPolicyDecision:
    """Compatibility helper for the review-only landing policy decision."""

    facts = LandingPolicyFacts(
        task_id="unknown",
        has_active_merge_unit=True,
        has_local_source=True,
        target_matches_checkout=True,
        dependency_ready=True,
        project_scope_ok=True,
        checkout_clean=True,
        source_head="source",
        target_head="target",
        clean_merge=True,
        ancestry_proof_available=True,
        representative_status="completed",
        rebase_status="none",
        rebase_resolution_kind="none",
        rebase_target_contained=True,
        verify=LandingVerifyEvidence(
            status="passed",
            current=True,
            identity_matched=True,
            epoch="epoch",
            gate_identity="gate",
            tree_fingerprint="tree",
        ),
        review=review,
        open_blockers=open_blockers,
        guarded_judgment_enabled=guarded_judgment_enabled,
    )
    return _evaluate_review_policy(policy=policy, facts=facts, judge=judge, park_overrides=())


def _evidence_refs(*refs: str | None) -> tuple[str, ...]:
    return tuple(ref for ref in refs if ref)


def _identity_evidence(facts: LandingPolicyFacts) -> tuple[str, ...]:
    return _evidence_refs(facts.task_id, facts.source_head, facts.target_head)


def _verify_evidence(verify: LandingVerifyEvidence | None, facts: LandingPolicyFacts) -> tuple[str, ...]:
    if verify is None:
        return _identity_evidence(facts)
    return _evidence_refs(facts.task_id, verify.epoch, verify.gate_identity, verify.tree_fingerprint, facts.source_head)


def _review_evidence(review: LandingReviewEvidence | None, facts: LandingPolicyFacts) -> tuple[str, ...]:
    if review is None:
        return _identity_evidence(facts)
    return _evidence_refs(facts.task_id, review.review_id, review.reviewed_head, facts.source_head)


def _spec_coherence_evidence(
    spec_coherence: LandingSpecCoherenceEvidence | None,
    facts: LandingPolicyFacts,
) -> tuple[str, ...]:
    if spec_coherence is None:
        return _identity_evidence(facts)
    return _evidence_refs(
        facts.task_id,
        spec_coherence.evidence_id,
        spec_coherence.reviewed_head,
        spec_coherence.changed_paths_fingerprint,
        facts.source_head,
    )


def _rebase_evidence(facts: LandingPolicyFacts) -> tuple[str, ...]:
    return _evidence_refs(
        facts.task_id,
        facts.rebase_outcome_id,
        facts.rebase_attempted_source_head,
        facts.rebase_attempted_target_head,
        facts.source_head,
        facts.target_head,
    )


def _rebase_fingerprint_from_facts(facts: LandingPolicyFacts) -> LandingRebaseFingerprint:
    return LandingRebaseFingerprint(
        outcome_id=facts.rebase_outcome_id,
        status=facts.rebase_status,
        changed_diff=facts.rebase_changed_diff,
        resolution_kind=facts.rebase_resolution_kind,
        no_op_subtype=facts.rebase_no_op_subtype,
        attempted_source_head=facts.rebase_attempted_source_head,
        attempted_target_head=facts.rebase_attempted_target_head,
        target_contained=facts.rebase_target_contained,
        provider_resolution_proof=facts.rebase_provider_resolution_proof,
    )


def _spec_coherence_fingerprint_from_facts(
    facts: LandingPolicyFacts,
) -> LandingSpecCoherenceFingerprint:
    spec = facts.spec_coherence
    if spec is None:
        return LandingSpecCoherenceFingerprint()
    return LandingSpecCoherenceFingerprint(
        task_or_artifact_id=spec.evidence_id,
        status=spec.status,
        verdict=spec.verdict,
        reviewed_head=spec.reviewed_head,
        changed_paths_fingerprint=spec.changed_paths_fingerprint,
    )


def _select_rebase_fingerprint(
    *,
    facts: LandingPolicyFacts,
    supplied: LandingRebaseFingerprint | None,
) -> LandingRebaseFingerprint:
    if supplied is None:
        return _rebase_fingerprint_from_facts(facts)
    if _facts_have_rebase_identity(facts):
        expected = _rebase_fingerprint_from_facts(facts)
        if supplied != expected:
            raise ValueError("supplied rebase fingerprint contradicts facts rebase evidence")
    return supplied


def _select_spec_coherence_fingerprint(
    *,
    facts: LandingPolicyFacts,
    supplied: LandingSpecCoherenceFingerprint | None,
) -> LandingSpecCoherenceFingerprint:
    if supplied is None:
        return _spec_coherence_fingerprint_from_facts(facts)
    if facts.spec_coherence is not None:
        expected = _spec_coherence_fingerprint_from_facts(facts)
        if supplied != expected:
            raise ValueError("supplied spec-coherence fingerprint contradicts facts evidence")
    return supplied


def _facts_have_rebase_identity(facts: LandingPolicyFacts) -> bool:
    default_facts = LandingPolicyFacts(task_id=facts.task_id)
    return any(
        (
            facts.rebase_outcome_id != default_facts.rebase_outcome_id,
            facts.rebase_status != default_facts.rebase_status,
            facts.rebase_resolution_kind != default_facts.rebase_resolution_kind,
            facts.rebase_changed_diff != default_facts.rebase_changed_diff,
            facts.rebase_no_op_subtype != default_facts.rebase_no_op_subtype,
            facts.rebase_attempted_source_head != default_facts.rebase_attempted_source_head,
            facts.rebase_attempted_target_head != default_facts.rebase_attempted_target_head,
            facts.rebase_target_contained != default_facts.rebase_target_contained,
            facts.rebase_provider_resolution_proof != default_facts.rebase_provider_resolution_proof,
        )
    )


def _mechanical_blocked_fact(facts: LandingPolicyFacts) -> LandBlocked | None:
    if (
        not facts.has_active_merge_unit
        or facts.merge_unit_state != "unmerged"
        or facts.representative_status not in {"completed", "unmerged"}
        or not facts.has_local_source
        or not facts.target_matches_checkout
        or not facts.dependency_ready
        or not facts.project_scope_ok
        or not facts.source_head
        or not facts.target_head
    ):
        return LandBlocked(
            "identity-proof-unavailable",
            "landing identity, dependency, scope, source, or target proof is unavailable",
            _identity_evidence(facts),
        )
    lifecycle_identity_block = _actionable_lifecycle_work_blocked_fact(
        facts,
        reason_codes=("identity-proof-unavailable",),
    )
    if lifecycle_identity_block is not None:
        return lifecycle_identity_block
    if not facts.checkout_clean:
        return LandBlocked("dirty-checkout", "tracked checkout is not clean", _identity_evidence(facts))
    lifecycle_rebase_block = _actionable_lifecycle_work_blocked_fact(
        facts,
        reason_codes=("rebase-or-conflict",),
    )
    if lifecycle_rebase_block is not None:
        return lifecycle_rebase_block
    if (
        not facts.ancestry_proof_available
        or not facts.clean_merge
        or not _rebase_state_consistent(facts)
    ):
        return LandBlocked(
            "rebase-or-conflict",
            "rebase, ancestry, or clean-merge proof is unavailable",
            _rebase_evidence(facts),
        )
    lifecycle_verify_block = _actionable_lifecycle_work_blocked_fact(
        facts,
        reason_codes=("verify-unavailable-or-red",),
    )
    if lifecycle_verify_block is not None:
        return lifecycle_verify_block
    verify = facts.verify
    if (
        verify is None
        or verify.status != "passed"
        or not verify.current
        or not verify.identity_matched
        or not verify.epoch
        or not verify.gate_identity
        or not verify.tree_fingerprint
    ):
        return LandBlocked(
            "verify-unavailable-or-red",
            "current green source verify evidence is unavailable",
            _verify_evidence(verify, facts),
        )
    spec = facts.spec_coherence
    if spec is not None and spec.required:
        if (
            spec.status != "completed"
            or spec.verdict != "APPROVED"
            or not spec.current
            or not spec.identity_matched
            or not spec.evidence_id
            or spec.reviewed_head != facts.source_head
            or not spec.changed_paths_fingerprint
        ):
            return LandBlocked(
                "required-review-unavailable",
                "required spec-coherence evidence is unavailable",
                _spec_coherence_evidence(spec, facts),
            )
    lifecycle_review_block = _actionable_lifecycle_work_blocked_fact(
        facts,
        reason_codes=("required-review-unavailable",),
    )
    if lifecycle_review_block is not None:
        return lifecycle_review_block
    return None


def _actionable_lifecycle_work_blocked_fact(
    facts: LandingPolicyFacts,
    *,
    reason_codes: tuple[LandBlockedReasonCode, ...],
) -> LandBlocked | None:
    for work in facts.actionable_lifecycle_work:
        reason_code = _actionable_lifecycle_work_reason_code(work)
        if reason_code not in reason_codes:
            continue
        fact = _actionable_lifecycle_work_fact(work, reason_code)
        return LandBlocked(reason_code, fact, (facts.task_id, work))
    return None


def _actionable_lifecycle_work_reason_code(work: str) -> LandBlockedReasonCode:
    normalized = work.strip().lower().replace("_", "-")
    phase = normalized.split(":", 1)[0]
    if phase in {"rebase", "needs-rebase"}:
        return "rebase-or-conflict"
    if phase in {"verify", "verify-fix", "run-verify", "create-verify-fix"}:
        return "verify-unavailable-or-red"
    if phase in {
        "review",
        "code-review",
        "resolution-review",
        "post-rebase-review",
        "spec-coherence",
        "spec-coherence-review",
    }:
        return "required-review-unavailable"
    return "identity-proof-unavailable"


def _actionable_lifecycle_work_fact(
    work: str,
    reason_code: LandBlockedReasonCode,
) -> str:
    if reason_code == "identity-proof-unavailable":
        return f"active lifecycle work identity is unavailable or mismatched: {work}"
    if reason_code == "rebase-or-conflict":
        return f"required rebase work remains: {work}"
    if reason_code == "verify-unavailable-or-red":
        return f"required verify work remains: {work}"
    if reason_code == "required-review-unavailable":
        return f"required review work remains: {work}"
    return f"actionable lifecycle work remains: {work}"


def _rebase_state_consistent(facts: LandingPolicyFacts) -> bool:
    status = facts.rebase_status
    kind = facts.rebase_resolution_kind
    if status in {"pending", "in_progress", "failed", "unavailable"}:
        return False
    if status == "none":
        return kind == "none" and facts.rebase_target_contained is True
    if status != "completed":
        return False
    if kind == "mechanical":
        return _valid_completed_rebase_proof(facts, require_provider_proof=False)
    if kind == "provider_resolved":
        return _valid_completed_rebase_proof(facts, require_provider_proof=True)
    if kind == "no_op":
        return _valid_no_op_rebase_proof(facts)
    return False


def _valid_completed_rebase_proof(
    facts: LandingPolicyFacts,
    *,
    require_provider_proof: bool,
) -> bool:
    if not facts.rebase_outcome_id:
        return False
    if not facts.rebase_attempted_source_head:
        return False
    if not facts.rebase_attempted_target_head or facts.rebase_attempted_target_head != facts.target_head:
        return False
    if facts.rebase_target_contained is not True:
        return False
    if require_provider_proof:
        return facts.rebase_provider_resolution_proof is True
    return facts.rebase_provider_resolution_proof is not True


def _valid_no_op_rebase_proof(facts: LandingPolicyFacts) -> bool:
    if not facts.rebase_outcome_id:
        return False
    if facts.rebase_no_op_subtype not in {
        "already_contained",
        "superseded_contained",
        "unchanged_target",
        "moot",
    }:
        return False
    if facts.rebase_provider_resolution_proof is not False:
        return False
    if facts.rebase_target_contained is not True:
        return False
    if not facts.rebase_attempted_target_head or facts.rebase_attempted_target_head != facts.target_head:
        return False
    if facts.rebase_no_op_subtype == "superseded_contained":
        return bool(facts.rebase_attempted_source_head)
    return (
        facts.rebase_changed_diff is False
        and bool(facts.rebase_attempted_source_head)
        and facts.rebase_attempted_source_head == facts.source_head
    )


def _evaluate_review_policy(
    *,
    policy: LandingPolicyName,
    facts: LandingPolicyFacts,
    judge: LandingJudge | None,
    park_overrides: tuple[LandingPolicyOverride, ...],
) -> LandingPolicyDecision:
    review = facts.review
    availability_block = _review_availability_block(facts)
    if availability_block is not None:
        return LandingPolicyDecision(False, blocked=availability_block)
    assert review is not None
    if not review.required:
        if facts.open_blockers or park_overrides:
            return LandingPolicyDecision(
                False,
                blocked=LandBlocked(
                    "policy-or-judge-refused",
                    "review-disabled landing cannot use guarded escalation",
                    _review_evidence(review, facts),
                ),
        )
        return LandingPolicyDecision(True)
    if review.verdict in {"APPROVED", "APPROVED_WITH_FOLLOWUPS"}:
        if facts.open_blockers:
            blocker = facts.open_blockers[0]
            return LandingPolicyDecision(
                False,
                blocked=LandBlocked(
                    "nondeferrable-blocker",
                    f"review blocker {blocker.finding_id} contradicts merge-permitting review",
                    _blocker_evidence(blocker),
                ),
            )
        if park_overrides:
            return _judge_for_overrides(facts=facts, judge=judge, allowed_overrides=park_overrides)
        return LandingPolicyDecision(True)
    if review.verdict != "CHANGES_REQUESTED" or review.mode not in {"plain_full", "resolution"}:
        return LandingPolicyDecision(
            False,
            blocked=LandBlocked(
                "required-review-unavailable",
                "required review evidence is unavailable",
                _review_evidence(review, facts),
            ),
        )
    if policy == "strict":
        return LandingPolicyDecision(
            False,
            blocked=LandBlocked(
                "nondeferrable-blocker",
                "strict policy refuses open review blockers",
                _review_evidence(review, facts),
            ),
        )

    nondeferrable = _first_nondeferrable_blocker(facts.open_blockers)
    if nondeferrable is not None:
        return LandingPolicyDecision(
            False,
            blocked=LandBlocked(
                "nondeferrable-blocker",
                f"review blocker {nondeferrable.finding_id} is non-deferable",
                _blocker_evidence(nondeferrable),
            ),
        )
    if not facts.open_blockers:
        return LandingPolicyDecision(
            False,
            blocked=LandBlocked(
                "nondeferrable-blocker",
                "changes-requested review has no blocker evidence",
                _review_evidence(review, facts),
            ),
        )
    overrides: tuple[LandingPolicyOverride, ...] = ("defer-review-blockers",)
    overrides = tuple(dict.fromkeys((*overrides, *park_overrides)))
    return _judge_for_overrides(facts=facts, judge=judge, allowed_overrides=overrides)


def _review_availability_block(facts: LandingPolicyFacts) -> LandBlocked | None:
    review = facts.review
    if review is None:
        return LandBlocked(
            "required-review-unavailable",
            "required review evidence is unavailable",
            _review_evidence(review, facts),
        )
    if not review.required:
        return None
    if (
        review.status != "completed"
        or not review.current
        or not review.parseable
        or not review.identity_matched
        or not review.review_id
        or review.reviewed_head != facts.source_head
        or review.verdict is None
        or review.verdict == "NEEDS_DISCUSSION"
        or review.mode == "unknown"
    ):
        return LandBlocked(
            "required-review-unavailable",
            "required review evidence is unavailable",
            _review_evidence(review, facts),
        )
    if review.mode == "spec_coherence":
        return LandBlocked(
            "required-review-unavailable",
            "spec-coherence review cannot satisfy code-review landing",
            _review_evidence(review, facts),
        )
    return None


def _judge_for_overrides(
    *,
    facts: LandingPolicyFacts,
    judge: LandingJudge | None,
    allowed_overrides: tuple[LandingPolicyOverride, ...],
) -> LandingPolicyDecision:
    if not facts.guarded_judgment_enabled or judge is None:
        return LandingPolicyDecision(
            False,
            blocked=LandBlocked(
                "policy-or-judge-refused",
                "guarded landing judgment is unavailable",
                _identity_evidence(facts),
            ),
        )
    judgment = judge()
    if judgment != "LAND":
        return LandingPolicyDecision(
            False,
            blocked=LandBlocked(
                "policy-or-judge-refused",
                "guarded landing judgment refused landing",
                _identity_evidence(facts),
            ),
            judgment_verdict=judgment,
        )
    return LandingPolicyDecision(
        True,
        allowed_overrides=allowed_overrides,
        judgment_verdict=judgment,
    )


def _classify_park(
    policy: LandingPolicyName,
    facts: LandingPolicyFacts,
) -> LandingPolicyDecision:
    if not facts.parked_reason:
        return LandingPolicyDecision(True)
    if policy == "strict":
        return LandingPolicyDecision(
            False,
            blocked=LandBlocked(
                "nondeferrable-blocker",
                "strict policy refuses parked-state overrides",
                _evidence_refs(facts.task_id, facts.parked_reason),
            ),
        )
    if facts.parked_reason == "review-blocker-adjudication-needed":
        if facts.review_blocker_adjudication_evidence_complete:
            return LandingPolicyDecision(True, allowed_overrides=("parked:review-blocker-adjudication-needed",))
        return LandingPolicyDecision(
            False,
            blocked=LandBlocked(
                "policy-or-judge-refused",
                "review-blocker adjudication evidence is incomplete",
                _evidence_refs(facts.task_id, facts.parked_reason),
            ),
        )
    override = GUARDED_PARK_OVERRIDES.get(facts.parked_reason)
    if override is not None:
        return LandingPolicyDecision(True, allowed_overrides=(override,))
    return LandingPolicyDecision(
        False,
        blocked=LandBlocked(
            "nondeferrable-blocker",
            f"parked reason {facts.parked_reason} is not deferrable",
            _evidence_refs(facts.task_id, facts.parked_reason),
        ),
    )


def _first_nondeferrable_blocker(
    blockers: tuple[LandingOpenBlocker, ...],
) -> LandingOpenBlocker | None:
    for blocker in blockers:
        if not blocker.deferrable or blocker.blocker_class in NONDEFERRABLE_BLOCKER_CLASSES:
            return blocker
    return None


def _blocker_evidence(blocker: LandingOpenBlocker) -> tuple[str, ...]:
    refs = [blocker.finding_id]
    if blocker.source:
        refs.append(blocker.source)
    if blocker.fingerprint:
        refs.append(blocker.fingerprint)
    return tuple(refs)
