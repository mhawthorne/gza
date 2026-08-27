"""Shared landing request, result, fingerprint, and policy models."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Literal, Protocol, cast

from gza.advance_engine import plan_manual_verify_gate_action
from gza.cli.advance_executor import (
    AdvanceActionExecutionContext,
    AdvanceActionExecutionResult,
    execute_advance_action,
)
from gza.db import SqliteTaskStore, Task as DbTask
from gza.review_tasks import DuplicateReviewError, create_resolution_review_task, create_review_task
from gza.review_verdict import get_review_report
from gza.review_verify_state import VerifyGateDecision, resolve_verify_gate_decision
from gza.runner import _compute_tree_fingerprint
from gza.watch_progress import review_matches_create_review_action

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
LandingVerifyAcquisitionStatus = Literal["current_green", "ran_verify", "blocked"]
LandingPostRebaseReviewStatus = Literal[
    "not_required",
    "reused_completed",
    "pending",
    "in_progress",
    "created",
    "blocked",
]
LandingPostRebaseReviewNeed = Literal["none", "resolution", "full"]
LandingRebaseOutcomeKind = Literal[
    "mechanical",
    "no_op",
    "provider_resolved",
    "recovered",
    "resumed",
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


def _normalize_optional_identity(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


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
        object.__setattr__(self, "evidence_refs", _normalize_evidence_refs(self.evidence_refs))
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
            refs = tuple(ref for ref in (self.checkpoint_id, self.target_head, self.gate_identity) if ref)
            object.__setattr__(self, "evidence_refs", refs)
        object.__setattr__(self, "evidence_refs", _normalize_evidence_refs(self.evidence_refs))
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

    def __post_init__(self) -> None:
        object.__setattr__(self, "evidence_refs", _normalize_evidence_refs(self.evidence_refs))


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
    judgment_artifact_id: str | None = None
    judgment_key: str | None = None
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
        judgment_refs = _normalize_evidence_refs((self.judgment_artifact_id, self.judgment_key))
        if self.merge_provenance == "manual_land_escalated" and len(judgment_refs) != 2:
            raise ValueError("manual_land_escalated result requires judgment artifact and key")
        if self.merge_provenance != "manual_land_escalated" and judgment_refs:
            raise ValueError("landing judgment identity requires manual_land_escalated provenance")
        object.__setattr__(self, "judgment_artifact_id", judgment_refs[0] if judgment_refs else None)
        object.__setattr__(self, "judgment_key", judgment_refs[1] if judgment_refs else None)


@dataclass(frozen=True)
class LandingFollowupFinding:
    """A normalized FOLLOWUP finding that must be materialized before merge."""

    finding_id: str
    fingerprint: str | None = None
    source: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "finding_id", self.finding_id.strip())
        object.__setattr__(self, "fingerprint", _normalize_optional_identity(self.fingerprint))
        object.__setattr__(self, "source", _normalize_optional_identity(self.source))


@dataclass(frozen=True)
class LandingFollowupMaterializationIdentity:
    """Exact durable lookup identity for a mandatory FOLLOWUP finding."""

    review_id: str
    finding_id: str
    source: str
    fingerprint: str | None = None

    def __post_init__(self) -> None:
        normalized_review_id = self.review_id.strip()
        normalized_finding_id = self.finding_id.strip()
        normalized_source = self.source.strip()
        normalized_fingerprint = _normalize_optional_identity(self.fingerprint)
        if not normalized_review_id:
            raise ValueError("follow-up materialization identity requires a review ID")
        if not normalized_finding_id:
            raise ValueError("follow-up materialization identity requires a finding ID")
        if not normalized_source:
            raise ValueError("follow-up materialization identity requires a source identity")
        if not normalized_fingerprint:
            raise ValueError("follow-up materialization identity requires a normalized content fingerprint")
        object.__setattr__(self, "review_id", normalized_review_id)
        object.__setattr__(self, "finding_id", normalized_finding_id)
        object.__setattr__(self, "source", normalized_source)
        object.__setattr__(self, "fingerprint", normalized_fingerprint)

    @property
    def durable_key(self) -> tuple[str, str]:
        return (self.review_id, self.finding_id)

    @property
    def fingerprint_key(self) -> str:
        return json.dumps(
            {
                "review": self.review_id,
                "source": self.source,
                "finding": self.finding_id,
                "content": self.fingerprint,
            },
            sort_keys=True,
            separators=(",", ":"),
        )


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
    followup_findings: tuple[LandingFollowupFinding, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "review_id", _normalize_optional_identity(self.review_id))
        object.__setattr__(self, "reviewed_head", _normalize_optional_identity(self.reviewed_head))


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
class LandingVerifyAcquisitionResult:
    """Landing-specific lifecycle verify acquisition outcome."""

    status: LandingVerifyAcquisitionStatus
    evidence: LandingVerifyEvidence
    action: dict[str, Any] | None = None
    execution: AdvanceActionExecutionResult | None = None
    blocked: LandBlocked | None = None

    def __post_init__(self) -> None:
        if self.status == "blocked" and self.blocked is None:
            raise ValueError("blocked verify acquisition requires a landing block")
        if self.status != "blocked" and self.blocked is not None:
            raise ValueError("non-blocked verify acquisition cannot carry a landing block")
        if self.status == "current_green" and self.action is not None:
            raise ValueError("current-green verify acquisition should not carry an executed action")
        if self.status == "ran_verify" and self.execution is None:
            raise ValueError("ran verify acquisition requires execution evidence")


@dataclass(frozen=True)
class LandingOpenBlocker:
    """A current review blocker and its deterministic deferral classification."""

    finding_id: str
    deferrable: bool
    blocker_class: LandingBlockerClass = "unknown"
    fingerprint: str | None = None
    source: str | None = None

    def __post_init__(self) -> None:
        finding_id = _normalize_required_ref(self.finding_id, "blocker finding ID")
        source = _normalize_required_ref(self.source, "blocker durable source")
        fingerprint = _normalize_required_ref(self.fingerprint, "blocker normalized fingerprint")
        object.__setattr__(self, "finding_id", finding_id)
        object.__setattr__(self, "source", source)
        object.__setattr__(self, "fingerprint", fingerprint)


@dataclass(frozen=True)
class LandingPostRebaseReviewRequest:
    """Inputs for landing's one-shot post-rebase review acquisition."""

    impl_task: DbTask
    source_head: str
    target_head: str
    pre_rebase_source_head: str | None = None
    rebase_task: DbTask | None = None
    rebase_outcome_identity: LandingRebaseOutcomeIdentity | None = None
    rebase_outcome_kind: LandingRebaseOutcomeKind | str | None = None
    changed_diff: bool | None = None
    conflict_resolved: bool = False
    resolution_provenance_complete: bool = True
    review_budget_used: bool = False
    trigger_source: str = "manual_land"


@dataclass(frozen=True)
class LandingPostRebaseReviewResult:
    """Landing-specific post-rebase review acquisition outcome."""

    status: LandingPostRebaseReviewStatus
    need: LandingPostRebaseReviewNeed
    review_task: DbTask | None = None
    action: dict[str, Any] | None = None
    review_budget_used: bool = False
    blocked: LandBlocked | None = None

    def __post_init__(self) -> None:
        if self.status == "blocked" and self.blocked is None:
            raise ValueError("blocked review acquisition requires a landing block")
        if self.status != "blocked" and self.blocked is not None:
            raise ValueError("non-blocked review acquisition cannot carry a landing block")
        if self.status in {"pending", "in_progress", "created", "reused_completed"} and self.review_task is None:
            raise ValueError("review-bearing acquisition status requires a review task")


@dataclass(frozen=True)
class LandingPostRebaseReviewTransition:
    """Result of landing's post-rebase review acquisition and policy transition."""

    review_result: LandingPostRebaseReviewResult
    decision: LandingPolicyDecision


def run_landing_post_rebase_review_transition(
    store: SqliteTaskStore,
    request: LandingPostRebaseReviewRequest,
    *,
    policy: LandingPolicyName,
    facts: LandingPolicyFacts,
    config: Any | None = None,
    judge: LandingJudge | None = None,
    create_full_review: Callable[..., DbTask] = create_review_task,
    create_resolution_review: Callable[..., DbTask] = create_resolution_review_task,
) -> LandingPostRebaseReviewTransition:
    """Acquire landing's one post-rebase review, then evaluate landing policy.

    This is the production transition between post-rebase review acquisition and
    guarded/strict landing policy. ``CHANGES_REQUESTED`` review evidence flows
    directly to the landing policy decision; this path does not dispatch improve
    work or start a second review after a completed exact review is available.
    """

    result = acquire_one_post_rebase_review(
        store,
        request,
        config=config,
        create_full_review=create_full_review,
        create_resolution_review=create_resolution_review,
    )
    decision = _consume_landing_post_rebase_review_result(
        result,
        policy=policy,
        facts=facts,
        config=config,
        judge=judge,
    )
    return LandingPostRebaseReviewTransition(result, decision)


def _consume_landing_post_rebase_review_result(
    result: LandingPostRebaseReviewResult,
    *,
    policy: LandingPolicyName,
    facts: LandingPolicyFacts,
    config: Any | None = None,
    judge: LandingJudge | None = None,
) -> LandingPolicyDecision:
    """Consume landing's one-shot post-rebase review result.

    This is the landing-specific transition seam between acquiring a
    post-rebase review and evaluating the landing policy. In particular,
    ``CHANGES_REQUESTED`` review evidence flows to the guarded/strict landing
    decision rather than back into the generic improve or review loop.
    """

    if result.status == "blocked":
        assert result.blocked is not None
        return LandingPolicyDecision(False, blocked=result.blocked)
    if result.status in {"created", "pending", "in_progress"}:
        return LandingPolicyDecision(
            False,
            blocked=LandBlocked(
                "required-review-unavailable",
                "post-rebase review has not completed",
                _evidence_refs(
                    result.review_task.id if result.review_task is not None else None,
                    facts.task_id,
                    facts.source_head,
                ),
            ),
        )

    policy_facts = facts
    if result.status == "reused_completed":
        assert result.review_task is not None
        project_dir = Path(getattr(config, "project_dir", Path.cwd()))
        report = get_review_report(project_dir, result.review_task)
        if report.verdict not in {"APPROVED", "APPROVED_WITH_FOLLOWUPS", "CHANGES_REQUESTED", "NEEDS_DISCUSSION"}:
            return LandingPolicyDecision(
                False,
                blocked=LandBlocked(
                    "required-review-unavailable",
                    "post-rebase review evidence is malformed or not merge-decision bearing",
                    _evidence_refs(result.review_task.id, facts.source_head),
                ),
            )
        mode: LandingReviewMode = "resolution" if result.need == "resolution" else "plain_full"
        if facts.review is not None:
            review = replace(
                facts.review,
                status="completed",
                mode=facts.review.mode if facts.review.mode != "unknown" else mode,
                verdict=cast(LandingReviewVerdict, report.verdict),
                current=True,
                parseable=True,
                identity_matched=True,
                review_id=result.review_task.id,
                reviewed_head=result.review_task.review_verify_head_sha or facts.source_head,
            )
        else:
            review = LandingReviewEvidence(
                required=True,
                status="completed",
                mode=mode,
                verdict=cast(LandingReviewVerdict, report.verdict),
                current=True,
                parseable=True,
                identity_matched=True,
                review_id=result.review_task.id,
                reviewed_head=result.review_task.review_verify_head_sha or facts.source_head,
            )
        policy_facts = replace(facts, review=review)

    return evaluate_landing_policy(policy=policy, facts=policy_facts, judge=judge)


class LandingCreateReviewResult(Protocol):
    status: str
    review_task: DbTask | None
    message: str


@dataclass(frozen=True)
class LandingRebaseOutcomeIdentity:
    """Structured durable rebase outcome proof used before review carry-forward."""

    outcome_id: str
    outcome_kind: LandingRebaseOutcomeKind | str
    attempted_source_head: str
    attempted_target_head: str
    live_source_head: str
    live_target_head: str
    target_contained: bool
    provider_resolution_proof: bool
    changed_diff: bool | None
    no_op_subtype: LandingRebaseNoOpSubtype | str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "outcome_id", _normalize_required_ref(self.outcome_id, "rebase outcome ID"))
        object.__setattr__(
            self,
            "attempted_source_head",
            _normalize_required_ref(self.attempted_source_head, "rebase attempted source head"),
        )
        object.__setattr__(
            self,
            "attempted_target_head",
            _normalize_required_ref(self.attempted_target_head, "rebase attempted target head"),
        )
        object.__setattr__(
            self,
            "live_source_head",
            _normalize_required_ref(self.live_source_head, "rebase live source head"),
        )
        object.__setattr__(
            self,
            "live_target_head",
            _normalize_required_ref(self.live_target_head, "rebase live target head"),
        )


LandingAdvanceExecutor = Callable[
    [DbTask, dict[str, Any], AdvanceActionExecutionContext],
    AdvanceActionExecutionResult,
]


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
    judgment_artifact_id: str | None = None
    judgment_key: str | None = None
    followup_materialization_identities: tuple[LandingFollowupMaterializationIdentity, ...] = ()

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
        judgment_refs = _normalize_evidence_refs((self.judgment_artifact_id, self.judgment_key))
        if self.allowed_overrides:
            if self.judgment_verdict != "LAND" or len(judgment_refs) != 2:
                raise ValueError("override-bearing landing policy decisions require validated LAND judgment identity")
        elif judgment_refs:
            raise ValueError("landing judgment identity requires an allowed override")
        object.__setattr__(self, "judgment_artifact_id", judgment_refs[0] if judgment_refs else None)
        object.__setattr__(self, "judgment_key", judgment_refs[1] if judgment_refs else None)
        if not self.allowed and self.followup_materialization_identities:
            raise ValueError("denied landing policy decisions cannot carry follow-up materialization inputs")


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
    followup_fingerprints: tuple[str, ...] = ()


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
                followup_fingerprints=tuple(
                    identity.fingerprint_key
                    for identity in _followup_materialization_identities(review)
                )
                if review is not None
                else (),
            ),
            verify=LandingVerifyFingerprint(
                epoch=verify.epoch if verify is not None else None,
                verdict=verify.status if verify is not None else None,
                gate_identity=verify.gate_identity if verify is not None else None,
                tree_fingerprint=verify.tree_fingerprint if verify is not None else None,
            ),
            rebase=resolved_rebase,
            blocker_fingerprints=tuple(sorted(_blocker_fingerprint(blocker) for blocker in facts.open_blockers)),
            policy_judgment_identity=policy_judgment_identity,
            adjudication_fingerprints=tuple(sorted(adjudication_fingerprints)),
            spec_coherence=resolved_spec,
        )


@dataclass(frozen=True)
class LandingJudgment:
    """Validated durable landing judgment returned by the semantic judge."""

    verdict: LandingJudgeVerdict
    artifact_id: str | None = None
    key: str | None = None

    def __post_init__(self) -> None:
        judgment_refs = _normalize_evidence_refs((self.artifact_id, self.key))
        if self.verdict == "LAND" and len(judgment_refs) != 2:
            raise ValueError("LAND judgment requires durable artifact and key")
        if self.verdict != "LAND" and judgment_refs:
            raise ValueError("non-LAND judgment cannot authorize artifact/key identity")
        object.__setattr__(self, "artifact_id", judgment_refs[0] if judgment_refs else None)
        object.__setattr__(self, "key", judgment_refs[1] if judgment_refs else None)


LandingJudge = Callable[[], LandingJudgment | LandingJudgeVerdict]
LandingLiveTreeResolver = Callable[[], str | None]


@dataclass(frozen=True)
class _LandingParkClassification:
    """Internal park classification before the semantic judge authorizes overrides."""

    blocked: LandBlocked | None = None
    allowed_overrides: tuple[LandingPolicyOverride, ...] = ()

    @property
    def allowed(self) -> bool:
        return self.blocked is None


def inspect_current_landing_verify_evidence(
    store: SqliteTaskStore,
    owner_task: DbTask,
    *,
    config: Any | None,
    git: Any | None,
    source_head: str | None = None,
    gate_identity: str | None = None,
    tree_fingerprint: str | None = None,
) -> LandingVerifyEvidence:
    """Return current canonical lifecycle verify evidence for landing.

    This reads only the lifecycle verify-gate store. Rebase/provider-side verify
    proof is intentionally not considered.
    """

    decision = resolve_verify_gate_decision(store, owner_task, config=config, git=git)
    return _landing_verify_evidence_from_decision(
        decision,
        expected_source_head=source_head,
        expected_gate_identity=gate_identity,
        expected_tree_fingerprint=tree_fingerprint,
    )


def acquire_landing_verify_evidence(
    store: SqliteTaskStore,
    owner_task: DbTask,
    *,
    config: Any,
    git: Any,
    target_branch: str,
    source_head: str | None = None,
    gate_identity: str | None = None,
    tree_fingerprint: str | None = None,
    context: AdvanceActionExecutionContext,
    execute_action: LandingAdvanceExecutor | None = None,
    live_tree_fingerprint_resolver: LandingLiveTreeResolver | None = None,
    member_tasks: tuple[DbTask, ...] | None = None,
) -> LandingVerifyAcquisitionResult:
    """Acquire green lifecycle verify evidence with one shared direct action.

    Missing or stale evidence is refreshed through the same direct verify action
    used by advance. Red/unavailable evidence is returned as a typed block; this
    helper never creates verify-fix or improve work.
    """

    def resolve_live_tree_fingerprint() -> str | None:
        if live_tree_fingerprint_resolver is not None:
            return live_tree_fingerprint_resolver()
        if git is not None and source_head is not None:
            return _compute_tree_fingerprint(git, head_sha=source_head)
        return tree_fingerprint

    initial_tree_fingerprint = resolve_live_tree_fingerprint()
    initial = inspect_current_landing_verify_evidence(
        store,
        owner_task,
        config=config,
        git=git,
        source_head=source_head,
        gate_identity=gate_identity,
        tree_fingerprint=initial_tree_fingerprint,
    )
    if _landing_verify_evidence_is_current_green(initial):
        return LandingVerifyAcquisitionResult("current_green", initial)
    if initial.status not in {"missing", "stale"}:
        return LandingVerifyAcquisitionResult(
            "blocked",
            initial,
            blocked=LandBlocked(
                "verify-unavailable-or-red",
                "current green source verify evidence is unavailable",
                _evidence_refs(owner_task.id, initial.epoch, initial.gate_identity, initial.tree_fingerprint, source_head),
            ),
        )

    action = plan_manual_verify_gate_action(
        config,
        store,
        git,
        owner_task,
        target_branch,
        verify_owner_task=owner_task,
        member_tasks=member_tasks,
        selected_for_merge=True,
    )
    action_type = str(action.get("type") or "")
    if action_type not in {"verify_gate", "reconcile_verify_gate_evidence"}:
        return LandingVerifyAcquisitionResult(
            "blocked",
            initial,
            action=action,
            blocked=LandBlocked(
                "verify-unavailable-or-red",
                "shared lifecycle verify prerequisite is unavailable",
                _evidence_refs(owner_task.id, action_type or None, source_head),
            ),
        )

    executor = execute_action or (lambda task, planned_action, execution_context: execute_advance_action(
        task=task,
        action=planned_action,
        context=execution_context,
    ))
    execution = executor(owner_task, action, context)
    refreshed_tree_fingerprint = resolve_live_tree_fingerprint()
    refreshed = inspect_current_landing_verify_evidence(
        store,
        owner_task,
        config=config,
        git=git,
        source_head=source_head,
        gate_identity=gate_identity,
        tree_fingerprint=refreshed_tree_fingerprint,
    )
    if _landing_verify_evidence_is_current_green(refreshed):
        return LandingVerifyAcquisitionResult(
            "ran_verify",
            refreshed,
            action=action,
            execution=execution,
        )
    return LandingVerifyAcquisitionResult(
        "blocked",
        refreshed,
        action=action,
        execution=execution,
        blocked=LandBlocked(
            "verify-unavailable-or-red",
            "current green source verify evidence is unavailable after shared verify",
            _evidence_refs(owner_task.id, refreshed.epoch, refreshed.gate_identity, refreshed.tree_fingerprint, source_head),
        ),
    )


def acquire_one_post_rebase_review(
    store: SqliteTaskStore,
    request: LandingPostRebaseReviewRequest,
    *,
    config: Any | None = None,
    create_full_review: Callable[..., DbTask] = create_review_task,
    create_resolution_review: Callable[..., DbTask] = create_resolution_review_task,
) -> LandingPostRebaseReviewResult:
    """Select or create landing's one allowed post-rebase review.

    Conflict resolution forces resolution mode even when the diff is proven
    unchanged. Changed or unknown diffs use resolution mode when provenance is
    complete, otherwise a full current-head fallback. Matching pending or
    in-progress reviews are reused only when mode and reviewed-head identity are
    exact.
    """

    review_budget_used = request.review_budget_used
    try:
        source_head = _normalize_required_ref(request.source_head, "landing post-rebase source head")
        target_head = _normalize_required_ref(request.target_head, "landing post-rebase target head")
    except ValueError as exc:
        return LandingPostRebaseReviewResult(
            status="blocked",
            need="full",
            review_budget_used=review_budget_used,
            blocked=LandBlocked(
                "required-review-unavailable",
                str(exc),
                _evidence_refs(request.impl_task.id),
            ),
        )
    need = _post_rebase_review_need(request, source_head=source_head, target_head=target_head)
    if need == "none":
        return LandingPostRebaseReviewResult(
            status="not_required",
            need="none",
            review_budget_used=review_budget_used,
        )
    action = _post_rebase_review_action(request, need)
    if action is None:
        return LandingPostRebaseReviewResult(
            status="blocked",
            need=need,
            review_budget_used=review_budget_used,
            blocked=LandBlocked(
                "required-review-unavailable",
                "post-rebase review identity is unavailable",
                _evidence_refs(request.impl_task.id, source_head, target_head),
            ),
        )

    exact_active, incompatible_active = _select_active_landing_review(store, request.impl_task, action=action)
    if incompatible_active is not None:
        return LandingPostRebaseReviewResult(
            status="blocked",
            need=need,
            action=action,
            review_budget_used=review_budget_used,
            blocked=LandBlocked(
                "required-review-unavailable",
                "active review does not match the required post-rebase review identity",
                _evidence_refs(incompatible_active.id, request.impl_task.id, source_head),
            ),
        )
    if exact_active is not None:
        if _landing_review_matches_required_identity(
            exact_active,
            subject_task_id=request.impl_task.id or "",
            action=action,
            completed=False,
        ):
            status = "in_progress" if exact_active.status == "in_progress" else "pending"
            return LandingPostRebaseReviewResult(
                cast(LandingPostRebaseReviewStatus, status),
                need,
                review_task=exact_active,
                action=action,
                review_budget_used=True,
            )

    exact = _find_exact_landing_review(store, request.impl_task, action=action)
    if exact is not None:
        if exact.status == "completed":
            verdict = _landing_review_verdict_from_task(config, exact)
            if verdict in {"APPROVED", "APPROVED_WITH_FOLLOWUPS", "CHANGES_REQUESTED"}:
                return LandingPostRebaseReviewResult(
                    status="reused_completed",
                    need=need,
                    review_task=exact,
                    action=action,
                    review_budget_used=review_budget_used,
                )
            return LandingPostRebaseReviewResult(
                status="blocked",
                need=need,
                review_task=exact,
                action=action,
                review_budget_used=review_budget_used,
                blocked=LandBlocked(
                    "required-review-unavailable",
                    "post-rebase review evidence is malformed or not merge-decision bearing",
                    _evidence_refs(exact.id, source_head),
                ),
            )
        if exact.status in {"failed", "stopped"}:
            return LandingPostRebaseReviewResult(
                status="blocked",
                need=need,
                review_task=exact,
                action=action,
                review_budget_used=review_budget_used,
                blocked=LandBlocked(
                    "required-review-unavailable",
                    f"latest exact post-rebase review ended with terminal status {exact.status}",
                    _evidence_refs(exact.id, source_head),
                ),
            )

    if review_budget_used:
        return LandingPostRebaseReviewResult(
            status="blocked",
            need=need,
            action=action,
            review_budget_used=review_budget_used,
            blocked=LandBlocked(
                "bounded-attempt-exhausted",
                "landing post-rebase review budget is exhausted",
                _evidence_refs(request.impl_task.id, source_head, target_head),
            ),
        )

    try:
        if need == "resolution":
            if request.rebase_task is None:
                raise ValueError("resolution review requires a rebase task")
            review = create_resolution_review(
                store,
                request.impl_task,
                config=config,
                rebase_task=request.rebase_task,
                resolved_head_sha=source_head,
                resolved_target_sha=target_head,
                trigger_source=request.trigger_source,
            )
        else:
            review = create_full_review(
                store,
                request.impl_task,
                config=config,
                trigger_source=request.trigger_source,
            )
            review.review_verify_head_sha = source_head
            store.update(review)
    except DuplicateReviewError as exc:
        active = exc.active_review
        if _landing_review_matches_required_identity(
            active,
            subject_task_id=request.impl_task.id or "",
            action=action,
            completed=False,
        ):
            status = "in_progress" if active.status == "in_progress" else "pending"
            return LandingPostRebaseReviewResult(
                status=cast(LandingPostRebaseReviewStatus, status),
                need=need,
                review_task=active,
                action=action,
                review_budget_used=True,
            )
        return LandingPostRebaseReviewResult(
            status="blocked",
            need=need,
            action=action,
            review_budget_used=review_budget_used,
            blocked=LandBlocked(
                "required-review-unavailable",
                "active review does not match the required post-rebase review identity",
                _evidence_refs(active.id, request.impl_task.id, source_head),
            ),
        )
    except ValueError as exc:
        if need == "resolution":
            fallback_request = LandingPostRebaseReviewRequest(
                impl_task=request.impl_task,
                source_head=source_head,
                target_head=target_head,
                pre_rebase_source_head=request.pre_rebase_source_head,
                rebase_task=request.rebase_task,
                changed_diff=request.changed_diff,
                conflict_resolved=request.conflict_resolved,
                resolution_provenance_complete=False,
                review_budget_used=review_budget_used,
                trigger_source=request.trigger_source,
            )
            return acquire_one_post_rebase_review(
                store,
                fallback_request,
                config=config,
                create_full_review=create_full_review,
                create_resolution_review=create_resolution_review,
            )
        return LandingPostRebaseReviewResult(
            status="blocked",
            need=need,
            action=action,
            review_budget_used=review_budget_used,
            blocked=LandBlocked(
                "required-review-unavailable",
                f"post-rebase review could not be created: {exc}",
                _evidence_refs(request.impl_task.id, source_head, target_head),
            ),
        )

    return LandingPostRebaseReviewResult(
        status="created",
        need=need,
        review_task=review,
        action=action,
        review_budget_used=True,
    )


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
    if park_decision.blocked is not None:
        return LandingPolicyDecision(False, blocked=park_decision.blocked)

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
        judgment_artifact_id=review_decision.judgment_artifact_id,
        judgment_key=review_decision.judgment_key,
        followup_materialization_identities=review_decision.followup_materialization_identities,
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
    return _normalize_evidence_refs(refs)


def _normalize_evidence_refs(refs: tuple[str | None, ...]) -> tuple[str, ...]:
    normalized: list[str] = []
    for ref in refs:
        if ref is None:
            continue
        stripped = ref.strip()
        if not stripped:
            raise ValueError("evidence references must be nonblank")
        normalized.append(stripped)
    return tuple(normalized)


def _normalize_required_ref(ref: str | None, label: str) -> str:
    refs = _normalize_evidence_refs((ref,))
    if not refs:
        raise ValueError(f"{label} is required")
    return refs[0]


def _landing_verify_evidence_from_decision(
    decision: VerifyGateDecision,
    *,
    expected_source_head: str | None,
    expected_gate_identity: str | None,
    expected_tree_fingerprint: str | None,
) -> LandingVerifyEvidence:
    epoch = decision.current_epoch
    result = decision.lookup.result
    metadata = decision.lookup.artifact_metadata
    gate_identity = _verify_gate_identity(epoch)
    tree_fingerprint = _verify_tree_fingerprint(metadata)
    expected_tree = _normalize_optional_nonblank_ref(expected_tree_fingerprint)
    current = decision.state == "passed"
    identity_matched = current
    if expected_source_head is not None:
        identity_matched = identity_matched and epoch is not None and epoch.reviewed_head_sha == expected_source_head
    if expected_gate_identity is not None:
        identity_matched = identity_matched and gate_identity == expected_gate_identity
    identity_matched = identity_matched and expected_tree is not None and tree_fingerprint == expected_tree
    status = cast(LandingVerifyStatus, decision.state if decision.state in {"passed", "failed", "stale", "unavailable", "missing"} else "missing")
    if result is not None and result.status not in {"passed", "failed", "unavailable"}:
        status = "malformed"
        current = False
        identity_matched = False
    return LandingVerifyEvidence(
        status=status,
        current=current,
        identity_matched=identity_matched,
        epoch=_verify_epoch_identity(epoch),
        gate_identity=gate_identity,
        tree_fingerprint=tree_fingerprint,
    )


def _landing_verify_evidence_is_current_green(evidence: LandingVerifyEvidence) -> bool:
    return (
        evidence.status == "passed"
        and evidence.current
        and evidence.identity_matched
        and bool(evidence.epoch)
        and bool(evidence.gate_identity)
        and bool(evidence.tree_fingerprint)
    )


def _normalize_optional_nonblank_ref(ref: str | None) -> str | None:
    if ref is None:
        return None
    stripped = ref.strip()
    return stripped or None


def _verify_epoch_identity(epoch: Any | None) -> str | None:
    if epoch is None:
        return None
    return json.dumps(
        {
            "branch": getattr(epoch, "reviewed_branch", None),
            "head": getattr(epoch, "reviewed_head_sha", None),
            "command": getattr(epoch, "verify_command", None),
            "timeout": getattr(epoch, "verify_timeout_seconds", None),
            "grace": getattr(epoch, "verify_timeout_grace_seconds", None),
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def _verify_gate_identity(epoch: Any | None) -> str | None:
    if epoch is None:
        return None
    return json.dumps(
        {
            "command": getattr(epoch, "verify_command", None),
            "timeout": getattr(epoch, "verify_timeout_seconds", None),
            "grace": getattr(epoch, "verify_timeout_grace_seconds", None),
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def _verify_tree_fingerprint(metadata: dict[str, Any] | None) -> str | None:
    if not isinstance(metadata, dict):
        return None
    aggregate = metadata.get("aggregate_details")
    if isinstance(aggregate, dict):
        value = aggregate.get("tree_fingerprint")
        normalized_value = _normalize_optional_nonblank_ref(value) if isinstance(value, str) else None
        if _aggregate_tree_fingerprint_is_complete(aggregate) and normalized_value is not None:
            return normalized_value
        return None
    value = metadata.get("tree_fingerprint")
    normalized_value = _normalize_optional_nonblank_ref(value) if isinstance(value, str) else None
    if normalized_value is not None:
        return normalized_value
    provenance = metadata.get("provenance")
    if isinstance(provenance, dict):
        value = provenance.get("tree_fingerprint")
        normalized_value = _normalize_optional_nonblank_ref(value) if isinstance(value, str) else None
        if normalized_value is not None:
            return normalized_value
    return None


def _aggregate_tree_fingerprint_is_complete(aggregate: dict[str, Any]) -> bool:
    if aggregate.get("tree_fingerprint_complete") is True:
        return True
    phase_results = aggregate.get("phase_results")
    runnable_count = aggregate.get("runnable_count")
    if not isinstance(phase_results, list) or not isinstance(runnable_count, int) or isinstance(runnable_count, bool):
        return False
    if runnable_count <= 0 or len(phase_results) != runnable_count:
        return False
    fingerprints = []
    for phase in phase_results:
        if not isinstance(phase, dict):
            return False
        value = phase.get("tree_fingerprint")
        if not isinstance(value, str) or not value:
            return False
        fingerprints.append(value)
    return bool(fingerprints) and all(fingerprint == fingerprints[-1] for fingerprint in fingerprints)


def _post_rebase_review_need(
    request: LandingPostRebaseReviewRequest,
    *,
    source_head: str | None = None,
    target_head: str | None = None,
) -> LandingPostRebaseReviewNeed:
    outcome_kind = request.rebase_outcome_kind
    if outcome_kind == "provider_resolved" or request.conflict_resolved:
        return "resolution" if request.resolution_provenance_complete else "full"
    if outcome_kind in {"recovered", "resumed"}:
        return "resolution" if request.resolution_provenance_complete else "full"
    if outcome_kind not in {"mechanical", "no_op"}:
        if request.changed_diff is False:
            return "full"
        return "resolution" if request.resolution_provenance_complete else "full"
    if (
        request.changed_diff is False
        and source_head is not None
        and target_head is not None
        and _valid_post_rebase_carry_forward_identity(request, source_head=source_head, target_head=target_head)
    ):
        return "none"
    return "full"


def _valid_post_rebase_carry_forward_identity(
    request: LandingPostRebaseReviewRequest,
    *,
    source_head: str,
    target_head: str,
) -> bool:
    identity = request.rebase_outcome_identity
    if identity is None:
        return False
    if identity.live_source_head != source_head or identity.live_target_head != target_head:
        return False
    if identity.attempted_target_head != target_head:
        return False
    if identity.target_contained is not True:
        return False
    if identity.provider_resolution_proof is not False:
        return False
    if identity.changed_diff is not False or request.changed_diff is not False:
        return False
    if identity.outcome_kind == "mechanical":
        try:
            pre_rebase_source_head = _normalize_required_ref(
                request.pre_rebase_source_head,
                "landing pre-rebase source head",
            )
        except ValueError:
            return False
        return (
            request.rebase_outcome_kind == "mechanical"
            and identity.no_op_subtype is None
            and identity.attempted_source_head == pre_rebase_source_head
        )
    if identity.outcome_kind != "no_op" or request.rebase_outcome_kind != "no_op":
        return False
    if identity.no_op_subtype not in {
        "already_contained",
        "superseded_contained",
        "unchanged_target",
        "moot",
    }:
        return False
    return identity.attempted_source_head == source_head


def _post_rebase_review_action(
    request: LandingPostRebaseReviewRequest,
    need: LandingPostRebaseReviewNeed,
) -> dict[str, Any] | None:
    if need == "none":
        return None
    try:
        source_head = _normalize_required_ref(request.source_head, "landing post-rebase source head")
        target_head = _normalize_required_ref(request.target_head, "landing post-rebase target head")
    except ValueError:
        return None
    if need == "full":
        return {
            "type": "create_review",
            "review_head_sha": source_head,
        }
    if request.rebase_task is None or not request.rebase_task.id:
        return None
    return {
        "type": "create_review",
        "review_mode": "resolution",
        "resolution_rebase_task_id": request.rebase_task.id,
        "resolution_head_sha": source_head,
        "resolution_target_sha": target_head,
    }


def _find_exact_landing_review(
    store: SqliteTaskStore,
    impl_task: DbTask,
    *,
    action: dict[str, Any],
) -> DbTask | None:
    if impl_task.id is None:
        return None
    candidates = [
        review
        for review in store.get_reviews_for_task(impl_task.id)
        if review.status in {"completed", "failed", "stopped"}
        and _landing_review_matches_required_identity(
            review,
            subject_task_id=impl_task.id,
            action=action,
            completed=review.status == "completed",
        )
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda review: (review.completed_at or review.started_at or review.created_at, review.id or ""))


def _select_active_landing_review(
    store: SqliteTaskStore,
    impl_task: DbTask,
    *,
    action: dict[str, Any],
) -> tuple[DbTask | None, DbTask | None]:
    if impl_task.id is None:
        return None, None
    candidates = [
        review
        for review in store.get_reviews_for_task(impl_task.id)
        if review.status in {"pending", "in_progress"}
    ]
    if not candidates:
        return None, None
    exact: list[DbTask] = []
    incompatible: list[DbTask] = []
    for review in candidates:
        if _landing_review_matches_required_identity(
            review,
            subject_task_id=impl_task.id,
            action=action,
            completed=False,
        ):
            exact.append(review)
        else:
            incompatible.append(review)
    def latest(review: DbTask) -> tuple[Any, str]:
        return review.started_at or review.created_at, review.id or ""

    if incompatible:
        return None, max(incompatible, key=latest)
    if exact:
        return max(exact, key=latest), None
    return None, None


def _landing_review_matches_required_identity(
    review: DbTask,
    *,
    subject_task_id: str,
    action: dict[str, Any],
    completed: bool,
) -> bool:
    if not subject_task_id:
        return False
    if not review_matches_create_review_action(review, subject_task_id=subject_task_id, action=action):
        return False
    review_mode = action.get("review_mode")
    if review_mode == "resolution":
        expected_head = action.get("resolution_head_sha")
        if not isinstance(expected_head, str) or not expected_head.strip():
            return False
        actual_head = review.review_verify_head_sha
        if actual_head is not None and actual_head != expected_head.strip():
            return False
        if completed and actual_head != expected_head.strip():
            return False
    else:
        expected_head = action.get("review_head_sha")
        if not isinstance(expected_head, str) or not expected_head.strip():
            return False
        if review.review_verify_head_sha != expected_head.strip():
            return False
    return True


def _landing_review_verdict_from_task(config: Any | None, review: DbTask) -> str | None:
    if review.output_content:
        try:
            return get_review_report(Path(getattr(config, "project_dir", ".")), review).verdict
        except Exception:
            return None
    try:
        return get_review_report(Path(getattr(config, "project_dir", ".")), review).verdict
    except Exception:
        return None


def _blocker_fingerprint(blocker: LandingOpenBlocker) -> str:
    return _normalize_required_ref(blocker.fingerprint, "blocker normalized fingerprint")


def _identity_evidence(facts: LandingPolicyFacts) -> tuple[str, ...]:
    return _evidence_refs(facts.task_id, facts.source_head, facts.target_head)


def _verify_evidence(verify: LandingVerifyEvidence | None, facts: LandingPolicyFacts) -> tuple[str, ...]:
    if verify is None:
        return _identity_evidence(facts)
    return _evidence_refs(facts.task_id, verify.epoch, verify.gate_identity, verify.tree_fingerprint, facts.source_head)


def _review_evidence(review: LandingReviewEvidence | None, facts: LandingPolicyFacts) -> tuple[str, ...]:
    if review is None:
        return _identity_evidence(facts)
    return _evidence_refs(
        facts.task_id,
        review.review_id,
        review.reviewed_head,
        facts.source_head,
        *(
            identity.fingerprint_key
            for identity in _followup_materialization_identities(review)
        ),
    )


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


def _followup_materialization_identities(
    review: LandingReviewEvidence,
) -> tuple[LandingFollowupMaterializationIdentity, ...]:
    try:
        return _validated_followup_materialization_identities(review)
    except ValueError:
        return ()


def _validated_followup_materialization_identities(
    review: LandingReviewEvidence,
) -> tuple[LandingFollowupMaterializationIdentity, ...]:
    if not review.followup_findings:
        return ()
    if not review.review_id:
        raise ValueError("follow-up materialization requires review identity")
    identities: list[LandingFollowupMaterializationIdentity] = []
    for followup in review.followup_findings:
        identities.append(
            LandingFollowupMaterializationIdentity(
                review_id=review.review_id,
                source=followup.source or "",
                finding_id=followup.finding_id,
                fingerprint=followup.fingerprint,
            )
        )
    durable_keys = [identity.durable_key for identity in identities]
    if len(durable_keys) != len(set(durable_keys)):
        raise ValueError("follow-up materialization identities must have unique durable keys")
    return tuple(sorted(identities, key=lambda identity: identity.fingerprint_key))


def _has_valid_followup_finding_set(review: LandingReviewEvidence) -> bool:
    if not review.followup_findings:
        return False
    try:
        _validated_followup_materialization_identities(review)
    except ValueError:
        return False
    return True


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
    if not facts.ancestry_proof_available or not facts.clean_merge or not _rebase_state_consistent(facts):
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
        return LandingPolicyDecision(
            True,
            followup_materialization_identities=_followup_materialization_identities(review),
        )
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
            return _judge_for_overrides(
                facts=facts,
                judge=judge,
                allowed_overrides=park_overrides,
            )
        return LandingPolicyDecision(
            True,
            followup_materialization_identities=_followup_materialization_identities(review),
        )
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
    if (
        review.required
        and (
            review.status != "completed"
            or not review.current
            or not review.parseable
            or not review.identity_matched
            or not review.review_id
            or review.reviewed_head != facts.source_head
            or review.verdict is None
            or review.verdict == "NEEDS_DISCUSSION"
            or review.mode == "unknown"
        )
    ):
        return LandBlocked(
            "required-review-unavailable",
            "required review evidence is unavailable",
            _review_evidence(review, facts),
        )
    if review.required and review.mode == "spec_coherence":
        return LandBlocked(
            "required-review-unavailable",
            "spec-coherence review cannot satisfy code-review landing",
            _review_evidence(review, facts),
        )
    if review.followup_findings:
        try:
            _validated_followup_materialization_identities(review)
        except ValueError:
            return LandBlocked(
                "required-review-unavailable",
                "review has no valid follow-up finding evidence",
                _review_evidence(review, facts),
            )
    if review.verdict == "APPROVED_WITH_FOLLOWUPS" and not _has_valid_followup_finding_set(review):
        return LandBlocked(
            "required-review-unavailable",
            "approved-with-followups review has no valid follow-up finding evidence",
            _review_evidence(review, facts),
        )
    if review.verdict == "APPROVED" and review.followup_findings:
        return LandBlocked(
            "required-review-unavailable",
            "approved review contradicts parsed follow-up finding evidence",
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
    try:
        judgment = _normalize_landing_judgment(judge())
    except ValueError:
        return LandingPolicyDecision(
            False,
            blocked=LandBlocked(
                "policy-or-judge-refused",
                "guarded landing judgment identity is invalid",
                _identity_evidence(facts),
            ),
        )
    if judgment.verdict != "LAND":
        return LandingPolicyDecision(
            False,
            blocked=LandBlocked(
                "policy-or-judge-refused",
                "guarded landing judgment refused landing",
                _identity_evidence(facts),
            ),
            judgment_verdict=judgment.verdict,
        )
    return LandingPolicyDecision(
        True,
        allowed_overrides=allowed_overrides,
        judgment_verdict=judgment.verdict,
        judgment_artifact_id=judgment.artifact_id,
        judgment_key=judgment.key,
        followup_materialization_identities=_followup_materialization_identities(facts.review)
        if facts.review is not None
        else (),
    )


def _normalize_landing_judgment(result: LandingJudgment | LandingJudgeVerdict) -> LandingJudgment:
    if isinstance(result, LandingJudgment):
        return result
    return LandingJudgment(result)


def _classify_park(
    policy: LandingPolicyName,
    facts: LandingPolicyFacts,
) -> _LandingParkClassification:
    if not facts.parked_reason:
        return _LandingParkClassification()
    if policy == "strict":
        return _LandingParkClassification(
            blocked=LandBlocked(
                "nondeferrable-blocker",
                "strict policy refuses parked-state overrides",
                _evidence_refs(facts.task_id, facts.parked_reason),
            ),
        )
    if facts.parked_reason == "review-blocker-adjudication-needed":
        if facts.review_blocker_adjudication_evidence_complete:
            return _LandingParkClassification(allowed_overrides=("parked:review-blocker-adjudication-needed",))
        return _LandingParkClassification(
            blocked=LandBlocked(
                "policy-or-judge-refused",
                "review-blocker adjudication evidence is incomplete",
                _evidence_refs(facts.task_id, facts.parked_reason),
            ),
        )
    override = GUARDED_PARK_OVERRIDES.get(facts.parked_reason)
    if override is not None:
        return _LandingParkClassification(allowed_overrides=(override,))
    return _LandingParkClassification(
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
