"""Declarative advance/iterate rule engine."""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from fnmatch import fnmatchcase
from pathlib import Path
from typing import Any

from gza.artifact_paths import InvalidArtifactPathError, resolve_artifact_path
from gza.branch_resolution import resolve_rebase_target_task
from gza.config import DEFAULT_ADVANCE_OFF_TOPIC_VERIFY_UNBLOCK, DEFAULT_MAX_NOOP_IMPROVE_CYCLES
from gza.console import prompt_available_width, shorten_prompt
from gza.db import (
    TASK_COMMENT_KIND_FEEDBACK,
    SqliteTaskStore,
    Task as DbTask,
    TaskArtifact,
    task_id_numeric_key,
    task_owns_merge_status,
)
from gza.flaky_investigations import (
    FlakyInvestigationEvidence,
    derive_flaky_targeted_command,
    normalize_flaky_investigation_dedup_key,
)
from gza.git import ResolvedMergeSourceRef
from gza.lifecycle_completion import merge_state_is_terminal_for_lifecycle
from gza.lineage import resolve_impl_task, walk_ancestors, walk_based_on_descendants
from gza.merge_state import resolve_task_merge_state_for_target
from gza.off_topic_verify import (
    classify_failure_diff_scope,
    parse_review_verify_failure_set,
)
from gza.operator_state import terminal_no_work_lifecycle_detail
from gza.plan_review_materialization import (
    build_plan_review_slice_task_specs,
    inspect_plan_review_materialization_for_repair,
    load_materialized_plan_slice_set,
    plan_review_manifest_digest,
)
from gza.plan_review_verdict import (
    PlanReviewManifest,
    get_plan_review_outcome,
)
from gza.project_discovery import (
    parse_name_status_project_paths,
)
from gza.query import (
    get_code_changing_descendants_for_root,
    get_implementation_review_evidence,
    resolve_lineage_root,
    resolve_same_branch_lineage_root,
)
from gza.rebase_diff import parse_rebase_diff_provenance
from gza.recovery_engine import (
    FailedRecoveryDecision,
    classify_failure_reason,
    decide_failed_task_recovery,
    get_failed_recovery_needs_attention_reason,
)
from gza.recovery_read_context import RecoveryReadContext
from gza.resume_policy import is_resumable_failed_task as _is_resumable_failed_task
from gza.review_clearance import (
    REVIEW_CLEARANCE_ARTIFACT_KIND,
    VERIFY_ONLY_NOOP_REVIEW_CLEARANCE_KIND,
    VERIFY_ONLY_NOOP_REVIEW_CLEARANCE_STATUS,
    is_verify_only_noop_review_clearance_status,
)
from gza.review_scope import (
    ResolutionReviewScope,
    build_resolution_review_scope,
    declares_resolution_review_mode,
    declares_spec_coherence_review_mode,
    parse_resolution_review_scope,
    parse_spec_coherence_review_scope,
)
from gza.review_tasks import (
    SPEC_COHERENCE_REVIEW_SCOPE,
    build_review_blocker_dispute_metadata,
    find_existing_review_blocker_adjudication_task,
    persist_off_topic_verify_clearance,
    review_blocker_dispute_matches_current,
)
from gza.review_verdict import (
    ParsedReviewReport,
    ReviewBlockerSummary,
    ReviewFinding,
    classify_review_blocker_finding,
    get_review_content,
    get_review_finding_fingerprint,
    get_review_finding_fingerprint_details,
    get_review_report,
    is_verify_timeout_only_review,
    summarize_review_blockers,
)
from gza.runner import (
    CROSS_PROJECT_TAG,
    PROJECT_SCOPE_VIOLATION_FAILURE_REASON,
    REVIEW_BLOCKER_RESOLUTION_ARTIFACT_KIND,
    ReviewVerifyResult,
    _extract_review_verify_phase_results,
    _filter_owned_artifact_paths,
    _find_out_of_scope_paths,
    _make_review_verify_result,
    _project_boundary,
    _review_is_verify_only_blocked_at_head,
    _task_has_current_passing_review_verify_evidence,
    _task_is_cross_project,
)
from gza.source_followup import (
    collect_non_dropped_implement_source_ids,
    resolve_source_followup_state,
    source_task_has_implementation_followup,
)

NEEDS_ATTENTION_LABEL = "Needs attention"
PARK_REASON_IMPROVE_NO_OP = "improve-no-op"
PARK_REASON_VERIFY_NOOP_BRANCH_TIP_UNAVAILABLE = "verify-noop-improve-branch-tip-unavailable"
PARK_REASON_VERIFY_NOOP_DIFF_PROBE_UNAVAILABLE = "verify-noop-improve-diff-probe-unavailable"
PARK_REASON_VERIFY_BLOCKED_NO_CODE_ISSUES = "verify-blocked-no-code-issues"
PARK_REASON_REVIEW_BLOCKER_ADJUDICATION_NEEDED = "review-blocker-adjudication-needed"
PARK_REASON_DUPLICATE_BLOCKER_NO_PROGRESS = "duplicate-blocker-no-progress"
PARK_REASON_REVIEW_MAX_CYCLES_REACHED = "review-max-cycles-reached"
PARK_REASON_RETRY_LIMIT_REACHED = "retry-limit-reached"
PARK_REASON_RETRYABLE_PROVIDER_ERROR = "retryable-provider-error"

WATCH_SURFACE_ONCE_NEEDS_ATTENTION_REASONS = frozenset(
    {
        "automatic-recovery-disabled",
        "branch-already-rebased-lineage-incomplete",
        "closing-review-failed-max-retries",
        "closing-review-needs-manual-refresh",
        PARK_REASON_DUPLICATE_BLOCKER_NO_PROGRESS,
        "explore-needs-follow-up-decision",
        PARK_REASON_IMPROVE_NO_OP,
        "merge-source-needs-manual-resolution",
        "plan-review-invalid-slices",
        "plan-review-materialization-repair-needed",
        "plan-review-max-cycles-reached",
        "plan-review-needs-manual-creation",
        "plan-review-repeatedly-failed",
        "project-scope-unverified",
        "project-scope-violation",
        "rebase-failed-needs-manual-resolution",
        "rebase-failure-circuit-breaker",
        "rebase-target-missing-merge-unit",
        "rebase-did-not-unblock-merge",
        "recovery-ambiguous",
        PARK_REASON_RETRYABLE_PROVIDER_ERROR,
        PARK_REASON_RETRY_LIMIT_REACHED,
        PARK_REASON_REVIEW_BLOCKER_ADJUDICATION_NEEDED,
        PARK_REASON_REVIEW_MAX_CYCLES_REACHED,
        "review-freshness-unverified",
        "review-needs-manual-creation",
        "review-verdict-needs-manual-attention",
        "stale-review-needs-manual-refresh",
        PARK_REASON_VERIFY_BLOCKED_NO_CODE_ISSUES,
        PARK_REASON_VERIFY_NOOP_BRANCH_TIP_UNAVAILABLE,
        PARK_REASON_VERIFY_NOOP_DIFF_PROBE_UNAVAILABLE,
    }
)

NOOP_IMPROVE_KIND_VERIFY_ONLY = "verify_only"
NOOP_IMPROVE_KIND_REAL_BLOCKER = "real_blocker"
FIX_HANDOFF_NEEDS_ATTENTION_REASONS = frozenset(
    {
        PARK_REASON_REVIEW_MAX_CYCLES_REACHED,
        "automatic-recovery-disabled",
        PARK_REASON_RETRY_LIMIT_REACHED,
        PARK_REASON_RETRYABLE_PROVIDER_ERROR,
    }
)
FAILED_RECOVERY_FIX_HANDOFF_NEEDS_ATTENTION_REASONS = frozenset(
    {
        "automatic-recovery-disabled",
        "retry-limit-reached",
        "retryable-provider-error",
    }
)
FAILED_RECOVERY_REARM_RECOMMENDATION_NEEDS_ATTENTION_REASONS = frozenset(
    {
        "retry-limit-reached",
        "retryable-provider-error",
    }
)
FAILED_RECOVERY_RETRY_OR_REIMPLEMENT_NEXT_STEP = (
    "Recommended next step: retry or re-implement instead. "
    "`uv run gza fix` only applies after a completed implementation."
)
DUPLICATE_BLOCKER_REVIEW_CYCLES = 3
REBASE_FAILURE_CIRCUIT_BREAKER_ATTEMPTS = 3

WORKER_CONSUMING_ACTIONS = frozenset(
    {
        "needs_rebase",
        "create_implement",
        "create_plan_review",
        "run_plan_review",
        "create_plan_improve",
        "run_plan_improve",
        "create_review",
        "run_review",
        "create_review_adjudication",
        "run_review_adjudication",
        "improve",
        "run_improve",
        "resume",
        "retry",
    }
)
MERGEABLE_EXECUTION_STATUSES = frozenset({"completed", "unmerged"})
VERIFY_BLOCKED_REVIEW_THRESHOLD = 2
_LOG = logging.getLogger(__name__)
VERIFY_COMMAND_OUTPUT_ARTIFACT_KIND = "verify_command_output"
VERIFY_ONLY_NOOP_RECOVERY_ATTENTION_ARTIFACT_KIND = "verify_only_noop_recovery_attention"
VERIFY_ONLY_NOOP_RECOVERY_ATTENTION_STATUS = "parked"


@dataclass(frozen=True)
class PostMergeRebaseState:
    """Local branch/target facts that can clear stale failed-rebase state."""

    merge_unit_state: str | None
    branch_tip_sha: str | None
    target_tip_sha: str | None
    target_is_ancestor_of_branch: bool | None
    branch_equals_target: bool
    already_merged: bool
    rebase_resolution_proved: bool
    reason: str | None
    warning: str | None = None
    rebase_target_missing_merge_unit: bool = False
    resolved_merge_state: str | None = None


@dataclass(frozen=True)
class DuplicateBlockerStreak:
    """Repeated primary-blocker streak for the latest completed review chain."""

    cycles: int
    fingerprint: tuple[str, str]
    title: str
    anchor: str
    review_task_ids: tuple[str, ...]


@dataclass(frozen=True)
class RebaseFailureStreak:
    """Repeated failed rebases with no later successful lineage progress."""

    attempts: int
    branch: str
    failed_task_ids: tuple[str, ...]


@dataclass(frozen=True)
class BranchHeadResolution:
    """Current branch-head probe result for verify-evidence freshness checks."""

    head_sha: str | None
    warning: str | None = None


@dataclass(frozen=True)
class PlanMaterializationState:
    """Whether the current approved plan-review manifest is already materialized."""

    materialized: bool
    task_ids: tuple[str, ...] = ()
    partial_slice_descendants_detected: bool = False
    partial_repair_candidate: PlanMaterializationRepairCandidate | None = None


@dataclass(frozen=True)
class PlanMaterializationRepairCandidate:
    """Validated pending slice subset that can be deterministically rematerialized."""

    partial_task_ids: tuple[str, ...]
    manifest_digest: str
    trigger_source: str


@dataclass(frozen=True)
class ReviewBlockerAdjudicationCandidate:
    """One disputed blocker eligible for an adjudication attempt."""

    finding: ReviewFinding
    dispute_metadata: Mapping[str, Any]
    dispute_artifact: TaskArtifact | None = None


@dataclass(frozen=True)
class ReviewBlockerResolutionStatus:
    """Latest persisted resolution state for one current review blocker."""

    finding: ReviewFinding
    latest_artifact: TaskArtifact | None = None
    state: str | None = None


@dataclass(frozen=True)
class OffTopicVerifyClearanceCandidate:
    """Execution-time clearance payload for one off-topic verify-only review block."""

    review_task: DbTask
    reviewed_head_sha: str
    tree_fingerprint: str
    evidences: tuple[FlakyInvestigationEvidence, ...]


@dataclass(frozen=True)
class SiblingReviewAttentionCandidate:
    """Sibling review whose unresolved code blockers should own no-op attention."""

    review_task: DbTask
    review_report: ParsedReviewReport
    related_improve_task: DbTask | None = None


@dataclass(frozen=True)
class NeedsAttentionDisplayEntry:
    """Rendered needs-attention display text plus known field boundaries."""

    text: str
    prompt_start: int
    prompt_end: int


@dataclass(frozen=True)
class AdvanceContext:
    """Resolved task state used by advance rules."""

    store: SqliteTaskStore
    git: Any
    task: DbTask
    task_type: str
    has_branch: bool
    target_branch: str

    requires_review: bool
    create_reviews: bool
    max_review_cycles: int
    advance_create_plan_reviews: bool
    require_plan_review_before_implement: bool
    max_plan_review_cycles: int
    max_failed_plan_review_retries: int
    max_plan_slices: int | None
    plan_slice_target_timeout_minutes: int
    max_noop_improve_cycles: int
    max_resume_attempts: int
    selected_for_merge: bool = False

    auto_implement_enabled: bool = True
    has_non_dropped_implement_descendant: bool = False
    active_plan_child: DbTask | None = None
    active_implement_child: DbTask | None = None
    has_non_dropped_plan_or_implement_descendant: bool = False

    merge_source_ref: str | None = None
    merge_source_warning: str | None = None
    post_merge_rebase_state: PostMergeRebaseState | None = None
    merge_state: str | None = None
    can_merge: bool = True
    spec_coherence_required: bool = False
    spec_coherence_changed_paths: tuple[str, ...] = ()
    spec_coherence_current_head_sha: str | None = None
    spec_coherence_inspection_error: str | None = None
    spec_coherence_active_review: DbTask | None = None
    spec_coherence_latest_completed_review: DbTask | None = None
    spec_coherence_review_verdict: str | None = None
    spec_coherence_review_current: bool = False
    strict_scope_violation_paths: tuple[str, ...] = ()
    strict_scope_inspection_error: str | None = None
    rebase_pending_or_running: DbTask | None = None
    rebase_failed: DbTask | None = None
    latest_completed_rebase: DbTask | None = None
    rebase_failure_streak: RebaseFailureStreak | None = None
    rebase_invalidates_review: bool = False
    review_invalidated_by_progress: bool = False
    review_invalidation_reason: str | None = None
    review_preserved_by_rebase: DbTask | None = None
    review_invalidated_by_rebase: DbTask | None = None
    resolution_review_required: bool = False
    resolution_review_metadata: ResolutionReviewScope | None = None
    resolution_review_metadata_invalid: bool = False
    current_review_head_sha: str | None = None
    current_review_head_probe_warning: str | None = None
    latest_reviewed_head_sha: str | None = None

    reviews: list[DbTask] | None = None
    review_root_task: DbTask | None = None
    active_review: DbTask | None = None
    latest_completed_review: DbTask | None = None
    latest_completed_code_change: DbTask | None = None
    effective_review_cleared_at: datetime | None = None
    review_cleared: bool = False
    created_investigation_task_ids: tuple[str, ...] = ()
    reused_investigation_task_ids: tuple[str, ...] = ()
    review_verdict: str | None = None
    review_report: ParsedReviewReport | None = None
    latest_review_blocker_summary: ReviewBlockerSummary | None = None
    followup_findings: tuple[ReviewFinding, ...] = ()
    recent_verify_timeout_only_reviews: tuple[DbTask, ...] = ()
    off_topic_verify_clearance_candidate: OffTopicVerifyClearanceCandidate | None = None

    completed_review_cycles: int = 0
    review_cycle_boundary_task_id: str | None = None
    review_cycle_boundary_reason: str | None = None
    active_improve_running: DbTask | None = None
    active_improve_pending: DbTask | None = None
    latest_noop_improve: DbTask | None = None
    consecutive_noop_improves: int = 0
    noop_improve_trigger: str | None = None
    noop_improve_verify_probe_warning: str | None = None
    noop_improve_verify_recovery_attention_message: str | None = None
    review_blocker_resolution_statuses: tuple[ReviewBlockerResolutionStatus, ...] = ()
    review_blockers_invalidated: bool = False
    review_blockers_revalidated: bool = False
    review_blocker_adjudication_needed: bool = False
    review_blocker_adjudication_needed_task: DbTask | None = None
    review_blocker_adjudication_candidate: ReviewBlockerAdjudicationCandidate | None = None
    active_review_blocker_adjudication: DbTask | None = None
    sibling_review_attention_candidate: SiblingReviewAttentionCandidate | None = None
    duplicate_blocker_streak: DuplicateBlockerStreak | None = None
    has_improve_after_review: bool = False
    has_fresh_unresolved_comments_since_latest_review: bool = False
    closing_review_action: dict[str, Any] | None = None
    latest_plan_source: DbTask | None = None
    superseded_plan_source: DbTask | None = None
    current_plan_review: DbTask | None = None
    active_plan_review_pending: DbTask | None = None
    active_plan_review_running: DbTask | None = None
    latest_completed_plan_review: DbTask | None = None
    failed_plan_review_count: int = 0
    plan_review_verdict: str | None = None
    parsed_plan_review_manifest: PlanReviewManifest | None = None
    validated_plan_review_manifest: PlanReviewManifest | None = None
    plan_review_validation_error: str | None = None
    recoverable_plan_review_schema_version_format_error: bool = False
    current_plan_improve: DbTask | None = None
    active_plan_improve_pending: DbTask | None = None
    active_plan_improve_running: DbTask | None = None
    plan_review_cycle_count: int = 0
    completed_plan_review_cycles: int = 0
    plan_materialization_state: PlanMaterializationState | None = None

    failed_recovery_decision: FailedRecoveryDecision | None = None
    failed_recovery_attention_reason: str | None = None
    is_resumable_failed_task: bool = False
    has_resume_children: bool = False
    resume_chain_depth: int = 0
    failure_reason: str | None = None


@dataclass(frozen=True)
class AdvanceRule:
    """A single ordered advance rule."""

    name: str
    matches: Callable[[AdvanceContext], bool]
    action: Callable[[AdvanceContext], dict[str, Any]]


@dataclass(frozen=True)
class ReviewCycleBoundary:
    """Durable progress boundary that scopes review-iteration accounting."""

    boundary_time: datetime | None = None
    boundary_task_id: str | None = None
    boundary_reason: str | None = None


@dataclass(frozen=True)
class StrictScopeInspection:
    """Resolved strict-scope inspection state for advance-time gating."""

    violation_paths: tuple[str, ...] = ()
    inspection_error: str | None = None


@dataclass(frozen=True)
class SpecCoherenceInspection:
    """Resolved behavior-spec coherence gate state for one branch."""

    required: bool = False
    changed_paths: tuple[str, ...] = ()
    inspection_error: str | None = None


def _resolve_current_merge_source(git: Any, branch: str) -> ResolvedMergeSourceRef:
    """Return the merge source chosen for advance planning and any warning."""
    resolve_fresh = getattr(git, "resolve_fresh_merge_source", None)
    if callable(resolve_fresh):
        resolved = resolve_fresh(branch)
        if isinstance(resolved, ResolvedMergeSourceRef):
            return resolved
        if isinstance(resolved, tuple) and len(resolved) == 2:
            return ResolvedMergeSourceRef(resolved[0], resolved[1])
        if isinstance(resolved, str):
            return ResolvedMergeSourceRef(resolved)
        if resolved is None:
            return ResolvedMergeSourceRef(None)

    resolve_fresh_ref = getattr(git, "resolve_fresh_merge_source_ref", None)
    if callable(resolve_fresh_ref):
        return ResolvedMergeSourceRef(resolve_fresh_ref(branch))

    ref_exists = getattr(git, "ref_exists", None)
    if callable(ref_exists):
        remote_ref = f"origin/{branch}"
        if ref_exists(remote_ref):
            return ResolvedMergeSourceRef(remote_ref)
    return ResolvedMergeSourceRef(branch)


def resolve_post_merge_rebase_state(
    store: SqliteTaskStore,
    git: Any,
    task: DbTask,
    target_branch: str,
    *,
    merge_source: ResolvedMergeSourceRef | None = None,
) -> PostMergeRebaseState:
    """Resolve local proof that stale failed-rebase state is no longer authoritative."""
    def _normalize_sha(value: object) -> str | None:
        return value if isinstance(value, str) and value else None

    merge_target_task = task
    if task.task_type == "rebase":
        resolved_target = resolve_rebase_target_task(store, task)
        if resolved_target is not None:
            merge_target_task = resolved_target

    merge_unit = (
        store.resolve_merge_unit_for_task(merge_target_task.id)
        if merge_target_task.id is not None
        else None
    )
    merge_unit_state = merge_unit.state if merge_unit is not None else None
    if merge_state_is_terminal_for_lifecycle(merge_unit_state):
        return PostMergeRebaseState(
            merge_unit_state=merge_unit_state,
            branch_tip_sha=None,
            target_tip_sha=None,
            target_is_ancestor_of_branch=None,
            branch_equals_target=False,
            already_merged=True,
            rebase_resolution_proved=True,
            reason=f"merge-unit-{merge_unit_state}",
        )

    if (
        task.task_type == "rebase"
        and merge_target_task.id is not None
        and merge_unit is None
        and bool(task.branch)
        and bool(merge_target_task.branch)
        and task.branch != merge_target_task.branch
    ):
        return PostMergeRebaseState(
            merge_unit_state=None,
            branch_tip_sha=None,
            target_tip_sha=None,
            target_is_ancestor_of_branch=None,
            branch_equals_target=False,
            already_merged=False,
            rebase_resolution_proved=False,
            reason="rebase-target-missing-merge-unit",
            rebase_target_missing_merge_unit=True,
        )

    branch_name = task.branch
    if not branch_name:
        return PostMergeRebaseState(
            merge_unit_state=merge_unit_state,
            branch_tip_sha=None,
            target_tip_sha=None,
            target_is_ancestor_of_branch=None,
            branch_equals_target=False,
            already_merged=False,
            rebase_resolution_proved=False,
            reason=None,
            warning="task branch is missing; cannot resolve post-merge rebase state",
        )

    proof_source = merge_source or _resolve_current_merge_source(git, branch_name)
    proof_ref = proof_source.ref
    if proof_source.warning:
        return PostMergeRebaseState(
            merge_unit_state=merge_unit_state,
            branch_tip_sha=None,
            target_tip_sha=None,
            target_is_ancestor_of_branch=None,
            branch_equals_target=False,
            already_merged=False,
            rebase_resolution_proved=False,
            reason=None,
            warning=proof_source.warning,
        )
    if not proof_ref:
        return PostMergeRebaseState(
            merge_unit_state=merge_unit_state,
            branch_tip_sha=None,
            target_tip_sha=None,
            target_is_ancestor_of_branch=None,
            branch_equals_target=False,
            already_merged=False,
            rebase_resolution_proved=False,
            reason=None,
            warning=(
                f"fresh merge source for branch '{branch_name}' is unavailable; "
                "cannot resolve post-merge rebase state"
            ),
        )

    rev_parse_if_exists = getattr(git, "rev_parse_if_exists", None)
    if not callable(rev_parse_if_exists):
        return PostMergeRebaseState(
            merge_unit_state=merge_unit_state,
            branch_tip_sha=None,
            target_tip_sha=None,
            target_is_ancestor_of_branch=None,
            branch_equals_target=False,
            already_merged=False,
            rebase_resolution_proved=False,
            reason=None,
            warning="git runtime cannot resolve local refs for post-merge rebase state",
        )

    try:
        branch_tip_sha = _normalize_sha(rev_parse_if_exists(proof_ref))
        target_tip_sha = _normalize_sha(rev_parse_if_exists(target_branch))
    except Exception as exc:
        return PostMergeRebaseState(
            merge_unit_state=merge_unit_state,
            branch_tip_sha=None,
            target_tip_sha=None,
            target_is_ancestor_of_branch=None,
            branch_equals_target=False,
            already_merged=False,
            rebase_resolution_proved=False,
            reason=None,
            warning=f"failed to resolve local refs for post-merge rebase state: {exc}",
        )

    branch_equals_target = (
        branch_tip_sha is not None
        and target_tip_sha is not None
        and branch_tip_sha == target_tip_sha
    )
    if branch_equals_target:
        return PostMergeRebaseState(
            merge_unit_state=merge_unit_state,
            branch_tip_sha=branch_tip_sha,
            target_tip_sha=target_tip_sha,
            target_is_ancestor_of_branch=True,
            branch_equals_target=True,
            already_merged=True,
            rebase_resolution_proved=True,
            reason="branch-tip-equals-target-tip",
        )

    if branch_tip_sha is None or target_tip_sha is None:
        missing_ref = proof_ref if branch_tip_sha is None else target_branch
        return PostMergeRebaseState(
            merge_unit_state=merge_unit_state,
            branch_tip_sha=branch_tip_sha,
            target_tip_sha=target_tip_sha,
            target_is_ancestor_of_branch=None,
            branch_equals_target=False,
            already_merged=False,
            rebase_resolution_proved=False,
            reason=None,
            warning=f"missing local ref '{missing_ref}' for post-merge rebase state",
        )

    is_ancestor = getattr(git, "is_ancestor", None)
    if not callable(is_ancestor):
        return PostMergeRebaseState(
            merge_unit_state=merge_unit_state,
            branch_tip_sha=branch_tip_sha,
            target_tip_sha=target_tip_sha,
            target_is_ancestor_of_branch=None,
            branch_equals_target=False,
            already_merged=False,
            rebase_resolution_proved=False,
            reason=None,
            warning="git runtime cannot check ancestry for post-merge rebase state",
        )

    try:
        target_is_ancestor_of_branch = is_ancestor(target_branch, proof_ref)
    except Exception as exc:
        return PostMergeRebaseState(
            merge_unit_state=merge_unit_state,
            branch_tip_sha=branch_tip_sha,
            target_tip_sha=target_tip_sha,
            target_is_ancestor_of_branch=None,
            branch_equals_target=False,
            already_merged=False,
            rebase_resolution_proved=False,
            reason=None,
            warning=f"failed to check post-merge ancestry for rebase state: {exc}",
        )
    if not isinstance(target_is_ancestor_of_branch, bool):
        return PostMergeRebaseState(
            merge_unit_state=merge_unit_state,
            branch_tip_sha=branch_tip_sha,
            target_tip_sha=target_tip_sha,
            target_is_ancestor_of_branch=None,
            branch_equals_target=False,
            already_merged=False,
            rebase_resolution_proved=False,
            reason=None,
            warning="git runtime returned non-boolean ancestry result for post-merge rebase state",
        )

    if target_is_ancestor_of_branch:
        return PostMergeRebaseState(
            merge_unit_state=merge_unit_state,
            branch_tip_sha=branch_tip_sha,
            target_tip_sha=target_tip_sha,
            target_is_ancestor_of_branch=True,
            branch_equals_target=False,
            already_merged=False,
            rebase_resolution_proved=True,
            reason="branch-contains-target-tip",
        )

    return PostMergeRebaseState(
        merge_unit_state=merge_unit_state,
        branch_tip_sha=branch_tip_sha,
        target_tip_sha=target_tip_sha,
        target_is_ancestor_of_branch=False,
        branch_equals_target=False,
        already_merged=False,
        rebase_resolution_proved=False,
        reason=None,
    )


def _resolve_and_persist_post_merge_rebase_state(
    store: SqliteTaskStore,
    git: Any,
    task: DbTask,
    target_branch: str,
    *,
    merge_source: ResolvedMergeSourceRef | None = None,
) -> PostMergeRebaseState:
    """Resolve local stale-rebase cleanup state and persist proven merge truth."""
    state = resolve_post_merge_rebase_state(
        store,
        git,
        task,
        target_branch,
        merge_source=merge_source,
    )
    resolved_merge_state = resolve_task_merge_state_for_target(
        store=store,
        task=task,
        git=git,
        target_branch=target_branch,
    )
    state = replace(state, resolved_merge_state=resolved_merge_state)
    if (
        (
            (state.reason == "branch-tip-equals-target-tip" and state.already_merged)
            or merge_state_is_terminal_for_lifecycle(resolved_merge_state)
        )
        and task.id is not None
        and task.status == "completed"
        and task.has_commits
        and task_owns_merge_status(task)
    ):
        merge_unit = store.resolve_merge_unit_for_task(task.id)
        if merge_unit is None and bool(task.branch):
            merge_unit = store.get_or_create_merge_unit_for_task(task)
        if merge_unit is not None:
            persisted_state = "merged"
            if resolved_merge_state is not None and merge_state_is_terminal_for_lifecycle(resolved_merge_state):
                persisted_state = resolved_merge_state
            if persisted_state == "merged":
                store.set_merge_unit_state(
                    merge_unit.id,
                    persisted_state,
                    merged_by_task_id=task.id,
                )
            else:
                store.set_merge_unit_state(merge_unit.id, persisted_state)
        else:
            store.set_merge_status(task.id, "merged")
    return state


def is_resumable_failure_reason(failure_reason: str | None) -> bool:
    """Return True when a failure reason is auto-resumable by advance."""
    return classify_failure_reason(failure_reason) == "timeout"


def is_resumable_failed_task(task: Any) -> bool:
    """Backward-compatible export for callers that still import this helper here."""
    return _is_resumable_failed_task(task)


def count_completed_review_cycles(store: SqliteTaskStore, impl_task_id: str) -> int:
    improve_tasks = store.get_improve_tasks_by_root(impl_task_id)
    return sum(1 for t in improve_tasks if t.status == "completed")


def resolve_review_cycle_boundary(
    *,
    completed_reviews: list[DbTask],
    latest_completed_review: DbTask | None,
    latest_completed_rebase: DbTask | None = None,
    latest_completed_code_change: DbTask | None = None,
    latest_reviewed_head_sha: str | None = None,
    current_review_head_sha: str | None = None,
) -> ReviewCycleBoundary:
    """Resolve the durable progress boundary for review-iteration accounting."""
    if latest_completed_review is None:
        return ReviewCycleBoundary()

    latest_review_time = _task_event_time(latest_completed_review)
    reviewed_head_sha = latest_reviewed_head_sha or latest_completed_review.review_verify_head_sha
    if not reviewed_head_sha:
        return ReviewCycleBoundary()

    if current_review_head_sha and current_review_head_sha != reviewed_head_sha:
        progress_candidates: list[tuple[datetime, DbTask, str]] = []
        if (
            latest_completed_rebase is not None
            and latest_completed_rebase.changed_diff is not False
            and _task_event_time(latest_completed_rebase) > latest_review_time
        ):
            progress_candidates.append(
                (
                    _task_event_time(latest_completed_rebase),
                    latest_completed_rebase,
                    "rebase_changed_diff",
                )
            )
        if (
            latest_completed_code_change is not None
            and latest_completed_code_change.id != latest_completed_review.id
            and _task_event_time(latest_completed_code_change) > latest_review_time
        ):
            progress_candidates.append(
                (
                    _task_event_time(latest_completed_code_change),
                    latest_completed_code_change,
                    "code_change_after_review",
                )
            )
        if progress_candidates:
            boundary_time, boundary_task, boundary_reason = max(progress_candidates, key=lambda item: item[0])
            return ReviewCycleBoundary(
                boundary_time=boundary_time,
                boundary_task_id=boundary_task.id,
                boundary_reason=boundary_reason,
            )

    boundary_review = latest_completed_review
    for review in completed_reviews[1:]:
        if review.review_verify_head_sha != reviewed_head_sha:
            break
        boundary_review = review

    return ReviewCycleBoundary(
        boundary_time=_task_event_time(boundary_review),
        boundary_task_id=boundary_review.id,
        boundary_reason="reviewed_head_epoch",
    )


def count_completed_review_cycles_since_boundary(
    store: SqliteTaskStore,
    impl_task_id: str,
    *,
    boundary: ReviewCycleBoundary,
) -> int:
    """Count completed improves after a durable progress boundary."""
    if boundary.boundary_time is None:
        return count_completed_review_cycles(store, impl_task_id)

    improve_tasks = store.get_improve_tasks_by_root(impl_task_id)
    return sum(
        1
        for task in improve_tasks
        if task.status == "completed" and _task_event_time(task) > boundary.boundary_time
    )


def _has_tag(task: DbTask | None, tag: str) -> bool:
    return task is not None and tag in task.tags


def _count_consecutive_noop_improves(improve_tasks: list[DbTask]) -> tuple[DbTask | None, int]:
    latest_noop_improve: DbTask | None = None
    consecutive_noops = 0
    for improve in improve_tasks:
        if improve.status == "completed":
            if improve.changed_diff is False:
                if latest_noop_improve is None:
                    latest_noop_improve = improve
                consecutive_noops += 1
                continue
            break
        if improve.status in {"failed", "dropped", "unmerged"}:
            break
    return latest_noop_improve, consecutive_noops


def _latest_completed_noop_improve(improve_tasks: list[DbTask]) -> DbTask | None:
    return next(
        (
            improve
            for improve in improve_tasks
            if improve.status == "completed" and improve.changed_diff is False
        ),
        None,
    )


def _latest_completed_code_changing_improve(improve_tasks: list[DbTask]) -> DbTask | None:
    return next(
        (
            improve
            for improve in improve_tasks
            if improve.status == "completed" and improve.changed_diff is True
        ),
        None,
    )


def _latest_review_is_verify_blocked_only(ctx: AdvanceContext) -> bool:
    latest_review_blocker_summary = getattr(ctx, "latest_review_blocker_summary", None)
    return (
        ctx.review_verdict == "CHANGES_REQUESTED"
        and latest_review_blocker_summary is not None
        and latest_review_blocker_summary.is_verify_blocked_only
    )


def _noop_improve_kind(ctx: AdvanceContext) -> str:
    if _latest_review_is_verify_blocked_only(ctx):
        return NOOP_IMPROVE_KIND_VERIFY_ONLY
    return NOOP_IMPROVE_KIND_REAL_BLOCKER


def _resolve_branch_head_sha(git: Any, branch: str | None) -> BranchHeadResolution:
    if not branch:
        return BranchHeadResolution(head_sha=None)
    rev_parse_if_exists = getattr(git, "rev_parse_if_exists", None)
    if not callable(rev_parse_if_exists):
        return BranchHeadResolution(head_sha=None)
    try:
        head_sha = rev_parse_if_exists(branch)
    except Exception as exc:
        return BranchHeadResolution(
            head_sha=None,
            warning=(
                f"branch-head probe failed for {branch}: {exc}"
            ),
        )
    return BranchHeadResolution(
        head_sha=head_sha if isinstance(head_sha, str) and head_sha else None
    )


def _task_has_current_failed_review_verify_evidence(
    *,
    task: DbTask,
    review_task: DbTask,
    current_branch: str | None,
    current_head_sha: str | None,
) -> bool:
    if task.review_verify_status not in {"failed", "unavailable"}:
        return False
    if task.review_verify_captured_at is None:
        return False
    if review_task.completed_at is not None and task.review_verify_captured_at <= review_task.completed_at:
        return False
    if not current_branch or task.review_verify_branch != current_branch:
        return False
    if not current_head_sha or task.review_verify_head_sha != current_head_sha:
        return False
    return True


def _matching_review_verify_artifact(store: SqliteTaskStore, task: DbTask) -> TaskArtifact | None:
    if task.id is None:
        return None
    artifacts = store.list_artifacts(task.id, kind=VERIFY_COMMAND_OUTPUT_ARTIFACT_KIND)
    if not artifacts:
        return None
    if task.review_verify_artifact_file:
        for artifact in artifacts:
            if artifact.path == task.review_verify_artifact_file:
                return artifact
    return artifacts[0]


def _resolve_review_verify_artifact_path(
    *,
    config: Any,
    store: SqliteTaskStore,
    task: DbTask,
) -> Path | None:
    artifact = _matching_review_verify_artifact(store, task)
    stored_path = artifact.path if artifact is not None and artifact.path else task.review_verify_artifact_file
    if not stored_path:
        return None
    try:
        return resolve_artifact_path(Path(config.project_dir), stored_path)
    except InvalidArtifactPathError:
        return None


def _read_review_verify_output(
    *,
    config: Any,
    store: SqliteTaskStore,
    task: DbTask,
) -> str | None:
    artifact_path = _resolve_review_verify_artifact_path(config=config, store=store, task=task)
    if artifact_path is None:
        return None
    try:
        return artifact_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None


def _extract_review_verify_tree_fingerprint(
    *,
    config: Any,
    store: SqliteTaskStore,
    task: DbTask,
) -> str | None:
    artifact = _matching_review_verify_artifact(store, task)
    if artifact is not None:
        metadata = artifact.metadata if isinstance(artifact.metadata, dict) else None
        candidate = metadata.get("tree_fingerprint") if metadata is not None else None
        if isinstance(candidate, str) and candidate:
            return candidate
    for phase in _extract_review_verify_phase_results(
        _read_review_verify_output(config=config, store=store, task=task)
    ):
        candidate = phase.get("tree_fingerprint")
        if isinstance(candidate, str) and candidate:
            return candidate
    return None


def _build_review_verify_result_from_task(
    *,
    config: Any,
    store: SqliteTaskStore,
    task: DbTask,
) -> ReviewVerifyResult | None:
    if (
        not task.review_verify_command
        or not task.review_verify_status
        or not task.review_verify_exit_status
        or task.review_verify_captured_at is None
    ):
        return None
    return ReviewVerifyResult(
        command=task.review_verify_command,
        status=task.review_verify_status,
        exit_status=task.review_verify_exit_status,
        captured_at=task.review_verify_captured_at,
        reviewed_branch=task.review_verify_branch,
        reviewed_head_sha=task.review_verify_head_sha,
        reviewed_base_sha=task.review_verify_base_sha,
        working_directory=task.review_verify_cwd,
        failure=task.review_verify_failure,
        output=_read_review_verify_output(config=config, store=store, task=task),
    )


def _latest_current_passing_review_verify_task(
    *,
    improve_tasks: list[DbTask],
    review_task: DbTask,
    current_branch: str | None,
    current_head_sha: str | None,
    before: datetime | None,
) -> DbTask | None:
    for improve in improve_tasks:
        if not _task_has_current_passing_review_verify_evidence(
            task=improve,
            review_task=review_task,
            current_branch=current_branch,
            current_head_sha=current_head_sha,
        ):
            continue
        if before is not None and improve.review_verify_captured_at is not None and improve.review_verify_captured_at >= before:
            continue
        return improve
    return None


def _resolve_off_topic_changed_paths(
    *,
    config: Any,
    git: Any,
    branch: str,
    target_branch: str,
) -> tuple[str, ...] | None:
    get_diff_name_status = getattr(git, "get_diff_name_status", None)
    if not callable(get_diff_name_status):
        return None
    try:
        name_status_output = get_diff_name_status(f"{target_branch}...{branch}", check=True)
    except Exception:
        return None
    parsed_name_status = parse_name_status_project_paths(name_status_output or "")
    return tuple(
        sorted(
            _filter_owned_artifact_paths(
                parsed_name_status.changed_paths,
                boundary=_project_boundary(config),
            )
        )
    )


def _resolve_target_tree_fingerprint(git: Any, target_branch: str | None) -> str | None:
    if not target_branch:
        return None
    resolve_refs = getattr(git, "resolve_refs", None)
    if not callable(resolve_refs):
        return None
    try:
        resolved = resolve_refs((target_branch,), peel="tree")
    except Exception:
        return None
    candidate = resolved.get(target_branch)
    return candidate if isinstance(candidate, str) and candidate else None


def _resolve_baseline_worktree_root(config: Any) -> Path:
    worktree_dir = Path(getattr(config, "worktree_dir", "."))
    if not worktree_dir.is_absolute():
        worktree_dir = Path(config.project_dir) / worktree_dir
    return worktree_dir.resolve() / "off-topic-target-baselines"


def _normalized_failing_node_signature(node: Any) -> tuple[str, str]:
    assertion_signature = getattr(node, "assertion_signature", None)
    return (
        str(getattr(node, "nodeid", "")),
        assertion_signature if isinstance(assertion_signature, str) else "",
    )


def _baseline_run_matches_failure_signatures(
    *,
    red_result: ReviewVerifyResult,
    baseline_results: tuple[ReviewVerifyResult, ...],
) -> bool:
    from gza.off_topic_verify import parse_review_verify_failure_set

    red_failure_set = parse_review_verify_failure_set(red_result)
    if not red_failure_set.available or not red_failure_set.failing_nodes:
        return False
    expected_signatures = {
        _normalized_failing_node_signature(node)
        for node in red_failure_set.failing_nodes
    }
    matched_same_signature = False
    for baseline_result in baseline_results:
        baseline_failure_set = parse_review_verify_failure_set(baseline_result)
        if baseline_result.status == "passed":
            continue
        if not baseline_failure_set.available or not baseline_failure_set.failing_nodes:
            return False
        observed_signatures = {
            _normalized_failing_node_signature(node)
            for node in baseline_failure_set.failing_nodes
        }
        if observed_signatures != expected_signatures:
            return False
        matched_same_signature = True
    return matched_same_signature


def _classify_off_topic_noop_improve_verify_clearance(
    *,
    config: Any,
    store: SqliteTaskStore,
    git: Any,
    target_branch: str,
    task: DbTask,
    latest_completed_review: DbTask,
    improve_tasks: list[DbTask],
    current_head_sha: str,
    latest_completed_noop_improve: DbTask,
    persist: bool,
) -> tuple[bool, datetime | None, str | None, tuple[str, ...], tuple[str, ...]]:
    from gza.off_topic_verify import (
        classify_failure_diff_scope,
        parse_review_verify_failure_set,
        run_local_target_baseline_plan,
        select_local_target_baseline_plan,
    )

    if task.branch is None:
        return False, None, None, (), ()
    if not getattr(
        config,
        "advance_off_topic_verify_unblock",
        DEFAULT_ADVANCE_OFF_TOPIC_VERIFY_UNBLOCK,
    ):
        return False, None, None, (), ()
    if not _task_has_current_failed_review_verify_evidence(
        task=latest_completed_noop_improve,
        review_task=latest_completed_review,
        current_branch=task.branch,
        current_head_sha=current_head_sha,
    ):
        return False, None, None, (), ()
    red_result = _build_review_verify_result_from_task(
        config=config,
        store=store,
        task=latest_completed_noop_improve,
    )
    if red_result is None:
        return False, None, None, (), ()
    red_tree_fingerprint = _extract_review_verify_tree_fingerprint(
        config=config,
        store=store,
        task=latest_completed_noop_improve,
    )
    if red_tree_fingerprint is None:
        return False, None, None, (), ()
    green_task = _latest_current_passing_review_verify_task(
        improve_tasks=improve_tasks,
        review_task=latest_completed_review,
        current_branch=task.branch,
        current_head_sha=current_head_sha,
        before=latest_completed_noop_improve.review_verify_captured_at,
    )
    if green_task is None or green_task.id is None:
        return False, None, None, (), ()
    green_tree_fingerprint = _extract_review_verify_tree_fingerprint(
        config=config,
        store=store,
        task=green_task,
    )
    if green_tree_fingerprint is None or green_tree_fingerprint != red_tree_fingerprint:
        return False, None, None, (), ()
    if not persist:
        return False, None, None, (), ()
    target_head_sha = (
        git.rev_parse_if_exists(target_branch)
        if target_branch and callable(getattr(git, "rev_parse_if_exists", None))
        else None
    )
    target_tree_fingerprint = _resolve_target_tree_fingerprint(git, target_branch)
    if not target_branch or not target_head_sha or not target_tree_fingerprint:
        return False, None, None, (), ()
    changed_paths = _resolve_off_topic_changed_paths(
        config=config,
        git=git,
        branch=task.branch,
        target_branch=target_branch,
    )
    if changed_paths is None:
        return False, None, None, (), ()
    failure_set = parse_review_verify_failure_set(red_result)
    diff_scope = classify_failure_diff_scope(
        failure_set,
        changed_paths=changed_paths,
        repo_root=_project_boundary(config).repo_root,
    )
    if diff_scope.outcome != "off_topic":
        return False, None, None, (), ()
    selection = select_local_target_baseline_plan(
        failure_set,
        diff_scope,
        target_branch=target_branch,
        target_head_sha=target_head_sha,
        target_tree_fingerprint=target_tree_fingerprint,
        relative_cwd=_project_boundary(config).scope_root.as_posix() or ".",
    )
    if not selection.available or selection.plan is None:
        return False, None, None, (), ()
    try:
        baseline_run = run_local_target_baseline_plan(
            selection.plan,
            repo_git=git,
            worktree_root=_resolve_baseline_worktree_root(config),
            timeout_seconds=int(getattr(config, "autonomous_verify_timeout_seconds", 120)),
            timeout_grace_seconds=float(getattr(config, "review_verify_timeout_grace_seconds", 5.0)),
        )
    except Exception as exc:
        return False, None, f"off-topic local-target baseline failed for {target_branch}: {exc}", (), ()
    if not _baseline_run_matches_failure_signatures(
        red_result=red_result,
        baseline_results=baseline_run.results,
    ):
        return False, None, None, (), ()
    if latest_completed_review.id is None:
        return False, None, None, (), ()
    payload = {
        "reason": "off_topic_verify_failure",
        "implementation_task_id": task.id,
        "review_task_id": latest_completed_review.id,
        "green_task_id": green_task.id,
        "red_task_id": latest_completed_noop_improve.id,
        "head_sha": current_head_sha,
        "tree_fingerprint": red_tree_fingerprint,
        "verify_command": red_result.command,
        "target_branch": target_branch,
        "target_head_sha": target_head_sha,
        "target_tree_fingerprint": target_tree_fingerprint,
        "changed_paths": list(changed_paths),
        "baseline_mode": selection.plan.mode,
        "shared_global_paths": list(diff_scope.shared_global_paths),
        "failing_nodes": [
            {
                "nodeid": node.nodeid,
                "path": node.path,
                "assertion_signature": node.assertion_signature,
                "failure_path": node.failure_path,
                "failure_line": node.failure_line,
                "traceback_paths": list(node.traceback_paths),
            }
            for node in failure_set.failing_nodes
        ],
        "baseline_results": [
            {
                "status": result.status,
                "exit_status": result.exit_status,
                "captured_at": result.captured_at.isoformat(),
            }
            for result in baseline_run.results
        ],
    }
    try:
        persisted = persist_off_topic_verify_clearance(
            store,
            config=config,
            review_task=latest_completed_review,
            impl_task=task,
            payload=payload,
            trigger_source="advance_off_topic_verify_unblock",
            review_clearance_artifact_kind=REVIEW_CLEARANCE_ARTIFACT_KIND,
            review_clearance_artifact_label="review_clearance",
            review_clearance_artifact_producer="advance_off_topic_verify_unblock",
        )
    except Exception as exc:
        return False, None, str(exc), (), ()
    return (
        True,
        persisted.review_cleared_at,
        None,
        tuple(task.id for task in persisted.created_tasks if task.id is not None),
        tuple(task.id for task in persisted.reused_tasks if task.id is not None),
    )


def _resolve_noop_improve_verify_clearance(
    *,
    config: Any,
    store: SqliteTaskStore,
    git: Any,
    project_dir: Path,
    target_branch: str,
    task: DbTask,
    latest_completed_review: DbTask | None,
    improve_tasks: list[DbTask],
    persist: bool,
) -> tuple[bool, datetime | None, str | None, tuple[str, ...], tuple[str, ...]]:
    """Return verify-only review clearance and optionally persist it."""
    if latest_completed_review is None or task.branch is None or task.id is None:
        return False, None, None, (), ()

    branch_head = _resolve_branch_head_sha(git, task.branch)
    if branch_head.warning is not None:
        return False, None, branch_head.warning, (), ()

    current_head_sha = branch_head.head_sha
    if current_head_sha is None:
        return False, None, None, (), ()
    if not _review_is_verify_only_blocked_at_head(
        project_dir=project_dir,
        review_task=latest_completed_review,
        current_branch=task.branch,
        current_head_sha=current_head_sha,
    ):
        return False, None, None, (), ()

    latest_completed_noop_improve = _latest_completed_noop_improve(improve_tasks)
    if latest_completed_noop_improve is None:
        return False, None, None, (), ()

    if _task_has_current_passing_review_verify_evidence(
        task=latest_completed_noop_improve,
        review_task=latest_completed_review,
        current_branch=task.branch,
        current_head_sha=current_head_sha,
    ):
        matching_clearance = _latest_matching_verify_only_noop_review_clearance(
            store=store,
            task=task,
            latest_completed_review=latest_completed_review,
            current_head_sha=current_head_sha,
        )
        if matching_clearance is not None:
            return True, matching_clearance.created_at, None, (), ()
        if _latest_matching_verify_only_noop_recovery_attention(
            store=store,
            noop_improve_task=latest_completed_noop_improve,
            latest_completed_review=latest_completed_review,
            current_head_sha=current_head_sha,
        ) is not None:
            return False, None, None, (), ()
        return False, None, None, (), ()

    (
        cleared_off_topic,
        clearance_time,
        clearance_warning,
        created_investigation_task_ids,
        reused_investigation_task_ids,
    ) = _classify_off_topic_noop_improve_verify_clearance(
        config=config,
        store=store,
        git=git,
        target_branch=target_branch,
        task=task,
        latest_completed_review=latest_completed_review,
        improve_tasks=improve_tasks,
        current_head_sha=current_head_sha,
        latest_completed_noop_improve=latest_completed_noop_improve,
        persist=persist,
    )
    if cleared_off_topic:
        return (
            True,
            clearance_time,
            None,
            created_investigation_task_ids,
            reused_investigation_task_ids,
        )
    return False, None, clearance_warning, (), ()


def _latest_matching_verify_only_noop_review_clearance(
    *,
    store: SqliteTaskStore,
    task: DbTask,
    latest_completed_review: DbTask | None,
    current_head_sha: str | None,
) -> TaskArtifact | None:
    """Return the latest structured verify-only clearance for this review/head."""
    if task.id is None or latest_completed_review is None or latest_completed_review.id is None:
        return None
    if not current_head_sha:
        return None
    for artifact in store.list_artifacts(task.id, kind=REVIEW_CLEARANCE_ARTIFACT_KIND):
        metadata = artifact.metadata if isinstance(artifact.metadata, dict) else None
        if not is_verify_only_noop_review_clearance_status(artifact.status):
            continue
        if artifact.head_sha != current_head_sha:
            continue
        if metadata is None:
            continue
        if metadata.get("review_task_id") != latest_completed_review.id:
            continue
        is_verify_only_noop_clearance = (
            metadata.get("clearance_kind") == VERIFY_ONLY_NOOP_REVIEW_CLEARANCE_KIND
            and metadata.get("clearance_status") == VERIFY_ONLY_NOOP_REVIEW_CLEARANCE_STATUS
            and metadata.get("reviewed_head_sha") == current_head_sha
        )
        is_off_topic_verify_clearance = metadata.get("reason") == "off_topic_verify_failure"
        if not is_verify_only_noop_clearance and not is_off_topic_verify_clearance:
            continue
        if (
            latest_completed_review.completed_at is not None
            and artifact.created_at < latest_completed_review.completed_at
        ):
            continue
        return artifact
    return None


def _latest_matching_verify_only_noop_recovery_attention(
    *,
    store: SqliteTaskStore,
    noop_improve_task: DbTask | None,
    latest_completed_review: DbTask | None,
    current_head_sha: str | None,
) -> TaskArtifact | None:
    """Return the latest persisted verify-only recovery park for this review/head."""
    if (
        noop_improve_task is None
        or noop_improve_task.id is None
        or latest_completed_review is None
        or latest_completed_review.id is None
        or not current_head_sha
    ):
        return None
    for artifact in store.list_artifacts(
        noop_improve_task.id,
        kind=VERIFY_ONLY_NOOP_RECOVERY_ATTENTION_ARTIFACT_KIND,
    ):
        metadata = artifact.metadata if isinstance(artifact.metadata, dict) else None
        if artifact.status != VERIFY_ONLY_NOOP_RECOVERY_ATTENTION_STATUS:
            continue
        if artifact.head_sha != current_head_sha or metadata is None:
            continue
        if metadata.get("review_task_id") != latest_completed_review.id:
            continue
        if metadata.get("noop_improve_kind") != NOOP_IMPROVE_KIND_VERIFY_ONLY:
            continue
        if metadata.get("reviewed_head_sha") != current_head_sha:
            continue
        if metadata.get("attention_reason") != PARK_REASON_IMPROVE_NO_OP:
            continue
        if (
            latest_completed_review.completed_at is not None
            and artifact.created_at < latest_completed_review.completed_at
        ):
            continue
        return artifact
    return None


def _resolve_noop_improve_verify_recovery_attention_message(
    *,
    store: SqliteTaskStore,
    project_dir: Path,
    task: DbTask,
    latest_completed_review: DbTask | None,
    latest_completed_noop_improve: DbTask | None,
    current_head_sha: str | None,
) -> str | None:
    """Return durable parked messaging for failed verify-only no-op recovery attempts."""
    if (
        latest_completed_review is None
        or latest_completed_noop_improve is None
        or task.branch is None
        or not current_head_sha
        or not _review_is_verify_only_blocked_at_head(
            project_dir=project_dir,
            review_task=latest_completed_review,
            current_branch=task.branch,
            current_head_sha=current_head_sha,
        )
    ):
        return None
    persisted_attention = _latest_matching_verify_only_noop_recovery_attention(
        store=store,
        noop_improve_task=latest_completed_noop_improve,
        latest_completed_review=latest_completed_review,
        current_head_sha=current_head_sha,
    )
    if persisted_attention is not None:
        metadata = persisted_attention.metadata if isinstance(persisted_attention.metadata, dict) else None
        message = metadata.get("message") if metadata is not None else None
        if isinstance(message, str) and message.strip():
            return message
        return (
            "SKIP: fresh verify did not clear the verify-only no-op review blocker; "
            "manual attention is required."
        )
    if _task_has_current_failed_review_verify_evidence(
        task=latest_completed_noop_improve,
        review_task=latest_completed_review,
        current_branch=task.branch,
        current_head_sha=current_head_sha,
    ):
        return (
            "SKIP: fresh verify did not clear the verify-only no-op review blocker; "
            "manual attention is required."
        )
    if _task_has_current_passing_review_verify_evidence(
        task=latest_completed_noop_improve,
        review_task=latest_completed_review,
        current_branch=task.branch,
        current_head_sha=current_head_sha,
    ) and _latest_matching_verify_only_noop_review_clearance(
        store=store,
        task=task,
        latest_completed_review=latest_completed_review,
        current_head_sha=current_head_sha,
    ) is None:
        return (
            "SKIP: verify-only no-op recovery has current passing verify evidence for this tip, "
            "but the required structured review_clearance is missing. "
            "Manual attention is required before merge."
        )
    return None


def _load_persisted_review_verify_result(
    *,
    project_dir: Path,
    task: DbTask,
) -> ReviewVerifyResult | None:
    if (
        task.review_verify_command is None
        or task.review_verify_status is None
        or task.review_verify_exit_status is None
        or task.review_verify_captured_at is None
    ):
        return None
    output: str | None = None
    artifact_path = task.review_verify_artifact_file
    if artifact_path:
        resolved_path = (project_dir / artifact_path).resolve()
        try:
            output = resolved_path.read_text(encoding="utf-8")
        except OSError:
            output = None
    return _make_review_verify_result(
        task.review_verify_command,
        status=task.review_verify_status,
        exit_status=task.review_verify_exit_status,
        captured_at=task.review_verify_captured_at,
        reviewed_branch=task.review_verify_branch,
        reviewed_head_sha=task.review_verify_head_sha,
        reviewed_base_sha=task.review_verify_base_sha,
        working_directory=task.review_verify_cwd,
        failure=task.review_verify_failure,
        output=output,
    )


def _review_verify_tree_fingerprint(result: ReviewVerifyResult | None) -> str | None:
    if result is None:
        return None
    fingerprint: str | None = None
    for phase in _extract_review_verify_phase_results(result.output):
        candidate = phase.get("tree_fingerprint")
        if isinstance(candidate, str) and candidate:
            fingerprint = candidate
    return fingerprint


def _resolve_off_topic_verify_clearance_candidate(
    *,
    config: Any,
    store: SqliteTaskStore,
    git: Any,
    project_dir: Path,
    task: DbTask,
    target_branch: str,
    latest_completed_review: DbTask | None,
    improve_tasks: list[DbTask],
) -> OffTopicVerifyClearanceCandidate | None:
    if not getattr(config, "advance_off_topic_verify_unblock", False):
        return None
    if latest_completed_review is None or task.id is None or task.branch is None:
        return None
    if latest_completed_review.review_verify_status != "failed":
        return None
    if not _review_is_verify_only_blocked_at_head(
        project_dir=project_dir,
        review_task=latest_completed_review,
        current_branch=task.branch,
        current_head_sha=latest_completed_review.review_verify_head_sha,
    ):
        return None

    latest_completed_noop_improve = _latest_completed_noop_improve(improve_tasks)
    if latest_completed_noop_improve is None:
        return None
    if not _task_has_current_passing_review_verify_evidence(
        task=latest_completed_noop_improve,
        review_task=latest_completed_review,
        current_branch=task.branch,
        current_head_sha=latest_completed_review.review_verify_head_sha,
    ):
        return None

    review_result = _load_persisted_review_verify_result(
        project_dir=project_dir,
        task=latest_completed_review,
    )
    improve_result = _load_persisted_review_verify_result(
        project_dir=project_dir,
        task=latest_completed_noop_improve,
    )
    if review_result is None or improve_result is None:
        return None
    review_tree_fingerprint = _review_verify_tree_fingerprint(review_result)
    improve_tree_fingerprint = _review_verify_tree_fingerprint(improve_result)
    if (
        not review_tree_fingerprint
        or not improve_tree_fingerprint
        or review_tree_fingerprint != improve_tree_fingerprint
    ):
        return None
    if review_result.reviewed_head_sha != improve_result.reviewed_head_sha:
        return None

    try:
        name_status_output = git.get_diff_name_status(f"{target_branch}...{task.branch}", check=True)
    except Exception:
        return None
    parsed_name_status = parse_name_status_project_paths(name_status_output or "")
    if not parsed_name_status.changed_paths:
        return None

    failure_set = parse_review_verify_failure_set(review_result)
    diff_scope = classify_failure_diff_scope(
        failure_set,
        changed_paths=parsed_name_status.changed_paths,
        repo_root=project_dir,
    )
    if diff_scope.outcome != "off_topic" or diff_scope.shared_global_paths:
        return None

    if latest_completed_review.id is None:
        return None
    if review_result.reviewed_head_sha is None:
        return None

    merge_unit = store.resolve_merge_unit_for_task(task.id)
    evidences: list[FlakyInvestigationEvidence] = []
    for node in failure_set.failing_nodes:
        targeted_command = derive_flaky_targeted_command(
            verify_command=review_result.command,
            nodeids=(node.nodeid,),
        )
        if not targeted_command:
            return None
        evidences.append(
            FlakyInvestigationEvidence(
                node=node,
                dedup_key=normalize_flaky_investigation_dedup_key(
                    node.nodeid,
                    node.assertion_signature,
                ),
                review_task_id=latest_completed_review.id,
                impl_task_id=task.id,
                merge_unit_id=merge_unit.id if merge_unit is not None else None,
                reviewed_head_sha=review_result.reviewed_head_sha,
                tree_fingerprint=review_tree_fingerprint,
                observed_branch=task.branch,
                target_branch=target_branch,
                verify_command=review_result.command,
                targeted_command=targeted_command,
                working_directory=review_result.working_directory,
                branch_pass_fail_counts=failure_set.pass_fail_counts,
                xdist=failure_set.xdist,
                branch_verify_status=review_result.status,
                branch_verify_exit_status=review_result.exit_status,
            )
        )
    if not evidences:
        return None
    return OffTopicVerifyClearanceCandidate(
        review_task=latest_completed_review,
        reviewed_head_sha=review_result.reviewed_head_sha,
        tree_fingerprint=review_tree_fingerprint,
        evidences=tuple(evidences),
    )


def _review_has_current_verify_blocker_clearance(
    *,
    store: SqliteTaskStore,
    impl_task: DbTask,
    project_dir: Path,
    review_task: DbTask,
    current_branch: str | None,
    current_head_sha: str | None,
) -> bool:
    """Return whether structured same-head clearance clears current verify blockers."""
    if review_task.review_verify_status != "failed":
        return False
    if not current_branch or review_task.review_verify_branch != current_branch:
        return False
    if not current_head_sha or review_task.review_verify_head_sha != current_head_sha:
        return False
    blocker_summary = summarize_review_blockers(get_review_content(project_dir, review_task))
    if blocker_summary.verify_failure_count + blocker_summary.verify_timeout_count == 0:
        return False
    return (
        _latest_matching_verify_only_noop_review_clearance(
            store=store,
            task=impl_task,
            latest_completed_review=review_task,
            current_head_sha=current_head_sha,
        )
        is not None
    )


def _primary_blocker_fingerprint(
    report: ParsedReviewReport,
) -> tuple[tuple[str, str], str, str] | None:
    primary_blocker = next(
        (finding for finding in report.findings if finding.severity == "BLOCKER"),
        None,
    )
    if primary_blocker is None:
        return None
    return get_review_finding_fingerprint_details(primary_blocker)


def _completed_rebase_between(rebases: list[DbTask], older: DbTask, newer: DbTask) -> bool:
    older_time = _task_event_time(older)
    newer_time = _task_event_time(newer)
    return any(older_time < _task_event_time(rebase) <= newer_time for rebase in rebases)


def _count_duplicate_primary_blocker_streak(
    config: Any,
    reviews: list[DbTask],
    completed_rebases: list[DbTask],
) -> DuplicateBlockerStreak | None:
    completed_reviews = sorted(
        (review for review in reviews if review.status == "completed"),
        key=_task_event_time,
        reverse=True,
    )
    if not completed_reviews:
        return None

    latest_review = completed_reviews[0]
    latest_report = get_review_report(Path(config.project_dir), latest_review)
    if latest_report.verdict != "CHANGES_REQUESTED":
        return None

    latest_fingerprint = _primary_blocker_fingerprint(latest_report)
    if latest_fingerprint is None or latest_review.id is None:
        return None

    fingerprint, title, anchor = latest_fingerprint
    review_task_ids: list[str] = [latest_review.id]
    streak = 1
    newer_review = latest_review

    for older_review in completed_reviews[1:]:
        if _completed_rebase_between(completed_rebases, older_review, newer_review):
            break

        older_report = get_review_report(Path(config.project_dir), older_review)
        if older_report.verdict != "CHANGES_REQUESTED":
            break

        older_fingerprint = _primary_blocker_fingerprint(older_report)
        if older_fingerprint is None or older_review.id is None:
            break

        if older_fingerprint[0] != fingerprint:
            break

        streak += 1
        review_task_ids.append(older_review.id)
        newer_review = older_review
        if streak >= DUPLICATE_BLOCKER_REVIEW_CYCLES:
            return DuplicateBlockerStreak(
                cycles=streak,
                fingerprint=fingerprint,
                title=title,
                anchor=anchor,
                review_task_ids=tuple(review_task_ids),
            )

    return None


def _resolution_fingerprint_tuple(metadata: Mapping[str, Any]) -> tuple[str, str] | None:
    fingerprint = metadata.get("finding_fingerprint")
    if not isinstance(fingerprint, Mapping):
        return None
    title = fingerprint.get("title")
    anchor = fingerprint.get("anchor")
    if not isinstance(title, str) or not isinstance(anchor, str) or not title or not anchor:
        return None
    return (title, anchor)


def _latest_disputed_blocker_artifact_for_review(
    store: SqliteTaskStore,
    *,
    resolution_statuses: tuple[ReviewBlockerResolutionStatus, ...],
    latest_noop_improve: DbTask | None,
) -> ReviewBlockerAdjudicationCandidate | None:
    if latest_noop_improve is None or latest_noop_improve.id is None:
        return None

    for status in resolution_statuses:
        artifact = status.latest_artifact
        metadata = artifact.metadata if artifact is not None else None
        if status.state != "disputed" or artifact is None or metadata is None:
            continue
        if metadata.get("source_task_id") != latest_noop_improve.id:
            continue
        return ReviewBlockerAdjudicationCandidate(
            finding=status.finding,
            dispute_metadata=build_review_blocker_dispute_metadata(artifact),
            dispute_artifact=artifact,
        )

    return None


def _normalize_review_state_head_sha(*head_candidates: str | None) -> str | None:
    for candidate in head_candidates:
        if isinstance(candidate, str):
            normalized = candidate.strip()
            if normalized:
                return normalized
    return None


def _resolution_is_current_repeated_review_adjudication(
    artifact: TaskArtifact,
    *,
    current_review_head_sha: str | None,
    latest_reviewed_head_sha: str | None,
) -> bool:
    metadata = artifact.metadata or {}
    if metadata.get("reason") != "repeated_reviewer_request":
        return True
    if metadata.get("disputed_artifact_id") is not None:
        return True
    expected_head_sha = _normalize_review_state_head_sha(
        current_review_head_sha,
        latest_reviewed_head_sha,
    )
    artifact_head_sha = _normalize_review_state_head_sha(artifact.head_sha)
    if expected_head_sha is None or artifact_head_sha is None:
        return False
    return artifact_head_sha == expected_head_sha


def _duplicate_blocker_adjudication_candidate_for_review(
    *,
    impl_task: DbTask,
    latest_completed_review: DbTask | None,
    review_report: ParsedReviewReport | None,
    duplicate_blocker_streak: DuplicateBlockerStreak | None,
    latest_completed_code_change: DbTask | None,
    current_review_head_sha: str | None,
    latest_reviewed_head_sha: str | None,
) -> ReviewBlockerAdjudicationCandidate | None:
    if latest_completed_review is None or latest_completed_review.id is None:
        return None
    if review_report is None or review_report.verdict != "CHANGES_REQUESTED":
        return None
    if duplicate_blocker_streak is None:
        return None

    finding = next(
        (
            candidate
            for candidate in review_report.findings
            if candidate.severity == "BLOCKER"
            and get_review_finding_fingerprint(candidate) == duplicate_blocker_streak.fingerprint
        ),
        None,
    )
    if finding is None:
        return None
    if classify_review_blocker_finding(finding) != "code":
        return None

    source_task = latest_completed_code_change
    if source_task is None or source_task.id is None:
        source_task = impl_task
    if source_task.id is None:
        return None

    head_sha = _normalize_review_state_head_sha(
        current_review_head_sha,
        latest_reviewed_head_sha,
    )
    if head_sha is None:
        return None
    evidence = (
        f"Lifecycle observed the same blocker fingerprint across {duplicate_blocker_streak.cycles} "
        f"consecutive CHANGES_REQUESTED review cycles ({', '.join(duplicate_blocker_streak.review_task_ids)}). "
        f"Current implementation state comes from {source_task.id}; adjudicate whether the blocker "
        "remains valid on the current branch state rather than repeating the review/improve loop."
    )
    dispute_metadata: dict[str, Any] = {
        "source_task_id": source_task.id,
        "disputed_source_task_id": source_task.id,
        "reason": "repeated_reviewer_request",
        "evidence": evidence,
        "current_state_citation": duplicate_blocker_streak.anchor,
    }
    if source_task.branch:
        dispute_metadata["source_branch"] = source_task.branch
    dispute_metadata["head_sha"] = head_sha

    return ReviewBlockerAdjudicationCandidate(
        finding=finding,
        dispute_metadata=dispute_metadata,
    )


def _latest_review_blocker_resolution_statuses(
    store: SqliteTaskStore,
    *,
    git: Any,
    project_dir: Path,
    review_task: DbTask,
    impl_task: DbTask,
    findings: tuple[ReviewFinding, ...],
    improve_tasks: list[DbTask],
    allow_verify_clearance: bool = True,
) -> tuple[ReviewBlockerResolutionStatus, ...]:
    if review_task.id is None or impl_task.id is None:
        return ()

    branch_head = _resolve_branch_head_sha(git, impl_task.branch)
    current_head_sha = branch_head.head_sha
    latest_reviewed_head_sha = _normalize_review_state_head_sha(review_task.review_verify_head_sha)
    verify_blockers_cleared = (
        allow_verify_clearance
        and (
            _review_has_current_verify_blocker_clearance(
                store=store,
                impl_task=impl_task,
                project_dir=project_dir,
                review_task=review_task,
                current_branch=impl_task.branch,
                current_head_sha=current_head_sha,
            )
            if current_head_sha is not None
            else False
        )
    )

    blockers: list[tuple[ReviewFinding, str, tuple[str, str] | None]] = []
    for finding in findings:
        if finding.severity != "BLOCKER":
            continue
        finding_kind = classify_review_blocker_finding(finding)
        fingerprint_details = (
            get_review_finding_fingerprint_details(finding) if finding_kind == "code" else None
        )
        blockers.append(
            (
                finding,
                finding_kind,
                fingerprint_details[0] if fingerprint_details is not None else None,
            )
        )
    if not blockers:
        return ()

    artifacts_by_key: dict[tuple[str, tuple[str, str]], list[TaskArtifact]] = {}
    for artifact in store.list_artifacts(review_task.id, kind=REVIEW_BLOCKER_RESOLUTION_ARTIFACT_KIND):
        metadata = artifact.metadata or {}
        if metadata.get("impl_task_id") != impl_task.id:
            continue
        finding_id = metadata.get("finding_id")
        fingerprint = _resolution_fingerprint_tuple(metadata)
        if not isinstance(finding_id, str) or fingerprint is None:
            continue
        key = (finding_id, fingerprint)
        artifacts_by_key.setdefault(key, []).append(artifact)

    statuses: list[ReviewBlockerResolutionStatus] = []
    for finding, finding_kind, fingerprint in blockers:
        latest_artifact: TaskArtifact | None = None
        state: str | None = None
        if finding_kind == "code" and fingerprint is not None:
            relevant_artifacts = sorted(
                artifacts_by_key.get((finding.id, fingerprint), []),
                key=lambda artifact: _normalize_time(artifact.created_at),
            )
            latest_dispute = next(
                (
                    artifact
                    for artifact in reversed(relevant_artifacts)
                    if (artifact.metadata or {}).get("state") == "disputed"
                ),
                None,
            )
            if latest_dispute is not None:
                latest_dispute_metadata = latest_dispute.metadata or {}
                dispute_source_task_id = latest_dispute_metadata.get("source_task_id")
                dispute_head_sha = latest_dispute.head_sha
                latest_artifact = latest_dispute
                state = "disputed"
                for artifact in reversed(relevant_artifacts):
                    metadata = artifact.metadata or {}
                    raw_state = metadata.get("state")
                    if raw_state not in {"invalid", "valid", "needs_human"}:
                        continue
                    if not review_blocker_dispute_matches_current(
                        current_dispute_artifact=latest_dispute,
                        metadata=metadata,
                    ):
                        continue
                    if metadata.get("dispute_source_task_id") != dispute_source_task_id:
                        continue
                    if dispute_head_sha != artifact.head_sha:
                        continue
                    latest_artifact = artifact
                    state = raw_state
                    break
            elif relevant_artifacts:
                latest_artifact = relevant_artifacts[-1]
                metadata = latest_artifact.metadata or {}
                raw_state = metadata.get("state")
                if (
                    isinstance(raw_state, str)
                    and raw_state
                    and _resolution_is_current_repeated_review_adjudication(
                        latest_artifact,
                        current_review_head_sha=current_head_sha,
                        latest_reviewed_head_sha=latest_reviewed_head_sha,
                    )
                ):
                    state = raw_state
        elif finding_kind != "code" and verify_blockers_cleared:
            state = "verify_cleared"
        statuses.append(
            ReviewBlockerResolutionStatus(
                finding=finding,
                latest_artifact=latest_artifact,
                state=state,
            )
        )
    return tuple(statuses)


def _find_sibling_review_attention_candidate(
    *,
    config: Any,
    store: SqliteTaskStore,
    git: Any,
    impl_task: DbTask,
    completed_reviews: list[DbTask],
    latest_completed_review: DbTask | None,
) -> SiblingReviewAttentionCandidate | None:
    if latest_completed_review is None or latest_completed_review.id is None or impl_task.id is None:
        return None

    latest_summary = summarize_review_blockers(_get_review_output_content(config, latest_completed_review))
    if not latest_summary.is_verify_blocked_only:
        return None

    project_dir = Path(config.project_dir)
    branch_head = _resolve_branch_head_sha(git, impl_task.branch)
    if branch_head.warning is not None:
        return None
    current_head_sha = branch_head.head_sha
    latest_reviewed_head_sha = _normalize_review_state_head_sha(latest_completed_review.review_verify_head_sha)
    latest_review_is_current = (
        current_head_sha is not None
        and latest_reviewed_head_sha is not None
        and current_head_sha == latest_reviewed_head_sha
    )
    for sibling_review in completed_reviews[1:]:
        if sibling_review.id is None:
            continue
        improve_tasks = store.get_improve_tasks_for(impl_task.id, sibling_review.id)

        sibling_report = get_review_report(project_dir, sibling_review)
        if sibling_report.verdict != "CHANGES_REQUESTED":
            continue

        sibling_summary = summarize_review_blockers(_get_review_output_content(config, sibling_review))
        if sibling_summary.is_verify_blocked_only:
            continue

        latest_code_improve = _latest_completed_code_changing_improve(improve_tasks)
        if (
            latest_review_is_current
            and latest_completed_review.completed_at is not None
            and latest_code_improve is not None
            and sibling_review.completed_at is not None
            and _task_event_time(latest_code_improve) > _normalize_time(sibling_review.completed_at)
            and _normalize_time(latest_completed_review.completed_at) > _task_event_time(latest_code_improve)
            and not any(task.status in {"pending", "in_progress"} for task in improve_tasks)
        ):
            continue

        resolution_statuses = _latest_review_blocker_resolution_statuses(
            store,
            git=git,
            project_dir=project_dir,
            review_task=sibling_review,
            impl_task=impl_task,
            findings=sibling_report.findings,
            improve_tasks=improve_tasks,
            allow_verify_clearance=True,
        )
        if resolution_statuses and all(
            _review_blocker_status_clears_current_blocker(status) for status in resolution_statuses
        ):
            continue

        active_improve = next(
            (task for task in improve_tasks if task.status == "in_progress"),
            None,
        )
        if active_improve is None:
            active_improve = next(
                (task for task in improve_tasks if task.status == "pending"),
                None,
            )
        related_improve_task = active_improve
        if related_improve_task is None:
            completed_improves = [task for task in improve_tasks if task.status == "completed"]
            if completed_improves:
                related_improve_task = max(completed_improves, key=_task_event_time)

        return SiblingReviewAttentionCandidate(
            review_task=sibling_review,
            review_report=sibling_report,
            related_improve_task=related_improve_task,
        )
    return None


def _resolve_review_blocker_adjudication_task(
    store: SqliteTaskStore,
    *,
    review_task: DbTask | None,
    impl_task: DbTask,
    candidate: ReviewBlockerAdjudicationCandidate | None,
) -> DbTask | None:
    if review_task is None or review_task.id is None or impl_task.id is None or candidate is None:
        return None
    return find_existing_review_blocker_adjudication_task(
        store,
        review_task_id=review_task.id,
        impl_task_id=impl_task.id,
        finding_id=candidate.finding.id,
        dispute_source_task_id=None,
        dispute_head_sha=None,
        dispute_metadata=candidate.dispute_metadata,
    )


def _resolve_review_blocker_adjudication_state(
    store: SqliteTaskStore,
    *,
    review_task: DbTask | None,
    impl_task: DbTask,
    candidate: ReviewBlockerAdjudicationCandidate | None,
    review_blockers_invalidated: bool,
    review_blockers_revalidated: bool,
) -> tuple[DbTask | None, bool, DbTask | None]:
    active_adjudication = _resolve_review_blocker_adjudication_task(
        store,
        review_task=review_task,
        impl_task=impl_task,
        candidate=candidate,
    )
    adjudication_needed = False
    adjudication_needed_task: DbTask | None = None
    if active_adjudication is not None and active_adjudication.status in {"completed", "failed"}:
        if not review_blockers_invalidated and not review_blockers_revalidated:
            adjudication_needed = True
            adjudication_needed_task = active_adjudication
    return active_adjudication, adjudication_needed, adjudication_needed_task


def _review_blocker_status_clears_current_blocker(status: ReviewBlockerResolutionStatus) -> bool:
    return status.state in {"invalid", "verify_cleared"}


def _task_id(task: DbTask | None) -> str:
    """Render a task id for user-facing action descriptions."""
    if task is None or task.id is None:
        return "unknown"
    return task.id


def _require_task_id(task: DbTask) -> str:
    if task.id is None:
        raise AssertionError("task.id must be set before building advance actions")
    return task.id


def _rebase_change_reason(rebase_task: DbTask | None) -> str:
    if rebase_task is None or rebase_task.changed_diff is True:
        return "changed diff"
    return "change unknown"


def _rebase_create_review_description(rebase_task: DbTask | None) -> str:
    task_id = _task_id(rebase_task)
    if rebase_task is not None and rebase_task.changed_diff is None:
        return f"Create resolution review (rebase {task_id} change unknown)"
    return f"Create resolution review (rebase {task_id} changed diff)"


def _rebase_pending_review_description(review_task: DbTask | None, rebase_task: DbTask | None) -> str:
    return (
        f"Run pending resolution review {_task_id(review_task)} "
        f"(rebase {_task_id(rebase_task)} {_rebase_change_reason(rebase_task)})"
    )


def _rebase_wait_review_description(review_task: DbTask | None, rebase_task: DbTask | None) -> str:
    return (
        f"SKIP: resolution review {_task_id(review_task)} in progress "
        f"(rebase {_task_id(rebase_task)} {_rebase_change_reason(rebase_task)})"
    )


def _stale_review_create_review_description(ctx: AdvanceContext) -> str:
    if ctx.review_invalidation_reason == "branch_head_advanced":
        return "Create review (branch head advanced after latest review)"
    return _rebase_create_review_description(ctx.review_invalidated_by_rebase)


def _stale_review_pending_review_description(ctx: AdvanceContext) -> str:
    if ctx.review_invalidation_reason == "branch_head_advanced":
        return (
            f"Run pending review {_task_id(ctx.active_review)} "
            "(branch head advanced after latest review)"
        )
    return _rebase_pending_review_description(ctx.active_review, ctx.review_invalidated_by_rebase)


def _stale_review_wait_review_description(ctx: AdvanceContext) -> str:
    if ctx.review_invalidation_reason == "branch_head_advanced":
        return (
            f"SKIP: review {_task_id(ctx.active_review)} in progress "
            "(branch head advanced after latest review)"
        )
    return _rebase_wait_review_description(ctx.active_review, ctx.review_invalidated_by_rebase)


def _review_freshness_probe_failed_description(ctx: AdvanceContext) -> str:
    if ctx.current_review_head_probe_warning is None:
        return "SKIP: latest review freshness could not be verified"
    return (
        "SKIP: latest review freshness could not be verified because "
        f"{ctx.current_review_head_probe_warning}"
    )


def _resolution_review_metadata_invalid_action(ctx: AdvanceContext) -> dict[str, Any]:
    review_root_task = getattr(ctx, "review_root_task", None)
    subject_task = review_root_task if review_root_task is not None else ctx.task
    return with_needs_attention(
        {
            "type": "needs_discussion",
            "description": "SKIP: required resolution-review metadata is missing or malformed",
        },
        reason="resolution-review-metadata-invalid",
        subject_task_id=subject_task.id,
    )


def _resolve_resolution_review_metadata_shas(
    ctx: AdvanceContext,
    *,
    rebase_task: DbTask | None,
) -> tuple[str | None, str | None, str | None]:
    """Resolve resolution-review head/target SHAs from provenance or live refs."""
    if rebase_task is None or rebase_task.id is None:
        return None, None, None
    provenance = parse_rebase_diff_provenance(rebase_task.review_scope)
    resolved_head_sha = provenance.resolved_head_sha if provenance is not None else None
    if not resolved_head_sha:
        resolved_head_sha = _resolve_branch_head_sha(ctx.git, rebase_task.branch).head_sha
    resolved_target_sha = provenance.resolved_target_sha if provenance is not None else None
    if not resolved_target_sha:
        resolved_target_sha = _resolve_branch_head_sha(ctx.git, ctx.target_branch).head_sha
    return rebase_task.id, resolved_head_sha, resolved_target_sha


def _resolve_planned_resolution_review_metadata(ctx: AdvanceContext) -> tuple[str | None, str | None, str | None]:
    """Resolve the metadata required to create a new resolution review."""
    return _resolve_resolution_review_metadata_shas(ctx, rebase_task=ctx.review_invalidated_by_rebase)


def _planned_resolution_review_metadata_is_valid(ctx: AdvanceContext) -> bool:
    rebase_task_id, resolved_head_sha, resolved_target_sha = _resolve_planned_resolution_review_metadata(ctx)
    return bool(rebase_task_id and resolved_head_sha and resolved_target_sha)


def _stale_review_create_review_action(ctx: AdvanceContext) -> dict[str, Any]:
    action: dict[str, Any] = {
        "type": "create_review",
        "description": _stale_review_create_review_description(ctx),
    }
    if ctx.review_invalidation_reason == "rebase_changed_diff":
        rebase_task = ctx.review_invalidated_by_rebase
        if rebase_task is None:
            raise AssertionError("rebase-changed stale review requires review_invalidated_by_rebase")
        rebase_task_id, resolved_head_sha, resolved_target_sha = _resolve_planned_resolution_review_metadata(ctx)
        if not rebase_task_id or not resolved_head_sha or not resolved_target_sha:
            raise AssertionError("resolution review action requires validated rebase/head/target metadata")
        action.update(
            {
                "review_mode": "resolution",
                "resolution_rebase_task_id": rebase_task_id,
                "resolution_head_sha": resolved_head_sha,
                "resolution_target_sha": resolved_target_sha,
            }
        )
    return action


def _resolution_review_metadata_matches_context(
    metadata: ResolutionReviewScope | None,
    *,
    ctx: AdvanceContext,
    impl_task: DbTask,
    rebase_task: DbTask | None,
) -> bool:
    if metadata is None or impl_task.id is None or rebase_task is None or rebase_task.id is None:
        return False
    _, resolved_head_sha, resolved_target_sha = _resolve_resolution_review_metadata_shas(
        ctx,
        rebase_task=rebase_task,
    )
    if not resolved_head_sha or not resolved_target_sha:
        return False
    return (
        metadata.implementation_task_id == impl_task.id
        and metadata.rebase_task_id == rebase_task.id
        and metadata.resolved_head_sha == resolved_head_sha
        and metadata.resolved_target_sha == resolved_target_sha
    )


def _resolution_review_can_be_repaired_from_context(
    *,
    review_task: DbTask,
    rebase_task: DbTask | None,
) -> bool:
    if rebase_task is None or rebase_task.completed_at is None:
        return False
    return _task_event_time(review_task) > _normalize_time(rebase_task.completed_at)


def _repair_resolution_review_scope_from_context(
    store: SqliteTaskStore,
    *,
    review_task: DbTask,
    ctx: AdvanceContext,
    impl_task: DbTask,
    rebase_task: DbTask | None,
) -> ResolutionReviewScope | None:
    if impl_task.id is None or rebase_task is None or rebase_task.id is None:
        return None
    _, resolved_head_sha, resolved_target_sha = _resolve_resolution_review_metadata_shas(
        ctx,
        rebase_task=rebase_task,
    )
    if not resolved_head_sha or not resolved_target_sha:
        return None
    provenance = parse_rebase_diff_provenance(rebase_task.review_scope)
    rebuilt_scope = build_resolution_review_scope(
        implementation_task_id=impl_task.id,
        rebase_task_id=rebase_task.id,
        resolved_head_sha=resolved_head_sha,
        resolved_target_sha=resolved_target_sha,
        pre_rebase_head_sha=provenance.old_tip if provenance is not None else None,
        pre_rebase_target_sha=provenance.target_at_start if provenance is not None else None,
        pre_rebase_merge_base_sha=provenance.merge_base_at_start if provenance is not None else None,
    )
    if review_task.review_scope != rebuilt_scope:
        review_task.review_scope = rebuilt_scope
        store.update(review_task)
    return parse_resolution_review_scope(review_task.review_scope)


def _resolve_valid_resolution_review_metadata(
    store: SqliteTaskStore,
    *,
    review_task: DbTask,
    ctx: AdvanceContext,
    impl_task: DbTask,
    rebase_task: DbTask | None,
) -> ResolutionReviewScope | None:
    scope_text = review_task.review_scope
    try:
        metadata = parse_resolution_review_scope(scope_text)
    except ValueError:
        metadata = None
        parse_failed = True
    else:
        parse_failed = False
    if _resolution_review_metadata_matches_context(
        metadata,
        ctx=ctx,
        impl_task=impl_task,
        rebase_task=rebase_task,
    ):
        return metadata
    if not _resolution_review_can_be_repaired_from_context(
        review_task=review_task,
        rebase_task=rebase_task,
    ):
        return None
    if declares_spec_coherence_review_mode(scope_text):
        return None
    if parse_failed and declares_resolution_review_mode(scope_text):
        return None
    return _repair_resolution_review_scope_from_context(
        store,
        review_task=review_task,
        ctx=ctx,
        impl_task=impl_task,
        rebase_task=rebase_task,
    )


def _no_branch_description(ctx: AdvanceContext) -> str:
    if ctx.task.status == "completed":
        return f"SKIP: completed {ctx.task_type} task has no branch; no mergeable commits found"
    return f"SKIP: {ctx.task.status} {ctx.task_type} task has no branch; no merge action available"


def _target_already_merged_description(ctx: AdvanceContext) -> str:
    state = ctx.post_merge_rebase_state
    reason = state.reason if state is not None else None
    if reason == "merge-unit-empty":
        return f"SKIP: {terminal_no_work_lifecycle_detail('empty')}"
    if reason == "merge-unit-redundant":
        return f"SKIP: {terminal_no_work_lifecycle_detail('redundant')}"
    return f"SKIP: target implementation already merged ({reason or 'post-merge proof'})"


def _merge_terminal_description(ctx: AdvanceContext) -> str:
    detail = terminal_no_work_lifecycle_detail(getattr(ctx, "merge_state", None))
    if detail is not None:
        return f"SKIP: {detail}"
    return "SKIP: already merged into target branch"


def _empty_merge_state_description(ctx: AdvanceContext) -> str:
    detail = terminal_no_work_lifecycle_detail(getattr(ctx, "merge_state", None))
    return f"SKIP: {detail or terminal_no_work_lifecycle_detail('empty')}"


def _rebase_target_missing_merge_unit_description(ctx: AdvanceContext) -> str:
    state = ctx.post_merge_rebase_state
    reason = state.reason if state is not None else None
    return f"SKIP: rebase target has no merge unit ({reason or 'missing-merge-unit'})"


def _merge_review_description(verdict: str, preserved_rebase: DbTask | None) -> str:
    if preserved_rebase is None:
        return f"Merge (review {verdict})"
    return f"Merge (review {verdict}, preserved across rebase {_task_id(preserved_rebase)})"


def _merge_review_cleared_description(ctx: AdvanceContext) -> str:
    if getattr(ctx, "review_blockers_invalidated", False):
        return "Merge (review-blocker-invalid: disputed blocker invalidated by adjudication)"
    return "Merge (previous review addressed)"


def _merge_review_cleared_action(ctx: AdvanceContext) -> dict[str, Any]:
    """Return merge action for a cleared review, including off-topic investigation ids."""
    action: dict[str, Any] = {
        "type": "merge",
        "description": _merge_review_cleared_description(ctx),
    }
    created_ids = getattr(ctx, "created_investigation_task_ids", ())
    reused_ids = getattr(ctx, "reused_investigation_task_ids", ())
    if created_ids:
        action["created_investigation_task_ids"] = created_ids
    if reused_ids:
        action["reused_investigation_task_ids"] = reused_ids
    return action


def _clear_off_topic_verify_blocker_description(ctx: AdvanceContext) -> str:
    candidate = getattr(ctx, "off_topic_verify_clearance_candidate", None)
    if candidate is None:
        return "Clear verify-only review blocker as off-topic"
    return (
        "Clear verify-only review blocker as off-topic and create/reuse "
        f"{len(candidate.evidences)} investigation task(s)"
    )


def _clear_off_topic_verify_blocker_action(ctx: AdvanceContext) -> dict[str, Any]:
    candidate = getattr(ctx, "off_topic_verify_clearance_candidate", None)
    return {
        "type": "clear_off_topic_verify_blocker",
        "description": _clear_off_topic_verify_blocker_description(ctx),
        "review_task": None if candidate is None else candidate.review_task,
        "off_topic_verify_clearance_candidate": candidate,
    }


def _recover_verify_only_noop_review_description(ctx: AdvanceContext) -> str:
    latest_noop_id = _task_id(ctx.latest_noop_improve)
    return (
        "Fresh verify on current tip for verify-only no-op improve recovery "
        f"(latest {latest_noop_id})"
    )


def _recover_verify_only_noop_review_action(ctx: AdvanceContext) -> dict[str, Any]:
    current_branch_head_sha = ctx.current_review_head_sha
    if current_branch_head_sha is None:
        current_branch_head_sha = ctx.latest_reviewed_head_sha
    return {
        "type": "recover_verify_only_noop_review",
        "description": _recover_verify_only_noop_review_description(ctx),
        "review_task": ctx.latest_completed_review,
        "latest_noop_improve_task": ctx.latest_noop_improve,
        "current_branch_head_sha": current_branch_head_sha,
        "noop_improve_kind": NOOP_IMPROVE_KIND_VERIFY_ONLY,
    }


def _noop_improve_followup_suffix(ctx: AdvanceContext) -> str:
    if ctx.latest_noop_improve is None or ctx.consecutive_noop_improves <= 0:
        return ""
    return f"; previous no-op improve {_task_id(ctx.latest_noop_improve)} made no tracked diff change"


def _needs_attention_subject_id(ctx: AdvanceContext) -> str | None:
    """Prefer the owning implement (merge-unit owner) over a leaf sub-task."""
    owner = _resolve_owning_implementation_task(ctx.store, ctx.task)
    if owner is not None and owner.id:
        return owner.id
    return ctx.task.id


def _review_blocker_adjudication_description(ctx: AdvanceContext) -> str:
    candidate = ctx.review_blocker_adjudication_candidate
    finding_id = candidate.finding.id if candidate is not None else "unknown"
    return (
        "Create review-blocker adjudication for blocker "
        f"{finding_id} on review {_task_id(ctx.latest_completed_review)}"
    )


def _run_review_blocker_adjudication_description(adjudication_task: DbTask | None) -> str:
    return f"Spawn worker for review-blocker adjudication {_task_id(adjudication_task)}"


def _wait_review_blocker_adjudication_description(adjudication_task: DbTask | None) -> str:
    return f"SKIP: review-blocker adjudication {_task_id(adjudication_task)} is in_progress"


def _review_blocker_adjudication_needed_action(ctx: AdvanceContext) -> dict[str, Any]:
    adjudication_task = getattr(ctx, "review_blocker_adjudication_needed_task", None)
    task_id = _task_id(adjudication_task)
    status = adjudication_task.status if adjudication_task is not None else "unknown"
    if status == "completed":
        detail = "completed with an unparseable or unsafe result"
    elif status == "failed":
        detail = "failed"
    else:
        detail = f"returned {status}"
    return with_needs_attention(
        {
            "type": "needs_discussion",
            "description": (
                "SKIP: review-blocker-adjudication-needed; adjudication "
                f"{task_id} {detail}. Review the dispute evidence and resolve the blocker manually."
            ),
            "review_adjudication_task": adjudication_task,
            "noop_improve_kind": _noop_improve_kind(ctx),
        },
        reason=PARK_REASON_REVIEW_BLOCKER_ADJUDICATION_NEEDED,
        subject_task_id=_needs_attention_subject_id(ctx),
    )


def _format_blocker_titles_for_attention(report: ParsedReviewReport, *, limit: int = 2) -> str:
    blockers = [finding for finding in report.findings if finding.severity == "BLOCKER"]
    if not blockers:
        return "unresolved blockers"
    rendered = [f"{finding.id} {finding.title}" for finding in blockers[:limit]]
    if len(blockers) > limit:
        rendered.append(f"+{len(blockers) - limit} more")
    return "; ".join(rendered)


def _noop_improve_sibling_review_attention_action(ctx: AdvanceContext) -> dict[str, Any]:
    candidate = getattr(ctx, "sibling_review_attention_candidate", None)
    if candidate is None:
        return _noop_improve_needs_discussion_action(ctx)
    latest_noop_id = _task_id(ctx.latest_noop_improve)
    blocker_summary = _format_blocker_titles_for_attention(candidate.review_report)
    sibling_improve = candidate.related_improve_task
    if sibling_improve is None:
        sibling_state = "That review has no improve targeting it."
    elif sibling_improve.status == "completed":
        sibling_state = (
            f"Completed improve {_task_id(sibling_improve)} did not clear that review's current blockers."
        )
    elif sibling_improve.status == "pending":
        sibling_state = (
            f"Pending improve {_task_id(sibling_improve)} is already queued for that review, "
            "but merge must still wait for those blockers to clear."
        )
    else:
        sibling_state = (
            f"Improve {_task_id(sibling_improve)} is already in progress for that review, "
            "but merge must still wait for those blockers to clear."
        )
    return with_needs_attention(
        {
            "type": "needs_discussion",
            "description": (
                f"SKIP: no-op improve {latest_noop_id} belongs to verify-only review "
                f"{_task_id(ctx.latest_completed_review)}, but unresolved code blockers remain on "
                f"sibling review {_task_id(candidate.review_task)}: {blocker_summary}. "
                f"{sibling_state}"
            ),
            "review_task": candidate.review_task,
        },
        reason="improve-no-op",
        subject_task_id=_needs_attention_subject_id(ctx),
    )


def _requires_sibling_review_attention(ctx: AdvanceContext) -> bool:
    candidate = getattr(ctx, "sibling_review_attention_candidate", None)
    summary = getattr(ctx, "latest_review_blocker_summary", None)
    return candidate is not None and summary is not None and summary.is_verify_blocked_only


def _noop_improve_needs_discussion_action(ctx: AdvanceContext) -> dict[str, Any]:
    latest_noop_id = _task_id(ctx.latest_noop_improve)
    source = "unresolved comments remain open after" if ctx.noop_improve_trigger == "comments" else "review feedback remains unresolved after"
    return with_needs_attention(
        {
            "type": "needs_discussion",
            "description": (
                f"SKIP: {ctx.consecutive_noop_improves} consecutive no-op improves reached "
                f"(latest {latest_noop_id}); {source} no tracked diff change."
            ),
            "noop_improve_kind": _noop_improve_kind(ctx),
        },
        reason=PARK_REASON_IMPROVE_NO_OP,
        subject_task_id=_needs_attention_subject_id(ctx),
    )


def _noop_improve_verify_probe_failure_action(ctx: AdvanceContext) -> dict[str, Any]:
    assert ctx.noop_improve_verify_probe_warning is not None
    latest_noop_id = _task_id(ctx.latest_noop_improve)
    return with_needs_attention(
        {
            "type": "needs_discussion",
            "description": (
                f"SKIP: {ctx.consecutive_noop_improves} consecutive no-op improves reached "
                f"(latest {latest_noop_id}), but verify-only auto-clear could not be validated "
                f"because {ctx.noop_improve_verify_probe_warning}. manual attention is required. "
                "Review remains uncleared until lifecycle can revalidate the same-head clearance evidence."
            ),
            "probe_warning": ctx.noop_improve_verify_probe_warning,
            "noop_improve_kind": NOOP_IMPROVE_KIND_VERIFY_ONLY,
        },
        reason=PARK_REASON_IMPROVE_NO_OP,
        subject_task_id=_needs_attention_subject_id(ctx),
    )


def _noop_improve_verify_recovery_attention_action(ctx: AdvanceContext) -> dict[str, Any]:
    assert ctx.noop_improve_verify_recovery_attention_message is not None
    return with_needs_attention(
        {
            "type": "needs_discussion",
            "description": ctx.noop_improve_verify_recovery_attention_message,
            "noop_improve_kind": NOOP_IMPROVE_KIND_VERIFY_ONLY,
        },
        reason=PARK_REASON_IMPROVE_NO_OP,
        subject_task_id=_needs_attention_subject_id(ctx),
    )


def _verify_blocked_no_code_issues_action(ctx: AdvanceContext) -> dict[str, Any]:
    return with_needs_attention(
        {
            "type": "needs_discussion",
            "description": (
                f"SKIP: last {VERIFY_BLOCKED_REVIEW_THRESHOLD} review cycles only blocked on "
                "verify_command timeout; code may be correct but cannot be verified. "
                "Investigate test performance or verify_timeout config."
            ),
            "review_task": ctx.latest_completed_review,
            "noop_improve_kind": NOOP_IMPROVE_KIND_VERIFY_ONLY,
        },
        reason=PARK_REASON_VERIFY_BLOCKED_NO_CODE_ISSUES,
        subject_task_id=_needs_attention_subject_id(ctx),
    )


def _noop_improve_limit_action(ctx: AdvanceContext) -> dict[str, Any]:
    """Resolve a no-op improve limit using persisted clearance, probe failures, or refresh review."""
    if ctx.noop_improve_verify_probe_warning is not None:
        return _noop_improve_verify_probe_failure_action(ctx)
    if _requires_sibling_review_attention(ctx):
        return _noop_improve_sibling_review_attention_action(ctx)
    if getattr(ctx, "noop_improve_verify_recovery_attention_message", None) is not None:
        return _noop_improve_verify_recovery_attention_action(ctx)
    if (
        ctx.review_verdict == "CHANGES_REQUESTED"
        and ctx.latest_completed_review is not None
        and ctx.latest_noop_improve is not None
        and ctx.latest_reviewed_head_sha is not None
        and ctx.current_review_head_sha is not None
        and ctx.current_review_head_sha == ctx.latest_reviewed_head_sha
        and _latest_review_is_verify_blocked_only(ctx)
        and _task_has_current_passing_review_verify_evidence(
            task=ctx.latest_noop_improve,
            review_task=ctx.latest_completed_review,
            current_branch=ctx.task.branch,
            current_head_sha=ctx.current_review_head_sha,
        )
    ):
        return {
            "type": "create_review",
            "description": "Create review (required before merge)",
        }
    if (
        ctx.review_verdict == "CHANGES_REQUESTED"
        and len(ctx.recent_verify_timeout_only_reviews) >= VERIFY_BLOCKED_REVIEW_THRESHOLD
        and ctx.active_improve_running is None
        and ctx.active_improve_pending is None
    ):
        return _verify_blocked_no_code_issues_action(ctx)
    return _noop_improve_needs_discussion_action(ctx)


def _duplicate_blocker_needs_attention_action(ctx: AdvanceContext) -> dict[str, Any]:
    assert ctx.duplicate_blocker_streak is not None
    streak = ctx.duplicate_blocker_streak
    return with_needs_attention(
        {
            "type": "needs_discussion",
            "description": (
                f"SKIP: same review blocker repeated for {streak.cycles} consecutive review cycles; "
                "needs manual intervention"
            ),
            "duplicate_blocker": {
                "cycles": streak.cycles,
                "title": streak.title,
                "anchor": streak.anchor,
                "review_task_ids": streak.review_task_ids,
            },
        },
        reason=PARK_REASON_DUPLICATE_BLOCKER_NO_PROGRESS,
        subject_task_id=ctx.task.id,
    )


def _rebase_did_not_unblock_merge_action(ctx: AdvanceContext) -> dict[str, Any]:
    return with_needs_attention(
        {
            "type": "needs_discussion",
            "description": "SKIP: completed rebase did not unblock merge; manual decision required",
        },
        reason="rebase-did-not-unblock-merge",
        subject_task_id=ctx.task.id,
    )


def _resolve_strict_scope_inspection(
    config: Any,
    git: Any,
    task: DbTask,
    *,
    merge_source_ref: str | None,
    target_branch: str,
) -> StrictScopeInspection:
    """Return out-of-scope branch paths that should park automation for humans."""
    if not getattr(config, "enforce_project_scope", False):
        return StrictScopeInspection()
    if task.task_type not in {"task", "implement", "improve", "fix", "rebase"}:
        return StrictScopeInspection()
    if not merge_source_ref:
        return StrictScopeInspection()

    revision_range = f"{target_branch}...{merge_source_ref}"
    try:
        name_status_output = git.get_diff_name_status(revision_range, check=True)
    except Exception as exc:
        detail = " ".join(str(exc).split())
        _LOG.warning(
            "Failed to inspect branch diff for strict project scope (%s): %s",
            revision_range,
            detail,
        )
        return StrictScopeInspection(inspection_error=detail)

    parsed_name_status = parse_name_status_project_paths(name_status_output or "")
    if not parsed_name_status.changed_paths:
        return StrictScopeInspection()

    filtered_paths = _filter_owned_artifact_paths(
        parsed_name_status.changed_paths,
        boundary=_project_boundary(config),
    )
    return StrictScopeInspection(
        violation_paths=tuple(
            _find_out_of_scope_paths(
                config,
                filtered_paths,
                task=task,
                strict_scope=True,
                declared_project_roots=parsed_name_status.declared_project_roots,
            )
        )
    )


def _matches_spec_coherence_path(path: str, patterns: tuple[str, ...]) -> bool:
    normalized = path.replace("\\", "/").lstrip("./")
    return any(fnmatchcase(normalized, pattern) for pattern in patterns)


def _resolve_spec_coherence_inspection(
    config: Any,
    git: Any,
    task: DbTask,
    *,
    merge_source_ref: str | None,
    target_branch: str,
) -> SpecCoherenceInspection:
    """Return whether the branch diff triggers the behavior-spec coherence gate."""
    spec_config = getattr(config, "spec_coherence", None)
    if not getattr(spec_config, "enabled", False):
        return SpecCoherenceInspection()
    patterns = tuple(getattr(spec_config, "paths", ()) or ())
    if not patterns or task.task_type != "implement" or not merge_source_ref:
        return SpecCoherenceInspection()

    revision_range = f"{target_branch}...{merge_source_ref}"
    try:
        name_status_output = git.get_diff_name_status(revision_range, check=True)
    except Exception as exc:
        detail = " ".join(str(exc).split())
        _LOG.warning(
            "Failed to inspect branch diff for spec coherence (%s): %s",
            revision_range,
            detail,
        )
        return SpecCoherenceInspection(required=True, inspection_error=detail)

    parsed_name_status = parse_name_status_project_paths(name_status_output or "")
    if not parsed_name_status.changed_paths:
        return SpecCoherenceInspection()

    matched_paths = tuple(
        path
        for path in parsed_name_status.changed_paths
        if _matches_spec_coherence_path(path, patterns)
    )
    if not matched_paths:
        return SpecCoherenceInspection()
    return SpecCoherenceInspection(required=True, changed_paths=matched_paths)


def _strict_scope_violation_action(ctx: AdvanceContext) -> dict[str, Any]:
    violation_paths = tuple(getattr(ctx, "strict_scope_violation_paths", ()))
    paths = ", ".join(violation_paths)
    description = (
        "SKIP: cross-project branch includes paths outside all discovered project roots: "
        f"{paths}. Fix the branch or add project configs so the affected roots are discoverable."
        if _task_is_cross_project(ctx.task)
        else (
            "SKIP: branch includes out-of-scope paths outside the strict project scope: "
            f"{paths}. Tag `{CROSS_PROJECT_TAG}` and re-advance if intended, or fix the branch."
        )
    )
    return with_needs_attention(
        {
            "type": "needs_discussion",
            "description": description,
            "failure_reason": PROJECT_SCOPE_VIOLATION_FAILURE_REASON,
            "out_of_scope_paths": violation_paths,
        },
        reason="project-scope-violation",
        subject_task_id=ctx.task.id,
    )


def _strict_scope_unverified_action(ctx: AdvanceContext) -> dict[str, Any]:
    detail = getattr(ctx, "strict_scope_inspection_error", None) or "unknown diff inspection failure"
    merge_source_ref = getattr(ctx, "merge_source_ref", None) or ctx.task.branch or "unknown"
    return with_needs_attention(
        {
            "type": "needs_discussion",
            "description": (
                "SKIP: strict project scope could not be verified for branch diff "
                f"`{merge_source_ref}`. No automation will proceed until the diff/ref problem is fixed "
                f"or the task is tagged `{CROSS_PROJECT_TAG}` if the wider scope is intended. "
                f"Inspection error: {detail}"
            ),
            "strict_scope_inspection_error": detail,
        },
        reason="project-scope-unverified",
        subject_task_id=ctx.task.id,
    )


def _branch_contains_target_tip(ctx: AdvanceContext) -> bool:
    state = ctx.post_merge_rebase_state
    if state is None:
        return False
    return (
        bool(getattr(state, "rebase_resolution_proved", False))
        or getattr(state, "target_is_ancestor_of_branch", None) is True
    )


def _rebase_failure_circuit_breaker_action(ctx: AdvanceContext) -> dict[str, Any]:
    assert ctx.rebase_failure_streak is not None
    streak = ctx.rebase_failure_streak
    latest_failed_id = streak.failed_task_ids[0] if streak.failed_task_ids else _task_id(ctx.rebase_failed)
    return with_needs_attention(
        {
            "type": "needs_discussion",
            "description": (
                f"SKIP: rebase circuit breaker tripped for branch '{streak.branch}' after "
                f"{streak.attempts} failed attempts with no intervening successful rebase, review, "
                f"or code change (latest {latest_failed_id}); manual intervention required"
            ),
            "rebase_failure_streak": {
                "attempts": streak.attempts,
                "branch": streak.branch,
                "failed_task_ids": streak.failed_task_ids,
            },
        },
        reason="rebase-failure-circuit-breaker",
        subject_task_id=_needs_attention_subject_id(ctx),
    )


def _already_rebased_but_lineage_incomplete_action(ctx: AdvanceContext) -> dict[str, Any]:
    branch = ctx.task.branch or "unknown"
    return with_needs_attention(
        {
            "type": "needs_discussion",
            "description": (
                f"SKIP: branch '{branch}' already contains the target tip, but "
                f"{ctx.task.status} {ctx.task_type} {_task_id(ctx.task)} is still incomplete; "
                "no further rebase will help"
            ),
        },
        reason="branch-already-rebased-lineage-incomplete",
        subject_task_id=_needs_attention_subject_id(ctx),
    )


def _failed_task_skip_action(ctx: AdvanceContext) -> dict[str, Any]:
    assert ctx.failed_recovery_decision is not None
    subject_task_id = ctx.task.id
    if ctx.task.task_type in {"review", "improve", "rebase", "fix"} and ctx.task.id is not None:
        implement_task = _resolve_owning_implementation_task(ctx.store, ctx.task)
        if implement_task is not None and implement_task.id is not None:
            subject_task_id = implement_task.id
    return failed_recovery_decision_to_action(
        ctx.task,
        ctx.failed_recovery_decision,
        needs_attention_reason=ctx.failed_recovery_attention_reason,
        subject_task_id=subject_task_id,
    )


def _failed_task_resume_or_retry_action(ctx: AdvanceContext) -> dict[str, Any]:
    assert ctx.failed_recovery_decision is not None
    if ctx.failed_recovery_decision.action == "reconcile":
        return failed_recovery_decision_to_action(ctx.task, ctx.failed_recovery_decision)
    if ctx.task.task_type == "rebase":
        return failed_recovery_decision_to_action(ctx.task, ctx.failed_recovery_decision)
    if not ctx.task.branch:
        return failed_recovery_decision_to_action(ctx.task, ctx.failed_recovery_decision)
    if not ctx.task.has_commits:
        return failed_recovery_decision_to_action(ctx.task, ctx.failed_recovery_decision)

    rebase_parent_task = _resolve_recovery_preflight_rebase_parent_task(ctx.store, ctx.task)
    same_branch_rebases = _get_same_branch_rebase_descendants_for_root(ctx.store, rebase_parent_task)
    active_same_branch_rebase = next(
        (
            rebase
            for rebase in same_branch_rebases
            if rebase.status in {"pending", "in_progress"}
        ),
        None,
    )
    deferred_action = failed_recovery_decision_to_action(ctx.task, ctx.failed_recovery_decision)
    recovery_preflight_metadata = {
        "deferred_action_type": deferred_action["type"],
        "failed_task_id": ctx.task.id,
        "recovery_task_id": ctx.failed_recovery_decision.recovery_task_id,
        "rebase_parent_task_id": rebase_parent_task.id,
        "rebase_parent_task_type": rebase_parent_task.task_type,
        "rebase_parent_branch": rebase_parent_task.branch,
        "reason": "recovery-preflight-rebase",
    }
    if active_same_branch_rebase is not None:
        return {
            "type": "skip",
            "description": f"SKIP: rebase {_task_id(active_same_branch_rebase)} already in progress",
            "recovery_task_id": ctx.failed_recovery_decision.recovery_task_id,
            "reason": "recovery-preflight-rebase",
            "active_rebase_task_id": active_same_branch_rebase.id,
            "recovery_preflight": recovery_preflight_metadata,
        }
    if not _branch_contains_target_tip(ctx):
        return {
            "type": "needs_rebase",
            "description": "Rebase before failed-task recovery",
            "reason": "recovery-preflight-rebase",
            "deferred_action_type": deferred_action["type"],
            "failed_task_id": ctx.task.id,
            "recovery_task_id": ctx.failed_recovery_decision.recovery_task_id,
            "rebase_parent_task_id": rebase_parent_task.id,
            "recovery_preflight": recovery_preflight_metadata,
        }
    return failed_recovery_decision_to_action(ctx.task, ctx.failed_recovery_decision)


def _resolve_recovery_preflight_rebase_parent_task(store: SqliteTaskStore, task: DbTask) -> DbTask:
    """Attach recovery-preflight rebases to the canonical same-branch implementation lineage."""
    if task.id is None or not task.branch:
        return task
    if task.task_type == "rebase":
        resolved_target = resolve_rebase_target_task(store, task)
        if resolved_target is not None:
            return resolved_target
        return task
    return resolve_same_branch_lineage_root(store, task)


def with_needs_attention(
    action: Mapping[str, Any],
    *,
    reason: str,
    subject_task_id: str | None,
) -> dict[str, Any]:
    """Attach shared needs-attention metadata to an advance action."""
    if not subject_task_id:
        raise AssertionError("needs-attention actions require subject_task_id")
    annotated = dict(action)
    annotated["needs_attention_reason"] = reason
    annotated["subject_task_id"] = subject_task_id
    return annotated


def _resolve_owning_implementation_task(store: SqliteTaskStore, task: DbTask) -> DbTask | None:
    if task.task_type == "implement":
        return task

    for ancestor in walk_ancestors(store, task, follow_based_on=True, follow_depends_on=True):
        if ancestor.task_type == "implement":
            return ancestor

    lineage_root = resolve_lineage_root(store, task)
    if lineage_root.task_type == "implement":
        return lineage_root
    return None


def get_action_subject_task_id(action: Mapping[str, Any]) -> str | None:
    value = action.get("subject_task_id")
    if isinstance(value, str) and value.strip():
        return value
    return None


def require_needs_attention_subject(action: Mapping[str, Any]) -> str:
    if classify_advance_action(action) != "needs_attention":
        return ""
    subject_task_id = get_action_subject_task_id(action)
    if subject_task_id is None:
        raise AssertionError(f"needs-attention action missing subject_task_id: {action!r}")
    return subject_task_id


def resolve_subject_task(
    store: SqliteTaskStore,
    action: Mapping[str, Any],
    row: Any | None = None,
    *,
    fallback_task: DbTask | None = None,
) -> DbTask:
    raw_subject_task_id = action.get("subject_task_id")
    subject_task_id = get_action_subject_task_id(action)
    if subject_task_id is not None:
        subject_task = store.get(subject_task_id)
        if subject_task is not None and subject_task.id == subject_task_id:
            if row is None or _subject_matches_row_lineage(store, subject_task, row):
                return subject_task
            _LOG.warning("Ignoring subject_task_id=%s outside row lineage for action %s", subject_task_id, action)
        else:
            _LOG.warning("Ignoring invalid subject_task_id=%r for action %s", subject_task_id, action)
    elif classify_advance_action(action) == "needs_attention":
        if raw_subject_task_id is None:
            _LOG.warning(
                "Falling back for needs-attention action without subject_task_id: %s",
                action,
            )
        else:
            _LOG.warning(
                "Falling back for needs-attention action with unusable subject_task_id=%r: %s",
                raw_subject_task_id,
                action,
            )
    if fallback_task is not None:
        return _resolve_subject_fallback_task(store, row, fallback_task=fallback_task)
    raise AssertionError(f"Unable to resolve subject task for action: {action!r}")


def _resolve_subject_fallback_task(
    store: SqliteTaskStore,
    row: Any | None,
    *,
    fallback_task: DbTask,
) -> DbTask:
    superseding_carrier = _resolve_superseding_recovery_carrier(store, row, fallback_task=fallback_task)
    if superseding_carrier is not None:
        return superseding_carrier
    return fallback_task


def _resolve_superseding_recovery_carrier(
    store: SqliteTaskStore,
    row: Any | None,
    *,
    fallback_task: DbTask,
) -> DbTask | None:
    failed_owner_id = fallback_task.id
    if row is None or failed_owner_id is None or fallback_task.status != "failed":
        return None

    owner_branch = (fallback_task.branch or "").strip()
    owner_unit = store.resolve_merge_unit_for_task(failed_owner_id)
    for candidate in (
        getattr(row, "lifecycle_action_task", None),
        getattr(row, "recovery_action_task", None),
    ):
        if not isinstance(candidate, DbTask):
            continue
        candidate_id = candidate.id
        if candidate_id is None or candidate_id == failed_owner_id:
            continue
        if candidate.status in {"failed", "dropped"}:
            continue
        if not _task_descends_from(store, candidate, failed_owner_id):
            continue
        candidate_branch = (candidate.branch or "").strip()
        if owner_branch and candidate_branch and owner_branch == candidate_branch:
            return candidate
        candidate_unit = store.resolve_merge_unit_for_task(candidate_id)
        if owner_unit is not None and candidate_unit is not None and candidate_unit.id == owner_unit.id:
            return candidate
    return None


def _task_descends_from(store: SqliteTaskStore, task: DbTask, ancestor_id: str) -> bool:
    current = task
    seen: set[str] = set()
    while current.based_on:
        parent_id = current.based_on
        if parent_id == ancestor_id:
            return True
        if parent_id in seen:
            return False
        seen.add(parent_id)
        parent = store.get(parent_id)
        if parent is None or parent.id is None:
            return False
        current = parent
    return False


def _subject_matches_row_lineage(store: SqliteTaskStore, subject_task: DbTask, row: Any) -> bool:
    candidate_ids: set[str] = set()
    for task in (
        getattr(row, "owner_task", None),
        *(getattr(row, "members", ()) or ()),
        *(getattr(row, "unresolved_tasks", ()) or ()),
        getattr(row, "lifecycle_action_task", None),
        getattr(row, "recovery_action_task", None),
        getattr(row, "recovery_leaf_task", None),
    ):
        if isinstance(task, DbTask) and task.id is not None:
            candidate_ids.add(task.id)
    if subject_task.id in candidate_ids:
        return True
    if not candidate_ids or subject_task.id is None:
        return False
    subject_root_id = resolve_lineage_root(store, subject_task).id
    return any(
        resolve_lineage_root(store, candidate).id == subject_root_id
        for candidate_id in candidate_ids
        if (candidate := store.get(candidate_id)) is not None and candidate.id is not None
    )


def failed_recovery_decision_to_action(
    task: DbTask,
    decision: FailedRecoveryDecision,
    *,
    needs_attention_reason: str | None = None,
    subject_task_id: str | None = None,
) -> dict[str, Any]:
    """Convert a shared failed-task recovery decision into an advance action dict."""
    description = f"SKIP: {decision.reason_text}"
    failure_reason = decision.reason_code or task.failure_reason or "UNKNOWN"
    if decision.action == "resume":
        description = f"Resume failed task ({failure_reason})"
    elif decision.action == "retry":
        description = f"Retry failed task ({failure_reason})"
    elif decision.action == "reconcile":
        description = f"Reconcile branch publication ({failure_reason})"
    action_type = "reconcile_branch_divergence" if decision.action == "reconcile" else decision.action
    action: dict[str, Any] = {
        "type": action_type,
        "description": description,
        "recovery_task_id": decision.recovery_task_id,
        "reuse_existing": decision.reuse_existing,
        "launch_mode": decision.launch_mode,
        "reason_code": decision.reason_code,
        "attempt_index": decision.attempt_index,
        "attempt_limit": decision.attempt_limit,
        "decision": decision,
    }
    if needs_attention_reason is not None:
        action["needs_attention_reason"] = needs_attention_reason
        subject_task_id = subject_task_id or task.id
        if not isinstance(subject_task_id, str) or not subject_task_id.strip():
            raise AssertionError("needs-attention failed-recovery actions require a valid subject_task_id")
    if subject_task_id is not None:
        action["subject_task_id"] = subject_task_id
    return action


def failed_recovery_decision_to_attention_action(
    store: SqliteTaskStore,
    task: DbTask,
    decision: FailedRecoveryDecision,
    *,
    max_recovery_attempts: int,
    read_context: RecoveryReadContext | None = None,
) -> dict[str, Any] | None:
    """Convert a terminal failed-task recovery stop into the shared attention action."""
    attention_reason = get_failed_recovery_needs_attention_reason(
        store,
        task,
        decision=decision,
        max_recovery_attempts=max_recovery_attempts,
        read_context=read_context,
    )
    action = failed_recovery_decision_to_action(
        task,
        decision,
        needs_attention_reason=attention_reason,
        subject_task_id=task.id,
    )
    if classify_advance_action(action) != "needs_attention":
        return None
    return action


def _default_subject_for_attention_action(ctx: AdvanceContext, action: Mapping[str, Any]) -> dict[str, Any]:
    annotated = dict(action)
    if classify_advance_action(annotated) == "needs_attention" and get_action_subject_task_id(annotated) is None:
        annotated["subject_task_id"] = ctx.task.id
    return annotated


def get_needs_attention_reason(action: Mapping[str, Any]) -> str | None:
    value = action.get("needs_attention_reason")
    if isinstance(value, str) and value:
        return value
    action_type = str(action.get("type", ""))
    legacy_reasons = {
        "awaiting_human": "awaiting-human-review",
        "needs_discussion": "needs-discussion",
        "max_cycles_reached": "review-max-cycles-reached",
        "max_improve_attempts": "max-improve-attempts-reached",
        "automatic_recovery_disabled": "automatic-recovery-disabled",
        "manual_review_required": "manual-review-required",
    }
    return legacy_reasons.get(action_type)


def is_needs_attention_action(action: Mapping[str, Any]) -> bool:
    return get_needs_attention_reason(action) is not None


def _resolve_needs_attention_subject_task(
    store: SqliteTaskStore,
    task: DbTask,
    action: Mapping[str, Any],
) -> DbTask:
    subject_task_id = get_action_subject_task_id(action)
    if isinstance(subject_task_id, str) and subject_task_id and subject_task_id != task.id:
        subject_task = store.get(subject_task_id)
        if subject_task is not None:
            return subject_task
    return task


def _resolve_failed_recovery_fix_task_id(
    store: SqliteTaskStore,
    task: DbTask,
    action: Mapping[str, Any],
) -> str | None:
    reason = get_needs_attention_reason(action)
    if reason is None or task.id is None:
        return None
    impl_task, resolve_error = resolve_impl_task(store, task.id)
    resolved_impl = impl_task if resolve_error is None and impl_task is not None else None
    if reason == "review-max-cycles-reached":
        return resolved_impl.id if resolved_impl is not None and resolved_impl.id is not None else task.id
    if reason not in FAILED_RECOVERY_FIX_HANDOFF_NEEDS_ATTENTION_REASONS:
        return None
    if resolved_impl is None or resolved_impl.status != "completed" or resolved_impl.id is None:
        return None
    failed_task = _resolve_needs_attention_subject_task(store, task, action)
    if (
        reason in FAILED_RECOVERY_REARM_RECOMMENDATION_NEEDS_ATTENTION_REASONS
        and classify_failure_reason(failed_task.failure_reason or "UNKNOWN") == "retryable"
    ):
        return None
    return resolved_impl.id


def needs_attention_recommends_fix(
    store: SqliteTaskStore,
    task: DbTask,
    action: Mapping[str, Any],
) -> bool:
    """Return True when the operator handoff for this attention state is `gza fix`."""
    return _resolve_failed_recovery_fix_task_id(store, task, action) is not None


def needs_attention_recommended_next_step(
    store: SqliteTaskStore,
    task: DbTask,
    action: Mapping[str, Any],
) -> str | None:
    """Return the operator next-step line for a needs-attention action, if any."""
    reason = get_needs_attention_reason(action)
    if reason is None or task.id is None:
        return None
    fix_task_id = _resolve_failed_recovery_fix_task_id(store, task, action)
    if fix_task_id is not None:
        return f"Recommended next step: uv run gza fix {fix_task_id}"
    if reason not in FAILED_RECOVERY_FIX_HANDOFF_NEEDS_ATTENTION_REASONS:
        return None
    if reason in FAILED_RECOVERY_REARM_RECOMMENDATION_NEEDS_ATTENTION_REASONS:
        impl_task, resolve_error = resolve_impl_task(store, task.id)
        resolved_impl = impl_task if resolve_error is None and impl_task is not None else None
        failed_task = _resolve_needs_attention_subject_task(store, task, action)
        if (
            resolved_impl is not None
            and resolved_impl.status == "completed"
            and resolved_impl.id is not None
            and classify_failure_reason(failed_task.failure_reason or "UNKNOWN") == "retryable"
        ):
            return f"Recommended next step: uv run gza unstick {resolved_impl.id} --reason retry-limit --run"
    return FAILED_RECOVERY_RETRY_OR_REIMPLEMENT_NEXT_STEP


def is_diverged_merge_source_warning(warning: str | None) -> bool:
    """Return True when the merge-source warning indicates local/remote divergence."""
    return isinstance(warning, str) and "diverged" in warning.lower()


def classify_advance_action(action: Mapping[str, Any]) -> str:
    """Bucket advance outcomes into actionable, needs_attention, or skip."""
    if is_needs_attention_action(action):
        return "needs_attention"
    action_type = str(action.get("type", "skip"))
    if action_type in {
        "skip",
        "wait_review",
        "wait_improve",
        "wait_plan_review",
        "wait_plan_improve",
        "wait_review_adjudication",
    }:
        return "skip"
    return "actionable"


def format_needs_attention_entry(task: DbTask, *, prompt: str, action: Mapping[str, Any]) -> str:
    """Render a stable needs-attention line shared by advance/watch/iterate."""
    description = str(action.get("description", "")).strip()
    if description.startswith("SKIP: "):
        description = description[len("SKIP: ") :]
    reason = get_needs_attention_reason(action) or "needs-attention"
    task_id = task.id or "unknown"
    task_type = task.task_type or "task"
    return f'{task_id} {task_type} "{prompt}" reason={reason} {description}'


def format_needs_attention_entry_for_display(
    task: DbTask,
    *,
    action: Mapping[str, Any],
    prefix: int = 0,
    suffix: int = 0,
) -> str:
    """Render a needs-attention line with the shared single-line short prompt."""
    return build_needs_attention_entry_for_display(
        task,
        action=action,
        prefix=prefix,
        suffix=suffix,
    ).text


def build_needs_attention_entry_for_display(
    task: DbTask,
    *,
    action: Mapping[str, Any],
    prefix: int = 0,
    suffix: int = 0,
) -> NeedsAttentionDisplayEntry:
    """Render a needs-attention line plus the displayed prompt boundaries."""
    prompt = shorten_prompt(
        task.prompt or "",
        prompt_available_width(prefix=prefix, suffix=suffix),
    )
    task_id = task.id or "unknown"
    task_type = task.task_type or "task"
    prompt_start = len(f'{task_id} {task_type} "')
    return NeedsAttentionDisplayEntry(
        text=format_needs_attention_entry(task, prompt=prompt, action=action),
        prompt_start=prompt_start,
        prompt_end=prompt_start + len(prompt),
    )


def format_needs_attention_lifecycle(action: Mapping[str, Any]) -> str:
    """Render compact shared needs-attention wording for `gza show` lifecycle lines."""
    description = str(action.get("description", "")).strip()
    if description.startswith("SKIP: "):
        description = description[len("SKIP: ") :]
    reason = get_needs_attention_reason(action) or "needs-attention"
    if description:
        return f"needs attention reason={reason} {description}"
    return f"needs attention reason={reason}"


def _review_priority_sort_key(task: DbTask) -> tuple[datetime, int]:
    """Deterministic tie-breaker for active reviews of the same status."""
    created_at = task.created_at or datetime.min
    if created_at.tzinfo is not None:
        created_at = created_at.astimezone(UTC).replace(tzinfo=None)
    return (created_at, task_id_numeric_key(task.id))


def _normalize_time(value: datetime | None) -> datetime:
    if value is None:
        return datetime.min
    if value.tzinfo is not None:
        return value.astimezone(UTC).replace(tzinfo=None)
    return value


def _task_event_time(task: DbTask) -> datetime:
    """Return the best available lifecycle timestamp for ordering tasks."""
    return _normalize_time(task.completed_at or task.created_at)


def _resolve_latest_plan_source(store: SqliteTaskStore, task: DbTask) -> DbTask:
    """Resolve the latest non-dropped plan source in a plan revision chain."""
    latest = task
    for descendant in walk_based_on_descendants(store, task):
        if descendant.task_type != "plan_improve":
            continue
        if descendant.status in {"dropped", "pending", "in_progress"}:
            continue
        if descendant.status != "completed":
            continue
        if _task_event_time(descendant) >= _task_event_time(latest):
            latest = descendant
    return latest


def _resolve_plan_review_state(
    *,
    config: Any,
    store: SqliteTaskStore,
    task: DbTask,
) -> dict[str, Any]:
    """Resolve plan-review lifecycle state for plan and plan_improve sources."""
    latest_plan_source = _resolve_latest_plan_source(store, task)
    if latest_plan_source.id is None:
        return {"latest_plan_source": latest_plan_source}

    source_children = store.get_lineage_children(latest_plan_source.id)
    plan_reviews = [
        child
        for child in source_children
        if child.task_type == "plan_review" and child.depends_on == latest_plan_source.id and child.status != "dropped"
    ]
    plan_improves = [
        child
        for child in source_children
        if child.task_type == "plan_improve" and child.based_on == latest_plan_source.id and child.status != "dropped"
    ]

    active_plan_review_pending = max(
        (child for child in plan_reviews if child.status == "pending"),
        default=None,
        key=_task_event_time,
    )
    active_plan_review_running = max(
        (child for child in plan_reviews if child.status == "in_progress"),
        default=None,
        key=_task_event_time,
    )
    latest_completed_plan_review = max(
        (child for child in plan_reviews if child.status == "completed"),
        default=None,
        key=_task_event_time,
    )
    failed_plan_review_count = sum(1 for child in plan_reviews if child.status == "failed")
    active_plan_improve_pending = max(
        (child for child in plan_improves if child.status == "pending"),
        default=None,
        key=_task_event_time,
    )
    active_plan_improve_running = max(
        (child for child in plan_improves if child.status == "in_progress"),
        default=None,
        key=_task_event_time,
    )
    current_plan_review = (
        active_plan_review_pending
        or active_plan_review_running
        or latest_completed_plan_review
    )
    current_plan_improve = active_plan_improve_pending or active_plan_improve_running

    plan_review_verdict: str | None = None
    validated_manifest: PlanReviewManifest | None = None
    validation_error: str | None = None
    recoverable_schema_version_format_error = False
    if latest_completed_plan_review is not None and latest_plan_source.id is not None:
        outcome = get_plan_review_outcome(
            Path(config.project_dir),
            latest_completed_plan_review,
            source_task_id=latest_plan_source.id,
            source_task_type=latest_plan_source.task_type,
            max_slice_timeout_minutes=_plan_review_timeout_budget_minutes(config),
            max_plan_slices=getattr(config, "max_plan_slices", None),
        )
        plan_review_verdict = outcome.verdict
        validated_manifest = outcome.manifest
        validation_error = outcome.validation_error
        recoverable_schema_version_format_error = outcome.recoverable_schema_version_format_error

    completed_plan_review_cycles = _count_consecutive_plan_review_cycles(
        config=config,
        store=store,
        latest_plan_source=latest_plan_source,
    )
    plan_materialization_state = _resolve_plan_materialization_state(
        config=config,
        store=store,
        latest_plan_source=latest_plan_source,
        latest_completed_plan_review=latest_completed_plan_review,
        manifest=validated_manifest,
    )

    return {
        "latest_plan_source": latest_plan_source,
        "superseded_plan_source": latest_plan_source if latest_plan_source.id != task.id else None,
        "current_plan_review": current_plan_review,
        "active_plan_review_pending": active_plan_review_pending,
        "active_plan_review_running": active_plan_review_running,
        "latest_completed_plan_review": latest_completed_plan_review,
        "failed_plan_review_count": failed_plan_review_count,
        "plan_review_verdict": plan_review_verdict,
        "parsed_plan_review_manifest": validated_manifest,
        "validated_plan_review_manifest": validated_manifest,
        "plan_review_validation_error": validation_error,
        "recoverable_plan_review_schema_version_format_error": recoverable_schema_version_format_error,
        "current_plan_improve": current_plan_improve,
        "active_plan_improve_pending": active_plan_improve_pending,
        "active_plan_improve_running": active_plan_improve_running,
        "plan_review_cycle_count": completed_plan_review_cycles,
        "completed_plan_review_cycles": completed_plan_review_cycles,
        "plan_materialization_state": plan_materialization_state,
    }


def _count_consecutive_plan_review_cycles(
    *,
    config: Any,
    store: SqliteTaskStore,
    latest_plan_source: DbTask,
) -> int:
    """Count consecutive CHANGES_REQUESTED reviews across the current revision chain."""
    if latest_plan_source.id is None:
        return 0

    cycles = 0
    current_source: DbTask | None = latest_plan_source
    while current_source is not None and current_source.id is not None:
        source_children = store.get_lineage_children(current_source.id)
        latest_completed_review = max(
            (
                child
                for child in source_children
                if child.task_type == "plan_review"
                and child.depends_on == current_source.id
                and child.status == "completed"
                and child.status != "dropped"
            ),
            default=None,
            key=_task_event_time,
        )
        if latest_completed_review is None:
            break

        outcome = get_plan_review_outcome(
            Path(config.project_dir),
            latest_completed_review,
            source_task_id=current_source.id,
            source_task_type=current_source.task_type,
            max_slice_timeout_minutes=_plan_review_timeout_budget_minutes(config),
            max_plan_slices=getattr(config, "max_plan_slices", None),
        )
        if outcome.verdict != "CHANGES_REQUESTED":
            break
        cycles += 1

        if current_source.task_type != "plan_improve" or current_source.based_on is None:
            break
        parent_source = store.get(current_source.based_on)
        if parent_source is None or parent_source.task_type not in {"plan", "plan_improve"}:
            break
        current_source = parent_source

    return cycles


def _plan_review_timeout_budget_minutes(config: Any) -> int:
    getter = getattr(config, "get_plan_slice_target_timeout_minutes", None)
    if callable(getter):
        return int(getter())
    value = getattr(config, "plan_slice_target_timeout_minutes", None)
    if value is None:
        value = getattr(config, "code_task_diff_timeout_cap_minutes", 30)
    return int(value or 30)


def _failed_plan_review_retry_limit_action(ctx: AdvanceContext) -> dict[str, Any]:
    return with_needs_attention(
        {
            "type": "needs_discussion",
            "description": (
                "SKIP: automated plan review repeatedly failed without a parseable verdict "
                f"({ctx.failed_plan_review_count} failed attempt"
                f"{'' if ctx.failed_plan_review_count == 1 else 's'}; "
                f"limit {ctx.max_failed_plan_review_retries})"
            ),
        },
        reason="plan-review-repeatedly-failed",
        subject_task_id=ctx.task.id,
    )


def _repair_plan_slice_materialization_action(ctx: AdvanceContext) -> dict[str, Any]:
    state = ctx.plan_materialization_state
    candidate = (
        state.partial_repair_candidate
        if state is not None and state.partial_repair_candidate is not None
        else PlanMaterializationRepairCandidate(
            partial_task_ids=(),
            manifest_digest="",
            trigger_source="plan-review",
        )
    )

    return {
        "type": "repair_plan_slice_materialization",
        "description": (
            "Repair partial plan-review slice materialization from approved review "
            f"{_task_id(ctx.latest_completed_plan_review)}"
        ),
        "plan_review_task": ctx.latest_completed_plan_review,
        "plan_source_task": ctx.latest_plan_source or ctx.task,
        "manifest": ctx.validated_plan_review_manifest,
        "manifest_digest": candidate.manifest_digest,
        "partial_task_ids": candidate.partial_task_ids,
        "repair_trigger_source": candidate.trigger_source,
    }


def _resolve_plan_materialization_state(
    *,
    config: Any,
    store: SqliteTaskStore,
    latest_plan_source: DbTask,
    latest_completed_plan_review: DbTask | None,
    manifest: PlanReviewManifest | None,
) -> PlanMaterializationState:
    if latest_completed_plan_review is None or manifest is None:
        return PlanMaterializationState(materialized=False)
    descendants = (
        _list_non_dropped_implement_descendants_for_plan_source(store, latest_plan_source.id)
        if latest_plan_source.id is not None
        else []
    )
    materialized_tasks = load_materialized_plan_slice_set(
        store,
        review_task=latest_completed_plan_review,
        plan_source_task=latest_plan_source,
        manifest=manifest,
    )
    if materialized_tasks is None:
        partial_descendants_detected, partial_repair_candidate = (
            _classify_plan_review_slice_descendants_for_materialization_state(
                config=config,
                store=store,
                latest_plan_source=latest_plan_source,
                latest_completed_plan_review=latest_completed_plan_review,
                manifest=manifest,
                descendants=descendants,
            )
        )
        return PlanMaterializationState(
            materialized=False,
            partial_slice_descendants_detected=partial_descendants_detected,
            partial_repair_candidate=partial_repair_candidate,
        )
    materialized_task_ids = tuple(task.id for task in materialized_tasks if task.id is not None)
    materialized_task_id_set = set(materialized_task_ids)
    live_descendant_ids = {task.id for task in descendants if task.id is not None}
    if (
        len(descendants) == len(materialized_tasks)
        and live_descendant_ids == materialized_task_id_set
    ):
        return PlanMaterializationState(
            materialized=True,
            task_ids=materialized_task_ids,
        )

    extra_descendants = [
        task for task in descendants if task.id is None or task.id not in materialized_task_id_set
    ]
    partial_descendants_detected, partial_repair_candidate = (
        _classify_plan_review_slice_descendants_for_materialization_state(
            config=config,
            store=store,
            latest_plan_source=latest_plan_source,
            latest_completed_plan_review=latest_completed_plan_review,
            manifest=manifest,
            descendants=extra_descendants,
        )
    )
    return PlanMaterializationState(
        materialized=False,
        task_ids=materialized_task_ids,
        partial_slice_descendants_detected=partial_descendants_detected,
        partial_repair_candidate=partial_repair_candidate,
    )


def _classify_plan_review_slice_descendants_for_materialization_state(
    *,
    config: Any,
    store: SqliteTaskStore,
    latest_plan_source: DbTask,
    latest_completed_plan_review: DbTask,
    manifest: PlanReviewManifest,
    descendants: list[DbTask],
) -> tuple[bool, PlanMaterializationRepairCandidate | None]:
    """Classify live slice descendants as repairable partial state or ambiguous extras."""
    if latest_plan_source.id is None or latest_completed_plan_review.id is None:
        return False, None

    if not descendants:
        return False, None

    inspection = inspect_plan_review_materialization_for_repair(
        store,
        review_task=latest_completed_plan_review,
        plan_source_task=latest_plan_source,
        manifest=manifest,
    )
    if inspection.blocked_reason is not None:
        return True, None

    require_review_before_merge = getattr(config, "require_review_before_merge", True)
    trigger_sources = ("manual", "plan-review")
    matched_any_descendant = False

    for trigger_source in trigger_sources:
        expected_specs = build_plan_review_slice_task_specs(
            plan_source_task=latest_plan_source,
            review_task=latest_completed_plan_review,
            manifest=manifest,
            trigger_source=trigger_source,
            require_review_before_merge=require_review_before_merge,
        )
        candidate = _build_plan_materialization_repair_candidate(
            descendants=descendants,
            plan_source_task=latest_plan_source,
            review_task=latest_completed_plan_review,
            manifest=manifest,
            trigger_source=trigger_source,
            require_review_before_merge=require_review_before_merge,
        )
        if candidate is not None:
            return True, candidate
        expected_prompts = {spec.prompt for spec in expected_specs}
        if any(task.prompt in expected_prompts for task in descendants) or any(
            _task_has_slice_like_plan_review_provenance(
                task=task,
                expected_specs=expected_specs,
                plan_source_task=latest_plan_source,
                descendants=descendants,
            )
            for task in descendants
        ):
            matched_any_descendant = True

    return matched_any_descendant, None


def _build_plan_materialization_repair_candidate(
    *,
    descendants: list[DbTask],
    plan_source_task: DbTask,
    review_task: DbTask,
    manifest: PlanReviewManifest,
    trigger_source: str,
    require_review_before_merge: bool,
) -> PlanMaterializationRepairCandidate | None:
    expected_specs = build_plan_review_slice_task_specs(
        plan_source_task=plan_source_task,
        review_task=review_task,
        manifest=manifest,
        trigger_source=trigger_source,
        require_review_before_merge=require_review_before_merge,
    )
    if not expected_specs:
        return None

    prompt_to_index: dict[str, int] = {}
    for index, spec in enumerate(expected_specs):
        if spec.prompt in prompt_to_index:
            return None
        prompt_to_index[spec.prompt] = index

    matched_tasks: dict[int, DbTask] = {}
    for task in descendants:
        spec_index = prompt_to_index.get(task.prompt)
        if spec_index is None:
            return None
        if task.status != "pending" or task.branch:
            return None
        if spec_index in matched_tasks:
            return None
        matched_tasks[spec_index] = task

    if not matched_tasks:
        return None

    for spec_index, task in matched_tasks.items():
        expected_spec = expected_specs[spec_index]
        if task.task_type != expected_spec.task_type:
            return None
        if task.trigger_source != expected_spec.trigger_source:
            return None
        if task.same_branch != expected_spec.same_branch:
            return None
        if task.review_scope != expected_spec.review_scope:
            return None
        if task.tags != expected_spec.tags:
            return None
        if task.create_review != expected_spec.create_review:
            return None
        if task.based_on != _resolve_partial_materialized_candidate_ref(expected_spec.based_on, matched_tasks):
            return None
        if task.depends_on != _resolve_partial_materialized_candidate_ref(expected_spec.depends_on, matched_tasks):
            return None

    ordered_partial_task_ids = tuple(
        task.id
        for spec_index, task in sorted(matched_tasks.items())
        if task.id is not None
    )
    if not ordered_partial_task_ids:
        return None

    return PlanMaterializationRepairCandidate(
        partial_task_ids=ordered_partial_task_ids,
        manifest_digest=plan_review_manifest_digest(manifest),
        trigger_source=trigger_source,
    )


def _resolve_partial_materialized_candidate_ref(
    value: str | None,
    indexed_tasks: Mapping[int, DbTask],
) -> str | None:
    if value is None:
        return None
    if not value.startswith("__new_task_idx__:"):
        return value
    index = int(value.split(":", 1)[1])
    resolved = indexed_tasks.get(index)
    if resolved is None:
        return None
    return resolved.id


def _task_has_slice_like_plan_review_provenance(
    *,
    task: DbTask,
    expected_specs: list[Any],
    plan_source_task: DbTask,
    descendants: list[DbTask],
) -> bool:
    """Return whether a descendant still matches the durable shape of a slice task."""
    return any(
        _task_matches_slice_like_expected_spec(
            task=task,
            expected_spec=expected_spec,
            plan_source_task=plan_source_task,
            descendants=descendants,
        )
        for expected_spec in expected_specs
    )


def _task_matches_slice_like_expected_spec(
    *,
    task: DbTask,
    expected_spec: Any,
    plan_source_task: DbTask,
    descendants: list[DbTask],
) -> bool:
    if plan_source_task.id is None:
        return False
    if task.task_type != expected_spec.task_type:
        return False
    if task.trigger_source != expected_spec.trigger_source:
        return False
    if task.same_branch != expected_spec.same_branch:
        return False
    if task.review_scope != expected_spec.review_scope:
        return False
    if task.tags != expected_spec.tags:
        return False
    if task.create_review != expected_spec.create_review:
        return False
    if not _partial_descendant_ref_matches_expected_shape(
        actual_ref=task.based_on,
        expected_ref=expected_spec.based_on,
        plan_source_task_id=plan_source_task.id,
        descendants=descendants,
    ):
        return False
    if not _partial_descendant_ref_matches_expected_shape(
        actual_ref=task.depends_on,
        expected_ref=expected_spec.depends_on,
        plan_source_task_id=plan_source_task.id,
        descendants=descendants,
    ):
        return False
    return True


def _partial_descendant_ref_matches_expected_shape(
    *,
    actual_ref: str | None,
    expected_ref: str | None,
    plan_source_task_id: str,
    descendants: list[DbTask],
) -> bool:
    if expected_ref is None:
        return actual_ref is None
    if not expected_ref.startswith("__new_task_idx__:"):
        return actual_ref == expected_ref
    if actual_ref is None or actual_ref == plan_source_task_id:
        return False
    return any(descendant.id == actual_ref for descendant in descendants if descendant.id is not None)


def _list_non_dropped_implement_descendants_for_plan_source(
    store: SqliteTaskStore,
    source_task_id: str,
) -> list[DbTask]:
    descendants: list[DbTask] = []
    for task in store.get_all():
        if (
            task.task_type == "implement"
            and task.status != "dropped"
            and _is_based_on_descendant_of_source(store, task, source_task_id)
        ):
            descendants.append(task)
    return descendants


def _is_based_on_descendant_of_source(store: SqliteTaskStore, task: DbTask, source_task_id: str) -> bool:
    """Return whether a task belongs to a source task's based_on chain."""
    current: DbTask | None = task
    seen: set[str] = set()

    while current is not None and current.based_on is not None:
        parent_id = current.based_on
        if parent_id == source_task_id:
            return True
        if parent_id in seen:
            return False
        seen.add(parent_id)
        current = store.get(parent_id)

    return False


def _failed_rebase_still_blocks_advance(ctx: AdvanceContext) -> bool:
    """Return True when a failed rebase remains the latest unresolved lineage state.

    A clean current branch tip is not enough to clear failed rebase residue. We only
    treat the lineage as having moved past that failure once later successful same-branch
    progress exists on the implementation lineage, either via a completed rebase/recovery
    or a later successful review outcome.
    """
    failed_rebase = ctx.rebase_failed
    if failed_rebase is None:
        return False
    if (
        ctx.post_merge_rebase_state is not None
        and ctx.post_merge_rebase_state.rebase_resolution_proved
    ):
        return False

    failed_rebase_time = _task_event_time(failed_rebase)

    latest_completed_rebase = ctx.latest_completed_rebase
    if latest_completed_rebase is not None:
        latest_completed_rebase_time = _task_event_time(latest_completed_rebase)
        if latest_completed_rebase_time > failed_rebase_time:
            return False

    review_cleared_at = ctx.effective_review_cleared_at
    if review_cleared_at is not None and _normalize_time(review_cleared_at) >= failed_rebase_time:
        return False

    latest_review = ctx.latest_completed_review
    if latest_review is None:
        return True

    latest_review_time = _task_event_time(latest_review)
    if latest_review_time < failed_rebase_time:
        return True

    return ctx.review_verdict not in {"APPROVED", "APPROVED_WITH_FOLLOWUPS"}


def _count_rebase_failure_streak(
    *,
    task: DbTask,
    rebase_children: list[DbTask],
    latest_completed_rebase: DbTask | None,
    latest_completed_review: DbTask | None,
    latest_completed_code_change: DbTask | None,
    review_cleared_at: datetime | None,
) -> RebaseFailureStreak | None:
    branch = task.branch
    if not branch:
        return None

    progress_times: list[datetime] = []
    if latest_completed_rebase is not None:
        progress_times.append(_task_event_time(latest_completed_rebase))
    if latest_completed_review is not None:
        progress_times.append(_task_event_time(latest_completed_review))
    if latest_completed_code_change is not None:
        progress_times.append(_task_event_time(latest_completed_code_change))
    if review_cleared_at is not None:
        progress_times.append(_normalize_time(review_cleared_at))
    progress_boundary = max(progress_times) if progress_times else None

    failed_rebases = [
        child
        for child in rebase_children
        if child.status == "failed"
        and child.branch == branch
        and (progress_boundary is None or _task_event_time(child) > progress_boundary)
    ]
    if not failed_rebases:
        return None

    failed_rebases.sort(key=_task_event_time, reverse=True)
    failed_task_ids = tuple(child.id for child in failed_rebases if child.id is not None)
    return RebaseFailureStreak(
        attempts=len(failed_rebases),
        branch=branch,
        failed_task_ids=failed_task_ids,
    )


def _latest_unresolved_comment_time(store: SqliteTaskStore, task_id: str) -> datetime | None:
    unresolved_comments = store.get_comments(
        task_id,
        unresolved_only=True,
        kinds=(TASK_COMMENT_KIND_FEEDBACK,),
    )
    if not unresolved_comments:
        return None
    return max(_normalize_time(comment.created_at) for comment in unresolved_comments)


def resolve_closing_review_action(
    *,
    task: DbTask,
    reviews: list[DbTask],
    latest_completed_review: DbTask | None,
    latest_completed_code_change: DbTask | None,
    max_failed_closing_review_retries: int = 3,
) -> dict[str, Any] | None:
    """Return the invariant-enforcing closing review action for a lineage, if any.

    The lifecycle invariant is satisfied once the newest completed code-change task
    is followed by at least one review task in any state. Pending reviews should be
    run, in-progress reviews should be waited on, and completed reviews already
    satisfy the invariant. Failed closing reviews block merge: they are retried up
    to max_failed_closing_review_retries times, then escalate to needs_attention.
    """
    if (
        task.task_type != "implement"
        or task.status != "completed"
        or not task.branch
        or latest_completed_code_change is None
    ):
        return None

    latest_code_change_time = _task_event_time(latest_completed_code_change)
    needs_closing_review = latest_completed_review is None or (
        latest_completed_review.completed_at is not None
        and latest_code_change_time > _normalize_time(latest_completed_review.completed_at)
    )
    if not needs_closing_review:
        return None

    if latest_completed_review is None:
        follow_on_reviews = list(reviews)
    else:
        follow_on_reviews = [
            review
            for review in reviews
            if _task_event_time(review) > latest_code_change_time
        ]

    if follow_on_reviews:
        active_follow_on_review = _select_active_review(follow_on_reviews)
        if active_follow_on_review is not None and active_follow_on_review.status == "pending":
            return {
                "type": "run_review",
                "description": (
                    f"Run pending closing review {_task_id(active_follow_on_review)}"
                ),
                "review_task": active_follow_on_review,
            }
        if active_follow_on_review is not None and active_follow_on_review.status == "in_progress":
            return {
                "type": "wait_review",
                "description": (
                    f"SKIP: closing review {_task_id(active_follow_on_review)} is in_progress"
                ),
                "review_task": active_follow_on_review,
            }
        # A completed follow-on review satisfies the invariant (verdict acted on elsewhere).
        # A failed closing review must NOT silently allow merge when code changed after an
        # established review baseline (latest_completed_review is not None). Only apply
        # bounded retry in that case: when latest_completed_review is None there may be
        # a completed resume that is invisible via get_reviews_for_task (its based_on
        # points to the original review, not the impl), so preserve the old behaviour
        # to avoid false retries.
        if latest_completed_review is not None:
            failed_count = sum(1 for r in follow_on_reviews if r.status == "failed")
            if failed_count > 0 and not any(r.status == "completed" for r in follow_on_reviews):
                assert task.id is not None
                if failed_count >= max_failed_closing_review_retries:
                    return with_needs_attention(
                        {
                            "type": "needs_discussion",
                            "description": (
                                f"SKIP: closing review failed {failed_count} time(s), needs manual intervention"
                            ),
                        },
                        reason="closing-review-failed-max-retries",
                        subject_task_id=task.id,
                    )
                return {
                    "type": "create_review",
                    "description": "Create closing review (previous attempt failed)",
                }
        return None

    if latest_completed_code_change.id == task.id:
        description = "Create closing review (latest implementation has no review yet)"
    else:
        description = "Create closing review (code changed since the last review)"
    return {
        "type": "create_review",
        "description": description,
    }


def _select_active_review(reviews: list[DbTask]) -> DbTask | None:
    """Prefer in-progress review over pending siblings, then newest deterministically."""
    in_progress = sorted(
        (r for r in reviews if r.status == "in_progress"),
        key=_review_priority_sort_key,
        reverse=True,
    )
    if in_progress:
        return in_progress[0]

    pending = sorted(
        (r for r in reviews if r.status == "pending"),
        key=_review_priority_sort_key,
        reverse=True,
    )
    if pending:
        return pending[0]
    return None


def _get_review_output_content(config: Any, review_task: DbTask) -> str | None:
    return get_review_content(Path(config.project_dir), review_task)


def _resolve_review_state(
    config: Any,
    store: SqliteTaskStore,
    task: DbTask,
    git: Any,
    target_branch: str,
    *,
    persist_review_clearance: bool,
) -> tuple[
    list[DbTask],
    DbTask | None,
    DbTask | None,
    datetime | None,
    bool,
    tuple[str, ...],
    tuple[str, ...],
    str | None,
    ParsedReviewReport | None,
    ReviewBlockerSummary | None,
    tuple[ReviewFinding, ...],
    tuple[DbTask, ...],
    int,
    DbTask | None,
    DbTask | None,
    DbTask | None,
    int,
    str | None,
    str | None,
    str | None,
    tuple[ReviewBlockerResolutionStatus, ...],
    bool,
    bool,
    bool,
    DbTask | None,
    ReviewBlockerAdjudicationCandidate | None,
    DbTask | None,
    bool,
    bool,
    DbTask | None,
    dict[str, Any] | None,
    OffTopicVerifyClearanceCandidate | None,
    SiblingReviewAttentionCandidate | None,
]:
    """Resolve review/improve lineage state for the implementation root task."""
    reviews = get_implementation_review_evidence(store, task)
    active_review = _select_active_review(reviews)
    completed_reviews = [r for r in reviews if r.status == "completed"]
    latest_completed_review = completed_reviews[0] if completed_reviews else None

    persisted_review_cleared = (
        latest_completed_review is not None
        and task.review_cleared_at is not None
        and latest_completed_review.completed_at is not None
        and task.review_cleared_at >= latest_completed_review.completed_at
    )
    effective_review_cleared_at = task.review_cleared_at if persisted_review_cleared else None
    review_cleared = persisted_review_cleared
    created_investigation_task_ids: tuple[str, ...] = ()
    reused_investigation_task_ids: tuple[str, ...] = ()

    review_verdict: str | None = None
    review_report: ParsedReviewReport | None = None
    latest_review_blocker_summary: ReviewBlockerSummary | None = None
    followup_findings: tuple[ReviewFinding, ...] = ()
    recent_verify_timeout_only_reviews: tuple[DbTask, ...] = ()
    completed_review_cycles = 0
    active_improve_running: DbTask | None = None
    active_improve_pending: DbTask | None = None
    latest_noop_improve: DbTask | None = None
    consecutive_noop_improves = 0
    noop_improve_trigger: str | None = None
    noop_improve_verify_probe_warning: str | None = None
    noop_improve_verify_recovery_attention_message: str | None = None
    review_blocker_resolution_statuses: tuple[ReviewBlockerResolutionStatus, ...] = ()
    review_blockers_invalidated = False
    review_blockers_revalidated = False
    review_blocker_adjudication_needed = False
    review_blocker_adjudication_needed_task: DbTask | None = None
    review_blocker_adjudication_candidate: ReviewBlockerAdjudicationCandidate | None = None
    active_review_blocker_adjudication: DbTask | None = None
    sibling_review_attention_candidate: SiblingReviewAttentionCandidate | None = None
    has_improve_after_review = False
    has_fresh_unresolved_comments_since_latest_review = False
    latest_completed_code_change: DbTask | None = None
    off_topic_verify_clearance_candidate: OffTopicVerifyClearanceCandidate | None = None
    completed_descendant_code_changes = [
        t
        for t in get_code_changing_descendants_for_root(store, task)
        if t.status == "completed"
        and not (t.task_type == "improve" and t.changed_diff is False)
    ]
    if completed_descendant_code_changes:
        latest_completed_code_change = max(completed_descendant_code_changes, key=_task_event_time)
    elif task.status == "completed" and latest_completed_review is None:
        latest_completed_code_change = task

    if latest_completed_review is not None:
        review_report = get_review_report(Path(config.project_dir), latest_completed_review)
        review_verdict = review_report.verdict
        latest_review_blocker_summary = summarize_review_blockers(
            _get_review_output_content(config, latest_completed_review)
        )
        followup_findings = tuple(
            finding for finding in review_report.findings if finding.severity == "FOLLOWUP"
        )

        if task.task_type == "implement":
            assert task.id is not None
            latest_comment_time = _latest_unresolved_comment_time(store, task.id)
            latest_review_time = _normalize_time(latest_completed_review.completed_at or latest_completed_review.created_at)
            if latest_comment_time is not None and latest_comment_time > latest_review_time:
                has_fresh_unresolved_comments_since_latest_review = True

        assert task.id is not None
        assert latest_completed_review.id is not None
        improve_tasks = store.get_improve_tasks_for(task.id, latest_completed_review.id)
        active_improve_running = next((t for t in improve_tasks if t.status == "in_progress"), None)
        active_improve_pending = next((t for t in improve_tasks if t.status == "pending"), None)
        latest_noop_improve, consecutive_noop_improves = _count_consecutive_noop_improves(improve_tasks)
        allow_off_topic_lane = bool(
            getattr(config, "advance_off_topic_verify_unblock", False)
            and latest_completed_review.review_verify_status == "failed"
        )
        if review_cleared and review_verdict == "CHANGES_REQUESTED" and latest_review_blocker_summary is not None:
            current_head_sha: str | None = None
            if task.branch is not None:
                current_head_sha = _resolve_branch_head_sha(git, task.branch).head_sha
            has_matching_verify_clearance = (
                latest_review_blocker_summary.is_verify_blocked_only
                and _latest_matching_verify_only_noop_review_clearance(
                    store=store,
                    task=task,
                    latest_completed_review=latest_completed_review,
                    current_head_sha=current_head_sha,
                )
                is not None
            )
            if latest_review_blocker_summary.is_verify_blocked_only and not has_matching_verify_clearance:
                review_cleared = False
                effective_review_cleared_at = None
        if not review_cleared:
            (
                review_cleared,
                effective_review_cleared_at,
                noop_improve_verify_probe_warning,
                created_investigation_task_ids,
                reused_investigation_task_ids,
            ) = _resolve_noop_improve_verify_clearance(
                config=config,
                store=store,
                git=git,
                project_dir=Path(config.project_dir),
                target_branch=target_branch,
                task=task,
                latest_completed_review=latest_completed_review,
                improve_tasks=improve_tasks,
                persist=persist_review_clearance,
            )
            branch_head = _resolve_branch_head_sha(git, task.branch)
            noop_improve_verify_recovery_attention_message = (
                _resolve_noop_improve_verify_recovery_attention_message(
                    store=store,
                    project_dir=Path(config.project_dir),
                    task=task,
                    latest_completed_review=latest_completed_review,
                    latest_completed_noop_improve=_latest_completed_noop_improve(improve_tasks),
                    current_head_sha=branch_head.head_sha,
                )
                if branch_head.warning is None
                else None
            )
        if not review_cleared and allow_off_topic_lane:
            off_topic_verify_clearance_candidate = _resolve_off_topic_verify_clearance_candidate(
                config=config,
                store=store,
                git=git,
                project_dir=Path(config.project_dir),
                task=task,
                target_branch=target_branch,
                latest_completed_review=latest_completed_review,
                improve_tasks=improve_tasks,
            )

        if latest_completed_review.completed_at is not None and latest_completed_code_change is not None:
            has_improve_after_review = (
                _task_event_time(latest_completed_code_change)
                > _normalize_time(latest_completed_review.completed_at)
            )

        if (
            task.task_type == "implement"
            and has_fresh_unresolved_comments_since_latest_review
            and (review_cleared or review_verdict in {"APPROVED", "APPROVED_WITH_FOLLOWUPS"})
        ):
            noop_improve_trigger = "comments"
        elif review_verdict == "CHANGES_REQUESTED":
            noop_improve_trigger = "review"

        if review_verdict == "CHANGES_REQUESTED":
            review_blocker_resolution_statuses = _latest_review_blocker_resolution_statuses(
                store,
                git=git,
                project_dir=Path(config.project_dir),
                review_task=latest_completed_review,
                impl_task=task,
                findings=review_report.findings,
                improve_tasks=improve_tasks,
                allow_verify_clearance=True,
            )
            all_current_review_blockers_cleared = bool(review_blocker_resolution_statuses) and all(
                _review_blocker_status_clears_current_blocker(status)
                for status in review_blocker_resolution_statuses
            )
            review_blockers_invalidated = all_current_review_blockers_cleared and any(
                status.state == "invalid" for status in review_blocker_resolution_statuses
            )
            review_blockers_revalidated = any(
                status.state == "valid" for status in review_blocker_resolution_statuses
            )
            if all_current_review_blockers_cleared:
                review_cleared = True
            if review_blockers_revalidated:
                consecutive_noop_improves = 0
                latest_noop_improve = None
            review_blocker_adjudication_candidate = _latest_disputed_blocker_artifact_for_review(
                store,
                resolution_statuses=review_blocker_resolution_statuses,
                latest_noop_improve=latest_noop_improve,
            )
            (
                active_review_blocker_adjudication,
                adjudication_needed_from_task,
                adjudication_needed_task_from_task,
            ) = _resolve_review_blocker_adjudication_state(
                store,
                review_task=latest_completed_review,
                impl_task=task,
                candidate=review_blocker_adjudication_candidate,
                review_blockers_invalidated=review_blockers_invalidated,
                review_blockers_revalidated=review_blockers_revalidated,
            )
            needs_human_status = next(
                (status for status in review_blocker_resolution_statuses if status.state == "needs_human"),
                None,
            )
            if needs_human_status is not None:
                review_blocker_adjudication_needed = True
                source_task_id = (needs_human_status.latest_artifact.metadata or {}).get("source_task_id") if needs_human_status.latest_artifact is not None else None
                if isinstance(source_task_id, str):
                    review_blocker_adjudication_needed_task = store.get(source_task_id)
            elif adjudication_needed_from_task:
                review_blocker_adjudication_needed = True
                review_blocker_adjudication_needed_task = adjudication_needed_task_from_task

        verify_timeout_only_reviews: list[DbTask] = []
        for review_task in completed_reviews:
            review_content = _get_review_output_content(config, review_task)
            if not is_verify_timeout_only_review(review_content):
                break
            verify_timeout_only_reviews.append(review_task)
            if len(verify_timeout_only_reviews) >= VERIFY_BLOCKED_REVIEW_THRESHOLD:
                break
        recent_verify_timeout_only_reviews = tuple(verify_timeout_only_reviews)
        sibling_review_attention_candidate = _find_sibling_review_attention_candidate(
            config=config,
            store=store,
            git=git,
            impl_task=task,
            completed_reviews=completed_reviews,
            latest_completed_review=latest_completed_review,
        )

    max_failed_closing_review_retries = int(
        getattr(config, "max_failed_closing_review_retries", 3)
    )
    closing_review_action = resolve_closing_review_action(
        task=task,
        reviews=reviews,
        latest_completed_review=latest_completed_review,
        latest_completed_code_change=latest_completed_code_change,
        max_failed_closing_review_retries=max_failed_closing_review_retries,
    )

    return (
        reviews,
        active_review,
        latest_completed_review,
        effective_review_cleared_at,
        review_cleared,
        created_investigation_task_ids,
        reused_investigation_task_ids,
        review_verdict,
        review_report,
        latest_review_blocker_summary,
        followup_findings,
        recent_verify_timeout_only_reviews,
        completed_review_cycles,
        active_improve_running,
        active_improve_pending,
        latest_noop_improve,
        consecutive_noop_improves,
        noop_improve_trigger,
        noop_improve_verify_probe_warning,
        noop_improve_verify_recovery_attention_message,
        review_blocker_resolution_statuses,
        review_blockers_invalidated,
        review_blockers_revalidated,
        review_blocker_adjudication_needed,
        review_blocker_adjudication_needed_task,
        review_blocker_adjudication_candidate,
        active_review_blocker_adjudication,
        has_improve_after_review,
        has_fresh_unresolved_comments_since_latest_review,
        latest_completed_code_change,
        closing_review_action,
        off_topic_verify_clearance_candidate,
        sibling_review_attention_candidate,
    )


def _resolve_impl_ancestor_by_based_on(store: SqliteTaskStore, task: DbTask) -> DbTask | None:
    """Resolve the nearest implementation ancestor for same-branch task lineages."""
    visited: set[str] = set()
    current: DbTask | None = task
    while current is not None:
        if current.id is not None:
            if current.id in visited:
                return None
            visited.add(current.id)
        if current.task_type == "implement":
            return current
        if current.based_on is None:
            return None
        current = store.get(current.based_on)
    return None


def _spec_coherence_reviews(reviews: list[DbTask] | None) -> list[DbTask]:
    return [
        review
        for review in (reviews or [])
        if (review.review_scope or "").strip() == SPEC_COHERENCE_REVIEW_SCOPE
        or declares_spec_coherence_review_mode(review.review_scope)
    ]


def _latest_completed_spec_coherence_review(
    reviews: list[DbTask] | None,
) -> DbTask | None:
    completed = [review for review in _spec_coherence_reviews(reviews) if review.status == "completed"]
    return completed[0] if completed else None


def _active_spec_coherence_review(reviews: list[DbTask] | None) -> DbTask | None:
    active = [review for review in _spec_coherence_reviews(reviews) if review.status in {"pending", "in_progress"}]
    return _select_active_review(active)


def _spec_coherence_review_is_current(
    *,
    review: DbTask | None,
    current_head_sha: str | None,
    current_changed_paths: tuple[str, ...],
) -> bool:
    if review is None:
        return False
    if not current_head_sha:
        return False
    try:
        metadata = parse_spec_coherence_review_scope(review.review_scope)
    except ValueError:
        return False
    if metadata is None:
        return False
    return (
        review.review_verify_head_sha == current_head_sha
        and metadata.reviewed_head_sha == current_head_sha
        and metadata.changed_paths == current_changed_paths
    )


def _is_implementation_owned_lineage(ctx: AdvanceContext) -> bool:
    """Whether this lineage inherits merge review gating from an implementation root."""
    return (ctx.review_root_task or ctx.task).task_type == "implement"


def execution_status_allows_merge(ctx: AdvanceContext) -> bool:
    """Return whether the current planning task has merge-eligible execution status."""
    return ctx.task.status in MERGEABLE_EXECUTION_STATUSES


def _review_cleared_is_merge_ready(ctx: AdvanceContext) -> bool:
    if not ctx.review_cleared:
        return False
    if getattr(ctx, "review_blockers_invalidated", False):
        return True
    if _latest_review_is_verify_blocked_only(ctx):
        current_head_sha = ctx.current_review_head_sha or ctx.latest_reviewed_head_sha
        if _latest_matching_verify_only_noop_review_clearance(
            store=ctx.store,
            task=ctx.task,
            latest_completed_review=ctx.latest_completed_review,
            current_head_sha=current_head_sha,
        ) is not None:
            return True
        return False
    if ctx.effective_review_cleared_at is not None:
        return True
    return False


def has_valid_review_for_merge(ctx: AdvanceContext) -> bool:
    """Return whether current review evidence is fresh enough to allow auto-merge."""
    if not _is_implementation_owned_lineage(ctx):
        return True
    if not ctx.requires_review:
        return True
    if ctx.review_invalidated_by_progress:
        return False
    if ctx.closing_review_action is not None:
        return False
    if ctx.latest_completed_review is None:
        return False
    if ctx.current_review_head_probe_warning is not None and ctx.latest_reviewed_head_sha is not None:
        return False
    if ctx.review_cleared:
        return _review_cleared_is_merge_ready(ctx)
    return ctx.review_verdict in {"APPROVED", "APPROVED_WITH_FOLLOWUPS"}


def _closing_review_requires_automation(ctx: AdvanceContext) -> bool:
    """Whether a closing-review action should preempt later merge fallback rules."""
    if ctx.closing_review_action is None:
        return False
    if not _is_implementation_owned_lineage(ctx):
        return False
    if not ctx.requires_review:
        return False
    return True


def _stale_review_refresh_required(ctx: AdvanceContext) -> bool:
    """Whether a stale implementation review must still be refreshed before merge."""
    if not ctx.review_invalidated_by_progress:
        return False
    if not _is_implementation_owned_lineage(ctx):
        return False
    if not ctx.requires_review:
        return False
    return True


def _active_review_requires_automation(ctx: AdvanceContext) -> bool:
    """Whether an active review should still block merge automation."""
    if ctx.review_cleared or ctx.active_review is None:
        return False
    if (
        ctx.review_invalidated_by_progress
        and _is_implementation_owned_lineage(ctx)
        and not ctx.requires_review
    ):
        return False
    return True


def _review_freshness_probe_failed(ctx: AdvanceContext) -> bool:
    """Whether review freshness must fail closed because live branch-head probing failed."""
    if ctx.current_review_head_probe_warning is None:
        return False
    if ctx.latest_reviewed_head_sha is None:
        return False
    if not _is_implementation_owned_lineage(ctx):
        return False
    if not ctx.requires_review:
        return False
    if ctx.latest_completed_review is None:
        return False
    if ctx.active_review is not None:
        return False
    return True


def _spec_coherence_gate_required(ctx: AdvanceContext) -> bool:
    return bool(ctx.spec_coherence_required)


def _spec_coherence_gate_currently_approved(ctx: AdvanceContext) -> bool:
    return (
        ctx.spec_coherence_required
        and ctx.spec_coherence_latest_completed_review is not None
        and ctx.spec_coherence_current_head_sha is not None
        and ctx.spec_coherence_review_current
        and ctx.spec_coherence_review_verdict == "APPROVED"
    )


def _spec_coherence_gate_needs_attention(ctx: AdvanceContext) -> dict[str, Any]:
    detail = getattr(ctx, "spec_coherence_inspection_error", None) or "unknown diff inspection failure"
    return with_needs_attention(
        {
            "type": "needs_discussion",
            "description": (
                "SKIP: behavior-spec coherence gate could not verify the current branch diff. "
                f"Inspection error: {detail}"
            ),
        },
        reason="spec-coherence-diff-unverified",
        subject_task_id=ctx.task.id,
    )


def _spec_coherence_needs_discussion_action(ctx: AdvanceContext) -> dict[str, Any]:
    verdict = getattr(ctx, "spec_coherence_review_verdict", None)
    return with_needs_attention(
        {
            "type": "needs_discussion",
            "description": (
                f"SKIP: behavior-spec coherence review verdict is "
                f"{verdict or 'unknown'}; manual discussion is required"
            ),
            "review_task": getattr(ctx, "spec_coherence_latest_completed_review", None),
        },
        reason="spec-coherence-needs-discussion",
        subject_task_id=ctx.task.id,
    )


def _spec_coherence_unknown_verdict_action(ctx: AdvanceContext) -> dict[str, Any]:
    verdict = getattr(ctx, "spec_coherence_review_verdict", None)
    return with_needs_attention(
        {
            "type": "needs_discussion",
            "description": (
                "SKIP: behavior-spec coherence review verdict is unknown or unparseable"
                f" ({verdict or 'missing'}); manual discussion is required"
            ),
            "review_task": getattr(ctx, "spec_coherence_latest_completed_review", None),
        },
        reason="spec-coherence-unknown-verdict",
        subject_task_id=ctx.task.id,
    )


def _spec_coherence_create_review_action(ctx: AdvanceContext) -> dict[str, Any]:
    return {
        "type": "create_review",
        "description": "Create behavior-spec coherence review",
        "review_mode": "spec_coherence",
        "review_head_sha": getattr(ctx, "spec_coherence_current_head_sha", None),
        "review_changed_paths": getattr(ctx, "spec_coherence_changed_paths", ()),
    }


def _closing_review_invariant_action(ctx: AdvanceContext) -> dict[str, Any]:
    """Return the enforced closing-review action, failing closed when auto-review is disabled."""
    assert ctx.closing_review_action is not None

    if ctx.closing_review_action.get("type") == "create_review" and not ctx.create_reviews:
        review_root_task = getattr(ctx, "review_root_task", None)
        subject_task_id = (
            review_root_task.id
            if review_root_task is not None and review_root_task.id is not None
            else ctx.task.id
        )
        if ctx.latest_completed_review is None:
            return with_needs_attention(
                {
                    "type": "needs_discussion",
                    "description": "SKIP: no review exists and advance_create_reviews=false (run gza review manually)",
                },
                reason="review-needs-manual-creation",
                subject_task_id=subject_task_id,
            )
        return with_needs_attention(
            {
                "type": "needs_discussion",
                "description": (
                    "SKIP: closing review required before merge and advance_create_reviews=false "
                    "(run gza review manually)"
                ),
            },
            reason="closing-review-needs-manual-refresh",
            subject_task_id=subject_task_id,
        )

    return _default_subject_for_attention_action(ctx, ctx.closing_review_action)


def _resolve_review_root_task(store: SqliteTaskStore, task: DbTask) -> DbTask:
    """Resolve the implementation task whose review state gates this branch lineage."""
    candidate = task
    if task.id is not None:
        merge_unit = store.resolve_merge_unit_for_task(task.id)
        if merge_unit is not None:
            representative = store.resolve_merge_unit_representative_task(
                merge_unit,
                preferred_task_id=task.id,
                require_actionable=True,
            )
            if representative is not None:
                candidate = representative
            else:
                owner = store.resolve_merge_unit_owner_task(merge_unit)
                if owner is not None:
                    candidate = owner

    impl_ancestor = _resolve_impl_ancestor_by_based_on(store, candidate)
    if impl_ancestor is not None:
        return impl_ancestor
    return candidate


def _get_same_branch_rebase_descendants_for_root(store: SqliteTaskStore, root_task: DbTask) -> list[DbTask]:
    """Return same-branch rebase descendants nested under the review-gated impl root."""
    if root_task.id is None:
        return []

    return [
        child
        for child in walk_based_on_descendants(store, root_task)
        if child.task_type == "rebase"
        and (root_task.branch is None or child.branch is None or child.branch == root_task.branch)
    ]


def _build_base_advance_context(
    *,
    config: Any,
    store: SqliteTaskStore,
    git: Any,
    task: DbTask,
    target_branch: str,
    has_branch: bool,
    effective_max_noop_improves: int,
    effective_max_resume: int,
    auto_implement_enabled: bool,
    failed_recovery_decision: FailedRecoveryDecision | None,
    failed_recovery_attention_reason: str | None,
    has_implementation_followup: bool,
    active_plan_child: DbTask | None,
    active_implement_child: DbTask | None,
    has_non_dropped_plan_or_implement_descendant: bool,
    is_resumable_failed: bool,
    has_resume_children: bool,
    resume_chain_depth: int,
) -> AdvanceContext:
    """Build the DB-known portion of advance context shared by cheap and full paths."""
    return AdvanceContext(
        store=store,
        git=git,
        task=task,
        task_type=task.task_type,
        has_branch=has_branch,
        target_branch=target_branch,
        requires_review=config.require_review_before_merge,
        create_reviews=config.advance_create_reviews,
        max_review_cycles=config.max_review_cycles,
        advance_create_plan_reviews=getattr(config, "advance_create_plan_reviews", True),
        require_plan_review_before_implement=getattr(config, "require_plan_review_before_implement", True),
        max_plan_review_cycles=getattr(config, "max_plan_review_cycles", 2),
        max_failed_plan_review_retries=getattr(config, "max_failed_plan_review_retries", 3),
        max_plan_slices=getattr(config, "max_plan_slices", None),
        plan_slice_target_timeout_minutes=_plan_review_timeout_budget_minutes(config),
        max_noop_improve_cycles=effective_max_noop_improves,
        max_resume_attempts=effective_max_resume,
        auto_implement_enabled=auto_implement_enabled,
        failed_recovery_decision=failed_recovery_decision,
        failed_recovery_attention_reason=failed_recovery_attention_reason,
        has_non_dropped_implement_descendant=has_implementation_followup,
        active_plan_child=active_plan_child,
        active_implement_child=active_implement_child,
        has_non_dropped_plan_or_implement_descendant=has_non_dropped_plan_or_implement_descendant,
        is_resumable_failed_task=is_resumable_failed,
        has_resume_children=has_resume_children,
        resume_chain_depth=resume_chain_depth,
        failure_reason=task.failure_reason,
    )


def _resolve_db_known_wait_action(ctx: AdvanceContext) -> dict[str, Any] | None:
    """Return the DB-proven closing-review wait action when git facts cannot change it."""
    if not _closing_review_requires_automation(ctx):
        return None
    closing_review_action = ctx.closing_review_action
    if closing_review_action is None or closing_review_action.get("type") != "wait_review":
        return None
    review_task = closing_review_action.get("review_task")
    if not isinstance(review_task, DbTask) or review_task.status != "in_progress":
        return None
    return _closing_review_invariant_action(ctx)


def _resolve_pre_closing_review_git_context(
    ctx: AdvanceContext,
    config: Any,
    store: SqliteTaskStore,
    git: Any,
    target_branch: str,
    *,
    persist_post_merge_rebase_state: bool,
) -> AdvanceContext:
    """Resolve git-backed facts needed before the closing-review invariant can win."""
    task = ctx.task
    review_root_task = ctx.review_root_task
    if review_root_task is None:
        raise AssertionError("git phase requires review_root_task")

    def _resolve_current_review_head_state() -> tuple[str | None, str | None]:
        branch_name = review_root_task.branch or task.branch
        branch_head = _resolve_branch_head_sha(git, branch_name)
        if branch_head.warning is not None:
            return None, branch_head.warning
        if isinstance(branch_head.head_sha, str) and branch_head.head_sha:
            return branch_head.head_sha, None
        if review_root_task.id is not None:
            merge_unit = store.resolve_merge_unit_for_task(review_root_task.id)
            if merge_unit is not None and isinstance(merge_unit.head_sha, str) and merge_unit.head_sha:
                return merge_unit.head_sha, None
        return None, None

    merge_source = _resolve_current_merge_source(git, task.branch or "")
    if persist_post_merge_rebase_state:
        post_merge_rebase_state = _resolve_and_persist_post_merge_rebase_state(
            store,
            git,
            task,
            target_branch,
            merge_source=merge_source,
        )
    else:
        post_merge_rebase_state = resolve_post_merge_rebase_state(
            store,
            git,
            task,
            target_branch,
            merge_source=merge_source,
        )
    merge_state = (
        post_merge_rebase_state.resolved_merge_state
        if persist_post_merge_rebase_state
        else resolve_task_merge_state_for_target(
            store=store,
            task=task,
            git=git,
            target_branch=target_branch,
        )
    )
    strict_scope_inspection = _resolve_strict_scope_inspection(
        config,
        git,
        task,
        merge_source_ref=merge_source.ref,
        target_branch=target_branch,
    )
    can_merge = (
        post_merge_rebase_state.already_merged
        or merge_state_is_terminal_for_lifecycle(merge_state)
        or (bool(merge_source.ref) and git.can_merge(merge_source.ref, target_branch))
    )
    current_impl_head_sha: str | None = None
    current_impl_head_probe_warning: str | None = None
    if (
        strict_scope_inspection.inspection_error is not None
        or strict_scope_inspection.violation_paths
        or post_merge_rebase_state.already_merged
        or merge_state_is_terminal_for_lifecycle(merge_state)
        or not can_merge
    ):
        spec_coherence_inspection = SpecCoherenceInspection()
    else:
        if getattr(getattr(config, "spec_coherence", None), "enabled", False):
            current_impl_head_sha, current_impl_head_probe_warning = _resolve_current_review_head_state()
        spec_coherence_inspection = _resolve_spec_coherence_inspection(
            config,
            git,
            review_root_task,
            merge_source_ref=merge_source.ref,
            target_branch=target_branch,
        )
        if spec_coherence_inspection.required:
            if current_impl_head_probe_warning is not None:
                spec_coherence_inspection = replace(
                    spec_coherence_inspection,
                    inspection_error=current_impl_head_probe_warning,
                )
            elif current_impl_head_sha is None:
                spec_coherence_inspection = replace(
                    spec_coherence_inspection,
                    inspection_error="could not resolve current implementation head for spec coherence gate",
                )
    rebase_root_task = review_root_task if review_root_task.task_type == "implement" else task
    assert rebase_root_task.id is not None
    rebase_children = _get_same_branch_rebase_descendants_for_root(store, rebase_root_task)
    rebase_pending_or_running = next((c for c in rebase_children if c.status in {"pending", "in_progress"}), None)
    failed_rebases = [c for c in rebase_children if c.status == "failed"]
    rebase_failed = max(failed_rebases, key=_task_event_time) if failed_rebases else None

    latest_completed_rebase: DbTask | None = None
    completed_rebases = [
        c
        for c in rebase_children
        if c.status == "completed" and c.completed_at is not None
    ]
    if completed_rebases:
        latest_completed_rebase = max(completed_rebases, key=lambda t: t.completed_at or datetime.min)

    rebase_invalidates_review = False
    review_invalidated_by_progress = False
    review_invalidation_reason: str | None = None
    review_preserved_by_rebase: DbTask | None = None
    review_invalidated_by_rebase: DbTask | None = None
    resolution_review_required = False
    resolution_review_metadata: ResolutionReviewScope | None = None
    resolution_review_metadata_invalid = False
    review_cycle_boundary = ReviewCycleBoundary()
    completed_review_cycles = ctx.completed_review_cycles
    latest_reviewed_head_sha = (
        ctx.latest_completed_review.review_verify_head_sha
        if ctx.latest_completed_review is not None
        else None
    )
    current_review_head_sha: str | None = None
    current_review_head_probe_warning: str | None = None
    if ctx.latest_completed_review is not None and latest_reviewed_head_sha is not None:
        current_review_head_sha, current_review_head_probe_warning = _resolve_current_review_head_state()

    spec_coherence_active_review = _active_spec_coherence_review(ctx.reviews)
    spec_coherence_latest_completed_review = _latest_completed_spec_coherence_review(ctx.reviews)
    spec_coherence_review_verdict: str | None = None
    if spec_coherence_latest_completed_review is not None:
        spec_coherence_review_verdict = get_review_report(
            Path(config.project_dir),
            spec_coherence_latest_completed_review,
        ).verdict
    spec_coherence_review_current = _spec_coherence_review_is_current(
        review=spec_coherence_latest_completed_review,
        current_head_sha=current_impl_head_sha,
        current_changed_paths=spec_coherence_inspection.changed_paths,
    )
    if (
        latest_completed_rebase is not None
        and ctx.latest_completed_review is not None
        and latest_completed_rebase.completed_at is not None
        and ctx.latest_completed_review.completed_at is not None
        and latest_completed_rebase.completed_at > ctx.latest_completed_review.completed_at
    ):
        if latest_completed_rebase.changed_diff is False:
            review_preserved_by_rebase = latest_completed_rebase
        else:
            rebase_invalidates_review = True
            review_invalidated_by_progress = True
            review_invalidation_reason = "rebase_changed_diff"
            review_invalidated_by_rebase = latest_completed_rebase
            resolution_review_required = True
    if (
        ctx.latest_completed_review is not None
        and latest_reviewed_head_sha is not None
        and current_review_head_sha is not None
        and latest_reviewed_head_sha != current_review_head_sha
        and ctx.latest_completed_code_change is not None
        and ctx.latest_completed_code_change.id != ctx.latest_completed_review.id
        and _task_event_time(ctx.latest_completed_code_change)
        > _task_event_time(ctx.latest_completed_review)
    ):
        review_invalidated_by_progress = True
        if review_invalidation_reason is None:
            review_invalidation_reason = "branch_head_advanced"

    if resolution_review_required and ctx.active_review is not None:
        resolution_review_metadata = _resolve_valid_resolution_review_metadata(
            store,
            review_task=ctx.active_review,
            ctx=ctx,
            impl_task=review_root_task,
            rebase_task=latest_completed_rebase,
        )
        if resolution_review_metadata is None:
            resolution_review_metadata_invalid = True
    elif (
        resolution_review_required
        and ctx.latest_completed_review is not None
        and latest_completed_rebase is not None
        and ctx.latest_completed_review.completed_at is not None
        and latest_completed_rebase.completed_at is not None
        and ctx.latest_completed_review.completed_at > latest_completed_rebase.completed_at
    ):
        resolution_review_metadata = _resolve_valid_resolution_review_metadata(
            store,
            review_task=ctx.latest_completed_review,
            ctx=ctx,
            impl_task=review_root_task,
            rebase_task=latest_completed_rebase,
        )
        if resolution_review_metadata is None:
            resolution_review_metadata_invalid = True
    elif resolution_review_required and not _planned_resolution_review_metadata_is_valid(
        replace(
            ctx,
            review_invalidated_by_rebase=latest_completed_rebase,
            current_review_head_sha=current_review_head_sha,
            post_merge_rebase_state=post_merge_rebase_state,
        )
    ):
        resolution_review_metadata_invalid = True

    if (
        not resolution_review_metadata_invalid
        and latest_completed_rebase is not None
        and latest_completed_rebase.changed_diff is not False
        and ctx.latest_completed_review is not None
        and ctx.latest_completed_review.completed_at is not None
        and latest_completed_rebase.completed_at is not None
        and ctx.latest_completed_review.completed_at > latest_completed_rebase.completed_at
    ):
        resolution_review_metadata = _resolve_valid_resolution_review_metadata(
            store,
            review_task=ctx.latest_completed_review,
            ctx=ctx,
            impl_task=review_root_task,
            rebase_task=latest_completed_rebase,
        )
        if resolution_review_metadata is None:
            resolution_review_metadata_invalid = True

    if (
        ctx.review_verdict == "CHANGES_REQUESTED"
        and review_root_task.id is not None
        and ctx.latest_completed_review is not None
    ):
        completed_reviews = [
            review
            for review in (ctx.reviews or [])
            if review.status == "completed"
        ]
        review_cycle_boundary = resolve_review_cycle_boundary(
            completed_reviews=completed_reviews,
            latest_completed_review=ctx.latest_completed_review,
            latest_completed_rebase=latest_completed_rebase,
            latest_completed_code_change=ctx.latest_completed_code_change,
            latest_reviewed_head_sha=latest_reviewed_head_sha,
            current_review_head_sha=current_review_head_sha,
        )
        completed_review_cycles = count_completed_review_cycles_since_boundary(
            store,
            review_root_task.id,
            boundary=review_cycle_boundary,
        )

    rebase_failure_streak = _count_rebase_failure_streak(
        task=task,
        rebase_children=rebase_children,
        latest_completed_rebase=latest_completed_rebase,
        latest_completed_review=ctx.latest_completed_review,
        latest_completed_code_change=ctx.latest_completed_code_change,
        review_cleared_at=ctx.effective_review_cleared_at,
    )

    return replace(
        ctx,
        merge_source_ref=merge_source.ref,
        merge_source_warning=merge_source.warning,
        post_merge_rebase_state=post_merge_rebase_state,
        merge_state=merge_state,
        can_merge=can_merge,
        spec_coherence_required=spec_coherence_inspection.required,
        spec_coherence_changed_paths=spec_coherence_inspection.changed_paths,
        spec_coherence_current_head_sha=current_impl_head_sha,
        spec_coherence_inspection_error=(
            current_impl_head_probe_warning
            if current_impl_head_probe_warning is not None
            else spec_coherence_inspection.inspection_error
        ),
        spec_coherence_active_review=spec_coherence_active_review,
        spec_coherence_latest_completed_review=spec_coherence_latest_completed_review,
        spec_coherence_review_verdict=spec_coherence_review_verdict,
        spec_coherence_review_current=spec_coherence_review_current,
        strict_scope_violation_paths=strict_scope_inspection.violation_paths,
        strict_scope_inspection_error=strict_scope_inspection.inspection_error,
        rebase_pending_or_running=rebase_pending_or_running,
        rebase_failed=rebase_failed,
        latest_completed_rebase=latest_completed_rebase,
        rebase_failure_streak=rebase_failure_streak,
        rebase_invalidates_review=rebase_invalidates_review,
        review_invalidated_by_progress=review_invalidated_by_progress,
        review_invalidation_reason=review_invalidation_reason,
        review_preserved_by_rebase=review_preserved_by_rebase,
        review_invalidated_by_rebase=review_invalidated_by_rebase,
        resolution_review_required=resolution_review_required,
        resolution_review_metadata=resolution_review_metadata,
        resolution_review_metadata_invalid=resolution_review_metadata_invalid,
        current_review_head_sha=current_review_head_sha,
        current_review_head_probe_warning=current_review_head_probe_warning,
        latest_reviewed_head_sha=latest_reviewed_head_sha,
        completed_review_cycles=completed_review_cycles,
        review_cycle_boundary_task_id=review_cycle_boundary.boundary_task_id,
        review_cycle_boundary_reason=review_cycle_boundary.boundary_reason,
    )


def _resolve_post_closing_review_git_context(
    ctx: AdvanceContext,
    config: Any,
    git: Any,
    target_branch: str,
) -> AdvanceContext:
    """Resolve late git-backed review-loop facts once pre-closing safety gates are clear."""
    review_root_task = ctx.review_root_task
    if review_root_task is None:
        raise AssertionError("git phase requires review_root_task")

    duplicate_blocker_streak = None
    review_blocker_adjudication_candidate = ctx.review_blocker_adjudication_candidate
    active_review_blocker_adjudication = ctx.active_review_blocker_adjudication
    review_blocker_adjudication_needed = ctx.review_blocker_adjudication_needed
    review_blocker_adjudication_needed_task = ctx.review_blocker_adjudication_needed_task
    if (
        ctx.task.task_type == "implement"
        and ctx.review_verdict == "CHANGES_REQUESTED"
        and not ctx.review_cleared
        and ctx.active_review is None
    ):
        rebase_children = _get_same_branch_rebase_descendants_for_root(
            ctx.store,
            review_root_task if review_root_task.task_type == "implement" else ctx.task,
        )
        duplicate_blocker_streak = _count_duplicate_primary_blocker_streak(
            config,
            ctx.reviews or [],
            [
                rebase
                for rebase in rebase_children
                if rebase.status == "completed" and rebase.completed_at is not None
            ],
        )
        if review_blocker_adjudication_candidate is None:
            review_blocker_adjudication_candidate = _duplicate_blocker_adjudication_candidate_for_review(
                impl_task=ctx.task,
                latest_completed_review=ctx.latest_completed_review,
                review_report=ctx.review_report,
                duplicate_blocker_streak=duplicate_blocker_streak,
                latest_completed_code_change=ctx.latest_completed_code_change,
                current_review_head_sha=ctx.current_review_head_sha,
                latest_reviewed_head_sha=ctx.latest_reviewed_head_sha,
            )
        if review_blocker_adjudication_candidate != ctx.review_blocker_adjudication_candidate:
            (
                active_review_blocker_adjudication,
                adjudication_needed_from_task,
                adjudication_needed_task_from_task,
            ) = _resolve_review_blocker_adjudication_state(
                ctx.store,
                review_task=ctx.latest_completed_review,
                impl_task=ctx.task,
                candidate=review_blocker_adjudication_candidate,
                review_blockers_invalidated=ctx.review_blockers_invalidated,
                review_blockers_revalidated=ctx.review_blockers_revalidated,
            )
            if adjudication_needed_from_task:
                review_blocker_adjudication_needed = True
                review_blocker_adjudication_needed_task = adjudication_needed_task_from_task

    return replace(
        ctx,
        active_review_blocker_adjudication=active_review_blocker_adjudication,
        review_blocker_adjudication_needed=review_blocker_adjudication_needed,
        review_blocker_adjudication_needed_task=review_blocker_adjudication_needed_task,
        review_blocker_adjudication_candidate=review_blocker_adjudication_candidate,
        duplicate_blocker_streak=duplicate_blocker_streak,
    )


def _matches_rule_before_closing_review_invariant(ctx: AdvanceContext) -> bool:
    """Return whether any higher-priority advance rule already matches this context."""
    for rule in ADVANCE_RULES:
        if rule.name == "closing_review_invariant":
            return False
        if rule.matches(ctx):
            return True
    raise AssertionError("closing_review_invariant rule missing from ADVANCE_RULES")


def resolve_advance_context(
    config: Any,
    store: SqliteTaskStore,
    git: Any,
    task: DbTask,
    target_branch: str,
    *,
    impl_based_on_ids: set[str] | None = None,
    max_resume_attempts: int | None = None,
    persist_post_merge_rebase_state: bool = True,
    persist_review_clearance: bool = True,
    read_context: RecoveryReadContext | None = None,
    selected_for_merge: bool = False,
) -> AdvanceContext:
    """Resolve state once, then let rules evaluate pure context."""
    assert task.id is not None

    effective_max_resume = max_resume_attempts if max_resume_attempts is not None else config.max_resume_attempts
    effective_max_noop_improves = int(
        getattr(config, "max_noop_improve_cycles", DEFAULT_MAX_NOOP_IMPROVE_CYCLES)
    )
    failed_recovery_decision: FailedRecoveryDecision | None = None
    failed_recovery_attention_reason: str | None = None
    if task.status == "failed":
        failed_recovery_decision = decide_failed_task_recovery(
            store,
            task,
            max_recovery_attempts=effective_max_resume,
            read_context=read_context,
        )
        failed_recovery_attention_reason = get_failed_recovery_needs_attention_reason(
            store,
            task,
            decision=failed_recovery_decision,
            max_recovery_attempts=effective_max_resume,
            read_context=read_context,
        )
    is_resumable_failed = (
        failed_recovery_decision is not None
        and failed_recovery_decision.action in {"resume", "retry"}
    )
    has_resume_children = False
    resume_chain_depth = 0
    active_plan_child: DbTask | None = None
    active_implement_child: DbTask | None = None
    has_non_dropped_plan_or_implement_descendant = False

    if task.task_type in {"plan", "explore"}:
        followup_state = resolve_source_followup_state(
            task,
            get_children=store.get_based_on_children,
        )
        active_plan_child = followup_state.active_plan_descendant
        active_implement_child = followup_state.active_implement_descendant
        has_non_dropped_plan_or_implement_descendant = (
            followup_state.has_non_dropped_plan_or_implement_descendant
        )
        if impl_based_on_ids is None:
            impl_based_on_ids = collect_non_dropped_implement_source_ids(store.get_all())
    has_implementation_followup = (
        task.task_type == "plan"
        and source_task_has_implementation_followup(
            task,
            followup_state,
            non_dropped_implement_source_ids=impl_based_on_ids,
        )
    )
    if task.task_type == "plan_improve" and impl_based_on_ids is None:
        impl_based_on_ids = collect_non_dropped_implement_source_ids(store.get_all())
    if task.task_type == "plan_improve":
        has_implementation_followup = task.id in (impl_based_on_ids or set())
    auto_implement_enabled = task.auto_implement is not False

    if task.task_type in {"plan", "plan_improve"}:
        base_ctx = _build_base_advance_context(
            config=config,
            store=store,
            git=git,
            task=task,
            target_branch=target_branch,
            has_branch=bool(task.branch),
            effective_max_noop_improves=effective_max_noop_improves,
            effective_max_resume=effective_max_resume,
            auto_implement_enabled=auto_implement_enabled,
            failed_recovery_decision=failed_recovery_decision,
            failed_recovery_attention_reason=failed_recovery_attention_reason,
            has_implementation_followup=has_implementation_followup,
            active_plan_child=active_plan_child,
            active_implement_child=active_implement_child,
            has_non_dropped_plan_or_implement_descendant=has_non_dropped_plan_or_implement_descendant,
            is_resumable_failed=is_resumable_failed,
            has_resume_children=has_resume_children,
            resume_chain_depth=resume_chain_depth,
        )
        return replace(
            base_ctx,
            selected_for_merge=selected_for_merge,
            **_resolve_plan_review_state(config=config, store=store, task=task),
        )

    if not task.branch:
        return replace(
            _build_base_advance_context(
                config=config,
                store=store,
                git=git,
                task=task,
                target_branch=target_branch,
                has_branch=False,
                effective_max_noop_improves=effective_max_noop_improves,
                effective_max_resume=effective_max_resume,
                auto_implement_enabled=auto_implement_enabled,
                failed_recovery_decision=failed_recovery_decision,
                failed_recovery_attention_reason=failed_recovery_attention_reason,
                has_implementation_followup=has_implementation_followup,
                active_plan_child=active_plan_child,
                active_implement_child=active_implement_child,
                has_non_dropped_plan_or_implement_descendant=has_non_dropped_plan_or_implement_descendant,
                is_resumable_failed=is_resumable_failed,
                has_resume_children=has_resume_children,
                resume_chain_depth=resume_chain_depth,
            ),
            selected_for_merge=selected_for_merge,
        )

    review_root_task = _resolve_review_root_task(store, task)
    (
        reviews,
        active_review,
        latest_completed_review,
        effective_review_cleared_at,
        review_cleared,
        created_investigation_task_ids,
        reused_investigation_task_ids,
        review_verdict,
        review_report,
        latest_review_blocker_summary,
        followup_findings,
        recent_verify_timeout_only_reviews,
        completed_review_cycles,
        active_improve_running,
        active_improve_pending,
        latest_noop_improve,
        consecutive_noop_improves,
        noop_improve_trigger,
        noop_improve_verify_probe_warning,
        noop_improve_verify_recovery_attention_message,
        review_blocker_resolution_statuses,
        review_blockers_invalidated,
        review_blockers_revalidated,
        review_blocker_adjudication_needed,
        review_blocker_adjudication_needed_task,
        review_blocker_adjudication_candidate,
        active_review_blocker_adjudication,
        has_improve_after_review,
        has_fresh_unresolved_comments_since_latest_review,
        latest_completed_code_change,
        closing_review_action,
        off_topic_verify_clearance_candidate,
        sibling_review_attention_candidate,
    ) = _resolve_review_state(
        config,
        store,
        review_root_task,
        git,
        target_branch=target_branch,
        persist_review_clearance=persist_review_clearance,
    )

    ctx = _build_base_advance_context(
        config=config,
        store=store,
        git=git,
        task=task,
        target_branch=target_branch,
        has_branch=True,
        effective_max_noop_improves=effective_max_noop_improves,
        effective_max_resume=effective_max_resume,
        auto_implement_enabled=auto_implement_enabled,
        failed_recovery_decision=failed_recovery_decision,
        failed_recovery_attention_reason=failed_recovery_attention_reason,
        has_implementation_followup=has_implementation_followup,
        active_plan_child=active_plan_child,
        active_implement_child=active_implement_child,
        has_non_dropped_plan_or_implement_descendant=has_non_dropped_plan_or_implement_descendant,
        is_resumable_failed=is_resumable_failed,
        has_resume_children=has_resume_children,
        resume_chain_depth=resume_chain_depth,
    )
    ctx = replace(
        ctx,
        selected_for_merge=selected_for_merge,
        reviews=reviews,
        review_root_task=review_root_task,
        active_review=active_review,
        latest_completed_review=latest_completed_review,
        latest_completed_code_change=latest_completed_code_change,
        effective_review_cleared_at=effective_review_cleared_at,
        review_cleared=review_cleared,
        created_investigation_task_ids=created_investigation_task_ids,
        reused_investigation_task_ids=reused_investigation_task_ids,
        review_verdict=review_verdict,
        review_report=review_report,
        latest_review_blocker_summary=latest_review_blocker_summary,
        followup_findings=followup_findings,
        recent_verify_timeout_only_reviews=recent_verify_timeout_only_reviews,
        completed_review_cycles=completed_review_cycles,
        active_improve_running=active_improve_running,
        active_improve_pending=active_improve_pending,
        latest_noop_improve=latest_noop_improve,
        consecutive_noop_improves=consecutive_noop_improves,
        noop_improve_trigger=noop_improve_trigger,
        noop_improve_verify_probe_warning=noop_improve_verify_probe_warning,
        noop_improve_verify_recovery_attention_message=noop_improve_verify_recovery_attention_message,
        review_blocker_resolution_statuses=review_blocker_resolution_statuses,
        review_blockers_invalidated=review_blockers_invalidated,
        review_blockers_revalidated=review_blockers_revalidated,
        review_blocker_adjudication_needed=review_blocker_adjudication_needed,
        review_blocker_adjudication_needed_task=review_blocker_adjudication_needed_task,
        review_blocker_adjudication_candidate=review_blocker_adjudication_candidate,
        active_review_blocker_adjudication=active_review_blocker_adjudication,
        sibling_review_attention_candidate=sibling_review_attention_candidate,
        has_improve_after_review=has_improve_after_review,
        has_fresh_unresolved_comments_since_latest_review=has_fresh_unresolved_comments_since_latest_review,
        closing_review_action=closing_review_action,
        off_topic_verify_clearance_candidate=off_topic_verify_clearance_candidate,
    )
    ctx = _resolve_pre_closing_review_git_context(
        ctx,
        config,
        store,
        git,
        target_branch,
        persist_post_merge_rebase_state=persist_post_merge_rebase_state,
    )
    if _resolve_db_known_wait_action(ctx) is not None or _matches_rule_before_closing_review_invariant(ctx):
        return ctx
    return _resolve_post_closing_review_git_context(
        ctx,
        config,
        git,
        target_branch,
    )


ADVANCE_RULES: list[AdvanceRule] = [
    AdvanceRule(
        name="failed_task_retry",
        matches=lambda ctx: ctx.failed_recovery_decision is not None and ctx.failed_recovery_decision.action == "retry",
        action=_failed_task_resume_or_retry_action,
    ),
    AdvanceRule(
        name="failed_task_resume",
        matches=lambda ctx: ctx.failed_recovery_decision is not None and ctx.failed_recovery_decision.action == "resume",
        action=_failed_task_resume_or_retry_action,
    ),
    AdvanceRule(
        name="failed_task_reconcile",
        matches=lambda ctx: (
            ctx.failed_recovery_decision is not None and ctx.failed_recovery_decision.action == "reconcile"
        ),
        action=_failed_task_resume_or_retry_action,
    ),
    AdvanceRule(
        name="failed_task_skip",
        matches=lambda ctx: ctx.failed_recovery_decision is not None,
        action=_failed_task_skip_action,
    ),
    AdvanceRule(
        name="superseded_plan_source",
        matches=lambda ctx: (
            ctx.task_type in {"plan", "plan_improve"}
            and ctx.task.status == "completed"
            and ctx.superseded_plan_source is not None
        ),
        action=lambda ctx: {
            "type": "skip",
            "description": f"SKIP: newer plan source {_task_id(ctx.superseded_plan_source)} supersedes this plan revision",
        },
    ),
    AdvanceRule(
        name="release_approved_plan_review",
        matches=lambda ctx: (
            ctx.task_type in {"plan", "plan_improve"}
            and ctx.task.status == "completed"
            and not ctx.has_non_dropped_implement_descendant
            and not ctx.auto_implement_enabled
            and ctx.require_plan_review_before_implement
            and ctx.plan_review_verdict == "APPROVED"
            and ctx.validated_plan_review_manifest is not None
            and ctx.latest_completed_plan_review is not None
            and not (
                ctx.plan_materialization_state is not None
                and ctx.plan_materialization_state.materialized
            )
        ),
        action=lambda ctx: {
            "type": "release_approved_plan_review",
            "description": (
                "Release held plan after approved plan review "
                f"{_task_id(ctx.latest_completed_plan_review)}"
            ),
            "plan_source_task": ctx.latest_plan_source or ctx.task,
            "plan_review_task": ctx.latest_completed_plan_review,
        },
    ),
    AdvanceRule(
        name="awaiting_human_plan_review",
        matches=lambda ctx: (
            ctx.task_type in {"plan", "plan_improve"}
            and ctx.task.status == "completed"
            and not ctx.has_non_dropped_implement_descendant
            and not ctx.auto_implement_enabled
        ),
        action=lambda ctx: with_needs_attention(
            {
                "type": "awaiting_human",
                "description": (
                    f"Awaiting human review: review the plan, then run 'uv run gza implement {ctx.task.id}' "
                    "to create implementation, or drop it if you decided not to implement."
                ),
            },
            reason="awaiting-human-review",
            subject_task_id=ctx.task.id,
        ),
    ),
    AdvanceRule(
        name="plan_has_implement",
        matches=lambda ctx: (
            ctx.task_type in {"plan", "plan_improve"}
            and ctx.task.status == "completed"
            and ctx.has_non_dropped_implement_descendant
            and (
                not ctx.auto_implement_enabled
                or not ctx.require_plan_review_before_implement
                or ctx.latest_completed_plan_review is None
                or (
                    ctx.plan_materialization_state is not None
                    and ctx.plan_materialization_state.materialized
                )
                or (
                    ctx.plan_review_verdict == "APPROVED"
                    and ctx.validated_plan_review_manifest is not None
                    and ctx.plan_materialization_state is not None
                    and not ctx.plan_materialization_state.partial_slice_descendants_detected
                )
            )
        ),
        action=lambda ctx: {
            "type": "skip",
            "reason": (
                "already_materialized"
                if ctx.plan_materialization_state is not None and ctx.plan_materialization_state.materialized
                else "already_has_implement"
            ),
            "description": (
                "SKIP: approved plan-review slices are already materialized"
                if ctx.plan_materialization_state is not None and ctx.plan_materialization_state.materialized
                else "SKIP: implement task already exists for this plan"
            ),
        },
    ),
    AdvanceRule(
        name="plan_partial_materialization_auto_repair",
        matches=lambda ctx: (
            ctx.task_type in {"plan", "plan_improve"}
            and ctx.task.status == "completed"
            and ctx.has_non_dropped_implement_descendant
            and ctx.auto_implement_enabled
            and ctx.require_plan_review_before_implement
            and ctx.plan_review_verdict == "APPROVED"
            and ctx.validated_plan_review_manifest is not None
            and ctx.latest_completed_plan_review is not None
            and ctx.plan_materialization_state is not None
            and ctx.plan_materialization_state.partial_repair_candidate is not None
            and not ctx.plan_materialization_state.materialized
        ),
        action=_repair_plan_slice_materialization_action,
    ),
    AdvanceRule(
        name="plan_partial_materialization_requires_repair",
        matches=lambda ctx: (
            ctx.task_type in {"plan", "plan_improve"}
            and ctx.task.status == "completed"
            and ctx.has_non_dropped_implement_descendant
            and ctx.auto_implement_enabled
            and ctx.require_plan_review_before_implement
            and ctx.plan_review_verdict == "APPROVED"
            and ctx.validated_plan_review_manifest is not None
            and ctx.plan_materialization_state is not None
            and ctx.plan_materialization_state.partial_slice_descendants_detected
            and not (
                ctx.plan_materialization_state is not None
                and ctx.plan_materialization_state.materialized
            )
        ),
        action=lambda ctx: with_needs_attention(
            {
                "type": "needs_discussion",
                "description": (
                    "SKIP: plan-review implement descendants exist without a recorded complete "
                    "materialization; repair or drop the partial slice set before retrying."
                ),
            },
            reason="plan-review-materialization-repair-needed",
            subject_task_id=ctx.task.id,
        ),
    ),
    AdvanceRule(
        name="plan_needs_implement",
        matches=lambda ctx: (
            ctx.task_type in {"plan", "plan_improve"}
            and ctx.task.status == "completed"
            and not ctx.has_non_dropped_implement_descendant
            and ctx.auto_implement_enabled
            and not ctx.require_plan_review_before_implement
        ),
        action=lambda ctx: {"type": "create_implement", "description": "Create and start implement task"},
    ),
    AdvanceRule(
        name="plan_run_review",
        matches=lambda ctx: (
            ctx.task_type in {"plan", "plan_improve"}
            and ctx.task.status == "completed"
            and not ctx.has_non_dropped_implement_descendant
            and ctx.auto_implement_enabled
            and ctx.require_plan_review_before_implement
            and ctx.active_plan_review_pending is not None
        ),
        action=lambda ctx: {
            "type": "run_plan_review",
            "description": f"Run pending plan review {_task_id(ctx.active_plan_review_pending)}",
            "plan_review_task": ctx.active_plan_review_pending,
        },
    ),
    AdvanceRule(
        name="plan_wait_review",
        matches=lambda ctx: (
            ctx.task_type in {"plan", "plan_improve"}
            and ctx.task.status == "completed"
            and not ctx.has_non_dropped_implement_descendant
            and ctx.auto_implement_enabled
            and ctx.require_plan_review_before_implement
            and ctx.active_plan_review_running is not None
        ),
        action=lambda ctx: {
            "type": "wait_plan_review",
            "description": f"Wait for plan review {_task_id(ctx.active_plan_review_running)}",
            "plan_review_task": ctx.active_plan_review_running,
        },
    ),
    AdvanceRule(
        name="plan_rederive_approved_review_schema_version_manifest",
        matches=lambda ctx: (
            ctx.task_type in {"plan", "plan_improve"}
            and ctx.task.status == "completed"
            and not ctx.has_non_dropped_implement_descendant
            and ctx.auto_implement_enabled
            and ctx.require_plan_review_before_implement
            and ctx.plan_review_verdict == "APPROVED"
            and ctx.validated_plan_review_manifest is None
            and ctx.plan_review_validation_error is not None
            and ctx.recoverable_plan_review_schema_version_format_error
            and ctx.active_plan_review_pending is None
            and ctx.active_plan_review_running is None
            and not (
                ctx.plan_materialization_state is not None
                and ctx.plan_materialization_state.materialized
            )
        ),
        action=lambda ctx: {
            "type": "create_plan_review",
            "description": (
                "Re-run plan review to re-derive approved slice manifest after "
                f"schema_version format mismatch in {_task_id(ctx.latest_completed_plan_review)}"
            ),
        },
    ),
    AdvanceRule(
        name="plan_invalid_approved_review",
        matches=lambda ctx: (
            ctx.task_type in {"plan", "plan_improve"}
            and ctx.task.status == "completed"
            and ctx.auto_implement_enabled
            and ctx.require_plan_review_before_implement
            and ctx.plan_review_verdict == "APPROVED"
            and ctx.validated_plan_review_manifest is None
            and ctx.plan_review_validation_error is not None
            and not (
                ctx.plan_materialization_state is not None
                and ctx.plan_materialization_state.materialized
            )
        ),
        action=lambda ctx: with_needs_attention(
            {
                "type": "needs_discussion",
                "description": (
                    "SKIP: approved plan review has an invalid slice manifest"
                    f" ({ctx.plan_review_validation_error})"
                ),
            },
            reason="plan-review-invalid-slices",
            subject_task_id=ctx.task.id,
        ),
    ),
    AdvanceRule(
        name="plan_materialize_approved_review",
        matches=lambda ctx: (
            ctx.task_type in {"plan", "plan_improve"}
            and ctx.task.status == "completed"
            and not ctx.has_non_dropped_implement_descendant
            and ctx.auto_implement_enabled
            and ctx.require_plan_review_before_implement
            and ctx.plan_review_verdict == "APPROVED"
            and ctx.validated_plan_review_manifest is not None
            and not (
                ctx.plan_materialization_state is not None
                and ctx.plan_materialization_state.materialized
            )
        ),
        action=lambda ctx: {
            "type": "materialize_plan_slices",
            "description": f"Materialize implementation slices from plan review {_task_id(ctx.latest_completed_plan_review)}",
            "plan_review_task": ctx.latest_completed_plan_review,
            "manifest": ctx.validated_plan_review_manifest,
            "plan_source_task": ctx.latest_plan_source or ctx.task,
        },
    ),
    AdvanceRule(
        name="plan_run_improve",
        matches=lambda ctx: (
            ctx.task_type in {"plan", "plan_improve"}
            and ctx.task.status == "completed"
            and ctx.auto_implement_enabled
            and ctx.require_plan_review_before_implement
            and ctx.plan_review_verdict == "CHANGES_REQUESTED"
            and ctx.active_plan_improve_pending is not None
            and not (
                ctx.plan_materialization_state is not None
                and ctx.plan_materialization_state.materialized
            )
        ),
        action=lambda ctx: {
            "type": "run_plan_improve",
            "description": f"Run pending plan improve {_task_id(ctx.active_plan_improve_pending)}",
            "plan_improve_task": ctx.active_plan_improve_pending,
        },
    ),
    AdvanceRule(
        name="plan_wait_improve",
        matches=lambda ctx: (
            ctx.task_type in {"plan", "plan_improve"}
            and ctx.task.status == "completed"
            and ctx.auto_implement_enabled
            and ctx.require_plan_review_before_implement
            and ctx.plan_review_verdict == "CHANGES_REQUESTED"
            and ctx.active_plan_improve_running is not None
            and not (
                ctx.plan_materialization_state is not None
                and ctx.plan_materialization_state.materialized
            )
        ),
        action=lambda ctx: {
            "type": "wait_plan_improve",
            "description": f"Wait for plan improve {_task_id(ctx.active_plan_improve_running)}",
            "plan_improve_task": ctx.active_plan_improve_running,
        },
    ),
    AdvanceRule(
        name="plan_max_cycles_reached",
        matches=lambda ctx: (
            ctx.task_type in {"plan", "plan_improve"}
            and ctx.task.status == "completed"
            and ctx.auto_implement_enabled
            and ctx.require_plan_review_before_implement
            and ctx.plan_review_verdict == "CHANGES_REQUESTED"
            and ctx.completed_plan_review_cycles >= ctx.max_plan_review_cycles
            and not (
                ctx.plan_materialization_state is not None
                and ctx.plan_materialization_state.materialized
            )
        ),
        action=lambda ctx: with_needs_attention(
            {
                "type": "needs_discussion",
                "description": "SKIP: plan review reached max_plan_review_cycles without approval",
            },
            reason="plan-review-max-cycles-reached",
            subject_task_id=ctx.task.id,
        ),
    ),
    AdvanceRule(
        name="plan_create_improve",
        matches=lambda ctx: (
            ctx.task_type in {"plan", "plan_improve"}
            and ctx.task.status == "completed"
            and ctx.auto_implement_enabled
            and ctx.require_plan_review_before_implement
            and ctx.plan_review_verdict == "CHANGES_REQUESTED"
            and not (
                ctx.plan_materialization_state is not None
                and ctx.plan_materialization_state.materialized
            )
        ),
        action=lambda ctx: {
            "type": "create_plan_improve",
            "description": f"Create and start plan improve task for plan review {_task_id(ctx.latest_completed_plan_review)}",
            "plan_review_task": ctx.latest_completed_plan_review,
            "plan_source_task": ctx.latest_plan_source or ctx.task,
        },
    ),
    AdvanceRule(
        name="plan_review_needs_discussion",
        matches=lambda ctx: (
            ctx.task_type in {"plan", "plan_improve"}
            and ctx.task.status == "completed"
            and ctx.auto_implement_enabled
            and ctx.require_plan_review_before_implement
            and ctx.latest_completed_plan_review is not None
            and ctx.plan_review_verdict in {None, "NEEDS_DISCUSSION"}
            and not (
                ctx.plan_materialization_state is not None
                and ctx.plan_materialization_state.materialized
            )
        ),
        action=lambda ctx: with_needs_attention(
            {
                "type": "needs_discussion",
                "description": (
                    "SKIP: plan review requires discussion"
                    if ctx.plan_review_verdict == "NEEDS_DISCUSSION"
                    else "SKIP: plan review verdict is unknown or unparseable"
                ),
            },
            reason=(
                "plan-review-needs-discussion"
                if ctx.plan_review_verdict == "NEEDS_DISCUSSION"
                else "plan-review-unknown-verdict"
            ),
            subject_task_id=ctx.task.id,
        ),
    ),
    AdvanceRule(
        name="plan_review_manual_creation_required",
        matches=lambda ctx: (
            ctx.task_type in {"plan", "plan_improve"}
            and ctx.task.status == "completed"
            and not ctx.has_non_dropped_implement_descendant
            and ctx.auto_implement_enabled
            and ctx.require_plan_review_before_implement
            and not ctx.advance_create_plan_reviews
            and ctx.active_plan_review_pending is None
            and ctx.active_plan_review_running is None
            and ctx.latest_completed_plan_review is None
        ),
        action=lambda ctx: with_needs_attention(
            {
                "type": "needs_discussion",
                "description": "SKIP: no plan review exists and advance_create_plan_reviews=false",
            },
            reason="plan-review-needs-manual-creation",
            subject_task_id=ctx.task.id,
        ),
    ),
    AdvanceRule(
        name="plan_review_failed_retry_limit",
        matches=lambda ctx: (
            ctx.task_type in {"plan", "plan_improve"}
            and ctx.task.status == "completed"
            and not ctx.has_non_dropped_implement_descendant
            and ctx.auto_implement_enabled
            and ctx.require_plan_review_before_implement
            and ctx.advance_create_plan_reviews
            and ctx.active_plan_review_pending is None
            and ctx.active_plan_review_running is None
            and ctx.latest_completed_plan_review is None
            and ctx.failed_plan_review_count > 0
            and ctx.failed_plan_review_count >= ctx.max_failed_plan_review_retries
        ),
        action=_failed_plan_review_retry_limit_action,
    ),
    AdvanceRule(
        name="plan_create_review",
        matches=lambda ctx: (
            ctx.task_type in {"plan", "plan_improve"}
            and ctx.task.status == "completed"
            and not ctx.has_non_dropped_implement_descendant
            and ctx.auto_implement_enabled
            and ctx.require_plan_review_before_implement
            and ctx.active_plan_review_pending is None
            and ctx.active_plan_review_running is None
            and ctx.latest_completed_plan_review is None
            and ctx.advance_create_plan_reviews
        ),
        action=lambda ctx: {"type": "create_plan_review", "description": "Create and start plan review task"},
    ),
    AdvanceRule(
        name="explore_needs_followup_decision",
        matches=lambda ctx: (
            ctx.task_type == "explore"
            and ctx.task.status == "completed"
            and not ctx.has_non_dropped_plan_or_implement_descendant
        ),
        action=lambda ctx: with_needs_attention(
            {
                "type": "needs_discussion",
                "description": (
                    "SKIP: completed explore has no plan or implement follow-up; "
                    "decide whether to drop it or spawn follow-up work"
                ),
            },
            reason="explore-needs-follow-up-decision",
            subject_task_id=ctx.task.id,
        ),
    ),
    AdvanceRule(
        name="no_branch",
        matches=lambda ctx: not ctx.has_branch,
        action=lambda ctx: {"type": "skip", "description": _no_branch_description(ctx)},
    ),
    AdvanceRule(
        name="merge_source_needs_reconcile",
        matches=lambda ctx: is_diverged_merge_source_warning(ctx.merge_source_warning),
        action=lambda ctx: {
            "type": "reconcile_branch_divergence",
            "description": (
                f"Reconcile diverged local/origin refs for '{ctx.task.branch}'"
                if ctx.task.branch
                else "Reconcile diverged local/origin refs"
            ),
        },
    ),
    AdvanceRule(
        name="merge_source_needs_manual_resolution",
        matches=lambda ctx: ctx.merge_source_warning is not None,
        action=lambda ctx: with_needs_attention(
            {
                "type": "needs_discussion",
                "description": f"SKIP: {ctx.merge_source_warning}",
            },
            reason="merge-source-needs-manual-resolution",
            subject_task_id=ctx.task.id,
        ),
    ),
    AdvanceRule(
        name="empty_branch",
        matches=lambda ctx: ctx.merge_state in {"empty", "redundant"},
        action=lambda ctx: {"type": "skip", "description": _empty_merge_state_description(ctx)},
    ),
    AdvanceRule(
        name="target_already_merged",
        matches=lambda ctx: (
            ctx.post_merge_rebase_state is not None
            and ctx.post_merge_rebase_state.already_merged
        ),
        action=lambda ctx: {
            "type": "skip",
            "description": _target_already_merged_description(ctx),
            "advance_reason": "target-already-merged",
        },
    ),
    AdvanceRule(
        name="rebase_target_missing_merge_unit",
        matches=lambda ctx: (
            ctx.task_type == "rebase"
            and ctx.post_merge_rebase_state is not None
            and ctx.post_merge_rebase_state.rebase_target_missing_merge_unit
        ),
        action=lambda ctx: {"type": "skip", "description": _rebase_target_missing_merge_unit_description(ctx)},
    ),
    AdvanceRule(
        name="already_merged",
        matches=lambda ctx: merge_state_is_terminal_for_lifecycle(ctx.merge_state),
        action=lambda ctx: {"type": "skip", "description": _merge_terminal_description(ctx)},
    ),
    AdvanceRule(
        name="strict_project_scope_unverified",
        matches=lambda ctx: ctx.strict_scope_inspection_error is not None,
        action=_strict_scope_unverified_action,
    ),
    AdvanceRule(
        name="strict_project_scope_violation",
        matches=lambda ctx: bool(ctx.strict_scope_violation_paths),
        action=_strict_scope_violation_action,
    ),
    AdvanceRule(
        name="spec_coherence_diff_unverified",
        matches=lambda ctx: _spec_coherence_gate_required(ctx) and ctx.spec_coherence_inspection_error is not None,
        action=_spec_coherence_gate_needs_attention,
    ),
    AdvanceRule(
        name="spec_coherence_run_pending_review",
        matches=lambda ctx: _spec_coherence_gate_required(ctx)
        and not _spec_coherence_gate_currently_approved(ctx)
        and ctx.spec_coherence_active_review is not None
        and ctx.spec_coherence_active_review.status == "pending",
        action=lambda ctx: {
            "type": "run_review",
            "description": f"Run pending behavior-spec coherence review {_task_id(ctx.spec_coherence_active_review)}",
            "review_task": ctx.spec_coherence_active_review,
        },
    ),
    AdvanceRule(
        name="spec_coherence_wait_review",
        matches=lambda ctx: _spec_coherence_gate_required(ctx)
        and not _spec_coherence_gate_currently_approved(ctx)
        and ctx.spec_coherence_active_review is not None
        and ctx.spec_coherence_active_review.status == "in_progress",
        action=lambda ctx: {
            "type": "wait_review",
            "description": f"SKIP: behavior-spec coherence review {_task_id(ctx.spec_coherence_active_review)} is in_progress",
            "review_task": ctx.spec_coherence_active_review,
        },
    ),
    AdvanceRule(
        name="spec_coherence_wait_improve",
        matches=lambda ctx: _spec_coherence_gate_required(ctx)
        and ctx.spec_coherence_review_current
        and ctx.spec_coherence_review_verdict == "CHANGES_REQUESTED"
        and ctx.active_improve_running is not None,
        action=lambda ctx: {
            "type": "wait_improve",
            "description": f"SKIP: improve task {_task_id(ctx.active_improve_running)} is in_progress",
            "improve_task": ctx.active_improve_running,
        },
    ),
    AdvanceRule(
        name="spec_coherence_run_pending_improve",
        matches=lambda ctx: _spec_coherence_gate_required(ctx)
        and ctx.spec_coherence_review_current
        and ctx.spec_coherence_review_verdict == "CHANGES_REQUESTED"
        and ctx.active_improve_pending is not None,
        action=lambda ctx: {
            "type": "run_improve",
            "description": f"Spawn worker for pending improve {_task_id(ctx.active_improve_pending)}",
            "improve_task": ctx.active_improve_pending,
        },
    ),
    AdvanceRule(
        name="spec_coherence_create_improve",
        matches=lambda ctx: _spec_coherence_gate_required(ctx)
        and ctx.spec_coherence_review_current
        and ctx.spec_coherence_review_verdict == "CHANGES_REQUESTED",
        action=lambda ctx: {
            "type": "improve",
            "description": "Create improve task (behavior-spec coherence review CHANGES_REQUESTED)",
            "review_task": ctx.spec_coherence_latest_completed_review,
        },
    ),
    AdvanceRule(
        name="spec_coherence_needs_discussion",
        matches=lambda ctx: _spec_coherence_gate_required(ctx)
        and ctx.spec_coherence_review_current
        and ctx.spec_coherence_review_verdict == "NEEDS_DISCUSSION",
        action=_spec_coherence_needs_discussion_action,
    ),
    AdvanceRule(
        name="spec_coherence_unknown_verdict",
        matches=lambda ctx: _spec_coherence_gate_required(ctx)
        and ctx.spec_coherence_review_current
        and ctx.spec_coherence_latest_completed_review is not None
        and ctx.spec_coherence_review_verdict not in {
            "APPROVED",
            "CHANGES_REQUESTED",
            "NEEDS_DISCUSSION",
        },
        action=_spec_coherence_unknown_verdict_action,
    ),
    AdvanceRule(
        name="spec_coherence_run_pending_ordinary_review",
        matches=lambda ctx: _spec_coherence_gate_required(ctx)
        and not _spec_coherence_gate_currently_approved(ctx)
        and ctx.spec_coherence_active_review is None
        and ctx.active_review is not None
        and ctx.active_review.status == "pending",
        action=lambda ctx: {
            "type": "run_review",
            "description": (
                f"Run pending review {_task_id(ctx.active_review)} before creating the "
                "behavior-spec coherence review"
            ),
            "review_task": ctx.active_review,
        },
    ),
    AdvanceRule(
        name="spec_coherence_wait_ordinary_review",
        matches=lambda ctx: _spec_coherence_gate_required(ctx)
        and not _spec_coherence_gate_currently_approved(ctx)
        and ctx.spec_coherence_active_review is None
        and ctx.active_review is not None
        and ctx.active_review.status == "in_progress",
        action=lambda ctx: {
            "type": "wait_review",
            "description": (
                f"SKIP: review {_task_id(ctx.active_review)} is in_progress before creating the "
                "behavior-spec coherence review"
            ),
            "review_task": ctx.active_review,
        },
    ),
    AdvanceRule(
        name="spec_coherence_create_review",
        matches=lambda ctx: _spec_coherence_gate_required(ctx)
        and not _spec_coherence_gate_currently_approved(ctx)
        and ctx.spec_coherence_active_review is None,
        action=_spec_coherence_create_review_action,
    ),
    AdvanceRule(
        name="conflict_rebase_running",
        matches=lambda ctx: not ctx.can_merge and ctx.rebase_pending_or_running is not None,
        action=lambda ctx: {
            "type": "skip",
            "description": f"SKIP: rebase {_task_id(ctx.rebase_pending_or_running)} already in progress",
        },
    ),
    AdvanceRule(
        name="conflict_rebase_failure_circuit_breaker",
        matches=lambda ctx: (
            not ctx.can_merge
            and ctx.rebase_failure_streak is not None
            and ctx.rebase_failure_streak.attempts >= REBASE_FAILURE_CIRCUIT_BREAKER_ATTEMPTS
        ),
        action=_rebase_failure_circuit_breaker_action,
    ),
    AdvanceRule(
        name="conflict_rebase_failed",
        matches=lambda ctx: not ctx.can_merge and _failed_rebase_still_blocks_advance(ctx),
        action=lambda ctx: with_needs_attention(
            {
                "type": "needs_discussion",
                "description": f"SKIP: rebase {_task_id(ctx.rebase_failed)} failed, needs manual resolution",
            },
            reason="rebase-failed-needs-manual-resolution",
            subject_task_id=ctx.task.id,
        ),
    ),
    AdvanceRule(
        name="conflict_rebase_completed_but_still_blocked",
        matches=lambda ctx: (
            not ctx.can_merge
            and ctx.latest_completed_rebase is not None
            and _branch_contains_target_tip(ctx)
        ),
        action=_rebase_did_not_unblock_merge_action,
    ),
    AdvanceRule(
        name="conflict_needs_rebase",
        matches=lambda ctx: (
            ctx.selected_for_merge
            and not ctx.can_merge
            and ctx.rebase_pending_or_running is None
            and not _branch_contains_target_tip(ctx)
        ),
        action=lambda ctx: {
            "type": "needs_rebase",
            "description": "rebase --resolve (conflicts detected)",
            "reason": "merge-selection-conflict-rebase",
        },
    ),
    AdvanceRule(
        name="already_rebased_but_lineage_incomplete",
        matches=lambda ctx: (
            not ctx.can_merge
            and _branch_contains_target_tip(ctx)
            and ctx.task.status != "completed"
        ),
        action=_already_rebased_but_lineage_incomplete_action,
    ),
    AdvanceRule(
        name="resolution_review_metadata_invalid",
        matches=lambda ctx: (
            ctx.resolution_review_metadata_invalid
            and _is_implementation_owned_lineage(ctx)
            and ctx.requires_review
        ),
        action=_resolution_review_metadata_invalid_action,
    ),
    AdvanceRule(
        name="stale_review_run_pending_review",
        matches=lambda ctx: (
            _stale_review_refresh_required(ctx)
            and ctx.create_reviews
            and ctx.active_review is not None
            and ctx.active_review.status == "pending"
        ),
        action=lambda ctx: {
            "type": "run_review",
            "description": _stale_review_pending_review_description(ctx),
            "review_task": ctx.active_review,
        },
    ),
    AdvanceRule(
        name="review_freshness_probe_failed",
        matches=_review_freshness_probe_failed,
        action=lambda ctx: with_needs_attention(
            {
                "type": "needs_discussion",
                "description": _review_freshness_probe_failed_description(ctx),
                "probe_warning": ctx.current_review_head_probe_warning,
            },
            reason="review-freshness-unverified",
            subject_task_id=(
                ctx.task.id
                if getattr(ctx, "review_root_task", None) is None
                else getattr(ctx.review_root_task, "id", ctx.task.id)
            ),
        ),
    ),
    AdvanceRule(
        name="stale_review_wait_review",
        matches=lambda ctx: (
            _stale_review_refresh_required(ctx)
            and ctx.create_reviews
            and ctx.active_review is not None
            and ctx.active_review.status == "in_progress"
        ),
        action=lambda ctx: {
            "type": "wait_review",
            "description": _stale_review_wait_review_description(ctx),
            "review_task": ctx.active_review,
        },
    ),
    AdvanceRule(
        name="stale_review_create_review",
        matches=lambda ctx: _stale_review_refresh_required(ctx) and ctx.create_reviews,
        action=_stale_review_create_review_action,
    ),
    AdvanceRule(
        name="stale_review_needs_manual_refresh",
        matches=lambda ctx: _stale_review_refresh_required(ctx) and not ctx.create_reviews,
        action=lambda ctx: with_needs_attention(
            {
                "type": "needs_discussion",
                "description": "SKIP: review must be refreshed before merge",
            },
            reason="stale-review-needs-manual-refresh",
            subject_task_id=(
                ctx.task.id
                if getattr(ctx, "review_root_task", None) is None
                else getattr(ctx.review_root_task, "id", ctx.task.id)
            ),
        ),
    ),
    AdvanceRule(
        name="failed_rebase_without_successful_review",
        matches=lambda ctx: _failed_rebase_still_blocks_advance(ctx),
        action=lambda ctx: with_needs_attention(
            {
                "type": "needs_discussion",
                "description": f"SKIP: rebase {_task_id(ctx.rebase_failed)} failed, needs manual resolution",
            },
            reason="rebase-failed-needs-manual-resolution",
            subject_task_id=ctx.task.id,
        ),
    ),
    AdvanceRule(
        name="failed_rebase_without_successful_review",
        matches=lambda ctx: _failed_rebase_still_blocks_advance(ctx),
        action=lambda ctx: with_needs_attention(
            {
                "type": "needs_discussion",
                "description": f"SKIP: rebase {_task_id(ctx.rebase_failed)} failed, needs manual resolution",
            },
            reason="rebase-failed-needs-manual-resolution",
            subject_task_id=ctx.task.id,
        ),
    ),
    AdvanceRule(
        name="closing_review_invariant",
        matches=_closing_review_requires_automation,
        action=_closing_review_invariant_action,
    ),
    AdvanceRule(
        name="review_pending",
        matches=lambda ctx: _active_review_requires_automation(ctx)
        and ctx.active_review is not None
        and ctx.active_review.status == "pending",
        action=lambda ctx: {
            "type": "run_review",
            "description": f"Spawn worker for pending review {_task_id(ctx.active_review)}",
            "review_task": ctx.active_review,
        },
    ),
    AdvanceRule(
        name="review_in_progress",
        matches=lambda ctx: _active_review_requires_automation(ctx)
        and ctx.active_review is not None
        and ctx.active_review.status == "in_progress",
        action=lambda ctx: {
            "type": "wait_review",
            "description": f"SKIP: review {_task_id(ctx.active_review)} is in_progress",
            "review_task": ctx.active_review,
        },
    ),
    AdvanceRule(
        name="fresh_comments_wait_improve",
        matches=lambda ctx: ctx.task_type == "implement"
        and ctx.latest_completed_review is not None
        and ctx.has_fresh_unresolved_comments_since_latest_review
        and (ctx.review_cleared or ctx.review_verdict in {"APPROVED", "APPROVED_WITH_FOLLOWUPS"})
        and ctx.active_improve_running is not None,
        action=lambda ctx: {
            "type": "wait_improve",
            "description": (
                "SKIP: unresolved comments newer than latest review; "
                f"improve task {_task_id(ctx.active_improve_running)} is in_progress"
            ),
            "improve_task": ctx.active_improve_running,
        },
    ),
    AdvanceRule(
        name="fresh_comments_run_pending_improve",
        matches=lambda ctx: ctx.task_type == "implement"
        and ctx.latest_completed_review is not None
        and ctx.has_fresh_unresolved_comments_since_latest_review
        and (ctx.review_cleared or ctx.review_verdict in {"APPROVED", "APPROVED_WITH_FOLLOWUPS"})
        and ctx.active_improve_pending is not None,
        action=lambda ctx: {
            "type": "run_improve",
            "description": (
                "Run pending improve for unresolved comments newer than latest review: "
                f"{_task_id(ctx.active_improve_pending)}"
            ),
            "improve_task": ctx.active_improve_pending,
        },
    ),
    AdvanceRule(
        name="fresh_comments_noop_improve_limit",
        matches=lambda ctx: ctx.task_type == "implement"
        and ctx.noop_improve_trigger == "comments"
        and ctx.consecutive_noop_improves >= ctx.max_noop_improve_cycles,
        action=_noop_improve_limit_action,
    ),
    AdvanceRule(
        name="fresh_comments_create_improve",
        matches=lambda ctx: ctx.task_type == "implement"
        and ctx.latest_completed_review is not None
        and ctx.has_fresh_unresolved_comments_since_latest_review
        and (ctx.review_cleared or ctx.review_verdict in {"APPROVED", "APPROVED_WITH_FOLLOWUPS"}),
        action=lambda ctx: {
            "type": "improve",
            "description": (
                "Create improve task (unresolved comments newer than latest review)"
                f"{_noop_improve_followup_suffix(ctx)}"
            ),
            "review_task": ctx.latest_completed_review,
        },
    ),
    AdvanceRule(
        name="review_approved_with_followups",
        matches=lambda ctx: execution_status_allows_merge(ctx)
        and has_valid_review_for_merge(ctx)
        and (not ctx.review_cleared)
        and ctx.latest_completed_review is not None
        and ctx.review_verdict == "APPROVED_WITH_FOLLOWUPS"
        and bool(ctx.followup_findings),
        action=lambda ctx: {
            "type": "merge_with_followups",
            "description": _merge_review_description("APPROVED_WITH_FOLLOWUPS", ctx.review_preserved_by_rebase),
            "review_task": ctx.latest_completed_review,
            "followup_findings": ctx.followup_findings,
        },
    ),
    AdvanceRule(
        name="review_approved",
        matches=lambda ctx: execution_status_allows_merge(ctx)
        and has_valid_review_for_merge(ctx)
        and (not ctx.review_cleared)
        and ctx.latest_completed_review is not None
        and ctx.review_verdict == "APPROVED",
        action=lambda ctx: {
            "type": "merge",
            "description": _merge_review_description("APPROVED", ctx.review_preserved_by_rebase),
            "review_task": ctx.latest_completed_review,
        },
    ),
    AdvanceRule(
        name="review_wait_improve",
        matches=lambda ctx: (not ctx.review_cleared)
        and ctx.review_verdict == "CHANGES_REQUESTED"
        and ctx.active_improve_running is not None,
        action=lambda ctx: {
            "type": "wait_improve",
            "description": f"SKIP: improve task {_task_id(ctx.active_improve_running)} is in_progress",
            "improve_task": ctx.active_improve_running,
        },
    ),
    AdvanceRule(
        name="review_run_pending_improve",
        matches=lambda ctx: (not ctx.review_cleared)
        and ctx.review_verdict == "CHANGES_REQUESTED"
        and ctx.active_improve_pending is not None,
        action=lambda ctx: {
            "type": "run_improve",
            "description": f"Spawn worker for pending improve {_task_id(ctx.active_improve_pending)}",
            "improve_task": ctx.active_improve_pending,
        },
    ),
    AdvanceRule(
        name="review_clear_off_topic_verify_blocker",
        matches=lambda ctx: (not ctx.review_cleared)
        and ctx.review_verdict == "CHANGES_REQUESTED"
        and ctx.active_improve_running is None
        and ctx.active_improve_pending is None
        and ctx.off_topic_verify_clearance_candidate is not None,
        action=_clear_off_topic_verify_blocker_action,
    ),
    AdvanceRule(
        name="review_blocker_adjudication_needed",
        matches=lambda ctx: (not ctx.review_cleared)
        and ctx.review_verdict == "CHANGES_REQUESTED"
        and ctx.review_blocker_adjudication_needed,
        action=_review_blocker_adjudication_needed_action,
    ),
    AdvanceRule(
        name="review_wait_blocker_adjudication",
        matches=lambda ctx: (not ctx.review_cleared)
        and ctx.review_verdict == "CHANGES_REQUESTED"
        and ctx.active_review_blocker_adjudication is not None
        and ctx.active_review_blocker_adjudication.status == "in_progress",
        action=lambda ctx: {
            "type": "wait_review_adjudication",
            "description": _wait_review_blocker_adjudication_description(
                ctx.active_review_blocker_adjudication
            ),
            "review_adjudication_task": ctx.active_review_blocker_adjudication,
        },
    ),
    AdvanceRule(
        name="review_run_pending_blocker_adjudication",
        matches=lambda ctx: (not ctx.review_cleared)
        and ctx.review_verdict == "CHANGES_REQUESTED"
        and ctx.active_review_blocker_adjudication is not None
        and ctx.active_review_blocker_adjudication.status == "pending",
        action=lambda ctx: {
            "type": "run_review_adjudication",
            "description": _run_review_blocker_adjudication_description(
                ctx.active_review_blocker_adjudication
            ),
            "review_adjudication_task": ctx.active_review_blocker_adjudication,
        },
    ),
    AdvanceRule(
        name="review_create_blocker_adjudication",
        matches=lambda ctx: (not ctx.review_cleared)
        and ctx.review_verdict == "CHANGES_REQUESTED"
        and ctx.active_improve_running is None
        and ctx.active_improve_pending is None
        and ctx.review_blocker_adjudication_candidate is not None
        and (
            ctx.review_blocker_adjudication_candidate.dispute_artifact is None
            or ctx.consecutive_noop_improves >= ctx.max_noop_improve_cycles
        )
        and ctx.active_review_blocker_adjudication is None,
        action=lambda ctx: {
            "type": "create_review_adjudication",
            "description": _review_blocker_adjudication_description(ctx),
            "review_task": ctx.latest_completed_review,
            "review_blocker_adjudication_candidate": ctx.review_blocker_adjudication_candidate,
        },
    ),
    AdvanceRule(
        name="review_recover_verify_only_noop_review",
        matches=lambda ctx: (not ctx.review_cleared)
        and ctx.review_verdict == "CHANGES_REQUESTED"
        and ctx.consecutive_noop_improves >= ctx.max_noop_improve_cycles
        and ctx.active_improve_running is None
        and ctx.active_improve_pending is None
        and ctx.latest_completed_review is not None
        and ctx.latest_completed_review.review_verify_status == "failed"
        and ctx.latest_noop_improve is not None
        and ctx.latest_reviewed_head_sha is not None
        and ctx.current_review_head_sha is not None
        and ctx.current_review_head_sha == ctx.latest_reviewed_head_sha
        and _latest_review_is_verify_blocked_only(ctx)
        and not _task_has_current_passing_review_verify_evidence(
            task=ctx.latest_noop_improve,
            review_task=ctx.latest_completed_review,
            current_branch=ctx.task.branch,
            current_head_sha=ctx.current_review_head_sha,
        )
        and ctx.current_review_head_probe_warning is None
        and ctx.noop_improve_verify_recovery_attention_message is None
        and ctx.noop_improve_verify_probe_warning is None,
        action=_recover_verify_only_noop_review_action,
    ),
    AdvanceRule(
        name="review_noop_improve_limit",
        matches=lambda ctx: (not ctx.review_cleared)
        and ctx.review_verdict == "CHANGES_REQUESTED"
        and ctx.consecutive_noop_improves >= ctx.max_noop_improve_cycles
        and ctx.active_improve_running is None
        and ctx.active_improve_pending is None,
        action=_noop_improve_limit_action,
    ),
    AdvanceRule(
        name="review_verify_blocked_no_code_issues",
        matches=lambda ctx: (not ctx.review_cleared)
        and ctx.review_verdict == "CHANGES_REQUESTED"
        and len(ctx.recent_verify_timeout_only_reviews) >= VERIFY_BLOCKED_REVIEW_THRESHOLD
        and ctx.active_improve_running is None
        and ctx.active_improve_pending is None,
        action=_verify_blocked_no_code_issues_action,
    ),
    AdvanceRule(
        name="review_duplicate_blocker_no_progress",
        matches=lambda ctx: (not ctx.review_cleared)
        and ctx.review_verdict == "CHANGES_REQUESTED"
        and ctx.duplicate_blocker_streak is not None
        and not ctx.review_blockers_revalidated,
        action=_duplicate_blocker_needs_attention_action,
    ),
    AdvanceRule(
        name="review_max_cycles",
        matches=lambda ctx: (not ctx.review_cleared)
        and ctx.review_verdict == "CHANGES_REQUESTED"
        and ctx.completed_review_cycles >= ctx.max_review_cycles
        and not ctx.review_blockers_revalidated,
        action=lambda ctx: with_needs_attention(
            {
                "type": "max_cycles_reached",
                "description": (
                    f"SKIP: max review cycles ({ctx.max_review_cycles}) reached, needs manual intervention"
                ),
            },
            reason=PARK_REASON_REVIEW_MAX_CYCLES_REACHED,
            subject_task_id=ctx.task.id,
        ),
    ),
    AdvanceRule(
        name="review_create_improve",
        matches=lambda ctx: (not ctx.review_cleared) and ctx.review_verdict == "CHANGES_REQUESTED",
        action=lambda ctx: {
            "type": "improve",
            "description": f"Create improve task (review CHANGES_REQUESTED){_noop_improve_followup_suffix(ctx)}",
            "review_task": ctx.latest_completed_review,
        },
    ),
    AdvanceRule(
        name="review_unknown_verdict",
        matches=lambda ctx: (not ctx.review_cleared) and ctx.latest_completed_review is not None,
        action=lambda ctx: with_needs_attention(
            {
                "type": "needs_discussion",
                "description": f"SKIP: review verdict is {ctx.review_verdict or 'unknown'}, needs manual attention",
                "review_task": ctx.latest_completed_review,
            },
            reason="review-verdict-needs-manual-attention",
            subject_task_id=_needs_attention_subject_id(ctx),
        ),
    ),
    AdvanceRule(
        name="review_cleared_but_sibling_review_unresolved",
        matches=lambda ctx: execution_status_allows_merge(ctx)
        and has_valid_review_for_merge(ctx)
        and ctx.review_cleared
        and ctx.latest_completed_review is not None
        and _requires_sibling_review_attention(ctx),
        action=_noop_improve_sibling_review_attention_action,
    ),
    AdvanceRule(
        name="reviews_all_cleared",
        matches=lambda ctx: execution_status_allows_merge(ctx)
        and has_valid_review_for_merge(ctx)
        and ctx.review_cleared
        and ctx.latest_completed_review is not None,
        action=_merge_review_cleared_action,
    ),
    AdvanceRule(
        name="non_implement_no_review",
        matches=lambda ctx: execution_status_allows_merge(ctx) and not _is_implementation_owned_lineage(ctx),
        action=lambda ctx: {"type": "merge", "description": "Merge task (no review yet)"},
    ),
    AdvanceRule(
        name="implement_create_review",
        matches=lambda ctx: ctx.requires_review and ctx.create_reviews,
        action=lambda ctx: {"type": "create_review", "description": "Create review (required before merge)"},
    ),
    AdvanceRule(
        name="implement_needs_manual_review",
        matches=lambda ctx: ctx.requires_review and not ctx.create_reviews,
        action=lambda ctx: with_needs_attention(
            {
                "type": "needs_discussion",
                "description": "SKIP: no review exists and advance_create_reviews=false (run gza review manually)",
            },
            reason="review-needs-manual-creation",
            subject_task_id=ctx.task.id,
        ),
    ),
    AdvanceRule(
        name="implement_no_review_required",
        matches=lambda ctx: execution_status_allows_merge(ctx) and not ctx.requires_review,
        action=lambda ctx: {"type": "merge", "description": "Merge task (no review yet)"},
    ),
]


def evaluate_advance_rules(
    config: Any,
    store: SqliteTaskStore,
    git: Any,
    task: DbTask,
    target_branch: str,
    *,
    impl_based_on_ids: set[str] | None = None,
    max_resume_attempts: int | None = None,
    persist_post_merge_rebase_state: bool = True,
    persist_review_clearance: bool = True,
    read_context: RecoveryReadContext | None = None,
    selected_for_merge: bool = False,
) -> dict[str, Any]:
    """Evaluate ordered advance rules for a task and return an action dict."""
    context = resolve_advance_context(
        config,
        store,
        git,
        task,
        target_branch,
        impl_based_on_ids=impl_based_on_ids,
        max_resume_attempts=max_resume_attempts,
        persist_post_merge_rebase_state=persist_post_merge_rebase_state,
        persist_review_clearance=persist_review_clearance,
        read_context=read_context,
        selected_for_merge=selected_for_merge,
    )

    for rule in ADVANCE_RULES:
        if rule.matches(context):
            action = rule.action(context)
            require_needs_attention_subject(action)
            return action

    return {"type": "skip", "description": "SKIP: no matching rule (unexpected)"}
