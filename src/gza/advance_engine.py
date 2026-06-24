"""Declarative advance/iterate rule engine."""

from __future__ import annotations

import logging
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from gza.branch_resolution import resolve_rebase_target_task
from gza.config import DEFAULT_MAX_NOOP_IMPROVE_CYCLES
from gza.console import prompt_available_width, shorten_prompt
from gza.db import SqliteTaskStore, Task as DbTask, task_id_numeric_key, task_owns_merge_status
from gza.git import ResolvedMergeSourceRef
from gza.lifecycle_completion import merge_state_is_terminal_for_lifecycle
from gza.lineage import walk_ancestors, walk_based_on_descendants
from gza.merge_state import resolve_task_merge_state_for_target
from gza.operator_state import terminal_no_work_lifecycle_detail
from gza.plan_review_materialization import load_materialized_plan_slice_set
from gza.plan_review_verdict import (
    PlanReviewManifest,
    get_plan_review_outcome,
)
from gza.project_discovery import (
    parse_name_status_project_paths,
)
from gza.query import (
    get_code_changing_descendants_for_root,
    get_reviews_for_root,
    resolve_lineage_root,
)
from gza.recovery_engine import (
    FailedRecoveryDecision,
    classify_failure_reason,
    decide_failed_task_recovery,
    get_failed_recovery_needs_attention_reason,
)
from gza.recovery_read_context import RecoveryReadContext
from gza.resume_policy import is_resumable_failed_task as _is_resumable_failed_task
from gza.review_verdict import (
    ParsedReviewReport,
    ReviewBlockerSummary,
    ReviewFinding,
    get_review_content,
    get_review_report,
    is_verify_timeout_only_review,
    summarize_review_blockers,
)
from gza.runner import (
    CROSS_PROJECT_TAG,
    PROJECT_SCOPE_VIOLATION_FAILURE_REASON,
    _filter_owned_artifact_paths,
    _find_out_of_scope_paths,
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
FIX_HANDOFF_NEEDS_ATTENTION_REASONS = frozenset(
    {
        "review-max-cycles-reached",
        "automatic-recovery-disabled",
        "retry-limit-reached",
        "retryable-provider-error",
    }
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
        "improve",
        "run_improve",
        "resume",
        "retry",
    }
)
MERGEABLE_EXECUTION_STATUSES = frozenset({"completed", "unmerged"})
VERIFY_BLOCKED_REVIEW_THRESHOLD = 2
_LOG = logging.getLogger(__name__)


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


@dataclass(frozen=True)
class AdvanceContext:
    """Resolved task state used by advance rules."""

    store: SqliteTaskStore
    task: DbTask
    task_type: str
    has_branch: bool

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
    strict_scope_violation_paths: tuple[str, ...] = ()
    strict_scope_inspection_error: str | None = None
    rebase_pending_or_running: DbTask | None = None
    rebase_failed: DbTask | None = None
    latest_completed_rebase: DbTask | None = None
    rebase_failure_streak: RebaseFailureStreak | None = None
    rebase_invalidates_review: bool = False
    review_preserved_by_rebase: DbTask | None = None
    review_invalidated_by_rebase: DbTask | None = None

    reviews: list[DbTask] | None = None
    review_root_task: DbTask | None = None
    active_review: DbTask | None = None
    latest_completed_review: DbTask | None = None
    latest_completed_code_change: DbTask | None = None
    review_cleared: bool = False
    review_verdict: str | None = None
    review_report: ParsedReviewReport | None = None
    latest_review_blocker_summary: ReviewBlockerSummary | None = None
    followup_findings: tuple[ReviewFinding, ...] = ()
    recent_verify_timeout_only_reviews: tuple[DbTask, ...] = ()

    completed_review_cycles: int = 0
    active_improve_running: DbTask | None = None
    active_improve_pending: DbTask | None = None
    latest_noop_improve: DbTask | None = None
    consecutive_noop_improves: int = 0
    noop_improve_trigger: str | None = None
    noop_improve_verify_probe_warning: str | None = None
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
class StrictScopeInspection:
    """Resolved strict-scope inspection state for advance-time gating."""

    violation_paths: tuple[str, ...] = ()
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


def _latest_review_is_verify_blocked_only(ctx: AdvanceContext) -> bool:
    latest_review_blocker_summary = getattr(ctx, "latest_review_blocker_summary", None)
    return (
        ctx.review_verdict == "CHANGES_REQUESTED"
        and latest_review_blocker_summary is not None
        and latest_review_blocker_summary.is_verify_blocked_only
    )


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


def _has_persisted_noop_improve_verify_clearance(
    *,
    git: Any,
    project_dir: Path,
    task: DbTask,
    latest_completed_review: DbTask | None,
    improve_tasks: list[DbTask],
) -> tuple[bool, str | None]:
    """Fail closed unless current branch-head evidence clears a verify-only review."""
    if latest_completed_review is None or task.branch is None:
        return False, None

    branch_head = _resolve_branch_head_sha(git, task.branch)
    if branch_head.warning is not None:
        return False, branch_head.warning

    current_head_sha = branch_head.head_sha
    if not _review_is_verify_only_blocked_at_head(
        project_dir=project_dir,
        review_task=latest_completed_review,
        current_branch=task.branch,
        current_head_sha=current_head_sha,
    ):
        return False, None

    return (
        any(
        improve.status == "completed"
        and improve.changed_diff is False
        and _task_has_current_passing_review_verify_evidence(
            task=improve,
            review_task=latest_completed_review,
            current_branch=task.branch,
            current_head_sha=current_head_sha,
        )
        for improve in improve_tasks
        ),
        None,
    )


def _normalize_blocker_title(title: str) -> str:
    normalized = re.sub(r"`+", "", title).strip().lower()
    normalized = re.sub(r"^#+\s*", "", normalized)
    normalized = re.sub(r"\s+", " ", normalized)
    normalized = re.sub(r"^(?:[a-z]+-)?b\d+\s*[:.)-]?\s*", "", normalized)
    return normalized.strip()


def _normalize_blocker_anchor(value: str) -> str:
    normalized = re.sub(r"`+", "", value).strip().lower()
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized


def _primary_blocker_fingerprint(
    report: ParsedReviewReport,
) -> tuple[tuple[str, str], str, str] | None:
    primary_blocker = next(
        (finding for finding in report.findings if finding.severity == "BLOCKER"),
        None,
    )
    if primary_blocker is None:
        return None

    title = _normalize_blocker_title(primary_blocker.title)
    if not title:
        return None

    raw_anchor: str | None = None
    if primary_blocker.open_state_citation:
        citation_tokens = [
            token.strip()
            for token in primary_blocker.open_state_citation.split(",")
            if token.strip()
        ]
        if citation_tokens:
            raw_anchor = citation_tokens[0]
    if raw_anchor is None and primary_blocker.fix_or_followup:
        raw_anchor = primary_blocker.fix_or_followup
    if raw_anchor is None:
        return None

    anchor = _normalize_blocker_anchor(raw_anchor)
    if not anchor:
        return None
    return (title, anchor), primary_blocker.title, anchor


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
        return f"Create review (rebase {task_id} change unknown)"
    return f"Create review (rebase {task_id} changed diff)"


def _rebase_pending_review_description(review_task: DbTask | None, rebase_task: DbTask | None) -> str:
    return f"Run pending review {_task_id(review_task)} (rebase {_task_id(rebase_task)} {_rebase_change_reason(rebase_task)})"


def _rebase_wait_review_description(review_task: DbTask | None, rebase_task: DbTask | None) -> str:
    return f"SKIP: review {_task_id(review_task)} in progress (rebase {_task_id(rebase_task)} {_rebase_change_reason(rebase_task)})"


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
        },
        reason="improve-no-op",
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
                f"because {ctx.noop_improve_verify_probe_warning}. Review remains uncleared until "
                "branch-head freshness can be established."
            ),
            "probe_warning": ctx.noop_improve_verify_probe_warning,
        },
        reason="improve-no-op",
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
        },
        reason="verify-blocked-no-code-issues",
        subject_task_id=_needs_attention_subject_id(ctx),
    )


def _noop_improve_limit_action(ctx: AdvanceContext) -> dict[str, Any]:
    """Park a no-op loop unless runner-owned verify evidence has already cleared it."""
    if ctx.noop_improve_verify_probe_warning is not None:
        return _noop_improve_verify_probe_failure_action(ctx)
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
        reason="duplicate-blocker-no-progress",
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
    return state.rebase_resolution_proved or state.target_is_ancestor_of_branch is True


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
        subject_task_id=ctx.task.id,
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
        subject_task_id=ctx.task.id,
    )


def _failed_task_skip_action(ctx: AdvanceContext) -> dict[str, Any]:
    assert ctx.failed_recovery_decision is not None
    subject_task_id = ctx.task.id
    if ctx.task.task_type in {"review", "improve", "rebase"} and ctx.task.id is not None:
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
    return failed_recovery_decision_to_action(ctx.task, ctx.failed_recovery_decision)


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
        return fallback_task
    raise AssertionError(f"Unable to resolve subject task for action: {action!r}")


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
    failure_reason = task.failure_reason or "UNKNOWN"
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


def needs_attention_recommends_fix(action: Mapping[str, Any]) -> bool:
    """Return True when the operator handoff for this attention state is `gza fix`."""
    reason = get_needs_attention_reason(action)
    return reason in FIX_HANDOFF_NEEDS_ATTENTION_REASONS


def is_diverged_merge_source_warning(warning: str | None) -> bool:
    """Return True when the merge-source warning indicates local/remote divergence."""
    return isinstance(warning, str) and "diverged" in warning.lower()


def classify_advance_action(action: Mapping[str, Any]) -> str:
    """Bucket advance outcomes into actionable, needs_attention, or skip."""
    if is_needs_attention_action(action):
        return "needs_attention"
    action_type = str(action.get("type", "skip"))
    if action_type in {"skip", "wait_review", "wait_improve", "wait_plan_review", "wait_plan_improve"}:
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
    prompt = shorten_prompt(
        task.prompt or "",
        prompt_available_width(prefix=prefix, suffix=suffix),
    )
    return format_needs_attention_entry(task, prompt=prompt, action=action)


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
    materialized_tasks = load_materialized_plan_slice_set(
        store,
        review_task=latest_completed_plan_review,
        plan_source_task=latest_plan_source,
        manifest=manifest,
    )
    if materialized_tasks is None:
        return PlanMaterializationState(materialized=False)
    return PlanMaterializationState(
        materialized=True,
        task_ids=tuple(task.id for task in materialized_tasks if task.id is not None),
    )


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

    review_cleared_at = ctx.task.review_cleared_at
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
    unresolved_comments = store.get_comments(task_id, unresolved_only=True)
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
) -> tuple[
    list[DbTask],
    DbTask | None,
    DbTask | None,
    bool,
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
    bool,
    bool,
    DbTask | None,
    dict[str, Any] | None,
]:
    """Resolve review/improve lineage state for the implementation root task."""
    reviews = get_reviews_for_root(store, task)
    active_review = _select_active_review(reviews)
    completed_reviews = [r for r in reviews if r.status == "completed"]
    latest_completed_review = completed_reviews[0] if completed_reviews else None

    persisted_review_cleared = (
        latest_completed_review is not None
        and task.review_cleared_at is not None
        and latest_completed_review.completed_at is not None
        and task.review_cleared_at >= latest_completed_review.completed_at
    )
    review_cleared = persisted_review_cleared

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
    has_improve_after_review = False
    has_fresh_unresolved_comments_since_latest_review = False
    latest_completed_code_change: DbTask | None = None
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
        if not review_cleared:
            review_cleared, noop_improve_verify_probe_warning = _has_persisted_noop_improve_verify_clearance(
                git=git,
                project_dir=Path(config.project_dir),
                task=task,
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
            completed_review_cycles = count_completed_review_cycles(store, task.id)

        verify_timeout_only_reviews: list[DbTask] = []
        for review_task in completed_reviews:
            review_content = _get_review_output_content(config, review_task)
            if not is_verify_timeout_only_review(review_content):
                break
            verify_timeout_only_reviews.append(review_task)
            if len(verify_timeout_only_reviews) >= VERIFY_BLOCKED_REVIEW_THRESHOLD:
                break
        recent_verify_timeout_only_reviews = tuple(verify_timeout_only_reviews)

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
        review_cleared,
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
        has_improve_after_review,
        has_fresh_unresolved_comments_since_latest_review,
        latest_completed_code_change,
        closing_review_action,
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


def _is_implementation_owned_lineage(ctx: AdvanceContext) -> bool:
    """Whether this lineage inherits merge review gating from an implementation root."""
    return (ctx.review_root_task or ctx.task).task_type == "implement"


def execution_status_allows_merge(ctx: AdvanceContext) -> bool:
    """Return whether the current planning task has merge-eligible execution status."""
    return ctx.task.status in MERGEABLE_EXECUTION_STATUSES


def has_valid_review_for_merge(ctx: AdvanceContext) -> bool:
    """Return whether current review evidence is fresh enough to allow auto-merge."""
    if not _is_implementation_owned_lineage(ctx):
        return True
    if not ctx.requires_review:
        return True
    if ctx.rebase_invalidates_review:
        return False
    if ctx.closing_review_action is not None:
        return False
    if ctx.latest_completed_review is None:
        return False
    if ctx.review_cleared:
        return True
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


def _stale_rebase_review_refresh_required(ctx: AdvanceContext) -> bool:
    """Whether a stale post-rebase review must still be refreshed before merge."""
    if not ctx.rebase_invalidates_review:
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
    if ctx.rebase_invalidates_review and _is_implementation_owned_lineage(ctx) and not ctx.requires_review:
        return False
    return True


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
    task: DbTask,
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
        task=task,
        task_type=task.task_type,
        has_branch=has_branch,
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
    review_preserved_by_rebase: DbTask | None = None
    review_invalidated_by_rebase: DbTask | None = None
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
            review_invalidated_by_rebase = latest_completed_rebase

    rebase_failure_streak = _count_rebase_failure_streak(
        task=task,
        rebase_children=rebase_children,
        latest_completed_rebase=latest_completed_rebase,
        latest_completed_review=ctx.latest_completed_review,
        latest_completed_code_change=ctx.latest_completed_code_change,
        review_cleared_at=task.review_cleared_at,
    )

    return replace(
        ctx,
        merge_source_ref=merge_source.ref,
        merge_source_warning=merge_source.warning,
        post_merge_rebase_state=post_merge_rebase_state,
        merge_state=merge_state,
        can_merge=can_merge,
        strict_scope_violation_paths=strict_scope_inspection.violation_paths,
        strict_scope_inspection_error=strict_scope_inspection.inspection_error,
        rebase_pending_or_running=rebase_pending_or_running,
        rebase_failed=rebase_failed,
        latest_completed_rebase=latest_completed_rebase,
        rebase_failure_streak=rebase_failure_streak,
        rebase_invalidates_review=rebase_invalidates_review,
        review_preserved_by_rebase=review_preserved_by_rebase,
        review_invalidated_by_rebase=review_invalidated_by_rebase,
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

    return replace(
        ctx,
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
    read_context: RecoveryReadContext | None = None,
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
            task=task,
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
        return replace(base_ctx, **_resolve_plan_review_state(config=config, store=store, task=task))

    if not task.branch:
        return _build_base_advance_context(
            config=config,
            store=store,
            task=task,
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
        )

    review_root_task = _resolve_review_root_task(store, task)
    (
        reviews,
        active_review,
        latest_completed_review,
        review_cleared,
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
        has_improve_after_review,
        has_fresh_unresolved_comments_since_latest_review,
        latest_completed_code_change,
        closing_review_action,
    ) = _resolve_review_state(config, store, review_root_task, git)

    ctx = _build_base_advance_context(
        config=config,
        store=store,
        task=task,
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
        reviews=reviews,
        review_root_task=review_root_task,
        active_review=active_review,
        latest_completed_review=latest_completed_review,
        latest_completed_code_change=latest_completed_code_change,
        review_cleared=review_cleared,
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
        has_improve_after_review=has_improve_after_review,
        has_fresh_unresolved_comments_since_latest_review=has_fresh_unresolved_comments_since_latest_review,
        closing_review_action=closing_review_action,
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
            )
        ),
        action=lambda ctx: {
            "type": "skip",
            "description": (
                "SKIP: approved plan-review slices are already materialized"
                if ctx.plan_materialization_state is not None and ctx.plan_materialization_state.materialized
                else "SKIP: implement task already exists for this plan"
            ),
        },
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
        action=lambda ctx: {"type": "skip", "description": _target_already_merged_description(ctx)},
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
        matches=lambda ctx: not ctx.can_merge and not _branch_contains_target_tip(ctx),
        action=lambda ctx: {"type": "needs_rebase", "description": "rebase --resolve (conflicts detected)"},
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
        name="post_rebase_run_pending_review",
        matches=lambda ctx: (
            _stale_rebase_review_refresh_required(ctx)
            and ctx.create_reviews
            and ctx.active_review is not None
            and ctx.active_review.status == "pending"
        ),
        action=lambda ctx: {
            "type": "run_review",
            "description": _rebase_pending_review_description(ctx.active_review, ctx.review_invalidated_by_rebase),
            "review_task": ctx.active_review,
        },
    ),
    AdvanceRule(
        name="post_rebase_wait_review",
        matches=lambda ctx: (
            _stale_rebase_review_refresh_required(ctx)
            and ctx.create_reviews
            and ctx.active_review is not None
            and ctx.active_review.status == "in_progress"
        ),
        action=lambda ctx: {
            "type": "wait_review",
            "description": _rebase_wait_review_description(ctx.active_review, ctx.review_invalidated_by_rebase),
            "review_task": ctx.active_review,
        },
    ),
    AdvanceRule(
        name="post_rebase_create_review",
        matches=lambda ctx: _stale_rebase_review_refresh_required(ctx) and ctx.create_reviews,
        action=lambda ctx: {
            "type": "create_review",
            "description": _rebase_create_review_description(ctx.review_invalidated_by_rebase),
        },
    ),
    AdvanceRule(
        name="stale_review_needs_manual_refresh",
        matches=lambda ctx: _stale_rebase_review_refresh_required(ctx) and not ctx.create_reviews,
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
        name="review_noop_improve_limit",
        matches=lambda ctx: (not ctx.review_cleared)
        and ctx.review_verdict == "CHANGES_REQUESTED"
        and ctx.consecutive_noop_improves >= ctx.max_noop_improve_cycles
        and ctx.active_improve_running is None
        and ctx.active_improve_pending is None,
        action=_noop_improve_limit_action,
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
        and ctx.duplicate_blocker_streak is not None,
        action=_duplicate_blocker_needs_attention_action,
    ),
    AdvanceRule(
        name="review_max_cycles",
        matches=lambda ctx: (not ctx.review_cleared)
        and ctx.review_verdict == "CHANGES_REQUESTED"
        and ctx.completed_review_cycles >= ctx.max_review_cycles,
        action=lambda ctx: with_needs_attention(
            {
                "type": "max_cycles_reached",
                "description": (
                    f"SKIP: max review cycles ({ctx.max_review_cycles}) reached, needs manual intervention"
                ),
            },
            reason="review-max-cycles-reached",
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
        name="reviews_all_cleared",
        matches=lambda ctx: execution_status_allows_merge(ctx)
        and has_valid_review_for_merge(ctx)
        and ctx.review_cleared
        and ctx.latest_completed_review is not None,
        action=lambda ctx: {"type": "merge", "description": "Merge (previous review addressed)"},
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
    read_context: RecoveryReadContext | None = None,
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
        read_context=read_context,
    )

    for rule in ADVANCE_RULES:
        if rule.matches(context):
            action = rule.action(context)
            require_needs_attention_subject(action)
            return action

    return {"type": "skip", "description": "SKIP: no matching rule (unexpected)"}
