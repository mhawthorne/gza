"""Declarative advance/iterate rule engine."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from gza.branch_resolution import resolve_rebase_target_task
from gza.console import prompt_available_width, shorten_prompt
from gza.db import SqliteTaskStore, Task as DbTask, task_id_numeric_key, task_owns_merge_status
from gza.git import ResolvedMergeSourceRef
from gza.merge_state import resolve_task_merge_state_for_target
from gza.query import get_code_changing_descendants_for_root, get_reviews_for_root
from gza.recovery_engine import (
    FailedRecoveryDecision,
    classify_failure_reason,
    decide_failed_task_recovery,
    get_failed_recovery_needs_attention_reason,
)
from gza.resume_policy import is_resumable_failed_task as _is_resumable_failed_task
from gza.review_verdict import ParsedReviewReport, ReviewFinding, get_review_report
from gza.source_followup import (
    collect_non_dropped_implement_source_ids,
    resolve_source_followup_state,
    source_task_has_implementation_followup,
)

NEEDS_ATTENTION_LABEL = "Needs attention"
ALLOW_NOOP_IMPROVE_TAG = "allow-noop-improve"

WORKER_CONSUMING_ACTIONS = frozenset(
    {
        "needs_rebase",
        "create_implement",
        "create_review",
        "run_review",
        "improve",
        "run_improve",
        "resume",
        "retry",
    }
)


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


@dataclass(frozen=True)
class AdvanceContext:
    """Resolved task state used by advance rules."""

    task: DbTask
    task_type: str
    has_branch: bool

    requires_review: bool
    create_reviews: bool
    max_review_cycles: int
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
    rebase_pending_or_running: DbTask | None = None
    rebase_failed: DbTask | None = None
    latest_completed_rebase: DbTask | None = None
    rebase_invalidates_review: bool = False
    review_preserved_by_rebase: DbTask | None = None
    review_invalidated_by_rebase: DbTask | None = None

    reviews: list[DbTask] | None = None
    active_review: DbTask | None = None
    latest_completed_review: DbTask | None = None
    review_cleared: bool = False
    review_verdict: str | None = None
    review_report: ParsedReviewReport | None = None
    followup_findings: tuple[ReviewFinding, ...] = ()

    completed_review_cycles: int = 0
    active_improve_running: DbTask | None = None
    active_improve_pending: DbTask | None = None
    latest_noop_improve: DbTask | None = None
    consecutive_noop_improves: int = 0
    noop_improve_allowed: bool = False
    noop_improve_trigger: str | None = None
    has_improve_after_review: bool = False
    has_fresh_unresolved_comments_since_latest_review: bool = False
    closing_review_action: dict[str, Any] | None = None

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
    if merge_unit_state == "merged":
        return PostMergeRebaseState(
            merge_unit_state=merge_unit_state,
            branch_tip_sha=None,
            target_tip_sha=None,
            target_is_ancestor_of_branch=None,
            branch_equals_target=False,
            already_merged=True,
            rebase_resolution_proved=True,
            reason="merge-unit-merged",
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
    if (
        state.reason == "branch-tip-equals-target-tip"
        and state.already_merged
        and task.id is not None
        and task.status == "completed"
        and task.has_commits
        and task_owns_merge_status(task)
    ):
        merge_unit = store.resolve_merge_unit_for_task(task.id)
        if merge_unit is not None:
            store.set_merge_unit_state(
                merge_unit.id,
                "merged",
                merged_by_task_id=task.id,
            )
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


def _task_id(task: DbTask | None) -> str:
    """Render a task id for user-facing action descriptions."""
    if task is None or task.id is None:
        return "unknown"
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
    return f"SKIP: target implementation already merged ({reason or 'post-merge proof'})"


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
    suffix = f"; previous no-op improve {_task_id(ctx.latest_noop_improve)} made no tracked diff change"
    if ctx.noop_improve_allowed:
        suffix += f" and `{ALLOW_NOOP_IMPROVE_TAG}` allows continuing"
    return suffix


def _noop_improve_needs_discussion_action(ctx: AdvanceContext) -> dict[str, Any]:
    latest_noop_id = _task_id(ctx.latest_noop_improve)
    source = "unresolved comments remain open after" if ctx.noop_improve_trigger == "comments" else "review feedback remains unresolved after"
    opt_out_note = f" Tag `{ALLOW_NOOP_IMPROVE_TAG}` to continue automation anyway." if not ctx.noop_improve_allowed else ""
    return with_needs_attention(
        {
            "type": "needs_discussion",
            "description": (
                f"SKIP: {ctx.consecutive_noop_improves} consecutive no-op improves reached "
                f"(latest {latest_noop_id}); {source} no tracked diff change.{opt_out_note}"
            ),
        },
        reason="improve-no-op",
    )


def _failed_task_skip_action(ctx: AdvanceContext) -> dict[str, Any]:
    assert ctx.failed_recovery_decision is not None
    return failed_recovery_decision_to_action(
        ctx.task,
        ctx.failed_recovery_decision,
        needs_attention_reason=ctx.failed_recovery_attention_reason,
    )


def _failed_task_resume_or_retry_action(ctx: AdvanceContext) -> dict[str, Any]:
    assert ctx.failed_recovery_decision is not None
    return failed_recovery_decision_to_action(ctx.task, ctx.failed_recovery_decision)


def with_needs_attention(
    action: Mapping[str, Any],
    *,
    reason: str,
) -> dict[str, Any]:
    """Attach shared needs-attention metadata to an advance action."""
    annotated = dict(action)
    annotated["needs_attention_reason"] = reason
    return annotated


def failed_recovery_decision_to_action(
    task: DbTask,
    decision: FailedRecoveryDecision,
    *,
    needs_attention_reason: str | None = None,
) -> dict[str, Any]:
    """Convert a shared failed-task recovery decision into an advance action dict."""
    description = f"SKIP: {decision.reason_text}"
    failure_reason = task.failure_reason or "UNKNOWN"
    if decision.action == "resume":
        description = f"Resume failed task ({failure_reason})"
    elif decision.action == "retry":
        description = f"Retry failed task ({failure_reason})"
    action: dict[str, Any] = {
        "type": decision.action,
        "description": description,
        "recovery_task_id": decision.recovery_task_id,
        "reuse_existing": decision.reuse_existing,
        "launch_mode": decision.launch_mode,
        "decision": decision,
    }
    if needs_attention_reason is not None:
        action["needs_attention_reason"] = needs_attention_reason
    return action


def failed_recovery_decision_to_attention_action(
    store: SqliteTaskStore,
    task: DbTask,
    decision: FailedRecoveryDecision,
    *,
    max_recovery_attempts: int,
) -> dict[str, Any] | None:
    """Convert a terminal failed-task recovery stop into the shared attention action."""
    attention_reason = get_failed_recovery_needs_attention_reason(
        store,
        task,
        decision=decision,
        max_recovery_attempts=max_recovery_attempts,
    )
    action = failed_recovery_decision_to_action(
        task,
        decision,
        needs_attention_reason=attention_reason,
    )
    if classify_advance_action(action) != "needs_attention":
        return None
    return action


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


def classify_advance_action(action: Mapping[str, Any]) -> str:
    """Bucket advance outcomes into actionable, needs_attention, or skip."""
    if is_needs_attention_action(action):
        return "needs_attention"
    action_type = str(action.get("type", "skip"))
    if action_type in {"skip", "wait_review", "wait_improve"}:
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
) -> dict[str, Any] | None:
    """Return the invariant-enforcing closing review action for a lineage, if any.

    The lifecycle invariant is satisfied once the newest completed code-change task
    is followed by at least one review task in any state. Pending reviews should be
    run, in-progress reviews should be waited on, and completed/failed reviews
    already satisfy the invariant for automatic lifecycle loops.
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


def _resolve_review_state(
    config: Any,
    store: SqliteTaskStore,
    task: DbTask,
) -> tuple[
    list[DbTask],
    DbTask | None,
    DbTask | None,
    bool,
    str | None,
    ParsedReviewReport | None,
    tuple[ReviewFinding, ...],
    int,
    DbTask | None,
    DbTask | None,
    DbTask | None,
    int,
    bool,
    str | None,
    bool,
    bool,
    dict[str, Any] | None,
]:
    """Resolve review/improve lineage state for the implementation root task."""
    reviews = get_reviews_for_root(store, task)
    active_review = _select_active_review(reviews)
    completed_reviews = [r for r in reviews if r.status == "completed"]
    latest_completed_review = completed_reviews[0] if completed_reviews else None

    review_cleared = (
        latest_completed_review is not None
        and task.review_cleared_at is not None
        and latest_completed_review.completed_at is not None
        and task.review_cleared_at >= latest_completed_review.completed_at
    )

    review_verdict: str | None = None
    review_report: ParsedReviewReport | None = None
    followup_findings: tuple[ReviewFinding, ...] = ()
    completed_review_cycles = 0
    active_improve_running: DbTask | None = None
    active_improve_pending: DbTask | None = None
    latest_noop_improve: DbTask | None = None
    consecutive_noop_improves = 0
    noop_improve_allowed = False
    noop_improve_trigger: str | None = None
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
        latest_completed_improve = next((t for t in improve_tasks if t.status == "completed"), None)
        noop_improve_allowed = (
            _has_tag(task, ALLOW_NOOP_IMPROVE_TAG)
            or _has_tag(latest_completed_review, ALLOW_NOOP_IMPROVE_TAG)
            or _has_tag(latest_completed_improve, ALLOW_NOOP_IMPROVE_TAG)
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

    closing_review_action = resolve_closing_review_action(
        task=task,
        reviews=reviews,
        latest_completed_review=latest_completed_review,
        latest_completed_code_change=latest_completed_code_change,
    )

    return (
        reviews,
        active_review,
        latest_completed_review,
        review_cleared,
        review_verdict,
        review_report,
        followup_findings,
        completed_review_cycles,
        active_improve_running,
        active_improve_pending,
        latest_noop_improve,
        consecutive_noop_improves,
        noop_improve_allowed,
        noop_improve_trigger,
        has_improve_after_review,
        has_fresh_unresolved_comments_since_latest_review,
        closing_review_action,
    )


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
) -> AdvanceContext:
    """Resolve state once, then let rules evaluate pure context."""
    assert task.id is not None

    effective_max_resume = max_resume_attempts if max_resume_attempts is not None else config.max_resume_attempts
    effective_max_noop_improves = int(getattr(config, "max_noop_improve_cycles", 2))

    failed_recovery_decision: FailedRecoveryDecision | None = None
    failed_recovery_attention_reason: str | None = None
    if task.status == "failed":
        failed_recovery_decision = decide_failed_task_recovery(
            store,
            task,
            max_recovery_attempts=effective_max_resume,
        )
        failed_recovery_attention_reason = get_failed_recovery_needs_attention_reason(
            store,
            task,
            decision=failed_recovery_decision,
            max_recovery_attempts=effective_max_resume,
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
    auto_implement_enabled = task.auto_implement is not False

    if task.task_type == "plan":
        return AdvanceContext(
            task=task,
            task_type=task.task_type,
            has_branch=bool(task.branch),
            requires_review=config.advance_requires_review,
            create_reviews=config.advance_create_reviews,
            max_review_cycles=config.max_review_cycles,
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

    if not task.branch:
        return AdvanceContext(
            task=task,
            task_type=task.task_type,
            has_branch=False,
            requires_review=config.advance_requires_review,
            create_reviews=config.advance_create_reviews,
            max_review_cycles=config.max_review_cycles,
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

    merge_source = _resolve_current_merge_source(git, task.branch)
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
    merge_state = resolve_task_merge_state_for_target(
        store=store,
        task=task,
        git=git,
        target_branch=target_branch,
    )
    can_merge = (
        post_merge_rebase_state.already_merged
        or merge_state == "merged"
        or (bool(merge_source.ref) and git.can_merge(merge_source.ref, target_branch))
    )
    rebase_children = [
        child
        for child in store.get_lineage_children(task.id)
        if child.task_type == "rebase" and (task.branch is None or child.branch is None or child.branch == task.branch)
    ]
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

    (
        reviews,
        active_review,
        latest_completed_review,
        review_cleared,
        review_verdict,
        review_report,
        followup_findings,
        completed_review_cycles,
        active_improve_running,
        active_improve_pending,
        latest_noop_improve,
        consecutive_noop_improves,
        noop_improve_allowed,
        noop_improve_trigger,
        has_improve_after_review,
        has_fresh_unresolved_comments_since_latest_review,
        closing_review_action,
) = _resolve_review_state(config, store, task)

    rebase_invalidates_review = False
    review_preserved_by_rebase: DbTask | None = None
    review_invalidated_by_rebase: DbTask | None = None
    if (
        latest_completed_rebase is not None
        and latest_completed_review is not None
        and latest_completed_rebase.completed_at is not None
        and latest_completed_review.completed_at is not None
        and latest_completed_rebase.completed_at > latest_completed_review.completed_at
    ):
        if latest_completed_rebase.changed_diff is False:
            review_preserved_by_rebase = latest_completed_rebase
        else:
            rebase_invalidates_review = True
            review_invalidated_by_rebase = latest_completed_rebase

    return AdvanceContext(
        task=task,
        task_type=task.task_type,
        has_branch=True,
        requires_review=config.advance_requires_review,
        create_reviews=config.advance_create_reviews,
        max_review_cycles=config.max_review_cycles,
        max_noop_improve_cycles=effective_max_noop_improves,
        max_resume_attempts=effective_max_resume,
        auto_implement_enabled=auto_implement_enabled,
        has_non_dropped_implement_descendant=has_implementation_followup,
        active_plan_child=active_plan_child,
        active_implement_child=active_implement_child,
        has_non_dropped_plan_or_implement_descendant=has_non_dropped_plan_or_implement_descendant,
        failed_recovery_decision=failed_recovery_decision,
        failed_recovery_attention_reason=failed_recovery_attention_reason,
        merge_source_ref=merge_source.ref,
        merge_source_warning=merge_source.warning,
        post_merge_rebase_state=post_merge_rebase_state,
        merge_state=merge_state,
        can_merge=can_merge,
        rebase_pending_or_running=rebase_pending_or_running,
        rebase_failed=rebase_failed,
        latest_completed_rebase=latest_completed_rebase,
        rebase_invalidates_review=rebase_invalidates_review,
        review_preserved_by_rebase=review_preserved_by_rebase,
        review_invalidated_by_rebase=review_invalidated_by_rebase,
        reviews=reviews,
        active_review=active_review,
        latest_completed_review=latest_completed_review,
        review_cleared=review_cleared,
        review_verdict=review_verdict,
        review_report=review_report,
        followup_findings=followup_findings,
        completed_review_cycles=completed_review_cycles,
        active_improve_running=active_improve_running,
        active_improve_pending=active_improve_pending,
        latest_noop_improve=latest_noop_improve,
        consecutive_noop_improves=consecutive_noop_improves,
        noop_improve_allowed=noop_improve_allowed,
        noop_improve_trigger=noop_improve_trigger,
        has_improve_after_review=has_improve_after_review,
        has_fresh_unresolved_comments_since_latest_review=has_fresh_unresolved_comments_since_latest_review,
        closing_review_action=closing_review_action,
        is_resumable_failed_task=is_resumable_failed,
        has_resume_children=has_resume_children,
        resume_chain_depth=resume_chain_depth,
        failure_reason=task.failure_reason,
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
        name="failed_task_skip",
        matches=lambda ctx: ctx.failed_recovery_decision is not None,
        action=_failed_task_skip_action,
    ),
    AdvanceRule(
        name="awaiting_human_plan_review",
        matches=lambda ctx: (
            ctx.task_type == "plan"
            and ctx.task.status == "completed"
            and not ctx.has_non_dropped_implement_descendant
            and not ctx.auto_implement_enabled
        ),
        action=lambda ctx: {
            "type": "awaiting_human",
            "description": (
                f"Awaiting human review: review the plan, then run 'uv run gza implement {ctx.task.id}' "
                "to create implementation, or drop it if you decided not to implement."
            ),
        },
    ),
    AdvanceRule(
        name="plan_needs_implement",
        matches=lambda ctx: (
            ctx.task_type == "plan"
            and ctx.task.status == "completed"
            and not ctx.has_non_dropped_implement_descendant
            and ctx.auto_implement_enabled
        ),
        action=lambda ctx: {"type": "create_implement", "description": "Create and start implement task"},
    ),
    AdvanceRule(
        name="plan_has_implement",
        matches=lambda ctx: (
            ctx.task_type == "plan"
            and ctx.task.status == "completed"
            and ctx.has_non_dropped_implement_descendant
        ),
        action=lambda ctx: {"type": "skip", "description": "SKIP: implement task already exists for this plan"},
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
        ),
    ),
    AdvanceRule(
        name="no_branch",
        matches=lambda ctx: not ctx.has_branch,
        action=lambda ctx: {"type": "skip", "description": _no_branch_description(ctx)},
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
        ),
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
        matches=lambda ctx: ctx.merge_state == "merged",
        action=lambda ctx: {"type": "skip", "description": "SKIP: already merged into target branch"},
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
        name="conflict_rebase_failed",
        matches=lambda ctx: not ctx.can_merge and _failed_rebase_still_blocks_advance(ctx),
        action=lambda ctx: with_needs_attention(
            {
                "type": "needs_discussion",
                "description": f"SKIP: rebase {_task_id(ctx.rebase_failed)} failed, needs manual resolution",
            },
            reason="rebase-failed-needs-manual-resolution",
        ),
    ),
    AdvanceRule(
        name="conflict_needs_rebase",
        matches=lambda ctx: not ctx.can_merge,
        action=lambda ctx: {"type": "needs_rebase", "description": "rebase --resolve (conflicts detected)"},
    ),
    AdvanceRule(
        name="post_rebase_run_pending_review",
        matches=lambda ctx: ctx.rebase_invalidates_review and ctx.active_review is not None and ctx.active_review.status == "pending",
        action=lambda ctx: {
            "type": "run_review",
            "description": _rebase_pending_review_description(ctx.active_review, ctx.review_invalidated_by_rebase),
            "review_task": ctx.active_review,
        },
    ),
    AdvanceRule(
        name="post_rebase_wait_review",
        matches=lambda ctx: ctx.rebase_invalidates_review and ctx.active_review is not None and ctx.active_review.status == "in_progress",
        action=lambda ctx: {
            "type": "wait_review",
            "description": _rebase_wait_review_description(ctx.active_review, ctx.review_invalidated_by_rebase),
            "review_task": ctx.active_review,
        },
    ),
    AdvanceRule(
        name="post_rebase_create_review",
        matches=lambda ctx: ctx.rebase_invalidates_review,
        action=lambda ctx: {"type": "create_review", "description": _rebase_create_review_description(ctx.review_invalidated_by_rebase)},
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
        ),
    ),
    AdvanceRule(
        name="closing_review_invariant",
        matches=lambda ctx: ctx.closing_review_action is not None,
        action=lambda ctx: dict(ctx.closing_review_action or {}),
    ),
    AdvanceRule(
        name="review_pending",
        matches=lambda ctx: (not ctx.review_cleared)
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
        matches=lambda ctx: (not ctx.review_cleared)
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
        and ctx.consecutive_noop_improves >= ctx.max_noop_improve_cycles
        and not ctx.noop_improve_allowed,
        action=_noop_improve_needs_discussion_action,
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
        matches=lambda ctx: (not ctx.review_cleared)
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
        matches=lambda ctx: (not ctx.review_cleared) and ctx.latest_completed_review is not None and ctx.review_verdict == "APPROVED",
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
        name="review_noop_improve_limit",
        matches=lambda ctx: (not ctx.review_cleared)
        and ctx.review_verdict == "CHANGES_REQUESTED"
        and ctx.consecutive_noop_improves >= ctx.max_noop_improve_cycles
        and not ctx.noop_improve_allowed,
        action=_noop_improve_needs_discussion_action,
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
        ),
    ),
    AdvanceRule(
        name="reviews_all_cleared",
        matches=lambda ctx: ctx.review_cleared and ctx.latest_completed_review is not None,
        action=lambda ctx: {"type": "merge", "description": "Merge (previous review addressed)"},
    ),
    AdvanceRule(
        name="non_implement_no_review",
        matches=lambda ctx: ctx.task_type != "implement",
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
        action=lambda ctx: {
            "type": "skip",
            "description": "SKIP: no review exists and advance_create_reviews=false (run gza review manually)",
        },
    ),
    AdvanceRule(
        name="implement_no_review_required",
        matches=lambda ctx: True,
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
    )

    for rule in ADVANCE_RULES:
        if rule.matches(context):
            return rule.action(context)

    return {"type": "skip", "description": "SKIP: no matching rule (unexpected)"}
