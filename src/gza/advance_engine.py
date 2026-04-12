"""Declarative advance/iterate rule engine."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from gza.db import SqliteTaskStore, Task as DbTask
from gza.query import get_improves_for_root, get_reviews_for_root
from gza.review_verdict import get_review_verdict

RESUMABLE_FAILURE_REASONS = frozenset({"MAX_STEPS", "MAX_TURNS", "TEST_FAILURE"})

WORKER_CONSUMING_ACTIONS = frozenset(
    {
        "needs_rebase",
        "create_implement",
        "create_review",
        "run_review",
        "improve",
        "run_improve",
        "resume",
    }
)


@dataclass(frozen=True)
class AdvanceContext:
    """Resolved task state used by advance rules."""

    task: DbTask
    task_type: str
    has_branch: bool

    requires_review: bool
    create_reviews: bool
    max_review_cycles: int
    max_resume_attempts: int

    has_implement_child: bool = False

    can_merge: bool = True
    rebase_pending_or_running: DbTask | None = None
    rebase_failed: DbTask | None = None
    rebase_invalidates_review: bool = False

    reviews: list[DbTask] | None = None
    active_review: DbTask | None = None
    latest_completed_review: DbTask | None = None
    review_cleared: bool = False
    review_verdict: str | None = None

    completed_review_cycles: int = 0
    active_improve_running: DbTask | None = None
    active_improve_pending: DbTask | None = None
    has_improve_after_review: bool = False

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


def is_resumable_failure_reason(failure_reason: str | None) -> bool:
    """Return True when a failure reason is auto-resumable by advance."""
    return failure_reason in RESUMABLE_FAILURE_REASONS


def is_resumable_failed_task(task: DbTask) -> bool:
    """Return True when a task is failed and eligible for automatic resume."""
    return (
        task.status == "failed"
        and task.session_id is not None
        and is_resumable_failure_reason(task.failure_reason)
    )


def _count_completed_review_cycles(store: SqliteTaskStore, impl_task_id: str) -> int:
    improve_tasks = store.get_improve_tasks_by_root(impl_task_id)
    return sum(1 for t in improve_tasks if t.status == "completed")


def _task_id(task: DbTask | None) -> str:
    """Render a task id for user-facing action descriptions."""
    if task is None or task.id is None:
        return "unknown"
    return task.id


def resolve_advance_context(
    config: Any,
    store: SqliteTaskStore,
    git: Any,
    task: DbTask,
    target_branch: str,
    *,
    impl_based_on_ids: set[str] | None = None,
    max_resume_attempts: int | None = None,
) -> AdvanceContext:
    """Resolve state once, then let rules evaluate pure context."""
    assert task.id is not None

    effective_max_resume = max_resume_attempts if max_resume_attempts is not None else config.max_resume_attempts

    is_resumable_failed = is_resumable_failed_task(task)
    has_resume_children = False
    resume_chain_depth = 0
    if is_resumable_failed:
        children = store.get_based_on_children(task.id)
        has_resume_children = bool(children)
        resume_chain_depth = store.count_resume_chain_depth(task.id)

    if task.task_type == "plan":
        if impl_based_on_ids is None:
            impl_based_on_ids = store.get_impl_based_on_ids()
        return AdvanceContext(
            task=task,
            task_type=task.task_type,
            has_branch=bool(task.branch),
            requires_review=config.advance_requires_review,
            create_reviews=config.advance_create_reviews,
            max_review_cycles=config.max_review_cycles,
            max_resume_attempts=effective_max_resume,
            has_implement_child=task.id in impl_based_on_ids,
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
            max_resume_attempts=effective_max_resume,
            is_resumable_failed_task=is_resumable_failed,
            has_resume_children=has_resume_children,
            resume_chain_depth=resume_chain_depth,
            failure_reason=task.failure_reason,
        )

    can_merge = git.can_merge(task.branch, target_branch)
    rebase_children = [child for child in store.get_lineage_children(task.id) if child.task_type == "rebase"]
    rebase_pending_or_running = next((c for c in rebase_children if c.status in {"pending", "in_progress"}), None)
    rebase_failed = next((c for c in rebase_children if c.status == "failed"), None)

    latest_completed_rebase: DbTask | None = None
    completed_rebases = [
        c
        for c in rebase_children
        if c.status == "completed" and c.completed_at is not None
    ]
    if completed_rebases:
        latest_completed_rebase = max(completed_rebases, key=lambda t: t.completed_at or datetime.min)

    reviews = get_reviews_for_root(store, task)
    active_review = next((r for r in reviews if r.status in ("pending", "in_progress")), None)
    completed_reviews = [r for r in reviews if r.status == "completed"]
    latest_completed_review = completed_reviews[0] if completed_reviews else None

    review_cleared = (
        latest_completed_review is not None
        and task.review_cleared_at is not None
        and latest_completed_review.completed_at is not None
        and task.review_cleared_at >= latest_completed_review.completed_at
    )

    rebase_invalidates_review = False
    if (
        latest_completed_rebase is not None
        and latest_completed_review is not None
        and latest_completed_rebase.completed_at is not None
        and latest_completed_review.completed_at is not None
    ):
        rebase_invalidates_review = latest_completed_rebase.completed_at > latest_completed_review.completed_at

    review_verdict: str | None = None
    completed_review_cycles = 0
    active_improve_running: DbTask | None = None
    active_improve_pending: DbTask | None = None
    has_improve_after_review = False

    if latest_completed_review is not None:
        review_verdict = get_review_verdict(Path(config.project_dir), latest_completed_review)

        if review_cleared and latest_completed_review.completed_at is not None:
            improves = [
                t
                for t in get_improves_for_root(store, task)
                if t.status == "completed" and t.completed_at is not None
            ]
            if improves:
                latest_improve = max(improves, key=lambda t: t.completed_at or datetime.min)
                if latest_improve.completed_at is not None:
                    has_improve_after_review = latest_improve.completed_at > latest_completed_review.completed_at

        if review_verdict == "CHANGES_REQUESTED":
            completed_review_cycles = _count_completed_review_cycles(store, task.id)
            assert latest_completed_review.id is not None
            improve_tasks = store.get_improve_tasks_for(task.id, latest_completed_review.id)
            active_improve_running = next((t for t in improve_tasks if t.status == "in_progress"), None)
            active_improve_pending = next((t for t in improve_tasks if t.status == "pending"), None)

    return AdvanceContext(
        task=task,
        task_type=task.task_type,
        has_branch=True,
        requires_review=config.advance_requires_review,
        create_reviews=config.advance_create_reviews,
        max_review_cycles=config.max_review_cycles,
        max_resume_attempts=effective_max_resume,
        can_merge=can_merge,
        rebase_pending_or_running=rebase_pending_or_running,
        rebase_failed=rebase_failed,
        rebase_invalidates_review=rebase_invalidates_review,
        reviews=reviews,
        active_review=active_review,
        latest_completed_review=latest_completed_review,
        review_cleared=review_cleared,
        review_verdict=review_verdict,
        completed_review_cycles=completed_review_cycles,
        active_improve_running=active_improve_running,
        active_improve_pending=active_improve_pending,
        has_improve_after_review=has_improve_after_review,
        is_resumable_failed_task=is_resumable_failed,
        has_resume_children=has_resume_children,
        resume_chain_depth=resume_chain_depth,
        failure_reason=task.failure_reason,
    )


ADVANCE_RULES: list[AdvanceRule] = [
    AdvanceRule(
        name="resume_has_children",
        matches=lambda ctx: ctx.is_resumable_failed_task and ctx.has_resume_children,
        action=lambda ctx: {"type": "skip", "description": "SKIP: resume child already exists"},
    ),
    AdvanceRule(
        name="resume_max_attempts",
        matches=lambda ctx: ctx.is_resumable_failed_task and ctx.resume_chain_depth >= ctx.max_resume_attempts,
        action=lambda ctx: {
            "type": "max_resume_attempts",
            "description": f"SKIP: max resume attempts ({ctx.max_resume_attempts}) reached",
        },
    ),
    AdvanceRule(
        name="resume_task",
        matches=lambda ctx: ctx.is_resumable_failed_task,
        action=lambda ctx: {
            "type": "resume",
            "description": (
                f"Resume (failed: {ctx.failure_reason or 'UNKNOWN'}, "
                f"attempt {ctx.resume_chain_depth + 1}/{ctx.max_resume_attempts})"
            ),
        },
    ),
    AdvanceRule(
        name="plan_needs_implement",
        matches=lambda ctx: ctx.task_type == "plan" and not ctx.has_implement_child,
        action=lambda ctx: {"type": "create_implement", "description": "Create and start implement task"},
    ),
    AdvanceRule(
        name="plan_has_implement",
        matches=lambda ctx: ctx.task_type == "plan" and ctx.has_implement_child,
        action=lambda ctx: {"type": "skip", "description": "SKIP: implement task already exists for this plan"},
    ),
    AdvanceRule(
        name="no_branch",
        matches=lambda ctx: not ctx.has_branch,
        action=lambda ctx: {"type": "skip", "description": "SKIP: task has no branch (no commits)"},
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
        matches=lambda ctx: not ctx.can_merge and ctx.rebase_failed is not None,
        action=lambda ctx: {
            "type": "needs_discussion",
            "description": f"SKIP: rebase {_task_id(ctx.rebase_failed)} failed, needs manual resolution",
        },
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
            "description": f"Run pending review {_task_id(ctx.active_review)} (post-rebase)",
            "review_task": ctx.active_review,
        },
    ),
    AdvanceRule(
        name="post_rebase_wait_review",
        matches=lambda ctx: ctx.rebase_invalidates_review and ctx.active_review is not None and ctx.active_review.status == "in_progress",
        action=lambda ctx: {
            "type": "wait_review",
            "description": f"SKIP: review {_task_id(ctx.active_review)} in progress (post-rebase)",
            "review_task": ctx.active_review,
        },
    ),
    AdvanceRule(
        name="post_rebase_create_review",
        matches=lambda ctx: ctx.rebase_invalidates_review,
        action=lambda ctx: {"type": "create_review", "description": "Create review (code changed by rebase since last review)"},
    ),
    AdvanceRule(
        name="cleared_run_pending_review",
        matches=lambda ctx: ctx.review_cleared and ctx.active_review is not None and ctx.active_review.status == "pending",
        action=lambda ctx: {
            "type": "run_review",
            "description": f"Spawn worker for pending review {_task_id(ctx.active_review)}",
            "review_task": ctx.active_review,
        },
    ),
    AdvanceRule(
        name="cleared_wait_review",
        matches=lambda ctx: ctx.review_cleared and ctx.active_review is not None and ctx.active_review.status == "in_progress",
        action=lambda ctx: {
            "type": "wait_review",
            "description": f"SKIP: review {_task_id(ctx.active_review)} is in_progress",
            "review_task": ctx.active_review,
        },
    ),
    AdvanceRule(
        name="cleared_needs_rereview",
        matches=lambda ctx: ctx.review_cleared and ctx.latest_completed_review is not None and ctx.has_improve_after_review,
        action=lambda ctx: {"type": "create_review", "description": "Create review (code changed since last review)"},
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
        name="review_approved",
        matches=lambda ctx: (not ctx.review_cleared) and ctx.latest_completed_review is not None and ctx.review_verdict == "APPROVED",
        action=lambda ctx: {
            "type": "merge",
            "description": "Merge (review APPROVED)",
            "review_task": ctx.latest_completed_review,
        },
    ),
    AdvanceRule(
        name="review_max_cycles",
        matches=lambda ctx: (not ctx.review_cleared)
        and ctx.review_verdict == "CHANGES_REQUESTED"
        and ctx.completed_review_cycles >= ctx.max_review_cycles,
        action=lambda ctx: {
            "type": "max_cycles_reached",
            "description": (
                f"SKIP: max review cycles ({ctx.max_review_cycles}) reached, needs manual intervention"
            ),
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
        name="review_create_improve",
        matches=lambda ctx: (not ctx.review_cleared) and ctx.review_verdict == "CHANGES_REQUESTED",
        action=lambda ctx: {
            "type": "improve",
            "description": "Create improve task (review CHANGES_REQUESTED)",
            "review_task": ctx.latest_completed_review,
        },
    ),
    AdvanceRule(
        name="review_unknown_verdict",
        matches=lambda ctx: (not ctx.review_cleared) and ctx.latest_completed_review is not None,
        action=lambda ctx: {
            "type": "needs_discussion",
            "description": f"SKIP: review verdict is {ctx.review_verdict or 'unknown'}, needs manual attention",
            "review_task": ctx.latest_completed_review,
        },
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
    )

    for rule in ADVANCE_RULES:
        if rule.matches(context):
            return rule.action(context)

    return {"type": "skip", "description": "SKIP: no matching rule (unexpected)"}
