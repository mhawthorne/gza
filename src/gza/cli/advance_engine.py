"""Shared advance/iterate state machine for deciding next task actions."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from gza.query import (
    get_improves_for_root as _get_improves_for_root_task,
    get_reviews_for_root as _get_reviews_for_root_task,
)

from ..db import SqliteTaskStore, Task as DbTask
from ._common import get_review_verdict


def _count_completed_review_cycles(store: SqliteTaskStore, impl_task_id: str) -> int:
    """Count completed review/improve cycles for an implementation task."""
    improve_tasks = store.get_improve_tasks_by_root(impl_task_id)
    return sum(1 for t in improve_tasks if t.status == "completed")


def _determine_resume_action(
    store: SqliteTaskStore,
    task: DbTask,
    *,
    max_resume_attempts: int,
) -> dict[str, Any] | None:
    """Return resume-oriented action for a failed task, or None if not resumable."""
    assert task.id is not None

    if task.status != "failed":
        return None
    if task.failure_reason not in ("MAX_STEPS", "MAX_TURNS"):
        return None
    if not task.session_id:
        return None

    children = store.get_based_on_children(task.id)
    if children:
        return {
            "type": "skip",
            "description": "SKIP: resume child already exists",
        }

    depth = store.count_resume_chain_depth(task.id)
    if depth >= max_resume_attempts:
        return {
            "type": "max_resume_attempts",
            "description": f"SKIP: max resume attempts ({max_resume_attempts}) reached",
        }

    failure_reason = task.failure_reason or "UNKNOWN"
    attempt_num = depth + 1
    return {
        "type": "resume",
        "description": f"Resume (failed: {failure_reason}, attempt {attempt_num}/{max_resume_attempts})",
    }


def determine_next_action(
    config: Any,
    store: SqliteTaskStore,
    git: Any,
    task: DbTask,
    target_branch: str,
    *,
    impl_based_on_ids: set[str] | None = None,
    max_resume_attempts: int | None = None,
) -> dict[str, Any]:
    """Determine the next action needed to advance a task."""
    assert task.id is not None

    if max_resume_attempts is None:
        max_resume_attempts = config.max_resume_attempts

    resume_action = _determine_resume_action(
        store,
        task,
        max_resume_attempts=max_resume_attempts,
    )
    if resume_action is not None:
        return resume_action

    if task.task_type == "plan":
        if impl_based_on_ids is None:
            impl_based_on_ids = store.get_impl_based_on_ids()
        if task.id not in impl_based_on_ids:
            return {
                "type": "create_implement",
                "description": "Create and start implement task",
            }
        return {
            "type": "skip",
            "description": "SKIP: implement task already exists for this plan",
        }

    if not task.branch:
        return {
            "type": "skip",
            "description": "SKIP: task has no branch (no commits)",
        }

    rebase_children = store.get_lineage_children(task.id)

    if not git.can_merge(task.branch, target_branch):
        for child in rebase_children:
            if child.task_type != "rebase":
                continue
            if child.status in {"in_progress", "pending"}:
                return {
                    "type": "skip",
                    "description": f"SKIP: rebase {child.id} already in progress",
                }
            if child.status == "failed":
                return {
                    "type": "needs_discussion",
                    "description": f"SKIP: rebase {child.id} failed, needs manual resolution",
                }
        return {
            "type": "needs_rebase",
            "description": "rebase --resolve (conflicts detected)",
        }

    completed_rebases = [
        c
        for c in rebase_children
        if c.task_type == "rebase" and c.status == "completed" and c.completed_at is not None
    ]

    reviews = _get_reviews_for_root_task(store, task)

    if reviews:
        active_review = next((r for r in reviews if r.status in ("pending", "in_progress")), None)
        completed_reviews = [r for r in reviews if r.status == "completed"]
        latest_completed_review = completed_reviews[0] if completed_reviews else None

        if completed_rebases and latest_completed_review and latest_completed_review.completed_at is not None:
            latest_rebase = max(completed_rebases, key=lambda t: t.completed_at or datetime.min)
            if latest_rebase.completed_at is not None and latest_rebase.completed_at > latest_completed_review.completed_at:
                if active_review:
                    if active_review.status == "pending":
                        return {
                            "type": "run_review",
                            "description": f"Run pending review {active_review.id} (post-rebase)",
                            "review_task": active_review,
                        }
                    return {
                        "type": "wait_review",
                        "description": f"SKIP: review {active_review.id} in progress (post-rebase)",
                        "review_task": active_review,
                    }
                return {
                    "type": "create_review",
                    "description": "Create review (code changed by rebase since last review)",
                }

        review_cleared = (
            latest_completed_review is not None
            and task.review_cleared_at is not None
            and latest_completed_review.completed_at is not None
            and task.review_cleared_at >= latest_completed_review.completed_at
        )

        if review_cleared and latest_completed_review and latest_completed_review.completed_at is not None:
            if active_review:
                if active_review.status == "pending":
                    return {
                        "type": "run_review",
                        "description": f"Spawn worker for pending review {active_review.id}",
                        "review_task": active_review,
                    }
                return {
                    "type": "wait_review",
                    "description": f"SKIP: review {active_review.id} is in_progress",
                    "review_task": active_review,
                }

            improves = _get_improves_for_root_task(store, task)
            completed_improves = [t for t in improves if t.status == "completed" and t.completed_at is not None]
            if completed_improves:
                latest_improve = max(completed_improves, key=lambda t: t.completed_at or datetime.min)
                if latest_improve.completed_at is not None and latest_improve.completed_at > latest_completed_review.completed_at:
                    return {
                        "type": "create_review",
                        "description": "Create review (code changed since last review)",
                    }

        if not review_cleared:
            if active_review and active_review.status == "pending":
                return {
                    "type": "run_review",
                    "description": f"Spawn worker for pending review {active_review.id}",
                    "review_task": active_review,
                }
            if active_review and active_review.status == "in_progress":
                return {
                    "type": "wait_review",
                    "description": f"SKIP: review {active_review.id} is in_progress",
                    "review_task": active_review,
                }

            if latest_completed_review is not None:
                latest_review = latest_completed_review
                verdict = get_review_verdict(config, latest_review)
                if verdict == "APPROVED":
                    return {
                        "type": "merge",
                        "description": "Merge (review APPROVED)",
                        "review_task": latest_review,
                    }
                if verdict == "CHANGES_REQUESTED":
                    completed_cycles = _count_completed_review_cycles(store, task.id)
                    if completed_cycles >= config.max_review_cycles:
                        return {
                            "type": "max_cycles_reached",
                            "description": (
                                "SKIP: max review cycles "
                                f"({config.max_review_cycles}) reached, needs manual intervention"
                            ),
                        }

                    assert latest_review.id is not None
                    existing_improve = store.get_improve_tasks_for(task.id, latest_review.id)
                    active_improve_running = [t for t in existing_improve if t.status == "in_progress"]
                    if active_improve_running:
                        return {
                            "type": "wait_improve",
                            "description": f"SKIP: improve task {active_improve_running[0].id} is in_progress",
                        }
                    active_improve_pending = [t for t in existing_improve if t.status == "pending"]
                    if active_improve_pending:
                        return {
                            "type": "run_improve",
                            "description": f"Spawn worker for pending improve {active_improve_pending[0].id}",
                            "improve_task": active_improve_pending[0],
                        }
                    return {
                        "type": "improve",
                        "description": "Create improve task (review CHANGES_REQUESTED)",
                        "review_task": latest_review,
                    }

                return {
                    "type": "needs_discussion",
                    "description": f"SKIP: review verdict is {verdict or 'unknown'}, needs manual attention",
                    "review_task": latest_review,
                }

        if latest_completed_review is not None:
            return {
                "type": "merge",
                "description": "Merge (previous review addressed)",
            }

    if task.task_type != "implement":
        return {
            "type": "merge",
            "description": "Merge task (no review yet)",
        }

    if config.advance_requires_review:
        if config.advance_create_reviews:
            return {
                "type": "create_review",
                "description": "Create review (required before merge)",
            }
        return {
            "type": "skip",
            "description": "SKIP: no review exists and advance_create_reviews=false (run gza review manually)",
        }

    return {
        "type": "merge",
        "description": "Merge task (no review yet)",
    }
