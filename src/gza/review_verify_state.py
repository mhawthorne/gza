"""Shared helpers for persisted review verify provenance."""

from __future__ import annotations

from gza.db import SqliteTaskStore, Task


def refresh_preserved_rebase_review_verify_heads(
    store: SqliteTaskStore,
    impl_task: Task | None,
    *,
    branch: str | None,
    old_head_sha: str | None,
    new_head_sha: str | None,
) -> int:
    """Retarget preserved verify evidence to a rewritten same-diff branch head.

    When a completed same-branch rebase proves the tracked diff is unchanged, the
    latest completed review remains valid. For verify-only review blockers, the
    latest review's runner-owned verify failure provenance and any no-op improve
    verify provenance for that review must follow the rewritten branch head so the
    persisted mergeable recognition continues to match the current tip.
    """
    if impl_task is None or impl_task.id is None or impl_task.task_type != "implement":
        return 0
    if not branch or not old_head_sha or not new_head_sha or old_head_sha == new_head_sha:
        return 0

    reviews = [
        review
        for review in store.get_reviews_for_task(impl_task.id)
        if review.status == "completed"
    ]
    if not reviews:
        return 0
    latest_review = max(
        reviews,
        key=lambda review: (review.completed_at or review.created_at, review.created_at),
    )
    if latest_review.id is None:
        return 0

    refreshed = 0
    if (
        latest_review.review_verify_branch == branch
        and latest_review.review_verify_head_sha == old_head_sha
    ):
        latest_review.review_verify_head_sha = new_head_sha
        store.update(latest_review)
        refreshed += 1

    for improve in store.get_improve_tasks_for(impl_task.id, latest_review.id):
        if improve.status != "completed" or improve.changed_diff is not False:
            continue
        if improve.review_verify_branch != branch or improve.review_verify_head_sha != old_head_sha:
            continue
        improve.review_verify_head_sha = new_head_sha
        store.update(improve)
        refreshed += 1

    return refreshed
