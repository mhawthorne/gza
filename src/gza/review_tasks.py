"""Shared helpers for creating review tasks."""

import re
from typing import Literal

from .db import SqliteTaskStore, Task
from .prompts import PromptBuilder


class DuplicateReviewError(ValueError):
    """Raised when attempting to create a duplicate active review task."""

    def __init__(self, active_review: Task) -> None:
        self.active_review = active_review
        super().__init__(
            f"An active review task already exists: #{active_review.id} ({active_review.status})"
        )


def build_auto_review_prompt(impl_task: Task) -> str:
    """Build prompt text for runner auto-created reviews.

    Preserves the historical slug-first prompt semantics used by runner auto-review.
    """
    if impl_task.slug:
        parts = impl_task.slug.split("-", 1)
        if len(parts) == 2:
            slug = re.sub(r"-\d+$", "", parts[1])
            return f"review {slug}"

    review_prompt = f"Review task #{impl_task.id}"
    if impl_task.prompt:
        review_prompt += f": {impl_task.prompt[:100]}"
    return review_prompt


def create_review_task(
    store: SqliteTaskStore,
    impl_task: Task,
    *,
    model: str | None = None,
    provider: str | None = None,
    prompt_mode: Literal["cli", "auto"] = "cli",
) -> Task:
    """Create a review task for a completed implementation task.

    Validates implementation type/state and prevents duplicate active reviews.
    """
    if impl_task.task_type != "implement":
        raise ValueError(
            f"Task #{impl_task.id} is a {impl_task.task_type} task. "
            "Expected an implementation task."
        )
    if impl_task.status != "completed":
        raise ValueError(
            f"Task #{impl_task.id} is {impl_task.status}. Can only review completed tasks."
        )
    if impl_task.id is None:
        raise ValueError("Cannot create review for task without an ID.")

    existing_reviews = store.get_reviews_for_task(impl_task.id)
    active_reviews = [r for r in existing_reviews if r.status in ("pending", "in_progress")]
    if active_reviews:
        raise DuplicateReviewError(active_reviews[0])

    if prompt_mode == "auto":
        review_prompt = build_auto_review_prompt(impl_task)
    else:
        review_prompt = PromptBuilder().review_task_prompt(impl_task.id, impl_task.prompt)

    return store.add(
        prompt=review_prompt,
        task_type="review",
        depends_on=impl_task.id,
        group=impl_task.group,
        based_on=impl_task.based_on,
        model=model,
        provider=provider,
    )
