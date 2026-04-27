"""Shared helpers for creating review and follow-up tasks."""

import re
from collections.abc import Iterable
from typing import Literal

from .db import SqliteTaskStore, Task
from .prompts import PromptBuilder
from .review_verdict import ReviewFinding
from .task_slug import (
    extract_task_id_suffix,
    get_base_task_slug,
    strip_derived_implement_prefixes,
)

_FOLLOWUP_PROMPT_PREFIX_RE = re.compile(
    r"^Follow-up\s+(\S+)\s+from review\s+(\S+)\s+for task\s+(\S+):"
)


class DuplicateReviewError(ValueError):
    """Raised when attempting to create a duplicate active review task."""

    def __init__(self, active_review: Task) -> None:
        self.active_review = active_review
        super().__init__(
            f"An active review task already exists: {active_review.id} ({active_review.status})"
        )


def _known_derived_suffixes_for_review(store: SqliteTaskStore, impl_task: Task) -> set[str]:
    """Collect task-id suffixes from an implementation task lineage.

    Includes the implementation task itself and ancestors reachable via
    ``based_on`` / ``depends_on``. This allows exact derived-prefix stripping
    without over-matching semantic ``*-impl-*`` slug segments.
    """
    known: set[str] = set()
    current = impl_task
    seen: set[str] = set()
    while current:
        suffix = extract_task_id_suffix(current.id)
        if suffix:
            known.add(suffix)
        if current.id is not None:
            current_id = str(current.id)
            if current_id in seen:
                break
            seen.add(current_id)
        parent_id = current.based_on or current.depends_on
        if parent_id is None:
            break
        parent = store.get(parent_id)
        if parent is None:
            break
        current = parent
    return known


def build_auto_review_prompt(
    impl_task: Task,
    project_prefix: str | None = None,
    known_task_id_suffixes: Iterable[str] | None = None,
) -> str:
    """Build prompt text for runner auto-created reviews.

    Preserves the historical slug-first prompt semantics used by runner auto-review.
    When project_prefix is provided, it is stripped from the slug-derived description
    so the prompt contains only the semantic portion (e.g. "review add-feature" rather
    than "review myproj-add-feature").
    """
    if impl_task.slug:
        slug = get_base_task_slug(impl_task.slug) if "-" in impl_task.slug else None
        if slug:
            # Derived implement slugs are "<task_id_suffix>-impl-<semantic-slug>".
            # Normalize first, then optionally strip project_prefix from semantic tail.
            normalized = strip_derived_implement_prefixes(slug, set(known_task_id_suffixes or ()))
            if normalized is None:
                slug = None
            else:
                slug = normalized
        if slug:
            if project_prefix and slug.startswith(f"{project_prefix}-"):
                slug = slug[len(project_prefix) + 1:]
            return f"review {slug}"

    return f"Review task {impl_task.id}"


def create_review_task(
    store: SqliteTaskStore,
    impl_task: Task,
    *,
    model: str | None = None,
    provider: str | None = None,
    prompt_mode: Literal["cli", "auto"] = "cli",
    project_prefix: str | None = None,
) -> Task:
    """Create a review task for a completed implementation task.

    Validates implementation type/state and prevents duplicate active reviews.
    """
    if impl_task.task_type != "implement":
        raise ValueError(
            f"Task {impl_task.id} is a {impl_task.task_type} task. "
            "Expected an implementation task."
        )
    if impl_task.status != "completed":
        raise ValueError(
            f"Task {impl_task.id} is {impl_task.status}. Can only review completed tasks."
        )
    if impl_task.id is None:
        raise ValueError("Cannot create review for task without an ID.")

    existing_reviews = store.get_reviews_for_task(impl_task.id)
    active_reviews = [r for r in existing_reviews if r.status in ("pending", "in_progress")]
    if active_reviews:
        raise DuplicateReviewError(active_reviews[0])

    if prompt_mode == "auto":
        known_suffixes = _known_derived_suffixes_for_review(store, impl_task)
        review_prompt = build_auto_review_prompt(
            impl_task,
            project_prefix=project_prefix,
            known_task_id_suffixes=known_suffixes,
        )
    else:
        review_prompt = PromptBuilder().review_task_prompt(impl_task.id, impl_task.prompt)
    inherited_tags = impl_task.tags or (() if impl_task.group is None else (impl_task.group,))

    return store.add(
        prompt=review_prompt,
        task_type="review",
        depends_on=impl_task.id,
        tags=inherited_tags,
        based_on=impl_task.id,
        model=model,
        provider=provider,
    )


def build_followup_prompt_prefix(review_task_id: str, impl_task_id: str, finding_id: str) -> str:
    """Build deterministic prompt prefix for auto-created follow-up tasks."""
    return f"Follow-up {finding_id} from review {review_task_id} for task {impl_task_id}:"


def build_followup_prompt(
    review_task_id: str,
    impl_task_id: str,
    finding: ReviewFinding,
) -> str:
    """Build full prompt for an auto-created follow-up implementation task."""
    prefix = build_followup_prompt_prefix(review_task_id, impl_task_id, finding.id)
    tail = (finding.fix_or_followup or "").strip()
    heading = f"{prefix} {tail}" if tail else prefix
    return f"{heading}\n\n## Follow-up finding to implement:\n\n{format_followup_finding_context(finding)}"


def format_followup_finding_context(finding: ReviewFinding) -> str:
    """Format canonical finding context for follow-up implementation tasks."""
    title = f" {finding.title}" if finding.title and finding.title != finding.id else ""
    if finding.body.strip():
        body = finding.body.strip()
    else:
        lines: list[str] = []
        if finding.evidence:
            lines.append(f"Evidence: {finding.evidence}")
        if finding.impact:
            lines.append(f"Impact: {finding.impact}")
        if finding.fix_or_followup:
            lines.append(f"Recommended follow-up: {finding.fix_or_followup}")
        if finding.tests:
            lines.append(f"Recommended tests: {finding.tests}")
        body = "\n".join(lines)
    return f"### {finding.id}{title}\n{body}".strip()


def extract_followup_prompt_parts(prompt: str) -> tuple[str, str, str] | None:
    """Return (finding_id, review_task_id, impl_task_id) for follow-up prompts."""
    match = _FOLLOWUP_PROMPT_PREFIX_RE.match(prompt.strip())
    if match is None:
        return None
    return match.group(1), match.group(2), match.group(3)


def find_existing_followup_task(
    store: SqliteTaskStore,
    *,
    review_task_id: str,
    impl_task_id: str,
    finding_id: str,
) -> Task | None:
    """Return an existing auto-created follow-up task for (review, finding), if any."""
    prefix = build_followup_prompt_prefix(review_task_id, impl_task_id, finding_id)
    for child in store.get_based_on_children(review_task_id):
        if child.task_type != "implement":
            continue
        if child.prompt.strip().startswith(prefix):
            return child
    return None


def create_or_reuse_followup_task(
    store: SqliteTaskStore,
    *,
    review_task: Task,
    impl_task: Task,
    finding: ReviewFinding,
) -> tuple[Task, bool]:
    """Create or reuse an idempotent follow-up task for a parsed FOLLOWUP finding.

    Returns:
        (task, created_now) where created_now is True only when a new row was created.
    """
    if review_task.id is None:
        raise ValueError("Cannot create follow-up for review without an ID.")
    if impl_task.id is None:
        raise ValueError("Cannot create follow-up for implementation without an ID.")

    existing = find_existing_followup_task(
        store,
        review_task_id=review_task.id,
        impl_task_id=impl_task.id,
        finding_id=finding.id,
    )
    if existing is not None:
        return existing, False

    prompt = build_followup_prompt(
        review_task.id,
        impl_task.id,
        finding,
    )
    inherited_tags = impl_task.tags or (() if impl_task.group is None else (impl_task.group,))
    created = store.add(
        prompt=prompt,
        task_type="implement",
        based_on=review_task.id,
        depends_on=impl_task.id,
        tags=inherited_tags,
    )
    return created, True
