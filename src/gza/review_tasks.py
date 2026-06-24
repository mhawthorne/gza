"""Shared helpers for creating review, follow-up, and adjudication tasks."""

import re
from collections.abc import Iterable, Mapping
from typing import Any, Literal

from .db import SqliteTaskStore, Task
from .derived_tags import resolve_derived_task_tags
from .prompts import PromptBuilder
from .review_scope import resolve_review_scope_for_impl
from .review_verdict import ReviewFinding
from .task_slug import (
    extract_task_id_suffix,
    get_base_task_slug,
    strip_derived_implement_prefixes,
)

_FOLLOWUP_PROMPT_PREFIX_RE = re.compile(
    r"^Follow-up\s+(\S+)\s+from review\s+(\S+)\s+for task\s+(\S+):"
)
_DEFERRED_BLOCKER_PROMPT_PREFIX_RE = re.compile(
    r"^Deferred blocker\s+(\S+)\s+from review\s+(\S+)\s+for task\s+(\S+):"
)
_REVIEW_BLOCKER_ADJUDICATION_PROMPT_PREFIX_RE = re.compile(
    r"^Adjudicate blocker\s+(\S+)\s+from review\s+(\S+)\s+for task\s+(\S+):"
)
_REVIEW_BLOCKER_ADJUDICATION_SOURCE_TASK_RE = re.compile(
    r"^Dispute source task:\s*(\S+)\s*$",
    re.MULTILINE,
)
_REVIEW_BLOCKER_ADJUDICATION_HEAD_SHA_RE = re.compile(
    r"^Dispute source head SHA:\s*(\S+)\s*$",
    re.MULTILINE,
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
    trigger_source: str,
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
    resolved_scope = resolve_review_scope_for_impl(store, impl_task)
    review_task = store.add(
        prompt=review_prompt,
        task_type="review",
        depends_on=impl_task.id,
        tags=resolve_derived_task_tags(impl_task),
        based_on=impl_task.id,
        review_scope=resolved_scope.summary if resolved_scope is not None else None,
        model=model,
        provider=provider,
        trigger_source=trigger_source,
    )
    impl_unit = store.resolve_merge_unit_for_task(impl_task.id)
    if impl_unit is not None:
        store.get_or_create_merge_unit_for_task(review_task)
    return review_task


def build_followup_prompt_prefix(review_task_id: str, impl_task_id: str, finding_id: str) -> str:
    """Build deterministic prompt prefix for auto-created follow-up tasks."""
    return f"Follow-up {finding_id} from review {review_task_id} for task {impl_task_id}:"


def build_deferred_blocker_prompt_prefix(review_task_id: str, impl_task_id: str, finding_id: str) -> str:
    """Build deterministic prompt prefix for auto-created deferred blocker tasks."""
    return f"Deferred blocker {finding_id} from review {review_task_id} for task {impl_task_id}:"


def build_review_blocker_adjudication_prompt_prefix(
    review_task_id: str,
    impl_task_id: str,
    finding_id: str,
) -> str:
    """Build deterministic prompt prefix for blocker adjudication tasks."""
    return f"Adjudicate blocker {finding_id} from review {review_task_id} for task {impl_task_id}:"


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


def build_deferred_blocker_prompt(
    review_task_id: str,
    impl_task_id: str,
    finding: ReviewFinding,
) -> str:
    """Build full prompt for an auto-created deferred blocker implementation task."""
    prefix = build_deferred_blocker_prompt_prefix(review_task_id, impl_task_id, finding.id)
    tail = (finding.fix_or_followup or finding.title or "").strip()
    heading = f"{prefix} {tail}" if tail else prefix
    open_state_citation = finding.open_state_citation or "not provided by review"
    canonical_context = format_blocker_finding_context(finding)
    return (
        f"{heading}\n\n"
        "## Deferred blocker to resolve\n\n"
        "This task was created because `gza merge` bypassed a BLOCKER-severity review finding "
        "during a manual merge override attempt.\n\n"
        f"Original implementation: {impl_task_id}\n"
        f"Review: {review_task_id}\n"
        f"Open-state citation: {open_state_citation}\n\n"
        f"{canonical_context}"
    )


def build_review_blocker_adjudication_prompt(
    review_task_id: str,
    impl_task_id: str,
    finding: ReviewFinding,
    dispute_metadata: Mapping[str, Any],
) -> str:
    """Build full prompt for a strict review-blocker adjudication task."""
    prefix = build_review_blocker_adjudication_prompt_prefix(review_task_id, impl_task_id, finding.id)
    tail = (finding.title or finding.id).strip()
    heading = f"{prefix} {tail}" if tail else prefix
    dispute_reason = str(dispute_metadata.get("reason", "")).strip() or "unknown"
    dispute_evidence = str(dispute_metadata.get("evidence", "")).strip() or "not provided"
    current_state_citation = str(dispute_metadata.get("current_state_citation", "")).strip() or "not provided"
    source_task_id = str(dispute_metadata.get("source_task_id", "")).strip() or "unknown"
    source_head_sha = str(dispute_metadata.get("head_sha", "")).strip()
    scope_citation = str(dispute_metadata.get("scope_citation", "")).strip()
    downstream_task_id = str(dispute_metadata.get("downstream_task_id", "")).strip()
    source_branch = str(dispute_metadata.get("source_branch", "")).strip()

    lines = [
        heading,
        "",
        "Return exactly one non-empty line: VALID, INVALID, or NEEDS_HUMAN.",
        "Do not add explanation, markdown, or code fences.",
        "Do not run tests or propose fixes. Judge only whether the disputed blocker remains a valid current blocker.",
        "",
        "Adjudication question:",
        "- VALID: the review blocker is still current, in scope, and actionable.",
        "- INVALID: the dispute evidence shows the blocker is stale, already satisfied, out of scope, or otherwise not valid.",
        "- NEEDS_HUMAN: the evidence is ambiguous, unsafe, or insufficient.",
        "",
        f"Implementation task: {impl_task_id}",
        f"Review task: {review_task_id}",
        f"Dispute source task: {source_task_id}",
    ]
    if source_branch:
        lines.append(f"Dispute source branch: {source_branch}")
    if source_head_sha:
        lines.append(f"Dispute source head SHA: {source_head_sha}")
    lines.extend(
        [
            "",
            "## Review blocker under dispute",
            "",
            format_blocker_finding_context(finding),
            "",
            "## Dispute evidence",
            "",
            f"Reason: {dispute_reason}",
            f"Evidence: {dispute_evidence}",
            f"Current-state citation: {current_state_citation}",
        ]
    )
    if scope_citation:
        lines.append(f"Scope citation: {scope_citation}")
    if downstream_task_id:
        lines.append(f"Downstream task: {downstream_task_id}")
    return "\n".join(lines).strip()


def _finding_heading(finding: ReviewFinding) -> str:
    title = f" {finding.title}" if finding.title and finding.title != finding.id else ""
    return f"### {finding.id}{title}"


def _finding_structured_body(
    finding: ReviewFinding,
    *,
    fix_label: str,
    tests_label: str,
) -> str:
    lines: list[str] = []
    if finding.evidence:
        lines.append(f"Evidence: {finding.evidence}")
    if finding.impact:
        lines.append(f"Impact: {finding.impact}")
    if finding.fix_or_followup:
        lines.append(f"{fix_label}: {finding.fix_or_followup}")
    if finding.tests:
        lines.append(f"{tests_label}: {finding.tests}")
    if finding.open_state_citation:
        lines.append(f"Open-state citation: {finding.open_state_citation}")
    return "\n".join(lines)


def format_followup_finding_context(finding: ReviewFinding) -> str:
    """Format canonical finding context for follow-up implementation tasks."""
    if finding.body.strip():
        body = finding.body.strip()
    else:
        body = _finding_structured_body(
            finding,
            fix_label="Recommended follow-up",
            tests_label="Recommended tests",
        )
    return f"{_finding_heading(finding)}\n{body}".strip()


def format_blocker_finding_context(finding: ReviewFinding) -> str:
    """Format canonical finding context for deferred blocker implementation tasks."""
    parts: list[str] = [_finding_heading(finding)]
    if finding.body.strip():
        parts.append(finding.body.strip())
    structured = _finding_structured_body(
        finding,
        fix_label="Required fix",
        tests_label="Required tests",
    )
    if structured:
        parts.append(structured)
    return "\n".join(part for part in parts if part).strip()


def extract_followup_prompt_parts(prompt: str) -> tuple[str, str, str] | None:
    """Return (finding_id, review_task_id, impl_task_id) for follow-up prompts."""
    match = _FOLLOWUP_PROMPT_PREFIX_RE.match(prompt.strip())
    if match is None:
        return None
    return match.group(1), match.group(2), match.group(3)


def extract_deferred_blocker_prompt_parts(prompt: str) -> tuple[str, str, str] | None:
    """Return (finding_id, review_task_id, impl_task_id) for deferred blocker prompts."""
    match = _DEFERRED_BLOCKER_PROMPT_PREFIX_RE.match(prompt.strip())
    if match is None:
        return None
    return match.group(1), match.group(2), match.group(3)


def extract_review_blocker_adjudication_prompt_parts(prompt: str) -> tuple[str, str, str] | None:
    """Return (finding_id, review_task_id, impl_task_id) for adjudication prompts."""
    match = _REVIEW_BLOCKER_ADJUDICATION_PROMPT_PREFIX_RE.match(prompt.strip())
    if match is None:
        return None
    return match.group(1), match.group(2), match.group(3)


def extract_review_blocker_adjudication_dispute_identity(
    prompt: str,
) -> tuple[str | None, str | None]:
    """Return (source_task_id, head_sha) embedded in an adjudication prompt."""
    source_task_match = _REVIEW_BLOCKER_ADJUDICATION_SOURCE_TASK_RE.search(prompt)
    head_sha_match = _REVIEW_BLOCKER_ADJUDICATION_HEAD_SHA_RE.search(prompt)
    source_task_id = source_task_match.group(1) if source_task_match is not None else None
    head_sha = head_sha_match.group(1) if head_sha_match is not None else None
    return source_task_id, head_sha


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


def find_existing_deferred_blocker_task(
    store: SqliteTaskStore,
    *,
    review_task_id: str,
    impl_task_id: str,
    finding_id: str,
) -> Task | None:
    """Return an existing auto-created deferred blocker task for (review, finding), if any."""
    prefix = build_deferred_blocker_prompt_prefix(review_task_id, impl_task_id, finding_id)
    for child in store.get_based_on_children(review_task_id):
        if child.task_type != "implement":
            continue
        if child.prompt.strip().startswith(prefix):
            return child
    return None


def find_existing_review_blocker_adjudication_task(
    store: SqliteTaskStore,
    *,
    review_task_id: str,
    impl_task_id: str,
    finding_id: str,
    dispute_source_task_id: str | None = None,
    dispute_head_sha: str | None = None,
) -> Task | None:
    """Return an existing adjudication task for (review, finding), if any."""
    prefix = build_review_blocker_adjudication_prompt_prefix(review_task_id, impl_task_id, finding_id)
    for child in store.get_based_on_children(review_task_id):
        if child.task_type != "internal":
            continue
        if child.prompt.strip().startswith(prefix):
            prompt_source_task_id, prompt_head_sha = (
                extract_review_blocker_adjudication_dispute_identity(child.prompt)
            )
            if dispute_source_task_id is not None and prompt_source_task_id != dispute_source_task_id:
                continue
            if dispute_head_sha is not None and prompt_head_sha != dispute_head_sha:
                continue
            return child
    return None


def create_or_reuse_followup_task(
    store: SqliteTaskStore,
    *,
    review_task: Task,
    impl_task: Task,
    finding: ReviewFinding,
    trigger_source: str,
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
    created = store.add(
        prompt=prompt,
        task_type="implement",
        based_on=review_task.id,
        depends_on=impl_task.id,
        review_scope=format_followup_finding_context(finding),
        tags=resolve_derived_task_tags(impl_task),
        trigger_source=trigger_source,
    )
    return created, True


def create_or_reuse_deferred_blocker_task(
    store: SqliteTaskStore,
    *,
    review_task: Task,
    impl_task: Task,
    finding: ReviewFinding,
    trigger_source: str,
) -> tuple[Task, bool]:
    """Create or reuse an idempotent deferred blocker task for a parsed BLOCKER finding."""
    if review_task.id is None:
        raise ValueError("Cannot create deferred blocker for review without an ID.")
    if impl_task.id is None:
        raise ValueError("Cannot create deferred blocker for implementation without an ID.")

    existing = find_existing_deferred_blocker_task(
        store,
        review_task_id=review_task.id,
        impl_task_id=impl_task.id,
        finding_id=finding.id,
    )
    if existing is not None:
        return existing, False

    prompt = build_deferred_blocker_prompt(
        review_task.id,
        impl_task.id,
        finding,
    )
    created = store.add(
        prompt=prompt,
        task_type="implement",
        based_on=review_task.id,
        depends_on=impl_task.id,
        review_scope=format_blocker_finding_context(finding),
        tags=resolve_derived_task_tags(impl_task),
        trigger_source=trigger_source,
        create_pr=True,
        urgent=True,
    )
    return created, True


def create_or_reuse_review_blocker_adjudication_task(
    store: SqliteTaskStore,
    *,
    review_task: Task,
    impl_task: Task,
    finding: ReviewFinding,
    dispute_metadata: Mapping[str, Any],
    trigger_source: str,
) -> tuple[Task, bool]:
    """Create or reuse an idempotent adjudication task for one disputed blocker."""
    if review_task.id is None:
        raise ValueError("Cannot create adjudication for review without an ID.")
    if impl_task.id is None:
        raise ValueError("Cannot create adjudication for implementation without an ID.")

    dispute_source_task_id = str(dispute_metadata.get("source_task_id", "")).strip() or None
    dispute_head_sha = str(dispute_metadata.get("head_sha", "")).strip() or None
    existing = find_existing_review_blocker_adjudication_task(
        store,
        review_task_id=review_task.id,
        impl_task_id=impl_task.id,
        finding_id=finding.id,
        dispute_source_task_id=dispute_source_task_id,
        dispute_head_sha=dispute_head_sha,
    )
    if existing is not None:
        return existing, False

    prompt = build_review_blocker_adjudication_prompt(
        review_task.id,
        impl_task.id,
        finding,
        dispute_metadata,
    )
    created = store.add(
        prompt=prompt,
        task_type="internal",
        based_on=review_task.id,
        depends_on=impl_task.id,
        same_branch=True,
        tags=resolve_derived_task_tags(impl_task),
        review_scope=format_blocker_finding_context(finding),
        trigger_source=trigger_source,
        urgent=True,
    )
    return created, True
