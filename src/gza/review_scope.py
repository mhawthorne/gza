"""Helpers for resolving gradeable review scope for implementation tasks."""

from __future__ import annotations

import re
from dataclasses import dataclass

from .db import TASK_COMMENT_KIND_REVIEW_SCOPE, SqliteTaskStore, Task, TaskComment

_SLICE_HEADER_RE = re.compile(
    r"^Implement\s+plan\s+(?P<plan_id>\S+),\s*slice\s+(?P<slice_label>.+?)(?::\s*(?P<summary>.+))?$",
    re.IGNORECASE,
)
_SECTION_RE = re.compile(r"^##\s+(?P<title>[^\n]+)\n", re.MULTILINE)


@dataclass(frozen=True)
class ReviewScope:
    """Resolved gradeable scope for an implementation review."""

    summary: str
    out_of_scope_context: str | None = None
    source: str = "task_field"


def _normalize_scope_text(text: str | None) -> str | None:
    if text is None:
        return None
    normalized = text.strip()
    return normalized or None


def _extract_markdown_section(prompt: str, title: str) -> str | None:
    pattern = re.compile(rf"^##\s+{re.escape(title)}\s*$", re.MULTILINE | re.IGNORECASE)
    match = pattern.search(prompt)
    if match is None:
        return None
    start = match.end()
    next_match = _SECTION_RE.search(prompt, start)
    end = next_match.start() if next_match is not None else len(prompt)
    content = prompt[start:end].strip()
    return content or None


def extract_review_scope_from_prompt(prompt: str) -> str | None:
    """Return a conservative gradeable scope summary from a sliced implement prompt."""
    resolved = _extract_review_scope_details_from_prompt(prompt)
    if resolved is None:
        return None
    return resolved.summary


def _extract_review_scope_details_from_prompt(prompt: str) -> ReviewScope | None:
    if not prompt.strip():
        return None

    first_line = prompt.strip().splitlines()[0].strip()
    match = _SLICE_HEADER_RE.match(first_line)
    if match is None:
        return None

    slice_label = match.group("slice_label").strip()
    summary = f"Slice {slice_label}"
    trailing_summary = _normalize_scope_text(match.group("summary"))
    if trailing_summary is not None:
        summary += f": {trailing_summary}"

    scope_block = _extract_markdown_section(prompt, "Scope")
    if scope_block is not None:
        summary = f"{summary}\n\n{scope_block}"

    out_of_scope_block = _extract_markdown_section(prompt, "Out of scope")
    out_of_scope_context = None
    if out_of_scope_block is not None:
        out_of_scope_context = out_of_scope_block

    return ReviewScope(
        summary=summary,
        out_of_scope_context=out_of_scope_context,
        source="prompt_parser",
    )


def get_latest_review_scope_comment_for_impl(
    store: SqliteTaskStore,
    impl_task: Task,
) -> TaskComment | None:
    """Return the newest review-scope comment that can supply review metadata.

    Only non-pending implementation tasks participate in post-hoc scope overrides.
    """
    if impl_task.id is None or impl_task.task_type != "implement":
        return None
    if impl_task.status == "pending":
        return None
    comment = store.get_latest_comment_by_kind(
        impl_task.id,
        kind=TASK_COMMENT_KIND_REVIEW_SCOPE,
    )
    if not isinstance(comment, TaskComment):
        return None
    return comment


def resolve_review_scope_for_impl(store: SqliteTaskStore, impl_task: Task) -> ReviewScope | None:
    """Resolve the authoritative review scope for an implementation task."""
    if impl_task.review_scope:
        return ReviewScope(summary=impl_task.review_scope.strip(), source="task_field")
    scope_comment = get_latest_review_scope_comment_for_impl(store, impl_task)
    if scope_comment is not None:
        return ReviewScope(
            summary=scope_comment.content,
            source=f"comment:{scope_comment.id}",
        )
    return _extract_review_scope_details_from_prompt(impl_task.prompt)
