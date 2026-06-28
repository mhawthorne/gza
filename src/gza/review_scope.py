"""Helpers for resolving gradeable review scope for implementation tasks."""

from __future__ import annotations

import re
from dataclasses import dataclass

from .db import TASK_COMMENT_KIND_REVIEW_SCOPE, SqliteTaskStore, Task, TaskComment
from .lineage import get_plan_for_task

_RESOLUTION_REVIEW_MODE_RE = re.compile(r"^Review mode:\s*resolution\s*$", re.MULTILINE)
_RESOLUTION_REVIEW_FIELD_RE = re.compile(
    r"^(Implementation task|Rebase task|Pre-rebase head SHA|Pre-rebase target SHA|Pre-rebase merge-base SHA|Resolved head SHA|Resolved target SHA):\s*(.*?)\s*$",
    re.MULTILINE,
)
_RESOLUTION_REVIEW_REQUIRED_FIELDS = (
    "Implementation task",
    "Rebase task",
    "Resolved head SHA",
    "Resolved target SHA",
)
_RESOLUTION_REVIEW_OPTIONAL_FIELDS = (
    "Pre-rebase head SHA",
    "Pre-rebase target SHA",
    "Pre-rebase merge-base SHA",
)
_RESOLUTION_REVIEW_ALLOWED_FIELDS = frozenset(
    _RESOLUTION_REVIEW_REQUIRED_FIELDS + _RESOLUTION_REVIEW_OPTIONAL_FIELDS
)

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


@dataclass(frozen=True)
class ResolutionReviewScope:
    """Parsed structured metadata for a resolution-scoped review."""

    implementation_task_id: str
    rebase_task_id: str
    resolved_head_sha: str
    resolved_target_sha: str
    pre_rebase_head_sha: str | None = None
    pre_rebase_target_sha: str | None = None
    pre_rebase_merge_base_sha: str | None = None


def declares_resolution_review_mode(scope: str | None) -> bool:
    """Return whether scope text claims to be resolution-review metadata."""
    return bool(scope and _RESOLUTION_REVIEW_MODE_RE.search(scope))


def build_resolution_review_scope(
    *,
    implementation_task_id: str,
    rebase_task_id: str,
    resolved_head_sha: str,
    resolved_target_sha: str,
    pre_rebase_head_sha: str | None = None,
    pre_rebase_target_sha: str | None = None,
    pre_rebase_merge_base_sha: str | None = None,
) -> str:
    """Build persisted structured scope text for a resolution-scoped review."""
    return "\n".join(
        (
            "Review mode: resolution",
            f"Implementation task: {implementation_task_id}",
            f"Rebase task: {rebase_task_id}",
            f"Pre-rebase head SHA: {pre_rebase_head_sha or ''}",
            f"Pre-rebase target SHA: {pre_rebase_target_sha or ''}",
            f"Pre-rebase merge-base SHA: {pre_rebase_merge_base_sha or ''}",
            f"Resolved head SHA: {resolved_head_sha}",
            f"Resolved target SHA: {resolved_target_sha}",
            "",
            "Review only the conflict-resolution delta introduced by this rebase.",
            "Do not re-review the whole implementation except where context is required.",
        )
    )


def parse_resolution_review_scope(scope: str | None) -> ResolutionReviewScope | None:
    """Parse persisted resolution-review metadata from review_scope text.

    Returns ``None`` when the scope is not a resolution-review scope at all.
    Raises ``ValueError`` when the scope claims to be a resolution review but the
    structured metadata block is malformed.
    """
    if not declares_resolution_review_mode(scope):
        return None
    assert scope is not None
    lines = scope.splitlines()
    mode_index = next(
        (
            index
            for index, raw_line in enumerate(lines)
            if raw_line.strip() == "Review mode: resolution"
        ),
        None,
    )
    if mode_index is None:
        raise ValueError("resolution review metadata header is malformed")

    raw_fields: dict[str, str] = {}
    for raw_line in lines[mode_index + 1 :]:
        line = raw_line.strip()
        if not line:
            break
        match = _RESOLUTION_REVIEW_FIELD_RE.match(line)
        if match is None:
            raise ValueError(f"resolution review metadata line is malformed: {line}")
        field_name, field_value = match.groups()
        if field_name not in _RESOLUTION_REVIEW_ALLOWED_FIELDS:
            raise ValueError(f"unexpected resolution review metadata field: {field_name}")
        if field_name in raw_fields:
            raise ValueError(f"duplicate resolution review metadata field: {field_name}")
        raw_fields[field_name] = field_value.strip()

    missing_fields = [field for field in _RESOLUTION_REVIEW_REQUIRED_FIELDS if not raw_fields.get(field)]
    if missing_fields:
        raise ValueError(
            "resolution review metadata is missing required fields: "
            + ", ".join(missing_fields)
        )
    fields = {
        field.lower().replace(" ", "_").replace("-", "_"): value.strip()
        for field, value in raw_fields.items()
    }
    implementation_task_id = fields.get("implementation_task")
    rebase_task_id = fields.get("rebase_task")
    resolved_head_sha = fields.get("resolved_head_sha")
    resolved_target_sha = fields.get("resolved_target_sha")
    assert implementation_task_id is not None
    assert rebase_task_id is not None
    assert resolved_head_sha is not None
    assert resolved_target_sha is not None
    return ResolutionReviewScope(
        implementation_task_id=implementation_task_id,
        rebase_task_id=rebase_task_id,
        resolved_head_sha=resolved_head_sha,
        resolved_target_sha=resolved_target_sha,
        pre_rebase_head_sha=fields.get("pre_rebase_head_sha") or None,
        pre_rebase_target_sha=fields.get("pre_rebase_target_sha") or None,
        pre_rebase_merge_base_sha=fields.get("pre_rebase_merge_base_sha") or None,
    )


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


def _summarize_implementation_prompt(prompt: str) -> str | None:
    for raw_line in prompt.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("## "):
            continue
        line = re.sub(r"^[*\-+]\s+", "", line)
        line = re.sub(r"^\d+\.\s+", "", line)
        normalized = line.strip()
        if normalized:
            return normalized
    return None


def _derive_plan_backed_review_scope(
    store: SqliteTaskStore,
    impl_task: Task,
) -> ReviewScope | None:
    plan_task = get_plan_for_task(store, impl_task)
    if plan_task is None or plan_task.id is None:
        return None

    prompt_summary = _summarize_implementation_prompt(impl_task.prompt)
    summary_lines = [f"Plan-backed implementation scope from {plan_task.id}."]
    if prompt_summary is not None:
        summary_lines.extend(("", f"Implementation request: {prompt_summary}"))
    summary_lines.extend(
        (
            "",
            "Treat the linked plan as background context, not as implementation instructions.",
        )
    )
    return ReviewScope(
        summary="\n".join(summary_lines),
        source=f"plan_fallback:{plan_task.id}",
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
    prompt_scope = _extract_review_scope_details_from_prompt(impl_task.prompt)
    if prompt_scope is not None:
        return prompt_scope
    return _derive_plan_backed_review_scope(store, impl_task)
