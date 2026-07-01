"""Helpers for resolving gradeable review scope for implementation tasks."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Literal

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
_SPEC_COHERENCE_REVIEW_MODE_RE = re.compile(r"^Review mode:\s*spec-coherence\s*$", re.MULTILINE)
_SPEC_COHERENCE_REVIEW_FIELD_RE = re.compile(
    r"^(Implementation task|Reviewed head SHA|Changed behavior-spec paths JSON):\s*(.*?)\s*$",
    re.MULTILINE,
)
_SPEC_COHERENCE_REVIEW_REQUIRED_FIELDS = (
    "Implementation task",
    "Reviewed head SHA",
    "Changed behavior-spec paths JSON",
)
_SPEC_COHERENCE_REVIEW_ALLOWED_FIELDS = frozenset(_SPEC_COHERENCE_REVIEW_REQUIRED_FIELDS)

_SLICE_HEADER_RE = re.compile(
    r"^Implement\s+plan\s+(?P<plan_id>\S+),\s*slice\s+(?P<slice_label>.+?)(?::\s*(?P<summary>.+))?$",
    re.IGNORECASE,
)
_SECTION_RE = re.compile(r"^##\s+(?P<title>[^\n]+)\n", re.MULTILINE)
_PLAN_REVIEW_PROVENANCE_FIELD_RE = re.compile(
    r"^- (?P<field>Plan source|Plan review|Slice):\s*(?P<value>.*?)\s*$"
)
_PLAN_REVIEW_SLICE_VALUE_RE = re.compile(r"^(?P<slice_id>\S+)(?:\s+\(.*\))?$")


ImplementSliceIdentityKind = Literal["plan_review_slice", "review_scope_fallback"]


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


@dataclass(frozen=True)
class SpecCoherenceReviewScope:
    """Parsed structured metadata for a spec-coherence review."""

    implementation_task_id: str
    reviewed_head_sha: str
    changed_paths: tuple[str, ...]


@dataclass(frozen=True)
class PlanReviewSliceProvenance:
    """Structured provenance embedded in materialized plan-review slice prompts."""

    plan_source_task_id: str
    plan_review_task_id: str
    slice_id: str


@dataclass(frozen=True)
class PlanReviewSliceProvenanceParseResult:
    """Parsed materialized slice provenance, including invalid-block detection."""

    provenance: PlanReviewSliceProvenance | None
    has_provenance_block: bool


@dataclass(frozen=True)
class ImplementSliceIdentity:
    """Conservative same-slice identity for failed implement-attempt reads."""

    kind: ImplementSliceIdentityKind
    review_scope: str
    plan_source_task_id: str | None = None
    plan_review_task_id: str | None = None
    slice_id: str | None = None


def declares_resolution_review_mode(scope: str | None) -> bool:
    """Return whether scope text claims to be resolution-review metadata."""
    return bool(scope and _RESOLUTION_REVIEW_MODE_RE.search(scope))


def declares_spec_coherence_review_mode(scope: str | None) -> bool:
    """Return whether scope text claims to be spec-coherence review metadata."""
    return bool(scope and _SPEC_COHERENCE_REVIEW_MODE_RE.search(scope))


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


def build_spec_coherence_review_scope(
    *,
    implementation_task_id: str,
    reviewed_head_sha: str,
    changed_paths: tuple[str, ...],
) -> str:
    """Build persisted structured scope text for a spec-coherence review."""
    normalized_paths = tuple(path.strip() for path in changed_paths if path.strip())
    if not implementation_task_id.strip():
        raise ValueError("spec coherence review scope requires an implementation task id")
    if not reviewed_head_sha.strip():
        raise ValueError("spec coherence review scope requires a reviewed head SHA")
    if not normalized_paths:
        raise ValueError("spec coherence review scope requires changed behavior-spec paths")
    return "\n".join(
        (
            "Review mode: spec-coherence",
            f"Implementation task: {implementation_task_id}",
            f"Reviewed head SHA: {reviewed_head_sha}",
            f"Changed behavior-spec paths JSON: {json.dumps(normalized_paths)}",
            "",
            "Review only the changed behavior-spec paths listed above against the rest of the behavior-spec set.",
        )
    )


def parse_spec_coherence_review_scope(scope: str | None) -> SpecCoherenceReviewScope | None:
    """Parse persisted structured metadata from spec-coherence review scope text."""
    if not declares_spec_coherence_review_mode(scope):
        return None
    assert scope is not None
    lines = scope.splitlines()
    mode_index = next(
        (
            index
            for index, raw_line in enumerate(lines)
            if raw_line.strip() == "Review mode: spec-coherence"
        ),
        None,
    )
    if mode_index is None:
        raise ValueError("spec coherence review metadata header is malformed")

    raw_fields: dict[str, str] = {}
    for raw_line in lines[mode_index + 1 :]:
        line = raw_line.strip()
        if not line:
            break
        match = _SPEC_COHERENCE_REVIEW_FIELD_RE.match(line)
        if match is None:
            raise ValueError(f"spec coherence review metadata line is malformed: {line}")
        field_name, field_value = match.groups()
        if field_name not in _SPEC_COHERENCE_REVIEW_ALLOWED_FIELDS:
            raise ValueError(f"unexpected spec coherence review metadata field: {field_name}")
        if field_name in raw_fields:
            raise ValueError(f"duplicate spec coherence review metadata field: {field_name}")
        raw_fields[field_name] = field_value.strip()

    missing_fields = [
        field for field in _SPEC_COHERENCE_REVIEW_REQUIRED_FIELDS if not raw_fields.get(field)
    ]
    if missing_fields:
        raise ValueError(
            "spec coherence review metadata is missing required fields: "
            + ", ".join(missing_fields)
        )
    paths_json = raw_fields["Changed behavior-spec paths JSON"]
    try:
        parsed_paths = json.loads(paths_json)
    except json.JSONDecodeError as exc:
        raise ValueError("spec coherence review paths JSON is malformed") from exc
    if not isinstance(parsed_paths, list):
        raise ValueError("spec coherence review paths JSON must decode to a list")
    normalized_paths = tuple(
        path.strip() for path in parsed_paths if isinstance(path, str) and path.strip()
    )
    if len(normalized_paths) != len(parsed_paths) or not normalized_paths:
        raise ValueError("spec coherence review paths JSON must contain only non-empty strings")

    return SpecCoherenceReviewScope(
        implementation_task_id=raw_fields["Implementation task"],
        reviewed_head_sha=raw_fields["Reviewed head SHA"],
        changed_paths=normalized_paths,
    )


def _normalize_scope_text(text: str | None) -> str | None:
    if text is None:
        return None
    normalized = text.strip()
    return normalized or None


def normalize_review_scope_identity_text(text: str | None) -> str | None:
    """Return conservative normalized scope text for same-slice identity matching."""
    normalized = _normalize_scope_text(text)
    if normalized is None:
        return None
    collapsed = re.sub(r"\s+", " ", normalized)
    return collapsed or None


def parse_plan_review_slice_provenance_result(
    prompt: str | None,
) -> PlanReviewSliceProvenanceParseResult:
    """Parse structured slice provenance and distinguish absence from invalid metadata."""
    if prompt is None:
        return PlanReviewSliceProvenanceParseResult(
            provenance=None,
            has_provenance_block=False,
        )
    lines = prompt.splitlines()
    provenance_indices = [
        index for index, raw_line in enumerate(lines) if raw_line.strip() == "Provenance:"
    ]
    if not provenance_indices:
        return PlanReviewSliceProvenanceParseResult(
            provenance=None,
            has_provenance_block=False,
        )
    if len(provenance_indices) != 1:
        return PlanReviewSliceProvenanceParseResult(
            provenance=None,
            has_provenance_block=True,
        )

    raw_fields: dict[str, str] = {}
    provenance_index = provenance_indices[0]
    for raw_line in lines[provenance_index + 1 :]:
        line = raw_line.strip()
        if not line:
            break
        match = _PLAN_REVIEW_PROVENANCE_FIELD_RE.match(line)
        if match is None:
            return PlanReviewSliceProvenanceParseResult(
                provenance=None,
                has_provenance_block=True,
            )
        field_name = match.group("field")
        field_value = match.group("value").strip()
        if not field_value or field_name in raw_fields:
            return PlanReviewSliceProvenanceParseResult(
                provenance=None,
                has_provenance_block=True,
            )
        raw_fields[field_name] = field_value

    if set(raw_fields) != {"Plan source", "Plan review", "Slice"}:
        return PlanReviewSliceProvenanceParseResult(
            provenance=None,
            has_provenance_block=True,
        )
    slice_match = _PLAN_REVIEW_SLICE_VALUE_RE.match(raw_fields["Slice"])
    if slice_match is None:
        return PlanReviewSliceProvenanceParseResult(
            provenance=None,
            has_provenance_block=True,
        )
    slice_id = slice_match.group("slice_id").strip()
    if not slice_id:
        return PlanReviewSliceProvenanceParseResult(
            provenance=None,
            has_provenance_block=True,
        )
    return PlanReviewSliceProvenanceParseResult(
        provenance=PlanReviewSliceProvenance(
            plan_source_task_id=raw_fields["Plan source"],
            plan_review_task_id=raw_fields["Plan review"],
            slice_id=slice_id,
        ),
        has_provenance_block=True,
    )


def parse_plan_review_slice_provenance(prompt: str | None) -> PlanReviewSliceProvenance | None:
    """Parse structured slice provenance from a materialized implement prompt."""
    return parse_plan_review_slice_provenance_result(prompt).provenance


def resolve_implement_slice_identity(
    *,
    prompt: str | None,
    review_scope: str | None,
) -> ImplementSliceIdentity | None:
    """Resolve the shared same-slice identity for an implement task, or fail closed."""
    normalized_review_scope = normalize_review_scope_identity_text(review_scope)
    if normalized_review_scope is None:
        return None
    provenance_result = parse_plan_review_slice_provenance_result(prompt)
    provenance = provenance_result.provenance
    if provenance is not None:
        return ImplementSliceIdentity(
            kind="plan_review_slice",
            review_scope=normalized_review_scope,
            plan_source_task_id=provenance.plan_source_task_id,
            plan_review_task_id=provenance.plan_review_task_id,
            slice_id=provenance.slice_id,
        )
    if provenance_result.has_provenance_block:
        return None
    return ImplementSliceIdentity(
        kind="review_scope_fallback",
        review_scope=normalized_review_scope,
    )


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
