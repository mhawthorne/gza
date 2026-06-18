"""Plan-review verdict and slice-manifest parsing helpers."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

from gza.db import Task

PlanReviewVerdict = Literal["APPROVED", "CHANGES_REQUESTED", "NEEDS_DISCUSSION"]

_VERDICT_TOKEN = r"(APPROVED|CHANGES_REQUESTED|NEEDS_DISCUSSION)"
_INLINE_VERDICT_PATTERN = re.compile(
    rf"^[^\S\r\n]*\*{{0,2}}Verdict\*{{0,2}}:\s*\*{{0,2}}({_VERDICT_TOKEN})\*{{0,2}}[^\S\r\n]*$",
    re.IGNORECASE | re.MULTILINE,
)
_VERDICT_TOKEN_LINE_PATTERN = re.compile(
    rf"^[^\S\r\n]*\*{{0,2}}({_VERDICT_TOKEN})\*{{0,2}}[^\S\r\n]*$",
    re.IGNORECASE | re.MULTILINE,
)
_H2_PATTERN = re.compile(r"^##\s+(.+?)\s*$", re.MULTILINE)
_HEADING_PATTERN = re.compile(r"^(#{2,6})\s+(.+?)\s*$", re.MULTILINE)
_JSON_FENCE_PATTERN = re.compile(r"```json\s*\n(.*?)\n```", re.IGNORECASE | re.DOTALL)
_SUPPORTED_SCHEMA_VERSIONS = frozenset({1})
_SOURCE_TASK_TYPES = frozenset({"plan", "plan_improve"})
SLICE_COMPLEXITIES = frozenset({"small", "medium", "large"})


class PlanReviewValidationError(ValueError):
    """Raised when a plan-review report or manifest violates the contract."""


@dataclass(frozen=True)
class PlanReviewSliceQuality:
    """Top-level slice-budget assertion emitted by the reviewer."""

    fits_single_task_budget: bool
    timeout_budget_minutes: int | None
    max_expected_files_changed_per_slice: int | None
    rationale: str | None


@dataclass(frozen=True)
class PlanReviewSlice:
    """One implementation slice proposed by an approved plan review."""

    slice_id: str
    title: str
    prompt: str
    scope: tuple[str, ...]
    out_of_scope: tuple[str, ...]
    acceptance_criteria: tuple[str, ...]
    depends_on_slices: tuple[str, ...]
    based_on_slice: str | None
    review_scope: str
    estimated_complexity: str
    expected_timeout_minutes: int
    requires_code_review: bool
    tags: tuple[str, ...]


@dataclass(frozen=True)
class PlanReviewManifest:
    """Validated machine-readable slice manifest."""

    schema_version: int
    source_task_id: str
    source_task_type: str
    verdict: PlanReviewVerdict
    slice_quality: PlanReviewSliceQuality
    slices: tuple[PlanReviewSlice, ...]
    manual_override: dict[str, Any] | None


@dataclass(frozen=True)
class ParsedPlanReviewReport:
    """Parsed review report with markdown verdict and optional raw manifest."""

    verdict: PlanReviewVerdict | None
    raw_manifest: dict[str, Any] | None


@dataclass(frozen=True)
class PlanReviewOutcome:
    """Parsed verdict plus validated manifest outcome for lifecycle consumers."""

    verdict: PlanReviewVerdict | None
    manifest: PlanReviewManifest | None
    validation_error: str | None = None


def _normalize_h2(name: str) -> str:
    return re.sub(r"[\s\-_]+", "", name.lower())


def _collect_verdict_matches(content: str, *, token_only: bool) -> list[tuple[int, PlanReviewVerdict]]:
    pattern = _VERDICT_TOKEN_LINE_PATTERN if token_only else _INLINE_VERDICT_PATTERN
    return [
        (match.start(), cast(PlanReviewVerdict, match.group(1).upper()))
        for match in pattern.finditer(content)
    ]


def _collect_heading_verdict_matches(content: str) -> list[tuple[int, PlanReviewVerdict]]:
    matches = list(_HEADING_PATTERN.finditer(content))
    verdicts: list[tuple[int, PlanReviewVerdict]] = []
    for idx, match in enumerate(matches):
        heading_level = len(match.group(1))
        heading_name = match.group(2).strip()
        if _normalize_h2(heading_name) != "verdict":
            continue
        start = match.end()
        end = len(content)
        for later in matches[idx + 1 :]:
            later_level = len(later.group(1))
            if later_level <= heading_level:
                end = later.start()
                break
        body = content[start:end]
        for offset, verdict in _collect_verdict_matches(body, token_only=True):
            verdicts.append((start + offset, verdict))
    return verdicts


def parse_plan_review_verdict(content: str | None) -> PlanReviewVerdict | None:
    """Parse the plan-review verdict from markdown content."""
    if not content or not content.strip():
        return None
    h2_sections = list(_H2_PATTERN.finditer(content))
    verdict_sections = [
        (
            match.end(),
            h2_sections[idx + 1].start() if idx + 1 < len(h2_sections) else len(content),
        )
        for idx, match in enumerate(h2_sections)
        if _normalize_h2(match.group(1)) == "verdict"
    ]
    if verdict_sections:
        start, end = verdict_sections[-1]
        section_body = content[start:end]
        section_matches = sorted(
            [
                *_collect_verdict_matches(section_body, token_only=False),
                *_collect_verdict_matches(section_body, token_only=True),
            ],
            key=lambda item: item[0],
        )
        unique_verdicts = tuple(dict.fromkeys(verdict for _, verdict in section_matches))
        if len(unique_verdicts) == 1:
            return unique_verdicts[0]
        return None

    matches = sorted(
        [
            *_collect_verdict_matches(content, token_only=False),
            *_collect_heading_verdict_matches(content),
        ],
        key=lambda item: item[0],
    )
    if not matches:
        return None
    return matches[-1][1]


def parse_plan_review_report(content: str | None) -> ParsedPlanReviewReport:
    """Parse markdown verdict and optional fenced JSON manifest from a review report."""
    verdict = parse_plan_review_verdict(content)
    if not content or not content.strip():
        return ParsedPlanReviewReport(verdict=verdict, raw_manifest=None)

    matches = _JSON_FENCE_PATTERN.findall(content)
    if len(matches) > 1:
        raise PlanReviewValidationError("plan review report must contain exactly one json manifest block")
    if not matches:
        return ParsedPlanReviewReport(verdict=verdict, raw_manifest=None)

    try:
        parsed = json.loads(matches[0])
    except json.JSONDecodeError as exc:
        raise PlanReviewValidationError(f"invalid plan review manifest json: {exc}") from exc
    raw_manifest = _require_object(parsed, "plan review manifest")
    return ParsedPlanReviewReport(verdict=verdict, raw_manifest=raw_manifest)


def validate_plan_review_report(
    content: str | None,
    *,
    source_task_id: str,
    source_task_type: str,
    max_slice_timeout_minutes: int,
    max_plan_slices: int | None = None,
) -> PlanReviewManifest | None:
    """Parse and validate a plan-review report against the v1 slice contract."""
    report = parse_plan_review_report(content)
    if report.verdict is None:
        raise PlanReviewValidationError("plan review report is missing a verdict")
    if report.verdict != "APPROVED":
        if report.raw_manifest is None:
            return None
        _validate_manifest_common(
            report.raw_manifest,
            markdown_verdict=report.verdict,
            source_task_id=source_task_id,
            source_task_type=source_task_type,
        )
        return None
    if report.raw_manifest is None:
        raise PlanReviewValidationError("approved plan review report must include a json manifest block")
    return validate_plan_review_manifest(
        report.raw_manifest,
        markdown_verdict=report.verdict,
        source_task_id=source_task_id,
        source_task_type=source_task_type,
        max_slice_timeout_minutes=max_slice_timeout_minutes,
        max_plan_slices=max_plan_slices,
    )


def validate_plan_review_manifest(
    raw_manifest: dict[str, Any],
    *,
    markdown_verdict: PlanReviewVerdict,
    source_task_id: str,
    source_task_type: str,
    max_slice_timeout_minutes: int,
    max_plan_slices: int | None = None,
) -> PlanReviewManifest:
    """Validate a parsed json manifest and return a structured representation."""
    manifest = _validate_manifest_common(
        raw_manifest,
        markdown_verdict=markdown_verdict,
        source_task_id=source_task_id,
        source_task_type=source_task_type,
    )
    if manifest.verdict != "APPROVED":
        raise PlanReviewValidationError("only APPROVED plan review manifests may include slices")
    if not manifest.slice_quality.fits_single_task_budget:
        raise PlanReviewValidationError("approved plan review manifest must assert fits_single_task_budget=true")
    if not manifest.slices:
        raise PlanReviewValidationError("approved plan review manifest must include at least one slice")
    if max_plan_slices is not None and len(manifest.slices) > max_plan_slices:
        raise PlanReviewValidationError("approved plan review manifest exceeds max_plan_slices")

    seen_slice_ids: set[str] = set()
    slice_index: dict[str, int] = {}
    for index, slice_manifest in enumerate(manifest.slices):
        if slice_manifest.slice_id in seen_slice_ids:
            raise PlanReviewValidationError(f"duplicate slice_id: {slice_manifest.slice_id}")
        seen_slice_ids.add(slice_manifest.slice_id)
        slice_index[slice_manifest.slice_id] = index

        if len(slice_manifest.depends_on_slices) > 1:
            raise PlanReviewValidationError(
                f"slice {slice_manifest.slice_id} exceeds the v1 single-dependency limit"
            )
        if slice_manifest.expected_timeout_minutes > max_slice_timeout_minutes:
            raise PlanReviewValidationError(
                f"slice {slice_manifest.slice_id} exceeds plan slice timeout budget "
                f"({slice_manifest.expected_timeout_minutes}m > {max_slice_timeout_minutes}m)"
            )
        if not slice_manifest.requires_code_review:
            raise PlanReviewValidationError(
                f"slice {slice_manifest.slice_id} must set requires_code_review=true"
            )

    for index, slice_manifest in enumerate(manifest.slices):
        for dep_id in slice_manifest.depends_on_slices:
            if dep_id not in slice_index:
                raise PlanReviewValidationError(
                    f"slice {slice_manifest.slice_id} references unknown dependency {dep_id}"
                )
            if slice_index[dep_id] >= index:
                raise PlanReviewValidationError(
                    f"slice {slice_manifest.slice_id} must depend only on earlier slices"
                )
        if slice_manifest.based_on_slice is not None:
            based_on_id = slice_manifest.based_on_slice
            if based_on_id not in slice_index:
                raise PlanReviewValidationError(
                    f"slice {slice_manifest.slice_id} references unknown based_on_slice {based_on_id}"
                )
            if slice_index[based_on_id] >= index:
                raise PlanReviewValidationError(
                    f"slice {slice_manifest.slice_id} must base only on an earlier slice"
                )
            if tuple(slice_manifest.depends_on_slices) != (based_on_id,):
                raise PlanReviewValidationError(
                    f"slice {slice_manifest.slice_id} must list based_on_slice {based_on_id} "
                    "as its only depends_on_slices entry"
                )

    return manifest


def get_plan_review_report(project_dir: Path, review_task: Task) -> ParsedPlanReviewReport:
    """Extract parsed plan-review report from cached output or the report file."""
    if review_task.output_content:
        return parse_plan_review_report(review_task.output_content)

    if not review_task.report_file:
        return ParsedPlanReviewReport(verdict=None, raw_manifest=None)

    review_path = project_dir / review_task.report_file
    if not review_path.exists():
        return ParsedPlanReviewReport(verdict=None, raw_manifest=None)

    return parse_plan_review_report(review_path.read_text())


def get_plan_review_outcome(
    project_dir: Path,
    review_task: Task,
    *,
    source_task_id: str,
    source_task_type: str,
    max_slice_timeout_minutes: int,
    max_plan_slices: int | None = None,
) -> PlanReviewOutcome:
    """Return lifecycle-ready verdict and manifest validation details."""
    content: str | None
    if review_task.output_content:
        content = review_task.output_content
    elif review_task.report_file:
        review_path = project_dir / review_task.report_file
        content = review_path.read_text() if review_path.exists() else None
    else:
        content = None

    try:
        report = parse_plan_review_report(content)
    except PlanReviewValidationError as exc:
        return PlanReviewOutcome(
            verdict=None,
            manifest=None,
            validation_error=str(exc),
        )
    try:
        manifest = validate_plan_review_report(
            content,
            source_task_id=source_task_id,
            source_task_type=source_task_type,
            max_slice_timeout_minutes=max_slice_timeout_minutes,
            max_plan_slices=max_plan_slices,
        )
    except PlanReviewValidationError as exc:
        return PlanReviewOutcome(
            verdict=report.verdict,
            manifest=None,
            validation_error=str(exc),
        )
    return PlanReviewOutcome(
        verdict=report.verdict,
        manifest=manifest,
    )


def _validate_manifest_common(
    raw_manifest: dict[str, Any],
    *,
    markdown_verdict: PlanReviewVerdict,
    source_task_id: str,
    source_task_type: str,
) -> PlanReviewManifest:
    schema_version = _require_positive_int(raw_manifest.get("schema_version"), "schema_version")
    if schema_version not in _SUPPORTED_SCHEMA_VERSIONS:
        raise PlanReviewValidationError(f"unsupported plan review manifest schema_version: {schema_version}")

    manifest_source_task_id = _require_non_empty_string(
        raw_manifest.get("source_task_id"),
        "source_task_id",
    )
    if manifest_source_task_id != source_task_id:
        raise PlanReviewValidationError(
            f"manifest source_task_id {manifest_source_task_id} does not match {source_task_id}"
        )

    manifest_source_task_type = _require_non_empty_string(
        raw_manifest.get("source_task_type"),
        "source_task_type",
    )
    if manifest_source_task_type not in _SOURCE_TASK_TYPES:
        raise PlanReviewValidationError(
            f"source_task_type must be one of {sorted(_SOURCE_TASK_TYPES)}"
        )
    if manifest_source_task_type != source_task_type:
        raise PlanReviewValidationError(
            f"manifest source_task_type {manifest_source_task_type} does not match {source_task_type}"
        )

    manifest_verdict_raw = _require_non_empty_string(raw_manifest.get("verdict"), "verdict")
    if manifest_verdict_raw not in {"APPROVED", "CHANGES_REQUESTED", "NEEDS_DISCUSSION"}:
        raise PlanReviewValidationError("manifest verdict must be APPROVED, CHANGES_REQUESTED, or NEEDS_DISCUSSION")
    manifest_verdict = cast(PlanReviewVerdict, manifest_verdict_raw)
    if manifest_verdict != markdown_verdict:
        raise PlanReviewValidationError(
            f"manifest verdict {manifest_verdict} does not match markdown verdict {markdown_verdict}"
        )

    slice_quality = _parse_slice_quality(raw_manifest.get("slice_quality"))
    raw_slices = raw_manifest.get("slices")
    if raw_slices is None:
        raw_slices = []
    if not isinstance(raw_slices, list):
        raise PlanReviewValidationError("slices must be a list")
    slices = tuple(_parse_slice(item) for item in raw_slices)

    manual_override = _optional_object(
        raw_manifest.get("manual_override"),
        "manual_override",
    )

    return PlanReviewManifest(
        schema_version=schema_version,
        source_task_id=manifest_source_task_id,
        source_task_type=manifest_source_task_type,
        verdict=manifest_verdict,
        slice_quality=slice_quality,
        slices=slices,
        manual_override=manual_override,
    )


def _parse_slice_quality(value: object) -> PlanReviewSliceQuality:
    value = _require_object(value, "slice_quality")
    fits_single_task_budget = value.get("fits_single_task_budget")
    if not isinstance(fits_single_task_budget, bool):
        raise PlanReviewValidationError("slice_quality.fits_single_task_budget must be a boolean")
    timeout_budget_minutes = _optional_positive_int(
        value.get("timeout_budget_minutes"),
        "slice_quality.timeout_budget_minutes",
    )
    max_expected_files_changed_per_slice = _optional_positive_int(
        value.get("max_expected_files_changed_per_slice"),
        "slice_quality.max_expected_files_changed_per_slice",
    )
    rationale = value.get("rationale")
    if rationale is not None and not isinstance(rationale, str):
        raise PlanReviewValidationError("slice_quality.rationale must be a string when present")
    return PlanReviewSliceQuality(
        fits_single_task_budget=fits_single_task_budget,
        timeout_budget_minutes=timeout_budget_minutes,
        max_expected_files_changed_per_slice=max_expected_files_changed_per_slice,
        rationale=rationale.strip() if isinstance(rationale, str) and rationale.strip() else None,
    )


def _parse_slice(value: object) -> PlanReviewSlice:
    value = _require_object(value, "each slice")
    slice_id = _require_non_empty_string(value.get("slice_id"), "slice_id")
    title = _require_non_empty_string(value.get("title"), f"slice {slice_id}.title")
    prompt = _require_non_empty_string(value.get("prompt"), f"slice {slice_id}.prompt")
    scope = _require_non_empty_str_list(
        _coerce_string_to_str_list(value.get("scope")),
        f"slice {slice_id}.scope",
    )
    out_of_scope = _require_str_list(
        _coerce_string_to_str_list(value.get("out_of_scope")),
        f"slice {slice_id}.out_of_scope",
    )
    acceptance_criteria = _require_non_empty_str_list(
        value.get("acceptance_criteria"),
        f"slice {slice_id}.acceptance_criteria",
    )
    depends_on_slices = _require_str_list(
        value.get("depends_on_slices"),
        f"slice {slice_id}.depends_on_slices",
    )
    based_on_slice = _optional_non_empty_string(
        value.get("based_on_slice"),
        f"slice {slice_id}.based_on_slice",
    )
    review_scope = _require_non_empty_string(
        value.get("review_scope"),
        f"slice {slice_id}.review_scope",
    )
    estimated_complexity = _require_non_empty_string(
        value.get("estimated_complexity"),
        f"slice {slice_id}.estimated_complexity",
    )
    if estimated_complexity not in SLICE_COMPLEXITIES:
        raise PlanReviewValidationError(
            f"slice {slice_id}.estimated_complexity must be one of {sorted(SLICE_COMPLEXITIES)}"
        )
    expected_timeout_minutes = _require_positive_int(
        value.get("expected_timeout_minutes"),
        f"slice {slice_id}.expected_timeout_minutes",
    )
    requires_code_review = value.get("requires_code_review")
    if not isinstance(requires_code_review, bool):
        raise PlanReviewValidationError(f"slice {slice_id}.requires_code_review must be a boolean")
    tags = _require_str_list(value.get("tags"), f"slice {slice_id}.tags")
    return PlanReviewSlice(
        slice_id=slice_id,
        title=title,
        prompt=prompt,
        scope=scope,
        out_of_scope=out_of_scope,
        acceptance_criteria=acceptance_criteria,
        depends_on_slices=depends_on_slices,
        based_on_slice=based_on_slice,
        review_scope=review_scope,
        estimated_complexity=estimated_complexity,
        expected_timeout_minutes=expected_timeout_minutes,
        requires_code_review=requires_code_review,
        tags=tags,
    )


def _require_positive_int(value: object, field_name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise PlanReviewValidationError(f"{field_name} must be an integer")
    if value <= 0:
        raise PlanReviewValidationError(f"{field_name} must be positive")
    return value


def _optional_positive_int(value: object, field_name: str) -> int | None:
    if value is None:
        return None
    return _require_positive_int(value, field_name)


def _require_non_empty_string(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise PlanReviewValidationError(f"{field_name} must be a non-empty string")
    return value.strip()


def _optional_non_empty_string(value: object, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_non_empty_string(value, field_name)


def _require_object(value: object, field_name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise PlanReviewValidationError(f"{field_name} must be an object")
    return cast(dict[str, Any], value)


def _optional_object(value: object, field_name: str) -> dict[str, Any] | None:
    if value is None:
        return None
    return _require_object(value, field_name)


def _require_str_list(value: object, field_name: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise PlanReviewValidationError(f"{field_name} must be a list")
    result: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item.strip():
            raise PlanReviewValidationError(f"{field_name} entries must be non-empty strings")
        result.append(item.strip())
    return tuple(result)


def _require_non_empty_str_list(value: object, field_name: str) -> tuple[str, ...]:
    result = _require_str_list(value, field_name)
    if not result:
        raise PlanReviewValidationError(f"{field_name} must not be empty")
    return result


def _coerce_string_to_str_list(value: object) -> object:
    if isinstance(value, str):
        return [value]
    return value
