"""Shared review verdict and finding parsing helpers."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, cast

from gza.db import Task

ReviewSeverity = Literal["BLOCKER", "FOLLOWUP", "NIT"]
ReviewTemplateVerdict = Literal["APPROVED", "CHANGES_REQUESTED", "NEEDS_DISCUSSION"]
ReviewParseError = Literal[
    "empty_content",
    "missing_required_sections",
    "malformed_checklist",
    "malformed_must_fix_section",
    "malformed_suggestions_section",
    "missing_verdict",
    "multiple",
]

_VERDICT_TOKEN = r"(APPROVED_WITH_FOLLOWUPS|APPROVED|CHANGES_REQUESTED|NEEDS_DISCUSSION)"

# Inline formats:
# - **Verdict: APPROVED**
# - **Verdict**: CHANGES_REQUESTED
# - Verdict: NEEDS_DISCUSSION
_INLINE_VERDICT_PATTERN = re.compile(
    rf"\*{{0,2}}Verdict\*{{0,2}}:\s*\*{{0,2}}{_VERDICT_TOKEN}\*{{0,2}}",
    re.IGNORECASE,
)

# Heading format:
# ## Verdict
#
# **CHANGES_REQUESTED**
_HEADING_VERDICT_PATTERN = re.compile(
    rf"#{{2,6}}\s+Verdict\s*\n+\s*\*{{0,2}}{_VERDICT_TOKEN}\*{{0,2}}",
    re.IGNORECASE,
)

_H2_PATTERN = re.compile(r"^##\s+(.+?)\s*$", re.MULTILINE)
_H3_PATTERN = re.compile(r"^###\s+(.+?)\s*$", re.MULTILINE)
_CHECKLIST_LINE_PATTERN = re.compile(
    r"^\s*(?:[-*]|\d+[.)])?\s*(yes|no)\s*[-:]\s*(.+?)\s*$",
    re.IGNORECASE,
)


MUST_FIX_WEIGHT = 20
SUGGESTION_WEIGHT = 3
SUMMARY_NO_WEIGHT = 10
SCORE_MIN = 0
SCORE_MAX = 100


@dataclass(frozen=True)
class ReviewFinding:
    """Structured review finding parsed from canonical markdown."""

    id: str
    severity: ReviewSeverity
    title: str
    body: str
    evidence: str | None
    impact: str | None
    fix_or_followup: str | None
    tests: str | None


@dataclass(frozen=True)
class ParsedReviewReport:
    """Structured review report parsed from markdown content."""

    verdict: str | None
    findings: tuple[ReviewFinding, ...]
    format_version: Literal["legacy", "v2", "unknown"]


@dataclass(frozen=True)
class ParsedReview:
    """Parsed review-template sections used for derived review scoring."""

    must_fix_count: int
    suggestion_count: int
    summary_checklist: tuple[tuple[str, bool], ...]
    verdict: ReviewTemplateVerdict | None
    unparseable: bool
    parse_error: ReviewParseError | None = None


def _extract_verdict(content: str) -> str | None:
    inline_match = _INLINE_VERDICT_PATTERN.search(content)
    if inline_match:
        return inline_match.group(1).upper()

    heading_match = _HEADING_VERDICT_PATTERN.search(content)
    if heading_match:
        return heading_match.group(1).upper()

    return None


def _parse_fields(block: str, *, labels: list[str]) -> dict[str, str]:
    values: dict[str, str] = {}
    active_label: str | None = None
    for raw_line in block.splitlines():
        line = raw_line.rstrip()
        stripped = line.strip()
        if not stripped:
            if active_label is not None:
                values[active_label] = (values.get(active_label, "") + "\n").strip("\n")
            continue
        matched = False
        for label in labels:
            prefix = f"{label}:"
            if stripped.lower().startswith(prefix.lower()):
                values[label] = stripped[len(prefix):].strip()
                active_label = label
                matched = True
                break
        if matched:
            continue
        if active_label is not None:
            prior = values.get(active_label, "")
            values[active_label] = f"{prior}\n{line}".strip("\n")
    return values


def _normalize_h2(name: str) -> str:
    return re.sub(r"[\s\-_]+", "", name.lower())


def _split_h2_sections(content: str) -> dict[str, str]:
    matches = list(_H2_PATTERN.finditer(content))
    sections: dict[str, str] = {}
    for idx, match in enumerate(matches):
        section_name = _normalize_h2(match.group(1))
        start = match.end()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(content)
        sections[section_name] = content[start:end].strip()
    return sections


def _split_h3_entries(section_body: str) -> list[tuple[str, str]]:
    matches = list(_H3_PATTERN.finditer(section_body))
    if not matches:
        return []
    entries: list[tuple[str, str]] = []
    for idx, match in enumerate(matches):
        heading = match.group(1).strip()
        start = match.end()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(section_body)
        body = section_body[start:end].strip()
        entries.append((heading, body))
    return entries


def _parse_finding_entries(
    section_body: str,
    *,
    severity: ReviewSeverity,
    fix_label: str,
    tests_label: str,
) -> list[ReviewFinding]:
    findings: list[ReviewFinding] = []
    for heading, body in _split_h3_entries(section_body):
        heading_parts = heading.split(None, 1)
        finding_id = heading_parts[0].strip()
        title = heading_parts[1].strip() if len(heading_parts) > 1 else heading.strip()
        parsed = _parse_fields(
            body,
            labels=["Evidence", "Impact", fix_label, tests_label],
        )
        finding = ReviewFinding(
            id=finding_id,
            severity=severity,
            title=title,
            body=body,
            evidence=parsed.get("Evidence"),
            impact=parsed.get("Impact"),
            fix_or_followup=parsed.get(fix_label),
            tests=parsed.get(tests_label),
        )
        findings.append(finding)
    return findings


def _section_is_none_literal(text: str) -> bool:
    return text.strip().rstrip(".").strip().lower() == "none"


def _parse_checklist(summary_body: str) -> tuple[tuple[tuple[str, bool], ...], bool]:
    checklist: list[tuple[str, bool]] = []
    malformed = False
    saw_list_marker = False
    for line in summary_body.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith(("-", "*")):
            saw_list_marker = True
        matched = _CHECKLIST_LINE_PATTERN.match(line)
        if matched is None:
            continue
        text = matched.group(2).strip()
        is_yes = matched.group(1).strip().lower() == "yes"
        checklist.append((text, is_yes))

    if saw_list_marker and not checklist:
        malformed = True
    return tuple(checklist), malformed


def _section_entries_and_malformed(
    sections: dict[str, str],
    keys: tuple[str, ...],
) -> tuple[list[tuple[str, str]], bool]:
    """Parse H3 entries from the first-present section keys and detect malformed bodies."""
    entries: list[tuple[str, str]] = []
    malformed = False
    for key in keys:
        body = sections.get(key, "")
        section_entries = _split_h3_entries(body)
        entries.extend(section_entries)
        if body.strip() and not section_entries and not _section_is_none_literal(body):
            malformed = True
    return entries, malformed


def parse_review_template(content: str | None) -> ParsedReview:
    """Parse review-template fields used for deterministic quality scoring."""
    if not content or not content.strip():
        return ParsedReview(0, 0, (), None, True, "empty_content")

    sections = _split_h2_sections(content)
    summary_body = sections.get("summary", "")
    must_fix_entries, malformed_must_fix_section = _section_entries_and_malformed(
        sections, ("blockers", "mustfix")
    )
    suggestion_entries, malformed_suggestions_section = _section_entries_and_malformed(
        sections, ("followups", "suggestions")
    )

    checklist, checklist_malformed = _parse_checklist(summary_body)
    verdict = _extract_verdict(content)
    normalized_verdict: ReviewTemplateVerdict | None = None
    if verdict in {"APPROVED", "CHANGES_REQUESTED", "NEEDS_DISCUSSION"}:
        normalized_verdict = cast(ReviewTemplateVerdict, verdict)

    parse_errors: list[ReviewParseError] = []
    missing_required_sections = (
        "summary" not in sections
        or not any(key in sections for key in ("blockers", "mustfix"))
        or not any(key in sections for key in ("followups", "suggestions"))
    )
    if missing_required_sections:
        parse_errors.append("missing_required_sections")
    if malformed_must_fix_section:
        parse_errors.append("malformed_must_fix_section")
    if malformed_suggestions_section:
        parse_errors.append("malformed_suggestions_section")
    if checklist_malformed:
        parse_errors.append("malformed_checklist")
    if normalized_verdict is None:
        parse_errors.append("missing_verdict")

    unparseable = bool(parse_errors)
    parse_error: ReviewParseError | None = None
    if len(parse_errors) == 1:
        parse_error = parse_errors[0]
    elif parse_errors:
        parse_error = "multiple"

    return ParsedReview(
        must_fix_count=len(must_fix_entries),
        suggestion_count=len(suggestion_entries),
        summary_checklist=checklist,
        verdict=normalized_verdict,
        unparseable=unparseable,
        parse_error=parse_error,
    )


def compute_review_score(parsed: ParsedReview) -> int:
    """Compute deterministic 0-100 review quality score from parsed review output.

    Weight constants are intentionally heuristic and expected to be tuned over time
    as real review analytics data accumulates.
    """
    has_structured_signals = bool(
        parsed.must_fix_count > 0
        or parsed.suggestion_count > 0
        or parsed.summary_checklist
    )
    if parsed.unparseable:
        # Fail closed for malformed or incomplete parsing outcomes, with one
        # explicit exception: missing verdict only.
        if parsed.parse_error != "missing_verdict" or not has_structured_signals:
            return 0

    no_count = sum(1 for _, is_yes in parsed.summary_checklist if not is_yes)
    score = (
        100
        - (MUST_FIX_WEIGHT * parsed.must_fix_count)
        - (SUGGESTION_WEIGHT * parsed.suggestion_count)
        - (SUMMARY_NO_WEIGHT * no_count)
    )
    return max(SCORE_MIN, min(SCORE_MAX, score))


def parse_review_report(content: str | None) -> ParsedReviewReport:
    """Parse verdict and structured findings from review markdown."""
    if not content:
        return ParsedReviewReport(verdict=None, findings=(), format_version="unknown")

    verdict = _extract_verdict(content)
    sections = _split_h2_sections(content)

    has_v2_headings = "blockers" in sections or "followups" in sections
    has_legacy_headings = "mustfix" in sections or "suggestions" in sections
    format_version: Literal["legacy", "v2", "unknown"] = "unknown"
    if has_v2_headings:
        format_version = "v2"
    elif has_legacy_headings:
        format_version = "legacy"

    findings: list[ReviewFinding] = []
    if "blockers" in sections:
        findings.extend(
            _parse_finding_entries(
                sections["blockers"],
                severity="BLOCKER",
                fix_label="Required fix",
                tests_label="Required tests",
            )
        )
    elif "mustfix" in sections:
        findings.extend(
            _parse_finding_entries(
                sections["mustfix"],
                severity="BLOCKER",
                fix_label="Required fix",
                tests_label="Required tests",
            )
        )

    if "followups" in sections:
        findings.extend(
            _parse_finding_entries(
                sections["followups"],
                severity="FOLLOWUP",
                fix_label="Recommended follow-up",
                tests_label="Recommended tests",
            )
        )

    return ParsedReviewReport(
        verdict=verdict,
        findings=tuple(findings),
        format_version=format_version,
    )


def parse_review_verdict(content: str | None) -> str | None:
    """Extract a normalized review verdict from markdown content."""
    return parse_review_report(content).verdict


def get_review_report(project_dir: Path, review_task: Task) -> ParsedReviewReport:
    """Extract parsed review report from cached output or the report file."""
    if review_task.output_content:
        return parse_review_report(review_task.output_content)

    if not review_task.report_file:
        return ParsedReviewReport(verdict=None, findings=(), format_version="unknown")

    review_path = project_dir / review_task.report_file
    if not review_path.exists():
        return ParsedReviewReport(verdict=None, findings=(), format_version="unknown")

    return parse_review_report(review_path.read_text())


def get_review_verdict(project_dir: Path, review_task: Task) -> str | None:
    """Extract the review verdict from cached output or the report file."""
    return get_review_report(project_dir, review_task).verdict


def get_review_score(project_dir: Path, review_task: Task) -> int | None:
    """Compute a deterministic review score from cached output or the report file.

    Returns ``None`` when no score source exists (no output content and no readable report).
    """
    if review_task.output_content:
        return compute_review_score(parse_review_template(review_task.output_content))

    if not review_task.report_file:
        return None

    review_path = project_dir / review_task.report_file
    if not review_path.exists():
        return None

    return compute_review_score(parse_review_template(review_path.read_text()))
