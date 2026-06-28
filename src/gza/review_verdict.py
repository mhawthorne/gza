"""Shared review verdict and finding parsing helpers."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, cast

from gza.db import Task

ReviewSeverity = Literal["BLOCKER", "FOLLOWUP", "NIT"]
DisputeReason = Literal["unreproducible", "already_satisfied", "stale", "out_of_scope", "review_error"]
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
    rf"^[^\S\r\n]*\*{{0,2}}Verdict\*{{0,2}}:\s*\*{{0,2}}({_VERDICT_TOKEN})\*{{0,2}}[^\S\r\n]*$",
    re.IGNORECASE | re.MULTILINE,
)

_VERDICT_TOKEN_LINE_PATTERN = re.compile(
    rf"^[^\S\r\n]*\*{{0,2}}({_VERDICT_TOKEN})\*{{0,2}}[^\S\r\n]*$",
    re.IGNORECASE | re.MULTILINE,
)

_H2_PATTERN = re.compile(r"^##\s+(.+?)\s*$", re.MULTILINE)
_H3_PATTERN = re.compile(r"^###\s+(.+?)\s*$", re.MULTILINE)
_HEADING_PATTERN = re.compile(r"^(#{2,6})\s+(.+?)\s*$", re.MULTILINE)
_CHECKLIST_LINE_PATTERN = re.compile(
    r"^\s*(?:[-*]|\d+[.)])?\s*(yes|no)\s*[-:]\s*(.+?)\s*$",
    re.IGNORECASE,
)
_DISPUTED_FINDING_ID_PATTERN = re.compile(r"^(?P<id>B\d+)(?:\b|[:.)-])", re.IGNORECASE)
_VERIFY_COMMAND_PATTERN = re.compile(r"\bverify_command\b", re.IGNORECASE)
_VERIFY_COMMAND_SUBJECT_PATTERN = re.compile(r"(?:verify_command|(?:\./)?bin/tests)\b", re.IGNORECASE)
_VERIFY_TIMEOUT_PATTERNS = (
    re.compile(r"\btimed\s+out\b", re.IGNORECASE),
    re.compile(r"\bexit\s+status:\s*timed\s+out\b", re.IGNORECASE),
    re.compile(r"(?<![A-Za-z0-9_])timeout(?![A-Za-z0-9_])", re.IGNORECASE),
)
_VERIFY_FAILURE_PATTERNS = (
    re.compile(
        r"\bverify_command\b\s+(?:failure|failures|failed|fails|failing|"
        r"errored|broken|did\s+not\s+pass|did\s+not\s+succeed|"
        r"exited\s+nonzero|exited\s+with\s+nonzero|"
        r"returned\s+nonzero|returned\s+failure)",
        re.IGNORECASE,
    ),
    re.compile(
        r"\bverify_command\b[^\n]{0,80}\b(?:result|exit|exited|status|outcome)\b"
        r"[^\n]{0,40}\b(?:fail|failed|failure|nonzero|non-zero|error)\b",
        re.IGNORECASE,
    ),
)
_VERIFY_FAILED_PHASE_PATTERN = re.compile(r"\bgza-verify\s+phase=failed\b", re.IGNORECASE)
_VERIFY_FAILED_STATUS_PATTERN = re.compile(r"(^|\n)\s*[-*]?\s*status:\s*failed\b", re.IGNORECASE)
_VERIFY_EXIT_STATUS_PATTERN = re.compile(r"(^|\n)\s*[-*]?\s*exit\s+status:\s*(?!timed\s+out\b).+", re.IGNORECASE)
_TITLE_LEADING_MARKER_PATTERN = re.compile(r"^\s*(?:[-*+]\s+|\d+[.)]\s+|[A-Z]\d+[:.)-]?\s+)+", re.IGNORECASE)
_BLOCKER_SECTION_PATTERN = re.compile(
    r"^##\s+(Blockers|Must-Fix)\s*$\n?(.*?)(?=^##\s+|\Z)",
    re.IGNORECASE | re.MULTILINE | re.DOTALL,
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
    open_state_citation: str | None = None


@dataclass(frozen=True)
class DisputedBlocker:
    """Structured disputed blocker item parsed from a code-task summary/report."""

    finding_id: str
    reason: DisputeReason
    evidence: str
    current_state_citation: str
    scope_citation: str | None = None
    downstream_task_id: str | None = None


@dataclass(frozen=True)
class ReviewBlockerAdjudication:
    """Strict adjudication outcome for one disputed review blocker."""

    verdict: Literal["VALID", "INVALID", "NEEDS_HUMAN"]


@dataclass(frozen=True)
class ParsedReviewReport:
    """Structured review report parsed from markdown content."""

    verdict: str | None
    findings: tuple[ReviewFinding, ...]
    format_version: Literal["legacy", "v2", "unknown"]
    blockers_section_present: bool = False
    blockers_section_explicit_none: bool = False
    blockers_section_malformed: bool = False


@dataclass(frozen=True)
class ReviewOutcome:
    """Shared review outcome summary for CLI display and automation."""

    verdict: str | None
    followup_findings: tuple[ReviewFinding, ...]


@dataclass(frozen=True)
class ReviewBlockerSummary:
    """Conservative classification of blocker types in a review report."""

    blocker_count: int
    verify_timeout_count: int
    verify_failure_count: int
    unknown_or_code_count: int

    @property
    def is_verify_timeout_only(self) -> bool:
        return self.blocker_count > 0 and self.verify_timeout_count == self.blocker_count

    @property
    def is_verify_blocked_only(self) -> bool:
        return self.blocker_count > 0 and (
            self.verify_timeout_count + self.verify_failure_count == self.blocker_count
        )


@dataclass(frozen=True)
class ReviewContractValidation:
    """Non-fatal validation warnings for the canonical review output contract."""

    blockers_missing_open_state_citation: tuple[str, ...] = ()
    blockers_with_malformed_open_state_citation: tuple[str, ...] = ()

    @property
    def has_warnings(self) -> bool:
        return bool(
            self.blockers_missing_open_state_citation
            or self.blockers_with_malformed_open_state_citation
        )


@dataclass(frozen=True)
class ParsedReview:
    """Parsed review-template sections used for derived review scoring."""

    must_fix_count: int
    suggestion_count: int
    summary_checklist: tuple[tuple[str, bool], ...]
    verdict: ReviewTemplateVerdict | None
    unparseable: bool
    parse_error: ReviewParseError | None = None


@dataclass(frozen=True)
class _VerdictExtraction:
    verdict: str | None
    has_h2_verdict_section: bool
    section_verdicts: tuple[str, ...] = ()


def _collect_verdict_matches(content: str, *, token_only: bool) -> list[tuple[int, str]]:
    pattern = _VERDICT_TOKEN_LINE_PATTERN if token_only else _INLINE_VERDICT_PATTERN
    return [(match.start(), match.group(1).upper()) for match in pattern.finditer(content)]


def _collect_heading_verdict_matches(content: str) -> list[tuple[int, str]]:
    matches = list(_HEADING_PATTERN.finditer(content))
    verdicts: list[tuple[int, str]] = []
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


def _extract_verdict_details(content: str) -> _VerdictExtraction:
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
            return _VerdictExtraction(
                verdict=unique_verdicts[0],
                has_h2_verdict_section=True,
                section_verdicts=unique_verdicts,
            )
        return _VerdictExtraction(
            verdict=None,
            has_h2_verdict_section=True,
            section_verdicts=unique_verdicts,
        )

    matches = sorted(
        [
            *_collect_verdict_matches(content, token_only=False),
            *_collect_heading_verdict_matches(content),
        ],
        key=lambda item: item[0],
    )
    if not matches:
        return _VerdictExtraction(verdict=None, has_h2_verdict_section=False)
    return _VerdictExtraction(verdict=matches[-1][1], has_h2_verdict_section=False)


def _extract_verdict(content: str) -> str | None:
    return _extract_verdict_details(content).verdict


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


def _is_valid_task_id_shape(value: str) -> bool:
    return bool(re.fullmatch(r"[a-z0-9]{1,12}-\d+", value.strip()))


def _normalize_disputed_blocker_finding_id(value: str) -> str:
    normalized = value.strip()
    if not normalized:
        return ""
    matched = _DISPUTED_FINDING_ID_PATTERN.match(normalized)
    if matched is None:
        return normalized
    return matched.group("id").upper()


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
            labels=["Evidence", "Open-state citation", "Impact", fix_label, tests_label],
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
            open_state_citation=parsed.get("Open-state citation"),
        )
        findings.append(finding)
    return findings


def parse_disputed_blockers(content: str | None) -> tuple[DisputedBlocker, ...]:
    """Parse structured disputed blocker items from a code-task summary/report."""
    if not content:
        return ()

    section_body = _split_h2_sections(content).get("disputedblockers", "")
    entries = _split_h3_entries(section_body)
    if not entries:
        return ()

    disputes: list[DisputedBlocker] = []
    for _heading, body in entries:
        parsed = _parse_fields(
            body,
            labels=[
                "Finding",
                "Reason",
                "Evidence",
                "Current-state citation",
                "Scope citation",
                "Downstream task",
            ],
        )
        finding_id = _normalize_disputed_blocker_finding_id(parsed.get("Finding") or "")
        reason = (parsed.get("Reason") or "").strip().lower()
        evidence = (parsed.get("Evidence") or "").strip()
        current_state_citation = (parsed.get("Current-state citation") or "").strip()
        scope_citation = (parsed.get("Scope citation") or "").strip() or None
        downstream_task_id = (parsed.get("Downstream task") or "").strip() or None

        if not finding_id or not reason or not evidence or not current_state_citation:
            continue
        if reason not in {"unreproducible", "already_satisfied", "stale", "out_of_scope", "review_error"}:
            continue
        if not _has_valid_open_state_citation_shape(current_state_citation):
            continue
        if scope_citation is not None and not _has_valid_open_state_citation_shape(scope_citation):
            continue
        if downstream_task_id is not None and not _is_valid_task_id_shape(downstream_task_id):
            continue

        disputes.append(
            DisputedBlocker(
                finding_id=finding_id,
                reason=cast(DisputeReason, reason),
                evidence=evidence,
                current_state_citation=current_state_citation,
                scope_citation=scope_citation,
                downstream_task_id=downstream_task_id,
            )
        )

    return tuple(disputes)


def parse_review_blocker_adjudication(
    content: str | None,
) -> ReviewBlockerAdjudication | None:
    """Parse a strict adjudication result from internal-task output."""
    if not content:
        return None

    lines = [line.strip() for line in content.splitlines() if line.strip()]
    if len(lines) != 1:
        return None

    verdict = lines[0].upper()
    if verdict not in {"VALID", "INVALID", "NEEDS_HUMAN"}:
        return None
    return ReviewBlockerAdjudication(verdict=cast(Literal["VALID", "INVALID", "NEEDS_HUMAN"], verdict))


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
    verdict_info = _extract_verdict_details(content)
    verdict = verdict_info.verdict
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
    if verdict_info.has_h2_verdict_section and len(verdict_info.section_verdicts) > 1:
        parse_errors.append("multiple")
    elif normalized_verdict is None:
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
    if parsed.unparseable and not _parsed_review_has_usable_score_signals(parsed):
        # Fail closed for malformed or incomplete parsing outcomes, with one
        # explicit exception: missing verdict only when structured findings exist.
        return 0

    no_count = sum(1 for _, is_yes in parsed.summary_checklist if not is_yes)
    score = (
        100
        - (MUST_FIX_WEIGHT * parsed.must_fix_count)
        - (SUGGESTION_WEIGHT * parsed.suggestion_count)
        - (SUMMARY_NO_WEIGHT * no_count)
    )
    return max(SCORE_MIN, min(SCORE_MAX, score))


def _parsed_review_has_usable_score_signals(parsed: ParsedReview) -> bool:
    has_structured_signals = bool(
        parsed.must_fix_count > 0
        or parsed.suggestion_count > 0
        or parsed.summary_checklist
    )
    return parsed.parse_error == "missing_verdict" and has_structured_signals


def _compute_backfillable_review_score(content: str | None) -> int | None:
    if not content:
        return None
    parsed = parse_review_template(content)
    if parsed.unparseable and not _parsed_review_has_usable_score_signals(parsed):
        return None
    return compute_review_score(parsed)


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

    blockers_section_present = "blockers" in sections or "mustfix" in sections
    blockers_section_body = sections.get("blockers")
    if blockers_section_body is None:
        blockers_section_body = sections.get("mustfix", "")
    blockers_section_explicit_none = False
    blockers_section_malformed = False
    if blockers_section_present:
        blockers_section_explicit_none = _section_is_none_literal(blockers_section_body)
        blockers_section_entries = _split_h3_entries(blockers_section_body)
        blockers_section_malformed = bool(
            blockers_section_body.strip()
            and not blockers_section_entries
            and not blockers_section_explicit_none
        )

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
        blockers_section_present=blockers_section_present,
        blockers_section_explicit_none=blockers_section_explicit_none,
        blockers_section_malformed=blockers_section_malformed,
    )


def _contains_verify_timeout_marker(text: str) -> bool:
    if not text:
        return False
    if _VERIFY_TIMEOUT_PATTERNS[0].search(text) or _VERIFY_TIMEOUT_PATTERNS[1].search(text):
        return True
    return _contains_standalone_timeout_token(text)


def _contains_verify_failure_marker(text: str) -> bool:
    if not text:
        return False
    return any(pattern.search(text) for pattern in _VERIFY_FAILURE_PATTERNS) or _contains_structured_verify_failure_marker(
        text
    )


def _contains_explicit_verify_timeout_subject(text: str) -> bool:
    if not text:
        return False
    return bool(
        _VERIFY_TIMEOUT_PATTERNS[0].search(text)
        or _VERIFY_TIMEOUT_PATTERNS[1].search(text)
        or _contains_standalone_timeout_token(text)
    )


def _contains_standalone_timeout_token(text: str) -> bool:
    for match in _VERIFY_TIMEOUT_PATTERNS[2].finditer(text):
        if _timeout_token_is_flag_or_assignment(text, match.start(), match.end()):
            continue
        return True
    return False


def _timeout_token_is_flag_or_assignment(text: str, start: int, end: int) -> bool:
    if start > 0 and text[start - 1] == "-":
        return True

    next_index = end
    while next_index < len(text) and text[next_index].isspace():
        next_index += 1
    if next_index < len(text) and text[next_index] == "=":
        return True

    # Ignore diagnostic/package labels such as `timeout-2.4.0`.
    return next_index < len(text) and text[next_index] == "-" and (next_index + 1) < len(text) and text[
        next_index + 1
    ].isalnum()


def _contains_structured_verify_failure_marker(text: str) -> bool:
    if _VERIFY_FAILED_PHASE_PATTERN.search(text):
        return True
    return bool(
        _VERIFY_FAILED_STATUS_PATTERN.search(text)
        and _VERIFY_EXIT_STATUS_PATTERN.search(text)
        and _VERIFY_COMMAND_SUBJECT_PATTERN.search(text)
    )


def _extract_open_state_like_citation_tokens(text: str) -> tuple[str, ...]:
    if not text:
        return ()
    return tuple(match.group(0).strip("`") for match in _OPEN_STATE_CITATION_TOKEN_PATTERN.finditer(text))


def _citation_path_from_token(token: str) -> str:
    normalized = token.strip().strip("`")
    return re.sub(r":\d+(?:-\d+)?$", "", normalized)


def _citation_points_at_verify_harness(path: str) -> bool:
    normalized = path.strip().strip("`").lstrip("./")
    if not normalized:
        return False
    if normalized.startswith(("bin/", "scripts/", "tests/", "tests_functional/")):
        return True
    return normalized in {
        "gza.yaml",
        "gza.local.yaml",
        "pyproject.toml",
        "pytest.ini",
        "mypy.ini",
        "tox.ini",
    }


def _citation_points_at_product_code(path: str) -> bool:
    normalized = path.strip().strip("`").lstrip("./")
    if not normalized:
        return False
    if normalized.startswith("src/"):
        return True
    return not _citation_points_at_verify_harness(normalized)


def _iter_structured_field_citation_tokens(finding: ReviewFinding) -> tuple[str, ...]:
    structured_fields = (
        finding.evidence,
        finding.open_state_citation,
        finding.impact,
        finding.fix_or_followup,
        finding.tests,
    )
    tokens: list[str] = []
    for field in structured_fields:
        tokens.extend(_extract_open_state_like_citation_tokens(field or ""))
    return tuple(tokens)


def _structured_fields_have_product_code_citation(finding: ReviewFinding) -> bool:
    for token in _iter_structured_field_citation_tokens(finding):
        if _citation_points_at_product_code(_citation_path_from_token(token)):
            return True
    return False


def _has_explicit_verify_timeout_subject_in_structured_fields(finding: ReviewFinding) -> bool:
    subject_fields = (
        finding.title,
        finding.evidence,
        finding.impact,
        finding.fix_or_followup,
    )
    return any(_contains_explicit_verify_timeout_subject(field or "") for field in subject_fields)


def _strip_leading_title_markers(title: str) -> str:
    normalized = title.strip()
    while True:
        stripped = _TITLE_LEADING_MARKER_PATTERN.sub("", normalized, count=1)
        if stripped == normalized:
            return normalized
        normalized = stripped.strip()


def _title_names_verify_command_as_subject(title: str | None) -> bool:
    if not title:
        return False
    return bool(_VERIFY_COMMAND_SUBJECT_PATTERN.match(_strip_leading_title_markers(title)))


def _classify_blocker_finding(finding: ReviewFinding) -> str:
    text = "\n".join(
        part
        for part in (
            finding.title,
            finding.body,
            finding.evidence,
            finding.impact,
            finding.fix_or_followup,
            finding.tests,
        )
        if part
    )
    if _contains_verify_timeout_marker(text):
        # A blocker is only timeout-eligible when its title names verify_command as
        # the subject. A code-focused title with body-only mentions of a timeout
        # symptom (e.g. "Worker loop ... until verify_command timeout") describes a
        # code defect with a timeout symptom, not a verify_command failure.
        if _title_names_verify_command_as_subject(finding.title) and _has_explicit_verify_timeout_subject_in_structured_fields(
            finding
        ):
            if _structured_fields_have_product_code_citation(finding):
                return "code"
            return "verify_timeout"
    if _contains_verify_failure_marker(text):
        if _structured_fields_have_product_code_citation(finding):
            return "code"
        return "verify_failure"
    return "code"


def classify_review_blocker_finding(finding: ReviewFinding) -> str:
    """Classify a blocker as code, verify failure, or verify timeout."""
    return _classify_blocker_finding(finding)


def normalize_review_finding_title(title: str) -> str:
    """Normalize a review finding title for deterministic fingerprint matching."""
    normalized = re.sub(r"`+", "", title).strip().lower()
    normalized = re.sub(r"^#+\s*", "", normalized)
    normalized = re.sub(r"\s+", " ", normalized)
    normalized = re.sub(r"^(?:[a-z]+-)?b\d+\s*[:.)-]?\s*", "", normalized)
    return normalized.strip()


def normalize_review_finding_anchor(value: str) -> str:
    """Normalize a review finding anchor value for deterministic matching."""
    normalized = re.sub(r"`+", "", value).strip().lower()
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized


def get_review_finding_fingerprint_details(
    finding: ReviewFinding,
) -> tuple[tuple[str, str], str, str] | None:
    """Return normalized fingerprint plus display title/anchor for one finding."""
    title = normalize_review_finding_title(finding.title)
    if not title:
        return None

    raw_anchor: str | None = None
    if finding.open_state_citation:
        citation_tokens = [
            token.strip()
            for token in finding.open_state_citation.split(",")
            if token.strip()
        ]
        if citation_tokens:
            raw_anchor = citation_tokens[0]
    if raw_anchor is None and finding.fix_or_followup:
        raw_anchor = finding.fix_or_followup
    if raw_anchor is None:
        return None

    anchor = normalize_review_finding_anchor(raw_anchor)
    if not anchor:
        return None
    return (title, anchor), finding.title, anchor


def get_review_finding_fingerprint(finding: ReviewFinding) -> tuple[str, str] | None:
    """Return the normalized deterministic fingerprint for one review finding."""
    details = get_review_finding_fingerprint_details(finding)
    if details is None:
        return None
    return details[0]


def _is_timeout_only_raw_blocker_line(line: str) -> bool:
    stripped = line.strip()
    if not stripped or stripped in {"```", "```text"}:
        return True

    normalized = stripped.lstrip("-* ").strip()
    if _contains_verify_timeout_marker(normalized):
        return True

    if ":" in normalized:
        _, _, remainder = normalized.partition(":")
        remainder = remainder.strip()
        return bool(remainder) and _contains_verify_timeout_marker(remainder)

    return False


def _raw_verify_timeout_only_blocker(content: str, report: ParsedReviewReport) -> bool:
    if report.verdict != "CHANGES_REQUESTED" or report.findings:
        return False
    match = _BLOCKER_SECTION_PATTERN.search(content)
    if match is None:
        return False
    blocker_body = match.group(2).strip()
    if not blocker_body:
        return False
    if not _VERIFY_COMMAND_PATTERN.search(blocker_body):
        return False
    if not _contains_verify_timeout_marker(blocker_body):
        return False

    return all(_is_timeout_only_raw_blocker_line(line) for line in blocker_body.splitlines())


def summarize_review_blockers(content: str | None) -> ReviewBlockerSummary:
    """Classify review blockers conservatively for lifecycle decisions."""
    if not content:
        return ReviewBlockerSummary(0, 0, 0, 0)

    report = parse_review_report(content)
    if report.verdict != "CHANGES_REQUESTED":
        return ReviewBlockerSummary(0, 0, 0, 0)

    blockers = [finding for finding in report.findings if finding.severity == "BLOCKER"]
    if blockers:
        verify_timeout_count = 0
        verify_failure_count = 0
        unknown_or_code_count = 0
        for blocker in blockers:
            kind = _classify_blocker_finding(blocker)
            if kind == "verify_timeout":
                verify_timeout_count += 1
            elif kind == "verify_failure":
                verify_failure_count += 1
            else:
                unknown_or_code_count += 1
        return ReviewBlockerSummary(
            blocker_count=len(blockers),
            verify_timeout_count=verify_timeout_count,
            verify_failure_count=verify_failure_count,
            unknown_or_code_count=unknown_or_code_count,
        )

    if _raw_verify_timeout_only_blocker(content, report):
        return ReviewBlockerSummary(
            blocker_count=1,
            verify_timeout_count=1,
            verify_failure_count=0,
            unknown_or_code_count=0,
        )

    return ReviewBlockerSummary(0, 0, 0, 0)


def is_verify_timeout_only_review(content: str | None) -> bool:
    return summarize_review_blockers(content).is_verify_timeout_only


def is_verify_blocked_only_review(content: str | None) -> bool:
    return summarize_review_blockers(content).is_verify_blocked_only


def parse_review_verdict(content: str | None) -> str | None:
    """Extract a normalized review verdict from markdown content."""
    return parse_review_report(content).verdict


_OPEN_STATE_CITATION_TOKEN_PATTERN = re.compile(r"`?[^,\s`]+:\d+(?:-\d+)?`?")


def _has_valid_open_state_citation_shape(citation: str) -> bool:
    tokens = [token.strip() for token in citation.split(",") if token.strip()]
    if not tokens:
        return False
    return all(_OPEN_STATE_CITATION_TOKEN_PATTERN.fullmatch(token) for token in tokens)


def validate_review_report_contract(content: str | None) -> ReviewContractValidation:
    """Return non-fatal review contract warnings for blocker citation requirements."""
    report = parse_review_report(content)
    missing: list[str] = []
    malformed: list[str] = []
    for finding in report.findings:
        if finding.severity != "BLOCKER":
            continue
        citation = (finding.open_state_citation or "").strip()
        if not citation:
            missing.append(finding.id)
            continue
        if not _has_valid_open_state_citation_shape(citation):
            malformed.append(finding.id)
    return ReviewContractValidation(
        blockers_missing_open_state_citation=tuple(missing),
        blockers_with_malformed_open_state_citation=tuple(malformed),
    )


def summarize_review_report(report: ParsedReviewReport) -> ReviewOutcome:
    """Extract display/automation-friendly review outcome details."""
    return ReviewOutcome(
        verdict=report.verdict,
        followup_findings=tuple(
            finding for finding in report.findings if finding.severity == "FOLLOWUP"
        ),
    )


def format_review_outcome(outcome: ReviewOutcome, *, unknown_label: str = "UNKNOWN") -> str:
    """Format a concise review outcome string, including parsed follow-up IDs."""
    verdict = outcome.verdict or unknown_label
    if not outcome.followup_findings:
        return verdict
    finding_ids = ", ".join(finding.id for finding in outcome.followup_findings)
    return f"{verdict} [follow-ups: {finding_ids}]"


def get_review_content(project_dir: Path, review_task: Task) -> str | None:
    """Return the canonical raw review markdown from cached output or report file."""
    if review_task.output_content:
        return review_task.output_content

    if not review_task.report_file:
        return None

    review_path = project_dir / review_task.report_file
    if not review_path.exists():
        return None

    return review_path.read_text()


def get_review_report(project_dir: Path, review_task: Task) -> ParsedReviewReport:
    """Extract parsed review report from cached output or the report file."""
    content = get_review_content(project_dir, review_task)
    if content is None:
        return ParsedReviewReport(verdict=None, findings=(), format_version="unknown")
    return parse_review_report(content)


def get_review_verdict(project_dir: Path, review_task: Task) -> str | None:
    """Extract the review verdict from cached output or the report file."""
    return get_review_report(project_dir, review_task).verdict


def get_review_outcome(project_dir: Path, review_task: Task) -> ReviewOutcome:
    """Extract parsed review outcome details from cached output or the report file."""
    return summarize_review_report(get_review_report(project_dir, review_task))


def get_review_score(project_dir: Path, review_task: Task) -> int | None:
    """Compute a deterministic review score from cached output or the report file.

    Returns ``None`` when no score source exists (no output content and no readable report).
    """
    content = get_review_content(project_dir, review_task)
    if content is None:
        return None
    return compute_review_score(parse_review_template(content))


def get_backfillable_review_score(project_dir: Path, review_task: Task) -> int | None:
    """Return score for reviews whose content is parseable enough to backfill.

    Unlike ``get_review_score()``, this skips malformed legacy content instead of
    persisting a synthetic zero score for it.
    """
    content = get_review_content(project_dir, review_task)
    if content is None:
        return None
    return _compute_backfillable_review_score(content)
