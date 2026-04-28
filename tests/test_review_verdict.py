"""Tests for shared review verdict/report parsing."""

from pathlib import Path

from gza.db import Task
from gza.review_verdict import (
    ParsedReview,
    compute_review_score,
    get_backfillable_review_score,
    parse_review_report,
    parse_review_template,
    parse_review_verdict,
)


class TestParseReviewVerdict:
    def test_inline_bold_wrapped(self) -> None:
        assert parse_review_verdict("**Verdict: APPROVED**") == "APPROVED"

    def test_inline_bold_label_only(self) -> None:
        assert parse_review_verdict("**Verdict**: CHANGES_REQUESTED") == "CHANGES_REQUESTED"

    def test_heading_with_bold_verdict(self) -> None:
        content = "## Verdict\n\n**NEEDS_DISCUSSION**\n"
        assert parse_review_verdict(content) == "NEEDS_DISCUSSION"

    def test_heading_with_approved_with_followups(self) -> None:
        content = "## Verdict\n\nAPPROVED_WITH_FOLLOWUPS\n"
        assert parse_review_verdict(content) == "APPROVED_WITH_FOLLOWUPS"

    def test_no_verdict(self) -> None:
        assert parse_review_verdict("Review text with no decision") is None

    def test_none_content(self) -> None:
        assert parse_review_verdict(None) is None


class TestParseReviewReport:
    def test_parses_new_blockers_and_followups(self) -> None:
        content = (
            "## Summary\n\n- Looks good.\n\n"
            "## Blockers\n\n"
            "### B1 API error handling\n"
            "Evidence: missing branch\n"
            "Impact: crashes\n"
            "Required fix: handle error path\n"
            "Required tests: add regression\n\n"
            "## Follow-Ups\n\n"
            "### F1 Tighten malformed input checks\n"
            "Evidence: optional field assumptions\n"
            "Impact: low risk hardening\n"
            "Recommended follow-up: validate malformed optional values\n"
            "Recommended tests: malformed-input case\n\n"
            "## Questions / Assumptions\n\nNone.\n\n"
            "## Verdict\n\nVerdict: APPROVED_WITH_FOLLOWUPS\n"
        )
        report = parse_review_report(content)
        assert report.verdict == "APPROVED_WITH_FOLLOWUPS"
        assert report.format_version == "v2"
        assert len(report.findings) == 2
        blocker = report.findings[0]
        followup = report.findings[1]
        assert blocker.id == "B1"
        assert blocker.severity == "BLOCKER"
        assert blocker.fix_or_followup == "handle error path"
        assert followup.id == "F1"
        assert followup.severity == "FOLLOWUP"
        assert followup.fix_or_followup == "validate malformed optional values"

    def test_legacy_suggestions_not_promoted_to_followups(self) -> None:
        content = (
            "## Summary\n\n- Legacy format.\n\n"
            "## Must-Fix\n\n"
            "### M1\n"
            "Required fix: do the thing\n\n"
            "## Suggestions\n\n"
            "### S1\n"
            "Suggestion: do another thing\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )
        report = parse_review_report(content)
        assert report.verdict == "CHANGES_REQUESTED"
        assert report.format_version == "legacy"
        assert len(report.findings) == 1
        assert report.findings[0].severity == "BLOCKER"


def _template_review(
    *,
    checklist: list[str] | None = None,
    must_fix: str = "None.",
    suggestions: str = "None.",
    verdict: str = "Verdict: APPROVED",
) -> str:
    checklist_lines = checklist or [
        "- Yes - Requirement 1",
        "- Yes - Requirement 2",
        "- Yes - Requirement 3",
        "- Yes - Requirement 4",
        "- Yes - Requirement 5",
    ]
    return (
        "## Summary\n\n"
        + "\n".join(checklist_lines)
        + "\n\n## Must-Fix\n\n"
        + must_fix
        + "\n\n## Suggestions\n\n"
        + suggestions
        + "\n\n## Questions / Assumptions\n\nNone.\n\n## Verdict\n\n"
        + verdict
        + "\n"
    )


def _template_review_v2(
    *,
    checklist: list[str] | None = None,
    blockers: str = "None.",
    followups: str = "None.",
    verdict: str = "Verdict: APPROVED",
) -> str:
    checklist_lines = checklist or [
        "- Yes - Requirement 1",
        "- Yes - Requirement 2",
        "- Yes - Requirement 3",
        "- Yes - Requirement 4",
        "- Yes - Requirement 5",
    ]
    return (
        "## Summary\n\n"
        + "\n".join(checklist_lines)
        + "\n\n## Blockers\n\n"
        + blockers
        + "\n\n## Follow-Ups\n\n"
        + followups
        + "\n\n## Questions / Assumptions\n\nNone.\n\n## Verdict\n\n"
        + verdict
        + "\n"
    )


class TestParseReviewTemplate:
    def test_parses_happy_path(self) -> None:
        parsed = parse_review_template(_template_review())
        assert parsed.must_fix_count == 0
        assert parsed.suggestion_count == 0
        assert len(parsed.summary_checklist) == 5
        assert parsed.verdict == "APPROVED"
        assert parsed.unparseable is False

    def test_handles_none_without_trailing_period(self) -> None:
        parsed = parse_review_template(_template_review(must_fix="None", suggestions="None"))
        assert parsed.must_fix_count == 0
        assert parsed.suggestion_count == 0
        assert parsed.unparseable is False

    def test_handles_mis_cased_yes_no_and_whitespace(self) -> None:
        parsed = parse_review_template(
            _template_review(
                checklist=[
                    " - yEs - item one  ",
                    " - nO - item two  ",
                ]
            )
        )
        assert parsed.summary_checklist[0] == ("item one", True)
        assert parsed.summary_checklist[1] == ("item two", False)

    def test_missing_section_marks_unparseable(self) -> None:
        parsed = parse_review_template(
            "## Summary\n\n- Yes - ok\n\n## Must-Fix\n\nNone.\n\n## Verdict\n\nVerdict: APPROVED\n"
        )
        assert parsed.unparseable is True

    def test_missing_verdict_marks_unparseable_but_keeps_counts(self) -> None:
        parsed = parse_review_template(
            _template_review(
                must_fix="### M1 Missing check\nRequired fix: add it",
                verdict="No verdict line here",
            )
        )
        assert parsed.unparseable is True
        assert parsed.parse_error == "missing_verdict"
        assert parsed.verdict is None
        assert parsed.must_fix_count == 1

    def test_parses_current_blockers_and_followups_template(self) -> None:
        parsed = parse_review_template(
            _template_review_v2(
                checklist=["- Yes - looks good", "- No - missing edge case"],
                blockers="### B1 Handle empty input\nRequired fix: guard early return",
                followups="### F1 Improve docs\nRecommended follow-up: add usage example",
                verdict="Verdict: CHANGES_REQUESTED",
            )
        )
        assert parsed.unparseable is False
        assert parsed.must_fix_count == 1
        assert parsed.suggestion_count == 1
        assert parsed.summary_checklist == (("looks good", True), ("missing edge case", False))
        assert parsed.verdict == "CHANGES_REQUESTED"


class TestComputeReviewScore:
    def test_clean_approved_scores_100(self) -> None:
        parsed = parse_review_template(_template_review())
        assert compute_review_score(parsed) == 100

    def test_must_fix_penalties_and_clamp(self) -> None:
        one = ParsedReview(must_fix_count=1, suggestion_count=0, summary_checklist=(), verdict="CHANGES_REQUESTED", unparseable=False)
        five = ParsedReview(must_fix_count=5, suggestion_count=0, summary_checklist=(), verdict="CHANGES_REQUESTED", unparseable=False)
        six = ParsedReview(must_fix_count=6, suggestion_count=0, summary_checklist=(), verdict="CHANGES_REQUESTED", unparseable=False)
        assert compute_review_score(one) == 80
        assert compute_review_score(five) == 0
        assert compute_review_score(six) == 0

    def test_suggestion_penalties(self) -> None:
        three = ParsedReview(must_fix_count=0, suggestion_count=3, summary_checklist=(), verdict="APPROVED", unparseable=False)
        ten = ParsedReview(must_fix_count=0, suggestion_count=10, summary_checklist=(), verdict="APPROVED", unparseable=False)
        assert compute_review_score(three) == 91
        assert compute_review_score(ten) == 70

    def test_mixed_penalties(self) -> None:
        parsed = ParsedReview(
            must_fix_count=2,
            suggestion_count=4,
            summary_checklist=(("Checklist item", False),),
            verdict="CHANGES_REQUESTED",
            unparseable=False,
        )
        assert compute_review_score(parsed) == 38

    def test_unparseable_review_without_signals_scores_zero(self) -> None:
        parsed = parse_review_template("this is garbage")
        assert parsed.unparseable is True
        assert compute_review_score(parsed) == 0

    def test_malformed_must_fix_body_scores_zero(self) -> None:
        parsed = parse_review_template(
            _template_review(
                must_fix="- broken freeform content without expected H3 entries",
                suggestions="None.",
                verdict="Verdict: APPROVED",
            )
        )
        assert parsed.unparseable is True
        assert parsed.parse_error == "malformed_must_fix_section"
        assert compute_review_score(parsed) == 0

    def test_malformed_suggestions_body_scores_zero(self) -> None:
        parsed = parse_review_template(
            _template_review(
                must_fix="None.",
                suggestions="- broken freeform suggestion content",
                verdict="Verdict: APPROVED",
            )
        )
        assert parsed.unparseable is True
        assert parsed.parse_error == "malformed_suggestions_section"
        assert compute_review_score(parsed) == 0

    def test_malformed_checklist_list_markers_without_yes_no_scores_zero(self) -> None:
        parsed = parse_review_template(
            _template_review(
                checklist=[
                    "- maybe - unclear checklist item one",
                    "- pending - unclear checklist item two",
                ],
                must_fix="None.",
                suggestions="None.",
                verdict="Verdict: CHANGES_REQUESTED",
            )
        )
        assert parsed.unparseable is True
        assert parsed.parse_error == "malformed_checklist"
        assert compute_review_score(parsed) == 0

    def test_missing_verdict_still_scores_from_parsed_fields(self) -> None:
        parsed = parse_review_template(
            _template_review(
                checklist=["- No - one missing"],
                suggestions="### S1 Follow-up\nSuggestion: update docs",
                verdict="No final verdict section",
            )
        )
        assert parsed.unparseable is True
        assert parsed.parse_error == "missing_verdict"
        assert compute_review_score(parsed) == 87

    def test_current_template_counts_contribute_to_score(self) -> None:
        parsed = parse_review_template(
            _template_review_v2(
                checklist=["- Yes - requirement 1", "- No - requirement 2"],
                blockers="### B1 Add guard\nRequired fix: check for None input",
                followups="### F1 Improve message\nRecommended follow-up: clarify operator hint",
                verdict="Verdict: CHANGES_REQUESTED",
            )
        )
        assert parsed.unparseable is False
        assert compute_review_score(parsed) == 67


class TestGetBackfillableReviewScore:
    def test_parseable_output_content_backfills(self, tmp_path: Path) -> None:
        review = Task(
            id="gza-1",
            prompt="review",
            status="completed",
            task_type="review",
            output_content=_template_review_v2(
                checklist=["- Yes - requirement 1", "- No - requirement 2"],
                blockers="### B1 Add guard\nRequired fix: check for None input",
                followups="### F1 Improve message\nRecommended follow-up: clarify operator hint",
                verdict="Verdict: CHANGES_REQUESTED",
            ),
        )
        assert get_backfillable_review_score(tmp_path, review) == 67

    def test_parseable_report_file_backfills(self, tmp_path: Path) -> None:
        review_path = tmp_path / ".gza" / "reports" / "review.md"
        review_path.parent.mkdir(parents=True, exist_ok=True)
        review_path.write_text(_template_review())
        review = Task(
            id="gza-2",
            prompt="review",
            status="completed",
            task_type="review",
            report_file=".gza/reports/review.md",
        )
        assert get_backfillable_review_score(tmp_path, review) == 100

    def test_malformed_review_is_not_backfilled(self, tmp_path: Path) -> None:
        review = Task(
            id="gza-3",
            prompt="review",
            status="completed",
            task_type="review",
            output_content=_template_review(
                must_fix="- broken freeform content without expected H3 entries",
                suggestions="None.",
                verdict="Verdict: APPROVED",
            ),
        )
        assert get_backfillable_review_score(tmp_path, review) is None

    def test_missing_verdict_with_structured_signals_is_backfillable(self, tmp_path: Path) -> None:
        review = Task(
            id="gza-4",
            prompt="review",
            status="completed",
            task_type="review",
            output_content=_template_review(
                checklist=["- No - one missing"],
                suggestions="### S1 Follow-up\nSuggestion: update docs",
                verdict="No final verdict section",
            ),
        )
        assert get_backfillable_review_score(tmp_path, review) == 87
