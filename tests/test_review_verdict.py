"""Tests for shared review verdict/report parsing."""

from gza.review_verdict import parse_review_report, parse_review_verdict


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
