"""Tests for shared review verdict parsing."""

from gza.review_verdict import parse_review_verdict


class TestParseReviewVerdict:
    def test_inline_bold_wrapped(self) -> None:
        assert parse_review_verdict("**Verdict: APPROVED**") == "APPROVED"

    def test_inline_bold_label_only(self) -> None:
        assert parse_review_verdict("**Verdict**: CHANGES_REQUESTED") == "CHANGES_REQUESTED"

    def test_heading_with_bold_verdict(self) -> None:
        content = "## Verdict\n\n**NEEDS_DISCUSSION**\n"
        assert parse_review_verdict(content) == "NEEDS_DISCUSSION"

    def test_heading_without_bold_verdict(self) -> None:
        content = "## Verdict\n\nCHANGES_REQUESTED\n"
        assert parse_review_verdict(content) == "CHANGES_REQUESTED"

    def test_no_verdict(self) -> None:
        assert parse_review_verdict("Review text with no decision") is None

    def test_none_content(self) -> None:
        assert parse_review_verdict(None) is None

    def test_canonical_structure_with_empty_must_fix_and_suggestions(self) -> None:
        content = (
            "## Summary\n\n"
            "- Looks good overall.\n\n"
            "## Must-Fix\n\n"
            "None.\n\n"
            "## Suggestions\n\n"
            "None.\n\n"
            "## Questions / Assumptions\n\n"
            "None.\n\n"
            "## Verdict\n\n"
            "No blockers found.\n"
            "Verdict: APPROVED\n"
        )
        assert parse_review_verdict(content) == "APPROVED"
