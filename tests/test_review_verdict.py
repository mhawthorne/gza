"""Tests for shared review verdict/report parsing."""

from pathlib import Path

from gza.db import Task
from gza.review_verdict import (
    ParsedReview,
    compute_review_score,
    get_backfillable_review_score,
    get_review_finding_fingerprint,
    is_verify_blocked_only_review,
    is_verify_timeout_only_review,
    parse_disputed_blockers,
    parse_review_blocker_adjudication,
    parse_review_report,
    parse_review_template,
    parse_review_verdict,
    summarize_review_blockers,
    validate_review_report_contract,
)


def _gza_4927_review_report() -> str:
    return (
        "## Summary\n\n- Implementation is aligned; autonomous verify failed.\n\n"
        "## Blockers\n\n"
        "### B1 verify_command failure: targeted pytest failure under the harness\n"
        "Evidence: verify_command failed with exit status 1 while running "
        "`./bin/tests -x -o faulthandler_timeout=120`; the captured output also cited "
        "`pytest_timeout` and `autonomous_verify_timeout_seconds` while the failing phase reported an assertion.\n"
        "Impact: the branch cannot pass autonomous verification.\n"
        "Required fix: fix the failing test or command, then rerun verify_command from the current tip.\n"
        "Required tests: rerun `./bin/tests -x -o faulthandler_timeout=120`.\n\n"
        "## Follow-Ups\n\nNone.\n\n"
        "## Questions / Assumptions\n\nNone.\n\n"
        "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
    )


def _pytest_timeout_plugin_failure_review_report() -> str:
    return (
        "## Summary\n\n- Verify failed in the unit phase.\n\n"
        "## Blockers\n\n"
        "### B1 verify_command failure: unit pytest assertion failure\n"
        "Evidence: verify_command failed with exit status 1 while running `./bin/tests`; "
        "pytest reported `plugins: xdist-3.8.0, timeout-2.4.0` before the assertion failure.\n"
        "Impact: the branch cannot pass autonomous verification.\n"
        "Required fix: fix the failing assertion, then rerun verify_command from the current tip.\n"
        "Required tests: rerun `./bin/tests` and add a regression for the failing assertion path.\n\n"
        "## Follow-Ups\n\nNone.\n\n"
        "## Questions / Assumptions\n\nNone.\n\n"
        "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
    )


def _gza_5013_review_report() -> str:
    return (
        "## Summary\n\n- Review found no product-code defect; autonomous verify failed.\n\n"
        "## Blockers\n\n"
        "### B1 - verify_command failure: unit phase failed under `./bin/tests -x`\n"
        "Evidence: ## verify_command result\n"
        "- Command: `./bin/tests -x -o faulthandler_timeout=120`\n"
        "- Status: failed\n"
        "- Exit status: 1\n"
        "- Failure: verify_command failed during autonomous review\n"
        "\n"
        "Failing output (trimmed):\n"
        "```text\n"
        "gza-verify phase=failed name=unit duration_seconds=38.10\n"
        "FAILED tests/cli/test_watch.py::test_review_verify_reclassifies_flaky_failure\n"
        "=========================== short test summary info ============================\n"
        "FAILED tests/cli/test_watch.py::test_review_verify_reclassifies_flaky_failure\n"
        "============================== 1 failed in 38.10s ==============================\n"
        "```\n"
        "Impact: the branch cannot pass autonomous verification from the current tip.\n"
        "Required fix: rerun `./bin/tests -x -o faulthandler_timeout=120` from the current tip and only keep a blocker if it still fails.\n"
        "Required tests: rerun `./bin/tests -x -o faulthandler_timeout=120`.\n\n"
        "## Follow-Ups\n\nNone.\n\n"
        "## Questions / Assumptions\n\nNone.\n\n"
        "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
    )


def _gza_4983_review_report() -> str:
    return (
        "## Summary\n\n- Product review passed; verify gate failed once under pytest fail-fast.\n\n"
        "## Blockers\n\n"
        "### B1 1. verify_command failure: targeted pytest failed in review\n"
        "Evidence: ## verify_command result\n"
        "- Command: `./bin/tests -x -o faulthandler_timeout=120`\n"
        "- Status: failed\n"
        "- Exit status: 1\n"
        "\n"
        "Failing output (trimmed):\n"
        "```text\n"
        "gza-verify phase=failed name=unit duration_seconds=44.72\n"
        "FAILED tests/test_runner.py::test_noop_improve_verify_only_auto_clear\n"
        "=========================== short test summary info ============================\n"
        "FAILED tests/test_runner.py::test_noop_improve_verify_only_auto_clear\n"
        "============================== 1 failed in 44.72s ==============================\n"
        "```\n"
        "Impact: the branch cannot clear review while autonomous verification reports a failure.\n"
        "Required fix: rerun the same verify command from the current tip and keep the blocker only if it reproduces.\n"
        "Required tests: rerun `./bin/tests -x -o faulthandler_timeout=120`.\n\n"
        "## Follow-Ups\n\nNone.\n\n"
        "## Questions / Assumptions\n\nNone.\n\n"
        "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
    )


def _gza_4668_review_report() -> str:
    return (
        "## Summary\n\n- Review only found a failed verify gate.\n\n"
        "## Blockers\n\n"
        "### B1 - ./bin/tests failure: autonomous verify failed once in review\n"
        "Evidence: ## verify_command result\n"
        "- Command: `./bin/tests -x -o faulthandler_timeout=120`\n"
        "- Status: failed\n"
        "- Exit status: 1\n"
        "\n"
        "Failing output (trimmed):\n"
        "```text\n"
        "gza-verify phase=failed name=unit duration_seconds=17.43\n"
        "FAILED tests/test_review_verdict.py::test_verify_failure_is_reclassified\n"
        "=========================== short test summary info ============================\n"
        "FAILED tests/test_review_verdict.py::test_verify_failure_is_reclassified\n"
        "============================== 1 failed in 17.43s ==============================\n"
        "```\n"
        "Impact: `./bin/tests` did not pass in the autonomous review worktree.\n"
        "Required fix: rerun `./bin/tests -x -o faulthandler_timeout=120` from the current tip and only escalate to code if a concrete defect reproduces.\n"
        "Required tests: rerun `./bin/tests -x -o faulthandler_timeout=120`.\n\n"
        "## Follow-Ups\n\nNone.\n\n"
        "## Questions / Assumptions\n\nNone.\n\n"
        "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
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

    def test_final_verdict_section_overrides_quoted_approved_in_body(self) -> None:
        content = (
            "## Summary\n\n- Found a blocker.\n\n"
            "## Blockers\n\n"
            "### B1 Invalid manifest still passes\n"
            "Evidence: manifest validation misses malformed entries.\n"
            "Open-state citation: `src/gza/review_verdict.py:162`\n"
            "Impact: bad review metadata can merge.\n"
            "Required fix: reject invalid manifests before lifecycle uses them.\n"
            "Required tests: add coverage for a completed `plan_review` with `Verdict: APPROVED` and an invalid manifest.\n\n"
            "## Follow-Ups\n\nNone.\n\n"
            "## Verdict\n\n"
            "Verdict: CHANGES_REQUESTED\n"
        )
        assert parse_review_verdict(content) == "CHANGES_REQUESTED"

    def test_final_verdict_section_overrides_quoted_changes_requested_in_body(self) -> None:
        content = (
            "## Summary\n\n- Ready to merge.\n\n"
            "## Blockers\n\nNone.\n\n"
            "## Follow-Ups\n\n"
            "### F1 Add fixture docs\n"
            "Evidence: review fixtures are hard to scan.\n"
            "Impact: low-risk maintenance overhead.\n"
            "Recommended follow-up: document the fixture that previously showed `Verdict: CHANGES_REQUESTED` in prose.\n"
            "Recommended tests: none.\n\n"
            "## Verdict\n\n"
            "Verdict: APPROVED\n"
        )
        assert parse_review_verdict(content) == "APPROVED"


class TestParseReviewReport:
    def test_parses_new_blockers_and_followups(self) -> None:
        content = (
            "## Summary\n\n- Looks good.\n\n"
            "## Blockers\n\n"
            "### B1 API error handling\n"
            "Evidence: missing branch\n"
            "Open-state citation: `src/api.py:12-18`\n"
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
        assert blocker.open_state_citation == "`src/api.py:12-18`"
        assert blocker.fix_or_followup == "handle error path"
        assert followup.id == "F1"
        assert followup.severity == "FOLLOWUP"
        assert followup.fix_or_followup == "validate malformed optional values"

    def test_parses_verify_command_failure_blocker_as_standard_blocker(self) -> None:
        content = (
            "## Summary\n\n- Verify failed.\n\n"
            "## Blockers\n\n"
            "### B1 verify_command failure: mypy NameError in query output\n"
            "Evidence: ```text\nsrc/gza/cli/query.py:823: error: Name \"oops\" is not defined  [name-defined]\n```\n"
            "Open-state citation: `src/gza/cli/query.py:823`\n"
            "Impact: the configured verify_command fails, so the branch cannot pass autonomous review.\n"
            "Required fix: define the referenced name or remove the bad reference so mypy passes.\n"
            "Required tests: add a targeted regression that exercises the changed query path and keep mypy clean for this file.\n\n"
            "## Follow-Ups\n\nNone.\n\n"
            "## Questions / Assumptions\n\nNone.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )
        report = parse_review_report(content)
        assert report.verdict == "CHANGES_REQUESTED"
        assert report.format_version == "v2"
        assert len(report.findings) == 1
        blocker = report.findings[0]
        assert blocker.id == "B1"
        assert blocker.severity == "BLOCKER"
        assert blocker.title == "verify_command failure: mypy NameError in query output"
        assert "Name \"oops\" is not defined" in (blocker.evidence or "")
        assert blocker.open_state_citation == "`src/gza/cli/query.py:823`"
        assert blocker.fix_or_followup == "define the referenced name or remove the bad reference so mypy passes."

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


class TestDisputedBlockers:
    def test_parses_valid_disputed_blockers(self) -> None:
        content = (
            "## Summary\n\n- No code change was needed.\n\n"
            "## Disputed Blockers\n\n"
            "### D1\n"
            "Finding: B1\n"
            "Reason: already_satisfied\n"
            "Evidence: The guard is already present on the current branch tip.\n"
            "Current-state citation: `src/api.py:12-18`\n"
            "Scope citation: `docs/plan.md:44-49`\n"
            "Downstream task: gza-77\n"
        )

        parsed = parse_disputed_blockers(content)

        assert len(parsed) == 1
        dispute = parsed[0]
        assert dispute.finding_id == "B1"
        assert dispute.reason == "already_satisfied"
        assert dispute.evidence == "The guard is already present on the current branch tip."
        assert dispute.current_state_citation == "`src/api.py:12-18`"
        assert dispute.scope_citation == "`docs/plan.md:44-49`"
        assert dispute.downstream_task_id == "gza-77"

    def test_parses_disputed_blocker_finding_with_id_and_title(self) -> None:
        content = (
            "## Summary\n\n- No code change was needed.\n\n"
            "## Disputed Blockers\n\n"
            "### D1\n"
            "Finding: B1 Missing API guard\n"
            "Reason: already_satisfied\n"
            "Evidence: The guard is already present on the current branch tip.\n"
            "Current-state citation: `src/api.py:12-18`\n"
        )

        parsed = parse_disputed_blockers(content)

        assert len(parsed) == 1
        assert parsed[0].finding_id == "B1"

    def test_ignores_malformed_disputed_blockers(self) -> None:
        content = (
            "## Summary\n\n- No code change was needed.\n\n"
            "## Disputed Blockers\n\n"
            "### D1\n"
            "Finding: B1\n"
            "Reason: not_a_real_reason\n"
            "Evidence: The guard is already present.\n"
            "Current-state citation: src/api.py\n"
        )

        assert parse_disputed_blockers(content) == ()

    def test_ignores_disputed_blocker_missing_finding(self) -> None:
        content = (
            "## Summary\n\n- No code change was needed.\n\n"
            "## Disputed Blockers\n\n"
            "### D1\n"
            "Reason: already_satisfied\n"
            "Evidence: The guard is already present.\n"
            "Current-state citation: `src/api.py:12-18`\n"
        )

        assert parse_disputed_blockers(content) == ()

    def test_review_finding_fingerprint_prefers_open_state_citation(self) -> None:
        report = parse_review_report(
            "## Summary\n\n- Found a blocker.\n\n"
            "## Blockers\n\n"
            "### B1 API error handling\n"
            "Evidence: missing branch\n"
            "Open-state citation: `src/api.py:12-18`\n"
            "Impact: crashes\n"
            "Required fix: handle error path\n"
            "Required tests: add regression\n\n"
            "## Follow-Ups\n\nNone.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )

        assert get_review_finding_fingerprint(report.findings[0]) == ("api error handling", "src/api.py:12-18")

    def test_legacy_report_without_open_state_citation_still_parses(self) -> None:
        content = (
            "## Summary\n\n- Legacy format.\n\n"
            "## Must-Fix\n\n"
            "### M1 Missing guard\n"
            "Evidence: old format body\n"
            "Required fix: add missing guard\n\n"
            "## Suggestions\n\n"
            "### S1\n"
            "Suggestion: do another thing\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )
        report = parse_review_report(content)
        assert report.findings[0].open_state_citation is None


class TestReviewBlockerAdjudication:
    def test_parses_strict_single_token_verdict(self) -> None:
        parsed = parse_review_blocker_adjudication("INVALID\n")
        assert parsed is not None
        assert parsed.verdict == "INVALID"

    def test_parses_valid_verdict(self) -> None:
        parsed = parse_review_blocker_adjudication("VALID\n")
        assert parsed is not None
        assert parsed.verdict == "VALID"

    def test_parses_needs_human_verdict(self) -> None:
        parsed = parse_review_blocker_adjudication("NEEDS_HUMAN\n")
        assert parsed is not None
        assert parsed.verdict == "NEEDS_HUMAN"

    def test_rejects_extra_text(self) -> None:
        assert parse_review_blocker_adjudication("INVALID\nBecause the blocker is stale.\n") is None

    def test_report_uses_verdict_from_final_verdict_section_not_quoted_body_text(self) -> None:
        content = (
            "## Summary\n\n- Found a blocker.\n\n"
            "## Blockers\n\n"
            "### B1 Invalid manifest still passes\n"
            "Evidence: manifest validation misses malformed entries.\n"
            "Open-state citation: `src/gza/review_verdict.py:162`\n"
            "Impact: bad review metadata can merge.\n"
            "Required fix: reject invalid manifests before lifecycle uses them.\n"
            "Required tests: add coverage for a completed `plan_review` with `Verdict: APPROVED` and an invalid manifest.\n\n"
            "## Follow-Ups\n\nNone.\n\n"
            "## Verdict\n\n"
            "Verdict: CHANGES_REQUESTED\n"
        )
        report = parse_review_report(content)
        assert report.verdict == "CHANGES_REQUESTED"


class TestValidateReviewReportContract:
    def test_flags_missing_open_state_citations_for_blockers(self) -> None:
        content = (
            "## Summary\n\n- Looks good.\n\n"
            "## Blockers\n\n"
            "### B1 Missing citation\n"
            "Evidence: branch lacks guard\n"
            "Impact: crash\n"
            "Required fix: add guard\n"
            "Required tests: regression\n\n"
            "## Follow-Ups\n\nNone.\n\n"
            "## Questions / Assumptions\n\nNone.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )
        validation = validate_review_report_contract(content)
        assert validation.blockers_missing_open_state_citation == ("B1",)
        assert validation.blockers_with_malformed_open_state_citation == ()

    def test_accepts_path_line_and_path_range_citations(self) -> None:
        content = (
            "## Summary\n\n- Looks good.\n\n"
            "## Blockers\n\n"
            "### B1 Valid citations\n"
            "Evidence: issue still open\n"
            "Open-state citation: `src/cli.py:41`, src/runner.py:120-133\n"
            "Impact: crash\n"
            "Required fix: add guard\n"
            "Required tests: regression\n\n"
            "## Follow-Ups\n\nNone.\n\n"
            "## Questions / Assumptions\n\nNone.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )
        validation = validate_review_report_contract(content)
        assert validation.blockers_missing_open_state_citation == ()
        assert validation.blockers_with_malformed_open_state_citation == ()

    def test_flags_malformed_open_state_citations(self) -> None:
        content = (
            "## Summary\n\n- Looks good.\n\n"
            "## Blockers\n\n"
            "### B1 Bad citation\n"
            "Evidence: issue still open\n"
            "Open-state citation: src/runner.py\n"
            "Impact: crash\n"
            "Required fix: add guard\n"
            "Required tests: regression\n\n"
            "## Follow-Ups\n\nNone.\n\n"
            "## Questions / Assumptions\n\nNone.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )
        validation = validate_review_report_contract(content)
        assert validation.blockers_missing_open_state_citation == ()
        assert validation.blockers_with_malformed_open_state_citation == ("B1",)


class TestVerifyBlockedReviewClassification:
    def test_classifies_verify_timeout_blocker_when_structured_citation_points_at_verify_harness(
        self,
    ) -> None:
        content = (
            "## Summary\n\n- Verify timed out.\n\n"
            "## Blockers\n\n"
            "### B1 verify_command failure: full verification timed out\n"
            "Evidence: lifecycle verify timed out at `120s` while running `./bin/tests`.\n"
            "Open-state citation: `bin/tests:150-155`\n"
            "Impact: the branch cannot be verified autonomously.\n"
            "Required fix: investigate the test-performance regression or prove the timeout is environmental.\n"
            "Required tests: rerun the exact verify command and add a narrow regression if this branch caused the slowdown.\n\n"
            "## Follow-Ups\n\nNone.\n\n"
            "## Questions / Assumptions\n\nNone.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )

        summary = summarize_review_blockers(content)

        assert summary.blocker_count == 1
        assert summary.verify_timeout_count == 1
        assert summary.verify_failure_count == 0
        assert summary.unknown_or_code_count == 0
        assert is_verify_timeout_only_review(content) is True
        assert is_verify_blocked_only_review(content) is True

    def test_classifies_verify_timeout_blocker_when_structured_fields_cite_verify_harness(self) -> None:
        content = (
            "## Summary\n\n- Verify timed out.\n\n"
            "## Blockers\n\n"
            "### B1 verify_command failure: full verification timed out\n"
            "Evidence: lifecycle verify timed out at `120s` while running `./bin/tests`; the harness stalled near `bin/tests:150-155`.\n"
            "Open-state citation: `bin/tests:150-155`\n"
            "Impact: autonomous verification cannot finish while the suite stays over budget.\n"
            "Required fix: inspect the verify harness around `bin/tests:150-155` before changing product code.\n"
            "Required tests: rerun `./bin/tests` and capture whether the timeout reproduces from the current tip.\n\n"
            "## Follow-Ups\n\nNone.\n\n"
            "## Questions / Assumptions\n\nNone.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )

        summary = summarize_review_blockers(content)

        assert summary.blocker_count == 1
        assert summary.verify_timeout_count == 1
        assert summary.verify_failure_count == 0
        assert summary.unknown_or_code_count == 0
        assert is_verify_timeout_only_review(content) is True
        assert is_verify_blocked_only_review(content) is True

    def test_keeps_timeout_shaped_blocker_as_code_when_structured_fields_cite_src(self) -> None:
        content = (
            "## Summary\n\n- Timeout is a symptom of a real defect.\n\n"
            "## Blockers\n\n"
            "### B1 verify_command failure: full verification timed out\n"
            "Evidence: `src/gza/review_verdict.py:590-605` loops over every review finding until verify_command times out.\n"
            "Open-state citation: `src/gza/review_verdict.py:590-605`\n"
            "Impact: product code keeps the suite running past the verify budget.\n"
            "Required fix: stop the pathological loop in `src/gza/review_verdict.py:590-605` before rerunning verify_command.\n"
            "Required tests: add regression coverage for the looping branch and rerun verify_command.\n\n"
            "## Follow-Ups\n\nNone.\n\n"
            "## Questions / Assumptions\n\nNone.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )

        summary = summarize_review_blockers(content)

        assert summary.blocker_count == 1
        assert summary.verify_timeout_count == 0
        assert summary.verify_failure_count == 0
        assert summary.unknown_or_code_count == 1
        assert is_verify_timeout_only_review(content) is False
        assert is_verify_blocked_only_review(content) is False

    def test_keeps_timeout_shaped_blocker_as_code_when_only_open_state_citation_points_at_src(
        self,
    ) -> None:
        content = (
            "## Summary\n\n- Timeout is a symptom of a real defect.\n\n"
            "## Blockers\n\n"
            "### B1 verify_command failure: full verification timed out\n"
            "Evidence: verify_command timed out after 120s while running the configured suite.\n"
            "Open-state citation: `src/gza/runner.py:903`\n"
            "Impact: product code still leaves the review path unable to complete under the verify budget.\n"
            "Required fix: fix the cited product-code path before rerunning autonomous verification.\n"
            "Required tests: add targeted regression coverage for the runner path and rerun verify_command.\n\n"
            "## Follow-Ups\n\nNone.\n\n"
            "## Questions / Assumptions\n\nNone.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )

        summary = summarize_review_blockers(content)

        assert summary.blocker_count == 1
        assert summary.verify_timeout_count == 0
        assert summary.verify_failure_count == 0
        assert summary.unknown_or_code_count == 1
        assert is_verify_timeout_only_review(content) is False
        assert is_verify_blocked_only_review(content) is False

    def test_does_not_classify_mixed_timeout_and_code_review_as_timeout_only(self) -> None:
        content = (
            "## Summary\n\n- Mixed blockers.\n\n"
            "## Blockers\n\n"
            "### B1 verify_command failure: timed out during pytest\n"
            "Evidence: verify_command timed out after 120s while running the configured suite.\n"
            "Open-state citation: `bin/tests:150-155`\n"
            "Impact: branch cannot be verified.\n"
            "Required fix: investigate the slowdown.\n"
            "Required tests: rerun the suite.\n\n"
            "### B2 Missing input validation\n"
            "Evidence: request path still accepts malformed IDs.\n"
            "Open-state citation: `src/gza/api.py:14`\n"
            "Impact: malformed requests still crash.\n"
            "Required fix: validate IDs before parsing.\n"
            "Required tests: add malformed-ID regression coverage.\n\n"
            "## Follow-Ups\n\nNone.\n\n"
            "## Questions / Assumptions\n\nNone.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )

        summary = summarize_review_blockers(content)

        assert summary.blocker_count == 2
        assert summary.verify_timeout_count == 1
        assert summary.unknown_or_code_count == 1
        assert is_verify_timeout_only_review(content) is False
        assert is_verify_blocked_only_review(content) is False

    def test_keeps_non_timeout_verify_failure_with_concrete_src_defect_as_code(self) -> None:
        content = (
            "## Summary\n\n- Verify failed.\n\n"
            "## Blockers\n\n"
            "### B1 verify_command failure: mypy NameError in query output\n"
            "Evidence: src/gza/cli/query.py:823: error: Name \"oops\" is not defined.\n"
            "Impact: the configured verify_command fails, so the branch cannot pass autonomous review.\n"
            "Required fix: define the referenced name or remove the bad reference so mypy passes.\n"
            "Required tests: rerun mypy and add a targeted regression for the changed query path.\n\n"
            "## Follow-Ups\n\nNone.\n\n"
            "## Questions / Assumptions\n\nNone.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )

        summary = summarize_review_blockers(content)

        assert summary.blocker_count == 1
        assert summary.verify_timeout_count == 0
        assert summary.verify_failure_count == 0
        assert summary.unknown_or_code_count == 1
        assert is_verify_timeout_only_review(content) is False
        assert is_verify_blocked_only_review(content) is False

    def test_classifies_gza_4927_verify_failure_report_as_verify_blocked_not_timeout(self) -> None:
        content = _gza_4927_review_report()

        summary = summarize_review_blockers(content)

        assert summary.blocker_count == 1
        assert summary.verify_failure_count == 1
        assert summary.verify_timeout_count == 0
        assert summary.unknown_or_code_count == 0
        assert is_verify_blocked_only_review(content) is True
        assert is_verify_timeout_only_review(content) is False

    def test_classifies_real_verify_failure_only_reports_as_verify_blocked_only(self) -> None:
        for content in (
            _gza_5013_review_report(),
            _gza_4983_review_report(),
            _gza_4668_review_report(),
        ):
            summary = summarize_review_blockers(content)

            assert summary.blocker_count == 1
            assert summary.verify_failure_count == 1
            assert summary.verify_timeout_count == 0
            assert summary.unknown_or_code_count == 0
            assert is_verify_blocked_only_review(content) is True
            assert is_verify_timeout_only_review(content) is False

    def test_does_not_treat_timeout_identifiers_or_flags_as_verify_timeout_markers(self) -> None:
        content = (
            "## Summary\n\n- Verify failed.\n\n"
            "## Blockers\n\n"
            "### B1 verify_command failure: command exited nonzero\n"
            "Evidence: verify_command failed while running "
            "`pytest -o faulthandler_timeout=120 --timeout=30`; the output also mentioned "
            "`pytest_timeout` and `autonomous_verify_timeout_seconds` before the assertion failure.\n"
            "Impact: the branch cannot pass autonomous verification.\n"
            "Required fix: fix the assertion failure and rerun verify_command.\n"
            "Required tests: rerun `pytest -o faulthandler_timeout=120 --timeout=30`.\n\n"
            "## Follow-Ups\n\nNone.\n\n"
            "## Questions / Assumptions\n\nNone.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )

        summary = summarize_review_blockers(content)

        assert summary.blocker_count == 1
        assert summary.verify_failure_count == 1
        assert summary.verify_timeout_count == 0
        assert summary.unknown_or_code_count == 0
        assert is_verify_blocked_only_review(content) is True
        assert is_verify_timeout_only_review(content) is False

    def test_keeps_verify_failure_plus_concrete_product_defect_as_code(self) -> None:
        content = (
            "## Summary\n\n- Verify failed because of a concrete defect.\n\n"
            "## Blockers\n\n"
            "### B1 - verify_command failure: NameError in review verdict classifier\n"
            "Evidence: ## verify_command result\n"
            "- Command: `./bin/tests -x`\n"
            "- Status: failed\n"
            "- Exit status: 1\n"
            "```text\n"
            "FAILED tests/test_review_verdict.py::test_classifier_handles_product_bug\n"
            "src/gza/review_verdict.py:663: NameError: name 'subject' is not defined\n"
            "============================== 1 failed in 3.21s ==============================\n"
            "```\n"
            "Open-state citation: `src/gza/review_verdict.py:663`\n"
            "Impact: the classifier crashes before autonomous verification can finish.\n"
            "Required fix: define the missing name in `src/gza/review_verdict.py:663` before rerunning verify_command.\n"
            "Required tests: add a regression for the crashing classifier path and rerun `./bin/tests -x`.\n\n"
            "## Follow-Ups\n\nNone.\n\n"
            "## Questions / Assumptions\n\nNone.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )

        summary = summarize_review_blockers(content)

        assert summary.blocker_count == 1
        assert summary.verify_failure_count == 0
        assert summary.verify_timeout_count == 0
        assert summary.unknown_or_code_count == 1
        assert is_verify_blocked_only_review(content) is False
        assert is_verify_timeout_only_review(content) is False

    def test_does_not_treat_pytest_timeout_plugin_version_as_verify_timeout_marker(self) -> None:
        content = _pytest_timeout_plugin_failure_review_report()

        summary = summarize_review_blockers(content)

        assert summary.blocker_count == 1
        assert summary.verify_failure_count == 1
        assert summary.verify_timeout_count == 0
        assert summary.unknown_or_code_count == 0
        assert is_verify_blocked_only_review(content) is True
        assert is_verify_timeout_only_review(content) is False

    def test_preserves_verify_failure_when_required_tests_mentions_standalone_timeout_budget(
        self,
    ) -> None:
        content = (
            "## Summary\n\n- Verify failed in the unit phase.\n\n"
            "## Blockers\n\n"
            "### B1 verify_command failure: unit pytest assertion failure\n"
            "Evidence: verify_command failed with exit status 1 while running `./bin/tests`.\n"
            "Impact: the branch cannot pass autonomous verification.\n"
            "Required fix: fix the failing assertion, then rerun verify_command from the current tip.\n"
            "Required tests: rerun `timeout 120 ./bin/tests` and add a regression for the failing assertion path.\n\n"
            "## Follow-Ups\n\nNone.\n\n"
            "## Questions / Assumptions\n\nNone.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )

        summary = summarize_review_blockers(content)

        assert summary.blocker_count == 1
        assert summary.verify_failure_count == 1
        assert summary.verify_timeout_count == 0
        assert summary.unknown_or_code_count == 0
        assert is_verify_blocked_only_review(content) is True
        assert is_verify_timeout_only_review(content) is False

    def test_does_not_classify_code_blocker_with_generic_verify_command_rerun_as_verify_failure(
        self,
    ) -> None:
        content = (
            "## Summary\n\n- Validation missing.\n\n"
            "## Blockers\n\n"
            "### B1 Missing input validation\n"
            "Evidence: request path still accepts malformed IDs.\n"
            "Open-state citation: `src/gza/api.py:14`\n"
            "Impact: malformed requests still crash.\n"
            "Required fix: validate IDs before parsing.\n"
            "Required tests: add malformed-ID regression coverage and rerun verify_command.\n\n"
            "## Follow-Ups\n\nNone.\n\n"
            "## Questions / Assumptions\n\nNone.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )

        summary = summarize_review_blockers(content)

        assert summary.blocker_count == 1
        assert summary.verify_timeout_count == 0
        assert summary.verify_failure_count == 0
        assert summary.unknown_or_code_count == 1
        assert is_verify_timeout_only_review(content) is False
        assert is_verify_blocked_only_review(content) is False

    def test_does_not_classify_code_focused_title_with_timeout_body_and_open_state_citation_as_timeout_only(
        self,
    ) -> None:
        content = (
            "## Summary\n\n- Worker loop bug surfaces as a verify timeout.\n\n"
            "## Blockers\n\n"
            "### B1 Worker loop leaves mocked task incomplete until verify_command timeout\n"
            "Evidence: the worker loop keeps spinning until verify_command timed out after 120s.\n"
            "Open-state citation: `tests/cli/test_execution.py:7214`\n"
            "Impact: the task never completes and the suite cannot pass.\n"
            "Required fix: exit the worker loop when the mocked task reaches its terminal state.\n"
            "Required tests: add a worker-loop regression that asserts the task completes well before the configured verify_command timeout.\n\n"
            "## Follow-Ups\n\nNone.\n\n"
            "## Questions / Assumptions\n\nNone.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )

        summary = summarize_review_blockers(content)

        assert summary.blocker_count == 1
        assert summary.verify_timeout_count == 0
        assert summary.verify_failure_count == 0
        assert summary.unknown_or_code_count == 1
        assert is_verify_timeout_only_review(content) is False
        assert is_verify_blocked_only_review(content) is False

    def test_classifies_structured_timeout_only_review_when_timeout_marker_is_only_in_evidence(
        self,
    ) -> None:
        content = (
            "## Summary\n\n- Verify timed out.\n\n"
            "## Blockers\n\n"
            "### B1 verify_command failure\n"
            "Evidence: Failure: verify_command timed out after 120s while running the configured suite.\n"
            "Open-state citation: `gza.yaml:5`\n"
            "Impact: the branch cannot be considered verified.\n"
            "Required fix: investigate the test-performance regression.\n"
            "Required tests: rerun the exact configured verify_command after narrowing the slowdown.\n\n"
            "## Follow-Ups\n\nNone.\n\n"
            "## Questions / Assumptions\n\nNone.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )

        summary = summarize_review_blockers(content)

        assert summary.blocker_count == 1
        assert summary.verify_timeout_count == 1
        assert summary.verify_failure_count == 0
        assert summary.unknown_or_code_count == 0
        assert is_verify_timeout_only_review(content) is True
        assert is_verify_blocked_only_review(content) is True

    def test_does_not_classify_structured_code_blocker_with_timeout_evidence_as_timeout_only(
        self,
    ) -> None:
        content = (
            "## Summary\n\n- Validation missing and verify rerun timed out.\n\n"
            "## Blockers\n\n"
            "### B1 Missing input validation\n"
            "Evidence: request path still accepts malformed IDs.\n"
            "Open-state citation: `src/gza/api.py:14`\n"
            "Impact: malformed requests still crash.\n"
            "Required fix: validate IDs before parsing.\n"
            "Required tests: add malformed-ID regression coverage, then rerun the exact verify command because "
            "verify_command timed out after 120s during review.\n\n"
            "## Follow-Ups\n\nNone.\n\n"
            "## Questions / Assumptions\n\nNone.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )

        summary = summarize_review_blockers(content)

        assert summary.blocker_count == 1
        assert summary.verify_timeout_count == 0
        assert summary.verify_failure_count == 0
        assert summary.unknown_or_code_count == 1
        assert is_verify_timeout_only_review(content) is False
        assert is_verify_blocked_only_review(content) is False

    def test_classifies_timeout_only_review_from_unstructured_blocker_section(self) -> None:
        content = (
            "## Summary\n\n- Verify timed out.\n\n"
            "## Blockers\n\n"
            "- verify_command timed out after 120s\n"
            "- Exit status: timed out\n\n"
            "## Follow-Ups\n\nNone.\n\n"
            "## Questions / Assumptions\n\nNone.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )

        summary = summarize_review_blockers(content)

        assert summary.blocker_count == 1
        assert summary.verify_timeout_count == 1
        assert summary.verify_failure_count == 0
        assert summary.unknown_or_code_count == 0
        assert is_verify_timeout_only_review(content) is True

    def test_does_not_classify_raw_generic_timeout_blocker_as_verify_timeout_only(self) -> None:
        content = (
            "## Summary\n\n- Product bug.\n\n"
            "## Blockers\n\n"
            "- Request timeout when loading the dashboard.\n\n"
            "## Follow-Ups\n\nNone.\n\n"
            "## Questions / Assumptions\n\nNone.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )

        summary = summarize_review_blockers(content)

        assert summary.blocker_count == 0
        assert summary.verify_timeout_count == 0
        assert summary.verify_failure_count == 0
        assert summary.unknown_or_code_count == 0
        assert is_verify_timeout_only_review(content) is False
        assert is_verify_blocked_only_review(content) is False

    def test_keeps_genuine_verify_timeout_detection_for_standalone_timeout_and_exit_status(self) -> None:
        content = (
            "## Summary\n\n- Verify timed out.\n\n"
            "## Blockers\n\n"
            "### B1 verify_command failure: timeout\n"
            "Evidence: verify_command timeout on the current tip.\n"
            "Impact: autonomous verification cannot complete.\n"
            "Required fix: investigate the timeout before rerunning verify_command.\n"
            "Required tests: rerun verify_command.\n\n"
            "### B2 verify_command failure: timed out during full verify\n"
            "Evidence: Exit status: timed out.\n"
            "Impact: autonomous verification cannot complete.\n"
            "Required fix: investigate the timeout before rerunning verify_command.\n"
            "Required tests: rerun verify_command.\n\n"
            "## Follow-Ups\n\nNone.\n\n"
            "## Questions / Assumptions\n\nNone.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )

        summary = summarize_review_blockers(content)

        assert summary.blocker_count == 2
        assert summary.verify_timeout_count == 2
        assert summary.verify_failure_count == 0
        assert summary.unknown_or_code_count == 0
        assert is_verify_timeout_only_review(content) is True
        assert is_verify_blocked_only_review(content) is True

    def test_does_not_classify_unstructured_mixed_blocker_section_as_timeout_only(self) -> None:
        content = (
            "## Summary\n\n- Mixed blockers.\n\n"
            "## Blockers\n\n"
            "- verify_command timed out after 120s\n"
            "- Missing validation still crashes malformed IDs\n\n"
            "## Follow-Ups\n\nNone.\n\n"
            "## Questions / Assumptions\n\nNone.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )

        summary = summarize_review_blockers(content)

        assert summary.blocker_count == 0
        assert summary.verify_timeout_count == 0
        assert summary.verify_failure_count == 0
        assert summary.unknown_or_code_count == 0
        assert is_verify_timeout_only_review(content) is False
        assert is_verify_blocked_only_review(content) is False


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

    def test_quoted_body_verdict_does_not_override_final_verdict_section(self) -> None:
        parsed = parse_review_template(
            _template_review_v2(
                blockers=(
                    "### B1 Invalid manifest still passes\n"
                    "Required fix: reject invalid manifests before lifecycle uses them.\n"
                    "Required tests: add coverage for a completed `plan_review` with `Verdict: APPROVED` and an invalid manifest."
                ),
                verdict="Verdict: CHANGES_REQUESTED",
            )
        )
        assert parsed.unparseable is False
        assert parsed.verdict == "CHANGES_REQUESTED"

    def test_conflicting_verdicts_in_verdict_section_mark_template_unparseable(self) -> None:
        parsed = parse_review_template(
            _template_review_v2(
                verdict="Verdict: APPROVED\nVerdict: CHANGES_REQUESTED",
            )
        )
        assert parsed.unparseable is True
        assert parsed.parse_error == "multiple"
        assert parsed.verdict is None


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
