"""Tests for the PromptBuilder class in gza.prompts."""

import re
from pathlib import Path
from unittest.mock import Mock

from gza.config import Config
from gza.db import SqliteTaskStore
from gza.prompts import PromptBuilder

REVIEW_CONTRACT_PARITY_CLAUSES = [
    "The provided diff is authoritative - do not use git commands to reconstruct, re-derive, or expand it.",
    "Start with a repo-rules/learnings pass: compare the diff and behavior against AGENTS.md, REVIEW.md, project docs, and `.gza/learnings.md`; call out violations or regressions explicitly.",
    "Reserve Must-Fix for: correctness defects, behavior regressions, repository/rules violations, missing observability for user/agent-visible fallbacks, and misleading output/contradictory signals.",
    "Treat unexplained deviations from the provided plan or request as Must-Fix.",
    "Treat silent broad-exception fallbacks as Must-Fix when they can alter user/agent-visible state without clear warning/error surfacing.",
    "Treat misleading output (UI/prompt/context contradictions) as Must-Fix when it can cause incorrect operator or agent decisions.",
    "If config/CLI/operator-facing behavior changed, missing or incorrect docs/help/release-note updates are Must-Fix when they can mislead operators.",
    "Push style, cleanup, and non-risky refactors to Suggestions.",
    "For each blocker, give a clear closure condition so an improve task can resolve all blockers in one pass.",
]

REVIEW_SUMMARY_CHECKLIST_COUNT = 6
REVIEW_SUMMARY_CHECKLIST_ITEMS = [
    "Did I check the diff against AGENTS.md and `.gza/learnings.md` and flag any violations/regressions?",
    "Did I check for silent broad-exception fallbacks that mask errors while changing user/agent-visible state?",
    "Did I check for misleading output (contradictory UI/prompt/context signals)?",
    "Was an `## Original plan:` or `## Original request:` section provided, and did I verify ask-adherence (plan decisions reflected in the diff, or request coverage) while calling out intentional deviations? If neither was provided, did I state \"No plan or request provided.\"?",
    "Did I require targeted regression tests that match each failure mode (not generic \"add tests\")?",
    "If config, CLI, or operator-facing behavior changed, did I verify docs/help/release-note impact?",
]


def _assert_contains_all_clauses(text: str, clauses: list[str]) -> None:
    for clause in clauses:
        assert clause in text


def _assert_summary_checklist_contract(text: str) -> None:
    assert f"exactly {REVIEW_SUMMARY_CHECKLIST_COUNT} bullets" in text
    for item in REVIEW_SUMMARY_CHECKLIST_ITEMS:
        assert item in text


class TestPromptBuilderBuild:
    """Tests for PromptBuilder.build()."""

    def test_build_base_prompt(self, tmp_path: Path):
        """Test that build() includes the task prompt in the output."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Do something useful", task_type="implement")

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        result = PromptBuilder().build(task, config, store)
        assert "Complete this task: Do something useful" in result

    def test_build_task_type_with_summary(self, tmp_path: Path):
        """Test that task type includes summary instructions."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Implement feature X", task_type="implement")

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        summary_path = Path("/workspace/.gza/summaries/test.md")
        result = PromptBuilder().build(task, config, store, summary_path=summary_path)

        assert str(summary_path) in result
        assert "What was accomplished" in result
        assert "Files changed" in result
        assert "Re-read AGENTS.md for repository-specific rules and conventions." in result
        assert "Add or update targeted tests for each changed behavior" in result
        assert "Inspect the final diff for accidental scope creep" in result

    def test_build_task_type_without_summary(self, tmp_path: Path):
        """Test that task type without summary includes fallback message."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Implement feature Z", task_type="implement")

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        result = PromptBuilder().build(task, config, store, summary_path=None)

        assert "report what you accomplished" in result
        assert "write a summary" not in result.lower()
        assert "Re-read AGENTS.md for repository-specific rules and conventions." in result
        assert ".gza/learnings.md" not in result
        assert "Add or update targeted tests for each changed behavior" in result
        assert "Inspect the final diff for accidental scope creep" in result

    def test_build_implement_type_with_summary(self, tmp_path: Path):
        """Test that implement type includes summary instructions."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Implement feature Y", task_type="implement")

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        summary_path = Path("/workspace/.gza/summaries/test.md")
        result = PromptBuilder().build(task, config, store, summary_path=summary_path)

        assert str(summary_path) in result
        assert "write a summary" in result.lower()
        assert "update the relevant help text or documentation" in result

    def test_build_improve_type_with_summary(self, tmp_path: Path):
        """Test that improve type includes summary instructions."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Improve the code", task_type="improve")

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        summary_path = Path("/workspace/.gza/summaries/improve-test.md")
        result = PromptBuilder().build(task, config, store, summary_path=summary_path)

        assert str(summary_path) in result
        assert "Treat **Suggestions** as optional follow-up work" in result
        assert "The review item you addressed" in result
        assert "If a Must-Fix item no longer applies" in result

    def test_build_fix_type_with_summary(self, tmp_path: Path):
        """Fix prompts include rescue instructions and summary contract."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Rescue stuck workflow", task_type="fix")

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.verify_command = "uv run pytest tests/ -q"

        summary_path = Path("/workspace/.gza/summaries/fix-test.md")
        result = PromptBuilder().build(task, config, store, summary_path=summary_path)

        assert str(summary_path) in result
        assert "This is a `fix` rescue task for a stuck implementation workflow." in result
        assert "## Blocker Closure Ledger (Machine Readable)" in result
        assert "fix_result: repaired_pending_review | needs_user | blocked_external | diagnosed_no_change" in result
        assert "Before finishing, run the following verification command" in result
        assert "uv run pytest tests/ -q" in result

    def test_build_task_type_with_summary_includes_learnings_check_when_file_exists(self, tmp_path: Path):
        """Task prompts include the learnings checklist line only when the file exists."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Implement feature L", task_type="implement")

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        gza_dir = tmp_path / ".gza"
        gza_dir.mkdir(parents=True, exist_ok=True)
        (gza_dir / "learnings.md").write_text("Use fixtures.")

        summary_path = Path("/workspace/.gza/summaries/test.md")
        result = PromptBuilder().build(task, config, store, summary_path=summary_path)

        assert "Re-read `.gza/learnings.md` for project-specific patterns" in result

    def test_build_explore_type_with_report_path(self, tmp_path: Path):
        """Test that explore type includes exploration instructions."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Explore codebase", task_type="explore")

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        report_path = Path("/workspace/.gza/explorations/test.md")
        result = PromptBuilder().build(task, config, store, report_path=report_path)

        assert "exploration/research task" in result.lower()
        assert str(report_path) in result
        assert "findings and recommendations" in result

    def test_build_explore_type_without_report_path(self, tmp_path: Path):
        """Test that explore type without report_path skips file instructions."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Explore codebase", task_type="explore")

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        result = PromptBuilder().build(task, config, store, report_path=None)

        # Without report_path, no file instructions should be added
        assert "exploration/research task" not in result.lower()

    def test_build_plan_type_with_report_path(self, tmp_path: Path):
        """Test that plan type includes planning instructions."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Design feature", task_type="plan")

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        report_path = Path("/workspace/.gza/plans/test.md")
        result = PromptBuilder().build(task, config, store, report_path=report_path)

        assert "planning task" in result.lower()
        assert str(report_path) in result
        assert "Overview of the approach" in result
        assert "Key design decisions" in result
        assert "Implementation steps" in result

    def test_build_review_type_with_report_path(self, tmp_path: Path):
        """Test that review type includes review instructions."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Review the code", task_type="review")

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        report_path = Path("/workspace/.gza/reviews/test.md")
        result = PromptBuilder().build(task, config, store, report_path=report_path)

        assert "review task" in result.lower()
        assert str(report_path) in result
        assert "AGENTS.md" in result
        assert "APPROVED" in result
        assert "CHANGES_REQUESTED" in result
        assert "Verdict:" in result
        assert "## Summary" in result
        assert "## Must-Fix" in result
        assert "## Suggestions" in result
        assert "## Questions / Assumptions" in result
        assert "## Verdict" in result
        assert "Do not rename, omit, or reorder these sections." in result
        assert "write exactly: None." in result
        assert "### M1" in result
        assert "### S1" in result
        assert "Evidence:" in result
        assert "Impact:" in result
        assert "Required fix:" in result
        assert "Required tests:" in result
        assert "repo-rules/learnings pass" in result
        assert "silent broad-exception fallbacks" in result
        assert "misleading output" in result
        assert "targeted regression tests" in result
        assert "config, CLI, or operator-facing behavior changed" in result
        assert "## Original plan:" in result
        assert "## Original request:" in result
        assert "provided diff is authoritative" in result
        assert "read unchanged source files" in result
        assert "No plan or request provided." in result
        assert "unexplained deviations from the provided plan or request" in result
        assert "Reserve Must-Fix for:" in result
        _assert_summary_checklist_contract(result)
        checklist_lines = re.findall(r"^\s*-\s.+\?$", result, flags=re.MULTILINE)
        assert len(checklist_lines) == REVIEW_SUMMARY_CHECKLIST_COUNT
        assert "Yes/No - ..." in result

    def test_code_review_interactive_skill_uses_canonical_summary_contract(self):
        """Test interactive review skill scaffolding matches canonical Summary requirements."""
        skill_path = (
            Path(__file__).resolve().parents[1]
            / "src"
            / "gza"
            / "skills"
            / "gza-code-review-interactive"
            / "SKILL.md"
        )
        content = skill_path.read_text()

        assert "<Provide 3-5 bullets summarizing the review>" in content
        assert "<1-2 sentence overview of the changes>" not in content
        _assert_summary_checklist_contract(content)
        assert (
            "<Reserve Must-Fix for: correctness defects, behavior regressions, repository/rules violations, missing observability for user/agent-visible fallbacks, and misleading output/contradictory signals.>"
            in content
        )
        assert (
            "<Treat silent broad-exception fallbacks as Must-Fix when they can alter user/agent-visible state without clear warning/error surfacing.>"
            in content
        )
        assert (
            "<Treat unexplained deviations from the provided plan or request as Must-Fix.>"
            in content
        )
        assert (
            "<Treat misleading output (UI/prompt/context contradictions) as Must-Fix when it can cause incorrect operator or agent decisions.>"
            in content
        )

    def test_code_review_interactive_skill_requires_authoritative_diff_and_ask_handoff(
        self,
    ):
        """Interactive review skill must hand off authoritative diff plus canonical ask context."""
        skill_path = (
            Path(__file__).resolve().parents[1]
            / "src"
            / "gza"
            / "skills"
            / "gza-code-review-interactive"
            / "SKILL.md"
        )
        content = skill_path.read_text()

        assert (
            "Pass the authoritative diff context (`## Implementation diff context`), canonical ask context section (exactly one of `## Original plan:` or `## Original request:` when available), and the PR number (if `--pr` was used and a PR was found) to the subagent."
            in content
        )
        assert (
            "Review the diff against the provided canonical ask context (`## Original plan:` or `## Original request:`) when present."
            in content
        )
        assert (
            "Pass the PR number (if `--pr` was used and a PR was found) or nothing to the subagent."
            not in content
        )

    def test_review_contract_parity_between_template_and_interactive_scaffold(self):
        """Test canonical review contract clauses are present across review entrypoints."""
        root = Path(__file__).resolve().parents[1]
        template_content = (
            root / "src" / "gza" / "prompts" / "templates" / "review.txt"
        ).read_text()
        interactive_skill_content = (
            root
            / "src"
            / "gza"
            / "skills"
            / "gza-code-review-interactive"
            / "SKILL.md"
        ).read_text()
        task_review_skill_content = (
            root / "src" / "gza" / "skills" / "gza-task-review" / "SKILL.md"
        ).read_text()

        _assert_contains_all_clauses(template_content, REVIEW_CONTRACT_PARITY_CLAUSES)
        _assert_contains_all_clauses(
            interactive_skill_content, REVIEW_CONTRACT_PARITY_CLAUSES
        )
        _assert_contains_all_clauses(
            task_review_skill_content, REVIEW_CONTRACT_PARITY_CLAUSES
        )
        _assert_summary_checklist_contract(template_content)
        _assert_summary_checklist_contract(interactive_skill_content)
        _assert_summary_checklist_contract(task_review_skill_content)
        assert "## Task Prompt Alignment" not in task_review_skill_content

    def test_task_review_skill_does_not_provide_second_ask_source(self):
        """Task-review scaffold should hand off only canonical ask context to subagents."""
        skill_path = (
            Path(__file__).resolve().parents[1]
            / "src"
            / "gza"
            / "skills"
            / "gza-task-review"
            / "SKILL.md"
        )
        content = skill_path.read_text()

        assert (
            "Review the diff against the provided ask context (`## Original plan:` or `## Original request:`)."
            in content
        )
        assert "- Task prompt: `<impl_prompt>`" not in content

    def test_task_review_skill_requires_parent_session_canonical_ask_capture(self):
        """Task-review scaffold must define canonical ask capture and fallback behavior."""
        skill_path = (
            Path(__file__).resolve().parents[1]
            / "src"
            / "gza"
            / "skills"
            / "gza-task-review"
            / "SKILL.md"
        )
        content = skill_path.read_text()

        assert "Capture one canonical ask section before spawning the reviewer:" in content
        assert (
            "If the caller already provided exactly one canonical ask section (`## Original plan:` or `## Original request:`), pass that section through unchanged."
            in content
        )
        assert (
            "If linked ask content exists but is unavailable on this machine, pass an explicit unavailable-content marker section"
            in content
        )
        assert (
            "(plan task <TASK_ID> exists but content unavailable on this machine - flag as blocker)"
            in content
        )
        assert (
            "If no retrievable plan or request exists for this task, pass no ask section and let the reviewer state: `No plan or request provided.`"
            in content
        )

    def test_build_review_type_with_review_md(self, tmp_path: Path):
        """Test that REVIEW.md content is included in review prompts."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Review the code", task_type="review")

        # Create REVIEW.md in project dir
        review_md = tmp_path / "REVIEW.md"
        review_md.write_text("# Custom Review Guidelines\n\nCheck for security issues.")

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        report_path = Path("/workspace/.gza/reviews/test.md")
        result = PromptBuilder().build(task, config, store, report_path=report_path)

        assert "Review Guidelines" in result
        assert "Check for security issues." in result

    def test_build_spec_file_included(self, tmp_path: Path):
        """Test that spec file content is included when task.spec is set."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Create spec file
        spec_file = tmp_path / "spec.md"
        spec_file.write_text("# Spec\n\nDo things carefully.")

        task = store.add(prompt="Implement per spec", task_type="implement")
        task.spec = "spec.md"
        store.update(task)
        task = store.get(task.id)

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        result = PromptBuilder().build(task, config, store)

        assert "## Specification" in result
        assert "Do things carefully." in result

    def test_build_unknown_type_fallback(self, tmp_path: Path):
        """Test that unknown task types get a fallback message."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Do something", task_type="implement")
        # Manually override task_type to an unknown value
        task.task_type = "unknown_type"

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        result = PromptBuilder().build(task, config, store)

        assert "report what you accomplished" in result


class TestPromptBuilderResumePrompt:
    """Tests for PromptBuilder.resume_prompt()."""

    def test_resume_prompt_contains_verification_instructions(self):
        """Test that resume prompt instructs agent to verify todo list."""
        result = PromptBuilder().resume_prompt()

        assert "interrupted" in result.lower()
        assert "git status" in result.lower()
        assert "git log" in result.lower()
        assert "todo list" in result.lower()
        assert "continue from the actual state" in result.lower()

    def test_resume_prompt_returns_string(self):
        """Test that resume_prompt returns a non-empty string."""
        result = PromptBuilder().resume_prompt()
        assert isinstance(result, str)
        assert len(result) > 0

    def test_resume_prompt_consistent(self):
        """Test that resume_prompt returns consistent output across calls."""
        builder = PromptBuilder()
        result1 = builder.resume_prompt()
        result2 = builder.resume_prompt()
        assert result1 == result2


class TestPromptBuilderPrDescription:
    """Tests for PromptBuilder.pr_description_prompt()."""

    def test_pr_description_includes_task_prompt(self):
        """Test that PR description prompt includes the task prompt."""
        result = PromptBuilder().pr_description_prompt(
            task_prompt="Add user authentication",
            commit_log="abc123 Add login endpoint",
            diff_stat="src/auth.py | 50 +++",
        )

        assert "Add user authentication" in result

    def test_pr_description_includes_commit_log(self):
        """Test that PR description prompt includes the commit log."""
        result = PromptBuilder().pr_description_prompt(
            task_prompt="Fix bug",
            commit_log="def456 Fix null pointer exception",
            diff_stat="src/utils.py | 5 +-",
        )

        assert "def456 Fix null pointer exception" in result

    def test_pr_description_includes_diff_stat(self):
        """Test that PR description prompt includes the diff stat."""
        result = PromptBuilder().pr_description_prompt(
            task_prompt="Refactor module",
            commit_log="ghi789 Refactor utils",
            diff_stat="src/module.py | 100 +++---",
        )

        assert "src/module.py | 100 +++---" in result

    def test_pr_description_includes_format_instructions(self):
        """Test that PR description prompt includes format instructions."""
        result = PromptBuilder().pr_description_prompt(
            task_prompt="Add feature",
            commit_log="jkl012 Add feature",
            diff_stat="src/feature.py | 20 +",
        )

        assert "TITLE:" in result
        assert "BODY:" in result
        assert "## Summary" in result
        assert "## Changes" in result

    def test_pr_description_includes_issue_linking_instructions(self):
        """Test that PR description prompt includes issue-linking guidance."""
        result = PromptBuilder().pr_description_prompt(
            task_prompt="Implement enhancement for issue #23",
            commit_log="abc123 Implement enhancement",
            diff_stat="src/feature.py | 20 +",
        )

        assert "Closes #<issue number>" in result
        assert "Closes #N" in result
        assert "exactly one final line" in result
        assert "Do not guess or infer issue numbers." in result

    def test_pr_description_disambiguates_non_issue_number_references(self):
        """Test that PR description prompt disambiguates task/PR numbers from issues."""
        result = PromptBuilder().pr_description_prompt(
            task_prompt="Improve implementation of task #465 based on review #480",
            commit_log="abc123 Improve implementation",
            diff_stat="src/feature.py | 20 +",
        )

        assert "Do not treat task IDs, PR numbers, or generic `#N` references as issues" in result
        assert "explicitly labeled as `issue #N` or `GitHub issue #N`" in result


class TestPromptBuilderImproveTask:
    """Tests for PromptBuilder.improve_task_prompt()."""

    def test_improve_task_prompt_includes_review_id(self):
        """Test that improve task prompt references the review ID."""
        result = PromptBuilder().improve_task_prompt(task_id=10, review_id=42)
        assert "42" in result
        assert "review" in result.lower()

    def test_improve_task_prompt_includes_task_id(self):
        """Test that improve task prompt references the task ID."""
        result = PromptBuilder().improve_task_prompt(task_id=10, review_id=42)
        assert "10" in result

    def test_improve_task_prompt_format(self):
        """Test the exact format of improve task prompt."""
        result = PromptBuilder().improve_task_prompt(task_id=5, review_id=7)
        assert result == "Improve implementation of task 5 based on review 7"


class TestPromptBuilderReviewTask:
    """Tests for PromptBuilder.review_task_prompt()."""

    def test_review_task_prompt_includes_task_id(self):
        """Test that review task prompt references the implementation task ID."""
        result = PromptBuilder().review_task_prompt(impl_task_id=15)
        assert "15" in result
        assert result.startswith("Review task 15")

    def test_review_task_prompt_with_impl_prompt(self):
        """Test that review task prompt ignores implementation prompt text."""
        result = PromptBuilder().review_task_prompt(
            impl_task_id=15, impl_prompt="Add user authentication with JWT tokens"
        )
        assert "15" in result
        assert "Add user authentication with JWT tokens" not in result

    def test_review_task_prompt_does_not_include_long_impl_prompt(self):
        """Test that review task prompt does not embed long implementation prompts."""
        long_prompt = "x" * 200
        result = PromptBuilder().review_task_prompt(
            impl_task_id=1, impl_prompt=long_prompt
        )
        assert "x" * 100 not in result

    def test_review_task_prompt_without_impl_prompt(self):
        """Test that review task prompt works without implementation prompt."""
        result = PromptBuilder().review_task_prompt(impl_task_id=5, impl_prompt=None)
        assert "5" in result
        assert ":" not in result.split("task 5")[1] if "task 5" in result else True

    def test_review_task_prompt_format_without_impl_prompt(self):
        """Test the format when no impl prompt is given includes self-contained diff guidance."""
        result = PromptBuilder().review_task_prompt(impl_task_id=3)
        assert result.startswith("Review task 3")
        assert "changed-files list" in result
        assert "inline diff/context" in result
        assert "provided diff is authoritative" in result
        assert "read unchanged source files" in result


class TestPromptBuilderFixTask:
    """Tests for PromptBuilder.fix_task_prompt()."""

    def test_fix_task_prompt_with_review(self):
        result = PromptBuilder().fix_task_prompt(task_id="gza-10", review_id="gza-12")
        assert result == "Rescue stuck implementation task gza-10 based on review gza-12"

    def test_fix_task_prompt_without_review(self):
        result = PromptBuilder().fix_task_prompt(task_id="gza-10")
        assert result == "Rescue stuck implementation task gza-10"


class TestVerifyCommandConfig:
    """Tests for verify_command field in Config."""

    def test_verify_command_loaded_from_yaml(self, tmp_path: Path):
        """Test that verify_command is loaded from gza.yaml."""
        from gza.config import Config

        config_file = tmp_path / "gza.yaml"
        config_file.write_text(
            "project_name: testproject\n"
            "verify_command: 'uv run pytest tests/'\n"
        )

        config = Config.load(tmp_path)
        assert config.verify_command == "uv run pytest tests/"

    def test_verify_command_defaults_to_empty(self, tmp_path: Path):
        """Test that verify_command defaults to empty string when not set."""
        from gza.config import Config

        config_file = tmp_path / "gza.yaml"
        config_file.write_text("project_name: testproject\n")

        config = Config.load(tmp_path)
        assert config.verify_command == ""

    def test_verify_command_validation_rejects_non_string(self, tmp_path: Path):
        """Test that verify_command validation fails for non-string values."""
        from gza.config import Config

        config_file = tmp_path / "gza.yaml"
        config_file.write_text(
            "project_name: testproject\n"
            "verify_command: 42\n"
        )

        is_valid, errors, warnings = Config.validate(tmp_path)
        assert not is_valid
        assert any("verify_command" in e for e in errors)

    def test_verify_command_not_unknown_field(self, tmp_path: Path):
        """Test that verify_command is not treated as an unknown field."""
        from gza.config import Config

        config_file = tmp_path / "gza.yaml"
        config_file.write_text(
            "project_name: testproject\n"
            "verify_command: 'uv run mypy src/'\n"
        )

        is_valid, errors, warnings = Config.validate(tmp_path)
        assert is_valid
        assert not any("verify_command" in w for w in warnings)


class TestReviewDiffThresholdConfig:
    """Tests for review diff/context threshold fields in Config."""

    def test_review_thresholds_loaded_from_yaml(self, tmp_path: Path):
        """review diff/context threshold fields are loaded from gza.yaml."""
        from gza.config import Config

        config_file = tmp_path / "gza.yaml"
        config_file.write_text(
            "project_name: testproject\n"
            "review_diff_small_threshold: 111\n"
            "review_diff_medium_threshold: 222\n"
            "review_context_file_limit: 7\n"
        )

        config = Config.load(tmp_path)
        assert config.review_diff_small_threshold == 111
        assert config.review_diff_medium_threshold == 222
        assert config.review_context_file_limit == 7

    def test_review_thresholds_have_defaults(self, tmp_path: Path):
        """review threshold fields use defaults when omitted."""
        from gza.config import (
            DEFAULT_REVIEW_CONTEXT_FILE_LIMIT,
            DEFAULT_REVIEW_DIFF_MEDIUM_THRESHOLD,
            DEFAULT_REVIEW_DIFF_SMALL_THRESHOLD,
            Config,
        )

        config_file = tmp_path / "gza.yaml"
        config_file.write_text("project_name: testproject\n")

        config = Config.load(tmp_path)
        assert config.review_diff_small_threshold == DEFAULT_REVIEW_DIFF_SMALL_THRESHOLD
        assert config.review_diff_medium_threshold == DEFAULT_REVIEW_DIFF_MEDIUM_THRESHOLD
        assert config.review_context_file_limit == DEFAULT_REVIEW_CONTEXT_FILE_LIMIT

    def test_review_thresholds_validation_rejects_invalid_values(self, tmp_path: Path):
        """validate rejects non-positive values and invalid ordering."""
        from gza.config import Config

        config_file = tmp_path / "gza.yaml"
        config_file.write_text(
            "project_name: testproject\n"
            "review_diff_small_threshold: 10\n"
            "review_diff_medium_threshold: 5\n"
            "review_context_file_limit: 0\n"
        )

        is_valid, errors, warnings = Config.validate(tmp_path)
        assert not is_valid
        assert any("review_diff_medium_threshold" in e for e in errors)
        assert any("review_context_file_limit" in e for e in errors)


class TestVerifyCommandInjection:
    """Tests for verify_command injection into prompts."""

    def test_verify_command_injected_for_task_type(self, tmp_path: Path):
        """Test that verify_command is appended for task type."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Do something", task_type="implement")

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.verify_command = "uv run mypy src/ && uv run pytest tests/ -x -q"

        result = PromptBuilder().build(task, config, store)

        assert "Before finishing, run the following verification command" in result
        assert "uv run mypy src/ && uv run pytest tests/ -x -q" in result

    def test_verify_command_injected_for_implement_type(self, tmp_path: Path):
        """Test that verify_command is appended for implement type."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Implement feature", task_type="implement")

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.verify_command = "uv run pytest tests/ -x -q"

        result = PromptBuilder().build(task, config, store)

        assert "Before finishing, run the following verification command" in result
        assert "uv run pytest tests/ -x -q" in result

    def test_verify_command_injected_for_improve_type(self, tmp_path: Path):
        """Test that verify_command is appended for improve type."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Improve the code", task_type="improve")

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.verify_command = "uv run pytest tests/"

        result = PromptBuilder().build(task, config, store)

        assert "Before finishing, run the following verification command" in result
        assert "uv run pytest tests/" in result

    def test_verify_command_not_injected_when_empty(self, tmp_path: Path):
        """Test that no verification instruction is added when verify_command is empty."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Do something", task_type="implement")

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.verify_command = ""

        result = PromptBuilder().build(task, config, store)

        assert "Before finishing, run the following verification command" not in result

    def test_verify_command_not_injected_for_explore_type(self, tmp_path: Path):
        """Test that verify_command is NOT injected for explore tasks."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Explore codebase", task_type="explore")

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.verify_command = "uv run pytest tests/"

        report_path = tmp_path / "report.md"
        result = PromptBuilder().build(task, config, store, report_path=report_path)

        assert "Before finishing, run the following verification command" not in result

    def test_verify_command_not_injected_for_plan_type(self, tmp_path: Path):
        """Test that verify_command is NOT injected for plan tasks."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Design feature", task_type="plan")

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.verify_command = "uv run pytest tests/"

        report_path = tmp_path / "report.md"
        result = PromptBuilder().build(task, config, store, report_path=report_path)

        assert "Before finishing, run the following verification command" not in result

    def test_verify_command_not_injected_for_review_type(self, tmp_path: Path):
        """Test that verify_command is NOT injected for review tasks."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Review the code", task_type="review")

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.verify_command = "uv run pytest tests/"

        report_path = tmp_path / "report.md"
        result = PromptBuilder().build(task, config, store, report_path=report_path)

        assert "Before finishing, run the following verification command" not in result

    def test_verify_command_appears_in_backticks(self, tmp_path: Path):
        """Test that the verify_command is wrapped in backticks in the prompt."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Implement feature", task_type="implement")

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.verify_command = "make test"

        result = PromptBuilder().build(task, config, store)

        assert "`make test`" in result
