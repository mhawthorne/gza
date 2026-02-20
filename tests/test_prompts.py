"""Tests for the PromptBuilder class in gza.prompts."""

from pathlib import Path
from unittest.mock import Mock

import pytest

from gza.prompts import PromptBuilder
from gza.config import Config
from gza.db import SqliteTaskStore


class TestPromptBuilderBuild:
    """Tests for PromptBuilder.build()."""

    def test_build_base_prompt(self, tmp_path: Path):
        """Test that build() includes the task prompt in the output."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Do something useful", task_type="task")

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        result = PromptBuilder().build(task, config, store)
        assert "Complete this task: Do something useful" in result

    def test_build_task_type_with_summary(self, tmp_path: Path):
        """Test that task type includes unit test and summary instructions."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Implement feature X", task_type="task")

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        summary_path = Path("/workspace/.gza/summaries/test.md")
        result = PromptBuilder().build(task, config, store, summary_path=summary_path)

        assert "uv run pytest tests/ -v" in result
        assert str(summary_path) in result
        assert "What was accomplished" in result
        assert "Files changed" in result

    def test_build_task_type_without_summary(self, tmp_path: Path):
        """Test that task type without summary includes fallback message."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Implement feature Z", task_type="task")

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        result = PromptBuilder().build(task, config, store, summary_path=None)

        assert "uv run pytest tests/ -v" in result
        assert "report what you accomplished" in result
        assert "write a summary" not in result.lower()

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

    def test_build_improve_type_with_summary(self, tmp_path: Path):
        """Test that improve type includes unit test and summary instructions."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Improve the code", task_type="improve")

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        summary_path = Path("/workspace/.gza/summaries/improve-test.md")
        result = PromptBuilder().build(task, config, store, summary_path=summary_path)

        assert "uv run pytest tests/ -v" in result
        assert str(summary_path) in result

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
        assert "APPROVED" in result
        assert "CHANGES_REQUESTED" in result
        assert "Verdict:" in result

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
        task = store.add(prompt="Do something", task_type="task")
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

        assert "verify your todo list against the actual state" in result.lower()
        assert "review your todo list from the previous session" in result.lower()
        assert "verify by checking the actual code/files" in result.lower()
        assert "update the todo list to reflect what is actually complete" in result.lower()
        assert "continue from where you left off" in result.lower()

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


class TestPromptBuilderImproveTask:
    """Tests for PromptBuilder.improve_task_prompt()."""

    def test_improve_task_prompt_includes_review_id(self):
        """Test that improve task prompt references the review ID."""
        result = PromptBuilder().improve_task_prompt(review_id=42)
        assert "42" in result
        assert "review" in result.lower()

    def test_improve_task_prompt_format(self):
        """Test the exact format of improve task prompt."""
        result = PromptBuilder().improve_task_prompt(review_id=7)
        assert result == "Improve implementation based on review #7"


class TestPromptBuilderReviewTask:
    """Tests for PromptBuilder.review_task_prompt()."""

    def test_review_task_prompt_includes_task_id(self):
        """Test that review task prompt references the implementation task ID."""
        result = PromptBuilder().review_task_prompt(impl_task_id=15)
        assert "15" in result
        assert "implementation" in result.lower()

    def test_review_task_prompt_with_impl_prompt(self):
        """Test that review task prompt includes implementation prompt excerpt."""
        result = PromptBuilder().review_task_prompt(
            impl_task_id=15, impl_prompt="Add user authentication with JWT tokens"
        )
        assert "15" in result
        assert "Add user authentication with JWT tokens" in result

    def test_review_task_prompt_truncates_long_impl_prompt(self):
        """Test that long implementation prompts are truncated to 100 chars."""
        long_prompt = "x" * 200
        result = PromptBuilder().review_task_prompt(
            impl_task_id=1, impl_prompt=long_prompt
        )
        # Should include at most 100 chars of the prompt
        assert "x" * 100 in result
        assert "x" * 101 not in result

    def test_review_task_prompt_without_impl_prompt(self):
        """Test that review task prompt works without implementation prompt."""
        result = PromptBuilder().review_task_prompt(impl_task_id=5, impl_prompt=None)
        assert "5" in result
        assert ":" not in result.split("task #5")[1] if "task #5" in result else True

    def test_review_task_prompt_format_without_impl_prompt(self):
        """Test the exact format when no impl prompt is given."""
        result = PromptBuilder().review_task_prompt(impl_task_id=3)
        assert result == "Review the implementation from task #3"
