"""Tests for runner module."""

from pathlib import Path
from unittest.mock import Mock, MagicMock, patch

import pytest

from gza.config import Config
from gza.db import SqliteTaskStore, Task
from gza.git import Git
from gza.providers import RunResult
from gza.runner import (
    build_prompt,
    SUMMARY_DIR,
    WIP_DIR,
    BACKUP_DIR,
    _build_context_from_chain,
    backup_database,
    _create_and_run_review_task,
    _run_non_code_task,
    _save_wip_changes,
    _restore_wip_changes,
    _squash_wip_commits,
    _run_result_to_stats,
    post_review_to_pr,
    run,
)


class TestBuildPrompt:
    """Tests for build_prompt function."""

    def test_build_prompt_task_type_with_summary_path(self, tmp_path: Path):
        """Test that build_prompt includes summary instructions for task type."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(
            prompt="Implement feature X",
            task_type="task",
        )

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        summary_path = Path("/workspace/.gza/summaries/test.md")

        prompt = build_prompt(task, config, store, summary_path=summary_path)

        assert "Complete this task: Implement feature X" in prompt
        assert str(summary_path) in prompt
        assert "write a summary" in prompt.lower()
        assert "What was accomplished" in prompt
        assert "Files changed" in prompt
        assert "notable decisions" in prompt.lower()

    def test_build_prompt_implement_type_with_summary_path(self, tmp_path: Path):
        """Test that build_prompt includes summary instructions for implement type."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(
            prompt="Implement feature Y",
            task_type="implement",
        )

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        summary_path = Path("/workspace/.gza/summaries/test.md")

        prompt = build_prompt(task, config, store, summary_path=summary_path)

        assert "Complete this task: Implement feature Y" in prompt
        assert str(summary_path) in prompt
        assert "write a summary" in prompt.lower()

    def test_build_prompt_task_type_without_summary_path(self, tmp_path: Path):
        """Test that build_prompt uses fallback message without summary path."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(
            prompt="Implement feature Z",
            task_type="task",
        )

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        prompt = build_prompt(task, config, store, summary_path=None)

        assert "Complete this task: Implement feature Z" in prompt
        assert "report what you accomplished" in prompt
        assert "write a summary" not in prompt.lower()

    def test_build_prompt_explore_type_ignores_summary_path(self, tmp_path: Path):
        """Test that explore tasks use report_path, not summary_path."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(
            prompt="Explore codebase",
            task_type="explore",
        )

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        report_path = Path("/workspace/.gza/explorations/test.md")
        summary_path = Path("/workspace/.gza/summaries/test.md")

        prompt = build_prompt(task, config, store, report_path=report_path, summary_path=summary_path)

        # Should use report_path for explore tasks
        assert str(report_path) in prompt
        assert "exploration/research task" in prompt.lower()
        # Should NOT include summary path
        assert str(summary_path) not in prompt

    def test_build_prompt_includes_learnings_when_file_exists(self, tmp_path: Path):
        """Test that build_prompt includes learnings.md content when it exists."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(
            prompt="Implement feature X",
            task_type="task",
        )

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        # Create .gza/learnings.md
        gza_dir = tmp_path / ".gza"
        gza_dir.mkdir(parents=True, exist_ok=True)
        learnings_content = "# Project Learnings\n\n- Use pytest fixtures for database setup\n"
        (gza_dir / "learnings.md").write_text(learnings_content)

        prompt = build_prompt(task, config, store)

        assert "Project Learnings" in prompt
        assert "Use pytest fixtures for database setup" in prompt

    def test_build_prompt_skips_learnings_when_no_file(self, tmp_path: Path):
        """Test that build_prompt works normally when learnings.md doesn't exist."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(
            prompt="Implement feature X",
            task_type="task",
        )

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        prompt = build_prompt(task, config, store)

        assert "Project Learnings" not in prompt
        assert "Complete this task: Implement feature X" in prompt

    def test_build_prompt_skips_learnings_when_skip_learnings_true(self, tmp_path: Path):
        """Test that build_prompt skips learnings.md when task.skip_learnings is True."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(
            prompt="One-off experimental task",
            task_type="task",
            skip_learnings=True,
        )

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        # Create .gza/learnings.md
        gza_dir = tmp_path / ".gza"
        gza_dir.mkdir(parents=True, exist_ok=True)
        learnings_content = "# Project Learnings\n\n- Use pytest fixtures\n"
        (gza_dir / "learnings.md").write_text(learnings_content)

        prompt = build_prompt(task, config, store)

        assert "Project Learnings" not in prompt
        assert "Complete this task: One-off experimental task" in prompt

    def test_build_prompt_learnings_include_preamble(self, tmp_path: Path):
        """Test that learnings injection includes the 'Accumulated Project Learnings' preamble."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(prompt="Implement feature Y", task_type="task")

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        gza_dir = tmp_path / ".gza"
        gza_dir.mkdir(parents=True, exist_ok=True)
        (gza_dir / "learnings.md").write_text("- Always use pytest fixtures\n")

        prompt = build_prompt(task, config, store)

        assert "## Accumulated Project Learnings" in prompt
        assert "Always use pytest fixtures" in prompt

    def test_build_prompt_learnings_unreadable_file_does_not_crash(self, tmp_path: Path):
        """Test that an unreadable learnings.md doesn't crash prompt building."""
        import os

        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(prompt="Implement feature Z", task_type="task")

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        gza_dir = tmp_path / ".gza"
        gza_dir.mkdir(parents=True, exist_ok=True)
        learnings_path = gza_dir / "learnings.md"
        learnings_path.write_text("- Some learning\n")
        # Make file unreadable
        os.chmod(learnings_path, 0o000)

        try:
            prompt = build_prompt(task, config, store)
            # Should not crash; learnings simply omitted
            assert "Some learning" not in prompt
            assert "Complete this task: Implement feature Z" in prompt
        finally:
            os.chmod(learnings_path, 0o644)


class TestReviewContextFromChain:
    """Tests for self-contained review context generation."""

    def test_review_context_includes_changed_files_diffstat_and_diff(self, tmp_path: Path):
        """Review context should include changed files, diffstat, and inline diff."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Implement feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test/feature-branch"
        store.update(impl_task)

        review_task = store.add(
            prompt="Review implementation",
            task_type="review",
            depends_on=impl_task.id,
        )

        mock_git = Mock(spec=Git)
        mock_git.default_branch.return_value = "main"
        mock_git.get_diff_numstat.return_value = "10\t2\tsrc/a.py\n3\t1\tsrc/b.py\n"
        mock_git.get_diff_stat.return_value = (
            " src/a.py | 12 ++++++++++\n src/b.py | 4 +++-\n 2 files changed, 13 insertions(+), 3 deletions(-)"
        )
        mock_git.get_diff.return_value = "diff --git a/src/a.py b/src/a.py\n@@ -1 +1 @@\n-old\n+new\n"

        context = _build_context_from_chain(review_task, store, tmp_path, mock_git)

        assert "## Implementation Diff Context" in context
        assert "Changed files:" in context
        assert "- src/a.py" in context
        assert "- src/b.py" in context
        assert "Diff summary:" in context
        assert "Full diff:" in context
        assert "Use git and file reading tools" not in context

    def test_review_context_large_diff_adds_targeted_excerpts(self, tmp_path: Path):
        """Large review diffs should include targeted diff excerpts."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Implement large feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test/large-branch"
        store.update(impl_task)

        review_task = store.add(
            prompt="Review large implementation",
            task_type="review",
            depends_on=impl_task.id,
        )

        mock_git = Mock(spec=Git)
        mock_git.default_branch.return_value = "main"
        mock_git.get_diff_numstat.return_value = (
            "1500\t400\tsrc/large_a.py\n800\t200\tsrc/large_b.py\n"
        )
        mock_git.get_diff_stat.return_value = " 2 files changed, 2300 insertions(+), 600 deletions(-)"
        mock_git._run.return_value = Mock(stdout="diff --git a/src/large_a.py b/src/large_a.py\n@@ ...", returncode=0)

        context = _build_context_from_chain(review_task, store, tmp_path, mock_git)

        assert "Targeted diff excerpts" in context
        assert "Additional changed files not expanded inline" not in context


class TestSummaryDirectory:
    """Tests for summary directory constant."""

    def test_summary_dir_constant_value(self):
        """Test that SUMMARY_DIR constant has the correct value."""
        assert SUMMARY_DIR == ".gza/summaries"


class TestReviewTaskSlugGeneration:
    """Tests for review task slug generation."""

    def test_review_task_uses_implementation_slug(self, tmp_path: Path):
        """Test that auto-created review tasks derive slug from implementation task."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Create a completed implementation task with a task_id
        impl_task = store.add(
            prompt="Add docker volumes support",
            task_type="implement",
        )
        impl_task.status = "completed"
        impl_task.task_id = "20260129-add-docker-volumes"
        store.update(impl_task)

        # Get the task to verify task_id is set
        impl_task = store.get(impl_task.id)
        assert impl_task.task_id == "20260129-add-docker-volumes"

        # Create a mock config and run function that captures the review task
        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"

        # Capture the review task that gets created
        def mock_run(config, task_id):
            return 0

        # Mock post_review_to_pr to avoid GitHub CLI dependency
        def mock_post_review_to_pr(*args, **kwargs):
            pass

        # Temporarily replace run and post_review_to_pr functions
        import gza.runner
        original_run = gza.runner.run
        original_post_review = gza.runner.post_review_to_pr
        gza.runner.run = mock_run
        gza.runner.post_review_to_pr = mock_post_review_to_pr

        try:
            # Call _create_and_run_review_task
            _create_and_run_review_task(impl_task, config, store)

            # Get the review task that was created
            review_task = store.get(2)
            assert review_task is not None
            assert review_task.task_type == "review"

            # Verify the prompt uses the slug format
            assert review_task.prompt == "review add-docker-volumes"
        finally:
            gza.runner.run = original_run
            gza.runner.post_review_to_pr = original_post_review

    def test_review_task_handles_retry_suffix(self, tmp_path: Path):
        """Test that review task slug handles retry suffix in implementation task_id."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Create an implementation task with retry suffix
        impl_task = store.add(
            prompt="Fix authentication bug",
            task_type="implement",
        )
        impl_task.status = "completed"
        impl_task.task_id = "20260129-fix-authentication-bug-2"
        store.update(impl_task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"

        def mock_run(config, task_id):
            return 0

        def mock_post_review_to_pr(*args, **kwargs):
            pass

        import gza.runner
        original_run = gza.runner.run
        original_post_review = gza.runner.post_review_to_pr
        gza.runner.run = mock_run
        gza.runner.post_review_to_pr = mock_post_review_to_pr

        try:
            _create_and_run_review_task(impl_task, config, store)

            review_task = store.get(2)
            assert review_task is not None
            # Should strip the retry suffix (-2) from the slug
            assert review_task.prompt == "review fix-authentication-bug"
        finally:
            gza.runner.run = original_run
            gza.runner.post_review_to_pr = original_post_review

    def test_review_task_fallback_without_task_id(self, tmp_path: Path):
        """Test that review task falls back gracefully if task_id is not set."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Create an implementation task without task_id
        impl_task = store.add(
            prompt="Implement feature",
            task_type="implement",
        )
        impl_task.status = "completed"
        # Don't set task_id
        store.update(impl_task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"

        def mock_run(config, task_id):
            return 0

        def mock_post_review_to_pr(*args, **kwargs):
            pass

        import gza.runner
        original_run = gza.runner.run
        original_post_review = gza.runner.post_review_to_pr
        gza.runner.run = mock_run
        gza.runner.post_review_to_pr = mock_post_review_to_pr

        try:
            _create_and_run_review_task(impl_task, config, store)

            review_task = store.get(2)
            assert review_task is not None
            # Should use fallback format
            assert "Review the implementation from task #1" in review_task.prompt
        finally:
            gza.runner.run = original_run
            gza.runner.post_review_to_pr = original_post_review

    def test_auto_review_delegates_to_run(self, tmp_path: Path):
        """Test that _create_and_run_review_task delegates PR posting to run().

        PR posting now happens in _run_non_code_task (called by run()), not in
        _create_and_run_review_task itself. This test verifies the delegation.
        """
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Create a completed implementation task with a PR
        impl_task = store.add(
            prompt="Add user authentication",
            task_type="implement",
        )
        impl_task.status = "completed"
        impl_task.task_id = "20260211-add-user-authentication"
        impl_task.branch = "gza/20260211-add-user-authentication"
        impl_task.pr_number = 123
        store.update(impl_task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"

        # Track that run() was called with the review task
        run_calls = []

        def mock_run(config, task_id):
            run_calls.append(task_id)
            return 0  # Success

        import gza.runner
        original_run = gza.runner.run

        gza.runner.run = mock_run

        try:
            # Call _create_and_run_review_task
            exit_code = _create_and_run_review_task(impl_task, config, store)

            # Verify success
            assert exit_code == 0

            # Verify run() was called with the review task id
            assert len(run_calls) == 1
            assert run_calls[0] == 2  # The created review task
        finally:
            gza.runner.run = original_run

    def test_auto_review_returns_run_exit_code(self, tmp_path: Path):
        """Test that _create_and_run_review_task returns the exit code from run()."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Create a completed implementation task
        impl_task = store.add(
            prompt="Add user authentication",
            task_type="implement",
        )
        impl_task.status = "completed"
        impl_task.task_id = "20260211-add-user-authentication"
        store.update(impl_task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"

        def mock_run_failure(config, task_id):
            return 1  # Failure

        import gza.runner
        original_run = gza.runner.run
        gza.runner.run = mock_run_failure

        try:
            # Call _create_and_run_review_task
            exit_code = _create_and_run_review_task(impl_task, config, store)

            # Verify failure code is returned
            assert exit_code == 1
        finally:
            gza.runner.run = original_run


class TestReviewNextSteps:
    """Tests for next steps output after review task completion."""

    def test_review_completion_suggests_improve(self, tmp_path: Path):
        """Test that review completion output suggests gza improve <impl_task_id>."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Create a completed implementation task
        impl_task = store.add(
            prompt="Add user authentication",
            task_type="implement",
        )
        impl_task.status = "completed"
        impl_task.task_id = "20260211-add-user-authentication"
        impl_task.branch = "gza/20260211-add-user-authentication"
        store.update(impl_task)

        # Create a review task that depends on it
        review_task = store.add(
            prompt="Review the implementation",
            task_type="review",
            depends_on=impl_task.id,
        )
        review_task.task_id = "20260212-review-the-implementation"
        store.update(review_task)

        # Setup config
        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False

        # Mock provider
        mock_provider = Mock()
        mock_provider.name = "MockProvider"
        mock_result = RunResult(
            exit_code=0,
            duration_seconds=10.0,
            num_turns_reported=5,
            cost_usd=0.05,
            session_id="test-session",
            error_type=None,
        )
        mock_provider.run.return_value = mock_result

        # Mock git
        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=0)
        mock_git.get_diff_numstat.return_value = ""
        mock_git.get_diff.return_value = ""

        # Create worktree directory and report file
        worktree_path = config.worktree_path / f"{review_task.task_id}-review"
        worktree_review_dir = worktree_path / ".gza" / "reviews"
        worktree_review_dir.mkdir(parents=True, exist_ok=True)
        report_file = worktree_review_dir / f"{review_task.task_id}.md"
        report_file.write_text("# Review\n\nChanges requested.")

        # Capture console output by collecting print calls
        printed_lines: list[str] = []

        def capture_print(*args, **kwargs):
            printed_lines.append(str(args[0]) if args else "")

        with patch('gza.runner.console') as mock_console, \
             patch('gza.runner.post_review_to_pr'):
            mock_console.print.side_effect = capture_print

            exit_code = _run_non_code_task(
                review_task, config, store, mock_provider, mock_git, resume=False
            )

            assert exit_code == 0

            # Verify improve suggestions appear in output
            all_output = "\n".join(printed_lines)
            assert f"gza improve {impl_task.id}" in all_output
            assert f"gza improve {impl_task.id} --run" not in all_output

    def test_review_completion_prints_verdict(self, tmp_path: Path):
        """Completed review output should print parsed review verdict."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Add user authentication", task_type="implement")
        impl_task.status = "completed"
        impl_task.task_id = "20260211-add-user-authentication"
        impl_task.branch = "gza/20260211-add-user-authentication"
        store.update(impl_task)

        review_task = store.add(
            prompt="Review the implementation",
            task_type="review",
            depends_on=impl_task.id,
        )
        review_task.task_id = "20260212-review-the-implementation"
        store.update(review_task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False

        mock_provider = Mock()
        mock_provider.name = "MockProvider"
        mock_provider.run.return_value = RunResult(
            exit_code=0,
            duration_seconds=10.0,
            num_turns_reported=5,
            cost_usd=0.05,
            session_id="test-session",
            error_type=None,
        )

        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=0)
        mock_git.get_diff_numstat.return_value = ""
        mock_git.get_diff.return_value = ""

        worktree_path = config.worktree_path / f"{review_task.task_id}-review"
        worktree_review_dir = worktree_path / ".gza" / "reviews"
        worktree_review_dir.mkdir(parents=True, exist_ok=True)
        report_file = worktree_review_dir / f"{review_task.task_id}.md"
        report_file.write_text("# Review\n\nVerdict: CHANGES_REQUESTED")

        printed_lines: list[str] = []

        def capture_print(*args, **kwargs):
            printed_lines.append(str(args[0]) if args else "")

        with patch('gza.runner.console') as mock_console, \
             patch('gza.runner.post_review_to_pr'):
            mock_console.print.side_effect = capture_print
            exit_code = _run_non_code_task(
                review_task, config, store, mock_provider, mock_git, resume=False
            )

        assert exit_code == 0
        assert "Verdict: " in "\n".join(printed_lines)
        assert "CHANGES_REQUESTED" in "\n".join(printed_lines)

    def test_non_review_task_does_not_suggest_improve(self, tmp_path: Path):
        """Test that explore/plan task completion does NOT suggest gza improve."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        explore_task = store.add(
            prompt="Explore codebase",
            task_type="explore",
        )
        explore_task.task_id = "20260212-explore-codebase"
        store.update(explore_task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False

        mock_provider = Mock()
        mock_provider.name = "MockProvider"
        mock_result = RunResult(
            exit_code=0,
            duration_seconds=5.0,
            num_turns_reported=3,
            cost_usd=0.02,
            session_id="test-session",
            error_type=None,
        )
        mock_provider.run.return_value = mock_result

        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=0)

        worktree_path = config.worktree_path / f"{explore_task.task_id}-explore"
        worktree_explore_dir = worktree_path / ".gza" / "explorations"
        worktree_explore_dir.mkdir(parents=True, exist_ok=True)
        report_file = worktree_explore_dir / f"{explore_task.task_id}.md"
        report_file.write_text("# Exploration\n\nFindings here.")

        printed_lines: list[str] = []

        def capture_print(*args, **kwargs):
            printed_lines.append(str(args[0]) if args else "")

        with patch('gza.runner.console') as mock_console:
            mock_console.print.side_effect = capture_print

            exit_code = _run_non_code_task(
                explore_task, config, store, mock_provider, mock_git, resume=False
            )

            assert exit_code == 0

            all_output = "\n".join(printed_lines)
            assert "gza improve" not in all_output


class TestRunNonCodeTaskDockerGitMetadata:
    """Tests for Docker review execution when worktree git metadata is invalid."""

    def test_docker_review_hides_and_restores_invalid_worktree_git_file(self, tmp_path: Path):
        """Invalid host gitdir metadata should be hidden during provider run and restored after."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Implement feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.task_id = "20260225-implement-feature"
        impl_task.branch = "test/feature-branch"
        store.update(impl_task)

        review_task = store.add(
            prompt="Review implementation",
            task_type="review",
            depends_on=impl_task.id,
        )
        review_task.task_id = "20260225-review-feature"
        store.update(review_task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = True

        worktree_path = config.worktree_path / f"{review_task.task_id}-review"
        worktree_path.mkdir(parents=True, exist_ok=True)
        original_git_file = worktree_path / ".git"
        original_git_content = "gitdir: /nonexistent/host/path/.git/worktrees/review\n"
        original_git_file.write_text(original_git_content)

        worktree_review_dir = worktree_path / ".gza" / "reviews"
        worktree_review_dir.mkdir(parents=True, exist_ok=True)
        report_file = worktree_review_dir / f"{review_task.task_id}.md"

        def provider_run(_config, _prompt, _log_file, _work_dir, resume_session_id=None):
            assert not (worktree_path / ".git").exists()
            assert (worktree_path / ".git.gza-host-worktree").exists()
            report_file.write_text("# Review\n\nVerdict: APPROVED")
            return RunResult(
                exit_code=0,
                duration_seconds=2.0,
                num_turns_reported=1,
                cost_usd=0.01,
                session_id="session-1",
                error_type=None,
            )

        mock_provider = Mock()
        mock_provider.name = "MockProvider"
        mock_provider.run.side_effect = provider_run

        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=0)
        mock_git.get_diff_numstat.return_value = "1\t1\tsrc/app.py\n"
        mock_git.get_diff_stat.return_value = " 1 file changed, 1 insertion(+), 1 deletion(-)"
        mock_git.get_diff.return_value = "diff --git a/src/app.py b/src/app.py\n@@ -1 +1 @@\n-old\n+new\n"

        with patch("gza.runner.post_review_to_pr"):
            exit_code = _run_non_code_task(
                review_task,
                config,
                store,
                mock_provider,
                mock_git,
                resume=False,
            )

        assert exit_code == 0
        assert original_git_file.exists()
        assert original_git_file.read_text() == original_git_content
        assert not (worktree_path / ".git.gza-host-worktree").exists()


class TestRunNonCodeTaskPRPosting:
    """Tests for _run_non_code_task PR posting behavior."""

    def test_run_non_code_task_posts_to_pr_for_review(self, tmp_path: Path):
        """Test that _run_non_code_task calls post_review_to_pr for completed review tasks."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Create a completed implementation task
        impl_task = store.add(
            prompt="Add user authentication",
            task_type="implement",
        )
        impl_task.status = "completed"
        impl_task.task_id = "20260211-add-user-authentication"
        impl_task.branch = "gza/20260211-add-user-authentication"
        impl_task.pr_number = 123
        store.update(impl_task)

        # Create a review task that depends on it
        review_task = store.add(
            prompt="Review the implementation",
            task_type="review",
            depends_on=impl_task.id,
        )
        review_task.task_id = "20260212-review-the-implementation"
        store.update(review_task)

        # Setup config
        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False

        # Create review directory structure
        review_dir = tmp_path / ".gza" / "reviews"
        review_dir.mkdir(parents=True, exist_ok=True)

        # Track if post_review_to_pr was called
        pr_post_called = []

        def mock_post_review_to_pr(review_task, impl_task, store, project_dir, required=False):
            pr_post_called.append({
                'review_id': review_task.id,
                'impl_id': impl_task.id,
                'required': required
            })

        # Mock provider
        mock_provider = Mock()
        mock_provider.name = "MockProvider"
        mock_result = RunResult(
            exit_code=0,
            duration_seconds=10.0,
            num_turns_reported=5,
            cost_usd=0.05,
            session_id="test-session",
            error_type=None,
        )
        mock_provider.run.return_value = mock_result

        # Mock git
        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=0)
        mock_git.get_diff_numstat.return_value = ""
        mock_git.get_diff.return_value = ""

        # Create worktree directory and report file (simulating provider writing it)
        worktree_path = config.worktree_path / f"{review_task.task_id}-review"
        worktree_review_dir = worktree_path / ".gza" / "reviews"
        worktree_review_dir.mkdir(parents=True, exist_ok=True)
        report_file = worktree_review_dir / f"{review_task.task_id}.md"
        report_file.write_text("# Review\n\nLooks good!")

        import gza.runner
        original_post_review = gza.runner.post_review_to_pr
        gza.runner.post_review_to_pr = mock_post_review_to_pr

        try:
            # Call _run_non_code_task
            exit_code = _run_non_code_task(
                review_task, config, store, mock_provider, mock_git, resume=False
            )

            # Verify success
            assert exit_code == 0

            # Verify post_review_to_pr was called
            assert len(pr_post_called) == 1
            assert pr_post_called[0]['review_id'] == review_task.id
            assert pr_post_called[0]['impl_id'] == impl_task.id
            assert pr_post_called[0]['required'] is False
        finally:
            gza.runner.post_review_to_pr = original_post_review


class TestMaxStepsHandling:
    """Tests for max-steps behavior in runner integration."""

    def test_run_result_to_stats_includes_step_fields(self):
        """Step metrics should be transferred from RunResult to TaskStats."""
        result = RunResult(
            exit_code=0,
            duration_seconds=12.3,
            num_steps_reported=7,
            num_steps_computed=8,
            num_turns_reported=5,
            num_turns_computed=5,
            cost_usd=0.12,
            input_tokens=100,
            output_tokens=200,
        )
        stats = _run_result_to_stats(result)
        assert stats.num_steps_reported == 7
        assert stats.num_steps_computed == 8
        assert stats.num_turns_reported == 5

    def test_non_code_task_marks_max_steps_failure_reason(self, tmp_path: Path):
        """Provider max_steps errors should be stored as MAX_STEPS."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(prompt="Plan task", task_type="plan")
        task.task_id = "20260225-plan-task"
        store.update(task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.timeout_minutes = 10
        config.max_steps = 2

        mock_provider = Mock()
        mock_provider.name = "MockProvider"
        mock_provider.run.return_value = RunResult(
            exit_code=0,
            duration_seconds=4.2,
            num_steps_computed=3,
            error_type="max_steps",
        )

        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=0)

        with patch("gza.runner.console"):
            exit_code = _run_non_code_task(task, config, store, mock_provider, mock_git)

        assert exit_code == 0
        failed = store.get(task.id)
        assert failed is not None
        assert failed.status == "failed"
        assert failed.failure_reason == "MAX_STEPS"

    def test_run_non_code_task_skips_pr_posting_for_explore(self, tmp_path: Path):
        """Test that _run_non_code_task does NOT call post_review_to_pr for explore tasks."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Create an explore task
        explore_task = store.add(
            prompt="Explore the codebase",
            task_type="explore",
        )
        explore_task.task_id = "20260212-explore-the-codebase"
        store.update(explore_task)

        # Setup config
        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False

        # Create exploration directory structure
        explore_dir = tmp_path / ".gza" / "explorations"
        explore_dir.mkdir(parents=True, exist_ok=True)

        # Track if post_review_to_pr was called
        pr_post_called = []

        def mock_post_review_to_pr(review_task, impl_task, store, project_dir, required=False):
            pr_post_called.append(True)

        # Mock provider
        mock_provider = Mock()
        mock_provider.name = "MockProvider"
        mock_result = RunResult(
            exit_code=0,
            duration_seconds=10.0,
            num_turns_reported=5,
            cost_usd=0.05,
            session_id="test-session",
            error_type=None,
        )
        mock_provider.run.return_value = mock_result

        # Mock git
        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=0)

        # Create worktree directory and report file
        worktree_path = config.worktree_path / f"{explore_task.task_id}-explore"
        worktree_explore_dir = worktree_path / ".gza" / "explorations"
        worktree_explore_dir.mkdir(parents=True, exist_ok=True)
        report_file = worktree_explore_dir / f"{explore_task.task_id}.md"
        report_file.write_text("# Exploration\n\nFindings here.")

        import gza.runner
        original_post_review = gza.runner.post_review_to_pr
        gza.runner.post_review_to_pr = mock_post_review_to_pr

        try:
            # Call _run_non_code_task
            exit_code = _run_non_code_task(
                explore_task, config, store, mock_provider, mock_git, resume=False
            )

            # Verify success
            assert exit_code == 0

            # Verify post_review_to_pr was NOT called (not a review task)
            assert len(pr_post_called) == 0
        finally:
            gza.runner.post_review_to_pr = original_post_review


class TestResumeVerificationPrompt:
    """Tests for resume verification prompt injection."""

    def test_resume_code_task_includes_verification_prompt(self, tmp_path: Path):
        """Test that resuming a code task includes todo list verification instructions."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Create a failed task with session_id
        task = store.add(
            prompt="Implement feature X",
            task_type="implement",
        )
        task.task_id = "20260212-implement-feature-x"
        task.branch = "gza/20260212-implement-feature-x"
        task.session_id = "test-session-123"
        store.mark_failed(task, log_file="logs/test.log", stats=None)

        # Setup config
        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.db_path = db_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.max_turns = 50
        config.timeout_minutes = 60
        config.branch_mode = "multi"
        config.project_name = "test"
        config.branch_strategy = Mock()
        config.branch_strategy.pattern = "{project}/{task_id}"
        config.branch_strategy.default_type = "feature"

        # Mock provider to capture the prompt
        captured_prompts = []

        def mock_provider_run(config, prompt, log_file, work_dir, resume_session_id=None):
            captured_prompts.append({
                'prompt': prompt,
                'resume_session_id': resume_session_id
            })
            return RunResult(
                exit_code=0,
                duration_seconds=10.0,
                num_turns_reported=5,
                cost_usd=0.05,
                session_id="test-session-123",
                error_type=None,
            )

        # Mock provider and git
        with patch('gza.runner.get_provider') as mock_get_provider, \
             patch('gza.runner.Git') as mock_git_class, \
             patch('gza.runner.load_dotenv'):

            mock_provider = Mock()
            mock_provider.name = "TestProvider"
            mock_provider.check_credentials.return_value = True
            mock_provider.verify_credentials.return_value = True
            mock_provider.run = mock_provider_run
            mock_get_provider.return_value = mock_provider

            # Mock Git
            mock_git = Mock()
            mock_git.default_branch.return_value = "main"
            mock_git._run.return_value = Mock(returncode=0)
            mock_git.branch_exists.return_value = True
            mock_git.worktree_add = Mock()
            mock_git.worktree_list.return_value = []

            # Mock has_changes to return True
            mock_worktree_git = Mock()
            mock_worktree_git.has_changes.return_value = True
            mock_worktree_git.add = Mock()
            mock_worktree_git.commit = Mock()
            mock_worktree_git.get_diff_numstat.return_value = ""
            # Mock _run for WIP functions (squash, restore)
            mock_log_result = Mock()
            mock_log_result.stdout = "WIP: gza task interrupted"
            mock_worktree_git._run.return_value = mock_log_result

            mock_git_class.side_effect = [mock_git, mock_worktree_git]

            # Create worktree directory
            worktree_path = config.worktree_path / task.task_id
            worktree_path.mkdir(parents=True, exist_ok=True)

            # Create summary file in worktree
            summary_dir = worktree_path / ".gza" / "summaries"
            summary_dir.mkdir(parents=True, exist_ok=True)
            summary_file = summary_dir / f"{task.task_id}.md"
            summary_file.write_text("# Summary\n\nCompleted the task.")

            # Run with resume=True
            result = run(config, task_id=task.id, resume=True)

            # Verify success
            assert result == 0

            # Verify prompt was captured
            assert len(captured_prompts) == 1
            prompt = captured_prompts[0]['prompt']
            resume_session_id = captured_prompts[0]['resume_session_id']

            # Verify verification instructions are in the prompt
            assert "verify your todo list against the actual state" in prompt.lower()
            assert "review your todo list from the previous session" in prompt.lower()
            assert "verify by checking the actual code/files" in prompt.lower()
            assert "update the todo list to reflect what is actually complete" in prompt.lower()
            assert "continue from where you left off" in prompt.lower()

            # Verify resume_session_id was passed
            assert resume_session_id == "test-session-123"

    def test_resume_non_code_task_includes_verification_prompt(self, tmp_path: Path):
        """Test that resuming a non-code task includes todo list verification instructions."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Create a failed review task with session_id
        impl_task = store.add(
            prompt="Implement feature X",
            task_type="implement",
        )
        impl_task.status = "completed"
        impl_task.task_id = "20260212-implement-feature-x"
        impl_task.branch = "gza/20260212-implement-feature-x"
        store.update(impl_task)

        review_task = store.add(
            prompt="Review feature X",
            task_type="review",
            depends_on=impl_task.id,
        )
        review_task.task_id = "20260212-review-feature-x"
        review_task.session_id = "test-session-456"
        store.mark_failed(review_task, log_file="logs/test.log", stats=None)

        # Setup config
        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False

        # Mock provider to capture the prompt
        captured_prompts = []

        def mock_provider_run(config, prompt, log_file, work_dir, resume_session_id=None):
            captured_prompts.append({
                'prompt': prompt,
                'resume_session_id': resume_session_id
            })
            return RunResult(
                exit_code=0,
                duration_seconds=10.0,
                num_turns_reported=5,
                cost_usd=0.05,
                session_id="test-session-456",
                error_type=None,
            )

        mock_provider = Mock()
        mock_provider.name = "TestProvider"
        mock_provider.run = mock_provider_run

        # Mock git
        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=0)
        mock_git.worktree_remove = Mock()

        # Create worktree directory and report file
        worktree_path = config.worktree_path / f"{review_task.task_id}-review"
        worktree_path.mkdir(parents=True, exist_ok=True)
        review_dir = worktree_path / ".gza" / "reviews"
        review_dir.mkdir(parents=True, exist_ok=True)
        report_file = review_dir / f"{review_task.task_id}.md"
        report_file.write_text("# Review\n\nLooks good!")

        # Mock post_review_to_pr to avoid GitHub CLI dependency
        with patch('gza.runner.post_review_to_pr'):
            # Call _run_non_code_task with resume=True
            exit_code = _run_non_code_task(
                review_task, config, store, mock_provider, mock_git, resume=True
            )

        # Verify success
        assert exit_code == 0

        # Verify prompt was captured
        assert len(captured_prompts) == 1
        prompt = captured_prompts[0]['prompt']
        resume_session_id = captured_prompts[0]['resume_session_id']

        # Verify verification instructions are in the prompt
        assert "verify your todo list against the actual state" in prompt.lower()
        assert "review your todo list from the previous session" in prompt.lower()
        assert "verify by checking the actual code/files" in prompt.lower()
        assert "update the todo list to reflect what is actually complete" in prompt.lower()
        assert "continue from where you left off" in prompt.lower()

        # Verify resume_session_id was passed
        assert resume_session_id == "test-session-456"

    def test_normal_run_does_not_include_verification_prompt(self, tmp_path: Path):
        """Test that normal (non-resume) runs use the standard prompt without verification."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Create a pending task
        task = store.add(
            prompt="Implement feature Y",
            task_type="implement",
        )

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        # Build normal prompt (not resume)
        prompt = build_prompt(task, config, store, summary_path=Path("/workspace/.gza/summaries/test.md"))

        # Verify it does NOT include verification instructions
        assert "verify your todo list" not in prompt.lower()
        assert "review your todo list from the previous session" not in prompt.lower()

        # Verify it includes the normal task prompt
        assert "Complete this task: Implement feature Y" in prompt


class TestWIPFunctionality:
    """Tests for WIP (Work In Progress) save/restore functionality."""

    def test_save_wip_changes_creates_commit_and_diff(self, tmp_path: Path):
        """Test that _save_wip_changes creates both a WIP commit and a diff backup."""
        # Setup
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Test task", task_type="implement")
        task.task_id = "20260212-test-task"

        # Create a mock git repo
        worktree_path = tmp_path / "worktree"
        worktree_path.mkdir()
        (worktree_path / "test.txt").write_text("test content")

        # Initialize git repo
        git = Git(worktree_path)
        git._run("init")
        git._run("config", "user.email", "test@example.com")
        git._run("config", "user.name", "Test User")

        # Create config
        config = Mock(spec=Config)
        config.project_dir = tmp_path

        # Save WIP changes
        _save_wip_changes(task, git, config, "test-branch")

        # Verify WIP commit was created
        log = git._run("log", "-1", "--pretty=%s").stdout.strip()
        assert log == "WIP: gza task interrupted"

        # Verify diff backup was created
        wip_file = tmp_path / WIP_DIR / "20260212-test-task.diff"
        assert wip_file.exists()
        diff_content = wip_file.read_text()
        assert "test.txt" in diff_content

    def test_save_wip_changes_with_no_changes(self, tmp_path: Path):
        """Test that _save_wip_changes does nothing when there are no changes."""
        # Setup
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Test task", task_type="implement")
        task.task_id = "20260212-test-task"

        # Create a git repo with initial commit
        worktree_path = tmp_path / "worktree"
        worktree_path.mkdir()
        git = Git(worktree_path)
        git._run("init")
        git._run("config", "user.email", "test@example.com")
        git._run("config", "user.name", "Test User")
        (worktree_path / "initial.txt").write_text("initial")
        git.add(".")
        git.commit("Initial commit")

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        # Save WIP changes (should do nothing)
        _save_wip_changes(task, git, config, "test-branch")

        # Verify no new commit was created
        log = git._run("log", "-1", "--pretty=%s").stdout.strip()
        assert log == "Initial commit"

        # Verify no WIP diff was created
        wip_file = tmp_path / WIP_DIR / "20260212-test-task.diff"
        assert not wip_file.exists()

    def test_restore_wip_changes_finds_wip_commit(self, tmp_path: Path):
        """Test that _restore_wip_changes detects existing WIP commit."""
        # Setup
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Test task", task_type="implement")
        task.task_id = "20260212-test-task"

        # Create a git repo with WIP commit
        worktree_path = tmp_path / "worktree"
        worktree_path.mkdir()
        git = Git(worktree_path)
        git._run("init")
        git._run("config", "user.email", "test@example.com")
        git._run("config", "user.name", "Test User")
        (worktree_path / "test.txt").write_text("test")
        git.add(".")
        git.commit("WIP: gza task interrupted\n\nTask ID: 20260212-test-task")

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        # Restore WIP changes (should detect existing commit)
        _restore_wip_changes(task, git, config, "test-branch")

        # Verify commit is still there (not modified)
        log = git._run("log", "-1", "--pretty=%s").stdout.strip()
        assert log == "WIP: gza task interrupted"

    def test_restore_wip_changes_applies_diff_backup(self, tmp_path: Path):
        """Test that _restore_wip_changes applies diff backup when no WIP commit exists."""
        # Setup
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Test task", task_type="implement")
        task.task_id = "20260212-test-task"

        # Create a git repo
        worktree_path = tmp_path / "worktree"
        worktree_path.mkdir()
        git = Git(worktree_path)
        git._run("init")
        git._run("config", "user.email", "test@example.com")
        git._run("config", "user.name", "Test User")
        (worktree_path / "initial.txt").write_text("initial")
        git.add(".")
        git.commit("Initial commit")

        # Create a WIP diff backup
        wip_dir = tmp_path / WIP_DIR
        wip_dir.mkdir(parents=True)
        wip_file = wip_dir / "20260212-test-task.diff"
        diff_content = """diff --git a/test.txt b/test.txt
new file mode 100644
index 0000000..9daeafb
--- /dev/null
+++ b/test.txt
@@ -0,0 +1 @@
+test
"""
        wip_file.write_text(diff_content)

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        # Restore WIP changes
        _restore_wip_changes(task, git, config, "test-branch")

        # Verify diff was applied and committed
        log = git._run("log", "-1", "--pretty=%s").stdout.strip()
        assert log == "WIP: restored from diff"

    def test_squash_wip_commits(self, tmp_path: Path):
        """Test that _squash_wip_commits squashes WIP commits."""
        # Setup
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Test task", task_type="implement")
        task.task_id = "20260212-test-task"

        # Create a git repo with multiple WIP commits
        worktree_path = tmp_path / "worktree"
        worktree_path.mkdir()
        git = Git(worktree_path)
        git._run("init")
        git._run("config", "user.email", "test@example.com")
        git._run("config", "user.name", "Test User")

        # Initial commit
        (worktree_path / "initial.txt").write_text("initial")
        git.add(".")
        git.commit("Initial commit")

        # WIP commit 1
        (worktree_path / "wip1.txt").write_text("wip1")
        git.add(".")
        git.commit("WIP: first attempt")

        # WIP commit 2
        (worktree_path / "wip2.txt").write_text("wip2")
        git.add(".")
        git.commit("WIP: second attempt")

        # Verify we have 3 commits
        log_before = git._run("log", "--oneline").stdout.strip().split("\n")
        assert len(log_before) == 3

        # Squash WIP commits
        _squash_wip_commits(git, task)

        # Verify we're back to 1 commit with changes staged
        log_after = git._run("log", "--oneline").stdout.strip().split("\n")
        assert len(log_after) == 1
        assert log_after[0].endswith("Initial commit")

        # Verify changes are staged
        assert git.has_changes(".", include_untracked=False)

    def test_squash_wip_commits_with_no_wip_commits(self, tmp_path: Path):
        """Test that _squash_wip_commits does nothing when there are no WIP commits."""
        # Setup
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Test task", task_type="implement")
        task.task_id = "20260212-test-task"

        # Create a git repo with normal commits
        worktree_path = tmp_path / "worktree"
        worktree_path.mkdir()
        git = Git(worktree_path)
        git._run("init")
        git._run("config", "user.email", "test@example.com")
        git._run("config", "user.name", "Test User")

        (worktree_path / "test.txt").write_text("test")
        git.add(".")
        git.commit("Normal commit")

        # Squash WIP commits (should do nothing)
        _squash_wip_commits(git, task)

        # Verify commit is unchanged
        log = git._run("log", "-1", "--pretty=%s").stdout.strip()
        assert log == "Normal commit"


class TestBackupDatabase:
    """Tests for backup_database function."""

    def test_creates_backup_when_none_exists(self, tmp_path: Path):
        """backup_database creates a backup file for the current hour."""
        import sqlite3

        # Create a source database
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
        conn.close()

        backup_database(db_path, tmp_path)

        backup_dir = tmp_path / BACKUP_DIR
        assert backup_dir.exists()

        from datetime import datetime
        hour_stamp = datetime.now().strftime("%Y%m%d%H")
        backup_file = backup_dir / f"gza-{hour_stamp}.db"
        assert backup_file.exists()
        assert backup_file.stat().st_size > 0

    def test_skips_backup_when_current_hour_exists(self, tmp_path: Path):
        """backup_database does not create a second backup in the same hour."""
        import sqlite3
        from datetime import datetime

        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
        conn.close()

        backup_dir = tmp_path / BACKUP_DIR
        backup_dir.mkdir(parents=True, exist_ok=True)

        hour_stamp = datetime.now().strftime("%Y%m%d%H")
        existing_backup = backup_dir / f"gza-{hour_stamp}.db"
        existing_backup.write_bytes(b"placeholder")
        original_mtime = existing_backup.stat().st_mtime

        backup_database(db_path, tmp_path)

        # File should not have been replaced
        assert existing_backup.stat().st_mtime == original_mtime
        assert existing_backup.read_bytes() == b"placeholder"

    def test_skips_backup_when_db_does_not_exist(self, tmp_path: Path):
        """backup_database does nothing when the source database is missing."""
        db_path = tmp_path / ".gza" / "gza.db"

        # Should not raise and should not create any backup dir
        backup_database(db_path, tmp_path)

        backup_dir = tmp_path / BACKUP_DIR
        assert not backup_dir.exists()

    def test_backup_is_valid_sqlite_database(self, tmp_path: Path):
        """The created backup is a valid SQLite database with the same content."""
        import sqlite3
        from datetime import datetime

        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
        conn.execute("INSERT INTO items VALUES (1, 'hello')")
        conn.commit()
        conn.close()

        backup_database(db_path, tmp_path)

        hour_stamp = datetime.now().strftime("%Y%m%d%H")
        backup_path = tmp_path / BACKUP_DIR / f"gza-{hour_stamp}.db"
        assert backup_path.exists()

        backup_conn = sqlite3.connect(str(backup_path))
        rows = backup_conn.execute("SELECT id, name FROM items").fetchall()
        backup_conn.close()

        assert rows == [(1, "hello")]


class TestNoChangesWithExistingCommits:
    """Tests for the fix that prevents false 'No changes made' failure on resume."""

    def _make_config(self, tmp_path: Path, db_path: Path) -> Mock:
        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.db_path = db_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.max_turns = 50
        config.timeout_minutes = 60
        config.branch_mode = "multi"
        config.project_name = "test"
        config.branch_strategy = Mock()
        config.branch_strategy.pattern = "{project}/{task_id}"
        config.branch_strategy.default_type = "feature"
        return config

    def test_resume_with_existing_commits_and_no_new_changes_succeeds(self, tmp_path: Path):
        """When resuming, if there are no uncommitted changes but the branch already has
        commits from a previous run, the task should succeed (not fail with 'No changes made')."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(
            prompt="Implement feature X",
            task_type="implement",
        )
        task.task_id = "20260212-implement-feature-x"
        task.branch = "test/20260212-implement-feature-x"
        task.session_id = "test-session-123"
        store.mark_failed(task, log_file="logs/test.log", stats=None)

        config = self._make_config(tmp_path, db_path)

        def mock_provider_run(cfg, prompt, log_file, work_dir, resume_session_id=None):
            return RunResult(
                exit_code=0,
                duration_seconds=5.0,
                num_turns_reported=2,
                cost_usd=0.02,
                session_id="test-session-123",
                error_type=None,
            )

        with patch('gza.runner.get_provider') as mock_get_provider, \
             patch('gza.runner.Git') as mock_git_class, \
             patch('gza.runner.load_dotenv'):

            mock_provider = Mock()
            mock_provider.name = "TestProvider"
            mock_provider.check_credentials.return_value = True
            mock_provider.verify_credentials.return_value = True
            mock_provider.run = mock_provider_run
            mock_get_provider.return_value = mock_provider

            mock_git = Mock()
            mock_git.default_branch.return_value = "main"
            mock_git._run.return_value = Mock(returncode=0)
            mock_git.branch_exists.return_value = True
            mock_git.count_commits_ahead.return_value = 0
            mock_git.worktree_add = Mock()
            mock_git.worktree_list.return_value = []

            mock_worktree_git = Mock()
            # No uncommitted changes (task already committed in previous run)
            mock_worktree_git.has_changes.return_value = False
            # Branch has 1 commit from the previous run
            mock_worktree_git.count_commits_ahead.return_value = 1
            mock_worktree_git.default_branch.return_value = "main"
            mock_worktree_git.get_diff_numstat.return_value = ""
            mock_log_result = Mock()
            mock_log_result.stdout = ""
            mock_worktree_git._run.return_value = mock_log_result

            mock_git_class.side_effect = [mock_git, mock_worktree_git]

            worktree_path = config.worktree_path / task.task_id
            worktree_path.mkdir(parents=True, exist_ok=True)

            result = run(config, task_id=task.id, resume=True)

        assert result == 0
        refreshed = store.get(task.id)
        assert refreshed.status == "completed", f"Expected 'completed', got '{refreshed.status}'"

    def test_no_changes_and_no_prior_commits_still_fails(self, tmp_path: Path):
        """When there are no uncommitted changes AND no commits on the branch,
        the task should still fail with 'No changes made'."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(
            prompt="Implement feature Y",
            task_type="implement",
        )
        task.task_id = "20260212-implement-feature-y"
        task.branch = "test/20260212-implement-feature-y"
        store.mark_in_progress(task)

        config = self._make_config(tmp_path, db_path)

        def mock_provider_run(cfg, prompt, log_file, work_dir, resume_session_id=None):
            return RunResult(
                exit_code=0,
                duration_seconds=5.0,
                num_turns_reported=2,
                cost_usd=0.02,
                session_id=None,
                error_type=None,
            )

        with patch('gza.runner.get_provider') as mock_get_provider, \
             patch('gza.runner.Git') as mock_git_class, \
             patch('gza.runner.load_dotenv'):

            mock_provider = Mock()
            mock_provider.name = "TestProvider"
            mock_provider.check_credentials.return_value = True
            mock_provider.verify_credentials.return_value = True
            mock_provider.run = mock_provider_run
            mock_get_provider.return_value = mock_provider

            mock_git = Mock()
            mock_git.default_branch.return_value = "main"
            mock_git._run.return_value = Mock(returncode=0)
            mock_git.branch_exists.return_value = False
            mock_git.count_commits_ahead.return_value = 0
            mock_git.worktree_add = Mock()
            mock_git.worktree_list.return_value = []

            mock_worktree_git = Mock()
            # No uncommitted changes
            mock_worktree_git.has_changes.return_value = False
            # No prior commits on the branch either
            mock_worktree_git.count_commits_ahead.return_value = 0
            mock_worktree_git.default_branch.return_value = "main"
            mock_log_result = Mock()
            mock_log_result.stdout = ""
            mock_worktree_git._run.return_value = mock_log_result

            mock_git_class.side_effect = [mock_git, mock_worktree_git]

            worktree_path = config.worktree_path / task.task_id
            worktree_path.mkdir(parents=True, exist_ok=True)

            result = run(config, task_id=task.id, resume=False)

        assert result == 0
        refreshed = store.get(task.id)
        assert refreshed.status == "failed", f"Expected 'failed', got '{refreshed.status}'"
