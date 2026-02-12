"""Tests for runner module."""

from pathlib import Path
from unittest.mock import Mock, MagicMock

import pytest

from gza.config import Config
from gza.db import SqliteTaskStore, Task
from gza.runner import build_prompt, SUMMARY_DIR, _create_and_run_review_task, post_review_to_pr


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

    def test_auto_review_calls_pr_posting_on_success(self, tmp_path: Path):
        """Test that _create_and_run_review_task calls post_review_to_pr on successful completion."""
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

        # Track if post_review_to_pr was called
        pr_post_called = []

        def mock_run(config, task_id):
            # Mark the review task as completed
            review_task = store.get(task_id)
            review_task.status = "completed"
            store.update(review_task)
            return 0  # Success

        def mock_post_review_to_pr(review_task, impl_task, store, project_dir, required=False):
            pr_post_called.append({
                'review_id': review_task.id,
                'impl_id': impl_task.id,
                'required': required
            })

        import gza.runner
        original_run = gza.runner.run
        original_post_review = gza.runner.post_review_to_pr
        gza.runner.run = mock_run
        gza.runner.post_review_to_pr = mock_post_review_to_pr

        try:
            # Call _create_and_run_review_task
            exit_code = _create_and_run_review_task(impl_task, config, store)

            # Verify success
            assert exit_code == 0

            # Verify post_review_to_pr was called
            assert len(pr_post_called) == 1
            assert pr_post_called[0]['review_id'] == 2  # The created review task
            assert pr_post_called[0]['impl_id'] == 1   # The implementation task
            assert pr_post_called[0]['required'] is False  # Default behavior
        finally:
            gza.runner.run = original_run
            gza.runner.post_review_to_pr = original_post_review

    def test_auto_review_skips_pr_posting_on_failure(self, tmp_path: Path):
        """Test that _create_and_run_review_task skips PR posting if review fails."""
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

        # Track if post_review_to_pr was called
        pr_post_called = []

        def mock_run(config, task_id):
            # Mark the review task as failed
            review_task = store.get(task_id)
            review_task.status = "failed"
            store.update(review_task)
            return 1  # Failure

        def mock_post_review_to_pr(review_task, impl_task, store, project_dir, required=False):
            pr_post_called.append(True)

        import gza.runner
        original_run = gza.runner.run
        original_post_review = gza.runner.post_review_to_pr
        gza.runner.run = mock_run
        gza.runner.post_review_to_pr = mock_post_review_to_pr

        try:
            # Call _create_and_run_review_task
            exit_code = _create_and_run_review_task(impl_task, config, store)

            # Verify failure
            assert exit_code == 1

            # Verify post_review_to_pr was NOT called (review failed)
            assert len(pr_post_called) == 0
        finally:
            gza.runner.run = original_run
            gza.runner.post_review_to_pr = original_post_review
