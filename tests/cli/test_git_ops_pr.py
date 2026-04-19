"""Tests for git operations CLI commands."""


import argparse
import io
import os
import shutil
import time
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from gza.cli import _determine_advance_action, cmd_advance
from gza.config import Config
from gza.db import SqliteTaskStore

from .conftest import (
    make_store,
    run_gza,
    setup_config,
    setup_db_with_tasks,
    setup_git_repo_with_task_branch,
)


class TestPrCommand:
    """Tests for 'gza pr' command."""

    def _make_completed_pr_task(self, tmp_path: Path, *, branch: str, pr_number: int | None = None):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Completed task ready for PR")
        task.status = "completed"
        task.branch = branch
        task.has_commits = True
        task.pr_number = pr_number
        store.update(task)
        return store, task

    def test_pr_task_not_found(self, tmp_path: Path):
        """PR command handles nonexistent task."""
        setup_config(tmp_path)

        # Create empty database
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        make_store(tmp_path)

        result = run_gza("pr", "testproject-999999", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "not found" in result.stdout

    def test_pr_task_not_completed(self, tmp_path: Path):
        """PR command rejects pending tasks."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Pending task", "status": "pending"},
        ])

        result = run_gza("pr", "testproject-1", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "not completed" in result.stdout

    def test_pr_task_no_branch(self, tmp_path: Path):
        """PR command rejects tasks without branches."""

        setup_config(tmp_path)

        store = make_store(tmp_path)
        task = store.add("Completed task without branch")
        task.status = "completed"
        task.branch = None
        task.has_commits = True
        store.update(task)

        result = run_gza("pr", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 1
        assert "no branch" in result.stdout

    def test_pr_task_no_commits(self, tmp_path: Path):
        """PR command rejects tasks without commits."""

        setup_config(tmp_path)

        store = make_store(tmp_path)
        task = store.add("Completed task without commits")
        task.status = "completed"
        task.branch = "feature/test"
        task.has_commits = False
        store.update(task)

        result = run_gza("pr", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 1
        assert "no commits" in result.stdout

    def test_pr_task_marked_merged_shows_distinct_error(self, tmp_path: Path):
        """PR command shows a distinct error message for tasks marked merged via --mark-only."""

        setup_config(tmp_path)

        store = make_store(tmp_path)
        task = store.add("Mark-only merged task")
        task.status = "completed"
        task.branch = "feature/mark-only-pr"
        task.has_commits = True
        task.merge_status = "merged"
        store.update(task)

        result = run_gza("pr", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 1
        assert "already marked as merged" in result.stdout
        # Should NOT say "merged into" since the branch was not actually merged
        assert "merged into" not in result.stdout

    def test_pr_cached_pr_still_errors_when_branch_is_merged(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        """Merged branches must still error even if the task has a cached PR number."""
        import argparse

        from gza.cli.git_ops import cmd_pr

        store, task = self._make_completed_pr_task(
            tmp_path,
            branch="feature/cached-pr-merged",
            pr_number=42,
        )

        git = Mock()
        git.default_branch.return_value = "main"
        git.get_log.return_value = "abc123 test"
        git.get_diff_stat.return_value = "1 file changed"
        git.needs_push.return_value = False
        git.is_merged.return_value = True

        gh = Mock()
        gh.is_available.return_value = True

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=str(task.id),
            title="Manual title",
            draft=False,
        )

        with (
            patch("gza.cli.git_ops.get_store", return_value=store),
            patch("gza.cli.git_ops.Git", return_value=git),
            patch("gza.pr_ops.GitHub", return_value=gh),
        ):
            rc = cmd_pr(args)

        output = capsys.readouterr().out
        assert rc == 1
        assert "already merged into main" in output

    def test_pr_existing_remote_pr_still_errors_when_branch_is_merged(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ):
        """Merged branches must still error before reusing an existing remote PR."""
        import argparse

        from gza.cli.git_ops import cmd_pr

        store, task = self._make_completed_pr_task(
            tmp_path,
            branch="feature/remote-pr-merged",
        )

        git = Mock()
        git.default_branch.return_value = "main"
        git.get_log.return_value = "abc123 test"
        git.get_diff_stat.return_value = "1 file changed"
        git.needs_push.return_value = False
        git.is_merged.return_value = True

        gh = Mock()
        gh.is_available.return_value = True

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=str(task.id),
            title="Manual title",
            draft=False,
        )

        with (
            patch("gza.cli.git_ops.get_store", return_value=store),
            patch("gza.cli.git_ops.Git", return_value=git),
            patch("gza.pr_ops.GitHub", return_value=gh),
        ):
            rc = cmd_pr(args)

        output = capsys.readouterr().out
        assert rc == 1
        assert "already merged into main" in output
        gh.pr_exists.assert_not_called()

    def test_pr_reuses_cached_pr_without_claiming_push_when_helper_did_not_push(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ):
        """`gza pr` should not print a push banner unless the shared helper actually pushes."""
        import argparse

        from gza.cli.git_ops import cmd_pr

        store, task = self._make_completed_pr_task(
            tmp_path,
            branch="feature/cached-pr",
            pr_number=42,
        )

        git = Mock()
        git.default_branch.return_value = "main"
        git.get_log.return_value = "abc123 test"
        git.get_diff_stat.return_value = "1 file changed"
        git.needs_push.return_value = True

        ensure_result = Mock(ok=True, status="cached", pr_url="https://github.com/o/r/pull/42", pr_number=42)
        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=str(task.id),
            title="Manual title",
            draft=False,
        )

        with (
            patch("gza.cli.git_ops.get_store", return_value=store),
            patch("gza.cli.git_ops.Git", return_value=git),
            patch("gza.cli.git_ops.ensure_task_pr", return_value=ensure_result) as ensure_pr,
        ):
            rc = cmd_pr(args)

        output = capsys.readouterr().out
        assert rc == 0
        assert "Pushing branch" not in output
        assert "PR already exists: #42" in output
        ensure_pr.assert_called_once()

    def test_generate_pr_content_uses_internal_task_output(self, tmp_path: Path):
        """PR content generation uses an internal task and parses output_content."""
        from gza.cli.git_ops import _generate_pr_content

        setup_config(tmp_path)
        store = SqliteTaskStore(tmp_path / ".gza" / "gza.db")
        source_task = store.add("Add auth and metrics", task_type="implement")
        source_task.slug = "20260318-impl-auth-and-metrics"
        store.update(source_task)

        def _mock_run(_config, task_id=None, **_kwargs):
            internal_task = store.get(task_id)
            assert internal_task is not None
            assert internal_task.task_type == "internal"
            assert internal_task.skip_learnings is True
            internal_task.status = "completed"
            internal_task.output_content = (
                "TITLE: Add auth and metrics\n\n"
                "BODY:\n"
                "## Summary\nAdds auth and metrics.\n\n"
                "## Changes\n- Added auth\n- Added metrics\n"
            )
            store.update(internal_task)
            return 0

        with patch("gza.cli.git_ops.runner_mod.run", side_effect=_mock_run):
            title, body = _generate_pr_content(
                source_task,
                commit_log="abc123 Add auth",
                diff_stat="1 file changed",
                config=Config.load(tmp_path),
                store=store,
            )

        assert title == "Add auth and metrics"
        assert "## Summary" in body
        assert "Added auth" in body

    def test_generate_pr_content_falls_back_on_malformed_output(self, tmp_path: Path):
        """Malformed internal-task output falls back to deterministic PR content."""
        from gza.cli.git_ops import _generate_pr_content

        setup_config(tmp_path)
        store = SqliteTaskStore(tmp_path / ".gza" / "gza.db")
        source_task = store.add("Add auth and metrics", task_type="implement")
        source_task.slug = "20260318-impl-auth-and-metrics"
        store.update(source_task)

        def _mock_run(_config, task_id=None, **_kwargs):
            internal_task = store.get(task_id)
            assert internal_task is not None
            internal_task.status = "completed"
            internal_task.output_content = "unexpected format without markers"
            store.update(internal_task)
            return 0

        with patch("gza.cli.git_ops.runner_mod.run", side_effect=_mock_run):
            title, body = _generate_pr_content(
                source_task,
                commit_log="abc123 Add auth",
                diff_stat="1 file changed",
                config=Config.load(tmp_path),
                store=store,
            )

        assert title == "Impl auth and metrics"
        assert "## Task Prompt" in body

    def test_generate_pr_content_marks_internal_task_failed_on_runner_exception(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        """Runner exceptions should not leave PR internal tasks in pending/in_progress."""
        from gza.cli.git_ops import _generate_pr_content

        setup_config(tmp_path)
        store = SqliteTaskStore(tmp_path / ".gza" / "gza.db")
        source_task = store.add("Add auth and metrics", task_type="implement")
        source_task.slug = "20260318-impl-auth-and-metrics"
        store.update(source_task)

        with patch(
            "gza.cli.git_ops.runner_mod.run",
            side_effect=RuntimeError("runner exploded"),
        ):
            title, body = _generate_pr_content(
                source_task,
                commit_log="abc123 Add auth",
                diff_stat="1 file changed",
                config=Config.load(tmp_path),
                store=store,
            )

        assert title == "Impl auth and metrics"
        assert "## Task Prompt" in body

        internal_tasks = [task for task in store.get_all() if task.task_type == "internal"]
        assert len(internal_tasks) == 1
        assert internal_tasks[0].status == "failed"
        assert internal_tasks[0].failure_reason == "UNKNOWN"

        captured = capsys.readouterr()
        assert f"internal task {internal_tasks[0].id} failed" in captured.err
