"""Tests for git operations CLI commands."""


import argparse
import io
import os
import time
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

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


class TestMergeCommand:
    """Tests for 'gza merge' command."""

    @pytest.mark.parametrize(
        "flags, allowed_error",
        [
            pytest.param(["--squash"], "Error merging", id="squash"),
            pytest.param(["--rebase"], "Error during rebase", id="rebase"),
            pytest.param(["--rebase", "--resolve"], "Error during rebase", id="rebase-resolve"),
        ],
    )
    def test_merge_accepts_valid_flags(self, tmp_path: Path, flags: list[str], allowed_error: str):
        """Merge command accepts valid flag combinations."""
        store, _git, task, _wt = setup_git_repo_with_task_branch(
            tmp_path, "Test merge flags", "feature/test-flags",
        )

        result = run_gza("merge", str(task.id), *flags, "--project", str(tmp_path))

        assert result.returncode == 0 or allowed_error in result.stdout

    @pytest.mark.parametrize(
        "flags, expected_error",
        [
            pytest.param(
                ["--rebase", "--squash"],
                "Cannot use --rebase and --squash together",
                id="rebase-and-squash",
            ),
            pytest.param(
                ["--remote"],
                "--remote requires --rebase",
                id="remote-without-rebase",
            ),
            pytest.param(
                ["--resolve"],
                "--resolve requires --rebase",
                id="resolve-without-rebase",
            ),
        ],
    )
    def test_merge_rejects_invalid_flags(
        self, tmp_path: Path, flags: list[str], expected_error: str
    ):
        """Merge command rejects invalid flag combinations."""
        store, _git, task, _wt = setup_git_repo_with_task_branch(
            tmp_path, "Test invalid flags", "feature/test-invalid",
        )

        result = run_gza("merge", str(task.id), *flags, "--project", str(tmp_path))

        assert result.returncode == 1
        assert expected_error in result.stdout

    def test_merge_rebase_with_remote(self, tmp_path: Path):
        """Merge command accepts --rebase --remote together."""
        from datetime import datetime

        from gza.git import Git

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Initialize a git repo with a remote
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        # Create a bare repo to use as remote
        remote_path = tmp_path / "remote.git"
        remote_path.mkdir()
        git._run("init", "--bare", str(remote_path))

        # Add remote and push
        git._run("remote", "add", "origin", str(remote_path))
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")
        git._run("push", "-u", "origin", "main")

        # Create a task with a branch
        task = store.add("Test rebase with remote")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = "feature/test-remote-rebase"
        store.update(task)

        # Create the branch and add a commit
        git._run("checkout", "-b", "feature/test-remote-rebase")
        (tmp_path / "feature.txt").write_text("feature content")
        git._run("add", "feature.txt")
        git._run("commit", "-m", "Add feature")
        git._run("checkout", "main")

        # Test that --rebase --remote flags work together
        result = run_gza("merge", str(task.id), "--rebase", "--remote", "--project", str(tmp_path))

        # Verify the command doesn't fail due to argument parsing
        assert "unrecognized arguments" not in result.stderr
        # Should either succeed or fail gracefully (not due to flag validation)
        assert "--remote requires --rebase" not in result.stdout

    def test_merge_requires_default_branch(self, tmp_path: Path):
        """gza merge errors out when run from a non-default branch."""
        _store, git, task, _wt = setup_git_repo_with_task_branch(
            tmp_path, "Require default branch", "feature/test-require-default",
        )

        git._run("checkout", "-b", "integration")

        result = run_gza("merge", str(task.id), "--project", str(tmp_path), cwd=tmp_path)

        assert result.returncode == 1
        assert "`gza merge` must be run from the default branch 'main'" in result.stdout
        assert "currently on 'integration'" in result.stdout

    def test_mark_only_allowed_from_non_default_branch(self, tmp_path: Path):
        """--mark-only bypasses the default-branch guard since it only updates the DB."""
        store, git, task, _wt = setup_git_repo_with_task_branch(
            tmp_path, "Mark only from non-default", "feature/test-mark-only-non-default",
        )

        git._run("checkout", "-b", "integration")

        result = run_gza(
            "merge", str(task.id), "--mark-only", "--project", str(tmp_path), cwd=tmp_path,
        )

        assert result.returncode == 0
        assert "Marked task" in result.stdout
        assert "--mark-only on non-default branch 'integration'" in result.stdout
        assert "default is 'main'" in result.stdout
        updated = store.get(task.id)
        assert updated is not None
        assert updated.merge_status == "merged"


    def test_squash_merge_creates_commit(self, tmp_path: Path):
        """Squash merge creates a commit, not just staged changes."""
        from datetime import datetime

        from gza.git import Git

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Initialize a git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        # Create initial commit on main
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Get the commit count before merge
        commits_before = git._run("rev-list", "--count", "HEAD")
        commit_count_before = int(commits_before.stdout.strip())

        # Create a task with a branch
        task = store.add("Add feature X")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = "feature/test-squash"
        store.update(task)

        # Create the branch and add multiple commits
        git._run("checkout", "-b", "feature/test-squash")
        (tmp_path / "feature1.txt").write_text("feature content 1")
        git._run("add", "feature1.txt")
        git._run("commit", "-m", "Add feature part 1")
        (tmp_path / "feature2.txt").write_text("feature content 2")
        git._run("add", "feature2.txt")
        git._run("commit", "-m", "Add feature part 2")
        git._run("checkout", "main")

        # Perform squash merge
        result = run_gza("merge", str(task.id), "--squash", "--project", str(tmp_path))

        # Verify the merge succeeded
        assert result.returncode == 0
        assert "Successfully squash merged" in result.stdout

        # Verify a commit was created (not just staged changes)
        commits_after = git._run("rev-list", "--count", "HEAD")
        commit_count_after = int(commits_after.stdout.strip())
        assert commit_count_after == commit_count_before + 1, "Expected one new commit"

        # Verify no staged changes remain
        staged_result = git._run("diff", "--cached", "--quiet", check=False)
        assert staged_result.returncode == 0, "Expected no staged changes after squash merge"

        # Verify the feature files are present
        assert (tmp_path / "feature1.txt").exists()
        assert (tmp_path / "feature2.txt").exists()

    def test_squash_merge_commit_message_includes_task_info(self, tmp_path: Path):
        """Squash merge commit message includes task information."""
        from datetime import datetime

        from gza.git import Git

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Initialize a git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        # Create initial commit on main
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Create a task with a descriptive prompt
        task_prompt = "Implement user authentication with JWT tokens"
        task = store.add(task_prompt)
        task.slug = "20260401-impl-auth-jwt"
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = "feature/auth"
        store.update(task)

        # Create the branch and add a commit
        git._run("checkout", "-b", "feature/auth")
        (tmp_path / "auth.txt").write_text("authentication code")
        git._run("add", "auth.txt")
        git._run("commit", "-m", "Add auth")
        git._run("checkout", "main")

        # Perform squash merge
        result = run_gza("merge", str(task.id), "--squash", "--project", str(tmp_path))
        assert result.returncode == 0

        # Get the commit message
        log_result = git._run("log", "-1", "--pretty=%B")
        commit_message = log_result.stdout.strip()

        # Verify the commit message contains task information
        assert f"Task {task.id}" in commit_message, "Commit message should include task ID"
        assert f"Slug: {task.slug}" in commit_message, "Commit message should include task slug"
        assert "Squash merge" in commit_message, "Commit message should indicate squash merge"

    def test_branch_shows_as_merged_after_squash(self, tmp_path: Path):
        """Branch shows as merged in 'gza unmerged' after squash merge completes."""
        from datetime import datetime

        from gza.git import Git

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Initialize a git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        # Create initial commit on main
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Create a task with a branch
        task = store.add("Add cool feature")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = "feature/cool"
        store.update(task)

        # Create the branch and add a commit
        git._run("checkout", "-b", "feature/cool")
        (tmp_path / "cool.txt").write_text("cool feature")
        git._run("add", "cool.txt")
        git._run("commit", "-m", "Add cool feature")
        git._run("checkout", "main")

        # Verify branch is not merged before squash using git directly
        is_merged_before = git.is_merged(task.branch, "main")
        assert not is_merged_before, "Branch should not be merged before squash merge"

        # Perform squash merge
        result = run_gza("merge", str(task.id), "--squash", "--project", str(tmp_path))
        assert result.returncode == 0

        # Verify branch now shows as merged using git directly
        is_merged_after = git.is_merged(task.branch, "main")
        assert is_merged_after, "Branch should be detected as merged after squash merge"

        # Verify the cool.txt file is present in main
        assert (tmp_path / "cool.txt").exists(), "Feature file should exist in main after merge"

    def test_mark_only_preserves_branch_and_marks_merged(self, tmp_path: Path):
        """--mark-only flag sets merge_status without deleting the branch."""
        from datetime import datetime

        from gza.db import SqliteTaskStore
        from gza.git import Git

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        from gza.config import Config
        config = Config.load(tmp_path)
        store = SqliteTaskStore(db_path, prefix=config.project_prefix)

        # Initialize a git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        # Create initial commit on main
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Create a task with a branch
        task = store.add("Test mark-only")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = "feature/mark-only"
        store.update(task)

        # Create the branch and add a commit
        git._run("checkout", "-b", "feature/mark-only")
        (tmp_path / "feature.txt").write_text("feature content")
        git._run("add", "feature.txt")
        git._run("commit", "-m", "Add feature")
        git._run("checkout", "main")

        # Verify branch exists
        assert git.branch_exists("feature/mark-only")
        assert not git.is_merged("feature/mark-only", "main")

        # Run merge with --mark-only
        result = run_gza("merge", str(task.id), "--mark-only", "--project", str(tmp_path))

        # Verify success
        assert result.returncode == 0
        assert f"Marked task {task.id} as merged" in result.stdout

        # Verify branch was NOT deleted
        assert git.branch_exists("feature/mark-only")

        # Verify merge_status was set in the database
        updated_task = store.get(task.id)
        assert updated_task is not None
        assert updated_task.merge_status == "merged"

    def test_mark_only_rejects_conflicting_flags(self, tmp_path: Path):
        """--mark-only flag rejects conflicting flags."""
        from datetime import datetime

        from gza.git import Git

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Initialize a git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        # Create initial commit on main
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Create a task with a branch
        task = store.add("Test conflicting flags")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = "feature/test"
        store.update(task)

        # Create the branch
        git._run("checkout", "-b", "feature/test")
        (tmp_path / "feature.txt").write_text("feature content")
        git._run("add", "feature.txt")
        git._run("commit", "-m", "Add feature")
        git._run("checkout", "main")

        # Test --mark-only with --rebase
        result = run_gza("merge", str(task.id), "--mark-only", "--rebase", "--project", str(tmp_path))
        assert result.returncode == 1
        assert "cannot be used with --rebase, --squash, or --delete" in result.stdout

        # Test --mark-only with --squash
        result = run_gza("merge", str(task.id), "--mark-only", "--squash", "--project", str(tmp_path))
        assert result.returncode == 1
        assert "cannot be used with --rebase, --squash, or --delete" in result.stdout

        # Test --mark-only with --delete
        result = run_gza("merge", str(task.id), "--mark-only", "--delete", "--project", str(tmp_path))
        assert result.returncode == 1
        assert "cannot be used with --rebase, --squash, or --delete" in result.stdout

    def test_mark_only_requires_completed_task(self, tmp_path: Path):
        """--mark-only flag requires task to be completed."""
        from gza.git import Git

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Initialize a git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        # Create initial commit
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Create a task with pending status
        task = store.add("Test pending task")
        task.branch = "feature/pending"
        store.update(task)

        # Create the branch
        git._run("checkout", "-b", "feature/pending")
        (tmp_path / "feature.txt").write_text("feature content")
        git._run("add", "feature.txt")
        git._run("commit", "-m", "Add feature")
        git._run("checkout", "main")

        # Try to mark-only a pending task
        result = run_gza("merge", str(task.id), "--mark-only", "--project", str(tmp_path))
        assert result.returncode == 1
        assert "not completed or unmerged" in result.stdout

    def test_merge_accepts_multiple_task_ids(self, tmp_path: Path):
        """Merge command accepts multiple task IDs."""
        from datetime import datetime

        from gza.git import Git

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Initialize a git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        # Create initial commit on main
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Create first task with a branch
        task1 = store.add("Test merge task 1")
        task1.status = "completed"
        task1.completed_at = datetime.now(UTC)
        task1.branch = "feature/test-1"
        store.update(task1)

        # Create the branch and add a commit
        git._run("checkout", "-b", "feature/test-1")
        (tmp_path / "feature1.txt").write_text("feature 1 content")
        git._run("add", "feature1.txt")
        git._run("commit", "-m", "Add feature 1")
        git._run("checkout", "main")

        # Create second task with a branch
        task2 = store.add("Test merge task 2")
        task2.status = "completed"
        task2.completed_at = datetime.now(UTC)
        task2.branch = "feature/test-2"
        store.update(task2)

        # Create the branch and add a commit
        git._run("checkout", "-b", "feature/test-2")
        (tmp_path / "feature2.txt").write_text("feature 2 content")
        git._run("add", "feature2.txt")
        git._run("commit", "-m", "Add feature 2")
        git._run("checkout", "main")

        # Test merging both tasks
        result = run_gza("merge", str(task1.id), str(task2.id), "--project", str(tmp_path))

        # Verify the command succeeds
        assert result.returncode == 0
        assert "Successfully merged 2 task(s)" in result.stdout
        assert f"{task1.id}" in result.stdout
        assert f"{task2.id}" in result.stdout

    def test_merge_stops_on_first_failure(self, tmp_path: Path):
        """Merge command stops on first failure and reports which tasks were merged."""
        from datetime import datetime

        from gza.git import Git

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Initialize a git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        # Create initial commit on main
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Create first task with a branch (will succeed)
        task1 = store.add("Test merge task 1")
        task1.status = "completed"
        task1.completed_at = datetime.now(UTC)
        task1.branch = "feature/test-1"
        store.update(task1)

        git._run("checkout", "-b", "feature/test-1")
        (tmp_path / "feature1.txt").write_text("feature 1 content")
        git._run("add", "feature1.txt")
        git._run("commit", "-m", "Add feature 1")
        git._run("checkout", "main")

        # Create second task that will fail (no branch)
        task2 = store.add("Test merge task 2 - no branch")
        task2.status = "completed"
        task2.completed_at = datetime.now(UTC)
        store.update(task2)

        # Create third task with a branch (won't be processed)
        task3 = store.add("Test merge task 3")
        task3.status = "completed"
        task3.completed_at = datetime.now(UTC)
        task3.branch = "feature/test-3"
        store.update(task3)

        git._run("checkout", "-b", "feature/test-3")
        (tmp_path / "feature3.txt").write_text("feature 3 content")
        git._run("add", "feature3.txt")
        git._run("commit", "-m", "Add feature 3")
        git._run("checkout", "main")

        # Test merging all three tasks
        result = run_gza("merge", str(task1.id), str(task2.id), str(task3.id), "--project", str(tmp_path))

        # Verify the command fails
        assert result.returncode == 1

        # Verify task 1 was merged successfully
        assert "Successfully merged 1 task(s)" in result.stdout
        assert f"{task1.id}" in result.stdout

        # Verify it stopped at task 2
        assert f"Stopped at task {task2.id}" in result.stdout

        # Verify task 3 is listed as not processed
        assert f"{task3.id}" in result.stdout
        assert "Remaining tasks not processed" in result.stdout

    def test_merge_multiple_with_squash(self, tmp_path: Path):
        """Merge command with --squash flag works with multiple tasks."""
        from datetime import datetime

        from gza.git import Git

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Initialize a git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        # Create initial commit on main
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Create first task with a branch
        task1 = store.add("Test squash merge 1")
        task1.status = "completed"
        task1.completed_at = datetime.now(UTC)
        task1.branch = "feature/squash-1"
        store.update(task1)

        git._run("checkout", "-b", "feature/squash-1")
        (tmp_path / "feature1.txt").write_text("feature 1 content")
        git._run("add", "feature1.txt")
        git._run("commit", "-m", "Add feature 1")
        git._run("checkout", "main")

        # Create second task with a branch
        task2 = store.add("Test squash merge 2")
        task2.status = "completed"
        task2.completed_at = datetime.now(UTC)
        task2.branch = "feature/squash-2"
        store.update(task2)

        git._run("checkout", "-b", "feature/squash-2")
        (tmp_path / "feature2.txt").write_text("feature 2 content")
        git._run("add", "feature2.txt")
        git._run("commit", "-m", "Add feature 2")
        git._run("checkout", "main")

        # Test squash merging both tasks
        result = run_gza("merge", str(task1.id), str(task2.id), "--squash", "--project", str(tmp_path))

        # Verify the command succeeds
        assert result.returncode == 0
        assert "Successfully merged 2 task(s)" in result.stdout
        assert "squash merged" in result.stdout

    def test_merge_no_args_fails(self, tmp_path: Path):
        """Merge command fails with an error when no task_ids and no --all are given."""
        from gza.git import Git

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)

        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        result = run_gza("merge", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "either provide task_id(s) or use --all" in result.stdout

    def test_merge_all_flag_merges_all_unmerged_tasks(self, tmp_path: Path):
        """--all flag finds and merges all unmerged done tasks."""
        from datetime import datetime

        from gza.git import Git

        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Create two completed tasks with branches and commits
        task1 = store.add("Unmerged task 1")
        task1.status = "completed"
        task1.completed_at = datetime.now(UTC)
        task1.branch = "feature/all-1"
        task1.has_commits = True
        store.update(task1)

        git._run("checkout", "-b", "feature/all-1")
        (tmp_path / "all1.txt").write_text("content 1")
        git._run("add", "all1.txt")
        git._run("commit", "-m", "Add all 1")
        git._run("checkout", "main")

        task2 = store.add("Unmerged task 2")
        task2.status = "completed"
        task2.completed_at = datetime.now(UTC)
        task2.branch = "feature/all-2"
        task2.has_commits = True
        store.update(task2)

        git._run("checkout", "-b", "feature/all-2")
        (tmp_path / "all2.txt").write_text("content 2")
        git._run("add", "all2.txt")
        git._run("commit", "-m", "Add all 2")
        git._run("checkout", "main")

        result = run_gza("merge", "--all", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Successfully merged 2 task(s)" in result.stdout

    def test_merge_all_flag_no_unmerged_tasks(self, tmp_path: Path):
        """--all flag reports no tasks when all branches are already merged."""
        from datetime import datetime

        from gza.git import Git

        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Create a completed task whose branch is already merged
        task = store.add("Already merged task")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = "feature/already-merged"
        task.has_commits = True
        store.update(task)

        git._run("checkout", "-b", "feature/already-merged")
        (tmp_path / "merged.txt").write_text("merged content")
        git._run("add", "merged.txt")
        git._run("commit", "-m", "Add merged content")
        git._run("checkout", "main")
        git._run("merge", "feature/already-merged")

        result = run_gza("merge", "--all", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "No unmerged done tasks found" in result.stdout

    def test_merge_all_flag_skips_tasks_without_commits(self, tmp_path: Path):
        """--all flag skips tasks that have no commits (has_commits=False or None)."""
        from datetime import datetime

        from gza.git import Git

        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Task with has_commits=False should be skipped
        task_no_commits = store.add("Task with no commits")
        task_no_commits.status = "completed"
        task_no_commits.completed_at = datetime.now(UTC)
        task_no_commits.branch = "feature/no-commits"
        task_no_commits.has_commits = False
        store.update(task_no_commits)

        result = run_gza("merge", "--all", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "No unmerged done tasks found" in result.stdout


class TestCheckoutCommand:
    """Tests for 'gza checkout' command."""

    def test_checkout_removes_clean_worktree(self, tmp_path: Path):
        """Checkout command removes clean worktree before checking out branch."""
        _store, _git, task, worktree_path = setup_git_repo_with_task_branch(
            tmp_path, "Test checkout task", "feature/test-checkout",
            worktree_name="test-checkout",
        )

        # Verify worktree exists
        assert worktree_path.exists()

        # Checkout the branch by task ID - should remove worktree first
        result = run_gza("checkout", str(task.id), "--project", str(tmp_path))

        # Verify success
        assert result.returncode == 0
        assert "Removing stale worktree" in result.stdout
        assert "Removed worktree" in result.stdout
        assert "Checked out" in result.stdout

    def test_checkout_fails_with_dirty_worktree(self, tmp_path: Path):
        """Checkout command fails if worktree has uncommitted changes."""
        _store, _git, task, worktree_path = setup_git_repo_with_task_branch(
            tmp_path, "Test checkout with dirty worktree", "feature/test-dirty",
            worktree_name="test-dirty",
        )

        # Add uncommitted changes to the worktree
        (worktree_path / "uncommitted.txt").write_text("uncommitted")

        # Checkout should fail due to dirty worktree
        result = run_gza("checkout", str(task.id), "--project", str(tmp_path))

        # Verify failure
        assert result.returncode == 1
        assert "uncommitted changes" in result.stdout

    def test_checkout_force_removes_dirty_worktree(self, tmp_path: Path):
        """Checkout --force removes worktree even with uncommitted changes."""
        _store, _git, task, worktree_path = setup_git_repo_with_task_branch(
            tmp_path, "Test checkout force", "feature/test-force",
            worktree_name="test-force",
        )

        # Add uncommitted changes to the worktree
        (worktree_path / "uncommitted.txt").write_text("uncommitted")

        # Checkout with --force should succeed
        result = run_gza("checkout", str(task.id), "--force", "--project", str(tmp_path))

        # Verify success
        assert result.returncode == 0
        assert "Removed worktree" in result.stdout
        assert "Checked out" in result.stdout


class TestRebaseCommand:
    """Tests for 'gza rebase' command."""

    def test_rebase_removes_clean_worktree(self, tmp_path: Path):
        """Rebase command removes clean worktree before rebasing."""
        _store, _git, task, worktree_path = setup_git_repo_with_task_branch(
            tmp_path, "Test rebase with worktree", "feature/test-rebase-wt",
            worktree_name="test-rebase-wt",
        )

        # Verify worktree exists
        assert worktree_path.exists()

        # Rebase should remove worktree first, then succeed
        result = run_gza("rebase", str(task.id), "--project", str(tmp_path))

        # Verify success
        assert result.returncode == 0
        assert "Removing stale worktree" in result.stdout
        assert "Removed worktree" in result.stdout
        assert "Successfully rebased" in result.stdout

    def test_rebase_force_removes_dirty_worktree(self, tmp_path: Path):
        """Rebase always force-removes dirty worktrees and succeeds cleanly."""
        _store, _git, task, worktree_path = setup_git_repo_with_task_branch(
            tmp_path, "Test rebase force", "feature/test-rebase-force",
            worktree_name="test-rebase-force",
        )

        # Add uncommitted changes to the old worktree
        (worktree_path / "uncommitted.txt").write_text("uncommitted")

        # Rebase should succeed: old dirty worktree is force-removed, fresh one created
        result = run_gza("rebase", str(task.id), "--project", str(tmp_path))

        # Verify success — old worktree removed, rebase completed in fresh worktree
        assert result.returncode == 0
        assert "Removed worktree" in result.stdout
        assert "Successfully rebased" in result.stdout

    def test_rebase_force_flag_accepted(self, tmp_path: Path):
        """Rebase --force flag is accepted (backward-compat no-op)."""
        _store, _git, task, worktree_path = setup_git_repo_with_task_branch(
            tmp_path, "Test rebase --force flag", "feature/test-rebase-force-flag",
            worktree_name="test-rebase-force-flag",
        )

        # Add uncommitted changes to the old worktree
        (worktree_path / "uncommitted.txt").write_text("uncommitted")

        # --force is now a no-op; rebase should still succeed
        result = run_gza("rebase", str(task.id), "--force", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Successfully rebased" in result.stdout

    def test_rebase_without_worktree(self, tmp_path: Path):
        """Rebase works normally when no worktree exists."""
        _store, _git, task, _worktree_path = setup_git_repo_with_task_branch(
            tmp_path, "Test rebase no worktree", "feature/test-rebase-nowt",
        )

        # Rebase should work normally
        result = run_gza("rebase", str(task.id), "--project", str(tmp_path))

        # Verify success (no worktree messages)
        assert result.returncode == 0
        assert "Removing stale worktree" not in result.stdout
        assert "Successfully rebased" in result.stdout

    def test_rebase_logs_task_id_and_newline(self, tmp_path: Path):
        """Rebase command logs 'Rebasing task #X...' and ends with a newline."""
        _store, _git, task, _worktree_path = setup_git_repo_with_task_branch(
            tmp_path, "Test rebase output format", "feature/test-rebase-output",
        )

        result = run_gza("rebase", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert f"Rebasing task {task.id}..." in result.stdout
        # Output should end with a newline (after trailing whitespace is stripped per line,
        # the last non-empty content is followed by a blank line)
        assert result.stdout.endswith("\n")

    def test_rebase_resolve_flag_accepted(self, tmp_path: Path):
        """Rebase command accepts --resolve flag."""
        _store, _git, task, _worktree_path = setup_git_repo_with_task_branch(
            tmp_path, "Test rebase with resolve", "feature/test-resolve",
        )

        # Mock the conflict resolution since we're just testing that the flag is accepted
        # and the basic flow works (we don't want to actually invoke Claude in tests)
        with patch('gza.cli.invoke_provider_resolve', return_value=False):
            # This should succeed without conflicts (no --resolve needed, but flag should work)
            result = run_gza("rebase", str(task.id), "--resolve", "--project", str(tmp_path))

            # Should succeed when there are no conflicts
            assert result.returncode == 0
            assert "Successfully rebased" in result.stdout

    def test_rebase_cleans_up_worktree_after_mechanical_success(self, tmp_path: Path):
        """Worktree is removed after a successful mechanical rebase (no conflicts)."""
        from gza.config import Config

        _store, _git, task, _wt = setup_git_repo_with_task_branch(
            tmp_path, "Test rebase cleanup mechanical", "feature/test-cleanup-mech",
        )
        config = Config.load(tmp_path)
        expected_worktree = config.worktree_path / str(task.id)

        result = run_gza("rebase", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Successfully rebased" in result.stdout
        assert not expected_worktree.exists(), (
            f"Worktree at {expected_worktree} should have been removed after successful rebase"
        )

    def test_rebase_cleans_up_worktree_on_push_failure(self, tmp_path: Path):
        """Worktree is removed even when force-push raises GitError."""
        from gza.cli.git_ops import cmd_rebase
        from gza.git import GitError

        _store, _git, task, _wt = setup_git_repo_with_task_branch(
            tmp_path, "Test rebase cleanup push fail", "feature/test-cleanup-push",
        )
        config = Config.load(tmp_path)
        expected_worktree = config.worktree_path / str(task.id)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=task.id,
            background=False,
            onto=None,
            remote=False,
            force=False,
            resolve=False,
        )

        # Patch push_force_with_lease to simulate a push failure after a provider-resolved rebase.
        # We also need invoke_provider_resolve to return True so the push is attempted.
        with patch("gza.cli.git_ops.invoke_provider_resolve", return_value=True), \
             patch("gza.git.Git.rebase", side_effect=GitError("conflict")), \
             patch("gza.git.Git.rebase_abort"), \
             patch("gza.git.Git.push_force_with_lease", side_effect=GitError("push failed")):
            with pytest.raises(GitError, match="push failed"):
                cmd_rebase(args)

        # Worktree must not exist regardless of push failure.
        assert not expected_worktree.exists(), (
            f"Worktree at {expected_worktree} should have been removed even after push failure"
        )


class TestDiffCommand:
    """Tests for 'gza diff' command."""

    def test_diff_runs_git_diff(self, tmp_path: Path):
        """Diff command runs git diff with colored output."""
        from gza.git import Git

        setup_config(tmp_path)

        # Initialize a git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        # Create initial commit
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Make changes to file
        (tmp_path / "file.txt").write_text("modified")

        # Run diff command - should show the changes
        # We redirect to avoid pager issues in tests
        result = run_gza("diff", "--project", str(tmp_path))

        assert result.returncode == 0
        # Should show the diff (contains color codes when forced with --color=always)
        assert "file.txt" in result.stdout

    def test_diff_with_stat_argument(self, tmp_path: Path):
        """Diff command passes --stat to git diff."""
        from gza.git import Git

        setup_config(tmp_path)

        # Initialize a git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        # Create initial commit
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Make changes
        (tmp_path / "file.txt").write_text("modified")

        # Run diff with --stat (using -- separator for pass-through args)
        result = run_gza("diff", "--project", str(tmp_path), "--", "--stat")

        assert result.returncode == 0
        assert "file.txt" in result.stdout

    def test_diff_with_task_id(self, tmp_path: Path):
        """Diff command resolves task ID to branch diff."""
        from gza.db import SqliteTaskStore
        from gza.git import Git

        setup_config(tmp_path)

        # Initialize a git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        # Create initial commit on main
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Create and checkout task branch
        git._run("checkout", "-b", "task-1-test")
        (tmp_path / "file.txt").write_text("modified")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Task changes")

        # Return to main
        git._run("checkout", "main")

        # Create task in database with branch (use same prefix as config: testproject)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path, prefix="testproject")
        task = store.add("Test task", task_type="implement")
        task.branch = "task-1-test"
        store.update(task)

        # Run diff with task ID (use full task.id so resolve_id returns it as-is)
        result = run_gza("diff", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        # Should show the diff between main and task branch
        assert "file.txt" in result.stdout
        assert "modified" in result.stdout or "initial" in result.stdout

    def test_diff_with_task_id_not_found(self, tmp_path: Path):
        """Diff falls back to git when a full prefixed task ID is not found in DB.

        This mirrors cmd_checkout behaviour: a _looks_like_task_id() match that
        doesn't resolve to a real task is passed through to git as a branch/ref.
        git will fail with a non-zero exit code when the ref is also invalid.
        """
        from gza.git import Git

        setup_config(tmp_path)

        # Initialize a git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")

        # Create empty database
        setup_db_with_tasks(tmp_path, [])

        # Run diff with non-existent full task ID — falls back to git, which fails
        # because the ref is also invalid.
        result = run_gza("diff", "testproject-999999", "--project", str(tmp_path))

        assert result.returncode != 0

    def test_diff_treats_bare_suffix_as_git_ref_not_task_id(self, tmp_path: Path):
        """Bare suffixes should be treated as git refs, not implicit task IDs."""
        from gza.git import Git

        setup_config(tmp_path)

        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        store = make_store(tmp_path)
        task = store.add("Task with branch")
        task.branch = "feature/task-branch"
        store.update(task)

        result = run_gza("diff", "000001", "--project", str(tmp_path))

        assert result.returncode != 0
        assert f"{task.branch}" not in result.stdout

    def test_diff_with_task_id_no_branch(self, tmp_path: Path):
        """Diff command shows error when task has no branch."""
        from gza.db import SqliteTaskStore
        from gza.git import Git

        setup_config(tmp_path)

        # Initialize a git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")

        # Create task without branch (use same prefix as config: testproject)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path, prefix="testproject")
        task = store.add("Test task", task_type="implement")
        # Don't set task.branch

        # Run diff with task ID that has no branch
        result = run_gza("diff", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 1
        assert f"Error: Task {task.id} has no branch" in result.stdout

    def test_diff_with_non_numeric_argument(self, tmp_path: Path):
        """Diff command passes non-numeric arguments through to git diff."""
        from gza.git import Git

        setup_config(tmp_path)

        # Initialize a git repo
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")

        # Create initial commit
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")

        # Make changes
        (tmp_path / "file.txt").write_text("modified")

        # Run diff with --cached (using -- separator for pass-through args)
        result = run_gza("diff", "--project", str(tmp_path), "--", "--cached")

        # Should run successfully (even if no staged changes)
        assert result.returncode == 0


class TestRebaseHelpers:
    """Tests for rebase helper functions."""

    def test_invoke_provider_resolve_uses_effective_codex_provider(self, tmp_path):
        """Auto-resolve uses effective provider selection (codex override)."""
        from types import SimpleNamespace
        from unittest.mock import Mock, patch

        from gza.cli import invoke_provider_resolve
        from gza.config import Config
        from gza.providers.base import RunResult

        config = Config(project_dir=tmp_path, project_name="test", provider="claude")
        task = SimpleNamespace(task_type="implement", provider="codex", provider_is_explicit=True, model=None)

        with patch("gza.cli.ensure_skill", return_value=True), \
             patch("gza.providers.get_provider") as mock_get_provider, \
             patch("pathlib.Path.exists", return_value=False):
            mock_provider = Mock()
            mock_provider.run.return_value = RunResult(exit_code=0)
            mock_get_provider.return_value = mock_provider

            result = invoke_provider_resolve(task, "feature", "main", config)
            assert result is True
            assert mock_get_provider.call_count == 1
            resolve_config = mock_get_provider.call_args.args[0]
            assert resolve_config.provider == "codex"
            assert resolve_config.use_docker is config.use_docker
            mock_provider.run.assert_called_once()
            assert mock_provider.run.call_args.args[1] == "/gza-rebase --auto --continue"

    def test_invoke_provider_resolve_uses_effective_gemini_provider(self, tmp_path):
        """Auto-resolve supports gemini provider selection from effective config."""
        from types import SimpleNamespace
        from unittest.mock import Mock, patch

        from gza.cli import invoke_provider_resolve
        from gza.config import Config
        from gza.providers.base import RunResult

        config = Config(project_dir=tmp_path, project_name="test", provider="gemini")
        task = SimpleNamespace(task_type="implement", provider=None, provider_is_explicit=False, model=None)

        with patch("gza.cli.ensure_skill", return_value=True), \
             patch("gza.providers.get_provider") as mock_get_provider, \
             patch("pathlib.Path.exists", return_value=False):
            mock_provider = Mock()
            mock_provider.run.return_value = RunResult(exit_code=0)
            mock_get_provider.return_value = mock_provider

            result = invoke_provider_resolve(task, "feature", "main", config)
            assert result is True
            assert mock_get_provider.call_count == 1
            resolve_config = mock_get_provider.call_args.args[0]
            assert resolve_config.provider == "gemini"
            mock_provider.run.assert_called_once()
            assert mock_provider.run.call_args.args[1] == "/gza-rebase --auto --continue"

    def test_invoke_provider_resolve_fails_fast_when_skill_missing(self, tmp_path, capsys, monkeypatch):
        """Auto-resolve fails before provider run when runtime skill is missing and auto-install fails."""
        from types import SimpleNamespace
        from unittest.mock import patch

        from gza.cli import invoke_provider_resolve
        from gza.config import Config

        codex_home = tmp_path / "codex-home"
        monkeypatch.setenv("CODEX_HOME", str(codex_home))
        config = Config(project_dir=tmp_path, project_name="test", provider="codex")
        task = SimpleNamespace(task_type="implement", provider=None, provider_is_explicit=False, model=None)

        with patch("gza.cli.ensure_skill", return_value=False), \
             patch("gza.providers.get_provider") as mock_get_provider:
            result = invoke_provider_resolve(task, "feature", "main", config)
            assert result is False
            assert mock_get_provider.call_count == 0

        out = capsys.readouterr().out
        assert "Missing required 'gza-rebase' skill for provider 'codex'" in out
        assert "uv run gza skills-install --target codex gza-rebase --project" in out

    def test_invoke_provider_resolve_uses_current_config_after_provider_switch(self, tmp_path):
        """Auto-resolve uses current config provider when task provider is non-explicit stale state."""
        from types import SimpleNamespace
        from unittest.mock import Mock, patch

        from gza.cli import invoke_provider_resolve
        from gza.config import Config
        from gza.providers.base import RunResult

        config = Config(project_dir=tmp_path, project_name="test", provider="codex")
        task = SimpleNamespace(task_type="implement", provider="claude", provider_is_explicit=False, model=None)

        with patch("gza.cli.ensure_skill", return_value=True), \
             patch("gza.providers.get_provider") as mock_get_provider, \
             patch("pathlib.Path.exists", return_value=False):
            mock_provider = Mock()
            mock_provider.run.return_value = RunResult(exit_code=0)
            mock_get_provider.return_value = mock_provider

            result = invoke_provider_resolve(task, "feature", "main", config)
            assert result is True
            assert mock_get_provider.call_count == 1
            resolve_config = mock_get_provider.call_args.args[0]
            assert resolve_config.provider == "codex"

    def test_invoke_provider_resolve_honors_use_docker_override(self, tmp_path):
        """Auto-resolve should respect config.use_docker when launching provider."""
        from types import SimpleNamespace
        from unittest.mock import Mock, patch

        from gza.cli import invoke_provider_resolve
        from gza.config import Config
        from gza.providers.base import RunResult

        config = Config(project_dir=tmp_path, project_name="test", provider="codex", use_docker=False)
        task = SimpleNamespace(task_type="implement", provider=None, provider_is_explicit=False, model=None)

        with patch("gza.cli.ensure_skill", return_value=True), \
             patch("gza.providers.get_provider") as mock_get_provider, \
             patch("pathlib.Path.exists", return_value=False):
            mock_provider = Mock()
            mock_provider.run.return_value = RunResult(exit_code=0)
            mock_get_provider.return_value = mock_provider

            result = invoke_provider_resolve(task, "feature", "main", config)

        assert result is True
        resolve_config = mock_get_provider.call_args.args[0]
        assert resolve_config.use_docker is False

    def test_invoke_provider_resolve_creates_internal_task_record(self, tmp_path):
        """Auto-resolve should persist an internal task so history/auditing can find it."""
        from types import SimpleNamespace
        from unittest.mock import Mock, patch

        from gza.cli import invoke_provider_resolve
        from gza.config import Config
        from gza.db import SqliteTaskStore
        from gza.providers.base import RunResult

        config = Config(project_dir=tmp_path, project_name="test", provider="codex")
        store = SqliteTaskStore(config.db_path)
        task = SimpleNamespace(
            id=42,
            task_type="implement",
            provider=None,
            provider_is_explicit=False,
            model=None,
        )

        with patch("gza.cli.ensure_skill", return_value=True), \
             patch("gza.providers.get_provider") as mock_get_provider, \
             patch("pathlib.Path.exists", return_value=False):
            mock_provider = Mock()
            mock_provider.run.return_value = RunResult(exit_code=0)
            mock_get_provider.return_value = mock_provider

            result = invoke_provider_resolve(task, "feature", "main", config)

        assert result is True
        internal_tasks = store.get_history(limit=None, task_type="internal")
        assert len(internal_tasks) == 1
        internal_task = internal_tasks[0]
        assert internal_task.status == "completed"
        assert internal_task.skip_learnings is True
        assert internal_task.output_content is not None
        assert "/gza-rebase --auto --continue" in internal_task.output_content

    def test_invoke_provider_resolve_marks_internal_task_failed_on_exception(self, tmp_path):
        """Provider exceptions should mark the tracking internal task as failed."""
        from types import SimpleNamespace
        from unittest.mock import Mock, patch

        from gza.cli import invoke_provider_resolve
        from gza.config import Config
        from gza.db import SqliteTaskStore

        config = Config(project_dir=tmp_path, project_name="test", provider="codex")
        store = SqliteTaskStore(config.db_path)
        task = SimpleNamespace(
            id=43,
            task_type="implement",
            provider=None,
            provider_is_explicit=False,
            model=None,
        )

        with patch("gza.cli.ensure_skill", return_value=True), \
             patch("gza.providers.get_provider") as mock_get_provider, \
             patch("pathlib.Path.exists", return_value=False):
            mock_provider = Mock()
            mock_provider.run.side_effect = RuntimeError("provider failure")
            mock_get_provider.return_value = mock_provider

            with pytest.raises(RuntimeError, match="provider failure"):
                invoke_provider_resolve(task, "feature", "main", config)

        internal_tasks = store.get_history(limit=None, task_type="internal")
        assert len(internal_tasks) == 1
        internal_task = internal_tasks[0]
        assert internal_task.status == "failed"
        assert internal_task.failure_reason == "UNKNOWN"

    def test_invoke_provider_resolve_marks_internal_task_failed_if_rebase_still_in_progress(self, tmp_path):
        """Auto-resolve should fail when git still reports active rebase markers."""
        from types import SimpleNamespace
        from unittest.mock import Mock, patch

        from gza.cli import invoke_provider_resolve
        from gza.config import Config
        from gza.db import SqliteTaskStore
        from gza.providers.base import RunResult

        config = Config(project_dir=tmp_path, project_name="test", provider="codex")
        store = SqliteTaskStore(config.db_path)
        task = SimpleNamespace(
            id=44,
            task_type="implement",
            provider=None,
            provider_is_explicit=False,
            model=None,
        )

        with patch("gza.cli.ensure_skill", return_value=True), \
             patch("gza.providers.get_provider") as mock_get_provider, \
             patch("gza.cli.git_ops.load_dotenv") as mock_load_dotenv, \
             patch("pathlib.Path.exists", side_effect=[True, False]):
            mock_provider = Mock()
            mock_provider.run.return_value = RunResult(exit_code=0)
            mock_get_provider.return_value = mock_provider

            result = invoke_provider_resolve(task, "feature", "main", config)

        assert result is False
        mock_load_dotenv.assert_called_once_with(tmp_path)
        internal_tasks = store.get_history(limit=None, task_type="internal")
        assert len(internal_tasks) == 1
        internal_task = internal_tasks[0]
        assert internal_task.status == "failed"
        assert internal_task.failure_reason == "UNKNOWN"

    def test_invoke_provider_resolve_marks_internal_task_failed_on_nonzero_exit(self, tmp_path):
        """Non-zero provider exits should fail resolve even without an exception."""
        from types import SimpleNamespace
        from unittest.mock import Mock, patch

        from gza.cli import invoke_provider_resolve
        from gza.config import Config
        from gza.db import SqliteTaskStore
        from gza.providers.base import RunResult

        config = Config(project_dir=tmp_path, project_name="test", provider="codex")
        store = SqliteTaskStore(config.db_path)
        task = SimpleNamespace(
            id=45,
            task_type="implement",
            provider=None,
            provider_is_explicit=False,
            model=None,
        )

        with patch("gza.cli.ensure_skill", return_value=True), \
             patch("gza.providers.get_provider") as mock_get_provider, \
             patch("gza.cli.git_ops.load_dotenv"), \
             patch("pathlib.Path.exists", return_value=False):
            mock_provider = Mock()
            mock_provider.run.return_value = RunResult(exit_code=1)
            mock_get_provider.return_value = mock_provider

            result = invoke_provider_resolve(task, "feature", "main", config)

        assert result is False
        internal_tasks = store.get_history(limit=None, task_type="internal")
        assert len(internal_tasks) == 1
        internal_task = internal_tasks[0]
        assert internal_task.status == "failed"
        assert internal_task.failure_reason == "UNKNOWN"
        assert internal_task.output_content is None

    def test_ensure_skill_returns_true_when_skill_already_present(self, tmp_path):
        """ensure_skill returns True immediately when the skill file already exists."""
        from gza.cli import ensure_skill

        skills_dir = tmp_path / ".claude" / "skills"
        skill_dir = skills_dir / "gza-rebase"
        skill_dir.mkdir(parents=True)
        (skill_dir / "SKILL.md").write_text("---\nname: gza-rebase\n---\n")

        result = ensure_skill("gza-rebase", "claude", tmp_path)
        assert result is True

    def test_ensure_skill_installs_when_missing(self, tmp_path):
        """ensure_skill auto-installs from bundled package when skill is absent."""
        from unittest.mock import patch

        from gza.cli import ensure_skill

        with patch("gza.cli._resolve_runtime_skill_dir") as mock_resolve, \
             patch("gza.skills_utils.copy_skill") as mock_copy:
            runtime_dir = tmp_path / ".claude" / "skills"
            mock_resolve.return_value = ("claude", runtime_dir)
            # Simulate successful install: copy_skill writes the file
            def fake_copy(name, target, force=False):
                skill_path = target / name / "SKILL.md"
                skill_path.parent.mkdir(parents=True, exist_ok=True)
                skill_path.write_text("---\nname: gza-rebase\n---\n")
                return True, "installed"
            mock_copy.side_effect = fake_copy

            result = ensure_skill("gza-rebase", "claude", tmp_path)
            assert result is True
            mock_copy.assert_called_once_with("gza-rebase", runtime_dir)

    def test_ensure_skill_returns_false_when_install_fails(self, tmp_path):
        """ensure_skill returns False when copy_skill fails."""
        from unittest.mock import patch

        from gza.cli import ensure_skill

        with patch("gza.cli._resolve_runtime_skill_dir") as mock_resolve, \
             patch("gza.skills_utils.copy_skill", return_value=(False, "copy failed: error")):
            runtime_dir = tmp_path / ".claude" / "skills"
            mock_resolve.return_value = ("claude", runtime_dir)

            result = ensure_skill("gza-rebase", "claude", tmp_path)
            assert result is False

    def test_ensure_skill_returns_false_for_unknown_provider(self, tmp_path):
        """ensure_skill returns False when the provider has no known skill dir."""
        from gza.cli import ensure_skill

        result = ensure_skill("gza-rebase", "unknown-provider", tmp_path)
        assert result is False

    # --- worktree-path variant tests ---

    def test_invoke_provider_resolve_worktree_uses_auto_without_continue(self, tmp_path):
        """When worktree_path is provided the provider is called with /gza-rebase --auto (no --continue)."""
        from types import SimpleNamespace
        from unittest.mock import Mock, patch

        from gza.cli import invoke_provider_resolve
        from gza.config import Config
        from gza.providers.base import RunResult

        config = Config(project_dir=tmp_path, project_name="test", provider="claude")
        task = SimpleNamespace(task_type="implement", provider=None, provider_is_explicit=False, model=None)

        with patch("gza.cli.ensure_skill", return_value=True), \
             patch("gza.providers.get_provider") as mock_get_provider, \
             patch("gza.cli.git_ops._is_rebase_in_progress", return_value=False), \
             patch("gza.skills_utils.copy_skill", return_value=(True, "installed")):
            mock_provider = Mock()
            mock_provider.run.return_value = RunResult(exit_code=0)
            mock_get_provider.return_value = mock_provider

            result = invoke_provider_resolve(task, "feature", "main", config, worktree_path=tmp_path)

        assert result is True
        mock_provider.run.assert_called_once()
        assert mock_provider.run.call_args.args[1] == "/gza-rebase --auto"

    def test_invoke_provider_resolve_worktree_runs_provider_in_worktree(self, tmp_path):
        """When worktree_path is provided the provider runs in that directory."""
        from types import SimpleNamespace
        from unittest.mock import Mock, patch

        from gza.cli import invoke_provider_resolve
        from gza.config import Config
        from gza.providers.base import RunResult

        worktree = tmp_path / "wt"
        worktree.mkdir()
        config = Config(project_dir=tmp_path, project_name="test", provider="claude")
        task = SimpleNamespace(task_type="implement", provider=None, provider_is_explicit=False, model=None)

        with patch("gza.cli.ensure_skill", return_value=True), \
             patch("gza.providers.get_provider") as mock_get_provider, \
             patch("gza.cli.git_ops._is_rebase_in_progress", return_value=False), \
             patch("gza.skills_utils.copy_skill", return_value=(True, "installed")):
            mock_provider = Mock()
            mock_provider.run.return_value = RunResult(exit_code=0)
            mock_get_provider.return_value = mock_provider

            invoke_provider_resolve(task, "feature", "main", config, worktree_path=worktree)

        # Provider must be run with the worktree as the working directory
        assert mock_provider.run.call_args.args[3] == worktree

    def test_invoke_provider_resolve_worktree_fails_if_rebase_still_in_progress(self, tmp_path):
        """Worktree path: returns False when _is_rebase_in_progress reports True."""
        from types import SimpleNamespace
        from unittest.mock import Mock, patch

        from gza.cli import invoke_provider_resolve
        from gza.config import Config
        from gza.db import SqliteTaskStore
        from gza.providers.base import RunResult

        config = Config(project_dir=tmp_path, project_name="test", provider="claude")
        store = SqliteTaskStore(config.db_path)
        task = SimpleNamespace(
            id=99,
            task_type="implement",
            provider=None,
            provider_is_explicit=False,
            model=None,
        )

        with patch("gza.cli.ensure_skill", return_value=True), \
             patch("gza.providers.get_provider") as mock_get_provider, \
             patch("gza.cli.git_ops.load_dotenv"), \
             patch("gza.cli.git_ops._is_rebase_in_progress", return_value=True), \
             patch("gza.skills_utils.copy_skill", return_value=(True, "installed")):
            mock_provider = Mock()
            mock_provider.run.return_value = RunResult(exit_code=0)
            mock_get_provider.return_value = mock_provider

            result = invoke_provider_resolve(task, "feature", "main", config, worktree_path=tmp_path)

        assert result is False
        internal_tasks = store.get_history(limit=None, task_type="internal")
        assert len(internal_tasks) == 1
        assert internal_tasks[0].status == "failed"

    def test_invoke_provider_resolve_worktree_records_auto_command_in_output(self, tmp_path):
        """Worktree path: internal task output_content contains /gza-rebase --auto."""
        from types import SimpleNamespace
        from unittest.mock import Mock, patch

        from gza.cli import invoke_provider_resolve
        from gza.config import Config
        from gza.db import SqliteTaskStore
        from gza.providers.base import RunResult

        config = Config(project_dir=tmp_path, project_name="test", provider="claude")
        store = SqliteTaskStore(config.db_path)
        task = SimpleNamespace(
            id=100,
            task_type="implement",
            provider=None,
            provider_is_explicit=False,
            model=None,
        )

        with patch("gza.cli.ensure_skill", return_value=True), \
             patch("gza.providers.get_provider") as mock_get_provider, \
             patch("gza.cli.git_ops._is_rebase_in_progress", return_value=False), \
             patch("gza.skills_utils.copy_skill", return_value=(True, "installed")):
            mock_provider = Mock()
            mock_provider.run.return_value = RunResult(exit_code=0)
            mock_get_provider.return_value = mock_provider

            result = invoke_provider_resolve(task, "feature", "main", config, worktree_path=tmp_path)

        assert result is True
        internal_tasks = store.get_history(limit=None, task_type="internal")
        assert len(internal_tasks) == 1
        internal_task = internal_tasks[0]
        assert internal_task.status == "completed"
        assert "/gza-rebase --auto" in internal_task.output_content
        assert "--continue" not in internal_task.output_content


class TestIsRebaseInProgress:
    """Tests for the _is_rebase_in_progress helper."""

    def test_returns_false_when_no_git_dir(self, tmp_path):
        """Returns False when there's no .git directory at all."""
        from gza.cli.git_ops import _is_rebase_in_progress
        assert _is_rebase_in_progress(tmp_path) is False

    def test_returns_false_when_no_rebase_markers(self, tmp_path):
        """Returns False for a normal repository with no rebase in progress."""
        from gza.cli.git_ops import _is_rebase_in_progress
        git_dir = tmp_path / ".git"
        git_dir.mkdir()
        assert _is_rebase_in_progress(tmp_path) is False

    def test_returns_true_when_rebase_merge_present(self, tmp_path):
        """Returns True when .git/rebase-merge directory exists."""
        from gza.cli.git_ops import _is_rebase_in_progress
        (tmp_path / ".git").mkdir()
        (tmp_path / ".git" / "rebase-merge").mkdir()
        assert _is_rebase_in_progress(tmp_path) is True

    def test_returns_true_when_rebase_apply_present(self, tmp_path):
        """Returns True when .git/rebase-apply directory exists."""
        from gza.cli.git_ops import _is_rebase_in_progress
        (tmp_path / ".git").mkdir()
        (tmp_path / ".git" / "rebase-apply").mkdir()
        assert _is_rebase_in_progress(tmp_path) is True

    def test_worktree_git_file_resolved_correctly(self, tmp_path):
        """Follows the gitdir: pointer in a worktree .git file."""
        from gza.cli.git_ops import _is_rebase_in_progress
        # Simulate a real git worktree: .git is a file pointing to the gitdir
        real_git_dir = tmp_path / "main-repo" / ".git" / "worktrees" / "wt1"
        real_git_dir.mkdir(parents=True)
        worktree = tmp_path / "worktree"
        worktree.mkdir()
        (worktree / ".git").write_text(f"gitdir: {real_git_dir}\n")
        # No rebase markers yet
        assert _is_rebase_in_progress(worktree) is False
        # Add a rebase-merge dir inside the actual git dir
        (real_git_dir / "rebase-merge").mkdir()
        assert _is_rebase_in_progress(worktree) is True


class TestMergeStatusTracking:
    """Tests for merge_status column tracking."""

    def _setup_git_repo(self, tmp_path: Path):
        """Set up a minimal git repo for testing."""
        from gza.git import Git
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")
        (tmp_path / "file.txt").write_text("initial")
        git._run("add", "file.txt")
        git._run("commit", "-m", "Initial commit")
        return git

    def test_merge_sets_merge_status_merged(self, tmp_path: Path):
        """Successful merge sets merge_status='merged' on the task."""
        from datetime import datetime

        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)

        # Create a task with merge_status='unmerged'
        task = store.add("Add feature")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = "feature/test"
        task.has_commits = True
        task.merge_status = "unmerged"
        store.update(task)

        # Create the feature branch with a commit
        git._run("checkout", "-b", "feature/test")
        (tmp_path / "feature.txt").write_text("feature")
        git._run("add", "feature.txt")
        git._run("commit", "-m", "Add feature")
        git._run("checkout", "main")

        # Run merge
        result = run_gza("merge", str(task.id), "--project", str(tmp_path))
        assert result.returncode == 0

        # Verify merge_status is 'merged'
        updated_task = store.get(task.id)
        assert updated_task is not None
        assert updated_task.merge_status == "merged"

    def test_squash_merge_sets_merge_status_merged(self, tmp_path: Path):
        """Squash merge also sets merge_status='merged'."""
        from datetime import datetime

        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)

        task = store.add("Add feature squash")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = "feature/squash"
        task.has_commits = True
        task.merge_status = "unmerged"
        store.update(task)

        git._run("checkout", "-b", "feature/squash")
        (tmp_path / "squash.txt").write_text("squash content")
        git._run("add", "squash.txt")
        git._run("commit", "-m", "Squash feature")
        git._run("checkout", "main")

        result = run_gza("merge", str(task.id), "--squash", "--project", str(tmp_path))
        assert result.returncode == 0

        updated_task = store.get(task.id)
        assert updated_task is not None
        assert updated_task.merge_status == "merged"

    def test_mark_only_sets_merge_status_merged(self, tmp_path: Path):
        """--mark-only flag sets merge_status='merged' in the database."""
        from datetime import datetime

        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)

        task = store.add("Mark only test")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = "feature/mark-only-status"
        task.has_commits = True
        task.merge_status = "unmerged"
        store.update(task)

        git._run("checkout", "-b", "feature/mark-only-status")
        (tmp_path / "mark.txt").write_text("mark content")
        git._run("add", "mark.txt")
        git._run("commit", "-m", "Mark feature")
        git._run("checkout", "main")

        result = run_gza("merge", str(task.id), "--mark-only", "--project", str(tmp_path))
        assert result.returncode == 0

        updated_task = store.get(task.id)
        assert updated_task is not None
        assert updated_task.merge_status == "merged"

    def test_cmd_unmerged_uses_db_query(self, tmp_path: Path):
        """gza unmerged uses merge_status='unmerged' DB query instead of git detection."""
        from datetime import datetime

        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)

        # Task with merge_status='unmerged' (branch exists)
        task1 = store.add("Unmerged task")
        task1.status = "completed"
        task1.completed_at = datetime.now(UTC)
        task1.branch = "feature/unmerged-task"
        task1.has_commits = True
        task1.merge_status = "unmerged"
        store.update(task1)

        git._run("checkout", "-b", "feature/unmerged-task")
        (tmp_path / "unmerged.txt").write_text("content")
        git._run("add", "unmerged.txt")
        git._run("commit", "-m", "Unmerged feature")
        git._run("checkout", "main")

        # Task with merge_status='merged'
        task2 = store.add("Merged task")
        task2.status = "completed"
        task2.completed_at = datetime.now(UTC)
        task2.branch = "feature/merged-task"
        task2.has_commits = True
        task2.merge_status = "merged"
        store.update(task2)

        # Task with merge_status=None
        task3 = store.add("No merge status")
        task3.status = "completed"
        task3.completed_at = datetime.now(UTC)
        task3.has_commits = False
        store.update(task3)

        result = run_gza("unmerged", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Unmerged task" in result.stdout
        assert "Merged task" not in result.stdout
        assert "No merge status" not in result.stdout

    def test_cmd_history_shows_merged_label(self, tmp_path: Path):
        """gza history shows [merged] label for tasks with merge_status='merged'."""
        from datetime import datetime

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Merged feature task")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.has_commits = True
        task.merge_status = "merged"
        store.update(task)

        result = run_gza("history", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "[merged]" in result.stdout
        assert "Merged feature task" in result.stdout

    def test_cmd_history_shows_unmerged_label_for_unmerged(self, tmp_path: Path):
        """gza history shows 'unmerged' text label for tasks with merge_status='unmerged'."""
        from datetime import datetime

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Unmerged feature")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.has_commits = True
        task.merge_status = "unmerged"
        store.update(task)

        result = run_gza("history", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "unmerged" in result.stdout
        assert "Unmerged feature" in result.stdout
        assert "[merged]" not in result.stdout

    def test_cmd_history_no_merge_label_without_merge_status(self, tmp_path: Path):
        """gza history shows no merge label for tasks without merge_status."""
        from datetime import datetime

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Regular completed task")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        result = run_gza("history", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "[merged]" not in result.stdout
        assert "completed" in result.stdout

    def test_cmd_show_displays_merge_status(self, tmp_path: Path):
        """gza show displays Merge Status when merge_status is set."""
        from datetime import datetime

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Test show merge status")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.merge_status = "merged"
        store.update(task)

        result = run_gza("show", str(task.id), "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Merge Status: merged" in result.stdout

    def test_cmd_show_no_merge_status_line_when_null(self, tmp_path: Path):
        """gza show does not display Merge Status when merge_status is None."""
        from datetime import datetime

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Test show no merge status")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        result = run_gza("show", str(task.id), "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Merge Status" not in result.stdout

    def test_cmd_show_displays_skip_learnings(self, tmp_path: Path):
        """gza show displays 'Skip Learnings: yes' when skip_learnings is True."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Test task with skip learnings", skip_learnings=True)

        result = run_gza("show", str(task.id), "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Skip Learnings: yes" in result.stdout

    def test_cmd_show_no_skip_learnings_line_when_false(self, tmp_path: Path):
        """gza show does not display Skip Learnings when skip_learnings is False."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Normal task")

        result = run_gza("show", str(task.id), "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Skip Learnings" not in result.stdout

    def test_cmd_show_warning_when_disk_report_newer(self, tmp_path: Path):
        """gza show displays a warning when the report file on disk is newer than task completion."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a completed plan task with output_content in DB
        task = store.add("Plan something", task_type="plan")
        assert task.id is not None
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.output_content = "Original plan content"
        task.report_file = ".gza/plans/20260101-plan-something.md"
        store.update(task)

        # Write a newer version of the report file to disk (after completed_at)
        report_path = tmp_path / ".gza" / "plans" / "20260101-plan-something.md"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text("Modified plan content on disk")
        # Set mtime to 2 seconds after completed_at to guarantee drift detection
        future_ts = task.completed_at.timestamp() + 2
        os.utime(report_path, (future_ts, future_ts))

        result = run_gza("show", str(task.id), "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Report on disk has been modified since task completion" in result.stdout

    def test_cmd_show_no_warning_when_disk_not_newer(self, tmp_path: Path):
        """gza show does not show drift warning when disk report is not newer than completion."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a completed task with a future completed_at
        task = store.add("Plan task", task_type="plan")
        assert task.id is not None
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.output_content = "Plan content"
        task.report_file = ".gza/plans/20260101-plan-task.md"
        store.update(task)

        # Write report file and set its mtime to 2 seconds BEFORE completed_at
        report_path = tmp_path / ".gza" / "plans" / "20260101-plan-task.md"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text("Plan content")
        past_ts = task.completed_at.timestamp() - 2
        os.utime(report_path, (past_ts, past_ts))

        result = run_gza("show", str(task.id), "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Report on disk has been modified since task completion" not in result.stdout

    def test_cmd_show_displays_disk_content_when_newer(self, tmp_path: Path):
        """gza show displays the disk version of the report when it is newer than DB content."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Explore something", task_type="explore")
        assert task.id is not None
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.output_content = "Original DB content"
        task.report_file = ".gza/explorations/20260101-explore-something.md"
        store.update(task)

        # Write newer disk content with mtime 2 seconds after completed_at
        report_path = tmp_path / ".gza" / "explorations" / "20260101-explore-something.md"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text("Updated disk content")
        future_ts = task.completed_at.timestamp() + 2
        os.utime(report_path, (future_ts, future_ts))

        result = run_gza("show", str(task.id), "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Updated disk content" in result.stdout
        assert "Original DB content" not in result.stdout


class TestAdvanceCommand:
    """Tests for 'gza advance' command."""

    def _setup_git_repo(self, tmp_path: Path):
        """Initialize a git repo in tmp_path with an initial commit on main."""
        from gza.git import Git
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")
        (tmp_path / "README.md").write_text("initial")
        git._run("add", "README.md")
        git._run("commit", "-m", "Initial commit")
        return git

    def _create_implement_task_with_branch(self, store, git, tmp_path, prompt="Implement feature"):
        """Create a completed implement task with a real git branch."""
        task = store.add(prompt, task_type="implement")
        branch = f"feat/task-{task.id}"

        # Create the branch with a commit
        git._run("checkout", "-b", branch)
        (tmp_path / f"feat_{task.id}.txt").write_text("feature")
        git._run("add", f"feat_{task.id}.txt")
        git._run("commit", "-m", f"Add feature for task {task.id}")
        git._run("checkout", "main")

        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = branch
        task.merge_status = "unmerged"
        task.has_commits = True
        store.update(task)
        return task

    def test_advance_no_eligible_tasks(self, tmp_path: Path):
        """advance command reports no tasks when none are eligible."""
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        make_store(tmp_path)  # create empty db

        self._setup_git_repo(tmp_path)

        result = run_gza("advance", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "No eligible tasks" in result.stdout

    def test_advance_dry_run_shows_actions(self, tmp_path: Path):
        """advance --dry-run shows planned actions without executing."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        result = run_gza("advance", "--dry-run", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Would advance" in result.stdout
        assert str(task.id) in result.stdout

    def test_advance_merges_approved_task(self, tmp_path: Path):
        """advance merges a task whose review is APPROVED."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        # Create a completed review task with APPROVED verdict
        review_prompt = f"Review implementation {task.id}"
        review_task = store.add(
            review_prompt,
            task_type="review",
            depends_on=task.id,
        )
        review_task.status = "completed"
        review_task.completed_at = datetime.now(UTC)
        review_task.output_content = "**Verdict: APPROVED**\n\nLooks good!"
        store.update(review_task)

        result = run_gza("advance", "--auto", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Merged" in result.stdout or "merged" in result.stdout

        # Verify merge status updated
        updated_task = store.get(task.id)
        assert updated_task is not None
        assert updated_task.merge_status == "merged"

    def test_advance_spawns_rebase_worker_on_conflicts(self, tmp_path: Path):
        """advance spawns a background rebase --resolve worker when conflicts are detected."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)

        # Create a branch that conflicts with main
        branch = "feat/conflicting"
        git._run("checkout", "-b", branch)
        (tmp_path / "README.md").write_text("feature version")
        git._run("add", "README.md")
        git._run("commit", "-m", "Conflict commit")
        git._run("checkout", "main")

        # Modify same file on main to create a conflict
        (tmp_path / "README.md").write_text("main version")
        git._run("add", "README.md")
        git._run("commit", "-m", "Main change")

        task = store.add("Conflicting feature", task_type="implement")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = branch
        task.merge_status = "unmerged"
        task.has_commits = True
        store.update(task)

        result = run_gza("advance", "--auto", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "rebase" in result.stdout.lower()
        assert "started rebase worker" in result.stdout.lower()

        # Task should still be unmerged (rebase worker runs in background)
        updated_task = store.get(task.id)
        assert updated_task is not None
        assert updated_task.merge_status == "unmerged"

    def test_advance_targets_current_branch_for_conflict_check_and_rebase(self, tmp_path: Path):
        """advance uses the current branch (not default) for conflict detection and rebase target."""
        from gza.cli import cmd_advance
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)

        # Create an integration branch and diverge it from main.
        git._run("checkout", "-b", "agent-sessions")
        (tmp_path / "README.md").write_text("agent branch version")
        git._run("add", "README.md")
        git._run("commit", "-m", "Agent branch change")

        # Create feature from main so it can merge into main cleanly.
        git._run("checkout", "main")
        git._run("checkout", "-b", "feat/target-mismatch")
        (tmp_path / "README.md").write_text("feature branch version")
        git._run("add", "README.md")
        git._run("commit", "-m", "Feature change")

        # Return to agent-sessions so advance target is non-default.
        git._run("checkout", "agent-sessions")

        task = store.add("Conflicting on agent-sessions only", task_type="explore")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = "feat/target-mismatch"
        task.merge_status = "unmerged"
        task.has_commits = True
        store.update(task)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
            batch=None,
        )

        with patch("gza.cli._spawn_background_worker", return_value=0):
            rc = cmd_advance(args)

        assert rc == 0
        rebases = [t for t in store.get_all() if t.task_type == "rebase" and t.based_on == task.id]
        assert len(rebases) == 1
        assert "onto 'agent-sessions'" in rebases[0].prompt
        assert store.get(task.id).merge_status == "unmerged"

    def test_advance_passes_current_branch_as_merge_target(self, tmp_path: Path):
        """advance passes current branch to _merge_single_task for merge actions."""
        from gza.cli import cmd_advance
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        git._run("checkout", "-b", "agent-sessions")
        git._run("checkout", "main")
        task = store.add("Explore merge target branch", task_type="explore")
        branch = f"feat/task-{task.id}"
        git._run("checkout", "-b", branch)
        (tmp_path / f"feat_{task.id}.txt").write_text("feature")
        git._run("add", f"feat_{task.id}.txt")
        git._run("commit", "-m", f"Add feature for task {task.id}")
        git._run("checkout", "main")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = branch
        task.merge_status = "unmerged"
        task.has_commits = True
        store.update(task)
        git._run("checkout", "agent-sessions")

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
            batch=None,
        )

        captured_targets: list[str] = []

        def fake_merge(task_id, config, store, git, merge_args, target_branch):
            captured_targets.append(target_branch)
            return 0

        with patch("gza.cli._merge_single_task", side_effect=fake_merge):
            rc = cmd_advance(args)

        assert rc == 0
        assert captured_targets == ["agent-sessions"]

    def test_advance_merge_conflict_fallback_creates_rebase_and_cleans_state(self, tmp_path: Path):
        """A merge conflict during execution resets git state and falls back to rebase task creation."""
        from gza.cli import cmd_advance
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = store.add("Explore fallback behavior", task_type="explore")
        branch = f"feat/task-{task.id}"
        git._run("checkout", "-b", branch)
        (tmp_path / f"feat_{task.id}.txt").write_text("feature one")
        git._run("add", f"feat_{task.id}.txt")
        git._run("commit", "-m", f"Commit 1 for task {task.id}")
        (tmp_path / f"feat_{task.id}.txt").write_text("feature two")
        git._run("add", f"feat_{task.id}.txt")
        git._run("commit", "-m", f"Commit 2 for task {task.id}")
        git._run("checkout", "main")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = branch
        task.merge_status = "unmerged"
        task.has_commits = True
        store.update(task)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
            batch=None,
        )

        with patch("gza.cli._determine_advance_action", return_value={"type": "merge", "description": "Merge"}):
            with patch("gza.cli._merge_single_task", return_value=1):
                with patch("gza.git.Git.can_merge", return_value=False):
                    with patch("gza.git.Git.reset_hard_head") as mock_reset:
                        with patch("gza.cli._spawn_background_worker", return_value=0):
                            rc = cmd_advance(args)

        assert rc == 0
        assert mock_reset.called
        rebases = [t for t in store.get_all() if t.task_type == "rebase" and t.based_on == task.id]
        assert len(rebases) == 1
        assert "onto 'main'" in rebases[0].prompt

    def test_advance_merge_conflict_fallback_reset_failure_is_hard_error(self, tmp_path: Path):
        """When reset_hard_head fails, advance increments error_count and skips rebase task creation."""
        from gza.cli import cmd_advance
        from gza.git import GitError
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = store.add("Explore fallback reset failure", task_type="explore")
        branch = f"feat/task-{task.id}"
        git._run("checkout", "-b", branch)
        (tmp_path / f"feat_{task.id}.txt").write_text("feature")
        git._run("add", f"feat_{task.id}.txt")
        git._run("commit", "-m", f"Commit for task {task.id}")
        git._run("checkout", "main")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = branch
        task.merge_status = "unmerged"
        task.has_commits = True
        store.update(task)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
            batch=None,
        )

        output_lines: list[str] = []

        def capture_print(msg: str = "", **kwargs: object) -> None:
            output_lines.append(str(msg))

        with patch("gza.cli._determine_advance_action", return_value={"type": "merge", "description": "Merge"}):
            with patch("gza.cli._merge_single_task", return_value=1):
                with patch("gza.git.Git.can_merge", return_value=False):
                    with patch("gza.git.Git.reset_hard_head", side_effect=GitError("reset failed")):
                        with patch("gza.cli._spawn_background_worker", return_value=0) as mock_spawn:
                            with patch("gza.cli.git_ops.console") as mock_console:
                                mock_console.print.side_effect = capture_print
                                cmd_advance(args)

        # No rebase task should be created
        rebases = [t for t in store.get_all() if t.task_type == "rebase" and t.based_on == task.id]
        assert len(rebases) == 0
        # No background worker spawned for rebase
        mock_spawn.assert_not_called()
        # Output should contain a red error message about failed cleanup
        combined = "\n".join(output_lines)
        assert "Cleanup failed" in combined or "Manual intervention" in combined

    def test_advance_skips_task_with_in_progress_rebase_child(self, tmp_path: Path):
        """advance skips a task when a rebase child is already in progress."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)

        # Create a branch that conflicts with main
        branch = "feat/conflicting2"
        git._run("checkout", "-b", branch)
        (tmp_path / "README.md").write_text("feature version")
        git._run("add", "README.md")
        git._run("commit", "-m", "Conflict commit")
        git._run("checkout", "main")
        (tmp_path / "README.md").write_text("main version")
        git._run("add", "README.md")
        git._run("commit", "-m", "Main change")

        task = store.add("Conflicting feature", task_type="implement")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = branch
        task.merge_status = "unmerged"
        task.has_commits = True
        store.update(task)

        # Create an in-progress rebase child
        rebase_child = store.add(
            "Rebase branch",
            task_type="rebase",
            based_on=task.id,
            same_branch=True,
        )
        rebase_child.status = "in_progress"
        store.update(rebase_child)

        config = Config.load(tmp_path)
        action = _determine_advance_action(config, store, git, task, "main")
        assert action['type'] == 'skip'
        assert f"rebase {rebase_child.id} already in progress" in action['description']

    def test_advance_needs_discussion_for_failed_rebase_child(self, tmp_path: Path):
        """advance returns needs_discussion when a rebase child has failed."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)

        # Create a branch that conflicts with main
        branch = "feat/conflicting3"
        git._run("checkout", "-b", branch)
        (tmp_path / "README.md").write_text("feature version")
        git._run("add", "README.md")
        git._run("commit", "-m", "Conflict commit")
        git._run("checkout", "main")
        (tmp_path / "README.md").write_text("main version")
        git._run("add", "README.md")
        git._run("commit", "-m", "Main change")

        task = store.add("Conflicting feature", task_type="implement")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = branch
        task.merge_status = "unmerged"
        task.has_commits = True
        store.update(task)

        # Create a failed rebase child
        rebase_child = store.add(
            "Rebase branch",
            task_type="rebase",
            based_on=task.id,
            same_branch=True,
        )
        rebase_child.status = "failed"
        store.update(rebase_child)

        config = Config.load(tmp_path)
        action = _determine_advance_action(config, store, git, task, "main")
        assert action['type'] == 'needs_discussion'
        assert f"rebase {rebase_child.id} failed" in action['description']

    def test_advance_merges_non_implement_task_without_review(self, tmp_path: Path):
        """advance merges a non-implement task (e.g. explore) directly, skipping review creation."""
        import argparse

        from gza.cli import cmd_advance
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)

        # Create a completed explore task with a branch but no review
        task = store.add("Explore the codebase", task_type="explore")
        branch = f"feat/task-{task.id}"
        git._run("checkout", "-b", branch)
        (tmp_path / f"explore_{task.id}.txt").write_text("notes")
        git._run("add", f"explore_{task.id}.txt")
        git._run("commit", "-m", f"Exploration for task {task.id}")
        git._run("checkout", "main")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = branch
        task.merge_status = "unmerged"
        task.has_commits = True
        store.update(task)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
        )

        rc = cmd_advance(args)

        assert rc == 0

        # Verify the task was merged directly without creating a review
        updated_task = store.get(task.id)
        assert updated_task is not None
        assert updated_task.merge_status == "merged"
        assert store.get_reviews_for_task(task.id) == []

    def test_advance_creates_review_for_implement_without_review(self, tmp_path: Path):
        """advance creates a review task for a completed implement task with no review."""
        import argparse
        from unittest.mock import patch

        from gza.cli import cmd_advance
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
        )

        # Patch _spawn_background_worker to avoid actually spawning processes
        with patch("gza.cli._spawn_background_worker", return_value=0):
            rc = cmd_advance(args)

        assert rc == 0

        # Verify a review task was created (not merged directly)
        reviews = store.get_reviews_for_task(task.id)
        assert len(reviews) == 1
        assert reviews[0].task_type == 'review'

    def test_advance_creates_improve_for_changes_requested(self, tmp_path: Path):
        """advance creates an improve task when review is CHANGES_REQUESTED."""
        import argparse
        from unittest.mock import patch

        from gza.cli import cmd_advance
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        # Create a review with CHANGES_REQUESTED
        review_task = store.add(
            f"Review {task.id}",
            task_type="review",
            depends_on=task.id,
        )
        review_task.status = "completed"
        review_task.completed_at = datetime.now(UTC)
        review_task.output_content = "**Verdict: CHANGES_REQUESTED**\n\nPlease fix the tests."
        store.update(review_task)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
        )

        # Patch _spawn_background_worker to avoid actually spawning processes
        with patch("gza.cli._spawn_background_worker", return_value=0):
            rc = cmd_advance(args)

        assert rc == 0

        # Verify improve task was created
        improve_tasks = store.get_improve_tasks_for(task.id, review_task.id)
        assert len(improve_tasks) == 1
        assert improve_tasks[0].task_type == "improve"

    def test_advance_orchestrates_implement_review_improve_merge_in_local_repo(self, tmp_path: Path):
        """advance orchestrates implement -> review -> improve -> merge in a local fixture repo."""
        import argparse

        from gza.cli import cmd_advance

        setup_config(tmp_path)
        store = make_store(tmp_path)
        git = self._setup_git_repo(tmp_path)
        impl_task = self._create_implement_task_with_branch(
            store,
            git,
            tmp_path,
            prompt="Implement feature via advance workflow",
        )
        assert impl_task.id is not None

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
        )

        spawned_types: list[str] = []

        def fake_spawn(worker_args, config, task_id, **_kw):
            task = store.get(task_id)
            assert task is not None
            spawned_types.append(task.task_type)

            if task.task_type == "review":
                # First review requests changes; second review approves.
                completed_reviews = [
                    r for r in store.get_reviews_for_task(impl_task.id)
                    if r.status == "completed"
                ]
                if completed_reviews:
                    verdict = "**Verdict: APPROVED**\n\nLooks good."
                else:
                    verdict = "**Verdict: CHANGES_REQUESTED**\n\nPlease fix nits."
                store.mark_completed(task, output_content=verdict)
                return 0

            if task.task_type == "improve":
                store.mark_completed(task, output_content="Applied requested fixes.")
                store.clear_review_state(impl_task.id)
                return 0

            raise AssertionError(f"Unexpected spawned task type: {task.task_type}")

        with patch("gza.cli._spawn_background_worker", side_effect=fake_spawn):
            # 1) implement -> create+run review (CHANGES_REQUESTED)
            assert cmd_advance(args) == 0
            reviews_after_first = store.get_reviews_for_task(impl_task.id)
            assert len(reviews_after_first) == 1
            assert reviews_after_first[0].status == "completed"
            assert "CHANGES_REQUESTED" in (reviews_after_first[0].output_content or "")
            assert store.get(impl_task.id).merge_status == "unmerged"

            # 2) changes requested -> create+run improve
            assert cmd_advance(args) == 0
            first_review = store.get_reviews_for_task(impl_task.id)[0]
            improves = store.get_improve_tasks_for(impl_task.id, first_review.id)
            assert len(improves) == 1
            assert improves[0].status == "completed"
            assert store.get(impl_task.id).review_cleared_at is not None

            # 3) improved code -> create+run re-review (APPROVED)
            assert cmd_advance(args) == 0
            reviews_after_second = store.get_reviews_for_task(impl_task.id)
            assert len(reviews_after_second) == 2
            assert "APPROVED" in (reviews_after_second[0].output_content or "")

            # 4) approved review -> merge
            assert cmd_advance(args) == 0

        updated_impl = store.get(impl_task.id)
        assert updated_impl is not None
        assert updated_impl.merge_status == "merged"
        assert git.is_merged(updated_impl.branch, "main")
        assert (tmp_path / f"feat_{impl_task.id}.txt").exists()
        assert spawned_types == ["review", "improve", "review"]

    def test_advance_single_task_id(self, tmp_path: Path):
        """advance with a specific task ID only advances that task."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task1 = self._create_implement_task_with_branch(store, git, tmp_path, "Feature A")
        task2 = self._create_implement_task_with_branch(store, git, tmp_path, "Feature B")

        # Give task1 an approved review so it can merge
        review = store.add(f"Review {task1.id}", task_type="review", depends_on=task1.id)
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        review.output_content = "**Verdict: APPROVED**"
        store.update(review)

        # Advance only task1
        result = run_gza("advance", str(task1.id), "--auto", "--project", str(tmp_path))
        assert result.returncode == 0

        # task1 should be merged, task2 should still be unmerged
        assert store.get(task1.id).merge_status == "merged"
        assert store.get(task2.id).merge_status == "unmerged"

    def test_advance_rejects_bare_integer_id(self, tmp_path: Path):
        """advance requires a full prefixed task ID."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path, "Feature via integer ID")

        # Give the task an approved review so it can merge
        review = store.add(f"Review {task.id}", task_type="review", depends_on=task.id)
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        review.output_content = "**Verdict: APPROVED**"
        store.update(review)

        result = run_gza("advance", "1", "--auto", "--project", str(tmp_path))
        assert result.returncode == 1
        assert "Use a full prefixed task ID" in result.stdout or "Use a full prefixed task ID" in result.stderr
        assert store.get(task.id).merge_status == "unmerged"

    def test_advance_max_limits_batch(self, tmp_path: Path):
        """advance --max N limits the number of tasks processed."""
        import argparse
        from unittest.mock import patch

        from gza.cli import cmd_advance
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task1 = self._create_implement_task_with_branch(store, git, tmp_path, "Feature A")
        task2 = self._create_implement_task_with_branch(store, git, tmp_path, "Feature B")
        task3 = self._create_implement_task_with_branch(store, git, tmp_path, "Feature C")

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=2,
            no_docker=True,
        )

        with patch("gza.cli._spawn_background_worker", return_value=0):
            rc = cmd_advance(args)

        assert rc == 0
        # Only 2 tasks should have been processed (not 3, due to --max 2).
        # Since these are implement tasks with no reviews, reviews are created.
        # Tasks are ordered by completed_at DESC (newest first), so task3 and
        # task2 are processed while task1 (oldest) is left untouched.
        review_counts = [
            len(store.get_reviews_for_task(t.id))
            for t in [task1, task2, task3]
        ]
        assert sum(review_counts) == 2
        # task1 is the oldest so it falls outside the --max 2 window.
        assert review_counts[0] == 0

    def test_advance_spawns_worker_for_pending_review(self, tmp_path: Path):
        """advance spawns a worker for a pending review instead of skipping."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        # Create a pending review
        store.add(
            f"Review {task.id}",
            task_type="review",
            depends_on=task.id,
        )
        # review_task.status is 'pending' by default

        result = run_gza("advance", "--auto", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Started review worker" in result.stdout

    def test_advance_force_propagates_to_run_review_worker(self, tmp_path: Path):
        """advance --force forwards force override when spawning run_review workers."""
        import argparse

        from gza.cli import cmd_advance

        setup_config(tmp_path)
        store = make_store(tmp_path)
        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        store.add(
            f"Review {task.id}",
            task_type="review",
            depends_on=task.id,
        )

        captured_force: list[bool] = []

        def fake_spawn(worker_args, _config, task_id, **_kw):
            del task_id
            captured_force.append(bool(getattr(worker_args, "force", False)))
            return 0

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
            force=True,
        )

        with patch("gza.cli._spawn_background_worker", side_effect=fake_spawn):
            rc = cmd_advance(args)

        assert rc == 0
        assert captured_force == [True]

    def test_advance_force_propagates_to_resume_worker(self, tmp_path: Path):
        """advance --force forwards force override when spawning resume workers."""
        import argparse

        from gza.cli import cmd_advance

        setup_config(tmp_path)
        store = make_store(tmp_path)
        self._setup_git_repo(tmp_path)

        failed_task = store.add("Resumable failure", task_type="implement")
        failed_task.status = "failed"
        failed_task.failure_reason = "MAX_STEPS"
        failed_task.session_id = "ses_resume_123"
        store.update(failed_task)

        captured_force: list[bool] = []

        def fake_spawn_resume(worker_args, _config, _task_id, **_kw):
            captured_force.append(bool(getattr(worker_args, "force", False)))
            return 0

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
            force=True,
        )

        with patch("gza.cli._spawn_background_resume_worker", side_effect=fake_spawn_resume):
            rc = cmd_advance(args)

        assert rc == 0
        assert captured_force == [True]

    def test_advance_waits_for_in_progress_review(self, tmp_path: Path):
        """advance skips a task whose review is in_progress."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        # Create an in_progress review
        review_task = store.add(
            f"Review {task.id}",
            task_type="review",
            depends_on=task.id,
        )
        review_task.status = "in_progress"
        store.update(review_task)

        result = run_gza("advance", "--auto", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "SKIP" in result.stdout
        assert "in_progress" in result.stdout

    def test_advance_task_not_found(self, tmp_path: Path):
        """advance with non-existent task ID returns error."""
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        make_store(tmp_path)  # create db
        self._setup_git_repo(tmp_path)

        result = run_gza("advance", "testproject-999999", "--project", str(tmp_path))
        assert result.returncode == 1
        assert "not found" in result.stdout

    def test_advance_dry_run_does_not_modify_state(self, tmp_path: Path):
        """advance --dry-run does not modify task state or create tasks."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        # Add approved review so action would be merge
        review = store.add(f"Review {task.id}", task_type="review", depends_on=task.id)
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        review.output_content = "**Verdict: APPROVED**"
        store.update(review)

        result = run_gza("advance", "--dry-run", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Would advance" in result.stdout

        # Task should still be unmerged
        updated_task = store.get(task.id)
        assert updated_task.merge_status == "unmerged"

    def test_advance_task_with_no_branch_is_skipped(self, tmp_path: Path):
        """advance skips tasks that have no branch (no commits)."""
        import argparse

        from gza.cli import cmd_advance
        setup_config(tmp_path)
        store = make_store(tmp_path)

        self._setup_git_repo(tmp_path)

        # Create a task with no branch
        task = store.add("Implement feature", task_type="implement")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.merge_status = "unmerged"
        task.branch = None
        store.update(task)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
        )
        rc = cmd_advance(args)

        assert rc == 0
        # No review tasks should have been created
        reviews = store.get_reviews_for_task(task.id)
        assert len(reviews) == 0

    def test_advance_needs_discussion_verdict_skips(self, tmp_path: Path):
        """advance skips tasks whose review verdict needs manual attention."""
        import argparse

        from gza.cli import cmd_advance
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        # Create a completed review with no recognizable verdict
        review_task = store.add(
            f"Review {task.id}",
            task_type="review",
            depends_on=task.id,
        )
        review_task.status = "completed"
        review_task.completed_at = datetime.now(UTC)
        review_task.output_content = "I have some thoughts but no verdict."
        store.update(review_task)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
        )
        rc = cmd_advance(args)

        assert rc == 0
        # Task should not have been merged or had new tasks created
        updated_task = store.get(task.id)
        assert updated_task.merge_status == "unmerged"

    def test_advance_non_implement_task_skipped_in_create_review(self, tmp_path: Path):
        """advance skips creating a review for non-implement task types."""
        import argparse

        from gza.cli import cmd_advance
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)

        # Create a plan-type task with a branch
        task = store.add("Plan something", task_type="plan")
        branch = f"plan/task-{task.id}"
        git._run("checkout", "-b", branch)
        (tmp_path / f"plan_{task.id}.txt").write_text("plan")
        git._run("add", f"plan_{task.id}.txt")
        git._run("commit", "-m", f"Plan task {task.id}")
        git._run("checkout", "main")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = branch
        task.merge_status = "unmerged"
        task.has_commits = True
        store.update(task)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
        )
        rc = cmd_advance(args)

        assert rc == 0
        # No review should have been created for a plan task
        reviews = store.get_reviews_for_task(task.id)
        assert len(reviews) == 0

    def test_advance_active_improve_already_exists_is_skipped(self, tmp_path: Path):
        """advance skips creating a new improve task when one is already active."""
        import argparse

        from gza.cli import cmd_advance
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        # Create a review with CHANGES_REQUESTED
        review_task = store.add(
            f"Review {task.id}",
            task_type="review",
            depends_on=task.id,
        )
        review_task.status = "completed"
        review_task.completed_at = datetime.now(UTC)
        review_task.output_content = "**Verdict: CHANGES_REQUESTED**\n\nPlease fix the tests."
        store.update(review_task)

        # Create an already-pending improve task
        existing_improve = store.add(
            f"Improve {task.id}",
            task_type="improve",
            depends_on=review_task.id,
            based_on=task.id,
            same_branch=True,
        )
        # status is 'pending' by default

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
        )
        rc = cmd_advance(args)

        assert rc == 0
        # No additional improve task should be created
        improve_tasks = store.get_improve_tasks_for(task.id, review_task.id)
        assert len(improve_tasks) == 1
        assert improve_tasks[0].id == existing_improve.id

    def test_advance_already_merged_task_returns_early(self, tmp_path: Path):
        """advance with a specific already-merged task ID exits with 0 early."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        # Mark task as already merged
        task.merge_status = "merged"
        store.update(task)

        result = run_gza("advance", str(task.id), "--project", str(tmp_path))
        assert result.returncode == 0
        assert "already merged" in result.stdout

    def test_advance_review_cleared_at_triggers_merge(self, tmp_path: Path):
        """advance merges when review_cleared_at marks prior review as addressed (no new review)."""
        import argparse
        from unittest.mock import patch

        from gza.cli import cmd_advance
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        # Create a completed review
        review_task = store.add(
            f"Review {task.id}",
            task_type="review",
            depends_on=task.id,
        )
        review_task.status = "completed"
        review_task.completed_at = datetime.now(UTC)
        review_task.output_content = "**Verdict: CHANGES_REQUESTED**\n\nFix things."
        store.update(review_task)

        # Set review_cleared_at on the task to a time AFTER the review completed
        # (simulates an improve task having run after the review)
        import time
        time.sleep(0.01)  # ensure strictly after
        task.review_cleared_at = datetime.now(UTC)
        store.update(task)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
        )

        with patch("gza.cli._spawn_background_worker", return_value=0):
            rc = cmd_advance(args)

        assert rc == 0
        # No new review should be created — task is merged directly after improve
        all_reviews = store.get_reviews_for_task(task.id)
        assert len(all_reviews) == 1  # only the original review
        assert store.get(task.id).merge_status == "merged"

    def test_advance_batch_limits_worker_spawning(self, tmp_path: Path):
        """advance --batch B stops after B workers have been started."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)

        # Create 3 implement tasks, each with a pending review (triggers run_review)
        tasks = []
        for i in range(3):
            task = self._create_implement_task_with_branch(store, git, tmp_path, f"Feature {i}")
            store.add(
                f"Review {task.id}",
                task_type="review",
                depends_on=task.id,
            )
            tasks.append(task)

        spawn_calls = []

        def fake_spawn(worker_args, config, task_id, **_kw):
            spawn_calls.append(task_id)
            return 0

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            batch=2,
            no_docker=True,
        )

        with patch("gza.cli._spawn_background_worker", side_effect=fake_spawn):
            with patch("sys.stdout", new_callable=io.StringIO) as mock_stdout:
                rc = cmd_advance(args)
                output = mock_stdout.getvalue()

        assert rc == 0
        # Only 2 workers should have been started, not 3
        assert len(spawn_calls) == 2
        # The third task should show a batch limit message
        assert "batch limit reached" in output
        assert f"{tasks[2].id}" in output

    def test_advance_batch_merge_does_not_count_toward_limit(self, tmp_path: Path):
        """advance --batch B: merge actions don't count toward the worker limit."""
        # Use advance_requires_review=false so unreviewed tasks merge directly
        (tmp_path / "gza.yaml").write_text(
            "project_name: test-project\nadvance_requires_review: false\n"
        )
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)

        # Create 2 tasks that will merge (with APPROVED reviews)
        merge_tasks = []
        for i in range(2):
            task = self._create_implement_task_with_branch(store, git, tmp_path, f"Merge {i}")
            review_task = store.add(
                f"Review {task.id}",
                task_type="review",
                depends_on=task.id,
            )
            review_task.status = "completed"
            review_task.completed_at = datetime.now(UTC)
            review_task.output_content = "**Verdict: APPROVED**"
            store.update(review_task)
            merge_tasks.append(task)

        # Create 2 tasks with pending reviews (will spawn workers)
        worker_tasks = []
        for i in range(2):
            task = self._create_implement_task_with_branch(store, git, tmp_path, f"Worker {i}")
            store.add(
                f"Review {task.id}",
                task_type="review",
                depends_on=task.id,
            )
            worker_tasks.append(task)

        spawn_calls = []

        def fake_spawn(worker_args, config, task_id, **_kw):
            spawn_calls.append(task_id)
            return 0

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            batch=1,
            no_docker=True,
        )

        with patch("gza.cli._spawn_background_worker", side_effect=fake_spawn):
            rc = cmd_advance(args)

        assert rc == 0
        # Both merge tasks should be merged (they don't count toward batch)
        for t in merge_tasks:
            assert store.get(t.id).merge_status == "merged"
        # Only 1 worker should have been spawned (batch=1)
        assert len(spawn_calls) == 1

    def test_advance_batch_enforced_on_failed_spawn(self, tmp_path: Path):
        """advance --batch 1 attempts only one spawn even when the first spawn fails."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)

        # Create 2 implement tasks, each with a pending review (triggers run_review)
        for i in range(2):
            task = self._create_implement_task_with_branch(store, git, tmp_path, f"Feature {i}")
            store.add(
                f"Review {task.id}",
                task_type="review",
                depends_on=task.id,
            )

        spawn_calls = []

        def fake_spawn_first_fails(worker_args, config, task_id, **_kw):
            spawn_calls.append(task_id)
            # First call fails, second would succeed — but with batch=1 it should never be called
            return 1 if len(spawn_calls) == 1 else 0

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            batch=1,
            no_docker=True,
        )

        with patch("gza.cli._spawn_background_worker", side_effect=fake_spawn_first_fails):
            cmd_advance(args)

        # With batch=1, the failed spawn still counts toward the limit,
        # so only 1 spawn attempt should be made (not 2)
        assert len(spawn_calls) == 1

    def test_advance_batch_zero_returns_error(self, tmp_path: Path):
        """advance --batch 0 is rejected with an error message."""
        setup_config(tmp_path)
        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            batch=0,
            no_docker=True,
        )
        rc = cmd_advance(args)
        assert rc == 1

    def test_advance_spawn_worker_failure_increments_error_count(self, tmp_path: Path):
        """advance returns 1 when _spawn_background_worker fails for an improve task."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        # Create a CHANGES_REQUESTED review so advance will try to spawn an improve worker
        review_task = store.add(
            f"Review {task.id}",
            task_type="review",
            depends_on=task.id,
        )
        review_task.status = "completed"
        review_task.completed_at = datetime.now(UTC)
        review_task.output_content = "**Verdict: CHANGES_REQUESTED**\n\nFix things."
        store.update(review_task)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
        )

        # Simulate worker spawn failure
        with patch("gza.cli._spawn_background_worker", return_value=1):
            rc = cmd_advance(args)

        assert rc == 1

    def test_advance_interactive_shows_plan_and_prompts(self, tmp_path: Path):
        """advance without --auto shows plan and prompts for confirmation."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        self._create_implement_task_with_branch(store, git, tmp_path)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=False,
            max=None,
            no_docker=True,
        )

        # Simulate user confirming with 'y'
        with patch("builtins.input", return_value="y") as mock_input:
            with patch("gza.cli._spawn_background_worker", return_value=0):
                rc = cmd_advance(args)

        assert rc == 0
        mock_input.assert_called_once()
        call_args = mock_input.call_args[0][0]
        assert "Proceed" in call_args

    def test_advance_interactive_aborts_on_no(self, tmp_path: Path):
        """advance without --auto exits without executing when user answers 'n'."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        # Add approved review so action would be merge
        review = store.add(f"Review {task.id}", task_type="review", depends_on=task.id)
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        review.output_content = "**Verdict: APPROVED**"
        store.update(review)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=False,
            max=None,
            no_docker=True,
        )

        with patch("builtins.input", return_value="n"):
            rc = cmd_advance(args)

        assert rc == 0
        # Task should NOT have been merged
        updated_task = store.get(task.id)
        assert updated_task.merge_status == "unmerged"

    def test_advance_interactive_eof_aborts(self, tmp_path: Path):
        """advance without --auto exits cleanly when stdin is closed (EOFError)."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        self._create_implement_task_with_branch(store, git, tmp_path)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=False,
            max=None,
            no_docker=True,
        )

        with patch("builtins.input", side_effect=EOFError):
            rc = cmd_advance(args)

        assert rc == 0

    def test_advance_auto_flag_skips_prompt(self, tmp_path: Path):
        """advance --auto executes without prompting."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        # Add approved review so action is merge
        review = store.add(f"Review {task.id}", task_type="review", depends_on=task.id)
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        review.output_content = "**Verdict: APPROVED**"
        store.update(review)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
        )

        with patch("builtins.input") as mock_input:
            with patch("gza.cli._spawn_background_worker", return_value=0):
                rc = cmd_advance(args)

        assert rc == 0
        mock_input.assert_not_called()
        assert store.get(task.id).merge_status == "merged"

    def test_advance_merges_run_before_workers(self, tmp_path: Path):
        """advance executes all merge actions before spawning any background workers.

        This test fails if the sort line in cmd_advance is removed: get_unmerged()
        returns tasks ORDER BY completed_at DESC, so task_spawn (the newer task)
        appears first. Without the sort, spawn happens before merge. The sort
        reorders so merge runs first.
        """
        import argparse
        from datetime import datetime
        from unittest.mock import patch

        from gza.cli import cmd_advance
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)

        # task_merge: APPROVED review → 'merge' action.
        # Given an EARLIER completed_at so it appears second in DB order (DESC).
        task_merge = self._create_implement_task_with_branch(store, git, tmp_path, "Feature merge")
        approved_review = store.add(
            f"Review {task_merge.id}", task_type="review", depends_on=task_merge.id
        )
        approved_review.status = "completed"
        approved_review.completed_at = datetime(2026, 1, 1, tzinfo=UTC)
        approved_review.output_content = "**Verdict: APPROVED**\n\nLooks great."
        store.update(approved_review)
        task_merge.completed_at = datetime(2026, 1, 1, tzinfo=UTC)
        store.update(task_merge)

        # task_spawn: pending review → 'run_review' action (spawns a worker).
        # Given a LATER completed_at so it appears first in DB order (DESC).
        # Without the sort, this causes spawn to execute before merge.
        task_spawn = self._create_implement_task_with_branch(store, git, tmp_path, "Feature spawn")
        store.add(f"Review {task_spawn.id}", task_type="review", depends_on=task_spawn.id)
        # Leave review status as default 'pending' — this triggers run_review action.
        task_spawn.completed_at = datetime(2026, 2, 1, tzinfo=UTC)
        store.update(task_spawn)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
        )

        call_log: list[str] = []

        def fake_merge(task_id, config, store, git, merge_args, default_branch):
            call_log.append('merge')
            return 0

        def fake_spawn(spawn_args, config, task_id=None, **_kw):
            call_log.append('spawn')
            return 0

        with patch("gza.cli._merge_single_task", side_effect=fake_merge):
            with patch("gza.cli._spawn_background_worker", side_effect=fake_spawn):
                rc = cmd_advance(args)

        assert rc == 0
        assert 'merge' in call_log, "Expected at least one merge call"
        assert 'spawn' in call_log, "Expected at least one worker spawn call"
        # All merges must complete before the first spawn
        last_merge_index = max(i for i, v in enumerate(call_log) if v == 'merge')
        first_spawn_index = min(i for i, v in enumerate(call_log) if v == 'spawn')
        assert last_merge_index < first_spawn_index, (
            f"Expected all merges before first spawn, got call order: {call_log}"
        )

    def test_advance_requires_review_true_create_true_creates_review_for_unreviewed(self, tmp_path: Path):
        """advance creates a review when advance_requires_review=True, advance_create_reviews=True."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(
            "project_name: test-project\n"
            "advance_create_reviews: true\n"
            "advance_requires_review: true\n"
        )
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
            batch=None,
        )

        with patch("gza.cli._spawn_background_worker", return_value=0):
            rc = cmd_advance(args)

        assert rc == 0
        reviews = store.get_reviews_for_task(task.id)
        assert len(reviews) == 1
        assert reviews[0].task_type == 'review'
        assert store.get(task.id).merge_status != "merged"

    def test_advance_requires_review_true_create_false_skips_unreviewed(self, tmp_path: Path):
        """advance skips unreviewed implement tasks when advance_create_reviews=False."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(
            "project_name: test-project\n"
            "advance_create_reviews: false\n"
            "advance_requires_review: true\n"
        )
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        config = Config.load(tmp_path)
        action = _determine_advance_action(config, store, git, task, "main")
        assert action['type'] == 'skip'

    def test_advance_requires_review_false_merges_unreviewed(self, tmp_path: Path):
        """advance merges unreviewed implement tasks when advance_requires_review=False."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(
            "project_name: test-project\n"
            "advance_requires_review: false\n"
        )
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
            batch=None,
        )

        rc = cmd_advance(args)

        assert rc == 0
        updated_task = store.get(task.id)
        assert updated_task is not None
        assert updated_task.merge_status == "merged"
        assert store.get_reviews_for_task(task.id) == []

    def test_advance_review_cleared_always_merges_regardless_of_config(self, tmp_path: Path):
        """advance merges when review is cleared by improve, even with advance_requires_review=True."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(
            "project_name: test-project\n"
            "advance_create_reviews: true\n"
            "advance_requires_review: true\n"
        )
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        # Create a completed review
        review_task = store.add(
            f"Review {task.id}",
            task_type="review",
            depends_on=task.id,
        )
        review_task.status = "completed"
        review_task.completed_at = datetime.now(UTC)
        review_task.output_content = "**Verdict: CHANGES_REQUESTED**\n\nFix things."
        store.update(review_task)

        # Mark review as cleared (simulates improve task having run)
        time.sleep(0.01)
        task.review_cleared_at = datetime.now(UTC)
        store.update(task)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
            batch=None,
        )

        with patch("gza.cli._spawn_background_worker", return_value=0):
            rc = cmd_advance(args)

        assert rc == 0
        assert store.get(task.id).merge_status == "merged"

    # Planned test #5 (advance_requires_review=True, APPROVED review → merge) is covered by
    # the pre-existing test_advance_merges_approved_task, which verifies this happy path.

    def test_advance_default_config_creates_review_for_unreviewed(self, tmp_path: Path):
        """advance creates a review for unreviewed implement tasks with default config."""
        # Default config — no explicit advance_* flags
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        config = Config.load(tmp_path)
        # Defaults: advance_create_reviews=True, advance_requires_review=True
        assert config.advance_create_reviews is True
        assert config.advance_requires_review is True

        action = _determine_advance_action(config, store, git, task, "main")
        assert action['type'] == 'create_review'

    def test_advance_failed_review_is_treated_as_unreviewed(self, tmp_path: Path):
        """Failed review tasks should not block creating a required review."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        failed_review = store.add(
            f"Review {task.id}",
            task_type="review",
            depends_on=task.id,
        )
        failed_review.status = "failed"
        failed_review.completed_at = datetime.now(UTC)
        failed_review.output_content = "**Verdict: APPROVED**"
        store.update(failed_review)

        config = Config.load(tmp_path)
        action = _determine_advance_action(config, store, git, task, "main")
        assert action['type'] == 'create_review'

    def _create_completed_improve(self, store, impl_task, review_task):
        """Create a completed improve task for the given impl and review tasks."""
        improve = store.add(
            f"Improve {impl_task.id}",
            task_type="improve",
            depends_on=review_task.id,
            based_on=impl_task.id,
            same_branch=True,
        )
        improve.status = "completed"
        improve.completed_at = datetime.now(UTC)
        store.update(improve)
        return improve

    def test_advance_skips_task_at_max_review_cycles(self, tmp_path: Path):
        """advance skips task when completed improve count >= max_review_cycles."""
        (tmp_path / "gza.yaml").write_text(
            "project_name: test-project\nmax_review_cycles: 2\n"
        )
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        # Create a CHANGES_REQUESTED review
        review_task = store.add(
            f"Review {task.id}",
            task_type="review",
            depends_on=task.id,
        )
        review_task.status = "completed"
        review_task.completed_at = datetime.now(UTC)
        review_task.output_content = "**Verdict: CHANGES_REQUESTED**\n\nPlease fix."
        store.update(review_task)

        # Create 2 completed improve tasks (= max_review_cycles)
        self._create_completed_improve(store, task, review_task)
        self._create_completed_improve(store, task, review_task)

        config = Config.load(tmp_path)
        assert config.max_review_cycles == 2

        action = _determine_advance_action(config, store, git, task, "main")
        assert action['type'] == 'max_cycles_reached'
        assert 'max review cycles' in action['description']
        assert '2' in action['description']

    def test_advance_creates_improve_when_under_cycle_limit(self, tmp_path: Path):
        """advance creates an improve task when completed cycles < max_review_cycles."""
        (tmp_path / "gza.yaml").write_text(
            "project_name: test-project\nmax_review_cycles: 3\n"
        )
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        # Create a CHANGES_REQUESTED review
        review_task = store.add(
            f"Review {task.id}",
            task_type="review",
            depends_on=task.id,
        )
        review_task.status = "completed"
        review_task.completed_at = datetime.now(UTC)
        review_task.output_content = "**Verdict: CHANGES_REQUESTED**\n\nPlease fix."
        store.update(review_task)

        # Create 1 completed improve (below limit of 3)
        self._create_completed_improve(store, task, review_task)

        config = Config.load(tmp_path)
        action = _determine_advance_action(config, store, git, task, "main")
        assert action['type'] == 'improve'

    def test_advance_rebase_after_review_forces_new_review(self, tmp_path: Path):
        """advance creates a new review when rebase completed after latest review."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        t0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
        t1 = datetime(2026, 1, 1, 12, 0, 1, tzinfo=UTC)

        # Create a completed APPROVED review (completed first)
        review_task = store.add(
            f"Review {task.id}",
            task_type="review",
            depends_on=task.id,
        )
        review_task.status = "completed"
        review_task.completed_at = t0
        review_task.output_content = "**Verdict: APPROVED**\n\nLooks good."
        store.update(review_task)

        # Create a completed rebase (completed AFTER the review)
        rebase_task = store.add(
            f"Rebase {task.id}",
            task_type="rebase",
            based_on=task.id,
        )
        rebase_task.status = "completed"
        rebase_task.completed_at = t1
        store.update(rebase_task)

        config = Config.load(tmp_path)
        action = _determine_advance_action(config, store, git, task, "main")
        assert action['type'] == 'create_review'
        assert 'rebase' in action['description'].lower()

    def test_advance_rebase_after_review_idempotent(self, tmp_path: Path):
        """advance does not create duplicate reviews after a rebase — reuses pending review."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        t0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
        t1 = datetime(2026, 1, 1, 12, 0, 1, tzinfo=UTC)

        # Completed review, then completed rebase after it
        review_task = store.add(
            f"Review {task.id}",
            task_type="review",
            depends_on=task.id,
        )
        review_task.status = "completed"
        review_task.completed_at = t0
        review_task.output_content = "**Verdict: APPROVED**\n\nLooks good."
        store.update(review_task)

        rebase_task = store.add(
            f"Rebase {task.id}",
            task_type="rebase",
            based_on=task.id,
        )
        rebase_task.status = "completed"
        rebase_task.completed_at = t1
        store.update(rebase_task)

        # First call should want to create a review
        config = Config.load(tmp_path)
        action = _determine_advance_action(config, store, git, task, "main")
        assert action['type'] == 'create_review'

        # Simulate the review being created (pending)
        new_review = store.add(
            f"Review {task.id} (post-rebase)",
            task_type="review",
            depends_on=task.id,
        )

        # Second call should run the pending review, not create another
        action2 = _determine_advance_action(config, store, git, task, "main")
        assert action2['type'] == 'run_review'
        assert str(new_review.id) in action2['description']

    def test_advance_rebase_before_review_does_not_force_new_review(self, tmp_path: Path):
        """advance does NOT force new review when rebase completed before the latest review."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        t0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
        t1 = datetime(2026, 1, 1, 12, 0, 1, tzinfo=UTC)

        # Create a completed rebase (completed first)
        rebase_task = store.add(
            f"Rebase {task.id}",
            task_type="rebase",
            based_on=task.id,
        )
        rebase_task.status = "completed"
        rebase_task.completed_at = t0
        store.update(rebase_task)

        # Create a completed APPROVED review (completed AFTER the rebase)
        review_task = store.add(
            f"Review {task.id}",
            task_type="review",
            depends_on=task.id,
        )
        review_task.status = "completed"
        review_task.completed_at = t1
        review_task.output_content = "**Verdict: APPROVED**\n\nLooks good."
        store.update(review_task)

        config = Config.load(tmp_path)
        action = _determine_advance_action(config, store, git, task, "main")
        # Should proceed to merge, not force a new review
        assert action['type'] != 'create_review'

    def test_advance_no_rebases_no_effect_on_review(self, tmp_path: Path):
        """advance with no rebase tasks does not affect review flow."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        # Create a completed APPROVED review (no rebase at all)
        review_task = store.add(
            f"Review {task.id}",
            task_type="review",
            depends_on=task.id,
        )
        review_task.status = "completed"
        review_task.completed_at = datetime.now(UTC)
        review_task.output_content = "**Verdict: APPROVED**\n\nLooks good."
        store.update(review_task)

        config = Config.load(tmp_path)
        action = _determine_advance_action(config, store, git, task, "main")
        # Should merge, not create another review
        assert action['type'] == 'merge'

    def test_advance_multiple_rebases_only_latest_matters(self, tmp_path: Path):
        """advance checks only the latest rebase against the latest review."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        t0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
        t1 = datetime(2026, 1, 1, 12, 0, 1, tzinfo=UTC)
        t2 = datetime(2026, 1, 1, 12, 0, 2, tzinfo=UTC)

        # Old rebase (before review)
        old_rebase = store.add(
            f"Old rebase {task.id}",
            task_type="rebase",
            based_on=task.id,
        )
        old_rebase.status = "completed"
        old_rebase.completed_at = t0
        store.update(old_rebase)

        # Review (after old rebase)
        review_task = store.add(
            f"Review {task.id}",
            task_type="review",
            depends_on=task.id,
        )
        review_task.status = "completed"
        review_task.completed_at = t1
        review_task.output_content = "**Verdict: APPROVED**\n\nLooks good."
        store.update(review_task)

        # New rebase (after review) — this is the one that should invalidate
        new_rebase = store.add(
            f"New rebase {task.id}",
            task_type="rebase",
            based_on=task.id,
        )
        new_rebase.status = "completed"
        new_rebase.completed_at = t2
        store.update(new_rebase)

        config = Config.load(tmp_path)
        action = _determine_advance_action(config, store, git, task, "main")
        assert action['type'] == 'create_review'

    def test_advance_needs_attention_summary_printed(self, tmp_path: Path):
        """advance prints Needs attention section for actionable skips."""
        (tmp_path / "gza.yaml").write_text(
            "project_name: test-project\nmax_review_cycles: 1\n"
        )
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        # Create a CHANGES_REQUESTED review and 1 completed improve (= max_review_cycles=1)
        review_task = store.add(
            f"Review {task.id}",
            task_type="review",
            depends_on=task.id,
        )
        review_task.status = "completed"
        review_task.completed_at = datetime.now(UTC)
        review_task.output_content = "**Verdict: CHANGES_REQUESTED**\n\nPlease fix."
        store.update(review_task)
        self._create_completed_improve(store, task, review_task)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
            batch=None,
            max_review_cycles=None,
        )

        with patch("sys.stdout", new_callable=io.StringIO) as mock_stdout:
            rc = cmd_advance(args)
            output = mock_stdout.getvalue()

        assert rc == 0
        assert "Needs attention" in output
        assert f"{task.id}" in output
        assert "max review cycles" in output

    def test_advance_max_review_cycles_cli_override(self, tmp_path: Path):
        """--max-review-cycles overrides the config value."""
        # Config has default max_review_cycles=3; 2 completed improves would normally allow more
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        review_task = store.add(
            f"Review {task.id}",
            task_type="review",
            depends_on=task.id,
        )
        review_task.status = "completed"
        review_task.completed_at = datetime.now(UTC)
        review_task.output_content = "**Verdict: CHANGES_REQUESTED**\n\nPlease fix."
        store.update(review_task)

        # Create 2 completed improves
        self._create_completed_improve(store, task, review_task)
        self._create_completed_improve(store, task, review_task)

        # With default max_review_cycles=3, action would be 'improve' (2 < 3)
        config = Config.load(tmp_path)
        action_default = _determine_advance_action(config, store, git, task, "main")
        assert action_default['type'] == 'improve'

        # Override to 2 — now 2 completed improves == limit → max_cycles_reached
        config.max_review_cycles = 2
        action_override = _determine_advance_action(config, store, git, task, "main")
        assert action_override['type'] == 'max_cycles_reached'

    def test_advance_max_review_cycles_dry_run(self, tmp_path: Path):
        """advance --dry-run shows max_cycles_reached action without executing."""
        (tmp_path / "gza.yaml").write_text(
            "project_name: test-project\nmax_review_cycles: 1\n"
        )
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        review_task = store.add(
            f"Review {task.id}",
            task_type="review",
            depends_on=task.id,
        )
        review_task.status = "completed"
        review_task.completed_at = datetime.now(UTC)
        review_task.output_content = "**Verdict: CHANGES_REQUESTED**\n\nPlease fix."
        store.update(review_task)
        self._create_completed_improve(store, task, review_task)

        result = run_gza("advance", "--dry-run", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Would advance" in result.stdout
        assert "max review cycles" in result.stdout


    def _create_failed_task(self, store, session_id="sess-abc", failure_reason="MAX_STEPS", prompt="Implement feature"):
        """Create a failed task with given failure_reason and session_id."""
        task = store.add(prompt, task_type="implement")
        task.status = "failed"
        task.failure_reason = failure_reason
        task.session_id = session_id
        task.completed_at = datetime.now(UTC)
        task.branch = f"feat/task-{task.id}"
        store.update(task)
        return task

    def test_advance_resumes_max_steps_failed_task(self, tmp_path: Path):
        """advance creates a resume child task and spawns worker for MAX_STEPS failed task."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        self._setup_git_repo(tmp_path)

        failed_task = self._create_failed_task(store, session_id="sess-abc", failure_reason="MAX_STEPS")

        result = run_gza("advance", "--auto", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Resume" in result.stdout
        assert "Created resume task" in result.stdout

        # Verify a resume child task was created
        children = store.get_based_on_children(failed_task.id)
        assert len(children) == 1
        child = children[0]
        assert child.based_on == failed_task.id
        assert child.session_id == failed_task.session_id

    def test_advance_resumes_max_turns_failed_task(self, tmp_path: Path):
        """advance creates a resume child task and spawns worker for MAX_TURNS failed task."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        self._setup_git_repo(tmp_path)

        failed_task = self._create_failed_task(store, session_id="sess-xyz", failure_reason="MAX_TURNS")

        result = run_gza("advance", "--auto", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Resume" in result.stdout

        children = store.get_based_on_children(failed_task.id)
        assert len(children) == 1
        assert children[0].session_id == "sess-xyz"

    def test_advance_resumes_test_failure_failed_task(self, tmp_path: Path):
        """advance resumes TEST_FAILURE failed tasks from the resumable-failed query set."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        self._setup_git_repo(tmp_path)

        failed_task = self._create_failed_task(store, session_id="sess-test", failure_reason="TEST_FAILURE")

        result = run_gza("advance", "--auto", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Resume (failed: TEST_FAILURE" in result.stdout

        children = store.get_based_on_children(failed_task.id)
        assert len(children) == 1
        assert children[0].session_id == "sess-test"

    def test_advance_skips_failed_task_at_max_attempts(self, tmp_path: Path):
        """advance skips a failed task when chain depth >= max_resume_attempts."""
        (tmp_path / "gza.yaml").write_text("project_name: test-project\nmax_resume_attempts: 1\n")
        store = make_store(tmp_path)
        self._setup_git_repo(tmp_path)

        # Create a chain: original (MAX_STEPS) → first_resume (MAX_STEPS)
        original = self._create_failed_task(store, session_id="sess-1", failure_reason="MAX_STEPS")
        first_resume = store.add("Implement feature", task_type="implement")
        first_resume.status = "failed"
        first_resume.failure_reason = "MAX_STEPS"
        first_resume.session_id = "sess-2"
        first_resume.based_on = original.id
        first_resume.completed_at = datetime.now(UTC)
        store.update(first_resume)

        # max_resume_attempts=1; original is skipped (already has a child),
        # first_resume (depth=1) is skipped (at max attempts)
        result = run_gza("advance", "--auto", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "max resume attempts" in result.stdout

        # Original should NOT get a new resume child (it already has first_resume)
        original_children = store.get_based_on_children(original.id)
        assert len(original_children) == 1  # only the pre-existing first_resume
        # first_resume should not have any new children (at max attempts)
        first_resume_children = store.get_based_on_children(first_resume.id)
        assert len(first_resume_children) == 0

    def test_advance_skips_failed_task_with_existing_resume_child(self, tmp_path: Path):
        """advance skips a failed task that already has a pending/in_progress child."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        self._setup_git_repo(tmp_path)

        failed_task = self._create_failed_task(store, session_id="sess-abc", failure_reason="MAX_STEPS")

        # Create an existing pending resume child
        child = store.add("Implement feature", task_type="implement")
        child.based_on = failed_task.id
        child.status = "pending"
        store.update(child)

        result = run_gza("advance", "--auto", "--project", str(tmp_path))

        assert result.returncode == 0
        # No new child should have been created (still just the one pre-existing)
        children = store.get_based_on_children(failed_task.id)
        assert len(children) == 1  # only the pre-existing child

    def test_advance_skips_failed_task_with_completed_resume_child(self, tmp_path: Path):
        """advance skips a failed task whose resume child already completed."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        self._setup_git_repo(tmp_path)

        failed_task = self._create_failed_task(store, session_id="sess-abc", failure_reason="MAX_STEPS")

        # Create a completed resume child (simulating a successful resume)
        child = store.add("Implement feature", task_type="implement")
        child.based_on = failed_task.id
        child.status = "completed"
        store.update(child)

        result = run_gza("advance", "--auto", "--project", str(tmp_path))

        assert result.returncode == 0
        # No new child should have been created
        children = store.get_based_on_children(failed_task.id)
        assert len(children) == 1  # only the pre-existing completed child

    def test_advance_skips_failed_task_with_failed_resume_child(self, tmp_path: Path):
        """advance skips a failed task whose resume child also failed (no double-resume of root)."""
        (tmp_path / "gza.yaml").write_text("project_name: test-project\nmax_resume_attempts: 1\n")
        store = make_store(tmp_path)
        self._setup_git_repo(tmp_path)

        # Original task #198 equivalent — failed with MAX_STEPS
        original = self._create_failed_task(store, session_id="sess-abc", failure_reason="MAX_STEPS")

        # Resume child #213 equivalent — also failed with MAX_STEPS
        child = store.add("Implement feature", task_type="implement")
        child.based_on = original.id
        child.status = "failed"
        child.failure_reason = "MAX_STEPS"
        child.session_id = "sess-abc"
        store.update(child)

        result = run_gza("advance", "--dry-run", "--project", str(tmp_path))

        assert result.returncode == 0
        # The original should NOT appear in the plan — only the child should
        # (and the child should be skipped due to max resume attempts)
        assert f"{original.id}" not in result.stdout
        assert "SKIP: max resume attempts" in result.stdout

    def test_advance_no_resume_failed_flag_skips(self, tmp_path: Path):
        """advance --no-resume-failed excludes failed tasks from processing."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        self._setup_git_repo(tmp_path)

        self._create_failed_task(store, session_id="sess-abc", failure_reason="MAX_STEPS")

        result = run_gza("advance", "--auto", "--no-resume-failed", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "No eligible tasks" in result.stdout

    def test_advance_dry_run_shows_resume_action(self, tmp_path: Path):
        """advance --dry-run shows resume action without executing."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        self._setup_git_repo(tmp_path)

        failed_task = self._create_failed_task(store, session_id="sess-abc", failure_reason="MAX_STEPS")

        result = run_gza("advance", "--dry-run", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Would advance" in result.stdout
        assert "Resume" in result.stdout

        # No resume child should have been created
        children = store.get_based_on_children(failed_task.id)
        assert len(children) == 0

    def test_advance_specific_failed_task_id(self, tmp_path: Path):
        """advance with a specific failed resumable task ID works."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        self._setup_git_repo(tmp_path)

        failed_task = self._create_failed_task(store, session_id="sess-abc", failure_reason="MAX_STEPS")

        result = run_gza("advance", str(failed_task.id), "--auto", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Resume" in result.stdout

        children = store.get_based_on_children(failed_task.id)
        assert len(children) == 1

    def test_advance_skips_failed_task_without_session_id(self, tmp_path: Path):
        """advance skips failed tasks without session_id (not resumable)."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        self._setup_git_repo(tmp_path)

        # Task with no session_id — not resumable
        self._create_failed_task(store, session_id=None, failure_reason="MAX_STEPS")

        result = run_gza("advance", "--auto", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "No eligible tasks" in result.stdout

    def test_advance_max_resume_attempts_flag_overrides_config(self, tmp_path: Path):
        """advance --max-resume-attempts N overrides the config value."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        self._setup_git_repo(tmp_path)

        # Create a chain of depth 1: original (MAX_STEPS) → first_resume (MAX_STEPS)
        original = self._create_failed_task(store, session_id="sess-1", failure_reason="MAX_STEPS")
        first_resume = store.add("Implement feature", task_type="implement")
        first_resume.status = "failed"
        first_resume.failure_reason = "MAX_STEPS"
        first_resume.session_id = "sess-2"
        first_resume.based_on = original.id
        first_resume.completed_at = datetime.now(UTC)
        store.update(first_resume)

        # With --max-resume-attempts 2, original is skipped (has child),
        # first_resume (depth=1 < 2) gets resumed
        result = run_gza("advance", "--auto", "--max-resume-attempts", "2", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Resume" in result.stdout
        # Original should NOT get a new child (already has first_resume)
        original_children = store.get_based_on_children(original.id)
        assert len(original_children) == 1  # only the pre-existing first_resume
        # first_resume should get a new resume child (depth=1 < max=2)
        first_resume_children = store.get_based_on_children(first_resume.id)
        assert len(first_resume_children) == 1

    def test_advance_mode_iterate_spawns_iterate_worker(self, tmp_path: Path):
        (tmp_path / "gza.yaml").write_text(
            "project_name: test-project\n"
            "advance_mode: iterate\n"
            "iterate_max_iterations: 7\n"
        )
        store = make_store(tmp_path)
        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=str(task.id),
            dry_run=False,
            auto=True,
            max=None,
            batch=None,
            new=False,
            no_docker=True,
            plans=False,
            unimplemented=False,
            create=False,
            no_resume_failed=False,
            max_resume_attempts=None,
            advance_type=None,
            max_review_cycles=None,
            squash_threshold=None,
        )

        with (
            patch("gza.cli._spawn_background_iterate_worker", return_value=0) as spawn_iterate,
            patch("gza.cli._spawn_background_worker") as spawn_worker,
        ):
            rc = cmd_advance(args)

        assert rc == 0
        spawn_worker.assert_not_called()
        spawn_iterate.assert_called_once()
        assert spawn_iterate.call_args.kwargs["max_iterations"] == 7


    def test_advance_new_batch_spawns_distinct_tasks(self, tmp_path: Path):
        """advance --new --batch N spawns a separate worker for each pending task.

        Regression: previously all N workers were spawned without explicit task IDs,
        so each one peeked at get_next_pending() and all displayed/claimed the same task.
        """
        setup_config(tmp_path)
        store = make_store(tmp_path)

        self._setup_git_repo(tmp_path)

        # Create 4 pending tasks
        t1 = store.add("Task one", task_type="implement")
        t2 = store.add("Task two", task_type="implement")
        t3 = store.add("Task three", task_type="implement")
        t4 = store.add("Task four", task_type="implement")

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            batch=4,
            new=True,
            no_docker=True,
            plans=False,
            unimplemented=False,
            create=False,
            no_resume_failed=False,
            max_resume_attempts=None,
            advance_type=None,
            max_review_cycles=None,
            squash_threshold=None,
        )

        spawned_task_ids: list[int | None] = []

        def fake_spawn(_args, _config, task_id=None, quiet=False):
            spawned_task_ids.append(task_id)
            return 0

        with patch("gza.cli._spawn_background_worker", side_effect=fake_spawn):
            rc = cmd_advance(args)

        assert rc == 0
        # Each of the 4 pending tasks should have been passed as an explicit task_id
        assert len(spawned_task_ids) == 4
        assert set(spawned_task_ids) == {t1.id, t2.id, t3.id, t4.id}

    def test_advance_iterate_new_batch_needs_rebase_consumes_slot(self, tmp_path: Path):
        """iterate mode accounts for needs_rebase as worker-consuming in --new --batch planning/execution."""
        import io

        (tmp_path / "gza.yaml").write_text(
            "project_name: test-project\n"
            "advance_mode: iterate\n"
        )
        store = make_store(tmp_path)
        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path, "Conflict task")
        pending = store.add("Pending task", task_type="implement")

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=str(task.id),
            dry_run=False,
            auto=True,
            max=None,
            batch=1,
            new=True,
            no_docker=True,
            plans=False,
            unimplemented=False,
            create=False,
            no_resume_failed=False,
            max_resume_attempts=None,
            advance_type=None,
            max_review_cycles=None,
            squash_threshold=None,
        )

        with (
            patch("gza.cli._determine_advance_action", return_value={"type": "needs_rebase", "description": "rebase"}),
            patch("gza.cli._spawn_background_iterate_worker", return_value=0) as spawn_iterate,
            patch("gza.cli._spawn_background_worker", return_value=0) as spawn_worker,
            patch("sys.stdout", new_callable=io.StringIO) as stdout,
        ):
            rc = cmd_advance(args)
            output = stdout.getvalue()

        assert rc == 0
        assert pending.id not in output
        assert "Will start 1 new pending task(s)" not in output
        spawn_iterate.assert_called_once()
        spawn_worker.assert_not_called()

    def test_advance_iterate_new_batch_create_implement_consumes_slot(self, tmp_path: Path):
        """iterate mode accounts for create_implement as worker-consuming in --new --batch planning/execution."""
        import io

        (tmp_path / "gza.yaml").write_text(
            "project_name: test-project\n"
            "advance_mode: iterate\n"
        )
        store = make_store(tmp_path)
        self._setup_git_repo(tmp_path)

        plan = store.add("Plan task", task_type="plan")
        plan.status = "completed"
        plan.completed_at = datetime.now(UTC)
        store.update(plan)
        pending = store.add("Pending task", task_type="implement")

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=str(plan.id),
            dry_run=False,
            auto=True,
            max=None,
            batch=1,
            new=True,
            no_docker=True,
            plans=False,
            unimplemented=False,
            create=False,
            no_resume_failed=False,
            max_resume_attempts=None,
            advance_type=None,
            max_review_cycles=None,
            squash_threshold=None,
        )

        with (
            patch("gza.cli._spawn_background_iterate_worker", return_value=0) as spawn_iterate,
            patch("gza.cli._spawn_background_worker", return_value=0) as spawn_worker,
            patch("sys.stdout", new_callable=io.StringIO) as stdout,
        ):
            rc = cmd_advance(args)
            output = stdout.getvalue()

        assert rc == 0
        assert pending.id not in output
        assert "Will start 1 new pending task(s)" not in output
        spawn_iterate.assert_called_once()
        spawn_worker.assert_not_called()


class TestAdvanceMergeSquashThreshold:
    """Tests for merge_squash_threshold feature in gza advance."""

    def _setup_git_repo(self, tmp_path: Path):
        """Initialize a git repo in tmp_path with an initial commit on main."""
        from gza.git import Git
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")
        (tmp_path / "README.md").write_text("initial")
        git._run("add", "README.md")
        git._run("commit", "-m", "Initial commit")
        return git

    def _create_non_implement_task_with_branch(self, store, git, tmp_path, num_commits=1, prompt="Explore the codebase"):
        """Create a completed non-implement task with a real git branch and multiple commits."""
        task = store.add(prompt, task_type="explore")
        branch = f"feat/task-{task.id}"

        git._run("checkout", "-b", branch)
        for i in range(num_commits):
            (tmp_path / f"file_{task.id}_{i}.txt").write_text(f"content {i}")
            git._run("add", f"file_{task.id}_{i}.txt")
            git._run("commit", "-m", f"Commit {i + 1} for task {task.id}")
        git._run("checkout", "main")

        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = branch
        task.merge_status = "unmerged"
        task.has_commits = True
        store.update(task)
        return task

    def test_advance_no_squash_when_threshold_zero(self, tmp_path: Path):
        """When merge_squash_threshold=0, always use regular merge regardless of commit count."""
        import argparse
        from unittest.mock import patch

        from gza.cli import cmd_advance
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        # 3 commits on branch, but threshold is 0 (disabled)
        self._create_non_implement_task_with_branch(store, git, tmp_path, num_commits=3)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
            squash_threshold=None,  # use config default (0)
        )

        with patch("gza.git.Git.merge") as mock_merge:
            mock_merge.return_value = None
            rc = cmd_advance(args)

        assert rc == 0
        # merge should have been called with squash=False
        assert mock_merge.called
        _, kwargs = mock_merge.call_args
        assert kwargs.get("squash", False) is False

    def test_advance_squash_when_commits_meet_threshold(self, tmp_path: Path):
        """When commit_count >= merge_squash_threshold, use squash merge."""
        import argparse
        from unittest.mock import patch

        from gza.cli import cmd_advance
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        # 3 commits, threshold=2 → should squash
        self._create_non_implement_task_with_branch(store, git, tmp_path, num_commits=3)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
            squash_threshold=2,  # squash when >= 2 commits
        )

        with patch("gza.git.Git.merge") as mock_merge:
            mock_merge.return_value = None
            rc = cmd_advance(args)

        assert rc == 0
        assert mock_merge.called
        _, kwargs = mock_merge.call_args
        assert kwargs.get("squash", False) is True

    def test_advance_no_squash_when_commits_below_threshold(self, tmp_path: Path):
        """When commit_count < merge_squash_threshold, use regular merge."""
        import argparse
        from unittest.mock import patch

        from gza.cli import cmd_advance
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        # 2 commits, threshold=3 → should NOT squash
        self._create_non_implement_task_with_branch(store, git, tmp_path, num_commits=2)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
            squash_threshold=3,  # squash only when >= 3 commits
        )

        with patch("gza.git.Git.merge") as mock_merge:
            mock_merge.return_value = None
            rc = cmd_advance(args)

        assert rc == 0
        assert mock_merge.called
        _, kwargs = mock_merge.call_args
        assert kwargs.get("squash", False) is False

    def test_advance_squash_threshold_cli_override(self, tmp_path: Path):
        """--squash-threshold N overrides config.merge_squash_threshold: dry-run shows auto-squash."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        # 2 commits; passing --squash-threshold 2 on the CLI should trigger auto-squash
        self._create_non_implement_task_with_branch(store, git, tmp_path, num_commits=2)

        result = run_gza("advance", "--dry-run", "--squash-threshold", "2", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "auto-squash" in result.stdout

    def test_advance_dry_run_shows_squash_annotation(self, tmp_path: Path):
        """Dry-run output includes '(auto-squash, N commits)' when threshold is met."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        # 3 commits, threshold=2 in config → auto-squash annotation should appear
        self._create_non_implement_task_with_branch(store, git, tmp_path, num_commits=3)

        # Write config with merge_squash_threshold=2
        (tmp_path / "gza.yaml").write_text("project_name: test-project\nmerge_squash_threshold: 2\n")

        result = run_gza("advance", "--dry-run", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "auto-squash" in result.stdout

    def test_default_merge_squash_threshold_is_zero(self, tmp_path: Path):
        """Default merge_squash_threshold is 0 (disabled)."""
        from gza.config import Config
        setup_config(tmp_path)
        config = Config.load(tmp_path)
        assert config.merge_squash_threshold == 0
        assert config.advance_mode == "work"
        assert config.max_resume_attempts == 3
        assert config.iterate_max_iterations == 5

    def test_yaml_merge_squash_threshold_parsed(self, tmp_path: Path):
        """merge_squash_threshold is correctly parsed from gza.yaml."""
        from gza.config import Config
        (tmp_path / "gza.yaml").write_text("project_name: test-project\nmerge_squash_threshold: 3\n")
        config = Config.load(tmp_path)
        assert config.merge_squash_threshold == 3

    def test_yaml_parses_advance_mode_and_iterate_max_iterations(self, tmp_path: Path):
        from gza.config import Config

        (tmp_path / "gza.yaml").write_text(
            "project_name: test-project\nadvance_mode: iterate\niterate_max_iterations: 9\n"
        )
        config = Config.load(tmp_path)
        assert config.advance_mode == "iterate"
        assert config.iterate_max_iterations == 9

    def test_invalid_type_raises_config_error(self, tmp_path: Path):
        """Non-integer merge_squash_threshold in yaml raises ConfigError, not bare ValueError."""
        import pytest

        from gza.config import Config, ConfigError
        (tmp_path / "gza.yaml").write_text("project_name: test-project\nmerge_squash_threshold: two\n")
        with pytest.raises(ConfigError):
            Config.load(tmp_path)

    def test_negative_value_raises_config_error(self, tmp_path: Path):
        """Negative merge_squash_threshold in yaml raises ConfigError."""
        import pytest

        from gza.config import Config, ConfigError
        (tmp_path / "gza.yaml").write_text("project_name: test-project\nmerge_squash_threshold: -1\n")
        with pytest.raises(ConfigError):
            Config.load(tmp_path)

class TestAdvanceUnimplementedCommand:
    """Tests for 'gza advance --unimplemented' command."""

    def test_advance_unimplemented_lists_completed_plan_and_explore_without_impl(self, tmp_path: Path):
        """advance --unimplemented lists completed plans/explores with no implement task."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan = store.add("Design the authentication system", task_type="plan")
        explore = store.add("Explore auth provider options", task_type="explore")
        plan.status = "completed"
        explore.status = "completed"
        from datetime import datetime
        now = datetime.now(UTC)
        plan.completed_at = now
        explore.completed_at = now
        store.update(plan)
        store.update(explore)

        result = run_gza("advance", "--unimplemented", "--project", str(tmp_path))

        assert result.returncode == 0
        assert str(plan.id) in result.stdout
        assert str(explore.id) in result.stdout
        assert "[plan]" in result.stdout
        assert "[explore]" in result.stdout
        assert "gza implement" in result.stdout

    def test_advance_unimplemented_excludes_tasks_with_impl(self, tmp_path: Path):
        """advance --unimplemented excludes tasks that already have an implement task."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        from datetime import datetime

        plan = store.add("A plan", task_type="plan")
        explore = store.add("An explore", task_type="explore")
        plan.status = "completed"
        explore.status = "completed"
        now = datetime.now(UTC)
        plan.completed_at = now
        explore.completed_at = now
        store.update(plan)
        store.update(explore)

        store.add("Implement plan", task_type="implement", based_on=plan.id)
        store.add("Implement explore", task_type="implement", based_on=explore.id)

        result = run_gza("advance", "--unimplemented", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "No completed plan/explore tasks without implementation tasks." in result.stdout

    def test_advance_unimplemented_guidance_distinguishes_plan_vs_explore(self, tmp_path: Path):
        """advance --unimplemented guidance is accurate for explores in list mode."""
        from datetime import datetime

        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan = store.add("Plan E", task_type="plan")
        explore = store.add("Explore E", task_type="explore")
        plan.status = "completed"
        explore.status = "completed"
        now = datetime.now(UTC)
        plan.completed_at = now
        explore.completed_at = now
        store.update(plan)
        store.update(explore)

        result = run_gza("advance", "--unimplemented", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Run 'gza advance' to create and start implement tasks for completed plan tasks." in result.stdout
        assert "Run 'gza advance --unimplemented --create' to create implement tasks for completed explore tasks." in result.stdout
        assert "Run 'gza advance' to create and start implement tasks." not in result.stdout

    def test_advance_unimplemented_create_queues_implement_tasks(self, tmp_path: Path):
        """advance --unimplemented --create creates implement tasks for each listed task."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        from datetime import datetime

        plan = store.add("Plan A", task_type="plan")
        plan.status = "completed"
        plan.completed_at = datetime.now(UTC)
        store.update(plan)

        explore = store.add("Explore B", task_type="explore")
        explore.status = "completed"
        explore.completed_at = datetime.now(UTC)
        store.update(explore)

        result = run_gza("advance", "--unimplemented", "--create", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Created" in result.stdout

        all_tasks = store.get_all()
        impl_tasks = [t for t in all_tasks if t.task_type == "implement"]
        assert len(impl_tasks) == 2
        by_based_on = {t.based_on: t for t in impl_tasks}
        assert plan.id in by_based_on
        assert explore.id in by_based_on
        assert by_based_on[plan.id].prompt.startswith(f"Implement plan from task {plan.id}")
        assert by_based_on[explore.id].prompt.startswith(f"Implement findings from task {explore.id}")

    def test_advance_unimplemented_dry_run_no_create(self, tmp_path: Path):
        """advance --unimplemented --create --dry-run shows preview but creates nothing."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        from datetime import datetime

        plan = store.add("Plan C", task_type="plan")
        plan.status = "completed"
        plan.completed_at = datetime.now(UTC)
        store.update(plan)

        result = run_gza("advance", "--unimplemented", "--create", "--dry-run", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "dry-run" in result.stdout.lower() or "Would create" in result.stdout

        all_tasks = store.get_all()
        impl_tasks = [t for t in all_tasks if t.task_type == "implement"]
        assert len(impl_tasks) == 0

    def test_advance_unimplemented_targeted_query_ignores_non_source_tasks(self, tmp_path: Path):
        """advance --unimplemented filters by implement based_on regardless of task noise."""
        from datetime import datetime

        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan_with_impl = store.add("Plan with impl", task_type="plan")
        plan_with_impl.status = "completed"
        plan_with_impl.completed_at = datetime.now(UTC)
        store.update(plan_with_impl)

        explore_without_impl = store.add("Explore without impl", task_type="explore")
        explore_without_impl.status = "completed"
        explore_without_impl.completed_at = datetime.now(UTC)
        store.update(explore_without_impl)

        assert plan_with_impl.id is not None and explore_without_impl.id is not None

        store.add("Impl 1", task_type="implement", based_on=plan_with_impl.id)

        for i in range(20):
            t = store.add(f"Task {i}", task_type="review")
            t.based_on = plan_with_impl.id
            store.update(t)

        result = run_gza("advance", "--unimplemented", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Explore without impl" in result.stdout
        assert "Plan with impl" not in result.stdout

    def test_advance_plans_alias_keeps_plan_only_behavior(self, tmp_path: Path):
        """legacy --plans remains supported and only targets plan tasks."""
        from datetime import datetime

        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan = store.add("Plan D", task_type="plan")
        explore = store.add("Explore D", task_type="explore")
        plan.status = "completed"
        explore.status = "completed"
        now = datetime.now(UTC)
        plan.completed_at = datetime.now(UTC)
        explore.completed_at = now
        store.update(plan)
        store.update(explore)

        result = run_gza("advance", "--plans", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "deprecated" in result.stderr.lower()
        assert str(plan.id) in result.stdout
        assert str(explore.id) not in result.stdout


class TestAdvanceAutoPlans:
    """Tests for auto-advancing completed plans via 'gza advance'."""

    def _setup_git_repo(self, tmp_path: Path):
        """Initialize a git repo in tmp_path with an initial commit on main."""
        from gza.git import Git
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")
        (tmp_path / "README.md").write_text("initial")
        git._run("add", "README.md")
        git._run("commit", "-m", "Initial commit")
        return git

    def _create_completed_plan(self, store, prompt="Design the feature"):
        """Create a completed plan task (no branch)."""
        from datetime import datetime
        plan = store.add(prompt, task_type="plan")
        plan.status = "completed"
        plan.completed_at = datetime.now(UTC)
        store.update(plan)
        return plan

    def _create_implement_task_with_branch(self, store, git, tmp_path, prompt="Implement feature", based_on=None):
        """Create a completed implement task with a real git branch."""
        from datetime import datetime
        task = store.add(prompt, task_type="implement", based_on=based_on)
        branch = f"feat/task-{task.id}"
        git._run("checkout", "-b", branch)
        (tmp_path / f"feat_{task.id}.txt").write_text("feature")
        git._run("add", f"feat_{task.id}.txt")
        git._run("commit", "-m", f"Add feature for task {task.id}")
        git._run("checkout", "main")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = branch
        task.merge_status = "unmerged"
        task.has_commits = True
        store.update(task)
        return task

    def test_advance_creates_implement_for_completed_plan(self, tmp_path: Path):
        """advance creates and starts an implement task for a completed plan with no implement child."""
        import io
        from unittest.mock import patch

        from gza.cli import cmd_advance

        setup_config(tmp_path)
        store = make_store(tmp_path)
        self._setup_git_repo(tmp_path)

        plan = self._create_completed_plan(store, "Design auth system")
        plan.slug = "20260305-design-auth-system-2"
        store.update(plan)

        spawn_calls = []

        def fake_spawn(worker_args, config, task_id=None, **_kw):
            spawn_calls.append(task_id)
            return 0

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            batch=None,
            no_docker=True,
        )

        with patch("gza.cli._spawn_background_worker", side_effect=fake_spawn):
            with patch("sys.stdout", new_callable=io.StringIO) as mock_stdout:
                rc = cmd_advance(args)
                output = mock_stdout.getvalue()

        assert rc == 0
        assert "Created implement task" in output
        assert "Started implement worker" in output

        # Verify the implement task was created with correct based_on
        all_tasks = store.get_all()
        impl_tasks = [t for t in all_tasks if t.task_type == "implement"]
        assert len(impl_tasks) == 1
        assert impl_tasks[0].based_on == plan.id
        assert impl_tasks[0].prompt == f"Implement plan from task {plan.id}: design-auth-system"

    def test_advance_skips_plan_with_existing_implement(self, tmp_path: Path):
        """advance skips a completed plan that already has an implement child.

        When targeted by task_id, the skip message is shown. In batch mode
        the plan is simply excluded from the candidate list.
        """
        import io

        from gza.cli import cmd_advance

        setup_config(tmp_path)
        store = make_store(tmp_path)
        self._setup_git_repo(tmp_path)

        plan = self._create_completed_plan(store, "Design auth system")
        # Create an implement task based on this plan
        store.add("Implement auth", task_type="implement", based_on=plan.id)

        # Target the plan by task_id to see the skip message
        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=plan.id,
            dry_run=True,
            auto=True,
            max=None,
            batch=None,
            no_docker=True,
        )

        with patch("sys.stdout", new_callable=io.StringIO) as mock_stdout:
            rc = cmd_advance(args)
            output = mock_stdout.getvalue()

        assert rc == 0
        assert "implement task already exists" in output

    def test_advance_type_plan_filters_to_plans_only(self, tmp_path: Path):
        """--type plan only processes plan tasks, not implement tasks."""
        import io

        from gza.cli import cmd_advance

        # Disable review requirement so implement would normally merge
        (tmp_path / "gza.yaml").write_text(
            "project_name: test-project\nadvance_requires_review: false\n"
        )

        store = make_store(tmp_path)
        git = self._setup_git_repo(tmp_path)

        plan = self._create_completed_plan(store, "Design feature X")
        self._create_implement_task_with_branch(store, git, tmp_path)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=True,
            auto=True,
            max=None,
            batch=None,
            no_docker=True,
            advance_type="plan",
        )

        with patch("sys.stdout", new_callable=io.StringIO) as mock_stdout:
            rc = cmd_advance(args)
            output = mock_stdout.getvalue()

        assert rc == 0
        # Plan should be in the output
        assert str(plan.id) in output
        assert "Create and start implement" in output
        # Implement task should NOT be in the output
        assert "Merge" not in output

    def test_advance_type_implement_filters_to_implements_only(self, tmp_path: Path):
        """--type implement only processes implement tasks, not plans."""
        import io

        from gza.cli import cmd_advance

        (tmp_path / "gza.yaml").write_text(
            "project_name: test-project\nadvance_requires_review: false\n"
        )

        store = make_store(tmp_path)
        git = self._setup_git_repo(tmp_path)

        self._create_completed_plan(store, "Design feature X")
        impl = self._create_implement_task_with_branch(store, git, tmp_path)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=True,
            auto=True,
            max=None,
            batch=None,
            no_docker=True,
            advance_type="implement",
        )

        with patch("sys.stdout", new_callable=io.StringIO) as mock_stdout:
            rc = cmd_advance(args)
            output = mock_stdout.getvalue()

        assert rc == 0
        # Implement task should be in the output
        assert str(impl.id) in output
        assert "Merge" in output
        # Plan should NOT appear (no "Create and start implement")
        assert "Create and start implement" not in output

    def test_advance_create_implement_respects_batch_limit(self, tmp_path: Path):
        """batch limit applies to plan->implement worker spawns."""
        import io
        from unittest.mock import patch

        from gza.cli import cmd_advance

        setup_config(tmp_path)
        store = make_store(tmp_path)
        self._setup_git_repo(tmp_path)

        # Create two completed plans
        self._create_completed_plan(store, "Plan A")
        self._create_completed_plan(store, "Plan B")

        spawn_calls = []

        def fake_spawn(worker_args, config, task_id=None, **_kw):
            spawn_calls.append(task_id)
            return 0

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            batch=1,
            no_docker=True,
        )

        with patch("gza.cli._spawn_background_worker", side_effect=fake_spawn):
            with patch("sys.stdout", new_callable=io.StringIO) as mock_stdout:
                rc = cmd_advance(args)
                output = mock_stdout.getvalue()

        assert rc == 0
        # Only one worker should have been spawned due to batch limit
        assert len(spawn_calls) == 1
        assert "batch limit reached" in output


class TestPrCommand:
    """Tests for 'gza pr' command."""

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


class TestRefreshCommand:
    """Tests for 'gza refresh' command."""

    def _setup_git_repo(self, tmp_path: Path):
        """Initialize a git repo with an initial commit."""
        from gza.git import Git
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")
        (tmp_path / "base.txt").write_text("base content")
        git._run("add", "base.txt")
        git._run("commit", "-m", "Initial commit")
        return git

    def test_refresh_single_task_with_branch(self, tmp_path: Path):
        """gza refresh <id> updates diff stats for a single task."""

        setup_config(tmp_path)
        git = self._setup_git_repo(tmp_path)

        # Create a feature branch with changes
        git._run("checkout", "-b", "feat/test-task")
        (tmp_path / "new_file.py").write_text("x = 1\ny = 2\n")
        git._run("add", "new_file.py")
        git._run("commit", "-m", "Add new file")
        git._run("checkout", "main")

        store = make_store(tmp_path)

        task = store.add("Test task", task_type="implement")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = "feat/test-task"
        task.merge_status = "unmerged"
        task.has_commits = True
        store.update(task)

        result = run_gza("refresh", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "in 1 files" in result.stdout

        retrieved = store.get(task.id)
        assert retrieved is not None
        assert retrieved.diff_files_changed == 1
        assert retrieved.diff_lines_added == 2
        assert retrieved.diff_lines_removed == 0

    def test_refresh_single_task_not_found(self, tmp_path: Path):
        """gza refresh <id> returns error when task doesn't exist."""
        setup_config(tmp_path)
        self._setup_git_repo(tmp_path)
        result = run_gza("refresh", "testproject-999999", "--project", str(tmp_path))
        assert result.returncode == 1
        assert "not found" in result.stdout or "not found" in result.stderr

    def test_refresh_single_task_no_branch(self, tmp_path: Path):
        """gza refresh <id> skips task without a branch."""

        setup_config(tmp_path)
        self._setup_git_repo(tmp_path)

        store = make_store(tmp_path)

        task = store.add("No branch task", task_type="explore")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        result = run_gza("refresh", str(task.id), "--project", str(tmp_path))
        assert result.returncode == 0
        assert "skipping" in result.stdout

    def test_refresh_single_task_branch_missing(self, tmp_path: Path):
        """gza refresh <id> warns and skips when branch no longer exists."""

        setup_config(tmp_path)
        self._setup_git_repo(tmp_path)

        store = make_store(tmp_path)

        task = store.add("Task with deleted branch", task_type="implement")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = "feat/deleted"
        task.merge_status = "unmerged"
        task.has_commits = True
        store.update(task)

        result = run_gza("refresh", str(task.id), "--project", str(tmp_path))
        assert result.returncode == 0
        assert "skipping" in result.stdout

    def test_refresh_all_unmerged(self, tmp_path: Path):
        """gza refresh (no args) refreshes all unmerged tasks."""

        setup_config(tmp_path)
        git = self._setup_git_repo(tmp_path)

        # Create two feature branches
        git._run("checkout", "-b", "feat/task-1")
        (tmp_path / "task1.py").write_text("a = 1\n")
        git._run("add", "task1.py")
        git._run("commit", "-m", "Task 1 work")
        git._run("checkout", "main")

        git._run("checkout", "-b", "feat/task-2")
        (tmp_path / "task2.py").write_text("b = 2\nc = 3\n")
        git._run("add", "task2.py")
        git._run("commit", "-m", "Task 2 work")
        git._run("checkout", "main")

        store = make_store(tmp_path)

        task1 = store.add("Task 1", task_type="implement")
        task1.status = "completed"
        task1.completed_at = datetime.now(UTC)
        task1.branch = "feat/task-1"
        task1.merge_status = "unmerged"
        task1.has_commits = True
        store.update(task1)

        task2 = store.add("Task 2", task_type="implement")
        task2.status = "completed"
        task2.completed_at = datetime.now(UTC)
        task2.branch = "feat/task-2"
        task2.merge_status = "unmerged"
        task2.has_commits = True
        store.update(task2)

        result = run_gza("refresh", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Refreshed 2 task(s)" in result.stdout
