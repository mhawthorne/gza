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

from gza.cli import cmd_advance
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
