"""Comprehensive tests for Git operations."""

from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

from gza.git import (
    Git,
    GitError,
    GitWorktreeHealthProbe,
    ResolvedGitRef,
    active_worktree_path_for_branch,
    cleanup_worktree_for_branch,
    parse_diff_numstat,
    resolve_ref_if_possible,
    validate_host_worktree_admin_metadata,
)


class TestGitInit:
    """Tests for Git initialization."""

    def test_init_with_repo_dir(self, tmp_path: Path):
        """Test Git initialization with a repository directory."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)
        assert git.repo_dir == repo_dir


class TestCleanupWorktreeForBranch:
    """Tests for cleanup_worktree_for_branch helper."""

    def test_raises_when_live_worktree_remove_fails_without_deleting_registration(self, tmp_path: Path):
        """A failed live remove must preserve the branch registration."""
        git = Git(tmp_path)
        worktree_path = tmp_path / "worktrees" / "branch"
        registration_dir = tmp_path / ".git" / "worktrees" / "feature-test"
        registration_dir.mkdir(parents=True)

        with patch.object(git, "worktree_list", return_value=[
            {"path": str(worktree_path), "branch": "refs/heads/feature/test"}
        ]), \
             patch.object(
                 git,
                 "worktree_remove",
                 return_value=MagicMock(returncode=1, stdout="", stderr="worktree is locked"),
             ) as mock_remove, \
             patch("gza.git._worktree_registration_dir_for_branch", return_value=registration_dir), \
            patch("gza.git.Git.has_changes", return_value=False):
            with pytest.raises(GitError, match="git worktree remove failed"):
                cleanup_worktree_for_branch(git, "feature/test", force=True)

            mock_remove.assert_called_once_with(worktree_path.resolve(strict=False), force=True)
            assert registration_dir.exists()

    def test_raises_when_worktree_still_registered_after_remove(self, tmp_path: Path):
        """A failed remove should not be reported as successful."""
        git = Git(tmp_path)
        worktree_path = tmp_path / "worktrees" / "branch"

        with patch.object(git, "worktree_list") as mock_list, \
             patch.object(git, "worktree_remove") as mock_remove, \
             patch("gza.git._worktree_registration_dir_for_branch", return_value=None), \
             patch("gza.git.Git.has_changes", return_value=False):
            mock_remove.return_value = MagicMock(returncode=0, stdout="", stderr="")
            mock_list.side_effect = [
                [{"path": str(worktree_path), "branch": "refs/heads/feature/test"}],
                [{"path": str(worktree_path), "branch": "refs/heads/feature/test"}],
            ]

            with pytest.raises(GitError, match="still registered"):
                cleanup_worktree_for_branch(git, "feature/test", force=True)

            mock_remove.assert_called_once_with(worktree_path.resolve(strict=False), force=True)

    def test_removes_targeted_registration_after_remove(self, tmp_path: Path):
        """A stale registration for the same branch is removed directly."""
        git = Git(tmp_path)
        worktree_path = tmp_path / "worktrees" / "branch"
        registration_dir = tmp_path / ".git" / "worktrees" / "feature-test"
        registration_dir.mkdir(parents=True)

        with patch.object(git, "worktree_list") as mock_list, \
             patch.object(git, "worktree_remove") as mock_remove, \
             patch("gza.git._worktree_registration_dir_for_branch", return_value=registration_dir), \
             patch("gza.git.Git.has_changes", return_value=False):
            mock_remove.return_value = MagicMock(returncode=0, stdout="", stderr="")
            mock_list.side_effect = [
                [{"path": str(worktree_path), "branch": "refs/heads/feature/test"}],
                [],
            ]

            result = cleanup_worktree_for_branch(git, "feature/test", force=True)

            assert result == worktree_path.resolve(strict=False)
            mock_remove.assert_called_once_with(worktree_path.resolve(strict=False), force=True)
            assert not registration_dir.exists()

    def test_removes_prunable_only_target_registration(self, tmp_path: Path):
        """A prunable registration is removed without touching unrelated cleanup."""
        git = Git(tmp_path)
        registration_dir = tmp_path / ".git" / "worktrees" / "feature-test"
        registration_dir.mkdir(parents=True)

        with patch.object(git, "worktree_list") as mock_list, \
             patch.object(git, "worktree_remove") as mock_remove, \
             patch("gza.git._worktree_registration_dir_for_branch", return_value=registration_dir):
            mock_list.side_effect = [
                [{"path": "/tmp/gza/stale", "branch": "refs/heads/feature/test", "prunable": "gone"}],
                [],
            ]

            result = cleanup_worktree_for_branch(git, "feature/test", force=True)

            assert result is None
            mock_remove.assert_not_called()
            assert not registration_dir.exists()

    def test_refuses_to_remove_live_worktree_outside_permitted_roots(self, tmp_path: Path):
        """Foreign live worktrees must fail closed and preserve registration."""
        git = Git(tmp_path)
        managed_root = tmp_path / "managed"
        foreign_path = tmp_path / "foreign" / "feature-test"
        registration_dir = tmp_path / ".git" / "worktrees" / "feature-test"
        registration_dir.mkdir(parents=True)

        with (
            patch.object(git, "worktree_list", side_effect=[
                [{"path": str(foreign_path), "branch": "refs/heads/feature/test"}],
                [{"path": str(foreign_path), "branch": "refs/heads/feature/test"}],
            ]),
            patch.object(git, "worktree_remove") as mock_remove,
            patch("gza.git._worktree_registration_dir_for_branch", return_value=registration_dir),
        ):
            with pytest.raises(GitError, match="Refusing to remove worktree for branch 'feature/test'") as exc_info:
                cleanup_worktree_for_branch(
                    git,
                    "feature/test",
                    force=True,
                    permitted_root_paths=[managed_root],
                )

            assert str(foreign_path.resolve(strict=False)) in str(exc_info.value)
            assert "git worktree remove --force" in str(exc_info.value)
            mock_remove.assert_not_called()
            assert registration_dir.exists()

    def test_removes_live_worktree_equal_to_permitted_root(self, tmp_path: Path):
        """A worktree exactly at the permitted root should still be removable."""
        git = Git(tmp_path)
        managed_root = tmp_path / "managed"
        worktree_path = managed_root
        registration_dir = tmp_path / ".git" / "worktrees" / "feature-test"
        registration_dir.mkdir(parents=True)

        with (
            patch.object(git, "worktree_list") as mock_list,
            patch.object(git, "worktree_remove") as mock_remove,
            patch("gza.git._worktree_registration_dir_for_branch", return_value=registration_dir),
            patch("gza.git.Git.has_changes", return_value=False),
        ):
            mock_remove.return_value = MagicMock(returncode=0, stdout="", stderr="")
            mock_list.side_effect = [
                [{"path": str(worktree_path), "branch": "refs/heads/feature/test"}],
                [],
            ]

            result = cleanup_worktree_for_branch(
                git,
                "feature/test",
                force=True,
                permitted_root_paths=[managed_root],
            )

            assert result == worktree_path.resolve(strict=False)
            mock_remove.assert_called_once_with(worktree_path.resolve(strict=False), force=True)
            assert not registration_dir.exists()

    def test_removes_live_worktree_within_permitted_roots(self, tmp_path: Path):
        """Managed live worktrees should still be removed normally."""
        git = Git(tmp_path)
        managed_root = tmp_path / "managed"
        worktree_path = managed_root / "feature-test"
        registration_dir = tmp_path / ".git" / "worktrees" / "feature-test"
        registration_dir.mkdir(parents=True)

        with (
            patch.object(git, "worktree_list") as mock_list,
            patch.object(git, "worktree_remove") as mock_remove,
            patch("gza.git._worktree_registration_dir_for_branch", return_value=registration_dir),
            patch("gza.git.Git.has_changes", return_value=False),
        ):
            mock_remove.return_value = MagicMock(returncode=0, stdout="", stderr="")
            mock_list.side_effect = [
                [{"path": str(worktree_path), "branch": "refs/heads/feature/test"}],
                [],
            ]

            result = cleanup_worktree_for_branch(
                git,
                "feature/test",
                force=True,
                permitted_root_paths=[managed_root],
            )

            assert result == worktree_path.resolve(strict=False)
            mock_remove.assert_called_once_with(worktree_path.resolve(strict=False), force=True)
            assert not registration_dir.exists()
class TestGitRun:
    """Tests for the _run helper method."""

    def test_run_successful_command(self, tmp_path: Path):
        """Test _run with a successful command."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch('subprocess.run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="success", stderr="")
            result = git._run("status")

            mock_run.assert_called_once()
            assert result.returncode == 0
            assert result.stdout == "success"

    def test_run_with_check_false_doesnt_raise(self, tmp_path: Path):
        """Test _run with check=False doesn't raise on non-zero exit."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch('subprocess.run') as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="error")
            result = git._run("status", check=False)

            assert result.returncode == 1

    def test_run_with_check_true_raises_on_error(self, tmp_path: Path):
        """Test _run with check=True raises GitError on non-zero exit."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch('subprocess.run') as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="error message")

            with pytest.raises(GitError) as exc_info:
                git._run("invalid-command")

            assert "error message" in str(exc_info.value)

    def test_run_with_stdin(self, tmp_path: Path):
        """Test _run passes stdin correctly."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch('subprocess.run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git._run("hash-object", "-w", "--stdin", stdin=b"test content")

            call_args = mock_run.call_args
            assert call_args.kwargs.get('input') == "test content"


class TestGitWorktreeHealth:
    """Tests for shared worktree-health primitives."""

    def test_worktree_health_probe_returns_command_and_outputs(self, tmp_path: Path):
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=128,
                stdout="stdout text",
                stderr="stderr text",
            )

            probe = git.worktree_health_probe()

        assert probe == GitWorktreeHealthProbe(
            command="git worktree list --porcelain",
            returncode=128,
            stdout="stdout text",
            stderr="stderr text",
        )
        assert probe.failed is True

    def test_validate_host_worktree_admin_metadata_detects_commondir_leak(self, tmp_path: Path):
        repo_dir = tmp_path / "repo"
        common_dir = repo_dir / ".git"
        registration_dir = common_dir / "worktrees" / "broken"
        registration_dir.mkdir(parents=True)
        (registration_dir / "commondir").write_text("/gza-git/common\n")
        (registration_dir / "gitdir").write_text("/workspace/repo/.git\n")
        git = Git(repo_dir)

        with patch("gza.git._git_common_dir", return_value=common_dir):
            validation = validate_host_worktree_admin_metadata(git)

        assert validation.is_healthy is False
        assert validation.suspected_container_path_marker == "/gza-git"
        assert len(validation.issues) == 1
        issue = validation.issues[0]
        assert issue.admin_file == "commondir"
        assert issue.problem == "containerized-commondir"
        assert issue.expected_value == "../.."
        assert issue.value == "/gza-git/common"

    def test_validate_host_worktree_admin_metadata_detects_gitdir_leak(self, tmp_path: Path):
        repo_dir = tmp_path / "repo"
        common_dir = repo_dir / ".git"
        registration_dir = common_dir / "worktrees" / "broken"
        registration_dir.mkdir(parents=True)
        (registration_dir / "commondir").write_text("../..\n")
        (registration_dir / "gitdir").write_text("/gza-git/repo/.git/worktrees/broken\n")
        git = Git(repo_dir)

        with patch("gza.git._git_common_dir", return_value=common_dir):
            validation = validate_host_worktree_admin_metadata(git)

        assert validation.is_healthy is False
        assert len(validation.issues) == 1
        issue = validation.issues[0]
        assert issue.admin_file == "gitdir"
        assert issue.problem == "containerized-gitdir"
        assert issue.expected_value is None
        assert issue.value == "/gza-git/repo/.git/worktrees/broken"

    def test_validate_host_worktree_admin_metadata_accepts_host_valid_files(self, tmp_path: Path):
        repo_dir = tmp_path / "repo"
        common_dir = repo_dir / ".git"
        registration_dir = common_dir / "worktrees" / "healthy"
        registration_dir.mkdir(parents=True)
        (registration_dir / "commondir").write_text("../..\n")
        (registration_dir / "gitdir").write_text("/workspace/repo/.git/worktrees/healthy\n")
        git = Git(repo_dir)

        with patch("gza.git._git_common_dir", return_value=common_dir):
            validation = validate_host_worktree_admin_metadata(git)

        assert validation.is_healthy is True
        assert validation.issues == ()

class TestBasicOperations:
    """Tests for basic git operations."""

    def test_current_branch(self, tmp_path: Path):
        """Test getting the current branch name."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="main\n", stderr="")
            branch = git.current_branch()

            mock_run.assert_called_once_with("rev-parse", "--abbrev-ref", "HEAD")
            assert branch == "main"

    def test_default_branch_from_origin_head(self, tmp_path: Path):
        """Test default_branch gets branch from origin HEAD."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="refs/remotes/origin/main\n", stderr="")
            branch = git.default_branch()

            assert branch == "main"

    def test_default_branch_fallback_main(self, tmp_path: Path):
        """Test default_branch falls back to main when origin HEAD fails."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            # First call fails (symbolic-ref), second succeeds (show-ref for main)
            mock_run.side_effect = [
                MagicMock(returncode=1, stdout="", stderr=""),  # symbolic-ref fails
                MagicMock(returncode=0, stdout="", stderr=""),  # main exists
            ]
            branch = git.default_branch()

            assert branch == "main"

    def test_default_branch_fallback_master(self, tmp_path: Path):
        """Test default_branch falls back to master when main doesn't exist."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            # First call fails (symbolic-ref), second fails (main doesn't exist), third succeeds (master exists)
            mock_run.side_effect = [
                MagicMock(returncode=1, stdout="", stderr=""),  # symbolic-ref fails
                MagicMock(returncode=1, stdout="", stderr=""),  # main doesn't exist
                MagicMock(returncode=0, stdout="", stderr=""),  # master exists
            ]
            branch = git.default_branch()

            assert branch == "master"

    def test_default_branch_defaults_to_master(self, tmp_path: Path):
        """Test default_branch returns 'master' when nothing else works."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            # All checks fail
            mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="")
            branch = git.default_branch()

            assert branch == "master"

    def test_checkout(self, tmp_path: Path):
        """Test checking out a branch."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git.checkout("feature-branch")

            mock_run.assert_called_once_with("checkout", "feature-branch")

    def test_checkout_detached(self, tmp_path: Path):
        """Test checking out a detached HEAD."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git.checkout_detached("main")

            mock_run.assert_called_once_with("checkout", "--detach", "main")

    def test_pull_success(self, tmp_path: Path):
        """Test pull returns True on success."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            result = git.pull()

            mock_run.assert_called_once_with("pull", "--ff-only", check=False)
            assert result is True

    def test_pull_failure(self, tmp_path: Path):
        """Test pull returns False on failure."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="error")
            result = git.pull()

            assert result is False

    def test_fetch_default_remote(self, tmp_path: Path):
        """Test fetch with default remote."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git.fetch()

            mock_run.assert_called_once_with("fetch", "origin")

    def test_fetch_custom_remote(self, tmp_path: Path):
        """Test fetch with custom remote."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git.fetch("upstream")

            mock_run.assert_called_once_with("fetch", "upstream")


class TestBranchOperations:
    """Tests for branch operations."""

    def test_create_branch_without_force(self, tmp_path: Path):
        """Test creating a new branch without force."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git.create_branch("new-branch")

            mock_run.assert_called_once_with("checkout", "-b", "new-branch")

    def test_create_branch_with_force(self, tmp_path: Path):
        """Test creating a branch with force deletes existing branch."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git.create_branch("new-branch", force=True)

            # Verify branch deletion and creation
            assert mock_run.call_count == 2
            mock_run.assert_any_call("branch", "-D", "new-branch", check=False)
            mock_run.assert_any_call("checkout", "-b", "new-branch")

    def test_branch_exists_returns_true(self, tmp_path: Path):
        """Test branch_exists returns True for existing branch."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            result = git.branch_exists("main")

            mock_run.assert_called_once_with("show-ref", "--verify", "--quiet", "refs/heads/main", check=False)
            assert result is True

    def test_branch_exists_returns_false(self, tmp_path: Path):
        """Test branch_exists returns False for non-existing branch."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="")
            result = git.branch_exists("nonexistent")

            assert result is False

    def test_local_branch_names_returns_all_local_heads(self, tmp_path: Path):
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, "_run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="main\nfeature/demo\n", stderr="")

            assert git.local_branch_names() == frozenset({"main", "feature/demo"})
            mock_run.assert_called_once_with(
                "for-each-ref",
                "--format=%(refname:strip=2)",
                "refs/heads/",
                check=True,
            )

    def test_delete_branch_without_force(self, tmp_path: Path):
        """Test deleting a branch without force."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git.delete_branch("old-branch")

            mock_run.assert_called_once_with("branch", "-d", "old-branch")

    def test_delete_branch_with_force(self, tmp_path: Path):
        """Test deleting a branch with force."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git.delete_branch("old-branch", force=True)

            mock_run.assert_called_once_with("branch", "-D", "old-branch")


class TestChangeDetection:
    """Tests for change detection and staging."""

    def test_has_changes_with_staged_changes(self, tmp_path: Path):
        """Test has_changes detects staged changes."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            # staged returns non-zero (has changes), others return zero
            mock_run.side_effect = [
                MagicMock(returncode=1, stdout="", stderr=""),  # diff --cached
                MagicMock(returncode=0, stdout="", stderr=""),  # diff
                MagicMock(returncode=0, stdout="", stderr=""),  # ls-files
            ]
            result = git.has_changes()

            assert result is True

    def test_has_changes_with_unstaged_changes(self, tmp_path: Path):
        """Test has_changes detects unstaged changes."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            # unstaged returns non-zero (has changes), others return zero
            mock_run.side_effect = [
                MagicMock(returncode=0, stdout="", stderr=""),  # diff --cached
                MagicMock(returncode=1, stdout="", stderr=""),  # diff
                MagicMock(returncode=0, stdout="", stderr=""),  # ls-files
            ]
            result = git.has_changes()

            assert result is True

    def test_has_changes_with_untracked_files(self, tmp_path: Path):
        """Test has_changes detects untracked files."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            # ls-files returns file names (untracked files exist)
            mock_run.side_effect = [
                MagicMock(returncode=0, stdout="", stderr=""),  # diff --cached
                MagicMock(returncode=0, stdout="", stderr=""),  # diff
                MagicMock(returncode=0, stdout="newfile.txt\n", stderr=""),  # ls-files
            ]
            result = git.has_changes()

            assert result is True

    def test_has_changes_no_changes(self, tmp_path: Path):
        """Test has_changes returns False when no changes exist."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            # All commands return success with no output
            mock_run.side_effect = [
                MagicMock(returncode=0, stdout="", stderr=""),  # diff --cached
                MagicMock(returncode=0, stdout="", stderr=""),  # diff
                MagicMock(returncode=0, stdout="", stderr=""),  # ls-files
            ]
            result = git.has_changes()

            assert result is False

    def test_has_changes_exclude_untracked(self, tmp_path: Path):
        """Test has_changes with include_untracked=False."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            # Only untracked files exist
            mock_run.side_effect = [
                MagicMock(returncode=0, stdout="", stderr=""),  # diff --cached
                MagicMock(returncode=0, stdout="", stderr=""),  # diff
            ]
            result = git.has_changes(include_untracked=False)

            # Should not call ls-files and should return False
            assert mock_run.call_count == 2
            assert result is False

    def test_has_changes_with_path(self, tmp_path: Path):
        """Test has_changes for a specific path."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.side_effect = [
                MagicMock(returncode=1, stdout="", stderr=""),  # diff --cached
                MagicMock(returncode=0, stdout="", stderr=""),  # diff
                MagicMock(returncode=0, stdout="", stderr=""),  # ls-files
            ]
            result = git.has_changes(path="src/")

            assert result is True
            # Verify path was passed to commands
            calls = [str(call) for call in mock_run.call_args_list]
            assert any("src/" in call for call in calls)

    def test_add_default_path(self, tmp_path: Path):
        """Test adding changes with default path."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git.add()

            mock_run.assert_called_once_with("add", ".")

    def test_add_specific_path(self, tmp_path: Path):
        """Test adding changes for a specific path."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git.add("src/file.py")

            mock_run.assert_called_once_with("add", "src/file.py")

    def test_commit(self, tmp_path: Path):
        """Test creating a commit."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git.commit("Test commit message")

            mock_run.assert_called_once_with("commit", "-m", "Test commit message")

    def test_amend(self, tmp_path: Path):
        """Test amending the last commit."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git.amend()

            mock_run.assert_called_once_with("commit", "--amend", "--no-edit")


class TestWorktreeOperations:
    """Tests for worktree operations."""

    def test_worktree_add_without_base_branch(self, tmp_path: Path):
        """Test creating a worktree without specifying base branch."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        worktree_path = tmp_path / "worktrees" / "feature"

        with patch.object(git, '_run') as mock_run, \
             patch('gza.git.Git') as mock_git_class:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            mock_worktree_git = MagicMock()
            mock_git_class.return_value = mock_worktree_git

            result = git.worktree_add(worktree_path, "feature-branch")

            # Verify worktree was created
            assert any("worktree" in str(call) and "add" in str(call) for call in mock_run.call_args_list)
            assert result == worktree_path

    def test_worktree_add_with_base_branch(self, tmp_path: Path):
        """Test creating a worktree with base branch specified."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        worktree_path = tmp_path / "worktrees" / "feature"

        with patch.object(git, '_run') as mock_run, \
             patch('gza.git.Git') as mock_git_class:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            mock_worktree_git = MagicMock()
            mock_git_class.return_value = mock_worktree_git

            result = git.worktree_add(worktree_path, "feature-branch", "main")

            # Verify base branch was included
            calls = [str(call) for call in mock_run.call_args_list]
            assert any("main" in call for call in calls)
            assert result == worktree_path

    def test_worktree_add_existing(self, tmp_path: Path):
        """Test creating a worktree from an existing ref."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        worktree_path = tmp_path / "worktrees" / "feature"

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")

            result = git.worktree_add_existing(worktree_path, "feature-branch")

            mock_run.assert_called_once_with("worktree", "add", str(worktree_path), "feature-branch")
            assert result == worktree_path

    def test_worktree_add_existing_detached(self, tmp_path: Path):
        """Test creating a detached worktree from an existing ref."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        worktree_path = tmp_path / "worktrees" / "feature"

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")

            result = git.worktree_add_existing(worktree_path, "main", detach=True)

            mock_run.assert_called_once_with("worktree", "add", "--detach", str(worktree_path), "main")
            assert result == worktree_path

    def test_worktree_add_removes_existing(self, tmp_path: Path):
        """Test worktree_add removes existing worktree."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        worktree_path = tmp_path / "worktrees" / "feature"
        worktree_path.mkdir(parents=True)

        with patch.object(git, '_run') as mock_run, \
             patch('gza.git.Git') as mock_git_class, \
             patch.object(git, 'worktree_remove') as mock_remove:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            mock_worktree_git = MagicMock()
            mock_git_class.return_value = mock_worktree_git

            git.worktree_add(worktree_path, "feature-branch")

            # Verify existing worktree was removed
            mock_remove.assert_called_once_with(worktree_path, force=True)

    def test_worktree_remove_without_force(self, tmp_path: Path):
        """Test removing a worktree without force."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        worktree_path = tmp_path / "worktrees" / "feature"

        with patch.object(git, '_run') as mock_run:
            result = MagicMock(returncode=0, stdout="", stderr="")
            mock_run.return_value = result
            returned = git.worktree_remove(worktree_path)

            # Verify worktree remove was called without --force
            calls = [str(call) for call in mock_run.call_args_list]
            assert any("worktree" in call and "remove" in call and "--force" not in call for call in calls)
            assert returned is result

    def test_worktree_remove_with_force(self, tmp_path: Path):
        """Test removing a worktree with force."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        worktree_path = tmp_path / "worktrees" / "feature"

        with patch.object(git, '_run') as mock_run:
            result = MagicMock(returncode=0, stdout="", stderr="")
            mock_run.return_value = result
            returned = git.worktree_remove(worktree_path, force=True)

            # Verify --force was included
            mock_run.assert_called_once()
            call_args = str(mock_run.call_args)
            assert "--force" in call_args
            assert returned is result

    def test_worktree_list_empty(self, tmp_path: Path):
        """Test listing worktrees when none exist."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            result = git.worktree_list()

            assert result == []

    def test_worktree_list_single(self, tmp_path: Path):
        """Test listing a single worktree."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        porcelain_output = """worktree /path/to/worktree
HEAD abc123def456
branch refs/heads/main
"""

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout=porcelain_output, stderr="")
            result = git.worktree_list()

            assert len(result) == 1
            assert result[0]["path"] == "/path/to/worktree"
            assert result[0]["head"] == "abc123def456"
            assert result[0]["branch"] == "refs/heads/main"

    def test_worktree_list_multiple(self, tmp_path: Path):
        """Test listing multiple worktrees."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        porcelain_output = """worktree /path/to/main
HEAD abc123
branch refs/heads/main

worktree /path/to/feature
HEAD def456
branch refs/heads/feature
"""

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout=porcelain_output, stderr="")
            result = git.worktree_list()

            assert len(result) == 2
            assert result[0]["path"] == "/path/to/main"
            assert result[1]["path"] == "/path/to/feature"

    def test_worktree_list_preserves_prunable_field(self, tmp_path: Path):
        """Parser keeps prunable metadata from porcelain output."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        porcelain_output = """worktree /path/to/stale
HEAD abc123
branch refs/heads/feature
prunable gitdir file points to non-existent location
"""

        with patch.object(git, "_run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout=porcelain_output, stderr="")
            result = git.worktree_list()

            assert len(result) == 1
            assert result[0]["path"] == "/path/to/stale"
            assert result[0]["branch"] == "refs/heads/feature"
            assert result[0]["prunable"] == "gitdir file points to non-existent location"

    def test_active_worktree_path_for_branch_ignores_prunable_entry(self, tmp_path: Path):
        """Active resolver should skip prunable registrations."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, "worktree_list", return_value=[
            {
                "path": "/path/to/stale",
                "branch": "refs/heads/feature/test",
                "prunable": "gone",
            }
        ]):
            result = active_worktree_path_for_branch(git, "feature/test")

            assert result is None


class TestRemoteOperations:
    """Tests for remote operations."""

    def test_remote_branch_exists_returns_true(self, tmp_path: Path):
        """Test remote_branch_exists returns True when branch exists."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="abc123\trefs/heads/feature\n", stderr="")
            result = git.remote_branch_exists("feature")

            mock_run.assert_called_once_with("ls-remote", "--heads", "origin", "feature", check=False)
            assert result is True

    def test_remote_branch_exists_returns_false(self, tmp_path: Path):
        """Test remote_branch_exists returns False when branch doesn't exist."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            result = git.remote_branch_exists("nonexistent")

            assert result is False

    def test_remote_branch_exists_custom_remote(self, tmp_path: Path):
        """Test remote_branch_exists with custom remote."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git.remote_branch_exists("feature", "upstream")

            mock_run.assert_called_once_with("ls-remote", "--heads", "upstream", "feature", check=False)

    def test_needs_push_remote_doesnt_exist(self, tmp_path: Path):
        """Test needs_push returns True when remote branch doesn't exist."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, 'remote_branch_exists', return_value=False):
            result = git.needs_push("feature")

            assert result is True

    def test_needs_push_has_commits_ahead(self, tmp_path: Path):
        """Test needs_push returns True when local has commits ahead."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, 'remote_branch_exists', return_value=True), \
             patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="3\n", stderr="")
            result = git.needs_push("feature")

            assert result is True

    def test_needs_push_no_commits_ahead(self, tmp_path: Path):
        """Test needs_push returns False when no commits ahead."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, 'remote_branch_exists', return_value=True), \
             patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="0\n", stderr="")
            result = git.needs_push("feature")

            assert result is False

    def test_needs_push_comparison_error(self, tmp_path: Path):
        """Test needs_push returns True on comparison error."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, 'remote_branch_exists', return_value=True), \
             patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="error")
            result = git.needs_push("feature")

            assert result is True

    def test_push_branch_with_upstream(self, tmp_path: Path):
        """Test pushing a branch with upstream tracking."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git.push_branch("feature")

            mock_run.assert_called_once_with("push", "-u", "origin", "feature")

    def test_push_branch_without_upstream(self, tmp_path: Path):
        """Test pushing a branch without upstream tracking."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git.push_branch("feature", set_upstream=False)

            mock_run.assert_called_once_with("push", "origin", "feature")

    def test_push_branch_custom_remote(self, tmp_path: Path):
        """Test pushing a branch to custom remote."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git.push_branch("feature", remote="upstream")

            mock_run.assert_called_once_with("push", "-u", "upstream", "feature")

    def test_push_force_with_lease_default_remote(self, tmp_path: Path):
        """Test force push with lease using default remote."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git.push_force_with_lease("feature")

            mock_run.assert_called_once_with("push", "--force-with-lease", "origin", "feature")

    def test_push_force_with_lease_custom_remote(self, tmp_path: Path):
        """Test force push with lease using custom remote."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git.push_force_with_lease("feature", remote="upstream")

            mock_run.assert_called_once_with("push", "--force-with-lease", "upstream", "feature")

    def test_push_ref_force_with_lease_default_remote(self, tmp_path: Path):
        """Test explicit lease push uses the expected source and remote ref."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, "_run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git.push_ref_force_with_lease(
                "refs/heads/feature",
                "feature",
                expected_remote_oid="abc123",
            )

            mock_run.assert_called_once_with(
                "push",
                "--force-with-lease=refs/heads/feature:abc123",
                "origin",
                "refs/heads/feature:refs/heads/feature",
            )

    def test_push_ref_force_with_lease_custom_remote(self, tmp_path: Path):
        """Test explicit lease push honors a custom remote."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, "_run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git.push_ref_force_with_lease(
                "HEAD",
                "feature",
                remote="upstream",
                expected_remote_oid="def456",
            )

            mock_run.assert_called_once_with(
                "push",
                "--force-with-lease=refs/heads/feature:def456",
                "upstream",
                "HEAD:refs/heads/feature",
            )


class TestMergeOperations:
    """Tests for merge operations."""

    def test_can_merge_returns_true(self, tmp_path: Path):
        """Test can_merge returns True for clean merge."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, 'branch_exists', return_value=True), \
             patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            result = git.can_merge("feature")

            assert result is True

    def test_can_merge_returns_false_on_conflict(self, tmp_path: Path):
        """Test can_merge returns False on conflict."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, 'branch_exists', return_value=True), \
             patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="conflict")
            result = git.can_merge("feature")

            assert result is False

    def test_can_merge_returns_false_for_nonexistent_branch(self, tmp_path: Path):
        """Test can_merge returns False for non-existent branch."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, 'branch_exists', return_value=False), \
             patch.object(git, 'ref_exists', return_value=False):
            result = git.can_merge("nonexistent")

            assert result is False

    def test_can_merge_accepts_remote_tracking_ref(self, tmp_path: Path):
        """Remote-tracking refs should be valid mergeability inputs."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, "branch_exists", return_value=False), \
             patch.object(git, "ref_exists", return_value=True), \
             patch.object(git, "_run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")

            result = git.can_merge("origin/feature", "main")

            assert result is True

    def test_can_merge_with_into_parameter(self, tmp_path: Path):
        """Test can_merge with into parameter specified."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, 'branch_exists', return_value=True), \
             patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git.can_merge("feature", "develop")

            # Verify merge-tree was called with correct branches
            calls = [str(call) for call in mock_run.call_args_list]
            assert any("develop" in call and "feature" in call for call in calls)

    def test_merge_without_squash(self, tmp_path: Path):
        """Test merge without squash uses --no-ff."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git.merge("feature")

            mock_run.assert_called_once_with("merge", "--no-ff", "feature")

    def test_merge_with_squash(self, tmp_path: Path):
        """Test merge with squash creates commit."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git.merge("feature", squash=True, commit_message="Squash merge feature")

            # Verify both merge and commit were called
            assert mock_run.call_count == 2
            calls = [str(call) for call in mock_run.call_args_list]
            assert any("--squash" in call for call in calls)
            assert any("Squash merge feature" in call for call in calls)

    def test_merge_with_squash_requires_message(self, tmp_path: Path):
        """Test merge with squash requires commit message."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")

            with pytest.raises(ValueError) as exc_info:
                git.merge("feature", squash=True)

            assert "commit_message is required" in str(exc_info.value)

    def test_merge_abort(self, tmp_path: Path):
        """Test aborting a merge."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git.merge_abort()

            mock_run.assert_called_once_with("merge", "--abort")

    def test_rebase(self, tmp_path: Path):
        """Test rebasing onto a branch."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git.rebase("main")

            mock_run.assert_called_once_with("rebase", "main")

    def test_rebase_abort(self, tmp_path: Path):
        """Test aborting a rebase."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git.rebase_abort()

            mock_run.assert_called_once_with("rebase", "--abort")


class TestUtilityOperations:
    """Tests for utility operations."""

    def test_get_log_with_oneline(self, tmp_path: Path):
        """Test getting log with oneline format."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="abc123 Commit message\n", stderr="")
            result = git.get_log("main..feature")

            mock_run.assert_called_once_with("log", "--oneline", "main..feature", check=False)
            assert result == "abc123 Commit message"

    def test_get_log_without_oneline(self, tmp_path: Path):
        """Test getting log without oneline format."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="Full log output\n", stderr="")
            result = git.get_log("main..feature", oneline=False)

            mock_run.assert_called_once_with("log", "main..feature", check=False)
            assert result == "Full log output"

    def test_get_diff_stat(self, tmp_path: Path):
        """Test getting diff stat."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        diff_stat = " file.py | 10 +++++-----\n 1 file changed, 5 insertions(+), 5 deletions(-)"

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout=diff_stat, stderr="")
            result = git.get_diff_stat("main...feature")

            mock_run.assert_called_once_with("diff", "--stat", "main...feature", check=False)
            assert "file.py" in result
            assert "1 file changed" in result

    def test_get_diff_stat_parsed_single_file(self, tmp_path: Path):
        """Test parsing diff stat for a single file."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        diff_stat = " file.py | 10 +++++-----\n 1 file changed, 5 insertions(+), 5 deletions(-)"

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout=diff_stat, stderr="")
            files, insertions, deletions = git.get_diff_stat_parsed("main...feature")
            assert files == 1
            assert insertions == 5
            assert deletions == 5

    def test_get_diff_stat_parsed_multiple_files(self, tmp_path: Path):
        """Test parsing diff stat for multiple files."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        diff_stat = (
            " src/a.py | 20 ++++++++++++++------\n"
            " src/b.py |  5 +++++\n"
            " 2 files changed, 25 insertions(+), 6 deletions(-)"
        )

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout=diff_stat, stderr="")
            files, insertions, deletions = git.get_diff_stat_parsed("main...feature")
            assert files == 2
            assert insertions == 25
            assert deletions == 6

    def test_get_diff_stat_parsed_insertions_only(self, tmp_path: Path):
        """Test parsing diff stat with only insertions."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        diff_stat = " new.py | 10 ++++++++++\n 1 file changed, 10 insertions(+)"

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout=diff_stat, stderr="")
            files, insertions, deletions = git.get_diff_stat_parsed("main...feature")
            assert files == 1
            assert insertions == 10
            assert deletions == 0

    def test_get_diff_stat_parsed_empty(self, tmp_path: Path):
        """Test parsing empty diff stat returns zeros."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            files, insertions, deletions = git.get_diff_stat_parsed("main...feature")
            assert files == 0
            assert insertions == 0
            assert deletions == 0

    def test_get_diff(self, tmp_path: Path):
        """Test getting full diff."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        full_diff = """diff --git a/file.py b/file.py
index abc123..def456 100644
--- a/file.py
+++ b/file.py
@@ -1,3 +1,3 @@
-old line
+new line
"""

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout=full_diff, stderr="")
            result = git.get_diff("main...feature")

            mock_run.assert_called_once_with("diff", "main...feature", check=False)
            assert "diff --git" in result
            assert "+new line" in result

    def test_count_commits_ahead(self, tmp_path: Path):
        """Test counting commits ahead."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="5\n", stderr="")
            result = git.count_commits_ahead("feature", "main")

            mock_run.assert_called_once_with("rev-list", "--count", "main..feature", check=False)
            assert result == 5

    def test_count_commits_ahead_error(self, tmp_path: Path):
        """Test count_commits_ahead returns 0 on error."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="error")
            result = git.count_commits_ahead("feature", "main")

            assert result == 0

    def test_count_commits_ahead_checked_error(self, tmp_path: Path):
        """Test count_commits_ahead_checked returns None on error."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="error")
            result = git.count_commits_ahead_checked("feature", "main")

            assert result is None

    def test_count_commits_behind(self, tmp_path: Path):
        """Test counting commits behind."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, "_run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="3\n", stderr="")
            result = git.count_commits_behind("feature", "main")

            mock_run.assert_called_once_with("rev-list", "--count", "feature..main", check=False)
            assert result == 3

    def test_count_commits_behind_error(self, tmp_path: Path):
        """Test count_commits_behind returns None on error."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, "_run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="error")
            result = git.count_commits_behind("feature", "main")

            assert result is None


class TestParseDiffNumstat:
    """Tests for parse_diff_numstat module-level function."""

    def test_empty_output(self):
        """Empty output returns zeros."""
        assert parse_diff_numstat("") == (0, 0, 0)

    def test_single_file(self):
        """Single file with additions and deletions."""
        output = "12\t3\tpath/to/file.py"
        assert parse_diff_numstat(output) == (1, 12, 3)

    def test_multiple_files(self):
        """Multiple files are summed correctly."""
        output = "12\t3\tpath/to/file.py\n5\t0\tsrc/other.py\n0\t7\tsrc/old.py"
        files, added, removed = parse_diff_numstat(output)
        assert files == 3
        assert added == 17
        assert removed == 10

    def test_binary_files_skipped(self):
        """Binary files (- in count columns) are excluded from count."""
        output = "12\t3\tfile.py\n-\t-\timage.png"
        files, added, removed = parse_diff_numstat(output)
        assert files == 1
        assert added == 12
        assert removed == 3

    def test_all_binary_files(self):
        """All binary files results in zeros."""
        output = "-\t-\timage.png\n-\t-\tfont.woff"
        assert parse_diff_numstat(output) == (0, 0, 0)

    def test_malformed_lines_skipped(self):
        """Lines that don't have 3 tab-separated parts are skipped."""
        output = "12\t3\tfile.py\ngarbage line\n5\t1\tother.py"
        files, added, removed = parse_diff_numstat(output)
        assert files == 2
        assert added == 17
        assert removed == 4

    def test_only_additions(self):
        """Files with only additions."""
        output = "20\t0\tnew_file.py"
        assert parse_diff_numstat(output) == (1, 20, 0)


class TestGetDiffNumstat:
    """Tests for Git.get_diff_numstat method."""

    def test_get_diff_numstat_calls_git(self, tmp_path: Path):
        """get_diff_numstat calls git diff --numstat with the given range."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        numstat_output = "12\t3\tfile.py\n5\t0\tother.py"
        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout=numstat_output, stderr="")
            result = git.get_diff_numstat("main...feature")

            mock_run.assert_called_once_with(
                "diff",
                "--numstat",
                "--find-renames",
                "--find-copies",
                "--find-copies-harder",
                "main...feature",
                check=False,
            )
            assert result == numstat_output

    def test_get_diff_numstat_scoped_to_paths(self, tmp_path: Path):
        """get_diff_numstat includes path filter after rename/copy flags."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, "_run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="1\t0\tsrc/file.py\n", stderr="")
            result = git.get_diff_numstat("main...feature", ("src/file.py",))

            assert result == "1\t0\tsrc/file.py"
            mock_run.assert_called_once_with(
                "diff",
                "--numstat",
                "--find-renames",
                "--find-copies",
                "--find-copies-harder",
                "main...feature",
                "--",
                "src/file.py",
                check=False,
            )

    def test_get_diff_numstat_strips_output(self, tmp_path: Path):
        """get_diff_numstat strips trailing whitespace from output."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="12\t3\tfile.py\n  ", stderr="")
            result = git.get_diff_numstat("main...feature")

        assert result == "12\t3\tfile.py"

    def test_get_diff_numstat_empty_on_error(self, tmp_path: Path):
        """get_diff_numstat returns empty string on error."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=128, stdout="", stderr="error")
            result = git.get_diff_numstat("main...feature")

        assert result == ""

    def test_get_diff_numstat_checked_raises_on_error(self, tmp_path: Path):
        """get_diff_numstat_checked raises instead of collapsing git probe failures."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, "_run") as mock_run:
            mock_run.return_value = MagicMock(returncode=128, stdout="", stderr="bad revision")
            with pytest.raises(GitError, match="bad revision"):
                git.get_diff_numstat_checked("main...feature")


class TestStatusPorcelain:
    """Tests for status_porcelain method."""

    def test_empty_status(self, tmp_path: Path):
        """Test status_porcelain returns empty set when no changes."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            result = git.status_porcelain()

        assert result == set()

    def test_modified_file(self, tmp_path: Path):
        """Test status_porcelain detects modified files."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout=" M src/foo.py\n", stderr="")
            result = git.status_porcelain()

        assert result == {("M", "src/foo.py")}

    def test_untracked_file(self, tmp_path: Path):
        """Test status_porcelain detects untracked files."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="?? new_file.txt\n", stderr="")
            result = git.status_porcelain()

        assert result == {("??", "new_file.txt")}

    def test_multiple_changes(self, tmp_path: Path):
        """Test status_porcelain with multiple change types."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout=" M src/foo.py\n?? new.txt\n D deleted.py\n",
                stderr="",
            )
            result = git.status_porcelain()

        assert result == {("M", "src/foo.py"), ("??", "new.txt"), ("D", "deleted.py")}

    def test_renamed_file(self, tmp_path: Path):
        """Test status_porcelain handles renames correctly."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="R  old.py -> new.py\n", stderr="")
            result = git.status_porcelain()

        assert result == {("R", "new.py")}

    def test_c_style_quoted_filename(self, tmp_path: Path):
        """Test status_porcelain decodes C-style quoted paths."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, '_run') as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout='?? "file\\040\\074x\\076\\040\\"q\\".txt"\n',
                stderr="",
            )
            result = git.status_porcelain()

        assert result == {("??", 'file <x> "q".txt')}

    def test_quoted_rename_with_arrow_in_old_name(self, tmp_path: Path):
        """Test status_porcelain handles quoted rename records with embedded delimiters."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, "_run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout='R  "old -> name.txt" -> "new -> name.txt"\n',
                stderr="",
            )
            result = git.status_porcelain()

        assert result == {("R", "new -> name.txt")}

    def test_raises_on_nonzero_status_probe(self, tmp_path: Path):
        """A failed porcelain probe must not be treated as a clean tree."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, "_run") as mock_run:
            mock_run.return_value = MagicMock(returncode=128, stdout="", stderr="not a git repository")
            with pytest.raises(GitError, match="git status --porcelain failed"):
                git.status_porcelain()


class TestExtractionGitHelpers:
    """Tests for git helpers used by extraction orchestration."""

    def test_ref_exists_uses_rev_parse_verify(self, tmp_path: Path):
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, "_run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            assert git.ref_exists("main") is True
            mock_run.assert_called_once_with(
                "rev-parse",
                "--verify",
                "--quiet",
                "main^{commit}",
                check=False,
            )

    def test_rev_parse_if_exists_returns_sha_or_none(self, tmp_path: Path):
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, "_run") as mock_run:
            mock_run.side_effect = [
                MagicMock(returncode=0, stdout="abc123\n", stderr=""),
                MagicMock(returncode=1, stdout="", stderr=""),
            ]

            assert git.rev_parse_if_exists("main") == "abc123"
            assert git.rev_parse_if_exists("missing") is None

    def test_is_ancestor_returns_false_for_non_ancestor(self, tmp_path: Path):
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, "_run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="")
            assert git.is_ancestor("main", "feature/demo") is False

    def test_resolve_ref_if_possible_uses_rev_parse_if_exists_when_available(self, tmp_path: Path):
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, "rev_parse_if_exists", return_value="abc123"):
            assert resolve_ref_if_possible(git, "main") == ResolvedGitRef("abc123")

    def test_resolve_ref_if_possible_falls_back_to_rev_parse(self, tmp_path: Path):
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with (
            patch.object(git, "rev_parse_if_exists", new=None),
            patch.object(git, "rev_parse", return_value="def456"),
        ):
            assert resolve_ref_if_possible(git, "main") == ResolvedGitRef("def456")

    def test_resolve_ref_if_possible_treats_missing_ref_as_silent_none(self, tmp_path: Path):
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, "rev_parse_if_exists", side_effect=GitError("missing")):
            assert resolve_ref_if_possible(git, "missing") == ResolvedGitRef(None)

    def test_resolve_ref_if_possible_reports_unexpected_resolution_error(self, tmp_path: Path):
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, "rev_parse_if_exists", side_effect=RuntimeError("boom")):
            assert resolve_ref_if_possible(git, "main") == ResolvedGitRef(
                None,
                "unexpected error resolving ref 'main': boom",
            )

    def test_resolve_merge_source_ref_prefers_local_branch(self, tmp_path: Path):
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, "_run") as mock_run:
            mock_run.side_effect = [
                MagicMock(returncode=0),  # branch_exists
            ]

            assert git.resolve_merge_source_ref("feature/demo") == "feature/demo"

    def test_resolve_merge_source_ref_falls_back_to_origin_ref(self, tmp_path: Path):
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, "_run") as mock_run:
            mock_run.side_effect = [
                MagicMock(returncode=1),  # branch_exists
                MagicMock(returncode=0),  # ref_exists(origin/feature/demo)
            ]

            assert git.resolve_merge_source_ref("feature/demo") == "origin/feature/demo"

    def test_resolve_merge_source_ref_returns_none_when_no_local_or_remote_ref(self, tmp_path: Path):
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, "_run") as mock_run:
            mock_run.side_effect = [
                MagicMock(returncode=1),  # branch_exists
                MagicMock(returncode=1),  # ref_exists(origin/feature/demo)
            ]

            assert git.resolve_merge_source_ref("feature/demo") is None

    def test_resolve_fresh_merge_source_prefers_local_when_origin_is_stale(self, tmp_path: Path):
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with (
            patch.object(git, "branch_exists", return_value=True),
            patch.object(git, "ref_exists", return_value=True),
            patch.object(git, "rev_parse_if_exists", side_effect=["local-sha", "remote-sha"]),
            patch.object(git, "count_commits_ahead", side_effect=[1, 0]),
        ):
            resolved = git.resolve_fresh_merge_source("feature/demo")

        assert resolved.ref == "feature/demo"
        assert resolved.warning is None

    def test_resolve_fresh_merge_source_prefers_origin_when_refs_match(self, tmp_path: Path):
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with (
            patch.object(git, "branch_exists", return_value=True),
            patch.object(git, "ref_exists", return_value=True),
            patch.object(git, "rev_parse_if_exists", side_effect=["same-sha", "same-sha"]),
        ):
            resolved = git.resolve_fresh_merge_source("feature/demo")

        assert resolved.ref == "origin/feature/demo"
        assert resolved.warning is None

    def test_resolve_fresh_merge_source_returns_warning_for_diverged_refs(self, tmp_path: Path):
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with (
            patch.object(git, "branch_exists", return_value=True),
            patch.object(git, "ref_exists", return_value=True),
            patch.object(git, "rev_parse_if_exists", side_effect=["local-sha", "remote-sha"]),
            patch.object(git, "count_commits_ahead", side_effect=[1, 1]),
        ):
            resolved = git.resolve_fresh_merge_source("feature/demo")

        assert resolved.ref is None
        assert resolved.warning is not None
        assert "diverged" in resolved.warning

    def test_get_diff_name_status_scoped_to_paths(self, tmp_path: Path):
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, "_run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="M\tsrc/file.py\n", stderr="")
            output = git.get_diff_name_status("main...feature", ("src/file.py",))
            assert output == "M\tsrc/file.py"
            mock_run.assert_called_once_with(
                "diff",
                "--name-status",
                "--find-renames",
                "--find-copies",
                "--find-copies-harder",
                "main...feature",
                "--",
                "src/file.py",
                check=False,
            )

    def test_get_diff_name_status_check_raises_on_nonzero_exit(self, tmp_path: Path):
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, "_run") as mock_run:
            mock_run.return_value = MagicMock(returncode=128, stdout="", stderr="fatal: bad revision")
            with pytest.raises(GitError, match="fatal: bad revision"):
                git.get_diff_name_status("main...feature", check=True)

    def test_get_diff_patch_for_paths_uses_rename_copy_detection(self, tmp_path: Path):
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, "_run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="diff --git ...", stderr="")
            output = git.get_diff_patch_for_paths("main...feature", ("src/file.py",), binary=True)

            assert output == "diff --git ..."
            mock_run.assert_called_once_with(
                "diff",
                "--find-renames",
                "--find-copies",
                "--find-copies-harder",
                "--binary",
                "main...feature",
                "--",
                "src/file.py",
                check=False,
            )

    def test_get_commit_name_status_scoped_to_paths(self, tmp_path: Path):
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, "_run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="M\tsrc/file.py\n", stderr="")
            output = git.get_commit_name_status("abc123", ("src/file.py",))
            assert output == "M\tsrc/file.py"
            mock_run.assert_called_once_with(
                "show",
                "--format=",
                "--name-status",
                "--find-renames",
                "--find-copies",
                "--find-copies-harder",
                "abc123",
                "--",
                "src/file.py",
                check=False,
            )

    def test_get_commit_numstat_scoped_to_paths(self, tmp_path: Path):
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, "_run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="1\t0\tsrc/file.py\n", stderr="")
            output = git.get_commit_numstat("abc123", ("src/file.py",))
            assert output == "1\t0\tsrc/file.py"
            mock_run.assert_called_once_with(
                "show",
                "--format=",
                "--numstat",
                "--find-renames",
                "--find-copies",
                "--find-copies-harder",
                "abc123",
                "--",
                "src/file.py",
                check=False,
            )

    def test_get_commit_patch_for_paths_uses_rename_copy_detection(self, tmp_path: Path):
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, "_run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="diff --git ...", stderr="")
            output = git.get_commit_patch_for_paths("abc123", ("src/file.py",), binary=True)

            assert output == "diff --git ..."
            mock_run.assert_called_once_with(
                "show",
                "--format=",
                "--find-renames",
                "--find-copies",
                "--find-copies-harder",
                "--binary",
                "abc123",
                "--",
                "src/file.py",
                check=False,
            )

    def test_apply_patch_file_uses_3way(self, tmp_path: Path):
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)
        patch_file = tmp_path / "selected.patch"
        patch_file.write_text("diff --git a/a b/a\n")

        with patch.object(git, "_run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            git.apply_patch_file(patch_file)
            mock_run.assert_called_once_with("apply", "--3way", str(patch_file), check=False)

    def test_apply_patch_file_result_returns_raw_outcome(self, tmp_path: Path):
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)
        patch_file = tmp_path / "selected.patch"
        patch_file.write_text("diff --git a/a b/a\n")

        with patch.object(git, "_run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stdout="stdout", stderr="stderr")
            result = git.apply_patch_file_result(patch_file)

        assert result.returncode == 1
        assert result.stdout == "stdout"
        assert result.stderr == "stderr"

    def test_reverse_check_patch_file_result_returns_raw_outcome(self, tmp_path: Path):
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)
        patch_file = tmp_path / "selected.patch"
        patch_file.write_text("diff --git a/a b/a\n")

        with patch.object(git, "_run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stdout="stdout", stderr="stderr")
            result = git.reverse_check_patch_file_result(patch_file)

        assert result.returncode == 1
        assert result.stdout == "stdout"
        assert result.stderr == "stderr"
        mock_run.assert_called_once_with("apply", "--check", "--reverse", str(patch_file), check=False)


class TestGitCached:
    """Tests for per-invocation git read caching."""

    def test_cached_scope_reuses_readonly_helper_results(self, tmp_path: Path) -> None:
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, "_run") as mock_run:
            mock_run.side_effect = [
                MagicMock(returncode=0, stdout="abc123\n", stderr=""),
                MagicMock(returncode=0, stdout="", stderr=""),
                MagicMock(returncode=0, stdout="", stderr=""),
                MagicMock(returncode=0, stdout="3\n", stderr=""),
            ]

            with git.cached():
                assert git.rev_parse_if_exists("main") == "abc123"
                assert git.rev_parse_if_exists("main") == "abc123"
                assert git.branch_exists("x") is True
                assert git.branch_exists("x") is True
                assert git.is_ancestor("a", "b") is True
                assert git.is_ancestor("a", "b") is True
                assert git.count_commits_ahead_checked("a", "b") == 3
                assert git.count_commits_ahead_checked("a", "b") == 3

        assert mock_run.call_count == 4

    def test_cached_scope_reuses_is_on_first_parent_history_probes(self, tmp_path: Path) -> None:
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, "_run") as mock_run:
            mock_run.side_effect = [
                MagicMock(returncode=0, stdout="feature-tip-sha\n", stderr=""),
                MagicMock(
                    returncode=0,
                    stdout="main-head\nfeature-tip-sha\nolder-mainline\n",
                    stderr="",
                ),
            ]

            with git.cached():
                assert git.is_on_first_parent_history("feature-tip", "main") is True
                assert git.is_on_first_parent_history("feature-tip", "main") is True

        assert mock_run.call_count == 2
        assert mock_run.call_args_list == [
            call("rev-parse", "--verify", "--quiet", "feature-tip^{commit}", check=False),
            call("rev-list", "--first-parent", "main", check=False),
        ]

    def test_cached_scope_does_not_leak_between_calls(self, tmp_path: Path) -> None:
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, "_run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="abc123\n", stderr="")

            with git.cached():
                assert git.rev_parse_if_exists("main") == "abc123"
                assert git.rev_parse_if_exists("main") == "abc123"

            assert git.rev_parse_if_exists("main") == "abc123"

            with git.cached():
                assert git.rev_parse_if_exists("main") == "abc123"

        assert mock_run.call_count == 3

    def test_mutation_clears_active_cache(self, tmp_path: Path) -> None:
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, "_run") as mock_run:
            mock_run.side_effect = [
                MagicMock(returncode=0, stdout="", stderr=""),
                MagicMock(returncode=0, stdout="", stderr=""),
                MagicMock(returncode=0, stdout="", stderr=""),
            ]

            with git.cached():
                assert git.branch_exists("x") is True
                git.checkout("feature/demo")
                assert git.branch_exists("x") is True

        assert mock_run.call_count == 3

    def test_nested_cached_scope_does_not_restore_stale_reads_after_mutation(self, tmp_path: Path) -> None:
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, "_run") as mock_run:
            mock_run.side_effect = [
                MagicMock(returncode=1, stdout="", stderr=""),
                MagicMock(returncode=0, stdout="", stderr=""),
                MagicMock(returncode=0, stdout="", stderr=""),
            ]

            with git.cached():
                assert git.branch_exists("feature/demo") is False
                with git.cached():
                    git.create_branch("feature/demo")
                assert git.branch_exists("feature/demo") is True

        assert mock_run.call_count == 3

    def test_rev_parse_caches_successes_only(self, tmp_path: Path) -> None:
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, "_run", side_effect=GitError("boom")) as mock_run:
            with git.cached():
                with pytest.raises(GitError, match="boom"):
                    git.rev_parse("missing")
                with pytest.raises(GitError, match="boom"):
                    git.rev_parse("missing")

        assert mock_run.call_count == 2

    def test_resolve_refs_batches_hits_and_misses(self, tmp_path: Path) -> None:
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, "_run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout="abc123 commit 1\nfeature/missing^{commit} missing\n",
                stderr="",
            )

            with git.cached():
                resolved = git.resolve_refs(["main", "feature/missing"])
                assert resolved == {"main": "abc123", "feature/missing": None}
                assert git.rev_parse_if_exists("main") == "abc123"
                assert git.rev_parse_if_exists("feature/missing") is None
                assert git.ref_exists("main") is True
                assert git.ref_exists("feature/missing") is False

        mock_run.assert_called_once_with(
            "cat-file",
            "--batch-check",
            check=False,
            stdin=b"main^{commit}\nfeature/missing^{commit}\n",
        )

    def test_resolve_refs_accepts_ambiguous_refs_with_batch_check_output(self, tmp_path: Path) -> None:
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, "_run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout="deadbeef commit 1\n",
                stderr="warning: refname 'same' is ambiguous.\n",
            )

            with git.cached():
                assert git.resolve_refs(["same"]) == {"same": "deadbeef"}
                assert git.rev_parse_if_exists("same") == "deadbeef"

        mock_run.assert_called_once()

    def test_resolve_refs_supports_tree_peel(self, tmp_path: Path) -> None:
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, "_run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout="feedface tree 1\nmissing^{tree} missing\n",
                stderr="",
            )

            with git.cached():
                assert git.resolve_refs(["main", "missing"], peel="tree") == {
                    "main": "feedface",
                    "missing": None,
                }

        mock_run.assert_called_once_with(
            "cat-file",
            "--batch-check",
            check=False,
            stdin=b"main^{tree}\nmissing^{tree}\n",
        )

    def test_resolve_refs_raises_for_batch_failure(self, tmp_path: Path) -> None:
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, "_run") as mock_run:
            mock_run.return_value = MagicMock(returncode=128, stdout="", stderr="fatal: bad revision")
            with pytest.raises(GitError, match="git cat-file --batch-check failed"):
                git.resolve_refs(["bad"])

    def test_branches_exist_batches_and_primes_branch_exists(self, tmp_path: Path) -> None:
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, "_run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout="main\nfeature/live\n",
                stderr="",
            )

            with git.cached():
                existing = git.branches_exist(["main", "feature/live", "feature/missing"])
                assert existing == {
                    "main": True,
                    "feature/live": True,
                    "feature/missing": False,
                }
                assert git.branch_exists("main") is True
                assert git.branch_exists("feature/missing") is False

        mock_run.assert_called_once_with(
            "for-each-ref",
            "--format=%(refname:strip=2)",
            "refs/heads/",
            check=True,
        )

    def test_refs_exist_batches_and_primes_ref_exists(self, tmp_path: Path) -> None:
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, "_run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout="abc123 commit 1\nmissing^{commit} missing\n",
                stderr="",
            )

            with git.cached():
                existing = git.refs_exist(["main", "missing"])
                assert existing == {"main": True, "missing": False}
                assert git.ref_exists("main") is True
                assert git.ref_exists("missing") is False
                assert git.rev_parse_if_exists("main") == "abc123"

        mock_run.assert_called_once()

    def test_rev_parse_preloaded_hit_matches_uncached_behavior(self, tmp_path: Path) -> None:
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, "_run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="abc123 commit 1\n", stderr="")

            with git.cached():
                assert git.resolve_refs(["main"]) == {"main": "abc123"}
                assert git.rev_parse("main") == "abc123"
                assert git.rev_parse_if_exists("main") == "abc123"

        mock_run.assert_called_once()

    def test_rev_parse_if_exists_preloaded_miss_matches_uncached_behavior(self, tmp_path: Path) -> None:
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, "_run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout="missing^{commit} missing\n",
                stderr="",
            )

            with git.cached():
                assert git.resolve_refs(["missing"]) == {"missing": None}
                assert git.rev_parse_if_exists("missing") is None
                assert git.ref_exists("missing") is False

        mock_run.assert_called_once()

    def test_rev_parse_preloaded_miss_preserves_failure_path(self, tmp_path: Path) -> None:
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        git = Git(repo_dir)

        with patch.object(git, "_run") as mock_run:
            mock_run.side_effect = [
                MagicMock(returncode=0, stdout="missing^{commit} missing\n", stderr=""),
                GitError("boom"),
            ]

            with git.cached():
                assert git.resolve_refs(["missing"]) == {"missing": None}
                with pytest.raises(GitError, match="boom"):
                    git.rev_parse("missing")

        assert mock_run.call_count == 2
