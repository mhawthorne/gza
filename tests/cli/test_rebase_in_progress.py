"""Tests for the rebase-in-progress helper."""

from gza.cli.git_ops import _is_rebase_in_progress


def test_returns_false_when_no_git_dir(tmp_path):
    assert _is_rebase_in_progress(tmp_path) is False


def test_returns_false_when_no_rebase_markers(tmp_path):
    git_dir = tmp_path / ".git"
    git_dir.mkdir()
    assert _is_rebase_in_progress(tmp_path) is False


def test_returns_true_when_rebase_merge_present(tmp_path):
    (tmp_path / ".git").mkdir()
    (tmp_path / ".git" / "rebase-merge").mkdir()
    assert _is_rebase_in_progress(tmp_path) is True


def test_returns_true_when_rebase_apply_present(tmp_path):
    (tmp_path / ".git").mkdir()
    (tmp_path / ".git" / "rebase-apply").mkdir()
    assert _is_rebase_in_progress(tmp_path) is True


def test_worktree_git_file_resolved_correctly(tmp_path):
    real_git_dir = tmp_path / "main-repo" / ".git" / "worktrees" / "wt1"
    real_git_dir.mkdir(parents=True)
    worktree = tmp_path / "worktree"
    worktree.mkdir()
    (worktree / ".git").write_text(f"gitdir: {real_git_dir}\n")
    assert _is_rebase_in_progress(worktree) is False
    (real_git_dir / "rebase-merge").mkdir()
    assert _is_rebase_in_progress(worktree) is True
