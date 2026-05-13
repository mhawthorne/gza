"""Tests for aggregate rebase diff comparison."""

from pathlib import Path

from gza.git import Git
from gza.rebase_diff import capture_rebase_diff_baseline, compute_rebase_changed_diff


def _init_repo(tmp_path: Path) -> tuple[Path, Git]:
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    git = Git(repo_dir)
    git._run("init", "-b", "main")
    git._run("config", "user.name", "Test User")
    git._run("config", "user.email", "test@example.com")
    _commit_file(git, repo_dir, "base.txt", "base\n", "Initial commit")
    return repo_dir, git


def _commit_file(git: Git, repo_dir: Path, relative_path: str, content: str, message: str) -> None:
    path = repo_dir / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    git._run("add", relative_path)
    git._run("commit", "-m", message)


def test_compute_rebase_changed_diff_preserves_clean_unchanged_rebase(tmp_path: Path) -> None:
    repo_dir, git = _init_repo(tmp_path)

    git._run("checkout", "-b", "feature")
    _commit_file(git, repo_dir, "feature.txt", "feature\n", "Add feature")

    baseline = capture_rebase_diff_baseline(git, branch="feature", target="main")

    git._run("checkout", "main")
    _commit_file(git, repo_dir, "main.txt", "main update\n", "Advance main")

    git._run("checkout", "-B", "feature", "main")
    _commit_file(git, repo_dir, "feature.txt", "feature\n", "Reapply feature")

    comparison = compute_rebase_changed_diff(git, baseline=baseline, branch="feature", target="main")

    assert comparison.changed_diff is False
    assert comparison.warning is None
    assert comparison.detail == "no (review can be preserved)"


def test_compute_rebase_changed_diff_preserves_aggregate_patch_across_commit_topology_change(
    tmp_path: Path,
) -> None:
    repo_dir, git = _init_repo(tmp_path)

    git._run("checkout", "-b", "feature")
    _commit_file(git, repo_dir, "feature.txt", "one\n", "Add first line")
    _commit_file(git, repo_dir, "feature.txt", "one\ntwo\n", "Add second line")

    baseline = capture_rebase_diff_baseline(git, branch="feature", target="main")

    git._run("checkout", "main")
    _commit_file(git, repo_dir, "main.txt", "main update\n", "Advance main")

    git._run("checkout", "-B", "feature", "main")
    _commit_file(git, repo_dir, "feature.txt", "one\ntwo\n", "Squash feature change")

    comparison = compute_rebase_changed_diff(git, baseline=baseline, branch="feature", target="main")

    assert comparison.changed_diff is False
    assert comparison.warning is None


def test_compute_rebase_changed_diff_detects_content_change(tmp_path: Path) -> None:
    repo_dir, git = _init_repo(tmp_path)

    git._run("checkout", "-b", "feature")
    _commit_file(git, repo_dir, "feature.txt", "one\n", "Add feature")

    baseline = capture_rebase_diff_baseline(git, branch="feature", target="main")

    git._run("checkout", "main")
    _commit_file(git, repo_dir, "main.txt", "main update\n", "Advance main")

    git._run("checkout", "-B", "feature", "main")
    _commit_file(git, repo_dir, "feature.txt", "one\ntwo\n", "Reapply with extra change")

    comparison = compute_rebase_changed_diff(git, baseline=baseline, branch="feature", target="main")

    assert comparison.changed_diff is True
    assert comparison.detail == "yes (review must be refreshed)"
