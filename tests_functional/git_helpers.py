"""Helpers for functional tests that need a real git repository."""

from pathlib import Path

from gza.git import Git


def init_repo_with_remote_tracking_only_feature(tmp_path: Path, branch: str) -> Git:
    git = Git(tmp_path)
    git._run("init", "-b", "main")
    git._run("config", "user.name", "Test User")
    git._run("config", "user.email", "test@example.com")
    (tmp_path / "base.txt").write_text("base\n")
    git._run("add", "base.txt")
    git._run("commit", "-m", "Initial commit")

    git._run("checkout", "-b", branch)
    feature_file = Path(branch.replace("/", "_") + ".txt")
    (tmp_path / feature_file).write_text("feature\n")
    git._run("add", str(feature_file))
    git._run("commit", "-m", "Feature commit")
    feature_sha = git.rev_parse("HEAD")
    git._run("checkout", "main")
    git._run("update-ref", f"refs/remotes/origin/{branch}", feature_sha)
    git._run("branch", "-D", branch)
    return git
