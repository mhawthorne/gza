"""Helpers for functional tests that need a real git repository."""

from datetime import UTC, datetime
from pathlib import Path

from gza.db import SqliteTaskStore
from gza.git import Git
from tests.cli.conftest import make_store, setup_config


def init_basic_repo(tmp_path: Path, *, default_branch: str = "main") -> Git:
    git = Git(tmp_path)
    git._run("init", "-b", default_branch)
    git._run("config", "user.name", "Test User")
    git._run("config", "user.email", "test@example.com")
    (tmp_path / "README.md").write_text("initial")
    git._run("add", "README.md")
    git._run("commit", "-m", "Initial commit")
    return git


def setup_git_repo_with_task_branch(
    tmp_path: Path,
    task_prompt: str,
    branch_name: str,
    *,
    status: str = "completed",
    worktree_name: str | None = None,
) -> tuple[SqliteTaskStore, Git, object, Path | None]:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    git = init_basic_repo(tmp_path)

    task = store.add(task_prompt)
    task.status = status
    if status in ("completed", "failed"):
        task.completed_at = datetime.now(UTC)
    task.branch = branch_name
    store.update(task)

    git._run("checkout", "-b", branch_name)
    (tmp_path / "feature.txt").write_text("feature content")
    git._run("add", "feature.txt")
    git._run("commit", "-m", "Add feature")
    git._run("checkout", "main")

    worktree_path = None
    if worktree_name is not None:
        worktree_path = tmp_path / "worktrees" / worktree_name
        worktree_path.parent.mkdir(parents=True, exist_ok=True)
        git._run("worktree", "add", str(worktree_path), branch_name)

    return store, git, task, worktree_path


def setup_unmerged_env(
    tmp_path: Path,
    *,
    task_prompt: str = "Add feature",
    task_type: str = "implement",
    task_id: str = "20260212-add-feature",
    branch: str = "feature/test",
    merge_status: str | None = "unmerged",
    status: str = "completed",
    has_commits: bool = True,
) -> tuple[SqliteTaskStore, object, Git]:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    git = init_basic_repo(tmp_path)

    task = store.add(task_prompt, task_type=task_type)
    task.status = status
    if status in ("completed", "failed", "dropped"):
        task.completed_at = datetime.now(UTC)
    task.branch = branch
    task.has_commits = has_commits
    task.merge_status = merge_status
    task.slug = task_id
    store.update(task)

    git._run("checkout", "-b", branch)
    if has_commits:
        (tmp_path / "feature.txt").write_text("feature")
        git._run("add", "feature.txt")
        git._run("commit", "-m", "Add feature")
    git._run("checkout", "main")

    return store, task, git


def init_repo_with_remote_tracking_only_feature(tmp_path: Path, branch: str) -> Git:
    git = init_basic_repo(tmp_path)
    (tmp_path / "base.txt").write_text("base\n")
    git._run("add", "base.txt")
    git._run("commit", "-m", "Base content")

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
