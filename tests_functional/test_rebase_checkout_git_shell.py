from pathlib import Path

import pytest

from gza.git import Git
from gza.config import Config
from gza.rebase_checkout import isolated_rebase_checkout
from tests_functional.git_helpers import init_basic_repo


def _resolve_worktree_gitdir(worktree_path: Path) -> Path:
    git_file = worktree_path / ".git"
    gitdir_line = git_file.read_text(encoding="utf-8").strip()
    assert gitdir_line.startswith("gitdir: ")
    return Path(gitdir_line.removeprefix("gitdir: ")).resolve()


def test_private_rebase_checkout_prune_isolated_from_canonical_worktree_registry(tmp_path: Path) -> None:
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    git = init_basic_repo(repo_dir)

    git._run("checkout", "-b", "feature/task-a")
    (repo_dir / "task-a.txt").write_text("task a\n", encoding="utf-8")
    git._run("add", "task-a.txt")
    git._run("commit", "-m", "Task A")

    git._run("checkout", "main")
    git._run("checkout", "-b", "feature/task-b")
    (repo_dir / "task-b.txt").write_text("task b\n", encoding="utf-8")
    git._run("add", "task-b.txt")
    git._run("commit", "-m", "Task B")
    git._run("checkout", "main")

    config = Config(
        project_dir=repo_dir,
        project_name="repo",
        worktree_dir=str(tmp_path / "managed-worktrees"),
    )
    config.worktree_path.mkdir(parents=True, exist_ok=True)

    canonical_worktree = config.worktree_path / "task-a-canonical"
    git.worktree_add_existing(canonical_worktree, "feature/task-a")
    canonical_metadata_dir = _resolve_worktree_gitdir(canonical_worktree)
    assert canonical_metadata_dir.exists()

    with isolated_rebase_checkout(
        config=config,
        source_git=git,
        branch="feature/task-b",
        target_ref="main",
        checkout_name="task-b-private",
    ) as checkout:
        assert (checkout.path / ".git").is_dir()

        private_list = checkout.git._run("worktree", "list", "--porcelain").stdout
        assert str(checkout.path) in private_list
        assert str(canonical_worktree) not in private_list

        checkout.git._run("worktree", "prune")

        canonical_list = git._run("worktree", "list", "--porcelain").stdout
        assert str(canonical_worktree) in canonical_list
        assert canonical_metadata_dir.exists()


@pytest.mark.functional
def test_private_rebase_checkout_fetches_from_enclosing_repo_for_subdirectory_project(tmp_path: Path) -> None:
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    root_git = init_basic_repo(repo_dir)

    project_dir = repo_dir / "server"
    project_dir.mkdir()
    (project_dir / "app.py").write_text("print('main')\n", encoding="utf-8")
    root_git._run("add", "server/app.py")
    root_git._run("commit", "-m", "Add server project")

    root_git._run("checkout", "-b", "feature/server-rebase")
    (project_dir / "app.py").write_text("print('feature')\n", encoding="utf-8")
    root_git._run("add", "server/app.py")
    root_git._run("commit", "-m", "Update server project")
    root_git._run("checkout", "main")

    config = Config(
        project_dir=project_dir,
        project_name="server",
        worktree_dir=str(tmp_path / "managed-worktrees"),
    )
    source_git = Git(project_dir)

    with isolated_rebase_checkout(
        config=config,
        source_git=source_git,
        branch="feature/server-rebase",
        target_ref="main",
        checkout_name="gzaserver-36",
    ) as checkout:
        assert checkout.source_repo == repo_dir.resolve()
        assert checkout.git.current_branch() == "feature/server-rebase"
        assert (checkout.path / "server" / "app.py").read_text(encoding="utf-8") == "print('feature')\n"
