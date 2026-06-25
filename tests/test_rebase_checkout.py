from pathlib import Path
from types import SimpleNamespace

from gza.config import Config
from gza.rebase_checkout import cleanup_isolated_rebase_checkout, create_isolated_rebase_checkout


class _FakeGit:
    instances: list["_FakeGit"] = []
    existing_refs: set[str] = set()
    config_values: dict[str, str] = {}

    def __init__(self, repo_dir: Path):
        self.repo_dir = Path(repo_dir)
        self.commands: list[tuple[str, ...]] = []
        self.checked_out: str | None = None
        self.reset_to: str | None = None
        self.cleaned = False
        _FakeGit.instances.append(self)

    def _run(self, *args: str, check: bool = True):
        self.commands.append(args)
        if args == ("init",):
            (self.repo_dir / ".git").mkdir(parents=True, exist_ok=True)
            return SimpleNamespace(stdout="", returncode=0)
        if args[:2] == ("config", "--get"):
            return SimpleNamespace(stdout=_FakeGit.config_values.get(args[2], "") + ("\n" if args[2] in _FakeGit.config_values else ""), returncode=0)
        return SimpleNamespace(stdout="", returncode=0)

    def ref_exists(self, ref: str) -> bool:
        return ref in _FakeGit.existing_refs

    def checkout(self, branch: str) -> None:
        self.checked_out = branch

    def reset_hard(self, ref: str) -> None:
        self.reset_to = ref

    def clean_force(self) -> None:
        self.cleaned = True


def test_create_isolated_rebase_checkout_uses_private_git_dir_and_local_fetch(monkeypatch, tmp_path: Path) -> None:
    _FakeGit.instances = []
    _FakeGit.existing_refs = {
        "refs/remotes/origin/feature/private-rebase",
        "refs/remotes/origin/main",
    }
    _FakeGit.config_values = {
        "user.name": "Test User",
        "user.email": "test@example.com",
    }
    monkeypatch.setattr("gza.rebase_checkout.Git", _FakeGit)

    source_repo = tmp_path / "repo"
    source_repo.mkdir()
    source_git = _FakeGit(source_repo)
    config = Config(
        project_dir=source_repo,
        project_name="repo",
        worktree_dir=str(tmp_path / "managed-worktrees"),
    )

    checkout = create_isolated_rebase_checkout(
        config=config,
        source_git=source_git,
        branch="feature/private-rebase",
        target_ref="main",
        checkout_name="gza-6049-s2",
    )

    checkout_git = checkout.git
    assert checkout.path.parent == config.worktree_path
    assert (checkout.path / ".git").is_dir()
    assert checkout_git.checked_out == "feature/private-rebase"
    assert checkout_git.reset_to == "feature/private-rebase"
    assert checkout_git.cleaned is True
    assert checkout.source_repo == source_repo.resolve()
    assert checkout.imported_refs == (
        "+refs/heads/feature/private-rebase:refs/heads/feature/private-rebase",
        "+refs/heads/main:refs/heads/main",
        "+refs/remotes/origin/feature/private-rebase:refs/remotes/origin/feature/private-rebase",
        "+refs/remotes/origin/main:refs/remotes/origin/main",
    )
    assert ("init",) in checkout_git.commands
    assert (
        "fetch",
        "--no-tags",
        str(source_repo.resolve()),
        *checkout.imported_refs,
    ) in checkout_git.commands

    cleanup_isolated_rebase_checkout(checkout)
    assert not checkout.path.exists()
