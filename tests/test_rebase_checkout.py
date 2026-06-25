from pathlib import Path
from types import SimpleNamespace

import pytest

from gza.config import Config
from gza.git import GitError
from gza.rebase_checkout import (
    ImportedRebaseTip,
    IsolatedRebaseCheckout,
    cleanup_isolated_rebase_checkout,
    create_isolated_rebase_checkout,
    import_isolated_rebase_tip,
)


class _FakeGit:
    instances: list["_FakeGit"] = []
    existing_refs: set[str] = set()
    config_values: dict[str, str] = {}
    rev_parse_values: dict[str, str | None] = {}
    update_ref_error: GitError | None = None

    def __init__(self, repo_dir: Path):
        self.repo_dir = Path(repo_dir)
        self.commands: list[tuple[str, ...]] = []
        self.checked_out: str | None = None
        self.reset_to: str | None = None
        self.cleaned = False
        self.updated_refs: list[tuple[str, str, str | None]] = []
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

    def rev_parse(self, ref: str) -> str:
        value = _FakeGit.rev_parse_values.get(ref)
        if not value:
            raise GitError(f"unknown ref {ref}")
        return value

    def rev_parse_if_exists(self, ref: str) -> str | None:
        return _FakeGit.rev_parse_values.get(ref)

    def update_ref(self, ref: str, new_oid: str, old_oid: str | None = None) -> None:
        self.updated_refs.append((ref, new_oid, old_oid))
        if _FakeGit.update_ref_error is not None:
            raise _FakeGit.update_ref_error


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
    _FakeGit.rev_parse_values = {}
    _FakeGit.update_ref_error = None
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


def test_import_isolated_rebase_tip_fetches_private_tip_and_updates_canonical_ref(monkeypatch, tmp_path: Path) -> None:
    _FakeGit.instances = []
    _FakeGit.existing_refs = set()
    _FakeGit.config_values = {}
    _FakeGit.rev_parse_values = {
        "refs/gza/rebase-import/import-rebase-123": "rebased-tip",
        "refs/heads/feature/private-rebase": "old-tip",
    }
    _FakeGit.update_ref_error = None
    monkeypatch.setattr("gza.rebase_checkout.uuid4", lambda: SimpleNamespace(hex="123"))

    destination_git = _FakeGit(tmp_path / "canonical")
    checkout = IsolatedRebaseCheckout(
        path=tmp_path / "isolated",
        git=_FakeGit(tmp_path / "isolated"),
        branch="feature/private-rebase",
        target_ref="main",
        imported_refs=(),
        source_repo=tmp_path / "canonical",
    )
    checkout.path.mkdir()

    imported = import_isolated_rebase_tip(
        destination_git=destination_git,
        checkout=checkout,
        branch="feature/private-rebase",
        expected_old_sha="old-tip",
        temp_ref_name="import rebase",
    )

    assert imported == ImportedRebaseTip(
        branch="feature/private-rebase",
        new_tip="rebased-tip",
        previous_tip="old-tip",
        temp_ref="refs/gza/rebase-import/import-rebase-123",
    )
    assert (
        "fetch",
        "--no-tags",
        str(checkout.path.resolve()),
        "+refs/heads/feature/private-rebase:refs/gza/rebase-import/import-rebase-123",
    ) in destination_git.commands
    assert destination_git.updated_refs == [
        ("refs/heads/feature/private-rebase", "rebased-tip", "old-tip"),
    ]
    assert ("update-ref", "-d", "refs/gza/rebase-import/import-rebase-123") in destination_git.commands


def test_import_isolated_rebase_tip_fails_closed_when_canonical_branch_moved(monkeypatch, tmp_path: Path) -> None:
    _FakeGit.instances = []
    _FakeGit.existing_refs = set()
    _FakeGit.config_values = {}
    _FakeGit.rev_parse_values = {
        "refs/gza/rebase-import/stale-branch-123": "rebased-tip",
        "refs/heads/feature/private-rebase": "unexpected-new-tip",
    }
    _FakeGit.update_ref_error = GitError("update-ref failed")
    monkeypatch.setattr("gza.rebase_checkout.uuid4", lambda: SimpleNamespace(hex="123"))

    destination_git = _FakeGit(tmp_path / "canonical")
    checkout = IsolatedRebaseCheckout(
        path=tmp_path / "isolated",
        git=_FakeGit(tmp_path / "isolated"),
        branch="feature/private-rebase",
        target_ref="main",
        imported_refs=(),
        source_repo=tmp_path / "canonical",
    )
    checkout.path.mkdir()

    with pytest.raises(
        GitError,
        match=(
            "Refusing to import rebased tip for feature/private-rebase: "
            "expected old SHA old-tip, found unexpected-new-tip"
        ),
    ):
        import_isolated_rebase_tip(
            destination_git=destination_git,
            checkout=checkout,
            branch="feature/private-rebase",
            expected_old_sha="old-tip",
            temp_ref_name="stale branch",
        )

    assert ("update-ref", "-d", "refs/gza/rebase-import/stale-branch-123") in destination_git.commands
