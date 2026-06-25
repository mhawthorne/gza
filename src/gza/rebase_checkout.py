"""Private git checkout helpers for provider-assisted rebase flows."""

from __future__ import annotations

import re
import shutil
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from .config import Config
from .git import Git


@dataclass(frozen=True)
class IsolatedRebaseCheckout:
    """Ready-to-run private checkout for provider conflict resolution."""

    path: Path
    git: Git
    branch: str
    target_ref: str
    imported_refs: tuple[str, ...]
    source_repo: Path


def _safe_checkout_stem(value: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9._-]+", "-", value).strip("-")
    return sanitized or "rebase"


def _copy_git_identity(*, source_git: Git, checkout_git: Git) -> None:
    for key in ("user.name", "user.email"):
        result = source_git._run("config", "--get", key, check=False)
        value = result.stdout.strip()
        if value:
            checkout_git._run("config", key, value)


def _build_import_refspecs(source_git: Git, *, branch: str, target_ref: str) -> tuple[str, ...]:
    refspecs: list[str] = [
        f"+refs/heads/{branch}:refs/heads/{branch}",
    ]
    if target_ref != branch:
        refspecs.append(f"+refs/heads/{target_ref}:refs/heads/{target_ref}")

    remote_tracking_branch = f"refs/remotes/origin/{branch}"
    if source_git.ref_exists(remote_tracking_branch):
        refspecs.append(f"+{remote_tracking_branch}:{remote_tracking_branch}")

    remote_tracking_target = f"refs/remotes/origin/{target_ref}"
    if target_ref != branch and source_git.ref_exists(remote_tracking_target):
        refspecs.append(f"+{remote_tracking_target}:{remote_tracking_target}")

    return tuple(dict.fromkeys(refspecs))


def create_isolated_rebase_checkout(
    *,
    config: Config,
    source_git: Git,
    branch: str,
    target_ref: str,
    checkout_name: str,
) -> IsolatedRebaseCheckout:
    """Create a standalone checkout with a private `.git/` directory.

    The checkout lives under ``config.worktree_path`` and imports only the refs
    needed for local rebase conflict resolution from the canonical repo using a
    path-based local fetch.
    """
    config.worktree_path.mkdir(parents=True, exist_ok=True)
    checkout_path = Path(
        tempfile.mkdtemp(
            prefix=f"{_safe_checkout_stem(checkout_name)}-rebase-git-",
            dir=config.worktree_path,
        )
    )
    checkout_git = Git(checkout_path)
    checkout_git._run("init")
    _copy_git_identity(source_git=source_git, checkout_git=checkout_git)

    imported_refs = _build_import_refspecs(source_git, branch=branch, target_ref=target_ref)
    checkout_git._run(
        "fetch",
        "--no-tags",
        str(source_git.repo_dir.resolve()),
        *imported_refs,
    )
    checkout_git.checkout(branch)
    checkout_git.reset_hard(branch)
    checkout_git.clean_force()

    return IsolatedRebaseCheckout(
        path=checkout_path,
        git=checkout_git,
        branch=branch,
        target_ref=target_ref,
        imported_refs=imported_refs,
        source_repo=source_git.repo_dir.resolve(),
    )


def cleanup_isolated_rebase_checkout(checkout: IsolatedRebaseCheckout) -> None:
    """Remove a private rebase checkout without touching canonical worktrees."""
    shutil.rmtree(checkout.path, ignore_errors=True)


@contextmanager
def isolated_rebase_checkout(
    *,
    config: Config,
    source_git: Git,
    branch: str,
    target_ref: str,
    checkout_name: str,
) -> Iterator[IsolatedRebaseCheckout]:
    """Yield a private rebase checkout and clean it up afterwards."""
    checkout = create_isolated_rebase_checkout(
        config=config,
        source_git=source_git,
        branch=branch,
        target_ref=target_ref,
        checkout_name=checkout_name,
    )
    try:
        yield checkout
    finally:
        cleanup_isolated_rebase_checkout(checkout)
