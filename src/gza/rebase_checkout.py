"""Private git checkout helpers for provider-assisted rebase flows."""

from __future__ import annotations

import re
import shutil
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from .config import Config
from .git import Git, GitError


@dataclass(frozen=True)
class IsolatedRebaseCheckout:
    """Ready-to-run private checkout for provider conflict resolution."""

    path: Path
    git: Git
    branch: str
    target_ref: str
    imported_refs: tuple[str, ...]
    source_repo: Path


@dataclass(frozen=True)
class ImportedRebaseTip:
    """Canonical repo import result for an isolated rebase checkout."""

    branch: str
    new_tip: str
    previous_tip: str
    temp_ref: str


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
    try:
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
    except Exception:
        shutil.rmtree(checkout_path, ignore_errors=True)
        raise

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


def import_isolated_rebase_tip(
    *,
    destination_git: Git,
    checkout: IsolatedRebaseCheckout,
    branch: str,
    expected_old_sha: str | None,
    temp_ref_name: str,
) -> ImportedRebaseTip:
    """Import a rebased branch tip back into the canonical repo with a stale-head guard."""
    if not expected_old_sha:
        raise GitError(f"Cannot import rebased tip for {branch} without an expected old SHA")

    temp_ref = f"refs/gza/rebase-import/{_safe_checkout_stem(temp_ref_name)}-{uuid4().hex}"
    destination_git._run(
        "fetch",
        "--no-tags",
        str(checkout.path.resolve()),
        f"+refs/heads/{branch}:{temp_ref}",
    )
    imported_tip = destination_git.rev_parse(temp_ref)
    branch_ref = f"refs/heads/{branch}"

    try:
        destination_git.update_ref(branch_ref, imported_tip, expected_old_sha)
    except GitError as exc:
        current_tip = destination_git.rev_parse_if_exists(branch_ref)
        if current_tip != expected_old_sha:
            raise GitError(
                "Refusing to import rebased tip for "
                f"{branch}: expected old SHA {expected_old_sha}, found {current_tip or 'missing'}"
            ) from exc
        raise
    finally:
        destination_git._run("update-ref", "-d", temp_ref, check=False)

    return ImportedRebaseTip(
        branch=branch,
        new_tip=imported_tip,
        previous_tip=expected_old_sha,
        temp_ref=temp_ref,
    )


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
