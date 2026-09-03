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
from .git import Git, GitError, validate_host_worktree_admin_metadata


@dataclass(frozen=True)
class IsolatedRebaseCheckout:
    """Ready-to-run private checkout for provider conflict resolution."""

    path: Path
    git: Git
    branch: str
    target_ref: str
    imported_refs: tuple[str, ...]
    source_repo: Path
    provider_target_ref: str | None = None


@dataclass(frozen=True)
class ImportedRebaseTip:
    """Canonical repo import result for an isolated rebase checkout."""

    branch: str
    new_tip: str
    previous_tip: str
    temp_ref: str


class StaleRebaseImportError(GitError):
    """Raised when the destination branch changed before isolated import."""


def _safe_checkout_stem(value: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9._-]+", "-", value).strip("-")
    return sanitized or "rebase"


def _copy_git_identity(*, source_git: Git, checkout_git: Git) -> None:
    for key in ("user.name", "user.email"):
        result = source_git._run("config", "--get", key, check=False)
        value = result.stdout.strip()
        if value:
            checkout_git._run("config", key, value)


def _assert_canonical_worktree_admin_metadata_healthy(source_git: Git, *, phase: str) -> None:
    """Fail closed if canonical worktree admin files contain container-only paths."""
    validation = validate_host_worktree_admin_metadata(source_git)
    if validation.is_healthy:
        return

    issue = validation.issues[0]
    expected = f" expected {issue.expected_value!r}." if issue.expected_value is not None else ""
    raise GitError(
        "Canonical worktree admin metadata became invalid during "
        f"{phase}: {issue.admin_path} contains {issue.value!r} ({issue.problem})."
        f"{expected} {issue.details}"
    )


def _build_import_refspecs(source_git: Git, *, branch: str, target_ref: str) -> tuple[str, ...]:
    refspecs: list[str] = [
        f"+refs/heads/{branch}:refs/heads/{branch}",
    ]
    if re.fullmatch(r"[0-9a-fA-F]{40}", target_ref):
        refspecs.append(f"+{target_ref}:refs/gza/rebase-target/{target_ref[:12]}")
        return tuple(dict.fromkeys(refspecs))

    if target_ref != branch:
        refspecs.append(f"+refs/heads/{target_ref}:refs/heads/{target_ref}")

    remote_tracking_branch = f"refs/remotes/origin/{branch}"
    if source_git.ref_exists(remote_tracking_branch):
        refspecs.append(f"+{remote_tracking_branch}:{remote_tracking_branch}")

    remote_tracking_target = f"refs/remotes/origin/{target_ref}"
    if target_ref != branch and source_git.ref_exists(remote_tracking_target):
        refspecs.append(f"+{remote_tracking_target}:{remote_tracking_target}")

    return tuple(dict.fromkeys(refspecs))


def stable_rebase_target_ref(target_ref: str) -> str:
    """Return the stable local ref/SHA providers must use for a captured target."""
    if re.fullmatch(r"[0-9a-fA-F]{40}", target_ref):
        return f"refs/gza/rebase-target/{target_ref[:12]}"
    return target_ref


def build_immutable_rebase_provider_prompt(
    *,
    auto_continue: bool,
    target_ref: str,
    target_sha: str,
) -> str:
    """Build the provider request for a rebase bound to an immutable target."""
    continue_flag = " --continue" if auto_continue else ""
    return (
        f"/gza-rebase --auto{continue_flag}\n\n"
        "Immutable rebase target supplied by gza:\n"
        f"- Rebase onto `{target_ref}`.\n"
        f"- Before rebasing, verify `{target_ref}` resolves to `{target_sha}`.\n"
        "- If the ref is missing or resolves to any other SHA, stop and report the mismatch.\n"
        "- Do not choose a default branch, remote branch, or similarly named mutable branch.\n"
    )


def append_immutable_rebase_target_instructions(
    prompt: str,
    *,
    target_ref: str,
    target_sha: str,
) -> str:
    """Append immutable target instructions to a task prompt."""
    target_block = build_immutable_rebase_provider_prompt(
        auto_continue=False,
        target_ref=target_ref,
        target_sha=target_sha,
    )
    return f"{prompt.rstrip()}\n\n{target_block}"


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
        _assert_canonical_worktree_admin_metadata_healthy(
            source_git,
            phase="isolated rebase checkout setup",
        )
        checkout_git._run("init")
        _copy_git_identity(source_git=source_git, checkout_git=checkout_git)

        source_repo = source_git.toplevel()
        imported_refs = _build_import_refspecs(source_git, branch=branch, target_ref=target_ref)
        provider_target_ref = stable_rebase_target_ref(target_ref)
        checkout_git._run(
            "fetch",
            "--no-tags",
            str(source_repo),
            *imported_refs,
        )
        checkout_git.checkout(branch)
        checkout_git.reset_hard(branch)
        checkout_git.clean_force()
        _assert_canonical_worktree_admin_metadata_healthy(
            source_git,
            phase="isolated rebase checkout setup",
        )
    except Exception:
        shutil.rmtree(checkout_path, ignore_errors=True)
        raise

    return IsolatedRebaseCheckout(
        path=checkout_path,
        git=checkout_git,
        branch=branch,
        target_ref=target_ref,
        provider_target_ref=provider_target_ref,
        imported_refs=imported_refs,
        source_repo=source_repo,
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
            raise StaleRebaseImportError(
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
        _assert_canonical_worktree_admin_metadata_healthy(
            source_git,
            phase="isolated rebase checkout cleanup",
        )
