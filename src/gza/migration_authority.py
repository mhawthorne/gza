"""Canonical checkout proof for shared database migrations."""

from __future__ import annotations

from pathlib import Path

from .db import MigrationAuthorityProof, SchemaIntegrityError
from .git import Git, GitError


def _has_git_checkout_marker(path: Path) -> bool:
    for candidate in (path, *path.parents):
        marker = candidate / ".git"
        try:
            marker.lstat()
        except FileNotFoundError:
            continue
        except (OSError, RuntimeError, ValueError):
            return False
        try:
            if marker.is_file():
                text = marker.read_text(encoding="utf-8").strip()
                if not text.startswith("gitdir:"):
                    return False
                raw_gitdir = text[len("gitdir:") :].strip()
                if not raw_gitdir:
                    return False
                gitdir = Path(raw_gitdir)
                if not gitdir.is_absolute():
                    gitdir = marker.parent / gitdir
                return (gitdir.resolve() / "HEAD").is_file()
            if marker.is_dir() and (marker / "HEAD").is_file() and (marker / "config").is_file():
                return True
        except (OSError, RuntimeError, ValueError):
            return False
        return False
    return False


def _resolve_primary_worktree_root(git: Git) -> Path | None:
    try:
        worktrees = git.worktree_list()
    except (GitError, OSError, RuntimeError, ValueError):
        return None
    if not worktrees:
        return None
    raw_path = worktrees[0].get("path")
    if not isinstance(raw_path, str) or not raw_path:
        return None
    try:
        return Path(raw_path).resolve()
    except (OSError, RuntimeError, ValueError):
        return None


def _observe_canonical_default_tip(config: object) -> tuple[Path, str, str]:
    project_dir = Path(getattr(config, "project_dir"))
    git = Git(project_dir)
    try:
        canonical_root = git.toplevel().resolve()
        primary_root = _resolve_primary_worktree_root(git)
        if primary_root != canonical_root:
            raise SchemaIntegrityError(
                "Shared database migration authority requires the primary canonical checkout."
            )
        default_branch = git.default_branch()
        current_branch = git.current_branch()
        if current_branch != default_branch:
            raise SchemaIntegrityError(
                "Shared database migration authority requires the default branch "
                f"{default_branch!r}; current checkout is {current_branch!r}."
            )
        head_sha = git.rev_parse_if_exists("HEAD")
        branch_sha = git.rev_parse_if_exists(f"refs/heads/{default_branch}")
    except (GitError, OSError, RuntimeError, ValueError) as exc:
        raise SchemaIntegrityError(f"Shared database migration authority could not be proven: {exc}") from exc
    if not head_sha or not branch_sha:
        raise SchemaIntegrityError(
            "Shared database migration authority requires both HEAD and the default branch ref to resolve."
        )
    if head_sha != branch_sha:
        raise SchemaIntegrityError(
            "Shared database migration authority requires HEAD to equal "
            f"refs/heads/{default_branch}."
        )
    return canonical_root, default_branch, head_sha


def resolve_canonical_migration_authority(config: object) -> MigrationAuthorityProof | None:
    """Return a revalidatable proof when ``config`` is the canonical default-branch checkout."""
    project_dir = Path(getattr(config, "project_dir"))
    if not _has_git_checkout_marker(project_dir):
        return None
    try:
        canonical_root, default_branch, head_sha = _observe_canonical_default_tip(config)
    except SchemaIntegrityError:
        return None

    def revalidate() -> None:
        current_root, current_default_branch, current_head_sha = _observe_canonical_default_tip(config)
        if current_root != canonical_root:
            raise SchemaIntegrityError(
                "Shared database migration authority proof became stale: canonical root changed."
            )
        if current_default_branch != default_branch:
            raise SchemaIntegrityError(
                "Shared database migration authority proof became stale: default branch changed."
            )
        if current_head_sha != head_sha:
            raise SchemaIntegrityError(
                "Shared database migration authority proof became stale: HEAD or default branch tip changed."
            )

    return MigrationAuthorityProof(
        canonical_root=canonical_root,
        default_branch=default_branch,
        head_sha=head_sha,
        revalidate=revalidate,
    )
