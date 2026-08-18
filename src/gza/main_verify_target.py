"""Local target ref proof helpers for main integration verify."""

from __future__ import annotations

from typing import Any

from .git import GitError


def current_local_target_head_sha(git: Any | None, *, target_branch: str | None = None) -> str | None:
    """Resolve the live local target branch head without ambiguous short-ref fallback."""
    if git is None:
        return None
    if not target_branch:
        return _rev_parse_if_exists(git, "HEAD")
    return _rev_parse_if_exists(git, f"refs/heads/{target_branch}")


def _rev_parse_if_exists(git: Any, ref: str) -> str | None:
    try:
        sha = git.rev_parse_if_exists(ref)
    except GitError:
        return None
    return sha if isinstance(sha, str) and sha else None
