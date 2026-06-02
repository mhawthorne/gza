"""Shared merge-state resolution helpers."""

from __future__ import annotations

import logging
from collections.abc import Callable
from numbers import Integral
from typing import Any

from .db import SqliteTaskStore, Task as DbTask
from .git import ResolvedMergeSourceRef

logger = logging.getLogger(__name__)


def classify_proven_merged_state(
    *,
    git: Any,
    source_ref: str,
    target_branch: str,
    on_warning: Callable[[str], None] | None = None,
) -> str:
    """Classify a proved-merged source as ``merged`` or zero-commit ``empty``."""
    current_target_state = "merged"
    count_commits_ahead_checked = getattr(git, "count_commits_ahead_checked", None)
    if callable(count_commits_ahead_checked):
        ahead_count = count_commits_ahead_checked(source_ref, target_branch)
        if isinstance(ahead_count, Integral) and ahead_count <= 0:
            current_target_state = "empty"
        elif ahead_count is None and on_warning is not None:
            on_warning(
                f"Could not prove whether merged source {source_ref!r} is empty against "
                f"{target_branch!r}; keeping merge state at 'merged' instead of "
                "classifying 'empty'"
            )
    return current_target_state


def resolve_task_merge_source(git: Any, branch: str) -> ResolvedMergeSourceRef:
    """Return the freshest merge source ref available for a branch."""
    resolve_fresh = getattr(git, "resolve_fresh_merge_source", None)
    if callable(resolve_fresh):
        resolved = resolve_fresh(branch)
        if isinstance(resolved, ResolvedMergeSourceRef):
            return resolved
        if isinstance(resolved, tuple) and len(resolved) == 2:
            return ResolvedMergeSourceRef(resolved[0], resolved[1])
        if isinstance(resolved, str):
            return ResolvedMergeSourceRef(resolved)
        if resolved is None:
            return ResolvedMergeSourceRef(None)

    resolve_fresh_ref = getattr(git, "resolve_fresh_merge_source_ref", None)
    if callable(resolve_fresh_ref):
        resolved_ref = resolve_fresh_ref(branch)
        if isinstance(resolved_ref, str) or resolved_ref is None:
            return ResolvedMergeSourceRef(resolved_ref)

    resolve_merge_source_ref = getattr(git, "resolve_merge_source_ref", None)
    if callable(resolve_merge_source_ref):
        resolved_ref = resolve_merge_source_ref(branch)
        if isinstance(resolved_ref, str) or resolved_ref is None:
            return ResolvedMergeSourceRef(resolved_ref)

    remote_ref = f"origin/{branch}"
    ref_exists = getattr(git, "ref_exists", None)
    if callable(ref_exists) and ref_exists(remote_ref):
        return ResolvedMergeSourceRef(remote_ref)

    branch_exists = getattr(git, "branch_exists", None)
    if callable(branch_exists) and branch_exists(branch):
        return ResolvedMergeSourceRef(branch)

    return ResolvedMergeSourceRef(branch)


def resolve_task_merge_state_for_target(
    *,
    store: SqliteTaskStore,
    task: DbTask,
    git: Any,
    target_branch: str,
) -> str | None:
    """Resolve merge state for a specific target branch.

    Stored merge-unit state remains authoritative for its recorded target, but
    live reachability against ``target_branch`` wins when it proves the source
    ref is already merged even if persistence is stale.
    """
    resolved_merge_unit = store.resolve_merge_unit_for_task(task.id) if task.id is not None else None
    merge_source = resolve_task_merge_source(git, task.branch) if task.branch else ResolvedMergeSourceRef(None)
    source_merge_ref = merge_source.ref
    if merge_source.warning:
        logger.warning(
            "Could not resolve freshest merge source for branch %r against %r: %s",
            task.branch,
            target_branch,
            merge_source.warning,
        )

    current_target_state: str | None = None
    if source_merge_ref is not None and git.is_merged(source_merge_ref, target_branch) is True:
        current_target_state = classify_proven_merged_state(
            git=git,
            source_ref=source_merge_ref,
            target_branch=target_branch,
            on_warning=logger.warning,
        )

    if resolved_merge_unit is not None:
        if resolved_merge_unit.state == "merged" and resolved_merge_unit.target_branch == target_branch:
            return current_target_state or "merged"
        if resolved_merge_unit.state == "merged":
            if current_target_state is not None:
                return current_target_state
            return None
        if current_target_state is not None:
            return current_target_state
        return resolved_merge_unit.state

    if current_target_state is not None:
        return current_target_state

    if task.merge_status == "merged":
        if not task.branch:
            return "merged"
        return None

    return task.merge_status
