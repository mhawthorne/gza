"""Shared merge-state resolution helpers."""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from numbers import Integral
from typing import Any, Literal, cast

from .db import SqliteTaskStore, Task as DbTask
from .git import ResolvedMergeSourceRef, resolve_ref_if_possible

MergeBranchState = Literal["merged", "unmerged", "empty", "unknown"]


@dataclass(frozen=True)
class BranchMergeClassification:
    """Resolved branch truth relative to a target branch."""

    state: MergeBranchState
    reason: str
    source_ref: str | None
    target_ref: str
    source_sha: str | None
    target_sha: str | None


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


def _source_is_merged_in_side_branch(git: Any, source_ref: str, target_branch: str) -> bool:
    """Return whether ``source_ref`` landed as a merged-in side branch of the target.

    A fully merged branch and a stale empty branch are indistinguishable by
    commit-count alone (both have zero commits ahead of the merge base). The
    distinguishing signal is first-parent membership: a stale empty branch tip
    sits *on* the target's first-parent mainline, while a genuinely merged
    ``--no-ff`` branch tip is a second parent, *off* that mainline. We only
    treat the latter as merged. When first-parent membership can't be probed
    (e.g. a minimal git stub), default to ``False`` so the zero-commit branch
    classifies as ``empty`` per the classifier's stated contract.
    """
    is_on_first_parent = getattr(git, "is_on_first_parent_history", None)
    if not callable(is_on_first_parent):
        return False
    try:
        return is_on_first_parent(source_ref, target_branch) is not True
    except Exception:
        return False


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


def classify_branch_merge_state_for_target(
    *,
    git: Any,
    source_branch: str | None,
    target_branch: str,
    persisted_state: str | None = None,
    merged_proof: bool | None = None,
) -> BranchMergeClassification:
    """Classify branch merge truth relative to ``target_branch``."""

    source_ref = resolve_task_merge_source(git, source_branch).ref if source_branch else None
    source_resolution = resolve_ref_if_possible(git, source_ref)
    target_resolution = resolve_ref_if_possible(git, target_branch)
    source_sha = source_resolution.sha
    target_sha = target_resolution.sha

    if source_ref is None:
        if persisted_state in {"merged", "unmerged", "empty"}:
            persisted_branch_state = cast(MergeBranchState, persisted_state)
            return BranchMergeClassification(
                state=persisted_branch_state,
                reason="missing-ref-persisted-state",
                source_ref=None,
                target_ref=target_branch,
                source_sha=None,
                target_sha=target_sha,
            )
        return BranchMergeClassification(
            state="unknown",
            reason="missing-ref",
            source_ref=None,
            target_ref=target_branch,
            source_sha=None,
            target_sha=target_sha,
        )

    if merged_proof is None:
        is_merged = getattr(git, "is_merged", None)
        merged_proof = False
        if callable(is_merged):
            try:
                merged_proof = is_merged(source_ref, target_branch) is True
            except Exception:
                merged_proof = False

    if source_sha is None:
        if merged_proof:
            state = cast(
                MergeBranchState,
                classify_proven_merged_state(
                    git=git,
                    source_ref=source_ref,
                    target_branch=target_branch,
                ),
            )
            return BranchMergeClassification(
                state=state,
                reason=(
                    "content-equivalent-unresolved-source-sha"
                    if state == "merged"
                    else "no-unique-commits-unresolved-source-sha"
                ),
                source_ref=source_ref,
                target_ref=target_branch,
                source_sha=None,
                target_sha=target_sha,
            )
        if persisted_state in {"merged", "unmerged", "empty"}:
            persisted_branch_state = cast(MergeBranchState, persisted_state)
            return BranchMergeClassification(
                state=persisted_branch_state,
                reason="missing-ref-persisted-state",
                source_ref=source_ref,
                target_ref=target_branch,
                source_sha=None,
                target_sha=target_sha,
            )
        return BranchMergeClassification(
            state="unknown",
            reason="missing-ref",
            source_ref=source_ref,
            target_ref=target_branch,
            source_sha=None,
            target_sha=target_sha,
        )

    if target_sha is None:
        if merged_proof:
            state = cast(
                MergeBranchState,
                classify_proven_merged_state(
                    git=git,
                    source_ref=source_ref,
                    target_branch=target_branch,
                ),
            )
            return BranchMergeClassification(
                state=state,
                reason=(
                    "content-equivalent-unresolved-target-sha"
                    if state == "merged"
                    else "no-unique-commits-unresolved-target-sha"
                ),
                source_ref=source_ref,
                target_ref=target_branch,
                source_sha=source_sha,
                target_sha=None,
            )
        return BranchMergeClassification(
            state="unknown",
            reason="missing-target-ref",
            source_ref=source_ref,
            target_ref=target_branch,
            source_sha=source_sha,
            target_sha=None,
        )

    unique_commits: int | None = None
    count_base_ref = target_branch
    merge_base = getattr(git, "merge_base", None)
    if callable(merge_base):
        try:
            resolved_merge_base = merge_base(source_ref, target_branch)
        except Exception:
            resolved_merge_base = None
        if isinstance(resolved_merge_base, str) and resolved_merge_base:
            count_base_ref = resolved_merge_base

    count_commits_ahead = getattr(git, "count_commits_ahead", None)
    if callable(count_commits_ahead):
        try:
            unique_commits = count_commits_ahead(source_ref, count_base_ref)
        except Exception:
            unique_commits = None

    if unique_commits is None:
        count_commits_ahead_checked = getattr(git, "count_commits_ahead_checked", None)
        if callable(count_commits_ahead_checked):
            ahead_count = count_commits_ahead_checked(source_ref, count_base_ref)
            if isinstance(ahead_count, Integral):
                unique_commits = int(ahead_count)

    if unique_commits is None and source_sha == target_sha:
        unique_commits = 0

    if unique_commits == 0:
        # Zero commits ahead of the merge base can mean two topologically
        # identical-but-different things once a branch is an ancestor of the
        # target: (a) a stale *empty* branch whose tip sits on the target's
        # first-parent mainline and that carried no work of its own, or (b) a
        # genuinely *merged* side branch (a ``--no-ff`` second parent, off the
        # mainline) whose commits really landed. gza merges non-squash with
        # ``--no-ff`` whenever the commit count is below ``merge_squash_threshold``
        # (always, when the threshold is 0/disabled), so (b) is real behavior,
        # not hypothetical. Tell them apart by first-parent membership; only a
        # proven merged-in side branch stays ``merged``.
        if (
            source_sha != target_sha
            and merged_proof
            and _source_is_merged_in_side_branch(git, source_ref, target_branch)
        ):
            return BranchMergeClassification(
                state="merged",
                reason="merged-side-branch-no-unique-commits",
                source_ref=source_ref,
                target_ref=target_branch,
                source_sha=source_sha,
                target_sha=target_sha,
            )
        return BranchMergeClassification(
            state="empty",
            reason="no-unique-commits",
            source_ref=source_ref,
            target_ref=target_branch,
            source_sha=source_sha,
            target_sha=target_sha,
        )

    if unique_commits and merged_proof:
        return BranchMergeClassification(
            state="merged",
            reason="content-equivalent-with-commits",
            source_ref=source_ref,
            target_ref=target_branch,
            source_sha=source_sha,
            target_sha=target_sha,
        )

    if unique_commits and not merged_proof:
        return BranchMergeClassification(
            state="unmerged",
            reason="not-equivalent",
            source_ref=source_ref,
            target_ref=target_branch,
            source_sha=source_sha,
            target_sha=target_sha,
        )

    if source_sha == target_sha:
        return BranchMergeClassification(
            state="empty",
            reason="equal-tips-no-count-proof",
            source_ref=source_ref,
            target_ref=target_branch,
            source_sha=source_sha,
            target_sha=target_sha,
        )

    if merged_proof:
        return BranchMergeClassification(
            state="merged",
            reason="content-equivalent-with-commits-unverified",
            source_ref=source_ref,
            target_ref=target_branch,
            source_sha=source_sha,
            target_sha=target_sha,
        )

    return BranchMergeClassification(
        state="unknown",
        reason="unique-commit-count-unavailable",
        source_ref=source_ref,
        target_ref=target_branch,
        source_sha=source_sha,
        target_sha=target_sha,
    )


def resolve_task_merge_state_for_target(
    *,
    store: SqliteTaskStore,
    task: DbTask,
    git: Any,
    target_branch: str,
) -> str | None:
    """Resolve merge state for a specific target branch."""

    resolved_merge_unit = store.resolve_merge_unit_for_task(task.id) if task.id is not None else None
    merge_source = resolve_task_merge_source(git, task.branch) if task.branch else ResolvedMergeSourceRef(None)
    source_merge_ref = merge_source.ref

    merged_proof = False
    is_merged = getattr(git, "is_merged", None)
    if source_merge_ref is not None and callable(is_merged):
        try:
            merged_proof = is_merged(source_merge_ref, target_branch) is True
        except Exception:
            merged_proof = False

    if resolved_merge_unit is not None and resolved_merge_unit.target_branch == target_branch:
        if resolved_merge_unit.state == "empty":
            return "empty"
        if resolved_merge_unit.state == "merged" and not merged_proof:
            return "merged"

    persisted_state: str | None = None
    if resolved_merge_unit is not None and resolved_merge_unit.target_branch == target_branch:
        persisted_state = resolved_merge_unit.state
    elif resolved_merge_unit is None:
        persisted_state = task.merge_status

    classification = classify_branch_merge_state_for_target(
        git=git,
        source_branch=task.branch,
        target_branch=target_branch,
        persisted_state=persisted_state,
        merged_proof=merged_proof,
    )
    current_target_state = classification.state if classification.state in {"merged", "empty"} else None

    if (
        current_target_state == "merged"
        and classification.reason == "content-equivalent-with-commits-unverified"
        and source_merge_ref is not None
    ):
        logger.warning(
            "Could not prove whether merged source %r is empty against %r; keeping merge state at "
            "'merged' instead of classifying 'empty'",
            source_merge_ref,
            target_branch,
        )

    if resolved_merge_unit is not None:
        if resolved_merge_unit.target_branch == target_branch:
            if resolved_merge_unit.state == "merged":
                return current_target_state or "merged"
            if current_target_state is not None:
                return current_target_state
            return resolved_merge_unit.state

        if current_target_state is not None:
            return current_target_state
        if resolved_merge_unit.state == "merged":
            return None
        return resolved_merge_unit.state if classification.reason == "missing-ref-persisted-state" else None

    if current_target_state is not None:
        return current_target_state

    if task.merge_status == "merged":
        if not task.branch:
            return "merged"
        return None

    if classification.state != "unknown":
        return classification.state
    return task.merge_status
