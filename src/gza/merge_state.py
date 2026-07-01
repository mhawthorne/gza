"""Shared merge-state resolution helpers."""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from numbers import Integral
from typing import Any, Literal, cast

from .db import SqliteTaskStore, Task as DbTask
from .git import ResolvedMergeSourceRef, resolve_ref_if_possible

MergeBranchState = Literal["merged", "unmerged", "empty", "redundant", "unknown"]


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


def effective_no_work_merge_state(task: DbTask, raw_state: str | None) -> str | None:
    """Return the operator-effective no-work state for legacy empty rows."""
    if raw_state == "empty" and task.has_commits is True:
        return "redundant"
    return raw_state


def _unresolved_proven_merged_reason(state: MergeBranchState, *, unresolved_ref: str) -> str:
    """Return the reason slug for a proved-merged classification with an unresolved ref."""
    if state == "merged":
        return f"content-equivalent-unresolved-{unresolved_ref}-sha"
    if state == "unmerged":
        return f"net-diff-unresolved-{unresolved_ref}-sha"
    return f"no-work-proof-unavailable-unresolved-{unresolved_ref}-sha"


def classify_proven_merged_state(
    *,
    git: Any,
    source_ref: str,
    target_branch: str,
    source_has_commits: bool | None = None,
    recorded_head_sha: str | None = None,
    on_warning: Callable[[str], None] | None = None,
) -> str:
    """Classify a proved-merged source, failing closed if recorded-head proof disagrees."""
    current_target_state = "merged"
    can_preserve_merged_provenance = source_has_commits is not False
    count_commits_ahead_checked = getattr(git, "count_commits_ahead_checked", None)
    if callable(count_commits_ahead_checked):
        ahead_count = count_commits_ahead_checked(source_ref, target_branch)
        if isinstance(ahead_count, Integral) and ahead_count <= 0:
            source_sha = resolve_ref_if_possible(git, source_ref).sha
            target_sha = resolve_ref_if_possible(git, target_branch).sha
            guarded_state = _terminal_source_ref_head_guard(
                git=git,
                source_ref=source_ref,
                target_branch=target_branch,
                recorded_head_sha=recorded_head_sha,
                source_sha=source_sha,
                target_sha=target_sha,
                on_warning=on_warning,
            )
            net_diff = _source_has_remaining_net_diff(
                git,
                source_ref,
                target_branch,
                on_warning=on_warning,
            )
            if net_diff is None and source_has_commits is not False and source_sha != target_sha:
                if on_warning is not None:
                    on_warning(
                        f"Could not prove remaining diff for merged source {source_ref!r} against "
                        f"{target_branch!r}; keeping merge state at 'merged' instead of "
                        "classifying a no-work state"
                    )
                if guarded_state is not None:
                    return guarded_state.state
                return "merged"
            side_branch_probe = None
            if source_sha is not None and target_sha is not None and source_sha != target_sha:
                side_branch_probe = _source_is_merged_in_side_branch(
                    git,
                    source_ref,
                    target_branch,
                    on_warning=on_warning,
                )
            if side_branch_probe is True and can_preserve_merged_provenance:
                if guarded_state is not None:
                    return guarded_state.state
                current_target_state = "merged"
            else:
                if guarded_state is not None:
                    return guarded_state.state
                current_target_state = "redundant" if source_has_commits is True else "empty"
        elif ahead_count is None and on_warning is not None:
            on_warning(
                f"Could not prove whether merged source {source_ref!r} is empty against "
                f"{target_branch!r}; keeping merge state at 'merged' instead of "
                "classifying 'empty'"
            )
    return current_target_state


def _source_is_merged_in_side_branch(
    git: Any,
    source_ref: str,
    target_branch: str,
    *,
    on_warning: Callable[[str], None] | None = None,
) -> bool | None:
    """Return whether ``source_ref`` landed as a merged-in side branch of the target.

    A fully merged branch and a stale empty branch are indistinguishable by
    commit-count alone (both have zero commits ahead of the merge base). The
    distinguishing signal is first-parent membership: a stale empty branch tip
    sits *on* the target's first-parent mainline, while a genuinely merged
    ``--no-ff`` branch tip is a second parent, *off* that mainline. We only
    treat the latter as merged. When first-parent membership can't be probed,
    fail closed to ``None`` so callers can classify by task provenance instead
    of fabricating landed provenance from an unavailable probe.
    """
    is_on_first_parent = getattr(git, "is_on_first_parent_history", None)
    if not callable(is_on_first_parent):
        if on_warning is not None:
            on_warning(
                f"Could not probe first-parent membership for merged source {source_ref!r} "
                f"against {target_branch!r}; classifying by task provenance instead"
            )
        return None
    try:
        return is_on_first_parent(source_ref, target_branch) is not True
    except Exception as exc:
        if on_warning is not None:
            detail = " ".join(str(exc).split()) or exc.__class__.__name__
            on_warning(
                f"Could not probe first-parent membership for merged source {source_ref!r} "
                f"against {target_branch!r}: {detail}; classifying by task provenance instead"
            )
        return None


def _source_has_remaining_net_diff(
    git: Any,
    source_ref: str,
    target_branch: str,
    *,
    on_warning: Callable[[str], None] | None = None,
) -> bool | None:
    """Return whether ``source_ref`` still has a non-empty diff against ``target_branch``."""
    has_net_diff = getattr(git, "has_non_empty_source_diff_against_target", None)
    if callable(has_net_diff):
        try:
            result = has_net_diff(source_ref, target_branch)
        except Exception as exc:
            if on_warning is not None:
                detail = " ".join(str(exc).split()) or exc.__class__.__name__
                on_warning(
                    f"Could not compare remaining diff for {source_ref!r} against {target_branch!r}: "
                    f"{detail}; classifying by remaining provenance checks instead"
                )
            return None
        if isinstance(result, bool):
            return result

    resolve_refs = getattr(git, "resolve_refs", None)
    if callable(resolve_refs):
        try:
            resolved = resolve_refs((target_branch, source_ref), peel="tree")
        except Exception as exc:
            if on_warning is not None:
                detail = " ".join(str(exc).split()) or exc.__class__.__name__
                on_warning(
                    f"Could not compare remaining diff for {source_ref!r} against {target_branch!r}: "
                    f"{detail}; classifying by remaining provenance checks instead"
                )
            return None
        source_tree = resolved.get(source_ref)
        target_tree = resolved.get(target_branch)
        if source_tree is not None and target_tree is not None and source_tree == target_tree:
            return False
        if on_warning is not None:
            on_warning(
                f"Could not compare remaining diff for {source_ref!r} against {target_branch!r}: "
                "diff proof unavailable; classifying by remaining provenance checks instead"
            )
        return None

    if on_warning is not None:
        on_warning(
            f"Could not compare remaining diff for {source_ref!r} against {target_branch!r}: "
            "diff proof unavailable; classifying by remaining provenance checks instead"
        )
    return None


def _source_ref_contains_recorded_head(
    git: Any,
    source_ref: str,
    recorded_head_sha: str,
    *,
    source_sha: str | None = None,
    on_warning: Callable[[str], None] | None = None,
) -> bool | None:
    """Return whether ``source_ref`` resolves to or contains ``recorded_head_sha``."""
    if source_sha == recorded_head_sha:
        return True

    is_ancestor = getattr(git, "is_ancestor", None)
    if not callable(is_ancestor):
        return None

    try:
        return is_ancestor(recorded_head_sha, source_ref)
    except Exception as exc:
        if on_warning is not None:
            detail = " ".join(str(exc).split()) or exc.__class__.__name__
            on_warning(
                f"Could not verify whether {source_ref!r} contains recorded head {recorded_head_sha!r}: "
                f"{detail}; validating no-work state against recorded-head patch presence instead"
            )
        return None


def _recorded_head_patch_is_present_on_target(
    git: Any,
    recorded_head_sha: str,
    target_branch: str,
    *,
    on_warning: Callable[[str], None] | None = None,
) -> bool | None:
    """Return whether ``recorded_head_sha`` is already represented on ``target_branch``."""
    patch_present = getattr(git, "is_patch_equivalent_commit_present_on_target", None)
    if not callable(patch_present):
        return None

    try:
        return patch_present(recorded_head_sha, target_branch)
    except Exception as exc:
        if on_warning is not None:
            detail = " ".join(str(exc).split()) or exc.__class__.__name__
            on_warning(
                f"Could not verify whether recorded head {recorded_head_sha!r} is already represented "
                f"on {target_branch!r}: {detail}; classifying by remaining provenance checks instead"
            )
        return None


def recorded_head_has_remaining_net_diff(
    git: Any,
    recorded_head_sha: str,
    target_branch: str,
    *,
    on_warning: Callable[[str], None] | None = None,
) -> bool | None:
    """Return whether recorded-head patches are still missing from ``target_branch``.

    This is the fail-closed recorded-head proof used to distinguish stale source-ref
    no-work classifications from real landed/no-work outcomes. ``False`` means the
    full patch set reachable from ``recorded_head_sha`` is already represented on the
    target; ``True`` means at least one patch is still missing; ``None`` means the
    proof could not be established.
    """
    head_patch_present = _recorded_head_patch_is_present_on_target(
        git,
        recorded_head_sha,
        target_branch,
        on_warning=on_warning,
    )
    if head_patch_present is None:
        return None
    return not head_patch_present


def _terminal_source_ref_head_guard(
    *,
    git: Any,
    source_ref: str,
    target_branch: str,
    recorded_head_sha: str | None,
    source_sha: str | None,
    target_sha: str | None,
    on_warning: Callable[[str], None] | None = None,
) -> BranchMergeClassification | None:
    """Fail closed when a terminal merged/no-work state is inferred from a stale source ref."""
    if not recorded_head_sha:
        return None

    contains_recorded_head = _source_ref_contains_recorded_head(
        git,
        source_ref,
        recorded_head_sha,
        source_sha=source_sha,
        on_warning=on_warning,
    )
    if contains_recorded_head is True:
        return None

    remaining_net_diff = recorded_head_has_remaining_net_diff(
        git,
        recorded_head_sha,
        target_branch,
        on_warning=on_warning,
    )
    if remaining_net_diff is True:
        return BranchMergeClassification(
            state="unmerged",
            reason="recorded-head-has-net-diff",
            source_ref=source_ref,
            target_ref=target_branch,
            source_sha=source_sha,
            target_sha=target_sha,
        )
    if remaining_net_diff is None:
        return BranchMergeClassification(
            state="unknown",
            reason="recorded-head-diff-unavailable",
            source_ref=source_ref,
            target_ref=target_branch,
            source_sha=source_sha,
            target_sha=target_sha,
        )
    return None


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
    source_has_commits: bool | None = None,
    recorded_head_sha: str | None = None,
    on_warning: Callable[[str], None] | None = None,
) -> BranchMergeClassification:
    """Classify branch merge truth relative to ``target_branch``."""

    source_ref = resolve_task_merge_source(git, source_branch).ref if source_branch else None
    source_resolution = resolve_ref_if_possible(git, source_ref)
    target_resolution = resolve_ref_if_possible(git, target_branch)
    source_sha = source_resolution.sha
    target_sha = target_resolution.sha

    def _terminal_classification(state: MergeBranchState, reason: str) -> BranchMergeClassification:
        assert source_ref is not None
        guarded_state = _terminal_source_ref_head_guard(
            git=git,
            source_ref=source_ref,
            target_branch=target_branch,
            recorded_head_sha=recorded_head_sha,
            source_sha=source_sha,
            target_sha=target_sha,
            on_warning=on_warning,
        )
        if guarded_state is not None:
            return guarded_state
        return BranchMergeClassification(
            state=state,
            reason=reason,
            source_ref=source_ref,
            target_ref=target_branch,
            source_sha=source_sha,
            target_sha=target_sha,
        )

    if source_ref is None:
        if persisted_state in {"merged", "unmerged", "empty", "redundant"}:
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
                    source_has_commits=source_has_commits,
                    recorded_head_sha=recorded_head_sha,
                ),
            )
            return BranchMergeClassification(
                state=state,
                reason=_unresolved_proven_merged_reason(state, unresolved_ref="source"),
                source_ref=source_ref,
                target_ref=target_branch,
                source_sha=None,
                target_sha=target_sha,
            )
        if persisted_state in {"merged", "unmerged", "empty", "redundant"}:
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
                    source_has_commits=source_has_commits,
                    recorded_head_sha=recorded_head_sha,
                ),
            )
            return BranchMergeClassification(
                state=state,
                reason=_unresolved_proven_merged_reason(state, unresolved_ref="target"),
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
        net_diff = _source_has_remaining_net_diff(
            git,
            source_ref,
            target_branch,
            on_warning=on_warning,
        )
        if net_diff is True and source_has_commits is not False and not merged_proof:
            return BranchMergeClassification(
                state="unmerged",
                reason="net-diff-despite-zero-unique-commits",
                source_ref=source_ref,
                target_ref=target_branch,
                source_sha=source_sha,
                target_sha=target_sha,
            )
        if net_diff is None and source_has_commits is not False:
            if source_sha != target_sha:
                if merged_proof:
                    side_branch_tree_proof = _source_is_merged_in_side_branch(
                        git,
                        source_ref,
                        target_branch,
                        on_warning=on_warning,
                    )
                    if side_branch_tree_proof is True:
                        return _terminal_classification(
                            "merged",
                            "merged-side-branch-no-tree-proof",
                        )
                return BranchMergeClassification(
                    state="unknown",
                    reason="net-diff-unavailable-for-zero-unique-commits",
                    source_ref=source_ref,
                    target_ref=target_branch,
                    source_sha=source_sha,
                    target_sha=target_sha,
                )
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
        side_branch_probe: bool | None = False
        can_preserve_merged_provenance = source_has_commits is not False
        if source_sha != target_sha and merged_proof:
            side_branch_probe = _source_is_merged_in_side_branch(
                git,
                source_ref,
                target_branch,
                on_warning=on_warning,
            )
        if side_branch_probe is True and can_preserve_merged_provenance:
            return _terminal_classification(
                "merged",
                "merged-side-branch-no-unique-commits",
            )
        if source_has_commits is True:
            no_work_state: MergeBranchState = "redundant"
            reason = "no-unique-commits-with-task-commits"
        elif source_has_commits is False:
            no_work_state = "empty"
            reason = "no-task-commits"
        else:
            no_work_state = "empty"
            reason = "no-unique-commits"
        return _terminal_classification(no_work_state, reason)

    if unique_commits and merged_proof:
        return _terminal_classification(
            "merged",
            "content-equivalent-with-commits",
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
        return _terminal_classification(
            "redundant" if source_has_commits is True else "empty",
            (
                "equal-tips-with-task-commits"
                if source_has_commits is True
                else "equal-tips-no-count-proof"
            ),
        )

    if merged_proof:
        return _terminal_classification(
            "merged",
            "content-equivalent-with-commits-unverified",
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
        resolved_no_work_state = effective_no_work_merge_state(task, resolved_merge_unit.state)
        if resolved_no_work_state in {"empty", "redundant"}:
            if source_merge_ref is None:
                return resolved_merge_unit.state
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
        source_has_commits=task.has_commits,
        recorded_head_sha=resolved_merge_unit.head_sha if resolved_merge_unit is not None else None,
        on_warning=logger.warning,
    )
    current_target_state = classification.state if classification.state in {"merged", "empty", "redundant"} else None

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
            if (
                effective_no_work_merge_state(task, resolved_merge_unit.state) in {"empty", "redundant"}
                and classification.state == "unmerged"
            ):
                return "unmerged"
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
