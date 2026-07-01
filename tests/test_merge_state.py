"""Unit tests for merge-state classification helpers."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path

import pytest

from gza.db import SqliteTaskStore
from gza.merge_state import (
    classify_branch_merge_state_for_target,
    classify_proven_merged_state,
    resolve_task_merge_state_for_target,
)

logger = logging.getLogger(__name__)


class _FakeGit:
    def __init__(
        self,
        *,
        source_ref: str | None = None,
        merge_source_warning: str | None = None,
        ref_shas: dict[str, str | None] | None = None,
        tree_shas: dict[str, str | None] | None = None,
        ahead_count: int | None = None,
        merged: bool = False,
        net_diff: bool | None = None,
        net_diff_by_ref: dict[str, bool | None] | None = None,
        patch_present_by_commit: dict[tuple[str, str], bool | None] | None = None,
        ancestors: dict[tuple[str, str], bool] | None = None,
        on_first_parent: bool = True,
        first_parent_error: Exception | None = None,
    ) -> None:
        self._source_ref = source_ref
        self._merge_source_warning = merge_source_warning
        self._ref_shas = ref_shas or {}
        self._tree_shas = tree_shas or {}
        self._ahead_count = ahead_count
        self._merged = merged
        self._net_diff = net_diff
        self._net_diff_by_ref = net_diff_by_ref or {}
        self._patch_present_by_commit = patch_present_by_commit or {}
        self._ancestors = ancestors or {}
        self._on_first_parent = on_first_parent
        self._first_parent_error = first_parent_error

    def resolve_fresh_merge_source(self, _branch: str):
        from gza.git import ResolvedMergeSourceRef

        return ResolvedMergeSourceRef(self._source_ref, self._merge_source_warning)

    def rev_parse_if_exists(self, ref: str) -> str | None:
        return self._ref_shas.get(ref)

    def resolve_refs(self, refs: tuple[str, ...], peel: str = "commit") -> dict[str, str | None]:
        if peel == "tree":
            return {ref: self._tree_shas.get(ref) for ref in refs}
        return {ref: self._ref_shas.get(ref) for ref in refs}

    def count_commits_ahead(self, _branch: str, _target: str) -> int:
        if self._ahead_count is None:
            raise RuntimeError("ahead-count unavailable")
        return self._ahead_count

    def count_commits_ahead_checked(self, _branch: str, _target: str) -> int | None:
        return self._ahead_count

    def is_merged(self, _branch: str, _target: str) -> bool:
        return self._merged

    def has_non_empty_source_diff_against_target(self, _source_ref: str, _target: str) -> bool | None:
        if _source_ref in self._net_diff_by_ref:
            return self._net_diff_by_ref[_source_ref]
        return self._net_diff

    def is_patch_equivalent_commit_present_on_target(self, commit: str, target: str) -> bool | None:
        return self._patch_present_by_commit.get((commit, target))

    def is_ancestor(self, ancestor: str, descendant: str) -> bool:
        return self._ancestors.get((ancestor, descendant), False)

    def is_on_first_parent_history(self, _commit: str, _target: str) -> bool:
        if self._first_parent_error is not None:
            raise self._first_parent_error
        return self._on_first_parent


class _GitWithoutTreeResolveRefs:
    def __init__(self, delegate: _FakeGit) -> None:
        self._delegate = delegate

    def __getattr__(self, name: str):
        if name == "resolve_refs":
            raise AttributeError(name)
        return getattr(self._delegate, name)


class _GitTreeResolveRefsRaises:
    def __init__(self, delegate: _FakeGit) -> None:
        self._delegate = delegate

    def __getattr__(self, name: str):
        if name == "resolve_refs":
            return self.resolve_refs
        return getattr(self._delegate, name)

    def resolve_refs(self, refs: tuple[str, ...], peel: str = "commit") -> dict[str, str | None]:
        if peel == "tree":
            raise RuntimeError("tree lookup exploded")
        return self._delegate.resolve_refs(refs, peel=peel)


def test_resolve_task_merge_state_classifies_zero_commit_branch_with_commits_as_redundant(
    tmp_path: Path,
) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    task = store.add("Implement empty branch", task_type="implement")
    store.mark_completed(task, has_commits=True, branch="feature/empty-branch")
    assert task.id is not None

    unit = store.resolve_merge_unit_for_task(task.id)
    assert unit is not None
    store.set_merge_unit_state(unit.id, "merged")

    refreshed = store.get(task.id)
    assert refreshed is not None

    result = resolve_task_merge_state_for_target(
        store=store,
        task=refreshed,
        git=_FakeGit(
            source_ref="feature/empty-branch",
            ref_shas={"feature/empty-branch": "same-sha", "main": "same-sha"},
            tree_shas={"feature/empty-branch": "shared-tree-sha", "main": "shared-tree-sha"},
            ahead_count=0,
            merged=True,
        ),
        target_branch="main",
    )

    assert result == "redundant"


def test_resolve_task_merge_state_keeps_merged_when_empty_probe_is_indeterminate(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    task = store.add("Implement merged branch", task_type="implement")
    store.mark_completed(task, has_commits=True, branch="feature/merged-branch")
    assert task.id is not None

    unit = store.resolve_merge_unit_for_task(task.id)
    assert unit is not None
    store.set_merge_unit_state(unit.id, "merged")

    refreshed = store.get(task.id)
    assert refreshed is not None

    with caplog.at_level("WARNING"):
        result = resolve_task_merge_state_for_target(
            store=store,
            task=refreshed,
            git=_FakeGit(
                source_ref="feature/merged-branch",
                ref_shas={"feature/merged-branch": "source-sha", "main": "target-sha"},
                ahead_count=None,
                merged=True,
            ),
            target_branch="main",
        )

    assert result == "merged"
    assert "keeping merge state at 'merged' instead of classifying 'empty'" in caplog.text


def test_resolve_task_merge_state_prefers_redundant_over_merged_when_task_commits_have_no_unique_commits(
    tmp_path: Path,
) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    task = store.add("Implement feature", task_type="implement")
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.has_commits = True
    task.branch = "feature/empty"
    task.merge_status = "merged"
    store.update(task)

    state = resolve_task_merge_state_for_target(
        store=store,
        task=task,
        git=_FakeGit(
            source_ref=task.branch,
            ref_shas={task.branch: "same-sha", "main": "same-sha"},
            tree_shas={task.branch: "shared-tree-sha", "main": "shared-tree-sha"},
            ahead_count=0,
            merged=True,
        ),
        target_branch="main",
    )

    assert state == "redundant"


def test_classify_stale_empty_branch_on_mainline_is_empty_not_merged() -> None:
    # B1: a stale empty branch (forked from an older target that then advanced)
    # is an ancestor of the target with zero unique commits and its tip sits on
    # the target's first-parent mainline. It carried no work -> empty, not merged.
    result = classify_branch_merge_state_for_target(
        git=_FakeGit(
            source_ref="feature/empty",
            ref_shas={"feature/empty": "old-main-sha", "main": "advanced-main-sha"},
            ahead_count=0,
            merged=True,
            on_first_parent=True,
        ),
        source_branch="feature/empty",
        target_branch="main",
        source_has_commits=False,
    )

    assert result.state == "empty"
    assert result.reason == "no-task-commits"


def test_classify_zero_unique_commits_with_task_commits_is_redundant() -> None:
    result = classify_branch_merge_state_for_target(
        git=_FakeGit(
            source_ref="feature/redundant",
            ref_shas={"feature/redundant": "old-main-sha", "main": "advanced-main-sha"},
            tree_shas={"feature/redundant": "shared-tree-sha", "main": "shared-tree-sha"},
            ahead_count=0,
            merged=True,
            on_first_parent=True,
        ),
        source_branch="feature/redundant",
        target_branch="main",
        source_has_commits=True,
    )

    assert result.state == "redundant"
    assert result.reason == "no-unique-commits-with-task-commits"


@pytest.mark.parametrize(
    ("git", "warning_text"),
    [
        (
            _GitWithoutTreeResolveRefs(
                _FakeGit(
                    source_ref="feature/redundant",
                    ref_shas={"feature/redundant": "old-main-sha", "main": "advanced-main-sha"},
                    ahead_count=0,
                    merged=False,
                )
            ),
            "diff proof unavailable",
        ),
        (
            _GitTreeResolveRefsRaises(
                _FakeGit(
                    source_ref="feature/redundant",
                    ref_shas={"feature/redundant": "old-main-sha", "main": "advanced-main-sha"},
                    ahead_count=0,
                    merged=False,
                )
            ),
            "tree lookup exploded",
        ),
        (
            _FakeGit(
                source_ref="feature/redundant",
                ref_shas={"feature/redundant": "old-main-sha", "main": "advanced-main-sha"},
                tree_shas={"feature/redundant": None, "main": "shared-tree-sha"},
                ahead_count=0,
                merged=False,
            ),
            "diff proof unavailable",
        ),
    ],
)
def test_classify_zero_unique_commits_with_task_commits_fails_closed_without_tree_proof(
    git: object,
    warning_text: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level("WARNING"):
        result = classify_branch_merge_state_for_target(
            git=git,
            source_branch="feature/redundant",
            target_branch="main",
            source_has_commits=True,
            on_warning=lambda message: logger.warning(message),
        )

    assert result.state == "unknown"
    assert result.reason == "net-diff-unavailable-for-zero-unique-commits"
    assert warning_text in caplog.text


def test_classify_no_ff_merged_side_branch_is_merged() -> None:
    # A genuinely --no-ff-merged branch (real gza behavior when the commit count
    # is below merge_squash_threshold) is also an ancestor with zero unique
    # commits, but its tip is a merged-in second parent, OFF the first-parent
    # mainline. Its commits really landed -> merged.
    result = classify_branch_merge_state_for_target(
        git=_FakeGit(
            source_ref="feature/merged",
            ref_shas={"feature/merged": "branch-tip-sha", "main": "merge-commit-sha"},
            tree_shas={"feature/merged": "shared-tree-sha", "main": "shared-tree-sha"},
            ahead_count=0,
            merged=True,
            on_first_parent=False,
        ),
        source_branch="feature/merged",
        target_branch="main",
        source_has_commits=True,
    )

    assert result.state == "merged"
    assert result.reason == "merged-side-branch-no-unique-commits"


def test_classify_merged_side_branch_stays_merged_after_target_advances() -> None:
    result = classify_branch_merge_state_for_target(
        git=_FakeGit(
            source_ref="feature/merged",
            ref_shas={"feature/merged": "branch-tip-sha", "main": "advanced-main-sha"},
            tree_shas={"feature/merged": "branch-tree-sha", "main": "advanced-main-tree-sha"},
            ahead_count=0,
            merged=True,
            net_diff=False,
            on_first_parent=False,
        ),
        source_branch="feature/merged",
        target_branch="main",
        source_has_commits=True,
    )

    assert result.state == "merged"
    assert result.reason == "merged-side-branch-no-unique-commits"


def test_classify_zero_unique_commits_with_live_net_diff_stays_unmerged() -> None:
    result = classify_branch_merge_state_for_target(
        git=_FakeGit(
            source_ref="feature/false-moot",
            ref_shas={"feature/false-moot": "branch-tip-sha", "main": "target-tip-sha"},
            tree_shas={"feature/false-moot": "branch-tree-sha", "main": "target-tree-sha"},
            ahead_count=0,
            merged=False,
            net_diff=True,
            on_first_parent=True,
        ),
        source_branch="feature/false-moot",
        target_branch="main",
        source_has_commits=True,
    )

    assert result.state == "unmerged"
    assert result.reason == "net-diff-despite-zero-unique-commits"


def test_classify_stale_source_missing_recorded_head_stays_unmerged() -> None:
    result = classify_branch_merge_state_for_target(
        git=_FakeGit(
            source_ref="origin/feature/false-redundant",
            ref_shas={"origin/feature/false-redundant": "base-sha", "main": "target-sha"},
            tree_shas={"origin/feature/false-redundant": "shared-tree-sha", "main": "shared-tree-sha"},
            ahead_count=0,
            merged=True,
            net_diff_by_ref={
                "origin/feature/false-redundant": False,
            },
            patch_present_by_commit={("recorded-head-sha", "main"): False},
            ancestors={("recorded-head-sha", "origin/feature/false-redundant"): False},
            on_first_parent=True,
        ),
        source_branch="feature/false-redundant",
        target_branch="main",
        source_has_commits=True,
        recorded_head_sha="recorded-head-sha",
    )

    assert result.state == "unmerged"
    assert result.reason == "recorded-head-has-net-diff"


def test_classify_stale_source_missing_recorded_head_can_still_be_redundant_when_head_has_no_diff() -> None:
    result = classify_branch_merge_state_for_target(
        git=_FakeGit(
            source_ref="origin/feature/redundant",
            ref_shas={"origin/feature/redundant": "base-sha", "main": "target-sha"},
            tree_shas={"origin/feature/redundant": "shared-tree-sha", "main": "shared-tree-sha"},
            ahead_count=0,
            merged=True,
            net_diff_by_ref={
                "origin/feature/redundant": False,
            },
            patch_present_by_commit={("recorded-head-sha", "main"): True},
            ancestors={("recorded-head-sha", "origin/feature/redundant"): False},
            on_first_parent=True,
        ),
        source_branch="feature/redundant",
        target_branch="main",
        source_has_commits=True,
        recorded_head_sha="recorded-head-sha",
    )

    assert result.state == "redundant"
    assert result.reason == "no-unique-commits-with-task-commits"


def test_classify_proved_merged_unresolved_source_sha_missing_recorded_head_stays_unmerged() -> None:
    result = classify_branch_merge_state_for_target(
        git=_FakeGit(
            source_ref="origin/feature/false-redundant",
            ref_shas={"origin/feature/false-redundant": None, "main": "target-sha"},
            tree_shas={"origin/feature/false-redundant": "shared-tree-sha", "main": "shared-tree-sha"},
            ahead_count=0,
            merged=True,
            net_diff_by_ref={"origin/feature/false-redundant": False},
            patch_present_by_commit={("recorded-head-sha", "main"): False},
            ancestors={("recorded-head-sha", "origin/feature/false-redundant"): False},
            on_first_parent=True,
        ),
        source_branch="feature/false-redundant",
        target_branch="main",
        source_has_commits=True,
        recorded_head_sha="recorded-head-sha",
    )

    assert result.state == "unmerged"
    assert result.reason == "net-diff-unresolved-source-sha"


def test_classify_proved_merged_unresolved_target_sha_missing_recorded_head_stays_unmerged() -> None:
    result = classify_branch_merge_state_for_target(
        git=_FakeGit(
            source_ref="origin/feature/false-redundant",
            ref_shas={"origin/feature/false-redundant": "base-sha", "main": None},
            tree_shas={"origin/feature/false-redundant": "shared-tree-sha", "main": "shared-tree-sha"},
            ahead_count=0,
            merged=True,
            net_diff_by_ref={"origin/feature/false-redundant": False},
            patch_present_by_commit={("recorded-head-sha", "main"): False},
            ancestors={("recorded-head-sha", "origin/feature/false-redundant"): False},
            on_first_parent=True,
        ),
        source_branch="feature/false-redundant",
        target_branch="main",
        source_has_commits=True,
        recorded_head_sha="recorded-head-sha",
    )

    assert result.state == "unmerged"
    assert result.reason == "net-diff-unresolved-target-sha"


def test_classify_proven_merged_state_preserves_no_ff_side_branch_as_merged() -> None:
    result = classify_proven_merged_state(
        git=_FakeGit(
            ref_shas={"feature/merged": "branch-tip-sha", "main": "merge-commit-sha"},
            tree_shas={"feature/merged": "shared-tree-sha", "main": "shared-tree-sha"},
            ahead_count=0,
            on_first_parent=False,
        ),
        source_ref="feature/merged",
        target_branch="main",
        source_has_commits=True,
    )

    assert result == "merged"


def test_classify_proven_merged_state_stays_merged_when_target_advanced_after_landing() -> None:
    result = classify_proven_merged_state(
        git=_FakeGit(
            ref_shas={"feature/merged": "branch-tip-sha", "main": "target-tip-sha"},
            tree_shas={"feature/merged": "branch-tree-sha", "main": "target-tree-sha"},
            ahead_count=0,
            net_diff=False,
            on_first_parent=False,
        ),
        source_ref="feature/merged",
        target_branch="main",
        source_has_commits=True,
    )

    assert result == "merged"


def test_classify_proven_merged_side_branch_missing_recorded_head_stays_unmerged() -> None:
    result = classify_proven_merged_state(
        git=_FakeGit(
            ref_shas={"feature/merged": "branch-tip-sha", "main": "target-tip-sha"},
            tree_shas={"feature/merged": "branch-tree-sha", "main": "target-tree-sha"},
            ahead_count=0,
            net_diff=False,
            patch_present_by_commit={("recorded-head-sha", "main"): False},
            ancestors={("recorded-head-sha", "feature/merged"): False},
            on_first_parent=False,
        ),
        source_ref="feature/merged",
        target_branch="main",
        source_has_commits=True,
        recorded_head_sha="recorded-head-sha",
    )

    assert result == "unmerged"


def test_classify_proven_merged_side_branch_allows_recorded_head_patch_equivalence() -> None:
    result = classify_proven_merged_state(
        git=_FakeGit(
            ref_shas={"feature/merged": "branch-tip-sha", "main": "target-tip-sha"},
            tree_shas={"feature/merged": "branch-tree-sha", "main": "target-tree-sha"},
            ahead_count=0,
            net_diff=False,
            patch_present_by_commit={("recorded-head-sha", "main"): True},
            ancestors={("recorded-head-sha", "feature/merged"): False},
            on_first_parent=False,
        ),
        source_ref="feature/merged",
        target_branch="main",
        source_has_commits=True,
        recorded_head_sha="recorded-head-sha",
    )

    assert result == "merged"


def test_classify_proven_merged_missing_recorded_head_without_patch_proof_is_unknown() -> None:
    result = classify_proven_merged_state(
        git=_FakeGit(
            ref_shas={"feature/merged": "branch-tip-sha", "main": "target-tip-sha"},
            ahead_count=0,
            net_diff=None,
            ancestors={("recorded-head-sha", "feature/merged"): False},
        ),
        source_ref="feature/merged",
        target_branch="main",
        source_has_commits=True,
        recorded_head_sha="recorded-head-sha",
    )

    assert result == "unknown"


@pytest.mark.parametrize(
    ("git", "warning_text"),
    [
        (
            _GitWithoutTreeResolveRefs(
                _FakeGit(
                    ref_shas={"feature/merged": "branch-tip-sha", "main": "merge-commit-sha"},
                    ahead_count=0,
                )
            ),
            "diff proof unavailable",
        ),
        (
            _GitTreeResolveRefsRaises(
                _FakeGit(
                    ref_shas={"feature/merged": "branch-tip-sha", "main": "merge-commit-sha"},
                    ahead_count=0,
                )
            ),
            "tree lookup exploded",
        ),
        (
            _FakeGit(
                ref_shas={"feature/merged": "branch-tip-sha", "main": "merge-commit-sha"},
                tree_shas={"feature/merged": None, "main": "shared-tree-sha"},
                ahead_count=0,
            ),
            "diff proof unavailable",
        ),
    ],
)
def test_classify_proven_merged_state_keeps_merged_without_tree_proof(
    git: object,
    warning_text: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level("WARNING"):
        result = classify_proven_merged_state(
            git=git,
            source_ref="feature/merged",
            target_branch="main",
            source_has_commits=True,
            on_warning=lambda message: logger.warning(message),
        )

    assert result == "merged"
    assert warning_text in caplog.text
    assert "instead of classifying a no-work state" in caplog.text


def test_classify_proven_merged_state_returns_redundant_for_task_commits_when_side_branch_probe_fails(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level("WARNING"):
        result = classify_proven_merged_state(
            git=_FakeGit(
                ref_shas={"feature/merged": "branch-tip-sha", "main": "merge-commit-sha"},
                tree_shas={"feature/merged": "shared-tree-sha", "main": "shared-tree-sha"},
                ahead_count=0,
                first_parent_error=RuntimeError("probe exploded"),
            ),
            source_ref="feature/merged",
            target_branch="main",
            source_has_commits=True,
            on_warning=lambda message: logger.warning(message),
        )

    assert result == "redundant"
    assert "Could not probe first-parent membership" in caplog.text


def test_classify_proven_merged_state_returns_empty_for_no_commit_branch_when_probe_fails(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level("WARNING"):
        result = classify_proven_merged_state(
            git=_FakeGit(
                ref_shas={"feature/empty": "branch-tip-sha", "main": "merge-commit-sha"},
                tree_shas={"feature/empty": "shared-tree-sha", "main": "shared-tree-sha"},
                ahead_count=0,
                first_parent_error=RuntimeError("probe exploded"),
            ),
            source_ref="feature/empty",
            target_branch="main",
            source_has_commits=False,
            on_warning=lambda message: logger.warning(message),
        )

    assert result == "empty"
    assert "Could not probe first-parent membership" in caplog.text


def test_classify_proven_merged_state_returns_empty_for_no_commit_branch_when_probe_missing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class _GitWithoutFirstParentProbe:
        def __init__(self) -> None:
            self._delegate = _FakeGit(
                ref_shas={"feature/empty": "branch-tip-sha", "main": "advanced-main-sha"},
                tree_shas={"feature/empty": "shared-tree-sha", "main": "shared-tree-sha"},
                ahead_count=0,
            )

        def __getattr__(self, name: str):
            if name == "is_on_first_parent_history":
                raise AttributeError(name)
            return getattr(self._delegate, name)

    with caplog.at_level("WARNING"):
        result = classify_proven_merged_state(
            git=_GitWithoutFirstParentProbe(),
            source_ref="feature/empty",
            target_branch="main",
            source_has_commits=False,
            on_warning=lambda message: logger.warning(message),
        )

    assert result == "empty"
    assert "Could not probe first-parent membership" in caplog.text


def test_classify_proven_merged_state_returns_redundant_for_task_commits_when_probe_missing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class _GitWithoutFirstParentProbe:
        def __init__(self) -> None:
            self._delegate = _FakeGit(
                ref_shas={"feature/redundant": "branch-tip-sha", "main": "advanced-main-sha"},
                tree_shas={"feature/redundant": "shared-tree-sha", "main": "shared-tree-sha"},
                ahead_count=0,
            )

        def __getattr__(self, name: str):
            if name == "is_on_first_parent_history":
                raise AttributeError(name)
            return getattr(self._delegate, name)

    with caplog.at_level("WARNING"):
        result = classify_proven_merged_state(
            git=_GitWithoutFirstParentProbe(),
            source_ref="feature/redundant",
            target_branch="main",
            source_has_commits=True,
            on_warning=lambda message: logger.warning(message),
        )

    assert result == "redundant"
    assert "Could not probe first-parent membership" in caplog.text


def test_classify_zero_unique_side_branch_missing_recorded_head_stays_unmerged() -> None:
    result = classify_branch_merge_state_for_target(
        git=_FakeGit(
            source_ref="feature/merged",
            ref_shas={"feature/merged": "branch-tip-sha", "main": "merge-commit-sha"},
            tree_shas={"feature/merged": "shared-tree-sha", "main": "shared-tree-sha"},
            ahead_count=0,
            merged=True,
            patch_present_by_commit={("recorded-head-sha", "main"): False},
            ancestors={("recorded-head-sha", "feature/merged"): False},
            on_first_parent=False,
        ),
        source_branch="feature/merged",
        target_branch="main",
        source_has_commits=True,
        recorded_head_sha="recorded-head-sha",
    )

    assert result.state == "unmerged"
    assert result.reason == "recorded-head-has-net-diff"


def test_classify_zero_unique_side_branch_allows_recorded_head_patch_equivalence() -> None:
    result = classify_branch_merge_state_for_target(
        git=_FakeGit(
            source_ref="feature/merged",
            ref_shas={"feature/merged": "branch-tip-sha", "main": "merge-commit-sha"},
            tree_shas={"feature/merged": "shared-tree-sha", "main": "shared-tree-sha"},
            ahead_count=0,
            merged=True,
            patch_present_by_commit={("recorded-head-sha", "main"): True},
            ancestors={("recorded-head-sha", "feature/merged"): False},
            on_first_parent=False,
        ),
        source_branch="feature/merged",
        target_branch="main",
        source_has_commits=True,
        recorded_head_sha="recorded-head-sha",
    )

    assert result.state == "merged"
    assert result.reason == "merged-side-branch-no-unique-commits"


@pytest.mark.parametrize(
    ("ahead_count", "merged", "patch_present", "expected_state", "expected_reason"),
    [
        (2, True, False, "unmerged", "recorded-head-has-net-diff"),
        (2, True, True, "merged", "content-equivalent-with-commits"),
        (None, True, False, "unmerged", "recorded-head-has-net-diff"),
        (None, True, True, "merged", "content-equivalent-with-commits-unverified"),
        (None, True, None, "unknown", "recorded-head-diff-unavailable"),
    ],
)
def test_classify_content_equivalent_terminal_states_respect_recorded_head_guard(
    ahead_count: int | None,
    merged: bool,
    patch_present: bool | None,
    expected_state: str,
    expected_reason: str,
) -> None:
    patch_present_by_commit = {}
    if patch_present is not None:
        patch_present_by_commit[("recorded-head-sha", "main")] = patch_present

    result = classify_branch_merge_state_for_target(
        git=_FakeGit(
            source_ref="feature/content-equivalent",
            ref_shas={"feature/content-equivalent": "branch-tip-sha", "main": "target-tip-sha"},
            tree_shas={"feature/content-equivalent": "branch-tree-sha", "main": "target-tree-sha"},
            ahead_count=ahead_count,
            merged=merged,
            patch_present_by_commit=patch_present_by_commit,
            ancestors={("recorded-head-sha", "feature/content-equivalent"): False},
        ),
        source_branch="feature/content-equivalent",
        target_branch="main",
        source_has_commits=True,
        recorded_head_sha="recorded-head-sha",
    )

    assert result.state == expected_state
    assert result.reason == expected_reason


def test_classify_zero_unique_commits_returns_redundant_when_side_branch_probe_fails(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level("WARNING"):
        result = classify_branch_merge_state_for_target(
            git=_FakeGit(
                source_ref="feature/redundant",
                ref_shas={"feature/redundant": "old-main-sha", "main": "advanced-main-sha"},
                tree_shas={"feature/redundant": "shared-tree-sha", "main": "shared-tree-sha"},
                ahead_count=0,
                merged=True,
                first_parent_error=RuntimeError("probe exploded"),
            ),
            source_branch="feature/redundant",
            target_branch="main",
            source_has_commits=True,
            on_warning=lambda message: logger.warning(message),
        )

    assert result.state == "redundant"
    assert result.reason == "no-unique-commits-with-task-commits"
    assert "Could not probe first-parent membership" in caplog.text


def test_classify_zero_unique_commits_returns_empty_for_no_commit_branch_when_probe_fails(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level("WARNING"):
        result = classify_branch_merge_state_for_target(
            git=_FakeGit(
                source_ref="feature/empty",
                ref_shas={"feature/empty": "old-main-sha", "main": "advanced-main-sha"},
                tree_shas={"feature/empty": "shared-tree-sha", "main": "shared-tree-sha"},
                ahead_count=0,
                merged=True,
                first_parent_error=RuntimeError("probe exploded"),
            ),
            source_branch="feature/empty",
            target_branch="main",
            source_has_commits=False,
            on_warning=lambda message: logger.warning(message),
        )

    assert result.state == "empty"
    assert result.reason == "no-task-commits"
    assert "Could not probe first-parent membership" in caplog.text


def test_classify_zero_unique_commits_returns_empty_for_no_commit_branch_when_probe_missing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class _GitWithoutFirstParentProbe:
        def __init__(self) -> None:
            self._delegate = _FakeGit(
                source_ref="feature/empty",
                ref_shas={"feature/empty": "old-main-sha", "main": "advanced-main-sha"},
                tree_shas={"feature/empty": "shared-tree-sha", "main": "shared-tree-sha"},
                ahead_count=0,
                merged=True,
            )

        def __getattr__(self, name: str):
            if name == "is_on_first_parent_history":
                raise AttributeError(name)
            return getattr(self._delegate, name)

    with caplog.at_level("WARNING"):
        result = classify_branch_merge_state_for_target(
            git=_GitWithoutFirstParentProbe(),
            source_branch="feature/empty",
            target_branch="main",
            source_has_commits=False,
            on_warning=lambda message: logger.warning(message),
        )

    assert result.state == "empty"
    assert result.reason == "no-task-commits"
    assert "Could not probe first-parent membership" in caplog.text


def test_classify_zero_unique_commits_returns_redundant_when_side_branch_probe_missing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class _GitWithoutFirstParentProbe:
        def __init__(self) -> None:
            self._delegate = _FakeGit(
                source_ref="feature/redundant",
                ref_shas={"feature/redundant": "old-main-sha", "main": "advanced-main-sha"},
                tree_shas={"feature/redundant": "shared-tree-sha", "main": "shared-tree-sha"},
                ahead_count=0,
                merged=True,
            )

        def __getattr__(self, name: str):
            if name == "is_on_first_parent_history":
                raise AttributeError(name)
            return getattr(self._delegate, name)

    with caplog.at_level("WARNING"):
        result = classify_branch_merge_state_for_target(
            git=_GitWithoutFirstParentProbe(),
            source_branch="feature/redundant",
            target_branch="main",
            source_has_commits=True,
            on_warning=lambda message: logger.warning(message),
        )

    assert result.state == "redundant"
    assert result.reason == "no-unique-commits-with-task-commits"
    assert "Could not probe first-parent membership" in caplog.text


def test_resolve_task_merge_state_stale_empty_branch_is_empty_not_merged(
    tmp_path: Path,
) -> None:
    # B1 regression at the resolve_task_merge_state_for_target boundary.
    store = SqliteTaskStore(tmp_path / "test.db")
    task = store.add("Never-worked branch", task_type="implement")
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.has_commits = False
    task.branch = "feature/empty"
    task.merge_status = "merged"
    store.update(task)

    state = resolve_task_merge_state_for_target(
        store=store,
        task=task,
        git=_FakeGit(
            source_ref=task.branch,
            ref_shas={task.branch: "old-main-sha", "main": "advanced-main-sha"},
            ahead_count=0,
            merged=True,
            net_diff=False,
            on_first_parent=True,
        ),
        target_branch="main",
    )

    assert state == "empty"


def test_resolve_task_merge_state_falls_back_to_persisted_empty_when_source_ref_missing(
    tmp_path: Path,
) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    task = store.add("No-op branch", task_type="implement")
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.has_commits = True
    task.branch = "feature/missing"
    store.update(task)
    assert task.id is not None

    unit = store.create_merge_unit(
        source_branch=task.branch,
        target_branch="main",
        owner_task_id=task.id,
        state="empty",
    )
    store.attach_task_to_merge_unit(task.id, unit.id, "owner")
    store.set_merge_unit_state(unit.id, "empty")

    refreshed = store.get(task.id)
    assert refreshed is not None
    state = resolve_task_merge_state_for_target(
        store=store,
        task=refreshed,
        git=_FakeGit(source_ref=None, ref_shas={"main": "target-sha"}),
        target_branch="main",
    )

    assert state == "empty"


def test_resolve_task_merge_state_falls_back_to_persisted_redundant_when_source_ref_missing(
    tmp_path: Path,
) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    task = store.add("No-op branch with commits", task_type="implement")
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.has_commits = True
    task.branch = "feature/missing"
    store.update(task)
    assert task.id is not None

    unit = store.create_merge_unit(
        source_branch=task.branch,
        target_branch="main",
        owner_task_id=task.id,
        state="redundant",
    )
    store.attach_task_to_merge_unit(task.id, unit.id, "owner")
    store.set_merge_unit_state(unit.id, "redundant")

    refreshed = store.get(task.id)
    assert refreshed is not None
    state = resolve_task_merge_state_for_target(
        store=store,
        task=refreshed,
        git=_FakeGit(source_ref=None, ref_shas={"main": "target-sha"}),
        target_branch="main",
    )

    assert state == "redundant"


@pytest.mark.parametrize("persisted_state", ["empty", "redundant"])
def test_resolve_task_merge_state_reclassifies_persisted_no_work_with_live_net_diff(
    tmp_path: Path,
    persisted_state: str,
) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    task = store.add("Stranded branch", task_type="implement")
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.has_commits = True
    task.branch = "feature/stranded"
    store.update(task)
    assert task.id is not None

    unit = store.create_merge_unit(
        source_branch=task.branch,
        target_branch="main",
        owner_task_id=task.id,
        state=persisted_state,
    )
    store.attach_task_to_merge_unit(task.id, unit.id, "owner")
    store.set_merge_unit_state(unit.id, persisted_state)

    refreshed = store.get(task.id)
    assert refreshed is not None
    state = resolve_task_merge_state_for_target(
        store=store,
        task=refreshed,
        git=_FakeGit(
            source_ref=task.branch,
            ref_shas={task.branch: "branch-tip-sha", "main": "target-tip-sha"},
            ahead_count=1,
            merged=False,
            net_diff=True,
        ),
        target_branch="main",
    )

    assert state == "unmerged"


def test_resolve_task_merge_state_uses_recorded_head_when_source_ref_is_stale_ancestor(
    tmp_path: Path,
) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    task = store.add("Recorded head diff must win", task_type="implement")
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.has_commits = True
    task.branch = "feature/false-redundant"
    store.update(task)
    assert task.id is not None

    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    store.refresh_merge_unit_head(unit.id, head_sha="recorded-head-sha")
    store.set_merge_unit_state(unit.id, "redundant")

    refreshed = store.get(task.id)
    assert refreshed is not None
    state = resolve_task_merge_state_for_target(
        store=store,
        task=refreshed,
        git=_FakeGit(
            source_ref="origin/feature/false-redundant",
            ref_shas={"origin/feature/false-redundant": "base-sha", "main": "target-sha"},
            tree_shas={"origin/feature/false-redundant": "shared-tree-sha", "main": "shared-tree-sha"},
            ahead_count=0,
            merged=True,
            net_diff_by_ref={
                "origin/feature/false-redundant": False,
            },
            patch_present_by_commit={("recorded-head-sha", "main"): False},
            ancestors={("recorded-head-sha", "origin/feature/false-redundant"): False},
            on_first_parent=True,
        ),
        target_branch="main",
    )

    assert state == "unmerged"


def test_resolve_task_merge_state_relabels_legacy_empty_with_task_commits_when_source_is_inspectable(
    tmp_path: Path,
) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    task = store.add("Legacy empty branch with commits", task_type="implement")
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.has_commits = True
    task.branch = "feature/legacy-empty"
    store.update(task)
    assert task.id is not None

    unit = store.create_merge_unit(
        source_branch=task.branch,
        target_branch="main",
        owner_task_id=task.id,
        state="empty",
    )
    store.attach_task_to_merge_unit(task.id, unit.id, "owner")
    store.set_merge_unit_state(unit.id, "empty")

    refreshed = store.get(task.id)
    assert refreshed is not None
    state = resolve_task_merge_state_for_target(
        store=store,
        task=refreshed,
        git=_FakeGit(
            source_ref=task.branch,
            ref_shas={task.branch: "old-main-sha", "main": "advanced-main-sha"},
            ahead_count=0,
            merged=True,
            net_diff=False,
            on_first_parent=True,
        ),
        target_branch="main",
    )

    assert state == "redundant"


def test_resolve_task_merge_state_does_not_log_merge_source_warning_side_effect(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    task = store.add("Diverged branch", task_type="implement")
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.has_commits = True
    task.branch = "feature/diverged"
    task.merge_status = "unmerged"
    store.update(task)

    warning = "Could not resolve freshest merge source for branch 'feature/diverged' against 'main': local/origin diverged"

    with caplog.at_level("WARNING"):
        state = resolve_task_merge_state_for_target(
            store=store,
            task=task,
            git=_FakeGit(
                source_ref="origin/feature/diverged",
                merge_source_warning=warning,
                ref_shas={"origin/feature/diverged": "branch-sha", "main": "target-sha"},
                ahead_count=1,
                merged=False,
            ),
            target_branch="main",
        )

    assert state == "unmerged"
    assert warning not in caplog.text


def test_classify_branch_merge_state_returns_unknown_without_persisted_state_for_missing_ref() -> None:
    result = classify_branch_merge_state_for_target(
        git=_FakeGit(source_ref=None, ref_shas={"main": "target-sha"}),
        source_branch="feature/missing",
        target_branch="main",
        persisted_state=None,
    )

    assert result.state == "unknown"
    assert result.reason == "missing-ref"
