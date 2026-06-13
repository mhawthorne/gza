"""Unit tests for merge-state classification helpers."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from gza.db import SqliteTaskStore
from gza.merge_state import classify_branch_merge_state_for_target, resolve_task_merge_state_for_target


class _FakeGit:
    def __init__(
        self,
        *,
        source_ref: str | None = None,
        merge_source_warning: str | None = None,
        ref_shas: dict[str, str | None] | None = None,
        ahead_count: int | None = None,
        merged: bool = False,
        on_first_parent: bool = True,
    ) -> None:
        self._source_ref = source_ref
        self._merge_source_warning = merge_source_warning
        self._ref_shas = ref_shas or {}
        self._ahead_count = ahead_count
        self._merged = merged
        self._on_first_parent = on_first_parent

    def resolve_fresh_merge_source(self, _branch: str):
        from gza.git import ResolvedMergeSourceRef

        return ResolvedMergeSourceRef(self._source_ref, self._merge_source_warning)

    def rev_parse_if_exists(self, ref: str) -> str | None:
        return self._ref_shas.get(ref)

    def count_commits_ahead(self, _branch: str, _target: str) -> int:
        if self._ahead_count is None:
            raise RuntimeError("ahead-count unavailable")
        return self._ahead_count

    def count_commits_ahead_checked(self, _branch: str, _target: str) -> int | None:
        return self._ahead_count

    def is_merged(self, _branch: str, _target: str) -> bool:
        return self._merged

    def is_on_first_parent_history(self, _commit: str, _target: str) -> bool:
        return self._on_first_parent


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


def test_classify_no_ff_merged_side_branch_is_merged() -> None:
    # A genuinely --no-ff-merged branch (real gza behavior when the commit count
    # is below merge_squash_threshold) is also an ancestor with zero unique
    # commits, but its tip is a merged-in second parent, OFF the first-parent
    # mainline. Its commits really landed -> merged.
    result = classify_branch_merge_state_for_target(
        git=_FakeGit(
            source_ref="feature/merged",
            ref_shas={"feature/merged": "branch-tip-sha", "main": "merge-commit-sha"},
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
