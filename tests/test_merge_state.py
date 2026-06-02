from pathlib import Path
from types import SimpleNamespace

import pytest

from gza.db import SqliteTaskStore
from gza.merge_state import resolve_task_merge_state_for_target


def test_resolve_task_merge_state_classifies_zero_commit_branch_as_empty(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    task = store.add("Implement empty branch", task_type="implement")
    store.mark_completed(task, has_commits=True, branch="feature/empty-branch")
    assert task.id is not None

    unit = store.resolve_merge_unit_for_task(task.id)
    assert unit is not None
    store.set_merge_unit_state(unit.id, "merged")

    refreshed = store.get(task.id)
    assert refreshed is not None

    git = SimpleNamespace(
        resolve_fresh_merge_source=lambda branch: ("feature/empty-branch", None),
        is_merged=lambda source, target: True,
        count_commits_ahead_checked=lambda source, target: 0,
    )

    assert (
        resolve_task_merge_state_for_target(
            store=store,
            task=refreshed,
            git=git,
            target_branch="main",
        )
        == "empty"
    )


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

    git = SimpleNamespace(
        resolve_fresh_merge_source=lambda branch: ("feature/merged-branch", None),
        is_merged=lambda source, target: True,
        count_commits_ahead_checked=lambda source, target: None,
    )

    with caplog.at_level("WARNING"):
        result = resolve_task_merge_state_for_target(
            store=store,
            task=refreshed,
            git=git,
            target_branch="main",
        )

    assert result == "merged"
    assert "keeping merge state at 'merged' instead of classifying 'empty'" in caplog.text
