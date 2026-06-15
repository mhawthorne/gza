from gza.db import Task
from gza.operator_state import (
    MOOT_EMPTY_LIFECYCLE_DETAIL,
    MOOT_REDUNDANT_LIFECYCLE_DETAIL,
    effective_no_work_merge_state,
    terminal_no_work_lifecycle_detail,
)


def test_terminal_no_work_lifecycle_detail_distinguishes_empty_and_redundant() -> None:
    assert terminal_no_work_lifecycle_detail("empty") == MOOT_EMPTY_LIFECYCLE_DETAIL
    assert terminal_no_work_lifecycle_detail("redundant") == MOOT_REDUNDANT_LIFECYCLE_DETAIL
    assert terminal_no_work_lifecycle_detail("empty") != terminal_no_work_lifecycle_detail("redundant")
    assert "no work" not in terminal_no_work_lifecycle_detail("redundant")
    assert terminal_no_work_lifecycle_detail("merged") is None


def test_legacy_empty_with_task_commits_is_operator_effective_redundant() -> None:
    task = Task(
        id="gza-legacy",
        prompt="Already present",
        status="completed",
        task_type="implement",
        has_commits=True,
    )

    assert effective_no_work_merge_state(task, "empty") == "redundant"
    assert effective_no_work_merge_state(task, "redundant") == "redundant"


def test_legacy_empty_without_task_commits_stays_empty() -> None:
    task = Task(
        id="gza-empty",
        prompt="No-op",
        status="completed",
        task_type="implement",
        has_commits=False,
    )

    assert effective_no_work_merge_state(task, "empty") == "empty"
