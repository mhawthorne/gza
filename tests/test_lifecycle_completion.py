from gza.db import Task
from gza.lifecycle_completion import task_is_complete_for_lifecycle


def test_completed_task_with_empty_merge_state_is_complete() -> None:
    task = Task(
        id="gza-1",
        prompt="Implement empty branch outcome",
        status="completed",
        task_type="implement",
        has_commits=True,
    )

    assert task_is_complete_for_lifecycle(task, merge_state="empty") is True
