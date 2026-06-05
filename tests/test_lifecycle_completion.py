from gza.db import Task
from gza.lifecycle_completion import (
    auto_review_skip_message_for_completed_code_task,
    should_auto_create_review_for_completed_code_task,
    task_is_complete_for_lifecycle,
)


def test_completed_task_with_empty_merge_state_is_complete() -> None:
    task = Task(
        id="gza-1",
        prompt="Implement empty branch outcome",
        status="completed",
        task_type="implement",
        has_commits=True,
    )

    assert task_is_complete_for_lifecycle(task, merge_state="empty") is True


def test_completed_implement_without_task_commits_skips_auto_review() -> None:
    task = Task(
        id="gza-2",
        prompt="Implement no-op",
        status="completed",
        task_type="implement",
        has_commits=False,
    )

    assert should_auto_create_review_for_completed_code_task(task) is False
    assert auto_review_skip_message_for_completed_code_task(task) == (
        "Skipping auto-review for gza-2: completed with no task commits; nothing to review."
    )


def test_completed_empty_implement_skips_auto_review_without_claiming_no_task_commits() -> None:
    task = Task(
        id="gza-3",
        prompt="Implement already landed",
        status="completed",
        task_type="implement",
        has_commits=True,
    )

    assert should_auto_create_review_for_completed_code_task(task, merge_state="empty") is False
    message = auto_review_skip_message_for_completed_code_task(task, merge_state="empty")
    assert message == (
        "Skipping auto-review for gza-3: no unique commits vs target (nothing to review)."
    )
    assert message is not None
    assert "no task commits" not in message
