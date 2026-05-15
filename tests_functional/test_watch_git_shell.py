"""Functional wrappers for watch tests that require a real git repo."""

from tests.cli.test_watch import _functional_test_execute_merge_action_marks_already_merged_task_without_error


def test_execute_merge_action_marks_already_merged_task_without_error(tmp_path) -> None:
    _functional_test_execute_merge_action_marks_already_merged_task_without_error(tmp_path)
