"""Functional wrappers for execution tests that require real git shell commands."""

from unittest.mock import MagicMock, patch

from tests.cli.test_execution import (
    TestIterateCommand as _UnitTestIterateCommand,
    TestMarkCompletedCommand as _UnitTestMarkCompletedCommand,
    TestReconciliation as _UnitTestReconciliation,
    TestSetStatusCommand as _UnitTestSetStatusCommand,
    _background_rebase_branch_error,
    run_gza,
)


def _iterate_git_runtime():
    mock_git = MagicMock()
    mock_git.current_branch.return_value = "main"
    mock_git.resolve_merge_source_ref.return_value = None
    mock_git.is_merged.return_value = False
    mock_git.can_merge.return_value = True
    return patch("gza.cli.Git", return_value=mock_git)


def test_reconciliation_detects_commits_on_worker_died(tmp_path) -> None:
    _UnitTestReconciliation()._functional_test_reconciliation_detects_commits_on_worker_died(tmp_path)


def test_reconciliation_no_commits_on_worker_died(tmp_path) -> None:
    _UnitTestReconciliation()._functional_test_reconciliation_no_commits_on_worker_died(tmp_path)


def test_dry_run_changes_requested_completed_improve_without_review_clear_creates_closing_review(tmp_path) -> None:
    with _iterate_git_runtime():
        _UnitTestIterateCommand()._functional_test_dry_run_changes_requested_completed_improve_without_review_clear_creates_closing_review(
            tmp_path
        )


def test_dry_run_completed_improve_without_review_clear_starts_from_closing_review(tmp_path) -> None:
    with _iterate_git_runtime():
        _UnitTestIterateCommand()._functional_test_dry_run_completed_improve_without_review_clear_starts_from_closing_review(
            tmp_path
        )


def test_mark_completed_default_verify_git_for_code_tasks(tmp_path) -> None:
    _UnitTestMarkCompletedCommand()._functional_test_mark_completed_default_verify_git_for_code_tasks(tmp_path)


def test_mark_completed_warns_if_not_failed(tmp_path) -> None:
    _UnitTestMarkCompletedCommand()._functional_test_mark_completed_warns_if_not_failed(tmp_path)


def test_mark_completed_errors_if_branch_missing_in_git(tmp_path) -> None:
    _UnitTestMarkCompletedCommand()._functional_test_mark_completed_errors_if_branch_missing_in_git(tmp_path)


def test_mark_completed_with_commits_sets_unmerged(tmp_path) -> None:
    _UnitTestMarkCompletedCommand()._functional_test_mark_completed_with_commits_sets_unmerged(tmp_path)


def test_mark_completed_without_commits_marks_completed(tmp_path) -> None:
    _UnitTestMarkCompletedCommand()._functional_test_mark_completed_without_commits_marks_completed(tmp_path)


def test_mark_completed_failed_task_no_warning(tmp_path) -> None:
    _UnitTestMarkCompletedCommand()._functional_test_mark_completed_failed_task_no_warning(tmp_path)


def test_mark_completed_cleans_up_running_worker(tmp_path) -> None:
    _UnitTestMarkCompletedCommand()._functional_test_mark_completed_cleans_up_running_worker(tmp_path)


def test_mark_completed_does_not_touch_already_completed_worker(tmp_path) -> None:
    _UnitTestMarkCompletedCommand()._functional_test_mark_completed_does_not_touch_already_completed_worker(tmp_path)


def test_advance_skips_dropped_tasks(tmp_path) -> None:
    _UnitTestSetStatusCommand()._functional_test_advance_skips_dropped_tasks(tmp_path)


def test_advance_explicit_completed_descendant_in_dropped_owner_lineage_is_ineligible(tmp_path) -> None:
    _UnitTestSetStatusCommand()._functional_test_advance_explicit_completed_descendant_in_dropped_owner_lineage_is_ineligible(
        tmp_path
    )


def test_background_phase1_validation_errors_write_to_stderr_only_rebase(tmp_path) -> None:
    argv, expected = _background_rebase_branch_error(tmp_path)

    result = run_gza(*argv)

    assert result.returncode == 1
    assert expected in result.stderr
    assert expected not in result.stdout
    assert "Error:" not in result.stdout
