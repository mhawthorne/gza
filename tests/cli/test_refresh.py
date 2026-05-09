"""Tests for `gza refresh` validation and output routing."""

import argparse
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import Mock, patch

from gza.cli.git_ops import cmd_refresh
from gza.sync_ops import BranchSyncResult

from tests.cli.conftest import make_store, setup_config


def _refresh_args(tmp_path: Path, task_id: str | None = None) -> argparse.Namespace:
    return argparse.Namespace(
        project_dir=tmp_path,
        task_id=task_id,
        include_failed=False,
    )


def test_refresh_single_task_not_found(tmp_path: Path, capsys) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    with (
        patch("gza.cli.git_ops.get_store", return_value=store),
        patch("gza.cli.git_ops.Git", return_value=Mock()),
    ):
        rc = cmd_refresh(_refresh_args(tmp_path, "testproject-999999"))

    assert rc == 1
    assert "not found" in capsys.readouterr().out


def test_refresh_single_task_no_branch(tmp_path: Path, capsys) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("No branch task", task_type="explore")
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    store.update(task)

    result = BranchSyncResult(branch="<missing>", task_ids=(task.id,), skipped_reason="no branch")

    with (
        patch("gza.cli.git_ops.get_store", return_value=store),
        patch("gza.cli.git_ops.Git", return_value=Mock()),
        patch("gza.cli.git_ops.refresh_branch_diff_stats", return_value=([result], 1)),
    ):
        rc = cmd_refresh(_refresh_args(tmp_path, str(task.id)))

    assert rc == 0
    assert "skipping" in capsys.readouterr().out


def test_refresh_single_task_branch_missing(tmp_path: Path, capsys) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Task with deleted branch", task_type="implement")
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feat/deleted"
    task.merge_status = "unmerged"
    task.has_commits = True
    store.update(task)

    result = BranchSyncResult(branch="feat/deleted", task_ids=(task.id,), skipped_reason="branch no longer exists")

    with (
        patch("gza.cli.git_ops.get_store", return_value=store),
        patch("gza.cli.git_ops.Git", return_value=Mock()),
        patch("gza.cli.git_ops.refresh_branch_diff_stats", return_value=([result], 1)),
    ):
        rc = cmd_refresh(_refresh_args(tmp_path, str(task.id)))

    assert rc == 0
    assert "skipping" in capsys.readouterr().out


def test_refresh_single_task_mismatched_target_branch_reports_skipped(tmp_path: Path, capsys) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Retargeted task", task_type="implement")
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/retargeted-default"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    result = BranchSyncResult(
        branch=task.branch,
        task_ids=(task.id,),
        skipped_reason="merge unit targets 'main', not requested target 'release'",
    )

    with (
        patch("gza.cli.git_ops.get_store", return_value=store),
        patch("gza.cli.git_ops.Git", return_value=Mock()),
        patch("gza.cli.git_ops.refresh_branch_diff_stats", return_value=([result], 1)),
    ):
        rc = cmd_refresh(_refresh_args(tmp_path, str(task.id)))

    assert rc == 0
    output = capsys.readouterr().out
    assert "merge unit targets 'main', not requested target 'release', skipping" in output
    assert "Refreshed 0 task(s), skipped 1." in output
