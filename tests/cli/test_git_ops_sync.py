"""Tests for the `gza sync` command."""

import argparse
from datetime import UTC, datetime
from unittest.mock import Mock, patch

from gza.cli.git_ops import cmd_sync
from gza.sync_ops import BranchSyncResult

from .conftest import make_store, setup_config


def _completed_branch_task(store, prompt: str, branch: str):
    task = store.add(prompt, task_type="implement")
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = branch
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)
    return task


def test_sync_explicit_task_id_expands_to_branch_cohort(tmp_path, capsys):
    setup_config(tmp_path)
    store = make_store(tmp_path)
    parent = _completed_branch_task(store, "Parent task", "feature/shared")
    child = store.add("Improve task", task_type="improve")
    child.status = "completed"
    child.completed_at = datetime.now(UTC)
    child.branch = "feature/shared"
    child.has_commits = True
    child.merge_status = "unmerged"
    child.based_on = parent.id
    child.same_branch = True
    store.update(child)

    args = argparse.Namespace(
        project_dir=tmp_path,
        task_ids=[parent.id],
        dry_run=True,
        git_only=True,
        pr_only=False,
        no_fetch=False,
    )

    with (
        patch("gza.cli.git_ops.get_store", return_value=store),
        patch("gza.cli.git_ops.Git", return_value=Mock()),
        patch(
            "gza.cli.git_ops.sync_branch_cohorts",
            return_value=(
                [
                    BranchSyncResult(
                        branch="feature/shared",
                        task_ids=(parent.id, child.id),
                        merge_status="unmerged",
                        reconciled=True,
                    )
                ],
                False,
            ),
        ) as sync_call,
    ):
        rc = cmd_sync(args)

    assert rc == 0
    cohorts = sync_call.call_args.args[2]
    assert len(cohorts) == 1
    assert {task.id for task in cohorts[0].tasks} == {parent.id, child.id}
    output = capsys.readouterr().out
    assert "feature/shared" in output


def test_sync_missing_explicit_task_id_returns_error(tmp_path, capsys):
    setup_config(tmp_path)
    store = make_store(tmp_path)

    args = argparse.Namespace(
        project_dir=tmp_path,
        task_ids=["testproject-999"],
        dry_run=False,
        git_only=True,
        pr_only=False,
        no_fetch=False,
    )

    with (
        patch("gza.cli.git_ops.get_store", return_value=store),
        patch("gza.cli.git_ops.Git", return_value=Mock()),
    ):
        rc = cmd_sync(args)

    assert rc == 1
    output = capsys.readouterr().out
    assert "not found" in output
    assert "Synced 0 branch(es), skipped 0, errors 1." in output


def test_sync_mixed_valid_and_missing_explicit_task_ids_report_accurate_totals(tmp_path, capsys):
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = _completed_branch_task(store, "Valid task", "feature/valid")

    args = argparse.Namespace(
        project_dir=tmp_path,
        task_ids=[task.id, "testproject-999"],
        dry_run=False,
        git_only=True,
        pr_only=False,
        no_fetch=False,
    )

    with (
        patch("gza.cli.git_ops.get_store", return_value=store),
        patch("gza.cli.git_ops.Git", return_value=Mock()),
        patch(
            "gza.cli.git_ops.sync_branch_cohorts",
            return_value=(
                [
                    BranchSyncResult(
                        branch="feature/valid",
                        task_ids=(task.id,),
                        merge_status="unmerged",
                        reconciled=True,
                    )
                ],
                False,
            ),
        ),
    ):
        rc = cmd_sync(args)

    assert rc == 1
    output = capsys.readouterr().out
    assert "Task testproject-999 not found" in output
    assert "Synced 1 branch(es), skipped 0, errors 1." in output
