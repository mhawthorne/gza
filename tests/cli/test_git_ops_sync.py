"""Tests for the `gza sync` command."""

import argparse
from datetime import UTC, datetime, timedelta
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


def test_sync_without_task_ids_uses_default_branch_cohort_builder(tmp_path, capsys):
    setup_config(tmp_path)
    store = make_store(tmp_path)
    cohort = Mock()
    cohort.branch = "feature/default-sync"
    cohort.code_tasks = ()

    args = argparse.Namespace(
        project_dir=tmp_path,
        task_ids=[],
        dry_run=True,
        git_only=True,
        pr_only=False,
        no_fetch=False,
    )

    with (
        patch("gza.cli.git_ops.get_store", return_value=store),
        patch("gza.cli.git_ops.Git", return_value=Mock()),
        patch("gza.cli.git_ops.build_default_branch_cohorts", return_value=[cohort]) as build_default,
        patch(
            "gza.cli.git_ops.sync_branch_cohorts",
            return_value=([BranchSyncResult(branch="feature/default-sync", task_ids=(), reconciled=True)], False),
        ) as sync_call,
    ):
        rc = cmd_sync(args)

    assert rc == 0
    build_default.assert_called_once_with(store)
    assert sync_call.call_args.args[2] == [cohort]
    output = capsys.readouterr().out
    assert "feature/default-sync" in output


def test_sync_git_only_reports_merged_when_origin_default_ref_proves_remote_merge(tmp_path, capsys):
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = _completed_branch_task(store, "Remote-only merge", "feature/remote-only-merge")

    git = Mock()
    git.default_branch.return_value = "main"
    git.fetch.return_value = None
    git.ref_exists.return_value = True
    git.branch_exists.return_value = True
    git.get_diff_numstat.return_value = "2\t1\tfeature.txt\n"

    def _is_merged(branch, into):
        return into == "origin/main"

    git.is_merged.side_effect = _is_merged

    args = argparse.Namespace(
        project_dir=tmp_path,
        task_ids=[task.id],
        dry_run=False,
        git_only=True,
        pr_only=False,
        no_fetch=False,
    )

    with (
        patch("gza.cli.git_ops.get_store", return_value=store),
        patch("gza.cli.git_ops.Git", return_value=git),
    ):
        rc = cmd_sync(args)

    assert rc == 0
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.merge_status == "merged"
    output = capsys.readouterr().out
    assert "feature/remote-only-merge | merge=merged" in output
    assert "marked merged" in output


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


def test_sync_reports_live_progress_messages(tmp_path, capsys):
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = _completed_branch_task(store, "Valid task", "feature/progress")

    args = argparse.Namespace(
        project_dir=tmp_path,
        task_ids=[task.id],
        dry_run=False,
        git_only=True,
        pr_only=False,
        no_fetch=False,
    )

    def _fake_sync(*_args, **kwargs):
        progress = kwargs["progress"]
        progress("Fetching origin")
        progress("[1/1] feature/progress")
        return (
            [
                BranchSyncResult(
                    branch="feature/progress",
                    task_ids=(task.id,),
                    merge_status="unmerged",
                    reconciled=True,
                )
            ],
            False,
        )

    with (
        patch("gza.cli.git_ops.get_store", return_value=store),
        patch("gza.cli.git_ops.Git", return_value=Mock()),
        patch("gza.cli.git_ops.sync_branch_cohorts", side_effect=_fake_sync),
    ):
        rc = cmd_sync(args)

    assert rc == 0
    output = capsys.readouterr().out
    assert "[sync] Fetching origin" in output
    assert "[sync] [1/1] feature/progress" in output


def test_sync_reports_when_default_candidates_are_cache_filtered(tmp_path, capsys):
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = _completed_branch_task(store, "Cached task", "feature/cached")
    task.completed_at = datetime.now(UTC) - timedelta(minutes=10)
    task.sync_last_synced_at = datetime.now(UTC) - timedelta(seconds=10)
    store.update(task)

    args = argparse.Namespace(
        project_dir=tmp_path,
        task_ids=[],
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

    assert rc == 0
    output = capsys.readouterr().out
    assert "default sync cache is still warm" in output
