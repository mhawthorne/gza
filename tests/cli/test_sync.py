"""Tests for the `gza sync` command."""

import argparse
from datetime import UTC, datetime, timedelta
from unittest.mock import Mock, patch

from gza.cli.git_ops import cmd_sync
from gza.sync_ops import BranchSyncResult
from tests.cli.conftest import make_store, setup_config


def _completed_branch_task(store, prompt: str, branch: str):
    task = store.add(prompt, task_type="implement")
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = branch
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)
    return task












def test_sync_reports_unexpected_ref_resolution_warning_without_clearing_stored_shas(tmp_path, capsys):
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = _completed_branch_task(store, "Resolution warning", "feature/resolution-warning")
    assert task.id is not None
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    store.refresh_merge_unit_head(unit.id, "head-old-123", "base-old-456")

    git = Mock()
    git.default_branch.return_value = "main"
    git.branch_exists.return_value = True
    git.is_merged.return_value = False
    git.get_diff_numstat.return_value = "2\t1\tfeature.txt\n"
    git.rev_parse_if_exists.side_effect = RuntimeError("boom")

    args = argparse.Namespace(
        project_dir=tmp_path,
        task_ids=[task.id],
        dry_run=False,
        git_only=True,
        pr_only=False,
        no_fetch=True,
    )

    with (
        patch("gza.cli.git_ops.get_store", return_value=store),
        patch("gza.cli.git_ops.Git", return_value=git),
    ):
        rc = cmd_sync(args)

    assert rc == 0
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.head_sha == "head-old-123"
    assert refreshed_unit.base_sha == "base-old-456"
    output = capsys.readouterr().out
    assert "unexpected error resolving ref 'feature/resolution-warning': boom" in output
    assert "unexpected error resolving ref 'main': boom" in output


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


def test_sync_reports_when_default_candidates_are_cache_filtered(tmp_path, capsys, monkeypatch):
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = _completed_branch_task(store, "Cached task", "feature/cached")

    class FrozenDateTime(datetime):
        current = datetime(2026, 6, 24, 12, 13, tzinfo=UTC)

        @classmethod
        def now(cls, tz=None):  # type: ignore[override]
            assert tz is not None
            return cls.current.astimezone(tz)

    # `get_sync_candidates` decides cache-filtering via `datetime.now()` inside
    # gza.db; freeze that clock and derive the fixture timestamps from the
    # same fixed instant so both sides move together under a shifted clock.
    monkeypatch.setattr("gza.db.datetime", FrozenDateTime)

    task.completed_at = FrozenDateTime.current - timedelta(minutes=10)
    task.sync_last_synced_at = FrozenDateTime.current - timedelta(seconds=10)
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




def test_sync_all_mismatched_targets_returns_success_without_fetch_or_github(tmp_path, capsys):
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = _completed_branch_task(store, "Retargeted task", "feature/retargeted-default")
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    assert unit.target_branch == "main"

    git = Mock()
    git.default_branch.return_value = "release"

    args = argparse.Namespace(
        project_dir=tmp_path,
        task_ids=[task.id],
        dry_run=False,
        git_only=False,
        pr_only=False,
        no_fetch=False,
    )

    with (
        patch("gza.cli.git_ops.get_store", return_value=store),
        patch("gza.cli.git_ops.Git", return_value=git),
        patch("gza.sync_ops.GitHub") as github_cls,
    ):
        rc = cmd_sync(args)

    assert rc == 0
    git.fetch.assert_not_called()
    github_cls.assert_not_called()
    output = capsys.readouterr().out
    assert f"{task.id}: skipped (merge unit targets 'main', not requested target 'release')" in output
    assert "Synced 0 branch(es), skipped 1, errors 0." in output
