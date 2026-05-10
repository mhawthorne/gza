"""Tests for git-oriented CLI helpers."""

import argparse
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from gza.cli.git_ops import _merge_single_task, _run_task_backed_rebase
from gza.config import Config

from .conftest import make_store, run_gza, setup_config


def test_merge_single_task_preflights_conflicts_before_merge(tmp_path, capsys) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Implement conflicting change", task_type="implement")
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/conflicts"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)

    git = SimpleNamespace(
        repo_dir=tmp_path,
        is_merged=MagicMock(return_value=False),
        default_branch=MagicMock(return_value="main"),
        has_changes=MagicMock(return_value=False),
        can_merge=MagicMock(return_value=False),
        merge=MagicMock(),
    )
    args = argparse.Namespace(
        rebase=False,
        squash=False,
        delete=False,
        mark_only=False,
        remote=False,
        resolve=False,
    )
    config = SimpleNamespace(project_dir=tmp_path)

    rc = _merge_single_task(task.id, config, store, git, args, "main")

    assert rc == 1
    git.can_merge.assert_called_once_with("feature/conflicts", "main")
    git.merge.assert_not_called()
    output = capsys.readouterr().out
    assert "has conflicts against 'main'" in output
    assert f"uv run gza rebase {task.id} --resolve" in output


def test_run_task_backed_rebase_refreshes_merge_unit_provenance(tmp_path) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    parent = store.add("Implement feature", task_type="implement")
    store.mark_completed(parent, has_commits=True, branch="feature/rebased", head_sha="head-old", base_sha="base-old")
    assert parent.id is not None
    unit = store.resolve_merge_unit_for_task(parent.id)
    assert unit is not None

    rebase_task = store.add("Rebase feature", task_type="rebase", based_on=parent.id, same_branch=True)
    rebase_task.branch = "feature/rebased"
    store.update(rebase_task)

    repo_git = MagicMock()
    repo_git.current_branch.return_value = "main"
    repo_git.worktree_remove.return_value = None
    repo_git._run.return_value = None

    worktree_git = MagicMock()
    worktree_git.current_branch.return_value = "feature/rebased"
    worktree_git.rebase.return_value = None
    worktree_git.rev_parse_if_exists.side_effect = lambda ref: {
        "feature/rebased": "head-new",
        "main": "base-new",
    }.get(ref)

    with (
        patch("gza.cli.git_ops.Git", side_effect=[repo_git, worktree_git]),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", return_value=None),
        patch("gza.cli.git_ops._branch_has_commits", return_value=True),
    ):
        rc = _run_task_backed_rebase(
            config=config,
            store=store,
            rebase_task=rebase_task,
            branch="feature/rebased",
            target_branch="main",
        )

    assert rc == 0
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.head_sha == "head-new"
    assert refreshed_unit.base_sha == "base-new"


def test_run_task_backed_rebase_surfaces_resolution_warnings_and_preserves_existing_merge_unit_provenance(
    tmp_path, capsys
) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    parent = store.add("Implement feature", task_type="implement")
    store.mark_completed(parent, has_commits=True, branch="feature/rebased", head_sha="head-old", base_sha="base-old")
    assert parent.id is not None
    unit = store.resolve_merge_unit_for_task(parent.id)
    assert unit is not None

    rebase_task = store.add("Rebase feature", task_type="rebase", based_on=parent.id, same_branch=True)
    rebase_task.branch = "feature/rebased"
    store.update(rebase_task)

    repo_git = MagicMock()
    repo_git.current_branch.return_value = "main"
    repo_git.worktree_remove.return_value = None
    repo_git._run.return_value = None

    worktree_git = MagicMock()
    worktree_git.current_branch.return_value = "feature/rebased"
    worktree_git.rebase.return_value = None
    worktree_git.rev_parse_if_exists.side_effect = RuntimeError("boom")

    with (
        patch("gza.cli.git_ops.Git", side_effect=[repo_git, worktree_git]),
        patch("gza.cli.git_ops.cleanup_worktree_for_branch", return_value=None),
        patch("gza.cli.git_ops._branch_has_commits", return_value=True),
    ):
        rc = _run_task_backed_rebase(
            config=config,
            store=store,
            rebase_task=rebase_task,
            branch="feature/rebased",
            target_branch="main",
        )

    assert rc == 0
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.head_sha == "head-old"
    assert refreshed_unit.base_sha == "base-old"
    output = capsys.readouterr()
    assert "unexpected error resolving ref 'feature/rebased': boom" in output.err
    assert "unexpected error resolving ref 'main': boom" in output.err


def test_rebase_background_creator_phase_failure_cleans_up_created_task_and_artifacts(tmp_path: Path) -> None:
    """Background rebase must roll back the created child when startup preparation fails."""

    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl_task = store.add("Implement feature", task_type="implement")
    impl_task.status = "completed"
    impl_task.branch = "test-project/20260129-implement-feature"
    impl_task.completed_at = datetime.now(UTC)
    store.update(impl_task)

    git = SimpleNamespace(
        current_branch=MagicMock(return_value="main"),
        default_branch=MagicMock(return_value="main"),
    )

    with (
        patch("gza.cli.git_ops.Git", return_value=git),
        patch("gza.cli.git_ops._require_default_branch", return_value=True),
        patch("gza.cli._common.prepare_task_startup_phase", side_effect=RuntimeError("creator boom")),
        patch(
            "gza.cli.git_ops._spawn_background_worker",
            side_effect=AssertionError("background worker should not spawn"),
        ),
    ):
        result = run_gza("rebase", str(impl_task.id), "--background", "--project", str(tmp_path))

    assert result.returncode == 1
    assert "creator boom" in result.stderr
    assert store.get_based_on_children(impl_task.id) == []

    logs_dir = tmp_path / ".gza" / "logs"
    if logs_dir.exists():
        assert list(logs_dir.iterdir()) == []

    workers_dir = tmp_path / ".gza" / "workers"
    if workers_dir.exists():
        assert list(workers_dir.iterdir()) == []
