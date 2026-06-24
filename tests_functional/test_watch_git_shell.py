"""Functional tests for watch flows that require a real git repo."""

import os
import shlex
import shutil
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from gza.cli.watch import _run_cycle, _WatchLog
from gza.cli.git_ops import _execute_merge_action, ensure_watch_main_checkout
from gza.config import Config
from tests.cli.conftest import make_store, setup_config

from tests_functional.git_helpers import init_basic_repo, setup_git_repo_with_task_branch


def _install_counting_git_shim(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    real_git = shutil.which("git")
    assert real_git is not None

    counter_path = tmp_path / "git-count.log"
    shim_dir = tmp_path / "bin"
    shim_dir.mkdir()
    shim_path = shim_dir / "git"
    shim_path.write_text(
        "#!/bin/sh\n"
        f"printf '%s\\n' \"$*\" >> {shlex.quote(str(counter_path))}\n"
        f"exec {shlex.quote(real_git)} \"$@\"\n"
    )
    shim_path.chmod(0o755)
    monkeypatch.setenv("PATH", f"{shim_dir}{os.pathsep}{os.environ['PATH']}")
    return counter_path


def _seed_watch_analysis_fixture(tmp_path: Path, *, branch_count: int) -> tuple[object, object]:
    store = make_store(tmp_path)
    git = init_basic_repo(tmp_path)

    for index in range(branch_count):
        branch = f"feature/watch-cache-{index}"
        task = store.add(f"Watch cache task {index}", task_type="implement")
        assert task.id is not None
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = branch
        task.has_commits = True
        task.merge_status = "unmerged"
        store.update(task)

        git._run("checkout", "-b", branch)
        (tmp_path / f"watch-cache-{index}.txt").write_text(f"{index}\n")
        git._run("add", f"watch-cache-{index}.txt")
        git._run("commit", "-m", f"Watch cache commit {index}")
        branch_sha = git.rev_parse("HEAD")
        git._run("checkout", "main")
        git._run("update-ref", f"refs/remotes/origin/{branch}", branch_sha)

    return store, git


def test_execute_merge_action_marks_already_merged_task_without_error(tmp_path) -> None:
    store, git, task, _wt = setup_git_repo_with_task_branch(
        tmp_path,
        "Already merged task",
        "feature/watch-already-merged-success",
    )
    config = Config.load(tmp_path)

    assert task.id is not None
    git._run("merge", "--no-ff", task.branch)
    store.set_merge_status(task.id, "unmerged")

    merge_result = _execute_merge_action(
        config,
        store,
        git,
        task,
        {"type": "merge"},
        target_branch="main",
        current_branch="main",
        already_merged_behavior="mark_merged",
    )

    assert merge_result.rc == 0
    assert merge_result.status == "already_merged"
    refreshed_task = store.get(task.id)
    assert refreshed_task is not None
    assert refreshed_task.merge_status == "merged"


@pytest.mark.functional
def test_watch_checkout_mutation_refreshes_cached_head_reads(tmp_path) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    git = init_basic_repo(tmp_path)

    git._run("checkout", "-b", "feature/watch-cache-refresh")
    (tmp_path / "feature.txt").write_text("feature branch\n")
    git._run("add", "feature.txt")
    git._run("commit", "-m", "Feature commit")
    feature_sha = git.rev_parse("HEAD")
    git._run("checkout", "main")
    main_sha = git.rev_parse("HEAD")

    workspace_git = ensure_watch_main_checkout(config, git, "main")

    with workspace_git.cached():
        assert workspace_git.rev_parse_if_exists("HEAD") == main_sha
        workspace_git.checkout_detached("feature/watch-cache-refresh")
        assert workspace_git.rev_parse_if_exists("HEAD") == feature_sha
        workspace_git.reset_hard("main")
        assert workspace_git.rev_parse_if_exists("HEAD") == main_sha


@pytest.mark.functional
def test_watch_cycle_real_git_dedupes_attention_and_emits_single_task_scoped_repair(tmp_path) -> None:
    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(config_path.read_text() + "advance_create_reviews: false\n")
    store = make_store(tmp_path)
    git = init_basic_repo(tmp_path)

    diverged = store.add("Diverged implementation", task_type="implement")
    assert diverged.id is not None
    diverged.status = "completed"
    diverged.completed_at = datetime.now(UTC)
    diverged.branch = "feature/diverged-watch"
    diverged.has_commits = True
    diverged.merge_status = "unmerged"
    store.update(diverged)

    attention = store.add("Awaiting review creation", task_type="implement")
    assert attention.id is not None
    attention.status = "completed"
    attention.completed_at = datetime.now(UTC)
    attention.branch = "feature/manual-review-watch"
    attention.has_commits = True
    attention.merge_status = "unmerged"
    store.update(attention)

    git._run("checkout", "-b", attention.branch)
    (tmp_path / "manual-review.txt").write_text("manual review branch\n")
    git._run("add", "manual-review.txt")
    git._run("commit", "-m", "Manual review branch")
    git._run("checkout", "main")

    git._run("checkout", "-b", diverged.branch)
    base_sha = git._run("rev-parse", "HEAD").stdout.strip()
    (tmp_path / "local-diverged.txt").write_text("local only\n")
    git._run("add", "local-diverged.txt")
    git._run("commit", "-m", "Local diverged commit")
    git._run("checkout", "-B", "tmp-diverged-remote", base_sha)
    (tmp_path / "remote-diverged.txt").write_text("remote only\n")
    git._run("add", "remote-diverged.txt")
    git._run("commit", "-m", "Remote diverged commit")
    remote_sha = git._run("rev-parse", "HEAD").stdout.strip()
    git._run("update-ref", f"refs/remotes/origin/{diverged.branch}", remote_sha)
    git._run("checkout", "main")
    git._run("branch", "-D", "tmp-diverged-remote")

    config = Config.load(tmp_path)
    log_path = tmp_path / ".gza" / "watch.log"
    log = _WatchLog(log_path, quiet=True)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli.watch.Git", return_value=git),
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=10,
            dry_run=False,
            log=log,
        )

    assert result.work_done is False
    text = log_path.read_text()
    manual_attention_lines = [
        line
        for line in text.splitlines()
        if attention.id in line and "reason=review-needs-manual-creation" in line
    ]
    assert len(manual_attention_lines) == 2
    assert sum(" ATTENTION " in line for line in manual_attention_lines) == 1
    assert "Needs attention (1 task):" in text
    assert "Could not resolve freshest merge source" not in text
    repair_lines = [
        line
        for line in text.splitlines()
        if " REPAIR " in line and diverged.id in line and diverged.branch in line
    ]
    assert len(repair_lines) == 1


@pytest.mark.functional
def test_watch_cycle_analysis_uses_cached_git_reads_for_repeated_branch_probes(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_config(tmp_path)
    branch_count = 8
    store, _git = _seed_watch_analysis_fixture(tmp_path, branch_count=branch_count)
    config = Config.load(tmp_path)
    log = _WatchLog(tmp_path / ".gza" / "watch.log", quiet=True)
    counter_path = _install_counting_git_shim(tmp_path, monkeypatch)
    counter_path.write_text("")

    no_verify_result = SimpleNamespace(
        merges_halted=False,
        state=SimpleNamespace(task=SimpleNamespace(id=None), alert_message=None),
    )

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli._common.reconcile_dead_pending_recovery_tasks"),
        patch("gza.cli.watch.check_main_integration_verify", return_value=no_verify_result),
    ):
        result = _run_cycle(
            config=config,
            store=store,
            batch=1,
            max_iterations=1,
            dry_run=True,
            log=log,
        )

    git_invocations = len(counter_path.read_text().splitlines())
    max_allowed_invocations = 80 + (20 * branch_count)

    assert result.work_done is True
    # Keep this budget loose enough for unrelated fixed-cost probes while still
    # failing if watch falls back to roughly-per-branch uncached git reads.
    assert git_invocations < max_allowed_invocations
