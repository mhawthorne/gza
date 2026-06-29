"""Functional tests for watch flows that require a real git repo."""

import argparse
import os
import shlex
import signal
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from gza.cli.watch import _CycleResult, _run_cycle, _WatchLog, cmd_watch
from gza.cli.git_ops import _execute_merge_action, ensure_watch_main_checkout
from gza.config import Config
from gza.git import Git, GitError
from gza.git_health import check_git_health as real_check_git_health, current_git_health_alert
from tests.cli.conftest import make_store, setup_config

from tests_functional.git_helpers import init_basic_repo, setup_git_repo_with_task_branch


def _install_counting_git_shim(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    counter_path = tmp_path / "git-count.log"
    shim_dir = tmp_path / "bin"
    shim_dir.mkdir()
    shim_path = shim_dir / "git"
    shim_path.write_text(
        "#!/bin/sh\n"
        f"printf '%s\\n' \"$*\" >> {shlex.quote(str(counter_path))}\n"
        f"exec {shlex.quote(Git._git_executable())} \"$@\"\n"
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


def _mark_task_running(store, task_id: str) -> None:
    task = store.get(task_id)
    assert task is not None
    task.status = "in_progress"
    task.started_at = datetime.now(UTC)
    task.running_pid = os.getpid()
    store.update(task)


def _setup_linked_worktree_watch_project(tmp_path: Path) -> tuple[object, Path, Path]:
    """Create a temp project that runs from a real linked git worktree."""
    canonical_repo = tmp_path / "canonical"
    canonical_repo.mkdir()
    git = init_basic_repo(canonical_repo)

    git._run("checkout", "-b", "feature/watch-health-linked")
    (canonical_repo / "feature.txt").write_text("linked worktree fixture\n")
    git._run("add", "feature.txt")
    git._run("commit", "-m", "Add linked worktree fixture")
    git._run("checkout", "main")

    project_dir = tmp_path / "worktrees" / "watch-health-project"
    project_dir.parent.mkdir(parents=True, exist_ok=True)
    git._run("worktree", "add", str(project_dir), "feature/watch-health-linked")

    setup_config(project_dir)
    config_path = project_dir / "gza.yaml"
    config_path.write_text(config_path.read_text() + "use_docker: false\n")
    store = make_store(project_dir)
    commondir_path = canonical_repo / ".git" / "worktrees" / project_dir.name / "commondir"
    assert commondir_path.exists()
    return store, project_dir, commondir_path


def _corrupt_linked_worktree_commondir_for_probe_failure(
    project_dir: Path,
    commondir_path: Path,
):
    """Write a container-style commondir value that makes real git probing fail.

    The exact `/gza-git/common` incident path can be a live mount inside agent
    environments, which aliases this linked worktree into the wrong repository
    instead of failing. Keep the same `/gza-git/common` failure class but fall
    back to an obviously missing descendant so the functional regression remains
    deterministic.
    """
    project_git = Git(project_dir)
    for candidate in ("/gza-git/common", "/gza-git/common/missing"):
        commondir_path.write_text(candidate)
        probe = project_git._run("worktree", "list", "--porcelain", check=False)
        if probe.returncode != 0:
            return candidate, probe
    pytest.fail("corrupt linked-worktree commondir did not break `git worktree list --porcelain`")


def _assert_corrupt_linked_worktree_probe_failed(corrupt_value: str, probe) -> None:
    assert corrupt_value.startswith("/gza-git/common")
    assert probe.returncode != 0
    assert any(
        fragment in probe.stderr
        for fragment in (
            "not a git repository",
            "invalid commondir",
            "Invalid path '/gza-git'",
        )
    ), probe.stderr


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


@pytest.mark.functional
def test_cmd_watch_global_git_health_halt_avoids_task_cascade_and_resumes_dispatch(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    init_basic_repo(tmp_path)

    first_pending = store.add("First pending plan", task_type="plan")
    second_pending = store.add("Second pending plan", task_type="plan")
    assert first_pending.id is not None
    assert second_pending.id is not None
    seeded_task_ids = {first_pending.id, second_pending.id}

    args = argparse.Namespace(
        project_dir=tmp_path,
        batch=1,
        poll=1,
        max_idle=10,
        max_iterations=10,
        dry_run=False,
        quiet=True,
        yes=True,
        resumed_reexec=False,
        group=None,
    )

    signal_handlers: dict[signal.Signals, object] = {}
    sleep_calls: list[int] = []
    probe_failures = [
        SimpleNamespace(
            worktree_list=MagicMock(
                side_effect=GitError(
                    "fatal: invalid commondir /gza-git/common\n"
                    "fatal: not a git repository: /workspace/.git/worktrees/broken"
                )
            )
        ),
        SimpleNamespace(worktree_list=MagicMock(return_value=[{"path": str(tmp_path)}])),
        SimpleNamespace(worktree_list=MagicMock(return_value=[{"path": str(tmp_path)}])),
    ]

    def register_signal(sig: signal.Signals, handler: object) -> object:
        signal_handlers[sig] = handler
        return object()

    def fake_sleep(seconds: int, _stop_requested) -> None:
        sleep_calls.append(seconds)
        if len(sleep_calls) == 2:
            handler = signal_handlers[signal.SIGTERM]
            assert callable(handler)
            handler(signal.SIGTERM, None)

    def probe_side_effect(store_arg, _git, persist=True):
        probe_git = probe_failures.pop(0)
        return real_check_git_health(store_arg, probe_git, persist=persist)

    def fake_spawn_background_worker(
        _args,
        _config,
        *,
        task_id: str,
        **_kwargs,
    ) -> int:
        _mark_task_running(store, task_id)
        return 0

    def wait_for_dispatch_start(**kwargs):
        task = kwargs["store"].get(str(kwargs["task_id"]))
        return True, f"task {kwargs['task_id']} reached running state", task

    def run_real_cycle_once_then_idle(**kwargs):
        if not hasattr(run_real_cycle_once_then_idle, "seen"):
            run_real_cycle_once_then_idle.seen = True  # type: ignore[attr-defined]
            return _run_cycle(**kwargs)
        return _CycleResult(work_done=False, running=1, pending=1)

    with (
        patch("gza.cli._common.reconcile_in_progress_tasks"),
        patch("gza.cli._common.prune_terminal_dead_workers"),
        patch("gza.cli._common.reconcile_dead_pending_recovery_tasks"),
        patch("gza.cli.watch._system_can_run_tasks", return_value=True),
        patch("gza.cli.watch.check_git_health", side_effect=probe_side_effect),
        patch("gza.cli.watch._spawn_background_worker", side_effect=fake_spawn_background_worker) as spawn_worker,
        patch("gza.cli.watch._wait_for_watch_dispatch_start", side_effect=wait_for_dispatch_start),
        patch("gza.cli.watch._run_cycle", side_effect=run_real_cycle_once_then_idle),
        patch("gza.cli.watch.signal.signal", side_effect=register_signal),
        patch("gza.cli.watch._sleep_interruptibly", side_effect=fake_sleep),
    ):
        rc = cmd_watch(args)

    assert rc == 128 + signal.SIGTERM
    assert sleep_calls == [1, 1]
    assert spawn_worker.call_count == 1
    assert spawn_worker.call_args.kwargs["task_id"] == first_pending.id

    refreshed_first = store.get(first_pending.id)
    refreshed_second = store.get(second_pending.id)
    assert refreshed_first is not None
    assert refreshed_second is not None
    assert refreshed_first.status == "in_progress"
    assert refreshed_first.failure_reason is None
    assert refreshed_second.status == "pending"
    assert refreshed_second.failure_reason is None
    assert current_git_health_alert(store) is None

    derived_children = [task for task in store.get_all() if task.based_on in seeded_task_ids]
    assert derived_children == []
    assert not any(
        task.id in seeded_task_ids and task.status == "failed" and task.failure_reason == "GIT_ERROR"
        for task in store.get_all()
    )

    log_text = (tmp_path / ".gza" / "watch.log").read_text()
    assert log_text.count(" ATTENTION ") == 1
    assert log_text.count("git worktree health RED - dispatch halted") == 1
    assert "Inspect `.git/worktrees/*/commondir` for container-only paths such as `/gza-git/common`" in log_text
    assert "RESUME    git worktree health restored - resuming dispatch" in log_text
    assert "START" in log_text
    assert "GIT_ERROR" not in log_text


@pytest.mark.functional
def test_watch_dry_run_halts_for_corrupt_linked_worktree_metadata_and_clears_after_repair(
    tmp_path: Path,
) -> None:
    store, project_dir, commondir_path = _setup_linked_worktree_watch_project(tmp_path)
    pending = store.add("Pending plan after linked-worktree repair", task_type="plan")
    assert pending.id is not None
    original_commondir = commondir_path.read_text().strip()
    seeded_ids = {task.id for task in store.get_all()}
    seeded_statuses = {task.id: task.status for task in store.get_all()}
    log_path = project_dir / ".gza" / "watch.log"

    corrupt_value, broken_probe = _corrupt_linked_worktree_commondir_for_probe_failure(
        project_dir,
        commondir_path,
    )
    _assert_corrupt_linked_worktree_probe_failed(corrupt_value, broken_probe)

    args = argparse.Namespace(
        project_dir=project_dir,
        batch=1,
        poll=1,
        max_idle=10,
        max_iterations=10,
        dry_run=True,
        quiet=True,
        yes=True,
        resumed_reexec=False,
        dispatch_mode=None,
        recovery_slots=1,
        max_resume_attempts=1,
        group=None,
    )

    if log_path.exists():
        log_path.unlink()

    with patch("gza.cli.watch.signal.signal", side_effect=lambda *_args: object()):
        held_rc = cmd_watch(args)

    assert held_rc == 0
    held_tasks = store.get_all()
    assert {task.id for task in held_tasks} == seeded_ids
    assert {task.id: task.status for task in held_tasks} == seeded_statuses
    assert not any(task.status == "in_progress" for task in held_tasks)
    assert not any(
        task.task_type in {"review", "improve", "rebase", "resume", "retry"}
        and task.based_on in seeded_ids
        for task in held_tasks
    )
    refreshed_pending = store.get(pending.id)
    assert refreshed_pending is not None
    assert refreshed_pending.status == "pending"
    assert refreshed_pending.log_file is None
    assert current_git_health_alert(store) is None

    held_log = log_path.read_text()
    assert held_log.count(" HOLD ") == 1
    assert held_log.count(" ATTENTION ") == 1
    assert held_log.count("git worktree health RED - dispatch halted") == 1
    assert "`git worktree list` failed (exit 128)" in held_log
    assert "Inspect `.git/worktrees/*/commondir`" in held_log
    assert "/gza-git/common" in held_log

    commondir_path.write_text(original_commondir)
    repaired_probe = Git(project_dir)._run("worktree", "list", "--porcelain", check=False)
    assert repaired_probe.returncode == 0

    signal_handlers: dict[signal.Signals, object] = {}

    def register_signal(sig: signal.Signals, handler: object) -> object:
        signal_handlers[sig] = handler
        return object()

    def fake_sleep(_seconds: int, _stop_requested) -> None:
        handler = signal_handlers[signal.SIGTERM]
        assert callable(handler)
        handler(signal.SIGTERM, None)

    log_path.unlink(missing_ok=True)
    with (
        patch("gza.cli.watch.signal.signal", side_effect=register_signal),
        patch("gza.cli.watch._sleep_interruptibly", side_effect=fake_sleep),
    ):
        resumed_rc = cmd_watch(args)

    assert resumed_rc == 128 + signal.SIGTERM
    resumed_tasks = store.get_all()
    assert {task.id for task in resumed_tasks} == seeded_ids
    resumed_pending = store.get(pending.id)
    assert resumed_pending is not None
    assert resumed_pending.status == "pending"
    assert current_git_health_alert(store) is None

    resumed_log = log_path.read_text()
    assert "git worktree health RED - dispatch halted" not in resumed_log
    assert " HOLD " not in resumed_log
    assert " ATTENTION " not in resumed_log
    assert any(
        "START" in line and pending.id in line and "[dry-run]" in line
        for line in resumed_log.splitlines()
    )
