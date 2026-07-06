"""Functional tests for execution flows that require a real git repo."""

import argparse
import os
import textwrap
import time
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from gza.cli import cmd_iterate
from gza.cli._common import reconcile_in_progress_tasks
from gza.config import Config
from gza.db import SqliteTaskStore
from gza.failure_reasons import mark_task_failed_from_cause
from gza.git import Git
from gza.workers import WorkerMetadata, WorkerRegistry
from tests.cli.conftest import invoke_gza, make_store, setup_config


def _iterate_git_runtime():
    mock_git = MagicMock()
    mock_git.current_branch.return_value = "main"
    mock_git.resolve_merge_source_ref.return_value = None
    mock_git.is_merged.return_value = False
    mock_git.can_merge.return_value = True
    return patch("gza.cli.Git", return_value=mock_git)


def _init_basic_repo(tmp_path, *, default_branch: str = "main") -> Git:
    git = Git(tmp_path)
    git._run("init", "-b", default_branch)
    git._run("config", "user.name", "Test User")
    git._run("config", "user.email", "test@example.com")
    (tmp_path / "README.md").write_text("initial")
    git._run("add", "README.md")
    git._run("commit", "-m", "Initial commit")
    return git


def _make_completed_impl(store: SqliteTaskStore):
    impl = store.add("Completed implementation", task_type="implement")
    impl.status = "completed"
    impl.branch = "gza/1-completed-implementation"
    impl.completed_at = datetime(2026, 1, 1, tzinfo=UTC)
    store.update(impl)
    assert impl.id is not None
    return impl


def _setup_store(tmp_path):
    setup_config(tmp_path)
    db_path = tmp_path / ".gza" / "gza.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return make_store(tmp_path)


def test_reconciliation_detects_commits_on_worker_died(tmp_path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    git = _init_basic_repo(tmp_path)

    git._run("checkout", "-b", "task-branch")
    (tmp_path / "work.py").write_text("print('hello')")
    git._run("add", "work.py")
    git._run("commit", "-m", "Task work")
    git._run("checkout", "main")

    task = store.add("Task with commits")
    store.mark_in_progress(task)
    task = store.get(task.id)
    assert task is not None
    task.running_pid = -1
    task.branch = "task-branch"
    store.update(task)

    config = Config.load(tmp_path)
    with patch("gza.cli._common.mark_task_failed_from_cause", wraps=mark_task_failed_from_cause) as mock_mark_failed:
        reconcile_in_progress_tasks(config)

    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.status == "failed"
    assert refreshed.failure_reason == "WORKER_DIED"
    assert refreshed.has_commits is True
    assert mock_mark_failed.call_count == 1
    assert mock_mark_failed.call_args.kwargs["explicit_reason"] == "WORKER_DIED"


def test_reconciliation_no_commits_on_worker_died(tmp_path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    _init_basic_repo(tmp_path)

    task = store.add("Task without branch")
    store.mark_in_progress(task)
    task = store.get(task.id)
    assert task is not None
    task.running_pid = -1
    store.update(task)

    config = Config.load(tmp_path)
    reconcile_in_progress_tasks(config)

    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.status == "failed"
    assert refreshed.failure_reason == "WORKER_DIED"
    assert refreshed.has_commits is not True


def test_reconciliation_worker_died_records_signal_and_output_tail(tmp_path: Path) -> None:
    """Reconciliation should persist signal and output-tail breadcrumbs for WORKER_DIED."""
    import json

    setup_config(tmp_path)
    store = make_store(tmp_path)
    _init_basic_repo(tmp_path)

    task = store.add("Killed worker task")
    store.mark_in_progress(task)
    task = store.get(task.id)
    assert task is not None
    task.running_pid = -1
    task.log_file = ".gza/logs/killed-worker.log"
    store.update(task)

    log_dir = tmp_path / ".gza" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    (log_dir / "killed-worker.log").write_text("")
    (log_dir / "killed-worker.ops.jsonl").write_text(
        json.dumps(
            {
                "type": "gza",
                "subtype": "command",
                "event": "verify_credentials_docker",
                "message": "preflight ok",
            }
        )
        + "\n"
    )

    workers_dir = tmp_path / ".gza" / "workers"
    workers_dir.mkdir(parents=True, exist_ok=True)
    startup_log = tmp_path / ".gza" / "workers" / "w-killed-startup.log"
    startup_log.write_text("stderr tail line\nstdout tail line\n")

    config = Config.load(tmp_path)
    registry = WorkerRegistry(config.workers_path)
    registry.register(
        WorkerMetadata(
            worker_id="w-killed",
            task_id=task.id,
            pid=999999,
            status="failed",
            exit_code=-9,
            startup_log_file=".gza/workers/w-killed-startup.log",
            completed_at=datetime.now(UTC).isoformat(),
        )
    )

    reconcile_in_progress_tasks(config)

    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.status == "failed"
    assert refreshed.failure_reason == "WORKER_DIED"

    ops_entries = [
        json.loads(line)
        for line in (log_dir / "killed-worker.ops.jsonl").read_text().splitlines()
        if line.strip()
    ]
    death_entry = next(entry for entry in reversed(ops_entries) if entry.get("event") == "death_detected")
    assert death_entry["signal"] == "SIGKILL"
    assert death_entry["exit_code"] == -9
    assert death_entry["stage"] == "after_preflight_before_worker_start"
    assert death_entry["output_tail"] == ["stderr tail line", "stdout tail line"]

    result = invoke_gza("show", str(task.id), "--project", str(tmp_path))
    assert result.returncode == 0
    assert "Worker Exit: SIGKILL, exit code -9" in result.stdout
    assert "Worker Output Tail:" in result.stdout


def test_dry_run_changes_requested_completed_improve_without_review_clear_creates_closing_review(tmp_path) -> None:
    setup_config(tmp_path)
    _init_basic_repo(tmp_path)
    store = make_store(tmp_path)
    impl = _make_completed_impl(store)
    git = Git(tmp_path)
    git._run("checkout", "-b", impl.branch)
    (tmp_path / "impl.txt").write_text("impl work")
    git._run("add", "impl.txt")
    git._run("commit", "-m", "Add impl work")
    git._run("checkout", "main")

    review = store.add("Review", task_type="review", depends_on=impl.id)
    review.status = "completed"
    review.output_content = "**Verdict: CHANGES_REQUESTED**"
    review.completed_at = datetime(2026, 1, 2, tzinfo=UTC)
    store.update(review)

    improve = store.add(
        "Completed improve",
        task_type="improve",
        based_on=impl.id,
        depends_on=review.id,
        same_branch=True,
    )
    improve.status = "completed"
    improve.completed_at = datetime(2026, 1, 3, tzinfo=UTC)
    store.update(improve)

    with _iterate_git_runtime():
        result = invoke_gza("iterate", str(impl.id), "--dry-run", "--project", str(tmp_path))

    assert result.returncode == 0
    assert "would iterate implementation" in result.stdout.lower()
    assert "first iteration 1/3 action: create_review" in result.stdout.lower()
    assert "code changed since the last review" in result.stdout.lower()


@pytest.mark.functional
def test_background_iterate_prepared_initial_review_continues_to_closing_review(tmp_path) -> None:
    setup_config(tmp_path)
    _init_basic_repo(tmp_path)
    store = make_store(tmp_path)
    impl = _make_completed_impl(store)
    git = Git(tmp_path)
    git._run("checkout", "-b", impl.branch)
    (tmp_path / "impl.txt").write_text("impl work")
    git._run("add", "impl.txt")
    git._run("commit", "-m", "Add impl work")
    git._run("checkout", "main")

    patch_dir = tmp_path / "patches"
    patch_dir.mkdir()
    (patch_dir / "sitecustomize.py").write_text(
        textwrap.dedent(
            f"""
            from datetime import UTC, datetime

            import gza.cli
            from gza.db import SqliteTaskStore


            ROOT_IMPL_ID = "{impl.id}"


            def _fake_run(config, task_id=None, **kwargs):
                store = SqliteTaskStore.from_config(config)
                task = store.get(task_id)
                assert task is not None
                task.status = "completed"
                task.completed_at = datetime.now(UTC)
                if task.task_type == "review":
                    completed_other_reviews = sum(
                        1
                        for candidate in store.get_reviews_for_task(ROOT_IMPL_ID)
                        if candidate.id != task.id and candidate.status == "completed"
                    )
                    task.output_content = (
                        "**Verdict: CHANGES_REQUESTED**"
                        if completed_other_reviews == 0
                        else "**Verdict: APPROVED**"
                    )
                elif task.task_type == "improve":
                    task.changed_diff = True
                store.update(task)
                return 0


            gza.cli.run = _fake_run
            """
        )
    )

    env = os.environ.copy()
    env["PYTHONPATH"] = str(patch_dir) + os.pathsep + env.get("PYTHONPATH", "")

    result = invoke_gza(
        "iterate",
        str(impl.id),
        "--background",
        "--project",
        str(tmp_path),
        cwd=tmp_path,
        env=env,
    )

    assert result.returncode == 0
    assert "Started task" in result.stdout

    registry = WorkerRegistry(tmp_path / ".gza" / "workers")
    deadline = time.monotonic() + 10
    workers = registry.list_all(include_completed=True)
    while (not workers or any(worker.status == "running" for worker in workers)) and time.monotonic() < deadline:
        time.sleep(0.1)
        workers = registry.list_all(include_completed=True)

    assert workers
    assert all(worker.status == "completed" for worker in workers)
    assert all(worker.exit_code == 0 for worker in workers)
    startup_log = (tmp_path / workers[0].startup_log_file).read_text()
    assert "Iteration 1/3: create_review" in startup_log
    assert "Iteration 2/3: improve" in startup_log
    assert "Iteration 2/3: create_review" in startup_log

    reviews = store.get_reviews_for_task(impl.id)
    improves = [
        task
        for review in reviews
        for task in store.get_improve_tasks_for(impl.id, review.id)
    ]

    assert len(reviews) == 2
    assert [task.output_content for task in reviews] == [
        "**Verdict: APPROVED**",
        "**Verdict: CHANGES_REQUESTED**",
    ]
    assert len(improves) == 1
    assert improves[0].status == "completed"
    assert improves[0].changed_diff is True

    dry_run = invoke_gza("iterate", str(impl.id), "--dry-run", "--project", str(tmp_path), cwd=tmp_path)
    assert dry_run.returncode == 0
    assert "create_review" not in dry_run.stdout
    assert "merge" in dry_run.stdout.lower()


@pytest.mark.functional
def test_background_iterate_changes_requested_improve_runs_closing_review(tmp_path) -> None:
    setup_config(tmp_path)
    _init_basic_repo(tmp_path)
    store = make_store(tmp_path)
    impl = _make_completed_impl(store)
    git = Git(tmp_path)
    git._run("checkout", "-b", impl.branch)
    (tmp_path / "impl.txt").write_text("impl work")
    git._run("add", "impl.txt")
    git._run("commit", "-m", "Add impl work")
    git._run("checkout", "main")

    review = store.add("Review", task_type="review", depends_on=impl.id)
    review.status = "completed"
    review.output_content = "**Verdict: CHANGES_REQUESTED**"
    review.completed_at = datetime(2026, 1, 2, tzinfo=UTC)
    store.update(review)
    assert review.id is not None

    patch_dir = tmp_path / "patches"
    patch_dir.mkdir()
    (patch_dir / "sitecustomize.py").write_text(
        textwrap.dedent(
            """
            from datetime import UTC, datetime

            import gza.cli.execution as execution


            def _fake_run_foreground(config, task_id, **kwargs):
                store = execution.get_store(config)
                task = store.get(task_id)
                assert task is not None
                task.status = "completed"
                task.completed_at = datetime.now(UTC)
                if task.task_type == "improve":
                    task.changed_diff = True
                if task.task_type == "review":
                    task.output_content = "**Verdict: APPROVED**"
                store.update(task)
                return 0


            execution._run_foreground = _fake_run_foreground
            """
        )
    )

    env = os.environ.copy()
    env["PYTHONPATH"] = str(patch_dir) + os.pathsep + env.get("PYTHONPATH", "")

    result = invoke_gza(
        "iterate",
        str(impl.id),
        "--background",
        "--project",
        str(tmp_path),
        cwd=tmp_path,
        env=env,
    )

    assert result.returncode == 0
    assert "Started task" in result.stdout

    registry = WorkerRegistry(tmp_path / ".gza" / "workers")
    deadline = time.monotonic() + 10
    workers = registry.list_all(include_completed=True)
    while (not workers or any(worker.status == "running" for worker in workers)) and time.monotonic() < deadline:
        time.sleep(0.1)
        workers = registry.list_all(include_completed=True)

    assert workers
    assert all(worker.status == "completed" for worker in workers)
    assert all(worker.exit_code == 0 for worker in workers)

    reviews = store.get_reviews_for_task(impl.id)
    improves = store.get_improve_tasks_for(impl.id, review.id)

    assert len(reviews) == 2
    assert sum(1 for task in reviews if task.status == "completed") == 2
    assert len(improves) == 1
    assert improves[0].status == "completed"
    assert improves[0].changed_diff is True
    assert all(task.status != "pending" for task in reviews)
    assert any(task.output_content == "**Verdict: APPROVED**" for task in reviews)

    dry_run = invoke_gza("iterate", str(impl.id), "--dry-run", "--project", str(tmp_path), cwd=tmp_path)
    assert dry_run.returncode == 0
    assert "create_review" not in dry_run.stdout
    assert "merge" in dry_run.stdout.lower()


@pytest.mark.functional
def test_background_iterate_repeated_changes_requested_cycles_reach_max_iterations(tmp_path) -> None:
    setup_config(tmp_path)
    _init_basic_repo(tmp_path)
    store = make_store(tmp_path)
    impl = _make_completed_impl(store)
    git = Git(tmp_path)
    git._run("checkout", "-b", impl.branch)
    (tmp_path / "impl.txt").write_text("impl work")
    git._run("add", "impl.txt")
    git._run("commit", "-m", "Add impl work")
    git._run("checkout", "main")

    review = store.add("Initial review", task_type="review", depends_on=impl.id)
    review.status = "completed"
    review.output_content = "**Verdict: CHANGES_REQUESTED**"
    review.completed_at = datetime(2026, 1, 2, tzinfo=UTC)
    store.update(review)
    assert review.id is not None

    max_iterations = 3
    patch_dir = tmp_path / "patches"
    patch_dir.mkdir()
    (patch_dir / "sitecustomize.py").write_text(
        textwrap.dedent(
            f"""
            from datetime import UTC, datetime

            import gza.cli.execution as execution


            MAX_ITERATIONS = {max_iterations}


            def _fake_run_foreground(config, task_id, **kwargs):
                store = execution.get_store(config)
                task = store.get(task_id)
                assert task is not None
                task.status = "completed"
                task.completed_at = datetime.now(UTC)
                if task.task_type == "improve":
                    task.changed_diff = True
                elif task.task_type == "review":
                    reviews = store.get_reviews_for_task("{impl.id}")
                    completed_other_reviews = sum(
                        1
                        for candidate in reviews
                        if candidate.id != task.id and candidate.status == "completed"
                    )
                    if completed_other_reviews <= MAX_ITERATIONS:
                        task.output_content = "**Verdict: CHANGES_REQUESTED**"
                    else:
                        task.output_content = "**Verdict: APPROVED**"
                store.update(task)
                return 0


            execution._run_foreground = _fake_run_foreground
            """
        )
    )

    env = os.environ.copy()
    env["PYTHONPATH"] = str(patch_dir) + os.pathsep + env.get("PYTHONPATH", "")

    result = invoke_gza(
        "iterate",
        str(impl.id),
        "--background",
        "--max-iterations",
        str(max_iterations),
        "--project",
        str(tmp_path),
        cwd=tmp_path,
        env=env,
    )

    assert result.returncode == 0
    assert "Started task" in result.stdout

    registry = WorkerRegistry(tmp_path / ".gza" / "workers")
    deadline = time.monotonic() + 10
    workers = registry.list_all(include_completed=True)
    while (not workers or any(worker.status == "running" for worker in workers)) and time.monotonic() < deadline:
        time.sleep(0.1)
        workers = registry.list_all(include_completed=True)

    assert workers
    assert all(worker.status != "running" for worker in workers)
    assert all(worker.exit_code == 2 for worker in workers)
    startup_log = (tmp_path / workers[0].startup_log_file).read_text()
    assert "Iteration 3/3: create_review" in startup_log
    assert "Max iterations (3) reached." in startup_log

    reviews = store.get_reviews_for_task(impl.id)
    all_improves = [
        task
        for candidate_review in reviews
        for task in store.get_improve_tasks_for(impl.id, candidate_review.id)
    ]

    assert len(reviews) == max_iterations + 1
    assert sum(1 for task in reviews if task.status == "completed") == max_iterations + 1
    assert len(all_improves) == max_iterations
    assert all(task.status == "completed" for task in all_improves)
    assert all(task.changed_diff is True for task in all_improves)
    assert all(task.output_content == "**Verdict: CHANGES_REQUESTED**" for task in reviews)
    assert all(task.status != "pending" for task in reviews)

    dry_run = invoke_gza(
        "iterate",
        str(impl.id),
        "--dry-run",
        "--max-iterations",
        str(max_iterations),
        "--project",
        str(tmp_path),
        cwd=tmp_path,
    )
    assert dry_run.returncode == 0
    assert "create_review" not in dry_run.stdout


@pytest.mark.functional
@pytest.mark.parametrize("flag", ["--depends-on", "--based-on"])
def test_add_implement_from_held_plan_refuses_precreated_child(tmp_path, flag: str) -> None:
    """Subprocess add must reject implement tasks sourced from a held completed plan."""

    setup_config(tmp_path)
    store = make_store(tmp_path)
    plan = store.add("Held plan", task_type="plan", auto_implement=False)
    assert plan.id is not None
    plan.status = "completed"
    plan.completed_at = datetime.now(UTC)
    store.update(plan)

    result = invoke_gza(
        "add",
        "--type",
        "implement",
        flag,
        str(plan.id),
        "Blocked implement",
        "--project",
        str(tmp_path),
    )

    assert result.returncode == 1
    normalized = " ".join(result.stdout.split())
    assert f"plan {plan.id} is held for review" in normalized
    assert f"uv run gza implement {plan.id}" in normalized
    assert f"uv run gza edit {plan.id} --no-hold-for-review" in normalized
    assert not any(task.prompt == "Blocked implement" for task in store.get_all())


@pytest.mark.functional
def test_incomplete_surfaces_release_guidance_for_existing_held_plan_child_deadlock(tmp_path) -> None:
    """Older inconsistent rows still surface explicit held-plan release guidance."""

    setup_config(tmp_path)
    store = make_store(tmp_path)
    plan = store.add("Held plan", task_type="plan", auto_implement=False)
    assert plan.id is not None
    plan.status = "completed"
    plan.completed_at = datetime.now(UTC)
    store.update(plan)
    child = store.add("Precreated implement child", task_type="implement", depends_on=plan.id)
    assert child.id is not None

    result = invoke_gza("incomplete", "--project", str(tmp_path))

    assert result.returncode == 0
    normalized = " ".join(result.stdout.split())
    assert child.id in normalized
    assert f"blocked: awaiting plan review for {plan.id}" in normalized
    assert f"release with uv run gza implement {plan.id}" in normalized
    assert f"or uv run gza edit {plan.id} --no-hold-for-review" in normalized


@pytest.mark.functional
def test_iterate_foreground_held_plan_block_surfaces_release_guidance(tmp_path) -> None:
    """Foreground iterate should report held-plan dependency blocks with release guidance."""

    setup_config(tmp_path)
    _init_basic_repo(tmp_path)
    store = make_store(tmp_path)
    plan = store.add("Held plan", task_type="plan", auto_implement=False)
    assert plan.id is not None
    plan.status = "completed"
    plan.completed_at = datetime.now(UTC)
    store.update(plan)

    impl = store.add("Blocked implement", task_type="implement", depends_on=plan.id)
    assert impl.id is not None

    result = invoke_gza("iterate", str(impl.id), "--project", str(tmp_path))

    assert result.returncode == 3
    normalized = " ".join(result.stdout.split())
    assert f"Task {impl.id} is blocked: awaiting plan review for {plan.id}" in normalized
    assert f"uv run gza implement {plan.id}" in normalized
    assert f"uv run gza edit {plan.id} --no-hold-for-review" in normalized


@pytest.mark.functional
def test_iterate_background_held_plan_block_does_not_spawn_worker(tmp_path) -> None:
    """Background iterate should refuse held-plan blocks before worker startup."""

    setup_config(tmp_path)
    _init_basic_repo(tmp_path)
    store = make_store(tmp_path)
    plan = store.add("Held plan", task_type="plan", auto_implement=False)
    assert plan.id is not None
    plan.status = "completed"
    plan.completed_at = datetime.now(UTC)
    store.update(plan)

    impl = store.add("Blocked implement", task_type="implement", depends_on=plan.id)
    assert impl.id is not None

    result = invoke_gza(
        "iterate",
        str(impl.id),
        "--background",
        "--no-docker",
        "--project",
        str(tmp_path),
    )

    assert result.returncode == 1
    normalized = " ".join((result.stdout + result.stderr).split())
    assert f"Task {impl.id} is blocked: awaiting plan review for {plan.id}" in normalized

    config = Config.load(tmp_path)
    registry = WorkerRegistry(config.workers_path)
    workers = [w for w in registry.list_all(include_completed=True) if w.task_id == impl.id]
    assert workers == []

    refreshed = store.get(impl.id)
    assert refreshed is not None
    assert refreshed.status == "pending"
    assert refreshed.slug is None
    assert refreshed.log_file is None


@pytest.mark.functional
def test_failed_task_retry_runs_then_iterates(tmp_path, capsys: pytest.CaptureFixture[str]) -> None:
    """gza iterate --retry on a failed task retries it then enters the loop via real engine transitions."""
    setup_config(tmp_path)
    _init_basic_repo(tmp_path)
    store = make_store(tmp_path)
    impl = store.add("Implement feature", task_type="implement")
    impl.status = "failed"
    impl.same_branch = True
    impl.branch = "feature/existing-impl-branch"
    store.update(impl)

    def fake_run_foreground(config, task_id, **kwargs):
        task = store.get(task_id)
        assert task is not None
        if task.task_type == "review":
            task.status = "completed"
            task.output_content = "**Verdict: APPROVED**"
            task.completed_at = datetime.now()
            store.update(task)
            return 0
        if task.status == "pending":
            task.status = "completed"
            if task.task_type == "implement":
                task.branch = "test-project/20260101-retry"
            task.completed_at = datetime.now()
            store.update(task)
            return 0
        raise AssertionError(f"unexpected task id: {task_id}")

    args = argparse.Namespace(
        project_dir=str(tmp_path),
        impl_task_id=str(impl.id),
        max_iterations=1,
        dry_run=False,
        no_docker=True,
        resume=False,
        retry=True,
        background=False,
    )
    mock_config = MagicMock(
        project_dir=tmp_path,
        use_docker=False,
        project_prefix="testproject",
        require_review_before_merge=False,
        advance_create_reviews=True,
        max_review_cycles=3,
        max_resume_attempts=1,
    )
    mock_git = MagicMock()
    mock_git.current_branch.return_value = "main"
    mock_git.can_merge.return_value = True
    with (
        patch("gza.cli.Config.load", return_value=mock_config),
        patch("gza.cli.get_store", return_value=store),
        patch("gza.cli._run_foreground", side_effect=fake_run_foreground) as run_fg,
        patch("gza.cli.Git", return_value=mock_git),
    ):
        result = cmd_iterate(args)
    output = capsys.readouterr().out

    assert result == 0
    assert run_fg.call_count >= 1
    first_task_id = run_fg.call_args_list[0][1]["task_id"]
    assert first_task_id != impl.id
    retry_task = store.get(first_task_id)
    assert retry_task is not None
    assert retry_task.same_branch is False
    assert retry_task.base_branch == "feature/existing-impl-branch"
    assert "Retrying failed implementation" in output
    assert "Iterate complete: MERGE_READY" in output


@pytest.mark.functional
def test_failed_task_resume_runs_then_iterates(tmp_path, capsys: pytest.CaptureFixture[str]) -> None:
    """gza iterate --resume on a failed task resumes then enters the loop via real engine transitions."""
    setup_config(tmp_path)
    _init_basic_repo(tmp_path)
    store = make_store(tmp_path)
    impl = store.add("Implement feature", task_type="implement")
    impl.status = "failed"
    impl.failure_reason = "MAX_TURNS"
    impl.session_id = "resume-session-1"
    store.update(impl)

    def fake_run_foreground(config, task_id, **kwargs):
        task = store.get(task_id)
        if task and task.status == "pending":
            task.status = "completed"
            if task.task_type == "review":
                task.output_content = "**Verdict: APPROVED**"
            elif task.task_type == "implement":
                task.branch = "test-project/20260101-resume"
            task.completed_at = datetime.now()
            store.update(task)
        return 0

    args = argparse.Namespace(
        project_dir=str(tmp_path),
        impl_task_id=str(impl.id),
        max_iterations=1,
        dry_run=False,
        no_docker=True,
        resume=True,
        retry=False,
        background=False,
    )
    mock_config = MagicMock(
        project_dir=tmp_path,
        use_docker=False,
        project_prefix="testproject",
        require_review_before_merge=False,
        advance_create_reviews=True,
        max_review_cycles=3,
        max_resume_attempts=1,
    )
    mock_git = MagicMock()
    mock_git.current_branch.return_value = "main"
    mock_git.can_merge.return_value = True

    with (
        patch("gza.cli.Config.load", return_value=mock_config),
        patch("gza.cli.get_store", return_value=store),
        patch("gza.cli._run_foreground", side_effect=fake_run_foreground) as run_fg,
        patch("gza.cli.Git", return_value=mock_git),
    ):
        result = cmd_iterate(args)
    output = capsys.readouterr().out

    assert result == 0
    assert run_fg.call_count >= 1
    first_task_id = run_fg.call_args_list[0][1]["task_id"]
    assert first_task_id != impl.id
    assert "Resuming failed implementation" in output
    assert "Iterate complete: MERGE_READY" in output


@pytest.mark.functional
def test_iterate_improve_resume_passes_resume_true_to_run_foreground(
    tmp_path,
) -> None:
    """When iterate picks resume for an improve, it must call _run_foreground with resume=True."""
    setup_config(tmp_path)
    _init_basic_repo(tmp_path)
    store = make_store(tmp_path)
    impl = store.add("Completed implementation", task_type="implement")
    impl.status = "completed"
    impl.branch = "test-project/20260101-impl"
    impl.completed_at = datetime.now(UTC)
    store.update(impl)

    review = store.add("Review", task_type="review", depends_on=impl.id, based_on=impl.id)
    review.status = "completed"
    review.output_content = "**Verdict: CHANGES_REQUESTED**"
    review.completed_at = datetime.now(UTC)
    store.update(review)

    failed_improve = store.add(
        "Improve",
        task_type="improve",
        depends_on=review.id,
        based_on=impl.id,
        same_branch=True,
    )
    failed_improve.status = "failed"
    failed_improve.failure_reason = "TIMEOUT"
    failed_improve.session_id = "improve-session"
    failed_improve.branch = impl.branch
    store.update(failed_improve)

    args = argparse.Namespace(
        impl_task_id=impl.id,
        max_iterations=1,
        dry_run=False,
        project_dir=tmp_path,
        no_docker=True,
        resume=False,
        retry=False,
        background=False,
    )
    mock_config = MagicMock(
        project_dir=tmp_path,
        use_docker=False,
        project_prefix="testproject",
        max_resume_attempts=3,
    )
    mock_git = MagicMock()
    mock_git.current_branch.return_value = "main"

    def fake_run_foreground(config, task_id, resume=False, **kwargs):
        task = store.get(task_id)
        assert task is not None
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        store.update(task)
        return 0

    improve_action = {"type": "improve", "description": "Create improve", "review_task": review}
    engine_actions = [improve_action, improve_action, {"type": "skip", "description": "done"}]

    with (
        patch("gza.cli.Config.load", return_value=mock_config),
        patch("gza.cli.get_store", return_value=store),
        patch("gza.cli.Git", return_value=mock_git),
        patch("gza.cli.determine_next_action", side_effect=engine_actions),
        patch("gza.cli._run_foreground", side_effect=fake_run_foreground) as run_fg,
    ):
        cmd_iterate(args)

    assert run_fg.call_count == 1
    assert run_fg.call_args_list[0].kwargs.get("resume") is True


def test_cycle_dry_run(tmp_path) -> None:
    setup_config(tmp_path)
    _init_basic_repo(tmp_path)
    store = make_store(tmp_path)
    impl = _make_completed_impl(store)

    result = invoke_gza("iterate", str(impl.id), "--dry-run", "--project", str(tmp_path))

    assert result.returncode == 0
    assert "dry-run" in result.stdout.lower()


def test_cycle_uses_default_iterations_when_flag_omitted(tmp_path) -> None:
    (tmp_path / "gza.yaml").write_text("project_name: test-project\ndb_path: .gza/gza.db\n")
    _init_basic_repo(tmp_path)
    store = make_store(tmp_path)
    impl = _make_completed_impl(store)

    result = invoke_gza("iterate", str(impl.id), "--dry-run", "--project", str(tmp_path))

    assert result.returncode == 0
    assert "max 3 iterations" in result.stdout


def test_dry_run_completed_improve_without_review_clear_starts_from_closing_review(tmp_path) -> None:
    setup_config(tmp_path)
    _init_basic_repo(tmp_path)
    store = make_store(tmp_path)
    impl = _make_completed_impl(store)
    git = Git(tmp_path)
    git._run("checkout", "-b", impl.branch)
    (tmp_path / "impl.txt").write_text("impl work")
    git._run("add", "impl.txt")
    git._run("commit", "-m", "Add impl work")
    git._run("checkout", "main")

    stale_review = store.add("Old review", task_type="review", depends_on=impl.id)
    stale_review.status = "completed"
    stale_review.output_content = "**Verdict: APPROVED**"
    stale_review.completed_at = datetime(2026, 1, 1, tzinfo=UTC)
    store.update(stale_review)

    improve = store.add(
        "Current write",
        task_type="improve",
        based_on=impl.id,
        depends_on=stale_review.id,
        same_branch=True,
    )
    improve.status = "completed"
    improve.completed_at = datetime(2026, 1, 2, tzinfo=UTC)
    store.update(improve)

    with _iterate_git_runtime():
        result = invoke_gza("iterate", str(impl.id), "--dry-run", "--project", str(tmp_path))

    assert result.returncode == 0
    assert "would iterate implementation" in result.stdout.lower()
    assert "first iteration 1/3 action: create_review" in result.stdout.lower()
    assert "code changed since the last review" in result.stdout.lower()


def test_mark_completed_default_verify_git_for_code_tasks(tmp_path) -> None:
    store = _setup_store(tmp_path)
    _init_basic_repo(tmp_path)

    task = store.add("Code task with no branch", task_type="implement")
    task.status = "failed"
    store.update(task)

    task = make_store(tmp_path).get_all()[0]
    result = invoke_gza("mark-completed", str(task.id), "--project", str(tmp_path))

    assert result.returncode == 1
    assert "no branch" in result.stdout


def test_mark_completed_warns_if_not_failed(tmp_path) -> None:
    store = _setup_store(tmp_path)
    git = _init_basic_repo(tmp_path)
    git._run("checkout", "-b", "gza/1-test-task")
    git._run("checkout", "main")

    task = store.add("Pending task")
    task.status = "pending"
    task.branch = "gza/1-test-task"
    store.update(task)

    result = invoke_gza("mark-completed", str(task.id), "--project", str(tmp_path))

    assert result.returncode == 0
    assert "Warning" in result.stdout
    assert "not in failed status" in result.stdout


def test_mark_completed_errors_if_branch_missing_in_git(tmp_path) -> None:
    store = _setup_store(tmp_path)
    _init_basic_repo(tmp_path)

    task = store.add("Failed task")
    task.status = "failed"
    task.branch = "gza/1-nonexistent-branch"
    store.update(task)

    result = invoke_gza("mark-completed", str(task.id), "--project", str(tmp_path))

    assert result.returncode == 1
    assert "does not exist" in result.stdout
    assert "Use --force" in result.stdout


def test_mark_completed_with_commits_sets_unmerged(tmp_path) -> None:
    store = _setup_store(tmp_path)
    git = _init_basic_repo(tmp_path)
    git._run("checkout", "-b", "gza/1-task-with-commits")
    (tmp_path / "feature.txt").write_text("feature")
    git._run("add", "feature.txt")
    git._run("commit", "-m", "Add feature")
    git._run("checkout", "main")

    task = store.add("Failed task with commits")
    task.status = "failed"
    task.branch = "gza/1-task-with-commits"
    store.update(task)

    result = invoke_gza(
        "mark-completed",
        str(task.id),
        "--reason",
        "EXTRACTION_ALREADY_MERGED",
        "--project",
        str(tmp_path),
    )

    assert result.returncode == 0
    assert "unmerged" in result.stdout

    updated = store.get(task.id)
    assert updated is not None
    assert updated.status == "completed"
    assert updated.merge_status == "unmerged"
    assert updated.has_commits is True
    assert updated.completion_reason == "EXTRACTION_ALREADY_MERGED"


def test_mark_completed_without_commits_marks_completed(tmp_path) -> None:
    store = _setup_store(tmp_path)
    git = _init_basic_repo(tmp_path)
    git._run("checkout", "-b", "gza/1-empty-branch")
    git._run("checkout", "main")

    task = store.add("Failed task no commits")
    task.status = "failed"
    task.branch = "gza/1-empty-branch"
    store.update(task)

    result = invoke_gza(
        "mark-completed",
        str(task.id),
        "--reason",
        "EXTRACTION_ALREADY_MERGED",
        "--project",
        str(tmp_path),
    )

    assert result.returncode == 0
    assert "No commits found" in result.stdout
    assert "completed" in result.stdout

    updated = store.get(task.id)
    assert updated is not None
    assert updated.status == "completed"
    assert updated.has_commits is False
    assert updated.completion_reason == "EXTRACTION_ALREADY_MERGED"


def test_mark_completed_failed_task_no_warning(tmp_path) -> None:
    store = _setup_store(tmp_path)
    git = _init_basic_repo(tmp_path)
    git._run("checkout", "-b", "gza/1-failed-branch")
    git._run("checkout", "main")

    task = store.add("Failed task")
    task.status = "failed"
    task.branch = "gza/1-failed-branch"
    store.update(task)

    result = invoke_gza("mark-completed", str(task.id), "--project", str(tmp_path))

    assert result.returncode == 0
    assert "Warning" not in result.stdout


def test_mark_completed_cleans_up_running_worker(tmp_path) -> None:
    store = _setup_store(tmp_path)
    git = _init_basic_repo(tmp_path)
    git._run("checkout", "-b", "gza/1-worker-task")
    git._run("checkout", "main")

    task = store.add("Failed task with worker")
    task.status = "failed"
    task.branch = "gza/1-worker-task"
    store.update(task)

    workers_path = tmp_path / ".gza" / "workers"
    workers_path.mkdir(parents=True, exist_ok=True)
    registry = WorkerRegistry(workers_path)
    worker = WorkerMetadata(
        worker_id="w-20260301-120000",
        pid=99999,
        task_id=task.id,
        task_slug=task.slug,
        started_at="2026-03-01T12:00:00+00:00",
        status="running",
        log_file=None,
        worktree=None,
        is_background=True,
    )
    registry.register(worker)

    pid_path = workers_path / "w-20260301-120000.pid"
    assert pid_path.exists()

    result = invoke_gza("mark-completed", str(task.id), "--project", str(tmp_path))

    assert result.returncode == 0
    updated_worker = registry.get("w-20260301-120000")
    assert updated_worker is not None
    assert updated_worker.status == "completed"
    assert not pid_path.exists()


def test_mark_completed_does_not_touch_already_completed_worker(tmp_path) -> None:
    store = _setup_store(tmp_path)
    git = _init_basic_repo(tmp_path)
    git._run("checkout", "-b", "gza/1-already-done-branch")
    git._run("checkout", "main")

    task = store.add("Failed task with done worker")
    task.status = "failed"
    task.branch = "gza/1-already-done-branch"
    store.update(task)

    workers_path = tmp_path / ".gza" / "workers"
    workers_path.mkdir(parents=True, exist_ok=True)
    registry = WorkerRegistry(workers_path)
    worker = WorkerMetadata(
        worker_id="w-20260301-130000",
        pid=99998,
        task_id=task.id,
        task_slug=task.slug,
        started_at="2026-03-01T13:00:00+00:00",
        status="failed",
        log_file=None,
        worktree=None,
        is_background=True,
        exit_code=1,
        completed_at="2026-03-01T13:05:00+00:00",
    )
    registry.register(worker)

    task = make_store(tmp_path).get_all()[0]
    result = invoke_gza("mark-completed", str(task.id), "--project", str(tmp_path))

    assert result.returncode == 0
    updated_worker = registry.get("w-20260301-130000")
    assert updated_worker is not None
    assert updated_worker.status == "failed"


def test_advance_skips_dropped_tasks(tmp_path) -> None:
    setup_config(tmp_path)
    db_path = tmp_path / ".gza" / "gza.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    store = SqliteTaskStore(db_path)
    _init_basic_repo(tmp_path)

    task = store.add("Dropped task", task_type="implement")
    task.status = "dropped"
    task.completed_at = datetime.now(UTC)
    store.update(task)

    result = invoke_gza("advance", "--project", str(tmp_path))
    assert result.returncode == 0
    assert "No eligible tasks" in result.stdout


def test_advance_explicit_completed_descendant_in_dropped_owner_lineage_is_ineligible(tmp_path) -> None:
    setup_config(tmp_path)
    db_path = tmp_path / ".gza" / "gza.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    store = SqliteTaskStore(db_path)
    _init_basic_repo(tmp_path)

    owner = store.add("Dropped implement owner", task_type="implement")
    owner.status = "dropped"
    owner.completed_at = datetime.now(UTC)
    owner.branch = "feature/dropped-owner"
    owner.has_commits = True
    owner.merge_status = "unmerged"
    store.update(owner)
    assert owner.id is not None

    descendant = store.add("Completed rebase descendant", task_type="rebase", based_on=owner.id, same_branch=True)
    descendant.status = "completed"
    descendant.completed_at = datetime.now(UTC)
    descendant.branch = owner.branch
    descendant.has_commits = True
    descendant.merge_status = "unmerged"
    store.update(descendant)
    assert descendant.id is not None

    result = invoke_gza("advance", str(descendant.id), "--dry-run", "--project", str(tmp_path))
    assert result.returncode == 0
    assert "No eligible tasks to advance" in result.stdout


def test_background_phase1_validation_errors_write_to_stderr_only_rebase(tmp_path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    _init_basic_repo(tmp_path)
    task = store.add("Completed implementation", task_type="implement")
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    store.update(task)

    expected = f"Error: Task {task.id} has no branch"
    result = invoke_gza("rebase", str(task.id), "--background", "--project", str(tmp_path))

    assert result.returncode == 1
    assert expected in result.stderr
    assert expected not in result.stdout
    assert "Error:" not in result.stdout
