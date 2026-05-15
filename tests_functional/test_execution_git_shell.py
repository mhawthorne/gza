"""Functional tests for execution flows that require a real git repo."""

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from gza.cli._common import reconcile_in_progress_tasks
from gza.config import Config
from gza.db import SqliteTaskStore
from gza.failure_reasons import mark_task_failed_from_cause
from gza.git import Git
from gza.workers import WorkerMetadata, WorkerRegistry
from tests.cli.conftest import make_store, run_gza, setup_config


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
        result = run_gza("iterate", str(impl.id), "--dry-run", "--project", str(tmp_path))

    assert result.returncode == 0
    assert "would iterate implementation" in result.stdout.lower()
    assert "first iteration 1/3 action: create_review" in result.stdout.lower()
    assert "code changed since the last review" in result.stdout.lower()


def test_cycle_dry_run(tmp_path) -> None:
    setup_config(tmp_path)
    _init_basic_repo(tmp_path)
    store = make_store(tmp_path)
    impl = _make_completed_impl(store)

    result = run_gza("iterate", str(impl.id), "--dry-run", "--project", str(tmp_path))

    assert result.returncode == 0
    assert "dry-run" in result.stdout.lower()


def test_cycle_uses_default_iterations_when_flag_omitted(tmp_path) -> None:
    (tmp_path / "gza.yaml").write_text("project_name: test-project\ndb_path: .gza/gza.db\n")
    _init_basic_repo(tmp_path)
    store = make_store(tmp_path)
    impl = _make_completed_impl(store)

    result = run_gza("iterate", str(impl.id), "--dry-run", "--project", str(tmp_path))

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
        result = run_gza("iterate", str(impl.id), "--dry-run", "--project", str(tmp_path))

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
    result = run_gza("mark-completed", str(task.id), "--project", str(tmp_path))

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

    result = run_gza("mark-completed", str(task.id), "--project", str(tmp_path))

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

    result = run_gza("mark-completed", str(task.id), "--project", str(tmp_path))

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

    result = run_gza(
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

    result = run_gza(
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

    result = run_gza("mark-completed", str(task.id), "--project", str(tmp_path))

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

    result = run_gza("mark-completed", str(task.id), "--project", str(tmp_path))

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
    result = run_gza("mark-completed", str(task.id), "--project", str(tmp_path))

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

    result = run_gza("advance", "--project", str(tmp_path))
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

    result = run_gza("advance", str(descendant.id), "--dry-run", "--project", str(tmp_path))
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
    result = run_gza("rebase", str(task.id), "--background", "--project", str(tmp_path))

    assert result.returncode == 1
    assert expected in result.stderr
    assert expected not in result.stdout
    assert "Error:" not in result.stdout
