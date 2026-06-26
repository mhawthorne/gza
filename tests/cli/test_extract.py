"""Fast unit tests for `gza extract` command wiring."""

import os
import threading
from argparse import Namespace
from pathlib import Path
from unittest.mock import MagicMock, patch

from gza.config import Config
from gza.concurrency import launch_permit, release_task_launch_permit, take_task_launch_permit
from gza.db import SqliteTaskStore, Task
from gza.extractions import ExtractionDraft, ExtractionError, FileDiffSummary, SourceSelection
from gza.git import Git

from .conftest import get_latest_task, make_store, setup_config


def _args(project_dir: Path, **overrides: object) -> Namespace:
    values: dict[str, object] = {
        "project_dir": project_dir,
        "no_docker": True,
        "max_turns": None,
        "source": None,
        "branch": None,
        "commits": None,
        "per_commit": False,
        "paths": (),
        "files_from": None,
        "prompt": None,
        "dry_run": False,
        "review": False,
        "create_pr": False,
        "branch_type": None,
        "model": None,
        "provider": None,
        "skip_learnings": False,
        "run": False,
        "background": False,
        "queue": False,
        "force": False,
        "base_branch": None,
        "tag": None,
        "tags": None,
        "no_tag": None,
        "any_tag": False,
    }
    values.update(overrides)
    return Namespace(**values)


def _draft(*, source: SourceSelection) -> ExtractionDraft:
    return ExtractionDraft(
        source=source,
        selected_paths=("src/extracted.py",),
        touched_paths=("src/extracted.py",),
        file_summaries=(
            FileDiffSummary(
                status="A",
                selected_path="src/extracted.py",
                old_path=None,
                new_path="src/extracted.py",
                additions=1,
                deletions=0,
                binary=False,
            ),
        ),
        patch=(
            "diff --git a/src/extracted.py b/src/extracted.py\n"
            "index e69de29..8c7e5a6 100644\n"
            "--- a/src/extracted.py\n"
            "+++ b/src/extracted.py\n"
            "@@ -0,0 +1 @@\n"
            "+print('seeded')\n"
        ),
        prompt="Carry over: Add extracted source module\n",
    )


def test_extract_branch_mode_treats_nonexistent_task_id_like_source_as_path(tmp_path: Path) -> None:
    from gza.cli.execution import cmd_extract

    setup_config(tmp_path)
    source = SourceSelection(
        source_task_id=None,
        source_branch="feature/source",
        source_base_ref="main",
    )
    draft = _draft(source=source)
    git = MagicMock(spec=Git)
    git.branch_exists.return_value = False
    bundle_dir = tmp_path / ".gza" / "extractions" / "20260427-target"

    with (
        patch("gza.cli.execution.Git", return_value=git),
        patch("gza.cli.execution.normalize_selected_paths", return_value=("testproject-9999",)) as mock_normalize,
        patch("gza.cli.execution.resolve_source_selection", return_value=source) as mock_resolve_source,
        patch("gza.cli.execution.plan_extraction", return_value=draft),
        patch("gza.cli.execution.generate_slug", return_value="20260427-target"),
        patch("gza.cli.execution.write_extraction_bundle", return_value=bundle_dir),
        patch("gza.cli.execution._print_extraction_plan_summary"),
    ):
        rc = cmd_extract(
            _args(
                tmp_path,
                source="testproject-9999",
                branch="feature/source",
                queue=True,
            )
        )

    assert rc == 0
    mock_normalize.assert_called_once_with(["testproject-9999"])
    mock_resolve_source.assert_called_once()
    assert mock_resolve_source.call_args.kwargs["source_task_id"] is None
    assert mock_resolve_source.call_args.kwargs["source_branch"] == "feature/source"


def test_extract_dry_run_uses_current_branch_when_no_source_selector(tmp_path: Path) -> None:
    from gza.cli.execution import cmd_extract

    setup_config(tmp_path)
    store = make_store(tmp_path)
    source = SourceSelection(
        source_task_id=None,
        source_branch="feature/current",
        source_base_ref="main",
    )
    draft = _draft(source=source)
    git = MagicMock(spec=Git)
    git.current_branch.return_value = "feature/current"

    with (
        patch("gza.cli.execution.Git", return_value=git),
        patch("gza.cli.execution.resolve_source_selection", return_value=source),
        patch("gza.cli.execution.infer_selected_paths", return_value=("src/extracted.py",)),
        patch("gza.cli.execution.plan_extraction", return_value=draft),
        patch("gza.cli.execution._print_extraction_plan_summary") as mock_print,
    ):
        rc = cmd_extract(_args(tmp_path, dry_run=True))

    assert rc == 0
    assert store.get_all() == []
    mock_print.assert_called_once()
    assert mock_print.call_args.kwargs["source_label"] == "branch feature/current"
    assert mock_print.call_args.kwargs["dry_run"] is True


def test_extract_defaults_to_queue_without_running_or_spawning(tmp_path: Path) -> None:
    from gza.cli.execution import cmd_extract

    setup_config(tmp_path)
    store = make_store(tmp_path)
    source = SourceSelection(
        source_task_id=None,
        source_branch="feature/source",
        source_base_ref="main",
    )
    draft = _draft(source=source)
    git = MagicMock(spec=Git)
    git.branch_exists.return_value = False

    with (
        patch("gza.cli.execution.Git", return_value=git),
        patch("gza.cli.execution.resolve_source_selection", return_value=source),
        patch("gza.cli.execution.normalize_selected_paths", return_value=("src/extracted.py",)),
        patch("gza.cli.execution.plan_extraction", return_value=draft),
        patch("gza.cli.execution._run_foreground", side_effect=AssertionError("should stay queued")),
        patch("gza.cli.execution._spawn_background_worker", side_effect=AssertionError("should stay queued")),
        patch("gza.cli.execution._spawn_background_workers", side_effect=AssertionError("should stay queued")),
    ):
        rc = cmd_extract(
            _args(
                tmp_path,
                branch="feature/source",
                paths=("src/extracted.py",),
            )
        )

    assert rc == 0
    created = get_latest_task(store, task_type="implement")
    assert created is not None
    assert created.status == "pending"


def test_extract_bundle_write_failure_marks_failed_task_when_delete_fails(tmp_path: Path) -> None:
    from gza.cli.execution import cmd_extract

    setup_config(tmp_path)
    store = make_store(tmp_path)
    source = SourceSelection(
        source_task_id=None,
        source_branch="feature/source",
        source_base_ref="main",
    )
    draft = _draft(source=source)
    git = MagicMock(spec=Git)
    git.branch_exists.return_value = False

    def _refuse_delete(self: SqliteTaskStore, task_id: str) -> bool:
        del task_id
        return False

    with (
        patch("gza.cli.execution.Git", return_value=git),
        patch("gza.cli.execution.resolve_source_selection", return_value=source),
        patch("gza.cli.execution.normalize_selected_paths", return_value=("src/extracted.py",)),
        patch("gza.cli.execution.plan_extraction", return_value=draft),
        patch("gza.cli.execution.generate_slug", return_value="20260427-target"),
        patch(
            "gza.cli.execution.write_extraction_bundle",
            side_effect=ExtractionError("bundle write failed"),
        ),
        patch.object(SqliteTaskStore, "delete", _refuse_delete),
    ):
        rc = cmd_extract(
            _args(
                tmp_path,
                branch="feature/source",
                paths=("src/extracted.py",),
                queue=True,
            )
        )

    assert rc == 1
    failed_task = get_latest_task(store, task_type="implement")
    assert failed_task is not None
    assert failed_task.status == "failed"
    assert failed_task.failure_reason == "EXTRACTION_BUNDLE_WRITE_FAILED"


def test_extract_per_commit_background_preserves_task_id_order_but_uses_parallel_spawner(tmp_path: Path) -> None:
    from gza.cli.execution import cmd_extract

    setup_config(tmp_path)
    source = SourceSelection(
        source_task_id=None,
        source_commits=("aaa111", "bbb222"),
        source_commit_subjects=("First commit", "Second commit"),
    )
    draft_one = _draft(
        source=SourceSelection(
            source_task_id=None,
            source_commits=("aaa111",),
            source_commit_subjects=("First commit",),
        )
    )
    draft_two = _draft(
        source=SourceSelection(
            source_task_id=None,
            source_commits=("bbb222",),
            source_commit_subjects=("Second commit",),
        )
    )
    git = MagicMock(spec=Git)
    created_tasks = [
        Task(id="gza-101", prompt="First extracted task"),
        Task(id="gza-102", prompt="Second extracted task"),
    ]

    with (
        patch("gza.cli.execution.Git", return_value=git),
        patch("gza.cli.execution.resolve_source_selection", return_value=source),
        patch("gza.cli.execution.infer_selected_paths", return_value=("src/extracted.py",)),
        patch("gza.cli.execution.plan_extraction", side_effect=[draft_one, draft_two]),
        patch(
            "gza.cli.execution._create_extract_task",
            side_effect=[(created_tasks[0], tmp_path / "bundle-one"), (created_tasks[1], tmp_path / "bundle-two")],
        ),
        patch("gza.cli.execution._print_extraction_plan_summary"),
        patch(
            "gza.cli.execution._prepare_task_for_reserved_launch",
            side_effect=lambda _config, task, **_kwargs: task,
        ),
        patch("gza.cli.execution._spawn_background_worker") as mock_spawn_one,
        patch("gza.cli.execution._spawn_background_workers", return_value=0) as mock_spawn_many,
    ):
        rc = cmd_extract(
            _args(
                tmp_path,
                commits=["aaa111", "bbb222"],
                per_commit=True,
                background=True,
            )
        )

    assert rc == 0
    mock_spawn_one.assert_not_called()
    mock_spawn_many.assert_called_once()
    worker_args = mock_spawn_many.call_args.args[0]
    assert worker_args.task_ids == ["gza-101", "gza-102"]
    prepared_tasks = mock_spawn_many.call_args.kwargs["prepared_tasks"]
    assert list(prepared_tasks) == ["gza-101", "gza-102"]
    assert prepared_tasks["gza-101"] is created_tasks[0]
    assert prepared_tasks["gza-102"] is created_tasks[1]


def test_extract_per_commit_background_refuses_before_creating_hidden_pending_tasks_when_capacity_is_short(
    tmp_path: Path,
    capsys,
) -> None:
    from gza.cli.execution import cmd_extract
    from gza.workers import WorkerRegistry

    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(config_path.read_text() + "max_concurrent: 1\n")
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    source = SourceSelection(
        source_task_id=None,
        source_commits=("aaa111", "bbb222"),
        source_commit_subjects=("First commit", "Second commit"),
    )
    drafts = [
        _draft(
            source=SourceSelection(
                source_task_id=None,
                source_commits=(commit,),
                source_commit_subjects=(subject,),
            )
        )
        for commit, subject in (
            ("aaa111", "First commit"),
            ("bbb222", "Second commit"),
        )
    ]
    git = MagicMock(spec=Git)
    created_ids: list[str] = []

    def create_task(**_kwargs):
        task = store.add(f"Extracted task {len(created_ids) + 1}", task_type="implement")
        assert task.id is not None
        created_ids.append(task.id)
        return task, tmp_path / ".gza" / "extractions" / f"bundle-{task.id}"

    def spawn_detached(_cmd, _config, worker_id):
        proc = MagicMock()
        proc.pid = os.getpid()
        return proc, f".gza/workers/{worker_id}-startup.log"

    with (
        patch("gza.cli.execution.Git", return_value=git),
        patch("gza.cli.execution.resolve_source_selection", return_value=source),
        patch("gza.cli.execution.infer_selected_paths", return_value=("src/extracted.py",)),
        patch("gza.cli.execution.normalize_selected_paths", return_value=("src/extracted.py",)),
        patch("gza.cli.execution.plan_extraction", side_effect=drafts),
        patch("gza.cli.execution._create_extract_task", side_effect=create_task),
        patch("gza.cli.execution._print_extraction_plan_summary"),
        patch("gza.cli._common._prepare_task_for_immediate_execution", side_effect=lambda _config, task, **_kwargs: task),
        patch("gza.cli._spawn_detached_worker_process", side_effect=spawn_detached) as mock_spawn,
    ):
        rc = cmd_extract(
            _args(
                tmp_path,
                commits=["aaa111", "bbb222"],
                per_commit=True,
                background=True,
            )
        )

    captured = capsys.readouterr()
    assert rc == 1
    assert mock_spawn.call_count == 0
    assert "already at max concurrent tasks:" in captured.err
    assert "limit is 1" in captured.err
    assert "Created extract implement task" not in captured.out

    assert created_ids == []
    tasks = {task.id: task for task in store.get_all() if task.id is not None}
    assert tasks == {}
    registry = WorkerRegistry(config.workers_path)
    workers = registry.list_all(include_completed=True)
    assert workers == []


def test_extract_foreground_refuses_before_creating_task_when_no_capacity(
    tmp_path: Path,
    capsys,
) -> None:
    from gza.cli.execution import cmd_extract

    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(config_path.read_text() + "max_concurrent: 1\n")
    store = make_store(tmp_path)
    source = SourceSelection(
        source_task_id=None,
        source_branch="feature/source",
        source_base_ref="main",
    )
    draft = _draft(source=source)
    git = MagicMock(spec=Git)
    git.branch_exists.return_value = False
    running_task = store.add("Already running task", task_type="implement")
    running_task.status = "in_progress"
    running_task.running_pid = os.getpid()
    store.update(running_task)

    with (
        patch("gza.cli.execution.Git", return_value=git),
        patch("gza.cli.execution.resolve_source_selection", return_value=source),
        patch("gza.cli.execution.normalize_selected_paths", return_value=("src/extracted.py",)),
        patch("gza.cli.execution.plan_extraction", return_value=draft),
        patch(
            "gza.cli.execution._run_foreground",
            side_effect=AssertionError("foreground run should not start without capacity"),
        ),
    ):
        rc = cmd_extract(
            _args(
                tmp_path,
                branch="feature/source",
                paths=("src/extracted.py",),
                run=True,
            )
        )

    captured = capsys.readouterr()
    assert rc == 1
    assert "already at max concurrent tasks: 1 running, limit is 1" in captured.out
    assert "Created extract implement task" not in captured.out
    tasks = store.get_all()
    assert [task.id for task in tasks] == [running_task.id]


def test_extract_per_commit_foreground_does_not_hold_future_reserved_permit_while_first_task_runs(
    tmp_path: Path,
) -> None:
    from gza.cli.execution import cmd_extract

    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(config_path.read_text() + "max_concurrent: 2\n")
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    source = SourceSelection(
        source_task_id=None,
        source_commits=("aaa111", "bbb222"),
        source_commit_subjects=("First commit", "Second commit"),
    )
    drafts = [
        _draft(
            source=SourceSelection(
                source_task_id=None,
                source_commits=(commit,),
                source_commit_subjects=(subject,),
            )
        )
        for commit, subject in (
            ("aaa111", "First commit"),
            ("bbb222", "Second commit"),
        )
    ]
    git = MagicMock(spec=Git)

    calls = {"count": 0}

    def fake_run_foreground(_config, task_id, *, prepared_task=None, **_kwargs) -> int:
        assert task_id is not None
        calls["count"] += 1

        if calls["count"] == 1:
            first_task = store.get(task_id)
            assert first_task is not None
            first_permit = take_task_launch_permit(task_id)
            assert first_permit is not None
            first_task.status = "in_progress"
            first_task.running_pid = os.getpid()
            store.update(first_task)
            first_permit.release()

            acquired = threading.Event()
            released = threading.Event()
            failure: list[BaseException] = []

            def _other_launcher() -> None:
                try:
                    permit = launch_permit(config, store)
                except BaseException as exc:  # pragma: no cover - failure asserted below
                    failure.append(exc)
                    return
                acquired.set()
                permit.release()
                released.set()

            thread = threading.Thread(target=_other_launcher)
            thread.start()
            thread.join(timeout=0.2)
            if thread.is_alive():
                if prepared_task is not None and prepared_task.id is not None:
                    release_task_launch_permit(str(prepared_task.id))
                thread.join(timeout=1)
                raise AssertionError("future extract task kept launch capacity locked during first foreground run")

            assert not failure
            assert acquired.is_set()
            assert released.is_set()
        else:
            permit = take_task_launch_permit(task_id)
            if permit is not None:
                permit.release()
        return 0

    with (
        patch("gza.cli.execution.Git", return_value=git),
        patch("gza.cli.execution.resolve_source_selection", return_value=source),
        patch("gza.cli.execution.normalize_selected_paths", return_value=("src/extracted.py",)),
        patch("gza.cli.execution.plan_extraction", side_effect=drafts),
        patch("gza.cli.execution.generate_slug", side_effect=["20260608-extract-first", "20260608-extract-second"]),
        patch("gza.cli.execution._run_foreground", side_effect=fake_run_foreground),
    ):
        rc = cmd_extract(
            _args(
                tmp_path,
                commits=["aaa111", "bbb222"],
                per_commit=True,
                paths=("src/extracted.py",),
                run=True,
            )
        )

    assert rc == 0


def test_extract_background_creator_phase_failure_removes_bundle_and_allows_retry(
    tmp_path: Path,
    capsys,
) -> None:
    from gza.cli.execution import cmd_extract

    setup_config(tmp_path)
    store = make_store(tmp_path)
    source = SourceSelection(
        source_task_id=None,
        source_branch="feature/source",
        source_base_ref="main",
    )
    draft = _draft(source=source)
    git = MagicMock(spec=Git)
    git.branch_exists.return_value = False
    fixed_slug = "20260427-target"
    bundle_dir = tmp_path / ".gza" / "extractions" / fixed_slug

    with (
        patch("gza.cli.execution.Git", return_value=git),
        patch("gza.cli.execution.resolve_source_selection", return_value=source),
        patch("gza.cli.execution.normalize_selected_paths", return_value=("src/extracted.py",)),
        patch("gza.cli.execution.plan_extraction", return_value=draft),
        patch("gza.cli.execution.generate_slug", return_value=fixed_slug),
        patch("gza.cli._common.prepare_task_startup_phase", side_effect=RuntimeError("creator boom")),
        patch(
            "gza.cli.execution._spawn_background_worker",
            side_effect=AssertionError("background worker should not spawn"),
        ),
    ):
        rc = cmd_extract(
            _args(
                tmp_path,
                branch="feature/source",
                paths=("src/extracted.py",),
                background=True,
            )
        )

    captured = capsys.readouterr()
    assert rc == 1
    assert "creator boom" in captured.err
    assert "Created extract implement task" not in captured.out
    assert store.get_all() == []
    assert not bundle_dir.exists()
    logs_dir = tmp_path / ".gza" / "logs"
    if logs_dir.exists():
        assert list(logs_dir.iterdir()) == []

    with (
        patch("gza.cli.execution.Git", return_value=git),
        patch("gza.cli.execution.resolve_source_selection", return_value=source),
        patch("gza.cli.execution.normalize_selected_paths", return_value=("src/extracted.py",)),
        patch("gza.cli.execution.plan_extraction", return_value=draft),
        patch("gza.cli.execution.generate_slug", return_value=fixed_slug),
    ):
        retry_rc = cmd_extract(
            _args(
                tmp_path,
                branch="feature/source",
                paths=("src/extracted.py",),
                queue=True,
            )
        )

    retry_captured = capsys.readouterr()
    assert retry_rc == 0
    assert retry_captured.out.count("Created extract implement task") == 1
    retried_tasks = store.get_all()
    assert len(retried_tasks) == 1
    assert "Created extract implement task" in retry_captured.out
    retried_task = get_latest_task(store, task_type="implement")
    assert retried_task is not None
    assert retried_task.slug == fixed_slug
    assert bundle_dir.exists()


def test_extract_background_slug_generation_failure_rolls_back_task_and_bundle(
    tmp_path: Path,
    capsys,
) -> None:
    from gza.cli.execution import cmd_extract

    setup_config(tmp_path)
    store = make_store(tmp_path)
    source = SourceSelection(
        source_task_id=None,
        source_branch="feature/source",
        source_base_ref="main",
    )
    draft = _draft(source=source)
    git = MagicMock(spec=Git)
    git.branch_exists.return_value = False

    with (
        patch("gza.cli.execution.Git", return_value=git),
        patch("gza.cli.execution.resolve_source_selection", return_value=source),
        patch("gza.cli.execution.normalize_selected_paths", return_value=("src/extracted.py",)),
        patch("gza.cli.execution.plan_extraction", return_value=draft),
        patch("gza.cli.execution.generate_slug", side_effect=RuntimeError("slug boom")),
        patch(
            "gza.cli.execution._spawn_background_worker",
            side_effect=AssertionError("background worker should not spawn"),
        ),
    ):
        rc = cmd_extract(
            _args(
                tmp_path,
                branch="feature/source",
                paths=("src/extracted.py",),
                background=True,
            )
        )

    captured = capsys.readouterr()
    assert rc == 1
    assert "slug boom" in captured.err
    assert "Created extract implement task" not in captured.out
    assert store.get_all() == []
    extraction_root = tmp_path / ".gza" / "extractions"
    if extraction_root.exists():
        assert list(extraction_root.iterdir()) == []
    logs_dir = tmp_path / ".gza" / "logs"
    if logs_dir.exists():
        assert list(logs_dir.iterdir()) == []
    workers_dir = tmp_path / ".gza" / "workers"
    if workers_dir.exists():
        assert list(workers_dir.iterdir()) == []


def test_extract_per_commit_background_prepares_every_created_task_before_spawning_workers(
    tmp_path: Path,
    capsys,
) -> None:
    from gza.cli.execution import cmd_extract

    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(config_path.read_text() + "max_concurrent: 3\n")
    store = make_store(tmp_path)
    source = SourceSelection(
        source_task_id=None,
        source_commits=("aaa111", "bbb222", "ccc333"),
        source_commit_subjects=("First commit", "Second commit", "Third commit"),
    )
    drafts = [
        _draft(
            source=SourceSelection(
                source_task_id=None,
                source_commits=(commit,),
                source_commit_subjects=(subject,),
            )
        )
        for commit, subject in (
            ("aaa111", "First commit"),
            ("bbb222", "Second commit"),
            ("ccc333", "Third commit"),
        )
    ]
    git = MagicMock(spec=Git)
    fixed_slugs = [
        "20260510-extract-first",
        "20260510-extract-second",
        "20260510-extract-third",
    ]
    prepare_calls = {"count": 0}

    def prepare_once(_config, _store, task):
        prepare_calls["count"] += 1
        return task

    with (
        patch("gza.cli.execution.Git", return_value=git),
        patch("gza.cli.execution.resolve_source_selection", return_value=source),
        patch("gza.cli.execution.normalize_selected_paths", return_value=("src/extracted.py",)),
        patch("gza.cli.execution.plan_extraction", side_effect=drafts),
        patch("gza.cli.execution.generate_slug", side_effect=fixed_slugs),
        patch("gza.cli._common.prepare_task_startup_phase", side_effect=prepare_once),
        patch("gza.cli.execution._spawn_background_workers", return_value=0) as mock_spawn_many,
    ):
        rc = cmd_extract(
            _args(
                tmp_path,
                commits=["aaa111", "bbb222", "ccc333"],
                per_commit=True,
                paths=("src/extracted.py",),
                background=True,
            )
        )

    captured = capsys.readouterr()
    assert rc == 0
    assert captured.err == ""
    assert captured.out.count("Created extract implement task") == 3
    assert prepare_calls["count"] == 3
    mock_spawn_many.assert_called_once()
    prepared_tasks = mock_spawn_many.call_args.kwargs["prepared_tasks"]
    assert list(prepared_tasks) == [task.id for task in sorted(store.get_all(), key=lambda task: task.id or "")]
    assert len(store.get_all()) == 3


def test_extract_per_commit_background_slug_generation_failure_rolls_back_prior_tasks(
    tmp_path: Path,
    capsys,
) -> None:
    from gza.cli.execution import cmd_extract

    setup_config(tmp_path)
    store = make_store(tmp_path)
    source = SourceSelection(
        source_task_id=None,
        source_commits=("aaa111", "bbb222"),
        source_commit_subjects=("First commit", "Second commit"),
    )
    drafts = [
        _draft(
            source=SourceSelection(
                source_task_id=None,
                source_commits=(commit,),
                source_commit_subjects=(subject,),
            )
        )
        for commit, subject in (
            ("aaa111", "First commit"),
            ("bbb222", "Second commit"),
        )
    ]
    git = MagicMock(spec=Git)
    first_slug = "20260511-extract-first"
    first_bundle_dir = tmp_path / ".gza" / "extractions" / first_slug

    with (
        patch("gza.cli.execution.Git", return_value=git),
        patch("gza.cli.execution.resolve_source_selection", return_value=source),
        patch("gza.cli.execution.normalize_selected_paths", return_value=("src/extracted.py",)),
        patch("gza.cli.execution.plan_extraction", side_effect=drafts),
        patch("gza.cli.execution.generate_slug", side_effect=[first_slug, RuntimeError("slug boom")]),
        patch(
            "gza.cli.execution._spawn_background_workers",
            side_effect=AssertionError("background workers should not spawn"),
        ),
    ):
        rc = cmd_extract(
            _args(
                tmp_path,
                commits=["aaa111", "bbb222"],
                per_commit=True,
                paths=("src/extracted.py",),
                background=True,
            )
        )

    captured = capsys.readouterr()
    assert rc == 1
    assert "slug boom" in captured.err
    assert "Created extract implement task" not in captured.out
    assert store.get_all() == []
    assert not first_bundle_dir.exists()
    extraction_root = tmp_path / ".gza" / "extractions"
    if extraction_root.exists():
        assert list(extraction_root.iterdir()) == []
    logs_dir = tmp_path / ".gza" / "logs"
    if logs_dir.exists():
        assert list(logs_dir.iterdir()) == []
    workers_dir = tmp_path / ".gza" / "workers"
    if workers_dir.exists():
        assert list(workers_dir.iterdir()) == []


def test_extract_per_commit_background_reuses_prepared_tasks_without_second_prepare(
    tmp_path: Path,
) -> None:
    from gza.cli.execution import cmd_extract
    from gza.workers import WorkerRegistry

    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    source = SourceSelection(
        source_task_id=None,
        source_commits=("aaa111", "bbb222"),
        source_commit_subjects=("First commit", "Second commit"),
    )
    drafts = [
        _draft(
            source=SourceSelection(
                source_task_id=None,
                source_commits=(commit,),
                source_commit_subjects=(subject,),
            )
        )
        for commit, subject in (
            ("aaa111", "First commit"),
            ("bbb222", "Second commit"),
        )
    ]
    git = MagicMock(spec=Git)
    fixed_slugs = [
        "20260510-extract-first",
        "20260510-extract-second",
    ]

    prepare_calls = {"count": 0}
    proc_counter = {"pid": 47000}

    def prepare_once(_config, _store, task):
        prepare_calls["count"] += 1
        if prepare_calls["count"] > len(drafts):
            raise AssertionError("background extract should not prepare tasks twice")
        return task

    def spawn_detached(_cmd, _config, worker_id):
        proc_counter["pid"] += 1
        proc = MagicMock()
        proc.pid = proc_counter["pid"]
        return proc, f".gza/workers/{worker_id}-startup.log"

    with (
        patch("gza.cli.execution.Git", return_value=git),
        patch("gza.cli.execution.resolve_source_selection", return_value=source),
        patch("gza.cli.execution.normalize_selected_paths", return_value=("src/extracted.py",)),
        patch("gza.cli.execution.plan_extraction", side_effect=drafts),
        patch("gza.cli.execution.generate_slug", side_effect=fixed_slugs),
        patch("gza.cli._common.prepare_task_startup_phase", side_effect=prepare_once),
        patch("gza.cli._spawn_detached_worker_process", side_effect=spawn_detached),
    ):
        rc = cmd_extract(
            _args(
                tmp_path,
                commits=["aaa111", "bbb222"],
                per_commit=True,
                paths=("src/extracted.py",),
                background=True,
            )
        )

    assert rc == 0
    assert prepare_calls["count"] == len(drafts)
    created_tasks = sorted(store.get_all(), key=lambda task: task.id)
    assert len(created_tasks) == 2
    registry = WorkerRegistry(config.workers_path)
    workers = sorted(registry.list_all(include_completed=True), key=lambda worker: worker.task_id or "")
    assert [worker.task_id for worker in workers] == [task.id for task in created_tasks]
