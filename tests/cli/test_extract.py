"""Command-level tests for `gza extract`."""

import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from gza.db import Task
from gza.extractions import ExtractionError
from gza.git import Git
from gza.providers import RunResult
from gza.runner import EXTRACTION_PRECHECK_FAILURE_REASON

from .conftest import get_latest_task, make_store, run_gza, setup_config


def _init_repo(tmp_path: Path) -> Git:
    git = Git(tmp_path)
    git._run("init", "-b", "main")
    git._run("config", "user.name", "Test User")
    git._run("config", "user.email", "test@example.com")
    (tmp_path / "README.md").write_text("base\n")
    git._run("add", "README.md")
    git._run("commit", "-m", "initial")
    return git


def _create_completed_source_task(tmp_path: Path, git: Git) -> Task:
    store = make_store(tmp_path)
    task = store.add("Add extracted source module", task_type="implement")
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/source"
    task.slug = "20260427-test-source"
    store.update(task)

    git._run("checkout", "-b", task.branch)
    (tmp_path / "src").mkdir(exist_ok=True)
    (tmp_path / "src" / "extracted.py").write_text("print('seeded')\n")
    git._run("add", "src/extracted.py")
    git._run("commit", "-m", "source change")
    git._run("checkout", "main")

    return task


def test_extract_rejects_conflicting_source_selectors(tmp_path: Path) -> None:
    setup_config(tmp_path)
    git = _init_repo(tmp_path)
    source_task = _create_completed_source_task(tmp_path, git)

    with_both = run_gza(
        "extract",
        str(source_task.id),
        "src/file.py",
        "--branch",
        source_task.branch,
        "--project",
        str(tmp_path),
    )
    assert with_both.returncode == 1
    assert "exactly one source selector" in with_both.stdout


def test_extract_help_includes_source_selectors_and_file_inputs(tmp_path: Path) -> None:
    setup_config(tmp_path)
    result = run_gza("extract", "--help", "--project", str(tmp_path))
    assert result.returncode == 0
    assert "Source full prefixed task ID to extract from" in result.stdout
    assert "--branch BRANCH" in result.stdout
    assert "--files-from FILE" in result.stdout
    assert "current branch" in result.stdout
    assert "Repo-relative files to extract from the source diff" in result.stdout
    assert "omit to extract all changed files" in result.stdout


def test_extract_without_selected_files_uses_full_source_diff(tmp_path: Path) -> None:
    setup_config(tmp_path)
    git = _init_repo(tmp_path)
    source_task = _create_completed_source_task(tmp_path, git)
    store = make_store(tmp_path)

    result = run_gza("extract", str(source_task.id), "-q", "--project", str(tmp_path))
    assert result.returncode == 0
    assert "Created extract implement task" in result.stdout
    assert "Selected files: 1" in result.stdout

    new_task = get_latest_task(store, task_type="implement")
    assert new_task is not None
    bundle_dir = tmp_path / ".gza" / "extractions" / new_task.slug
    manifest = json.loads((bundle_dir / "manifest.json").read_text())
    assert manifest["selected_paths"] == ["src/extracted.py"]
    assert manifest["touched_paths"] == ["src/extracted.py"]


def test_extract_without_source_selector_uses_current_branch(tmp_path: Path) -> None:
    setup_config(tmp_path)
    git = _init_repo(tmp_path)
    source_task = _create_completed_source_task(tmp_path, git)
    store = make_store(tmp_path)

    git._run("checkout", source_task.branch)

    result = run_gza("extract", "-q", "--project", str(tmp_path))

    assert result.returncode == 0
    assert f"Source: branch {source_task.branch}" in result.stdout
    assert "Selected files: 1" in result.stdout

    new_task = get_latest_task(store, task_type="implement")
    assert new_task is not None
    bundle_dir = tmp_path / ".gza" / "extractions" / new_task.slug
    manifest = json.loads((bundle_dir / "manifest.json").read_text())
    assert manifest["selected_paths"] == ["src/extracted.py"]


def test_extract_current_branch_with_first_positional_path_uses_path_not_source(tmp_path: Path) -> None:
    setup_config(tmp_path)
    git = _init_repo(tmp_path)
    source_task = _create_completed_source_task(tmp_path, git)
    store = make_store(tmp_path)

    git._run("checkout", source_task.branch)

    result = run_gza(
        "extract",
        "src/extracted.py",
        "-q",
        "--project",
        str(tmp_path),
    )

    assert result.returncode == 0
    assert f"Source: branch {source_task.branch}" in result.stdout
    assert "Selected files: 1" in result.stdout

    new_task = get_latest_task(store, task_type="implement")
    assert new_task is not None
    bundle_dir = tmp_path / ".gza" / "extractions" / new_task.slug
    manifest = json.loads((bundle_dir / "manifest.json").read_text())
    assert manifest["selected_paths"] == ["src/extracted.py"]


def test_extract_files_from_directory_reports_error_without_traceback(tmp_path: Path) -> None:
    setup_config(tmp_path)
    git = _init_repo(tmp_path)
    source_task = _create_completed_source_task(tmp_path, git)

    paths_dir = tmp_path / "selected-paths"
    paths_dir.mkdir()

    result = run_gza(
        "extract",
        str(source_task.id),
        "--files-from",
        str(paths_dir),
        "--queue",
        "--project",
        str(tmp_path),
    )

    assert result.returncode == 1
    assert "Error: Unable to read --files-from path" in result.stdout
    assert "Traceback" not in result.stdout
    assert "Traceback" not in result.stderr


def test_extract_files_from_decode_error_reports_error_without_traceback(tmp_path: Path) -> None:
    setup_config(tmp_path)
    git = _init_repo(tmp_path)
    source_task = _create_completed_source_task(tmp_path, git)

    paths_file = tmp_path / "paths.txt"
    paths_file.write_bytes(b"\xff\xfe")

    result = run_gza(
        "extract",
        str(source_task.id),
        "--files-from",
        str(paths_file),
        "--queue",
        "--project",
        str(tmp_path),
    )

    assert result.returncode == 1
    assert "Error: Unable to read --files-from path" in result.stdout
    assert "Traceback" not in result.stdout
    assert "Traceback" not in result.stderr


def test_extract_rejects_non_code_source_task(tmp_path: Path) -> None:
    setup_config(tmp_path)
    git = _init_repo(tmp_path)
    store = make_store(tmp_path)

    non_code = store.add("Exploration output", task_type="plan")
    non_code.status = "completed"
    non_code.completed_at = datetime.now(UTC)
    non_code.branch = "feature/plan-source"
    non_code.slug = "20260427-plan-source"
    store.update(non_code)

    git._run("checkout", "-b", non_code.branch)
    (tmp_path / "notes.txt").write_text("plan notes\n")
    git._run("add", "notes.txt")
    git._run("commit", "-m", "plan notes")
    git._run("checkout", "main")

    result = run_gza(
        "extract",
        str(non_code.id),
        "notes.txt",
        "--queue",
        "--project",
        str(tmp_path),
    )
    assert result.returncode == 1
    assert "must be a code task" in result.stdout


def test_extract_creates_implement_task_and_bundle(tmp_path: Path) -> None:
    setup_config(tmp_path)
    git = _init_repo(tmp_path)
    source_task = _create_completed_source_task(tmp_path, git)
    store = make_store(tmp_path)

    result = run_gza(
        "extract",
        str(source_task.id),
        "src/extracted.py",
        "--prompt",
        "Complete the carry-over and verify tests.",
        "--queue",
        "--project",
        str(tmp_path),
    )

    assert result.returncode == 0
    assert "Created extract implement task" in result.stdout
    assert f"Source: task {source_task.id}" in result.stdout
    assert "Selected files: 1" in result.stdout

    new_task = get_latest_task(store, task_type="implement")
    assert new_task is not None
    assert new_task.id != source_task.id
    assert new_task.slug is not None
    assert new_task.prompt.startswith("Carry over: Add extracted source module\n")
    assert "Operator intent:" in new_task.prompt
    assert new_task.slug.endswith("carry-over-add-extracted-source-module")

    bundle_dir = tmp_path / ".gza" / "extractions" / new_task.slug
    assert (bundle_dir / "manifest.json").exists()
    assert (bundle_dir / "selected.patch").exists()
    assert (bundle_dir / "prompt.md").exists()

    manifest = (bundle_dir / "manifest.json").read_text()
    assert f'"source_task_id": "{source_task.id}"' in manifest
    assert '"selected_paths": [' in manifest
    assert '"src/extracted.py"' in manifest


def test_extract_branch_with_single_positional_path_succeeds(tmp_path: Path) -> None:
    setup_config(tmp_path)
    git = _init_repo(tmp_path)
    source_task = _create_completed_source_task(tmp_path, git)
    store = make_store(tmp_path)

    result = run_gza(
        "extract",
        "--branch",
        source_task.branch,
        "src/extracted.py",
        "--queue",
        "--project",
        str(tmp_path),
    )

    assert result.returncode == 0
    assert "Created extract implement task" in result.stdout
    assert f"Source: branch {source_task.branch}" in result.stdout
    assert "Selected files: 1" in result.stdout

    new_task = get_latest_task(store, task_type="implement")
    assert new_task is not None
    bundle_dir = tmp_path / ".gza" / "extractions" / new_task.slug
    manifest = json.loads((bundle_dir / "manifest.json").read_text())
    assert manifest["selected_paths"] == ["src/extracted.py"]


def test_extract_branch_without_selected_files_uses_full_source_diff(tmp_path: Path) -> None:
    setup_config(tmp_path)
    git = _init_repo(tmp_path)
    source_task = _create_completed_source_task(tmp_path, git)
    store = make_store(tmp_path)

    git._run("checkout", source_task.branch)
    (tmp_path / "src" / "second.py").write_text("print('second')\n")
    git._run("add", "src/second.py")
    git._run("commit", "-m", "add second file")
    git._run("checkout", "main")

    result = run_gza(
        "extract",
        "--branch",
        source_task.branch,
        "-q",
        "--project",
        str(tmp_path),
    )

    assert result.returncode == 0
    assert "Selected files: 2" in result.stdout

    new_task = get_latest_task(store, task_type="implement")
    assert new_task is not None
    bundle_dir = tmp_path / ".gza" / "extractions" / new_task.slug
    manifest = json.loads((bundle_dir / "manifest.json").read_text())
    assert manifest["selected_paths"] == ["src/extracted.py", "src/second.py"]


def test_extract_branch_with_multiple_positional_paths_preserves_all(tmp_path: Path) -> None:
    setup_config(tmp_path)
    git = _init_repo(tmp_path)
    source_task = _create_completed_source_task(tmp_path, git)
    store = make_store(tmp_path)

    git._run("checkout", source_task.branch)
    (tmp_path / "src" / "second.py").write_text("print('second')\n")
    git._run("add", "src/second.py")
    git._run("commit", "-m", "add second file")
    git._run("checkout", "main")

    result = run_gza(
        "extract",
        "--branch",
        source_task.branch,
        "src/extracted.py",
        "src/second.py",
        "--queue",
        "--project",
        str(tmp_path),
    )

    assert result.returncode == 0
    assert "Selected files: 2" in result.stdout

    new_task = get_latest_task(store, task_type="implement")
    assert new_task is not None
    bundle_dir = tmp_path / ".gza" / "extractions" / new_task.slug
    manifest = json.loads((bundle_dir / "manifest.json").read_text())
    assert manifest["selected_paths"] == ["src/extracted.py", "src/second.py"]


def test_extract_branch_with_nonexistent_task_id_like_path_treats_it_as_path(tmp_path: Path) -> None:
    setup_config(tmp_path)
    git = _init_repo(tmp_path)
    source_task = _create_completed_source_task(tmp_path, git)
    store = make_store(tmp_path)

    git._run("checkout", source_task.branch)
    (tmp_path / "testproject-9999").write_text("path that looks like an id\n")
    git._run("add", "testproject-9999")
    git._run("commit", "-m", "add id-like file path")
    git._run("checkout", "main")

    result = run_gza(
        "extract",
        "--branch",
        source_task.branch,
        "testproject-9999",
        "--queue",
        "--project",
        str(tmp_path),
    )

    assert result.returncode == 0
    assert "Selected files: 1" in result.stdout

    new_task = get_latest_task(store, task_type="implement")
    assert new_task is not None
    bundle_dir = tmp_path / ".gza" / "extractions" / new_task.slug
    manifest = json.loads((bundle_dir / "manifest.json").read_text())
    assert manifest["selected_paths"] == ["testproject-9999"]


@pytest.mark.parametrize(
    "error",
    [ExtractionError("bundle write failed"), OSError("disk full")],
)
def test_extract_bundle_write_failure_does_not_leave_pending_task(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    error: Exception,
) -> None:
    setup_config(tmp_path)
    git = _init_repo(tmp_path)
    source_task = _create_completed_source_task(tmp_path, git)
    store = make_store(tmp_path)

    def _raise_bundle_error(*, project_dir: Path, task: Task, draft: object) -> Path:
        raise error

    monkeypatch.setattr("gza.cli.execution.write_extraction_bundle", _raise_bundle_error)

    result = run_gza(
        "extract",
        str(source_task.id),
        "src/extracted.py",
        "--queue",
        "--project",
        str(tmp_path),
    )

    assert result.returncode == 1
    assert "Error:" in result.stdout
    assert "bundle write failed" in result.stdout or "disk full" in result.stdout
    assert store.get_pending() == []

    no_pending = run_gza("work", "--no-docker", "--project", str(tmp_path))
    assert no_pending.returncode == 0
    assert "No pending tasks found" in no_pending.stdout


def test_extract_queued_duplicates_get_distinct_slugs_and_bundle_dirs(tmp_path: Path) -> None:
    setup_config(tmp_path)
    git = _init_repo(tmp_path)
    source_task = _create_completed_source_task(tmp_path, git)
    store = make_store(tmp_path)

    first = run_gza(
        "extract",
        str(source_task.id),
        "src/extracted.py",
        "--queue",
        "--project",
        str(tmp_path),
    )
    second = run_gza(
        "extract",
        str(source_task.id),
        "src/extracted.py",
        "--queue",
        "--project",
        str(tmp_path),
    )

    assert first.returncode == 0
    assert second.returncode == 0

    extract_tasks = [
        task
        for task in store.get_pending()
        if task.task_type == "implement" and task.prompt.startswith("Carry over: Add extracted source module\n")
    ]
    assert len(extract_tasks) == 2
    first_task, second_task = sorted(extract_tasks, key=lambda task: task.id or "")
    assert first_task.slug is not None
    assert second_task.slug is not None
    assert first_task.slug != second_task.slug

    first_bundle = tmp_path / ".gza" / "extractions" / first_task.slug
    second_bundle = tmp_path / ".gza" / "extractions" / second_task.slug
    assert first_bundle != second_bundle
    assert first_bundle.exists()
    assert second_bundle.exists()

    first_manifest = json.loads((first_bundle / "manifest.json").read_text())
    second_manifest = json.loads((second_bundle / "manifest.json").read_text())
    assert first_manifest["target_task_id"] == first_task.id
    assert first_manifest["target_slug"] == first_task.slug
    assert second_manifest["target_task_id"] == second_task.id
    assert second_manifest["target_slug"] == second_task.slug


def test_extract_queued_duplicates_run_without_extraction_identity_collision(tmp_path: Path) -> None:
    setup_config(tmp_path)
    git = _init_repo(tmp_path)
    source_task = _create_completed_source_task(tmp_path, git)
    store = make_store(tmp_path)

    run_gza(
        "extract",
        str(source_task.id),
        "src/extracted.py",
        "--queue",
        "--project",
        str(tmp_path),
    )
    run_gza(
        "extract",
        str(source_task.id),
        "src/extracted.py",
        "--queue",
        "--project",
        str(tmp_path),
    )

    mock_provider = Mock()
    mock_provider.name = "TestProvider"
    mock_provider.check_credentials.return_value = True
    mock_provider.verify_credentials.return_value = True
    mock_provider.run.return_value = RunResult(
        exit_code=0,
        duration_seconds=1.0,
        num_turns_reported=1,
        cost_usd=0.01,
        error_type=None,
    )

    with (
        patch("gza.runner.get_provider", return_value=mock_provider),
        patch("gza.runner.get_effective_config_for_task", return_value=("", "claude", 50)),
        patch("gza.runner.load_dotenv"),
    ):
        result = run_gza(
            "work",
            "--count",
            "2",
            "--no-docker",
            "--project",
            str(tmp_path),
        )

    assert result.returncode == 0
    assert "Extraction bundle target identity mismatch" not in result.stdout

    extract_tasks = [
        task
        for task in store.get_all()
        if task.task_type == "implement" and task.prompt.startswith("Carry over: Add extracted source module\n")
    ]
    assert len(extract_tasks) == 2
    for task in extract_tasks:
        assert task.failure_reason != EXTRACTION_PRECHECK_FAILURE_REASON
