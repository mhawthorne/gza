"""Focused tests for extraction seeding in runner code-task flow."""

import json
from pathlib import Path
from unittest.mock import Mock, patch

from gza.config import Config
from gza.db import SqliteTaskStore, TaskStats
from gza.git import GitError
from gza.providers import RunResult
from gza.runner import (
    EXTRACTION_PRECHECK_FAILURE_REASON,
    _complete_code_task,
    _seed_extraction_bundle_if_present,
    run,
)


def _build_config(tmp_path: Path, db_path: Path) -> Config:
    config = Mock(spec=Config)
    config.project_dir = tmp_path
    config.db_path = db_path
    config.log_path = tmp_path / "logs"
    config.log_path.mkdir(parents=True, exist_ok=True)
    config.worktree_path = tmp_path / "worktrees"
    config.worktree_path.mkdir(parents=True, exist_ok=True)
    config.workers_path = tmp_path / ".gza" / "workers"
    config.workers_path.mkdir(parents=True, exist_ok=True)
    config.use_docker = False
    config.timeout_minutes = 10
    config.max_steps = 50
    config.max_turns = 50
    config.model = ""
    config.branch_mode = "multi"
    config.project_name = "test-project"
    config.project_prefix = "testproject"
    config.branch_strategy = Mock()
    config.branch_strategy.pattern = "{project}/{task_slug}"
    config.branch_strategy.default_type = "feature"
    config.get_provider_for_task.return_value = "claude"
    config.get_reasoning_effort_for_task.return_value = ""
    config.chat_text_display_length = 120
    config.learnings_interval = 0
    config.learnings_window = 25
    config.claude = Mock(args=[])
    config.tmux = Mock(session_name=None)
    return config


def test_seed_extraction_bundle_applies_patch_and_returns_paths(tmp_path: Path) -> None:
    task_store = SqliteTaskStore(tmp_path / "test.db", prefix="testproject")
    task = task_store.add("Extracted task", task_type="implement")
    task.slug = "20260427-target"
    task_store.update(task)

    project_bundle = tmp_path / ".gza" / "extractions" / "20260427-target"
    project_bundle.mkdir(parents=True, exist_ok=True)
    (project_bundle / "manifest.json").write_text(
        json.dumps(
            {
                "version": 1,
                "target_task_id": task.id,
                "target_slug": "20260427-target",
                "selected_paths": ["src/file.py"],
                "touched_paths": ["src/file.py"],
                "patch_path": "selected.patch",
            }
        )
    )
    (project_bundle / "selected.patch").write_text(
        "diff --git a/src/file.py b/src/file.py\n"
        "index e69de29..8c7e5a6 100644\n"
        "--- a/src/file.py\n"
        "+++ b/src/file.py\n"
        "@@ -0,0 +1 @@\n"
        "+print('hello')\n"
    )
    (project_bundle / "prompt.md").write_text("seed prompt\n")

    worktree = tmp_path / "worktree"
    worktree.mkdir()
    log_file = tmp_path / "logs" / "task.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)

    config = Mock(spec=Config)
    config.project_dir = tmp_path

    worktree_git = Mock()

    seeded = _seed_extraction_bundle_if_present(
        task,
        config,
        worktree,
        worktree_git,
        log_file,
        resume=False,
    )

    assert seeded == {"src/file.py"}
    worktree_git.apply_patch_check.assert_called_once()
    worktree_git.apply_patch_file.assert_called_once()


def test_seed_extraction_bundle_accepts_quoted_diff_headers(tmp_path: Path) -> None:
    task_store = SqliteTaskStore(tmp_path / "test.db", prefix="testproject")
    task = task_store.add("Extracted task", task_type="implement")
    task.slug = "20260427-target-quoted"
    task_store.update(task)

    project_bundle = tmp_path / ".gza" / "extractions" / task.slug
    project_bundle.mkdir(parents=True, exist_ok=True)
    (project_bundle / "manifest.json").write_text(
        json.dumps(
            {
                "version": 1,
                "target_task_id": task.id,
                "target_slug": task.slug,
                "selected_paths": ["src/file with space.py"],
                "touched_paths": ["src/file with space.py"],
                "patch_path": "selected.patch",
            }
        )
    )
    (project_bundle / "selected.patch").write_text(
        'diff --git "a/src/file with space.py" "b/src/file with space.py"\n'
        "index e69de29..8c7e5a6 100644\n"
        "--- a/src/file with space.py\n"
        "+++ b/src/file with space.py\n"
        "@@ -0,0 +1 @@\n"
        "+print('hello')\n"
    )
    (project_bundle / "prompt.md").write_text("seed prompt\n")

    worktree = tmp_path / "worktree"
    worktree.mkdir()
    log_file = tmp_path / "logs" / "task.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)

    config = Mock(spec=Config)
    config.project_dir = tmp_path
    worktree_git = Mock()

    seeded = _seed_extraction_bundle_if_present(
        task,
        config,
        worktree,
        worktree_git,
        log_file,
        resume=False,
    )

    assert seeded == {"src/file with space.py"}
    worktree_git.apply_patch_check.assert_called_once()
    worktree_git.apply_patch_file.assert_called_once()


def test_complete_code_task_stages_seeded_paths_even_without_provider_edits(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db", prefix="testproject")
    task = store.add("Extracted task", task_type="implement")
    task.slug = "20260427-seeded"
    task.status = "in_progress"
    store.update(task)

    config = Mock(spec=Config)
    config.project_dir = tmp_path
    config.log_path = tmp_path / "logs"
    config.log_path.mkdir(parents=True, exist_ok=True)

    log_file = config.log_path / "seeded.log"
    worktree_summary_path = tmp_path / "worktree-summary.md"
    worktree_summary_path.write_text("# Summary\n")
    summary_path = tmp_path / ".gza" / "summaries" / "seeded.md"

    worktree_git = Mock()
    worktree_git.status_porcelain.return_value = {("M", "src/file.py")}
    worktree_git.default_branch.return_value = "main"
    worktree_git.get_diff_numstat.return_value = "1\t1\tsrc/file.py\n"
    worktree_git._run.return_value = Mock(stdout="", returncode=0, stderr="")

    with patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None):
        rc = _complete_code_task(
            task,
            config,
            store,
            worktree_git,
            log_file,
            "feature/seeded",
            TaskStats(duration_seconds=1.0, num_steps_computed=1, cost_usd=0.0),
            0,
            pre_run_status={("M", "src/file.py")},
            worktree_summary_path=worktree_summary_path,
            summary_path=summary_path,
            summary_dir=summary_path.parent,
            seeded_paths={"src/file.py"},
        )

    assert rc == 0
    worktree_git.add.assert_any_call("src/file.py")
    assert worktree_git.commit.call_count == 1


def test_complete_code_task_ignores_missing_seeded_paths_without_pathspec_crash(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db", prefix="testproject")
    task = store.add("Extracted task", task_type="implement")
    task.slug = "20260427-seeded-missing"
    task.status = "in_progress"
    store.update(task)

    config = Mock(spec=Config)
    config.project_dir = tmp_path
    config.log_path = tmp_path / "logs"
    config.log_path.mkdir(parents=True, exist_ok=True)

    log_file = config.log_path / "seeded-missing.log"
    worktree_summary_path = tmp_path / "worktree-summary.md"
    worktree_summary_path.write_text("# Summary\n")
    summary_path = tmp_path / ".gza" / "summaries" / "seeded-missing.md"

    worktree_git = Mock()
    worktree_git.repo_dir = tmp_path
    worktree_git.status_porcelain.return_value = set()
    worktree_git.default_branch.return_value = "main"
    worktree_git.count_commits_ahead.return_value = 0

    with patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None):
        rc = _complete_code_task(
            task,
            config,
            store,
            worktree_git,
            log_file,
            "feature/seeded-missing",
            TaskStats(duration_seconds=1.0, num_steps_computed=1, cost_usd=0.0),
            0,
            pre_run_status=set(),
            worktree_summary_path=worktree_summary_path,
            summary_path=summary_path,
            summary_dir=summary_path.parent,
            seeded_paths={"src/seeded.py"},
        )

    assert rc == 0
    worktree_git.add.assert_not_called()
    worktree_git.commit.assert_not_called()
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.status == "failed"
    assert refreshed.failure_reason == "UNKNOWN"


def test_complete_code_task_does_not_commit_when_seeded_paths_reverted_to_clean(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db", prefix="testproject")
    task = store.add("Extracted task", task_type="implement")
    task.slug = "20260427-seeded-clean"
    task.status = "in_progress"
    store.update(task)

    seeded_file = tmp_path / "src" / "seeded.py"
    seeded_file.parent.mkdir(parents=True, exist_ok=True)
    seeded_file.write_text("print('clean')\n")

    config = Mock(spec=Config)
    config.project_dir = tmp_path
    config.log_path = tmp_path / "logs"
    config.log_path.mkdir(parents=True, exist_ok=True)

    log_file = config.log_path / "seeded-clean.log"
    worktree_summary_path = tmp_path / "worktree-summary.md"
    worktree_summary_path.write_text("# Summary\n")
    summary_path = tmp_path / ".gza" / "summaries" / "seeded-clean.md"

    worktree_git = Mock()
    worktree_git.status_porcelain.return_value = set()
    worktree_git.default_branch.return_value = "main"
    worktree_git.count_commits_ahead.return_value = 0

    with patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None):
        rc = _complete_code_task(
            task,
            config,
            store,
            worktree_git,
            log_file,
            "feature/seeded-clean",
            TaskStats(duration_seconds=1.0, num_steps_computed=1, cost_usd=0.0),
            0,
            pre_run_status=set(),
            worktree_summary_path=worktree_summary_path,
            summary_path=summary_path,
            summary_dir=summary_path.parent,
            seeded_paths={"src/seeded.py"},
        )

    assert rc == 0
    worktree_git.add.assert_not_called()
    worktree_git.commit.assert_not_called()
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.status == "failed"
    assert refreshed.failure_reason == "UNKNOWN"


def test_run_marks_failed_when_extraction_precheck_fails(tmp_path: Path) -> None:
    db_path = tmp_path / "test.db"
    store = SqliteTaskStore(db_path, prefix="testproject")
    task = store.add("Extracted task", task_type="implement")
    task.slug = "20260427-failing-extract"
    store.update(task)

    project_bundle = tmp_path / ".gza" / "extractions" / task.slug
    project_bundle.mkdir(parents=True, exist_ok=True)
    (project_bundle / "manifest.json").write_text(
        json.dumps(
            {
                "version": 1,
                "target_task_id": task.id,
                "target_slug": task.slug,
                "selected_paths": ["src/file.py"],
                "touched_paths": ["src/file.py"],
                "patch_path": "selected.patch",
            }
        )
    )
    (project_bundle / "selected.patch").write_text(
        "diff --git a/src/file.py b/src/file.py\n"
        "--- a/src/file.py\n"
        "+++ b/src/file.py\n"
        "@@ -0,0 +1 @@\n"
        "+print('boom')\n"
    )
    (project_bundle / "prompt.md").write_text("prompt\n")

    config = _build_config(tmp_path, db_path)

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

    mock_main_git = Mock()
    mock_main_git.default_branch.return_value = "main"
    mock_main_git.branch_exists.return_value = False
    mock_main_git.worktree_list.return_value = []
    mock_main_git.worktree_add.return_value = config.worktree_path / task.slug
    mock_main_git.count_commits_ahead.return_value = 0
    mock_main_git._run.return_value = Mock(returncode=0, stdout="", stderr="")

    mock_worktree_git = Mock()
    mock_worktree_git.status_porcelain.return_value = set()
    mock_worktree_git.apply_patch_check.side_effect = GitError("apply check failed")

    with (
        patch("gza.runner.get_provider", return_value=mock_provider),
        patch("gza.runner.get_effective_config_for_task", return_value=("", "claude", 50)),
        patch("gza.runner.Git", side_effect=[mock_main_git, mock_worktree_git]),
        patch("gza.runner.load_dotenv"),
    ):
        rc = run(config, task_id=task.id)

    assert rc == 1
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.status == "failed"
    assert refreshed.failure_reason == EXTRACTION_PRECHECK_FAILURE_REASON
    assert mock_provider.run.call_count == 0


def test_run_marks_failed_when_extraction_manifest_identity_mismatches_task(tmp_path: Path) -> None:
    db_path = tmp_path / "test.db"
    store = SqliteTaskStore(db_path, prefix="testproject")
    task = store.add("Extracted task", task_type="implement")
    task.slug = "20260427-mismatch"
    store.update(task)

    project_bundle = tmp_path / ".gza" / "extractions" / task.slug
    project_bundle.mkdir(parents=True, exist_ok=True)
    (project_bundle / "manifest.json").write_text(
        json.dumps(
            {
                "version": 1,
                "target_task_id": "testproject-999",
                "target_slug": task.slug,
                "selected_paths": ["src/file.py"],
                "touched_paths": ["src/file.py"],
                "patch_path": "selected.patch",
            }
        )
    )
    (project_bundle / "selected.patch").write_text(
        "diff --git a/src/file.py b/src/file.py\n"
        "--- a/src/file.py\n"
        "+++ b/src/file.py\n"
        "@@ -0,0 +1 @@\n"
        "+print('mismatch')\n"
    )
    (project_bundle / "prompt.md").write_text("prompt\n")

    config = _build_config(tmp_path, db_path)

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

    mock_main_git = Mock()
    mock_main_git.default_branch.return_value = "main"
    mock_main_git.branch_exists.return_value = False
    mock_main_git.worktree_list.return_value = []
    mock_main_git.worktree_add.return_value = config.worktree_path / task.slug
    mock_main_git.count_commits_ahead.return_value = 0
    mock_main_git._run.return_value = Mock(returncode=0, stdout="", stderr="")

    mock_worktree_git = Mock()
    mock_worktree_git.status_porcelain.return_value = set()

    with (
        patch("gza.runner.get_provider", return_value=mock_provider),
        patch("gza.runner.get_effective_config_for_task", return_value=("", "claude", 50)),
        patch("gza.runner.Git", side_effect=[mock_main_git, mock_worktree_git]),
        patch("gza.runner.load_dotenv"),
    ):
        rc = run(config, task_id=task.id)

    assert rc == 1
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.status == "failed"
    assert refreshed.failure_reason == EXTRACTION_PRECHECK_FAILURE_REASON
    assert mock_provider.run.call_count == 0


def test_run_marks_failed_when_extraction_manifest_unreadable(tmp_path: Path) -> None:
    db_path = tmp_path / "test.db"
    store = SqliteTaskStore(db_path, prefix="testproject")
    task = store.add("Extracted task", task_type="implement")
    task.slug = "20260427-unreadable-manifest"
    store.update(task)

    project_bundle = tmp_path / ".gza" / "extractions" / task.slug
    project_bundle.mkdir(parents=True, exist_ok=True)
    manifest_path = project_bundle / "manifest.json"
    manifest_path.write_text("{}")
    (project_bundle / "selected.patch").write_text("diff --git a/a b/a\n")
    (project_bundle / "prompt.md").write_text("prompt\n")

    config = _build_config(tmp_path, db_path)

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

    mock_main_git = Mock()
    mock_main_git.default_branch.return_value = "main"
    mock_main_git.branch_exists.return_value = False
    mock_main_git.worktree_list.return_value = []
    mock_main_git.worktree_add.return_value = config.worktree_path / task.slug
    mock_main_git.count_commits_ahead.return_value = 0
    mock_main_git._run.return_value = Mock(returncode=0, stdout="", stderr="")

    mock_worktree_git = Mock()
    mock_worktree_git.status_porcelain.return_value = set()

    original_read_text = Path.read_text

    def _patched_read_text(path: Path, *args: object, **kwargs: object) -> str:
        if path.name == "manifest.json" and task.slug in str(path):
            raise OSError("manifest read failed")
        return original_read_text(path, *args, **kwargs)

    with (
        patch("gza.runner.get_provider", return_value=mock_provider),
        patch("gza.runner.get_effective_config_for_task", return_value=("", "claude", 50)),
        patch("gza.runner.Git", side_effect=[mock_main_git, mock_worktree_git]),
        patch("gza.runner.load_dotenv"),
        patch("pathlib.Path.read_text", new=_patched_read_text),
    ):
        rc = run(config, task_id=task.id)

    assert rc == 1
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.status == "failed"
    assert refreshed.failure_reason == EXTRACTION_PRECHECK_FAILURE_REASON
    assert mock_provider.run.call_count == 0


def test_run_marks_failed_when_extraction_manifest_touched_paths_is_null(tmp_path: Path) -> None:
    db_path = tmp_path / "test.db"
    store = SqliteTaskStore(db_path, prefix="testproject")
    task = store.add("Extracted task", task_type="implement")
    task.slug = "20260427-manifest-touched-null"
    store.update(task)

    project_bundle = tmp_path / ".gza" / "extractions" / task.slug
    project_bundle.mkdir(parents=True, exist_ok=True)
    (project_bundle / "manifest.json").write_text(
        json.dumps(
            {
                "version": 1,
                "target_task_id": task.id,
                "target_slug": task.slug,
                "selected_paths": ["src/file.py"],
                "touched_paths": None,
                "patch_path": "selected.patch",
            }
        )
    )
    (project_bundle / "selected.patch").write_text(
        "diff --git a/src/file.py b/src/file.py\n"
        "--- a/src/file.py\n"
        "+++ b/src/file.py\n"
        "@@ -0,0 +1 @@\n"
        "+print('seeded')\n"
    )
    (project_bundle / "prompt.md").write_text("prompt\n")

    config = _build_config(tmp_path, db_path)

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

    mock_main_git = Mock()
    mock_main_git.default_branch.return_value = "main"
    mock_main_git.branch_exists.return_value = False
    mock_main_git.worktree_list.return_value = []
    mock_main_git.worktree_add.return_value = config.worktree_path / task.slug
    mock_main_git.count_commits_ahead.return_value = 0
    mock_main_git._run.return_value = Mock(returncode=0, stdout="", stderr="")

    mock_worktree_git = Mock()
    mock_worktree_git.status_porcelain.return_value = set()

    with (
        patch("gza.runner.get_provider", return_value=mock_provider),
        patch("gza.runner.get_effective_config_for_task", return_value=("", "claude", 50)),
        patch("gza.runner.Git", side_effect=[mock_main_git, mock_worktree_git]),
        patch("gza.runner.load_dotenv"),
    ):
        rc = run(config, task_id=task.id)

    assert rc == 1
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.status == "failed"
    assert refreshed.failure_reason == EXTRACTION_PRECHECK_FAILURE_REASON
    assert mock_provider.run.call_count == 0


def test_run_marks_failed_when_extraction_manifest_patch_path_traverses_outside_bundle(tmp_path: Path) -> None:
    db_path = tmp_path / "test.db"
    store = SqliteTaskStore(db_path, prefix="testproject")
    task = store.add("Extracted task", task_type="implement")
    task.slug = "20260427-unsafe-patch-traversal"
    store.update(task)

    project_bundle = tmp_path / ".gza" / "extractions" / task.slug
    project_bundle.mkdir(parents=True, exist_ok=True)
    (project_bundle / "manifest.json").write_text(
        json.dumps(
            {
                "version": 1,
                "target_task_id": task.id,
                "target_slug": task.slug,
                "selected_paths": ["src/file.py"],
                "touched_paths": ["src/file.py"],
                "patch_path": "../other.patch",
            }
        )
    )
    (project_bundle / "selected.patch").write_text(
        "diff --git a/src/file.py b/src/file.py\n"
        "--- a/src/file.py\n"
        "+++ b/src/file.py\n"
        "@@ -0,0 +1 @@\n"
        "+print('seeded')\n"
    )
    (project_bundle / "prompt.md").write_text("prompt\n")

    config = _build_config(tmp_path, db_path)

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

    mock_main_git = Mock()
    mock_main_git.default_branch.return_value = "main"
    mock_main_git.branch_exists.return_value = False
    mock_main_git.worktree_list.return_value = []
    mock_main_git.worktree_add.return_value = config.worktree_path / task.slug
    mock_main_git.count_commits_ahead.return_value = 0
    mock_main_git._run.return_value = Mock(returncode=0, stdout="", stderr="")

    mock_worktree_git = Mock()
    mock_worktree_git.status_porcelain.return_value = set()

    with (
        patch("gza.runner.get_provider", return_value=mock_provider),
        patch("gza.runner.get_effective_config_for_task", return_value=("", "claude", 50)),
        patch("gza.runner.Git", side_effect=[mock_main_git, mock_worktree_git]),
        patch("gza.runner.load_dotenv"),
    ):
        rc = run(config, task_id=task.id)

    assert rc == 1
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.status == "failed"
    assert refreshed.failure_reason == EXTRACTION_PRECHECK_FAILURE_REASON
    assert mock_provider.run.call_count == 0


def test_run_marks_failed_when_extraction_manifest_patch_path_is_absolute(tmp_path: Path) -> None:
    db_path = tmp_path / "test.db"
    store = SqliteTaskStore(db_path, prefix="testproject")
    task = store.add("Extracted task", task_type="implement")
    task.slug = "20260427-unsafe-patch-absolute"
    store.update(task)

    project_bundle = tmp_path / ".gza" / "extractions" / task.slug
    project_bundle.mkdir(parents=True, exist_ok=True)
    (project_bundle / "manifest.json").write_text(
        json.dumps(
            {
                "version": 1,
                "target_task_id": task.id,
                "target_slug": task.slug,
                "selected_paths": ["src/file.py"],
                "touched_paths": ["src/file.py"],
                "patch_path": "/tmp/other.patch",
            }
        )
    )
    (project_bundle / "selected.patch").write_text(
        "diff --git a/src/file.py b/src/file.py\n"
        "--- a/src/file.py\n"
        "+++ b/src/file.py\n"
        "@@ -0,0 +1 @@\n"
        "+print('seeded')\n"
    )
    (project_bundle / "prompt.md").write_text("prompt\n")

    config = _build_config(tmp_path, db_path)

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

    mock_main_git = Mock()
    mock_main_git.default_branch.return_value = "main"
    mock_main_git.branch_exists.return_value = False
    mock_main_git.worktree_list.return_value = []
    mock_main_git.worktree_add.return_value = config.worktree_path / task.slug
    mock_main_git.count_commits_ahead.return_value = 0
    mock_main_git._run.return_value = Mock(returncode=0, stdout="", stderr="")

    mock_worktree_git = Mock()
    mock_worktree_git.status_porcelain.return_value = set()

    with (
        patch("gza.runner.get_provider", return_value=mock_provider),
        patch("gza.runner.get_effective_config_for_task", return_value=("", "claude", 50)),
        patch("gza.runner.Git", side_effect=[mock_main_git, mock_worktree_git]),
        patch("gza.runner.load_dotenv"),
    ):
        rc = run(config, task_id=task.id)

    assert rc == 1
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.status == "failed"
    assert refreshed.failure_reason == EXTRACTION_PRECHECK_FAILURE_REASON
    assert mock_provider.run.call_count == 0


def test_run_marks_failed_when_extraction_patch_read_fails(tmp_path: Path) -> None:
    db_path = tmp_path / "test.db"
    store = SqliteTaskStore(db_path, prefix="testproject")
    task = store.add("Extracted task", task_type="implement")
    task.slug = "20260427-unreadable-patch"
    store.update(task)

    project_bundle = tmp_path / ".gza" / "extractions" / task.slug
    project_bundle.mkdir(parents=True, exist_ok=True)
    (project_bundle / "manifest.json").write_text(
        json.dumps(
            {
                "version": 1,
                "target_task_id": task.id,
                "target_slug": task.slug,
                "selected_paths": ["src/file.py"],
                "touched_paths": ["src/file.py"],
                "patch_path": "selected.patch",
            }
        )
    )
    patch_path = project_bundle / "selected.patch"
    patch_path.write_text(
        "diff --git a/src/file.py b/src/file.py\n"
        "--- a/src/file.py\n"
        "+++ b/src/file.py\n"
        "@@ -0,0 +1 @@\n"
        "+print('seeded')\n"
    )
    (project_bundle / "prompt.md").write_text("prompt\n")

    config = _build_config(tmp_path, db_path)

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

    mock_main_git = Mock()
    mock_main_git.default_branch.return_value = "main"
    mock_main_git.branch_exists.return_value = False
    mock_main_git.worktree_list.return_value = []
    mock_main_git.worktree_add.return_value = config.worktree_path / task.slug
    mock_main_git.count_commits_ahead.return_value = 0
    mock_main_git._run.return_value = Mock(returncode=0, stdout="", stderr="")

    mock_worktree_git = Mock()
    mock_worktree_git.status_porcelain.return_value = set()
    mock_worktree_git.get_diff_numstat.return_value = ""
    mock_worktree_git._run.return_value = Mock(returncode=0, stdout="", stderr="")

    original_read_text = Path.read_text

    def _patched_read_text(path: Path, *args: object, **kwargs: object) -> str:
        if path.name == "selected.patch" and task.slug in str(path):
            raise UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid start byte")
        return original_read_text(path, *args, **kwargs)

    with (
        patch("gza.runner.get_provider", return_value=mock_provider),
        patch("gza.runner.get_effective_config_for_task", return_value=("", "claude", 50)),
        patch("gza.runner.Git", side_effect=[mock_main_git, mock_worktree_git]),
        patch("gza.runner.load_dotenv"),
        patch("pathlib.Path.read_text", new=_patched_read_text),
    ):
        rc = run(config, task_id=task.id)

    assert rc == 1
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.status == "failed"
    assert refreshed.failure_reason == EXTRACTION_PRECHECK_FAILURE_REASON
    assert mock_provider.run.call_count == 0
