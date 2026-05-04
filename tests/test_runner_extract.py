"""Focused tests for extraction seeding in runner code-task flow."""

import json
from pathlib import Path
from unittest.mock import Mock, patch

from gza.config import Config
from gza.db import SqliteTaskStore, TaskStats
from gza.git import Git, GitApplyResult, GitError
from gza.providers import RunResult
from gza.runner import (
    EXTRACTION_ALREADY_MERGED_COMPLETION_REASON,
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


def _init_repo(tmp_path: Path, *, initial_content: str) -> Git:
    git = Git(tmp_path)
    git._run("init", "-b", "main")
    git._run("config", "user.name", "Test User")
    git._run("config", "user.email", "test@example.com")
    target = tmp_path / "src" / "file.py"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(initial_content)
    git._run("add", "src/file.py")
    git._run("commit", "-m", "initial")
    return git


def _write_bundle(
    project_dir: Path,
    task_id: str,
    task_slug: str,
    *,
    patch_text: str,
    source_branch: str,
    source_base_ref: str = "main",
    selected_paths: list[str] | None = None,
) -> None:
    selected = selected_paths or ["src/file.py"]
    bundle_dir = project_dir / ".gza" / "extractions" / task_slug
    bundle_dir.mkdir(parents=True, exist_ok=True)
    (bundle_dir / "manifest.json").write_text(
        json.dumps(
            {
                "version": 1,
                "source_branch": source_branch,
                "source_base_ref": source_base_ref,
                "target_task_id": task_id,
                "target_slug": task_slug,
                "selected_paths": selected,
                "touched_paths": selected,
                "patch_path": "selected.patch",
            }
        )
    )
    (bundle_dir / "selected.patch").write_text(patch_text)
    (bundle_dir / "prompt.md").write_text("seed prompt\n")


def _add_detached_worktree(git: Git, path: Path) -> Git:
    git._run("worktree", "add", "--detach", str(path), "main")
    return Git(path)


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
                "source_branch": "feature/source",
                "source_base_ref": "main",
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
    worktree_git.ref_exists.return_value = False
    worktree_git.apply_patch_file_result.return_value = GitApplyResult(
        returncode=0,
        stdout="",
        stderr="",
    )

    seeded = _seed_extraction_bundle_if_present(
        task,
        config,
        worktree,
        worktree_git,
        log_file,
        resume=False,
    )

    assert seeded.seeded_paths == frozenset({"src/file.py"})
    assert seeded.completion_reason is None
    worktree_git.apply_patch_file_result.assert_called_once()


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
                "source_branch": "feature/source",
                "source_base_ref": "main",
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
    worktree_git.ref_exists.return_value = False
    worktree_git.apply_patch_file_result.return_value = GitApplyResult(
        returncode=0,
        stdout="",
        stderr="",
    )

    seeded = _seed_extraction_bundle_if_present(
        task,
        config,
        worktree,
        worktree_git,
        log_file,
        resume=False,
    )

    assert seeded.seeded_paths == frozenset({"src/file with space.py"})
    assert seeded.completion_reason is None
    worktree_git.apply_patch_file_result.assert_called_once()


def test_seed_extraction_bundle_marks_already_merged_when_rederived_diff_is_empty(tmp_path: Path) -> None:
    git = _init_repo(tmp_path, initial_content="base\n")
    store = SqliteTaskStore(tmp_path / "test.db", prefix="testproject")
    task = store.add("Extracted task", task_type="implement")
    task.slug = "20260427-already-merged"
    store.update(task)

    git._run("checkout", "-b", "feature/source")
    target = tmp_path / "src" / "file.py"
    target.write_text("feature\n")
    git._run("add", "src/file.py")
    git._run("commit", "-m", "feature change")
    stored_patch = git.get_diff_patch_for_paths("main...feature/source", ("src/file.py",), binary=True)

    git._run("checkout", "main")
    git._run("merge", "--no-ff", "feature/source", "-m", "merge feature")

    _write_bundle(
        tmp_path,
        task.id,
        task.slug,
        patch_text=stored_patch,
        source_branch="feature/source",
    )
    worktree_path = tmp_path / "worktree-already-merged"
    worktree_git = _add_detached_worktree(git, worktree_path)

    config = Mock(spec=Config)
    config.project_dir = tmp_path
    log_file = tmp_path / "logs" / "already-merged.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)

    seeded = _seed_extraction_bundle_if_present(
        task,
        config,
        worktree_path,
        worktree_git,
        log_file,
        resume=False,
    )

    assert seeded.seeded_paths == frozenset()
    assert seeded.completion_reason == EXTRACTION_ALREADY_MERGED_COMPLETION_REASON
    assert (worktree_path / "src" / "file.py").read_text() == "feature\n"
    assert "re-derived hunks=0" in log_file.read_text()


def test_seed_extraction_bundle_marks_already_merged_when_selected_paths_are_equivalent_on_base(
    tmp_path: Path,
) -> None:
    git = _init_repo(tmp_path, initial_content="base\n")
    store = SqliteTaskStore(tmp_path / "test.db", prefix="testproject")
    task = store.add("Extracted task", task_type="implement")
    task.slug = "20260427-already-merged-cherry-pick"
    store.update(task)

    git._run("checkout", "-b", "feature/source")
    target = tmp_path / "src" / "file.py"
    target.write_text("feature\n")
    git._run("add", "src/file.py")
    git._run("commit", "-m", "feature change")
    stored_patch = git.get_diff_patch_for_paths("main...feature/source", ("src/file.py",), binary=True)

    git._run("checkout", "main")
    target.write_text("feature\n")
    git._run("add", "src/file.py")
    git._run("commit", "-m", "equivalent change on main")

    _write_bundle(
        tmp_path,
        task.id,
        task.slug,
        patch_text=stored_patch,
        source_branch="feature/source",
    )
    worktree_path = tmp_path / "worktree-already-merged-equivalent"
    worktree_git = _add_detached_worktree(git, worktree_path)

    config = Mock(spec=Config)
    config.project_dir = tmp_path
    log_file = tmp_path / "logs" / "already-merged-equivalent.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)

    seeded = _seed_extraction_bundle_if_present(
        task,
        config,
        worktree_path,
        worktree_git,
        log_file,
        resume=False,
    )

    assert seeded.seeded_paths == frozenset()
    assert seeded.completion_reason == EXTRACTION_ALREADY_MERGED_COMPLETION_REASON
    assert (worktree_path / "src" / "file.py").read_text() == "feature\n"
    assert "adds nothing to the current base for selected paths" in log_file.read_text()


def test_seed_extraction_bundle_rederives_patch_and_applies_with_context_drift(tmp_path: Path) -> None:
    git = _init_repo(tmp_path, initial_content="a\nb\nc\n")
    store = SqliteTaskStore(tmp_path / "test.db", prefix="testproject")
    task = store.add("Extracted task", task_type="implement")
    task.slug = "20260427-drift"
    store.update(task)

    git._run("checkout", "-b", "feature/source")
    target = tmp_path / "src" / "file.py"
    target.write_text("a\nbranch\nc\n")
    git._run("add", "src/file.py")
    git._run("commit", "-m", "feature change")
    stored_patch = git.get_diff_patch_for_paths("main...feature/source", ("src/file.py",), binary=True)

    git._run("checkout", "main")
    target.write_text("header\na\nb\nc\n")
    git._run("add", "src/file.py")
    git._run("commit", "-m", "main drift")

    _write_bundle(
        tmp_path,
        task.id,
        task.slug,
        patch_text=stored_patch,
        source_branch="feature/source",
    )
    worktree_path = tmp_path / "worktree-drift"
    worktree_git = _add_detached_worktree(git, worktree_path)

    config = Mock(spec=Config)
    config.project_dir = tmp_path
    log_file = tmp_path / "logs" / "drift.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)

    seeded = _seed_extraction_bundle_if_present(
        task,
        config,
        worktree_path,
        worktree_git,
        log_file,
        resume=False,
    )

    assert seeded.seeded_paths == frozenset({"src/file.py"})
    assert seeded.completion_reason is None
    assert (worktree_path / "src" / "file.py").read_text() == "header\na\nbranch\nc\n"
    assert "re-derived hunks=1" in log_file.read_text()


def test_seed_extraction_bundle_falls_back_to_stored_patch_when_source_branch_is_unreachable(tmp_path: Path) -> None:
    git = _init_repo(tmp_path, initial_content="base\n")
    store = SqliteTaskStore(tmp_path / "test.db", prefix="testproject")
    task = store.add("Extracted task", task_type="implement")
    task.slug = "20260427-stored-fallback"
    store.update(task)

    git._run("checkout", "-b", "feature/source")
    target = tmp_path / "src" / "file.py"
    target.write_text("feature\n")
    git._run("add", "src/file.py")
    git._run("commit", "-m", "feature change")
    stored_patch = git.get_diff_patch_for_paths("main...feature/source", ("src/file.py",), binary=True)

    git._run("checkout", "main")
    git._run("branch", "-D", "feature/source")

    _write_bundle(
        tmp_path,
        task.id,
        task.slug,
        patch_text=stored_patch,
        source_branch="feature/source",
    )
    worktree_path = tmp_path / "worktree-stored-fallback"
    worktree_git = _add_detached_worktree(git, worktree_path)

    config = Mock(spec=Config)
    config.project_dir = tmp_path
    log_file = tmp_path / "logs" / "stored-fallback.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)

    seeded = _seed_extraction_bundle_if_present(
        task,
        config,
        worktree_path,
        worktree_git,
        log_file,
        resume=False,
    )

    assert seeded.seeded_paths == frozenset({"src/file.py"})
    assert seeded.completion_reason is None
    assert (worktree_path / "src" / "file.py").read_text() == "feature\n"
    assert "source branch 'feature/source' unreachable" in log_file.read_text()


def test_seed_extraction_bundle_retries_after_runtime_patch_artifact_left_by_failed_attempt(
    tmp_path: Path,
) -> None:
    store = SqliteTaskStore(tmp_path / "test.db", prefix="testproject")
    task = store.add("Extracted task", task_type="implement")
    task.slug = "20260427-runtime-artifact"
    store.update(task)

    patch_text = (
        "diff --git a/src/file.py b/src/file.py\n"
        "--- a/src/file.py\n"
        "+++ b/src/file.py\n"
        "@@ -1 +1 @@\n"
        "-base\n"
        "+feature\n"
    )
    _write_bundle(
        tmp_path,
        task.id,
        task.slug,
        patch_text=patch_text,
        source_branch="feature/source",
    )

    worktree_path = tmp_path / "worktree-runtime-artifact"
    worktree_path.mkdir()
    log_file = tmp_path / "logs" / "runtime-artifact.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)

    config = Mock(spec=Config)
    config.project_dir = tmp_path

    worktree_git = Mock()
    worktree_git.ref_exists.side_effect = [True, True, True, True]
    worktree_git.get_diff_patch_for_paths.return_value = patch_text
    worktree_git.apply_patch_file_result.return_value = GitApplyResult(
        returncode=1,
        stdout="",
        stderr="apply failed",
    )
    worktree_git.status_porcelain.return_value = set()

    with patch("gza.runner.write_log_entry"):
        with patch("gza.extractions.load_patch_text", return_value=patch_text):
            with patch("gza.extractions.parse_patch_touched_paths", return_value=["src/file.py"]):
                with patch("gza.runner.parse_patch_touched_paths", return_value=["src/file.py"]):
                    with patch("gza.runner.load_patch_text", return_value=patch_text):
                        first_error = None
                        try:
                            _seed_extraction_bundle_if_present(
                                task,
                                config,
                                worktree_path,
                                worktree_git,
                                log_file,
                                resume=False,
                            )
                        except GitError as exc:
                            first_error = exc

                        assert first_error is not None
                        runtime_patch_path = (
                            worktree_path / ".gza" / "extractions" / task.slug / "selected.runtime.patch"
                        )
                        assert runtime_patch_path.exists()

                        second_error = None
                        try:
                            _seed_extraction_bundle_if_present(
                                task,
                                config,
                                worktree_path,
                                worktree_git,
                                log_file,
                                resume=False,
                            )
                        except GitError as exc:
                            second_error = exc

    assert second_error is not None
    assert "unexpected files" not in str(second_error)
    assert worktree_git.get_diff_patch_for_paths.call_count == 4
    assert worktree_git.apply_patch_file_result.call_count == 2


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
                "source_branch": "feature/source",
                "source_base_ref": "main",
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
    mock_worktree_git.ref_exists.return_value = False
    mock_worktree_git.apply_patch_file_result.return_value = GitApplyResult(
        returncode=1,
        stdout="",
        stderr="apply failed",
    )

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


def test_run_continues_when_runtime_rederived_patch_applies_with_conflicts(tmp_path: Path) -> None:
    db_path = tmp_path / "test.db"
    store = SqliteTaskStore(db_path, prefix="testproject")
    task = store.add("Extracted task", task_type="implement")
    task.slug = "20260427-conflicted-rederived"
    store.update(task)

    patch_text = (
        "diff --git a/src/file.py b/src/file.py\n"
        "--- a/src/file.py\n"
        "+++ b/src/file.py\n"
        "@@ -0,0 +1 @@\n"
        "+print('boom')\n"
    )
    _write_bundle(
        tmp_path,
        task.id,
        task.slug,
        patch_text=patch_text,
        source_branch="feature/source",
    )

    config = _build_config(tmp_path, db_path)

    mock_provider = Mock()
    mock_provider.name = "TestProvider"
    mock_provider.check_credentials.return_value = True
    mock_provider.verify_credentials.return_value = True
    mock_provider.run.return_value = RunResult(
        exit_code=1,
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
    mock_worktree_git.has_changes.return_value = False
    mock_worktree_git.ref_exists.side_effect = [True, True]
    mock_worktree_git.get_diff_patch_for_paths.return_value = patch_text
    mock_worktree_git.apply_patch_file_result.return_value = GitApplyResult(
        returncode=1,
        stdout="",
        stderr="with conflicts",
    )
    mock_worktree_git.status_porcelain.return_value = {("UU", "src/file.py")}

    with (
        patch("gza.runner.get_provider", return_value=mock_provider),
        patch("gza.runner.get_effective_config_for_task", return_value=("", "claude", 50)),
        patch("gza.runner.Git", side_effect=[mock_main_git, mock_worktree_git]),
        patch("gza.runner.load_dotenv"),
        patch("gza.runner.build_prompt", return_value="prompt"),
        patch("gza.runner.extract_failure_reason", return_value="UNKNOWN"),
    ):
        rc = run(config, task_id=task.id)

    assert rc == 0
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.failure_reason == "UNKNOWN"
    assert mock_provider.run.call_count == 1
    log_file = config.log_path / f"{task.slug}.log"
    assert "runtime re-derived patch with conflicts" in log_file.read_text()


def test_run_continues_when_stored_fallback_patch_applies_with_conflicts(tmp_path: Path) -> None:
    db_path = tmp_path / "test.db"
    store = SqliteTaskStore(db_path, prefix="testproject")
    task = store.add("Extracted task", task_type="implement")
    task.slug = "20260427-conflicted-stored-fallback"
    store.update(task)

    patch_text = (
        "diff --git a/src/file.py b/src/file.py\n"
        "--- a/src/file.py\n"
        "+++ b/src/file.py\n"
        "@@ -0,0 +1 @@\n"
        "+print('boom')\n"
    )
    _write_bundle(
        tmp_path,
        task.id,
        task.slug,
        patch_text=patch_text,
        source_branch="feature/source",
    )

    config = _build_config(tmp_path, db_path)

    mock_provider = Mock()
    mock_provider.name = "TestProvider"
    mock_provider.check_credentials.return_value = True
    mock_provider.verify_credentials.return_value = True
    mock_provider.run.return_value = RunResult(
        exit_code=1,
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
    mock_worktree_git.has_changes.return_value = False
    mock_worktree_git.ref_exists.return_value = False
    mock_worktree_git.apply_patch_file_result.return_value = GitApplyResult(
        returncode=1,
        stdout="",
        stderr="with conflicts",
    )
    mock_worktree_git.status_porcelain.return_value = {("UU", "src/file.py")}

    with (
        patch("gza.runner.get_provider", return_value=mock_provider),
        patch("gza.runner.get_effective_config_for_task", return_value=("", "claude", 50)),
        patch("gza.runner.Git", side_effect=[mock_main_git, mock_worktree_git]),
        patch("gza.runner.load_dotenv"),
        patch("gza.runner.build_prompt", return_value="prompt"),
        patch("gza.runner.extract_failure_reason", return_value="UNKNOWN"),
    ):
        rc = run(config, task_id=task.id)

    assert rc == 0
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.failure_reason == "UNKNOWN"
    assert refreshed.failure_reason != EXTRACTION_PRECHECK_FAILURE_REASON
    assert mock_provider.run.call_count == 1
    log_file = config.log_path / f"{task.slug}.log"
    assert "stored patch fallback with conflicts" in log_file.read_text()


def test_run_completes_without_provider_when_extraction_diff_is_already_merged(tmp_path: Path) -> None:
    db_path = tmp_path / "test.db"
    store = SqliteTaskStore(db_path, prefix="testproject")
    task = store.add("Extracted task", task_type="implement")
    task.slug = "20260427-already-merged-run"
    store.update(task)

    project_bundle = tmp_path / ".gza" / "extractions" / task.slug
    project_bundle.mkdir(parents=True, exist_ok=True)
    (project_bundle / "manifest.json").write_text(
        json.dumps(
            {
                "version": 1,
                "source_branch": "feature/source",
                "source_base_ref": "main",
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
        "index e69de29..8c7e5a6 100644\n"
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

    mock_main_git = Mock()
    mock_main_git.default_branch.return_value = "main"
    mock_main_git.branch_exists.return_value = False
    mock_main_git.worktree_list.return_value = []
    mock_main_git.worktree_add.return_value = config.worktree_path / task.slug
    mock_main_git.count_commits_ahead.return_value = 0
    mock_main_git._run.return_value = Mock(returncode=0, stdout="", stderr="")

    mock_worktree_git = Mock()
    mock_worktree_git.ref_exists.side_effect = [True, True]
    mock_worktree_git.get_diff_patch_for_paths.return_value = ""

    with (
        patch("gza.runner.get_provider", return_value=mock_provider),
        patch("gza.runner.get_effective_config_for_task", return_value=("", "claude", 50)),
        patch("gza.runner.Git", side_effect=[mock_main_git, mock_worktree_git]),
        patch("gza.runner.load_dotenv"),
    ):
        rc = run(config, task_id=task.id)

    assert rc == 0
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.status == "completed"
    assert refreshed.completion_reason == EXTRACTION_ALREADY_MERGED_COMPLETION_REASON
    assert mock_provider.run.call_count == 0


def test_run_completes_without_provider_when_selected_extraction_scope_is_already_merged(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "test.db"
    store = SqliteTaskStore(db_path, prefix="testproject")
    task = store.add("Extracted task", task_type="implement")
    task.slug = "20260427-already-merged-selected-scope"
    store.update(task)

    patch_text = (
        "diff --git a/src/file.py b/src/file.py\n"
        "index e69de29..8c7e5a6 100644\n"
        "--- a/src/file.py\n"
        "+++ b/src/file.py\n"
        "@@ -0,0 +1 @@\n"
        "+print('boom')\n"
    )
    _write_bundle(
        tmp_path,
        task.id,
        task.slug,
        patch_text=patch_text,
        source_branch="feature/source",
    )

    config = _build_config(tmp_path, db_path)

    mock_provider = Mock()
    mock_provider.name = "TestProvider"
    mock_provider.check_credentials.return_value = True
    mock_provider.verify_credentials.return_value = True

    mock_main_git = Mock()
    mock_main_git.default_branch.return_value = "main"
    mock_main_git.branch_exists.return_value = False
    mock_main_git.worktree_list.return_value = []
    mock_main_git.worktree_add.return_value = config.worktree_path / task.slug
    mock_main_git.count_commits_ahead.return_value = 0
    mock_main_git._run.return_value = Mock(returncode=0, stdout="", stderr="")

    mock_worktree_git = Mock()
    mock_worktree_git.ref_exists.side_effect = [True, True]
    mock_worktree_git.get_diff_patch_for_paths.side_effect = [patch_text, ""]

    with (
        patch("gza.runner.get_provider", return_value=mock_provider),
        patch("gza.runner.get_effective_config_for_task", return_value=("", "claude", 50)),
        patch("gza.runner.Git", side_effect=[mock_main_git, mock_worktree_git]),
        patch("gza.runner.load_dotenv"),
    ):
        rc = run(config, task_id=task.id)

    assert rc == 0
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.status == "completed"
    assert refreshed.completion_reason == EXTRACTION_ALREADY_MERGED_COMPLETION_REASON
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
                "source_branch": "feature/source",
                "source_base_ref": "main",
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
                "source_branch": "feature/source",
                "source_base_ref": "main",
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
                "source_branch": "feature/source",
                "source_base_ref": "main",
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
                "source_branch": "feature/source",
                "source_base_ref": "main",
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
                "source_branch": "feature/source",
                "source_base_ref": "main",
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
