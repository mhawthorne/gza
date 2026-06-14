"""Functional tests for runner timeout flows that require a real git repo."""

import json
import logging
import subprocess
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from gza.config import Config
from gza.db import Task
from gza.git import Git
from gza.log_paths import ops_log_path_for
from gza.runner import (
    ProjectBoundary,
    _build_timeout_resume_context,
    _compute_tree_fingerprint,
    _extract_verify_phase_checkpoints,
    _resolve_task_timeout_budget,
    _save_wip_changes,
    _write_timeout_resume_checkpoint,
)
from tests_functional.git_helpers import init_basic_repo


def _init_runner_repo(tmp_path: Path) -> Git:
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    git = init_basic_repo(repo_dir)
    tracked = repo_dir / "tracked.txt"
    tracked.write_text("base\n")
    git._run("add", "tracked.txt")
    git._run("commit", "-m", "initial")
    return git


def _init_scoped_repo(tmp_path: Path) -> tuple[Path, Path, Git]:
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    git = init_basic_repo(repo_dir)
    project_dir = repo_dir / "services" / "foo"
    scoped_file = project_dir / "src" / "app.py"
    out_of_scope_file = repo_dir / "services" / "bar" / "other.py"
    scoped_file.parent.mkdir(parents=True)
    out_of_scope_file.parent.mkdir(parents=True)
    scoped_file.write_text("base\n")
    out_of_scope_file.write_text("base\n")
    git._run("add", ".")
    git._run("commit", "-m", "initial scoped files")
    return repo_dir, project_dir, git


def test_resolve_task_timeout_budget_warns_when_diff_probe_returns_nonzero(tmp_path: Path) -> None:
    task = Task(id="gza-1", prompt="Implement feature", status="pending", task_type="implement")
    config = Mock(spec=Config)
    config.project_dir = tmp_path
    config.timeout_minutes = 10
    config.get_timeout_minutes_for_task.return_value = 10
    config.code_task_diff_timeout_medium_threshold = 400
    config.code_task_diff_timeout_large_threshold = 1200
    config.code_task_diff_timeout_medium_minutes = 30
    config.code_task_diff_timeout_large_minutes = 45
    config.code_task_diff_timeout_cap_minutes = 45
    config._project_boundary_cache = ProjectBoundary(
        repo_root=tmp_path,
        scope_root=Path("."),
        local_dependencies=(),
    )
    git = _init_runner_repo(tmp_path)
    task_logger = Mock()
    original_run = git._run

    def failing_diff_numstat(*args: str, check: bool = True, stdin: bytes | None = None):
        if args[:2] == ("diff", "--numstat"):
            return subprocess.CompletedProcess(["git", *args], 128, "", "bad revision")
        return original_run(*args, check=check, stdin=stdin)

    with patch.object(git, "_run", side_effect=failing_diff_numstat):
        budget = _resolve_task_timeout_budget(
            task=task,
            config=config,
            provider="claude",
            git=git,
            branch_name="feature/test",
            default_branch="main",
            task_logger=task_logger,
        )

    assert budget.minutes == 10
    assert budget.reason == "base timeout for task type 'implement' (diff inspection unavailable)"
    task_logger.warning.assert_called_once()
    assert "below scaling thresholds" not in budget.reason


def test_resolve_task_timeout_budget_ignores_out_of_scope_diff_for_subdir_project(tmp_path: Path) -> None:
    repo_dir, project_dir, git = _init_scoped_repo(tmp_path)
    task = Task(id="gza-1", prompt="Implement feature", status="pending", task_type="implement")
    config = Config(project_dir=project_dir, project_name="foo")
    git._run("checkout", "-b", "feature/test")
    (project_dir / "src" / "app.py").write_text("base\nsmall\n")
    (repo_dir / "services" / "bar" / "other.py").write_text("".join(f"line {i}\n" for i in range(1300)))
    git._run("add", ".")
    git._run("commit", "-m", "feature")

    budget = _resolve_task_timeout_budget(
        task=task,
        config=config,
        provider="claude",
        git=git,
        branch_name="feature/test",
        default_branch="main",
    )

    assert budget.minutes == 10
    assert budget.diff_lines == 1
    assert budget.diff_files == 1
    assert budget.reason == "base timeout for task type 'implement'; below scaling thresholds"


def test_resolve_task_timeout_budget_scales_large_in_scope_diff_for_subdir_project(tmp_path: Path) -> None:
    repo_dir, project_dir, git = _init_scoped_repo(tmp_path)
    task = Task(id="gza-1", prompt="Implement feature", status="pending", task_type="implement")
    config = Config(project_dir=project_dir, project_name="foo")
    git._run("checkout", "-b", "feature/test")
    (project_dir / "src" / "app.py").write_text("".join(f"line {i}\n" for i in range(500)))
    git._run("add", ".")
    git._run("commit", "-m", "feature")

    budget = _resolve_task_timeout_budget(
        task=task,
        config=config,
        provider="claude",
        git=git,
        branch_name="feature/test",
        default_branch="main",
    )

    assert budget.minutes == 30
    assert budget.diff_lines == 501
    assert budget.diff_files == 1
    assert budget.reason == "medium reviewable diff (501 changed lines across 1 files); scaled from base 10m"


def test_timeout_resume_context_reuses_codex_command_execution_phase_on_exact_tree_match(
    tmp_path: Path,
) -> None:
    config = Mock(spec=Config)
    config.project_dir = tmp_path
    git = _init_runner_repo(tmp_path)
    fingerprint = _compute_tree_fingerprint(git)
    log_file = tmp_path / "task.log"
    log_file.write_text(
        json.dumps(
            {
                "type": "item.completed",
                "item": {
                    "type": "command_execution",
                    "command": "./bin/tests",
                    "aggregated_output": (
                        "ruff ok\n"
                        "gza-verify phase=passed name=ruff duration_seconds=1.0 "
                        f"tree_fingerprint={fingerprint}\n"
                    ),
                    "exit_code": 0,
                },
            }
        )
        + "\n"
    )
    ops_log = ops_log_path_for(log_file)
    ops_log.write_text("")

    _write_timeout_resume_checkpoint(
        config=config,
        task_id="gza-1",
        log_file=log_file,
        worktree_git=git,
        wip_state="commit+diff",
    )

    context = _build_timeout_resume_context(
        config=config,
        checkpoint_task_id="gza-1",
        worktree_git=git,
    )

    assert context is not None
    assert "Last known command: `./bin/tests`" in context
    assert "Saved WIP state: commit+diff" in context
    assert "Reusable successful verify phases" in context
    assert "ruff" in context


def test_timeout_resume_context_invalidates_real_bin_tests_phase_after_later_edit(tmp_path: Path) -> None:
    config = Mock(spec=Config)
    config.project_dir = tmp_path
    git = _init_runner_repo(tmp_path)
    tracked = git.repo_dir / "tracked.txt"
    task = Task(
        id="gza-1",
        slug="20260601-gza-1",
        prompt="Implement feature",
        status="running",
        task_type="implement",
    )

    tracked.write_text("before-timeout\n")
    assert git.status_porcelain() == {("M", "tracked.txt")}
    phase_fingerprint = _compute_tree_fingerprint(git)

    log_file = tmp_path / "task.log"
    log_file.write_text(
        json.dumps(
            {
                "type": "item.completed",
                "item": {
                    "type": "command_execution",
                    "command": "./bin/tests",
                    "exit_code": 124,
                },
            }
        )
        + "\n"
    )
    ops_log = ops_log_path_for(log_file)
    ops_log.write_text(
        json.dumps(
            {
                "type": "gza",
                "source": "provider",
                "subtype": "process_output",
                "timestamp": "2026-06-01T00:00:00Z",
                "message": (
                    "gza-verify phase=passed name=ruff duration_seconds=1.0 "
                    f"tree_fingerprint={phase_fingerprint}"
                ),
            }
        )
        + "\n"
    )

    wip_state = _save_wip_changes(task, git, config, "feature/test")
    assert wip_state == "commit+diff"

    _write_timeout_resume_checkpoint(
        config=config,
        task_id="gza-1",
        log_file=log_file,
        worktree_git=git,
        wip_state=wip_state,
        pre_save_tree_fingerprint=phase_fingerprint,
    )

    tracked.write_text("after-timeout\n")
    assert git.status_porcelain() == {("M", "tracked.txt")}

    context = _build_timeout_resume_context(
        config=config,
        checkpoint_task_id="gza-1",
        worktree_git=git,
    )

    assert context is not None
    assert "No reusable verify checkpoints are valid for the current tree fingerprint." in context
    assert "Reusable successful verify phases" not in context


def test_timeout_resume_context_reuses_phase_after_wip_commit_without_later_edit(tmp_path: Path) -> None:
    config = Mock(spec=Config)
    config.project_dir = tmp_path
    git = _init_runner_repo(tmp_path)
    tracked = git.repo_dir / "tracked.txt"
    task = Task(
        id="gza-1",
        slug="20260601-gza-1",
        prompt="Implement feature",
        status="running",
        task_type="implement",
    )

    tracked.write_text("before-timeout\n")
    assert git.status_porcelain() == {("M", "tracked.txt")}
    phase_fingerprint = _compute_tree_fingerprint(git)

    log_file = tmp_path / "task.log"
    log_file.write_text(
        json.dumps(
            {
                "type": "item.completed",
                "item": {
                    "type": "command_execution",
                    "command": "./bin/tests",
                    "exit_code": 124,
                },
            }
        )
        + "\n"
    )
    ops_log = ops_log_path_for(log_file)
    ops_log.write_text(
        json.dumps(
            {
                "type": "gza",
                "source": "provider",
                "subtype": "process_output",
                "timestamp": "2026-06-01T00:00:00Z",
                "message": (
                    "gza-verify phase=passed name=ruff duration_seconds=1.0 "
                    f"tree_fingerprint={phase_fingerprint}"
                ),
            }
        )
        + "\n"
    )

    wip_state = _save_wip_changes(task, git, config, "feature/test")
    assert wip_state == "commit+diff"
    assert git.status_porcelain() == set()

    _write_timeout_resume_checkpoint(
        config=config,
        task_id="gza-1",
        log_file=log_file,
        worktree_git=git,
        wip_state=wip_state,
        pre_save_tree_fingerprint=phase_fingerprint,
    )

    context = _build_timeout_resume_context(
        config=config,
        checkpoint_task_id="gza-1",
        worktree_git=git,
    )

    assert context is not None
    assert "Reusable successful verify phases for the current tree fingerprint: ruff" in context
    assert "No reusable verify checkpoints are valid for the current tree fingerprint." not in context


def test_timeout_resume_context_ignores_legacy_phase_without_tree_fingerprint(tmp_path: Path) -> None:
    config = Mock(spec=Config)
    config.project_dir = tmp_path
    git = _init_runner_repo(tmp_path)
    log_file = tmp_path / "task.log"
    log_file.write_text(
        json.dumps(
            {
                "type": "item.completed",
                "item": {
                    "type": "command_execution",
                    "command": "./bin/tests",
                    "exit_code": 124,
                },
            }
        )
        + "\n"
    )
    ops_log = ops_log_path_for(log_file)
    ops_log.write_text(
        json.dumps(
            {
                "type": "gza",
                "source": "provider",
                "subtype": "process_output",
                "timestamp": "2026-06-01T00:00:00Z",
                "message": "gza-verify phase=passed name=ruff duration_seconds=1.0",
            }
        )
        + "\n"
    )

    _write_timeout_resume_checkpoint(
        config=config,
        task_id="gza-1",
        log_file=log_file,
        worktree_git=git,
        wip_state="commit+diff",
    )

    context = _build_timeout_resume_context(
        config=config,
        checkpoint_task_id="gza-1",
        worktree_git=git,
    )

    assert context is not None
    assert "No reusable verify checkpoints are valid for the current tree fingerprint." in context


def test_extract_verify_phase_checkpoints_ignores_phase_without_tree_fingerprint(tmp_path: Path) -> None:
    log_file = tmp_path / "task.log"
    log_file.write_text(
        json.dumps(
            {
                "type": "item.completed",
                "item": {
                    "type": "command_execution",
                    "command": "./bin/tests --quick",
                    "aggregated_output": (
                        "gza-verify phase=start name=ruff\n"
                        "gza-verify phase=passed name=ruff duration_seconds=0.0\n"
                    ),
                    "exit_code": 0,
                },
            }
        )
        + "\n"
    )
    ops_log = ops_log_path_for(log_file)
    ops_log.write_text("")

    checkpoints = _extract_verify_phase_checkpoints(log_file, ops_log)

    assert checkpoints == []


def test_timeout_resume_context_disables_reuse_when_tree_fingerprinting_fails(tmp_path: Path) -> None:
    config = Mock(spec=Config)
    config.project_dir = tmp_path
    git = _init_runner_repo(tmp_path)
    fingerprint = _compute_tree_fingerprint(git)
    assert isinstance(fingerprint, str)
    log_file = tmp_path / "task.log"
    log_file.write_text(
        json.dumps(
            {
                "type": "item.completed",
                "item": {
                    "type": "command_execution",
                    "command": "./bin/tests",
                    "exit_code": 124,
                },
            }
        )
        + "\n"
    )
    ops_log = ops_log_path_for(log_file)
    ops_log.write_text(
        json.dumps(
            {
                "type": "gza",
                "source": "provider",
                "subtype": "process_output",
                "timestamp": "2026-06-01T00:00:00Z",
                "message": (
                    "gza-verify phase=passed name=ruff duration_seconds=1.0 "
                    f"tree_fingerprint={fingerprint}"
                ),
            }
        )
        + "\n"
    )
    original_run = git._run

    def failing_tree_probe(*args: str, check: bool = True, stdin: bytes | None = None):
        if args and args[0] in {"diff", "ls-files"}:
            return subprocess.CompletedProcess(["git", *args], 128, "", "gitdir unavailable")
        return original_run(*args, check=check, stdin=stdin)

    with patch.object(git, "_run", side_effect=failing_tree_probe):
        _write_timeout_resume_checkpoint(
            config=config,
            task_id="gza-1",
            log_file=log_file,
            worktree_git=git,
            wip_state="commit+diff",
        )
        context = _build_timeout_resume_context(
            config=config,
            checkpoint_task_id="gza-1",
            worktree_git=git,
        )

    assert context is not None
    assert "Reusable successful verify phases" not in context
    assert (
        "No reusable verify checkpoints are valid because exact-tree fingerprinting failed "
        "for the current worktree."
    ) in context


def test_timeout_resume_context_does_not_offer_provider_wrapper_as_next_command(tmp_path: Path) -> None:
    config = Mock(spec=Config)
    config.project_dir = tmp_path
    git = _init_runner_repo(tmp_path)
    log_file = tmp_path / "task.log"
    log_file.write_text(
        json.dumps(
            {
                "type": "message",
                "content": "provider timed out before any command_execution item was emitted",
            }
        )
        + "\n"
    )
    ops_log = ops_log_path_for(log_file)
    ops_log.write_text(
        json.dumps(
            {
                "event": "provider_exec_start",
                "command": "timeout 45m codex exec resume session-123",
            }
        )
        + "\n"
    )

    _write_timeout_resume_checkpoint(
        config=config,
        task_id="gza-1",
        log_file=log_file,
        worktree_git=git,
        wip_state="commit+diff",
    )

    context = _build_timeout_resume_context(
        config=config,
        checkpoint_task_id="gza-1",
        worktree_git=git,
    )

    assert context is not None
    assert "Last known command:" not in context
    assert "First command to run next: timeout 45m codex exec resume session-123" not in context
    assert "Provider wrapper at timeout: `timeout 45m codex exec resume session-123`" in context
    assert (
        "First command to run next: unknown; inspect the current worktree and verification state, "
        "then continue from the interrupted step without relaunching the provider wrapper."
    ) in context


def test_timeout_resume_context_warns_when_checkpoint_json_is_malformed(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    config = Mock(spec=Config)
    config.project_dir = tmp_path
    git = _init_runner_repo(tmp_path)
    checkpoint_path = tmp_path / ".gza" / "checkpoints" / "gza-1.json"
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint_path.write_text("{not-json", encoding="utf-8")

    with caplog.at_level(logging.WARNING):
        context = _build_timeout_resume_context(
            config=config,
            checkpoint_task_id="gza-1",
            worktree_git=git,
        )

    assert context is not None
    assert "Timeout checkpoint context was unavailable" in context
    assert "No reusable verify phases should be trusted from the interrupted run." in context
    assert "Reusable successful verify phases" not in context
    assert "failed to parse checkpoint file" in context
    assert "timeout resume checkpoint exists but could not be reused" in caplog.text


def test_timeout_resume_context_warns_when_checkpoint_json_is_not_a_mapping(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    config = Mock(spec=Config)
    config.project_dir = tmp_path
    git = _init_runner_repo(tmp_path)
    checkpoint_path = tmp_path / ".gza" / "checkpoints" / "gza-1.json"
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint_path.write_text('["not", "a", "mapping"]', encoding="utf-8")

    with caplog.at_level(logging.WARNING):
        context = _build_timeout_resume_context(
            config=config,
            checkpoint_task_id="gza-1",
            worktree_git=git,
        )

    assert context is not None
    assert "Timeout checkpoint context was unavailable" in context
    assert "No reusable verify phases should be trusted from the interrupted run." in context
    assert "Reusable successful verify phases" not in context
    assert "did not contain a JSON object" in context
    assert "timeout resume checkpoint exists but could not be reused" in caplog.text
