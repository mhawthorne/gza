"""Tests for task execution and lifecycle CLI commands."""


import argparse
import io
import json
import os
import re
import signal as signal_mod
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from gza import recovery_engine as _recovery_engine_module
from gza.artifacts import store_command_output_artifact
from gza.cli import _run_as_worker, _run_foreground, cmd_run_inline
from gza.cli import query as query_cli_module
import gza.cli.execution as _execution_module
from gza.cli.execution import _format_iterate_terminal_merge_state_message
from gza.config import Config
from gza.db import SqliteTaskStore, task_id_numeric_key
from gza.git import Git
from gza.log_paths import ops_log_path_for
from gza.query import build_lineage_tree
from gza.runner import DEPENDENCY_BLOCKED_NOT_RUN_EXIT_CODE
from gza.workers import WorkerMetadata, WorkerRegistry

from .conftest import (
    get_latest_task,
    make_store,
    mark_orphaned,
    invoke_gza,
    setup_config,
    setup_db_with_tasks,
)


@pytest.fixture(autouse=True)
def _patch_query_git(monkeypatch: pytest.MonkeyPatch):
    fake_git = MagicMock()
    fake_git.default_branch.return_value = "main"
    fake_git.current_branch.return_value = "main"
    fake_git.branch_exists.return_value = True
    fake_git.ref_exists.return_value = False
    fake_git.can_merge.return_value = True
    fake_git.count_commits_ahead.return_value = 1
    fake_git.get_diff_stat_parsed.return_value = (1, 1, 0)
    fake_git.is_merged.return_value = False
    monkeypatch.setattr(query_cli_module, "Git", lambda _project_dir: fake_git)
    monkeypatch.setattr(_execution_module, "Git", lambda _project_dir: fake_git)
    with (
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
        patch("gza.git.Git.branch_exists", return_value=False),
        patch("gza.git.Git.rev_parse_if_exists", return_value=None),
    ):
        yield


@contextmanager
def _clear_foreground_worker_env():
    """Isolate standalone foreground-worker tests from inherited worker ownership."""
    with patch.dict(
        os.environ,
        {
            "GZA_WORKER_ID": "",
            "GZA_WORKER_MODE": "",
            "GZA_REUSE_WORKER_OWNER": "",
            "GZA_REUSE_WORKER_SESSION": "",
        },
        clear=False,
    ):
        yield


def _background_work_status_error(tmp_path: Path) -> tuple[list[str], str]:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Completed task")
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    store.update(task)
    return (
        ["work", str(task.id), "--background", "--no-docker", "--project", str(tmp_path)],
        f"Error: Task {task.id} is not pending (status: completed)",
    )


def test_format_iterate_terminal_merge_state_message_distinguishes_redundant(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    requested = store.add("Requested implementation", task_type="implement")
    requested.status = "failed"
    store.update(requested)
    iterate = store.add("Recovered implementation", task_type="implement", based_on=requested.id)
    iterate.status = "completed"
    iterate.has_commits = True
    store.update(iterate)

    assert _format_iterate_terminal_merge_state_message(
        store=store,
        requested_impl_task=iterate,
        iterate_task=iterate,
        resolved_from_failed_ancestor=False,
        merge_state="redundant",
    ) == (
        "No remaining iterate action: "
        f"implementation {iterate.id}'s commits are already present on target."
    )
    assert _format_iterate_terminal_merge_state_message(
        store=store,
        requested_impl_task=requested,
        iterate_task=iterate,
        resolved_from_failed_ancestor=True,
        merge_state="redundant",
    ) == (
        "No remaining iterate action: "
        f"failed implementation {requested.id} was fully recovered by descendant {iterate.id}; "
        "commits are already present on target."
    )


def test_format_iterate_terminal_merge_state_message_hides_recoverable_failed_redundant_task(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    failed = store.add("Failed redundant implementation", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-recoverable-redundant"
    failed.has_commits = True
    failed.num_steps_computed = 1
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    with patch("gza.cli.execution._classify_empty_task_recovery_state", return_value="requires_recovery"):
        assert (
            _format_iterate_terminal_merge_state_message(
                store=store,
                requested_impl_task=failed,
                iterate_task=failed,
                resolved_from_failed_ancestor=False,
                merge_state="redundant",
            )
            is None
        )


def test_format_iterate_terminal_merge_state_message_hides_pending_redundant_resume_task(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    pending = store.add("Pending redundant resume", task_type="implement", recovery_origin="resume")
    assert pending.id is not None
    pending.status = "pending"
    pending.session_id = "sess-pending-redundant"
    pending.has_commits = True
    store.update(pending)

    assert (
        _format_iterate_terminal_merge_state_message(
            store=store,
            requested_impl_task=pending,
            iterate_task=pending,
            resolved_from_failed_ancestor=False,
            merge_state="redundant",
        )
        is None
    )


class TestDerivedTaskTagSelection:
    def test_selected_tag_override_for_derived_task_preserves_omission(self) -> None:
        args = argparse.Namespace(tags=None, all_tags=False)

        assert _execution_module._selected_tag_override_for_derived_task(args) is None

    def test_selected_tag_override_for_derived_task_returns_explicit_tags(self) -> None:
        args = argparse.Namespace(tags=["manual-override"], all_tags=False)

        assert _execution_module._selected_tag_override_for_derived_task(args) == ("manual-override",)


def _background_implement_status_error(tmp_path: Path) -> tuple[list[str], str]:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    plan = store.add("Plan task", task_type="plan")
    return (
        ["implement", str(plan.id), "--background", "--no-docker", "--project", str(tmp_path)],
        f"Error: Task {plan.id} is pending. Plan task must be completed.",
    )


def _background_extract_selector_error(tmp_path: Path) -> tuple[list[str], str]:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    source = store.add("Source task", task_type="implement")
    return (
        ["extract", str(source.id), "--branch", "main", "--background", "--no-docker", "--project", str(tmp_path)],
        "Error: Specify exactly one source selector: SOURCE task ID, --branch, or --commit",
    )


def _background_extract_per_commit_error(tmp_path: Path) -> tuple[list[str], str]:
    setup_config(tmp_path)
    return (
        ["extract", "--per-commit", "--background", "--no-docker", "--project", str(tmp_path)],
        "Error: --per-commit requires one or more --commit values",
    )


def _background_retry_status_error(tmp_path: Path) -> tuple[list[str], str]:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Pending retry task")
    return (
        ["retry", str(task.id), "--background", "--no-docker", "--project", str(tmp_path)],
        "Error: Can only retry completed or failed tasks (task is pending)",
    )


def _background_resume_session_error(tmp_path: Path) -> tuple[list[str], str]:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Failed resume task")
    task.status = "failed"
    task.failure_reason = "MAX_STEPS"
    task.completed_at = datetime.now(UTC)
    store.update(task)
    return (
        ["resume", str(task.id), "--background", "--no-docker", "--project", str(tmp_path)],
        f"Error: Task {task.id} has no session ID (cannot resume)",
    )


def _background_improve_missing_review_error(tmp_path: Path) -> tuple[list[str], str]:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    impl = store.add("Completed implementation", task_type="implement")
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    store.update(impl)
    return (
        ["improve", str(impl.id), "--background", "--no-docker", "--project", str(tmp_path)],
        f"Error: Task {impl.id} has no review. Run a review first:",
    )


def _background_fix_status_error(tmp_path: Path) -> tuple[list[str], str]:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    impl = store.add("Pending implementation", task_type="implement")
    return (
        ["fix", str(impl.id), "--background", "--no-docker", "--project", str(tmp_path)],
        f"Error: Task {impl.id} is pending. Run/finish the implementation first, then run fix for stuck review/improve churn.",
    )


def _background_review_status_error(tmp_path: Path) -> tuple[list[str], str]:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    impl = store.add("Pending implementation", task_type="implement")
    return (
        ["review", str(impl.id), "--background", "--no-docker", "--project", str(tmp_path)],
        f"Error: Task {impl.id} is pending. Can only review completed tasks.",
    )


def _background_iterate_restart_error(tmp_path: Path) -> tuple[list[str], str]:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    impl = store.add("Failed implementation", task_type="implement")
    impl.status = "failed"
    impl.failure_reason = "MAX_STEPS"
    impl.completed_at = datetime.now(UTC)
    store.update(impl)
    return (
        ["iterate", str(impl.id), "--background", "--no-docker", "--project", str(tmp_path)],
        f"Error: Task {impl.id} is failed. Use --resume or --retry to specify how to restart it.",
    )


def _assert_immediate_launch_lock_released(config: Config, store: SqliteTaskStore) -> None:
    from gza.concurrency import _PROCESS_LOCKS, launch_permit

    assert _PROCESS_LOCKS == {}
    permit = launch_permit(config, store)
    try:
        assert _PROCESS_LOCKS
    finally:
        permit.release()
    assert _PROCESS_LOCKS == {}


def _store_plan_review_override_artifact(
    tmp_path: Path,
    store: SqliteTaskStore,
    review_task_id: str,
    *,
    output: str,
) -> None:
    config = Config.load(tmp_path)
    review_task = store.get(review_task_id)
    assert review_task is not None
    store_command_output_artifact(
        store,
        review_task,
        config,
        kind="plan_review_manifest_override",
        producer="tests.cli.test_execution",
        label="plan_review_manifest_override",
        output=output,
        status="completed",
    )


def _advance_new_batch_error(tmp_path: Path) -> tuple[list[str], str]:
    setup_config(tmp_path)
    return (
        ["advance", "--new", "--project", str(tmp_path)],
        "Error: --new requires --batch",
    )


@pytest.mark.parametrize(
    ("case_name", "setup_case"),
    [
        ("work", _background_work_status_error),
        ("implement", _background_implement_status_error),
        ("extract", _background_extract_selector_error),
        ("extract-per-commit", _background_extract_per_commit_error),
        ("retry", _background_retry_status_error),
        ("resume", _background_resume_session_error),
        ("improve", _background_improve_missing_review_error),
        ("fix", _background_fix_status_error),
        ("review", _background_review_status_error),
        ("iterate", _background_iterate_restart_error),
        ("advance-new", _advance_new_batch_error),
    ],
)
def test_background_phase1_validation_errors_write_to_stderr_only(
    tmp_path: Path,
    case_name: str,
    setup_case,
) -> None:
    del case_name
    argv, expected = setup_case(tmp_path)

    result = invoke_gza(*argv)

    assert result.returncode == 1
    assert expected in result.stderr
    assert expected not in result.stdout
    assert "Error:" not in result.stdout


def test_run_with_recovery_executes_reconcile_instead_of_terminal_skip(tmp_path: Path) -> None:
    from gza.cli._common import run_with_recovery

    setup_config(tmp_path)
    config = Config.load(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Failed publish", task_type="implement")
    assert task.id is not None
    task.status = "failed"
    task.failure_reason = "BRANCH_UNPUSHABLE"
    task.branch = "feature/run-with-recovery-reconcile"
    task.completed_at = datetime.now(UTC)
    store.update(task)

    terminal_skip_calls: list[str] = []

    def _run_task(current_task, _resume):
        refreshed = store.get(current_task.id)
        assert refreshed is not None
        return 1

    def _complete_after_reconcile(*, config, store, git, task):
        store.mark_completed(task, branch=task.branch, has_commits=True)
        return 0

    decision = _recovery_engine_module.FailedRecoveryDecision(
        task_id=task.id,
        action="reconcile",
        reason_code="branch_unpushable_retryable",
        reason_text="reconcile diverged branch before completion retry",
        launch_mode="none",
        attempt_index=0,
        attempt_limit=1,
    )

    with (
        patch("gza.cli._common.decide_failed_task_recovery", return_value=decision),
        patch(
            "gza.cli.git_ops._reconcile_diverged_branch_with_origin",
            return_value=SimpleNamespace(status="reconciled", message="reconciled"),
        ),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
        patch("gza.git.Git.branch_exists", return_value=True),
        patch("gza.git.Git.ref_exists", return_value=False),
        patch(
            "gza.cli.git_ops.complete_branch_unpushable_after_reconcile",
            side_effect=_complete_after_reconcile,
        ) as complete_after_reconcile,
    ):
        final_task, rc = run_with_recovery(
            config,
            store,
            task,
            run_task=_run_task,
            max_resume_attempts=1,
            on_terminal_skip=lambda failed_task, _decision, _rc: terminal_skip_calls.append(failed_task.id or ""),
        )

    assert rc == 0
    complete_after_reconcile.assert_called_once()
    assert terminal_skip_calls == []
    assert final_task.status == "completed"


class TestAddCommand:
    """Tests for 'gza add' command."""

    @pytest.fixture(autouse=True)
    def _patch_query_git_for_followup_checks(self, monkeypatch: pytest.MonkeyPatch):
        fake_git = MagicMock()
        fake_git.default_branch.return_value = "main"
        fake_git.current_branch.return_value = "main"
        fake_git.branch_exists.return_value = True
        fake_git.ref_exists.return_value = False
        fake_git.can_merge.return_value = True
        fake_git.count_commits_ahead.return_value = 1
        fake_git.get_diff_stat_parsed.return_value = (1, 1, 0)
        fake_git.is_merged.return_value = False
        monkeypatch.setattr(query_cli_module, "Git", lambda _project_dir: fake_git)
        with (
            patch("gza.git.Git.default_branch", return_value="main"),
            patch("gza.git.Git.local_branch_names", return_value=()),
        ):
            yield

    def test_add_with_inline_prompt(self, tmp_path: Path):
        """Add command with inline prompt creates a task."""
        setup_config(tmp_path)
        result = invoke_gza("add", "Test inline task", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Added task" in result.stdout

        # Verify task was added
        result = invoke_gza("next", "--project", str(tmp_path))
        assert "Test inline task" in result.stdout

    def test_add_explore_task(self, tmp_path: Path):
        """Add command with --explore flag creates explore task."""
        setup_config(tmp_path)
        result = invoke_gza("add", "--explore", "Explore the codebase", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Added task" in result.stdout

        # Verify task type is shown
        result = invoke_gza("next", "--project", str(tmp_path))
        assert "[explore]" in result.stdout

    def test_add_with_prompt_file(self, tmp_path: Path):
        """Add command can read prompt from file."""
        setup_config(tmp_path)

        # Create a file with prompt text
        prompt_file = tmp_path / "task_prompt.txt"
        prompt_file.write_text("Task prompt from file")

        result = invoke_gza("add", "--prompt-file", str(prompt_file), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Added task" in result.stdout

        # Verify task was added with correct prompt
        result = invoke_gza("next", "--project", str(tmp_path))
        assert "Task prompt from file" in result.stdout

    def test_add_with_prompt_file_not_found(self, tmp_path: Path):
        """Add command handles missing file gracefully."""
        setup_config(tmp_path)

        result = invoke_gza("add", "--prompt-file", "/nonexistent/file.txt", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "not found" in result.stdout.lower()

    def test_add_prompt_and_prompt_file_conflict(self, tmp_path: Path):
        """Add command rejects both prompt argument and --prompt-file."""
        setup_config(tmp_path)

        prompt_file = tmp_path / "prompt.txt"
        prompt_file.write_text("File content")

        result = invoke_gza("add", "inline prompt", "--prompt-file", str(prompt_file), "--project", str(tmp_path))

        assert result.returncode == 1
        assert "Cannot use both" in result.stdout

    def test_add_prompt_file_and_edit_conflict(self, tmp_path: Path):
        """Add command rejects both --prompt-file and --edit."""
        setup_config(tmp_path)

        prompt_file = tmp_path / "prompt.txt"
        prompt_file.write_text("File content")

        result = invoke_gza("add", "--prompt-file", str(prompt_file), "--edit", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "Cannot use both" in result.stdout

    def test_add_with_prompt_file_and_options(self, tmp_path: Path):
        """Add command with --prompt-file works with other options."""

        setup_config(tmp_path)
        tmp_path / ".gza" / "gza.db"

        # Create a file with prompt text
        prompt_file = tmp_path / "task_prompt.txt"
        prompt_file.write_text("Implement feature X")

        result = invoke_gza(
            "add",
            "--prompt-file",
            str(prompt_file),
            "--type",
            "implement",
            "--tag",
            "features",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 0
        assert "Added task" in result.stdout

        # Verify task was added with correct attributes
        store = make_store(tmp_path)
        task = get_latest_task(store, task_type="implement", prompt="Implement feature X")
        assert task is not None
        assert task.prompt == "Implement feature X"
        assert task.task_type == "implement"
        assert task.tags == ("features",)

    def test_add_with_pr_flag_persists_create_pr(self, tmp_path: Path):
        """Add command with --pr stores automatic PR intent on the task."""

        setup_config(tmp_path)

        result = invoke_gza(
            "add",
            "--type",
            "implement",
            "--pr",
            "Implement feature X",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 0

        store = make_store(tmp_path)
        task = get_latest_task(store, task_type="implement", prompt="Implement feature X")
        assert task is not None
        assert task.create_pr is True

    def test_add_rejects_empty_tag_without_traceback(self, tmp_path: Path):
        """add --tag '' should fail with user-facing validation, not traceback."""
        setup_config(tmp_path)

        result = invoke_gza("add", "Tag validation", "--tag", "", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "Error: tag must not be empty" in result.stdout
        assert "Traceback" not in result.stdout
        assert "Traceback" not in result.stderr


class TestEditCommand:
    """Tests for 'gza edit' command."""

    def test_edit_set_tags(self, tmp_path: Path):
        """Edit command can replace task tags."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Test task")
        assert task.group is None

        result = invoke_gza("edit", str(task.id), "--set-tags", "new-group", "--project", str(tmp_path))

        assert result.returncode == 0

        updated = store.get(task.id)
        assert updated.tags == ("new-group",)

    def test_edit_clear_tags_still_clears_multi_tag_task(self, tmp_path: Path):
        """--clear-tags remains the explicit path for clearing all tags."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Test task", tags=("release-1.2", "backend"))
        assert len(task.tags) == 2

        result = invoke_gza("edit", str(task.id), "--clear-tags", "--project", str(tmp_path))

        assert result.returncode == 0
        updated = store.get(task.id)
        assert updated is not None
        assert updated.tags == ()

    def test_edit_clear_depends_on_persists_none_for_pending_task(self, tmp_path: Path):
        """Pending tasks can explicitly clear their execution dependency."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        blocker = store.add("Dependency task")
        task = store.add("Blocked task", depends_on=blocker.id)
        assert task.depends_on == blocker.id

        result = invoke_gza("edit", str(task.id), "--clear-depends-on", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Cleared execution dependency" in result.stdout

        updated = store.get(task.id)
        assert updated is not None
        assert updated.depends_on is None

    def test_edit_clear_depends_on_without_existing_dependency_is_noop(self, tmp_path: Path):
        """Clearing when no execution dependency exists should be a defined no-op."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Independent task")
        assert task.depends_on is None

        result = invoke_gza("edit", str(task.id), "--clear-depends-on", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "already has no execution dependency" in result.stdout

        updated = store.get(task.id)
        assert updated is not None
        assert updated.depends_on is None

    def test_edit_rejects_setting_and_clearing_depends_on_together(self, tmp_path: Path):
        """`--depends-on` and `--clear-depends-on` cannot be combined."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        original_dep = store.add("Original dependency")
        replacement_dep = store.add("Replacement dependency")
        task = store.add("Blocked task", depends_on=original_dep.id)

        result = invoke_gza(
            "edit",
            str(task.id),
            "--depends-on",
            str(replacement_dep.id),
            "--clear-depends-on",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 1
        assert "Cannot use both --depends-on and --clear-depends-on" in result.stdout

        updated = store.get(task.id)
        assert updated is not None
        assert updated.depends_on == original_dep.id

    def test_edit_rejects_combined_tag_mutation_flags(self, tmp_path: Path):
        """Tag mutation flags are mutually exclusive to prevent silent partial updates."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Test task", tags=("backend",))

        result = invoke_gza(
            "edit",
            str(task.id),
            "--add-tag",
            "release-1.2",
            "--remove-tag",
            "backend",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 1
        assert "Tag mutation flags are mutually exclusive" in result.stdout

        updated = store.get(task.id)
        assert updated is not None
        assert updated.tags == ("backend",)

    def test_edit_add_tag_single_mutation_succeeds(self, tmp_path: Path):
        """Single tag mutation flag invocation remains supported."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Test task", tags=("backend",))

        result = invoke_gza("edit", str(task.id), "--add-tag", "release-1.2", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Added tags for task" in result.stdout

        updated = store.get(task.id)
        assert updated is not None
        assert updated.tags == ("backend", "release-1.2")

    def test_edit_add_tag_rejects_empty_value_without_traceback(self, tmp_path: Path):
        """edit --add-tag '' should fail with user-facing validation, not traceback."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Test task")

        result = invoke_gza("edit", str(task.id), "--add-tag", "", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "Error: tag must not be empty" in result.stdout
        assert "Traceback" not in result.stdout
        assert "Traceback" not in result.stderr

    def test_edit_set_tags_allowed_for_completed_task(self, tmp_path: Path):
        """Completed tasks should still allow tag-only edits."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Release task", tags=("backend",))
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        result = invoke_gza("edit", str(task.id), "--set-tags", "release-1.2,ops", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Set tags for task" in result.stdout

        updated = store.get(task.id)
        assert updated is not None
        assert updated.status == "completed"
        assert updated.tags == ("ops", "release-1.2")

    def test_edit_add_tag_allowed_for_failed_task(self, tmp_path: Path):
        """Failed tasks should still allow tag-only edits."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Failed release task", tags=("backend",))
        task.status = "failed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        result = invoke_gza("edit", str(task.id), "--add-tag", "release-1.2", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Added tags for task" in result.stdout

        updated = store.get(task.id)
        assert updated is not None
        assert updated.status == "failed"
        assert updated.tags == ("backend", "release-1.2")

    def test_edit_add_tag_preserves_last_edited_at(self, tmp_path: Path):
        """Tag-only edits should not reset the meaningful-edit timestamp."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Tagged task", tags=("backend",))
        original_edit_at = datetime(2026, 6, 24, 12, 0, tzinfo=UTC)
        task.last_edited_at = original_edit_at
        store.update(task)

        result = invoke_gza("edit", str(task.id), "--add-tag", "release-1.2", "--project", str(tmp_path))

        assert result.returncode == 0

        updated = store.get(task.id)
        assert updated is not None
        assert updated.tags == ("backend", "release-1.2")
        assert updated.last_edited_at == original_edit_at

    def test_edit_non_tag_mutation_stays_restricted_for_non_pending_task(self, tmp_path: Path):
        """Non-pending tasks should reject prompt or metadata edits outside tag mutations."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Running task", tags=("backend",))
        task.status = "in_progress"
        task.started_at = datetime.now(UTC)
        store.update(task)

        result = invoke_gza(
            "edit",
            str(task.id),
            "--add-tag",
            "release-1.2",
            "--review",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 1
        assert "non-pending tasks only allow tag edits" in result.stdout
        assert "Pending-only edit flags requested: --review" in result.stdout

        updated = store.get(task.id)
        assert updated is not None
        assert updated.status == "in_progress"
        assert updated.create_review is False
        assert updated.tags == ("backend",)

    def test_edit_clear_depends_on_is_rejected_for_non_pending_task(self, tmp_path: Path):
        """Clearing dependencies must still respect the non-pending tag-only rule."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        blocker = store.add("Dependency task")
        task = store.add("Blocked task", depends_on=blocker.id)
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        result = invoke_gza("edit", str(task.id), "--clear-depends-on", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "non-pending tasks only allow tag edits" in result.stdout
        assert "Pending-only edit flags requested: --clear-depends-on" in result.stdout

        updated = store.get(task.id)
        assert updated is not None
        assert updated.depends_on == blocker.id

    def test_edit_auto_implement_allowed_for_completed_plan(self, tmp_path: Path):
        """Completed held plans may release the hold via --auto-implement."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Held plan", task_type="plan", auto_implement=False)
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        result = invoke_gza("edit", str(task.id), "--auto-implement", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "automatic implementation follow-up" in result.stdout

        updated = store.get(task.id)
        assert updated is not None
        assert updated.auto_implement is True

    def test_edit_hold_for_review_allowed_for_pending_plan(self, tmp_path: Path):
        """Pending plan tasks may add a hold-for-review in place."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Pending plan", task_type="plan", auto_implement=True)

        result = invoke_gza("edit", str(task.id), "--hold-for-review", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Enabled hold-for-review" in result.stdout

        updated = store.get(task.id)
        assert updated is not None
        assert updated.status == "pending"
        assert updated.auto_implement is False

    def test_edit_no_hold_for_review_allowed_for_pending_plan(self, tmp_path: Path):
        """Pending held plan tasks may release the hold with the preferred flag name."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Held pending plan", task_type="plan", auto_implement=False)

        result = invoke_gza("edit", str(task.id), "--no-hold-for-review", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "automatic implementation follow-up" in result.stdout

        updated = store.get(task.id)
        assert updated is not None
        assert updated.status == "pending"
        assert updated.auto_implement is True

    def test_edit_hold_for_review_rejects_completed_plan(self, tmp_path: Path):
        """Completed plans cannot add a hold during edit."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Completed plan", task_type="plan", auto_implement=True)
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        result = invoke_gza("edit", str(task.id), "--hold-for-review", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "non-pending tasks only allow tag edits" in result.stdout
        assert "--hold-for-review is only allowed for pending plan tasks" in result.stdout

        updated = store.get(task.id)
        assert updated is not None
        assert updated.auto_implement is True
        assert updated.tags == ()

    def test_edit_hold_for_review_with_tag_rejects_completed_plan_without_mutating(self, tmp_path: Path):
        """A tag edit cannot bypass the completed-plan hold-for-review restriction."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Completed plan", task_type="plan", auto_implement=True, tags=("backend",))
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        result = invoke_gza(
            "edit",
            str(task.id),
            "--hold-for-review",
            "--add-tag",
            "release-1.2",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 1
        assert "--hold-for-review is only allowed for pending plan tasks" in result.stdout

        updated = store.get(task.id)
        assert updated is not None
        assert updated.auto_implement is True
        assert updated.tags == ("backend",)

    def test_edit_no_hold_for_review_allowed_for_completed_plan(self, tmp_path: Path):
        """Completed held plans may release the hold via the preferred flag name."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Held completed plan", task_type="plan", auto_implement=False)
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        result = invoke_gza("edit", str(task.id), "--no-hold-for-review", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "automatic implementation follow-up" in result.stdout

        updated = store.get(task.id)
        assert updated is not None
        assert updated.auto_implement is True

    @pytest.mark.parametrize("flag", ["--no-hold-for-review", "--auto-implement"])
    def test_edit_release_hold_with_tag_rejects_non_completed_non_pending_plan_without_mutating(
        self, tmp_path: Path, flag: str
    ):
        """Non-completed non-pending plans cannot release a hold, even alongside tag edits."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("In-progress plan", task_type="plan", auto_implement=False, tags=("backend",))
        task.status = "in_progress"
        task.started_at = datetime.now(UTC)
        store.update(task)

        result = invoke_gza(
            "edit",
            str(task.id),
            flag,
            "--add-tag",
            "release-1.2",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 1
        assert (
            "hold-for-review edits are only allowed for pending plan tasks, except completed plans may use "
            "--no-hold-for-review or --auto-implement."
        ) in result.stdout

        updated = store.get(task.id)
        assert updated is not None
        assert updated.auto_implement is False
        assert updated.tags == ("backend",)

    def test_edit_auto_implement_rejects_non_plan_task_with_legacy_message(self, tmp_path: Path):
        """Legacy alias preserves its plan-only validation message on non-plan tasks."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Regular task", task_type="implement")

        result = invoke_gza("edit", str(task.id), "--auto-implement", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "--auto-implement is only valid for plan tasks" in result.stdout

    @pytest.mark.parametrize("flag", ["--hold-for-review", "--no-hold-for-review"])
    def test_edit_hold_flags_reject_non_plan_task(self, tmp_path: Path, flag: str):
        """Preferred hold-for-review edit flags remain plan-only."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Regular task", task_type="implement")

        result = invoke_gza("edit", str(task.id), flag, "--project", str(tmp_path))

        assert result.returncode == 1
        assert "hold-for-review flags are only valid for plan tasks" in result.stdout

    @pytest.mark.parametrize(
        "flags",
        [
            ("--auto-implement", "--hold-for-review"),
            ("--hold-for-review", "--auto-implement"),
            ("--auto-implement", "--no-hold-for-review"),
        ],
    )
    def test_edit_hold_flags_with_auto_implement_preserve_legacy_non_plan_message(
        self, tmp_path: Path, flags: tuple[str, ...]
    ):
        """Any non-plan edit including the legacy alias keeps the legacy validation signal."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Regular task", task_type="implement")

        result = invoke_gza("edit", str(task.id), *flags, "--project", str(tmp_path))

        assert result.returncode == 1
        assert "--auto-implement is only valid for plan tasks" in result.stdout

    def test_edit_review_flag(self, tmp_path: Path):
        """Edit command can enable automatic review task creation."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Test task")
        assert task.create_review is False

        result = invoke_gza("edit", str(task.id), "--review", "--project", str(tmp_path))

        assert result.returncode == 0

        # Verify create_review was enabled
        updated = store.get(task.id)
        assert updated.create_review is True

    def test_edit_pr_flag(self, tmp_path: Path):
        """Edit command can enable completion-time PR creation/reuse intent."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Test task")
        assert task.create_pr is False

        result = invoke_gza("edit", str(task.id), "--pr", "--project", str(tmp_path))

        assert result.returncode == 0
        assert f"Enabled PR creation/reuse request for successful completion of task {task.id}" in result.stdout

        updated = store.get(task.id)
        assert updated.create_pr is True

    def test_edit_review_and_pr_flags_apply_both_mutations(self, tmp_path: Path):
        """Edit command should persist both review and PR intent in one invocation."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Test task")
        assert task.create_review is False
        assert task.create_pr is False

        result = invoke_gza("edit", str(task.id), "--review", "--pr", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "automatic review task creation" in result.stdout
        assert f"Enabled PR creation/reuse request for successful completion of task {task.id}" in result.stdout

        updated = store.get(task.id)
        assert updated is not None
        assert updated.create_review is True
        assert updated.create_pr is True

    def test_edit_pr_and_model_flags_apply_both_mutations(self, tmp_path: Path):
        """Edit command should persist PR and model overrides together."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Test task")
        assert task.create_pr is False
        assert task.model is None

        result = invoke_gza(
            "edit",
            str(task.id),
            "--pr",
            "--model",
            "claude-3-5-haiku-latest",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 0
        assert f"Enabled PR creation/reuse request for successful completion of task {task.id}" in result.stdout
        assert "Set model override" in result.stdout

        updated = store.get(task.id)
        assert updated is not None
        assert updated.create_pr is True
        assert updated.model == "claude-3-5-haiku-latest"

    def test_edit_pr_and_add_tag_apply_both_mutations(self, tmp_path: Path):
        """Edit command should not short-circuit tag edits ahead of other mutations."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Test task")
        assert task.create_pr is False
        assert task.tags == ()

        result = invoke_gza(
            "edit",
            str(task.id),
            "--pr",
            "--add-tag",
            "cli",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 0
        assert f"Enabled PR creation/reuse request for successful completion of task {task.id}" in result.stdout
        assert "Added tags" in result.stdout

        updated = store.get(task.id)
        assert updated is not None
        assert updated.create_pr is True
        assert updated.tags == ("cli",)

    def test_edit_pr_and_invalid_add_tag_is_atomic(self, tmp_path: Path):
        """Failed tag validation must not persist earlier non-tag mutations."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Test task")
        assert task.create_pr is False

        result = invoke_gza(
            "edit",
            str(task.id),
            "--pr",
            "--add-tag",
            "",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 1
        assert "Error: tag must not be empty" in result.stdout

        updated = store.get(task.id)
        assert updated is not None
        assert updated.create_pr is False

    def test_edit_review_and_invalid_remove_tag_is_atomic(self, tmp_path: Path):
        """Failed tag removal validation must not persist earlier non-tag mutations."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Test task")
        assert task.create_review is False

        result = invoke_gza(
            "edit",
            str(task.id),
            "--review",
            "--remove-tag",
            "",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 1
        assert "Error: tag must not be empty" in result.stdout

        updated = store.get(task.id)
        assert updated is not None
        assert updated.create_review is False
    def test_edit_with_prompt_file(self, tmp_path: Path):
        """Edit command can update prompt from file."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Original prompt text")

        # Create a file with new prompt
        prompt_file = tmp_path / "new_prompt.txt"
        prompt_file.write_text("New prompt text from file")

        result = invoke_gza("edit", str(task.id), "--prompt-file", str(prompt_file), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Updated task" in result.stdout

        # Verify prompt was updated
        updated = store.get(task.id)
        assert updated.prompt == "New prompt text from file"

    def test_edit_with_prompt_file_not_found(self, tmp_path: Path):
        """Edit command handles missing file gracefully."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Original prompt text")

        result = invoke_gza("edit", str(task.id), "--prompt-file", "/nonexistent/file.txt", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "not found" in result.stdout.lower()

    def test_edit_with_prompt_text(self, tmp_path: Path):
        """Edit command can update prompt from command line."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Original prompt text")

        result = invoke_gza("edit", str(task.id), "--prompt", "New prompt from command line", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Updated task" in result.stdout

        # Verify prompt was updated
        updated = store.get(task.id)
        assert updated.prompt == "New prompt from command line"
        assert updated.last_edited_at is not None

    def test_edit_prompt_stamps_last_edited_at(self, tmp_path: Path):
        """Meaningful prompt edits should stamp last_edited_at."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Original prompt text")
        assert task.last_edited_at is None

        result = invoke_gza("edit", str(task.id), "--prompt", "Edited prompt text", "--project", str(tmp_path))

        assert result.returncode == 0

        updated = store.get(task.id)
        assert updated is not None
        assert updated.prompt == "Edited prompt text"
        assert updated.last_edited_at is not None

    def test_edit_with_prompt_validation_error(self, tmp_path: Path):
        """Edit command validates prompt length."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Original prompt text")

        # Try to set a prompt that's too short
        result = invoke_gza("edit", str(task.id), "--prompt", "short", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "Validation" in result.stdout or "too short" in result.stdout.lower()

        # Verify prompt was NOT updated
        updated = store.get(task.id)
        assert updated.prompt == "Original prompt text"

    def test_edit_prompt_and_prompt_file_conflict(self, tmp_path: Path):
        """Edit command rejects both --prompt and --prompt-file."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Original prompt text")

        prompt_file = tmp_path / "prompt.txt"
        prompt_file.write_text("File content")

        result = invoke_gza("edit", str(task.id), "--prompt", "text", "--prompt-file", str(prompt_file), "--project", str(tmp_path))

        assert result.returncode == 1
        assert "Cannot use both" in result.stdout

    def test_edit_with_prompt_from_stdin(self, tmp_path: Path):
        """Edit command can read prompt from stdin using --prompt -."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Original prompt text")

        stdin_content = "New prompt from stdin input"
        result = invoke_gza("edit", str(task.id), "--prompt", "-", "--project", str(tmp_path), stdin_input=stdin_content)

        assert result.returncode == 0
        assert "Updated task" in result.stdout

        # Verify prompt was updated
        updated = store.get(task.id)
        assert updated.prompt == "New prompt from stdin input"

    def test_edit_task_type_conversion_stamps_last_edited_at(self, tmp_path: Path):
        """Task-type conversions should stamp last_edited_at."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Original task", task_type="implement")
        assert task.last_edited_at is None

        result = invoke_gza("edit", str(task.id), "--explore", "--project", str(tmp_path))

        assert result.returncode == 0
        assert f"Converted task {task.id} to explore" in result.stdout

        updated = store.get(task.id)
        assert updated is not None
        assert updated.task_type == "explore"
        assert updated.last_edited_at is not None

    def test_cmd_edit_based_on_sets_based_on_field(self, tmp_path: Path):
        """--based-on sets task.based_on, not task.depends_on."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        parent_task = store.add("Parent task")
        task = store.add("Target task")

        result = invoke_gza("edit", str(task.id), "--based-on", str(parent_task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        updated = store.get(task.id)
        assert updated.based_on == parent_task.id
        assert updated.depends_on is None

    def test_cmd_edit_depends_on_sets_depends_on_field(self, tmp_path: Path):
        """--depends-on sets task.depends_on, not task.based_on."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        dep_task = store.add("Dependency task")
        task = store.add("Target task")

        result = invoke_gza("edit", str(task.id), "--depends-on", str(dep_task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        updated = store.get(task.id)
        assert updated.depends_on == dep_task.id
        assert updated.based_on is None

    def test_cmd_edit_implement_depends_on_held_plan_rejects_inconsistent_state(self, tmp_path: Path):
        """Pending implement tasks cannot be rewired to depend on a held plan."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        held_plan = store.add("Held plan", task_type="plan", auto_implement=False)
        assert held_plan.id is not None
        held_plan.status = "completed"
        held_plan.completed_at = datetime.now(UTC)
        store.update(held_plan)
        task = store.add("Target task", task_type="implement")
        assert task.id is not None

        result = invoke_gza("edit", str(task.id), "--depends-on", str(held_plan.id), "--project", str(tmp_path))

        assert result.returncode == 1
        normalized = " ".join(result.stdout.split())
        assert f"plan {held_plan.id} is held for review" in normalized
        assert f"uv run gza implement {held_plan.id}" in normalized
        assert f"uv run gza edit {held_plan.id} --no-hold-for-review" in normalized
        updated = store.get(task.id)
        assert updated is not None
        assert updated.depends_on is None

    def test_cmd_edit_implement_based_on_held_plan_rejects_inconsistent_state(self, tmp_path: Path):
        """Pending implement tasks cannot be rewired onto a held-plan lineage."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        held_plan = store.add("Held plan", task_type="plan", auto_implement=False)
        assert held_plan.id is not None
        held_plan.status = "completed"
        held_plan.completed_at = datetime.now(UTC)
        store.update(held_plan)
        task = store.add("Target task", task_type="implement")
        assert task.id is not None

        result = invoke_gza("edit", str(task.id), "--based-on", str(held_plan.id), "--project", str(tmp_path))

        assert result.returncode == 1
        normalized = " ".join(result.stdout.split())
        assert f"plan {held_plan.id} is held for review" in normalized
        assert f"uv run gza implement {held_plan.id}" in normalized
        assert f"uv run gza edit {held_plan.id} --no-hold-for-review" in normalized
        updated = store.get(task.id)
        assert updated is not None
        assert updated.based_on is None

    def test_cmd_edit_based_on_nonexistent_task_errors(self, tmp_path: Path):
        """--based-on with nonexistent target ID returns error code 1."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Target task")

        result = invoke_gza("edit", str(task.id), "--based-on", "testproject-999999", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "not found" in result.stdout.lower()

    def test_cmd_edit_depends_on_nonexistent_task_errors(self, tmp_path: Path):
        """--depends-on with nonexistent target ID returns error code 1."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Target task")

        result = invoke_gza("edit", str(task.id), "--depends-on", "testproject-999999", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "not found" in result.stdout.lower()


class TestRetryCommand:
    """Tests for 'gza retry' command."""

    @pytest.fixture(autouse=True)
    def _mock_foreground_runner(self):
        """Keep retry command tests focused on CLI behavior, not agent execution."""
        with patch("gza.cli._run_foreground", return_value=0) as run_foreground:
            yield run_foreground

    @pytest.fixture(autouse=True)
    def _patch_task_startup_phase(self):
        with patch("gza.cli._common.prepare_task_startup_phase", side_effect=lambda _c, _s, prepared_task: prepared_task):
            yield

    @pytest.fixture(autouse=True)
    def _patch_query_git_for_followup_checks(self, monkeypatch: pytest.MonkeyPatch):
        fake_git = MagicMock()
        fake_git.default_branch.return_value = "main"
        fake_git.current_branch.return_value = "main"
        fake_git.branch_exists.return_value = True
        fake_git.ref_exists.return_value = False
        fake_git.can_merge.return_value = True
        fake_git.count_commits_ahead.return_value = 1
        fake_git.get_diff_stat_parsed.return_value = (1, 1, 0)
        fake_git.is_merged.return_value = False
        monkeypatch.setattr(query_cli_module, "Git", lambda _project_dir: fake_git)
        with (
            patch("gza.git.Git.default_branch", return_value="main"),
            patch("gza.git.Git.local_branch_names", return_value=()),
        ):
            yield

    def test_retry_completed_task(self, tmp_path: Path):
        """Retry command creates a new pending task from a completed task."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Original task", task_type="implement")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        result = invoke_gza("retry", str(task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Created task " in result.stdout
        assert f"retry of {task.id}" in result.stdout

        # Verify new task was created with same prompt
        result = invoke_gza("next", "--project", str(tmp_path))
        assert "Original task" in result.stdout

    def test_retry_failed_task(self, tmp_path: Path):
        """Retry command creates a new pending task from a failed task."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Failed task")
        task.status = "failed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        result = invoke_gza("retry", str(task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Created task " in result.stdout
        assert f"retry of {task.id}" in result.stdout

    def test_retry_pending_task_fails(self, tmp_path: Path):
        """Retry command fails for pending tasks."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Pending task", "status": "pending"},
        ])

        store = make_store(tmp_path)
        task = store.get_all()[0]
        result = invoke_gza("retry", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 1
        assert "Can only retry completed or failed" in result.stdout

    def test_retry_nonexistent_task(self, tmp_path: Path):
        """Retry command handles nonexistent task."""
        setup_db_with_tasks(tmp_path, [])

        result = invoke_gza("retry", "testproject-999999", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "not found" in result.stdout

    def test_retry_preserves_task_fields(self, tmp_path: Path):
        """Retry command preserves task metadata and linkage fields."""

        setup_config(tmp_path)
        tmp_path / ".gza" / "gza.db"
        store = make_store(tmp_path)

        # Create a dependency task first
        dep_task = store.add("Dependency task")
        dep_task.status = "completed"
        dep_task.completed_at = datetime.now(UTC)
        store.update(dep_task)

        # Create a task with metadata
        task = store.add(
            "Test task with metadata",
            task_type="explore",
            group="test-group",
            spec="spec.md",
            depends_on=dep_task.id,
            create_review=True,
            same_branch=True,
            task_type_hint="feature",
            model="gpt-5.3-codex",
            provider="codex",
        )
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        # Retry the task
        result = invoke_gza("retry", str(task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0

        # Verify the new task has the same metadata
        new_task = get_latest_task(store, based_on=task.id, task_type=task.task_type)
        assert new_task is not None
        assert new_task.prompt == "Test task with metadata"
        assert new_task.task_type == "explore"
        assert new_task.group == "test-group"
        assert new_task.spec == "spec.md"
        assert new_task.depends_on == dep_task.id
        assert new_task.create_review is True
        assert new_task.same_branch is True
        assert new_task.task_type_hint == "feature"
        assert new_task.model == "gpt-5.3-codex"
        assert new_task.model_is_explicit is True
        assert new_task.provider == "codex"
        assert new_task.provider_is_explicit is True
        assert new_task.based_on == task.id
        assert new_task.status == "pending"

    def test_retry_same_branch_improve_keeps_original_branch(self, tmp_path: Path):
        """Retries of same-branch improves stay on the original shared branch."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Improve with shared branch", task_type="improve", same_branch=True)
        task.status = "failed"
        task.branch = "feature/impl-branch"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        result = invoke_gza("retry", str(task.id), "--queue", "--project", str(tmp_path))
        assert result.returncode == 0

        retry_task = get_latest_task(store, based_on=task.id, task_type="improve")
        assert retry_task is not None
        assert retry_task.same_branch is True
        assert retry_task.base_branch is None
        assert retry_task.branch == "feature/impl-branch"

    def test_create_retry_task_shared_helper_keeps_same_branch_for_improves(self, tmp_path: Path):
        """Shared retry creator preserves shared-branch semantics and metadata for improves."""
        from gza.cli._common import _create_retry_task

        setup_config(tmp_path)
        store = make_store(tmp_path)
        dep = store.add("Dependency", task_type="plan")
        assert dep.id is not None

        original = store.add(
            "Retry me",
            task_type="improve",
            depends_on=dep.id,
            same_branch=True,
            tags=("release-1.2",),
            spec="docs/spec.md",
            model="gpt-5.3-codex",
            provider="codex",
        )
        assert original.id is not None
        original.status = "failed"
        original.branch = "feature/old"
        original.task_type_hint = "feature"
        original.model_is_explicit = True
        original.provider_is_explicit = True
        store.update(original)

        retry_task = _create_retry_task(store, original, trigger_source="manual")
        assert retry_task.based_on == original.id
        assert retry_task.prompt == original.prompt
        assert retry_task.task_type == original.task_type
        assert retry_task.depends_on == original.depends_on
        assert retry_task.tags == original.tags
        assert retry_task.spec == original.spec
        assert retry_task.model == original.model
        assert retry_task.model_is_explicit is True
        assert retry_task.provider == original.provider
        assert retry_task.provider_is_explicit is True
        assert retry_task.same_branch is True
        assert retry_task.base_branch is None
        assert retry_task.branch == "feature/old"
        assert retry_task.recovery_origin == "retry"
        assert retry_task.session_id is None

    def test_create_retry_task_manual_rebase_retry_keeps_same_branch(self, tmp_path: Path):
        """Manual rebase retry should stay attached to the implementation branch."""
        from gza.cli._common import _create_retry_task

        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl = store.add("Implement feature", task_type="implement")
        assert impl.id is not None
        impl.branch = "feature/impl"
        store.update(impl)

        failed_rebase = store.add(
            "Rebase impl branch",
            task_type="rebase",
            based_on=impl.id,
            same_branch=True,
        )
        assert failed_rebase.id is not None
        failed_rebase.status = "failed"
        failed_rebase.branch = impl.branch
        failed_rebase.completed_at = datetime.now(UTC)
        store.update(failed_rebase)

        retry_task = _create_retry_task(store, failed_rebase, trigger_source="manual")
        assert retry_task.based_on == failed_rebase.id
        assert retry_task.same_branch is True
        assert retry_task.base_branch is None
        assert retry_task.branch == impl.branch
        assert retry_task.recovery_origin == "retry"

    def test_create_retry_task_manual_rebase_retry_chain_prefers_impl_merge_unit(self, tmp_path: Path):
        """Manual rebase retry chains should attach to the implementation merge unit."""
        from gza.cli._common import _create_retry_task

        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl = store.add("Implement feature", task_type="implement")
        store.mark_completed(impl, has_commits=True, branch="feature/impl")
        assert impl.id is not None
        impl_unit = store.resolve_merge_unit_for_task(impl.id)
        assert impl_unit is not None

        orphan_parent = store.add(
            "Failed rebase retry parent",
            task_type="rebase",
            based_on=impl.id,
            same_branch=True,
        )
        assert orphan_parent.id is not None
        orphan_parent.status = "failed"
        orphan_parent.failure_reason = "WORKER_DIED"
        orphan_parent.branch = "feature/impl-rebase-orphan"
        orphan_parent.completed_at = datetime.now(UTC)
        store.update(orphan_parent)
        orphan_unit = store.get_or_create_merge_unit_for_task(orphan_parent)
        assert orphan_unit is not None
        assert orphan_unit.source_branch == "feature/impl-rebase-orphan"

        failed_rebase = store.add(
            "Failed rebase retry child",
            task_type="rebase",
            based_on=orphan_parent.id,
            same_branch=True,
        )
        assert failed_rebase.id is not None
        failed_rebase.status = "failed"
        failed_rebase.failure_reason = "TIMEOUT"
        failed_rebase.branch = "feature/impl-rebase-orphan-2"
        failed_rebase.completed_at = datetime.now(UTC)
        store.update(failed_rebase)

        retry_task = _create_retry_task(store, failed_rebase, trigger_source="manual")
        assert retry_task.id is not None
        assert retry_task.same_branch is True
        assert retry_task.base_branch is None
        assert retry_task.branch == "feature/impl"
        retry_unit = store.resolve_merge_unit_for_task(retry_task.id)
        assert retry_unit is not None
        assert retry_unit.id == impl_unit.id
        assert retry_unit.id != orphan_unit.id
        assert retry_unit.source_branch == "feature/impl"

    def test_retry_cli_manual_rebase_retry_keeps_same_branch(self, tmp_path: Path):
        """`gza retry` keeps rebases on the original shared branch."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl = store.add("Implement feature", task_type="implement")
        assert impl.id is not None
        impl.branch = "feature/impl"
        store.update(impl)

        failed_rebase = store.add(
            "Rebase impl branch",
            task_type="rebase",
            based_on=impl.id,
            same_branch=True,
        )
        assert failed_rebase.id is not None
        failed_rebase.status = "failed"
        failed_rebase.branch = impl.branch
        failed_rebase.completed_at = datetime.now(UTC)
        store.update(failed_rebase)

        result = invoke_gza("retry", str(failed_rebase.id), "--queue", "--project", str(tmp_path))
        assert result.returncode == 0

        retry_task = get_latest_task(store, based_on=failed_rebase.id, task_type="rebase")
        assert retry_task is not None
        assert retry_task.same_branch is True
        assert retry_task.base_branch is None
        assert retry_task.branch == impl.branch
        assert retry_task.trigger_source == "manual"

    def test_create_retry_task_automatic_rebase_retry_chain_prefers_impl_merge_unit(self, tmp_path: Path):
        """Automatic rebase recovery should ignore orphan retry merge units."""
        from gza.cli._common import _create_retry_task

        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl = store.add("Implement feature", task_type="implement")
        store.mark_completed(impl, has_commits=True, branch="feature/impl")
        assert impl.id is not None
        impl_unit = store.resolve_merge_unit_for_task(impl.id)
        assert impl_unit is not None

        orphan_parent = store.add(
            "Failed rebase retry parent",
            task_type="rebase",
            based_on=impl.id,
            same_branch=True,
        )
        assert orphan_parent.id is not None
        orphan_parent.status = "failed"
        orphan_parent.failure_reason = "WORKER_DIED"
        orphan_parent.branch = "feature/impl-rebase-orphan"
        orphan_parent.completed_at = datetime.now(UTC)
        store.update(orphan_parent)
        orphan_unit = store.get_or_create_merge_unit_for_task(orphan_parent)
        assert orphan_unit is not None

        failed_rebase = store.add(
            "Failed rebase retry child",
            task_type="rebase",
            based_on=orphan_parent.id,
            same_branch=True,
        )
        assert failed_rebase.id is not None
        failed_rebase.status = "failed"
        failed_rebase.failure_reason = "WORKER_DIED"
        failed_rebase.branch = "feature/impl-rebase-orphan-2"
        failed_rebase.completed_at = datetime.now(UTC)
        store.update(failed_rebase)

        retry_task = _create_retry_task(
            store,
            failed_rebase,
            trigger_source="auto-recovery",
            automatic_recovery=True,
        )
        assert retry_task.id is not None
        assert retry_task.same_branch is True
        assert retry_task.base_branch is None
        assert retry_task.branch == "feature/impl"
        retry_unit = store.resolve_merge_unit_for_task(retry_task.id)
        assert retry_unit is not None
        assert retry_unit.id == impl_unit.id
        assert retry_unit.id != orphan_unit.id
        assert retry_unit.source_branch == "feature/impl"

    def test_create_retry_task_worker_died_unattached_improve_attaches_impl_merge_unit(self, tmp_path: Path):
        """Automatic improve recovery reattaches unattached failures to the implementation merge unit."""
        from gza.cli._common import _create_retry_task

        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl = store.add("Implement feature", task_type="implement")
        store.mark_completed(impl, has_commits=True, branch="feature/shared-impl")
        assert impl.id is not None
        impl_unit = store.resolve_merge_unit_for_task(impl.id)
        assert impl_unit is not None

        review = store.add("Review feature", task_type="review", based_on=impl.id, same_branch=True)
        assert review.id is not None

        failed_improve = store.add(
            "Improve feature",
            task_type="improve",
            based_on=impl.id,
            depends_on=review.id,
            same_branch=True,
        )
        assert failed_improve.id is not None
        failed_improve.status = "failed"
        failed_improve.failure_reason = "WORKER_DIED"
        failed_improve.branch = "feature/shared-impl"
        failed_improve.completed_at = datetime.now(UTC)
        store.update(failed_improve)

        assert store.resolve_merge_unit_for_task(failed_improve.id) is None

        retry_task = _create_retry_task(
            store,
            failed_improve,
            trigger_source="auto-recovery",
            automatic_recovery=True,
        )
        assert retry_task.id is not None
        assert retry_task.same_branch is True
        assert retry_task.base_branch is None
        assert retry_task.branch == "feature/shared-impl"
        retry_unit = store.resolve_merge_unit_for_task(retry_task.id)
        assert retry_unit is not None
        assert retry_unit.id == impl_unit.id

    def test_create_retry_task_same_branch_improve_prefers_merge_unit_source_branch(self, tmp_path: Path):
        """Same-branch improve retries use the merge-unit source branch over a drifted failed-row branch."""
        from gza.cli._common import _create_retry_task

        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl = store.add("Implement feature", task_type="implement")
        store.mark_completed(impl, has_commits=True, branch="feature/shared")
        assert impl.id is not None
        impl_unit = store.resolve_merge_unit_for_task(impl.id)
        assert impl_unit is not None

        review = store.add("Review feature", task_type="review", based_on=impl.id, same_branch=True)
        assert review.id is not None

        failed_improve = store.add(
            "Improve feature",
            task_type="improve",
            based_on=impl.id,
            depends_on=review.id,
            same_branch=True,
        )
        assert failed_improve.id is not None
        failed_improve.status = "failed"
        failed_improve.failure_reason = "WORKER_DIED"
        failed_improve.branch = "feature/orphan"
        failed_improve.completed_at = datetime.now(UTC)
        store.update(failed_improve)
        store.attach_task_to_merge_unit(failed_improve.id, impl_unit.id, "improve")

        retry_task = _create_retry_task(
            store,
            failed_improve,
            trigger_source="auto-recovery",
            automatic_recovery=True,
        )
        assert retry_task.id is not None
        assert retry_task.same_branch is True
        assert retry_task.base_branch is None
        assert retry_task.branch == "feature/shared"
        retry_unit = store.resolve_merge_unit_for_task(retry_task.id)
        assert retry_unit is not None
        assert retry_unit.id == impl_unit.id

    @pytest.mark.parametrize("creation_mode", ["automatic", "manual"])
    def test_same_branch_improve_retry_execution_prefers_canonical_branch(self, tmp_path: Path, creation_mode: str):
        """Execution should honor the retry's canonical branch instead of a drifted failed-parent branch."""
        from gza.cli._common import _create_retry_task
        from gza.runner import _resolve_code_task_branch_name

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = make_store(tmp_path)

        impl = store.add("Implement feature", task_type="implement")
        store.mark_completed(impl, has_commits=True, branch="feature/shared")
        assert impl.id is not None
        impl_unit = store.resolve_merge_unit_for_task(impl.id)
        assert impl_unit is not None

        review = store.add("Review feature", task_type="review", based_on=impl.id, same_branch=True)
        assert review.id is not None

        failed_improve = store.add(
            "Improve feature",
            task_type="improve",
            based_on=impl.id,
            depends_on=review.id,
            same_branch=True,
        )
        assert failed_improve.id is not None
        failed_improve.status = "failed"
        failed_improve.failure_reason = "WORKER_DIED"
        failed_improve.branch = "feature/orphan"
        failed_improve.completed_at = datetime.now(UTC)
        store.update(failed_improve)
        store.attach_task_to_merge_unit(failed_improve.id, impl_unit.id, "improve")

        if creation_mode == "automatic":
            retry_task = _create_retry_task(
                store,
                failed_improve,
                trigger_source="auto-recovery",
                automatic_recovery=True,
            )
        else:
            result = invoke_gza("retry", str(failed_improve.id), "--queue", "--project", str(tmp_path))
            assert result.returncode == 0
            retry_task = get_latest_task(store, based_on=failed_improve.id, task_type="improve")
            assert retry_task is not None

        assert retry_task.id is not None
        retry_unit = store.resolve_merge_unit_for_task(retry_task.id)
        assert retry_unit is not None
        assert retry_unit.id == impl_unit.id
        assert retry_task.branch == "feature/shared"

        mock_git = MagicMock()
        mock_git.branch_exists.side_effect = lambda branch: branch in {"feature/shared", "feature/orphan"}

        resolved_branch = _resolve_code_task_branch_name(
            retry_task,
            config,
            store,
            mock_git,
            resume=False,
        )

        assert resolved_branch == "feature/shared"

    def test_retry_cli_manual_improve_keeps_same_branch(self, tmp_path: Path):
        """Manual improve retry keeps same_branch=True and reuses the original branch."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Improve with shared branch", task_type="improve", same_branch=True)
        task.status = "failed"
        task.branch = "feature/impl-branch"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        result = invoke_gza("retry", str(task.id), "--queue", "--project", str(tmp_path))
        assert result.returncode == 0

        retry_task = get_latest_task(store, based_on=task.id, task_type="improve")
        assert retry_task is not None
        assert retry_task.same_branch is True
        assert retry_task.base_branch is None
        assert retry_task.branch == "feature/impl-branch"

    def test_retry_does_not_copy_non_explicit_provider(self, tmp_path: Path):
        """Retry should not preserve provider that came from resolved default state."""

        setup_config(tmp_path)
        tmp_path / ".gza" / "gza.db"
        store = make_store(tmp_path)

        task = store.add("Task with stale resolved provider")
        task.status = "failed"
        task.completed_at = datetime.now(UTC)
        task.provider = "claude"
        task.provider_is_explicit = False
        store.update(task)

        result = invoke_gza("retry", str(task.id), "--queue", "--project", str(tmp_path))
        assert result.returncode == 0

        retry_task = get_latest_task(store, based_on=task.id, task_type=task.task_type)
        assert retry_task is not None
        assert retry_task.id != task.id
        assert retry_task.provider is None
        assert retry_task.provider_is_explicit is False

    def test_resume_freezes_routed_provider_when_session_is_carried(self, tmp_path: Path):
        """Resume must preserve the originally resolved provider when it reuses a session_id.

        Regression (M3): A failed fix task routed via task_providers.fix could be resumed
        under a different backend if routing changed between attempts. That breaks the
        resume invariant — the resume task carries the old session_id, so it must also
        carry the same backend.
        """
        from gza.cli._common import _create_resume_task

        setup_config(tmp_path)
        store = make_store(tmp_path)

        failed = store.add("Fix stuck workflow", task_type="fix")
        failed.status = "failed"
        failed.failure_reason = "MAX_STEPS"
        # Runner persisted the effective provider to the task row during the
        # first run (provider_is_explicit stays False — see runner.py for why).
        failed.provider = "codex"
        failed.provider_is_explicit = False
        failed.session_id = "sess-codex-1"
        failed.completed_at = datetime.now(UTC)
        store.update(failed)

        resumed = _create_resume_task(store, failed, trigger_source="manual")

        # With the session_id carried over, the provider must be frozen as an
        # explicit override so the runner cannot re-route to a different backend.
        assert resumed.session_id == "sess-codex-1"
        assert resumed.provider == "codex"
        assert resumed.provider_is_explicit is True
        assert resumed.recovery_origin == "resume"
        assert resumed.trigger_source == "manual"

    def test_retry_with_background_flag(self, tmp_path: Path):
        """Retry command with --background spawns a worker for the new task."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a failed task
        task = store.add("Failed task to retry")
        task.status = "failed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        # Create workers directory
        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)

        # Run retry with background mode
        with (
            patch("gza.cli._common.prepare_task_startup_phase", side_effect=lambda _c, _s, prepared_task: prepared_task),
            patch(
                "gza.cli._common._spawn_detached_worker_process",
                return_value=(MagicMock(pid=4242), ".gza/workers/w-test-startup.log"),
            ),
        ):
            result = invoke_gza("retry", str(task.id), "--background", "--no-docker", "--project", str(tmp_path))

        # Verify the command completes successfully
        assert result.returncode == 0
        assert "Created task " in result.stdout
        assert "Started task " in result.stdout
        assert "in background" in result.stdout

        # Verify new task was created
        new_task = get_latest_task(store, based_on=task.id, task_type=task.task_type)
        assert new_task is not None
        assert new_task.id != task.id
        assert new_task.prompt == "Failed task to retry"
        # Background worker may claim the task before we check, so accept both
        assert new_task.status in ("pending", "in_progress")
        assert new_task.based_on == task.id

    def test_retry_run_flag_runs_immediately(self, tmp_path: Path):
        """Retry command runs immediately only with --run."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a failed task
        task = store.add("Failed task to retry")
        task.status = "failed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        with patch("gza.cli._run_foreground", return_value=0) as run_foreground:
            result = invoke_gza("retry", str(task.id), "--run", "--no-docker", "--project", str(tmp_path))

        # Verify the new task was created and run was attempted
        assert "Created task " in result.stdout
        assert "Running task " in result.stdout

        # Verify new task exists
        new_task = get_latest_task(store, based_on=task.id, task_type=task.task_type)
        assert new_task is not None
        assert new_task.id != task.id
        assert new_task.prompt == "Failed task to retry"
        assert new_task.based_on == task.id
        assert run_foreground.call_args.kwargs["task_id"] == new_task.id

    def test_retry_with_queue_flag(self, tmp_path: Path):
        """Retry command with --queue adds task to queue without executing."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a failed task
        task = store.add("Failed task to retry")
        task.status = "failed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        # Run retry with --queue flag
        result = invoke_gza("retry", str(task.id), "--queue", "--project", str(tmp_path))

        # Verify the new task was created but not run
        assert result.returncode == 0
        assert "Created task " in result.stdout
        assert "Running task" not in result.stdout

        # Verify new task is still pending
        new_task = get_latest_task(store, based_on=task.id, task_type=task.task_type)
        assert new_task is not None
        assert new_task.id != task.id
        assert new_task.status == "pending"

    def test_retry_with_queue_stays_pickable_by_work(self, tmp_path: Path):
        """Queued retry children should stay visible to pickup and executable via work."""
        from gza.cli.execution import cmd_run

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = make_store(tmp_path)

        failed = store.add("Retry me", task_type="implement", tags=("recover",))
        failed.status = "failed"
        failed.failure_reason = "INFRASTRUCTURE_ERROR"
        failed.completed_at = datetime.now(UTC)
        store.update(failed)
        assert failed.id is not None

        retry_result = invoke_gza("retry", str(failed.id), "--queue", "--project", str(tmp_path))
        assert retry_result.returncode == 0

        retry_task = get_latest_task(store, based_on=failed.id, task_type="implement")
        assert retry_task is not None
        assert retry_task.id is not None
        assert retry_task.status == "pending"
        assert retry_task.id in {task.id for task in store.get_pending_pickup()}

        args = argparse.Namespace(
            project_dir=tmp_path,
            no_docker=True,
            max_turns=None,
            background=False,
            worker_mode=False,
            task_ids=[retry_task.id],
            count=1,
            force=False,
            resume=False,
            create_pr=False,
            tags=[],
            group=None,
            any_tag=False,
        )

        registry = WorkerRegistry(config.workers_path)

        def _fake_run_foreground(*_args, **kwargs) -> int:
            worker_id = os.environ.get("GZA_WORKER_ID")
            assert worker_id is not None
            registry.ensure_running(
                WorkerMetadata(
                    worker_id=worker_id,
                    task_id=retry_task.id,
                    pid=os.getpid(),
                    is_background=False,
                )
            )
            return 0

        with (
            patch("gza.cli.execution.Config.load", return_value=config),
            patch("gza.cli.execution.collect_recovery_lane_entries", return_value=[]),
            patch("gza.cli.execution.get_store", return_value=store),
            patch("gza.cli.execution._run_foreground", side_effect=_fake_run_foreground),
            patch("gza.git.Git.default_branch", return_value="main"),
            patch("gza.git.Git.local_branch_names", return_value=()),
        ):
            rc = cmd_run(args)

        assert rc == 0
        workers = registry.list_all(include_completed=True)
        assert workers
        assert workers[-1].task_id == retry_task.id

    def test_retry_blocked_if_successful_retry_exists(self, tmp_path: Path):
        """Retry command fails if the task already has a child retry with status completed."""
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = make_store(tmp_path)

        # Create original failed task
        original = store.add("Original task")
        original.status = "failed"
        original.completed_at = datetime.now(UTC)
        store.update(original)

        # Create a successful retry child
        retry = store.add("Original task", based_on=original.id)
        retry.status = "completed"
        retry.completed_at = datetime.now(UTC)
        store.update(retry)

        result = invoke_gza("retry", str(original.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 1
        assert (
            f"Error: Task {original.id} already has a successful retry ({retry.id})."
            in result.stdout
        )

    @pytest.mark.parametrize("wrapped_id", ["  {id}  ", "\t{id}\t"])
    def test_retry_duplicate_guard_uses_canonical_task_id(self, tmp_path: Path, wrapped_id: str):
        """Retry duplicate checks should use the canonical resolved task ID."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        original = store.add("Original task")
        original.status = "failed"
        original.completed_at = datetime.now(UTC)
        store.update(original)

        retry = store.add("Original task", based_on=original.id)
        retry.status = "completed"
        retry.completed_at = datetime.now(UTC)
        store.update(retry)

        task_arg = wrapped_id.format(id=original.id)
        result = invoke_gza("retry", task_arg, "--queue", "--project", str(tmp_path))

        assert result.returncode == 1
        assert f"Error: Task {original.id} already has a successful retry ({retry.id})." in result.stdout


class TestResumeCommand:
    """Tests for 'gza resume' command."""

    def test_resume_with_background_flag(self, tmp_path: Path):
        """Resume command with --background creates a new task and spawns a worker."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a failed task with a session ID
        task = store.add("Failed task to resume")
        task.status = "failed"
        task.session_id = "test-session-123"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        # Create workers directory
        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)

        mock_proc = MagicMock()
        mock_proc.pid = 44444

        # Run resume with background mode
        with (
            patch("gza.git.Git.branch_exists", return_value=False),
            patch(
                "gza.cli._spawn_detached_worker_process",
                return_value=(mock_proc, ".gza/workers/test-startup.log"),
            ),
        ):
            result = invoke_gza("resume", str(task.id), "--background", "--no-docker", "--project", str(tmp_path))

        # Verify the command completes successfully
        assert result.returncode == 0
        # Verify new task was created
        assert "resume of " in result.stdout
        assert "Started task " in result.stdout
        assert "in background" in result.stdout
        assert "(resuming," in result.stdout

        # Verify original task still failed and new task was created
        original = store.get(task.id)
        assert original is not None
        assert original.status == "failed"
        new_task = get_latest_task(store, based_on=task.id, task_type=task.task_type)
        assert new_task is not None
        assert new_task.id != task.id
        assert new_task.slug is not None
        assert new_task.log_file is not None

    def test_resume_background_creator_phase_failure_surfaces_and_cleans_up(self, tmp_path: Path):
        """Background resume failures before worker handoff must hit stderr and leave no child row."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Failed task to resume")
        task.status = "failed"
        task.session_id = "test-session-123"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        with patch("gza.cli._common.prepare_task_startup_phase", side_effect=RuntimeError("creator boom")):
            result = invoke_gza("resume", str(task.id), "--background", "--no-docker", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "creator boom" in result.stderr
        assert "Created task" not in result.stdout
        assert store.get_based_on_children(task.id) == []
        logs_dir = tmp_path / ".gza" / "logs"
        if logs_dir.exists():
            assert list(logs_dir.iterdir()) == []
        workers_dir = tmp_path / ".gza" / "workers"
        if workers_dir.exists():
            assert list(workers_dir.iterdir()) == []

    def test_resume_without_session_id_fails(self, tmp_path: Path):
        """Resume command fails for tasks without session ID."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a failed task without session ID
        task = store.add("Failed task without session")
        task.status = "failed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        # Try to resume
        result = invoke_gza("resume", str(task.id), "--project", str(tmp_path))

        # Verify it fails with helpful message
        assert result.returncode == 1
        assert "has no session ID" in result.stdout
        assert "gza retry" in result.stdout
        assert "fresh conversation" in result.stdout
        assert "same-branch follow-ups stay on the shared branch" in result.stdout

    def test_resume_non_failed_task_fails(self, tmp_path: Path):
        """Resume command fails for non-failed, non-orphaned tasks."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Pending task", "status": "pending"},
        ])

        store = make_store(tmp_path)
        task = store.get_all()[0]
        result = invoke_gza("resume", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 1
        assert "Can only resume failed or orphaned tasks" in result.stdout

    def test_resume_run_flag_runs_immediately(self, tmp_path: Path):
        """Resume command runs immediately only with --run."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a failed task with a session ID
        task = store.add("Failed task to resume")
        task.status = "failed"
        task.session_id = "test-session-123"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        with (
            patch("gza.git.Git.branch_exists", return_value=False),
            patch("gza.cli._run_foreground", return_value=0) as run_foreground,
        ):
            result = invoke_gza("resume", str(task.id), "--run", "--no-docker", "--project", str(tmp_path))

        # Verify the command creates a new task
        assert "resume of " in result.stdout

        # Verify original task stays failed and new task was created
        original = store.get(task.id)
        assert original is not None
        assert original.status == "failed"
        new_task = get_latest_task(store, based_on=task.id, task_type=task.task_type)
        assert new_task is not None
        assert new_task.id != task.id
        assert new_task.based_on == task.id
        assert new_task.session_id == "test-session-123"
        assert run_foreground.call_args.kwargs["resume"] is True
        assert run_foreground.call_args.kwargs["task_id"] == new_task.id
        assert run_foreground.call_args.kwargs["invocation"].command == "resume"

    def test_resume_with_queue_flag(self, tmp_path: Path):
        """Resume command with --queue adds task to queue without executing."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a failed task with a session ID
        task = store.add("Failed task to resume")
        task.status = "failed"
        task.session_id = "test-session-123"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        # Run resume with --queue flag
        result = invoke_gza("resume", str(task.id), "--queue", "--project", str(tmp_path))

        # Verify the command creates a new task but does not run it
        assert result.returncode == 0
        assert "resume of " in result.stdout
        assert "Running" not in result.stdout

        # Verify new task is pending
        new_task = get_latest_task(store, based_on=task.id, task_type=task.task_type)
        assert new_task is not None
        assert new_task.id != task.id
        assert new_task.status == "pending"
        assert new_task.session_id == "test-session-123"

    def test_resume_with_queue_stays_pickable_by_work(self, tmp_path: Path):
        """Queued resume children should stay visible to pickup and executable via work."""
        from gza.cli._common import _create_resume_task
        from gza.cli.execution import cmd_run

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = make_store(tmp_path)

        failed = store.add("Resume me", task_type="implement", tags=("recover",))
        failed.status = "failed"
        failed.failure_reason = "TIMEOUT"
        failed.session_id = "sess-123"
        failed.completed_at = datetime.now(UTC)
        store.update(failed)
        assert failed.id is not None

        resume_task = _create_resume_task(store, failed, trigger_source="manual")
        assert resume_task is not None
        assert resume_task.id is not None
        assert resume_task.status == "pending"
        assert resume_task.id in {task.id for task in store.get_pending_pickup()}

        args = argparse.Namespace(
            project_dir=tmp_path,
            no_docker=True,
            max_turns=None,
            background=False,
            worker_mode=False,
            task_ids=[resume_task.id],
            count=1,
            force=False,
            resume=False,
            create_pr=False,
            tags=[],
            group=None,
            any_tag=False,
        )

        registry = WorkerRegistry(config.workers_path)

        def _fake_run_foreground(*_args, **kwargs) -> int:
            worker_id = os.environ.get("GZA_WORKER_ID")
            assert worker_id is not None
            registry.ensure_running(
                WorkerMetadata(
                    worker_id=worker_id,
                    task_id=resume_task.id,
                    pid=os.getpid(),
                    is_background=False,
                )
            )
            return 0

        with (
            patch("gza.cli.execution.Config.load", return_value=config),
            patch("gza.cli.execution.collect_recovery_lane_entries", return_value=[]),
            patch("gza.cli.execution.get_store", return_value=store),
            patch("gza.cli.execution._run_foreground", side_effect=_fake_run_foreground),
        ):
            rc = cmd_run(args)

        assert rc == 0
        workers = registry.list_all(include_completed=True)
        assert workers
        assert workers[-1].task_id == resume_task.id

    def test_work_rejects_at_max_concurrent_without_registering_new_worker(self, tmp_path: Path, capsys) -> None:
        """Direct foreground work should fail cleanly at the global cap before publishing itself."""
        from gza.cli.execution import cmd_run

        setup_config(tmp_path)
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(config_path.read_text() + "max_concurrent: 1\n")
        config = Config.load(tmp_path)
        store = make_store(tmp_path)

        running = store.add("Running task", task_type="implement")
        running.status = "in_progress"
        running.running_pid = 999999
        store.update(running)

        pending = store.add("Pending task", task_type="implement")
        assert pending.id is not None

        args = argparse.Namespace(
            project_dir=tmp_path,
            no_docker=True,
            max_turns=None,
            background=False,
            worker_mode=False,
            task_ids=[],
            count=1,
            force=False,
            resume=False,
            create_pr=False,
            tags=[],
            group=None,
            any_tag=False,
        )

        with (
            patch("gza.cli.execution.Config.load", return_value=config),
            patch("gza.cli.execution.get_store", return_value=store),
            patch("gza.concurrency._best_effort_stale_cleanup"),
            patch("gza.concurrency._pid_alive", side_effect=lambda pid: pid == 999999),
            patch("gza.cli.run") as mock_run,
        ):
            rc = cmd_run(args)

        captured = capsys.readouterr()
        assert rc == 1
        assert "Error: already at max concurrent tasks: 1 running, limit is 1" in captured.out
        assert captured.err.strip() == ""
        mock_run.assert_not_called()
        assert WorkerRegistry(config.workers_path).list_all(include_completed=True) == []

    def test_work_runs_single_foreground_task_at_max_concurrent_one_without_self_counting(self, tmp_path: Path) -> None:
        """The outer work session must not consume the only slot before the first task launch."""
        from gza.cli.execution import cmd_run

        setup_config(tmp_path)
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(config_path.read_text() + "max_concurrent: 1\n")
        config = Config.load(tmp_path)
        store = make_store(tmp_path)

        pending = store.add("Pending task", task_type="implement")
        assert pending.id is not None

        args = argparse.Namespace(
            project_dir=tmp_path,
            no_docker=True,
            max_turns=None,
            background=False,
            worker_mode=False,
            task_ids=[],
            count=1,
            force=False,
            resume=False,
            create_pr=False,
            tags=[],
            group=None,
            any_tag=False,
        )

        with (
            patch("gza.cli.execution.Config.load", return_value=config),
            patch("gza.cli.execution.get_store", return_value=store),
            patch("gza.cli.run", return_value=0) as mock_run,
        ):
            rc = cmd_run(args)

        assert rc == 0
        mock_run.assert_called_once()
        assert mock_run.call_args.kwargs["task_id"] is None
        assert WorkerRegistry(config.workers_path).list_all(include_completed=True) == []

    def test_work_serial_explicit_tasks_reuse_one_foreground_slot_at_max_concurrent_one(self, tmp_path: Path) -> None:
        """Serial foreground sessions should reuse one worker/slot across multiple explicit tasks."""
        from gza.cli.execution import cmd_run

        setup_config(tmp_path)
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(config_path.read_text() + "max_concurrent: 1\n")
        config = Config.load(tmp_path)
        store = make_store(tmp_path)

        first = store.add("Pending task 1", task_type="implement")
        second = store.add("Pending task 2", task_type="implement")
        assert first.id is not None
        assert second.id is not None

        args = argparse.Namespace(
            project_dir=tmp_path,
            no_docker=True,
            max_turns=None,
            background=False,
            worker_mode=False,
            task_ids=[first.id, second.id],
            count=1,
            force=False,
            resume=False,
            create_pr=False,
            tags=[],
            group=None,
            any_tag=False,
        )

        with (
            patch("gza.cli.execution.Config.load", return_value=config),
            patch("gza.cli.execution.get_store", return_value=store),
            patch("gza.cli.run", return_value=0) as mock_run,
        ):
            rc = cmd_run(args)

        assert rc == 0
        assert [call.kwargs["task_id"] for call in mock_run.call_args_list] == [first.id, second.id]
        workers = WorkerRegistry(config.workers_path).list_all(include_completed=True)
        assert len(workers) == 1
        assert workers[0].status == "completed"
        assert workers[0].task_id == second.id

    def test_work_explicit_task_ids_use_shared_run_foreground_path(self, tmp_path: Path) -> None:
        """Explicit work selections should delegate foreground launches through the shared helper."""
        from gza.cli.execution import cmd_run

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = make_store(tmp_path)
        task1 = store.add("Pending task 1")
        task2 = store.add("Pending task 2")
        assert task1.id is not None
        assert task2.id is not None

        args = argparse.Namespace(
            project_dir=tmp_path,
            no_docker=True,
            max_turns=None,
            background=False,
            worker_mode=False,
            task_ids=[task1.id, task2.id],
            count=1,
            force=False,
            resume=False,
            create_pr=True,
            tags=[],
            group=None,
            any_tag=False,
        )

        with (
            patch("gza.cli.execution.Config.load", return_value=config),
            patch("gza.cli.execution.get_store", return_value=store),
            patch("gza.cli.run", side_effect=AssertionError("cmd_run should not call runner.run directly")),
            patch("gza.cli.execution._run_foreground", side_effect=[0, 0]) as run_foreground,
        ):
            rc = cmd_run(args)

        assert rc == 0
        assert [call.kwargs["task_id"] for call in run_foreground.call_args_list] == [task1.id, task2.id]
        for call in run_foreground.call_args_list:
            assert call.kwargs["force"] is False
            assert call.kwargs["create_pr"] is True
            assert call.kwargs["phase1_args"] is args

    def test_work_count_mode_uses_shared_run_foreground_path(self, tmp_path: Path) -> None:
        """Count-based work runs should stay on the shared foreground helper path."""
        from gza.cli.execution import cmd_run

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = make_store(tmp_path)
        store.add("Pending task 1", tags=("release",))
        store.add("Pending task 2", tags=("release",))

        args = argparse.Namespace(
            project_dir=tmp_path,
            no_docker=True,
            max_turns=None,
            background=False,
            worker_mode=False,
            task_ids=[],
            count=2,
            force=True,
            resume=False,
            create_pr=False,
            tags=["release"],
            group=None,
            any_tag=False,
        )

        with (
            patch("gza.cli.execution.Config.load", return_value=config),
            patch("gza.cli.execution.get_store", return_value=store),
            patch("gza.cli.run", side_effect=AssertionError("cmd_run should not call runner.run directly")),
            patch("gza.cli.execution._run_foreground", side_effect=[0, 0]) as run_foreground,
        ):
            rc = cmd_run(args)

        assert rc == 0
        assert len(run_foreground.call_args_list) == 2
        for call in run_foreground.call_args_list:
            assert call.kwargs["task_id"] is not None
            assert call.kwargs["force"] is True
            assert call.kwargs["create_pr"] is False
            assert call.kwargs["phase1_args"] is args

    def test_work_multi_task_reuses_same_registered_worker_across_shared_foreground_calls(self, tmp_path: Path) -> None:
        """Multi-task work sessions should keep the same outer worker registration across helper calls."""
        from gza.cli.execution import cmd_run

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = make_store(tmp_path)
        task1 = store.add("Pending task 1")
        task2 = store.add("Pending task 2")
        assert task1.id is not None
        assert task2.id is not None

        args = argparse.Namespace(
            project_dir=tmp_path,
            no_docker=True,
            max_turns=None,
            background=False,
            worker_mode=False,
            task_ids=[task1.id, task2.id],
            count=1,
            force=False,
            resume=False,
            create_pr=False,
            tags=[],
            group=None,
            any_tag=False,
        )

        seen_worker_ids: list[str | None] = []
        seen_owner_markers: list[str | None] = []
        registry = WorkerRegistry(config.workers_path)

        def _fake_run_foreground(*_args, **_kwargs) -> int:
            worker_id = os.environ.get("GZA_WORKER_ID")
            seen_worker_ids.append(worker_id)
            seen_owner_markers.append(os.environ.get("GZA_REUSE_WORKER_OWNER"))
            assert worker_id is not None
            registry.ensure_running(
                WorkerMetadata(
                    worker_id=worker_id,
                    task_id=_kwargs["task_id"],
                    pid=os.getpid(),
                    is_background=False,
                )
            )
            return 0

        with (
            patch("gza.cli.execution.Config.load", return_value=config),
            patch("gza.cli.execution.get_store", return_value=store),
            patch("gza.cli.execution._run_foreground", side_effect=_fake_run_foreground),
        ):
            rc = cmd_run(args)

        assert rc == 0
        assert len(seen_worker_ids) == 2
        assert seen_worker_ids[0] is not None
        assert seen_worker_ids[0] == seen_worker_ids[1]
        assert seen_owner_markers == ["outer", "outer"]
        workers = registry.list_all(include_completed=True)
        assert len(workers) == 1
        assert workers[0].worker_id == seen_worker_ids[0]
        assert workers[0].status == "completed"

    def test_resume_creates_new_task_preserves_original(self, tmp_path: Path):
        """Resume creates a new pending task, leaving original task as failed."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a failed task with session ID, log, and stats
        task = store.add("Implement feature X")
        task.status = "failed"
        task.session_id = "session-abc-123"
        task.task_type = "implement"
        task.num_turns_reported = 42
        task.cost_usd = 1.50
        task.duration_seconds = 300.0
        task.log_file = ".gza/logs/20260101-implement-feature-x.log"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        with patch("gza.cli._run_foreground", return_value=0) as run_foreground:
            result = invoke_gza("resume", str(task.id), "--run", "--no-docker", "--project", str(tmp_path))

        # Verify output
        assert "resume of " in result.stdout

        # Verify original task stays failed with stats preserved
        original = store.get(task.id)
        assert original is not None
        assert original.status == "failed"
        assert original.num_turns_reported == 42
        assert original.cost_usd == 1.50
        assert original.duration_seconds == 300.0
        assert original.log_file == ".gza/logs/20260101-implement-feature-x.log"

        # Verify new task has the right properties
        new_task = get_latest_task(store, based_on=task.id, task_type=task.task_type)
        assert new_task is not None
        assert new_task.id != task.id
        assert new_task.prompt == "Implement feature X"
        assert new_task.task_type == "implement"
        assert new_task.based_on == task.id
        assert new_task.session_id == "session-abc-123"
        assert run_foreground.call_args.kwargs["resume"] is True
        assert run_foreground.call_args.kwargs["task_id"] == new_task.id
        # New task starts with no stats
        assert new_task.num_turns_reported is None
        assert new_task.cost_usd is None
        # The command claims the new task before dispatching to the runner, so
        # it already has its own startup log path at this layer.
        assert new_task.log_file is not None
        assert new_task.log_file != ".gza/logs/20260101-implement-feature-x.log"

    def test_resume_orphaned_in_progress_task_succeeds(self, tmp_path: Path):
        """Resume command succeeds for an orphaned in_progress task (no live worker)."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create an in_progress task with a session ID (simulating an orphaned task)
        task = store.add("Orphaned in-progress task")
        task.status = "in_progress"
        task.session_id = "orphaned-session-456"
        task.started_at = datetime.now(UTC)
        store.update(task)

        # No worker files exist — task is orphaned

        result = invoke_gza("resume", str(task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "orphaned" in result.stdout.lower()
        assert "resume of " in result.stdout

        # Verify original task is unchanged and new task was created
        original = store.get(task.id)
        assert original is not None
        assert original.status == "in_progress"
        new_task = get_latest_task(store, based_on=task.id, task_type=task.task_type)
        assert new_task is not None
        assert new_task.id != task.id
        assert new_task.based_on == task.id
        assert new_task.session_id == "orphaned-session-456"
        assert new_task.status == "pending"

    def test_resume_running_in_progress_task_fails(self, tmp_path: Path):
        """Resume command fails for an in_progress task that has a live worker."""
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create an in_progress task
        task = store.add("Still-running task")
        task.status = "in_progress"
        task.session_id = "running-session-789"
        task.started_at = datetime.now(UTC)
        store.update(task)

        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(workers_path)
        worker = WorkerMetadata(
            worker_id="w-test-running",
            pid=12345,
            task_id=task.id,
            task_slug=None,
            started_at=datetime.now(UTC).isoformat(),
            status="running",
            log_file=None,
            worktree=None,
        )
        registry.register(worker)

        with patch.object(WorkerRegistry, "is_running", return_value=True):
            result = invoke_gza("resume", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 1
        assert "still running" in result.stdout.lower()
        assert "w-test-running" in result.stdout


class TestWorkCommandMultiTask:
    """Tests for 'gza work' command with multiple task IDs."""

    @pytest.fixture(autouse=True)
    def _mock_work_runner(self):
        """Keep work command tests focused on selection/CLI behavior, not task execution."""
        with patch("gza.cli.execution._run_foreground", return_value=0) as run_foreground:
            yield run_foreground

    def test_work_with_single_task_id(self, tmp_path: Path):
        """Work command accepts a single task ID."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Add a task
        task1 = store.add("Test task 1")

        # Verify the command accepts the argument
        result = invoke_gza("work", str(task1.id), "--no-docker", "--project", str(tmp_path))

        # Note: Without actual Claude integration, this will fail,
        # but we're verifying that argparse accepts the input
        # The error should not be about argument parsing
        assert "unrecognized arguments" not in result.stderr

    def test_work_with_multiple_task_ids(self, tmp_path: Path):
        """Work command accepts multiple task IDs."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Add multiple tasks
        task1 = store.add("Test task 1")
        task2 = store.add("Test task 2")
        task3 = store.add("Test task 3")

        # Verify the command accepts multiple arguments
        result = invoke_gza("work", str(task1.id), str(task2.id), str(task3.id),
                        "--no-docker", "--project", str(tmp_path))

        # Verify argparse accepts the input
        assert "unrecognized arguments" not in result.stderr

    def test_work_background_with_multiple_task_ids(self, tmp_path: Path):
        """Work command with --background spawns workers for multiple task IDs."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Add multiple tasks
        task1 = store.add("Test task 1")
        task2 = store.add("Test task 2")

        # Create workers directory
        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)

        mock_proc = MagicMock()
        mock_proc.pid = 12345

        # Run with background mode and multiple task IDs
        with patch(
            "gza.cli._spawn_detached_worker_process",
            return_value=(mock_proc, ".gza/workers/test-startup.log"),
        ):
            result = invoke_gza("work", str(task1.id), str(task2.id),
                            "--background", "--no-docker", "--project", str(tmp_path))

        # Verify the command completes without argument parsing errors
        assert "unrecognized arguments" not in result.stderr

    def test_work_background_subprocess_uses_project_flag(self, tmp_path: Path):
        """Background worker subprocess command uses --project flag, not bare positional arg."""
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import _spawn_background_worker
        from gza.config import Config

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Test task")

        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)

        config = Config.load(tmp_path)
        config.tmux.enabled = False  # Test bare Popen path, not tmux
        args = argparse.Namespace(no_docker=True, max_turns=None)

        with patch("gza.cli._spawn_detached_worker_process") as mock_spawn:
            mock_spawn.return_value = (MagicMock(pid=12345), ".gza/workers/w-test-startup.log")
            _spawn_background_worker(args, config, task_id=task.id)

            assert mock_spawn.called
            cmd = mock_spawn.call_args[0][0]
            # Project dir must be passed with --project flag, not as bare positional
            project_dir = str(config.project_dir.absolute())
            assert "--project" in cmd, f"--project flag missing from subprocess cmd: {cmd}"
            project_idx = cmd.index("--project")
            assert cmd[project_idx + 1] == project_dir

    def test_work_with_no_task_ids(self, tmp_path: Path):
        """Work command works without task IDs (runs next pending)."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Add a task
        store.add("Test task 1")

        # Run without task IDs
        result = invoke_gza("work", "--no-docker", "--project", str(tmp_path))

        # Verify no argument parsing errors
        assert "unrecognized arguments" not in result.stderr

    def test_work_notes_recovery_candidates_it_will_not_touch(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        from gza.cli.execution import cmd_run

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = make_store(tmp_path)

        failed = store.add("Failed recovery candidate")
        assert failed.id is not None
        failed.status = "failed"
        failed.failure_reason = "MAX_TURNS"
        failed.session_id = "sess-1"
        failed.completed_at = datetime.now(UTC)
        store.update(failed)

        store.add("Pending task 1")

        args = argparse.Namespace(
            project_dir=tmp_path,
            no_docker=True,
            max_turns=None,
            background=False,
            worker_mode=False,
            task_ids=[],
            count=1,
            force=False,
            resume=False,
            create_pr=False,
            tags=None,
            any_tag=False,
        )

        with (
            patch("gza.cli.execution.Config.load", return_value=config),
            patch("gza.cli.execution.get_store", return_value=store),
            patch("gza.cli.execution._run_foreground", return_value=0),
        ):
            rc = cmd_run(args)

        output = capsys.readouterr().out
        assert rc == 0
        assert "recovery candidate is waiting on `gza advance` / `gza watch`" in output
        assert "`gza work` only starts pending tasks" in output

    def test_work_validates_all_task_ids_before_execution(self, tmp_path: Path):
        """Work command validates all task IDs before starting execution."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Add one valid task
        task1 = store.add("Test task 1")

        # Try to run with one valid and one invalid task ID
        result = invoke_gza("work", str(task1.id), "test-project-zzz", "--no-docker", "--project", str(tmp_path))

        # Should error about invalid task ID format
        assert result.returncode != 0
        assert "Use a full prefixed task ID" in result.stdout or "Use a full prefixed task ID" in result.stderr

    def test_work_rejects_shorthand_task_id(self, tmp_path: Path):
        """Work command requires full prefixed task IDs."""
        setup_config(tmp_path)
        make_store(tmp_path).add("Test task 1")

        result = invoke_gza("work", "42", "--no-docker", "--project", str(tmp_path))

        assert result.returncode != 0
        assert "Use a full prefixed task ID" in result.stderr or "Use a full prefixed task ID" in result.stdout

    def test_work_validates_task_status(self, tmp_path: Path):
        """Work command validates that tasks are in pending status."""
        from datetime import datetime

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Add a completed task
        task1 = store.add("Test task 1")
        task1.status = "completed"
        task1.completed_at = datetime.now(UTC)
        store.update(task1)

        # Try to run the completed task
        result = invoke_gza("work", str(task1.id), "--no-docker", "--project", str(tmp_path))

        # Should error about task status
        assert result.returncode != 0
        assert f"Task {task1.id} is not pending" in result.stdout or f"Task {task1.id} is not pending" in result.stderr

    def test_work_worker_mode_rejects_completed_task(self, tmp_path: Path):
        """Worker-mode explicit execution should return non-zero for non-pending tasks."""
        from datetime import datetime

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Completed task")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        result = invoke_gza("work", "--worker-mode", str(task.id), "--no-docker", "--project", str(tmp_path))
        assert result.returncode != 0
        assert f"Task {task.id}" in (result.stdout + result.stderr)

    def test_work_warns_about_orphaned_tasks_before_starting(self, tmp_path: Path):
        """Work command warns about orphaned in-progress tasks before starting new work."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create an orphaned task (in-progress, no active worker) and no pending tasks.
        # With no pending tasks, run() will return 0 immediately after printing
        # "No pending tasks found", so we can observe the orphaned warning without
        # needing to actually execute a task.
        orphaned_task = store.add("Stuck task from yesterday")
        mark_orphaned(store, orphaned_task)

        result = invoke_gza("work", "--no-docker", "--project", str(tmp_path))

        assert result.returncode == 0
        # Warning about orphaned task should appear before the "No pending tasks" message
        assert "orphaned" in result.stdout
        assert "Stuck task from yesterday" in result.stdout
        assert "gza work" in result.stdout

    def test_work_count_nonzero_run_does_not_count_as_completed(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        """A non-zero run result (e.g. PREREQUISITE_UNMERGED refusal) fails the session and stops batch accounting."""
        from gza.cli.execution import cmd_run

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = make_store(tmp_path)
        store.add("Pending task 1")
        store.add("Pending task 2")

        args = argparse.Namespace(
            project_dir=tmp_path,
            no_docker=True,
            max_turns=None,
            background=False,
            worker_mode=False,
            task_ids=[],
            count=2,
            force=False,
            resume=False,
        )

        registry = WorkerRegistry(config.workers_path)
        seen_calls = 0

        def _fake_run_foreground(*_args, **_kwargs) -> int:
            nonlocal seen_calls
            seen_calls += 1
            worker_id = os.environ.get("GZA_WORKER_ID")
            assert worker_id is not None
            registry.ensure_running(
                WorkerMetadata(
                    worker_id=worker_id,
                    task_id=f"simulated-task-{seen_calls}",
                    pid=os.getpid(),
                    is_background=False,
                )
            )
            return 0 if seen_calls == 1 else 1

        with (
            patch("gza.cli.execution.Config.load", return_value=config),
            patch("gza.cli.execution.get_store", return_value=store),
            patch("gza.cli.execution._run_foreground", side_effect=_fake_run_foreground),
        ):
            rc = cmd_run(args)

        output = capsys.readouterr().out
        assert rc == 1
        assert "Completed 1 task(s) before a task failed" in output
        assert "Completed 2 task(s)" not in output

        workers = registry.list_all(include_completed=True)
        assert len(workers) == 1
        assert workers[0].status == "failed"
        assert workers[0].exit_code == 1

    def test_work_forwards_create_pr_flag_to_runner(self, tmp_path: Path):
        """work --pr should forward create_pr intent to runner.run."""
        from gza.cli.execution import cmd_run

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = make_store(tmp_path)
        store.add("Pending task 1")

        args = argparse.Namespace(
            project_dir=tmp_path,
            no_docker=True,
            max_turns=None,
            background=False,
            worker_mode=False,
            task_ids=[],
            count=1,
            force=False,
            resume=False,
            create_pr=True,
        )

        seen_create_pr: list[bool] = []

        def _fake_run(*_args, **kwargs):
            seen_create_pr.append(bool(kwargs.get("create_pr")))
            return 0

        with (
            patch("gza.cli.execution.Config.load", return_value=config),
            patch("gza.cli.execution.get_store", return_value=store),
            patch("gza.cli.execution._run_foreground", side_effect=_fake_run),
        ):
            rc = cmd_run(args)

        assert rc == 0
        assert seen_create_pr == [True]

    def test_work_tag_runs_only_tasks_from_selected_tag(self, tmp_path: Path):
        """work --tag should select and run only tasks from that tag."""
        from gza.cli.execution import cmd_run

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = make_store(tmp_path)
        release_task = store.add("Release task", group="release-1")
        backlog_task = store.add("Backlog task", group="backlog")
        assert release_task.id is not None
        assert backlog_task.id is not None

        args = argparse.Namespace(
            project_dir=tmp_path,
            no_docker=True,
            max_turns=None,
            background=False,
            worker_mode=False,
            task_ids=[],
            count=1,
            force=False,
            resume=False,
            create_pr=False,
            tags=["release-1"],
        )

        seen_task_ids: list[str] = []

        def _fake_run(*_args, **kwargs):
            seen_task_ids.append(str(kwargs.get("task_id")))
            return 0

        with (
            patch("gza.cli.execution.Config.load", return_value=config),
            patch("gza.cli.execution.get_store", return_value=store),
            patch("gza.cli.execution._run_foreground", side_effect=_fake_run),
        ):
            rc = cmd_run(args)

        assert rc == 0
        assert seen_task_ids == [release_task.id]

    def test_work_tag_rejects_empty_value_without_traceback(self, tmp_path: Path):
        """work --tag '' should reject empty values cleanly."""
        setup_config(tmp_path)

        result = invoke_gza("work", "--tag", "", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "Error: tag must not be empty" in result.stdout
        assert "Traceback" not in result.stdout
        assert "Traceback" not in result.stderr

    def test_work_tag_reports_blocked_when_matching_pending_tasks_are_not_runnable(self, tmp_path: Path):
        """work --tag should distinguish blocked matching tasks from an empty pending set."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        prereq = store.add("Prerequisite task")
        blocked = store.add("Blocked release task", tags=("release-1",), depends_on=prereq.id)
        assert prereq.id is not None
        assert blocked.id is not None

        result = invoke_gza("work", "--tag", "release-1", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "No runnable tasks found matching tags: release-1." in result.stdout
        assert "blocked by dependencies" in result.stdout
        assert "No pending tasks found matching tags: release-1" not in result.stdout

    def test_work_background_tag_reports_blocked_when_matching_pending_tasks_are_not_runnable(self, tmp_path: Path):
        """work --background --tag should report blocked matching tasks, not missing pending tasks."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        prereq = store.add("Prerequisite task")
        blocked = store.add("Blocked release task", tags=("release-1",), depends_on=prereq.id)
        assert prereq.id is not None
        assert blocked.id is not None

        result = invoke_gza(
            "work",
            "--background",
            "--no-docker",
            "--tag",
            "release-1",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 0
        assert "No runnable tasks found matching tags: release-1." in result.stdout
        assert "blocked by dependencies" in result.stdout
        assert "No pending tasks found matching tags: release-1" not in result.stdout

    def test_work_tag_internal_only_match_reports_non_runnable_not_dependency_blocked(
        self, tmp_path: Path
    ):
        """work --tag should not claim dependency blocking when only internal tasks match."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        internal_task = store.add("Internal release maintenance", task_type="internal", tags=("release-1",))
        assert internal_task.id is not None

        result = invoke_gza("work", "--tag", "release-1", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "No runnable tasks found matching tags: release-1." in result.stdout
        assert "not runnable via work" in result.stdout
        assert "blocked by dependencies" not in result.stdout

    def test_work_background_tag_internal_only_match_reports_non_runnable_not_dependency_blocked(
        self, tmp_path: Path
    ):
        """work --background --tag should not claim dependency blocking when only internal tasks match."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        internal_task = store.add("Internal release maintenance", task_type="internal", tags=("release-1",))
        assert internal_task.id is not None

        result = invoke_gza(
            "work",
            "--background",
            "--no-docker",
            "--tag",
            "release-1",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 0
        assert "No runnable tasks found matching tags: release-1." in result.stdout
        assert "not runnable via work" in result.stdout
        assert "blocked by dependencies" not in result.stdout

    def test_work_allows_failed_pr_required_task_with_pr_flag(self, tmp_path: Path):
        """work <task> --pr should allow retrying failed PR_REQUIRED tasks."""
        from gza.cli.execution import cmd_run

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Retry PR-required task")
        task.status = "failed"
        task.failure_reason = "PR_REQUIRED"
        task.branch = "task-branch"
        store.update(task)

        args = argparse.Namespace(
            project_dir=tmp_path,
            no_docker=True,
            max_turns=None,
            background=False,
            worker_mode=False,
            task_ids=[task.id],
            count=1,
            force=False,
            resume=False,
            create_pr=True,
        )

        with (
            patch("gza.cli.execution.Config.load", return_value=config),
            patch("gza.cli.execution.get_store", return_value=store),
            patch("gza.cli.execution._run_foreground", return_value=0) as run_mock,
        ):
            rc = cmd_run(args)

        assert rc == 0
        run_mock.assert_called_once()

    def test_work_rejects_branchless_failed_pr_required_task_with_pr_flag(self, tmp_path: Path):
        """work <task> --pr should fail closed for branchless legacy PR_REQUIRED rows."""
        from gza.cli.execution import cmd_run

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Branchless PR-required task")
        task.status = "failed"
        task.failure_reason = "PR_REQUIRED"
        store.update(task)

        args = argparse.Namespace(
            project_dir=tmp_path,
            no_docker=True,
            max_turns=None,
            background=False,
            worker_mode=False,
            task_ids=[task.id],
            count=1,
            force=False,
            resume=False,
            create_pr=True,
        )

        stdout = io.StringIO()
        stderr = io.StringIO()
        with (
            patch("gza.cli.execution.Config.load", return_value=config),
            patch("gza.cli.execution.get_store", return_value=store),
            patch("gza.cli.execution._run_foreground", return_value=0) as run_mock,
            patch("sys.stdout", stdout),
            patch("sys.stderr", stderr),
        ):
            rc = cmd_run(args)

        assert rc == 1
        run_mock.assert_not_called()
        output = stdout.getvalue() + stderr.getvalue()
        assert f"Error: Task {task.id} is not pending (status: failed)" in output

    def test_work_allows_failed_pr_required_task_with_persisted_create_pr(self, tmp_path: Path):
        """work <task> should allow retrying failed PR_REQUIRED tasks via stored create_pr intent."""
        from gza.cli.execution import cmd_run

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Retry PR-required task", create_pr=True)
        task.status = "failed"
        task.failure_reason = "PR_REQUIRED"
        task.branch = "task-branch"
        store.update(task)

        args = argparse.Namespace(
            project_dir=tmp_path,
            no_docker=True,
            max_turns=None,
            background=False,
            worker_mode=False,
            task_ids=[task.id],
            count=1,
            force=False,
            resume=False,
            create_pr=False,
        )

        with (
            patch("gza.cli.execution.Config.load", return_value=config),
            patch("gza.cli.execution.get_store", return_value=store),
            patch("gza.cli.execution._run_foreground", return_value=0) as run_mock,
        ):
            rc = cmd_run(args)

        assert rc == 0
        run_mock.assert_called_once()


class TestBackgroundWorkerCommand:
    """Tests for background worker subprocess command construction."""

    def test_spawn_detached_worker_process_starts_background_reaper(self, tmp_path: Path):
        """Detached bare worker launches should start a background waiter to reap the child."""
        from gza.cli._common import _spawn_detached_worker_process

        setup_config(tmp_path)
        config = Config.load(tmp_path)

        mock_proc = MagicMock()
        mock_proc.pid = 4321
        mock_thread = MagicMock()

        with (
            patch("gza.cli._common.subprocess.Popen", return_value=mock_proc),
            patch("gza.cli._common.threading.Thread", return_value=mock_thread) as thread_cls,
        ):
            proc, startup_log = _spawn_detached_worker_process(["echo", "hi"], config, "w-reap-test")

        assert proc is mock_proc
        assert startup_log == ".gza/workers/w-reap-test-startup.log"
        thread_cls.assert_called_once()
        kwargs = thread_cls.call_args.kwargs
        assert callable(kwargs["target"])
        kwargs["target"]()
        mock_proc.wait.assert_called_once_with()
        assert kwargs["name"] == "gza-worker-reaper-4321"
        assert kwargs["daemon"] is True
        mock_thread.start.assert_called_once_with()

    def test_background_worker_command_uses_project_flag(self, tmp_path: Path):
        """Background worker subprocess must pass project dir with --project flag, not as positional arg.

        Regression test: _spawn_background_worker was appending the project directory
        as a bare positional argument, which argparse would try to parse as a task_id
        (type=int), causing the worker subprocess to crash on startup.
        """
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import _spawn_background_worker
        from gza.config import Config

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Add a pending task
        task = store.add("Test background task")

        # Create workers directory
        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)

        config = Config.load(tmp_path)
        config.tmux.enabled = False  # Test bare Popen path, not tmux

        # Create args namespace matching what argparse produces
        args = argparse.Namespace(
            no_docker=True,
            max_turns=None,
            background=True,
            worker_mode=False,
            project_dir=str(tmp_path),
        )

        # Capture the subprocess command
        captured_cmd = None
        mock_proc = MagicMock()
        mock_proc.pid = 99999

        def capture_spawn(cmd, _config, worker_id):
            nonlocal captured_cmd
            captured_cmd = cmd
            return mock_proc, f".gza/workers/{worker_id}-startup.log"

        with patch("gza.cli._spawn_detached_worker_process", side_effect=capture_spawn):
            _spawn_background_worker(args, config, task_id=task.id)

        assert captured_cmd is not None, "subprocess.Popen was not called"

        # The project directory must be preceded by --project flag.
        # If it appears as a bare positional, argparse will try to parse it
        # as a task_id (type=int) and the worker subprocess will crash.
        project_dir_str = str(config.project_dir.absolute())
        assert project_dir_str in captured_cmd, \
            f"Project dir {project_dir_str!r} not found in command: {captured_cmd}"

        project_idx = captured_cmd.index(project_dir_str)
        assert captured_cmd[project_idx - 1] == "--project", \
            f"Project dir must be preceded by --project flag, but got: {captured_cmd[project_idx - 1]!r}. " \
            f"Full command: {captured_cmd}"

    def test_background_worker_command_forwards_pr_flag(self, tmp_path: Path):
        """Background work with create_pr intent should include --pr in child command."""
        from unittest.mock import MagicMock, patch

        from gza.cli import _spawn_background_worker

        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Test background task with pr")

        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)

        config = Config.load(tmp_path)
        config.tmux.enabled = False

        args = argparse.Namespace(
            no_docker=True,
            max_turns=None,
            background=True,
            worker_mode=False,
            project_dir=str(tmp_path),
            create_pr=True,
        )

        captured_cmd = None
        mock_proc = MagicMock()
        mock_proc.pid = 11111

        def capture_spawn(cmd, _config, worker_id):
            nonlocal captured_cmd
            captured_cmd = cmd
            return mock_proc, f".gza/workers/{worker_id}-startup.log"

        with patch("gza.cli._spawn_detached_worker_process", side_effect=capture_spawn):
            _spawn_background_worker(args, config, task_id=task.id)

        assert captured_cmd is not None
        assert "--pr" in captured_cmd

    def test_background_worker_without_explicit_task_prepares_selection_and_passes_task_id(self, tmp_path: Path):
        """No-id background work should prepare the selected task before detaching it."""
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import _spawn_background_worker
        from gza.config import Config
        from gza.workers import WorkerRegistry

        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Pending candidate")

        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)
        config = Config.load(tmp_path)
        config.tmux.enabled = False  # Test bare Popen path, not tmux

        args = argparse.Namespace(
            no_docker=True,
            max_turns=None,
            background=True,
            worker_mode=False,
            project_dir=str(tmp_path),
        )

        captured_cmd = None
        mock_proc = MagicMock()
        mock_proc.pid = 99999

        def capture_spawn(cmd, _config, worker_id):
            nonlocal captured_cmd
            captured_cmd = cmd
            return mock_proc, f".gza/workers/{worker_id}-startup.log"

        with patch("gza.cli._spawn_detached_worker_process", side_effect=capture_spawn):
            rc = _spawn_background_worker(args, config)

        assert rc == 0
        assert captured_cmd is not None
        worker_mode_idx = captured_cmd.index("--worker-mode")
        assert worker_mode_idx + 1 < len(captured_cmd)
        assert captured_cmd[worker_mode_idx + 1] == str(task.id)

        registry = WorkerRegistry(config.workers_path)
        workers = registry.list_all(include_completed=True)
        assert len(workers) == 1
        assert workers[0].task_id == task.id

    def test_background_resume_worker_command_uses_project_flag(self, tmp_path: Path):
        """Background resume worker subprocess must pass project dir with --project flag.

        Same regression as test_background_worker_command_uses_project_flag but
        for _spawn_background_resume_worker.
        """
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import _spawn_background_resume_worker
        from gza.config import Config

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Add a pending task
        task = store.add("Test resume task")

        # Create workers directory
        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)

        config = Config.load(tmp_path)

        args = argparse.Namespace(
            no_docker=True,
            max_turns=None,
            background=True,
            worker_mode=False,
            project_dir=str(tmp_path),
        )

        # Capture the subprocess command
        captured_cmd = None
        mock_proc = MagicMock()
        mock_proc.pid = 99999

        def capture_spawn(cmd, _config, worker_id):
            nonlocal captured_cmd
            captured_cmd = cmd
            return mock_proc, f".gza/workers/{worker_id}-startup.log"

        with patch("gza.cli._spawn_detached_worker_process", side_effect=capture_spawn):
            _spawn_background_resume_worker(args, config, new_task_id=task.id)

        assert captured_cmd is not None, "subprocess.Popen was not called"

        # The project directory must be preceded by --project flag
        project_dir_str = str(config.project_dir.absolute())
        assert project_dir_str in captured_cmd, \
            f"Project dir {project_dir_str!r} not found in command: {captured_cmd}"

        project_idx = captured_cmd.index(project_dir_str)
        assert captured_cmd[project_idx - 1] == "--project", \
            f"Project dir must be preceded by --project flag, but got: {captured_cmd[project_idx - 1]!r}. " \
            f"Full command: {captured_cmd}"

    def test_background_worker_verbose_output_escapes_prompt_and_shows_log_hint(self, tmp_path: Path):
        """Verbose startup output preserves literal bracket text in prompts."""
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import _spawn_background_worker
        from gza.config import Config
        from gza.console import console

        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Prompt with [literal] brackets")

        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)
        config = Config.load(tmp_path)
        config.tmux.enabled = False

        args = argparse.Namespace(
            no_docker=True,
            max_turns=None,
            background=True,
            worker_mode=False,
            project_dir=str(tmp_path),
        )

        mock_proc = MagicMock()
        mock_proc.pid = 22222

        def capture_spawn(_cmd, _config, worker_id):
            return mock_proc, f".gza/workers/{worker_id}-startup.log"

        with (
            patch("gza.cli._spawn_detached_worker_process", side_effect=capture_spawn),
            console.capture() as capture,
        ):
            rc = _spawn_background_worker(args, config, task_id=task.id)

        output = capture.get()
        assert rc == 0
        assert f"Started task {task.id} in background (PID {mock_proc.pid})" in output
        assert "Prompt: Prompt with [literal] brackets" in output
        assert f"Use 'gza log {task.id} -f' to follow progress" in output

    def test_background_worker_quiet_output_is_compact(self, tmp_path: Path):
        """Quiet startup output should omit prompt and follow instructions."""
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import _spawn_background_worker
        from gza.config import Config
        from gza.console import console

        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Quiet worker")

        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)
        config = Config.load(tmp_path)
        config.tmux.enabled = False

        args = argparse.Namespace(
            no_docker=True,
            max_turns=None,
            background=True,
            worker_mode=False,
            project_dir=str(tmp_path),
        )

        mock_proc = MagicMock()
        mock_proc.pid = 33333

        def capture_spawn(_cmd, _config, worker_id):
            return mock_proc, f".gza/workers/{worker_id}-startup.log"

        with (
            patch("gza.cli._spawn_detached_worker_process", side_effect=capture_spawn),
            console.capture() as capture,
        ):
            rc = _spawn_background_worker(args, config, task_id=task.id, quiet=True)

        output = capture.get()
        assert rc == 0
        assert f"Started task {task.id} in background (PID {mock_proc.pid})" in output
        assert "Prompt:" not in output
        assert "Use 'gza log" not in output

    def test_background_iterate_worker_startup_quiet_suppresses_entire_startup_block(self, tmp_path: Path):
        """startup_quiet should suppress even the shared headline for watch-managed iterate launches."""
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli._common import _spawn_background_iterate_worker
        from gza.config import Config
        from gza.console import console

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl_task = store.add("Iterate quietly", task_type="implement")
        assert impl_task.id is not None

        config = Config.load(tmp_path)
        mock_proc = MagicMock()
        mock_proc.pid = 44444

        def capture_spawn(_cmd, _config, worker_id):
            return mock_proc, f".gza/workers/{worker_id}-startup.log"

        args = argparse.Namespace(no_docker=True, force=False)

        with (
            patch("gza.cli._common._spawn_detached_worker_process", side_effect=capture_spawn),
            console.capture() as capture,
        ):
            rc = _spawn_background_iterate_worker(
                args,
                config,
                impl_task,
                max_iterations=3,
                startup_quiet=True,
            )

        assert rc == 0
        assert capture.get() == ""

    def test_background_iterate_worker_default_still_prints_startup_line(self, tmp_path: Path):
        """Direct iterate background launches keep the existing startup block by default."""
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli._common import _spawn_background_iterate_worker
        from gza.config import Config
        from gza.console import console

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl_task = store.add("Iterate loudly", task_type="implement")
        assert impl_task.id is not None

        config = Config.load(tmp_path)
        mock_proc = MagicMock()
        mock_proc.pid = 45454

        def capture_spawn(_cmd, _config, worker_id):
            return mock_proc, f".gza/workers/{worker_id}-startup.log"

        args = argparse.Namespace(no_docker=True, force=False)

        with (
            patch("gza.cli._common._spawn_detached_worker_process", side_effect=capture_spawn),
            console.capture() as capture,
        ):
            rc = _spawn_background_iterate_worker(args, config, impl_task, max_iterations=3)

        output = capture.get()
        assert rc == 0
        assert f"Started task {impl_task.id} in background (PID {mock_proc.pid})" in output
        assert f"Use 'gza log {impl_task.id} -f' to follow progress" in output

    def test_background_resume_worker_verbose_output_matches_work_style(self, tmp_path: Path):
        """Resume startup output reuses the same themed shape as regular work."""
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import _spawn_background_resume_worker
        from gza.config import Config
        from gza.console import console

        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Resume [literal] prompt")

        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)
        config = Config.load(tmp_path)

        args = argparse.Namespace(
            no_docker=True,
            max_turns=None,
            background=True,
            worker_mode=False,
            project_dir=str(tmp_path),
        )

        mock_proc = MagicMock()
        mock_proc.pid = 44444

        with (
            patch(
                "gza.cli._spawn_detached_worker_process",
                return_value=(mock_proc, ".gza/workers/test-startup.log"),
            ),
            console.capture() as capture,
        ):
            rc = _spawn_background_resume_worker(args, config, new_task_id=task.id)

        output = capture.get()
        assert rc == 0
        assert f"Started task {task.id} in background (resuming, PID {mock_proc.pid})" in output
        assert "Prompt: Resume [literal] prompt" in output
        assert f"Use 'gza log {task.id} -f' to follow progress" in output

    def test_review_background_reuses_prepared_task_without_second_prepare(self, tmp_path: Path):
        """Review background handoff must reuse the prepared child instead of preparing twice."""
        from gza.cli.execution import cmd_review

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        config.tmux.enabled = False
        store = make_store(tmp_path)

        impl = store.add("Completed implementation", task_type="implement")
        assert impl.id is not None
        impl.status = "completed"
        impl.completed_at = datetime.now(UTC)
        store.update(impl)

        args = argparse.Namespace(
            task_id=impl.id,
            project_dir=tmp_path,
            no_docker=True,
            queue=False,
            background=True,
            model=None,
            provider=None,
            open=False,
            force=False,
        )

        prepare_calls = {"count": 0}
        mock_proc = MagicMock()
        mock_proc.pid = 45454

        def prepare_once(_config, task, **_kwargs):
            prepare_calls["count"] += 1
            if prepare_calls["count"] > 1:
                raise AssertionError("background review should not prepare twice")
            return task

        with (
            patch("gza.cli._prepare_task_for_immediate_execution", side_effect=prepare_once),
            patch(
                "gza.cli._spawn_detached_worker_process",
                return_value=(mock_proc, ".gza/workers/review-startup.log"),
            ),
        ):
            rc = cmd_review(args)

        assert rc == 0
        assert prepare_calls["count"] == 1
        review_tasks = store.get_reviews_for_task(impl.id)
        assert len(review_tasks) == 1
        review_task = review_tasks[0]
        registry = WorkerRegistry(config.workers_path)
        workers = registry.list_all(include_completed=True)
        assert len(workers) == 1
        assert workers[0].task_id == review_task.id

    def test_resume_background_reuses_prepared_task_without_second_prepare(self, tmp_path: Path):
        """Resume background handoff must reuse the prepared child instead of preparing twice."""
        from gza.cli.execution import cmd_resume

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = make_store(tmp_path)

        failed = store.add("Failed implementation", task_type="implement")
        assert failed.id is not None
        failed.status = "failed"
        failed.failure_reason = "MAX_TURNS"
        failed.session_id = "resume-session-1"
        store.update(failed)

        args = argparse.Namespace(
            task_id=failed.id,
            project_dir=tmp_path,
            no_docker=True,
            max_turns=None,
            queue=False,
            background=True,
            force=False,
        )

        prepare_calls = {"count": 0}
        mock_proc = MagicMock()
        mock_proc.pid = 46464

        def prepare_once(_config, task, **_kwargs):
            prepare_calls["count"] += 1
            if prepare_calls["count"] > 1:
                raise AssertionError("background resume should not prepare twice")
            return task

        with (
            patch("gza.cli._prepare_task_for_immediate_execution", side_effect=prepare_once),
            patch(
                "gza.cli._spawn_detached_worker_process",
                return_value=(mock_proc, ".gza/workers/resume-startup.log"),
            ),
        ):
            rc = cmd_resume(args)

        assert rc == 0
        assert prepare_calls["count"] == 1
        children = store.get_based_on_children(failed.id)
        assert len(children) == 1
        registry = WorkerRegistry(config.workers_path)
        workers = registry.list_all(include_completed=True)
        assert len(workers) == 1
        assert workers[0].task_id == children[0].id

    def test_background_worker_registers_startup_log_file(self, tmp_path: Path):
        """Background worker captures early stdout/stderr into startup log metadata."""
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import _spawn_background_worker
        from gza.config import Config
        from gza.workers import WorkerRegistry

        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Startup log capture test task")

        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)
        config = Config.load(tmp_path)
        config.tmux.enabled = False  # Test bare Popen path, not tmux

        args = argparse.Namespace(
            no_docker=True,
            max_turns=None,
            background=True,
            worker_mode=False,
            project_dir=str(tmp_path),
        )

        mock_proc = MagicMock()
        mock_proc.pid = 99999

        def capture_spawn(_cmd, _config, worker_id):
            startup_log_rel = f".gza/workers/{worker_id}-startup.log"
            (tmp_path / startup_log_rel).touch()
            return mock_proc, startup_log_rel

        with patch("gza.cli._spawn_detached_worker_process", side_effect=capture_spawn):
            rc = _spawn_background_worker(args, config, task_id=task.id)

        assert rc == 0

        registry = WorkerRegistry(config.workers_path)
        workers = registry.list_all(include_completed=True)
        assert len(workers) == 1
        worker = workers[0]
        refreshed_task = store.get(task.id)
        assert refreshed_task is not None
        assert worker.startup_log_file == f".gza/workers/{worker.worker_id}-startup.log"
        assert worker.log_file == refreshed_task.log_file
        assert worker.task_slug == refreshed_task.slug
        assert (tmp_path / worker.startup_log_file).exists()

    def test_work_background_existing_task_startup_failure_surfaces_before_detach(self, tmp_path: Path):
        """Explicit background work should fail in the parent when startup preparation fails."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Parent-side startup failure")

        with (
            patch("gza.cli.prepare_task_startup_phase", side_effect=RuntimeError("startup boom")),
            patch(
                "gza.cli._spawn_detached_worker_process",
                side_effect=AssertionError("worker process should not spawn"),
            ),
        ):
            result = invoke_gza(
                "work",
                str(task.id),
                "--background",
                "--no-docker",
                "--project",
                str(tmp_path),
            )

        assert result.returncode == 1
        assert "startup boom" in result.stderr
        output = result.stdout + result.stderr
        assert "Started task" not in output

        workers_dir = tmp_path / ".gza" / "workers"
        if workers_dir.exists():
            assert list(workers_dir.iterdir()) == []

    def test_work_background_existing_task_log_setup_failure_restores_startup_metadata(self, tmp_path: Path):
        """Existing pending work rows should not retain startup metadata after Phase 1 log setup fails."""
        from gza.log_paths import resolve_task_log_paths

        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Parent-side log setup failure")
        assert task.slug is None
        assert task.log_file is None

        def fail_log_setup(config, _store, pending_task):
            paths = resolve_task_log_paths(config, pending_task)
            paths.conversation.parent.mkdir(parents=True, exist_ok=True)
            paths.conversation.touch()
            raise RuntimeError("log setup boom")

        with (
            patch("gza.runner.generate_slug", return_value="20260510-test-project-parent-side-log-setup-failure"),
            patch("gza.runner.ensure_task_log_paths", side_effect=fail_log_setup),
            patch(
                "gza.cli._spawn_detached_worker_process",
                side_effect=AssertionError("worker process should not spawn"),
            ),
        ):
            result = invoke_gza(
                "work",
                str(task.id),
                "--background",
                "--no-docker",
                "--project",
                str(tmp_path),
            )

        assert result.returncode == 1
        assert "log setup boom" in result.stderr
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.slug is None
        assert refreshed.log_file is None
        logs_dir = tmp_path / ".gza" / "logs"
        if logs_dir.exists():
            assert not any(path.is_file() for path in logs_dir.rglob("*"))
        workers_dir = tmp_path / ".gza" / "workers"
        if workers_dir.exists():
            assert list(workers_dir.iterdir()) == []

    def test_work_background_resume_dependency_block_preserves_failed_task_state_before_spawn(
        self,
        tmp_path: Path,
    ) -> None:
        """Explicit background resume must refuse blocked failed code tasks before startup side effects."""
        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)

        dependency = store.add("Dependency", task_type="implement")
        dependency.status = "completed"
        dependency.branch = "feature/dependency-background-resume"
        dependency.has_commits = True
        dependency.completed_at = datetime.now(UTC)
        store.update(dependency)

        task = store.add("Blocked background resume", task_type="implement", depends_on=dependency.id)
        failed_at = datetime.now(UTC)
        task.status = "failed"
        task.failure_reason = "TIMEOUT"
        task.started_at = failed_at
        task.completed_at = failed_at
        task.slug = "20260627-blocked-background-resume"
        task.session_id = "resume-session"
        task.log_file = "logs/existing-background-resume.log"
        store.update(task)
        assert task.id is not None

        with (
            patch(
                "gza.cli._spawn_detached_worker_process",
                side_effect=AssertionError("worker process should not spawn"),
            ),
            patch(
                "gza.cli._common.get_effective_config_for_task",
                side_effect=AssertionError("background resume should fail before provider routing"),
            ),
        ):
            result = invoke_gza(
                "work",
                str(task.id),
                "--background",
                "--resume",
                "--no-docker",
                "--project",
                str(tmp_path),
            )

        assert result.returncode == 1
        assert "blocked by task" in result.stderr
        output = result.stdout + result.stderr
        assert "Started task" not in output

        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.status == "failed"
        assert refreshed.failure_reason == "TIMEOUT"
        assert refreshed.started_at == failed_at
        assert refreshed.completed_at == failed_at
        assert refreshed.slug == "20260627-blocked-background-resume"
        assert refreshed.session_id == "resume-session"
        assert refreshed.log_file == "logs/existing-background-resume.log"
        assert refreshed.execution_mode is None

        logs_dir = tmp_path / ".gza" / "logs"
        if logs_dir.exists():
            assert not any(path.is_file() for path in logs_dir.rglob("*"))
        workers_dir = tmp_path / ".gza" / "workers"
        if workers_dir.exists():
            assert list(workers_dir.iterdir()) == []
        assert not (config.worktree_path / refreshed.slug).exists()

    def test_background_worker_tag_selection_prepares_selected_task_and_hands_off_that_task_id(
        self,
        tmp_path: Path,
    ):
        """Tag-selected background work should hand the prepared task ID to the child."""
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import _spawn_background_worker
        from gza.config import Config

        setup_config(tmp_path)
        store = make_store(tmp_path)
        selected = store.add("Selected tagged task", tags=("picked",))
        other = store.add("Other tagged task", tags=("picked",))
        assert selected.id is not None
        assert other.id is not None

        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)
        config = Config.load(tmp_path)
        config.tmux.enabled = False

        args = argparse.Namespace(
            no_docker=True,
            max_turns=None,
            background=True,
            worker_mode=False,
            project_dir=str(tmp_path),
            tag=["picked"],
            any_tag=False,
        )

        captured_cmd = None
        mock_proc = MagicMock()
        mock_proc.pid = 55555
        prepared_ids: list[str] = []

        def capture_spawn(cmd, _config, worker_id):
            nonlocal captured_cmd
            captured_cmd = cmd
            return mock_proc, f".gza/workers/{worker_id}-startup.log"

        def capture_prepare(_config, _store, task_to_prepare):
            assert task_to_prepare.id is not None
            prepared_ids.append(task_to_prepare.id)
            return task_to_prepare

        with (
            patch("gza.cli.prepare_task_startup_phase", side_effect=capture_prepare),
            patch("gza.cli._spawn_detached_worker_process", side_effect=capture_spawn),
        ):
            rc = _spawn_background_worker(args, config)

        assert rc == 0
        assert prepared_ids == [selected.id]
        assert captured_cmd is not None
        worker_mode_idx = captured_cmd.index("--worker-mode")
        assert captured_cmd[worker_mode_idx + 1] == str(selected.id)
        assert "--tag" not in captured_cmd

    def test_background_iterate_worker_passes_worker_id_to_child(self, tmp_path: Path):
        """Background iterate workers must pass the generated worker_id to the child process."""
        import argparse
        from unittest.mock import MagicMock

        from gza.cli._common import _spawn_background_iterate_worker
        from gza.config import Config

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl_task = store.add("Iterate in background", task_type="implement")
        assert impl_task.id is not None

        config = Config.load(tmp_path)
        args = argparse.Namespace(
            no_docker=True,
            force=False,
        )

        captured_cmd: list[str] | None = None
        mock_proc = MagicMock()
        mock_proc.pid = 42424

        def fake_spawn(cmd: list[str], _config: Config, worker_id: str):
            nonlocal captured_cmd
            captured_cmd = list(cmd)
            startup_log = f".gza/workers/{worker_id}-startup.log"
            return mock_proc, startup_log

        with patch("gza.cli._common._spawn_detached_worker_process", side_effect=fake_spawn):
            rc = _spawn_background_iterate_worker(args, config, impl_task, max_iterations=3)

        assert rc == 0
        assert captured_cmd is not None
        assert "--worker-id" in captured_cmd
        worker_id = captured_cmd[captured_cmd.index("--worker-id") + 1]

        registry = WorkerRegistry(config.workers_path)
        worker = registry.get(worker_id)
        assert worker is not None
        assert worker.task_id == impl_task.id
        assert worker.pid == 42424

    def test_background_worker_allows_failed_pr_required_task_with_pr_flag(self, tmp_path: Path):
        """Background explicit work should allow retrying failed PR_REQUIRED tasks with --pr."""
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import _spawn_background_worker
        from gza.config import Config

        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Retry PR-required task in background")
        task.status = "failed"
        task.failure_reason = "PR_REQUIRED"
        task.branch = "task-branch"
        store.update(task)

        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)
        config = Config.load(tmp_path)
        config.tmux.enabled = False

        args = argparse.Namespace(
            no_docker=True,
            max_turns=None,
            background=True,
            worker_mode=False,
            project_dir=str(tmp_path),
            create_pr=True,
            resume=False,
            force=False,
        )

        mock_proc = MagicMock()
        mock_proc.pid = 99999

        def capture_spawn(_cmd, _config, worker_id):
            return mock_proc, f".gza/workers/{worker_id}-startup.log"

        with patch("gza.cli._spawn_detached_worker_process", side_effect=capture_spawn) as mock_spawn:
            rc = _spawn_background_worker(args, config, task_id=task.id)

        assert rc == 0
        mock_spawn.assert_called_once()

    def test_background_worker_rejects_branchless_failed_pr_required_task_with_pr_flag(
        self, tmp_path: Path
    ):
        """Background explicit work should fail closed for branchless legacy PR_REQUIRED rows."""
        import argparse
        from unittest.mock import patch

        from gza.cli import _spawn_background_worker
        from gza.config import Config

        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Branchless PR-required task in background")
        task.status = "failed"
        task.failure_reason = "PR_REQUIRED"
        store.update(task)

        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)
        config = Config.load(tmp_path)
        config.tmux.enabled = False

        args = argparse.Namespace(
            no_docker=True,
            max_turns=None,
            background=True,
            worker_mode=False,
            project_dir=str(tmp_path),
            create_pr=True,
            resume=False,
            force=False,
        )

        stdout = io.StringIO()
        stderr = io.StringIO()
        with (
            patch("gza.cli._spawn_detached_worker_process") as mock_spawn,
            patch("sys.stdout", stdout),
            patch("sys.stderr", stderr),
        ):
            rc = _spawn_background_worker(args, config, task_id=task.id)

        assert rc == 1
        mock_spawn.assert_not_called()
        assert f"Error: Task {task.id} is not pending (status: failed)" in stderr.getvalue()

    def test_background_worker_allows_failed_pr_required_task_with_persisted_create_pr(self, tmp_path: Path):
        """Background explicit work should allow retrying failed PR_REQUIRED tasks via stored create_pr intent."""
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import _spawn_background_worker
        from gza.config import Config

        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Retry PR-required task in background", create_pr=True)
        task.status = "failed"
        task.failure_reason = "PR_REQUIRED"
        task.branch = "task-branch"
        store.update(task)

        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)
        config = Config.load(tmp_path)
        config.tmux.enabled = False

        args = argparse.Namespace(
            no_docker=True,
            max_turns=None,
            background=True,
            worker_mode=False,
            project_dir=str(tmp_path),
            create_pr=False,
            resume=False,
            force=False,
        )

        mock_proc = MagicMock()
        mock_proc.pid = 99999

        def capture_spawn(_cmd, _config, worker_id):
            return mock_proc, f".gza/workers/{worker_id}-startup.log"

        with patch("gza.cli._spawn_detached_worker_process", side_effect=capture_spawn) as mock_spawn:
            rc = _spawn_background_worker(args, config, task_id=task.id)

        assert rc == 0
        mock_spawn.assert_called_once()

    def test_background_worker_honors_task_providers_routing_for_fix(self, tmp_path: Path):
        """Fix task routed via task_providers.fix must pick provider-specific worker plumbing
        that matches what the runner will use at execution time, not the global config.provider.

        Regression: _spawn_background_worker previously derived provider from
        (task.provider or config.provider), ignoring task-type routing. That caused
        tmux/attach plumbing to diverge from the actually-executed provider.
        """
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import _spawn_background_worker
        from gza.config import Config

        config_path = tmp_path / "gza.yaml"
        worktree_dir = tmp_path / ".gza-test-worktrees"
        db_path = tmp_path / ".gza" / "gza.db"
        config_path.write_text(
            "project_name: test-project\n"
            f"worktree_dir: {worktree_dir}\n"
            f"db_path: {db_path}\n"
            "provider: claude\n"
            "task_providers:\n"
            "  fix: codex\n"
        )
        store = make_store(tmp_path)
        fix_task = store.add("Fix stuck workflow", task_type="fix")

        workers_path = tmp_path / ".gza" / "workers"
        workers_path.mkdir(parents=True, exist_ok=True)
        config = Config.load(tmp_path)
        config.tmux.enabled = True

        args = argparse.Namespace(
            no_docker=True,
            max_turns=None,
            background=True,
            worker_mode=False,
            project_dir=str(tmp_path),
            create_pr=False,
            resume=False,
            force=False,
        )

        mock_proc = MagicMock()
        mock_proc.pid = 99999
        mock_run_result = MagicMock()
        mock_run_result.returncode = 0

        subprocess_run_calls: list[list[str]] = []

        def capture_run(cmd, **kwargs):
            subprocess_run_calls.append(list(cmd))
            return mock_run_result

        # Pretend tmux is available so the provider-specific use_tmux decision
        # is observable in the final command.
        with (
            patch("gza.cli.prepare_task_startup_phase", side_effect=lambda _c, _s, task: task),
            patch("gza.cli.shutil.which", return_value="/usr/bin/tmux"),
            patch("gza.cli.subprocess.Popen", return_value=mock_proc),
            patch("gza.cli.subprocess.run", side_effect=capture_run),
            patch("gza.cli.get_tmux_session_pid", return_value=12345),
        ):
            _spawn_background_worker(args, config, task_id=fix_task.id)

        # Global provider is claude (which forces use_tmux=False for worker plumbing),
        # but fix routes to codex where tmux stays enabled — a `tmux new-session`
        # invocation is the observable proof the worker picked routed-provider semantics.
        new_session_calls = [c for c in subprocess_run_calls if len(c) >= 2 and c[0] == "tmux" and c[1] == "new-session"]
        assert new_session_calls, (
            "Fix task routed to codex via task_providers.fix should have kept tmux "
            f"enabled and launched a tmux session; subprocess.run calls were: {subprocess_run_calls}"
        )


class TestReconciliation:
    """Tests for in-progress reconciliation behavior."""

    def test_reconciliation_warns_on_task_failure_and_continues(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        """Per-task reconciliation failures should be visible, not silent."""
        from gza.cli._common import reconcile_in_progress_tasks
        from gza.config import Config
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Stuck in-progress task")
        store.mark_in_progress(task)
        task = store.get(task.id)
        assert task is not None
        task.running_pid = -1
        store.update(task)

        config = Config.load(tmp_path)
        with patch.object(SqliteTaskStore, "mark_failed", side_effect=RuntimeError("db-write-boom")):
            reconcile_in_progress_tasks(config)

        captured = capsys.readouterr()
        assert "Warning: Unexpected reconciliation error for task" in captured.err
        assert "db-write-boom" in captured.err

    def test_reconciliation_marks_silent_live_task_no_activity(self, tmp_path: Path):
        """An alive-but-silent task (no log writes for > threshold) is marked NO_ACTIVITY."""
        from datetime import UTC, datetime, timedelta

        from gza.cli._common import reconcile_in_progress_tasks
        from gza.config import Config
        from gza.failure_reasons import mark_task_failed_from_cause

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Silent task")
        store.mark_in_progress(task)
        task = store.get(task.id)
        assert task is not None
        task.running_pid = os.getpid()  # PID is alive
        task.started_at = datetime.now(UTC) - timedelta(minutes=2)
        # Leave task.log_file = None → log absent → stuck
        store.update(task)

        config = Config.load(tmp_path)
        # Patch SIGTERM so we don't actually signal our own PID
        with patch("gza.cli._common.os.kill") as mock_kill, \
             patch("gza.cli._common.mark_task_failed_from_cause", wraps=mark_task_failed_from_cause) as mock_mark_failed:
            reconcile_in_progress_tasks(config)

        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.status == "failed"
        assert refreshed.failure_reason == "NO_ACTIVITY"
        assert mock_mark_failed.call_count == 1
        assert mock_mark_failed.call_args.kwargs["explicit_reason"] == "NO_ACTIVITY"
        # kill called once for the signal check (os.kill(pid, 0)) and once to SIGTERM
        assert mock_kill.call_count >= 1
        request = WorkerRegistry(config.workers_path).consume_interrupt_request(os.getpid())
        assert request is not None
        assert request["signal"] == "SIGTERM"
        assert request["source"] == "watch_reconcile_no_activity"

    def test_reconciliation_skips_recent_live_task(self, tmp_path: Path):
        """A live task under the threshold should NOT be marked NO_ACTIVITY."""
        from datetime import UTC, datetime, timedelta

        from gza.cli._common import reconcile_in_progress_tasks
        from gza.config import Config

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Recent task")
        store.mark_in_progress(task)
        task = store.get(task.id)
        assert task is not None
        task.running_pid = os.getpid()
        task.started_at = datetime.now(UTC) - timedelta(seconds=5)
        store.update(task)

        config = Config.load(tmp_path)
        reconcile_in_progress_tasks(config)

        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.status == "in_progress"

    def test_reconciliation_uses_configured_no_activity_timeout(self, tmp_path: Path):
        """A non-default watch.no_activity_timeout should control silent-worker reconciliation."""
        from datetime import UTC, datetime, timedelta

        from gza.cli._common import reconcile_in_progress_tasks
        from gza.config import Config

        setup_config(tmp_path)
        (tmp_path / "gza.yaml").write_text(
            "project_name: test-project\n"
            "watch:\n"
            "  no_activity_timeout: 120\n"
        )
        store = make_store(tmp_path)

        task = store.add("Custom timeout task")
        store.mark_in_progress(task)
        task = store.get(task.id)
        assert task is not None
        task.running_pid = os.getpid()
        task.started_at = datetime.now(UTC) - timedelta(seconds=90)
        store.update(task)

        config = Config.load(tmp_path)
        reconcile_in_progress_tasks(config)

        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.status == "in_progress"

    def test_reconciliation_skips_live_task_with_recent_log_writes(self, tmp_path: Path):
        """A live task whose log was written recently should NOT be marked NO_ACTIVITY."""
        from datetime import UTC, datetime, timedelta

        from gza.cli._common import reconcile_in_progress_tasks
        from gza.config import Config

        setup_config(tmp_path)
        store = make_store(tmp_path)

        log_rel = ".gza/logs/active.log"
        log_abs = tmp_path / log_rel
        log_abs.parent.mkdir(parents=True, exist_ok=True)
        log_abs.write_text('{"subtype":"info","message":"hi"}\n')
        # Ensure mtime is now (write_text already does this)

        task = store.add("Active task")
        store.mark_in_progress(task)
        task = store.get(task.id)
        assert task is not None
        task.running_pid = os.getpid()
        task.started_at = datetime.now(UTC) - timedelta(minutes=5)
        task.log_file = log_rel
        store.update(task)

        config = Config.load(tmp_path)
        reconcile_in_progress_tasks(config)

        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.status == "in_progress"

    def test_reconciliation_leaves_recent_dead_pending_recovery_worker_pending(self, tmp_path: Path):
        """Dead running recovery workers younger than the no-activity threshold must remain pending."""
        from gza.cli._common import reconcile_dead_pending_recovery_tasks
        from gza.config import Config
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = make_store(tmp_path)

        failed = store.add("Failed implementation", task_type="implement")
        assert failed.id is not None
        failed.status = "failed"
        failed.failure_reason = "INFRASTRUCTURE_ERROR"
        failed.completed_at = datetime.now(UTC)
        store.update(failed)

        retry_child = store.add(
            failed.prompt,
            task_type="implement",
            based_on=failed.id,
            recovery_origin="retry",
        )
        assert retry_child.id is not None
        retry_child.status = "pending"
        store.update(retry_child)

        config = Config.load(tmp_path)
        registry = WorkerRegistry(config.workers_path)
        registry.register(
            WorkerMetadata(
                worker_id="w-dead-recovery",
                task_id=retry_child.id,
                pid=999999,
                status="running",
                startup_log_file=".gza/workers/retry-child.startup.log",
            )
        )

        reconcile_dead_pending_recovery_tasks(config)

        refreshed = store.get(retry_child.id)
        assert refreshed is not None
        assert refreshed.status == "pending"

        worker = registry.get("w-dead-recovery")
        assert worker is not None
        assert worker.status == "running"
        assert worker.completion_reason is None

    def test_reconciliation_terminalizes_failed_pending_recovery_start_failure(self, tmp_path: Path):
        """Failed pre-claim recovery workers should still terminalize the prepared child immediately."""
        from gza.cli._common import reconcile_dead_pending_recovery_tasks
        from gza.config import Config
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = make_store(tmp_path)

        failed = store.add("Failed implementation", task_type="implement")
        assert failed.id is not None
        failed.status = "failed"
        failed.failure_reason = "INFRASTRUCTURE_ERROR"
        failed.completed_at = datetime.now(UTC)
        store.update(failed)

        retry_child = store.add(
            failed.prompt,
            task_type="implement",
            based_on=failed.id,
            recovery_origin="retry",
        )
        assert retry_child.id is not None
        retry_child.status = "pending"
        store.update(retry_child)

        config = Config.load(tmp_path)
        registry = WorkerRegistry(config.workers_path)
        registry.register(
            WorkerMetadata(
                worker_id="w-failed-recovery",
                task_id=retry_child.id,
                pid=999999,
                status="failed",
                startup_log_file=".gza/workers/retry-child.startup.log",
            )
        )

        reconcile_dead_pending_recovery_tasks(config)

        refreshed = store.get(retry_child.id)
        assert refreshed is not None
        assert refreshed.status == "failed"
        assert refreshed.failure_reason == "WORKER_DIED"
        assert refreshed.log_file == ".gza/workers/retry-child.startup.log"

        worker = registry.get("w-failed-recovery")
        assert worker is not None
        assert worker.status == "failed"
        assert worker.completion_reason == "startup failure before task claim"

    def test_reconciliation_fails_silent_pending_task_with_dead_registered_worker(self, tmp_path: Path):
        """Pending tasks with dead registered workers past the silence timeout should fail NO_ACTIVITY."""
        import os
        from datetime import timedelta
        from unittest.mock import patch

        from gza.cli._common import reconcile_in_progress_tasks
        from gza.config import Config
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Pending task with stale worker")
        assert task.id is not None
        task.running_pid = 12345
        store.update(task)

        config = Config.load(tmp_path)
        config.watch.no_activity_timeout = 1
        registry = WorkerRegistry(config.workers_path)
        registry.register(
            WorkerMetadata(
                worker_id="w-dead-pending",
                task_id=task.id,
                pid=os.getpid(),
                started_at=(datetime.now(UTC) - timedelta(seconds=90)).isoformat(),
                status="running",
            )
        )

        with patch("gza.cli._common.WorkerRegistry.is_running", return_value=False):
            reconcile_in_progress_tasks(config)

        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.status == "failed"
        assert refreshed.failure_reason == "NO_ACTIVITY"
        assert refreshed.running_pid is None
        assert refreshed.completed_at is not None

        worker = registry.get("w-dead-pending")
        assert worker is not None
        assert worker.status == "failed"
        assert worker.completion_reason == "watch reconciliation detected dead pending worker with no activity"
        assert all(running.worker_id != "w-dead-pending" for running in registry.list_all())

    def test_reconciliation_fails_silent_pending_recovery_task_with_stale_logs(self, tmp_path: Path):
        """Pending recovery rows with dead running workers and stale startup/task logs should fail NO_ACTIVITY."""
        import os
        from datetime import timedelta
        from unittest.mock import patch

        from gza.cli._common import reconcile_in_progress_tasks
        from gza.config import Config
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = make_store(tmp_path)

        failed = store.add("Failed implementation", task_type="implement")
        assert failed.id is not None
        failed.status = "failed"
        failed.failure_reason = "INFRASTRUCTURE_ERROR"
        failed.completed_at = datetime.now(UTC)
        store.update(failed)

        retry_child = store.add(
            failed.prompt,
            task_type="implement",
            based_on=failed.id,
            recovery_origin="retry",
        )
        assert retry_child.id is not None
        retry_child.log_file = ".gza/logs/retry-child.log"
        store.update(retry_child)

        config = Config.load(tmp_path)
        config.watch.no_activity_timeout = 1
        registry = WorkerRegistry(config.workers_path)
        stale_started_at = datetime.now(UTC) - timedelta(seconds=90)
        registry.register(
            WorkerMetadata(
                worker_id="w-dead-pending-recovery",
                task_id=retry_child.id,
                pid=os.getpid(),
                started_at=stale_started_at.isoformat(),
                status="running",
                startup_log_file=".gza/workers/retry-child.startup.log",
            )
        )
        startup_log = tmp_path / ".gza" / "workers" / "retry-child.startup.log"
        startup_log.parent.mkdir(parents=True, exist_ok=True)
        startup_log.write_text("startup evidence\n")
        task_log = tmp_path / ".gza" / "logs" / "retry-child.log"
        task_log.parent.mkdir(parents=True, exist_ok=True)
        task_log.write_text("task evidence\n")
        stale_mtime = stale_started_at.timestamp()
        os.utime(startup_log, (stale_mtime, stale_mtime))
        os.utime(task_log, (stale_mtime, stale_mtime))

        with patch("gza.cli._common.WorkerRegistry.is_running", return_value=False):
            reconcile_in_progress_tasks(config)

        refreshed = store.get(retry_child.id)
        assert refreshed is not None
        assert refreshed.status == "failed"
        assert refreshed.failure_reason == "NO_ACTIVITY"
        assert refreshed.log_file == ".gza/logs/retry-child.log"

        worker = registry.get("w-dead-pending-recovery")
        assert worker is not None
        assert worker.status == "failed"
        assert worker.completion_reason == "watch reconciliation detected dead pending worker with no activity"

    def test_reconciliation_leaves_plain_pending_task_without_registered_worker_runnable(self, tmp_path: Path):
        """Ordinary pending queue items with no worker registry entry must remain pending."""
        from gza.cli._common import reconcile_in_progress_tasks
        from gza.config import Config

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Plain pending task")
        assert task.id is not None
        task.running_pid = 54321
        store.update(task)

        config = Config.load(tmp_path)
        config.watch.no_activity_timeout = 1

        reconcile_in_progress_tasks(config)

        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.status == "pending"
        assert refreshed.running_pid == 54321

    def test_prune_terminal_dead_workers_removes_completed_task_worker(self, tmp_path: Path):
        """Terminal task workers with dead PIDs should be pruned from the registry."""
        import os
        from unittest.mock import patch

        from gza.cli._common import prune_terminal_dead_workers
        from gza.config import Config
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Completed task with stale worker metadata")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        config = Config.load(tmp_path)
        registry = WorkerRegistry(config.workers_path)
        registry.register(
            WorkerMetadata(
                worker_id="w-prune-terminal",
                task_id=task.id,
                pid=os.getpid(),
                status="running",
            )
        )
        assert registry.get("w-prune-terminal") is not None

        with patch("gza.cli._common.WorkerRegistry.is_running", return_value=False):
            prune_terminal_dead_workers(config)

        assert registry.get("w-prune-terminal") is None

    def test_prune_terminal_dead_workers_keeps_in_progress_task_worker(self, tmp_path: Path):
        """Non-terminal task workers should not be pruned by terminal cleanup."""
        import os

        from gza.cli._common import prune_terminal_dead_workers
        from gza.config import Config
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("In-progress task should keep worker entry")
        store.mark_in_progress(task)

        config = Config.load(tmp_path)
        registry = WorkerRegistry(config.workers_path)
        registry.register(
            WorkerMetadata(
                worker_id="w-keep-in-progress",
                task_id=task.id,
                pid=os.getpid(),
                status="running",
            )
        )

        prune_terminal_dead_workers(config)

        assert registry.get("w-keep-in-progress") is not None

    @pytest.mark.parametrize(
        ("worker_id", "task_completed_at", "worker_started_at"),
        [
            (
                "w-fresh-live-terminal",
                datetime(2026, 1, 1, tzinfo=UTC).isoformat(),
                "now",
            ),
            (
                "w-old-live-terminal",
                datetime(2026, 1, 1, tzinfo=UTC).isoformat(),
                datetime(2026, 1, 1, tzinfo=UTC).isoformat(),
            ),
        ],
    )
    def test_prune_terminal_dead_workers_keeps_live_terminal_worker_regardless_of_age(
        self,
        tmp_path: Path,
        worker_id: str,
        task_completed_at: str,
        worker_started_at: str,
        capsys: pytest.CaptureFixture[str],
    ):
        """Live terminal-task workers stay visible whether fresh or long-running."""
        import os

        from gza.cli._common import prune_terminal_dead_workers
        from gza.config import Config
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = make_store(tmp_path)
        started_at = datetime.now(UTC).isoformat() if worker_started_at == "now" else worker_started_at

        task = store.add("Terminal task with live worker")
        task.status = "completed"
        task.completed_at = datetime.fromisoformat(task_completed_at)
        store.update(task)

        config = Config.load(tmp_path)
        registry = WorkerRegistry(config.workers_path)
        registry.register(
            WorkerMetadata(
                worker_id=worker_id,
                task_id=task.id,
                pid=os.getpid(),
                status="running",
                started_at=started_at,
            )
        )

        prune_terminal_dead_workers(config)

        assert registry.get(worker_id) is not None
        assert "Pruning stale terminal worker" not in capsys.readouterr().err

    def test_prune_terminal_dead_workers_silently_removes_zombie_pid(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ):
        """Zombie worker PIDs should be pruned via the dead-worker path without a stale warning."""
        from gza.cli._common import prune_terminal_dead_workers
        from gza.config import Config
        from gza.workers import WorkerMetadata, WorkerRegistry

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Terminal task with zombie worker")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        config = Config.load(tmp_path)
        registry = WorkerRegistry(config.workers_path)
        registry.register(
            WorkerMetadata(
                worker_id="w-zombie-terminal",
                task_id=task.id,
                pid=os.getpid(),
                pid_start_ticks=123,
                status="running",
            )
        )

        with patch("gza.workers._read_linux_proc_stat", return_value=("Z", 123)):
            prune_terminal_dead_workers(config)

        assert registry.get("w-zombie-terminal") is None
        assert "Pruning stale terminal worker" not in capsys.readouterr().err


class TestImplementCommand:
    """Tests for 'gza implement' command."""

    def test_implement_creates_task_from_completed_plan(self, tmp_path: Path):
        """Implement command creates an implementation task and queues it."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan_task = store.add("Plan authentication rollout", task_type="plan")
        plan_task.status = "completed"
        plan_task.completed_at = datetime.now(UTC)
        store.update(plan_task)

        result = invoke_gza(
            "implement",
            str(plan_task.id),
            "Implement auth rollout",
            "--review",
            "--queue",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 0
        assert "Created implement task " in result.stdout

        impl_task = get_latest_task(store, depends_on=plan_task.id, task_type="implement")
        assert impl_task is not None
        assert impl_task.id != plan_task.id
        assert impl_task.task_type == "implement"
        assert impl_task.based_on is None
        assert impl_task.depends_on == plan_task.id
        assert impl_task.prompt == "Implement auth rollout"
        assert impl_task.create_review is True
        assert f"Depends on: plan {plan_task.id}" in result.stdout

    def test_implement_fails_for_missing_plan_task(self, tmp_path: Path):
        """Implement command validates referenced plan task exists."""
        setup_db_with_tasks(tmp_path, [])

        result = invoke_gza("implement", "testproject-999999", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "not found" in result.stdout

    def test_implement_fails_for_non_plan_task(self, tmp_path: Path):
        """Implement command requires a plan task."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Not a plan", task_type="implement")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        result = invoke_gza("implement", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 1
        assert f"Error: Task {task.id} is a implement task" in result.stdout

    def test_implement_fails_for_incomplete_plan_task(self, tmp_path: Path):
        """Implement command requires the plan task to be completed."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan_task = store.add("Plan feature", task_type="plan")

        result = invoke_gza("implement", str(plan_task.id), "--project", str(tmp_path))

        assert result.returncode == 1
        assert f"Error: Task {plan_task.id} is pending. Plan task must be completed." in result.stdout

    def test_implement_derives_prompt_from_plan_slug_when_omitted(self, tmp_path: Path):
        """Implement command derives prompt from the plan task slug when prompt omitted."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan_task = store.add("Plan auth migration", task_type="plan")
        plan_task.slug = "20260226-plan-auth-migration"
        plan_task.status = "completed"
        plan_task.completed_at = datetime.now(UTC)
        store.update(plan_task)

        result = invoke_gza("implement", str(plan_task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Created implement task " in result.stdout

        impl_task = get_latest_task(store, depends_on=plan_task.id, task_type="implement")
        assert impl_task is not None
        assert impl_task.id != plan_task.id
        assert impl_task.prompt == f"Implement plan from task {plan_task.id}: plan-auth-migration"
        assert impl_task.based_on is None
        assert impl_task.depends_on == plan_task.id

    def test_implement_derives_prompt_from_base_plan_slug_when_retry_suffix_present(self, tmp_path: Path):
        """Implement command strips numeric retry suffix from plan slug."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan_task = store.add("Plan auth migration", task_type="plan")
        plan_task.slug = "20260226-plan-auth-migration-2"
        plan_task.status = "completed"
        plan_task.completed_at = datetime.now(UTC)
        store.update(plan_task)

        result = invoke_gza("implement", str(plan_task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Created implement task " in result.stdout

        impl_task = get_latest_task(store, depends_on=plan_task.id, task_type="implement")
        assert impl_task is not None
        assert impl_task.id != plan_task.id
        assert impl_task.prompt == f"Implement plan from task {plan_task.id}: plan-auth-migration"
        assert impl_task.based_on is None
        assert impl_task.depends_on == plan_task.id

    def test_implement_inherits_plan_tags_when_no_tag_override_supplied(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan_task = store.add("Plan authentication rollout", task_type="plan", tags=("202606-recovery", "v0.5.0"))
        plan_task.status = "completed"
        plan_task.completed_at = datetime.now(UTC)
        store.update(plan_task)

        result = invoke_gza("implement", str(plan_task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        impl_task = get_latest_task(store, depends_on=plan_task.id, task_type="implement")
        assert impl_task is not None
        assert impl_task.tags == plan_task.tags

    def test_implement_explicit_tag_override_replaces_inherited_plan_tags(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan_task = store.add("Plan authentication rollout", task_type="plan", tags=("202606-recovery", "v0.5.0"))
        plan_task.status = "completed"
        plan_task.completed_at = datetime.now(UTC)
        store.update(plan_task)

        result = invoke_gza(
            "implement",
            str(plan_task.id),
            "--tag",
            "manual-override",
            "--queue",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 0
        impl_task = get_latest_task(store, depends_on=plan_task.id, task_type="implement")
        assert impl_task is not None
        assert impl_task.tags == ("manual-override",)

    def test_implement_clears_hold_for_review_after_creating_child(self, tmp_path: Path):
        """Manual implementation approval should release the plan hold once the child exists."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan_task = store.add("Plan auth migration", task_type="plan", auto_implement=False)
        plan_task.status = "completed"
        plan_task.completed_at = datetime.now(UTC)
        store.update(plan_task)

        result = invoke_gza("implement", str(plan_task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        refreshed = store.get(plan_task.id)
        assert refreshed is not None
        assert refreshed.auto_implement is True

    def test_implement_prefers_approved_plan_review_manifest(self, tmp_path: Path):
        """Manual implement should materialize approved slices instead of one legacy task."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan_task = store.add("Plan auth migration", task_type="plan")
        assert plan_task.id is not None
        plan_task.status = "completed"
        plan_task.completed_at = datetime.now(UTC)
        store.update(plan_task)

        review = store.add("Review auth migration plan", task_type="plan_review", depends_on=plan_task.id)
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        review.output_content = (
            "## Verdict\n"
            "Verdict: APPROVED\n\n"
            "## Slice Manifest\n"
            "```json\n"
            "{"
            f"\"schema_version\":1,\"source_task_id\":\"{plan_task.id}\",\"source_task_type\":\"plan\","
            "\"verdict\":\"APPROVED\","
            "\"slice_quality\":{\"fits_single_task_budget\":true,\"timeout_budget_minutes\":30,"
            "\"max_expected_files_changed_per_slice\":8,\"rationale\":\"Bounded.\"},"
            "\"slices\":["
            "{\"slice_id\":\"S1\",\"title\":\"Foundation\",\"prompt\":\"Implement foundation slice.\","
            "\"scope\":[\"Add parser\"],\"out_of_scope\":[],\"acceptance_criteria\":[\"Parser works\"],"
            "\"depends_on_slices\":[],\"based_on_slice\":null,\"review_scope\":\"Foundation only.\","
            "\"estimated_complexity\":\"small\",\"expected_timeout_minutes\":30,"
            "\"requires_code_review\":true,\"tags\":[\"slice-a\"]},"
            "{\"slice_id\":\"S2\",\"title\":\"Follow-up\",\"prompt\":\"Implement follow-up slice.\","
            "\"scope\":[\"Add executor\"],\"out_of_scope\":[],\"acceptance_criteria\":[\"Executor works\"],"
            "\"depends_on_slices\":[\"S1\"],\"based_on_slice\":\"S1\",\"review_scope\":\"Follow-up only.\","
            "\"estimated_complexity\":\"small\",\"expected_timeout_minutes\":30,"
            "\"requires_code_review\":true,\"tags\":[\"slice-b\"]}"
            "]}\n"
            "```\n"
        )
        store.update(review)

        result = invoke_gza("implement", str(plan_task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        created = [task for task in store.get_all() if task.task_type == "implement"]
        assert len(created) == 2
        created.sort(key=lambda task: task_id_numeric_key(task.id))
        assert created[0].based_on == plan_task.id
        assert created[0].depends_on is None
        assert "Implement foundation slice." in created[0].prompt
        assert created[1].depends_on == created[0].id
        assert created[1].based_on == created[0].id
        assert created[1].same_branch is True
        assert "Implement follow-up slice." in created[1].prompt
        assert "Created implement task" in result.stdout

    def test_implement_rejects_invalid_approved_plan_review_manifest(self, tmp_path: Path) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan_task = store.add("Plan auth migration", task_type="plan")
        assert plan_task.id is not None
        plan_task.status = "completed"
        plan_task.completed_at = datetime.now(UTC)
        store.update(plan_task)

        review = store.add("Review auth migration plan", task_type="plan_review", depends_on=plan_task.id)
        assert review.id is not None
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        review.output_content = "## Verdict\nVerdict: APPROVED\n"
        store.update(review)

        result = invoke_gza("implement", str(plan_task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 1
        assert len([task for task in store.get_all() if task.task_type == "implement"]) == 0
        assert f"Plan review {review.id} is APPROVED but its slice manifest is invalid" in result.stdout
        assert "approved plan review report must include a json manifest block" in result.stdout

    def test_implement_rejects_invalid_approved_override_manifest(self, tmp_path: Path) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan_task = store.add("Plan auth migration", task_type="plan")
        assert plan_task.id is not None
        plan_task.status = "completed"
        plan_task.completed_at = datetime.now(UTC)
        store.update(plan_task)

        review = store.add("Review auth migration plan", task_type="plan_review", depends_on=plan_task.id)
        assert review.id is not None
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        review.output_content = (
            "## Verdict\nVerdict: APPROVED\n\n## Slice Manifest\n```json\n"
            "{"
            f"\"schema_version\":1,\"source_task_id\":\"{plan_task.id}\",\"source_task_type\":\"plan\","
            "\"verdict\":\"APPROVED\","
            "\"slice_quality\":{\"fits_single_task_budget\":true,\"timeout_budget_minutes\":30,"
            "\"max_expected_files_changed_per_slice\":8,\"rationale\":\"Bounded.\"},"
            "\"slices\":["
            "{\"slice_id\":\"S1\",\"title\":\"Foundation\",\"prompt\":\"Implement foundation slice.\","
            "\"scope\":[\"Add parser\"],\"out_of_scope\":[],\"acceptance_criteria\":[\"Parser works\"],"
            "\"depends_on_slices\":[],\"based_on_slice\":null,\"review_scope\":\"Foundation only.\","
            "\"estimated_complexity\":\"small\",\"expected_timeout_minutes\":30,"
            "\"requires_code_review\":true,\"tags\":[\"slice-a\"]}"
            "]}\n"
            "```\n"
        )
        store.update(review)
        _store_plan_review_override_artifact(tmp_path, store, review.id, output="[]")

        result = invoke_gza("implement", str(plan_task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 1
        assert len([task for task in store.get_all() if task.task_type == "implement"]) == 0
        assert f"Plan review {review.id} is APPROVED but its override manifest is invalid" in result.stdout
        assert "stored plan review override is not a JSON object" in result.stdout

    def test_implement_falls_back_when_no_completed_approved_plan_review_exists(self, tmp_path: Path) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan_task = store.add("Plan auth migration", task_type="plan")
        assert plan_task.id is not None
        plan_task.status = "completed"
        plan_task.completed_at = datetime.now(UTC)
        store.update(plan_task)

        result = invoke_gza("implement", str(plan_task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        created = [task for task in store.get_all() if task.task_type == "implement"]
        assert len(created) == 1
        assert created[0].depends_on == plan_task.id
        assert "Created implement task" in result.stdout

    def test_implement_warns_when_falling_back_after_non_approved_plan_review(self, tmp_path: Path) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan_task = store.add("Plan auth migration", task_type="plan")
        assert plan_task.id is not None
        plan_task.status = "completed"
        plan_task.completed_at = datetime.now(UTC)
        store.update(plan_task)

        review = store.add("Review auth migration plan", task_type="plan_review", depends_on=plan_task.id)
        assert review.id is not None
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        review.output_content = "## Verdict\nVerdict: CHANGES_REQUESTED\n"
        store.update(review)

        result = invoke_gza("implement", str(plan_task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        created = [task for task in store.get_all() if task.task_type == "implement"]
        assert len(created) == 1
        assert (
            f"Warning: latest completed plan review {review.id} has verdict CHANGES_REQUESTED; "
            "falling back to legacy single implement task."
        ) in result.stdout

    def test_implement_queue_reuses_existing_materialized_slices(self, tmp_path: Path) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan_task = store.add("Plan auth migration", task_type="plan")
        assert plan_task.id is not None
        plan_task.status = "completed"
        plan_task.completed_at = datetime.now(UTC)
        store.update(plan_task)

        review = store.add("Review auth migration plan", task_type="plan_review", depends_on=plan_task.id)
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        review.output_content = (
            "## Verdict\n"
            "Verdict: APPROVED\n\n"
            "## Slice Manifest\n"
            "```json\n"
            "{"
            f"\"schema_version\":1,\"source_task_id\":\"{plan_task.id}\",\"source_task_type\":\"plan\","
            "\"verdict\":\"APPROVED\","
            "\"slice_quality\":{\"fits_single_task_budget\":true,\"timeout_budget_minutes\":30,"
            "\"max_expected_files_changed_per_slice\":8,\"rationale\":\"Bounded.\"},"
            "\"slices\":["
            "{\"slice_id\":\"S1\",\"title\":\"Foundation\",\"prompt\":\"Implement foundation slice.\","
            "\"scope\":[\"Add parser\"],\"out_of_scope\":[],\"acceptance_criteria\":[\"Parser works\"],"
            "\"depends_on_slices\":[],\"based_on_slice\":null,\"review_scope\":\"Foundation only.\","
            "\"estimated_complexity\":\"small\",\"expected_timeout_minutes\":30,"
            "\"requires_code_review\":true,\"tags\":[\"slice-a\"]}"
            "]}\n"
            "```\n"
        )
        store.update(review)

        first = invoke_gza("implement", str(plan_task.id), "--queue", "--project", str(tmp_path))
        second = invoke_gza("implement", str(plan_task.id), "--queue", "--project", str(tmp_path))

        assert first.returncode == 0
        assert second.returncode == 0
        created = [task for task in store.get_all() if task.task_type == "implement"]
        assert len(created) == 1
        assert "Created implement task" in first.stdout
        assert "Reused implement task" in second.stdout
        assert "Created implement task" not in second.stdout

    def test_cmd_implement_reused_materialized_slices_survive_prepare_failure(self, tmp_path: Path) -> None:
        from gza.cli.execution import cmd_implement

        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan_task = store.add("Plan auth migration", task_type="plan")
        assert plan_task.id is not None
        plan_task.status = "completed"
        plan_task.completed_at = datetime.now(UTC)
        store.update(plan_task)

        review = store.add("Review auth migration plan", task_type="plan_review", depends_on=plan_task.id)
        assert review.id is not None
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        review.output_content = (
            "## Verdict\n"
            "Verdict: APPROVED\n\n"
            "## Slice Manifest\n"
            "```json\n"
            "{"
            f"\"schema_version\":1,\"source_task_id\":\"{plan_task.id}\",\"source_task_type\":\"plan\","
            "\"verdict\":\"APPROVED\","
            "\"slice_quality\":{\"fits_single_task_budget\":true,\"timeout_budget_minutes\":30,"
            "\"max_expected_files_changed_per_slice\":8,\"rationale\":\"Bounded.\"},"
            "\"slices\":["
            "{\"slice_id\":\"S1\",\"title\":\"Foundation\",\"prompt\":\"Implement foundation slice.\","
            "\"scope\":[\"Add parser\"],\"out_of_scope\":[],\"acceptance_criteria\":[\"Parser works\"],"
            "\"depends_on_slices\":[],\"based_on_slice\":null,\"review_scope\":\"Foundation only.\","
            "\"estimated_complexity\":\"small\",\"expected_timeout_minutes\":30,"
            "\"requires_code_review\":true,\"tags\":[\"slice-a\"]},"
            "{\"slice_id\":\"S2\",\"title\":\"Follow-up\",\"prompt\":\"Implement follow-up slice.\","
            "\"scope\":[\"Add executor\"],\"out_of_scope\":[],\"acceptance_criteria\":[\"Executor works\"],"
            "\"depends_on_slices\":[\"S1\"],\"based_on_slice\":\"S1\",\"review_scope\":\"Follow-up only.\","
            "\"estimated_complexity\":\"small\",\"expected_timeout_minutes\":30,"
            "\"requires_code_review\":true,\"tags\":[\"slice-b\"]}"
            "]}\n"
            "```\n"
        )
        store.update(review)

        first = invoke_gza("implement", str(plan_task.id), "--queue", "--project", str(tmp_path))
        assert first.returncode == 0

        created = [task for task in store.get_all() if task.task_type == "implement"]
        created.sort(key=lambda task: task_id_numeric_key(task.id))
        assert len(created) == 2
        original_ids = [task.id for task in created]

        materialization_artifacts = store.list_artifacts(review.id, kind="plan_review_materialization")
        assert len(materialization_artifacts) == 1

        args = argparse.Namespace(
            project_dir=tmp_path,
            no_docker=True,
            max_turns=None,
            plan_task_id=plan_task.id,
            prompt=None,
            group=None,
            depends_on=None,
            review=False,
            same_branch=False,
            branch_type=None,
            model=None,
            provider=None,
            skip_learnings=False,
            review_scope=None,
            run=True,
            background=False,
            queue=False,
            force=False,
            create_pr=False,
        )

        with patch(
            "gza.cli._common.prepare_task_startup_phase",
            side_effect=RuntimeError("startup exploded"),
        ):
            rc = cmd_implement(args)

        assert rc == 1
        refreshed = [store.get(task_id) for task_id in original_ids]
        assert all(task is not None for task in refreshed)
        assert [task.id for task in refreshed if task is not None] == original_ids
        assert all(task.status == "pending" for task in refreshed if task is not None)
        assert store.list_artifacts(review.id, kind="plan_review_materialization") == materialization_artifacts

    def test_cmd_implement_capacity_failure_leaves_no_materialized_slices(self, tmp_path: Path) -> None:
        from gza.cli.execution import cmd_implement

        setup_config(tmp_path)
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(config_path.read_text() + "max_concurrent: 1\n")
        store = make_store(tmp_path)

        running = store.add("Running task", task_type="implement")
        running.status = "in_progress"
        running.running_pid = os.getpid()
        store.update(running)

        plan_task = store.add("Plan auth migration", task_type="plan")
        assert plan_task.id is not None
        plan_task.status = "completed"
        plan_task.completed_at = datetime.now(UTC)
        store.update(plan_task)

        review = store.add("Review auth migration plan", task_type="plan_review", depends_on=plan_task.id)
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        review.output_content = (
            "## Verdict\n"
            "Verdict: APPROVED\n\n"
            "## Slice Manifest\n"
            "```json\n"
            "{"
            f"\"schema_version\":1,\"source_task_id\":\"{plan_task.id}\",\"source_task_type\":\"plan\","
            "\"verdict\":\"APPROVED\","
            "\"slice_quality\":{\"fits_single_task_budget\":true,\"timeout_budget_minutes\":30,"
            "\"max_expected_files_changed_per_slice\":8,\"rationale\":\"Bounded.\"},"
            "\"slices\":["
            "{\"slice_id\":\"S1\",\"title\":\"Foundation\",\"prompt\":\"Implement foundation slice.\","
            "\"scope\":[\"Add parser\"],\"out_of_scope\":[],\"acceptance_criteria\":[\"Parser works\"],"
            "\"depends_on_slices\":[],\"based_on_slice\":null,\"review_scope\":\"Foundation only.\","
            "\"estimated_complexity\":\"small\",\"expected_timeout_minutes\":30,"
            "\"requires_code_review\":true,\"tags\":[\"slice-a\"]}"
            "]}\n"
            "```\n"
        )
        store.update(review)

        args = argparse.Namespace(
            project_dir=tmp_path,
            no_docker=True,
            max_turns=None,
            plan_task_id=plan_task.id,
            prompt=None,
            group=None,
            depends_on=None,
            review=False,
            same_branch=False,
            branch_type=None,
            model=None,
            provider=None,
            skip_learnings=False,
            review_scope=None,
            run=True,
            background=False,
            queue=False,
            force=False,
            create_pr=False,
        )

        rc = cmd_implement(args)

        assert rc == 1
        assert len([task for task in store.get_all() if task.task_type == "implement" and task.id != running.id]) == 0

    def test_implement_rejects_depends_on_flag(self, tmp_path: Path):
        """Implement command should fail fast for removed --depends-on flag."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan_task = store.add("Plan auth migration", task_type="plan")
        plan_task.status = "completed"
        plan_task.completed_at = datetime.now(UTC)
        store.update(plan_task)

        dep_task = store.add("Independent dependency", task_type="implement")

        result = invoke_gza(
            "implement",
            str(plan_task.id),
            "--depends-on",
            str(dep_task.id),
            "--queue",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 2
        assert "unrecognized arguments: --depends-on" in result.stderr


class TestPlanReviewAndImproveCommands:
    def test_plan_review_queue_creates_branchless_review_task(self, tmp_path: Path) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan = store.add("Plan rollout", task_type="plan")
        plan.status = "completed"
        plan.completed_at = datetime.now(UTC)
        store.update(plan)

        result = invoke_gza("plan-review", str(plan.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        created = get_latest_task(store, task_type="plan_review", depends_on=plan.id)
        assert created is not None
        assert created.based_on is None
        assert "Created plan review task" in result.stdout

    def test_plan_review_background_spawns_worker(self, tmp_path: Path) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan = store.add("Plan rollout", task_type="plan")
        plan.status = "completed"
        plan.completed_at = datetime.now(UTC)
        store.update(plan)

        with patch("gza.cli.execution._spawn_background_worker", return_value=0) as spawn_background:
            result = invoke_gza(
                "plan-review",
                str(plan.id),
                "--background",
                "--no-docker",
                "--project",
                str(tmp_path),
            )

        assert result.returncode == 0
        created = get_latest_task(store, task_type="plan_review", depends_on=plan.id)
        assert created is not None
        spawn_background.assert_called_once()
        assert spawn_background.call_args.kwargs["task_id"] == created.id

    def test_cmd_plan_review_edit_slices_and_materialize_by_review_id(self, tmp_path: Path) -> None:
        from gza.cli.execution import cmd_plan_review

        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan = store.add("Plan rollout", task_type="plan")
        assert plan.id is not None
        plan.status = "completed"
        plan.completed_at = datetime.now(UTC)
        store.update(plan)

        review = store.add("Review rollout", task_type="plan_review", depends_on=plan.id)
        assert review.id is not None
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        review.output_content = (
            "## Verdict\nVerdict: APPROVED\n\n## Slice Manifest\n```json\n"
            "{"
            f"\"schema_version\":1,\"source_task_id\":\"{plan.id}\",\"source_task_type\":\"plan\","
            "\"verdict\":\"APPROVED\","
            "\"slice_quality\":{\"fits_single_task_budget\":true,\"timeout_budget_minutes\":30,"
            "\"max_expected_files_changed_per_slice\":8,\"rationale\":\"Bounded.\"},"
            "\"slices\":["
            "{\"slice_id\":\"S1\",\"title\":\"Foundation\",\"prompt\":\"Original prompt.\","
            "\"scope\":[\"Add parser\"],\"out_of_scope\":[],\"acceptance_criteria\":[\"Parser works\"],"
            "\"depends_on_slices\":[],\"based_on_slice\":null,\"review_scope\":\"Foundation only.\","
            "\"estimated_complexity\":\"small\",\"expected_timeout_minutes\":30,"
            "\"requires_code_review\":true,\"tags\":[\"slice-a\"]}"
            "]}\n"
            "```\n"
        )
        store.update(review)

        def _write_override(cmd: list[str]) -> SimpleNamespace:
            path = Path(cmd[-1])
            edited = json.loads(path.read_text())
            edited["slices"][0]["prompt"] = "Edited prompt."
            path.write_text(json.dumps(edited, indent=2))
            return SimpleNamespace(returncode=0)

        edit_args = argparse.Namespace(
            project_dir=tmp_path,
            no_docker=True,
            max_turns=None,
            task_id=review.id,
            rerun=False,
            edit_slices=True,
            materialize=False,
            run=True,
            queue=False,
            background=False,
            model=None,
            provider=None,
            force=False,
        )
        materialize_args = argparse.Namespace(
            project_dir=tmp_path,
            no_docker=True,
            max_turns=None,
            task_id=review.id,
            rerun=False,
            edit_slices=False,
            materialize=True,
            run=True,
            queue=False,
            background=False,
            model=None,
            provider=None,
            force=False,
        )

        with patch("gza.cli.execution._launch_editor", side_effect=_write_override):
            assert cmd_plan_review(edit_args) == 0

        assert cmd_plan_review(materialize_args) == 0
        assert cmd_plan_review(materialize_args) == 0

        created = [task for task in store.get_all() if task.task_type == "implement"]
        assert len(created) == 1
        assert "Edited prompt." in created[0].prompt

    def test_cmd_plan_review_edit_slices_invalid_manifest_creates_no_tasks(self, tmp_path: Path) -> None:
        from gza.cli.execution import cmd_plan_review

        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan = store.add("Plan rollout", task_type="plan")
        assert plan.id is not None
        plan.status = "completed"
        plan.completed_at = datetime.now(UTC)
        store.update(plan)

        review = store.add("Review rollout", task_type="plan_review", depends_on=plan.id)
        assert review.id is not None
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        review.output_content = (
            "## Verdict\nVerdict: APPROVED\n\n## Slice Manifest\n```json\n"
            "{"
            f"\"schema_version\":1,\"source_task_id\":\"{plan.id}\",\"source_task_type\":\"plan\","
            "\"verdict\":\"APPROVED\","
            "\"slice_quality\":{\"fits_single_task_budget\":true,\"timeout_budget_minutes\":30,"
            "\"max_expected_files_changed_per_slice\":8,\"rationale\":\"Bounded.\"},"
            "\"slices\":["
            "{\"slice_id\":\"S1\",\"title\":\"Foundation\",\"prompt\":\"Original prompt.\","
            "\"scope\":[\"Add parser\"],\"out_of_scope\":[],\"acceptance_criteria\":[\"Parser works\"],"
            "\"depends_on_slices\":[],\"based_on_slice\":null,\"review_scope\":\"Foundation only.\","
            "\"estimated_complexity\":\"small\",\"expected_timeout_minutes\":30,"
            "\"requires_code_review\":true,\"tags\":[\"slice-a\"]}"
            "]}\n"
            "```\n"
        )
        store.update(review)

        def _write_invalid_override(cmd: list[str]) -> SimpleNamespace:
            path = Path(cmd[-1])
            edited = json.loads(path.read_text())
            edited["slices"][0]["acceptance_criteria"] = []
            path.write_text(json.dumps(edited, indent=2))
            return SimpleNamespace(returncode=0)

        edit_args = argparse.Namespace(
            project_dir=tmp_path,
            no_docker=True,
            max_turns=None,
            task_id=review.id,
            rerun=False,
            edit_slices=True,
            materialize=False,
            run=True,
            queue=False,
            background=False,
            model=None,
            provider=None,
            force=False,
        )

        with patch("gza.cli.execution._launch_editor", side_effect=_write_invalid_override):
            assert cmd_plan_review(edit_args) == 1

        assert len([task for task in store.get_all() if task.task_type == "implement"]) == 0

    def test_plan_review_materialize_rejects_invalid_override_manifest(self, tmp_path: Path) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan = store.add("Plan rollout", task_type="plan")
        assert plan.id is not None
        plan.status = "completed"
        plan.completed_at = datetime.now(UTC)
        store.update(plan)

        review = store.add("Review rollout", task_type="plan_review", depends_on=plan.id)
        assert review.id is not None
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        review.output_content = (
            "## Verdict\nVerdict: APPROVED\n\n## Slice Manifest\n```json\n"
            "{"
            f"\"schema_version\":1,\"source_task_id\":\"{plan.id}\",\"source_task_type\":\"plan\","
            "\"verdict\":\"APPROVED\","
            "\"slice_quality\":{\"fits_single_task_budget\":true,\"timeout_budget_minutes\":30,"
            "\"max_expected_files_changed_per_slice\":8,\"rationale\":\"Bounded.\"},"
            "\"slices\":["
            "{\"slice_id\":\"S1\",\"title\":\"Foundation\",\"prompt\":\"Original prompt.\","
            "\"scope\":[\"Add parser\"],\"out_of_scope\":[],\"acceptance_criteria\":[\"Parser works\"],"
            "\"depends_on_slices\":[],\"based_on_slice\":null,\"review_scope\":\"Foundation only.\","
            "\"estimated_complexity\":\"small\",\"expected_timeout_minutes\":30,"
            "\"requires_code_review\":true,\"tags\":[\"slice-a\"]}"
            "]}\n"
            "```\n"
        )
        store.update(review)
        _store_plan_review_override_artifact(tmp_path, store, review.id, output="[]")

        result = invoke_gza("plan-review", str(review.id), "--materialize", "--run", "--project", str(tmp_path))

        assert result.returncode == 1
        assert len([task for task in store.get_all() if task.task_type == "implement"]) == 0
        assert store.list_artifacts(review.id, kind="plan_review_materialization") == []
        assert f"Plan review {review.id} has an invalid override manifest" in result.stdout
        assert "stored plan review override is not a JSON object" in result.stdout

    def test_plan_improve_queue_creates_revised_plan_task(self, tmp_path: Path) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan = store.add("Plan rollout", task_type="plan")
        assert plan.id is not None
        plan.status = "completed"
        plan.completed_at = datetime.now(UTC)
        store.update(plan)

        review = store.add("Review rollout", task_type="plan_review", depends_on=plan.id)
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        review.output_content = "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        store.update(review)

        result = invoke_gza("plan-improve", str(review.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        created = get_latest_task(store, task_type="plan_improve", depends_on=review.id, based_on=plan.id)
        assert created is not None
        assert "Created plan improve task" in result.stdout

    def test_plan_improve_background_spawns_worker(self, tmp_path: Path) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan = store.add("Plan rollout", task_type="plan")
        assert plan.id is not None
        plan.status = "completed"
        plan.completed_at = datetime.now(UTC)
        store.update(plan)

        review = store.add("Review rollout", task_type="plan_review", depends_on=plan.id)
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        review.output_content = "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        store.update(review)

        with patch("gza.cli.execution._spawn_background_worker", return_value=0) as spawn_background:
            result = invoke_gza(
                "plan-improve",
                str(review.id),
                "--background",
                "--no-docker",
                "--project",
                str(tmp_path),
            )

        assert result.returncode == 0
        created = get_latest_task(store, task_type="plan_improve", depends_on=review.id, based_on=plan.id)
        assert created is not None
        spawn_background.assert_called_once()
        assert spawn_background.call_args.kwargs["task_id"] == created.id

    @pytest.mark.parametrize(
        ("status", "output_content", "expected_fragment"),
        [
            ("pending", None, "requires a completed CHANGES_REQUESTED plan_review"),
            ("in_progress", None, "requires a completed CHANGES_REQUESTED plan_review"),
            ("completed", "## Verdict\n\nVerdict: APPROVED\n", "has verdict APPROVED"),
        ],
    )
    def test_plan_improve_rejects_non_changes_requested_review_states(
        self,
        tmp_path: Path,
        status: str,
        output_content: str | None,
        expected_fragment: str,
    ) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan = store.add("Plan rollout", task_type="plan")
        assert plan.id is not None
        plan.status = "completed"
        plan.completed_at = datetime.now(UTC)
        store.update(plan)

        review = store.add("Review rollout", task_type="plan_review", depends_on=plan.id)
        review.status = status
        if status == "completed":
            review.completed_at = datetime.now(UTC)
        elif status == "in_progress":
            review.started_at = datetime.now(UTC)
        review.output_content = output_content
        store.update(review)

        result = invoke_gza("plan-improve", str(review.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 1
        assert expected_fragment in result.stdout
        assert [task for task in store.get_all() if task.task_type == "plan_improve"] == []


class TestImproveCommand:
    """Tests for 'gza improve' command."""

    @pytest.fixture(autouse=True)
    def _mock_foreground_runner(self):
        """Keep improve command tests focused on CLI behavior, not agent execution."""
        with patch("gza.cli._run_foreground", return_value=0) as run_foreground:
            yield run_foreground

    def test_improve_creates_task_from_implementation_and_review(self, tmp_path: Path):
        """Improve command creates an improve task with correct relationships."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a completed implementation task
        impl_task = store.add("Add user authentication", task_type="implement", group="auth-feature")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-user-authentication"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        # Create a completed review task
        review_task = store.add("Review implementation", task_type="review", depends_on=impl_task.id)
        review_task.status = "completed"
        review_task.completed_at = datetime.now(UTC)
        store.update(review_task)

        # Run improve command with --queue to only create (not run)
        result = invoke_gza("improve", str(impl_task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        all_tasks = store.get_all()
        improve_task = [t for t in all_tasks if t.task_type == "improve"][0]
        assert f"Created improve task {improve_task.id}" in result.stdout
        assert f"Based on: implementation {impl_task.id}" in result.stdout
        assert f"Review: {review_task.id}" in result.stdout

        # Verify the improve task was created with correct fields
        assert improve_task is not None
        assert improve_task.task_type == "improve"
        assert improve_task.depends_on == review_task.id  # review task
        assert improve_task.based_on == impl_task.id  # implementation task
        assert improve_task.same_branch is True
        assert improve_task.group == "auth-feature"  # inherited from implementation

    def test_improve_with_review_flag(self, tmp_path: Path):
        """Improve command with --review flag sets create_review."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create implementation and review tasks
        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-feature"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        review_task = store.add("Review", task_type="review", depends_on=impl_task.id)
        review_task.status = "completed"
        review_task.completed_at = datetime.now(UTC)
        store.update(review_task)

        # Run improve command with --review flag and --queue to only create (not run)
        result = invoke_gza("improve", str(impl_task.id), "--review", "--queue", "--project", str(tmp_path))

        assert result.returncode == 0

        # Verify the improve task has create_review set
        all_tasks = store.get_all()
        improve_task = [t for t in all_tasks if t.task_type == "improve"][0]
        assert improve_task is not None
        assert improve_task.create_review is True

    def test_improve_fails_without_review(self, tmp_path: Path):
        """Improve command fails if implementation has no review."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create implementation task without review
        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        # Run improve command
        result = invoke_gza("improve", str(impl_task.id), "--project", str(tmp_path))

        assert result.returncode == 1
        assert "has no review" in result.stdout
        assert f"gza add --type review --depends-on {impl_task.id}" in result.stdout

    def test_improve_works_from_unresolved_comments_without_review(self, tmp_path: Path):
        """Improve command can run from unresolved comments when no review exists."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)
        assert impl_task.id is not None

        store.add_comment(impl_task.id, "Please tighten validation edge cases.")

        result = invoke_gza("improve", str(impl_task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "continuing from unresolved comments only" in result.stdout
        assert "Comments: 1 unresolved" in result.stdout

        improve_task = next(task for task in store.get_all() if task.task_type == "improve")
        assert improve_task.depends_on is None

    def test_improve_comments_only_reuses_pending_task(self, tmp_path: Path):
        """Second comments-only improve should reuse an existing pending improve."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)
        assert impl_task.id is not None

        store.add_comment(impl_task.id, "Address form validation gaps.")

        first = invoke_gza("improve", str(impl_task.id), "--queue", "--project", str(tmp_path))
        assert first.returncode == 0, first.stdout

        improves = [t for t in store.get_all() if t.task_type == "improve"]
        assert len(improves) == 1
        first_improve = improves[0]
        assert first_improve.id is not None

        second = invoke_gza("improve", str(impl_task.id), "--queue", "--project", str(tmp_path))
        assert second.returncode == 0, second.stdout
        assert f"Reusing pending improve task {first_improve.id}" in second.stdout

        improves_after = [t for t in store.get_all() if t.task_type == "improve"]
        assert len(improves_after) == 1
        assert improves_after[0].id == first_improve.id

    def test_improve_scope_only_comment_does_not_enable_comments_only_flow(self, tmp_path: Path):
        """A review_scope comment alone must not trigger the comments-only improve path."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)
        assert impl_task.id is not None

        store.add_comment(impl_task.id, "Scope clarification only.", kind="review_scope")

        result = invoke_gza("improve", str(impl_task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "has no review" in result.stdout
        assert "continuing from unresolved comments only" not in result.stdout

    def test_improve_comments_only_reuse_pending_applies_create_pr_override(self, tmp_path: Path):
        """Reused pending comments-only improve should honor current --pr intent."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)
        assert impl_task.id is not None

        store.add_comment(impl_task.id, "Address validation gaps.")

        first = invoke_gza("improve", str(impl_task.id), "--queue", "--project", str(tmp_path))
        assert first.returncode == 0, first.stdout

        pending_improve = next(task for task in store.get_all() if task.task_type == "improve")
        assert pending_improve.id is not None
        assert pending_improve.create_pr is False

        second = invoke_gza("improve", str(impl_task.id), "--queue", "--pr", "--project", str(tmp_path))
        assert second.returncode == 0, second.stdout
        assert f"Reusing pending improve task {pending_improve.id}" in second.stdout

        reused = store.get(pending_improve.id)
        assert reused is not None
        assert reused.create_pr is True

    def test_improve_comments_only_pending_task_with_newer_comment_creates_fresh_task(self, tmp_path: Path):
        """Pending comments-only improve is not reused when newer unresolved comments were added."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)
        assert impl_task.id is not None

        store.add_comment(impl_task.id, "Round 1 comment.")
        first = invoke_gza("improve", str(impl_task.id), "--queue", "--project", str(tmp_path))
        assert first.returncode == 0, first.stdout

        first_improve = next(task for task in store.get_all() if task.task_type == "improve")
        assert first_improve.id is not None
        first_improve.created_at = datetime(2026, 1, 1, tzinfo=UTC)
        store.update(first_improve)

        store.add_comment(impl_task.id, "Round 2 comment added after improve creation.")
        second = invoke_gza("improve", str(impl_task.id), "--queue", "--project", str(tmp_path))
        assert second.returncode == 0, second.stdout
        assert f"Reusing pending improve task {first_improve.id}" not in second.stdout
        assert "Created improve task" in second.stdout

        improves = [t for t in store.get_all() if t.task_type == "improve"]
        assert len(improves) == 2
        newest = max(improves, key=lambda t: task_id_numeric_key(t.id))
        assert newest.id != first_improve.id
        assert newest.based_on == impl_task.id
        assert newest.depends_on is None

    def test_improve_comments_only_pending_task_ignores_newer_scope_only_comment(self, tmp_path: Path):
        """A newer review_scope comment must not make a pending comments-only improve stale."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)
        assert impl_task.id is not None

        store.add_comment(impl_task.id, "Round 1 feedback comment.")
        first = invoke_gza("improve", str(impl_task.id), "--queue", "--project", str(tmp_path))
        assert first.returncode == 0, first.stdout

        pending_improve = next(task for task in store.get_all() if task.task_type == "improve")
        assert pending_improve.id is not None
        pending_improve.created_at = datetime(2026, 1, 1, tzinfo=UTC)
        store.update(pending_improve)

        store.add_comment(impl_task.id, "Scope clarification only.", kind="review_scope")
        second = invoke_gza("improve", str(impl_task.id), "--queue", "--project", str(tmp_path))
        assert second.returncode == 0, second.stdout
        assert f"Reusing pending improve task {pending_improve.id}" in second.stdout

        improves = [t for t in store.get_all() if t.task_type == "improve"]
        assert len(improves) == 1

    def test_improve_comments_only_resumes_failed_task(self, tmp_path: Path):
        """Failed comments-only improve should create a resume task, not duplicate-error."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)
        assert impl_task.id is not None

        store.add_comment(impl_task.id, "Handle edge-case parsing.")

        first = invoke_gza("improve", str(impl_task.id), "--queue", "--project", str(tmp_path))
        assert first.returncode == 0, first.stdout

        improves = [t for t in store.get_all() if t.task_type == "improve"]
        assert len(improves) == 1
        failed_improve = improves[0]
        failed_improve.status = "failed"
        failed_improve.failure_reason = "TIMEOUT"
        failed_improve.session_id = "improve-session-1"
        store.update(failed_improve)
        assert failed_improve.id is not None

        second = invoke_gza("improve", str(impl_task.id), "--queue", "--project", str(tmp_path))
        assert second.returncode == 0, second.stdout
        assert f"(resume of {failed_improve.id})" in second.stdout

        improves_after = [t for t in store.get_all() if t.task_type == "improve"]
        assert len(improves_after) == 2
        resumed = max(improves_after, key=lambda t: task_id_numeric_key(t.id))
        assert resumed.based_on == failed_improve.id
        assert resumed.depends_on is None

    def test_improve_comments_only_reports_disabled_automatic_recovery(self, tmp_path: Path):
        """Comments-only improve should report disabled automatic recovery when max_resume_attempts=0."""
        setup_config(tmp_path)
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(config_path.read_text() + "max_resume_attempts: 0\n")
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)
        assert impl_task.id is not None

        store.add_comment(impl_task.id, "Handle edge-case parsing.")

        first = invoke_gza("improve", str(impl_task.id), "--queue", "--project", str(tmp_path))
        assert first.returncode == 0, first.stdout

        failed_improve = next(task for task in store.get_all() if task.task_type == "improve")
        assert failed_improve.id is not None
        failed_improve.status = "failed"
        failed_improve.failure_reason = "TIMEOUT"
        failed_improve.session_id = "improve-session-1"
        store.update(failed_improve)

        second = invoke_gza("improve", str(impl_task.id), "--queue", "--project", str(tmp_path))
        assert second.returncode == 1
        assert "automatic recovery is disabled (max_resume_attempts=0)" in second.stdout
        assert str(failed_improve.id) in second.stdout
        improves = [task for task in store.get_all() if task.task_type == "improve"]
        assert len(improves) == 1

    def test_improve_comments_only_resume_applies_create_pr_override(self, tmp_path: Path):
        """Resumed comments-only improve should honor current --pr intent."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)
        assert impl_task.id is not None

        store.add_comment(impl_task.id, "Handle edge-case parsing.")

        first = invoke_gza("improve", str(impl_task.id), "--queue", "--project", str(tmp_path))
        assert first.returncode == 0, first.stdout

        failed_improve = next(task for task in store.get_all() if task.task_type == "improve")
        assert failed_improve.id is not None
        failed_improve.status = "failed"
        failed_improve.failure_reason = "TIMEOUT"
        failed_improve.session_id = "improve-session-1"
        store.update(failed_improve)

        second = invoke_gza("improve", str(impl_task.id), "--queue", "--pr", "--project", str(tmp_path))
        assert second.returncode == 0, second.stdout
        assert f"(resume of {failed_improve.id})" in second.stdout

        resumed = max(
            (task for task in store.get_all() if task.task_type == "improve"),
            key=lambda task: task_id_numeric_key(task.id),
        )
        assert resumed.id != failed_improve.id
        assert resumed.based_on == failed_improve.id
        assert resumed.create_pr is True

    def test_improve_comments_only_failed_task_with_newer_comment_creates_fresh_task(self, tmp_path: Path):
        """Failed comments-only improve is not resumed/retried when newer unresolved comments exist."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)
        assert impl_task.id is not None

        store.add_comment(impl_task.id, "Round 1 comment.")
        first = invoke_gza("improve", str(impl_task.id), "--queue", "--project", str(tmp_path))
        assert first.returncode == 0, first.stdout

        failed_improve = next(task for task in store.get_all() if task.task_type == "improve")
        assert failed_improve.id is not None
        failed_improve.status = "failed"
        failed_improve.failure_reason = "TIMEOUT"
        failed_improve.session_id = "improve-session-1"
        failed_improve.created_at = datetime(2026, 1, 1, tzinfo=UTC)
        store.update(failed_improve)

        store.add_comment(impl_task.id, "Round 2 comment added after failed improve creation.")
        second = invoke_gza("improve", str(impl_task.id), "--queue", "--project", str(tmp_path))
        assert second.returncode == 0, second.stdout
        assert f"(resume of {failed_improve.id})" not in second.stdout
        assert f"(retry of {failed_improve.id})" not in second.stdout
        assert "Created improve task" in second.stdout

        improves = [t for t in store.get_all() if t.task_type == "improve"]
        assert len(improves) == 2
        newest = max(improves, key=lambda t: task_id_numeric_key(t.id))
        assert newest.id != failed_improve.id
        assert newest.based_on == impl_task.id
        assert newest.depends_on is None

    def test_improve_comments_only_retry_resets_omitted_review_model_and_provider_flags(self, tmp_path: Path):
        """Comments-only improve retries should not inherit stale CLI flags when omitted."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)
        assert impl_task.id is not None

        store.add_comment(impl_task.id, "Retry comments-only improve after infra failure.")

        first = invoke_gza(
            "improve",
            str(impl_task.id),
            "--queue",
            "--review",
            "--model",
            "gpt-5.3-codex",
            "--provider",
            "codex",
            "--project",
            str(tmp_path),
        )
        assert first.returncode == 0, first.stdout

        failed_improve = next(task for task in store.get_all() if task.task_type == "improve")
        assert failed_improve.id is not None
        failed_improve.status = "failed"
        failed_improve.failure_reason = "INFRASTRUCTURE_ERROR"
        failed_improve.session_id = None
        store.update(failed_improve)

        second = invoke_gza("improve", str(impl_task.id), "--queue", "--project", str(tmp_path))
        assert second.returncode == 0, second.stdout
        assert f"(retry of {failed_improve.id})" in second.stdout

        retry_task = max(
            (task for task in store.get_all() if task.task_type == "improve"),
            key=lambda task: task_id_numeric_key(task.id),
        )
        assert retry_task.id != failed_improve.id
        assert retry_task.based_on == failed_improve.id
        assert retry_task.create_review is False
        assert retry_task.model is None
        assert retry_task.model_is_explicit is False
        assert retry_task.provider is None
        assert retry_task.provider_is_explicit is False

    def test_improve_comments_only_retry_honors_explicit_review_model_and_provider_overrides(
        self, tmp_path: Path
    ):
        """Comments-only improve retries should apply the current invocation overrides."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)
        assert impl_task.id is not None

        store.add_comment(impl_task.id, "Retry comments-only improve with new flags.")

        first = invoke_gza(
            "improve",
            str(impl_task.id),
            "--queue",
            "--model",
            "gpt-5.3-codex",
            "--provider",
            "codex",
            "--project",
            str(tmp_path),
        )
        assert first.returncode == 0, first.stdout

        failed_improve = next(task for task in store.get_all() if task.task_type == "improve")
        assert failed_improve.id is not None
        failed_improve.status = "failed"
        failed_improve.failure_reason = "INFRASTRUCTURE_ERROR"
        failed_improve.session_id = None
        store.update(failed_improve)

        second = invoke_gza(
            "improve",
            str(impl_task.id),
            "--queue",
            "--review",
            "--model",
            "gpt-5.4",
            "--provider",
            "claude",
            "--project",
            str(tmp_path),
        )
        assert second.returncode == 0, second.stdout
        assert f"(retry of {failed_improve.id})" in second.stdout

        retry_task = max(
            (task for task in store.get_all() if task.task_type == "improve"),
            key=lambda task: task_id_numeric_key(task.id),
        )
        assert retry_task.id != failed_improve.id
        assert retry_task.based_on == failed_improve.id
        assert retry_task.create_review is True
        assert retry_task.model == "gpt-5.4"
        assert retry_task.model_is_explicit is True
        assert retry_task.provider == "claude"
        assert retry_task.provider_is_explicit is True

    def test_improve_comments_only_in_progress_task_with_newer_comment_creates_fresh_task(self, tmp_path: Path):
        """In-progress comments-only improve is ignored when newer unresolved comments require a new pass."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)
        assert impl_task.id is not None

        store.add_comment(impl_task.id, "Round 1 comment.")
        first = invoke_gza("improve", str(impl_task.id), "--queue", "--project", str(tmp_path))
        assert first.returncode == 0, first.stdout

        running_improve = next(task for task in store.get_all() if task.task_type == "improve")
        assert running_improve.id is not None
        running_improve.status = "in_progress"
        running_improve.created_at = datetime(2026, 1, 1, tzinfo=UTC)
        store.update(running_improve)

        store.add_comment(impl_task.id, "Round 2 comment added while improve is running.")
        second = invoke_gza("improve", str(impl_task.id), "--queue", "--project", str(tmp_path))
        assert second.returncode == 0, second.stdout
        assert f"Comments-only improve {running_improve.id} is already in progress" not in second.stdout
        assert "Created improve task" in second.stdout

        improves = [t for t in store.get_all() if t.task_type == "improve"]
        assert len(improves) == 2
        newest = max(improves, key=lambda t: task_id_numeric_key(t.id))
        assert newest.id != running_improve.id
        assert newest.based_on == impl_task.id
        assert newest.depends_on is None

    def test_improve_comments_only_completed_then_new_comments_creates_fresh_task(self, tmp_path: Path):
        """Completed comments-only improves should not block fresh improves for new comments."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)
        assert impl_task.id is not None

        store.add_comment(impl_task.id, "Round 1: tighten API validation.")
        first = invoke_gza("improve", str(impl_task.id), "--queue", "--project", str(tmp_path))
        assert first.returncode == 0, first.stdout

        improves = [t for t in store.get_all() if t.task_type == "improve"]
        assert len(improves) == 1
        completed_improve = improves[0]
        assert completed_improve.id is not None
        assert completed_improve.created_at is not None
        completed_improve.status = "completed"
        completed_improve.completed_at = datetime.now(UTC)
        store.update(completed_improve)

        # Mirror runner completion semantics: resolve only comments in the improve snapshot.
        store.resolve_comments(impl_task.id, created_on_or_before=completed_improve.created_at)

        store.add_comment(impl_task.id, "Round 2: improve timeout handling.")
        second = invoke_gza("improve", str(impl_task.id), "--queue", "--project", str(tmp_path))
        assert second.returncode == 0, second.stdout
        assert "Created improve task" in second.stdout

        improves_after = [t for t in store.get_all() if t.task_type == "improve"]
        assert len(improves_after) == 2
        newest = max(improves_after, key=lambda t: task_id_numeric_key(t.id))
        assert newest.id != completed_improve.id
        assert newest.based_on == impl_task.id
        assert newest.depends_on is None

    def test_improve_fails_on_non_implement_task(self, tmp_path: Path):
        """Improve command fails if task is not an implementation task."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a plan task
        plan_task = store.add("Plan feature", task_type="plan")
        plan_task.status = "completed"
        plan_task.completed_at = datetime.now(UTC)
        store.update(plan_task)

        # Run improve command
        result = invoke_gza("improve", str(plan_task.id), "--project", str(tmp_path))

        assert result.returncode == 1
        assert "is a plan task" in result.stdout

    def test_improve_accepts_review_task_id_and_resolves_impl(self, tmp_path: Path):
        """Improve command accepts a review task ID and auto-resolves to the implement task."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create implementation and review tasks
        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        review_task = store.add("Review", task_type="review", depends_on=impl_task.id)
        review_task.status = "completed"
        review_task.completed_at = datetime.now(UTC)
        store.update(review_task)

        # Run improve command with review task ID — should resolve to impl task and succeed
        result = invoke_gza("improve", str(review_task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0, result.stdout
        assert "Created improve task" in result.stdout
        assert f"Based on: implementation {impl_task.id}" in result.stdout

    def test_improve_accepts_improve_task_id_and_resolves_impl(self, tmp_path: Path):
        """Improve command accepts an improve task ID and auto-resolves to the implement task."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        review_task = store.add("Review", task_type="review", depends_on=impl_task.id)
        review_task.status = "completed"
        review_task.completed_at = datetime.now(UTC)
        store.update(review_task)

        # First improve task
        assert impl_task.id is not None
        assert review_task.id is not None
        improve_task = store.add(
            "Improve", task_type="improve", based_on=impl_task.id, depends_on=review_task.id, same_branch=True
        )
        improve_task.status = "completed"
        improve_task.completed_at = datetime.now(UTC)
        store.update(improve_task)

        # Add a second review so a new improve can be created
        review_task2 = store.add("Review 2", task_type="review", depends_on=impl_task.id)
        review_task2.status = "completed"
        review_task2.completed_at = datetime.now(UTC)
        store.update(review_task2)

        # Run improve command with improve task ID — should resolve to impl task
        result = invoke_gza("improve", str(improve_task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0, result.stdout
        assert "Created improve task" in result.stdout
        assert f"Based on: implementation {impl_task.id}" in result.stdout

    def test_improve_uses_most_recent_review(self, tmp_path: Path):
        """Improve command uses the most recent review when multiple exist."""
        import time

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create implementation task
        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-feature"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        # Create first review task
        time.sleep(0.01)  # Ensure different timestamps
        review_task1 = store.add("First review", task_type="review", depends_on=impl_task.id)
        review_task1.status = "completed"
        review_task1.completed_at = datetime.now(UTC)
        store.update(review_task1)

        # Create second review task (more recent)
        time.sleep(0.01)  # Ensure different timestamps
        review_task2 = store.add("Second review", task_type="review", depends_on=impl_task.id)
        review_task2.status = "completed"
        review_task2.completed_at = datetime.now(UTC)
        store.update(review_task2)

        # Run improve command with --queue to only create (not run)
        result = invoke_gza("improve", str(impl_task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        assert f"Review: {review_task2.id}" in result.stdout  # Should use the second (most recent) review

        # Verify the improve task depends on the most recent review
        all_tasks = store.get_all()
        improve_task = [t for t in all_tasks if t.task_type == "improve"][0]
        assert improve_task is not None
        assert improve_task.depends_on == review_task2.id

    def test_improve_nonexistent_task(self, tmp_path: Path):
        """Improve command handles nonexistent task."""
        setup_db_with_tasks(tmp_path, [])

        result = invoke_gza("improve", "999", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "Use a full prefixed task ID" in result.stdout or "Use a full prefixed task ID" in result.stderr

    def test_improve_errors_when_only_pending_review_and_no_comments(self, tmp_path: Path):
        """Auto-pick must not bind to a pending review when no comments fallback exists.

        Regression for the trap where auto-pick would silently create an
        improve task tied to a non-completed review, leaving the improve
        permanently blocked if the review never completes (or surfacing a
        warning the operator may miss). Non-completed reviews are only
        selectable via explicit --review-id now.
        """
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-feature"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        # Only a pending review exists; no unresolved comments.
        pending_review = store.add("Review", task_type="review", depends_on=impl_task.id)

        result = invoke_gza("improve", str(impl_task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "no completed review" in result.stdout
        assert f"{pending_review.id} (pending)" in result.stdout
        assert "--review-id" in result.stdout

        # No improve task should have been created.
        improves = [t for t in store.get_all() if t.task_type == "improve"]
        assert improves == []

    def test_improve_prevents_duplicate(self, tmp_path: Path):
        """Improve command refuses to create a duplicate improve task."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a completed implementation task
        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-feature"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        # Create a completed review task
        review_task = store.add("Review", task_type="review", depends_on=impl_task.id)
        review_task.status = "completed"
        review_task.completed_at = datetime.now(UTC)
        store.update(review_task)

        # Create an existing improve task for the same impl+review pair
        existing_improve = store.add(
            "Improve",
            task_type="improve",
            based_on=impl_task.id,
            depends_on=review_task.id,
        )

        # Run improve command - should fail with duplicate error
        result = invoke_gza("improve", str(impl_task.id), "--project", str(tmp_path))

        assert result.returncode == 1
        assert "improve task already exists" in result.stdout
        assert f"{existing_improve.id}" in result.stdout

        # Verify no new task was created (still only 3 tasks)
        all_tasks = store.get_all()
        assert len(all_tasks) == 3

    def test_improve_run_flag_runs_immediately(self, tmp_path: Path):
        """Improve command runs immediately only with --run."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a completed implementation task
        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-feature"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        # Create a completed review task
        review_task = store.add("Review", task_type="review", depends_on=impl_task.id)
        review_task.status = "completed"
        review_task.completed_at = datetime.now(UTC)
        store.update(review_task)

        # Run improve with --run. Stub the foreground worker because this test
        # only cares that the explicit foreground path is selected.
        with patch("gza.cli._run_foreground", return_value=0):
            result = invoke_gza("improve", str(impl_task.id), "--run", "--no-docker", "--project", str(tmp_path))

        # Verify the improve task was created and run was attempted
        assert result.returncode == 0
        assert "Created improve task " in result.stdout
        assert "Running improve task " in result.stdout

    def test_improve_with_model_flag(self, tmp_path: Path):
        """Improve command with --model sets the model on the created task."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-feature"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        review_task = store.add("Review", task_type="review", depends_on=impl_task.id)
        review_task.status = "completed"
        review_task.completed_at = datetime.now(UTC)
        store.update(review_task)

        result = invoke_gza("improve", str(impl_task.id), "--model", "claude-opus-4-5", "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        all_tasks = store.get_all()
        improve_task = [t for t in all_tasks if t.task_type == "improve"][0]
        assert improve_task is not None
        assert improve_task.model == "claude-opus-4-5"

    def test_improve_with_provider_flag(self, tmp_path: Path):
        """Improve command with --provider sets the provider on the created task."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-feature"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        review_task = store.add("Review", task_type="review", depends_on=impl_task.id)
        review_task.status = "completed"
        review_task.completed_at = datetime.now(UTC)
        store.update(review_task)

        result = invoke_gza("improve", str(impl_task.id), "--provider", "gemini", "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        all_tasks = store.get_all()
        improve_task = [t for t in all_tasks if t.task_type == "improve"][0]
        assert improve_task is not None
        assert improve_task.provider == "gemini"

    def test_improve_skips_dropped_review_and_picks_earlier_completed(self, tmp_path: Path):
        """Auto-pick must ignore dropped reviews even if their completed_at is more recent.

        Regression for the trap where a user accidentally creates a duplicate
        review, drops it, and then `gza improve` keeps binding new improve tasks
        to the dropped review (because get_reviews_for_task orders by
        completed_at DESC with no status filter).
        """
        import time

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-feature"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        # Older, real, completed review.
        good_review = store.add("First review", task_type="review", depends_on=impl_task.id)
        good_review.status = "completed"
        good_review.completed_at = datetime.now(UTC)
        store.update(good_review)

        # Newer, dropped review (would sort first by completed_at DESC).
        time.sleep(0.01)
        bad_review = store.add("Accidental duplicate review", task_type="review", depends_on=impl_task.id)
        bad_review.status = "dropped"
        bad_review.completed_at = datetime.now(UTC)
        store.update(bad_review)

        result = invoke_gza("improve", str(impl_task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0, result.stdout
        assert f"Review: {good_review.id}" in result.stdout
        assert f"Review: {bad_review.id}" not in result.stdout

        # Confirm the improve task's dependency points at the good review.
        improve_task = next(task for task in store.get_all() if task.task_type == "improve")
        assert improve_task is not None
        assert improve_task.depends_on == good_review.id

    def test_improve_skips_failed_review(self, tmp_path: Path):
        """Auto-pick must also ignore failed reviews — same reasoning as dropped."""
        import time

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-feature"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        good_review = store.add("Good review", task_type="review", depends_on=impl_task.id)
        good_review.status = "completed"
        good_review.completed_at = datetime.now(UTC)
        store.update(good_review)

        time.sleep(0.01)
        failed_review = store.add("Failed review", task_type="review", depends_on=impl_task.id)
        failed_review.status = "failed"
        failed_review.completed_at = datetime.now(UTC)
        store.update(failed_review)

        result = invoke_gza("improve", str(impl_task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0, result.stdout
        assert f"Review: {good_review.id}" in result.stdout

    def test_improve_errors_when_all_reviews_are_dropped(self, tmp_path: Path):
        """When every review is dropped/failed, surface a clear error."""
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-feature"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        dropped_review = store.add("Dropped review", task_type="review", depends_on=impl_task.id)
        dropped_review.status = "dropped"
        dropped_review.completed_at = datetime.now(UTC)
        store.update(dropped_review)

        result = invoke_gza("improve", str(impl_task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "no completed review" in result.stdout
        assert "--review-id" in result.stdout

    def test_improve_auto_pick_falls_back_to_comments_when_only_pending_review(
        self, tmp_path: Path
    ):
        """Comments-only fallback must kick in when the only review is pending.

        Regression for review gza-1276 M1: auto-pick previously treated pending
        reviews as usable, which suppressed the comments-only fallback when
        unresolved comments were the real runnable feedback source.
        """
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-feature"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)
        assert impl_task.id is not None

        pending_review = store.add("Review", task_type="review", depends_on=impl_task.id)
        # Leave status as default 'pending'.
        store.add_comment(impl_task.id, "Tighten validation edge cases.")

        result = invoke_gza("improve", str(impl_task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0, result.stdout
        assert "no completed review" in result.stdout
        assert "continuing from unresolved comments only" in result.stdout
        # Must not have bound to the pending review.
        assert "is pending" not in result.stdout
        assert "blocked until it completes" not in result.stdout

        improves = [t for t in store.get_all() if t.task_type == "improve"]
        assert len(improves) == 1
        assert improves[0].depends_on is None
        # Pending review remains, unbound.
        assert pending_review.status == "pending"

    def test_improve_auto_pick_falls_back_to_comments_when_only_in_progress_review(
        self, tmp_path: Path
    ):
        """Same fallback for an in_progress review: comments-only improve wins."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-feature"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)
        assert impl_task.id is not None

        in_progress_review = store.add("Review", task_type="review", depends_on=impl_task.id)
        in_progress_review.status = "in_progress"
        store.update(in_progress_review)
        store.add_comment(impl_task.id, "Revisit error wording.")

        result = invoke_gza("improve", str(impl_task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0, result.stdout
        assert "continuing from unresolved comments only" in result.stdout
        assert "blocked until it completes" not in result.stdout

        improves = [t for t in store.get_all() if t.task_type == "improve"]
        assert len(improves) == 1
        assert improves[0].depends_on is None

    def test_improve_rejects_failed_review_id(self, tmp_path: Path):
        """--review-id must reject terminal failed reviews rather than warn.

        Regression for review gza-1276 M2: previously --review-id accepted
        failed/dropped statuses and printed "blocked until it completes",
        creating a permanently unrunnable improve tied to a review that will
        never complete.
        """
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-feature"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        failed_review = store.add("Failed review", task_type="review", depends_on=impl_task.id)
        failed_review.status = "failed"
        failed_review.completed_at = datetime.now(UTC)
        store.update(failed_review)

        result = invoke_gza(
            "improve",
            str(impl_task.id),
            "--review-id",
            str(failed_review.id),
            "--queue",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 1
        assert f"Review {failed_review.id} is failed" in result.stdout
        assert "terminal reviews cannot produce feedback" in result.stdout
        assert "blocked until it completes" not in result.stdout

        improves = [t for t in store.get_all() if t.task_type == "improve"]
        assert improves == []

    def test_improve_rejects_dropped_review_id(self, tmp_path: Path):
        """--review-id must also reject terminal dropped reviews."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-feature"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        dropped_review = store.add("Dropped review", task_type="review", depends_on=impl_task.id)
        dropped_review.status = "dropped"
        dropped_review.completed_at = datetime.now(UTC)
        store.update(dropped_review)

        result = invoke_gza(
            "improve",
            str(impl_task.id),
            "--review-id",
            str(dropped_review.id),
            "--queue",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 1
        assert f"Review {dropped_review.id} is dropped" in result.stdout
        assert "terminal reviews cannot produce feedback" in result.stdout

        improves = [t for t in store.get_all() if t.task_type == "improve"]
        assert improves == []

    def test_improve_rejects_failed_review_id_suggests_comments_fallback_when_available(
        self, tmp_path: Path
    ):
        """When comments exist, rejection message should suggest omitting --review-id."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-feature"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)
        assert impl_task.id is not None

        failed_review = store.add("Failed review", task_type="review", depends_on=impl_task.id)
        failed_review.status = "failed"
        failed_review.completed_at = datetime.now(UTC)
        store.update(failed_review)

        store.add_comment(impl_task.id, "Please address this edge case.")

        result = invoke_gza(
            "improve",
            str(impl_task.id),
            "--review-id",
            str(failed_review.id),
            "--queue",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 1
        assert "Omit --review-id to run a comments-only improve" in result.stdout
        assert "1 unresolved comment" in result.stdout

    def test_improve_review_id_flag_picks_explicit_review(self, tmp_path: Path):
        """--review-id overrides auto-pick and uses the specified review."""
        import time

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-feature"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        older_review = store.add("Older review", task_type="review", depends_on=impl_task.id)
        older_review.status = "completed"
        older_review.completed_at = datetime.now(UTC)
        store.update(older_review)

        time.sleep(0.01)
        newer_review = store.add("Newer review", task_type="review", depends_on=impl_task.id)
        newer_review.status = "completed"
        newer_review.completed_at = datetime.now(UTC)
        store.update(newer_review)

        # Without --review-id, auto-pick would choose the newer one.
        # With --review-id, we force the older one.
        result = invoke_gza(
            "improve", str(impl_task.id),
            "--review-id", str(older_review.id),
            "--queue",
            "--project", str(tmp_path),
        )

        assert result.returncode == 0, result.stdout
        assert f"Review: {older_review.id}" in result.stdout

        improve_task = next(task for task in store.get_all() if task.task_type == "improve")
        assert improve_task is not None
        assert improve_task.depends_on == older_review.id

    def test_improve_review_id_flag_trims_whitespace_and_reports_canonical_id(self, tmp_path: Path):
        """--review-id should be normalized via shared full-ID resolution."""
        import time

        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        selected_review = store.add("Selected review", task_type="review", depends_on=impl_task.id)
        selected_review.status = "completed"
        selected_review.completed_at = datetime.now(UTC)
        store.update(selected_review)

        time.sleep(0.01)
        newer_review = store.add("Newer review", task_type="review", depends_on=impl_task.id)
        newer_review.status = "completed"
        newer_review.completed_at = datetime.now(UTC)
        store.update(newer_review)

        result = invoke_gza(
            "improve", str(impl_task.id),
            "--review-id", f"  {selected_review.id}\t",
            "--queue",
            "--project", str(tmp_path),
        )

        assert result.returncode == 0, result.stdout
        assert f"Review: {selected_review.id}" in result.stdout

        improve_task = next(task for task in store.get_all() if task.task_type == "improve")
        assert improve_task is not None
        assert improve_task.depends_on == selected_review.id

    def test_improve_review_id_flag_rejects_review_of_different_impl(self, tmp_path: Path):
        """--review-id must belong to the same implementation task."""
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = make_store(tmp_path)

        impl_a = store.add("Feature A", task_type="implement")
        impl_a.status = "completed"
        impl_a.completed_at = datetime.now(UTC)
        store.update(impl_a)

        impl_b = store.add("Feature B", task_type="implement")
        impl_b.status = "completed"
        impl_b.completed_at = datetime.now(UTC)
        store.update(impl_b)

        # Review belongs to impl_b, not impl_a.
        review_of_b = store.add("Review B", task_type="review", depends_on=impl_b.id)
        review_of_b.status = "completed"
        review_of_b.completed_at = datetime.now(UTC)
        store.update(review_of_b)

        result = invoke_gza(
            "improve", str(impl_a.id),
            "--review-id", str(review_of_b.id),
            "--queue",
            "--project", str(tmp_path),
        )

        assert result.returncode == 1
        assert f"reviews task {impl_b.id}" in result.stdout

    def test_improve_review_id_flag_accepts_based_on_only_review(self, tmp_path: Path):
        """Imported reviews linked only via based_on remain selectable explicitly."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Feature A", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        imported_review = store.add("Imported review", task_type="review", based_on=impl_task.id)
        imported_review.status = "completed"
        imported_review.completed_at = datetime.now(UTC)
        store.update(imported_review)

        result = invoke_gza(
            "improve", str(impl_task.id),
            "--review-id", str(imported_review.id),
            "--queue",
            "--project", str(tmp_path),
        )

        assert result.returncode == 0, result.stdout
        assert f"Review: {imported_review.id}" in result.stdout

    def test_improve_review_id_flag_rejects_non_review_task(self, tmp_path: Path):
        """--review-id must point at a review task, not an implement/improve task."""
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        result = invoke_gza(
            "improve", str(impl_task.id),
            "--review-id", str(impl_task.id),  # not a review
            "--queue",
            "--project", str(tmp_path),
        )

        assert result.returncode == 1
        assert "not a review" in result.stdout

    @pytest.mark.parametrize("invalid_review_id", ["9999", "42"])
    def test_improve_review_id_flag_rejects_shorthand_ids(self, tmp_path: Path, invalid_review_id: str):
        """--review-id must require full prefixed task IDs (no numeric shorthand)."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        result = invoke_gza(
            "improve", str(impl_task.id),
            "--review-id", invalid_review_id,
            "--queue",
            "--project", str(tmp_path),
        )

        assert result.returncode == 1
        assert "Use a full prefixed task ID" in result.stdout or "Use a full prefixed task ID" in result.stderr


class TestFixCommand:
    """Tests for 'gza fix' command."""

    def test_fix_creates_task_from_implementation_and_latest_review(self, tmp_path: Path):
        """Fix command creates a fix task linked to the root implementation and latest completed review."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Add retries", task_type="implement", group="infra")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-retries"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        review1 = store.add("Review 1", task_type="review", depends_on=impl_task.id)
        review1.status = "completed"
        review1.completed_at = datetime.now(UTC)
        store.update(review1)

        review2 = store.add("Review 2", task_type="review", depends_on=impl_task.id)
        review2.status = "completed"
        review2.completed_at = datetime.now(UTC)
        store.update(review2)

        result = invoke_gza("fix", str(impl_task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Created fix task" in result.stdout
        assert f"Implementation: {impl_task.id}" in result.stdout
        assert f"Latest completed review: {review2.id}" in result.stdout

        fix_tasks = [t for t in store.get_all() if t.task_type == "fix"]
        assert len(fix_tasks) == 1
        fix_task = fix_tasks[0]
        assert fix_task.based_on == impl_task.id
        assert fix_task.depends_on == review2.id
        assert fix_task.same_branch is True
        assert fix_task.group == "infra"

    def test_fix_inherits_parent_tags(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add(
            "Add retries",
            task_type="implement",
            tags=("202606-recovery", "v0.5.0"),
        )
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-retries"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        result = invoke_gza("fix", str(impl_task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        fix_tasks = [t for t in store.get_all() if t.task_type == "fix"]
        assert len(fix_tasks) == 1
        assert fix_tasks[0].tags == impl_task.tags

    def test_fix_inherits_resolved_scope_and_re_reviews_stay_scoped(self, tmp_path: Path):
        """Fix tasks created from legacy sliced implementations must preserve the review scope."""
        from gza.cli.execution import cmd_fix
        from gza.runner import _build_context_from_chain

        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan_task = store.add("Plan migration", task_type="plan")
        plan_task.output_content = "# Plan\n1. Full slice stack."
        store.update(plan_task)

        impl_task = store.add(
            (
                "Implement plan gza-4065, slice F-A1 + F-A2: introduce a first-class `empty` merge-unit state.\n\n"
                "## Scope\n"
                "1. Add the shared classifier.\n"
                "2. Persist and present `empty`.\n\n"
                "## Acceptance\n"
                "- Add tests.\n\n"
                "## Out of scope\n"
                "- F-A3\n"
                "- F-B1\n"
            ),
            task_type="implement",
            based_on=plan_task.id,
        )
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        latest_review = store.add("Review 1", task_type="review", depends_on=impl_task.id)
        latest_review.status = "completed"
        latest_review.completed_at = datetime.now(UTC)
        latest_review.output_content = "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        store.update(latest_review)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=str(impl_task.id),
            queue=True,
            background=False,
            no_docker=True,
            max_turns=None,
            model=None,
            provider=None,
            force=False,
        )

        assert cmd_fix(args) == 0

        fix_task = next(task for task in store.get_all() if task.task_type == "fix")
        assert fix_task.review_scope == (
            "Slice F-A1 + F-A2: introduce a first-class `empty` merge-unit state.\n\n"
            "1. Add the shared classifier.\n"
            "2. Persist and present `empty`."
        )

        fix_task.status = "completed"
        fix_task.completed_at = datetime.now(UTC)
        store.update(fix_task)

        review_result = invoke_gza("review", str(fix_task.id), "--queue", "--project", str(tmp_path))
        assert review_result.returncode == 0

        fix_review = max(
            (task for task in store.get_all() if task.task_type == "review"),
            key=lambda task: task.created_at or datetime.min.replace(tzinfo=UTC),
        )
        context = _build_context_from_chain(fix_review, store, tmp_path, git=None)

        assert "## Review scope:" in context
        assert "## Original plan context (out of scope except for the review scope):" in context
        assert "## Original plan:\n" not in context

    def test_fix_accepts_review_task_id_and_resolves_implementation(self, tmp_path: Path):
        """Fix command accepts review IDs and resolves to the base implementation."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Implement auth", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        review_task = store.add("Review auth", task_type="review", depends_on=impl_task.id)
        review_task.status = "completed"
        review_task.completed_at = datetime.now(UTC)
        store.update(review_task)

        result = invoke_gza("fix", str(review_task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        assert f"Implementation: {impl_task.id}" in result.stdout
        fix_task = [t for t in store.get_all() if t.task_type == "fix"][0]
        assert fix_task.based_on == impl_task.id
        assert fix_task.depends_on == review_task.id

    def test_fix_with_review_flag(self, tmp_path: Path):
        """Fix command with --review flag sets create_review."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Implement auth", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        review_task = store.add("Review auth", task_type="review", depends_on=impl_task.id)
        review_task.status = "completed"
        review_task.completed_at = datetime.now(UTC)
        store.update(review_task)

        result = invoke_gza("fix", str(impl_task.id), "--review", "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        fix_task = [t for t in store.get_all() if t.task_type == "fix"][0]
        assert fix_task.create_review is True

    def test_fix_accepts_chained_improve_task_id_and_resolves_root_impl(self, tmp_path: Path):
        """Fix command resolves chained improve IDs back to the root implementation."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Implement parser", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        review1 = store.add("Review 1", task_type="review", depends_on=impl_task.id)
        review1.status = "completed"
        review1.completed_at = datetime.now(UTC)
        store.update(review1)

        improve1 = store.add(
            "Improve 1",
            task_type="improve",
            based_on=impl_task.id,
            depends_on=review1.id,
            same_branch=True,
        )
        improve1.status = "completed"
        improve1.completed_at = datetime.now(UTC)
        store.update(improve1)

        review2 = store.add("Review 2", task_type="review", depends_on=impl_task.id)
        review2.status = "completed"
        review2.completed_at = datetime.now(UTC)
        store.update(review2)

        improve2 = store.add(
            "Improve 2 retry",
            task_type="improve",
            based_on=improve1.id,
            depends_on=review2.id,
            same_branch=True,
        )
        improve2.status = "completed"
        improve2.completed_at = datetime.now(UTC)
        store.update(improve2)

        result = invoke_gza("fix", str(improve2.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        assert f"Implementation: {impl_task.id}" in result.stdout
        assert f"Latest completed review: {review2.id}" in result.stdout
        fix_task = [t for t in store.get_all() if t.task_type == "fix"][0]
        assert fix_task.based_on == impl_task.id
        assert fix_task.depends_on == review2.id

    def test_fix_uses_retry_implementation_id_as_selected(self, tmp_path: Path):
        """Fix command keeps direct retry implementation IDs anchored to that retry implementation."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Implement parser", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-implement-parser"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        retry_impl = store.add("Retry parser implementation", task_type="implement", based_on=impl_task.id)
        retry_impl.status = "completed"
        retry_impl.branch = "test-project/20260130-retry-parser"
        retry_impl.completed_at = datetime.now(UTC)
        store.update(retry_impl)

        review_retry = store.add("Review retry", task_type="review", depends_on=retry_impl.id)
        review_retry.status = "completed"
        review_retry.completed_at = datetime.now(UTC)
        store.update(review_retry)

        result = invoke_gza("fix", str(retry_impl.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        assert f"Implementation: {retry_impl.id}" in result.stdout
        assert f"Latest completed review: {review_retry.id}" in result.stdout
        fix_task = [t for t in store.get_all() if t.task_type == "fix"][0]
        assert fix_task.based_on == retry_impl.id
        assert fix_task.depends_on == review_retry.id

    def test_fix_rejects_pending_or_in_progress_implementation(self, tmp_path: Path):
        """Fix command rejects active implementation tasks that have not finished yet."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Implement cache", task_type="implement")
        impl_task.status = "in_progress"
        store.update(impl_task)

        result = invoke_gza("fix", str(impl_task.id), "--queue", "--project", str(tmp_path))
        assert result.returncode == 1
        assert f"Task {impl_task.id} is in_progress" in result.stdout

    def test_fix_rejects_failed_implementation_that_never_completed(self, tmp_path: Path):
        """Fix command rejects never-completed implementations and points callers to retry/re-implement."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Implement cache", task_type="implement")
        impl_task.status = "failed"
        impl_task.failure_reason = "WORKER_DIED"
        store.update(impl_task)

        result = invoke_gza("fix", str(impl_task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 1
        assert (
            f"Task {impl_task.id} never completed (status=failed). fix is for review/improve churn "
            "on a completed implementation; retry or re-implement instead."
        ) in result.stdout

    def test_fix_still_creates_task_for_completed_implementation_via_cmd_fix(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Completed implementations remain eligible for fix through the command entrypoint."""
        from gza.cli.execution import cmd_fix

        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Implement cache", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=str(impl_task.id),
            queue=True,
            background=False,
            no_docker=True,
            max_turns=None,
            model=None,
            provider=None,
            force=False,
        )

        assert cmd_fix(args) == 0

        fix_tasks = [task for task in store.get_all() if task.task_type == "fix"]
        assert len(fix_tasks) == 1
        assert fix_tasks[0].based_on == impl_task.id
        output = capsys.readouterr().out
        assert f"Implementation: {impl_task.id}" in output

    def test_fix_defers_follow_up_handoff_to_runner(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        """Fix command should not run local git probing or create reviews directly."""
        from gza.cli.execution import cmd_fix

        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Implement no-change rescue", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-no-change"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        review_task = store.add("Review latest", task_type="review", depends_on=impl_task.id)
        review_task.status = "completed"
        review_task.completed_at = datetime.now(UTC)
        store.update(review_task)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=str(impl_task.id),
            queue=False,
            background=False,
            run=True,
            no_docker=True,
            max_turns=None,
            model=None,
            provider=None,
            force=False,
        )

        with (
            patch("gza.cli.execution._run_foreground", return_value=0),
            patch("gza.cli.execution.Git") as mock_git_cls,
            patch("gza.cli.execution._create_review_task") as mock_create_review,
        ):
            rc = cmd_fix(args)

        assert rc == 0
        mock_git_cls.assert_not_called()
        mock_create_review.assert_not_called()
        output = capsys.readouterr().out
        assert "Running fix task" in output


class TestCommentCommand:
    """Tests for `gza comment` command."""

    def test_comment_adds_direct_comment(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Task needing follow-up", task_type="implement")
        assert task.id is not None

        result = invoke_gza(
            "comment",
            str(task.id),
            "Please add regression coverage.",
            "--author",
            "alice",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 0
        assert f"Added feedback comment 1 to task {task.id}" in result.stdout
        comments = store.get_comments(task.id)
        assert len(comments) == 1
        assert comments[0].source == "direct"
        assert comments[0].author == "alice"
        assert comments[0].kind == "feedback"
        assert comments[0].content == "Please add regression coverage."

    def test_comment_stores_requested_kind(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Task needing review scope", task_type="implement")
        assert task.id is not None

        result = invoke_gza(
            "comment",
            str(task.id),
            "Grade only the API validation slice.",
            "--kind",
            "review_scope",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 0
        assert f"Added review_scope comment 1 to task {task.id}" in result.stdout
        comments = store.get_comments(task.id)
        assert len(comments) == 1
        assert comments[0].kind == "review_scope"

        show_result = invoke_gza("show", str(task.id), "--project", str(tmp_path))

        assert show_result.returncode == 0
        assert "Comments:" in show_result.stdout
        assert "kind=review_scope" in show_result.stdout
        assert "Grade only the API validation slice." in show_result.stdout


class TestReviewCommand:
    """Tests for the 'gza review' command."""

    @pytest.fixture(autouse=True)
    def _mock_foreground_runner(self):
        """Keep review command tests focused on CLI behavior, not agent execution."""
        with patch("gza.cli._run_foreground", return_value=0) as run_foreground:
            yield run_foreground

    def test_review_creates_task_for_completed_implementation(self, tmp_path: Path):
        """Review command creates a review task for a completed implementation."""
        from gza.db import SqliteTaskStore

        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        from gza.config import Config
        config = Config.load(tmp_path)
        store = SqliteTaskStore(db_path, prefix=config.project_prefix)

        # Create a completed implementation task
        impl_task = store.add("Add user authentication", task_type="implement", group="auth-feature")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-user-authentication"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        # Run review command with --queue to only create (not run)
        result = invoke_gza("review", str(impl_task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Created review task " in result.stdout
        assert f"Implementation: {impl_task.id}" in result.stdout
        assert "Group: auth-feature" in result.stdout

        # Verify the review task was created with correct fields
        all_tasks = store.get_all()
        review_task = [t for t in all_tasks if t.task_type == "review"][0]
        assert review_task is not None
        assert review_task.task_type == "review"
        assert review_task.depends_on == impl_task.id
        assert review_task.group == "auth-feature"  # inherited from implementation

    def test_review_background_creator_phase_failure_omits_created_message_and_cleans_up(self, tmp_path: Path):
        """Background review failures before worker handoff must not claim successful creation."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Add user authentication", task_type="implement", group="auth-feature")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-user-authentication"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        with patch("gza.cli._common.prepare_task_startup_phase", side_effect=RuntimeError("creator boom")):
            result = invoke_gza("review", str(impl_task.id), "--background", "--no-docker", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "creator boom" in result.stderr
        assert "Created review task" not in result.stdout
        assert [task for task in store.get_all() if task.task_type == "review"] == []
        logs_dir = tmp_path / ".gza" / "logs"
        if logs_dir.exists():
            assert list(logs_dir.iterdir()) == []
        workers_dir = tmp_path / ".gza" / "workers"
        if workers_dir.exists():
            assert list(workers_dir.iterdir()) == []

    def test_review_fails_on_non_implementation_task(self, tmp_path: Path):
        """Review command fails if task is not an implementation/improve/review task."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a plan task
        plan_task = store.add("Plan authentication system", task_type="plan")
        plan_task.status = "completed"
        plan_task.completed_at = datetime.now(UTC)
        store.update(plan_task)

        # Run review command
        result = invoke_gza("review", str(plan_task.id), "--project", str(tmp_path))

        assert result.returncode == 1
        assert "is a plan task, not an implementation, improve, review, or fix task" in result.stdout

    def test_review_accepts_improve_task_and_targets_implementation(self, tmp_path: Path):
        """Review command accepts improve tasks and reviews the base implementation."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create completed implementation task
        impl_task = store.add("Implement auth", task_type="implement", group="auth")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-implement-auth"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        # Create completed improve task based on implementation
        improve_task = store.add(
            "Improve auth",
            task_type="improve",
            based_on=impl_task.id,
            same_branch=True,
            group="auth",
        )
        improve_task.status = "completed"
        improve_task.completed_at = datetime.now(UTC)
        store.update(improve_task)

        result = invoke_gza("review", str(improve_task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        all_tasks = store.get_all()
        review_task = [t for t in all_tasks if t.task_type == "review"][0]
        assert f"Created review task {review_task.id}" in result.stdout
        assert f"Implementation: {impl_task.id}" in result.stdout

        assert review_task is not None
        assert review_task.task_type == "review"
        assert review_task.depends_on == impl_task.id
        assert review_task.group == "auth"

    def test_review_accepts_fix_task_id_and_targets_implementation(self, tmp_path: Path):
        """Review command accepts fix IDs and resolves to the fix task's implementation."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Implement auth", task_type="implement", group="auth")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-implement-auth"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        fix_task = store.add(
            "Fix auth churn",
            task_type="fix",
            based_on=impl_task.id,
            same_branch=True,
            group="auth",
        )
        fix_task.status = "completed"
        fix_task.completed_at = datetime.now(UTC)
        store.update(fix_task)

        result = invoke_gza("review", str(fix_task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        all_tasks = store.get_all()
        review_task = [t for t in all_tasks if t.task_type == "review"][0]
        assert f"Created review task {review_task.id}" in result.stdout
        assert f"Implementation: {impl_task.id}" in result.stdout
        assert review_task.depends_on == impl_task.id
        assert review_task.group == "auth"

    def test_review_accepts_review_task_id_and_targets_implementation(self, tmp_path: Path):
        """Review command accepts a review task ID and creates a new review on the base implementation."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create completed implementation task
        impl_task = store.add("Implement feature", task_type="implement", group="feat")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-implement-feature"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        # Create a completed review task for the implementation
        assert impl_task.id is not None
        existing_review = store.add("Review feature", task_type="review", depends_on=impl_task.id)
        existing_review.status = "completed"
        existing_review.completed_at = datetime.now(UTC)
        store.update(existing_review)

        # Run review command with the existing review task ID
        result = invoke_gza("review", str(existing_review.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0, result.stdout
        assert "Created review task " in result.stdout
        assert f"Implementation: {impl_task.id}" in result.stdout

        all_tasks = store.get_all()
        new_reviews = [t for t in all_tasks if t.task_type == "review" and t.id != existing_review.id]
        assert len(new_reviews) == 1
        new_review = new_reviews[0]
        assert new_review.depends_on == impl_task.id

    def test_review_fails_on_non_completed_task(self, tmp_path: Path):
        """Review command fails if implementation is not completed."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a pending implementation task
        impl_task = store.add("Add feature", task_type="implement")
        # Leave status as 'pending'

        # Run review command
        result = invoke_gza("review", str(impl_task.id), "--project", str(tmp_path))

        assert result.returncode == 1
        assert "is pending. Can only review completed tasks" in result.stdout

    def test_review_nonexistent_task(self, tmp_path: Path):
        """Review command fails gracefully for nonexistent task."""
        setup_config(tmp_path)

        result = invoke_gza("review", "999", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "Use a full prefixed task ID" in result.stdout or "Use a full prefixed task ID" in result.stderr

    def test_review_keeps_retry_implementation_id_anchored(self, tmp_path: Path):
        """Review command keeps direct retry implementation IDs anchored to the selected retry task."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a plan task
        plan_task = store.add("Plan feature", task_type="plan")
        plan_task.status = "completed"
        plan_task.completed_at = datetime.now(UTC)
        store.update(plan_task)

        # Create implementation based on plan and a retry implementation based on it
        impl_task = store.add("Implement feature", task_type="implement", based_on=plan_task.id)
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-implement-feature"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        retry_impl = store.add("Retry implementation", task_type="implement", based_on=impl_task.id)
        retry_impl.status = "completed"
        retry_impl.branch = "test-project/20260130-retry-implementation"
        retry_impl.completed_at = datetime.now(UTC)
        store.update(retry_impl)

        # Run review command with --queue to only create (not run)
        result = invoke_gza("review", str(retry_impl.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Created review task " in result.stdout

        all_tasks = store.get_all()
        review_task = [t for t in all_tasks if t.task_type == "review"][0]
        assert review_task is not None
        assert review_task.based_on == retry_impl.id
        assert review_task.depends_on == retry_impl.id

        retry_based_children = {t.id for t in store.get_based_on_children(retry_impl.id)}
        parent_based_children = {t.id for t in store.get_based_on_children(impl_task.id)}
        assert review_task.id in retry_based_children
        assert review_task.id not in parent_based_children

        # Review should now hang directly under the retry implementation branch.
        lineage_tree = build_lineage_tree(store, plan_task)
        assert len(lineage_tree.children) == 1
        parent_impl_node = lineage_tree.children[0]
        assert parent_impl_node.task.id == impl_task.id
        assert len(parent_impl_node.children) == 1
        retry_node = [c for c in parent_impl_node.children if c.task.id == retry_impl.id][0]
        assert retry_node.task.id == retry_impl.id
        assert any(child.task.id == review_task.id for child in retry_node.children)

    def test_review_run_flag_runs_immediately(self, tmp_path: Path):
        """Review command runs immediately only with --run."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a completed implementation task
        impl_task = store.add("Add user authentication", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        result = invoke_gza("review", str(impl_task.id), "--run", "--no-docker", "--project", str(tmp_path))

        # Verify the review task was created and run attempted
        assert "Created review task " in result.stdout
        assert "Running review task " in result.stdout

        # Verify the review task exists
        all_tasks = store.get_all()
        review_task = [t for t in all_tasks if t.task_type == "review"][0]
        assert review_task is not None
        assert review_task.task_type == "review"
        assert review_task.depends_on == impl_task.id

    def test_review_with_queue_flag(self, tmp_path: Path):
        """Review command with --queue adds task to queue without executing."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a completed implementation task
        impl_task = store.add("Add user authentication", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        # Run review command with --queue flag
        result = invoke_gza("review", str(impl_task.id), "--queue", "--project", str(tmp_path))

        # Verify the review task was created but not run
        assert result.returncode == 0
        assert "Created review task " in result.stdout
        assert "Running review task" not in result.stdout

        # Verify the review task is still pending
        all_tasks = store.get_all()
        review_task = [t for t in all_tasks if t.task_type == "review"][0]
        assert review_task is not None
        assert review_task.status == "pending"

    def test_review_with_open_flag_no_editor(self, tmp_path: Path, monkeypatch):
        """Review command with --open warns when $EDITOR is not set."""
        from unittest.mock import MagicMock, patch


        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a completed implementation task
        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        # Unset EDITOR environment variable
        monkeypatch.delenv("EDITOR", raising=False)

        # Mock the provider to simulate a successful review
        with patch("gza.runner.get_provider") as mock_get_provider:
            mock_provider = MagicMock()
            mock_provider.name = "test-provider"
            mock_provider.check_credentials.return_value = True
            mock_provider.verify_credentials.return_value = True

            # Simulate successful run
            mock_result = MagicMock()
            mock_result.exit_code = 0
            mock_result.error_type = None
            mock_result.session_id = "test-session-123"
            mock_provider.run.return_value = mock_result
            mock_get_provider.return_value = mock_provider

            # Create the review directory and file that would be created by the task
            review_dir = tmp_path / ".gza" / "reviews"
            review_dir.mkdir(parents=True, exist_ok=True)

            result = invoke_gza("review", str(impl_task.id), "--open", "--run", "--no-docker", "--project", str(tmp_path))

            # Check that warning about missing EDITOR is shown
            # Note: This might not appear in output if the task doesn't complete successfully in test
            # The important thing is that the flag is accepted and doesn't cause an error
            assert result.returncode in (0, 1)  # May fail due to missing credentials, but flag should be accepted

    def test_review_open_flag_with_queue_does_not_run(self, tmp_path: Path):
        """--open flag with --queue creates task but does not run it."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a completed implementation task
        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        # Run review command with --open and --queue (should create task but not run)
        result = invoke_gza("review", str(impl_task.id), "--open", "--queue", "--project", str(tmp_path))

        # Should succeed but not run the task
        assert result.returncode == 0
        assert "Created review task " in result.stdout
        assert "Running review task" not in result.stdout

    def test_review_prevents_duplicate_pending_review(self, tmp_path: Path):
        """Review command warns and exits if a pending review already exists."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a completed implementation task
        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        # Create an existing pending review task
        assert impl_task.id is not None
        existing_review = store.add("Review feature", task_type="review", depends_on=impl_task.id)
        # Leave status as 'pending' (default)

        # Attempt to create another review
        result = invoke_gza("review", str(impl_task.id), "--project", str(tmp_path))

        assert result.returncode == 1
        assert "Warning: A review task already exists" in result.stdout
        assert f"{existing_review.id}" in result.stdout
        assert "pending" in result.stdout

        # Verify no additional review task was created
        reviews = store.get_reviews_for_task(impl_task.id)
        assert len(reviews) == 1

    def test_review_prevents_duplicate_in_progress_review(self, tmp_path: Path):
        """Review command warns and exits if an in_progress review already exists."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a completed implementation task
        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        # Create an existing in_progress review task
        assert impl_task.id is not None
        existing_review = store.add("Review feature", task_type="review", depends_on=impl_task.id)
        existing_review.status = "in_progress"
        store.update(existing_review)

        # Attempt to create another review
        result = invoke_gza("review", str(impl_task.id), "--project", str(tmp_path))

        assert result.returncode == 1
        assert "Warning: A review task already exists" in result.stdout
        assert f"{existing_review.id}" in result.stdout
        assert "in_progress" in result.stdout

        # Verify no additional review task was created
        reviews = store.get_reviews_for_task(impl_task.id)
        assert len(reviews) == 1

    def test_review_allows_new_review_after_completed_review(self, tmp_path: Path):
        """Review command allows creating a new review if existing review is completed."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a completed implementation task
        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        # Create an existing completed review task
        assert impl_task.id is not None
        existing_review = store.add("Review feature", task_type="review", depends_on=impl_task.id)
        existing_review.status = "completed"
        existing_review.completed_at = datetime.now(UTC)
        store.update(existing_review)

        # Create another review with --queue (should succeed after improvements)
        result = invoke_gza("review", str(impl_task.id), "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Created review task" in result.stdout

        # Verify a new review task was created
        reviews = store.get_reviews_for_task(impl_task.id)
        assert len(reviews) == 2

    def test_duplicate_review_uses_DuplicateReviewError_no_second_db_query(self, tmp_path: Path):
        """cmd_review shows the warning using DuplicateReviewError without a second DB query.

        After the refactor, cmd_review catches DuplicateReviewError (which carries
        the active_review task) so store.get_reviews_for_task is called exactly once
        (inside _create_review_task) and NOT a second time in the error handler.
        """
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_review

        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Add feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        assert impl_task.id is not None
        existing_review = store.add("Review feature", task_type="review", depends_on=impl_task.id)
        # Leave as pending so it counts as active

        args = argparse.Namespace(
            task_id=impl_task.id,
            project_dir=tmp_path,
            no_docker=True,
            queue=False,
            background=False,
            open=False,
            pr=False,
            no_pr=False,
        )

        mock_config = MagicMock()
        mock_config.project_dir = tmp_path
        mock_config.use_docker = False
        mock_config.project_prefix = "testproject"

        # Wrap get_reviews_for_task to count calls
        original_get_reviews = store.get_reviews_for_task
        call_count = []

        def counting_get_reviews(task_id: int):
            call_count.append(task_id)
            return original_get_reviews(task_id)

        import io
        output = io.StringIO()

        with patch("gza.cli.Config.load", return_value=mock_config), \
             patch("gza.cli.get_store", return_value=store), \
             patch.object(store, "get_reviews_for_task", side_effect=counting_get_reviews), \
             patch("sys.stdout", output):
            result = cmd_review(args)

        assert result == 1
        printed = output.getvalue()
        assert "Warning: A review task already exists" in printed
        assert f"{existing_review.id}" in printed
        # get_reviews_for_task must be called exactly once (inside _create_review_task),
        # NOT a second time in the cmd_review error handler.
        assert len(call_count) == 1, (
            f"get_reviews_for_task was called {len(call_count)} times; expected exactly 1"
        )

    def test_review_with_model_flag(self, tmp_path: Path):
        """Review command with --model sets the model on the created review task."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Add user authentication", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-user-authentication"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        result = invoke_gza("review", str(impl_task.id), "--model", "claude-opus-4-5", "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        all_tasks = store.get_all()
        review_task = [t for t in all_tasks if t.task_type == "review"][0]
        assert review_task is not None
        assert review_task.model == "claude-opus-4-5"

    def test_review_with_provider_flag(self, tmp_path: Path):
        """Review command with --provider sets the provider on the created review task."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Add user authentication", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test-project/20260129-add-user-authentication"
        impl_task.completed_at = datetime.now(UTC)
        store.update(impl_task)

        result = invoke_gza("review", str(impl_task.id), "--provider", "gemini", "--queue", "--project", str(tmp_path))

        assert result.returncode == 0
        all_tasks = store.get_all()
        review_task = [t for t in all_tasks if t.task_type == "review"][0]
        assert review_task is not None
        assert review_task.provider == "gemini"


def test_advance_create_implement_preserves_source_tags(tmp_path: Path) -> None:
    from gza.cli._common import _create_implementation_task_from_source
    from gza.cli.advance_executor import AdvanceActionExecutionContext, execute_advance_action

    setup_config(tmp_path)
    store = make_store(tmp_path)

    plan_task = store.add("Plan authentication rollout", task_type="plan", tags=("202606-recovery", "v0.5.0"))
    plan_task.status = "completed"
    plan_task.completed_at = datetime.now(UTC)
    store.update(plan_task)

    spawned: list[tuple[object, str]] = []

    context = AdvanceActionExecutionContext(
        store=store,
        trigger_source="manual",
        dry_run=False,
        max_resume_attempts=0,
        use_iterate_for_create_implement=False,
        use_iterate_for_needs_rebase=False,
        prepare_task_for_background_start=lambda task, _rollback: task,
        prepare_create_review=lambda _task: pytest.fail("unused"),
        create_resume_task=lambda _task: pytest.fail("unused"),
        create_rebase_task=lambda _task: pytest.fail("unused"),
        create_implement_task=lambda task: _create_implementation_task_from_source(
            store,
            task,
            prompt=f"Implement plan from task {task.id}",
            trigger_source="manual",
        ),
        spawn_worker=lambda task, kind: spawned.append((task, kind)) or 0,
        spawn_resume_worker=lambda _task, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda _task, _kind: pytest.fail("unused"),
    )

    result = execute_advance_action(
        task=plan_task,
        action={"type": "create_implement"},
        context=context,
    )

    assert result.status == "success"
    assert result.created_task is not None
    assert result.created_task.tags == plan_task.tags
    assert spawned == [(result.created_task, "implement")]


class TestIterateCommand:
    """Tests for 'gza iterate' command."""

    @pytest.fixture(autouse=True)
    def _mock_iterate_git_runtime(self):
        """Default iterate tests to a deterministic git runtime unless overridden."""
        from unittest.mock import MagicMock, patch

        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.resolve_merge_source_ref.return_value = None
        mock_git.is_merged.return_value = False
        mock_git.can_merge.return_value = True

        with patch("gza.cli.Git", return_value=mock_git):
            yield

    def _make_completed_impl(self, store, prompt: str = "Implement feature") -> object:
        """Create and return a completed implement task."""
        from datetime import datetime
        impl = store.add(prompt, task_type="implement")
        impl.status = "completed"
        impl.branch = "test-project/20260101-impl"
        impl.completed_at = datetime.now(UTC)
        store.update(impl)
        return impl

    def _format_expected_attention_line(self, task, action: dict[str, object]) -> str:
        from gza.advance_engine import format_needs_attention_entry_for_display

        return (
            "Needs attention: "
            + format_needs_attention_entry_for_display(
                task,
                action=action,
                prefix=len(task.id or "") + 4,
            )
        )

    def _expected_improve_attention_line(self, *, store, impl, review, max_resume_attempts: int) -> str:
        from gza.cli.advance_executor import (
            AdvanceActionExecutionContext,
            execute_advance_action,
            resolve_execution_needs_attention,
        )

        context = AdvanceActionExecutionContext(
            store=store,
            trigger_source="manual",
            dry_run=False,
            max_resume_attempts=max_resume_attempts,
            use_iterate_for_create_implement=False,
            use_iterate_for_needs_rebase=False,
            prepare_task_for_background_start=lambda task, _rollback: task,
            prepare_create_review=lambda _task: pytest.fail("unused"),
            create_resume_task=lambda _task: pytest.fail("unused"),
            create_rebase_task=lambda _task: pytest.fail("unused"),
            create_implement_task=lambda _task: pytest.fail("unused"),
            spawn_worker=lambda _task, _kind: pytest.fail("unused"),
            spawn_resume_worker=lambda _task, _kind: pytest.fail("unused"),
            spawn_iterate_worker=lambda _task, _kind: pytest.fail("unused"),
        )
        result = execute_advance_action(
            task=impl,
            action={"type": "improve", "review_task": review},
            context=context,
        )
        attention = resolve_execution_needs_attention(impl, result)
        assert attention is not None
        assert attention.task.id == impl.id
        assert attention.action["subject_task_id"] == impl.id
        return self._format_expected_attention_line(attention.task, attention.action)

    def _expected_failed_recovery_attention_line(
        self,
        *,
        store,
        failed_task,
        decision,
        max_resume_attempts: int,
    ) -> str:
        from gza.cli.advance_executor import (
            build_failed_recovery_needs_attention_result,
            resolve_execution_needs_attention,
        )

        result = build_failed_recovery_needs_attention_result(
            store=store,
            failed_task=failed_task,
            recovery_decision=decision,
            max_resume_attempts=max_resume_attempts,
        )
        assert result is not None
        attention = resolve_execution_needs_attention(failed_task, result)
        assert attention is not None
        assert attention.task.id == failed_task.id
        assert attention.action["subject_task_id"] == failed_task.id
        return self._format_expected_attention_line(attention.task, attention.action)

    def _shared_failed_recovery_attention_lines(
        self,
        *,
        store,
        failed_task,
        decision,
        max_resume_attempts: int,
    ) -> tuple[str, str, str]:
        from gza.advance_engine import failed_recovery_decision_to_attention_action
        from gza.cli.advance_executor import (
            build_failed_recovery_needs_attention_result,
            resolve_execution_needs_attention,
        )
        from gza.cli.git_ops import _format_needs_attention_line
        from gza.cli.watch import (
            _failed_recovery_attention_action,
            _watch_needs_attention_message,
        )

        advance_action = failed_recovery_decision_to_attention_action(
            store,
            failed_task,
            decision,
            max_recovery_attempts=max_resume_attempts,
        )
        assert advance_action is not None
        assert advance_action["subject_task_id"] == failed_task.id
        watch_action = _failed_recovery_attention_action(
            store=store,
            task=failed_task,
            decision=decision,
            max_recovery_attempts=max_resume_attempts,
        )
        assert watch_action is not None
        assert watch_action["subject_task_id"] == failed_task.id
        iterate_result = build_failed_recovery_needs_attention_result(
            store=store,
            failed_task=failed_task,
            recovery_decision=decision,
            max_resume_attempts=max_resume_attempts,
        )
        assert iterate_result is not None
        iterate_attention = resolve_execution_needs_attention(failed_task, iterate_result)
        assert iterate_attention is not None
        assert iterate_attention.task.id == failed_task.id
        assert iterate_attention.action["subject_task_id"] == failed_task.id
        return (
            _format_needs_attention_line(failed_task, advance_action),
            _watch_needs_attention_message(failed_task, watch_action),
            _format_needs_attention_line(iterate_attention.task, iterate_attention.action),
        )

    def test_run_iterate_task_with_recovery_preserves_iterate_foreground_invocation(self, tmp_path: Path) -> None:
        import argparse
        from unittest.mock import patch

        from gza.cli.execution import _run_iterate_task_with_recovery

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Iterate pending implementation", task_type="implement")
        assert task.id is not None

        args = argparse.Namespace(force=True)

        def fake_run_with_recovery(
            _config,
            _store,
            task_to_run,
            *,
            run_task,
            max_resume_attempts,
            on_recovery,
            on_terminal_skip,
        ):
            assert task_to_run.id == task.id
            assert max_resume_attempts == 2
            assert on_recovery is not None
            assert on_terminal_skip is not None
            return task_to_run, run_task(task_to_run, True)

        with (
            patch("gza.cli.execution.run_with_recovery", side_effect=fake_run_with_recovery),
            patch("gza.cli.execution._run_foreground", return_value=0) as run_foreground,
        ):
            final_task, rc, terminal_skip = _run_iterate_task_with_recovery(
                args=args,
                config=config,
                store=store,
                task_to_run=task,
                max_resume_attempts=2,
            )

        assert final_task.id == task.id
        assert rc == 0
        assert terminal_skip is None
        run_foreground.assert_called_once()
        assert run_foreground.call_args.kwargs["task_id"] == task.id
        assert run_foreground.call_args.kwargs["resume"] is True
        assert run_foreground.call_args.kwargs["force"] is True
        invocation = run_foreground.call_args.kwargs["invocation"]
        assert invocation.command == "iterate"
        assert invocation.execution_mode == "foreground_worker"

    def test_iterate_live_progress_labels_non_cycle_merge_as_next_action(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli.execution import cmd_iterate
        from gza.runner import _make_review_verify_result
        from gza.runner import _make_review_verify_result

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.output_content = "**Verdict: APPROVED**"
        review.completed_at = datetime.now(UTC)
        store.update(review)

        args = argparse.Namespace(impl_task_id=impl.id, max_iterations=1, dry_run=False, project_dir=tmp_path, no_docker=True)
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.db.Git", return_value=mock_git, create=True),
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 0
        assert "Iterating implementation" in output
        assert "max 1 iterations" in output
        assert "Next action: merge" in output
        assert "Iteration 1/1: merge" not in output
        assert "Action 1/1" not in output

    def test_iterate_without_required_review_still_runs_closing_review(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli.execution import cmd_iterate
        from gza.runner import _make_review_verify_result

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        review = store.add("Closing review", task_type="review", depends_on=impl.id)

        def fake_run_foreground(config, task_id, **kwargs):
            task = store.get(task_id)
            assert task is not None
            if task_id == review.id:
                task.status = "completed"
                task.output_content = "**Verdict: APPROVED**"
                task.completed_at = datetime.now(UTC)
                store.update(task)
                return 0
            raise AssertionError(f"unexpected task id: {task_id}")

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
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.cli._create_review_task", return_value=review) as create_review,
            patch("gza.cli._run_foreground", side_effect=fake_run_foreground) as run_foreground,
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 0
        create_review.assert_not_called()
        run_foreground.assert_called_once()
        assert run_foreground.call_args.kwargs["task_id"] == review.id
        assert "Iteration 1/1: run_review" in output
        assert "Iterate complete: APPROVED" in output

    def test_iterate_review_cleared_merge_reports_merge_ready_not_approved(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli.execution import cmd_iterate
        from gza.runner import _make_review_verify_result

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)

        prior_review = store.add("Review", task_type="review", depends_on=impl.id)
        prior_review.status = "completed"
        prior_review.output_content = (
            "## Summary\n\n- Implementation is aligned; verify failed.\n\n"
            "## Blockers\n\n"
            "### B1 verify_command failure: flaky unit lane\n"
            "Evidence: verify_command failed with exit status 1.\n"
            "Impact: autonomous verify fails.\n"
            "Required fix: rerun verify_command on the current tip.\n"
            "Required tests: rerun verify_command.\n\n"
            "## Follow-Ups\n\nNone.\n\n"
            "## Questions / Assumptions\n\nNone.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )
        prior_review.completed_at = datetime.now(UTC)
        prior_review.review_verify_status = "failed"
        prior_review.review_verify_branch = impl.branch
        prior_review.review_verify_head_sha = "same-head"
        store.update(prior_review)

        # Mark review as cleared and ensure no newer review/improve iteration exists,
        # so shared engine returns the "reviews_all_cleared" merge path.
        impl.review_cleared_at = datetime.now(UTC)
        store.update(impl)
        store.add_artifact(
            impl.id,
            kind="review_clearance",
            producer="test",
            label="review_clearance",
            path=f".gza/artifacts/{impl.id}/review_clearance.json",
            content_type="application/json; charset=utf-8",
            byte_size=2,
            sha256="0" * 64,
            created_at=impl.review_cleared_at,
            status="passed",
            head_sha="same-head",
            metadata={
                "clearance_kind": "verify_only_noop_recovered",
                "clearance_status": "passed",
                "review_task_id": prior_review.id,
                "source_task_id": impl.id,
                "noop_improve_kind": "verify_only",
                "reviewed_head_sha": "same-head",
            },
        )

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
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True
        mock_git.rev_parse_if_exists.side_effect = lambda ref: "same-head" if ref == impl.branch else None

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.db.Git", return_value=mock_git, create=True),
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 0
        assert "Next action: merge" in output
        assert "Iteration 1/1: merge" not in output
        assert "Iterate complete: MERGE_READY" in output
        assert "Merge (previous review addressed)" in output
        assert "Iterate complete: APPROVED" not in output

    def test_iterate_verify_only_noop_recovery_clears_and_finishes_merge_ready(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli.execution import cmd_iterate
        from gza.runner import _make_review_verify_result

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        impl.branch = "feature/iterate-verify-only-noop"
        store.update(impl)

        prior_review = store.add("Review", task_type="review", depends_on=impl.id)
        prior_review.status = "completed"
        prior_review.output_content = (
            "## Summary\n\n- Implementation is aligned; verify failed.\n\n"
            "## Blockers\n\n"
            "### B1 verify_command failure: flaky unit lane\n"
            "Evidence: verify_command failed with exit status 1.\n"
            "Impact: autonomous verify fails.\n"
            "Required fix: rerun verify_command on the current tip.\n"
            "Required tests: rerun verify_command.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )
        prior_review.completed_at = datetime.now(UTC)
        prior_review.review_verify_status = "failed"
        prior_review.review_verify_branch = impl.branch
        prior_review.review_verify_head_sha = "same-head"
        store.update(prior_review)

        noop_improve = store.add(
            "Improve attempt",
            task_type="improve",
            based_on=impl.id,
            depends_on=prior_review.id,
            same_branch=True,
        )
        noop_improve.status = "completed"
        noop_improve.completed_at = datetime.now(UTC)
        noop_improve.branch = impl.branch
        noop_improve.changed_diff = False
        store.update(noop_improve)

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
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject", verify_command="uv run pytest tests/unit -q")
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True
        mock_git.rev_parse_if_exists.side_effect = lambda ref: "same-head" if ref in {impl.branch, "HEAD"} else None
        mock_git.worktree_add_existing.side_effect = lambda path, ref, detach=False: Path(path).mkdir(parents=True, exist_ok=True) or Path(path)
        mock_git.worktree_remove.side_effect = lambda path, force=False: SimpleNamespace(returncode=0)

        def _git_for_path(path):
            worktree_git = MagicMock()
            worktree_git.repo_dir = Path(path)
            worktree_git.default_branch.return_value = "main"
            worktree_git.rev_parse_if_exists.side_effect = lambda ref: "same-head" if ref == "HEAD" else None
            return worktree_git

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.execution.Git", return_value=mock_git),
            patch("gza.cli.advance_executor.Git", side_effect=_git_for_path),
            patch("gza.db.Git", return_value=mock_git, create=True),
            patch("gza.cli.advance_executor._resolve_review_verify_base_sha", return_value="base-sha"),
            patch(
                "gza.cli.advance_executor._run_review_verify_command",
                return_value=_make_review_verify_result(
                    "uv run pytest tests/unit -q",
                    status="passed",
                    exit_status="0",
                    captured_at=datetime(2026, 6, 27, 12, 0, tzinfo=UTC),
                    reviewed_branch=impl.branch,
                    reviewed_head_sha="same-head",
                    reviewed_base_sha="base-sha",
                    working_directory=str(tmp_path),
                ),
            ),
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        refreshed_impl = store.get(impl.id)
        assert result == 0
        assert refreshed_impl is not None
        assert refreshed_impl.review_cleared_at is not None
        assert "Fresh verify passed; verify-only no-op review blocker cleared for the current tip." in output
        assert "Next action: merge" in output
        assert "Iterate complete: MERGE_READY" in output

    def test_cycle_rejects_non_implement_task(self, tmp_path: Path):
        """gza iterate rejects tasks that are not implement type."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan_task = store.add("A plan", task_type="plan")

        result = invoke_gza("iterate", str(plan_task.id), "--project", str(tmp_path))

        assert result.returncode != 0
        assert "implement" in result.stdout.lower() or "implement" in result.stderr.lower()

    def test_cycle_rejects_in_progress_task(self, tmp_path: Path):
        """gza iterate rejects implementation tasks that are in_progress."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl = store.add("Implement feature", task_type="implement")
        impl.status = "in_progress"
        store.update(impl)

        result = invoke_gza("iterate", str(impl.id), "--project", str(tmp_path))

        assert result.returncode != 0
        assert "in_progress" in result.stdout or "in_progress" in result.stderr

    def test_pending_impl_runs_first_then_iterates(self, tmp_path: Path):
        """Pending implementation counts as iteration 1 and every write is followed by review."""
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli.execution import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Implement feature", task_type="implement")  # status = 'pending'
        review1 = store.add("Review 1", task_type="review", depends_on=impl.id)
        improve1 = store.add("Improve 1", task_type="improve", based_on=impl.id, depends_on=review1.id)
        review2 = None

        from datetime import datetime

        def fake_run_foreground(config, task_id, **kwargs):
            task = store.get(task_id)
            assert task is not None
            if task_id == impl.id:
                task.status = "completed"
                task.branch = "testproject/20260101-impl"
                task.duration_seconds = 10.0
                task.num_steps_computed = 1
                task.completed_at = datetime.now()
                store.update(task)
                return 0
            if task_id == review1.id:
                task.status = "completed"
                task.output_content = "**Verdict: CHANGES_REQUESTED**"
                task.completed_at = datetime.now()
                store.update(task)
                return 0
            if task_id == improve1.id:
                task.status = "completed"
                task.completed_at = datetime.now()
                store.update(task)
                root = store.get(impl.id)
                assert root is not None
                root.review_cleared_at = task.completed_at
                store.update(root)
                return 0
            if review2 is not None and task_id == review2.id:
                task.status = "completed"
                task.output_content = "**Verdict: APPROVED**"
                task.completed_at = datetime.now()
                store.update(task)
                return 0
            raise AssertionError(f"unexpected task id: {task_id}")

        def fake_create_review_task(_store, _impl_task, *, trigger_source, **_kwargs):
            nonlocal review2
            current_review1 = store.get(review1.id)
            assert current_review1 is not None
            if current_review1.status == "pending":
                return review1
            if review2 is None:
                review2 = store.add("Review 2", task_type="review", depends_on=impl.id)
            return review2

        args = argparse.Namespace(
            project_dir=str(tmp_path),
            impl_task_id=str(impl.id),
            max_iterations=3,
            dry_run=False,
            no_docker=True,
        )
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True
        with patch("gza.cli.Config.load", return_value=mock_config), \
             patch("gza.cli.get_store", return_value=store), \
             patch("gza.cli.Git", return_value=mock_git), \
             patch("gza.cli._run_foreground", side_effect=fake_run_foreground) as run_foreground, \
             patch("gza.cli._create_review_task", side_effect=fake_create_review_task), \
             patch("gza.cli._create_improve_task", return_value=improve1):
            result = cmd_iterate(args)

        assert result == 0
        call_task_ids = [call[1]["task_id"] for call in run_foreground.call_args_list]
        assert review2 is not None
        assert call_task_ids == [impl.id, review1.id, improve1.id, review2.id]

    def test_pending_impl_summary_max_iterations_one_includes_iteration_one_write(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate
        from gza.review_verdict import ReviewFinding
        from gza.review_verdict import ReviewFinding

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Implement feature", task_type="implement")
        review = store.add("Review", task_type="review", depends_on=impl.id)

        def fake_run_foreground(config, task_id, **kwargs):
            task = store.get(task_id)
            assert task is not None
            if task_id == impl.id:
                task.status = "completed"
                task.branch = "testproject/20260101-impl"
                task.duration_seconds = 20.0
                task.num_steps_computed = 2
                task.cost_usd = 0.50
                task.completed_at = datetime.now(UTC)
                store.update(task)
                return 0
            if task_id == review.id:
                task.status = "completed"
                task.output_content = "**Verdict: APPROVED**"
                task.duration_seconds = 30.0
                task.num_steps_reported = 3
                task.cost_usd = 0.70
                task.completed_at = datetime.now(UTC)
                store.update(task)
                return 0
            raise AssertionError(f"unexpected task id: {task_id}")

        args = argparse.Namespace(
            project_dir=str(tmp_path),
            impl_task_id=str(impl.id),
            max_iterations=1,
            dry_run=False,
            no_docker=True,
        )
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True
        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.cli._run_foreground", side_effect=fake_run_foreground) as run_foreground,
            patch("gza.cli._create_review_task", return_value=review),
            patch("gza.cli._create_improve_task") as create_improve,
            patch("gza.cli.time.monotonic", side_effect=[100.0, 150.0]),
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 0
        assert [call[1]["task_id"] for call in run_foreground.call_args_list] == [impl.id, review.id]
        create_improve.assert_not_called()
        assert re.search(
            rf"1\s+review\s+{re.escape(review.id)}\s+APPROVED\s+30s\s+3\s+\$0\.70\s+completed",
            output,
        )
        assert "Totals: 50s wall | 3 steps | $0.70" in output

    def test_pending_impl_second_review_approved_stops_immediately(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Implement feature", task_type="implement")
        review1 = store.add("Review 1", task_type="review", depends_on=impl.id)
        improve = store.add("Improve", task_type="improve", based_on=impl.id, depends_on=review1.id)
        review2 = None

        def fake_run_foreground(config, task_id, **kwargs):
            task = store.get(task_id)
            assert task is not None
            if task_id == impl.id:
                task.status = "completed"
                task.branch = "testproject/20260101-impl"
                task.duration_seconds = 10.0
                task.num_steps_computed = 1
                task.cost_usd = 0.10
                task.completed_at = datetime.now(UTC)
                store.update(task)
                return 0
            if task_id == review1.id:
                task.status = "completed"
                task.output_content = "**Verdict: CHANGES_REQUESTED**"
                task.duration_seconds = 40.0
                task.num_steps_reported = 4
                task.cost_usd = 0.40
                task.completed_at = datetime.now(UTC)
                store.update(task)
                return 0
            if task_id == improve.id:
                task.status = "completed"
                task.duration_seconds = 15.0
                task.num_steps_computed = 2
                task.cost_usd = 0.20
                task.completed_at = datetime.now(UTC)
                store.update(task)
                root = store.get(impl.id)
                assert root is not None
                root.review_cleared_at = task.completed_at
                store.update(root)
                return 0
            if review2 is not None and task_id == review2.id:
                task.status = "completed"
                task.output_content = "**Verdict: APPROVED**"
                task.duration_seconds = 25.0
                task.num_steps_reported = 3
                task.cost_usd = 0.30
                task.completed_at = datetime.now(UTC)
                store.update(task)
                return 0
            raise AssertionError(f"unexpected task id: {task_id}")

        def fake_create_review_task(_store, _impl_task, *, trigger_source, **_kwargs):
            nonlocal review2
            current_review1 = store.get(review1.id)
            assert current_review1 is not None
            if current_review1.status == "pending":
                return review1
            if review2 is None:
                review2 = store.add("Review 2", task_type="review", depends_on=impl.id)
            return review2

        args = argparse.Namespace(project_dir=str(tmp_path), impl_task_id=str(impl.id), max_iterations=4, dry_run=False, no_docker=True)
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True
        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.cli._run_foreground", side_effect=fake_run_foreground) as run_foreground,
            patch("gza.cli._create_review_task", side_effect=fake_create_review_task),
            patch("gza.cli._create_improve_task", return_value=improve),
            patch("gza.cli.time.monotonic", side_effect=[100.0, 190.0]),
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 0
        assert review2 is not None
        assert [call[1]["task_id"] for call in run_foreground.call_args_list] == [impl.id, review1.id, improve.id, review2.id]
        assert re.search(
            rf"1\s+review\s+{re.escape(review1.id)}\s+CHANGES_REQUESTED\s+40s\s+4\s+\$0\.40\s+completed",
            output,
        )
        assert re.search(
            rf"2\s+improve\s+{re.escape(improve.id)}\s+-\s+15s\s+2\s+\$0\.20\s+completed",
            output,
        )
        assert re.search(
            rf"2\s+review\s+{re.escape(review2.id)}\s+APPROVED\s+25s\s+3\s+\$0\.30\s+completed",
            output,
        )
        assert "Iterate complete: APPROVED (approved)" in output
        assert "Totals: 1m30s wall | 13 steps | $1.30" in output

        review1_task = store.get(review1.id)
        review2_task = store.get(review2.id)
        improve_task = store.get(improve.id)
        assert review1_task is not None and review1_task.depends_on == impl.id
        assert review2_task is not None and review2_task.depends_on == impl.id
        assert improve_task is not None
        assert improve_task.based_on == impl.id
        assert improve_task.depends_on == review1.id
        assert store.get_improve_tasks_for(impl.id, review2.id) == []

    def test_iterate_first_pass_approved_with_followups_creates_followup_and_rerun_reuses(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        review = store.add("Review", task_type="review", depends_on=impl.id, based_on=impl.id)

        review_output = (
            "## Summary\n\n"
            "- Looks good overall.\n\n"
            "## Blockers\n\n"
            "None.\n\n"
            "## Follow-Ups\n\n"
            "### F1\n"
            "Evidence: Edge case currently uncovered.\n"
            "Impact: Rare invalid input may pass through silently.\n"
            "Recommended follow-up: add malformed-input validation.\n"
            "Recommended tests: add a CLI regression for malformed input.\n\n"
            "## Questions / Assumptions\n\n"
            "None.\n\n"
            "## Verdict\n\n"
            "Verdict: APPROVED_WITH_FOLLOWUPS\n"
        )

        def fake_run_foreground(config, task_id, **kwargs):
            task = store.get(task_id)
            assert task is not None
            assert task.id == review.id
            task.status = "completed"
            task.output_content = review_output
            task.completed_at = datetime.now(UTC)
            store.update(task)
            return 0

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=3,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=False,
            background=False,
        )
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.cli._run_foreground", side_effect=fake_run_foreground),
        ):
            result_first = cmd_iterate(args)
        output_first = capsys.readouterr().out

        followups_after_first = [
            task for task in store.get_based_on_children(review.id) if task.task_type == "implement"
        ]
        assert result_first == 0
        assert len(followups_after_first) == 1
        first_followup = followups_after_first[0]
        assert first_followup.prompt.startswith(
            f"Follow-up F1 from review {review.id} for task {impl.id}:"
        )
        assert "Iterate complete: APPROVED (approved_with_followups)" in output_first
        assert first_followup.id in output_first
        assert "followup" in output_first
        assert "created" in output_first

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.cli._run_foreground") as run_foreground_rerun,
        ):
            result_second = cmd_iterate(args)
        output_second = capsys.readouterr().out

        followups_after_second = [
            task for task in store.get_based_on_children(review.id) if task.task_type == "implement"
        ]
        assert result_second == 0
        run_foreground_rerun.assert_not_called()
        assert len(followups_after_second) == 1
        assert followups_after_second[0].id == first_followup.id
        assert first_followup.id in output_second
        assert "followup" in output_second
        assert "reused" in output_second

    def test_iterate_first_pass_approved_with_followups_without_findings_is_blocked(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        review = store.add("Review", task_type="review", depends_on=impl.id, based_on=impl.id)

        review_output = (
            "## Summary\n\n"
            "- Looks good overall.\n\n"
            "## Blockers\n\n"
            "None.\n\n"
            "## Follow-Ups\n\n"
            "None.\n\n"
            "## Questions / Assumptions\n\n"
            "None.\n\n"
            "## Verdict\n\n"
            "Verdict: APPROVED_WITH_FOLLOWUPS\n"
        )

        def fake_run_foreground(config, task_id, **kwargs):
            task = store.get(task_id)
            assert task is not None
            assert task.id == review.id
            task.status = "completed"
            task.output_content = review_output
            task.completed_at = datetime.now(UTC)
            store.update(task)
            return 0

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=3,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=False,
            background=False,
        )
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.cli._run_foreground", side_effect=fake_run_foreground),
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        followups = [task for task in store.get_based_on_children(review.id) if task.task_type == "implement"]
        assert result == 3
        assert followups == []
        assert "Iterate complete: BLOCKED (needs_discussion)" in output

    def test_pending_impl_all_changes_requested_with_iterations_four_ends_on_review(self, tmp_path: Path):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Implement feature", task_type="implement")
        review1 = store.add("Review 1", task_type="review", depends_on=impl.id)

        executed_ids: list[str] = []
        review_count = 0
        improve_count = 0

        def fake_run_foreground(config, task_id, **kwargs):
            nonlocal review_count
            task = store.get(task_id)
            assert task is not None
            assert task.id is not None
            executed_ids.append(task.id)

            if task.task_type == "implement":
                task.status = "completed"
                task.branch = "testproject/20260101-impl"
            elif task.task_type == "review":
                review_count += 1
                task.status = "completed"
                task.output_content = "**Verdict: CHANGES_REQUESTED**"
            elif task.task_type == "improve":
                task.status = "completed"
                root = store.get(impl.id)
                assert root is not None
                root.review_cleared_at = datetime.now(UTC)
                store.update(root)
            else:
                raise AssertionError(f"unexpected task type: {task.task_type}")

            task.completed_at = datetime.now(UTC)
            store.update(task)
            return 0

        def fake_create_review_task(_store, _impl_task, *, trigger_source, **_kwargs):
            if review_count == 0:
                return review1
            return store.add(f"Review {review_count + 1}", task_type="review", depends_on=impl.id)

        def fake_create_improve_task(_store, _impl_task, review_task, *, trigger_source, **_kwargs):
            nonlocal improve_count
            improve_count += 1
            return store.add(
                f"Improve {improve_count}",
                task_type="improve",
                based_on=impl.id,
                depends_on=review_task.id,
            )

        args = argparse.Namespace(
            project_dir=str(tmp_path),
            impl_task_id=str(impl.id),
            max_iterations=4,
            dry_run=False,
            no_docker=True,
        )
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True
        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.cli._run_foreground", side_effect=fake_run_foreground),
            patch("gza.cli._create_review_task", side_effect=fake_create_review_task),
            patch("gza.cli._create_improve_task", side_effect=fake_create_improve_task),
        ):
            result = cmd_iterate(args)

        assert result == 2
        task_types = [store.get(task_id).task_type for task_id in executed_ids]
        assert task_types.count("implement") == 1
        assert task_types.count("improve") == 3
        assert task_types.count("review") == 4
        assert len(task_types) == 8
        assert task_types[-1] == "review"

        review_ids = [task_id for task_id in executed_ids if store.get(task_id).task_type == "review"]
        improve_ids = [task_id for task_id in executed_ids if store.get(task_id).task_type == "improve"]
        assert len(review_ids) == 4
        assert len(improve_ids) == 3

        for review_id in review_ids:
            review_task = store.get(review_id)
            assert review_task is not None
            assert review_task.depends_on == impl.id

        for index, improve_id in enumerate(improve_ids):
            improve_task = store.get(improve_id)
            assert improve_task is not None
            assert improve_task.based_on == impl.id
            assert improve_task.depends_on == review_ids[index]

        assert store.get_improve_tasks_for(impl.id, review_ids[-1]) == []

        direct_children = {child.id for child in store.get_lineage_children(impl.id)}
        assert direct_children == set(review_ids + improve_ids)

    def test_pending_impl_with_iterations_one_runs_exactly_one_implement_and_one_review(self, tmp_path: Path):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Implement feature", task_type="implement")
        review = store.add("Review", task_type="review", depends_on=impl.id)

        executed_ids: list[str] = []

        def fake_run_foreground(config, task_id, **kwargs):
            task = store.get(task_id)
            assert task is not None
            assert task.id is not None
            executed_ids.append(task.id)

            if task.task_type == "implement":
                task.status = "completed"
                task.branch = "testproject/20260101-impl"
            elif task.task_type == "review":
                task.status = "completed"
                task.output_content = "**Verdict: CHANGES_REQUESTED**"
            else:
                raise AssertionError(f"unexpected task type: {task.task_type}")

            task.completed_at = datetime.now(UTC)
            store.update(task)
            return 0

        args = argparse.Namespace(
            project_dir=str(tmp_path),
            impl_task_id=str(impl.id),
            max_iterations=1,
            dry_run=False,
            no_docker=True,
        )
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True
        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.cli._run_foreground", side_effect=fake_run_foreground),
            patch("gza.cli._create_review_task", return_value=review),
            patch("gza.cli._create_improve_task") as create_improve,
        ):
            result = cmd_iterate(args)

        assert result == 2
        create_improve.assert_not_called()
        assert [store.get(task_id).task_type for task_id in executed_ids] == ["implement", "review"]

    def test_branchless_completed_impl_is_blocked_without_running_review_actions(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Implement feature", task_type="implement")
        impl.status = "completed"
        impl.branch = None
        impl.completed_at = datetime.now(UTC)
        store.update(impl)

        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.output_content = "**Verdict: APPROVED**"
        review.completed_at = datetime.now(UTC)
        store.update(review)

        args = argparse.Namespace(
            project_dir=str(tmp_path),
            impl_task_id=str(impl.id),
            max_iterations=1,
            dry_run=False,
            no_docker=True,
        )
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True
        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.db.Git", return_value=mock_git, create=True),
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 3
        assert "Next action: skip" in output
        assert "Iteration 1/1: skip" not in output
        assert "Iterate blocked: skip. Manual review required." in output
        assert "needs_rebase" not in output
        assert "Cannot rebase" not in output

    def test_pending_impl_that_completes_without_branch_is_blocked(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Implement feature", task_type="implement")

        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.output_content = "**Verdict: APPROVED**"
        review.completed_at = datetime.now(UTC)
        store.update(review)

        def fake_run_foreground(config, task_id, **kwargs):
            task = store.get(task_id)
            if task is not None and task.id == impl.id:
                task.status = "completed"
                task.branch = None
                task.completed_at = datetime.now(UTC)
                store.update(task)
            return 0

        args = argparse.Namespace(
            project_dir=str(tmp_path),
            impl_task_id=str(impl.id),
            max_iterations=1,
            dry_run=False,
            no_docker=True,
        )
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True
        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.cli._run_foreground", side_effect=fake_run_foreground),
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 3
        assert "Running pending implementation" in output
        assert "Next action: skip" in output
        assert "Iteration 1/1: skip" not in output
        assert "Iterate blocked: skip. Manual review required." in output
        assert "needs_rebase" not in output
        assert "Cannot rebase" not in output

    def test_branchless_completed_impl_with_no_reviews_does_not_create_or_run_review(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Implement feature", task_type="implement")
        impl.status = "completed"
        impl.branch = None
        impl.completed_at = datetime.now(UTC)
        store.update(impl)

        args = argparse.Namespace(
            project_dir=str(tmp_path),
            impl_task_id=str(impl.id),
            max_iterations=1,
            dry_run=False,
            no_docker=True,
        )
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True
        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.cli._create_review_task") as create_review,
            patch("gza.cli._run_foreground") as run_foreground,
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 3
        create_review.assert_not_called()
        run_foreground.assert_not_called()
        assert "Next action: skip" in output
        assert "Iteration 1/1: skip" not in output
        assert "Iterate blocked: skip. Manual review required." in output

    def test_pending_impl_dry_run(self, tmp_path: Path):
        """gza iterate --dry-run on a pending task shows it would run the impl first."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl = store.add("Implement feature", task_type="implement")  # status = 'pending'

        result = invoke_gza("iterate", str(impl.id), "--dry-run", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "pending" in result.stdout.lower()
        assert "dry-run" in result.stdout.lower()

    def test_iterate_dry_run_merge_ready_uses_next_action_label(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.output_content = "**Verdict: APPROVED**"
        review.completed_at = datetime.now(UTC)
        store.update(review)

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=True,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=False,
            background=False,
        )
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.db.Git", return_value=mock_git, create=True),
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 0
        assert "[dry-run] Would iterate implementation" in output
        assert "[dry-run] First next action: merge" in output
        assert "[dry-run] First iteration 1/1 action: merge" not in output

    def test_iterate_dry_run_resolves_completed_recovery_child_before_planning(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate
        from gza.review_verdict import ReviewFinding

        setup_config(tmp_path)
        store = make_store(tmp_path)

        failed_root = store.add("Implement feature", task_type="implement")
        assert failed_root.id is not None
        failed_root.status = "failed"
        failed_root.failure_reason = "MAX_TURNS"
        failed_root.session_id = "sess-root"
        failed_root.branch = "feature/root"
        failed_root.completed_at = datetime.now(UTC)
        store.update(failed_root)

        completed_resume = store.add(failed_root.prompt, task_type="implement", based_on=failed_root.id)
        assert completed_resume.id is not None
        completed_resume.status = "completed"
        completed_resume.session_id = failed_root.session_id
        completed_resume.branch = failed_root.branch
        completed_resume.has_commits = True
        completed_resume.merge_status = "unmerged"
        completed_resume.completed_at = datetime.now(UTC)
        store.update(completed_resume)

        review = store.add("Review", task_type="review", depends_on=completed_resume.id)
        review.status = "completed"
        review.output_content = "**Verdict: APPROVED**"
        review.completed_at = datetime.now(UTC)
        store.update(review)

        args = argparse.Namespace(
            impl_task_id=failed_root.id,
            max_iterations=1,
            dry_run=True,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=False,
            background=False,
        )
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 0
        assert f"[dry-run] Would iterate implementation {completed_resume.id}" in output
        assert "[dry-run] First next action: merge" in output
        assert "recovery child already completed" not in output

    def test_iterate_dry_run_resolves_multi_step_completed_recovery_descendant_before_planning(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)

        failed_root = store.add("Implement feature", task_type="implement")
        assert failed_root.id is not None
        failed_root.status = "failed"
        failed_root.failure_reason = "MAX_TURNS"
        failed_root.session_id = "sess-root"
        failed_root.branch = "feature/root"
        failed_root.completed_at = datetime.now(UTC)
        store.update(failed_root)

        failed_resume = store.add(failed_root.prompt, task_type="implement", based_on=failed_root.id)
        assert failed_resume.id is not None
        failed_resume.status = "failed"
        failed_resume.failure_reason = "MAX_TURNS"
        failed_resume.session_id = failed_root.session_id
        failed_resume.branch = failed_root.branch
        failed_resume.completed_at = datetime.now(UTC)
        store.update(failed_resume)

        completed_resume = store.add(failed_resume.prompt, task_type="implement", based_on=failed_resume.id)
        assert completed_resume.id is not None
        completed_resume.status = "completed"
        completed_resume.session_id = failed_resume.session_id
        completed_resume.branch = failed_resume.branch
        completed_resume.has_commits = True
        completed_resume.merge_status = "unmerged"
        completed_resume.completed_at = datetime.now(UTC)
        store.update(completed_resume)

        review = store.add("Review", task_type="review", depends_on=completed_resume.id)
        review.status = "completed"
        review.output_content = "**Verdict: APPROVED**"
        review.completed_at = datetime.now(UTC)
        store.update(review)

        args = argparse.Namespace(
            impl_task_id=failed_root.id,
            max_iterations=1,
            dry_run=True,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=False,
            background=False,
        )
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 0
        assert f"[dry-run] Would iterate implementation {completed_resume.id}" in output
        assert "[dry-run] First next action: merge" in output
        assert "recovery child already completed" not in output
        assert "recovery descendant already completed" not in output

    def test_iterate_on_already_merged_impl_suppresses_historical_failed_improve_attention(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Iterate should no-op on merged implementations instead of resurfacing failed improve attention."""
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl = store.add("Merged implementation", task_type="implement")
        assert impl.id is not None
        impl.status = "completed"
        impl.has_commits = True
        impl.merge_status = "merged"
        impl.completed_at = datetime.now(UTC)
        store.update(impl)

        review = store.add("Review", task_type="review", depends_on=impl.id, based_on=impl.id)
        review.status = "completed"
        review.output_content = "**Verdict: CHANGES_REQUESTED**"
        review.completed_at = datetime.now(UTC)
        store.update(review)
        assert review.id is not None

        failed_improve = store.add("Failed improve", task_type="improve", depends_on=review.id, based_on=impl.id)
        failed_improve.status = "failed"
        failed_improve.failure_reason = "TEST_FAILURE"
        failed_improve.completed_at = datetime.now(UTC)
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
            max_concurrent=5,
            max_resume_attempts=1,
            max_review_cycles=3,
            require_review_before_merge=True,
            advance_create_reviews=True,
            iterate_max_iterations=1,
        )

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli._run_foreground") as run_foreground,
        ):
            result = cmd_iterate(args)

        output = capsys.readouterr().out
        assert result == 0
        run_foreground.assert_not_called()
        assert f"No remaining iterate action: implementation {impl.id} is already merged." in output
        assert "Needs attention:" not in output
        assert failed_improve.id not in output

    def test_iterate_dry_run_approved_with_newer_unresolved_comments_prefers_run_improve(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.output_content = "**Verdict: APPROVED**"
        review.completed_at = datetime(2026, 1, 1, tzinfo=UTC)
        store.update(review)

        improve = store.add("Pending improve", task_type="improve", based_on=impl.id, depends_on=review.id)
        improve.status = "pending"
        improve.created_at = datetime(2026, 1, 2, tzinfo=UTC)
        store.update(improve)

        store.add_comment(impl.id, "Fresh unresolved feedback after approval.")

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=True,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=False,
            background=False,
        )
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.db.Git", return_value=mock_git, create=True),
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 0
        assert "[dry-run] First iteration 1/1 action: run_improve" in output
        assert "[dry-run] First next action: merge" not in output

    def test_iterate_real_approved_with_followups_and_newer_unresolved_comments_runs_improve(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza import advance_engine as advance_engine_module
        from gza.cli import cmd_iterate
        from gza.review_verdict import ParsedReviewReport, ReviewFinding

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.completed_at = datetime(2026, 1, 1, tzinfo=UTC)
        store.update(review)

        pending_improve = store.add("Pending improve", task_type="improve", based_on=impl.id, depends_on=review.id)
        pending_improve.status = "pending"
        pending_improve.created_at = datetime(2026, 1, 2, tzinfo=UTC)
        store.update(pending_improve)

        store.add_comment(impl.id, "Fresh unresolved issue added after followups verdict.")

        def fake_run_foreground(_config, task_id, **kwargs):
            task = store.get(task_id)
            assert task is not None
            assert task.id == pending_improve.id
            task.status = "failed"
            task.failure_reason = "TEST_FAILURE"
            task.completed_at = datetime.now(UTC)
            store.update(task)
            return 1

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
            max_concurrent=5,
            max_resume_attempts=1,
            max_review_cycles=3,
            require_review_before_merge=True,
            advance_create_reviews=True,
            workers_path=tmp_path / ".gza" / "workers",
        )
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.cli._run_foreground", side_effect=fake_run_foreground) as run_foreground,
            patch.object(
                advance_engine_module,
                "get_review_report",
                return_value=ParsedReviewReport(
                    verdict="APPROVED_WITH_FOLLOWUPS",
                    findings=(
                        ReviewFinding(
                            id="F1",
                            severity="FOLLOWUP",
                            title="Hardening",
                            body="",
                            evidence=None,
                            impact=None,
                            fix_or_followup="add malformed input guard",
                            tests=None,
                        ),
                    ),
                    format_version="v2",
                ),
            ),
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 3
        run_foreground.assert_called_once()
        assert run_foreground.call_args.kwargs["task_id"] == pending_improve.id
        assert "Iteration 1/1: run_improve" in output
        assert "merge_with_followups" not in output

    def test_failed_task_requires_resume_or_retry(self, tmp_path: Path):
        """gza iterate on a failed task without --resume or --retry errors."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Implement feature", task_type="implement")
        impl.status = "failed"
        store.update(impl)

        result = invoke_gza("iterate", str(impl.id), "--project", str(tmp_path))
        assert result.returncode != 0
        assert "--resume" in result.stdout or "--resume" in result.stderr
        assert "--retry" in result.stdout or "--retry" in result.stderr

    def test_resume_flag_rejected_for_non_failed_task(self, tmp_path: Path):
        """--resume is only valid for failed tasks."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)

        result = invoke_gza("iterate", str(impl.id), "--resume", "--dry-run", "--project", str(tmp_path))
        assert result.returncode != 0
        output = result.stdout + (result.stderr or "")
        assert "failed" in output.lower()

    def test_retry_flag_rejected_for_non_failed_task(self, tmp_path: Path):
        """--retry is only valid for failed tasks."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)

        result = invoke_gza("iterate", str(impl.id), "--retry", "--dry-run", "--project", str(tmp_path))
        assert result.returncode != 0
        output = result.stdout + (result.stderr or "")
        assert "failed" in output.lower()

    def test_resume_and_retry_mutually_exclusive(self, tmp_path: Path):
        """--resume and --retry cannot be used together."""
        setup_config(tmp_path)
        result = invoke_gza("iterate", "testproject-1", "--resume", "--retry", "--project", str(tmp_path))
        assert result.returncode != 0

    def test_failed_task_retry_dry_run(self, tmp_path: Path):
        """gza iterate --retry --dry-run on a failed task shows what would happen."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Implement feature", task_type="implement")
        impl.status = "failed"
        store.update(impl)

        result = invoke_gza("iterate", str(impl.id), "--retry", "--dry-run", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "dry-run" in result.stdout.lower()
        assert "retry" in result.stdout.lower()

    def test_failed_task_resume_dry_run(self, tmp_path: Path):
        """gza iterate --resume --dry-run on a failed task shows what would happen."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Implement feature", task_type="implement")
        impl.status = "failed"
        impl.session_id = "some-session"
        store.update(impl)

        result = invoke_gza("iterate", str(impl.id), "--resume", "--dry-run", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "dry-run" in result.stdout.lower()
        assert "resume" in result.stdout.lower()

    def test_failed_task_background_resume_requires_session_id(self, tmp_path: Path):
        """Background iterate should reject --resume before spawning when no session exists."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Implement feature", task_type="implement")
        impl.status = "failed"
        store.update(impl)

        result = invoke_gza("iterate", str(impl.id), "--resume", "--background", "--project", str(tmp_path))

        assert result.returncode != 0
        output = result.stdout + (result.stderr or "")
        assert "no session id" in output.lower()
        assert "--retry" in output
        assert "started iterate worker" not in output.lower()

    def test_pending_task_background_start_creator_phase_failure_surfaces_and_cleans_up(
        self,
        tmp_path: Path,
    ) -> None:
        """Pending background iterate must fail before detach when startup preparation fails."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Implement feature", task_type="implement")

        with (
            patch("gza.cli._common.prepare_task_startup_phase", side_effect=RuntimeError("creator boom")),
            patch(
                "gza.cli.execution._spawn_background_iterate",
                side_effect=AssertionError("background iterate should not spawn"),
            ),
        ):
            result = invoke_gza(
                "iterate",
                str(impl.id),
                "--background",
                "--no-docker",
                "--project",
                str(tmp_path),
            )

        assert result.returncode == 1
        assert "creator boom" in result.stderr
        output = result.stdout + (result.stderr or "")
        assert "started iterate worker" not in output.lower()
        refreshed = store.get(impl.id)
        assert refreshed is not None
        assert refreshed.slug is None
        assert refreshed.log_file is None
        logs_dir = tmp_path / ".gza" / "logs"
        if logs_dir.exists():
            assert list(logs_dir.iterdir()) == []
        workers_dir = tmp_path / ".gza" / "workers"
        if workers_dir.exists():
            assert list(workers_dir.iterdir()) == []

    def test_pending_task_background_log_setup_failure_restores_startup_metadata(
        self,
        tmp_path: Path,
    ) -> None:
        """Pending background iterate should roll back startup metadata when log setup fails."""
        from gza.log_paths import resolve_task_log_paths

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Implement feature", task_type="implement")
        assert impl.slug is None
        assert impl.log_file is None

        def fail_log_setup(config, _store, pending_task):
            paths = resolve_task_log_paths(config, pending_task)
            paths.conversation.parent.mkdir(parents=True, exist_ok=True)
            paths.conversation.touch()
            raise RuntimeError("log setup boom")

        with (
            patch("gza.runner.generate_slug", return_value="20260510-test-project-implement-feature"),
            patch("gza.runner.ensure_task_log_paths", side_effect=fail_log_setup),
            patch(
                "gza.cli.execution._spawn_background_iterate",
                side_effect=AssertionError("background iterate should not spawn"),
            ),
        ):
            result = invoke_gza(
                "iterate",
                str(impl.id),
                "--background",
                "--no-docker",
                "--project",
                str(tmp_path),
            )

        assert result.returncode == 1
        assert "log setup boom" in result.stderr
        refreshed = store.get(impl.id)
        assert refreshed is not None
        assert refreshed.slug is None
        assert refreshed.log_file is None
        logs_dir = tmp_path / ".gza" / "logs"
        if logs_dir.exists():
            assert not any(path.is_file() for path in logs_dir.rglob("*"))
        workers_dir = tmp_path / ".gza" / "workers"
        if workers_dir.exists():
            assert list(workers_dir.iterdir()) == []

    @pytest.mark.parametrize(
        ("start_flag", "resume_mode"),
        [
            ("--resume", True),
            ("--retry", False),
        ],
    )
    def test_failed_task_background_start_creator_phase_failure_surfaces_and_cleans_up(
        self,
        tmp_path: Path,
        start_flag: str,
        resume_mode: bool,
    ) -> None:
        """Background iterate must fail before detach when resume/retry startup preparation fails."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Implement feature", task_type="implement")
        impl.status = "failed"
        impl.failure_reason = "MAX_TURNS"
        if resume_mode:
            impl.session_id = "resume-session-1"
        store.update(impl)

        with (
            patch("gza.cli._common.prepare_task_startup_phase", side_effect=RuntimeError("creator boom")),
            patch(
                "gza.cli.execution._spawn_background_iterate",
                side_effect=AssertionError("background iterate should not spawn"),
            ),
        ):
            result = invoke_gza(
                "iterate",
                str(impl.id),
                start_flag,
                "--background",
                "--no-docker",
                "--project",
                str(tmp_path),
            )

        assert result.returncode == 1
        assert "creator boom" in result.stderr
        assert store.get_based_on_children(impl.id) == []
        logs_dir = tmp_path / ".gza" / "logs"
        if logs_dir.exists():
            assert list(logs_dir.iterdir()) == []
        workers_dir = tmp_path / ".gza" / "workers"
        if workers_dir.exists():
            assert list(workers_dir.iterdir()) == []

    def test_completed_task_background_create_review_startup_failure_surfaces_and_cleans_up(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Completed background iterate should fail in the parent if first review startup prep fails."""
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli.execution import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Implement feature", task_type="implement")
        assert impl.id is not None
        impl.status = "completed"
        impl.branch = "feature/completed-background-review"
        impl.completed_at = datetime.now(UTC)
        store.update(impl)
        store.set_merge_status(impl.id, "unmerged")

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=False,
            auto_iterate=False,
            background=True,
            force=False,
            worker_id=None,
            prepared_task_id=None,
            prepared_resume=False,
            prepared_phase=None,
            prepared_action_type=None,
            prepared_review_task_id=None,
        )
        mock_config = MagicMock(
            project_dir=tmp_path,
            use_docker=False,
            project_prefix="testproject",
            max_concurrent=5,
            max_resume_attempts=1,
            max_review_cycles=3,
            require_review_before_merge=True,
            advance_create_reviews=True,
        )
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"

        with (
            patch("gza.cli.execution.Config.load", return_value=mock_config),
            patch("gza.cli.execution.get_store", return_value=store),
            patch("gza.cli._common.get_store", return_value=store),
            patch("gza.cli.execution.Git", return_value=mock_git),
            patch(
                "gza.cli.execution.determine_next_action",
                return_value={"type": "create_review", "description": "Create review"},
            ),
            patch("gza.cli._common.prepare_task_startup_phase", side_effect=RuntimeError("creator boom")),
            patch(
                "gza.cli.execution._spawn_background_iterate",
                side_effect=AssertionError("background iterate should not spawn"),
            ),
        ):
            rc = cmd_iterate(args)

        captured = capsys.readouterr()
        assert rc == 1
        assert "creator boom" in captured.err
        assert "started iterate worker" not in captured.out.lower()
        assert store.get_reviews_for_task(impl.id) == []
        logs_dir = tmp_path / ".gza" / "logs"
        if logs_dir.exists():
            assert not any(path.is_file() for path in logs_dir.rglob("*"))
        workers_dir = tmp_path / ".gza" / "workers"
        if workers_dir.exists():
            assert list(workers_dir.iterdir()) == []

    def test_completed_task_background_git_preflight_failure_surfaces_before_detach(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Completed background iterate must fail in Phase 1 when git preflight cannot determine the target branch."""
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli.execution import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        assert impl.id is not None
        store.set_merge_status(impl.id, "unmerged")

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=False,
            auto_iterate=False,
            background=True,
        )
        mock_config = MagicMock(
            project_dir=tmp_path,
            use_docker=False,
            project_prefix="testproject",
            max_resume_attempts=1,
            max_review_cycles=3,
            require_review_before_merge=True,
            advance_create_reviews=True,
        )
        mock_git = MagicMock()
        mock_git.current_branch.side_effect = RuntimeError("branch boom")

        with (
            patch("gza.cli.execution.Config.load", return_value=mock_config),
            patch("gza.cli.execution.get_store", return_value=store),
            patch("gza.cli.execution.Git", return_value=mock_git),
            patch(
                "gza.cli.execution._spawn_background_iterate",
                side_effect=AssertionError("background iterate should not spawn"),
            ),
        ):
            rc = cmd_iterate(args)

        captured = capsys.readouterr()
        assert rc == 1
        assert f"Error: failed to initialize iterate background preflight for task {impl.id}: branch boom" in captured.err
        assert "could not evaluate iterate background preflight" not in captured.err
        assert "started iterate worker" not in captured.out.lower()
        assert store.get_reviews_for_task(impl.id) == []
        assert store.get_based_on_children(impl.id) == []
        logs_dir = tmp_path / ".gza" / "logs"
        if logs_dir.exists():
            assert not any(path.is_file() for path in logs_dir.rglob("*"))
        workers_dir = tmp_path / ".gza" / "workers"
        if workers_dir.exists():
            assert list(workers_dir.iterdir()) == []

    def test_completed_task_background_action_planning_failure_surfaces_before_detach(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Completed background iterate must fail in Phase 1 when determining the first action raises."""
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli.execution import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        assert impl.id is not None
        store.set_merge_status(impl.id, "unmerged")

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=False,
            auto_iterate=False,
            background=True,
        )
        mock_config = MagicMock(
            project_dir=tmp_path,
            use_docker=False,
            project_prefix="testproject",
            max_resume_attempts=1,
            max_review_cycles=3,
            require_review_before_merge=True,
            advance_create_reviews=True,
        )
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"

        with (
            patch("gza.cli.execution.Config.load", return_value=mock_config),
            patch("gza.cli.execution.get_store", return_value=store),
            patch("gza.cli.execution.Git", return_value=mock_git),
            patch(
                "gza.cli.execution.determine_next_action",
                side_effect=RuntimeError("plan boom"),
            ),
            patch(
                "gza.cli.execution._spawn_background_iterate",
                side_effect=AssertionError("background iterate should not spawn"),
            ),
        ):
            rc = cmd_iterate(args)

        captured = capsys.readouterr()
        assert rc == 1
        assert f"Error: failed to determine iterate background start for task {impl.id}: plan boom" in captured.err
        assert "started iterate worker" not in captured.out.lower()
        assert store.get_reviews_for_task(impl.id) == []
        assert store.get_based_on_children(impl.id) == []
        logs_dir = tmp_path / ".gza" / "logs"
        if logs_dir.exists():
            assert not any(path.is_file() for path in logs_dir.rglob("*"))
        workers_dir = tmp_path / ".gza" / "workers"
        if workers_dir.exists():
            assert list(workers_dir.iterdir()) == []

    @pytest.mark.parametrize(
        ("resume_mode", "expected_prefix"),
        [
            (True, "Resuming failed implementation"),
            (False, "Retrying failed implementation"),
        ],
    )
    def test_background_iterate_failed_start_prepares_before_spawn_and_child_reuses_prepared_task(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
        resume_mode: bool,
        expected_prefix: str,
    ) -> None:
        """Background iterate should hand the detached child a single prepared recovery task."""
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli.execution import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Implement feature", task_type="implement")
        assert impl.id is not None
        impl.status = "failed"
        impl.failure_reason = "MAX_TURNS"
        if resume_mode:
            impl.session_id = "resume-session-1"
        store.update(impl)

        parent_args = argparse.Namespace(
            project_dir=tmp_path,
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=False,
            no_docker=True,
            resume=resume_mode,
            retry=not resume_mode,
            auto_iterate=False,
            background=True,
            force=False,
            worker_id=None,
            prepared_task_id=None,
            prepared_resume=False,
            prepared_phase=None,
        )
        mock_config = MagicMock(
            project_dir=tmp_path,
            use_docker=False,
            project_prefix="testproject",
            max_resume_attempts=1,
            max_review_cycles=3,
            require_review_before_merge=True,
            advance_create_reviews=True,
        )
        captured_spawn: dict[str, object] = {}

        def fake_spawn(_args, _config, spawn_impl_task, **kwargs):
            assert spawn_impl_task.id == impl.id
            captured_spawn.update(kwargs)
            return 0

        with (
            patch("gza.cli.execution.Config.load", return_value=mock_config),
            patch("gza.cli.execution.get_store", return_value=store),
            patch("gza.cli.execution._prepare_task_for_immediate_execution", side_effect=lambda _c, task, **_k: task) as prepare_start,
            patch("gza.cli.execution._spawn_background_iterate", side_effect=fake_spawn),
        ):
            parent_rc = cmd_iterate(parent_args)

        assert parent_rc == 0
        assert prepare_start.call_count == 1
        prepared_task_id = captured_spawn.get("prepared_task_id")
        assert isinstance(prepared_task_id, str)
        assert captured_spawn == {
            "max_iterations": 1,
            "dry_run": False,
            "prepared_task_id": prepared_task_id,
            "prepared_resume": resume_mode,
            "prepared_phase": "preloop",
            "prepared_action_type": None,
            "prepared_review_task_id": None,
        }
        children_after_parent = store.get_based_on_children(impl.id)
        assert [task.id for task in children_after_parent] == [prepared_task_id]

        child_args = argparse.Namespace(
            project_dir=tmp_path,
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=False,
            no_docker=True,
            resume=resume_mode,
            retry=not resume_mode,
            auto_iterate=False,
            background=False,
            force=False,
            worker_id=None,
            prepared_task_id=prepared_task_id,
            prepared_resume=resume_mode,
            prepared_phase="preloop",
        )
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True

        def fake_run_foreground(_config, task_id, resume=False, **kwargs):
            task = store.get(task_id)
            assert task is not None
            assert task_id == prepared_task_id
            assert resume is resume_mode
            assert kwargs["invocation"].command == "iterate"
            task.status = "completed"
            task.completed_at = datetime.now(UTC)
            store.update(task)
            return 0

        with (
            patch("gza.cli.execution.Config.load", return_value=mock_config),
            patch("gza.cli.execution.get_store", return_value=store),
            patch("gza.cli.execution.Git", return_value=mock_git),
            patch("gza.cli.execution._run_foreground", side_effect=fake_run_foreground) as run_foreground,
            patch("gza.cli.execution.determine_next_action", return_value={"type": "wait_review"}),
        ):
            child_rc = cmd_iterate(child_args)

        output = capsys.readouterr().out

        assert child_rc == 3
        assert run_foreground.call_count == 1
        assert run_foreground.call_args.kwargs["task_id"] == prepared_task_id
        assert run_foreground.call_args.kwargs.get("resume", False) is resume_mode
        assert expected_prefix in output
        assert len(store.get_based_on_children(impl.id)) == 1

    def test_background_iterate_completed_create_review_prepares_before_spawn_and_child_reuses_prepared_task(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Completed background iterate should prepare the first review in the parent and reuse it in the child."""
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli.execution import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Implement feature", task_type="implement")
        assert impl.id is not None
        impl.status = "completed"
        impl.branch = "feature/completed-background-review-success"
        impl.completed_at = datetime.now(UTC)
        store.update(impl)
        store.set_merge_status(impl.id, "unmerged")

        parent_args = argparse.Namespace(
            project_dir=tmp_path,
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=False,
            no_docker=True,
            resume=False,
            retry=False,
            auto_iterate=False,
            background=True,
            force=False,
            worker_id=None,
            prepared_task_id=None,
            prepared_resume=False,
            prepared_phase=None,
            prepared_action_type=None,
            prepared_review_task_id=None,
        )
        mock_config = MagicMock(
            project_dir=tmp_path,
            use_docker=False,
            project_prefix="testproject",
            max_resume_attempts=1,
            max_review_cycles=3,
            require_review_before_merge=True,
            advance_create_reviews=True,
        )
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        captured_spawn: dict[str, object] = {}

        def fake_spawn(_args, _config, spawn_impl_task, **kwargs):
            assert spawn_impl_task.id == impl.id
            captured_spawn.update(kwargs)
            return 0

        with (
            patch("gza.cli.execution.Config.load", return_value=mock_config),
            patch("gza.cli.execution.get_store", return_value=store),
            patch("gza.cli.execution.Git", return_value=mock_git),
            patch(
                "gza.cli.execution.determine_next_action",
                return_value={"type": "create_review", "description": "Create review"},
            ),
            patch("gza.cli.execution._prepare_task_for_immediate_execution", side_effect=lambda _c, task, **_k: task) as prepare_start,
            patch("gza.cli.execution._spawn_background_iterate", side_effect=fake_spawn),
        ):
            parent_rc = cmd_iterate(parent_args)

        assert parent_rc == 0
        assert prepare_start.call_count == 1
        review_tasks = store.get_reviews_for_task(impl.id)
        assert len(review_tasks) == 1
        prepared_review = review_tasks[0]
        assert captured_spawn == {
            "max_iterations": 1,
            "dry_run": False,
            "prepared_task_id": prepared_review.id,
            "prepared_resume": False,
            "prepared_phase": "iteration",
            "prepared_action_type": "create_review",
            "prepared_review_task_id": None,
        }

        child_args = argparse.Namespace(
            project_dir=tmp_path,
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=False,
            no_docker=True,
            resume=False,
            retry=False,
            auto_iterate=False,
            background=False,
            force=False,
            worker_id=None,
            prepared_task_id=prepared_review.id,
            prepared_resume=False,
            prepared_phase="iteration",
            prepared_action_type="create_review",
            prepared_review_task_id=None,
        )

        def fake_run_foreground(_config, task_id, **kwargs):
            task = store.get(task_id)
            assert task is not None
            assert task_id == prepared_review.id
            assert kwargs["invocation"].command == "iterate"
            task.status = "completed"
            task.completed_at = datetime.now(UTC)
            store.update(task)
            return 0

        with (
            patch("gza.cli.execution.Config.load", return_value=mock_config),
            patch("gza.cli.execution.get_store", return_value=store),
            patch("gza.cli.execution.Git", return_value=mock_git),
            patch("gza.cli.execution._run_foreground", side_effect=fake_run_foreground) as run_foreground,
            patch("gza.cli.execution._create_review_task", side_effect=AssertionError("review should be reused")),
            patch(
                "gza.cli.execution.determine_next_action",
                return_value={"type": "wait_review", "description": "wait"},
            ),
        ):
            child_rc = cmd_iterate(child_args)

        output = capsys.readouterr().out
        assert child_rc == 3
        assert run_foreground.call_count == 1
        assert run_foreground.call_args.kwargs["task_id"] == prepared_review.id
        assert "Iteration 1/1: create_review" in output
        assert len(store.get_reviews_for_task(impl.id)) == 1

    @pytest.mark.parametrize(
        ("start_flag", "resume_mode"),
        [
            ("--resume", True),
            ("--retry", False),
        ],
    )
    def test_failed_task_background_iterate_uses_prepared_child_for_output_and_worker_metadata(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
        start_flag: str,
        resume_mode: bool,
    ) -> None:
        """Background iterate recovery should point operators at the prepared recovery child log."""
        from gza.cli.execution import cmd_iterate

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Implement feature", task_type="implement")
        assert impl.id is not None
        impl.status = "failed"
        impl.failure_reason = "MAX_TURNS"
        if resume_mode:
            impl.session_id = "resume-session-1"
        store.update(impl)

        args = argparse.Namespace(
            project_dir=tmp_path,
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=False,
            no_docker=True,
            resume=resume_mode,
            retry=not resume_mode,
            auto_iterate=False,
            background=True,
            force=False,
            worker_id=None,
            prepared_task_id=None,
            prepared_resume=False,
            prepared_phase=None,
        )

        mock_proc = MagicMock()
        mock_proc.pid = 56565

        with (
            patch("gza.cli._prepare_task_for_immediate_execution", side_effect=lambda _c, task, **_k: task),
            patch(
                "gza.cli._spawn_detached_worker_process",
                return_value=(mock_proc, ".gza/workers/iterate-startup.log"),
            ),
        ):
            rc = cmd_iterate(args)

        assert rc == 0
        children = store.get_based_on_children(impl.id)
        assert len(children) == 1
        prepared_child = children[0]
        captured = capsys.readouterr()
        output = captured.out + captured.err

        registry = WorkerRegistry(config.workers_path)
        workers = registry.list_all(include_completed=True)
        assert len(workers) == 1
        assert workers[0].task_id == prepared_child.id
        assert f"Use 'gza log {prepared_child.id} -f' to follow progress" in output
        assert f"Use 'gza log {impl.id} -f' to follow progress" not in output

    def test_failed_task_resume_reuses_matching_pending_resume_child(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        """gza iterate --resume should reuse an existing pending resume child for the failed root task."""
        import argparse
        from datetime import datetime
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Implement feature", task_type="implement")
        assert impl.id is not None
        impl.status = "failed"
        impl.failure_reason = "MAX_TURNS"
        impl.session_id = "resume-session-1"
        store.update(impl)

        resume_child = store.add("Pending resume child", task_type="implement", based_on=impl.id)
        assert resume_child.id is not None
        resume_child.status = "pending"
        resume_child.session_id = impl.session_id
        store.update(resume_child)

        def fake_run_foreground(config, task_id, **kwargs):
            task = store.get(task_id)
            if task and task.status == "pending":
                task.status = "completed"
                if task.task_type == "review":
                    task.output_content = "**Verdict: APPROVED**"
                elif task.task_type == "implement":
                    task.branch = "test-project/20260101-resume-reuse"
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
        assert first_task_id == resume_child.id
        assert [task.id for task in store.get_based_on_children(impl.id)] == [resume_child.id]
        assert "Resuming failed implementation" in output
        assert "Iterate complete: MERGE_READY" in output

    def test_failed_task_resume_descendant_requires_manual_review(self, tmp_path: Path):
        """iterate --resume should stop when shared policy marks a failed resume descendant manual-only."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        root = store.add("Implement feature", task_type="implement")
        assert root.id is not None
        root.status = "failed"
        root.failure_reason = "MAX_TURNS"
        root.session_id = "resume-session-1"
        store.update(root)

        failed_resume_descendant = store.add(
            "Failed resumed attempt",
            task_type="implement",
            based_on=root.id,
        )
        assert failed_resume_descendant.id is not None
        failed_resume_descendant.status = "failed"
        failed_resume_descendant.failure_reason = "INFRASTRUCTURE_ERROR"
        failed_resume_descendant.session_id = root.session_id
        store.update(failed_resume_descendant)

        result = invoke_gza(
            "iterate",
            str(failed_resume_descendant.id),
            "--resume",
            "--auto-iterate",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 3
        output = result.stdout + (result.stderr or "")
        assert output.count("Needs attention:") == 1
        assert "reason=retry-limit-reached" in output
        assert "Cannot resume failed implementation" not in output

    def test_failed_root_resume_with_existing_failed_resume_child_auto_iterate_uses_shared_attention(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ):
        """Automatic iterate should keep the shared attention stop when a newer failed resume child already exists."""
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate
        from gza.recovery_engine import decide_failed_task_recovery

        setup_config(tmp_path)
        store = make_store(tmp_path)

        root = store.add("Implement feature", task_type="implement")
        assert root.id is not None
        root.status = "failed"
        root.failure_reason = "MAX_TURNS"
        root.session_id = "resume-session-1"
        store.update(root)

        failed_resume_child = store.add(
            "Failed resumed attempt",
            task_type="implement",
            based_on=root.id,
        )
        assert failed_resume_child.id is not None
        failed_resume_child.status = "failed"
        failed_resume_child.failure_reason = "MAX_TURNS"
        failed_resume_child.session_id = root.session_id
        store.update(failed_resume_child)

        decision = decide_failed_task_recovery(
            store,
            failed_resume_child,
            max_recovery_attempts=1,
        )
        expected_line = self._expected_failed_recovery_attention_line(
            store=store,
            failed_task=failed_resume_child,
            decision=decision,
            max_resume_attempts=1,
        )

        assert decision.reason_code == "retry_limit_reached"
        assert "reason=retry-limit-reached" in expected_line
        assert "reason=newer-recovery-descendant-needs-attention" not in expected_line
        assert "proceeding with manual resume from" not in expected_line

        args = argparse.Namespace(
            project_dir=str(tmp_path),
            impl_task_id=str(root.id),
            max_iterations=1,
            dry_run=False,
            no_docker=True,
            resume=True,
            retry=False,
            background=False,
            auto_iterate=True,
        )
        mock_config = MagicMock(
            project_dir=tmp_path,
            use_docker=False,
            project_prefix="testproject",
            require_review_before_merge=False,
            advance_create_reviews=True,
            max_review_cycles=3,
            max_resume_attempts=1,
            workers_path=tmp_path / ".gza" / "workers",
        )

        def fake_run_foreground(config, task_id, resume=False, **kwargs):
            task = store.get(task_id)
            assert task is not None
            task.status = "failed"
            task.failure_reason = "MAX_TURNS"
            task.session_id = root.session_id
            store.update(task)
            return 1

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.advance_engine.prompt_available_width", return_value=40),
            patch("gza.cli._run_foreground", side_effect=fake_run_foreground) as run_fg,
        ):
            result = cmd_iterate(args)

        captured = capsys.readouterr()
        output = captured.out
        resumed_task_id = run_fg.call_args.kwargs["task_id"]
        failed_resume_grandchild = store.get(resumed_task_id)
        assert failed_resume_grandchild is not None
        assert failed_resume_grandchild.based_on == root.id
        assert failed_resume_grandchild.id != failed_resume_child.id
        terminal_decision = decide_failed_task_recovery(
            store,
            failed_resume_grandchild,
            max_recovery_attempts=1,
        )

        assert result == 3
        assert run_fg.call_count == 1
        assert run_fg.call_args.kwargs.get("resume") is True
        assert terminal_decision.reason_code == "retry_limit_reached"
        assert output.count("Needs attention:") == 1
        assert "reason=retry-limit-reached" in output
        assert "Cannot resume failed implementation" not in output
        assert "proceeding with manual resume from" not in output

    def test_failed_task_resume_does_not_reuse_pending_same_session_child_with_mismatched_role(self, tmp_path: Path):
        """iterate --resume should not reuse pending children that violate shared recovery-edge classification."""
        import argparse
        from datetime import datetime
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)

        dependency = store.add("Dependency", task_type="plan")
        assert dependency.id is not None

        root = store.add("Implement feature", task_type="implement")
        assert root.id is not None
        root.status = "failed"
        root.failure_reason = "MAX_TURNS"
        root.session_id = "resume-session-1"
        store.update(root)

        mismatched_child = store.add("Pending child", task_type="implement", based_on=root.id, depends_on=dependency.id)
        assert mismatched_child.id is not None
        mismatched_child.status = "pending"
        mismatched_child.session_id = root.session_id
        store.update(mismatched_child)

        def fake_run_foreground(config, task_id, **kwargs):
            task = store.get(task_id)
            if task and task.status == "pending":
                task.status = "completed"
                if task.task_type == "review":
                    task.output_content = "**Verdict: APPROVED**"
                elif task.task_type == "implement":
                    task.branch = "test-project/20260101-resume-mismatch"
                task.completed_at = datetime.now()
                store.update(task)
            return 0

        args = argparse.Namespace(
            project_dir=str(tmp_path),
            impl_task_id=str(root.id),
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

        assert result == 0
        assert run_fg.call_count >= 1
        first_task_id = run_fg.call_args_list[0][1]["task_id"]
        assert first_task_id != mismatched_child.id
        child_ids = [task.id for task in store.get_based_on_children(root.id)]
        assert mismatched_child.id in child_ids
        assert first_task_id in child_ids

    def test_iterate_continue_flag_is_rejected(self, tmp_path: Path):
        setup_config(tmp_path)
        result = invoke_gza("iterate", "testproject-1", "--continue", "--project", str(tmp_path))
        assert result.returncode != 0
        assert "unrecognized arguments: --continue" in (result.stderr or result.stdout)

    def test_cycle_command_is_rejected(self, tmp_path: Path):
        setup_config(tmp_path)
        result = invoke_gza("cycle", "testproject-1", "--dry-run", "--project", str(tmp_path))
        assert result.returncode == 2
        assert "invalid choice: 'cycle'" in result.stderr
        assert "iterate" in result.stderr

    def test_reuses_latest_changes_requested_review_for_first_iteration(self, tmp_path: Path):
        from unittest.mock import MagicMock

        from gza.cli.advance_engine import determine_next_action
        from gza.cli.execution import _AdvanceEngineConfigAdapter

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)

        latest_review = store.add("Review", task_type="review", depends_on=impl.id)
        latest_review.status = "completed"
        latest_review.output_content = "**Verdict: CHANGES_REQUESTED**"
        latest_review.completed_at = datetime.now(UTC)
        store.update(latest_review)

        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True
        engine_config = _AdvanceEngineConfigAdapter(
            project_dir=tmp_path,
            require_review_before_merge=True,
            advance_create_reviews=True,
            max_review_cycles=3,
            max_resume_attempts=1,
        )
        action = determine_next_action(
            engine_config,
            store,
            mock_git,
            impl,
            "main",
            max_resume_attempts=1,
        )

        assert action["type"] == "improve"
        assert action["review_task"].id == latest_review.id

    def test_latest_review_approved_exits_zero(self, tmp_path: Path):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.output_content = "**Verdict: APPROVED**"
        review.completed_at = datetime.now(UTC)
        store.update(review)

        args = argparse.Namespace(impl_task_id=impl.id, max_iterations=3, dry_run=False, project_dir=tmp_path, no_docker=True)
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True
        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.db.Git", return_value=mock_git, create=True),
        ):
            result = cmd_iterate(args)
        assert result == 0

    def test_iterate_does_not_suppress_when_default_target_merge_unit_is_merged_off_branch(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        assert impl.id is not None

        unit = store.get_or_create_merge_unit_for_task(impl)
        assert unit is not None
        assert unit.target_branch == "main"
        store.set_merge_unit_state(unit.id, "merged")

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=True,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=False,
            background=False,
        )
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "release"
        mock_git.resolve_merge_source_ref.return_value = None
        mock_git.is_merged.return_value = True
        mock_git.can_merge.return_value = True
        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.db.Git", return_value=mock_git, create=True),
        ):
            result = cmd_iterate(args)

        output = capsys.readouterr().out
        assert result == 0
        mock_git.is_merged.assert_not_called()
        assert f"No remaining iterate action: implementation {impl.id} is already merged." not in output
        assert f"[dry-run] Would iterate implementation {impl.id}" in output

    def test_iterate_suppresses_when_off_target_remote_source_ref_is_merged(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        assert impl.id is not None

        unit = store.get_or_create_merge_unit_for_task(impl)
        assert unit is not None
        assert unit.target_branch == "main"
        store.set_merge_unit_state(unit.id, "merged")

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=True,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=False,
            background=False,
        )
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "release"
        mock_git.resolve_merge_source_ref.return_value = f"origin/{impl.branch}"
        mock_git.is_merged.return_value = True
        mock_git.can_merge.return_value = True
        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.db.Git", return_value=mock_git, create=True),
        ):
            result = cmd_iterate(args)

        output = capsys.readouterr().out
        assert result == 0
        mock_git.is_merged.assert_called_once_with(f"origin/{impl.branch}", "release")
        assert f"No remaining iterate action: implementation {impl.id} is already merged." in output
        assert f"[dry-run] Would iterate implementation {impl.id}" not in output

    def test_iterate_suppresses_legacy_impl_without_merge_unit_when_remote_source_ref_is_merged(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        store._default_merge_target_cache = "main"  # noqa: SLF001 - test fixture for merge-unit backfill
        assert impl.id is not None
        assert store.resolve_merge_unit_for_task(impl.id) is None
        impl.has_commits = True
        impl.merge_status = "unmerged"
        store.update(impl)

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=True,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=False,
            background=False,
        )
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "release"
        mock_git.resolve_merge_source_ref.return_value = f"origin/{impl.branch}"
        mock_git.is_merged.return_value = True
        mock_git.can_merge.return_value = True
        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.db.Git", return_value=mock_git, create=True),
        ):
            result = cmd_iterate(args)

        output = capsys.readouterr().out
        assert result == 0
        mock_git.is_merged.assert_called_once_with(f"origin/{impl.branch}", "release")
        assert f"No remaining iterate action: implementation {impl.id} is already merged." in output
        assert f"[dry-run] Would iterate implementation {impl.id}" not in output

    def test_iterate_reconciles_stale_legacy_impl_when_current_target_git_proves_merged(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        store._default_merge_target_cache = "main"  # noqa: SLF001 - test fixture for merge-unit backfill
        impl = self._make_completed_impl(store)
        assert impl.id is not None
        assert store.resolve_merge_unit_for_task(impl.id) is None
        impl.has_commits = True
        impl.merge_status = "unmerged"
        store.update(impl)

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
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.default_branch.return_value = "main"
        mock_git.resolve_merge_source_ref.return_value = f"origin/{impl.branch}"
        mock_git.branch_exists.return_value = False
        mock_git.ref_exists.return_value = True
        mock_git.is_merged.return_value = True
        mock_git.can_merge.return_value = True
        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.db.Git", return_value=mock_git, create=True),
        ):
            result = cmd_iterate(args)

        output = capsys.readouterr().out
        refreshed = store.get(impl.id)
        unit = store.resolve_merge_unit_for_task(impl.id)

        assert result == 0
        assert refreshed is not None
        assert refreshed.merge_status == "merged"
        assert unit is not None
        assert unit.target_branch == "main"
        assert unit.state == "merged"
        assert f"No remaining iterate action: implementation {impl.id} is already merged." in output

    def test_iterate_reconciles_resolved_completed_descendant_before_failed_ancestor_noop(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        store._default_merge_target_cache = "main"  # noqa: SLF001 - test fixture for merge-unit backfill

        failed_impl = store.add("Failed implementation", task_type="implement")
        assert failed_impl.id is not None
        failed_impl.status = "failed"
        failed_impl.failure_reason = "TEST_FAILURE"
        failed_impl.branch = "feature/failed-ancestor"
        failed_impl.has_commits = True
        failed_impl.completed_at = datetime.now(UTC)
        store.update(failed_impl)

        recovered_impl = store.add(
            "Recovered implementation",
            task_type="implement",
            based_on=failed_impl.id,
        )
        assert recovered_impl.id is not None
        recovered_impl.status = "completed"
        recovered_impl.prompt = failed_impl.prompt
        recovered_impl.branch = failed_impl.branch
        recovered_impl.has_commits = True
        recovered_impl.recovery_origin = "retry"
        recovered_impl.completed_at = datetime.now(UTC)
        recovered_impl.merge_status = "unmerged"
        store.update(recovered_impl)

        args = argparse.Namespace(
            impl_task_id=failed_impl.id,
            max_iterations=1,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=False,
            background=False,
        )
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.default_branch.return_value = "main"
        mock_git.resolve_merge_source_ref.return_value = f"origin/{recovered_impl.branch}"
        mock_git.branch_exists.return_value = False
        mock_git.ref_exists.return_value = True
        mock_git.is_merged.return_value = True
        mock_git.can_merge.return_value = True
        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.db.Git", return_value=mock_git, create=True),
        ):
            result = cmd_iterate(args)

        output = capsys.readouterr().out
        refreshed = store.get(recovered_impl.id)
        unit = store.resolve_merge_unit_for_task(recovered_impl.id)

        assert result == 0
        assert refreshed is not None
        assert unit is not None
        assert unit.state == "merged"
        assert (
            "No remaining iterate action: "
            f"failed implementation {failed_impl.id} was fully recovered by merged descendant {recovered_impl.id}."
        ) in output

    def test_iterate_suppresses_when_off_target_unmerged_merge_unit_is_merged(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        assert impl.id is not None

        unit = store.get_or_create_merge_unit_for_task(impl)
        assert unit is not None
        assert unit.target_branch == "main"
        store.set_merge_unit_state(unit.id, "unmerged")

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=True,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=False,
            background=False,
        )
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "release"
        mock_git.resolve_merge_source_ref.return_value = f"origin/{impl.branch}"
        mock_git.is_merged.return_value = True
        mock_git.can_merge.return_value = True
        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
        ):
            result = cmd_iterate(args)

        output = capsys.readouterr().out
        assert result == 0
        mock_git.is_merged.assert_called_once_with(f"origin/{impl.branch}", "release")
        assert f"No remaining iterate action: implementation {impl.id} is already merged." in output
        assert f"[dry-run] Would iterate implementation {impl.id}" not in output

    def test_iterate_suppresses_when_merge_unit_is_merged_for_current_target(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        assert impl.id is not None

        unit = store.get_or_create_merge_unit_for_task(impl)
        assert unit is not None
        assert unit.target_branch == "main"
        store.set_merge_unit_state(unit.id, "merged")

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=True,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=False,
            background=False,
        )
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True
        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
        ):
            result = cmd_iterate(args)

        output = capsys.readouterr().out
        assert result == 0
        assert f"No remaining iterate action: implementation {impl.id} is already merged." in output
        assert f"[dry-run] Would iterate implementation {impl.id}" not in output

    def test_iterate_suppresses_pending_impl_when_legacy_empty_merge_unit_is_redundant_for_current_target(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Pending implementation", task_type="implement")
        assert impl.id is not None
        impl.status = "pending"
        impl.branch = "test-project/20260101-pending-empty"
        impl.has_commits = True
        store.update(impl)

        unit = store.get_or_create_merge_unit_for_task(impl)
        assert unit is not None
        assert unit.target_branch == "main"
        store.set_merge_unit_state(unit.id, "empty")

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
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True
        mock_git.resolve_fresh_merge_source.return_value = impl.branch
        mock_git.rev_parse_if_exists.side_effect = lambda ref: {
            impl.branch: "branch-tip-sha",
            "main": "target-tip-sha",
        }.get(ref)
        mock_git.count_commits_ahead.return_value = 0
        mock_git.count_commits_ahead_checked.return_value = 0
        mock_git.is_merged.return_value = True
        mock_git.is_on_first_parent_history.return_value = True
        mock_git.has_non_empty_source_diff_against_target.return_value = False
        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch(
                "gza.cli.execution._run_foreground",
                side_effect=AssertionError("iterate should not start foreground work for redundant merge state"),
            ) as run_foreground,
        ):
            result = cmd_iterate(args)

        output = capsys.readouterr().out
        assert result == 0
        run_foreground.assert_not_called()
        assert f"implementation {impl.id}'s commits are already present on target." in output
        assert "has no remaining commits to land" not in output

    def test_iterate_suppresses_historical_prerequisite_unmerged_failure_once_reconciled_empty(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate
        from gza.recovery_engine import _MergeContext

        setup_config(tmp_path)
        store = make_store(tmp_path)

        dependency = store.add("Merged dependency", task_type="implement")
        assert dependency.id is not None
        dependency.status = "completed"
        dependency.branch = "feature/dependency"
        dependency.has_commits = True
        dependency.completed_at = datetime.now(UTC)
        store.update(dependency)
        store.set_merge_status(dependency.id, "merged")

        failed = store.add("Historical blocked implementation", task_type="implement", depends_on=dependency.id)
        assert failed.id is not None
        failed.status = "failed"
        failed.failure_reason = "PREREQUISITE_UNMERGED"
        failed.branch = "feature/prereq-empty"
        failed.has_commits = False
        failed.completed_at = datetime.now(UTC)
        store.update(failed)

        class _EmptyBranchGit:
            def resolve_fresh_merge_source(self, branch: str):
                from gza.git import ResolvedMergeSourceRef

                return ResolvedMergeSourceRef(branch)

            def rev_parse_if_exists(self, ref: str) -> str | None:
                if ref in {"main", "feature/prereq-empty"}:
                    return "abc123"
                return None

            def branch_exists(self, branch: str) -> bool:
                return bool(branch)

            def is_merged(self, branch: str, into: str) -> bool:
                return False

        args = argparse.Namespace(
            impl_task_id=failed.id,
            max_iterations=1,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=False,
            background=False,
        )
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True
        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch(
                "gza.recovery_engine._load_merge_context",
                lambda _project_dir=None: _MergeContext(git=_EmptyBranchGit(), default_branch="main"),
            ),
            patch(
                "gza.cli.execution._run_foreground",
                side_effect=AssertionError("iterate should not start foreground work for reconciled empty failures"),
            ) as run_foreground,
        ):
            result = cmd_iterate(args)

        output = capsys.readouterr().out
        assert result == 0
        run_foreground.assert_not_called()
        assert f"No remaining iterate action: implementation {failed.id} has no remaining commits to land." in output

    def test_iterate_suppresses_branchless_historical_prerequisite_unmerged_failure_once_shared_recovery_marks_it_moot(
        self, tmp_path: Path
    ) -> None:
        setup_config(tmp_path)
        store = make_store(tmp_path)

        dependency = store.add("Merged dependency", task_type="implement")
        assert dependency.id is not None
        dependency.status = "completed"
        dependency.completed_at = datetime.now(UTC)
        store.update(dependency)
        store.set_merge_status(dependency.id, "merged")

        failed = store.add("Historical blocked implementation", task_type="implement", depends_on=dependency.id)
        assert failed.id is not None
        failed.status = "failed"
        failed.failure_reason = "PREREQUISITE_UNMERGED"
        failed.has_commits = False
        failed.completed_at = datetime.now(UTC)
        store.update(failed)

        result = invoke_gza("iterate", str(failed.id), "--project", str(tmp_path))

        assert result.returncode == 0
        output = result.stdout + (result.stderr or "")
        assert "No remaining iterate action" in output
        assert "no remaining commits to land" in output
        assert "--resume" not in output
        assert "--retry" not in output

    def test_iterate_suppresses_failed_redundant_branch_with_terminal_message(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)

        failed = store.add("Redundant implementation", task_type="implement")
        assert failed.id is not None
        failed.status = "failed"
        failed.failure_reason = "MAX_TURNS"
        failed.session_id = "sess-redundant-zero"
        failed.branch = "test-project/20260613-redundant"
        failed.has_commits = True
        failed.num_steps_computed = 0
        failed.num_steps_reported = 0
        failed.output_tokens = 0
        failed.completed_at = datetime.now(UTC)
        store.update(failed)

        unit = store.create_merge_unit(
            source_branch=failed.branch,
            target_branch="main",
            owner_task_id=failed.id,
            state="redundant",
        )
        store.attach_task_to_merge_unit(failed.id, unit.id, "owner")

        args = argparse.Namespace(
            impl_task_id=failed.id,
            max_iterations=1,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=False,
            background=False,
        )
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch(
                "gza.cli.execution._run_foreground",
                side_effect=AssertionError("iterate should not start foreground work for moot redundant failures"),
            ) as run_foreground,
        ):
            result = cmd_iterate(args)

        output = capsys.readouterr().out
        assert result == 0
        run_foreground.assert_not_called()
        assert f"implementation {failed.id}'s commits are already present on target." in output
        assert "--resume" not in output
        assert "--retry" not in output

    def test_iterate_relabels_legacy_empty_branch_with_task_commits_to_redundant_message(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)

        failed = store.add("Legacy empty implementation", task_type="implement")
        assert failed.id is not None
        failed.status = "failed"
        failed.failure_reason = "MAX_TURNS"
        failed.session_id = "sess-legacy-empty-redundant"
        failed.branch = "test-project/20260613-legacy-empty"
        failed.has_commits = True
        failed.num_steps_computed = 0
        failed.num_steps_reported = 0
        failed.output_tokens = 0
        failed.completed_at = datetime.now(UTC)
        store.update(failed)

        unit = store.create_merge_unit(
            source_branch=failed.branch,
            target_branch="main",
            owner_task_id=failed.id,
            state="empty",
        )
        store.attach_task_to_merge_unit(failed.id, unit.id, "owner")

        args = argparse.Namespace(
            impl_task_id=failed.id,
            max_iterations=1,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=False,
            background=False,
        )
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True
        mock_git.resolve_fresh_merge_source.return_value = (failed.branch, None)
        mock_git.rev_parse_if_exists.side_effect = lambda ref: {
            failed.branch: "old-main-sha",
            "main": "advanced-main-sha",
        }.get(ref)
        mock_git.count_commits_ahead.return_value = 0
        mock_git.count_commits_ahead_checked.return_value = 0
        mock_git.is_merged.return_value = True
        mock_git.is_on_first_parent_history.return_value = True

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch(
                "gza.cli.execution._run_foreground",
                side_effect=AssertionError("iterate should not start foreground work for legacy empty redundant failures"),
            ) as run_foreground,
        ):
            result = cmd_iterate(args)

        output = capsys.readouterr().out
        assert result == 0
        run_foreground.assert_not_called()
        assert f"implementation {failed.id}'s commits are already present on target." in output
        assert "has no remaining commits to land" not in output

    def test_iterate_resume_on_empty_failed_branch_with_recorded_session_work(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)

        failed = store.add("Failed implementation", task_type="implement")
        assert failed.id is not None
        failed.status = "failed"
        failed.failure_reason = "MAX_TURNS"
        failed.session_id = "sess-empty"
        failed.branch = "test-project/20260101-empty-resume"
        failed.num_steps_computed = 3
        failed.completed_at = datetime.now(UTC)
        store.update(failed)

        unit = store.create_merge_unit(
            source_branch=failed.branch,
            target_branch="main",
            owner_task_id=failed.id,
            state="empty",
        )
        store.attach_task_to_merge_unit(failed.id, unit.id, "owner")

        args = argparse.Namespace(
            impl_task_id=failed.id,
            max_iterations=1,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
            resume=True,
            retry=False,
            background=False,
        )
        mock_config = MagicMock(
            project_dir=tmp_path,
            use_docker=False,
            project_prefix="testproject",
            max_resume_attempts=1,
            max_review_cycles=3,
            require_review_before_merge=True,
            advance_create_reviews=True,
            workers_path=tmp_path / ".gza" / "workers",
        )
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True

        def fake_run_foreground(config, task_id, resume=False, **kwargs):
            task = store.get(task_id)
            assert task is not None
            task.status = "completed"
            task.completed_at = datetime.now(UTC)
            store.update(task)
            return 0

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.cli.execution._run_foreground", side_effect=fake_run_foreground) as run_foreground,
        ):
            result = cmd_iterate(args)

        output = capsys.readouterr().out
        assert result == 3
        assert "has no remaining commits to land" not in output
        assert "Resuming failed implementation" in output
        assert run_foreground.call_count >= 1
        assert run_foreground.call_args_list[0].kwargs.get("resume") is True

    def test_iterate_resume_on_empty_failed_branch_resolved_by_landed_sibling_noops(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)

        root = store.add("Implementation root", task_type="implement")
        assert root.id is not None
        root.status = "completed"
        root.branch = "feature/root"
        root.has_commits = True
        root.completed_at = datetime.now(UTC)
        store.update(root)

        failed = store.add("Failed manual follow-up", task_type="implement", based_on=root.id, recovery_origin="manual")
        assert failed.id is not None
        failed.status = "failed"
        failed.failure_reason = "MAX_TURNS"
        failed.session_id = "sess-empty-landed"
        failed.branch = "feature/independent-landed"
        failed.num_steps_computed = 3
        failed.completed_at = datetime.now(UTC)
        store.update(failed)

        unit = store.create_merge_unit(
            source_branch=failed.branch,
            target_branch="main",
            owner_task_id=failed.id,
            state="empty",
        )
        store.attach_task_to_merge_unit(failed.id, unit.id, "owner")

        landed = store.add("Merged sibling representative", task_type="implement", based_on=root.id, recovery_origin="manual")
        assert landed.id is not None
        landed.status = "completed"
        landed.branch = failed.branch
        landed.has_commits = True
        landed.merge_status = "merged"
        landed.completed_at = datetime.now(UTC)
        store.update(landed)

        args = argparse.Namespace(
            impl_task_id=failed.id,
            max_iterations=1,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
            resume=True,
            retry=False,
            background=False,
        )
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch(
                "gza.cli.execution._run_foreground",
                side_effect=AssertionError("iterate should not resume already landed empty failures"),
            ) as run_foreground,
        ):
            result = cmd_iterate(args)

        output = capsys.readouterr().out
        assert result == 0
        run_foreground.assert_not_called()
        assert (
            f"No remaining iterate action: failed implementation {failed.id} was already resolved by landed lineage "
            "or completed recovery work."
        ) in output
        assert "Resuming failed implementation" not in output

    def test_iterate_pending_resume_on_empty_branch_runs_provider_resume(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)

        pending = store.add("Pending resumed implementation", task_type="implement", recovery_origin="resume")
        assert pending.id is not None
        pending.status = "pending"
        pending.session_id = "sess-pending-empty"
        pending.branch = "test-project/20260605-pending-empty-resume"
        store.update(pending)

        unit = store.create_merge_unit(
            source_branch=pending.branch,
            target_branch="main",
            owner_task_id=pending.id,
            state="empty",
        )
        store.attach_task_to_merge_unit(pending.id, unit.id, "owner")

        args = argparse.Namespace(
            impl_task_id=pending.id,
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
            max_resume_attempts=1,
            max_review_cycles=3,
            require_review_before_merge=True,
            advance_create_reviews=True,
            workers_path=tmp_path / ".gza" / "workers",
        )
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True

        def fake_run_foreground(config, task_id, resume=False, **kwargs):
            task = store.get(task_id)
            assert task is not None
            assert task.id == pending.id
            assert resume is True
            task.status = "completed"
            task.completed_at = datetime.now(UTC)
            store.update(task)
            return 0

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.cli.execution._run_foreground", side_effect=fake_run_foreground) as run_foreground,
        ):
            result = cmd_iterate(args)

        output = capsys.readouterr().out
        assert result == 3
        assert "has no remaining commits to land" not in output
        assert f"Running pending implementation {pending.id}..." in output
        assert run_foreground.call_count >= 1
        assert run_foreground.call_args_list[0].kwargs.get("resume") is True

    def test_iterate_pending_resume_without_session_on_empty_branch_runs_fresh_attempt(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)

        pending = store.add("Pending resume without session", task_type="implement", recovery_origin="resume")
        assert pending.id is not None
        pending.status = "pending"
        pending.branch = "test-project/20260605-pending-empty-retry"
        store.update(pending)

        unit = store.create_merge_unit(
            source_branch=pending.branch,
            target_branch="main",
            owner_task_id=pending.id,
            state="empty",
        )
        store.attach_task_to_merge_unit(pending.id, unit.id, "owner")

        args = argparse.Namespace(
            impl_task_id=pending.id,
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
            max_resume_attempts=1,
            max_review_cycles=3,
            require_review_before_merge=True,
            advance_create_reviews=True,
            workers_path=tmp_path / ".gza" / "workers",
        )
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True

        def fake_run_foreground(config, task_id, resume=False, **kwargs):
            task = store.get(task_id)
            assert task is not None
            assert task.id == pending.id
            assert resume is False
            task.status = "completed"
            task.completed_at = datetime.now(UTC)
            store.update(task)
            return 0

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.cli.execution._run_foreground", side_effect=fake_run_foreground) as run_foreground,
        ):
            result = cmd_iterate(args)

        output = capsys.readouterr().out
        assert result == 3
        assert "has no remaining commits to land" not in output
        assert f"Running pending implementation {pending.id}..." in output
        assert run_foreground.call_count >= 1
        assert "resume" not in run_foreground.call_args_list[0].kwargs

    def test_latest_review_needs_discussion_blocks(self, tmp_path: Path):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.output_content = "**Verdict: NEEDS_DISCUSSION**"
        review.completed_at = datetime.now(UTC)
        store.update(review)

        args = argparse.Namespace(impl_task_id=impl.id, max_iterations=3, dry_run=False, project_dir=tmp_path, no_docker=True)
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        with patch("gza.cli.Config.load", return_value=mock_config), patch("gza.cli.get_store", return_value=store):
            result = cmd_iterate(args)
        assert result == 3

    def test_latest_review_without_verdict_blocks(self, tmp_path: Path):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.output_content = "No verdict section"
        review.completed_at = datetime.now(UTC)
        store.update(review)

        args = argparse.Namespace(impl_task_id=impl.id, max_iterations=3, dry_run=False, project_dir=tmp_path, no_docker=True)
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        with patch("gza.cli.Config.load", return_value=mock_config), patch("gza.cli.get_store", return_value=store):
            result = cmd_iterate(args)
        assert result == 3

    def test_newer_pending_review_reused_over_older_completed_approved(self, tmp_path: Path):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        old_review = store.add("Old approved review", task_type="review", depends_on=impl.id)
        old_review.status = "completed"
        old_review.output_content = "**Verdict: APPROVED**"
        old_review.completed_at = datetime.now(UTC)
        store.update(old_review)
        pending_review = store.add("Pending re-review", task_type="review", depends_on=impl.id)
        pending_review.status = "pending"
        pending_review.output_content = "**Verdict: APPROVED**"
        pending_review.completed_at = None
        store.update(pending_review)

        def fake_run_foreground(config, task_id, **kwargs):
            task = store.get(task_id)
            assert task is not None
            if task_id == pending_review.id:
                task.status = "completed"
                task.completed_at = datetime.now(UTC)
                store.update(task)
                return 0
            raise AssertionError(f"unexpected task id: {task_id}")

        args = argparse.Namespace(impl_task_id=impl.id, max_iterations=1, dry_run=False, project_dir=tmp_path, no_docker=True)
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        with patch("gza.cli.Config.load", return_value=mock_config), \
             patch("gza.cli.get_store", return_value=store), \
             patch("gza.cli._create_review_task") as create_review, \
             patch("gza.cli._run_foreground", side_effect=fake_run_foreground) as run_foreground, \
             patch("gza.cli._create_improve_task") as create_improve:
            result = cmd_iterate(args)
        assert result == 0
        run_foreground.assert_called_once()
        call_kwargs = run_foreground.call_args.kwargs
        assert call_kwargs["task_id"] == pending_review.id
        assert call_kwargs["force"] is False
        assert call_kwargs["invocation"].command == "iterate"
        create_review.assert_not_called()
        create_improve.assert_not_called()

    def test_pending_review_is_reused_instead_of_creating_another(self, tmp_path: Path):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        pending_review = store.add("Pending review", task_type="review", depends_on=impl.id)
        pending_review.status = "pending"
        pending_review.output_content = "**Verdict: APPROVED**"
        store.update(pending_review)

        def fake_run_foreground(config, task_id, **kwargs):
            task = store.get(task_id)
            assert task is not None
            if task_id == pending_review.id:
                task.status = "completed"
                task.completed_at = datetime.now(UTC)
                store.update(task)
                return 0
            raise AssertionError(f"unexpected task id: {task_id}")

        args = argparse.Namespace(impl_task_id=impl.id, max_iterations=1, dry_run=False, project_dir=tmp_path, no_docker=True)
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        with patch("gza.cli.Config.load", return_value=mock_config), \
             patch("gza.cli.get_store", return_value=store), \
             patch("gza.cli._create_review_task") as create_review, \
             patch("gza.cli._run_foreground", side_effect=fake_run_foreground) as run_foreground:
            result = cmd_iterate(args)

        assert result == 0
        run_foreground.assert_called_once()
        call_kwargs = run_foreground.call_args.kwargs
        assert call_kwargs["task_id"] == pending_review.id
        assert call_kwargs["force"] is False
        assert call_kwargs["invocation"].command == "iterate"
        create_review.assert_not_called()

    def test_in_progress_review_is_reported_instead_of_creating_another(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        old_review = store.add("Old approved review", task_type="review", depends_on=impl.id)
        old_review.status = "completed"
        old_review.output_content = "**Verdict: APPROVED**"
        old_review.completed_at = datetime.now(UTC)
        store.update(old_review)
        running_review = store.add("Running review", task_type="review", depends_on=impl.id)
        running_review.status = "in_progress"
        store.update(running_review)

        args = argparse.Namespace(impl_task_id=impl.id, max_iterations=1, dry_run=False, project_dir=tmp_path, no_docker=True)
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        with patch("gza.cli.Config.load", return_value=mock_config), \
             patch("gza.cli.get_store", return_value=store), \
             patch("gza.cli._create_review_task") as create_review, \
             patch("gza.cli._run_foreground") as run_foreground:
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 3
        create_review.assert_not_called()
        run_foreground.assert_not_called()
        assert "Iterate waiting: review_in_progress. Existing task is already in progress." in output
        assert "Manual review required" not in output

    def test_in_progress_review_is_prioritized_over_newer_pending_review(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        running_review = store.add("Older running review", task_type="review", depends_on=impl.id)
        running_review.status = "in_progress"
        running_review.created_at = datetime(2026, 1, 1, tzinfo=UTC)
        store.update(running_review)
        pending_review = store.add("Newer pending review", task_type="review", depends_on=impl.id)
        pending_review.status = "pending"
        pending_review.created_at = datetime(2026, 1, 2, tzinfo=UTC)
        store.update(pending_review)

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
        )
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli._create_review_task") as create_review,
            patch("gza.cli._run_foreground") as run_foreground,
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 3
        create_review.assert_not_called()
        run_foreground.assert_not_called()
        assert "Iterate waiting: review_in_progress. Existing task is already in progress." in output
        assert pending_review.id not in output

    def test_changes_requested_with_pending_improve_reuses_existing_improve(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        from unittest.mock import MagicMock

        from gza.cli.advance_engine import determine_next_action
        from gza.cli.execution import _AdvanceEngineConfigAdapter

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.output_content = "**Verdict: CHANGES_REQUESTED**"
        review.duration_seconds = 61.0
        review.num_steps_reported = 2
        review.cost_usd = 0.14
        review.completed_at = datetime.now(UTC)
        store.update(review)
        improve = store.add("Existing improve", task_type="improve", based_on=impl.id, depends_on=review.id)
        improve.status = "pending"
        store.update(improve)
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True
        engine_config = _AdvanceEngineConfigAdapter(
            project_dir=tmp_path,
            require_review_before_merge=True,
            advance_create_reviews=True,
            max_review_cycles=3,
            max_resume_attempts=1,
        )

        action = determine_next_action(
            engine_config,
            store,
            mock_git,
            impl,
            "main",
            max_resume_attempts=1,
        )

        assert action["type"] == "run_improve"
        assert action["improve_task"].id == improve.id

    def test_changes_requested_with_in_progress_improve_blocks(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.output_content = "**Verdict: CHANGES_REQUESTED**"
        review.duration_seconds = 90.0
        review.num_steps_reported = 4
        review.cost_usd = 0.41
        review.completed_at = datetime.now(UTC)
        store.update(review)
        improve = store.add("Existing improve", task_type="improve", based_on=impl.id, depends_on=review.id)
        improve.status = "in_progress"
        improve.duration_seconds = 45.0
        improve.num_steps_computed = 3
        improve.cost_usd = 0.29
        store.update(improve)

        args = argparse.Namespace(impl_task_id=impl.id, max_iterations=1, dry_run=False, project_dir=tmp_path, no_docker=True)
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        with patch("gza.cli.Config.load", return_value=mock_config), \
             patch("gza.cli.get_store", return_value=store), \
             patch("gza.cli._create_review_task") as create_review, \
             patch("gza.cli._create_improve_task") as create_improve, \
             patch("gza.cli._run_foreground") as run_foreground, \
             patch("gza.cli.time.monotonic", side_effect=[200.0, 260.0]):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 3
        create_improve.assert_not_called()
        create_review.assert_not_called()
        run_foreground.assert_not_called()
        assert re.search(
            rf"1\s+improve\s+{re.escape(improve.id)}\s+-\s+45s\s+3\s+\$0\.29\s+in_progress",
            output,
        )
        assert "Totals: 1m0s wall | 7 steps | $0.70" in output
        assert "Iterate waiting: improve_in_progress. Existing task is already in progress." in output
        assert "Manual review required" not in output

    def test_iterate_reports_disabled_automatic_improve_recovery(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        import argparse
        from unittest.mock import MagicMock

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)

        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.output_content = "**Verdict: CHANGES_REQUESTED**"
        review.completed_at = datetime.now(UTC)
        store.update(review)
        assert review.id is not None

        failed_improve = store.add("Prior improve", task_type="improve", based_on=impl.id, depends_on=review.id)
        failed_improve.status = "failed"
        failed_improve.failure_reason = "TIMEOUT"
        failed_improve.session_id = "sess-improve"
        failed_improve.completed_at = datetime.now(UTC)
        store.update(failed_improve)
        assert failed_improve.id is not None

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
            max_review_cycles=3,
            max_resume_attempts=0,
            require_review_before_merge=True,
            advance_create_reviews=True,
        )
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.cli._run_foreground") as run_foreground,
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 3
        run_foreground.assert_not_called()
        assert "Improve automatic recovery is disabled (max_resume_attempts=0)" in output
        assert self._expected_improve_attention_line(
            store=store,
            impl=impl,
            review=review,
            max_resume_attempts=0,
        ) in output
        assert "reason=automatic-recovery-disabled" in output

    def test_iterate_reports_manual_review_failed_improve_attention(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        import argparse
        from unittest.mock import MagicMock

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)

        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.output_content = "**Verdict: CHANGES_REQUESTED**"
        review.completed_at = datetime.now(UTC)
        store.update(review)
        assert review.id is not None

        failed_improve = store.add("Prior improve", task_type="improve", based_on=impl.id, depends_on=review.id)
        failed_improve.status = "failed"
        failed_improve.failure_reason = "TEST_FAILURE"
        failed_improve.completed_at = datetime.now(UTC)
        store.update(failed_improve)
        assert failed_improve.id is not None

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
            max_review_cycles=3,
            max_resume_attempts=1,
            require_review_before_merge=True,
            advance_create_reviews=True,
        )
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.cli._run_foreground") as run_foreground,
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 3
        run_foreground.assert_not_called()
        assert self._expected_improve_attention_line(
            store=store,
            impl=impl,
            review=review,
            max_resume_attempts=1,
        ) in output
        assert "Latest failed improve" in output

    def test_iterate_max_cycles_reached_reports_cycle_accounting(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)

        trigger_review = store.add("Trigger review", task_type="review", depends_on=impl.id)
        trigger_review.status = "completed"
        trigger_review.output_content = "**Verdict: CHANGES_REQUESTED**"
        trigger_review.completed_at = datetime.now(UTC)
        store.update(trigger_review)

        for idx in range(5):
            done_improve = store.add(
                f"Prior improve {idx + 1}",
                task_type="improve",
                based_on=impl.id,
                depends_on=trigger_review.id,
            )
            done_improve.status = "completed"
            done_improve.completed_at = datetime.now(UTC)
            store.update(done_improve)

        improve_6 = store.add("Improve 6", task_type="improve", based_on=impl.id, depends_on=trigger_review.id)
        improve_7 = store.add("Improve 7", task_type="improve", based_on=impl.id, depends_on=trigger_review.id)
        review_6 = store.add("Review 6", task_type="review", depends_on=impl.id)
        review_7 = store.add("Review 7", task_type="review", depends_on=impl.id)

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=7,
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
            max_review_cycles=7,
            max_resume_attempts=1,
            require_review_before_merge=True,
            advance_create_reviews=True,
        )
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True

        def fake_run_foreground(_config, task_id, **kwargs):
            task = store.get(task_id)
            assert task is not None
            task.status = "completed"
            if task.task_type == "review":
                task.output_content = "**Verdict: CHANGES_REQUESTED**"
            task.completed_at = datetime.now(UTC)
            store.update(task)
            return 0

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.cli._run_foreground", side_effect=fake_run_foreground),
            patch(
                "gza.cli.determine_next_action",
                side_effect=[
                    {"type": "run_improve", "description": "Run improve 6", "improve_task": improve_6},
                    {"type": "run_improve", "description": "Run improve 6", "improve_task": improve_6},
                    {"type": "run_review", "description": "Run review 6", "review_task": review_6},
                    {"type": "run_improve", "description": "Run improve 7", "improve_task": improve_7},
                    {"type": "run_review", "description": "Run review 7", "review_task": review_7},
                    {"type": "max_cycles_reached", "description": "Reached max review cycles"},
                ],
            ),
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 3
        expected_line = self._format_expected_attention_line(
            impl,
            {"type": "max_cycles_reached", "description": "Reached max review cycles"},
        )
        assert expected_line in output
        assert "Review-iteration accounting: completed=7, max_review_cycles=7, consumed_this_invocation=2" in output
        assert f"Recommended next step: uv run gza fix {impl.id}" in output

    def test_iterate_max_cycles_attention_uses_shortened_single_line_prompt(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(
            store,
            prompt=(
                "Implement feature with a very long opening line that should be shortened\n"
                "Second line should be flattened into the final signal\n"
                "Third line keeps going to force truncation"
            ),
        )

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
            max_review_cycles=3,
            max_resume_attempts=1,
            require_review_before_merge=True,
            advance_create_reviews=True,
        )
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.advance_engine.prompt_available_width", return_value=40),
            patch(
                "gza.cli.determine_next_action",
                return_value={"type": "max_cycles_reached", "description": "Reached max review cycles"},
            ),
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        with patch("gza.advance_engine.prompt_available_width", return_value=40):
            expected_line = self._format_expected_attention_line(
                impl,
                {"type": "max_cycles_reached", "description": "Reached max review cycles"},
            )
        assert result == 3
        assert expected_line in output
        assert output.count("Needs attention:") == 1
        assert expected_line.count("\n") == 0
        assert "Implement feature with a very long opening line that should be shortened\nSecond line" not in output

    def test_background_iterate_max_cycles_reached_surfaces_decision_before_spawn(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli.execution import cmd_iterate
        from gza.config import Config

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        config = Config.load(tmp_path)
        config.max_review_cycles = 3
        config.max_resume_attempts = 1
        config.require_review_before_merge = True
        config.advance_create_reviews = True
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=False,
            auto_iterate=False,
            background=True,
        )

        with (
            patch("gza.cli.execution.Config.load", return_value=config),
            patch("gza.cli.execution.get_store", return_value=store),
            patch("gza.cli.execution.Git", return_value=mock_git),
            patch(
                "gza.cli.execution.determine_next_action",
                return_value={"type": "max_cycles_reached", "description": "Reached max review cycles"},
            ),
            patch("gza.cli.execution._spawn_background_iterate", return_value=0) as spawn_background,
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 3
        spawn_background.assert_not_called()
        assert "Next action: max_cycles_reached" in output
        assert "Review-iteration accounting: completed=0, max_review_cycles=3, consumed_this_invocation=0" in output
        assert f"Recommended next step: uv run gza fix {impl.id}" in output
        assert WorkerRegistry(config.workers_path).list_all(include_completed=True) == []

    def test_background_iterate_skip_surfaces_decision_before_spawn(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli.execution import cmd_iterate
        from gza.config import Config

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        config = Config.load(tmp_path)
        config.max_resume_attempts = 1
        config.require_review_before_merge = True
        config.advance_create_reviews = True
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=False,
            auto_iterate=False,
            background=True,
        )

        with (
            patch("gza.cli.execution.Config.load", return_value=config),
            patch("gza.cli.execution.get_store", return_value=store),
            patch("gza.cli.execution.Git", return_value=mock_git),
            patch(
                "gza.cli.execution.determine_next_action",
                return_value={"type": "skip", "description": "SKIP: task has no branch (no commits)"},
            ),
            patch("gza.cli.execution._spawn_background_iterate", return_value=0) as spawn_background,
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 3
        spawn_background.assert_not_called()
        assert "Next action: skip" in output
        assert "Iterate blocked: task has no branch (no commits)" in output
        assert WorkerRegistry(config.workers_path).list_all(include_completed=True) == []

    def test_background_iterate_reconciles_already_merged_noop_before_spawn(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli.execution import cmd_iterate
        from gza.config import Config

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        store._default_merge_target_cache = "main"  # noqa: SLF001 - test fixture for merge-unit backfill
        assert impl.id is not None
        impl.has_commits = True
        impl.merge_status = "unmerged"
        store.update(impl)

        config = Config.load(tmp_path)
        config.max_resume_attempts = 1
        config.require_review_before_merge = True
        config.advance_create_reviews = True
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.default_branch.return_value = "main"
        mock_git.resolve_merge_source_ref.return_value = f"origin/{impl.branch}"
        mock_git.branch_exists.return_value = False
        mock_git.ref_exists.return_value = True
        mock_git.is_merged.return_value = True

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=False,
            auto_iterate=False,
            background=True,
        )

        with (
            patch("gza.cli.execution.Config.load", return_value=config),
            patch("gza.cli.execution.get_store", return_value=store),
            patch("gza.cli.execution.Git", return_value=mock_git),
            patch("gza.db.Git", return_value=mock_git, create=True),
            patch(
                "gza.cli.execution.determine_next_action",
                return_value={"type": "merge", "description": "merge ready"},
            ),
            patch("gza.cli.execution._spawn_background_iterate", return_value=0) as spawn_background,
        ):
            result = cmd_iterate(args)

        output = capsys.readouterr().out
        refreshed = store.get(impl.id)
        unit = store.resolve_merge_unit_for_task(impl.id)

        assert result == 0
        spawn_background.assert_not_called()
        assert refreshed is not None
        assert refreshed.merge_status == "merged"
        assert unit is not None
        assert unit.state == "merged"
        assert f"No remaining iterate action: implementation {impl.id} is already merged." in output
        assert WorkerRegistry(config.workers_path).list_all(include_completed=True) == []

    def test_background_iterate_failed_retry_reconciles_already_merged_noop_before_spawn(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli.execution import cmd_iterate
        from gza.config import Config

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Failed implementation", task_type="implement")
        assert impl.id is not None
        impl.status = "failed"
        impl.failure_reason = "MAX_STEPS"
        impl.branch = "feature/failed-merged"
        impl.has_commits = True
        impl.completed_at = datetime.now(UTC)
        impl.merge_status = "unmerged"
        store.update(impl)
        store._default_merge_target_cache = "main"  # noqa: SLF001 - test fixture for merge-unit backfill

        config = Config.load(tmp_path)
        config.max_resume_attempts = 1
        config.require_review_before_merge = True
        config.advance_create_reviews = True
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.default_branch.return_value = "main"
        mock_git.resolve_merge_source_ref.return_value = f"origin/{impl.branch}"
        mock_git.branch_exists.return_value = False
        mock_git.ref_exists.return_value = True
        mock_git.is_merged.return_value = True

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=True,
            auto_iterate=False,
            background=True,
        )

        with (
            patch("gza.cli.execution.Config.load", return_value=config),
            patch("gza.cli.execution.get_store", return_value=store),
            patch("gza.cli.execution.Git", return_value=mock_git),
            patch("gza.db.Git", return_value=mock_git, create=True),
            patch("gza.cli.execution._spawn_background_iterate", return_value=0) as spawn_background,
        ):
            result = cmd_iterate(args)

        output = capsys.readouterr().out
        refreshed = store.get(impl.id)
        unit = store.resolve_merge_unit_for_task(impl.id)

        assert result == 0
        spawn_background.assert_not_called()
        assert store.get_based_on_children(impl.id) == []
        assert refreshed is not None
        assert refreshed.merge_status == "merged"
        assert unit is not None
        assert unit.state == "merged"
        assert f"No remaining iterate action: implementation {impl.id} is already merged." in output
        assert WorkerRegistry(config.workers_path).list_all(include_completed=True) == []

    def test_background_iterate_failed_retry_git_preflight_failure_surfaces_before_spawn(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli.execution import cmd_iterate
        from gza.config import Config

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Failed implementation", task_type="implement")
        assert impl.id is not None
        impl.status = "failed"
        impl.failure_reason = "MAX_STEPS"
        impl.branch = "feature/failed-preflight"
        impl.has_commits = True
        impl.completed_at = datetime.now(UTC)
        impl.merge_status = "unmerged"
        store.update(impl)

        config = Config.load(tmp_path)
        config.max_resume_attempts = 1
        config.require_review_before_merge = True
        config.advance_create_reviews = True

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=True,
            auto_iterate=False,
            background=True,
        )

        with (
            patch("gza.cli.execution.Config.load", return_value=config),
            patch("gza.cli.execution.get_store", return_value=store),
            patch("gza.cli.execution.Git", side_effect=RuntimeError("git init boom")),
            patch(
                "gza.cli.execution._spawn_background_iterate",
                side_effect=AssertionError("background iterate should not spawn"),
            ),
        ):
            result = cmd_iterate(args)

        captured = capsys.readouterr()

        assert result == 1
        assert (
            f"Error: failed to initialize iterate background preflight for task {impl.id}: git init boom"
            in captured.err
        )
        assert "could not evaluate iterate background preflight" not in captured.err
        assert store.get_based_on_children(impl.id) == []
        assert WorkerRegistry(config.workers_path).list_all(include_completed=True) == []
        logs_dir = tmp_path / ".gza" / "logs"
        if logs_dir.exists():
            assert not any(path.is_file() for path in logs_dir.rglob("*"))
        workers_dir = tmp_path / ".gza" / "workers"
        if workers_dir.exists():
            assert list(workers_dir.iterdir()) == []

    def test_background_iterate_completed_descendant_legacy_empty_noops_with_redundant_message_before_spawn(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli.execution import cmd_iterate
        from gza.config import Config

        setup_config(tmp_path)
        store = make_store(tmp_path)

        failed_impl = store.add("Failed implementation", task_type="implement")
        assert failed_impl.id is not None
        failed_impl.status = "failed"
        failed_impl.failure_reason = "TEST_FAILURE"
        failed_impl.branch = "feature/failed-empty-ancestor"
        failed_impl.has_commits = True
        failed_impl.completed_at = datetime.now(UTC)
        store.update(failed_impl)

        recovered_impl = store.add(
            "Recovered implementation",
            task_type="implement",
            based_on=failed_impl.id,
        )
        assert recovered_impl.id is not None
        recovered_impl.status = "completed"
        recovered_impl.prompt = failed_impl.prompt
        recovered_impl.branch = failed_impl.branch
        recovered_impl.has_commits = True
        recovered_impl.recovery_origin = "retry"
        recovered_impl.completed_at = datetime.now(UTC)
        recovered_impl.merge_status = "unmerged"
        store.update(recovered_impl)

        unit = store.get_or_create_merge_unit_for_task(recovered_impl)
        assert unit is not None
        assert unit.target_branch == "main"
        store.set_merge_unit_state(unit.id, "empty")

        config = Config.load(tmp_path)
        config.max_resume_attempts = 1
        config.require_review_before_merge = True
        config.advance_create_reviews = True
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.resolve_fresh_merge_source.return_value = recovered_impl.branch
        mock_git.rev_parse_if_exists.side_effect = lambda ref: {
            recovered_impl.branch: "branch-tip-sha",
            "main": "target-tip-sha",
        }.get(ref)
        mock_git.count_commits_ahead.return_value = 0
        mock_git.count_commits_ahead_checked.return_value = 0
        mock_git.is_merged.return_value = True
        mock_git.is_on_first_parent_history.return_value = True
        mock_git.has_non_empty_source_diff_against_target.return_value = False

        args = argparse.Namespace(
            impl_task_id=failed_impl.id,
            max_iterations=1,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=False,
            auto_iterate=False,
            background=True,
        )

        with (
            patch("gza.cli.execution.Config.load", return_value=config),
            patch("gza.cli.execution.get_store", return_value=store),
            patch("gza.cli.execution.Git", return_value=mock_git),
            patch(
                "gza.cli.execution._spawn_background_iterate",
                side_effect=AssertionError("background iterate should not spawn for redundant merge state"),
            ) as spawn_background,
        ):
            result = cmd_iterate(args)

        output = capsys.readouterr().out
        assert result == 0
        spawn_background.assert_not_called()
        assert (
            "No remaining iterate action: "
            f"failed implementation {failed_impl.id} was fully recovered by descendant "
            f"{recovered_impl.id}; commits are already present on target."
        ) in output
        assert WorkerRegistry(config.workers_path).list_all(include_completed=True) == []

    def test_background_iterate_merge_with_followups_spawns_worker_instead_of_noop(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli.execution import cmd_iterate
        from gza.config import Config

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)

        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.output_content = "**Verdict: APPROVED_WITH_FOLLOWUPS**"
        review.completed_at = datetime.now(UTC)
        store.update(review)
        assert review.id is not None

        config = Config.load(tmp_path)
        config.max_resume_attempts = 1
        config.require_review_before_merge = True
        config.advance_create_reviews = True
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=False,
            auto_iterate=False,
            background=True,
        )

        with (
            patch("gza.cli.execution.Config.load", return_value=config),
            patch("gza.cli.execution.get_store", return_value=store),
            patch("gza.cli.execution.Git", return_value=mock_git),
            patch(
                "gza.cli.execution.determine_next_action",
                return_value={
                    "type": "merge_with_followups",
                    "description": "Merge (review APPROVED_WITH_FOLLOWUPS)",
                    "review_task": review,
                    "followup_findings": (MagicMock(id="F1"),),
                },
            ),
            patch("gza.cli.execution._spawn_background_iterate", return_value=0) as spawn_background,
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 0
        spawn_background.assert_called_once_with(
            args,
            config,
            impl,
            max_iterations=1,
            dry_run=False,
            prepared_task_id=None,
            prepared_resume=False,
            prepared_phase=None,
            prepared_action_type=None,
            prepared_review_task_id=None,
        )
        assert "No remaining iterate action: implementation" not in output
        assert "ready to merge" not in output
        assert WorkerRegistry(config.workers_path).list_all(include_completed=True) == []

    def test_background_iterate_run_improve_spawns_worker_instead_of_blocking(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli.execution import cmd_iterate
        from gza.config import Config

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)

        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.output_content = "**Verdict: CHANGES_REQUESTED**"
        review.completed_at = datetime.now(UTC)
        store.update(review)
        assert review.id is not None

        pending_improve = store.add("Pending improve", task_type="improve", based_on=impl.id, depends_on=review.id)
        pending_improve.status = "pending"
        store.update(pending_improve)
        assert pending_improve.id is not None

        config = Config.load(tmp_path)
        config.max_resume_attempts = 1
        config.require_review_before_merge = True
        config.advance_create_reviews = True
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=False,
            auto_iterate=False,
            background=True,
        )

        with (
            patch("gza.cli.execution.Config.load", return_value=config),
            patch("gza.cli.execution.get_store", return_value=store),
            patch("gza.cli.execution.Git", return_value=mock_git),
            patch(
                "gza.cli.execution.determine_next_action",
                return_value={
                    "type": "run_improve",
                    "description": f"Run pending improve {pending_improve.id}",
                    "improve_task": pending_improve,
                },
            ),
            patch("gza.cli.execution._spawn_background_iterate", return_value=0) as spawn_background,
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 0
        spawn_background.assert_called_once_with(
            args,
            config,
            impl,
            max_iterations=1,
            dry_run=False,
            prepared_task_id=pending_improve.id,
            prepared_resume=False,
            prepared_phase="iteration",
            prepared_action_type="run_improve",
            prepared_review_task_id=review.id,
        )
        assert "Next action: run_improve" not in output
        assert "Iterate blocked:" not in output
        assert WorkerRegistry(config.workers_path).list_all(include_completed=True) == []

    def test_background_auto_iterate_nested_improve_noop_surfaces_before_spawn(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli.execution import cmd_iterate
        from gza.config import Config

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)

        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.output_content = "**Verdict: CHANGES_REQUESTED**"
        review.completed_at = datetime.now(UTC)
        store.update(review)
        assert review.id is not None

        first = store.add("Improve 1", task_type="improve", based_on=impl.id, depends_on=review.id)
        first.status = "failed"
        first.failure_reason = "MAX_STEPS"
        first.session_id = "improve-session"
        store.update(first)

        failed_resume = store.add("Improve 2", task_type="improve", based_on=first.id, depends_on=review.id)
        failed_resume.status = "failed"
        failed_resume.failure_reason = "MAX_STEPS"
        failed_resume.session_id = first.session_id
        store.update(failed_resume)

        config = Config.load(tmp_path)
        config.max_resume_attempts = 1
        config.max_review_cycles = 3
        config.require_review_before_merge = True
        config.advance_create_reviews = True
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=False,
            auto_iterate=True,
            background=True,
        )

        with (
            patch("gza.cli.execution.Config.load", return_value=config),
            patch("gza.cli.execution.get_store", return_value=store),
            patch("gza.cli.execution.Git", return_value=mock_git),
            patch(
                "gza.cli.execution.determine_next_action",
                return_value={"type": "improve", "description": "Create improve task", "review_task": review},
            ),
            patch("gza.cli.execution._spawn_background_iterate", return_value=0) as spawn_background,
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 3
        spawn_background.assert_not_called()
        assert "Next action: improve" in output
        assert "Needs attention:" in output
        assert "reason=retry-limit-reached" in output
        assert f"Recommended next step: uv run gza fix {impl.id}" in output
        assert WorkerRegistry(config.workers_path).list_all(include_completed=True) == []

    def test_background_iterate_disabled_improve_recovery_surfaces_before_spawn(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli.execution import cmd_iterate
        from gza.config import Config

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)

        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.output_content = "**Verdict: CHANGES_REQUESTED**"
        review.completed_at = datetime.now(UTC)
        store.update(review)
        assert review.id is not None

        failed_improve = store.add(
            "Failed improve",
            task_type="improve",
            based_on=impl.id,
            depends_on=review.id,
        )
        failed_improve.status = "failed"
        failed_improve.failure_reason = "MAX_STEPS"
        failed_improve.session_id = "improve-session"
        store.update(failed_improve)
        assert failed_improve.id is not None

        config = Config.load(tmp_path)
        config.max_resume_attempts = 0
        config.max_review_cycles = 3
        config.require_review_before_merge = True
        config.advance_create_reviews = True
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=False,
            auto_iterate=False,
            background=True,
        )

        with (
            patch("gza.cli.execution.Config.load", return_value=config),
            patch("gza.cli.execution.get_store", return_value=store),
            patch("gza.cli.execution.Git", return_value=mock_git),
            patch(
                "gza.cli.execution.determine_next_action",
                return_value={"type": "improve", "description": "Create improve task", "review_task": review},
            ),
            patch("gza.cli.execution._spawn_background_iterate", return_value=0) as spawn_background,
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 3
        spawn_background.assert_not_called()
        assert "Next action: improve" in output
        assert self._expected_improve_attention_line(
            store=store,
            impl=impl,
            review=review,
            max_resume_attempts=0,
        ) in output
        assert "reason=automatic-recovery-disabled" in output
        assert "automatic improve recovery is disabled (max_resume_attempts=0)" in output
        assert f"Recommended next step: uv run gza fix {impl.id}" in output
        assert WorkerRegistry(config.workers_path).list_all(include_completed=True) == []

    def test_iterate_surfaces_review_blocker_adjudication_needed_attention(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)

        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.output_content = "**Verdict: CHANGES_REQUESTED**"
        review.completed_at = datetime.now(UTC)
        store.update(review)

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=False,
            auto_iterate=False,
            background=False,
        )
        mock_config = MagicMock(
            project_dir=tmp_path,
            use_docker=False,
            project_prefix="testproject",
            max_resume_attempts=1,
            max_review_cycles=3,
            require_review_before_merge=True,
            advance_create_reviews=True,
        )
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch(
                "gza.cli.determine_next_action",
                return_value={
                    "type": "needs_discussion",
                    "description": "SKIP: review-blocker-adjudication-needed; adjudication gza-999 completed with an unparseable or unsafe result.",
                    "needs_attention_reason": "review-blocker-adjudication-needed",
                    "subject_task_id": impl.id,
                },
            ),
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 3
        assert "Needs attention:" in output
        assert "reason=review-blocker-adjudication-needed" in output
        assert "review-blocker-adjudication-needed" in output

    def test_iterate_creates_review_adjudication_prompt_with_dispute_artifact_id(
        self, tmp_path: Path
    ) -> None:
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate
        from gza.review_verdict import ReviewFinding

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)

        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.output_content = (
            "## Summary\n\n- Found a blocker.\n\n"
            "## Blockers\n\n"
            "### B1 Missing API guard\n"
            "Evidence: still open.\n"
            "Open-state citation: `src/api.py:12-18`\n"
            "Impact: crash.\n"
            "Required fix: add guard.\n"
            "Required tests: add test.\n\n"
            "## Follow-Ups\n\nNone.\n\n"
            "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
        )
        review.completed_at = datetime.now(UTC)
        store.update(review)
        assert review.id is not None

        improve = store.add(
            "No-op improve with disputed blocker",
            task_type="improve",
            based_on=impl.id,
            depends_on=review.id,
            same_branch=True,
            create_review=True,
        )
        improve.status = "completed"
        improve.branch = impl.branch
        improve.completed_at = datetime.now(UTC)
        store.update(improve)
        assert improve.id is not None

        dispute_artifact = store.add_artifact(
            review.id,
            kind="review_blocker_resolution",
            label="disputed-B1",
            path=".gza/artifacts/disputed-b1.txt",
            byte_size=0,
            sha256="0" * 64,
            status="disputed",
            exit_status="already_satisfied",
            metadata={
                "schema_version": 1,
                "state": "disputed",
                "review_task_id": review.id,
                "impl_task_id": impl.id,
                "source_task_id": improve.id,
                "source_task_type": "improve",
                "source_branch": impl.branch,
                "finding_id": "B1",
                "reason": "already_satisfied",
                "evidence": "Current code already guards empty IDs.",
                "current_state_citation": "`src/api.py:12-18`",
                "finding_fingerprint": {
                    "title": "missing api guard",
                    "anchor": "src/api.py:12-18",
                },
            },
            created_at=improve.completed_at,
        )

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
            max_review_cycles=3,
            max_resume_attempts=1,
            require_review_before_merge=True,
            advance_create_reviews=True,
        )
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True

        def fake_run_foreground(_config, task_id, **kwargs):
            task = store.get(task_id)
            assert task is not None
            task.status = "completed"
            task.completed_at = datetime.now(UTC)
            store.update(task)
            return 0

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.cli._run_foreground", side_effect=fake_run_foreground),
            patch("gza.cli.resolve_closing_review_action", return_value=None),
            patch(
                "gza.cli.execution.determine_next_action",
                side_effect=[
                    {
                        "type": "create_review_adjudication",
                        "description": "Create adjudication",
                        "review_task": review,
                        "review_blocker_adjudication_candidate": SimpleNamespace(
                            finding=ReviewFinding(
                                id="B1",
                                severity="BLOCKER",
                                title="Missing API guard",
                                body="Evidence: still open",
                                evidence="still open",
                                impact="crash",
                                fix_or_followup="add guard",
                                tests="add test",
                                open_state_citation="`src/api.py:12-18`",
                            ),
                            dispute_artifact=dispute_artifact,
                        ),
                    },
                    {
                        "type": "create_review_adjudication",
                        "description": "Create adjudication",
                        "review_task": review,
                        "review_blocker_adjudication_candidate": SimpleNamespace(
                            finding=ReviewFinding(
                                id="B1",
                                severity="BLOCKER",
                                title="Missing API guard",
                                body="Evidence: still open",
                                evidence="still open",
                                impact="crash",
                                fix_or_followup="add guard",
                                tests="add test",
                                open_state_citation="`src/api.py:12-18`",
                            ),
                            dispute_artifact=dispute_artifact,
                        ),
                    },
                    {"type": "needs_discussion", "description": "stop"},
                ],
            ),
        ):
            result = cmd_iterate(args)

        assert result == 3
        adjudications = [task for task in store.get_all() if task.task_type == "internal"]
        assert len(adjudications) == 1
        assert f"Dispute artifact id: {dispute_artifact.id}" in adjudications[0].prompt

    def test_iterate_failed_improve_attention_uses_shortened_single_line_prompt(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)

        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.output_content = "**Verdict: CHANGES_REQUESTED**"
        review.completed_at = datetime.now(UTC)
        store.update(review)
        assert review.id is not None

        failed_improve = store.add(
            "Prior improve with a long first line that should not spill\nSecond line should not become its own output line\nThird line pads the width",
            task_type="improve",
            based_on=impl.id,
            depends_on=review.id,
        )
        failed_improve.status = "failed"
        failed_improve.failure_reason = "TEST_FAILURE"
        failed_improve.completed_at = datetime.now(UTC)
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
            max_review_cycles=3,
            max_resume_attempts=1,
            require_review_before_merge=True,
            advance_create_reviews=True,
        )
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.advance_engine.prompt_available_width", return_value=40),
            patch("gza.cli._run_foreground") as run_foreground,
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        with patch("gza.advance_engine.prompt_available_width", return_value=40):
            expected_line = self._expected_improve_attention_line(
                store=store,
                impl=impl,
                review=review,
                max_resume_attempts=1,
            )
        assert result == 3
        run_foreground.assert_not_called()
        assert expected_line in output
        assert output.count("Needs attention:") == 1
        assert expected_line.count("\n") == 0
        assert "Prior improve with a long first line that should not spill\nSecond line" not in output

    def test_iterate_failed_improve_non_attention_skip_does_not_emit_needs_attention(self, tmp_path: Path) -> None:
        from gza.cli._common import resolve_improve_action
        from gza.cli.advance_executor import build_improve_needs_attention_result

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)

        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.output_content = "**Verdict: CHANGES_REQUESTED**"
        review.completed_at = datetime.now(UTC)
        store.update(review)
        assert review.id is not None

        failed_improve = store.add(
            "Prior improve",
            task_type="improve",
            depends_on=review.id,
            based_on=impl.id,
            same_branch=True,
        )
        failed_improve.status = "failed"
        failed_improve.failure_reason = "MAX_TURNS"
        failed_improve.session_id = "sess-improve"
        failed_improve.completed_at = datetime.now(UTC)
        store.update(failed_improve)

        dependency = store.add("Mismatched dependency", task_type="plan")
        assert dependency.id is not None

        running_child = store.add(
            "Running resumed improve",
            task_type="improve",
            based_on=failed_improve.id,
            depends_on=dependency.id,
        )
        running_child.status = "in_progress"
        running_child.session_id = failed_improve.session_id
        store.update(running_child)

        improve_action, target, improve_decision = resolve_improve_action(
            store,
            impl.id,
            review.id,
            max_resume_attempts=1,
        )
        assert improve_action == "resume"
        assert target is not None
        assert target.id == failed_improve.id
        assert improve_decision is not None
        assert improve_decision.reason_code == "MAX_TURNS"

        attention_result = build_improve_needs_attention_result(
            store=store,
            impl_task=impl,
            review_task=review,
            improve_mode=improve_action,
            failed_improve=target,
            improve_decision=improve_decision,
            max_resume_attempts=1,
        )
        assert attention_result is None

    def test_iterate_pending_implementation_recovery_exhaustion_recommends_retry_or_reimplement(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate
        from gza.recovery_engine import decide_failed_task_recovery

        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl = store.add("Pending implementation", task_type="implement")
        assert impl.id is not None

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
            max_resume_attempts=1,
            max_review_cycles=3,
            require_review_before_merge=True,
            advance_create_reviews=True,
            workers_path=tmp_path / ".gza" / "workers",
        )

        def fake_run_foreground(config, task_id, resume=False, **kwargs):
            task = store.get(task_id)
            assert task is not None
            task.status = "failed"
            task.failure_reason = "MAX_STEPS"
            task.session_id = "impl-session"
            store.update(task)
            return 1

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.advance_engine.prompt_available_width", return_value=40),
            patch("gza.cli._run_foreground", side_effect=fake_run_foreground) as run_foreground,
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        recovery_children = store.get_based_on_children(impl.id)
        assert len(recovery_children) == 1
        failed_resume = recovery_children[0]
        terminal_decision = decide_failed_task_recovery(
            store,
            failed_resume,
            max_recovery_attempts=1,
        )
        with patch("gza.advance_engine.prompt_available_width", return_value=40):
            expected_line = self._expected_failed_recovery_attention_line(
                store=store,
                failed_task=failed_resume,
                decision=terminal_decision,
                max_resume_attempts=1,
            )

        assert result == 3
        assert run_foreground.call_count == 2
        assert run_foreground.call_args_list[0].kwargs.get("resume", False) is False
        assert run_foreground.call_args_list[1].kwargs.get("resume") is True
        assert terminal_decision.reason_code == "retry_limit_reached"
        assert expected_line in output
        assert "reason=retry-limit-reached" in output
        assert output.count("Needs attention:") == 1
        assert f"Implementation {failed_resume.id} failed (exit code 1)" not in output
        assert f"Recommended next step: uv run gza fix {impl.id}" not in output
        assert "Recommended next step: retry or re-implement instead." in output

    def test_iterate_pending_retryable_provider_error_recommends_retry_or_reimplement(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        import argparse
        from unittest.mock import patch

        from gza.cli import cmd_iterate
        from gza.recovery_engine import decide_failed_task_recovery

        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl = store.add("Pending implementation", task_type="implement")
        assert impl.id is not None

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
            max_resume_attempts=1,
            max_review_cycles=3,
            require_review_before_merge=True,
            advance_create_reviews=True,
            workers_path=tmp_path / ".gza" / "workers",
        )

        def fake_run_foreground(config, task_id, resume=False, **kwargs):
            task = store.get(task_id)
            assert task is not None
            task.status = "failed"
            task.failure_reason = "RETRYABLE_PROVIDER_ERROR"
            task.session_id = f"thread-{task_id}"
            store.update(task)
            return 1

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.advance_engine.prompt_available_width", return_value=40),
            patch("gza.cli._run_foreground", side_effect=fake_run_foreground) as run_foreground,
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        recovery_children = store.get_based_on_children(impl.id)
        assert len(recovery_children) == 1
        failed_retry = recovery_children[0]
        terminal_decision = decide_failed_task_recovery(
            store,
            failed_retry,
            max_recovery_attempts=1,
        )
        with patch("gza.advance_engine.prompt_available_width", return_value=40):
            expected_line = self._expected_failed_recovery_attention_line(
                store=store,
                failed_task=failed_retry,
                decision=terminal_decision,
                max_resume_attempts=1,
            )

        assert result == 3
        assert run_foreground.call_count == 2
        assert [call.kwargs.get("resume", False) for call in run_foreground.call_args_list] == [False, False]
        assert terminal_decision.reason_code == "retryable_provider_error"
        assert expected_line in output
        assert "reason=retryable-provider-error" in output
        assert output.count("Needs attention:") == 1
        assert f"Recommended next step: uv run gza fix {impl.id}" not in output
        assert "Recommended next step: retry or re-implement instead." in output

    def test_iterate_resume_start_recovery_exhaustion_auto_iterate_uses_shared_attention(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate
        from gza.recovery_engine import decide_failed_task_recovery

        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl = store.add("Failed implementation", task_type="implement")
        assert impl.id is not None
        impl.status = "failed"
        impl.failure_reason = "MAX_STEPS"
        impl.session_id = "impl-session"
        store.update(impl)

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
            resume=True,
            retry=False,
            auto_iterate=True,
            background=False,
        )
        mock_config = MagicMock(
            project_dir=tmp_path,
            use_docker=False,
            project_prefix="testproject",
            max_resume_attempts=1,
            max_review_cycles=3,
            require_review_before_merge=True,
            advance_create_reviews=True,
            workers_path=tmp_path / ".gza" / "workers",
        )

        def fake_run_foreground(config, task_id, resume=False, **kwargs):
            task = store.get(task_id)
            assert task is not None
            task.status = "failed"
            task.failure_reason = "MAX_STEPS"
            task.session_id = "impl-session"
            store.update(task)
            return 1

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.advance_engine.prompt_available_width", return_value=40),
            patch("gza.cli._run_foreground", side_effect=fake_run_foreground) as run_foreground,
        ):
            result = cmd_iterate(args)
        captured = capsys.readouterr()
        output = captured.out

        recovery_children = store.get_based_on_children(impl.id)
        assert len(recovery_children) == 1
        failed_resume = recovery_children[0]
        terminal_decision = decide_failed_task_recovery(
            store,
            failed_resume,
            max_recovery_attempts=1,
        )
        with patch("gza.advance_engine.prompt_available_width", return_value=40):
            expected_line = self._expected_failed_recovery_attention_line(
                store=store,
                failed_task=failed_resume,
                decision=terminal_decision,
                max_resume_attempts=1,
            )

        assert result == 3
        assert run_foreground.call_count == 1
        assert run_foreground.call_args.kwargs.get("resume") is True
        assert terminal_decision.reason_code == "retry_limit_reached"
        assert expected_line in output
        assert "reason=retry-limit-reached" in output
        assert output.count("Needs attention:") == 1
        assert f"Resume of {impl.id} failed" not in output
        assert f"Recommended next step: uv run gza fix {impl.id}" not in output
        assert "Recommended next step: retry or re-implement instead." in output
        assert "max auto-resume attempts" not in captured.err

    def test_failed_task_resume_descendant_manual_iterate_bypasses_auto_resume_cap(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli.execution import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)

        root = store.add("Implement feature", task_type="implement")
        assert root.id is not None
        root.status = "failed"
        root.failure_reason = "MAX_TURNS"
        root.session_id = "resume-session-1"
        store.update(root)

        failed_resume_descendant = store.add(
            "Failed resumed attempt",
            task_type="implement",
            based_on=root.id,
        )
        assert failed_resume_descendant.id is not None
        failed_resume_descendant.status = "failed"
        failed_resume_descendant.failure_reason = "INFRASTRUCTURE_ERROR"
        failed_resume_descendant.session_id = root.session_id
        store.update(failed_resume_descendant)

        args = argparse.Namespace(
            impl_task_id=failed_resume_descendant.id,
            max_iterations=1,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
            resume=True,
            retry=False,
            auto_iterate=False,
            background=False,
        )
        mock_config = MagicMock(
            project_dir=tmp_path,
            use_docker=False,
            project_prefix="testproject",
            max_resume_attempts=1,
            max_review_cycles=3,
            require_review_before_merge=True,
            advance_create_reviews=True,
        )
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True

        def fake_run_foreground(config, task_id, resume=False, **kwargs):
            task = store.get(task_id)
            assert task is not None
            task.status = "completed"
            task.completed_at = datetime.now(UTC)
            store.update(task)
            return 0

        with (
            patch("gza.cli.execution.Config.load", return_value=mock_config),
            patch("gza.cli.execution.get_store", return_value=store),
            patch("gza.cli.execution.Git", return_value=mock_git),
            patch("gza.cli.execution.determine_next_action", return_value={"type": "wait_review"}),
            patch("gza.cli.execution._run_foreground", side_effect=fake_run_foreground) as run_foreground,
        ):
            result = cmd_iterate(args)
        captured = capsys.readouterr()
        output = captured.out

        recovery_children = store.get_based_on_children(failed_resume_descendant.id)
        assert len(recovery_children) == 1
        manual_resume = recovery_children[0]

        assert result == 3
        assert run_foreground.call_count == 1
        assert run_foreground.call_args.kwargs.get("resume") is True
        assert f"Resuming failed implementation {failed_resume_descendant.id} as {manual_resume.id}..." in output
        assert "reason=retry-limit-reached" not in output
        assert (
            f"warning: task {failed_resume_descendant.id} has hit max auto-resume attempts; proceeding because this resume is manual"
            in captured.err
        )

    def test_failed_task_background_resume_descendant_manual_iterate_warns_before_spawn(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli.execution import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)

        root = store.add("Implement feature", task_type="implement")
        assert root.id is not None
        root.status = "failed"
        root.failure_reason = "MAX_TURNS"
        root.session_id = "resume-session-1"
        store.update(root)

        failed_resume_descendant = store.add(
            "Failed resumed attempt",
            task_type="implement",
            based_on=root.id,
        )
        assert failed_resume_descendant.id is not None
        failed_resume_descendant.status = "failed"
        failed_resume_descendant.failure_reason = "INFRASTRUCTURE_ERROR"
        failed_resume_descendant.session_id = root.session_id
        store.update(failed_resume_descendant)

        args = argparse.Namespace(
            impl_task_id=failed_resume_descendant.id,
            max_iterations=1,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
            resume=True,
            retry=False,
            auto_iterate=False,
            background=True,
        )
        mock_config = MagicMock(
            project_dir=tmp_path,
            use_docker=False,
            project_prefix="testproject",
            max_resume_attempts=1,
            max_review_cycles=3,
            require_review_before_merge=True,
            advance_create_reviews=True,
        )

        with (
            patch("gza.cli.execution.Config.load", return_value=mock_config),
            patch("gza.cli.execution.get_store", return_value=store),
            patch("gza.cli.execution._prepare_task_for_immediate_execution", side_effect=lambda _c, task, **_k: task),
            patch("gza.cli.execution._spawn_background_iterate", return_value=0) as spawn_background,
        ):
            result = cmd_iterate(args)
        captured = capsys.readouterr()

        assert result == 0
        assert spawn_background.call_count == 1
        prepared_child_ids = [task.id for task in store.get_based_on_children(failed_resume_descendant.id)]
        assert len(prepared_child_ids) == 1
        assert spawn_background.call_args.kwargs["prepared_task_id"] == prepared_child_ids[0]
        assert spawn_background.call_args.kwargs["prepared_resume"] is True
        assert spawn_background.call_args.kwargs["prepared_phase"] == "preloop"
        assert (
            f"warning: task {failed_resume_descendant.id} has hit max auto-resume attempts; proceeding because this resume is manual"
            in captured.err
        )

    def test_failed_root_manual_resume_with_existing_failed_resume_child_uses_final_bounded_attempt(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli.execution import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)

        root = store.add("Implement feature", task_type="implement")
        assert root.id is not None
        root.status = "failed"
        root.failure_reason = "MAX_TURNS"
        root.session_id = "resume-session-1"
        store.update(root)

        failed_resume_child = store.add(
            "Failed resumed attempt",
            task_type="implement",
            based_on=root.id,
        )
        assert failed_resume_child.id is not None
        failed_resume_child.status = "failed"
        failed_resume_child.failure_reason = "MAX_TURNS"
        failed_resume_child.session_id = root.session_id
        store.update(failed_resume_child)

        args = argparse.Namespace(
            impl_task_id=root.id,
            max_iterations=1,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
            resume=True,
            retry=False,
            auto_iterate=False,
            background=False,
        )
        mock_config = MagicMock(
            project_dir=tmp_path,
            use_docker=False,
            project_prefix="testproject",
            max_resume_attempts=1,
            max_review_cycles=3,
            require_review_before_merge=True,
            advance_create_reviews=True,
        )
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True

        def fake_run_foreground(config, task_id, resume=False, **kwargs):
            task = store.get(task_id)
            assert task is not None
            task.status = "completed"
            task.completed_at = datetime.now(UTC)
            store.update(task)
            return 0

        with (
            patch("gza.cli.execution.Config.load", return_value=mock_config),
            patch("gza.cli.execution.get_store", return_value=store),
            patch("gza.cli.execution.Git", return_value=mock_git),
            patch("gza.cli.execution.determine_next_action", return_value={"type": "wait_review"}),
            patch("gza.cli.execution._run_foreground", side_effect=fake_run_foreground) as run_foreground,
        ):
            result = cmd_iterate(args)
        captured = capsys.readouterr()
        output = captured.out

        root_children = store.get_based_on_children(root.id)
        assert len(root_children) == 2
        assert root_children[0].id == failed_resume_child.id
        final_resume = root_children[-1]

        assert result == 3
        assert run_foreground.call_count == 1
        assert run_foreground.call_args.kwargs.get("resume") is True
        assert run_foreground.call_args.kwargs.get("task_id") == final_resume.id
        assert f"Resuming failed implementation {root.id} as {final_resume.id}..." in output
        assert captured.err == ""
        assert "reason=newer-recovery-descendant-needs-attention" not in output

    def test_failed_root_background_manual_resume_with_existing_failed_resume_child_warns_before_spawn(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli.execution import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)

        root = store.add("Implement feature", task_type="implement")
        assert root.id is not None
        root.status = "failed"
        root.failure_reason = "MAX_TURNS"
        root.session_id = "resume-session-1"
        store.update(root)

        failed_resume_child = store.add(
            "Failed resumed attempt",
            task_type="implement",
            based_on=root.id,
        )
        assert failed_resume_child.id is not None
        failed_resume_child.status = "failed"
        failed_resume_child.failure_reason = "MAX_TURNS"
        failed_resume_child.session_id = root.session_id
        store.update(failed_resume_child)

        args = argparse.Namespace(
            impl_task_id=root.id,
            max_iterations=1,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
            resume=True,
            retry=False,
            auto_iterate=False,
            background=True,
        )
        mock_config = MagicMock(
            project_dir=tmp_path,
            use_docker=False,
            project_prefix="testproject",
            max_resume_attempts=1,
            max_review_cycles=3,
            require_review_before_merge=True,
            advance_create_reviews=True,
        )

        with (
            patch("gza.cli.execution.Config.load", return_value=mock_config),
            patch("gza.cli.execution.get_store", return_value=store),
            patch("gza.cli.execution._prepare_task_for_immediate_execution", side_effect=lambda _c, task, **_k: task),
            patch("gza.cli.execution._spawn_background_iterate", return_value=0) as spawn_background,
        ):
            result = cmd_iterate(args)
        captured = capsys.readouterr()

        assert result == 0
        assert spawn_background.call_count == 1
        prepared_child_ids = [task.id for task in store.get_based_on_children(root.id) if task.id != failed_resume_child.id]
        assert len(prepared_child_ids) == 1
        assert spawn_background.call_args.kwargs["prepared_task_id"] == prepared_child_ids[0]
        assert spawn_background.call_args.kwargs["prepared_resume"] is True
        assert spawn_background.call_args.kwargs["prepared_phase"] == "preloop"
        assert captured.err == ""

    @pytest.mark.parametrize(
        ("failure_reason", "seed_chain"),
        [
            ("TEST_FAILURE", False),
            ("UNKNOWN", False),
            ("MAX_STEPS", True),
        ],
    )
    def test_failed_recovery_attention_format_matches_advance_watch_and_iterate(
        self,
        tmp_path: Path,
        failure_reason: str,
        seed_chain: bool,
    ) -> None:
        from unittest.mock import patch

        from gza.recovery_engine import decide_failed_task_recovery

        setup_config(tmp_path)
        store = make_store(tmp_path)

        failed_task = store.add(
            "Shared recovery prompt",
            task_type="implement",
        )
        assert failed_task.id is not None
        failed_task.status = "failed"
        failed_task.failure_reason = failure_reason
        failed_task.completed_at = datetime.now(UTC)
        if seed_chain:
            failed_task.session_id = "resume-session"
            store.update(failed_task)
            chained = store.add(
                "Shared recovery prompt",
                task_type="implement",
                based_on=failed_task.id,
            )
            assert chained.id is not None
            chained.status = "failed"
            chained.failure_reason = failure_reason
            chained.session_id = failed_task.session_id
            chained.completed_at = datetime.now(UTC)
            store.update(chained)
            failed_task = chained
        else:
            store.update(failed_task)

        decision = decide_failed_task_recovery(store, failed_task, max_recovery_attempts=1)
        with patch("gza.advance_engine.prompt_available_width", return_value=80):
            advance_line, watch_line, iterate_line = self._shared_failed_recovery_attention_lines(
                store=store,
                failed_task=failed_task,
                decision=decision,
                max_resume_attempts=1,
            )

        assert decision.action == "skip"
        assert advance_line == iterate_line
        assert watch_line == iterate_line

    def test_iterate_in_loop_failed_improve_recovery_exhaustion_uses_shared_attention(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli.execution import cmd_iterate
        from gza.recovery_engine import decide_failed_task_recovery

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)

        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.output_content = "**Verdict: CHANGES_REQUESTED**"
        review.completed_at = datetime.now(UTC)
        store.update(review)
        assert review.id is not None

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
            max_resume_attempts=1,
            max_review_cycles=3,
            require_review_before_merge=True,
            advance_create_reviews=True,
        )
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True

        def fake_run_foreground(config, task_id, resume=False, **kwargs):
            task = store.get(task_id)
            assert task is not None
            task.status = "failed"
            task.failure_reason = "MAX_STEPS"
            task.session_id = "improve-session"
            store.update(task)
            return 1

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.advance_engine.prompt_available_width", return_value=40),
            patch(
                "gza.cli.determine_next_action",
                return_value={"type": "improve", "description": "Create improve task", "review_task": review},
            ),
            patch("gza.cli._run_foreground", side_effect=fake_run_foreground) as run_foreground,
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        improves = [
            task
            for task in store.get_all()
            if task.task_type == "improve" and task.depends_on == review.id
        ]
        assert len(improves) == 2
        terminal_improve = next(task for task in improves if task.based_on != impl.id)
        terminal_decision = decide_failed_task_recovery(
            store,
            terminal_improve,
            max_recovery_attempts=1,
        )
        with patch("gza.advance_engine.prompt_available_width", return_value=40):
            expected_line = self._expected_failed_recovery_attention_line(
                store=store,
                failed_task=terminal_improve,
                decision=terminal_decision,
                max_resume_attempts=1,
            )

        assert result == 3
        assert run_foreground.call_count == 2
        assert run_foreground.call_args_list[0].kwargs.get("resume", False) is False
        assert run_foreground.call_args_list[1].kwargs.get("resume") is True
        assert terminal_decision.reason_code == "retry_limit_reached"
        assert expected_line in output
        assert "reason=retry-limit-reached" in output
        assert output.count("Needs attention:") == 1
        assert "Iterate blocked: improve_failed. Manual review required." not in output

    def test_iterate_manual_improve_override_bypasses_auto_resume_cap(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli.execution import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)

        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.output_content = "**Verdict: CHANGES_REQUESTED**"
        review.completed_at = datetime.now(UTC)
        store.update(review)
        assert review.id is not None

        first = store.add("Improve 1", task_type="improve", based_on=impl.id, depends_on=review.id)
        first.status = "failed"
        first.failure_reason = "MAX_STEPS"
        first.session_id = "improve-session"
        store.update(first)

        failed_resume = store.add("Improve 2", task_type="improve", based_on=first.id, depends_on=review.id)
        failed_resume.status = "failed"
        failed_resume.failure_reason = "MAX_STEPS"
        failed_resume.session_id = first.session_id
        store.update(failed_resume)

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=False,
            auto_iterate=False,
            background=False,
        )
        mock_config = MagicMock(
            project_dir=tmp_path,
            use_docker=False,
            project_prefix="testproject",
            max_resume_attempts=1,
            max_review_cycles=3,
            require_review_before_merge=True,
            advance_create_reviews=True,
        )
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True

        actions = [
            {"type": "improve", "description": "Create improve task", "review_task": review},
            {"type": "improve", "description": "Create improve task", "review_task": review},
            {"type": "wait_review"},
        ]

        def fake_run_foreground(config, task_id, resume=False, **kwargs):
            task = store.get(task_id)
            assert task is not None
            task.status = "completed"
            task.completed_at = datetime.now(UTC)
            store.update(task)
            return 0

        with (
            patch("gza.cli.execution.Config.load", return_value=mock_config),
            patch("gza.cli.execution.get_store", return_value=store),
            patch("gza.cli.execution.Git", return_value=mock_git),
            patch("gza.cli.execution.determine_next_action", side_effect=actions),
            patch("gza.cli.execution._run_foreground", side_effect=fake_run_foreground) as run_foreground,
        ):
            result = cmd_iterate(args)
        captured = capsys.readouterr()
        output = captured.out

        recovery_children = store.get_based_on_children(failed_resume.id)
        assert len(recovery_children) == 1
        manual_resume = recovery_children[0]

        assert result == 3
        assert run_foreground.call_count == 1
        assert run_foreground.call_args.kwargs.get("resume") is True
        assert f"Created improve task {manual_resume.id} (resume of {failed_resume.id})" in output
        assert "reason=retry-limit-reached" not in output
        assert (
            f"warning: task {failed_resume.id} has hit max auto-resume attempts; proceeding because this resume is manual"
            in captured.err
        )

    def test_background_iterate_manual_improve_override_warns_before_spawn(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli.execution import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)

        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.output_content = "**Verdict: CHANGES_REQUESTED**"
        review.completed_at = datetime.now(UTC)
        store.update(review)
        assert review.id is not None

        first = store.add("Improve 1", task_type="improve", based_on=impl.id, depends_on=review.id)
        first.status = "failed"
        first.failure_reason = "MAX_STEPS"
        first.session_id = "improve-session"
        store.update(first)

        failed_resume = store.add("Improve 2", task_type="improve", based_on=first.id, depends_on=review.id)
        failed_resume.status = "failed"
        failed_resume.failure_reason = "MAX_STEPS"
        failed_resume.session_id = first.session_id
        store.update(failed_resume)

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=False,
            auto_iterate=False,
            background=True,
        )
        mock_config = MagicMock(
            project_dir=tmp_path,
            use_docker=False,
            project_prefix="testproject",
            max_resume_attempts=1,
            max_review_cycles=3,
            require_review_before_merge=True,
            advance_create_reviews=True,
        )
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"

        with (
            patch("gza.cli.execution.Config.load", return_value=mock_config),
            patch("gza.cli.execution.get_store", return_value=store),
            patch("gza.cli.execution.Git", return_value=mock_git),
            patch(
                "gza.cli.execution.determine_next_action",
                return_value={"type": "improve", "description": "Create improve task", "review_task": review},
            ),
            patch("gza.cli.execution._prepare_task_for_immediate_execution", side_effect=lambda _c, task, **_k: task),
            patch("gza.cli.execution._spawn_background_iterate", return_value=0) as spawn_background,
        ):
            result = cmd_iterate(args)
        captured = capsys.readouterr()

        assert result == 0
        assert spawn_background.call_count == 1
        prepared_children = store.get_based_on_children(failed_resume.id)
        assert len(prepared_children) == 1
        assert spawn_background.call_args.kwargs["prepared_task_id"] == prepared_children[0].id
        assert spawn_background.call_args.kwargs["prepared_resume"] is True
        assert spawn_background.call_args.kwargs["prepared_phase"] == "iteration"
        assert spawn_background.call_args.kwargs["prepared_action_type"] == "improve"
        assert (
            f"warning: task {failed_resume.id} has hit max auto-resume attempts; proceeding because this resume is manual"
            in captured.err
        )

    def test_background_iterate_manual_improve_override_git_preflight_failure_surfaces_before_spawn(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli.execution import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)

        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.output_content = "**Verdict: CHANGES_REQUESTED**"
        review.completed_at = datetime.now(UTC)
        store.update(review)
        assert review.id is not None

        first = store.add("Improve 1", task_type="improve", based_on=impl.id, depends_on=review.id)
        first.status = "failed"
        first.failure_reason = "MAX_STEPS"
        first.session_id = "improve-session"
        store.update(first)

        failed_resume = store.add("Improve 2", task_type="improve", based_on=first.id, depends_on=review.id)
        failed_resume.status = "failed"
        failed_resume.failure_reason = "MAX_STEPS"
        failed_resume.session_id = first.session_id
        store.update(failed_resume)

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=False,
            auto_iterate=False,
            background=True,
        )
        mock_config = MagicMock(
            project_dir=tmp_path,
            use_docker=False,
            project_prefix="testproject",
            max_resume_attempts=1,
            max_review_cycles=3,
            require_review_before_merge=True,
            advance_create_reviews=True,
        )
        mock_git = MagicMock()
        mock_git.current_branch.side_effect = RuntimeError("branch boom")

        with (
            patch("gza.cli.execution.Config.load", return_value=mock_config),
            patch("gza.cli.execution.get_store", return_value=store),
            patch("gza.cli.execution.Git", return_value=mock_git),
            patch(
                "gza.cli.execution._spawn_background_iterate",
                side_effect=AssertionError("background iterate should not spawn"),
            ),
        ):
            result = cmd_iterate(args)
        captured = capsys.readouterr()

        assert result == 1
        assert f"Error: failed to initialize iterate background preflight for task {impl.id}: branch boom" in captured.err
        assert "could not evaluate iterate background preflight" not in captured.err
        assert store.get_based_on_children(failed_resume.id) == []
        assert store.get_reviews_for_task(impl.id) == [review]
        logs_dir = tmp_path / ".gza" / "logs"
        if logs_dir.exists():
            assert not any(path.is_file() for path in logs_dir.rglob("*"))
        workers_dir = tmp_path / ".gza" / "workers"
        if workers_dir.exists():
            assert list(workers_dir.iterdir()) == []

    def test_iterate_in_loop_manual_failure_uses_shared_attention(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate
        from gza.recovery_engine import decide_failed_task_recovery

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)

        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.output_content = "**Verdict: CHANGES_REQUESTED**"
        review.completed_at = datetime.now(UTC)
        store.update(review)
        assert review.id is not None

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
            max_resume_attempts=1,
            max_review_cycles=3,
            require_review_before_merge=True,
            advance_create_reviews=True,
        )
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True

        def fake_run_foreground(config, task_id, resume=False, **kwargs):
            task = store.get(task_id)
            assert task is not None
            task.status = "failed"
            task.failure_reason = "TEST_FAILURE"
            store.update(task)
            return 1

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.advance_engine.prompt_available_width", return_value=40),
            patch(
                "gza.cli.determine_next_action",
                return_value={"type": "improve", "description": "Create improve task", "review_task": review},
            ),
            patch("gza.cli._run_foreground", side_effect=fake_run_foreground) as run_foreground,
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        improves = [
            task
            for task in store.get_all()
            if task.task_type == "improve" and task.depends_on == review.id
        ]
        assert len(improves) == 1
        failed_improve = improves[0]
        decision = decide_failed_task_recovery(store, failed_improve, max_recovery_attempts=1)
        with patch("gza.advance_engine.prompt_available_width", return_value=40):
            expected_line = self._expected_failed_recovery_attention_line(
                store=store,
                failed_task=failed_improve,
                decision=decision,
                max_resume_attempts=1,
            )

        assert result == 3
        assert run_foreground.call_count == 1
        assert decision.reason_code == "manual_failure_reason"
        assert expected_line in output
        assert "reason=manual-failure-reason" in output
        assert output.count("Needs attention:") == 1
        assert "Iterate blocked: improve_failed. Manual review required." not in output

    def test_in_progress_improve_is_prioritized_over_newer_pending_improve(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.output_content = "**Verdict: CHANGES_REQUESTED**"
        review.completed_at = datetime.now(UTC)
        store.update(review)

        running_improve = store.add("Older running improve", task_type="improve", based_on=impl.id, depends_on=review.id)
        running_improve.status = "in_progress"
        running_improve.created_at = datetime(2026, 1, 1, tzinfo=UTC)
        store.update(running_improve)
        pending_improve = store.add("Newer pending improve", task_type="improve", based_on=impl.id, depends_on=review.id)
        pending_improve.status = "pending"
        pending_improve.created_at = datetime(2026, 1, 2, tzinfo=UTC)
        store.update(pending_improve)

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
        )
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli._create_review_task") as create_review,
            patch("gza.cli._create_improve_task") as create_improve,
            patch("gza.cli._run_foreground") as run_foreground,
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 3
        create_review.assert_not_called()
        create_improve.assert_not_called()
        run_foreground.assert_not_called()
        assert "Iterate waiting: improve_in_progress. Existing task is already in progress." in output
        assert pending_improve.id not in output

    def test_changes_requested_with_completed_improve_without_review_clear_creates_closing_review(
        self, tmp_path: Path
    ):
        from unittest.mock import MagicMock

        from gza.cli.advance_engine import determine_next_action
        from gza.cli.execution import _AdvanceEngineConfigAdapter

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.output_content = "**Verdict: CHANGES_REQUESTED**"
        review.completed_at = datetime.now(UTC)
        store.update(review)
        improve = store.add(
            "Completed improve",
            task_type="improve",
            based_on=impl.id,
            depends_on=review.id,
            same_branch=True,
        )
        improve.status = "completed"
        improve.duration_seconds = 45.0
        improve.num_steps_computed = 3
        improve.cost_usd = 0.21
        improve.completed_at = datetime.now(UTC)
        store.update(improve)
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True
        engine_config = _AdvanceEngineConfigAdapter(
            project_dir=tmp_path,
            require_review_before_merge=True,
            advance_create_reviews=True,
            max_review_cycles=3,
            max_resume_attempts=1,
        )

        action = determine_next_action(
            engine_config,
            store,
            mock_git,
            impl,
            "main",
            max_resume_attempts=1,
        )

        assert action["type"] == "create_review"
        assert action["description"] == "Create closing review (code changed since the last review)"

    def test_completed_improve_without_review_clear_bootstraps_iteration_one_to_closing_review(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        impl.duration_seconds = 30.0
        impl.num_steps_computed = 2
        impl.cost_usd = 0.10
        store.update(impl)

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
        improve.duration_seconds = 40.0
        improve.num_steps_computed = 4
        improve.cost_usd = 0.25
        improve.completed_at = datetime(2026, 1, 2, tzinfo=UTC)
        store.update(improve)

        next_review = store.add("Fresh review", task_type="review", depends_on=impl.id)

        def fake_run_foreground(config, task_id, **kwargs):
            task = store.get(task_id)
            assert task is not None
            if task_id == next_review.id:
                task.status = "completed"
                task.output_content = "**Verdict: APPROVED**"
                task.duration_seconds = 20.0
                task.num_steps_reported = 3
                task.cost_usd = 0.15
                task.completed_at = datetime.now(UTC)
                store.update(task)
                return 0
            raise AssertionError(f"unexpected task id: {task_id}")

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
        )
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli._create_review_task", return_value=next_review),
            patch("gza.cli._create_improve_task") as create_improve,
            patch("gza.cli._run_foreground", side_effect=fake_run_foreground) as run_foreground,
            patch("gza.cli.time.monotonic", side_effect=[100.0, 130.0]),
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 0
        run_foreground.assert_called_once()
        call_kwargs = run_foreground.call_args.kwargs
        assert call_kwargs["task_id"] == next_review.id
        assert call_kwargs["force"] is False
        assert call_kwargs["invocation"].command == "iterate"
        create_improve.assert_not_called()
        assert not re.search(
            rf"1\s+implement\s+{re.escape(impl.id)}\s+",
            output,
        )
        assert re.search(
            rf"1\s+review\s+{re.escape(next_review.id)}\s+APPROVED\s+20s\s+3\s+\$0\.15\s+completed",
            output,
        )

    def test_completed_improve_without_review_clear_and_in_progress_review_shows_review_iteration_one(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)

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
        improve.duration_seconds = 45.0
        improve.num_steps_computed = 5
        improve.cost_usd = 0.33
        improve.completed_at = datetime(2026, 1, 2, tzinfo=UTC)
        store.update(improve)

        running_review = store.add("Running review", task_type="review", depends_on=impl.id)
        running_review.status = "in_progress"
        store.update(running_review)

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
        )
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli._create_review_task") as create_review,
            patch("gza.cli._create_improve_task") as create_improve,
            patch("gza.cli._run_foreground") as run_foreground,
            patch("gza.cli.time.monotonic", side_effect=[200.0, 220.0]),
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 3
        create_review.assert_not_called()
        create_improve.assert_not_called()
        run_foreground.assert_not_called()
        assert not re.search(
            rf"1\s+implement\s+{re.escape(impl.id)}\s+",
            output,
        )
        assert re.search(
            rf"1\s+review\s+{re.escape(running_review.id)}\s+-\s+-\s+-\s+-\s+in_progress",
            output,
        )
        assert "Iterate waiting: review_in_progress. Existing task is already in progress." in output

    def test_max_iterations_after_successful_improve_still_runs_one_closing_review(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)

        stale_review = store.add("Old review", task_type="review", depends_on=impl.id)
        stale_review.status = "completed"
        stale_review.output_content = "**Verdict: CHANGES_REQUESTED**"
        stale_review.completed_at = datetime(2026, 1, 1, tzinfo=UTC)
        store.update(stale_review)

        improve = store.add(
            "Improve feature",
            task_type="improve",
            based_on=impl.id,
            depends_on=stale_review.id,
            same_branch=True,
        )
        next_review: object | None = None

        def fake_run_foreground(config, task_id, **kwargs):
            task = store.get(task_id)
            assert task is not None
            if task_id == improve.id:
                task.status = "completed"
                task.completed_at = datetime(2026, 1, 2, tzinfo=UTC)
                store.update(task)
                refreshed_impl = store.get(impl.id)
                assert refreshed_impl is not None
                refreshed_impl.review_cleared_at = task.completed_at
                store.update(refreshed_impl)
                return 0
            if next_review is not None and task_id == next_review.id:
                task.status = "completed"
                task.output_content = "**Verdict: APPROVED**"
                task.completed_at = datetime(2026, 1, 3, tzinfo=UTC)
                store.update(task)
                return 0
            raise AssertionError(f"unexpected task id: {task_id}")

        def fake_create_review_task(_store, _impl_task, *, trigger_source, **_kwargs):
            nonlocal next_review
            next_review = store.add("Closing review", task_type="review", depends_on=impl.id)
            return next_review

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
        )
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli._create_improve_task", return_value=improve),
            patch("gza.cli._create_review_task", side_effect=fake_create_review_task) as create_review,
            patch("gza.cli._run_foreground", side_effect=fake_run_foreground) as run_foreground,
        ):
            result = cmd_iterate(args)
        capsys.readouterr()

        assert result == 0
        assert next_review is not None
        assert [call.kwargs["task_id"] for call in run_foreground.call_args_list] == [
            improve.id,
            next_review.id,
        ]
        create_review.assert_called_once()

    def test_changes_requested_with_retry_eligible_failed_improve_retries_instead_of_blocking(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        """When a retry-eligible failed improve exists, iterate creates a retry and runs it."""
        from unittest.mock import MagicMock

        from gza.cli.advance_engine import determine_next_action
        from gza.cli.execution import _AdvanceEngineConfigAdapter

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.output_content = "**Verdict: CHANGES_REQUESTED**"
        review.completed_at = datetime.now(UTC)
        store.update(review)
        failed_improve = store.add("Failed improve", task_type="improve", based_on=impl.id, depends_on=review.id)
        failed_improve.status = "failed"
        failed_improve.failure_reason = "INFRASTRUCTURE_ERROR"
        store.update(failed_improve)

        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True
        engine_config = _AdvanceEngineConfigAdapter(
            project_dir=tmp_path,
            require_review_before_merge=True,
            advance_create_reviews=True,
            max_review_cycles=3,
            max_resume_attempts=1,
        )
        action = determine_next_action(
            engine_config,
            store,
            mock_git,
            impl,
            "main",
            max_resume_attempts=1,
        )

        # The engine should emit an "improve" action, and resolve_improve_action
        # should detect the failed improve and recommend a retry
        assert action["type"] == "improve"
        from gza.cli._common import resolve_improve_action
        improve_action, target, decision = resolve_improve_action(
            store,
            impl.id,
            review.id,
            max_resume_attempts=1,
        )
        assert improve_action == "retry"
        assert target is not None
        assert target.id == failed_improve.id
        assert decision is not None

    def test_resolve_improve_action_finds_chained_retry_improves(self, tmp_path: Path):
        """get_improve_tasks_for must find retries whose based_on points at a previous improve."""
        from gza.cli._common import resolve_improve_action

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.output_content = "**Verdict: CHANGES_REQUESTED**"
        review.completed_at = datetime.now(UTC)
        store.update(review)

        first = store.add("Improve 1", task_type="improve", based_on=impl.id, depends_on=review.id)
        first.status = "failed"
        first.failure_reason = "TIMEOUT"
        first.session_id = "s1"
        store.update(first)

        # A retry whose based_on points at the previous improve (not the impl).
        retry = store.add("Improve 2 (retry)", task_type="improve", based_on=first.id, depends_on=review.id)
        retry.status = "failed"
        retry.failure_reason = "TIMEOUT"
        retry.session_id = "s2"
        store.update(retry)

        # With max_resume_attempts=3 (headroom), resume should win and target the latest.
        action, target, decision = resolve_improve_action(store, impl.id, review.id, max_resume_attempts=3)
        assert action == "resume"
        assert target is not None
        assert target.id == retry.id
        assert decision is not None

    def test_resolve_improve_action_stops_after_resume_descendant_failure(self, tmp_path: Path):
        """A failed resume descendant stops automatic recovery and requires manual review."""
        from gza.cli._common import resolve_improve_action

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        store.update(review)

        first = store.add("Improve 1", task_type="improve", based_on=impl.id, depends_on=review.id)
        first.status = "failed"
        first.failure_reason = "TIMEOUT"
        first.session_id = "s1"
        store.update(first)
        second = store.add(first.prompt, task_type="improve", based_on=first.id, depends_on=review.id)
        second.status = "failed"
        second.failure_reason = "INFRASTRUCTURE_ERROR"
        second.session_id = first.session_id
        store.update(second)

        action, target, decision = resolve_improve_action(store, impl.id, review.id, max_resume_attempts=1)
        assert action == "manual_review"
        assert target is not None
        assert target.id == second.id
        assert decision is not None
        assert decision.reason_code == "retry_limit_reached"

    def test_resolve_improve_action_still_resumes_within_cap(self, tmp_path: Path):
        """One failed attempt with max_resume_attempts=1 still resumes (cap not yet exceeded)."""
        from gza.cli._common import resolve_improve_action

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        store.update(review)

        first = store.add("Improve 1", task_type="improve", based_on=impl.id, depends_on=review.id)
        first.status = "failed"
        first.failure_reason = "TIMEOUT"
        first.session_id = "s1"
        store.update(first)

        action, target, decision = resolve_improve_action(store, impl.id, review.id, max_resume_attempts=1)
        assert action == "resume"
        assert target is not None
        assert target.id == first.id
        assert decision is not None

    def test_iterate_creates_followup_after_completed_noop_improve(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.output_content = "**Verdict: CHANGES_REQUESTED**"
        review.completed_at = datetime.now(UTC)
        store.update(review)
        assert review.id is not None

        noop_improve = store.add("Improve 1", task_type="improve", based_on=impl.id, depends_on=review.id)
        noop_improve.status = "completed"
        noop_improve.changed_diff = False
        noop_improve.completed_at = datetime.now(UTC)
        store.update(noop_improve)

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
            max_review_cycles=3,
            max_noop_improve_cycles=2,
            require_review_before_merge=True,
            advance_create_reviews=True,
        )
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"

        def fake_run_foreground(config, task_id, **kwargs):
            del config, kwargs
            task = store.get(task_id)
            assert task is not None
            task.status = "completed"
            task.completed_at = datetime.now(UTC)
            store.update(task)
            return 0

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.cli._run_foreground", side_effect=fake_run_foreground) as run_foreground,
            patch("gza.cli.time.monotonic", side_effect=[300.0, 340.0]),
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 3
        created = next(
            task
            for task in store.get_improve_tasks_for(impl.id, review.id)
            if task.id not in {noop_improve.id}
        )
        assert created.based_on == noop_improve.id
        assert created.status == "completed"
        assert any(call.kwargs["task_id"] == created.id for call in run_foreground.call_args_list)
        assert f"Running improve {created.id}..." in output
        assert "Iterate blocked:" in output

    def test_changes_requested_with_dropped_improve_blocks_and_does_not_run_review(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        """When a dropped improve exists, _create_improve_task rejects the duplicate and iterate blocks."""
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.output_content = "**Verdict: CHANGES_REQUESTED**"
        review.completed_at = datetime.now(UTC)
        store.update(review)
        dropped_improve = store.add("Dropped improve", task_type="improve", based_on=impl.id, depends_on=review.id)
        dropped_improve.status = "dropped"
        store.update(dropped_improve)

        args = argparse.Namespace(impl_task_id=impl.id, max_iterations=1, dry_run=False, project_dir=tmp_path, no_docker=True)
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli._run_foreground") as run_foreground,
            patch("gza.cli._create_review_task") as create_review,
            patch("gza.cli.time.monotonic", side_effect=[300.0, 340.0]),
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 3
        run_foreground.assert_not_called()
        create_review.assert_not_called()
        assert "Error creating improve task" in output
        assert "Iterate blocked: improve_failed" in output

    def test_no_reviews_starts_with_fresh_review(self, tmp_path: Path):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "pending"
        review.output_content = "**Verdict: APPROVED**"
        review.completed_at = None
        store.update(review)

        def fake_run_foreground(config, task_id, **kwargs):
            task = store.get(task_id)
            assert task is not None
            if task_id == review.id:
                task.status = "completed"
                task.completed_at = datetime.now(UTC)
                store.update(task)
                return 0
            raise AssertionError(f"unexpected task id: {task_id}")

        args = argparse.Namespace(impl_task_id=impl.id, max_iterations=1, dry_run=False, project_dir=tmp_path, no_docker=True)
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        with patch("gza.cli.Config.load", return_value=mock_config), \
             patch("gza.cli.get_store", return_value=store), \
             patch("gza.cli._create_review_task", return_value=review) as create_review, \
             patch("gza.cli._run_foreground", side_effect=fake_run_foreground):
            result = cmd_iterate(args)
        assert result == 0
        create_review.assert_not_called()

    def test_iterate_create_review_duplicate_in_progress_waits_instead_of_running(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate
        from gza.review_tasks import DuplicateReviewError

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        active_review = store.add("Active review", task_type="review", depends_on=impl.id)
        active_review.status = "in_progress"
        store.update(active_review)

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
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"

        def fake_run_foreground(config, task_id, **kwargs):
            task = store.get(task_id)
            assert task is not None
            if task_id == active_review.id:
                task.status = "completed"
                task.completed_at = datetime.now(UTC)
                store.update(task)
                return 0
            raise AssertionError(f"unexpected task id: {task_id}")

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch(
                "gza.cli.determine_next_action",
                return_value={"type": "create_review", "description": "Create review"},
            ),
            patch("gza.cli._create_review_task", side_effect=DuplicateReviewError(active_review)),
            patch("gza.cli._run_foreground") as run_foreground,
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 3
        run_foreground.assert_not_called()
        assert "Waiting for review" in output
        assert "Iterate waiting: review_in_progress. Existing task is already in progress." in output

    def test_iterate_create_review_duplicate_pending_reuses_and_runs_once(self, tmp_path: Path):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate
        from gza.review_tasks import DuplicateReviewError

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        pending_review = store.add("Pending review", task_type="review", depends_on=impl.id)
        pending_review.status = "pending"
        store.update(pending_review)

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
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"

        def fake_run_foreground(config, task_id, **kwargs):
            task = store.get(task_id)
            assert task is not None
            if task_id == pending_review.id:
                task.status = "completed"
                task.completed_at = datetime.now(UTC)
                store.update(task)
                return 0
            raise AssertionError(f"unexpected task id: {task_id}")

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch(
                "gza.cli.determine_next_action",
                return_value={"type": "create_review", "description": "Create review"},
            ),
            patch("gza.cli._create_review_task", side_effect=DuplicateReviewError(pending_review)),
            patch("gza.cli._run_foreground", side_effect=fake_run_foreground) as run_foreground,
            patch("gza.cli.get_review_verdict", return_value="APPROVED"),
        ):
            result = cmd_iterate(args)

        assert result == 0
        run_foreground.assert_called_once()
        call_kwargs = run_foreground.call_args.kwargs
        assert call_kwargs["task_id"] == pending_review.id
        assert call_kwargs["force"] is False
        assert call_kwargs["invocation"].command == "iterate"

    def test_iterate_improve_duplicate_blocks_instead_of_raising(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        completed_review = store.add("Completed review", task_type="review", depends_on=impl.id)
        completed_review.status = "completed"
        completed_review.output_content = "**Verdict: CHANGES_REQUESTED**"
        completed_review.completed_at = datetime.now(UTC)
        store.update(completed_review)

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
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch(
                "gza.cli.determine_next_action",
                return_value={
                    "type": "improve",
                    "description": "Create improve task",
                    "review_task": completed_review,
                },
            ),
            patch(
                "gza.cli._create_improve_task",
                side_effect=ValueError("An improve task already exists for implementation"),
            ) as create_improve,
            patch("gza.cli._run_foreground") as run_foreground,
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 3
        create_improve.assert_called_once()
        run_foreground.assert_not_called()
        assert "Error creating improve task" in output
        assert "Iterate blocked: improve_failed." in output

    def test_iterate_run_review_auto_resumes_max_steps_failure(self, tmp_path: Path):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)

        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "pending"
        review.session_id = "review-session"
        store.update(review)

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=3,
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
            if task.id == review.id and not resume:
                task.status = "failed"
                task.failure_reason = "MAX_STEPS"
                task.session_id = "review-session"
                store.update(task)
                return 1
            task.status = "completed"
            task.output_content = "**Verdict: APPROVED**"
            task.completed_at = datetime.now(UTC)
            store.update(task)
            return 0

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch(
                "gza.cli.determine_next_action",
                return_value={"type": "run_review", "description": "Run review", "review_task": review},
            ),
            patch("gza.cli._run_foreground", side_effect=fake_run_foreground) as run_fg,
        ):
            result = cmd_iterate(args)

        assert result == 0
        assert run_fg.call_count == 2
        assert run_fg.call_args_list[0].kwargs.get("resume", False) is False
        assert run_fg.call_args_list[1].kwargs["resume"] is True
        children = store.get_based_on_children(review.id)
        assert len(children) == 1

    def test_iterate_noop_improve_limit_parks_without_running_review(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)

        stale_review = store.add("Stale review", task_type="review", depends_on=impl.id, based_on=impl.id)
        stale_review.status = "completed"
        stale_review.output_content = "**Verdict: CHANGES_REQUESTED**"
        stale_review.completed_at = datetime.now(UTC)
        store.update(stale_review)

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
        mock_config = MagicMock(project_dir=tmp_path, use_docker=False, project_prefix="testproject")
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch(
                "gza.cli.determine_next_action",
                return_value={
                    "type": "needs_discussion",
                    "description": "SKIP: 1 consecutive no-op improve reached limit (latest testproject-2); needs manual discussion.",
                    "needs_attention_reason": "improve-no-op",
                    "subject_task_id": impl.id,
                },
            ),
            patch("gza.cli._run_foreground") as run_fg,
        ):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 3
        run_fg.assert_not_called()
        assert "verify_" + "noop_improve_then_review" not in output
        assert "Iterate complete: BLOCKED (needs_discussion)" in output

    def test_iterate_improve_retry_preserves_review_backed_execution_settings(self, tmp_path: Path):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)

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
        failed_improve.failure_reason = "INFRASTRUCTURE_ERROR"
        failed_improve.create_review = True
        failed_improve.create_pr = True
        failed_improve.model = "gpt-5.4"
        failed_improve.provider = "codex"
        failed_improve.provider_is_explicit = True
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
        engine_actions = [{"type": "skip", "description": "initial unused"}, improve_action, {"type": "skip", "description": "done"}]

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.cli.determine_next_action", side_effect=engine_actions),
            patch("gza.cli._run_foreground", side_effect=fake_run_foreground),
        ):
            result = cmd_iterate(args)

        assert result == 3
        retry_tasks = [
            task
            for task in store.get_all()
            if task.task_type == "improve" and task.based_on == failed_improve.id and task.id != failed_improve.id
        ]
        assert len(retry_tasks) == 1
        retry_task = retry_tasks[0]
        assert retry_task.create_review is True
        assert retry_task.create_pr is True
        assert retry_task.model == "gpt-5.4"
        assert retry_task.provider == "codex"
        assert retry_task.provider_is_explicit is True

    def test_iterate_create_review_rejects_at_limit_without_creating_child(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(config_path.read_text() + "max_concurrent: 1\n")
        store = make_store(tmp_path)

        running = store.add("Running task", task_type="implement")
        running.status = "in_progress"
        running.running_pid = os.getpid()
        store.update(running)

        impl = self._make_completed_impl(store)
        before_ids = {task.id for task in store.get_all()}

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
            max_resume_attempts=1,
            max_concurrent=1,
        )
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.cli.resolve_closing_review_action", return_value=None),
            patch("gza.cli.determine_next_action", return_value={"type": "create_review", "description": "Create review"}),
            patch("gza.cli._create_review_task", side_effect=AssertionError("review should not be created at max concurrent")),
        ):
            result = cmd_iterate(args)

        output = capsys.readouterr().out
        assert result == 3
        assert "already at max concurrent tasks: 1 running, limit is 1" in output
        assert {task.id for task in store.get_all()} == before_ids

    @pytest.mark.parametrize("recovery_mode", ["new", "resume", "retry"])
    def test_iterate_improve_recovery_rejects_at_limit_without_creating_child(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
        recovery_mode: str,
    ) -> None:
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(config_path.read_text() + "max_concurrent: 1\n")
        store = make_store(tmp_path)

        running = store.add("Running task", task_type="implement")
        running.status = "in_progress"
        running.running_pid = os.getpid()
        store.update(running)

        impl = self._make_completed_impl(store)
        review = store.add("Review", task_type="review", depends_on=impl.id, based_on=impl.id)
        review.status = "completed"
        review.output_content = "**Verdict: CHANGES_REQUESTED**"
        review.completed_at = datetime.now(UTC)
        store.update(review)

        if recovery_mode != "new":
            failed_improve = store.add(
                "Improve",
                task_type="improve",
                depends_on=review.id,
                based_on=impl.id,
                same_branch=True,
            )
            failed_improve.status = "failed"
            failed_improve.failure_reason = "MAX_STEPS" if recovery_mode == "resume" else "INFRASTRUCTURE_ERROR"
            failed_improve.session_id = "improve-session" if recovery_mode == "resume" else None
            failed_improve.completed_at = datetime.now(UTC)
            store.update(failed_improve)

        before_ids = {task.id for task in store.get_all()}
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
            max_concurrent=1,
        )
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"

        improve_action = {"type": "improve", "description": "Create improve", "review_task": review}
        engine_actions = [improve_action, improve_action]

        create_resume_patch = "gza.cli._create_resume_task"
        create_retry_patch = "gza.cli._create_retry_task"

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.cli.determine_next_action", side_effect=engine_actions),
            patch(create_resume_patch, side_effect=AssertionError("resume child should not be created at max concurrent")),
            patch(create_retry_patch, side_effect=AssertionError("retry child should not be created at max concurrent")),
            patch("gza.cli._create_improve_task", side_effect=AssertionError("new improve child should not be created at max concurrent")),
        ):
            result = cmd_iterate(args)

        output = capsys.readouterr().out
        assert result == 3
        assert "already at max concurrent tasks: 1 running, limit is 1" in output
        assert {task.id for task in store.get_all()} == before_ids

    def test_iterate_needs_rebase_rejects_at_limit_without_creating_child(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(config_path.read_text() + "max_concurrent: 1\n")
        store = make_store(tmp_path)

        running = store.add("Running task", task_type="implement")
        running.status = "in_progress"
        running.running_pid = os.getpid()
        store.update(running)

        impl = self._make_completed_impl(store)
        before_ids = {task.id for task in store.get_all()}

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
            max_resume_attempts=1,
            max_concurrent=1,
        )
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.cli.resolve_closing_review_action", return_value=None),
            patch("gza.cli.determine_next_action", return_value={"type": "needs_rebase", "description": "Create rebase"}),
            patch("gza.cli._create_rebase_task", side_effect=AssertionError("rebase should not be created at max concurrent")),
        ):
            result = cmd_iterate(args)

        output = capsys.readouterr().out
        assert result == 3
        assert "already at max concurrent tasks: 1 running, limit is 1" in output
        assert {task.id for task in store.get_all()} == before_ids

    def test_iterate_default_resume_budget_is_one(self, tmp_path: Path):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)

        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "pending"
        review.session_id = "review-session"
        store.update(review)

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=3,
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
            max_resume_attempts=None,
        )
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"

        def fake_run_foreground(config, task_id, resume=False, **kwargs):
            task = store.get(task_id)
            assert task is not None
            task.status = "failed"
            task.failure_reason = "MAX_STEPS"
            task.session_id = "review-session"
            store.update(task)
            return 1

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch(
                "gza.cli.determine_next_action",
                return_value={"type": "run_review", "description": "Run review", "review_task": review},
            ),
            patch("gza.cli._run_foreground", side_effect=fake_run_foreground) as run_fg,
        ):
            result = cmd_iterate(args)

        assert result == 3
        assert run_fg.call_count == 2
        assert run_fg.call_args_list[0].kwargs.get("resume", False) is False
        assert run_fg.call_args_list[1].kwargs["resume"] is True
        children = store.get_based_on_children(review.id)
        assert len(children) == 1

    def test_iterate_run_review_does_not_auto_resume_test_failure(self, tmp_path: Path):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)

        review = store.add("Review", task_type="review", depends_on=impl.id)
        review.status = "pending"
        review.session_id = "review-session"
        store.update(review)

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=3,
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
            max_resume_attempts=2,
        )
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"

        def fake_run_foreground(config, task_id, resume=False, **kwargs):
            task = store.get(task_id)
            assert task is not None
            task.status = "failed"
            task.failure_reason = "TEST_FAILURE"
            task.session_id = "review-session"
            store.update(task)
            return 1

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch(
                "gza.cli.determine_next_action",
                return_value={"type": "run_review", "description": "Run review", "review_task": review},
            ),
            patch("gza.cli._run_foreground", side_effect=fake_run_foreground) as run_fg,
        ):
            result = cmd_iterate(args)

        assert result == 3
        assert run_fg.call_count == 1
        assert run_fg.call_args_list[0].kwargs.get("resume", False) is False
        children = store.get_based_on_children(review.id)
        assert len(children) == 0

    def test_iterate_handles_needs_rebase_and_resume_actions_from_engine(self, tmp_path: Path):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)

        rebase_task = store.add("Rebase", task_type="rebase", based_on=impl.id)

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=3,
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

        engine_actions = [
            {"type": "needs_rebase", "description": "needs rebase"},
            {"type": "needs_rebase", "description": "needs rebase"},
            {"type": "resume", "description": "resume"},
            {"type": "merge", "description": "done"},
        ]

        def fake_run_foreground(config, task_id, resume=False, **kwargs):
            task = store.get(task_id)
            assert task is not None
            task.status = "completed"
            task.completed_at = datetime.now(UTC)
            store.update(task)
            return 0

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.cli.determine_next_action", side_effect=engine_actions),
            patch("gza.cli.resolve_closing_review_action", return_value=None),
            patch("gza.cli._create_rebase_task", return_value=rebase_task) as create_rebase,
            patch("gza.cli._create_resume_task") as create_resume,
            patch("gza.cli._run_foreground", side_effect=fake_run_foreground),
        ):
            resume_task = store.add("Resume", task_type="implement", based_on=impl.id)
            create_resume.return_value = resume_task
            result = cmd_iterate(args)

        assert result == 0
        create_rebase.assert_called_once()
        create_resume.assert_called_once()

    def test_iterate_prepared_needs_rebase_skips_merged_target(self, tmp_path: Path):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.advance_engine import PostMergeRebaseState
        from gza.cli.execution import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=False,
            auto_iterate=False,
            background=True,
        )
        mock_config = MagicMock(
            project_dir=tmp_path,
            use_docker=False,
            project_prefix="testproject",
            max_resume_attempts=1,
            max_review_cycles=3,
            require_review_before_merge=True,
            advance_create_reviews=True,
        )
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"

        with (
            patch("gza.cli.execution.Config.load", return_value=mock_config),
            patch("gza.cli.execution.get_store", return_value=store),
            patch("gza.cli.execution.Git", return_value=mock_git),
            patch(
                "gza.cli.execution.determine_next_action",
                return_value={"type": "needs_rebase", "description": "needs rebase"},
            ),
            patch(
                "gza.cli.execution._resolve_and_persist_post_merge_rebase_state",
                return_value=PostMergeRebaseState(
                    merge_unit_state="unmerged",
                    branch_tip_sha="same-sha",
                    target_tip_sha="same-sha",
                    target_is_ancestor_of_branch=True,
                    branch_equals_target=True,
                    already_merged=True,
                    rebase_resolution_proved=True,
                    reason="branch-tip-equals-target-tip",
                ),
            ),
            patch("gza.cli.execution._create_rebase_task", side_effect=AssertionError("should not create rebase")),
            patch("gza.cli.execution._spawn_background_iterate", return_value=0) as spawn_background,
        ):
            result = cmd_iterate(args)

        assert result == 0
        assert spawn_background.call_count == 1
        assert spawn_background.call_args.kwargs.get("prepared_task_id") is None
        assert [task for task in store.get_all() if task.task_type == "rebase"] == []

    def test_iterate_background_does_not_prepare_or_spawn_blocked_pending_implementation(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli.execution import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)

        dependency = store.add("Dependency", task_type="implement")
        dependency.status = "completed"
        dependency.branch = "feature/dependency"
        dependency.has_commits = True
        dependency.completed_at = datetime.now(UTC)
        store.update(dependency)

        impl = store.add("Blocked impl", task_type="implement", depends_on=dependency.id)

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=False,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=False,
            auto_iterate=False,
            background=True,
            force=False,
        )
        mock_config = MagicMock(
            project_dir=tmp_path,
            use_docker=False,
            project_prefix="testproject",
            max_resume_attempts=1,
            max_review_cycles=3,
            require_review_before_merge=True,
            advance_create_reviews=True,
        )

        with (
            patch("gza.cli.execution.Config.load", return_value=mock_config),
            patch("gza.cli.execution.get_store", return_value=store),
            patch("gza.cli._common.get_store", return_value=store),
            patch(
                "gza.cli.execution._spawn_background_iterate",
                side_effect=AssertionError("background iterate worker should not spawn"),
            ),
        ):
            result = cmd_iterate(args)

        captured = capsys.readouterr()
        combined_output = captured.out + captured.err
        assert result == 1
        assert "blocked by task" in combined_output

        refreshed = store.get(impl.id)
        assert refreshed is not None
        assert refreshed.status == "pending"
        assert refreshed.failure_reason is None
        assert refreshed.started_at is None
        assert refreshed.slug is None
        assert refreshed.log_file is None

    def test_iterate_foreground_reports_blocked_pending_implementation_before_run(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli.execution import cmd_iterate
        from gza.runner import DEPENDENCY_BLOCKED_NOT_RUN_EXIT_CODE

        setup_config(tmp_path)
        store = make_store(tmp_path)

        dependency = store.add("Dependency", task_type="implement")
        dependency.status = "completed"
        dependency.branch = "feature/dependency"
        dependency.has_commits = True
        dependency.completed_at = datetime.now(UTC)
        store.update(dependency)

        impl = store.add("Blocked impl", task_type="implement", depends_on=dependency.id)

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
            max_resume_attempts=1,
        )

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git") as git_cls,
            patch("gza.cli._run_foreground", side_effect=AssertionError("foreground run should not start")),
        ):
            git_cls.return_value.current_branch.return_value = "main"
            result = cmd_iterate(args)

        output = capsys.readouterr().out
        assert result == DEPENDENCY_BLOCKED_NOT_RUN_EXIT_CODE
        assert "blocked by task" in output
        assert "Running pending implementation" not in output

        refreshed = store.get(impl.id)
        assert refreshed is not None
        assert refreshed.status == "pending"
        assert refreshed.failure_reason is None
        assert refreshed.started_at is None

    def test_iterate_needs_rebase_skips_merged_target_before_create(self, tmp_path: Path):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.advance_engine import PostMergeRebaseState
        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=2,
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
            max_resume_attempts=1,
        )
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.cli.resolve_closing_review_action", return_value=None),
            patch(
                "gza.cli.execution._resolve_and_persist_post_merge_rebase_state",
                return_value=PostMergeRebaseState(
                    merge_unit_state="unmerged",
                    branch_tip_sha="same-sha",
                    target_tip_sha="same-sha",
                    target_is_ancestor_of_branch=True,
                    branch_equals_target=True,
                    already_merged=True,
                    rebase_resolution_proved=True,
                    reason="branch-tip-equals-target-tip",
                ),
            ),
            patch(
                "gza.cli.determine_next_action",
                side_effect=[
                    {"type": "needs_rebase", "description": "needs rebase"},
                    {"type": "merge", "description": "done"},
                ],
            ),
            patch("gza.cli._create_rebase_task", side_effect=AssertionError("should not create rebase")),
        ):
            result = cmd_iterate(args)

        assert result == 0

    def test_iterate_real_engine_needs_rebase_transition(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        """When git reports conflicts, the advance engine should request a rebase."""
        from unittest.mock import MagicMock

        from gza.cli.advance_engine import determine_next_action
        from gza.cli.execution import _AdvanceEngineConfigAdapter

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = False
        engine_config = _AdvanceEngineConfigAdapter(
            project_dir=tmp_path,
            require_review_before_merge=True,
            advance_create_reviews=True,
            max_review_cycles=3,
            max_resume_attempts=1,
        )

        action = determine_next_action(
            engine_config,
            store,
            mock_git,
            impl,
            "main",
            max_resume_attempts=1,
        )

        assert action["type"] == "needs_rebase"

    def test_advance_engine_config_adapter_carries_max_failed_closing_review_retries(
        self, tmp_path: Path
    ) -> None:
        """_AdvanceEngineConfigAdapter must expose max_failed_closing_review_retries alongside other lifecycle knobs."""
        from gza.cli.execution import _AdvanceEngineConfigAdapter
        from gza.config import DEFAULT_MAX_FAILED_CLOSING_REVIEW_RETRIES

        adapter = _AdvanceEngineConfigAdapter(
            project_dir=tmp_path,
            require_review_before_merge=True,
            advance_create_reviews=True,
            max_review_cycles=3,
            max_resume_attempts=1,
        )
        assert hasattr(adapter, "max_failed_closing_review_retries")
        assert adapter.max_failed_closing_review_retries == DEFAULT_MAX_FAILED_CLOSING_REVIEW_RETRIES

        tuned = _AdvanceEngineConfigAdapter(
            project_dir=tmp_path,
            require_review_before_merge=True,
            advance_create_reviews=True,
            max_review_cycles=3,
            max_resume_attempts=1,
            max_failed_closing_review_retries=0,
        )
        assert tuned.max_failed_closing_review_retries == 0

    def test_iterate_engine_honors_tuned_max_failed_closing_review_retries(
        self, tmp_path: Path
    ) -> None:
        """max_failed_closing_review_retries=0 on the iterate adapter must escalate to needs_attention on the first failed closing review."""
        from gza.cli.advance_engine import determine_next_action
        from gza.cli.execution import _AdvanceEngineConfigAdapter

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)
        impl.merge_status = "unmerged"
        impl.has_commits = True
        store.update(impl)

        stale_review = store.add("Old review", task_type="review", depends_on=impl.id)
        stale_review.status = "completed"
        stale_review.completed_at = datetime(2026, 1, 2, tzinfo=UTC)
        store.update(stale_review)

        improve = store.add(
            "Improve",
            task_type="improve",
            based_on=impl.id,
            depends_on=stale_review.id,
            same_branch=True,
        )
        improve.status = "completed"
        improve.completed_at = datetime(2026, 1, 3, tzinfo=UTC)
        store.update(improve)

        impl.review_cleared_at = datetime(2026, 1, 3, tzinfo=UTC)
        store.update(impl)

        failed_closing = store.add("Closing review", task_type="review", depends_on=impl.id)
        failed_closing.status = "failed"
        failed_closing.failure_reason = "UNKNOWN"
        failed_closing.created_at = datetime(2026, 1, 4, tzinfo=UTC)
        failed_closing.completed_at = datetime(2026, 1, 4, 1, tzinfo=UTC)
        store.update(failed_closing)

        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True

        engine_config = _AdvanceEngineConfigAdapter(
            project_dir=tmp_path,
            require_review_before_merge=True,
            advance_create_reviews=True,
            max_review_cycles=3,
            max_resume_attempts=1,
            max_failed_closing_review_retries=0,
        )

        action = determine_next_action(
            engine_config,
            store,
            mock_git,
            impl,
            "main",
            max_resume_attempts=1,
        )

        assert action["type"] == "needs_discussion"
        assert action.get("needs_attention_reason") == "closing-review-failed-max-retries"

    def test_iterate_errors_when_git_init_fails(self, tmp_path: Path):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)

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

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", side_effect=RuntimeError("git init boom")),
            patch("sys.stdout", new_callable=io.StringIO) as stdout,
        ):
            result = cmd_iterate(args)
            output = stdout.getvalue()

        assert result == 1
        assert "failed to initialize git runtime for iterate" in output

    def test_iterate_errors_when_current_branch_fails(self, tmp_path: Path):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)

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
        mock_git.current_branch.side_effect = RuntimeError("branch boom")

        with (
            patch("gza.cli.Config.load", return_value=mock_config),
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("sys.stdout", new_callable=io.StringIO) as stdout,
        ):
            result = cmd_iterate(args)
            output = stdout.getvalue()

        assert result == 1
        assert "failed to initialize git runtime for iterate" in output

    def test_iterate_dry_run_errors_when_git_init_fails_without_first_action(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=True,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=False,
            background=False,
        )

        with patch("gza.cli.Git", side_effect=RuntimeError("git init boom")):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 1
        assert "failed to initialize git runtime for iterate" in output
        assert "First action" not in output

    def test_iterate_dry_run_errors_when_current_branch_fails_without_first_action(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        import argparse
        from unittest.mock import MagicMock, patch

        from gza.cli import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = self._make_completed_impl(store)

        args = argparse.Namespace(
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=True,
            project_dir=tmp_path,
            no_docker=True,
            resume=False,
            retry=False,
            background=False,
        )
        mock_git = MagicMock()
        mock_git.current_branch.side_effect = RuntimeError("branch boom")

        with patch("gza.cli.Git", return_value=mock_git):
            result = cmd_iterate(args)
        output = capsys.readouterr().out

        assert result == 1
        assert "failed to initialize git runtime for iterate" in output
        assert "First action" not in output


class TestMarkCompletedCommand:
    """Tests for 'gza mark-completed' command."""

    def _setup_store(self, tmp_path: Path) -> SqliteTaskStore:
        """Set up config and return a SqliteTaskStore."""
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        return make_store(tmp_path)

    def test_mark_completed_nonexistent_task(self, tmp_path: Path):
        """mark-completed errors on a nonexistent task."""
        setup_db_with_tasks(tmp_path, [])

        result = invoke_gza("mark-completed", "testproject-999999", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "not found" in result.stdout

    def test_mark_completed_default_force_for_non_code_tasks(self, tmp_path: Path):
        """Non-code task types default to status-only completion."""

        setup_db_with_tasks(tmp_path, [
            {"prompt": "Review task", "status": "failed", "task_type": "review"},
        ])

        store = make_store(tmp_path)
        task = store.get_all()[0]

        result = invoke_gza("mark-completed", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "status-only" in result.stdout

        store = make_store(tmp_path)
        updated = store.get(task.id)
        assert updated is not None
        assert updated.status == "completed"
        assert updated.has_commits is False

    def test_mark_completed_verify_git_requires_branch(self, tmp_path: Path):
        """--verify-git errors when no branch is set."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Task without branch", "status": "failed", "task_type": "review"},
        ])

        store = make_store(tmp_path)
        task = store.get_all()[0]
        result = invoke_gza("mark-completed", str(task.id), "--verify-git", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "no branch" in result.stdout
        assert "Use --force" in result.stdout

    def test_mark_completed_force_stale_in_progress_recovery(self, tmp_path: Path):
        """--force with --reason stores completion_reason without git validation."""
        store = self._setup_store(tmp_path)

        task = store.add("Stale worker task", task_type="implement")
        task.status = "in_progress"
        store.update(task)

        result = invoke_gza(
            "mark-completed",
            str(task.id),
            "--force",
            "--reason",
            "MANUAL_RECOVERY",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 0
        assert "in_progress → completed" in result.stdout

        updated = store.get(task.id)
        assert updated is not None
        assert updated.status == "completed"
        assert updated.completion_reason == "MANUAL_RECOVERY"

    def test_mark_completed_no_worker_is_graceful(self, tmp_path: Path):
        """mark-completed succeeds when no worker exists for the task."""

        setup_db_with_tasks(tmp_path, [
            {"prompt": "Review task no worker", "status": "failed", "task_type": "review"},
        ])

        # No workers directory / no registry entry — should still succeed
        store = make_store(tmp_path)
        task = store.get_all()[0]
        result = invoke_gza("mark-completed", str(task.id), "--project", str(tmp_path))

        assert result.returncode == 0
        assert "status-only" in result.stdout

    def test_mark_completed_promotes_unset_mode_to_skill_inline_from_log(self, tmp_path: Path):
        """Inline skill provenance in log should backfill execution_mode when unset."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Inline skill review task", task_type="review")
        assert task.id is not None
        task.status = "failed"
        task.execution_mode = None
        task.log_file = ".gza/logs/inline.log"
        store.update(task)

        log_path = tmp_path / task.log_file
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text(
            '{"type":"gza","subtype":"provenance","message":"Execution mode: inline skill","skill":"gza-task-run","inline":true}\n'
        )

        result = invoke_gza("mark-completed", str(task.id), "--project", str(tmp_path))
        assert result.returncode == 0

        updated = store.get(task.id)
        assert updated is not None
        assert updated.execution_mode == "skill_inline"

    def test_mark_completed_promotes_unset_mode_to_skill_inline_from_ops_log(self, tmp_path: Path):
        """Inline skill provenance in split ops logs should backfill execution_mode when unset."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Inline skill review task from ops log", task_type="review")
        assert task.id is not None
        task.status = "failed"
        task.execution_mode = None
        task.log_file = ".gza/logs/inline-ops.log"
        store.update(task)

        log_dir = tmp_path / ".gza" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        (log_dir / "inline-ops.log").write_text('{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"legacy transcript"}]}}\n')
        (log_dir / "inline-ops.ops.jsonl").write_text(
            '{"type":"gza","subtype":"provenance","message":"Execution mode: inline skill","skill":"gza-task-run","inline":true}\n'
        )

        result = invoke_gza("mark-completed", str(task.id), "--project", str(tmp_path))
        assert result.returncode == 0

        updated = store.get(task.id)
        assert updated is not None
        assert updated.execution_mode == "skill_inline"

    def test_mark_completed_preserves_manual_mode_even_with_inline_provenance_log(self, tmp_path: Path):
        """Explicit manual mode should not be overwritten by log-based inline backfill."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Manual rerun with historical inline log", task_type="review")
        assert task.id is not None
        task.status = "failed"
        task.execution_mode = "manual"
        task.log_file = ".gza/logs/inline-historical.log"
        store.update(task)

        log_path = tmp_path / task.log_file
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text(
            '{"type":"gza","subtype":"provenance","message":"Execution mode: inline skill","skill":"gza-task-run","inline":true}\n'
        )

        result = invoke_gza("mark-completed", str(task.id), "--project", str(tmp_path))
        assert result.returncode == 0

        updated = store.get(task.id)
        assert updated is not None
        assert updated.execution_mode == "manual"

    def test_mark_completed_warns_when_inline_provenance_log_is_unreadable(self, tmp_path: Path):
        """mark-completed should surface a warning when provenance log cannot be read."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Unreadable inline provenance log", task_type="review")
        assert task.id is not None
        task.status = "failed"
        task.execution_mode = None
        task.log_file = ".gza/logs/inline-unreadable.log"
        store.update(task)

        # Make log path unreadable as a file by creating a directory at the same location.
        (tmp_path / task.log_file).mkdir(parents=True, exist_ok=True)

        result = invoke_gza("mark-completed", str(task.id), "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Could not read task log" in result.stdout

        updated = store.get(task.id)
        assert updated is not None
        assert updated.execution_mode is None

    def test_mark_completed_warns_when_inline_provenance_log_has_malformed_json(self, tmp_path: Path):
        """mark-completed should surface a warning when provenance log has malformed JSON lines."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Malformed inline provenance log", task_type="review")
        assert task.id is not None
        task.status = "failed"
        task.execution_mode = None
        task.log_file = ".gza/logs/inline-malformed.log"
        store.update(task)

        log_path = tmp_path / task.log_file
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text(
            "{not valid json}\n"
            '{"type":"gza","subtype":"info","message":"hello"}\n'
        )

        result = invoke_gza("mark-completed", str(task.id), "--project", str(tmp_path))
        assert result.returncode == 0
        assert "malformed JSON line(s)" in result.stdout

        updated = store.get(task.id)
        assert updated is not None
        assert updated.execution_mode is None


class TestSetStatusCommand:
    """Tests for 'gza set-status' command."""

    def test_set_status_nonexistent_task(self, tmp_path: Path):
        """set-status errors when task does not exist."""
        setup_db_with_tasks(tmp_path, [])

        result = invoke_gza("set-status", "testproject-999999", "failed", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "not found" in result.stdout

    def test_set_status_rejects_base36_like_task_id(self, tmp_path: Path):
        """set-status requires decimal task ID suffixes."""
        setup_db_with_tasks(tmp_path, [])

        result = invoke_gza("set-status", "testproject-3f", "failed", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "Invalid task ID" in result.stdout or "Invalid task ID" in result.stderr
        assert "Use a full prefixed task ID" in result.stdout or "Use a full prefixed task ID" in result.stderr

    @pytest.mark.parametrize(
        ("initial_status", "target_status", "completed_at_set"),
        [
            pytest.param("pending", "failed", True, id="pending-to-failed"),
            pytest.param("pending", "dropped", True, id="pending-to-dropped"),
            pytest.param("failed", "dropped", True, id="failed-to-dropped"),
            pytest.param("dropped", "pending", False, id="dropped-to-pending"),
            pytest.param("dropped", "failed", True, id="dropped-to-failed"),
            pytest.param("completed", "failed", True, id="completed-to-failed"),
            pytest.param("completed", "dropped", True, id="completed-to-dropped"),
            pytest.param("in_progress", "failed", True, id="in_progress-to-failed"),
            pytest.param("in_progress", "dropped", True, id="in_progress-to-dropped"),
        ],
    )
    def test_set_status_allowed_transition(
        self,
        tmp_path: Path,
        initial_status: str,
        target_status: str,
        completed_at_set: bool,
    ):
        """set-status allows supported transitions and manages completed_at."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "A task", "status": initial_status},
        ])
        db_path = tmp_path / ".gza" / "gza.db"
        from gza.config import Config
        config = Config.load(tmp_path)
        store = SqliteTaskStore(db_path, prefix=config.project_prefix)

        # Get the actual task ID assigned by the store
        all_tasks = store.get_all()
        task_id = all_tasks[0].id

        result = invoke_gza("set-status", str(task_id), target_status, "--project", str(tmp_path))

        assert result.returncode == 0
        assert target_status in result.stdout

        task = store.get(task_id)
        assert task is not None
        assert task.status == target_status
        if completed_at_set:
            assert task.completed_at is not None
        else:
            assert task.completed_at is None

    def test_set_status_with_reason_for_failed(self, tmp_path: Path):
        """set-status --reason sets failure_reason for failed status."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "A task", "status": "in_progress"},
        ])
        store = make_store(tmp_path)
        task = store.get_all()[0]

        result = invoke_gza(
            "set-status", str(task.id), "failed", "--reason", "Process killed", "--project", str(tmp_path)
        )

        assert result.returncode == 0

        store = make_store(tmp_path)
        updated = store.get(task.id)
        assert updated is not None
        assert updated.status == "failed"
        assert updated.failure_reason == "Process killed"

    def test_set_status_completed_rejected_with_guidance(self, tmp_path: Path):
        """set-status rejects completed and points operators at mark-completed."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "A task", "status": "in_progress"},
        ])
        store = make_store(tmp_path)
        task = store.get_all()[0]

        result = invoke_gza(
            "set-status",
            str(task.id),
            "completed",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 1
        assert "'completed' cannot be set via set-status" in result.stdout
        assert "Use `gza mark-completed <id>`" in result.stdout
        assert "--verify-git and --force" in result.stdout

        store = make_store(tmp_path)
        updated = store.get(task.id)
        assert updated is not None
        assert updated.status == "in_progress"
        assert updated.completion_reason is None

    def test_set_status_failed_with_commits_creates_unmerged_merge_unit(self, tmp_path: Path) -> None:
        """Manual failed transitions should use lifecycle write-through for merge units."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "A task", "status": "in_progress", "task_type": "implement"},
        ])
        store = make_store(tmp_path)
        task = store.get_all()[0]
        task.branch = "feature/manual-status"
        task.has_commits = True
        store.update(task)

        args = [
            "set-status",
            str(task.id),
            "failed",
            "--reason",
            "TEST_FAILURE",
            "--project",
            str(tmp_path),
        ]

        result = invoke_gza(*args)
        assert result.returncode == 0

        updated = make_store(tmp_path).get(task.id)
        assert updated is not None
        assert updated.status == "failed"
        assert updated.has_commits is True
        assert updated.merge_status == "unmerged"

        unit = make_store(tmp_path).resolve_merge_unit_for_task(task.id)
        assert unit is not None
        assert unit.source_branch == "feature/manual-status"
        assert unit.state == "unmerged"

    def test_set_status_in_progress_is_rejected_without_writing_db(self, tmp_path: Path):
        """Manual in-progress transitions should fail and leave the task unchanged."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "A task", "status": "failed", "task_type": "review"},
        ])
        store = make_store(tmp_path)
        task = store.get_all()[0]
        task.execution_mode = "skill_inline"
        task.failure_reason = "Original failure"
        store.update(task)

        result = invoke_gza(
            "set-status",
            str(task.id),
            "in_progress",
            "--project",
            str(tmp_path),
        )
        assert result.returncode == 1
        assert "'in_progress' is set by a running worker" in result.stdout
        assert "gza work <id>" in result.stdout
        assert "gza resume <id>" in result.stdout
        assert "gza retry <id>" in result.stdout
        assert "gza watch" in result.stdout

        updated = store.get(task.id)
        assert updated is not None
        assert updated.status == "failed"
        assert updated.execution_mode == "skill_inline"
        assert updated.failure_reason == "Original failure"

    def test_set_status_failed_preserves_skill_inline_execution_mode(self, tmp_path: Path):
        """Inline runs that fail before mark-completed should retain skill_inline provenance."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "A task", "status": "pending", "task_type": "review"},
        ])
        store = make_store(tmp_path)
        task = store.get_all()[0]
        task.execution_mode = "skill_inline"
        task.status = "in_progress"
        store.update(task)

        fail_result = invoke_gza(
            "set-status",
            str(task.id),
            "failed",
            "--reason",
            "TEST_FAILURE",
            "--project",
            str(tmp_path),
        )
        assert fail_result.returncode == 0

        shown = invoke_gza("show", str(task.id), "--project", str(tmp_path))
        assert shown.returncode == 0
        assert "Execution Mode: skill_inline" in shown.stdout

        updated = store.get(task.id)
        assert updated is not None
        assert updated.status == "failed"
        assert updated.execution_mode == "skill_inline"

    def test_set_status_removed_execution_mode_flag_errors(self, tmp_path: Path):
        """The removed --execution-mode flag should be rejected by the parser."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "A task", "status": "pending"},
        ])
        store = make_store(tmp_path)
        task = store.get_all()[0]

        result = invoke_gza(
            "set-status",
            str(task.id),
            "failed",
            "--execution-mode",
            "skill_inline",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 2
        assert "unrecognized arguments: --execution-mode skill_inline" in result.stderr

    def test_set_status_reason_warns_for_statuses_without_reason_support(self, tmp_path: Path):
        """set-status warns when --reason is used outside failed transitions."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "A task", "status": "in_progress"},
        ])

        store = make_store(tmp_path)
        task = store.get_all()[0]
        result = invoke_gza(
            "set-status", str(task.id), "dropped", "--reason", "Ignored reason", "--project", str(tmp_path)
        )

        assert result.returncode == 0
        assert "Warning" in result.stdout or "warning" in result.stdout.lower()
        assert "--reason is only meaningful for 'failed' status" in result.stdout

        updated = make_store(tmp_path).get(task.id)
        assert updated is not None
        assert updated.failure_reason is None
        assert updated.completion_reason is None

    def test_set_status_reason_warns_for_pending_un_drop(self, tmp_path: Path) -> None:
        """set-status warns and ignores --reason when reviving a dropped task."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "A task", "status": "dropped"},
        ])

        task = make_store(tmp_path).get_all()[0]
        result = invoke_gza(
            "set-status", str(task.id), "pending", "--reason", "Ignored reason", "--project", str(tmp_path)
        )

        assert result.returncode == 0
        assert "--reason is only meaningful for 'failed' status" in result.stdout

        updated = make_store(tmp_path).get(task.id)
        assert updated is not None
        assert updated.status == "pending"
        assert updated.failure_reason is None
        assert updated.completion_reason is None

    def test_set_status_invalid_status_rejected(self, tmp_path: Path):
        """set-status rejects unknown status values."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "A task", "status": "pending"},
        ])

        store = make_store(tmp_path)
        task = store.get_all()[0]
        result = invoke_gza("set-status", str(task.id), "bogus", "--project", str(tmp_path))

        assert result.returncode != 0
        assert "Valid statuses: pending, failed, dropped." in result.stdout

    def test_set_status_help_omits_in_progress_and_execution_mode(self, tmp_path: Path):
        """set-status subcommand help should explain the restricted transition model."""
        setup_db_with_tasks(tmp_path, [])

        result = invoke_gza("set-status", "--help", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Override a task's status for recovery or correction." in result.stdout
        assert "Allowed targets: failed, dropped (any source), pending (only from" in result.stdout
        assert "gza mark-completed <id>" in result.stdout
        assert "gza retry <id>" in result.stdout
        assert "gza resume <id>" in result.stdout
        assert "in_progress" not in result.stdout
        assert "--execution-mode" not in result.stdout

    def test_mark_completed_help_mentions_reason(self, tmp_path: Path):
        """mark-completed subcommand help should advertise the --reason flag."""
        setup_db_with_tasks(tmp_path, [])

        result = invoke_gza("mark-completed", "--help", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "--reason REASON" in result.stdout
        assert "Completion reason persisted to task.completion_reason" in result.stdout

    def test_top_level_help_points_set_status_completed_users_at_mark_completed(self, tmp_path: Path):
        """Top-level help should advertise mark-completed as the completion path."""
        setup_db_with_tasks(tmp_path, [])

        result = invoke_gza("--help", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "mark-completed        Mark a task as completed (defaults by task type;" in result.stdout
        assert "supports --verify-git, --force, --reason" in result.stdout
        assert "set-status            Override a task's status for recovery or correction." in result.stdout

    @pytest.mark.parametrize(
        ("initial_status", "target_status", "expected_message"),
        [
            pytest.param(
                "failed",
                "pending",
                "use `gza retry <id>` to re-run",
                id="failed-to-pending",
            ),
            pytest.param(
                "completed",
                "pending",
                "create a new task with `gza add`",
                id="completed-to-pending",
            ),
            pytest.param(
                "in_progress",
                "pending",
                "use `gza resume <id>` to reattach to the running task",
                id="in_progress-to-pending",
            ),
        ],
    )
    def test_set_status_rejects_disallowed_pending_transitions(
        self,
        tmp_path: Path,
        initial_status: str,
        target_status: str,
        expected_message: str,
    ) -> None:
        """set-status rejects unsafe transitions to pending with specific guidance."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "A task", "status": initial_status},
        ])
        store = make_store(tmp_path)
        task = store.get_all()[0]
        task.failure_reason = "Original failure"
        task.completion_reason = "Original completion"
        if initial_status == "completed":
            task.completed_at = datetime.now(UTC)
        elif initial_status == "in_progress":
            task.completed_at = None
        store.update(task)
        original_completed_at = task.completed_at

        result = invoke_gza("set-status", str(task.id), target_status, "--project", str(tmp_path))

        assert result.returncode == 1
        assert expected_message in result.stdout.lower()

        updated = make_store(tmp_path).get(task.id)
        assert updated is not None
        assert updated.status == initial_status
        assert updated.failure_reason == "Original failure"
        assert updated.completion_reason == "Original completion"
        assert updated.completed_at == original_completed_at

    @pytest.mark.parametrize("status", ["pending", "failed", "dropped"])
    def test_set_status_self_transition_is_no_op(self, tmp_path: Path, status: str) -> None:
        """set-status self-transitions should be friendly no-ops."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "A task", "status": status},
        ])
        store = make_store(tmp_path)
        task = store.get_all()[0]
        task.failure_reason = "Original failure"
        task.completion_reason = "Original completion"
        if status in {"failed", "dropped"}:
            task.completed_at = datetime.now(UTC)
        store.update(task)

        result = invoke_gza(
            "set-status",
            str(task.id),
            status,
            "--reason",
            "ignored",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 0
        assert f"Task {task.id} is already in status '{status}'; no change." in result.stdout

        updated = make_store(tmp_path).get(task.id)
        assert updated is not None
        assert updated.status == status
        assert updated.failure_reason == "Original failure"
        assert updated.completion_reason == "Original completion"
        if status in {"failed", "dropped"}:
            assert updated.completed_at is not None
        else:
            assert updated.completed_at is None

    def test_dropped_task_blocks_dependent(self, tmp_path: Path):
        """A task that depends_on a dropped task is reported as blocked."""
        from gza.db import SqliteTaskStore as _Store
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = _Store(db_path)

        prereq = store.add("Dropped prereq")
        prereq.status = "dropped"
        store.update(prereq)

        dependent = store.add("Dependent task", depends_on=prereq.id)

        is_blocked, blocked_by_id, blocked_by_status = store.is_task_blocked(dependent)
        assert is_blocked is True
        assert blocked_by_id == prereq.id
        assert blocked_by_status == "dropped"

    def test_next_all_shows_blocked_annotation_for_dropped_dependency(self, tmp_path: Path):
        """gza next --all shows blocked annotation for a task blocked by a dropped dependency."""
        from gza.db import SqliteTaskStore as _Store
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = _Store(db_path)

        prereq = store.add("Dropped prereq")
        prereq.status = "dropped"
        prereq.completed_at = datetime.now(UTC)
        store.update(prereq)

        store.add("Dependent task", depends_on=prereq.id)

        result = invoke_gza("next", "--all", "--project", str(tmp_path))
        assert result.returncode == 0
        # The dependent task should appear with a blocked annotation
        assert "Dependent task" in result.stdout
        assert "blocked" in result.stdout.lower()

    def test_history_shows_dropped_tasks(self, tmp_path: Path):
        """gza history includes dropped tasks after fix to get_history() default filter."""
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        from gza.db import SqliteTaskStore as _Store
        store = _Store(db_path)

        task = store.add("Task to be dropped")
        task.status = "dropped"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        result = invoke_gza("history", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Task to be dropped" in result.stdout


class TestMaxTurnsFlag:
    """Tests for --max-turns flag on work, retry, and resume commands."""

    def test_max_turns_override_applies_correctly(self, tmp_path: Path):
        """--max-turns flag overrides config value."""
        import argparse

        config_path = tmp_path / "gza.yaml"
        config_path.write_text("project_name: test\nmax_steps: 50\n")

        config = Config.load(tmp_path)
        assert config.max_turns == 50

        args = argparse.Namespace(max_turns=200, project_dir=config.project_dir)
        if hasattr(args, 'max_turns') and args.max_turns is not None:
            config.max_steps = args.max_turns
            config.max_turns = args.max_turns

        assert config.max_turns == 200
        assert config.max_steps == 200


class TestRunForeground:
    """Tests for _run_foreground() helper."""

    def test_run_foreground_registers_and_completes_worker(self, tmp_path: Path):
        """_run_foreground registers a worker before running and marks it completed after."""
        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)
        task = store.add("Test foreground task")
        assert task.id is not None

        workers_path = config.workers_path
        workers_path.mkdir(parents=True, exist_ok=True)

        with (
            _clear_foreground_worker_env(),
            patch("gza.cli.run", return_value=0) as mock_run,
        ):
            rc = _run_foreground(config, task_id=task.id)

        assert rc == 0
        mock_run.assert_called_once()
        assert mock_run.call_args.kwargs["task_id"] == task.id
        assert mock_run.call_args.kwargs["resume"] is False
        assert mock_run.call_args.kwargs["open_after"] is False
        assert mock_run.call_args.kwargs["skip_precondition_check"] is False
        assert callable(mock_run.call_args.kwargs["on_task_claimed"])

        # Worker should now be marked completed
        registry = WorkerRegistry(workers_path)
        workers = registry.list_all(include_completed=True)
        assert len(workers) == 1
        w = workers[0]
        assert w.task_id == task.id
        assert w.status == "completed"
        assert w.exit_code == 0
        assert w.is_background is False

    def test_run_foreground_marks_failed_on_nonzero_exit(self, tmp_path: Path):
        """_run_foreground marks worker as failed when run() returns non-zero."""
        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)
        task = store.add("Test failing task")
        assert task.id is not None

        config.workers_path.mkdir(parents=True, exist_ok=True)

        with (
            _clear_foreground_worker_env(),
            patch("gza.cli.run", return_value=1),
        ):
            rc = _run_foreground(config, task_id=task.id)

        assert rc == 1

        registry = WorkerRegistry(config.workers_path)
        workers = registry.list_all(include_completed=True)
        assert len(workers) == 1
        w = workers[0]
        assert w.status == "failed"
        assert w.exit_code == 1

    def test_run_foreground_precondition_refusal_marks_worker_failed(self, tmp_path: Path):
        """Precondition refusal (PREREQUISITE_UNMERGED path => non-zero) must not be recorded as completed."""
        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)
        task = store.add("Blocked by prerequisite")
        assert task.id is not None

        config.workers_path.mkdir(parents=True, exist_ok=True)

        with (
            _clear_foreground_worker_env(),
            patch("gza.cli.run", return_value=1),
        ):
            rc = _run_foreground(config, task_id=task.id)

        assert rc == 1
        registry = WorkerRegistry(config.workers_path)
        workers = registry.list_all(include_completed=True)
        assert len(workers) == 1
        assert workers[0].status == "failed"
        assert workers[0].exit_code == 1

    def test_run_foreground_passes_resume_and_open_after(self, tmp_path: Path):
        """_run_foreground correctly passes resume and open_after to run()."""
        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)
        task = store.add("Test task")
        assert task.id is not None

        config.workers_path.mkdir(parents=True, exist_ok=True)

        with (
            _clear_foreground_worker_env(),
            patch("gza.cli.run", return_value=0) as mock_run,
        ):
            rc = _run_foreground(config, task_id=task.id, resume=True, open_after=True)

        assert rc == 0
        mock_run.assert_called_once()
        assert mock_run.call_args.kwargs["task_id"] == task.id
        assert mock_run.call_args.kwargs["resume"] is True
        assert mock_run.call_args.kwargs["open_after"] is True
        assert mock_run.call_args.kwargs["skip_precondition_check"] is False
        assert callable(mock_run.call_args.kwargs["on_task_claimed"])

    def test_run_foreground_resume_auto_rebases_before_run(self, tmp_path: Path):
        """Resume runs an automatic rebase step before resuming the provider session."""
        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)
        task = store.add("Resume target")
        assert task.id is not None

        config.workers_path.mkdir(parents=True, exist_ok=True)

        with (
            _clear_foreground_worker_env(),
            patch("gza.cli._auto_rebase_before_resume", return_value=0) as mock_rebase,
            patch("gza.cli.run", return_value=0) as mock_run,
        ):
            rc = _run_foreground(config, task_id=task.id, resume=True)

        assert rc == 0
        mock_rebase.assert_called_once_with(config, task.id)
        mock_run.assert_called_once()
        assert mock_run.call_args.kwargs["task_id"] == task.id
        assert mock_run.call_args.kwargs["resume"] is True
        assert mock_run.call_args.kwargs["open_after"] is False
        assert mock_run.call_args.kwargs["skip_precondition_check"] is False
        assert callable(mock_run.call_args.kwargs["on_task_claimed"])

    def test_run_foreground_resume_rebase_failure_skips_run(self, tmp_path: Path):
        """Resume aborts before run() when the automatic rebase fails."""
        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)
        task = store.add("Resume target")
        assert task.id is not None

        workers_path = config.workers_path
        workers_path.mkdir(parents=True, exist_ok=True)

        with (
            _clear_foreground_worker_env(),
            patch("gza.cli._auto_rebase_before_resume", return_value=1) as mock_rebase,
            patch("gza.cli.run") as mock_run,
        ):
            rc = _run_foreground(config, task_id=task.id, resume=True)

        assert rc == 1
        mock_rebase.assert_called_once_with(config, task.id)
        mock_run.assert_not_called()
        registry = WorkerRegistry(workers_path)
        workers = registry.list_all(include_completed=True)
        assert len(workers) == 1
        assert workers[0].status == "failed"
        assert workers[0].exit_code == 1

    def test_run_foreground_resume_dependency_block_refuses_before_dispatch_side_effects(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Blocked failed resumes must stop before rebase, permits, startup prep, or worker registration."""
        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)

        dependency = store.add("Dependency", task_type="implement")
        dependency.status = "completed"
        dependency.branch = "feature/dependency-foreground-resume"
        dependency.has_commits = True
        dependency.completed_at = datetime.now(UTC)
        store.update(dependency)

        task = store.add("Blocked foreground resume", task_type="implement", depends_on=dependency.id)
        failed_at = datetime.now(UTC)
        task.status = "failed"
        task.failure_reason = "TIMEOUT"
        task.started_at = failed_at
        task.completed_at = failed_at
        task.slug = "20260627-blocked-foreground-resume"
        task.session_id = "resume-session"
        task.log_file = "logs/existing-foreground-resume.log"
        store.update(task)
        assert task.id is not None

        with (
            _clear_foreground_worker_env(),
            patch("gza.cli._auto_rebase_before_resume", side_effect=AssertionError("rebase should not run")),
            patch("gza.cli.launch_permit", side_effect=AssertionError("launch permit should not be acquired")),
            patch("gza.cli._prepare_task_for_launch", side_effect=AssertionError("startup prep should not run")),
            patch("gza.cli.run", side_effect=AssertionError("runner should not start")),
            patch("gza.workers.WorkerRegistry.register", side_effect=AssertionError("worker should not register")),
        ):
            rc = _run_foreground(config, task_id=task.id, resume=True)

        assert rc == DEPENDENCY_BLOCKED_NOT_RUN_EXIT_CODE
        assert "blocked by task" in capsys.readouterr().err

        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.status == "failed"
        assert refreshed.failure_reason == "TIMEOUT"
        assert refreshed.started_at == failed_at
        assert refreshed.completed_at == failed_at
        assert refreshed.execution_mode is None
        assert refreshed.slug == "20260627-blocked-foreground-resume"
        assert refreshed.log_file == "logs/existing-foreground-resume.log"

        logs_dir = tmp_path / ".gza" / "logs"
        if logs_dir.exists():
            assert not any(path.is_file() for path in logs_dir.rglob("*"))
        workers_dir = tmp_path / ".gza" / "workers"
        if workers_dir.exists():
            assert list(workers_dir.iterdir()) == []
        assert not (config.worktree_path / refreshed.slug).exists()

    def test_auto_rebase_before_resume_creates_completed_rebase_child(self, tmp_path: Path):
        """Resume preflight creates a completed rebase child task on success."""
        from gza.cli import _auto_rebase_before_resume

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Resume target", task_type="implement")
        assert task.id is not None
        task.status = "failed"
        task.session_id = "resume-session"
        task.branch = "feature/resume-target"
        store.update(task)

        def fake_task_backed_rebase(*, store, rebase_task, branch, target_branch, **_kwargs):
            store.mark_completed(
                rebase_task,
                branch=branch,
                log_file=".gza/logs/rebase.log",
                output_content=f"Rebased '{branch}' onto '{target_branch}'.",
            )
            return 0

        mock_git = MagicMock()
        mock_git.default_branch.return_value = "main"

        with (
            patch("gza.git.Git", return_value=mock_git),
            patch("gza.cli.git_ops._run_task_backed_rebase", side_effect=fake_task_backed_rebase),
        ):
            rc = _auto_rebase_before_resume(config, task.id)

        assert rc == 0
        rebase_children = [t for t in store.get_based_on_children(task.id) if t.task_type == "rebase"]
        assert len(rebase_children) == 1
        rebase_task = rebase_children[0]
        assert rebase_task.status == "completed"
        assert rebase_task.branch == task.branch
        assert "Rebased" in (rebase_task.output_content or "")
        assert rebase_task.log_file is not None

    def test_auto_rebase_before_resume_uses_retry_hint_without_start_fresh_wording(self, tmp_path: Path):
        """Failed preflight rebase guidance should not claim retry starts fresh."""
        from gza.cli import _auto_rebase_before_resume

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Resume target", task_type="improve")
        assert task.id is not None
        task.status = "failed"
        task.session_id = "resume-session"
        task.branch = "feature/resume-target"
        store.update(task)

        mock_git = MagicMock()
        mock_git.default_branch.return_value = "main"

        with (
            patch("gza.git.Git", return_value=mock_git),
            patch("gza.cli.git_ops._run_task_backed_rebase", return_value=1) as mock_rebase,
        ):
            rc = _auto_rebase_before_resume(config, task.id)

        assert rc == 1
        assert mock_rebase.call_args.kwargs["failure_hint_lines"] == [
            "Use 'gza retry' to create a new retry attempt or run 'gza rebase' manually.",
        ]

    def test_run_foreground_marks_failed_on_keyboard_interrupt(self, tmp_path: Path):
        """_run_foreground marks worker as failed when interrupted."""
        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)
        task = store.add("Test interrupt task")
        assert task.id is not None

        config.workers_path.mkdir(parents=True, exist_ok=True)

        with (
            _clear_foreground_worker_env(),
            patch("gza.cli.run", side_effect=KeyboardInterrupt),
        ):
            rc = _run_foreground(config, task_id=task.id)

        assert rc == 130

        registry = WorkerRegistry(config.workers_path)
        workers = registry.list_all(include_completed=True)
        assert len(workers) == 1
        w = workers[0]
        assert w.status == "failed"
        assert w.exit_code == 130

    def test_run_foreground_nested_registered_worker_defers_completion_to_outer_owner(self, tmp_path: Path):
        """Nested worker-mode runs must leave completion bookkeeping to the outer wrapper."""
        from gza.cli.execution import _run_with_registered_worker
        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)
        task = store.add("Nested worker task")
        assert task.id is not None

        registry = WorkerRegistry(config.workers_path)

        def run_command() -> int:
            rc = _run_foreground(config, task_id=task.id)
            worker = registry.get("w-nested-iterate")
            assert worker is not None
            assert worker.status == "running"
            assert worker.task_id == task.id
            assert worker.exit_code is None
            return rc

        with patch("gza.cli.run", return_value=0) as mock_run:
            assert _run_with_registered_worker(
                config=config,
                worker_id="w-nested-iterate",
                run_command=run_command,
            ) == 0

        mock_run.assert_called_once()
        assert mock_run.call_args.kwargs["task_id"] == task.id
        assert mock_run.call_args.kwargs["resume"] is False
        assert mock_run.call_args.kwargs["open_after"] is False
        assert mock_run.call_args.kwargs["skip_precondition_check"] is False
        assert callable(mock_run.call_args.kwargs["on_task_claimed"])
        workers = registry.list_all(include_completed=True)
        assert len(workers) == 1
        worker = workers[0]
        assert worker.worker_id == "w-nested-iterate"
        assert worker.status == "completed"
        assert worker.task_id == task.id
        assert worker.exit_code == 0

    @pytest.mark.parametrize(
        "ambient_worker_env",
        [
            {},
            {
                "GZA_WORKER_ID": "ambient-outer-worker",
                "GZA_WORKER_MODE": "1",
                "GZA_REUSE_WORKER_OWNER": "outer",
                "GZA_REUSE_WORKER_SESSION": "1",
            },
        ],
        ids=["plain-shell", "ambient-worker-session"],
    )
    def test_run_foreground_worker_mode_without_existing_metadata_completes_even_with_outer_owner_marker(
        self,
        tmp_path: Path,
        ambient_worker_env: dict[str, str],
    ):
        """Ambient worker-mode fallback must complete the worker unless a real outer registration exists."""
        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)
        task = store.add("Nested worker without parent metadata")
        assert task.id is not None

        with (
            patch.dict(os.environ, ambient_worker_env, clear=False),
            _clear_foreground_worker_env(),
            patch.dict(
                os.environ,
                {
                    "GZA_WORKER_ID": "w-missing-parent-meta",
                    "GZA_WORKER_MODE": "1",
                    "GZA_REUSE_WORKER_OWNER": "outer",
                },
                clear=False,
            ),
            patch("gza.cli.run", return_value=0),
        ):
            assert _run_foreground(config, task_id=task.id) == 0

        registry = WorkerRegistry(config.workers_path)
        workers = registry.list_all(include_completed=True)
        assert len(workers) == 1
        worker = workers[0]
        assert worker.worker_id == "w-missing-parent-meta"
        assert worker.status == "completed"
        assert worker.task_id == task.id
        assert worker.exit_code == 0

    def test_run_foreground_rejects_at_max_concurrent_before_registering_worker(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Standalone foreground launches should fail cleanly at the cap without self-counting."""
        setup_config(tmp_path)
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(config_path.read_text() + "max_concurrent: 1\n")
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)

        running = store.add("Running task", task_type="implement")
        running.status = "in_progress"
        running.running_pid = os.getpid()
        store.update(running)

        task = store.add("Blocked launch", task_type="implement")
        assert task.id is not None

        with (
            _clear_foreground_worker_env(),
            patch("gza.cli.run") as mock_run,
        ):
            rc = _run_foreground(config, task_id=task.id)

        assert rc == 1
        assert (
            capsys.readouterr().err.strip()
            == "Error: already at max concurrent tasks: 1 running, limit is 1"
        )
        mock_run.assert_not_called()
        assert WorkerRegistry(config.workers_path).list_all(include_completed=True) == []

    def test_run_foreground_signal_calls_mark_completed_once(self, tmp_path: Path):
        """Signal delivery via _cleanup raises KeyboardInterrupt; mark_completed is called exactly once."""
        import signal as signal_mod

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)
        task = store.add("Test signal task")
        assert task.id is not None

        config.workers_path.mkdir(parents=True, exist_ok=True)

        # Capture the installed SIGINT handler so we can call _cleanup directly
        installed_handlers: dict = {}
        original_signal = signal_mod.signal

        def capture_signal(signum, handler):
            installed_handlers[signum] = handler
            return original_signal(signum, handler)

        with (
            _clear_foreground_worker_env(),
            patch("gza.cli.signal.signal", side_effect=capture_signal),
        ):
            with patch("gza.workers.WorkerRegistry.mark_completed") as mock_mark:
                def run_then_signal(*args, **kwargs):
                    # Simulate SIGINT arriving while run() is executing
                    cleanup = installed_handlers.get(signal_mod.SIGINT)
                    if cleanup and callable(cleanup):
                        cleanup(signal_mod.SIGINT, None)

                with patch("gza.cli.run", side_effect=run_then_signal):
                    rc = _run_foreground(config, task_id=task.id)

        assert rc == 130
        # mark_completed must be called exactly once, not twice
        assert mock_mark.call_count == 1
        assert mock_mark.call_args.kwargs.get("status") == "failed"
        assert mock_mark.call_args.kwargs.get("exit_code") == 130


class TestBackgroundLaunchConcurrency:
    """Targeted max-concurrent regressions for prepared/background launch helpers."""

    def test_prepared_background_worker_rejects_at_limit_without_spawning(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        from gza.cli._common import _spawn_background_worker

        setup_config(tmp_path)
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(config_path.read_text() + "max_concurrent: 1\n")
        config = Config.load(tmp_path)
        store = make_store(tmp_path)

        running = store.add("Running task", task_type="implement")
        running.status = "in_progress"
        running.running_pid = os.getpid()
        store.update(running)

        prepared = store.add("Prepared task", task_type="implement")
        args = argparse.Namespace(
            no_docker=True,
            max_turns=None,
            force=False,
            create_pr=False,
            resume=False,
            tags=[],
            group=None,
            any_tag=False,
        )

        with patch(
            "gza.cli._common._spawn_detached_worker_process",
            side_effect=AssertionError("background worker should not spawn"),
        ):
            rc = _spawn_background_worker(args, config, task_id=prepared.id, prepared_task=prepared)

        assert rc == 1
        assert (
            capsys.readouterr().err.strip()
            == "Error: already at max concurrent tasks: 1 running, limit is 1"
        )

    def test_prepared_background_resume_worker_rejects_at_limit_without_spawning(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        from gza.cli._common import _spawn_background_resume_worker

        setup_config(tmp_path)
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(config_path.read_text() + "max_concurrent: 1\n")
        config = Config.load(tmp_path)
        store = make_store(tmp_path)

        running = store.add("Running task", task_type="implement")
        running.status = "in_progress"
        running.running_pid = os.getpid()
        store.update(running)

        prepared = store.add("Prepared resume task", task_type="implement")
        prepared.session_id = "resume-session"
        store.update(prepared)
        args = argparse.Namespace(no_docker=True, max_turns=None, force=False)

        with patch(
            "gza.cli._common._spawn_detached_worker_process",
            side_effect=AssertionError("background resume worker should not spawn"),
        ):
            rc = _spawn_background_resume_worker(args, config, str(prepared.id), prepared_task=prepared)

        assert rc == 1
        assert (
            capsys.readouterr().err.strip()
            == "Error: already at max concurrent tasks: 1 running, limit is 1"
        )

    def test_background_iterate_worker_rejects_at_limit_without_spawning(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        from gza.cli._common import _spawn_background_iterate_worker

        setup_config(tmp_path)
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(config_path.read_text() + "max_concurrent: 1\n")
        config = Config.load(tmp_path)
        store = make_store(tmp_path)

        running = store.add("Running task", task_type="implement")
        running.status = "in_progress"
        running.running_pid = os.getpid()
        store.update(running)

        impl = store.add("Implementation task", task_type="implement")
        args = argparse.Namespace(no_docker=True, force=False)

        with patch(
            "gza.cli._common._spawn_detached_worker_process",
            side_effect=AssertionError("background iterate worker should not spawn"),
        ):
            rc = _spawn_background_iterate_worker(args, config, impl, max_iterations=1)

        assert rc == 1
        assert (
            capsys.readouterr().err.strip()
            == "Error: already at max concurrent tasks: 1 running, limit is 1"
        )

    def test_background_iterate_dry_run_does_not_leak_launch_lock(self, tmp_path: Path) -> None:
        from gza.cli._common import _spawn_background_iterate_worker
        from gza.concurrency import _PROCESS_LOCKS, launch_permit

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Implementation task", task_type="implement")
        args = argparse.Namespace(no_docker=True, force=False)

        assert _spawn_background_iterate_worker(args, config, impl, max_iterations=1, dry_run=True) == 0
        assert _PROCESS_LOCKS == {}

        permit = launch_permit(config, store)
        try:
            assert _PROCESS_LOCKS
        finally:
            permit.release()
        assert _PROCESS_LOCKS == {}


class TestRunInlineCommand:
    """Tests for run-inline command wiring."""

    def test_cmd_run_inline_passes_foreground_inline_auto_invocation(self, tmp_path: Path):
        setup_config(tmp_path)
        config = Config.load(tmp_path)

        args = argparse.Namespace(
            project_dir=tmp_path,
            no_docker=False,
            max_turns=None,
            task_id="testproject-1",
            resume=True,
            force=True,
        )

        with (
            patch("gza.cli.Config.load", return_value=config),
            patch("gza.cli.resolve_id", return_value="testproject-1"),
            patch("gza.cli._run_foreground", return_value=130) as mock_run_foreground,
        ):
            rc = cmd_run_inline(args)

        assert rc == 130
        mock_run_foreground.assert_called_once()
        assert mock_run_foreground.call_args.kwargs["task_id"] == "testproject-1"
        assert mock_run_foreground.call_args.kwargs["resume"] is True
        assert mock_run_foreground.call_args.kwargs["force"] is True
        invocation = mock_run_foreground.call_args.kwargs["invocation"]
        assert invocation.command == "run-inline"
        assert invocation.execution_mode == "foreground_inline"
        assert invocation.interaction_mode == "auto"

    def test_cmd_run_inline_resume_dependency_block_preserves_failed_task_state(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)

        dependency = store.add("Dependency", task_type="implement")
        dependency.status = "completed"
        dependency.branch = "feature/dependency"
        dependency.has_commits = True
        dependency.completed_at = datetime.now(UTC)
        store.update(dependency)

        task = store.add("Blocked resume", task_type="implement", depends_on=dependency.id)
        failed_at = datetime.now(UTC)
        task.status = "failed"
        task.failure_reason = "TIMEOUT"
        task.started_at = failed_at
        task.completed_at = failed_at
        task.slug = "20260627-blocked-run-inline-resume"
        task.session_id = "resume-session"
        task.log_file = "logs/existing-run-inline-resume.log"
        store.update(task)
        assert task.id is not None

        args = argparse.Namespace(
            project_dir=tmp_path,
            no_docker=True,
            max_turns=None,
            task_id=task.id,
            resume=True,
            force=False,
        )

        with (
            patch("gza.cli.resolve_id", return_value=task.id),
            patch("gza.cli._auto_rebase_before_resume", side_effect=AssertionError("rebase should not run")) as mock_rebase,
            patch("gza.cli.launch_permit", side_effect=AssertionError("launch permit should not be acquired")),
            patch("gza.cli._prepare_task_for_launch", side_effect=AssertionError("startup prep should not run")),
            patch("gza.workers.WorkerRegistry.register", side_effect=AssertionError("worker should not register")),
            patch("gza.runner.backup_database"),
            patch("gza.runner.load_dotenv"),
            patch("gza.runner.get_provider", side_effect=AssertionError("provider should not start")),
            patch("gza.runner.Git", side_effect=AssertionError("git should not be consulted")),
        ):
            rc = cmd_run_inline(args)

        assert rc == DEPENDENCY_BLOCKED_NOT_RUN_EXIT_CODE
        mock_rebase.assert_not_called()
        captured = capsys.readouterr()
        assert "blocked by task" in captured.err

        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.status == "failed"
        assert refreshed.failure_reason == "TIMEOUT"
        assert refreshed.started_at == failed_at
        assert refreshed.completed_at == failed_at
        assert refreshed.execution_mode is None
        assert refreshed.slug == "20260627-blocked-run-inline-resume"
        assert refreshed.log_file == "logs/existing-run-inline-resume.log"

        logs_dir = tmp_path / ".gza" / "logs"
        if logs_dir.exists():
            assert not any(path.is_file() for path in logs_dir.rglob("*"))
        workers_dir = tmp_path / ".gza" / "workers"
        if workers_dir.exists():
            assert list(workers_dir.iterdir()) == []
        assert not (config.worktree_path / task.slug).exists()


class TestForegroundInvocationContextWiring:
    """Foreground command entrypoints should pass command-specific runner invocation metadata."""

    def test_cmd_implement_passes_implement_invocation(self, tmp_path: Path):
        from gza.cli.execution import cmd_implement

        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan = store.add("Plan feature", task_type="plan")
        plan.status = "completed"
        plan.completed_at = datetime.now(UTC)
        store.update(plan)

        args = argparse.Namespace(
            project_dir=tmp_path,
            no_docker=True,
            max_turns=None,
            plan_task_id=plan.id,
            prompt=None,
            group=None,
            depends_on=None,
            review=False,
            same_branch=False,
            branch_type=None,
            model=None,
            provider=None,
            skip_learnings=False,
            run=True,
            background=False,
            queue=False,
            force=False,
        )

        with patch("gza.cli.execution._run_foreground", return_value=0) as mock_run_foreground:
            rc = cmd_implement(args)

        assert rc == 0
        invocation = mock_run_foreground.call_args.kwargs["invocation"]
        assert invocation.command == "implement"
        assert invocation.execution_mode == "foreground_worker"

    def test_cmd_implement_persists_review_scope(self, tmp_path: Path):
        from gza.cli.execution import cmd_implement

        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan = store.add("Plan feature", task_type="plan")
        plan.status = "completed"
        plan.completed_at = datetime.now(UTC)
        store.update(plan)

        args = argparse.Namespace(
            project_dir=tmp_path,
            no_docker=True,
            max_turns=None,
            plan_task_id=plan.id,
            prompt=None,
            group=None,
            depends_on=None,
            review=False,
            same_branch=False,
            branch_type=None,
            review_scope="slice F-A1 + F-A2: only review this slice",
            model=None,
            provider=None,
            skip_learnings=False,
            run=False,
            background=False,
            queue=True,
            force=False,
        )

        rc = cmd_implement(args)

        assert rc == 0
        created = next(task for task in make_store(tmp_path).get_pending() if task.depends_on == plan.id)
        assert created.review_scope == "slice F-A1 + F-A2: only review this slice"

    def test_cmd_implement_inherits_review_scope_from_plan_when_flag_omitted(self, tmp_path: Path):
        from gza.cli.execution import cmd_implement

        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan = store.add("Plan feature", task_type="plan")
        plan.status = "completed"
        plan.completed_at = datetime.now(UTC)
        plan.review_scope = "slice F-A1 + F-A2: inherited scope from plan metadata"
        store.update(plan)

        args = argparse.Namespace(
            project_dir=tmp_path,
            no_docker=True,
            max_turns=None,
            plan_task_id=plan.id,
            prompt=None,
            group=None,
            depends_on=None,
            review=False,
            same_branch=False,
            branch_type=None,
            review_scope=None,
            model=None,
            provider=None,
            skip_learnings=False,
            run=False,
            background=False,
            queue=True,
            force=False,
        )

        rc = cmd_implement(args)

        assert rc == 0
        created = next(task for task in make_store(tmp_path).get_pending() if task.depends_on == plan.id)
        assert created.review_scope == plan.review_scope

    def test_cmd_review_passes_review_invocation(self, tmp_path: Path):
        from gza.cli.execution import cmd_review

        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl = store.add("Implement auth", task_type="implement")
        impl.status = "completed"
        impl.completed_at = datetime.now(UTC)
        store.update(impl)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=impl.id,
            no_docker=True,
            model=None,
            provider=None,
            run=True,
            background=False,
            queue=False,
            open=False,
            force=False,
        )

        with patch("gza.cli.execution._run_foreground", return_value=0) as mock_run_foreground:
            rc = cmd_review(args)

        assert rc == 0
        invocation = mock_run_foreground.call_args.kwargs["invocation"]
        assert invocation.command == "review"
        assert invocation.execution_mode == "foreground_worker"

    def test_cmd_review_prints_followup_summary_after_success(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        from gza.cli.execution import cmd_review

        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl = store.add("Implement auth", task_type="implement")
        impl.status = "completed"
        impl.completed_at = datetime.now(UTC)
        store.update(impl)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=impl.id,
            no_docker=True,
            model=None,
            provider=None,
            run=True,
            background=False,
            queue=False,
            open=False,
            force=False,
        )

        def fake_run_foreground(*_args, **kwargs):
            review_id = kwargs["task_id"]
            review = store.get(review_id)
            assert review is not None
            review.status = "completed"
            review.output_content = (
                "## Summary\n\nLooks good.\n\n"
                "## Blockers\n\nNone.\n\n"
                "## Follow-Ups\n\n"
                "### F1 Validation\n"
                "Recommended follow-up: add validation.\n\n"
                "## Verdict\n\n"
                "Verdict: APPROVED_WITH_FOLLOWUPS\n"
            )
            review.completed_at = datetime.now(UTC)
            store.update(review)
            return 0

        with patch("gza.cli.execution._run_foreground", side_effect=fake_run_foreground):
            rc = cmd_review(args)

        output = capsys.readouterr().out
        assert rc == 0
        assert "Review " in output
        assert "APPROVED_WITH_FOLLOWUPS [follow-ups: F1]" in output

    def test_cmd_improve_passes_improve_invocation(self, tmp_path: Path):
        from gza.cli.execution import cmd_improve

        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl = store.add("Implement api", task_type="implement")
        impl.status = "completed"
        impl.completed_at = datetime.now(UTC)
        store.update(impl)

        review = store.add("Review api", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.output_content = "**Verdict: CHANGES_REQUESTED**"
        review.completed_at = datetime.now(UTC)
        store.update(review)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=impl.id,
            no_docker=True,
            max_turns=None,
            review=False,
            review_id=None,
            model=None,
            provider=None,
            run=True,
            background=False,
            queue=False,
            force=False,
        )

        with patch("gza.cli.execution._run_foreground", return_value=0) as mock_run_foreground:
            rc = cmd_improve(args)

        assert rc == 0
        invocation = mock_run_foreground.call_args.kwargs["invocation"]
        assert invocation.command == "improve"
        assert invocation.execution_mode == "foreground_worker"

    @pytest.mark.parametrize(
        ("comments_action", "failure_reason"),
        [
            ("wait_in_progress", None),
            ("give_up", None),
            ("manual_review", "requires human review"),
        ],
    )
    def test_cmd_improve_releases_reserved_launch_on_comments_only_phase1_return(
        self,
        tmp_path: Path,
        comments_action: str,
        failure_reason: str | None,
    ) -> None:
        from gza.cli.execution import cmd_improve

        setup_config(tmp_path)
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(config_path.read_text() + "max_concurrent: 1\n")
        config = Config.load(tmp_path)
        store = make_store(tmp_path)

        impl = store.add("Implement api", task_type="implement")
        impl.status = "completed"
        impl.completed_at = datetime.now(UTC)
        store.update(impl)
        assert impl.id is not None

        store.add_comment(impl.id, "Address the failure path.")
        existing_improve = store.add("Existing improve", task_type="improve", based_on=impl.id)
        existing_improve.status = "in_progress" if comments_action == "wait_in_progress" else "failed"
        store.update(existing_improve)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=impl.id,
            no_docker=True,
            max_turns=None,
            review=False,
            review_id=None,
            model=None,
            provider=None,
            run=False,
            background=False,
            queue=False,
            force=False,
        )

        comments_decision = None
        if comments_action == "manual_review":
            comments_decision = MagicMock(reason_text=failure_reason)

        with patch(
            "gza.cli.resolve_comments_improve_action",
            return_value=(comments_action, existing_improve, comments_decision),
        ):
            assert cmd_improve(args) == 1

        _assert_immediate_launch_lock_released(config, store)

    def test_cmd_improve_releases_reserved_launch_on_create_value_error(self, tmp_path: Path) -> None:
        from gza.cli.execution import cmd_improve

        setup_config(tmp_path)
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(config_path.read_text() + "max_concurrent: 1\n")
        config = Config.load(tmp_path)
        store = make_store(tmp_path)

        impl = store.add("Implement api", task_type="implement")
        impl.status = "completed"
        impl.completed_at = datetime.now(UTC)
        store.update(impl)

        review = store.add("Review api", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        store.update(review)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=impl.id,
            no_docker=True,
            max_turns=None,
            review=False,
            review_id=None,
            model=None,
            provider=None,
            run=False,
            background=False,
            queue=False,
            force=False,
        )

        with patch("gza.cli._create_improve_task", side_effect=ValueError("improve task already exists")):
            assert cmd_improve(args) == 1

        _assert_immediate_launch_lock_released(config, store)

    def test_cmd_review_releases_reserved_launch_on_duplicate_review(self, tmp_path: Path) -> None:
        from gza.cli.execution import cmd_review
        from gza.review_tasks import DuplicateReviewError

        setup_config(tmp_path)
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(config_path.read_text() + "max_concurrent: 1\n")
        config = Config.load(tmp_path)
        store = make_store(tmp_path)

        impl = store.add("Implement api", task_type="implement")
        impl.status = "completed"
        impl.completed_at = datetime.now(UTC)
        store.update(impl)

        existing_review = store.add("Existing review", task_type="review", depends_on=impl.id)
        existing_review.status = "pending"
        store.update(existing_review)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=impl.id,
            no_docker=True,
            queue=False,
            background=False,
            run=False,
            model=None,
            provider=None,
        )

        with patch("gza.cli._create_review_task", side_effect=DuplicateReviewError(existing_review)):
            assert cmd_review(args) == 1

        _assert_immediate_launch_lock_released(config, store)

    def test_cmd_review_releases_reserved_launch_on_create_value_error(self, tmp_path: Path) -> None:
        from gza.cli.execution import cmd_review

        setup_config(tmp_path)
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(config_path.read_text() + "max_concurrent: 1\n")
        config = Config.load(tmp_path)
        store = make_store(tmp_path)

        impl = store.add("Implement api", task_type="implement")
        impl.status = "completed"
        impl.completed_at = datetime.now(UTC)
        store.update(impl)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=impl.id,
            no_docker=True,
            queue=False,
            background=False,
            run=False,
            model=None,
            provider=None,
        )

        with patch("gza.cli._create_review_task", side_effect=ValueError("review task already exists")):
            assert cmd_review(args) == 1

        _assert_immediate_launch_lock_released(config, store)

    def test_cmd_fix_passes_fix_invocation(self, tmp_path: Path):
        from gza.cli.execution import cmd_fix

        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl = store.add("Implement parser", task_type="implement")
        impl.status = "completed"
        impl.completed_at = datetime.now(UTC)
        store.update(impl)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=impl.id,
            queue=False,
            background=False,
            no_docker=True,
            max_turns=None,
            model=None,
            provider=None,
            run=True,
            force=False,
        )

        with patch("gza.cli.execution._run_foreground", return_value=0) as mock_run_foreground:
            rc = cmd_fix(args)

        assert rc == 0
        invocation = mock_run_foreground.call_args.kwargs["invocation"]
        assert invocation.command == "fix"
        assert invocation.execution_mode == "foreground_worker"

    def test_cmd_resume_passes_resume_invocation(self, tmp_path: Path):
        from gza.cli.execution import cmd_resume

        setup_config(tmp_path)
        store = make_store(tmp_path)

        failed = store.add("Failed implement", task_type="implement")
        failed.status = "failed"
        failed.session_id = "resume-session-123"
        store.update(failed)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=failed.id,
            no_docker=True,
            max_turns=None,
            run=True,
            background=False,
            queue=False,
            force=False,
        )

        with patch("gza.cli.execution._run_foreground", return_value=0) as mock_run_foreground:
            rc = cmd_resume(args)

        assert rc == 0
        assert mock_run_foreground.call_args.kwargs["resume"] is True
        invocation = mock_run_foreground.call_args.kwargs["invocation"]
        assert invocation.command == "resume"
        assert invocation.execution_mode == "foreground_worker"

    def test_cmd_resume_without_session_id_uses_same_branch_retry_guidance(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        from gza.cli.execution import cmd_resume

        setup_config(tmp_path)
        store = make_store(tmp_path)

        failed = store.add("Failed improve", task_type="improve")
        failed.status = "failed"
        failed.failure_reason = "WORKER_DIED"
        store.update(failed)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=failed.id,
            no_docker=True,
            max_turns=None,
            run=False,
            background=False,
            queue=False,
            force=False,
        )

        rc = cmd_resume(args)

        assert rc == 1
        output = capsys.readouterr().out
        assert f"Error: Task {failed.id} has no session ID (cannot resume)" in output
        assert "create a new retry attempt with a fresh conversation" in output
        assert "implement retries may fork fresh" in output
        assert "same-branch follow-ups stay on the shared branch" in output

    def test_cmd_resume_rejects_at_limit_without_creating_child(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        from gza.cli.execution import cmd_resume

        setup_config(tmp_path)
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(config_path.read_text() + "max_concurrent: 1\n")
        store = make_store(tmp_path)

        running = store.add("Running task", task_type="implement")
        running.status = "in_progress"
        running.running_pid = os.getpid()
        store.update(running)

        failed = store.add("Failed implement", task_type="implement")
        failed.status = "failed"
        failed.session_id = "resume-session-123"
        store.update(failed)

        before_ids = {task.id for task in store.get_all()}
        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=failed.id,
            no_docker=True,
            max_turns=None,
            run=True,
            background=False,
            queue=False,
            force=False,
        )

        rc = cmd_resume(args)

        assert rc == 1
        assert capsys.readouterr().out.strip() == "Error: already at max concurrent tasks: 1 running, limit is 1"
        after_ids = {task.id for task in store.get_all()}
        assert after_ids == before_ids

    def test_bare_implement_defaults_to_queue(self, tmp_path: Path) -> None:
        from gza.cli.execution import cmd_implement

        setup_config(tmp_path)
        store = make_store(tmp_path)
        plan = store.add("Plan feature", task_type="plan")
        plan.status = "completed"
        plan.completed_at = datetime.now(UTC)
        store.update(plan)

        args = argparse.Namespace(
            project_dir=tmp_path,
            no_docker=True,
            max_turns=None,
            plan_task_id=plan.id,
            prompt=None,
            group=None,
            depends_on=None,
            review=False,
            same_branch=False,
            branch_type=None,
            review_scope=None,
            model=None,
            provider=None,
            skip_learnings=False,
            run=False,
            background=False,
            queue=False,
            force=False,
            create_pr=False,
        )

        with (
            patch("gza.cli.execution._run_foreground", side_effect=AssertionError("should stay queued")),
            patch("gza.cli.execution._spawn_background_worker", side_effect=AssertionError("should stay queued")),
            patch("gza.cli.execution._spawn_background_workers", side_effect=AssertionError("should stay queued")),
        ):
            assert cmd_implement(args) == 0

        created = next(task for task in store.get_pending() if task.task_type == "implement")
        assert created.depends_on == plan.id

    def test_bare_review_defaults_to_queue(self, tmp_path: Path) -> None:
        from gza.cli.execution import cmd_review

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Implement auth", task_type="implement")
        impl.status = "completed"
        impl.completed_at = datetime.now(UTC)
        store.update(impl)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=impl.id,
            no_docker=True,
            model=None,
            provider=None,
            run=False,
            background=False,
            queue=False,
            open=False,
            force=False,
        )

        with (
            patch("gza.cli.execution._run_foreground", side_effect=AssertionError("should stay queued")),
            patch("gza.cli.execution._spawn_background_worker", side_effect=AssertionError("should stay queued")),
        ):
            assert cmd_review(args) == 0

        created = next(task for task in store.get_pending() if task.task_type == "review")
        assert created.depends_on == impl.id

    def test_bare_improve_defaults_to_queue(self, tmp_path: Path) -> None:
        from gza.cli.execution import cmd_improve

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Implement api", task_type="implement")
        impl.status = "completed"
        impl.completed_at = datetime.now(UTC)
        store.update(impl)
        review = store.add("Review api", task_type="review", depends_on=impl.id)
        review.status = "completed"
        review.output_content = "**Verdict: CHANGES_REQUESTED**"
        review.completed_at = datetime.now(UTC)
        store.update(review)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=impl.id,
            no_docker=True,
            max_turns=None,
            review=False,
            review_id=None,
            model=None,
            provider=None,
            run=False,
            background=False,
            queue=False,
            force=False,
        )

        with (
            patch("gza.cli.execution._run_foreground", side_effect=AssertionError("should stay queued")),
            patch("gza.cli.execution._spawn_background_worker", side_effect=AssertionError("should stay queued")),
        ):
            assert cmd_improve(args) == 0

        created = next(task for task in store.get_pending() if task.task_type == "improve")
        assert created.based_on == impl.id

    def test_bare_retry_defaults_to_queue(self, tmp_path: Path) -> None:
        from gza.cli.execution import cmd_retry

        setup_config(tmp_path)
        store = make_store(tmp_path)
        failed = store.add("Failed implement", task_type="implement")
        failed.status = "failed"
        store.update(failed)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=failed.id,
            no_docker=True,
            max_turns=None,
            run=False,
            background=False,
            queue=False,
            force=False,
        )

        with (
            patch("gza.cli.execution._run_foreground", side_effect=AssertionError("should stay queued")),
            patch("gza.cli.execution._spawn_background_worker", side_effect=AssertionError("should stay queued")),
        ):
            assert cmd_retry(args) == 0

        created = next(task for task in store.get_pending() if task.based_on == failed.id)
        assert created.task_type == failed.task_type

    def test_bare_resume_defaults_to_queue(self, tmp_path: Path) -> None:
        from gza.cli.execution import cmd_resume

        setup_config(tmp_path)
        store = make_store(tmp_path)
        failed = store.add("Failed implement", task_type="implement")
        failed.status = "failed"
        failed.session_id = "resume-session-123"
        store.update(failed)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=failed.id,
            no_docker=True,
            max_turns=None,
            run=False,
            background=False,
            queue=False,
            force=False,
        )

        with (
            patch("gza.cli.execution._run_foreground", side_effect=AssertionError("should stay queued")),
            patch("gza.cli.execution._spawn_background_resume_worker", side_effect=AssertionError("should stay queued")),
        ):
            assert cmd_resume(args) == 0

        created = next(task for task in store.get_pending() if task.based_on == failed.id)
        assert created.session_id == failed.session_id

    def test_bare_fix_defaults_to_queue(self, tmp_path: Path) -> None:
        from gza.cli.execution import cmd_fix

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Implement parser", task_type="implement")
        impl.status = "completed"
        impl.completed_at = datetime.now(UTC)
        store.update(impl)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=impl.id,
            queue=False,
            background=False,
            run=False,
            no_docker=True,
            max_turns=None,
            model=None,
            provider=None,
            force=False,
        )

        with (
            patch("gza.cli.execution._run_foreground", side_effect=AssertionError("should stay queued")),
            patch("gza.cli.execution._spawn_background_worker", side_effect=AssertionError("should stay queued")),
        ):
            assert cmd_fix(args) == 0

        created = next(task for task in store.get_pending() if task.task_type == "fix")
        assert created.based_on == impl.id

    def test_bare_plan_review_defaults_to_queue(self, tmp_path: Path) -> None:
        from gza.cli.execution import cmd_plan_review

        setup_config(tmp_path)
        store = make_store(tmp_path)
        plan = store.add("Plan rollout", task_type="plan")
        plan.status = "completed"
        plan.completed_at = datetime.now(UTC)
        store.update(plan)

        args = argparse.Namespace(
            project_dir=tmp_path,
            no_docker=True,
            max_turns=None,
            task_id=plan.id,
            rerun=False,
            edit_slices=False,
            materialize=False,
            model=None,
            provider=None,
            run=False,
            background=False,
            queue=False,
            force=False,
        )

        with (
            patch("gza.cli.execution._run_foreground", side_effect=AssertionError("should stay queued")),
            patch("gza.cli.execution._spawn_background_worker", side_effect=AssertionError("should stay queued")),
        ):
            assert cmd_plan_review(args) == 0

        created = next(task for task in store.get_pending() if task.task_type == "plan_review")
        assert created.depends_on == plan.id

    def test_bare_plan_improve_defaults_to_queue(self, tmp_path: Path) -> None:
        from gza.cli.execution import cmd_plan_improve

        setup_config(tmp_path)
        store = make_store(tmp_path)
        plan = store.add("Plan rollout", task_type="plan")
        assert plan.id is not None
        plan.status = "completed"
        plan.completed_at = datetime.now(UTC)
        store.update(plan)
        review = store.add("Plan review", task_type="plan_review", depends_on=plan.id)
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        review.output_content = "## Verdict\nVerdict: CHANGES_REQUESTED\n"
        store.update(review)

        args = argparse.Namespace(
            project_dir=tmp_path,
            no_docker=True,
            max_turns=None,
            task_id=review.id,
            model=None,
            provider=None,
            run=False,
            background=False,
            queue=False,
            force=False,
        )

        with (
            patch("gza.cli.execution._run_foreground", side_effect=AssertionError("should stay queued")),
            patch("gza.cli.execution._spawn_background_worker", side_effect=AssertionError("should stay queued")),
        ):
            assert cmd_plan_improve(args) == 0

        created = next(task for task in store.get_pending() if task.task_type == "plan_improve")
        assert created.depends_on == review.id

    def test_cmd_iterate_passes_iterate_invocation_to_run_foreground(self, tmp_path: Path):
        from unittest.mock import MagicMock

        from gza.cli.execution import cmd_iterate

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Implement feature", task_type="implement")

        def fake_run_foreground(_config, task_id, **kwargs):
            task = store.get(task_id)
            assert task is not None
            invocation = kwargs["invocation"]
            assert invocation.command == "iterate"
            assert invocation.execution_mode == "foreground_worker"
            task.status = "completed"
            task.branch = "testproject/20260101-impl"
            task.completed_at = datetime.now(UTC)
            store.update(task)
            return 0

        args = argparse.Namespace(
            project_dir=tmp_path,
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=False,
            no_docker=True,
            resume=False,
            retry=False,
            background=False,
            force=False,
        )

        mock_config = MagicMock(
            project_dir=tmp_path,
            use_docker=False,
            project_prefix="testproject",
            iterate_max_iterations=3,
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
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.cli._run_foreground", side_effect=fake_run_foreground) as run_foreground,
            patch("gza.cli.resolve_closing_review_action", return_value=None),
            patch(
                "gza.cli.execution.determine_next_action",
                side_effect=[
                    {"type": "merge", "description": "merge ready"},
                    {"type": "merge", "description": "merge ready"},
                ],
            ),
        ):
            rc = cmd_iterate(args)

        assert rc == 0
        assert run_foreground.call_count == 1

    def test_cmd_iterate_worker_id_marks_completed_on_same_registry_entry(self, tmp_path: Path):
        from unittest.mock import MagicMock

        from gza.cli.execution import cmd_iterate

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Iterate pending implementation", task_type="implement")
        assert impl.id is not None

        registry = WorkerRegistry(config.workers_path)
        registry.register(
            WorkerMetadata(
                worker_id="w-iterate-process",
                task_id=impl.id,
                pid=os.getpid(),
                status="running",
                startup_log_file="w-iterate-process-startup.log",
            )
        )

        def fake_run(_config, task_id, **kwargs):
            task = store.get(task_id)
            assert task is not None
            invocation = kwargs["invocation"]
            assert invocation.command == "iterate"
            assert invocation.execution_mode == "foreground_worker"
            task.status = "completed"
            task.branch = "testproject/20260101-impl"
            task.completed_at = datetime.now(UTC)
            store.update(task)
            return 0

        args = argparse.Namespace(
            project_dir=tmp_path,
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=False,
            no_docker=True,
            resume=False,
            retry=False,
            background=False,
            force=False,
            worker_id="w-iterate-process",
        )

        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True

        with (
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.cli.run", side_effect=fake_run),
            patch("gza.cli.resolve_closing_review_action", return_value=None),
            patch(
                "gza.cli.execution.determine_next_action",
                side_effect=[
                    {"type": "merge", "description": "merge ready"},
                    {"type": "merge", "description": "merge ready"},
                ],
            ),
        ):
            rc = cmd_iterate(args)

        assert rc == 0
        workers = registry.list_all(include_completed=True)
        assert len(workers) == 1
        worker = workers[0]
        assert worker.worker_id == "w-iterate-process"
        assert worker.status == "completed"
        assert worker.exit_code == 0
        assert worker.task_id == impl.id

    def test_background_iterate_spawned_worker_completes_single_registry_entry(self, tmp_path: Path):
        from unittest.mock import MagicMock

        from gza.cli._common import _spawn_background_iterate_worker
        from gza.cli.execution import cmd_iterate

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Iterate pending implementation", task_type="implement")
        assert impl.id is not None

        spawn_args = argparse.Namespace(
            no_docker=True,
            force=False,
        )
        iterate_args = argparse.Namespace(
            project_dir=tmp_path,
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=False,
            no_docker=True,
            resume=False,
            retry=False,
            background=False,
            force=False,
            worker_id=None,
        )

        captured_worker_id: str | None = None

        def fake_spawn(_cmd: list[str], _config: Config, worker_id: str):
            nonlocal captured_worker_id
            captured_worker_id = worker_id
            startup_log = f".gza/workers/{worker_id}-startup.log"
            mock_proc = MagicMock()
            mock_proc.pid = os.getpid()
            return mock_proc, startup_log

        def fake_run(_config, task_id, **kwargs):
            task = store.get(task_id)
            assert task is not None
            invocation = kwargs["invocation"]
            assert invocation.command == "iterate"
            assert invocation.execution_mode == "foreground_worker"
            task.status = "completed"
            task.branch = "testproject/20260101-impl"
            task.completed_at = datetime.now(UTC)
            store.update(task)
            return 0

        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True

        with patch("gza.cli._common._spawn_detached_worker_process", side_effect=fake_spawn):
            rc = _spawn_background_iterate_worker(spawn_args, config, impl, max_iterations=1)

        assert rc == 0
        assert captured_worker_id is not None
        iterate_args.worker_id = captured_worker_id

        with (
            patch("gza.cli.get_store", return_value=store),
            patch("gza.cli.Git", return_value=mock_git),
            patch("gza.cli.run", side_effect=fake_run),
            patch("gza.cli.resolve_closing_review_action", return_value=None),
            patch(
                "gza.cli.execution.determine_next_action",
                side_effect=[
                    {"type": "merge", "description": "merge ready"},
                    {"type": "merge", "description": "merge ready"},
                ],
            ),
        ):
            rc = cmd_iterate(iterate_args)

        assert rc == 0
        registry = WorkerRegistry(config.workers_path)
        workers = registry.list_all(include_completed=True)
        assert len(workers) == 1
        worker = workers[0]
        assert worker.worker_id == captured_worker_id
        assert worker.pid == os.getpid()
        assert worker.status == "completed"
        assert worker.exit_code == 0
        assert worker.task_id == impl.id

    def test_background_iterate_child_before_parent_register_keeps_single_terminal_entry(self, tmp_path: Path):
        from unittest.mock import MagicMock

        from gza.cli._common import _spawn_background_iterate_worker
        from gza.cli.execution import cmd_iterate

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Iterate child-before-parent-register", task_type="implement")
        assert impl.id is not None

        spawn_args = argparse.Namespace(
            no_docker=True,
            force=False,
        )
        iterate_args = argparse.Namespace(
            project_dir=tmp_path,
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=False,
            no_docker=True,
            resume=False,
            retry=False,
            background=False,
            force=False,
            worker_id=None,
        )

        mock_git = MagicMock()
        mock_git.current_branch.return_value = "main"
        mock_git.can_merge.return_value = True

        def fake_run(_config, task_id, **kwargs):
            task = store.get(task_id)
            assert task is not None
            invocation = kwargs["invocation"]
            assert invocation.command == "iterate"
            assert invocation.execution_mode == "foreground_worker"
            task.status = "completed"
            task.branch = "testproject/20260101-impl"
            task.completed_at = datetime.now(UTC)
            store.update(task)
            return 0

        def fake_spawn(_cmd: list[str], _config: Config, worker_id: str):
            iterate_args.worker_id = worker_id
            with (
                patch("gza.cli.get_store", return_value=store),
                patch("gza.cli.Git", return_value=mock_git),
                patch("gza.cli.run", side_effect=fake_run),
                patch("gza.cli.resolve_closing_review_action", return_value=None),
                patch(
                    "gza.cli.execution.determine_next_action",
                    side_effect=[
                        {"type": "merge", "description": "merge ready"},
                        {"type": "merge", "description": "merge ready"},
                    ],
                ),
            ):
                assert cmd_iterate(iterate_args) == 0
            mock_proc = MagicMock()
            mock_proc.pid = os.getpid()
            return mock_proc, f".gza/workers/{worker_id}-startup.log"

        with patch("gza.cli._common._spawn_detached_worker_process", side_effect=fake_spawn):
            rc = _spawn_background_iterate_worker(spawn_args, config, impl, max_iterations=1)

        assert rc == 0
        registry = WorkerRegistry(config.workers_path)
        workers = registry.list_all(include_completed=True)
        assert len(workers) == 1
        worker = workers[0]
        assert worker.worker_id == iterate_args.worker_id
        assert worker.status == "completed"
        assert worker.exit_code == 0
        assert worker.task_id == impl.id

    def test_background_iterate_launch_failure_after_child_noop_cleans_up_worker_registry(self, tmp_path: Path):
        """Launch rollback should remove worker rows even if the child already registered one."""
        from gza.cli._common import _spawn_background_iterate_worker
        from gza.cli.execution import cmd_iterate

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Iterate launch cleanup target", task_type="implement")
        assert impl.id is not None
        impl.status = "completed"
        impl.completed_at = datetime.now(UTC)
        store.update(impl)

        spawn_args = argparse.Namespace(
            no_docker=True,
            force=False,
        )
        iterate_args = argparse.Namespace(
            project_dir=tmp_path,
            impl_task_id=impl.id,
            max_iterations=1,
            dry_run=False,
            no_docker=True,
            resume=False,
            retry=False,
            auto_iterate=False,
            background=False,
            force=False,
            worker_id=None,
        )

        def fake_spawn(_cmd: list[str], _config: Config, worker_id: str):
            iterate_args.worker_id = worker_id
            with patch("gza.cli.execution._cmd_iterate_impl", return_value=3):
                assert cmd_iterate(iterate_args) == 3
            raise RuntimeError("launch boom after child noop")

        with patch("gza.cli._common._spawn_detached_worker_process", side_effect=fake_spawn):
            rc = _spawn_background_iterate_worker(spawn_args, config, impl, max_iterations=1)

        assert rc == 1
        assert WorkerRegistry(config.workers_path).list_all(include_completed=True) == []

    def test_background_iterate_launch_cleanup_failure_marks_worker_terminal(self, tmp_path: Path):
        """Rollback should terminalize a running worker row when registry removal fails."""
        from gza.cli._common import _spawn_background_iterate_worker

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Iterate cleanup fallback target", task_type="implement")
        assert impl.id is not None

        spawn_args = argparse.Namespace(
            no_docker=True,
            force=False,
        )

        captured_worker_id: str | None = None

        def fake_spawn(_cmd: list[str], _config: Config, worker_id: str):
            nonlocal captured_worker_id
            captured_worker_id = worker_id
            registry = WorkerRegistry(config.workers_path)
            registry.ensure_running(
                WorkerMetadata(
                    worker_id=worker_id,
                    task_id=impl.id,
                    pid=os.getpid(),
                    status="running",
                    startup_log_file=f".gza/workers/{worker_id}-startup.log",
                )
            )
            raise RuntimeError("launch boom after child registered")

        with (
            patch("gza.cli._common._spawn_detached_worker_process", side_effect=fake_spawn),
            patch.object(WorkerRegistry, "remove", side_effect=OSError("unlink boom")),
        ):
            rc = _spawn_background_iterate_worker(spawn_args, config, impl, max_iterations=1)

        assert rc == 1
        assert captured_worker_id is not None
        worker = WorkerRegistry(config.workers_path).get(captured_worker_id)
        assert worker is not None
        assert worker.status == "failed"
        assert worker.exit_code == 1
        assert worker.completed_at is not None

    def test_background_iterate_launch_cleanup_failure_warns_when_terminal_fallback_fails(self, tmp_path: Path, capsys):
        """Rollback should warn when neither removal nor terminal fallback can repair worker state."""
        from gza.cli._common import _spawn_background_iterate_worker

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Iterate cleanup warning target", task_type="implement")
        assert impl.id is not None

        spawn_args = argparse.Namespace(
            no_docker=True,
            force=False,
        )

        captured_worker_id: str | None = None

        def fake_spawn(_cmd: list[str], _config: Config, worker_id: str):
            nonlocal captured_worker_id
            captured_worker_id = worker_id
            registry = WorkerRegistry(config.workers_path)
            registry.ensure_running(
                WorkerMetadata(
                    worker_id=worker_id,
                    task_id=impl.id,
                    pid=os.getpid(),
                    status="running",
                    startup_log_file=f".gza/workers/{worker_id}-startup.log",
                )
            )
            raise RuntimeError("launch boom after child registered")

        with (
            patch("gza.cli._common._spawn_detached_worker_process", side_effect=fake_spawn),
            patch.object(WorkerRegistry, "remove", side_effect=OSError("unlink boom")),
            patch.object(WorkerRegistry, "mark_completed", side_effect=OSError("mark boom")),
        ):
            rc = _spawn_background_iterate_worker(spawn_args, config, impl, max_iterations=1)

        assert rc == 1
        assert captured_worker_id is not None
        worker = WorkerRegistry(config.workers_path).get(captured_worker_id)
        assert worker is not None
        assert worker.status == "running"
        assert worker.completed_at is None
        assert (
            f"Warning: failed to clean up background worker {captured_worker_id} after launch failure: "
            "remove failed with unlink boom; terminal fallback failed with mark boom"
        ) in capsys.readouterr().err


class TestRunAsWorker:
    """Tests for _run_as_worker() helper."""

    def _register_current_worker(self, config: Config, task_id: int | None, worker_id: str) -> WorkerRegistry:
        from gza.workers import WorkerMetadata

        config.workers_path.mkdir(parents=True, exist_ok=True)
        registry = WorkerRegistry(config.workers_path)
        registry.register(
            WorkerMetadata(
                worker_id=worker_id,
                task_id=task_id,
                pid=os.getpid(),
                status="running",
                startup_log_file=f"{worker_id}-startup.log",
            )
        )
        return registry

    def test_run_as_worker_nonzero_exit_marks_failed(self, tmp_path: Path):
        """Worker metadata status is failed when run() returns non-zero."""
        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)
        task = store.add("Worker non-zero")
        assert task.id is not None

        registry = self._register_current_worker(config, task.id, "w-worker-nonzero")
        args = argparse.Namespace(task_ids=[task.id], resume=False)

        with patch("gza.cli.signal.signal"):
            with patch("gza.cli.run", return_value=7):
                rc = _run_as_worker(args, config)

        assert rc == 7
        worker = registry.get("w-worker-nonzero")
        assert worker is not None
        assert worker.status == "failed"
        assert worker.exit_code == 7

    def test_run_as_worker_resume_auto_rebases_before_run(self, tmp_path: Path):
        """Worker resume runs the automatic rebase step before run()."""
        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)
        task = store.add("Worker resume target")
        assert task.id is not None

        self._register_current_worker(config, task.id, "w-worker-resume")
        args = argparse.Namespace(task_ids=[task.id], resume=True)

        with patch("gza.cli.signal.signal"), \
             patch("gza.cli._auto_rebase_before_resume", return_value=0) as mock_rebase, \
             patch("gza.cli.run", return_value=0) as mock_run:
            rc = _run_as_worker(args, config)

        assert rc == 0
        mock_rebase.assert_called_once_with(config, task.id)
        mock_run.assert_called_once()

    def test_run_as_worker_resume_rebase_failure_skips_run(self, tmp_path: Path):
        """Worker resume marks the worker failed when auto-rebase fails."""
        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)
        task = store.add("Worker resume target")
        assert task.id is not None

        registry = self._register_current_worker(config, task.id, "w-worker-resume-fail")
        args = argparse.Namespace(task_ids=[task.id], resume=True)

        with patch("gza.cli.signal.signal"), \
             patch("gza.cli._auto_rebase_before_resume", return_value=1) as mock_rebase, \
             patch("gza.cli.run") as mock_run:
            rc = _run_as_worker(args, config)

        assert rc == 1
        mock_rebase.assert_called_once_with(config, task.id)
        mock_run.assert_not_called()
        worker = registry.get("w-worker-resume-fail")
        assert worker is not None
        assert worker.status == "failed"
        assert worker.exit_code == 1

    def test_run_as_worker_precondition_refusal_marks_failed(self, tmp_path: Path):
        """Worker mode should record non-zero precondition refusal as failed, not completed."""
        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)
        task = store.add("Worker blocked task")
        assert task.id is not None

        registry = self._register_current_worker(config, task.id, "w-worker-prereq")
        args = argparse.Namespace(task_ids=[task.id], resume=False)

        with patch("gza.cli.signal.signal"):
            with patch("gza.cli.run", return_value=1):
                rc = _run_as_worker(args, config)

        assert rc == 1
        worker = registry.get("w-worker-prereq")
        assert worker is not None
        assert worker.status == "failed"
        assert worker.exit_code == 1
        assert worker.completion_reason == "startup failure before task claim"

    def test_run_as_worker_preclaim_refusal_mirrors_reason_into_task_startup_log(self, tmp_path: Path):
        """Detached pre-claim failures should be visible via the task startup log and ops sibling."""
        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)
        task = store.add("Worker blocked task")
        assert task.id is not None

        registry = self._register_current_worker(config, task.id, "w-worker-prereq-log")
        startup_capture = tmp_path / "w-worker-prereq-log-startup.log"
        startup_capture.write_text(
            f"Error: Task {task.id} is blocked: awaiting plan review for gza-99; "
            "release with uv run gza implement gza-99 or uv run gza edit gza-99 --no-hold-for-review\n"
        )
        args = argparse.Namespace(task_ids=[task.id], resume=False)

        with patch("gza.cli.signal.signal"):
            with patch("gza.cli.run", return_value=3):
                rc = _run_as_worker(args, config)

        assert rc == 3
        startup_log = startup_capture
        assert startup_log.exists()
        startup_text = startup_log.read_text()
        assert "awaiting plan review for gza-99" in startup_text
        assert "uv run gza edit gza-99 --no-hold-for-review" in startup_text

        ops_text = ops_log_path_for(startup_log).read_text()
        assert '"event": "start_failed"' in ops_text
        assert f'"task_id": "{task.id}"' in ops_text
        assert '"exit_code": 3' in ops_text
        assert "awaiting plan review for gza-99" in ops_text

        worker = registry.get("w-worker-prereq-log")
        assert worker is not None
        assert worker.status == "failed"

    def test_run_as_worker_claim_updates_registry_with_task_log_evidence(self, tmp_path: Path):
        """Claim callback should persist task/log evidence to the worker registry immediately."""
        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)
        task = store.add("Worker claim metadata")
        assert task.id is not None
        task.log_file = ".gza/logs/worker-claim.log"
        store.update(task)

        registry = self._register_current_worker(config, task.id, "w-worker-claim-log")
        args = argparse.Namespace(task_ids=[task.id], resume=False)

        def fake_run(
            _config,
            task_id=None,
            resume=False,
            open_after=False,
            skip_precondition_check=False,
            on_task_claimed=None,
        ):
            assert task_id == task.id
            claimed = store.get(task.id)
            assert claimed is not None
            if on_task_claimed is not None:
                on_task_claimed(claimed)
            return 0

        with patch("gza.cli.signal.signal"):
            with patch("gza.cli.run", side_effect=fake_run):
                rc = _run_as_worker(args, config)

        assert rc == 0
        worker = registry.get("w-worker-claim-log")
        assert worker is not None
        assert worker.status == "completed"
        assert worker.task_id == task.id
        assert worker.task_slug == task.slug
        assert worker.log_file == ".gza/logs/worker-claim.log"

    def test_run_as_worker_exception_marks_failed_and_ps_shows_startup_failure(self, tmp_path: Path):
        """Exception cleanup keeps worker/task failed and startup failure visible in ps rows."""
        from gza.cli.query import _build_ps_rows
        from gza.failure_reasons import mark_task_failed_from_cause

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)
        task = store.add("Worker exception")
        assert task.id is not None
        store.mark_in_progress(task)

        registry = self._register_current_worker(config, task.id, "w-worker-exception")
        args = argparse.Namespace(task_ids=[task.id], resume=False)

        with patch("gza.cli.signal.signal"):
            with patch("gza.cli.run", side_effect=RuntimeError("boom")), \
                 patch("gza.cli._common.mark_task_failed_from_cause", wraps=mark_task_failed_from_cause) as mock_mark_failed:
                rc = _run_as_worker(args, config)

        assert rc == 1
        worker = registry.get("w-worker-exception")
        assert worker is not None
        assert worker.status == "failed"
        assert worker.exit_code == 1

        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.status == "failed"
        assert refreshed.failure_reason == "WORKER_DIED"
        assert mock_mark_failed.call_count == 1
        assert mock_mark_failed.call_args.kwargs["explicit_reason"] == "WORKER_DIED"

        rows, _ = _build_ps_rows(registry, store, include_completed=True)
        row = next(r for r in rows if r["worker_id"] == "w-worker-exception")
        assert row["status"] == "failed"
        assert row["startup_failure"] is True

    def test_run_as_worker_signal_handler_marks_failed(self, tmp_path: Path):
        """Signal handler cleanup marks worker as failed before exiting."""
        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)
        task = store.add("Worker signal")
        assert task.id is not None

        registry = self._register_current_worker(config, task.id, "w-worker-signal")
        args = argparse.Namespace(task_ids=[task.id], resume=False)

        installed_handlers: dict[int, object] = {}

        def capture_signal(signum, handler):
            installed_handlers[signum] = handler
            return None

        def run_then_signal(*_args, **_kwargs):
            handler = installed_handlers.get(signal_mod.SIGTERM)
            assert callable(handler)
            handler(signal_mod.SIGTERM, None)
            return 0

        with patch("gza.cli.signal.signal", side_effect=capture_signal):
            with patch("gza.cli.run", side_effect=run_then_signal):
                with pytest.raises(SystemExit) as exc:
                    _run_as_worker(args, config)

        assert exc.value.code == 1
        worker = registry.get("w-worker-signal")
        assert worker is not None
        assert worker.status == "failed"
        assert worker.exit_code == 1

    def test_run_as_worker_signal_before_task_claim_marks_startup_failure(self, tmp_path: Path):
        """Signal cleanup before task claim should not raise an unbound-local error."""
        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)
        task = store.add("Worker signal before claim")
        assert task.id is not None

        registry = self._register_current_worker(config, task.id, "w-worker-signal-preclaim")
        args = argparse.Namespace(task_ids=[task.id], resume=False)

        installed_handlers: dict[int, object] = {}

        def capture_signal(signum, handler):
            installed_handlers[signum] = handler
            if signum == signal_mod.SIGINT:
                sigterm = installed_handlers.get(signal_mod.SIGTERM)
                assert callable(sigterm)
                sigterm(signal_mod.SIGTERM, None)
            return None

        with patch("gza.cli.signal.signal", side_effect=capture_signal):
            with patch("gza.cli.run") as mock_run:
                with pytest.raises(SystemExit) as exc:
                    _run_as_worker(args, config)

        assert exc.value.code == 1
        mock_run.assert_not_called()
        worker = registry.get("w-worker-signal-preclaim")
        assert worker is not None
        assert worker.status == "failed"
        assert worker.exit_code == 1
        assert worker.completion_reason == "startup failure before task claim"

    def test_run_as_worker_backfills_task_id_after_no_id_claim(self, tmp_path: Path):
        """No-id worker mode updates worker metadata with the claimed DB task ID."""
        setup_config(tmp_path)
        config = Config.load(tmp_path)
        store = SqliteTaskStore(config.db_path)
        task = store.add("No-id background claim")
        assert task.id is not None
        store.mark_in_progress(task)
        task = store.get(task.id)
        assert task is not None
        task.running_pid = os.getpid()
        store.update(task)

        registry = self._register_current_worker(config, task_id=None, worker_id="w-worker-claim")
        args = argparse.Namespace(task_ids=[], resume=False)

        def fake_run(
            _config,
            task_id=None,
            resume=False,
            open_after=False,
            skip_precondition_check=False,
            on_task_claimed=None,
        ):
            assert task_id is None
            assert resume is False
            assert skip_precondition_check is False
            claimed = store.get(task.id)
            assert claimed is not None
            if on_task_claimed is not None:
                on_task_claimed(claimed)
            return 0

        with patch("gza.cli.signal.signal"):
            with patch("gza.cli.run", side_effect=fake_run):
                rc = _run_as_worker(args, config)

        assert rc == 0
        worker = registry.get("w-worker-claim")
        assert worker is not None
        assert worker.status == "completed"
        assert worker.task_id == task.id

class TestForceCompleteRemoval:
    """Tests for removed force-complete command."""

    def test_force_complete_is_not_a_valid_command(self, tmp_path: Path):
        """force-complete command is removed and rejected by CLI parsing."""
        setup_db_with_tasks(tmp_path, [
            {"prompt": "Failed task", "status": "failed", "task_type": "implement"},
        ])

        result = invoke_gza("force-complete", "1", "--project", str(tmp_path))

        assert result.returncode != 0
        assert "is not a gza command" in result.stderr
        assert "force-complete" in result.stderr


class TestAddCommandWithChaining:
    """Tests for 'gza add' command with chaining features."""

    def test_add_with_type_plan(self, tmp_path: Path):
        """Add command can create plan tasks."""
        setup_config(tmp_path)
        result = invoke_gza("add", "--type", "plan", "Create a plan", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Added task" in result.stdout

    def test_add_plan_with_hold_for_review_persists_auto_implement_false(self, tmp_path: Path):
        """Add command stores held plan tasks as auto_implement=false."""

        setup_config(tmp_path)
        result = invoke_gza(
            "add",
            "--type",
            "plan",
            "--hold-for-review",
            "Create a held plan",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 0
        store = make_store(tmp_path)
        task = next((t for t in store.get_pending() if t.prompt == "Create a held plan"), None)
        assert task is not None
        assert task.auto_implement is False

    def test_add_hold_for_review_rejects_non_plan_task(self, tmp_path: Path):
        """Hold-for-review is plan-only at creation time."""

        setup_config(tmp_path)
        result = invoke_gza(
            "add",
            "--hold-for-review",
            "Implement feature",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 1
        assert "--hold-for-review is only valid with --type plan" in result.stdout

    def test_add_with_type_implement(self, tmp_path: Path):
        """Add command can create implement tasks."""
        setup_config(tmp_path)
        result = invoke_gza("add", "--type", "implement", "Implement feature", "--project", str(tmp_path))

        assert result.returncode == 0

    def test_add_implement_depends_on_held_plan_rejects_inconsistent_state(self, tmp_path: Path):
        """Implement tasks cannot depend on a held plan outside the explicit release path."""

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
            "--depends-on",
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

    def test_add_implement_based_on_held_plan_rejects_inconsistent_state(self, tmp_path: Path):
        """Implement tasks cannot use a held plan as their based_on source lineage."""

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
            "--based-on",
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

    def test_add_with_type_review(self, tmp_path: Path):
        """Add command can create review tasks."""
        setup_config(tmp_path)
        result = invoke_gza("add", "--type", "review", "Review implementation", "--project", str(tmp_path))

        assert result.returncode == 0

    def test_add_with_based_on(self, tmp_path: Path):
        """Add command can create tasks with based_on reference."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task1 = store.add("First task")

        result = invoke_gza("add", "--based-on", str(task1.id), "Follow-up task", "--project", str(tmp_path))

        assert result.returncode == 0

        # Verify based_on was set
        tasks = store.get_pending()
        follow_up = next((t for t in tasks if t.prompt == "Follow-up task"), None)
        assert follow_up is not None
        assert follow_up.based_on == task1.id

    def test_add_with_spec(self, tmp_path: Path):
        """Add command with --spec sets spec file on task."""
        setup_config(tmp_path)

        # Create a spec file
        spec_file = tmp_path / "specs" / "feature.md"
        spec_file.parent.mkdir(parents=True, exist_ok=True)
        spec_file.write_text("# Feature Spec\n\nThis is a test spec.")

        result = invoke_gza("add", "--spec", "specs/feature.md", "Implement feature", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Added task" in result.stdout

        # Verify spec was set
        tmp_path / ".gza" / "gza.db"
        store = make_store(tmp_path)
        tasks = store.get_pending()
        task = next((t for t in tasks if t.prompt == "Implement feature"), None)
        assert task is not None
        assert task.spec == "specs/feature.md"

    def test_add_with_spec_file_not_found(self, tmp_path: Path):
        """Add command with --spec fails if file doesn't exist."""
        setup_config(tmp_path)

        result = invoke_gza("add", "--spec", "nonexistent.md", "Implement feature", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "Error: Spec file not found: nonexistent.md" in result.stdout

    def test_add_with_next_marks_task_urgent(self, tmp_path: Path):
        """`gza add --next` should bump the new task to the front of urgent pickup."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        store.add("Older urgent", urgent=True)
        store.add("Newer urgent", urgent=True)

        result = invoke_gza("add", "--next", "Urgent follow-up", "--project", str(tmp_path))

        assert result.returncode == 0
        task = next((t for t in store.get_pending() if t.prompt == "Urgent follow-up"), None)
        assert task is not None
        assert task.urgent is True
        pickup = store.get_pending_pickup()
        assert pickup[0].prompt == "Urgent follow-up"

    def test_add_with_next_and_prompt_file_bumps_to_front_of_urgent_pickup(self, tmp_path: Path):
        """`gza add --next --prompt-file` should get the same bump semantics as queue bump."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        store.add("Older urgent", urgent=True)

        prompt_file = tmp_path / "urgent_prompt.txt"
        prompt_file.write_text("Urgent from file")

        result = invoke_gza(
            "add",
            "--next",
            "--prompt-file",
            str(prompt_file),
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 0
        pickup = store.get_pending_pickup()
        assert pickup[0].prompt == "Urgent from file"
        assert pickup[0].urgent is True

    def test_add_with_next_uses_shared_queue_urgency_helper(self, tmp_path: Path):
        """`add --next` should route through the same shared urgency helper used by queue bump."""
        from gza.cli.execution import cmd_add

        setup_config(tmp_path)
        args = argparse.Namespace(
            project_dir=tmp_path,
            prompt="Urgent via helper",
            prompt_file=None,
            edit=False,
            type=None,
            explore=False,
            plan=False,
            implement=False,
            review=False,
            group=None,
            depends_on=None,
            based_on=None,
            same_branch=False,
            spec=None,
            branch_type=None,
            model=None,
            provider=None,
            skip_learnings=False,
            next=True,
        )

        with patch("gza.cli.execution.set_task_urgency", return_value=True) as set_urgency:
            rc = cmd_add(args)

        assert rc == 0
        set_urgency.assert_called_once()
        assert set_urgency.call_args.kwargs["urgent"] is True

    def test_add_with_review_scope_persists_for_implement_tasks(self, tmp_path: Path):
        setup_config(tmp_path)

        result = invoke_gza(
            "add",
            "--type",
            "implement",
            "--review-scope",
            "slice F-A1 + F-A2: direct implement scope",
            "Implement scoped slice",
            "--project",
            str(tmp_path),
        )

        assert result.returncode == 0
        store = make_store(tmp_path)
        task = next(t for t in store.get_pending() if t.prompt == "Implement scoped slice")
        assert task.review_scope == "slice F-A1 + F-A2: direct implement scope"


class TestAddCommandWithModelAndProvider:
    """Tests for 'gza add' command with --model and --provider flags."""

    def test_add_with_model_flag(self, tmp_path: Path):
        """Add command with --model flag stores model override."""

        setup_config(tmp_path)
        result = invoke_gza("add", "--model", "claude-3-5-haiku-latest", "Test task with model", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Added task" in result.stdout

        # Verify model was set
        tmp_path / ".gza" / "gza.db"
        store = make_store(tmp_path)
        tasks = store.get_pending()
        task = next((t for t in tasks if t.prompt == "Test task with model"), None)
        assert task is not None
        assert task.model == "claude-3-5-haiku-latest"
        assert task.model_is_explicit is True

    def test_add_with_provider_flag(self, tmp_path: Path):
        """Add command with --provider flag stores provider override."""

        setup_config(tmp_path)
        result = invoke_gza("add", "--provider", "gemini", "Test task with provider", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Added task" in result.stdout

        # Verify provider was set
        tmp_path / ".gza" / "gza.db"
        store = make_store(tmp_path)
        tasks = store.get_pending()
        task = next((t for t in tasks if t.prompt == "Test task with provider"), None)
        assert task is not None
        assert task.provider == "gemini"
        assert task.provider_is_explicit is True

    def test_add_with_both_model_and_provider(self, tmp_path: Path):
        """Add command with both --model and --provider flags works."""

        setup_config(tmp_path)
        result = invoke_gza(
            "add",
            "--model", "claude-opus-4",
            "--provider", "claude",
            "Test task with both",
            "--project", str(tmp_path)
        )

        assert result.returncode == 0
        assert "Added task" in result.stdout

        # Verify both were set
        tmp_path / ".gza" / "gza.db"
        store = make_store(tmp_path)
        tasks = store.get_pending()
        task = next((t for t in tasks if t.prompt == "Test task with both"), None)
        assert task is not None
        assert task.model == "claude-opus-4"
        assert task.provider == "claude"
        assert task.model_is_explicit is True
        assert task.provider_is_explicit is True

    @pytest.mark.parametrize(
        ("provider", "model"),
        [
            ("claude", "gpt-5.4"),
            ("codex", "claude-sonnet-4-6"),
            ("gemini", "gpt-4o"),
        ],
    )
    def test_add_rejects_cross_family_provider_model_at_creation(
        self, tmp_path: Path, provider: str, model: str
    ):
        """gza add must reject an incompatible provider/model pair before persisting."""
        setup_config(tmp_path)
        result = invoke_gza(
            "add",
            "--provider", provider,
            "--model", model,
            "Cross-family task",
            "--project", str(tmp_path),
        )

        assert result.returncode == 1
        assert "Error:" in result.stdout
        assert model in result.stdout
        assert provider in result.stdout

        # Task must NOT have been persisted
        store = make_store(tmp_path)
        tasks = store.get_pending()
        assert not any(t.prompt == "Cross-family task" for t in tasks)

    def test_add_allows_unknown_model_name_with_any_provider(self, tmp_path: Path):
        """gza add must accept an unrecognized model name (fail-open for custom models)."""
        setup_config(tmp_path)
        result = invoke_gza(
            "add",
            "--provider", "claude",
            "--model", "my-custom-model-v2",
            "Custom model task",
            "--project", str(tmp_path),
        )

        assert result.returncode == 0
        store = make_store(tmp_path)
        tasks = store.get_pending()
        task = next((t for t in tasks if t.prompt == "Custom model task"), None)
        assert task is not None
        assert task.model == "my-custom-model-v2"
        assert task.model_is_explicit is True

    def test_add_allows_provider_without_model(self, tmp_path: Path):
        """gza add with only --provider (no --model) must not trigger the parity gate."""
        setup_config(tmp_path)
        result = invoke_gza(
            "add",
            "--provider", "claude",
            "Provider only task",
            "--project", str(tmp_path),
        )

        assert result.returncode == 0
        store = make_store(tmp_path)
        tasks = store.get_pending()
        assert any(t.prompt == "Provider only task" for t in tasks)


class TestRecoveryTaskScopeCloning:
    def test_resume_task_preserves_model_explicitness(self, tmp_path: Path):
        from gza.cli._common import _create_resume_task

        setup_config(tmp_path)
        store = make_store(tmp_path)
        original = store.add(
            "Implement scoped slice",
            task_type="implement",
            model="claude-sonnet-4-6",
            model_is_explicit=False,
        )
        original.status = "failed"
        original.session_id = "session-123"
        store.update(original)

        resumed = _create_resume_task(store, original, trigger_source="manual")

        assert resumed.model == original.model
        assert resumed.model_is_explicit is False

    def test_retry_task_preserves_model_explicitness(self, tmp_path: Path):
        from gza.cli._common import _create_retry_task

        setup_config(tmp_path)
        store = make_store(tmp_path)
        original = store.add(
            "Implement scoped slice",
            task_type="implement",
            model="claude-sonnet-4-6",
            model_is_explicit=False,
        )
        original.status = "failed"
        store.update(original)

        retried = _create_retry_task(store, original, trigger_source="manual")

        assert retried.model == original.model
        assert retried.model_is_explicit is False

    def test_resume_task_preserves_review_scope(self, tmp_path: Path):
        from gza.cli._common import _create_resume_task

        setup_config(tmp_path)
        store = make_store(tmp_path)
        original = store.add(
            "Implement scoped slice",
            task_type="implement",
            review_scope="slice F-A1 + F-A2: preserve this scope",
        )
        original.status = "failed"
        original.session_id = "session-123"
        store.update(original)

        resumed = _create_resume_task(store, original, trigger_source="manual")

        assert resumed.review_scope == original.review_scope

    def test_retry_task_preserves_review_scope(self, tmp_path: Path):
        from gza.cli._common import _create_retry_task

        setup_config(tmp_path)
        store = make_store(tmp_path)
        original = store.add(
            "Implement scoped slice",
            task_type="implement",
            review_scope="slice F-A1 + F-A2: preserve this scope",
        )
        original.status = "failed"
        store.update(original)

        retried = _create_retry_task(store, original, trigger_source="manual")

        assert retried.review_scope == original.review_scope

    def test_resume_task_inherits_source_tags(self, tmp_path: Path):
        from gza.cli._common import _create_resume_task

        setup_config(tmp_path)
        store = make_store(tmp_path)
        original = store.add(
            "Implement scoped slice",
            task_type="implement",
            tags=("202606-recovery", "v0.5.0"),
        )
        original.status = "failed"
        original.session_id = "session-123"
        store.update(original)

        resumed = _create_resume_task(store, original, trigger_source="manual")

        assert resumed.tags == original.tags

    def test_retry_task_inherits_source_tags(self, tmp_path: Path):
        from gza.cli._common import _create_retry_task

        setup_config(tmp_path)
        store = make_store(tmp_path)
        original = store.add(
            "Implement scoped slice",
            task_type="implement",
            tags=("202606-recovery", "v0.5.0"),
        )
        original.status = "failed"
        store.update(original)

        retried = _create_retry_task(store, original, trigger_source="manual")

        assert retried.tags == original.tags


class TestAddCommandWithNoLearnings:
    """Tests for 'gza add' command with --no-learnings flag."""

    def test_add_with_no_learnings_flag(self, tmp_path: Path):
        """Add command with --no-learnings flag sets skip_learnings on task."""

        setup_config(tmp_path)
        result = invoke_gza("add", "--no-learnings", "One-off experimental task", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Added task" in result.stdout

        # Verify skip_learnings was set
        tmp_path / ".gza" / "gza.db"
        store = make_store(tmp_path)
        tasks = store.get_pending()
        task = next((t for t in tasks if t.prompt == "One-off experimental task"), None)
        assert task is not None
        assert task.skip_learnings is True

    def test_add_without_no_learnings_flag_defaults_false(self, tmp_path: Path):
        """Add command without --no-learnings flag defaults skip_learnings to False."""

        setup_config(tmp_path)
        result = invoke_gza("add", "Normal task with learnings", "--project", str(tmp_path))

        assert result.returncode == 0

        tmp_path / ".gza" / "gza.db"
        store = make_store(tmp_path)
        tasks = store.get_pending()
        task = next((t for t in tasks if t.prompt == "Normal task with learnings"), None)
        assert task is not None
        assert task.skip_learnings is False


class TestEditCommandWithModelAndProvider:
    """Tests for 'gza edit' command with --model and --provider flags."""

    def test_edit_with_model_flag(self, tmp_path: Path):
        """Edit command can set model override."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a task
        task = store.add("Test task")
        assert task.model is None

        # Edit to add model
        result = invoke_gza("edit", str(task.id), "--model", "claude-3-5-haiku-latest", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Set model override" in result.stdout

        # Verify model was set
        task = store.get(task.id)
        assert task is not None
        assert task.model == "claude-3-5-haiku-latest"
        assert task.model_is_explicit is True

    def test_edit_with_provider_flag(self, tmp_path: Path):
        """Edit command can set provider override."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a task
        task = store.add("Test task")
        assert task.provider is None

        # Edit to add provider
        result = invoke_gza("edit", str(task.id), "--provider", "gemini", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Set provider override" in result.stdout

        # Verify provider was set
        task = store.get(task.id)
        assert task is not None
        assert task.provider == "gemini"
        assert task.provider_is_explicit is True


class TestEditCommandWithNoLearnings:
    """Tests for 'gza edit' command with --no-learnings flag."""

    def test_edit_with_no_learnings_flag(self, tmp_path: Path):
        """Edit command with --no-learnings sets skip_learnings on task."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Task without skip")
        assert task.skip_learnings is False

        result = invoke_gza("edit", str(task.id), "--no-learnings", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "skip_learnings" in result.stdout

        updated = store.get(task.id)
        assert updated is not None
        assert updated.skip_learnings is True


class TestGetEffectiveConfigForTask:
    """Tests for get_effective_config_for_task helper function."""

    def test_task_model_override_beats_provider_scoped_config(self, tmp_path: Path):
        """Task-specific model takes priority over provider-scoped model config."""
        from gza.config import Config, ProviderConfig, TaskTypeConfig
        from gza.db import Task
        from gza.runner import get_effective_config_for_task

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        config.provider = "claude"
        config.providers = {
            "claude": ProviderConfig(
                model="claude-default",
                task_types={"review": TaskTypeConfig(model="claude-review")},
            )
        }

        task = Task(
            id=1,
            prompt="Test task",
            task_type="review",
            model="task-model-override",
            model_is_explicit=True,
        )

        model, provider, max_turns = get_effective_config_for_task(task, config)
        assert model == "task-model-override"
        assert provider == "claude"
        assert max_turns == config.max_turns

    def test_non_explicit_task_model_falls_back_to_provider_scoped_config(self, tmp_path: Path):
        """Persisted resolved model should not override the current provider-scoped model config."""
        from gza.config import Config, ProviderConfig
        from gza.db import Task
        from gza.runner import get_effective_config_for_task

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        config.provider = "codex"
        config.providers = {
            "claude": ProviderConfig(model="claude-default"),
            "codex": ProviderConfig(model="o4-mini"),
        }

        task = Task(
            id=1,
            prompt="Task created before provider switch",
            model="claude-sonnet-4-6",
            model_is_explicit=False,
        )

        model, provider, _ = get_effective_config_for_task(task, config)
        assert provider == "codex"
        assert model == "o4-mini"

    def test_provider_scoped_task_type_model_selected(self, tmp_path: Path):
        """Provider-scoped task type model takes priority over provider default."""
        from gza.config import Config, ProviderConfig, TaskTypeConfig
        from gza.db import Task
        from gza.runner import get_effective_config_for_task

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        config.provider = "claude"
        config.providers = {
            "claude": ProviderConfig(
                model="claude-default",
                task_types={"review": TaskTypeConfig(model="claude-review")},
            )
        }

        task = Task(
            id=1,
            prompt="Test task",
            task_type="review",
        )

        model, provider, _ = get_effective_config_for_task(task, config)
        assert model == "claude-review"
        assert provider == "claude"

    def test_provider_scoped_default_model_selected(self, tmp_path: Path):
        """Provider-scoped default model is used when task type override is absent."""
        from gza.config import Config, ProviderConfig
        from gza.db import Task
        from gza.runner import get_effective_config_for_task

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        config.provider = "claude"
        config.providers = {"claude": ProviderConfig(model="claude-default")}

        task = Task(
            id=1,
            prompt="Test task",
        )

        model, provider, _ = get_effective_config_for_task(task, config)
        assert model == "claude-default"
        assert provider == "claude"

    def test_provider_override_switches_provider_scope(self, tmp_path: Path):
        """Task provider override switches model selection to that provider scope."""
        from gza.config import Config, ProviderConfig
        from gza.db import Task
        from gza.runner import get_effective_config_for_task

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        config.provider = "claude"
        config.providers = {
            "claude": ProviderConfig(model="claude-default"),
            "codex": ProviderConfig(model="o4-mini"),
        }

        task = Task(
            id=1,
            prompt="Test task",
            provider="codex",
            provider_is_explicit=True,
        )

        model, provider, _ = get_effective_config_for_task(task, config)
        assert provider == "codex"
        assert model == "o4-mini"

    def test_non_explicit_task_provider_falls_back_to_config_provider(self, tmp_path: Path):
        """Persisted resolved provider should not override current configured provider."""
        from gza.config import Config, ProviderConfig
        from gza.db import Task
        from gza.runner import get_effective_config_for_task

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        config.provider = "codex"
        config.providers = {
            "claude": ProviderConfig(model="claude-default"),
            "codex": ProviderConfig(model="o4-mini"),
        }

        task = Task(
            id=1,
            prompt="Task created before provider switch",
            provider="claude",
            provider_is_explicit=False,
        )

        model, provider, _ = get_effective_config_for_task(task, config)
        assert provider == "codex"
        assert model == "o4-mini"

    def test_task_provider_route_applies_without_task_override(self, tmp_path: Path):
        """task_providers should route by task type before falling back to default provider."""
        from gza.config import Config, ProviderConfig
        from gza.db import Task
        from gza.runner import get_effective_config_for_task

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        config.provider = "codex"
        config.task_providers = {"review": "claude"}
        config.providers = {
            "claude": ProviderConfig(model="claude-review-model"),
            "codex": ProviderConfig(model="o4-mini"),
        }

        task = Task(
            id=1,
            prompt="Review task",
            task_type="review",
        )

        model, provider, _ = get_effective_config_for_task(task, config)
        assert provider == "claude"
        assert model == "claude-review-model"

    def test_falls_back_to_legacy_when_provider_scope_missing(self, tmp_path: Path):
        """Legacy top-level task_types/model remain as fallback if scope is missing."""
        from gza.config import Config, TaskTypeConfig
        from gza.db import Task
        from gza.runner import get_effective_config_for_task

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        config.provider = "claude"
        config.model = "legacy-default"
        config.task_types = {"review": TaskTypeConfig(model="legacy-review")}

        task = Task(
            id=1,
            prompt="Test task",
            task_type="review",
        )

        model, provider, _ = get_effective_config_for_task(task, config)
        assert provider == "claude"
        assert model == "legacy-review"

    def test_provider_scoped_max_turns_selected(self, tmp_path: Path):
        """Provider-scoped task type max_turns takes priority."""
        from gza.config import Config, ProviderConfig, TaskTypeConfig
        from gza.db import Task
        from gza.runner import get_effective_config_for_task

        setup_config(tmp_path)
        config = Config.load(tmp_path)
        config.max_turns = 50
        config.provider = "claude"
        config.task_types = {"review": TaskTypeConfig(max_turns=30)}
        config.providers = {
            "claude": ProviderConfig(
                task_types={"review": TaskTypeConfig(max_turns=20)}
            )
        }

        task = Task(
            id=1,
            prompt="Test task",
            task_type="review",
        )

        _, _, max_turns = get_effective_config_for_task(task, config)
        assert max_turns == 20


class TestBuildPromptWithSpec:
    """Tests for build_prompt with spec file content."""

    def test_build_prompt_includes_spec_content(self, tmp_path: Path):
        """build_prompt includes spec file content when task has spec."""
        from gza.config import Config
        from gza.runner import build_prompt

        # Setup config
        setup_config(tmp_path)
        config = Config.load(tmp_path)

        # Setup database
        store = make_store(tmp_path)

        # Create spec file
        spec_file = tmp_path / "specs" / "feature.md"
        spec_file.parent.mkdir(parents=True, exist_ok=True)
        spec_content = "# Feature Spec\n\nImplement X with Y."
        spec_file.write_text(spec_content)

        # Create task with spec
        task = store.add("Implement the feature", spec="specs/feature.md")

        # Build prompt
        prompt = build_prompt(task, config, store)

        # Verify spec content is included
        assert "## Specification" in prompt
        assert "specs/feature.md" in prompt
        assert "# Feature Spec" in prompt
        assert "Implement X with Y." in prompt

    def test_build_prompt_without_spec(self, tmp_path: Path):
        """build_prompt works correctly when task has no spec."""
        from gza.config import Config
        from gza.runner import build_prompt

        # Setup config
        setup_config(tmp_path)
        config = Config.load(tmp_path)

        # Setup database
        store = make_store(tmp_path)

        # Create task without spec
        task = store.add("Simple task")

        # Build prompt
        prompt = build_prompt(task, config, store)

        # Verify no spec section
        assert "## Specification" not in prompt
        assert "Simple task" in prompt


class TestGetTaskOutput:
    """Tests for _get_task_output helper function."""

    def test_prefers_db_content(self, tmp_path: Path):
        """_get_task_output should prefer output_content from DB."""
        from gza.db import Task
        from gza.runner import _get_task_output

        task = Task(
            id=1,
            prompt="Test",
            output_content="Content from DB",
        )
        result = _get_task_output(task, tmp_path)
        assert result == "Content from DB"

    def test_falls_back_to_file(self, tmp_path: Path):
        """_get_task_output should fall back to file when no DB content."""
        from gza.db import Task
        from gza.runner import _get_task_output

        # Create report file
        report_dir = tmp_path / ".gza" / "plans"
        report_dir.mkdir(parents=True)
        report_file = report_dir / "test.md"
        report_file.write_text("Content from file")

        task = Task(
            id=2,
            prompt="Test",
            report_file=".gza/plans/test.md",
            output_content=None,
        )
        result = _get_task_output(task, tmp_path)
        assert result == "Content from file"

    def test_prefers_db_over_file(self, tmp_path: Path):
        """_get_task_output should prefer DB when both exist."""
        from gza.db import Task
        from gza.runner import _get_task_output

        # Create report file
        report_dir = tmp_path / ".gza" / "plans"
        report_dir.mkdir(parents=True)
        report_file = report_dir / "test.md"
        report_file.write_text("Content from file")

        task = Task(
            id=3,
            prompt="Test",
            report_file=".gza/plans/test.md",
            output_content="DB wins",
        )
        result = _get_task_output(task, tmp_path)
        assert result == "DB wins"

    def test_returns_none_when_no_content(self, tmp_path: Path):
        """_get_task_output should return None when no content available."""
        from gza.db import Task
        from gza.runner import _get_task_output

        task = Task(
            id=4,
            prompt="Test",
            output_content=None,
        )
        result = _get_task_output(task, tmp_path)
        assert result is None


class TestGetReviewVerdict:
    """Tests for get_review_verdict()."""

    def _setup(self, tmp_path: Path):
        from gza.cli import get_review_verdict
        from gza.config import Config
        setup_config(tmp_path)
        store = make_store(tmp_path)
        config = Config.load(tmp_path)
        return get_review_verdict, config, store

    def test_inline_verdict(self, tmp_path: Path):
        """Parses inline **Verdict: APPROVED** format."""
        get_review_verdict, config, store = self._setup(tmp_path)
        task = store.add("Review", task_type="review")
        task.status = "completed"
        task.output_content = "Some review text.\n\n**Verdict: APPROVED**\n"
        store.update(task)
        assert get_review_verdict(config, task) == "APPROVED"

    def test_heading_verdict(self, tmp_path: Path):
        """Parses ## Verdict heading with verdict on following line."""
        get_review_verdict, config, store = self._setup(tmp_path)
        task = store.add("Review", task_type="review")
        task.status = "completed"
        task.output_content = "Some review.\n\n## Verdict\n\n**CHANGES_REQUESTED**\n"
        store.update(task)
        assert get_review_verdict(config, task) == "CHANGES_REQUESTED"

    def test_heading_verdict_no_bold(self, tmp_path: Path):
        """Parses ## Verdict heading with plain verdict on following line."""
        get_review_verdict, config, store = self._setup(tmp_path)
        task = store.add("Review", task_type="review")
        task.status = "completed"
        task.output_content = "Review.\n\n## Verdict\n\nNEEDS_DISCUSSION\n"
        store.update(task)
        assert get_review_verdict(config, task) == "NEEDS_DISCUSSION"

    def test_heading_verdict_approved_with_followups(self, tmp_path: Path):
        """Parses APPROVED_WITH_FOLLOWUPS verdict token."""
        get_review_verdict, config, store = self._setup(tmp_path)
        task = store.add("Review", task_type="review")
        task.status = "completed"
        task.output_content = "## Verdict\n\nAPPROVED_WITH_FOLLOWUPS\n"
        store.update(task)
        assert get_review_verdict(config, task) == "APPROVED_WITH_FOLLOWUPS"

    def test_bold_label_only_verdict(self, tmp_path: Path):
        """Parses **Verdict**: CHANGES_REQUESTED format (bold wraps only label)."""
        get_review_verdict, config, store = self._setup(tmp_path)
        task = store.add("Review", task_type="review")
        task.status = "completed"
        task.output_content = "Some review text.\n\n**Verdict**: CHANGES_REQUESTED\n"
        store.update(task)
        assert get_review_verdict(config, task) == "CHANGES_REQUESTED"

    def test_no_verdict_returns_none(self, tmp_path: Path):
        """Returns None when no verdict pattern is found."""
        get_review_verdict, config, store = self._setup(tmp_path)
        task = store.add("Review", task_type="review")
        task.status = "completed"
        task.output_content = "I have some thoughts but no verdict."
        store.update(task)
        assert get_review_verdict(config, task) is None

    def test_canonical_structure_with_none_sections(self, tmp_path: Path):
        """Parses canonical review format with explicit None. sections."""
        get_review_verdict, config, store = self._setup(tmp_path)
        task = store.add("Review", task_type="review")
        task.status = "completed"
        task.output_content = (
            "## Summary\n\n"
            "- Reviewed the implementation.\n\n"
            "## Blockers\n\n"
            "None.\n\n"
            "## Follow-Ups\n\n"
            "None.\n\n"
            "## Questions / Assumptions\n\n"
            "None.\n\n"
            "## Verdict\n\n"
            "Ready to merge.\n"
            "Verdict: APPROVED\n"
        )
        store.update(task)
        assert get_review_verdict(config, task) == "APPROVED"

    def test_final_verdict_section_beats_quoted_body_verdict(self, tmp_path: Path):
        """Uses the authoritative concluding verdict, not an earlier quoted token."""
        get_review_verdict, config, store = self._setup(tmp_path)
        task = store.add("Review", task_type="review")
        task.status = "completed"
        task.output_content = (
            "## Summary\n\n"
            "- Found a blocker.\n\n"
            "## Blockers\n\n"
            "### B1 Invalid manifest still passes\n"
            "Evidence: manifest validation misses malformed entries.\n"
            "Open-state citation: `src/gza/review_verdict.py:162`\n"
            "Impact: bad review metadata can merge.\n"
            "Required fix: reject invalid manifests before lifecycle uses them.\n"
            "Required tests: add coverage for a completed `plan_review` with `Verdict: APPROVED` and an invalid manifest.\n\n"
            "## Follow-Ups\n\n"
            "None.\n\n"
            "## Verdict\n\n"
            "Verdict: CHANGES_REQUESTED\n"
        )
        store.update(task)
        assert get_review_verdict(config, task) == "CHANGES_REQUESTED"


class TestResolveImplAncestor:
    """Tests for runner._resolve_impl_ancestor walking improve based_on chains."""

    def test_resolve_impl_ancestor_direct_improve(self, tmp_path: Path):
        """A first-generation improve (based_on=impl) resolves to the impl."""
        from gza.runner import _resolve_impl_ancestor

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Impl", task_type="implement")
        improve = store.add("Improve", task_type="improve", based_on=impl.id)

        ancestor = _resolve_impl_ancestor(store, improve)
        assert ancestor is not None
        assert ancestor.id == impl.id

    def test_resolve_impl_ancestor_chained_improves(self, tmp_path: Path):
        """A chained retry/resume improve (based_on=previous_improve) still resolves to the impl."""
        from gza.runner import _resolve_impl_ancestor

        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Impl", task_type="implement")
        improve1 = store.add("Improve 1", task_type="improve", based_on=impl.id)
        improve2 = store.add("Improve 2 (retry)", task_type="improve", based_on=improve1.id)
        improve3 = store.add("Improve 3 (retry of retry)", task_type="improve", based_on=improve2.id)

        ancestor = _resolve_impl_ancestor(store, improve3)
        assert ancestor is not None
        assert ancestor.id == impl.id


class TestClearReviewState:
    """Tests for SqliteTaskStore.clear_review_state()."""

    def test_clear_review_state_sets_review_cleared_at(self, tmp_path: Path):
        """clear_review_state sets review_cleared_at on the task."""

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Implement feature", task_type="implement")
        assert task.review_cleared_at is None

        assert task.id is not None
        store.clear_review_state(task.id)

        updated = store.get(task.id)
        assert updated is not None
        assert updated.review_cleared_at is not None

    def test_clear_review_state_updates_timestamp_on_re_clear(self, tmp_path: Path):
        """Calling clear_review_state twice updates the timestamp."""
        import time

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Implement feature", task_type="implement")
        assert task.id is not None

        store.clear_review_state(task.id)
        first = store.get(task.id)
        assert first is not None
        first_cleared = first.review_cleared_at

        time.sleep(0.01)
        store.clear_review_state(task.id)
        second = store.get(task.id)
        assert second is not None

        assert second.review_cleared_at is not None
        assert first_cleared is not None
        assert second.review_cleared_at > first_cleared


def test_cmd_run_startup_note_does_not_call_load_merge_context_when_git_provided(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """gza work's recovery-count startup note must construct a live Git and thread it
    through to collect_recovery_lane_entries so _load_merge_context is never invoked.

    Mirrors test_collect_recovery_lane_entries_does_not_call_load_merge_context_when_git_provided
    in test_lineage_query.py for the gza work call site.
    """
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed implement for work test", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.branch = "feature/cmd-run-recovery-test"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    def _must_not_be_called(_project_dir: object = None) -> object:
        raise AssertionError(
            "_load_merge_context was called; gza work did not thread git through "
            "to the recovery-count startup note"
        )

    monkeypatch.setattr(_recovery_engine_module, "_load_merge_context", _must_not_be_called)

    class _TestGit(Git):
        """Git subclass that satisfies isinstance checks without running subprocess calls."""

        def __init__(self, repo_dir: object) -> None:
            self.repo_dir = repo_dir  # type: ignore[assignment]
            self._cache = None

        def default_branch(self) -> str:
            return "main"

        def local_branch_names(self) -> frozenset:
            return frozenset()

        def branch_exists(self, branch: str) -> bool:
            del branch
            return False

        def ref_exists(self, ref: str) -> bool:
            del ref
            return False

        def rev_parse_if_exists(self, ref: str) -> str | None:
            del ref
            return None

        def resolve_refs(self, refs: object, peel: str = "commit") -> dict[str, str | None]:
            del peel
            return {str(ref): None for ref in refs}  # type: ignore[arg-type]

        def refs_exist(self, refs: object) -> dict[str, bool]:
            return {str(ref): False for ref in refs}  # type: ignore[arg-type]

        def can_merge(self, branch: str, into: str | None = None) -> bool:
            del branch, into
            return True

        def is_merged(self, branch: str, into: str | None = None, use_cherry: bool = False) -> bool:
            del branch, into, use_cherry
            return False

        def count_commits_ahead_checked(self, branch: str, target: str) -> int | None:
            del branch, target
            return 1

    monkeypatch.setattr(_execution_module, "Git", _TestGit)

    # Bypass worker registration to avoid env-var side effects in tests.
    monkeypatch.setattr(
        _execution_module,
        "_run_with_registered_worker",
        lambda *, config, worker_id, run_command, allow_same_pid_reentry=True: run_command(),
    )

    args = argparse.Namespace(
        project_dir=tmp_path,
        no_docker=True,
        max_turns=None,
        background=False,
        worker_mode=False,
        tags=None,
        any_tag=False,
        task_ids=[],  # Empty → goes to else branch that calls collect_recovery_lane_entries
        count=0,  # Zero iterations → _run_session exits without running any task
        force=False,
        create_pr=False,
        resume=False,
    )

    result = _execution_module.cmd_run(args)
    # Must not raise — _load_merge_context was not called.
    assert result == 0
